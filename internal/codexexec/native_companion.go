package codexexec

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeconnect"
)

var (
	errNativeConduitAuthority = errors.New("native conduit original authority is unavailable")
	errNativeConduitPolicy    = errors.New("native conduit policy is invalid")
	errNativeConduitLimit     = errors.New("native conduit aggregate limit exceeded")
	errNativeConduitIO        = errors.New("native conduit connection failed")
	errNativeConduitStopped   = errors.New("native conduit original execution ended")
)

const nativeConduitBuffer = 8 << 10

// These are private trusted implementation dependencies, never accepted-input
// fields. Production does not accept a resolver/dialer/proxy override.
type nativeCompanionDependencies struct {
	lookup func(context.Context, string) ([]netip.Addr, error)
	dial   func(context.Context, string, string) (net.Conn, error)
}

func nativeCompanionNetwork() nativeCompanionDependencies {
	return nativeCompanionDependencies{
		lookup: func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		},
		dial: (&net.Dialer{}).DialContext,
	}
}

// nativeCompanion is created only under the original consumed prepared entry.
// It owns opaque connections, not an agent loop, credentials, model operations
// or success attestations. Losing its original owner removes permission.
type nativeCompanion struct {
	self   *nativeCompanion
	owner  *nativeConduitOwner
	ctx    context.Context
	cancel context.CancelFunc
	deps   nativeCompanionDependencies
	stop   func() bool

	mu                sync.Mutex
	started, stopping bool
	active            context.Context
	cancelActive      context.CancelFunc
	socket            *attachedworkerdaemon.NativeSocketV2
	connections       map[net.Conn]struct{}
	used              uint64
	inbound, outbound uint64
	err               error
	done              chan struct{}
	doneOnce          sync.Once
	workers           sync.WaitGroup
}

func (*nativeCompanion) String() string     { return "NativeCompanion{original:[redacted]}" }
func (c *nativeCompanion) GoString() string { return c.String() }

func (v *nativeConduitOwner) newCompanion(ctx context.Context, deps nativeCompanionDependencies) (*nativeCompanion, error) {
	if ctx == nil || ctx.Err() != nil || v == nil || v.self != v || v.child == nil ||
		!v.child.Matches(v.attempt, v.authority.ProviderResource) || deps.lookup == nil || deps.dial == nil {
		return nil, errNativeConduitAuthority
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return nil, errNativeConduitAuthority
	}
	if v.currentEnvelopeV2(ctx, v.nativeNowV2()) != nil {
		return nil, errNativeConduitAuthority
	}
	identity, err := verifyNativeCompanionHost(v.conduitCompanion())
	if err != nil || !sameNativeCompanionHost(v.hostIdentity, identity) {
		return nil, errNativeConduitAuthority
	}
	limits := v.conduitLimits()
	if !v.conduitPolicyValid() || limits.Connections > 32 ||
		limits.InboundBytes > 64<<20 || limits.OutboundBytes > 64<<20 ||
		limits.CompanionGoroutines < 3*limits.Connections+2 ||
		limits.CompanionMemoryBytes < limits.Connections*(4*nativeConduitBuffer+nativeconnect.MaxHeaderBytes) {
		return nil, errNativeConduitPolicy
	}
	life, err := v.child.Lifetime()
	if err != nil {
		return nil, errNativeConduitAuthority
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.companion != nil {
		if !v.companion.current() {
			return nil, errNativeConduitAuthority
		}
		return v.companion, nil // share original counters; never replenish them
	}
	call, cancel := context.WithCancel(ctx)
	c := &nativeCompanion{owner: v, ctx: call, cancel: cancel, deps: deps,
		connections: make(map[net.Conn]struct{}), done: make(chan struct{}),
		inbound: limits.InboundBytes, outbound: limits.OutboundBytes}
	c.self = c
	c.stop = context.AfterFunc(life, cancel)
	context.AfterFunc(call, c.shutdown)
	if call.Err() != nil || life.Err() != nil || v.child.Current() != nil {
		cancel()
		c.stop()
		return nil, errNativeConduitAuthority
	}
	v.companion = c
	return c, nil
}

func (c *nativeCompanion) current() bool {
	if c == nil || c.self != c || c.owner == nil || c.owner.self != c.owner || c.ctx.Err() != nil ||
		!nativeCompanionHostCurrent(c.owner.conduitCompanion(), c.owner.hostIdentity) {
		return false
	}
	c.mu.Lock()
	ended := c.endedLocked()
	c.mu.Unlock()
	return !ended && c.owner.child.Matches(c.owner.attempt, c.owner.authority.ProviderResource)
}

// Err notification and an IO deadline may become observable in either order.
// This reads only the original authority; it never creates another lifetime.
func (c *nativeCompanion) endedLocked() bool {
	if c.stopping || c.ctx.Err() != nil {
		return true
	}
	if c.active == nil {
		return false // constructor preflight, before Start installs active
	}
	deadline, bounded := c.active.Deadline()
	return c.active.Err() != nil || bounded && !time.Now().Before(deadline)
}

func (c *nativeCompanion) unavailable() error {
	c.mu.Lock()
	ended := c.endedLocked()
	c.mu.Unlock()
	if ended {
		return errNativeConduitStopped
	}
	return errNativeConduitAuthority
}

type nativeCompanionIOOutcome struct {
	err    error
	closed bool // a sibling's intentional close is handled by the join owner
}

// Observe each operation BEFORE waiting for a sibling. A later deadline must
// never erase an earlier real failure. Only known shutdown errors or a timeout
// at the ORIGINAL active deadline are normal termination, not arbitrary IO,
// malformed CONNECT, aggregate exhaustion or an independently shorter timeout.
func (c *nativeCompanion) observeIO(err, failure error, ioDeadline time.Time) nativeCompanionIOOutcome {
	if err == nil {
		return nativeCompanionIOOutcome{}
	}
	c.mu.Lock()
	ended := c.endedLocked()
	var activeDeadline time.Time
	if c.active != nil {
		activeDeadline, _ = c.active.Deadline()
	}
	c.mu.Unlock()
	closed := errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe)
	var timeout net.Error
	expired := errors.As(err, &timeout) && timeout.Timeout() && !activeDeadline.IsZero() &&
		ioDeadline.Equal(activeDeadline) && !time.Now().Before(activeDeadline)
	if errors.Is(err, errNativeConduitStopped) || expired || ended && (closed || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		c.cancel()
		c.shutdown()
		return nativeCompanionIOOutcome{err: errNativeConduitStopped}
	}
	if errors.Is(err, errNativeConduitLimit) {
		failure = errNativeConduitLimit
	}
	if !closed {
		c.fail(failure)
	}
	return nativeCompanionIOOutcome{err: failure, closed: closed}
}

func (c *nativeCompanion) observeAccept(err error, deadline time.Time) error {
	// This original listener's pre/post-Accept identity check also returns
	// ErrSupervisorConfig when our shutdown has stopped/closed it. Normalize
	// only that wrapper shape after authority ended, never arbitrary IO or a
	// live listener's identity/configuration failure.
	if errors.Is(err, attachedworkerdaemon.ErrSupervisorConfig) && c.unavailable() == errNativeConduitStopped {
		err = errNativeConduitStopped
	}
	return c.observeIO(err, errNativeConduitIO, deadline).err
}

func (c *nativeCompanion) Start(active context.Context, socket *attachedworkerdaemon.NativeSocketV2) error {
	if !c.current() || socket == nil || active == nil || active.Err() != nil {
		return errNativeConduitAuthority
	}
	if c.owner.currentEnvelopeV2(active, c.owner.nativeNowV2()) != nil {
		return errNativeConduitAuthority
	}
	deadline, bounded := active.Deadline()
	ownerDeadline, ownerBounded := c.ctx.Deadline()
	if !bounded || !ownerBounded || deadline.After(ownerDeadline) {
		return errNativeConduitAuthority
	}
	if !c.owner.conduitSocketMatches(socket) {
		return errNativeConduitAuthority
	}
	c.mu.Lock()
	if c.started || c.stopping {
		c.mu.Unlock()
		return errNativeConduitAuthority
	}
	// DNS/dial must stop on either owner, even when this passive supervisor
	// port is tested independently of nativeConduitOwner.run's context join.
	call, cancelActive := context.WithCancel(active)
	c.socket, c.started, c.active, c.cancelActive = socket, true, call, cancelActive
	c.mu.Unlock()
	// Join the original supervisor execution context, not its short-lived
	// preparation context. Its tighter deadline also owns all network activity.
	context.AfterFunc(active, c.cancel)
	ready := make(chan struct{})
	go func() {
		defer c.doneOnce.Do(func() { close(c.done) })
		close(ready)
		for c.current() {
			client, err := socket.AcceptConnectionV2()
			if err != nil {
				if err := c.observeAccept(err, deadline); err != errNativeConduitStopped {
					c.fail(err)
				}
				break
			}
			if err := c.admit(client); err != nil {
				c.retire(client)
				if err != errNativeConduitStopped {
					c.fail(err)
				}
				break
			}
			c.workers.Add(1)
			go func() {
				defer c.workers.Done()
				if err := c.tunnel(client); err != nil {
					c.fail(err)
				}
			}()
		}
		c.shutdown()
		c.workers.Wait()
	}()
	select {
	case <-ready:
		if c.current() {
			return nil
		}
	case <-c.ctx.Done():
	}
	return errNativeConduitAuthority
}

func (c *nativeCompanion) admit(conn net.Conn) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.endedLocked() {
		return errNativeConduitStopped
	}
	if c.active == nil {
		return errNativeConduitAuthority
	}
	if c.used >= c.owner.conduitLimits().Connections {
		return errNativeConduitLimit
	}
	c.used++ // consumed before parsing or DNS; rejected connections never refund
	c.connections[conn] = struct{}{}
	return nil
}

func (c *nativeCompanion) track(conn net.Conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.endedLocked() || c.active == nil {
		return false
	}
	c.connections[conn] = struct{}{}
	return true
}

func (c *nativeCompanion) retire(conn net.Conn) {
	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		c.fail(errNativeConduitIO)
	}
	c.mu.Lock()
	delete(c.connections, conn)
	c.mu.Unlock()
}

func (c *nativeCompanion) tunnel(client net.Conn) (returnErr error) {
	defer func() {
		if returnErr == errNativeConduitStopped {
			c.cancel()
			c.shutdown()
			returnErr = nil // expiry still fails the original supervisor invocation
		}
	}()
	defer c.retire(client)
	if !c.current() {
		return c.unavailable()
	}
	c.mu.Lock()
	active := c.active
	c.mu.Unlock()
	if active == nil || active.Err() != nil {
		return c.unavailable()
	}
	if c.owner.currentEnvelopeV2(active, c.owner.nativeNowV2()) != nil {
		return errNativeConduitAuthority
	}
	// A slow CONNECT cannot occupy its slot for the whole native turn. This
	// tighter handshake bound never renews the original execution lifetime.
	headerDeadline := time.Now().Add(5 * time.Second)
	if deadline, ok := active.Deadline(); ok && deadline.Before(headerDeadline) {
		headerDeadline = deadline
	}
	if err := client.SetDeadline(headerDeadline); err != nil {
		return c.observeIO(err, errNativeConduitIO, headerDeadline).err
	}
	meter := &nativeMeteredConn{Conn: client, owner: c}
	reader := bufio.NewReaderSize(meter, nativeconnect.MaxHeaderBytes)
	host, err := nativeconnect.ReadCONNECT(reader, c.owner.conduitHosts())
	if err != nil {
		if meter.readErr != nil {
			return c.observeIO(meter.readErr, errNativeConduitIO, headerDeadline).err
		}
		return errNativeConduitPolicy
	}
	addresses, err := c.deps.lookup(active, host)
	deadline, _ := active.Deadline()
	if err != nil {
		return c.observeIO(err, errNativeConduitAuthority, deadline).err
	}
	if !c.current() {
		return c.unavailable()
	}
	ip, err := nativeconnect.PinPublicIPv4(addresses)
	if err != nil {
		return errNativeConduitPolicy
	}
	peer, err := c.deps.dial(active, "tcp4", net.JoinHostPort(ip.String(), "443"))
	if err != nil || peer == nil {
		if peer != nil {
			c.retire(peer)
		}
		if err == nil {
			return errNativeConduitIO
		}
		return c.observeIO(err, errNativeConduitIO, deadline).err
	}
	defer c.retire(peer)
	// Verify the actual original connection, not just the requested target.
	remote, ok := peer.RemoteAddr().(*net.TCPAddr)
	if !ok || remote.Port != 443 || remote.Zone != "" || !remote.IP.Equal(net.IP(ip.AsSlice())) {
		return errNativeConduitAuthority
	}
	if !c.current() || !c.track(peer) {
		return c.unavailable()
	}
	for _, conn := range []net.Conn{client, peer} {
		if err := conn.SetDeadline(deadline); err != nil {
			return c.observeIO(err, errNativeConduitAuthority, deadline).err
		}
	}
	if _, err := io.WriteString(meter, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return c.observeIO(err, errNativeConduitIO, deadline).err
	}
	if !c.current() {
		return c.unavailable()
	}
	joined := make(chan nativeCompanionIOOutcome, 2)
	go func() {
		_, err := io.CopyBuffer(peer, reader, make([]byte, nativeConduitBuffer))
		joined <- c.observeIO(err, errNativeConduitIO, deadline)
	}()
	go func() {
		_, err := io.CopyBuffer(meter, peer, make([]byte, nativeConduitBuffer))
		joined <- c.observeIO(err, errNativeConduitIO, deadline)
	}()
	first := <-joined
	if first.err != nil && first.err != errNativeConduitStopped {
		c.fail(first.err)
	}
	c.retire(peer)
	c.retire(client)
	if second := <-joined; second.err != nil && second.err != errNativeConduitStopped && !second.closed {
		c.fail(second.err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err // transport EOF/expiry is not provider outcome or invocation success
}

// Workload-facing traffic, including CONNECT framing/status, shares one
// aggregate budget. Reservations precede IO and unused capacity is refunded;
// concurrent relays cannot overspend by each observing the same remainder.
type nativeMeteredConn struct {
	net.Conn
	owner   *nativeCompanion
	readErr error // original zero-byte read cause before CONNECT parser collapses it
}

func (c *nativeCompanion) reserve(outbound bool, requested int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	remaining := &c.inbound
	if outbound {
		remaining = &c.outbound
	}
	if c.endedLocked() {
		return 0, errNativeConduitStopped
	}
	if c.active == nil {
		return 0, errNativeConduitAuthority
	}
	if *remaining == 0 {
		return 0, errNativeConduitLimit
	}
	n := min(uint64(requested), *remaining)
	*remaining -= n
	return int(n), nil
}

func (c *nativeCompanion) refund(outbound bool, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if outbound {
		c.outbound += uint64(n)
	} else {
		c.inbound += uint64(n)
	}
}

func (m *nativeMeteredConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	reserved, err := m.owner.reserve(true, len(p))
	if err != nil {
		m.readErr = err
		return 0, err
	}
	n, err := m.Conn.Read(p[:reserved])
	m.owner.refund(true, reserved-n)
	if n == 0 {
		m.readErr = err
	}
	return n, err
}
func (m *nativeMeteredConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	reserved, err := m.owner.reserve(false, len(p))
	if err != nil {
		return 0, err
	}
	n, err := m.Conn.Write(p[:reserved])
	m.owner.refund(false, reserved-n)
	if err == nil && n < len(p) {
		err = errNativeConduitLimit
	}
	return n, err
}

func (c *nativeCompanion) fail(err error) {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
	}
	c.mu.Unlock()
	c.cancel()
	c.shutdown()
}

func (c *nativeCompanion) shutdown() {
	c.mu.Lock()
	c.stopping = true
	socket, started, cancelActive := c.socket, c.started, c.cancelActive
	connections := make([]net.Conn, 0, len(c.connections))
	for conn := range c.connections {
		connections = append(connections, conn)
	}
	c.mu.Unlock()
	if cancelActive != nil {
		cancelActive()
	}
	if socket != nil {
		if err := socket.StopAcceptingV2(); err != nil {
			c.mu.Lock()
			if c.err == nil {
				c.err = errNativeConduitIO
			}
			c.mu.Unlock()
		}
	}
	for _, conn := range connections {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			c.mu.Lock()
			if c.err == nil {
				c.err = errNativeConduitIO
			}
			c.mu.Unlock()
		}
	}
	if !started {
		c.doneOnce.Do(func() { close(c.done) })
	}
}

func (c *nativeCompanion) Alive() bool {
	if c == nil || c.self != c {
		return true
	}
	select {
	case <-c.done:
		return false
	default:
		return true
	}
}

func (c *nativeCompanion) Stop(ctx context.Context) error {
	if c == nil || c.self != c || ctx == nil {
		return errNativeConduitAuthority
	}
	c.cancel()
	c.stop()
	c.shutdown()
	select {
	case <-c.done:
	case <-ctx.Done():
		select {
		case <-c.done:
		default:
			return ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}
