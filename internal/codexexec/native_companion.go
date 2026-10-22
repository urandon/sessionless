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
	active := c.active
	c.mu.Unlock()
	return (active == nil || active.Err() == nil) && c.owner.child.Matches(c.owner.attempt, c.owner.authority.ProviderResource)
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
				if c.ctx.Err() == nil {
					c.fail(errNativeConduitIO)
				}
				break
			}
			if !c.admit(client) {
				_ = client.Close()
				c.fail(errNativeConduitLimit)
				break
			}
			c.workers.Add(1)
			go func() {
				defer c.workers.Done()
				if err := c.tunnel(client); err != nil && c.ctx.Err() == nil {
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

func (c *nativeCompanion) admit(conn net.Conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping || c.ctx.Err() != nil || c.active == nil || c.active.Err() != nil || c.used >= c.owner.conduitLimits().Connections {
		return false
	}
	c.used++ // consumed before parsing or DNS; rejected connections never refund
	c.connections[conn] = struct{}{}
	return true
}

func (c *nativeCompanion) track(conn net.Conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping || c.ctx.Err() != nil || c.active == nil || c.active.Err() != nil {
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

func (c *nativeCompanion) tunnel(client net.Conn) error {
	defer c.retire(client)
	if !c.current() {
		return errNativeConduitAuthority
	}
	c.mu.Lock()
	active := c.active
	c.mu.Unlock()
	if active == nil || active.Err() != nil {
		return errNativeConduitAuthority
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
	if client.SetDeadline(headerDeadline) != nil {
		return errNativeConduitIO
	}
	meter := &nativeMeteredConn{Conn: client, owner: c}
	reader := bufio.NewReaderSize(meter, nativeconnect.MaxHeaderBytes)
	host, err := nativeconnect.ReadCONNECT(reader, c.owner.conduitHosts())
	if err != nil {
		return errNativeConduitPolicy
	}
	addresses, err := c.deps.lookup(active, host)
	if err != nil || !c.current() {
		return errNativeConduitAuthority
	}
	ip, err := nativeconnect.PinPublicIPv4(addresses)
	if err != nil {
		return errNativeConduitPolicy
	}
	peer, err := c.deps.dial(active, "tcp4", net.JoinHostPort(ip.String(), "443"))
	if err != nil || peer == nil {
		if peer != nil {
			_ = peer.Close()
		}
		return errNativeConduitIO
	}
	defer c.retire(peer)
	// Verify the actual original connection, not just the requested target.
	remote, ok := peer.RemoteAddr().(*net.TCPAddr)
	if !ok || remote.Port != 443 || remote.Zone != "" || !remote.IP.Equal(net.IP(ip.AsSlice())) ||
		!c.current() || !c.track(peer) {
		return errNativeConduitAuthority
	}
	deadline, ok := active.Deadline()
	if !ok || client.SetDeadline(deadline) != nil || peer.SetDeadline(deadline) != nil {
		return errNativeConduitAuthority
	}
	if _, err := io.WriteString(meter, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil || !c.current() {
		return errNativeConduitIO
	}
	joined := make(chan error, 2)
	go func() {
		_, err := io.CopyBuffer(peer, reader, make([]byte, nativeConduitBuffer))
		joined <- err
	}()
	go func() {
		_, err := io.CopyBuffer(meter, peer, make([]byte, nativeConduitBuffer))
		joined <- err
	}()
	first := <-joined
	_ = peer.Close()
	_ = client.Close()
	second := <-joined
	if c.ctx.Err() != nil {
		return nil // the original owner decides cancellation/terminal semantics
	}
	if errors.Is(first, errNativeConduitLimit) || errors.Is(second, errNativeConduitLimit) {
		return errNativeConduitLimit
	}
	if first != nil || second != nil && !errors.Is(second, net.ErrClosed) && !errors.Is(second, io.ErrClosedPipe) {
		return errNativeConduitIO
	}
	return nil // transport EOF is not provider acceptance/nonacceptance evidence
}

// Workload-facing traffic, including CONNECT framing/status, shares one
// aggregate budget. Reservations precede IO and unused capacity is refunded;
// concurrent relays cannot overspend by each observing the same remainder.
type nativeMeteredConn struct {
	net.Conn
	owner *nativeCompanion
}

func (c *nativeCompanion) reserve(outbound bool, requested int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	remaining := &c.inbound
	if outbound {
		remaining = &c.outbound
	}
	if c.stopping || c.ctx.Err() != nil || c.active == nil || c.active.Err() != nil || *remaining == 0 {
		return 0
	}
	n := min(uint64(requested), *remaining)
	*remaining -= n
	return int(n)
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
	reserved := m.owner.reserve(true, len(p))
	if reserved == 0 {
		return 0, errNativeConduitLimit
	}
	n, err := m.Conn.Read(p[:reserved])
	m.owner.refund(true, reserved-n)
	return n, err
}
func (m *nativeMeteredConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	reserved := m.owner.reserve(false, len(p))
	if reserved == 0 {
		return 0, errNativeConduitLimit
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
