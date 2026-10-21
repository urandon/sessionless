// Package nativeharnessrelay supplies the contained, opaque loopback-to-Unix
// byte relay. It carries no credentials, destination selection, CONNECT parser,
// TLS termination, current Session authority or provider-success evidence.
package nativeharnessrelay

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"path/filepath"
	"sync"
	"time"
)

const bufferBytes = 8 << 10

var (
	ErrConfig    = errors.New("invalid native relay configuration")
	ErrLimit     = errors.New("native relay aggregate limit exceeded")
	ErrIO        = errors.New("native relay IO failed")
	ErrNotJoined = errors.New("native relay workers have not joined")
)

// Limits are conservative ceilings for this source-only candidate, not measured
// production sizing or process-quota evidence. Both byte budgets include all
// opaque bytes, including native CONNECT framing, and never renew per connection.
type Limits struct {
	Connections                uint32
	BytesToUnix, BytesFromUnix uint64
	Active                     time.Duration
}

type dependencies struct {
	listen func(context.Context) (net.Listener, error)
	dial   func(context.Context, string) (net.Conn, error)
}

// Relay is one original local byte relay, never another workload supervisor.
// Its owner must Close and join it before treating the containing workload as
// quiescent. A copied value cannot operate the original sockets.
type Relay struct {
	self     *Relay
	ctx      context.Context
	cancel   context.CancelFunc
	listener net.Listener
	address  netip.AddrPort
	socket   string
	deps     dependencies
	limit    uint32

	mu           sync.Mutex
	used         uint32
	to, from     uint64
	manual       bool
	err          error
	connections  map[net.Conn]struct{}
	shutdownOnce sync.Once
	workers      sync.WaitGroup
	done         chan struct{}
}

// Start opens only TCP4 127.0.0.1:0, then relays to this exact immutable mounted
// Unix locator. The path is not proof of an original Session or socket identity:
// the trusted host companion still owns those checks. No proxy environment,
// DNS, alternate address, fallback or reconnect path is consulted.
func Start(ctx context.Context, socketPath string, limits Limits) (*Relay, error) {
	return start(ctx, socketPath, limits, dependencies{
		listen: func(ctx context.Context) (net.Listener, error) {
			return (&net.ListenConfig{}).Listen(ctx, "tcp4", "127.0.0.1:0")
		},
		dial: func(ctx context.Context, path string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		},
	})
}

func start(ctx context.Context, socketPath string, limits Limits, deps dependencies) (*Relay, error) {
	if ctx == nil || ctx.Err() != nil || !validSocket(socketPath) || deps.listen == nil || deps.dial == nil ||
		limits.Connections == 0 || limits.Connections > 8 || limits.BytesToUnix == 0 || limits.BytesToUnix > 64<<20 ||
		limits.BytesFromUnix == 0 || limits.BytesFromUnix > 64<<20 || limits.Active <= 0 || limits.Active > time.Minute {
		return nil, ErrConfig
	}
	active, cancel := context.WithTimeout(ctx, limits.Active)
	listener, err := deps.listen(active)
	if err != nil || listener == nil {
		cancel()
		if listener != nil {
			err = errors.Join(err, listener.Close())
		}
		return nil, errors.Join(ErrIO, err)
	}
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || address == nil || address.Zone != "" || !address.IP.Equal(net.IPv4(127, 0, 0, 1)) || address.Port <= 0 || address.Port > 65535 || active.Err() != nil {
		activeErr := active.Err()
		cancel()
		return nil, errors.Join(ErrConfig, activeErr, listener.Close())
	}
	r := &Relay{ctx: active, cancel: cancel, listener: listener, socket: socketPath, deps: deps,
		address: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(address.Port)), limit: limits.Connections,
		to: limits.BytesToUnix, from: limits.BytesFromUnix, connections: make(map[net.Conn]struct{}), done: make(chan struct{})}
	r.self = r
	context.AfterFunc(active, r.shutdown)
	go r.run()
	return r, nil
}

func validSocket(path string) bool {
	if !filepath.IsAbs(path) || path == string(filepath.Separator) || len(path) > 100 || filepath.Clean(path) != path {
		return false
	}
	for _, b := range []byte(path) {
		if b <= ' ' || b > '~' {
			return false
		}
	}
	return true
}

// Address reports actual Listen readiness only; it confers no execution or
// network authority and does not attest that the original relay is still live.
func (r *Relay) Address() netip.AddrPort {
	if r == nil || r.self != r {
		return netip.AddrPort{}
	}
	return r.address
}

// Wait observes completion without granting more time or cancelling the relay.
// A failed join is never successful cleanup; a later Wait may observe completion.
func (r *Relay) Wait(ctx context.Context) error {
	if r == nil || r.self != r || ctx == nil {
		return ErrConfig
	}
	select {
	case <-r.done:
	case <-ctx.Done():
		select {
		case <-r.done:
		default:
			return errors.Join(ErrNotJoined, ctx.Err())
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

// Close cancels only this original relay and waits within the caller's cleanup
// bound. It never stops any daemon, model, engine or unrelated workload.
func (r *Relay) Close(ctx context.Context) error {
	if r == nil || r.self != r || ctx == nil {
		return ErrConfig
	}
	r.mu.Lock()
	if r.ctx.Err() == nil {
		r.manual = true
	}
	r.mu.Unlock()
	r.cancel()
	return r.Wait(ctx)
}

func (r *Relay) record(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	r.err = errors.Join(r.err, err)
	r.mu.Unlock()
}

func (r *Relay) fail(err error) {
	r.record(err)
	r.cancel()
}

func (r *Relay) closeConn(conn net.Conn) {
	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrClosedPipe) {
		r.fail(errors.Join(ErrIO, err))
	}
}

func (r *Relay) shutdown() {
	r.shutdownOnce.Do(func() {
		if err := r.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			r.fail(errors.Join(ErrIO, err))
		}
		r.mu.Lock()
		owned := make([]net.Conn, 0, len(r.connections))
		for conn := range r.connections {
			owned = append(owned, conn)
		}
		r.mu.Unlock()
		for _, conn := range owned {
			r.closeConn(conn)
		}
	})
}

func (r *Relay) run() {
	defer close(r.done)
	defer r.cancel()
	for r.ctx.Err() == nil {
		client, err := r.listener.Accept()
		if err != nil || client == nil {
			if client != nil {
				r.closeConn(client)
			}
			if r.ctx.Err() == nil {
				r.fail(errors.Join(ErrIO, err))
			}
			break
		}
		r.mu.Lock()
		admitted := r.ctx.Err() == nil && r.used < r.limit
		if admitted {
			r.used++ // consumed before Unix dial; failure/EOF never refunds a slot
			r.connections[client] = struct{}{}
		}
		r.mu.Unlock()
		if !admitted {
			r.closeConn(client)
			if r.ctx.Err() == nil {
				r.fail(ErrLimit)
			}
			break
		}
		r.workers.Add(1)
		go func() { defer r.workers.Done(); r.relay(client) }()
	}
	r.shutdown()
	r.workers.Wait()
	r.mu.Lock()
	if r.err == nil && !r.manual {
		r.err = r.ctx.Err()
	}
	r.mu.Unlock()
}

func (r *Relay) retire(conn net.Conn) {
	r.closeConn(conn)
	r.mu.Lock()
	delete(r.connections, conn)
	r.mu.Unlock()
}

func (r *Relay) relay(client net.Conn) {
	defer r.retire(client)
	deadline, _ := r.ctx.Deadline()
	if r.ctx.Err() != nil || client.SetDeadline(deadline) != nil {
		if r.ctx.Err() == nil {
			r.fail(ErrIO)
		}
		return
	}
	peer, err := r.deps.dial(r.ctx, r.socket)
	if err != nil || peer == nil {
		if peer != nil {
			r.closeConn(peer)
		}
		if r.ctx.Err() == nil {
			r.fail(errors.Join(ErrIO, err))
		}
		return
	}
	defer r.retire(peer)
	r.mu.Lock()
	current := r.ctx.Err() == nil
	if current {
		r.connections[peer] = struct{}{}
	}
	r.mu.Unlock()
	if !current || peer.SetDeadline(deadline) != nil {
		if current {
			r.fail(ErrIO)
		}
		return
	}
	joined := make(chan error, 2)
	go func() { joined <- r.copy(peer, client, true) }()
	go func() { joined <- r.copy(client, peer, false) }()
	first := <-joined
	r.closeConn(client)
	r.closeConn(peer)
	second := <-joined
	if r.ctx.Err() == nil {
		if first != nil {
			r.fail(first)
		} else if second != nil && !errors.Is(second, net.ErrClosed) && !errors.Is(second, io.ErrClosedPipe) {
			r.fail(second)
		}
	}
}

// Explicit fixed-size copies prevent ReaderFrom/WriterTo optimizations from
// bypassing accounting or allocating buffers outside this structural bound.
// Budget is reserved before reads; concurrent blocked readers can conservatively
// exhaust it early, but can never receive/forward bytes beyond the aggregate cap.
func (r *Relay) copy(dst, src net.Conn, toUnix bool) error {
	buffer := make([]byte, bufferBytes)
	for {
		r.mu.Lock()
		remaining := &r.from
		if toUnix {
			remaining = &r.to
		}
		reserved := min(*remaining, uint64(len(buffer)))
		*remaining -= reserved
		r.mu.Unlock()
		if reserved == 0 {
			return ErrLimit
		}
		n, readErr := src.Read(buffer[:reserved])
		if n < 0 || n > int(reserved) {
			return ErrIO
		}
		r.mu.Lock()
		*remaining += reserved - uint64(n)
		r.mu.Unlock()
		for offset := 0; offset < n; {
			written, err := dst.Write(buffer[offset:n])
			if written < 0 || written > n-offset {
				return ErrIO
			}
			offset += written
			if err != nil {
				return errors.Join(ErrIO, err)
			}
			if written == 0 {
				return errors.Join(ErrIO, io.ErrNoProgress)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return errors.Join(ErrIO, readErr)
		}
		if n == 0 {
			return errors.Join(ErrIO, io.ErrNoProgress)
		}
	}
}
