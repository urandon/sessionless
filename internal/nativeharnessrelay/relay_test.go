package nativeharnessrelay

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type fakeListener struct {
	clients  chan net.Conn
	closed   chan struct{}
	once     sync.Once
	closeErr error
	address  net.Addr
}

func newFakeListener() *fakeListener {
	return &fakeListener{clients: make(chan net.Conn, 8), closed: make(chan struct{}),
		address: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 31217}}
}
func (l *fakeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.clients:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *fakeListener) Close() error   { l.once.Do(func() { close(l.closed) }); return l.closeErr }
func (l *fakeListener) Addr() net.Addr { return l.address }

func limitsFixture() Limits {
	return Limits{Connections: 2, BytesToUnix: 1 << 20, BytesFromUnix: 1 << 20, Active: time.Minute}
}

func fixtureRelay(t *testing.T, limits Limits, dial func(context.Context, string) (net.Conn, error)) (*Relay, *fakeListener) {
	t.Helper()
	l := newFakeListener()
	r, err := start(t.Context(), "/run/exact-native.sock", limits, dependencies{
		listen: func(context.Context) (net.Listener, error) { return l, nil }, dial: dial})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Close(ctx); errors.Is(err, ErrNotJoined) || (err != nil && !errors.Is(err, ErrLimit) && !errors.Is(err, ErrIO) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) {
			t.Errorf("original relay cleanup not joined or unknown: %v", err)
		}
	})
	return r, l
}

func receive[T any](t *testing.T, ch <-chan T, operation string) T {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case got := <-ch:
		return got
	case <-timer.C:
		select {
		case got := <-ch:
			return got
		default:
			t.Fatalf("%s did not join", operation)
			var zero T
			return zero
		}
	}
}

func closeRelay(t *testing.T, r *Relay) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	return r.Close(ctx)
}

func TestRelayTransparentOpaqueBytesAndPartialWrites(t *testing.T) {
	for _, chunk := range []int{1, 7, bufferBytes} {
		t.Run("write-chunk-"+strconv.Itoa(chunk), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// CONNECT, binary TLS-like bytes and a coalesced suffix are opaque.
				request := append([]byte("CONNECT arbitrary.invalid:443 HTTP/1.1\r\nHost: arbitrary.invalid:443\r\n\r\n"), []byte{0x16, 0, 0xff, '\r', '\n', 1}...)
				reply := bytes.Repeat([]byte{0, 0xff, 0x17, 'x'}, 103)
				peerSeen := make(chan []byte, 1)
				peerDone := make(chan error, 1)
				r, l := fixtureRelay(t, limitsFixture(), func(_ context.Context, path string) (net.Conn, error) {
					if path != "/run/exact-native.sock" {
						return nil, errors.New("Unix path changed")
					}
					a, b := net.Pipe()
					t.Cleanup(func() { _ = a.Close(); _ = b.Close(); receive(t, peerDone, "opaque peer") })
					go func() {
						got := make([]byte, len(request))
						_, err := io.ReadFull(b, got)
						peerSeen <- got
						if err == nil {
							_, err = b.Write(reply)
						}
						peerDone <- err
					}()
					return &shortWriter{Conn: a, chunk: chunk}, nil
				})
				a, b := net.Pipe()
				t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
				l.clients <- a
				clientDone := make(chan error, 1)
				go func() { _, err := b.Write(request); clientDone <- err }()
				got := make([]byte, len(reply))
				if _, err := io.ReadFull(b, got); err != nil || !bytes.Equal(got, reply) {
					t.Fatalf("opaque reply changed: %x err=%v", got, err)
				}
				if got := receive(t, peerSeen, "opaque request"); !bytes.Equal(got, request) {
					t.Fatalf("request changed: %x", got)
				}
				if err := receive(t, clientDone, "opaque writer"); err != nil {
					t.Fatal(err)
				}
				if got := r.Address(); got != netip.MustParseAddrPort("127.0.0.1:31217") {
					t.Fatalf("Address=%v", got)
				}
				if err := closeRelay(t, r); err != nil {
					t.Fatalf("opaque relay close: %v", err)
				}
			})
		})
	}
}

type shortWriter struct {
	net.Conn
	chunk int
}

func (w *shortWriter) Write(p []byte) (int, error) { return w.Conn.Write(p[:min(len(p), w.chunk)]) }

func TestRelayConnectionSlotsNeverReplenish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var dials atomic.Int32
		peerEnds := make(chan struct{}, 2)
		dialReady := make(chan struct{}, 2)
		r, l := fixtureRelay(t, limitsFixture(), func(context.Context, string) (net.Conn, error) {
			dials.Add(1)
			a, b := net.Pipe()
			go func() { defer b.Close(); _, _ = io.Copy(io.Discard, b); peerEnds <- struct{}{} }()
			dialReady <- struct{}{}
			return a, nil
		})
		for range 2 {
			a, b := net.Pipe()
			l.clients <- a
			receive(t, dialReady, "original Unix dial")
			// A completed opaque connection still consumes its original slot.
			_ = b.Close()
			receive(t, peerEnds, "closed peer")
		}
		a, b := net.Pipe()
		defer b.Close()
		l.clients <- a
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := r.Wait(ctx); !errors.Is(err, ErrLimit) {
			t.Fatalf("third accepted connection: %v; want ErrLimit", err)
		}
		if dials.Load() != 2 {
			t.Fatalf("Unix dials=%d, want 2", dials.Load())
		}
		if n, err := b.Read(make([]byte, 1)); n != 0 || err == nil {
			t.Fatalf("over-limit client remained open: n=%d err=%v", n, err)
		}
	})
}

func TestRelayAggregateByteCapsBothDirections(t *testing.T) {
	for _, toUnix := range []bool{false, true} {
		t.Run(map[bool]string{false: "from-unix", true: "to-unix"}[toUnix], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				limits := limitsFixture()
				limits.BytesFromUnix = 3
				limits.BytesToUnix = 3
				peerReady := make(chan net.Conn, 1)
				r, l := fixtureRelay(t, limits, func(context.Context, string) (net.Conn, error) { a, b := net.Pipe(); peerReady <- b; return a, nil })
				a, client := net.Pipe()
				t.Cleanup(func() { _ = a.Close(); _ = client.Close() })
				l.clients <- a
				peer := receive(t, peerReady, "byte-budget peer")
				t.Cleanup(func() { _ = peer.Close() })
				writer, reader := client, peer
				if !toUnix {
					writer, reader = peer, client
				}
				written := make(chan error, 1)
				go func() { _, err := writer.Write([]byte("abcdef")); written <- err }()
				got, err := io.ReadAll(reader)
				if err != nil || string(got) != "abc" {
					t.Fatalf("forwarded=%q err=%v, want exactly capped abc", got, err)
				}
				if err := receive(t, written, "over-budget writer"); err == nil {
					t.Fatal("writer crossed cap")
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				if err := r.Wait(ctx); !errors.Is(err, ErrLimit) {
					t.Fatalf("byte budget error=%v", err)
				}
			})
		})
	}
}

func TestRelayFailedUnixDialRetiresOriginal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := errors.New("exact Unix dial failed")
		var calls atomic.Int32
		r, l := fixtureRelay(t, limitsFixture(), func(context.Context, string) (net.Conn, error) { calls.Add(1); return nil, want })
		a, b := net.Pipe()
		defer b.Close()
		l.clients <- a
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := r.Wait(ctx); !errors.Is(err, want) || !errors.Is(err, ErrIO) {
			t.Fatalf("dial failure lost cause: %v", err)
		}
		if calls.Load() != 1 {
			t.Fatalf("dial retried: %d", calls.Load())
		}
		if n, err := b.Read(make([]byte, 1)); n != 0 || err == nil {
			t.Fatalf("failed-dial client still open: n=%d err=%v", n, err)
		}
	})
}

func TestRelayCancellationJoinsBothCopyDirections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peerReady := make(chan net.Conn, 1)
		r, l := fixtureRelay(t, limitsFixture(), func(context.Context, string) (net.Conn, error) { a, b := net.Pipe(); peerReady <- b; return a, nil })
		a, b := net.Pipe()
		defer b.Close()
		l.clients <- a
		peer := receive(t, peerReady, "blocked peer")
		defer peer.Close()
		if err := closeRelay(t, r); err != nil {
			t.Fatalf("cancel+join: %v", err)
		}
		for _, conn := range []net.Conn{b, peer} {
			if n, err := conn.Read(make([]byte, 1)); n != 0 || err == nil {
				t.Fatalf("joined connection still open: n=%d err=%v", n, err)
			}
		}
		if err := r.Wait(t.Context()); err != nil {
			t.Fatalf("joined Wait changed result: %v", err)
		}
		// A facade carrying the same socket/context locators is not the original
		// handle. Avoid copying its live mutex/WaitGroup merely to test identity.
		copied := &Relay{self: r, address: r.address, ctx: r.ctx, done: r.done, cancel: r.cancel, listener: r.listener}
		if copied.Address().IsValid() || !errors.Is(copied.Close(t.Context()), ErrConfig) || !errors.Is(copied.Wait(t.Context()), ErrConfig) {
			t.Fatal("copied relay facade operated original identity")
		}
	})
}

func TestRelayLateDialCannotCertifyCleanupBeforeJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dialStarted, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		defer unblock()
		peerReady := make(chan net.Conn, 1)
		r, l := fixtureRelay(t, limitsFixture(), func(context.Context, string) (net.Conn, error) {
			close(dialStarted)
			<-release
			a, b := net.Pipe()
			peerReady <- b
			return a, nil
		})
		a, b := net.Pipe()
		defer b.Close()
		l.clients <- a
		receive(t, dialStarted, "started dial")
		expired, cancel := context.WithCancel(t.Context())
		cancel()
		if err := r.Close(expired); !errors.Is(err, ErrNotJoined) {
			t.Fatalf("unjoined dial certified clean: %v", err)
		}
		unblock()
		peer := receive(t, peerReady, "late peer")
		defer peer.Close()
		if err := closeRelay(t, r); err != nil {
			t.Fatalf("late peer did not join: %v", err)
		}
		if n, err := peer.Read(make([]byte, 1)); n != 0 || err == nil {
			t.Fatalf("late peer kept open: n=%d err=%v", n, err)
		}
	})
}

func TestRelayRetainsUnknownCloseFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := errors.New("unknown close outcome")
		l := newFakeListener()
		l.closeErr = want
		r, err := start(t.Context(), "/run/native.sock", limitsFixture(), dependencies{listen: func(context.Context) (net.Listener, error) { return l, nil }, dial: func(context.Context, string) (net.Conn, error) { return nil, errors.New("unused") }})
		if err != nil {
			t.Fatal(err)
		}
		if err := closeRelay(t, r); !errors.Is(err, want) || !errors.Is(err, ErrIO) {
			t.Fatalf("unknown cleanup lost: %v", err)
		}
	})
}

type unknownCloseConn struct {
	net.Conn
	err error
}

func (c *unknownCloseConn) Close() error {
	_ = c.Conn.Close()
	return c.err
}

func TestRelayUnknownConnectionCloseRetiresOriginal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := errors.New("original connection close outcome unknown")
		var dials atomic.Int32
		dialReady, peerDone := make(chan struct{}, 1), make(chan struct{}, 1)
		r, listener := fixtureRelay(t, limitsFixture(), func(context.Context, string) (net.Conn, error) {
			dials.Add(1)
			a, b := net.Pipe()
			go func() { defer b.Close(); _, _ = io.Copy(io.Discard, b); peerDone <- struct{}{} }()
			dialReady <- struct{}{}
			return a, nil
		})
		a, client := net.Pipe()
		defer client.Close()
		listener.clients <- &unknownCloseConn{Conn: a, err: want}
		receive(t, dialReady, "original connection dial")
		_ = client.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := r.Wait(ctx); !errors.Is(err, want) || !errors.Is(err, ErrIO) || errors.Is(err, ErrNotJoined) {
			t.Fatalf("unknown original close did not retire/join relay: %v", err)
		}
		receive(t, peerDone, "unknown-close original peer")
		receive(t, listener.closed, "unknown-close original listener")
		late, lateClient := net.Pipe()
		defer late.Close()
		defer lateClient.Close()
		listener.clients <- late
		synctest.Wait()
		if dials.Load() != 1 {
			t.Fatalf("unknown close permitted new Unix dial: %d", dials.Load())
		}
	})
}

func TestRelayOriginalDeadlineNeverRenewsAndExpiredWaitRechecksDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		limits := limitsFixture()
		limits.Active = 30 * time.Second
		l := newFakeListener()
		deadlines := make(chan time.Time, 2)
		peers := make(chan net.Conn, 2)
		r, err := start(parent, "/run/native.sock", limits, dependencies{listen: func(context.Context) (net.Listener, error) { return l, nil }, dial: func(ctx context.Context, _ string) (net.Conn, error) {
			d, _ := ctx.Deadline()
			deadlines <- d
			a, b := net.Pipe()
			peers <- b
			return a, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := closeRelay(t, r); errors.Is(err, ErrNotJoined) {
				t.Errorf("expired relay did not join cleanup: %v", err)
			}
		})
		original, _ := parent.Deadline()
		a, b := net.Pipe()
		defer b.Close()
		l.clients <- a
		synctest.Wait()
		if got := <-deadlines; !got.Equal(original) {
			t.Fatalf("first deadline=%v want %v", got, original)
		}
		peer := <-peers
		defer peer.Close()
		time.Sleep(time.Second)
		a2, b2 := net.Pipe()
		defer b2.Close()
		l.clients <- a2
		synctest.Wait()
		if got := <-deadlines; !got.Equal(original) {
			t.Fatalf("next connection renewed deadline: %v", got)
		}
		peer2 := <-peers
		defer peer2.Close()
		if err := r.ctx.Err(); err != nil {
			t.Fatalf("relay retired before the original deadline: %v", err)
		}
		select {
		case <-r.done:
			t.Fatal("relay joined before the original deadline")
		default:
		}
		time.Sleep(time.Second)
		synctest.Wait()
		cancelled, stop := context.WithCancel(t.Context())
		stop()
		// Pinned IO and the active timer expire together. Either can report the
		// bounded failure first; the original clock, not select scheduling,
		// decides that expiry was not early and the workers genuinely joined.
		if time.Now().Before(original) {
			t.Fatal("relay stopped before its authoritative active deadline")
		}
		if err := r.Wait(cancelled); err == nil || errors.Is(err, ErrNotJoined) {
			t.Fatalf("completed expiry lost or unjoined: %v", err)
		}
		for _, conn := range []net.Conn{b, peer, b2, peer2} {
			if n, err := conn.Read(make([]byte, 1)); n != 0 || err == nil {
				t.Fatalf("expiry kept connection alive: n=%d err=%v", n, err)
			}
		}
	})
}

func TestRelayAggregateBytesDoNotRenewAcrossConnections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limits := limitsFixture()
		limits.BytesToUnix = 3
		peers := make(chan net.Conn, 2)
		r, listener := fixtureRelay(t, limits, func(context.Context, string) (net.Conn, error) {
			a, b := net.Pipe()
			peers <- b
			return a, nil
		})
		first, client := net.Pipe()
		defer client.Close()
		listener.clients <- first
		peer := receive(t, peers, "first aggregate peer")
		defer peer.Close()
		written := make(chan error, 1)
		go func() { _, err := client.Write([]byte("a")); written <- err }()
		one := make([]byte, 1)
		if _, err := io.ReadFull(peer, one); err != nil || string(one) != "a" {
			t.Fatalf("first budget byte: %q, %v", one, err)
		}
		if err := receive(t, written, "first aggregate write"); err != nil {
			t.Fatal(err)
		}
		_ = client.Close()
		if tail, err := io.ReadAll(peer); err != nil || len(tail) != 0 {
			t.Fatalf("first connection did not finish exactly: %q, %v", tail, err)
		}
		synctest.Wait()
		second, client2 := net.Pipe()
		defer client2.Close()
		listener.clients <- second
		peer2 := receive(t, peers, "second aggregate peer")
		defer peer2.Close()
		go func() { _, err := client2.Write([]byte("bcd")); written <- err }()
		got, err := io.ReadAll(peer2)
		if err != nil || string(got) != "bc" {
			t.Fatalf("second connection replenished bytes: %q, %v; want bc", got, err)
		}
		if err := receive(t, written, "second aggregate write"); err == nil {
			t.Fatal("second connection exceeded remaining aggregate budget")
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := r.Wait(ctx); !errors.Is(err, ErrLimit) {
			t.Fatalf("aggregate byte failure: %v", err)
		}
	})
}

type fixtureIOConn struct {
	net.Conn
	reader io.Reader
	write  func([]byte) (int, error)
}

func (c *fixtureIOConn) Read(p []byte) (int, error)  { return c.reader.Read(p) }
func (c *fixtureIOConn) Write(p []byte) (int, error) { return c.write(p) }
func (*fixtureIOConn) ReadFrom(io.Reader) (int64, error) {
	panic("ReaderFrom bypassed explicit accounting")
}
func (*fixtureIOConn) WriteTo(io.Writer) (int64, error) {
	panic("WriterTo bypassed explicit accounting")
}

func TestRelayCopyHandlesPartialErrorNoProgressAndMinimumBudget(t *testing.T) {
	want := errors.New("partial write failed")
	for _, kind := range []string{"partial-error", "zero-progress", "one-byte-cap"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var received bytes.Buffer
				r := &Relay{to: 8, from: 8}
				if kind == "one-byte-cap" {
					r.to = 1
				}
				source := &fixtureIOConn{reader: strings.NewReader("abc")}
				target := &fixtureIOConn{write: func(p []byte) (int, error) {
					switch kind {
					case "partial-error":
						n, _ := received.Write(p[:1])
						return n, want
					case "zero-progress":
						return 0, nil
					default:
						return received.Write(p)
					}
				}}
				err := r.copy(target, source, true)
				switch kind {
				case "partial-error":
					if !errors.Is(err, want) || !errors.Is(err, ErrIO) || received.String() != "a" {
						t.Fatalf("partial IO cause/prefix: %q, %v", received.String(), err)
					}
				case "zero-progress":
					if !errors.Is(err, io.ErrNoProgress) || received.Len() != 0 {
						t.Fatalf("zero progress retried or changed bytes: %q, %v", received.String(), err)
					}
				case "one-byte-cap":
					if !errors.Is(err, ErrLimit) || received.String() != "a" {
						t.Fatalf("minimum cap overspent: %q, %v", received.String(), err)
					}
				}
			})
		})
	}
}

func TestRelayConfigurationRejectsBeforeListen(t *testing.T) {
	valid := limitsFixture()
	cases := []struct {
		name, path string
		limits     Limits
	}{
		{"relative", "native.sock", valid}, {"root", "/", valid}, {"cleaning", "/run/../run/native.sock", valid}, {"trailing separator", "/run/native.sock/", valid}, {"NUL", "/run/native\x00.sock", valid}, {"long", "/" + strings.Repeat("x", 100), valid},
	}
	for _, mutate := range []func(*Limits){func(l *Limits) { l.Connections = 0 }, func(l *Limits) { l.Connections = 9 }, func(l *Limits) { l.BytesToUnix = 0 }, func(l *Limits) { l.BytesFromUnix = 0 }, func(l *Limits) { l.BytesToUnix = 64<<20 + 1 }, func(l *Limits) { l.BytesFromUnix = 64<<20 + 1 }, func(l *Limits) { l.Active = 0 }, func(l *Limits) { l.Active = time.Minute + 1 }} {
		l := valid
		mutate(&l)
		cases = append(cases, struct {
			name, path string
			limits     Limits
		}{"bad limits", "/run/native.sock", l})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := start(t.Context(), tc.path, tc.limits, dependencies{listen: func(context.Context) (net.Listener, error) { t.Fatal("invalid config listened"); return nil, nil }, dial: func(context.Context, string) (net.Conn, error) { t.Fatal("invalid config dialed"); return nil, nil }}); !errors.Is(err, ErrConfig) {
				t.Fatalf("config error=%v", err)
			}
		})
	}
	if _, err := Start(nil, "/run/native.sock", valid); !errors.Is(err, ErrConfig) {
		t.Fatalf("nil ctx=%v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Start(ctx, "/run/native.sock", valid); !errors.Is(err, ErrConfig) {
		t.Fatalf("cancelled ctx=%v", err)
	}
	var zero Relay
	if err := zero.Close(t.Context()); !errors.Is(err, ErrConfig) {
		t.Fatalf("unstarted Close=%v", err)
	}
	if err := zero.Wait(t.Context()); !errors.Is(err, ErrConfig) {
		t.Fatalf("unstarted Wait=%v", err)
	}
}

func TestRelayListenerFailureAndForeignBindCloseOriginal(t *testing.T) {
	for _, kind := range []string{"failed-listen", "wrong-address", "expired-during-listen"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				l := newFakeListener()
				want := errors.New("listener creation failed")
				deps := dependencies{listen: func(context.Context) (net.Listener, error) {
					if kind == "failed-listen" {
						return l, want
					}
					if kind == "wrong-address" {
						l.address = &net.TCPAddr{IP: net.IPv4zero, Port: 31217}
					}
					if kind == "expired-during-listen" {
						cancel()
					}
					return l, nil
				}, dial: func(context.Context, string) (net.Conn, error) { t.Fatal("failed listener dialed"); return nil, nil }}
				if _, err := start(ctx, "/run/native.sock", limitsFixture(), deps); err == nil {
					t.Fatal("failed/foreign listener accepted")
				} else if kind == "failed-listen" && !errors.Is(err, want) {
					t.Fatalf("listen cause lost: %v", err)
				}
				receive(t, l.closed, "failed listener close")
			})
		})
	}
}
