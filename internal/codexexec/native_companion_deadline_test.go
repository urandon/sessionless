//go:build darwin || linux

package codexexec

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

// The original Session/claim/companion constructor is real, but all tunnel IO
// below is net.Pipe. No listener, process, native binary or provider runs.
// The channel deliberately holds the asynchronous owner notification after
// the original standard-library active context and socket deadlines expire.
func TestNativeCompanionDeadlineIOPrecedesOwnerNotification(t *testing.T) {
	testNativeCompanionDeadlineIO(t, false)
}

func TestNativeCompanionDeadlineFirstCloseErrorRemainsSticky(t *testing.T) {
	testNativeCompanionDeadlineIO(t, true)
}

func testNativeCompanionDeadlineIO(t *testing.T, failFirstClose bool) {
	f := newNativeConstructorFixture(t)
	entry, err := NewNativePreparedProcessBoundaryV2(t.Context(), f.config, f.session, f.input, f.bundle)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = entry.native.child.Close() })
	synctest.Test(t, func(t *testing.T) {
		owner, cancelOwner := context.WithTimeout(context.Background(), time.Minute)
		defer cancelOwner()
		client, workload := net.Pipe()
		peer, remote := net.Pipe()
		closeProbe := &companionFirstCloseErrorConn{Conn: peer}
		var peerConn net.Conn = peer
		if failFirstClose {
			peerConn = closeProbe
		}
		t.Cleanup(func() {
			_ = client.Close()
			_ = workload.Close()
			_ = peer.Close()
			_ = remote.Close()
		})
		c, err := entry.native.newCompanion(owner, nativeCompanionDependencies{
			lookup: companionLookup,
			dial: func(context.Context, string, string) (net.Conn, error) {
				return companionPeerConn{peerConn}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		active, cancelActive := context.WithTimeout(owner, time.Second)
		defer cancelActive()
		c.active, c.cancelActive = active, cancelActive
		c.connections[client] = struct{}{}
		c.used = 1
		notificationEntered, releaseNotification, notificationJoined := make(chan struct{}), make(chan struct{}), make(chan struct{})
		context.AfterFunc(active, func() {
			defer close(notificationJoined)
			close(notificationEntered)
			<-releaseNotification
			c.cancel()
		})
		finished := make(chan error, 1)
		joined := make(chan struct{})
		// LIFO cleanup releases both barriers/IO before any bounded join.
		t.Cleanup(func() {
			close(releaseNotification)
			cancelActive()
			_ = client.Close()
			_ = workload.Close()
			_ = peer.Close()
			_ = remote.Close()
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for _, done := range []<-chan struct{}{joined, notificationJoined} {
				select {
				case <-done:
				case <-cleanup.Done():
					select {
					case <-done:
					default:
						t.Error("owned fake tunnel/notification did not join cleanup")
					}
				}
			}
			_ = c.Stop(cleanup)
		})
		go func() {
			defer close(joined)
			err := c.tunnel(client)
			if err != nil && c.ctx.Err() == nil {
				c.fail(err) // same worker error handoff as Start
			}
			finished <- err
		}()
		if _, err := io.WriteString(workload, companionCONNECT); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(workload)
		if status, err := reader.ReadString('\n'); err != nil || status != "HTTP/1.1 200 Connection Established\r\n" {
			t.Fatalf("owned CONNECT status=%q err=%v", status, err)
		}
		if end, err := reader.ReadString('\n'); err != nil || end != "\r\n" {
			t.Fatalf("owned CONNECT terminator=%q err=%v", end, err)
		}
		synctest.Wait() // both original opaque copies are blocked before expiry
		if active.Err() != nil || c.ctx.Err() != nil {
			t.Fatal("fixture terminated before its original active deadline")
		}
		time.Sleep(time.Second) // logical deadline advance, not readiness evidence
		synctest.Wait()
		if owner.Err() != nil || !errors.Is(active.Err(), context.DeadlineExceeded) {
			t.Fatal("fixture did not preserve the tighter original deadline and live outer owner")
		}
		select {
		case <-notificationEntered:
		default:
			t.Fatal("original active deadline did not enter delayed notification")
		}
		select {
		case err := <-finished:
			if !failFirstClose && err != nil {
				t.Fatalf("original deadline IO became a conduit failure before owner notification: %v", err)
			}
		default:
			t.Fatal("original socket deadline did not join both opaque copies")
		}
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		stopErr := c.Stop(cleanup)
		if failFirstClose {
			if !errors.Is(stopErr, errNativeConduitIO) || closeProbe.closeCount() < 2 {
				t.Fatalf("first unknown Close lost after successful retry: closes=%d err=%v", closeProbe.closeCount(), stopErr)
			}
			if err := closeProbe.Close(); err != nil {
				t.Fatalf("owned Close retry did not actually succeed: %v", err)
			}
		} else if stopErr != nil || c.Alive() {
			t.Fatalf("deadline joined conduit was tainted: alive=%t err=%v", c.Alive(), stopErr)
		}
		if c.Alive() {
			t.Fatal("owned original conduit remained alive after joined Stop")
		}
	})
}

// Actually closes the owned pipe but reports an unknown first Close. A later
// successful retry is not evidence that the first result was clean.
type companionFirstCloseErrorConn struct {
	net.Conn
	mu sync.Mutex
	n  int
}

func (c *companionFirstCloseErrorConn) Close() error {
	c.mu.Lock()
	c.n++
	first := c.n == 1
	c.mu.Unlock()
	err := c.Conn.Close()
	if first {
		return errors.New("owned first-close failure")
	}
	return err
}

func (c *companionFirstCloseErrorConn) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func TestNativeCompanionEarlyFaultSurvivesLateSiblingJoin(t *testing.T) {
	for _, limit := range []bool{false, true} {
		name := "IO"
		if limit {
			name = "aggregate-limit"
		}
		t.Run(name, func(t *testing.T) {
			f := newNativeConstructorFixture(t)
			entry, err := NewNativePreparedProcessBoundaryV2(t.Context(), f.config, f.session, f.input, f.bundle)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = entry.native.child.Close() })
			synctest.Test(t, func(t *testing.T) {
				owner, cancelOwner := context.WithTimeout(context.Background(), time.Minute)
				defer cancelOwner()
				client, workload := net.Pipe()
				peer, remote := net.Pipe()
				lateRead, releaseRead, failRead := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var releaseOnce, failOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(releaseRead) }) }
				fail := func() { failOnce.Do(func() { close(failRead) }) }
				clientConn := &companionLateReadConn{Conn: client, entered: lateRead, release: releaseRead}
				peerConn := &companionEarlyFaultConn{Conn: peer, trigger: failRead, limit: limit}
				c, err := entry.native.newCompanion(owner, nativeCompanionDependencies{
					lookup: companionLookup,
					dial: func(context.Context, string, string) (net.Conn, error) {
						return companionPeerConn{peerConn}, nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				active, cancelActive := context.WithTimeout(owner, time.Second)
				c.active, c.cancelActive = active, cancelActive
				c.connections[clientConn] = struct{}{}
				c.used = 1
				if limit {
					// The real status write exhausts this original aggregate. The
					// first opaque inbound byte must then hit reserve's real limit.
					c.inbound = uint64(len("HTTP/1.1 200 Connection Established\r\n\r\n"))
				}
				context.AfterFunc(active, c.cancel)
				finished := make(chan error, 1)
				joined := make(chan struct{})
				// Installed before launch/Fatal: the controlled second Read may
				// ignore Close only until this exact bounded test-owned gate opens.
				t.Cleanup(func() {
					release()
					fail()
					cancelActive()
					_ = client.Close()
					_ = workload.Close()
					_ = peer.Close()
					_ = remote.Close()
					cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					select {
					case <-joined:
					case <-cleanup.Done():
						select {
						case <-joined:
						default:
							t.Error("owned late-sibling tunnel did not join")
						}
					}
					_ = c.Stop(cleanup)
				})
				go func() {
					defer close(joined)
					err := c.tunnel(clientConn)
					if err != nil {
						c.fail(err)
					}
					finished <- err
				}()
				if _, err := io.WriteString(workload, companionCONNECT); err != nil {
					t.Fatal(err)
				}
				reader := bufio.NewReader(workload)
				for _, want := range []string{"HTTP/1.1 200 Connection Established\r\n", "\r\n"} {
					if line, err := reader.ReadString('\n'); err != nil || line != want {
						t.Fatalf("CONNECT response=%q err=%v", line, err)
					}
				}
				synctest.Wait()
				select {
				case <-lateRead:
				default:
					t.Fatal("second opaque copy did not enter owned barrier")
				}
				fail()
				synctest.Wait()
				want := errNativeConduitIO
				if limit {
					want = errNativeConduitLimit
				}
				c.mu.Lock()
				firstErr := c.err
				c.mu.Unlock()
				if !errors.Is(firstErr, want) {
					t.Fatalf("early fault not retained before sibling join: got %v want %v", firstErr, want)
				}
				time.Sleep(time.Second) // cross original bound, not barrier synchronization
				release()
				synctest.Wait()
				select {
				case err := <-finished:
					if !errors.Is(err, want) {
						t.Fatalf("late join erased original fault: got %v want %v", err, want)
					}
				default:
					t.Fatal("owned second copy did not join")
				}
				cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := c.Stop(cleanup); !errors.Is(err, want) {
					t.Fatalf("Stop erased original fault: got %v want %v", err, want)
				}
			})
		})
	}
}

type companionLateReadConn struct {
	net.Conn
	reads   int // one original copy owns Read
	entered chan struct{}
	release <-chan struct{}
}

func (c *companionLateReadConn) Read(p []byte) (int, error) {
	c.reads++
	if c.reads == 1 {
		return c.Conn.Read(p)
	}
	close(c.entered)
	<-c.release
	return 0, net.ErrClosed
}

type companionEarlyFaultConn struct {
	net.Conn
	trigger <-chan struct{}
	limit   bool
}

func (c *companionEarlyFaultConn) Read(p []byte) (int, error) {
	<-c.trigger
	if c.limit {
		p[0] = 'x'
		return 1, nil
	}
	return 0, errors.New("owned early opaque read failure")
}

func TestNativeCompanionShortHeaderDeadlineRemainsFailure(t *testing.T) {
	f := newNativeConstructorFixture(t)
	entry, err := NewNativePreparedProcessBoundaryV2(t.Context(), f.config, f.session, f.input, f.bundle)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = entry.native.child.Close() })
	synctest.Test(t, func(t *testing.T) {
		owner, cancelOwner := context.WithTimeout(context.Background(), time.Minute)
		defer cancelOwner()
		client, workload := net.Pipe()
		c, err := entry.native.newCompanion(owner, nativeCompanionDependencies{
			lookup: companionLookup,
			dial: func(context.Context, string, string) (net.Conn, error) {
				t.Error("short incomplete CONNECT reached dial")
				return nil, errNativeConduitIO
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		active, cancelActive := context.WithTimeout(owner, 10*time.Second)
		c.active, c.cancelActive = active, cancelActive
		c.connections[client] = struct{}{}
		finished, joined := make(chan error, 1), make(chan struct{})
		t.Cleanup(func() {
			cancelActive()
			_ = client.Close()
			_ = workload.Close()
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			select {
			case <-joined:
			case <-cleanup.Done():
				select {
				case <-joined:
				default:
					t.Error("owned header reader did not join")
				}
			}
			_ = c.Stop(cleanup)
		})
		go func() {
			defer close(joined)
			err := c.tunnel(client)
			if err != nil {
				c.fail(err)
			}
			finished <- err
		}()
		synctest.Wait()
		time.Sleep(5 * time.Second) // only the original shorter IO deadline expires
		synctest.Wait()
		select {
		case err := <-finished:
			if !errors.Is(err, errNativeConduitIO) {
				t.Fatalf("shorter header deadline disappeared: %v", err)
			}
		default:
			t.Fatal("original five-second CONNECT bound did not end header read")
		}
		if owner.Err() != nil {
			t.Fatal("outer original owner unexpectedly expired")
		}
		time.Sleep(5 * time.Second)
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.Stop(cleanup); !errors.Is(err, errNativeConduitIO) {
			t.Fatalf("later execution bound erased shorter header failure: %v", err)
		}
	})
}

func TestNativeCompanionLifetimeDenialIsNotCapacity(t *testing.T) {
	f := newNativeConstructorFixture(t)
	entry, err := NewNativePreparedProcessBoundaryV2(t.Context(), f.config, f.session, f.input, f.bundle)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = entry.native.child.Close() })
	synctest.Test(t, func(t *testing.T) {
		owner, cancelOwner := context.WithTimeout(context.Background(), time.Minute)
		defer cancelOwner()
		c, err := entry.native.newCompanion(owner, nativeCompanionDependencies{
			lookup: companionLookup,
			dial: func(context.Context, string, string) (net.Conn, error) {
				return nil, errNativeConduitIO
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		active, cancelActive := context.WithTimeout(owner, time.Second)
		defer cancelActive()
		c.active, c.cancelActive = active, cancelActive
		c.inbound, c.outbound, c.used = 0, 0, c.owner.conduitLimits().Connections
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = c.Stop(cleanup)
		})
		for _, outbound := range []bool{false, true} {
			if _, err := c.reserve(outbound, 1); !errors.Is(err, errNativeConduitLimit) {
				t.Fatalf("live exhausted byte budget not a limit: %v", err)
			}
		}
		if err := c.admit(nil); !errors.Is(err, errNativeConduitLimit) {
			t.Fatalf("live exhausted connection budget not a limit: %v", err)
		}
		time.Sleep(time.Second)
		for _, outbound := range []bool{false, true} {
			if _, err := c.reserve(outbound, 1); !errors.Is(err, errNativeConduitStopped) {
				t.Fatalf("expired byte reservation manufactured a limit: %v", err)
			}
		}
		if err := c.admit(nil); !errors.Is(err, errNativeConduitStopped) {
			t.Fatalf("expired admission manufactured a limit: %v", err)
		}
		if c.used != c.owner.conduitLimits().Connections || c.inbound != 0 || c.outbound != 0 {
			t.Fatal("denied admission/reservation replenished original budgets")
		}
	})
}

func TestNativeCompanionAcceptErrorIsNotBlanketSuppressed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		expired bool
		err     error
		clean   bool
	}{
		{"live-original-check-failure", false, attachedworkerdaemon.ErrSupervisorConfig, false},
		{"ended-original-listener-closed", true, attachedworkerdaemon.ErrSupervisorConfig, true},
		{"ended-unknown-accept-failure", true, errors.New("owned accept failure"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				owner, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				active, cancelActive := context.WithTimeout(owner, time.Second)
				defer cancelActive()
				c := &nativeCompanion{ctx: owner, cancel: cancel, active: active, cancelActive: cancelActive,
					connections: make(map[net.Conn]struct{}), done: make(chan struct{}), stop: func() bool { return false }}
				c.self = c
				if tc.expired {
					time.Sleep(time.Second)
				}
				deadline, _ := active.Deadline()
				got := c.observeAccept(tc.err, deadline)
				want := errNativeConduitIO
				if tc.clean {
					want = errNativeConduitStopped
				}
				if !errors.Is(got, want) {
					t.Fatalf("Accept classification got %v want %v", got, want)
				}
				cleanup, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				err := c.Stop(cleanup)
				if tc.clean && err != nil || !tc.clean && !errors.Is(err, errNativeConduitIO) {
					t.Fatalf("Accept Stop changed the original outcome: %v", err)
				}
			})
		})
	}
}
