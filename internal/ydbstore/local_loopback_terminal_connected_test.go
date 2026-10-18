package ydbstore_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localloopback"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func TestLocalLoopbackTerminalConnectedJoinsActualInvoke(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		close(entered) // actual complete HTTP request, before any response
		<-release
	}))
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); server.Close() })
	f := ydbstore.NewLocalLoopbackConnectedFixture(t, server.URL+"/v1")
	k, err := localloopback.NewKeeperV1(f.Source, f.OperationIssuer, f.Bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	// Copy BEFORE the original Keeper's first method/mutex use. Copying a used
	// mutex would make this test invalid regardless of its identity rejection.
	copied := new(localloopback.KeeperV1)
	reflect.ValueOf(copied).Elem().Set(reflect.ValueOf(k).Elem())
	t.Cleanup(func() {
		if err := k.Close(); err != nil {
			t.Error(err)
		}
	})
	f.Complete(t, k)
	c, err := k.Preflight(f.Context, f.Allocation, f.ExecutionSource, f.Operation, f.Payload())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	proof, err := c.Publication()
	if err != nil {
		t.Fatal(err)
	}
	registered, capability := f.RegisterAndReserve(t, proof)
	config, err := f.Source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result localloopback.ResultV1
		err    error
	}
	done, joined := make(chan outcome, 1), make(chan struct{})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		_ = c.Close()
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-joined:
		case <-timer.C:
			select {
			case <-joined:
			default:
				t.Error("original HTTP invocation did not join during cleanup")
			}
		}
	})
	go func() {
		defer close(joined)
		result, err := c.Invoke(f.Context, registered, capability, f.Payload())
		done <- outcome{result, err}
	}()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-entered:
	case <-timer.C:
		select {
		case <-entered:
		default:
			t.Fatal("owned peer did not observe original request")
		}
	}
	timer.Stop()
	d := time.Now().Add(config.Limits.CleanupTimeout / 2)
	if err := copied.SealTerminal(d); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("copied keeper sealed original owner: %v", err)
	}
	if err := copied.StopAndJoinTerminal(t.Context()); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("copied keeper joined original owner: %v", err)
	}
	if err := k.SealTerminal(d); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("active actual Invoke became eligible: %v", err)
	}
	if err := k.StopAndJoinTerminal(t.Context()); err != nil {
		t.Fatalf("actual blocked socket/Invoke/monitor did not join: %v", err)
	}
	var got outcome
	outcomeTimer := time.NewTimer(10 * time.Second)
	defer outcomeTimer.Stop()
	select {
	case got = <-done:
	case <-outcomeTimer.C:
		select {
		case got = <-done:
		default:
			t.Fatal("actual Invoke did not return after its monitor joined")
		}
	}
	if !errors.Is(got.err, domain.ErrMembershipDenied) || !got.result.SendAttempted || len(got.result.Body) != 0 {
		t.Fatalf("stopped actual read claimed completed response: attempted=%v err=%v", got.result.SendAttempted, got.err)
	}
	if err := k.SealTerminal(d); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("actual invoked/drained frontier became eligible")
	}
	f.AssertOriginalCapabilityConsumed(t, capability)
	if k.VerifyOwnedProxyPublication(proof, time.Now().UTC().Truncate(time.Microsecond)) != nil {
		t.Fatal("terminal stop lost historical socket authentication")
	}
	if _, err := f.Source.Configuration(); err != nil {
		t.Fatalf("terminal stop closed borrowed Source: %v", err)
	}
}

// Genuine Source/bootstrap/Store/issuer and owned numeric peer; SQL/funding
// remain scripted. Seal is transport admission only, not Store freeze or a
// retirement/custody/erasure/terminal-success receipt.
func TestLocalLoopbackTerminalConnectedActiveSealZeroSend(t *testing.T) {
	p := newLocalLoopbackPeer(t, false)
	r := prepareLocalLoopbackConnected(t, p)
	config, err := r.fixture.Source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	d := time.Now().Add(config.Limits.CleanupTimeout / 2)
	if err := r.keeper.SealTerminal(d); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("active original socket incorrectly eligible: %v", err)
	}
	if _, err := r.connection.Publication(); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("sealed original connection published fresh proof: %v", err)
	}
	if result, err := r.connection.Invoke(r.fixture.Context, r.registered, r.capability, r.fixture.Payload()); !errors.Is(err, domain.ErrMembershipDenied) || result.SendAttempted || result.RequestBytes != 0 || len(result.Body) != 0 {
		t.Fatalf("sealed original connection invoked: err=%v attempted=%v bytes=%d", err, result.SendAttempted, result.RequestBytes)
	}
	if conn, err := r.keeper.Preflight(r.fixture.Context, r.fixture.Allocation, r.fixture.ExecutionSource, r.fixture.Operation, r.fixture.Payload()); conn != nil || !errors.Is(err, domain.ErrMembershipDenied) {
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatalf("sealed original keeper redialed: conn=%v err=%v", conn != nil, err)
	}
	if err := r.keeper.StopAndJoinTerminal(t.Context()); err != nil {
		t.Fatalf("original active socket failure-cleanup did not join: %v", err)
	}
	peer := p.waitResult(t)
	if peer.err != nil || len(peer.body) != 0 || len(peer.extra) != 0 {
		t.Fatalf("terminal denial emitted bytes before actual EOF: bytes=%d extra=%d err=%v", len(peer.body), len(peer.extra), peer.err)
	}
	if err := r.keeper.SealTerminal(d); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("drained failed frontier became eligible")
	}
	r.assertHistoricalOnly(t)
	r.fixture.AssertOriginalCapabilityUntouchedThenBurn(t, r.capability)
	if _, err := r.fixture.Source.Configuration(); err != nil || r.fixture.Context.Err() != nil {
		t.Fatalf("transport terminal cleanup closed borrowed Source/caller: %v", err)
	}
}

func TestLocalLoopbackTerminalConnectedIdleOriginal(t *testing.T) {
	p := newLocalLoopbackPeer(t, false)
	f := ydbstore.NewLocalLoopbackConnectedFixture(t, p.endpoint())
	k, err := localloopback.NewKeeperV1(f.Source, f.OperationIssuer, f.Bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := k.Close(); err != nil {
			t.Error(err)
		}
	})
	f.Complete(t, k)
	config, err := f.Source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	d := time.Now().Add(config.Limits.CleanupTimeout / 2)
	if err := k.SealTerminal(d); err != nil {
		t.Fatalf("idle genuine original seal: %v", err)
	}
	if err := k.SealTerminal(d); err != nil {
		t.Fatalf("exact repeated seal: %v", err)
	}
	if err := k.SealTerminal(d.Add(time.Second)); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("deadline renewed")
	}
	if conn, err := k.Preflight(f.Context, f.Allocation, f.ExecutionSource, f.Operation, f.Payload()); conn != nil || !errors.Is(err, domain.ErrMembershipDenied) {
		if conn != nil {
			_ = conn.Close()
		}
		t.Fatalf("idle sealed keeper acquired a socket: %v", err)
	}
	if err := k.StopAndJoinTerminal(t.Context()); err != nil {
		t.Fatalf("idle actual owned stop/join: %v", err)
	}
	select {
	case <-p.accepted:
		t.Fatal("sealed idle keeper dialed an owned peer")
	default:
	}
	if _, err := f.Source.Configuration(); err != nil {
		t.Fatalf("idle cleanup closed borrowed Source: %v", err)
	}
}
