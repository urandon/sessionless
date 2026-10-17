package localoperator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Explicit synthetic PRIVATE keeper state exercises lifetime ownership only.
// This is not an OpenFresh/private-file/installation/source-authentication proof;
// genuine OpenFresh wiring is covered by the connected Store/source tests.
func syntheticExecutionSource(t *testing.T, parent context.Context, expires time.Time) *Source {
	t.Helper()
	lifetime, cancel := context.WithDeadline(parent, expires)
	s := &Source{ctx: lifetime, cancel: cancel, key: &sourceKey{bytes: [32]byte{1}}, config: domain.ConfiguredInferenceProfileV1{ExpiresAt: expires}}
	s.self = s
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func requireLifetimeOpen(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-ctx.Done():
		t.Fatalf("owned lifetime prematurely done: %v", ctx.Err())
	default:
	}
}

func requireLifetimeDone(t *testing.T, ctx context.Context, want error) {
	t.Helper()
	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), want) {
			t.Fatalf("lifetime cause got=%v want=%v", ctx.Err(), want)
		}
	default:
		t.Fatal("retained original lifetime did not observe revocation")
	}
}

func TestExecutionLifetimeCloseRevokesOnlyOwnedChild(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		source := syntheticExecutionSource(t, parent, time.Now().Add(time.Minute))
		retained, err := source.ExecutionLifetime()
		if err != nil || retained == nil || retained == parent {
			t.Fatalf("owned child unavailable or is caller: %v", err)
		}
		requireLifetimeOpen(t, retained)
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
		requireLifetimeDone(t, retained, context.Canceled)
		requireLifetimeOpen(t, parent)
		if source.key.bytes != ([32]byte{}) {
			t.Error("Close stopped clearing retained source key")
		}
		if got, err := source.ExecutionLifetime(); !errors.Is(err, ErrDenied) || got != nil {
			t.Error("closed source exposed fresh lifetime")
		}
		if err := source.Close(); err != nil {
			t.Fatalf("repeated Close not idempotent: %v", err)
		}
		requireLifetimeOpen(t, parent)
	})
}

func TestExecutionLifetimeParentCancellationRevokesRetainedChild(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		source := syntheticExecutionSource(t, parent, time.Now().Add(time.Minute))
		retained, err := source.ExecutionLifetime()
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		requireLifetimeDone(t, retained, context.Canceled)
		if got, err := source.ExecutionLifetime(); !errors.Is(err, ErrDenied) || got != nil {
			t.Error("cancelled parent admitted another lifetime")
		}
		if source.key.bytes != ([32]byte{}) {
			t.Error("cancelled source stopped clearing key on denied access")
		}
	})
}

func TestExecutionLifetimeDeniesNilZeroAndCopiedKeepers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var absent *Source
		if got, err := absent.ExecutionLifetime(); !errors.Is(err, ErrDenied) || got != nil {
			t.Error("nil source exposed lifetime")
		}
		if err := absent.Close(); err != nil {
			t.Error("nil Close lost existing idempotence")
		}
		if got, err := new(Source).ExecutionLifetime(); !errors.Is(err, ErrDenied) || got != nil {
			t.Error("zero source exposed lifetime")
		}
		source := syntheticExecutionSource(t, t.Context(), time.Now().Add(time.Minute))
		retained, err := source.ExecutionLifetime()
		if err != nil {
			t.Fatal(err)
		}
		// Copy the private retained identity/context/cancel/key facts, without
		// copying sync.Mutex. A copy still cannot act as its original keeper.
		copy := &Source{self: source.self, ctx: source.ctx, cancel: source.cancel, key: source.key, config: source.config.Clone()}
		if got, err := copy.ExecutionLifetime(); !errors.Is(err, ErrDenied) || got != nil {
			t.Error("copied keeper exposed original retained lifetime")
		}
		if err := copy.Close(); !errors.Is(err, ErrDenied) {
			t.Error("copied keeper closed original lifetime")
		}
		requireLifetimeOpen(t, retained)
		if source.key.bytes == ([32]byte{}) {
			t.Error("denied copied Close cleared original keeper key")
		}
	})
}

func TestExecutionLifetimeFixedOriginalWindowCannotRenew(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		expires := time.Now().Add(10 * time.Second)
		source := syntheticExecutionSource(t, t.Context(), expires)
		retained, err := source.ExecutionLifetime()
		if err != nil {
			t.Fatal(err)
		}
		if deadline, ok := retained.Deadline(); !ok || !deadline.Equal(expires) {
			t.Fatal("original configuration deadline was not retained")
		}
		configuration, err := source.Configuration()
		if err != nil {
			t.Fatal(err)
		}
		configuration.ExpiresAt = expires.Add(time.Hour) // caller-owned clone, never renewal
		before := time.NewTimer(9 * time.Second)
		t.Cleanup(func() { before.Stop() })
		<-before.C // deterministic virtual boundary, not readiness or wall sleep
		requireLifetimeOpen(t, retained)
		again, err := source.ExecutionLifetime()
		if err != nil || again != retained {
			t.Fatal("repeated access reminted the original lifetime")
		}
		if deadline, ok := again.Deadline(); !ok || !deadline.Equal(expires) {
			t.Fatal("caller clone or access extended source window")
		}
		<-retained.Done() // synctest advances to the ORIGINAL installed expiry
		if !time.Now().Equal(expires) {
			t.Fatal("original lifetime exceeded original source deadline")
		}
		requireLifetimeDone(t, retained, context.DeadlineExceeded)
		if got, err := source.ExecutionLifetime(); !errors.Is(err, ErrDenied) || got != nil {
			t.Error("expired source created a renewed lifetime")
		}
		requireLifetimeOpen(t, t.Context())
	})
}

func TestExecutionLifetimeEarlierParentDeadlineRemainsShortest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		expires := time.Now().Add(5 * time.Second)
		parent, cancel := context.WithDeadline(t.Context(), expires)
		t.Cleanup(cancel)
		source := syntheticExecutionSource(t, parent, expires.Add(time.Second))
		retained, err := source.ExecutionLifetime()
		if err != nil {
			t.Fatal(err)
		}
		if deadline, ok := retained.Deadline(); !ok || !deadline.Equal(expires) {
			t.Fatal("source lengthened original caller deadline")
		}
		<-retained.Done()
		requireLifetimeDone(t, retained, context.DeadlineExceeded)
		if !time.Now().Equal(expires) {
			t.Fatal("source outlived shorter parent deadline")
		}
	})
}

func TestExecutionLifetimeConcurrentCloseAndAccessRetainSameSignal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := syntheticExecutionSource(t, t.Context(), time.Now().Add(time.Minute))
		retained, err := source.ExecutionLifetime()
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var joined sync.WaitGroup
		for range 8 {
			joined.Go(func() {
				<-start
				got, err := source.ExecutionLifetime()
				if err == nil && got != retained {
					t.Error("concurrent access replaced retained lifetime")
				}
				if err != nil && (!errors.Is(err, ErrDenied) || got != nil) {
					t.Error("denial exposed another signal")
				}
			})
		}
		joined.Go(func() {
			<-start
			if err := source.Close(); err != nil {
				t.Error(err)
			}
		})
		close(start)
		joined.Wait()
		requireLifetimeDone(t, retained, context.Canceled)
		requireLifetimeOpen(t, t.Context())
	})
}
