//go:build darwin || linux

package codexexec

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var errPreparedExecutionCleanupTimeout = errors.New("test-owned Execute cleanup join timed out")

// Test-only cleanup primitive: cancel THEN wait on independent completion.
// It cannot consume a scenario result, and a timeout never means quiescence.
func cancelAndJoinPreparedExecution(cancel context.CancelFunc, done <-chan struct{}, bound time.Duration) error {
	cancel()
	select {
	case <-done:
		return nil
	default:
	}
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		select {
		case <-done:
			return nil
		default:
			return errPreparedExecutionCleanupTimeout
		}
	}
}

type observationBoundaryFixture struct {
	entered chan struct{}
	proceed <-chan struct{}
	result  ProcessResultV1
	err     error
}

func (fixture *observationBoundaryFixture) Run(context.Context, ProcessInvocationV1) (ProcessResultV1, error) {
	close(fixture.entered)
	<-fixture.proceed
	return fixture.result, fixture.err
}

func (*observationBoundaryFixture) Cancel(context.Context, AuthorityV1) error { return nil }

func TestPreparedBoundaryObservationDistinguishesPendingAndReturned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		proceed := make(chan struct{})
		var release sync.Once
		defer release.Do(func() { close(proceed) })
		cause := errors.New("public delegate failure")
		fixture := &observationBoundaryFixture{entered: make(chan struct{}), proceed: proceed,
			result: ProcessResultV1{ExitCode: 17, Cancelled: true, Stdout: []byte("public protocol")}, err: cause}
		boundary := &recordingPreparedBoundary{delegate: fixture}
		if got := boundary.snapshot(); got.Returned || got.Err != nil || got.Process.ExitCode != 0 {
			t.Fatalf("before invocation observation=%+v", got)
		}
		done := make(chan struct{})
		go func() { _, _ = boundary.Run(context.Background(), ProcessInvocationV1{}); close(done) }()
		<-fixture.entered
		synctest.Wait()
		if got := boundary.snapshot(); got.Returned || got.Err != nil || got.Process.ExitCode != 0 {
			t.Fatalf("blocked delegate must be pending: %+v", got)
		}
		release.Do(func() { close(proceed) })
		<-done
		got := boundary.snapshot()
		if !got.Returned || got.Process.ExitCode != 17 || !got.Process.Cancelled || !errors.Is(got.Err, cause) {
			t.Fatalf("returned atomic observation=%+v", got)
		}
		fixture.result.Stdout[0] = 'X'
		got.Process.Stdout[0] = 'Y'
		if stored := boundary.snapshot(); string(stored.Process.Stdout) != "public protocol" {
			t.Fatalf("snapshot aliases mutable delegate/caller output: %q", stored.Process.Stdout)
		}
	})
}

func TestPreparedExecutionCleanupCancelsAndJoinsWithoutConsumingResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done, finish, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var release sync.Once
		defer release.Do(func() { close(finish) })
		result := make(chan error, 1)
		cause := errors.New("public completed execution")
		go func() {
			<-ctx.Done()
			close(canceled)
			<-finish
			result <- cause
			close(done)
		}()
		joined := make(chan error, 1)
		go func() { joined <- cancelAndJoinPreparedExecution(cancel, done, time.Second) }()
		<-canceled
		synctest.Wait()
		select {
		case err := <-joined:
			t.Fatalf("cleanup returned before owned execution completed: %v", err)
		default:
		}
		release.Do(func() { close(finish) })
		if err := <-joined; err != nil {
			t.Fatalf("cleanup join=%v", err)
		}
		if err := <-result; !errors.Is(err, cause) {
			t.Fatalf("cleanup consumed/changed scenario result: %v", err)
		}
		// A readiness failure may already have consumed result. Independent done
		// still joins immediately without waiting for another result publication.
		if err := cancelAndJoinPreparedExecution(cancel, done, time.Second); err != nil {
			t.Fatalf("already consumed result prevented independent join: %v", err)
		}
	})
}

func TestPreparedExecutionCleanupTimeoutIsNotQuiescence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		joined := make(chan error, 1)
		start := time.Now()
		go func() { joined <- cancelAndJoinPreparedExecution(cancel, done, time.Second) }()
		if err := <-joined; !errors.Is(err, errPreparedExecutionCleanupTimeout) {
			t.Fatalf("unfinished execution cleanup=%v", err)
		}
		if !errors.Is(ctx.Err(), context.Canceled) || time.Since(start) != time.Second {
			t.Fatalf("logical cleanup bound/cancellation elapsed=%s error=%v", time.Since(start), ctx.Err())
		}
		select {
		case <-done:
			t.Fatal("timeout fabricated completion")
		default:
		}
	})
}

func TestPreparedExecutionCleanupJoinsBeforeEarlierResourceCleanup(t *testing.T) {
	joined := false
	// t.Run is deliberately OUTSIDE the synctest bubble (Go 1.26 forbids it
	// inside). Cleanup callbacks themselves execute in the bubble's test.
	t.Run("registered before launch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			// This represents earlier fixture TempDir cleanup in LIFO order.
			t.Cleanup(func() {
				if !joined {
					t.Error("resource cleanup ran before execution joined")
				}
				select {
				case <-done:
				default:
					t.Error("resource cleanup observed live owned execution")
				}
			})
			t.Cleanup(func() {
				if err := cancelAndJoinPreparedExecution(cancel, done, time.Second); err != nil {
					t.Errorf("cleanup join=%v", err)
					return
				}
				joined = true
			})
			go func() { <-ctx.Done(); close(done) }()
			// Return without consuming a result: the same cleanup path serves a
			// parent test's failure/Fatal/early-return as well as normal success.
		})
	})
	if !joined {
		t.Fatal("registered cleanup never joined")
	}
}
