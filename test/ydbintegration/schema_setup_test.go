//go:build ydbintegration

package ydbintegration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/ydbmigrate"
	ydbmigrations "gitcode.com/urandon/sessionless/migrations/ydb"
)

const integrationSchemaSetupLimit = 16
const integrationSchemaSetupTimeout = 2 * time.Minute

var errIntegrationSchemaSetup = errors.New("integration baseline schema setup failed; underlying details redacted")
var errIntegrationSchemaSetupCapacity = errors.New("integration baseline schema setup exceeded 16 exact connections")

// Keep the original error available for causal assertions without printing a
// connection string, credentials, or backend response in a shared CI log.
type integrationSchemaSetupFailure struct{ cause error }

func (failure integrationSchemaSetupFailure) Error() string {
	reason := fmt.Sprintf("backend error type %T; details redacted", failure.cause)
	if errors.Is(failure.cause, context.DeadlineExceeded) {
		reason = "caller/setup work deadline exceeded"
	} else if errors.Is(failure.cause, context.Canceled) {
		reason = "caller/setup work cancelled"
	}
	return errIntegrationSchemaSetup.Error() + ": " + reason
}
func (failure integrationSchemaSetupFailure) Unwrap() error { return failure.cause }

type integrationSchemaSetupEntry struct {
	done    chan struct{}
	failure error
}

type integrationSchemaSetupCache struct {
	mu           sync.Mutex
	byConnection map[string]*integrationSchemaSetupEntry
}

var integrationSchemaSetups integrationSchemaSetupCache

// This process-local baseline is scoped to the one immutable embedded migration
// filesystem below. Ordinary fixtures previously repeated every migration-status,
// lock-renewal and checksum query even on an already-migrated database. Dedicated
// migration tests and the CI apply-twice guard still call the migrator directly.
// A test resetting this database must likewise migrate directly, not reuse this
// successful baseline. The work context has a two-minute cap, not an unconditional
// wall-time guarantee: Migrator.Up retains its own detached cleanup/unlock rules.
func ensureIntegrationSchema(ctx context.Context, connectionString string) error {
	return integrationSchemaSetups.ensure(ctx, connectionString, func(ctx context.Context) error {
		migrator, err := ydbmigrate.New(ydbmigrate.Config{
			ConnectionString: connectionString,
			OwnerID:          uniqueID("integration-schema"),
		}, ydbmigrations.Files)
		if err != nil {
			return err
		}
		return migrator.Up(ctx)
	})
}

// The first caller performs setup synchronously outside the map lock. Followers
// wait for the same result, but retain their own shorter cancellation/deadline.
// An attempted setup, including one cancelled by its caller, is never retried.
func (cache *integrationSchemaSetupCache) ensure(ctx context.Context, connectionString string, apply func(context.Context) error) error {
	if cache == nil || ctx == nil || connectionString == "" || apply == nil {
		return integrationSchemaSetupFailure{cause: errors.New("invalid schema setup inputs")}
	}
	ctx, cancel := context.WithTimeout(ctx, integrationSchemaSetupTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return integrationSchemaSetupFailure{cause: err}
	}
	cache.mu.Lock()
	if cache.byConnection == nil {
		cache.byConnection = make(map[string]*integrationSchemaSetupEntry)
	}
	entry, found := cache.byConnection[connectionString]
	if !found {
		if len(cache.byConnection) >= integrationSchemaSetupLimit {
			cache.mu.Unlock()
			return errIntegrationSchemaSetupCapacity
		}
		entry = &integrationSchemaSetupEntry{done: make(chan struct{})}
		cache.byConnection[connectionString] = entry
	}
	cache.mu.Unlock()
	if !found {
		err := apply(ctx)
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			entry.failure = integrationSchemaSetupFailure{cause: err}
		}
		close(entry.done)
		return entry.failure
	}
	select {
	case <-entry.done:
		if err := ctx.Err(); err != nil {
			return integrationSchemaSetupFailure{cause: err}
		}
		return entry.failure
	case <-ctx.Done():
		return integrationSchemaSetupFailure{cause: ctx.Err()}
	}
}

func TestIntegrationSchemaSetupReuseAndExactKeys(t *testing.T) {
	var cache integrationSchemaSetupCache
	var calls int
	apply := func(context.Context) error { calls++; return nil }
	for _, key := range []string{"grpc://db/local?a=1&b=2", "grpc://db/local?a=1&b=2", "grpc://db/local?b=2&a=1"} {
		if err := cache.ensure(t.Context(), key, apply); err != nil {
			t.Fatalf("baseline setup: %v", err)
		}
	}
	if calls != 2 {
		t.Fatalf("setup calls=%d, want 2 for two exact keys", calls)
	}
}

func TestIntegrationSchemaSetupConcurrentOnceAndIndependentKeys(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var cache integrationSchemaSetupCache
		var calls atomic.Int32
		started, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		var group sync.WaitGroup
		t.Cleanup(func() { unblock(); group.Wait() })
		results := make(chan error, 9)
		apply := func(context.Context) error { calls.Add(1); close(started); <-release; return nil }
		group.Go(func() { results <- cache.ensure(t.Context(), "same", apply) })
		<-started
		for range 8 {
			group.Go(func() {
				results <- cache.ensure(t.Context(), "same", func(context.Context) error {
					calls.Add(1)
					return errors.New("duplicate setup executed")
				})
			})
		}
		synctest.Wait()
		if got := calls.Load(); got != 1 {
			t.Fatalf("blocked setup calls=%d, want 1", got)
		}
		// A blocked application must not retain the global cache mutex.
		if err := cache.ensure(t.Context(), "independent", func(context.Context) error { return nil }); err != nil {
			t.Fatalf("independent setup while first blocked: %v", err)
		}
		unblock()
		group.Wait()
		for range 9 {
			if err := <-results; err != nil {
				t.Errorf("shared setup result: %v", err)
			}
		}
		if got := calls.Load(); got != 1 {
			t.Errorf("final setup calls=%d, want 1", got)
		}
	})
}

func TestIntegrationSchemaSetupStickyFailureAndRedaction(t *testing.T) {
	var cache integrationSchemaSetupCache
	cause := errors.New("private-dsn-with-secret")
	calls := 0
	for range 2 {
		err := cache.ensure(t.Context(), "private-connection", func(context.Context) error { calls++; return cause })
		if !errors.Is(err, cause) {
			t.Fatalf("setup error=%v, want retained original cause", err)
		}
		if strings.Contains(err.Error(), "private") {
			t.Fatal("setup error disclosed private input or backend message")
		}
	}
	if calls != 1 {
		t.Fatalf("failed setup calls=%d, want 1 without retry", calls)
	}
}

func TestIntegrationSchemaSetupFiniteOriginalDeadline(t *testing.T) {
	for _, tc := range []struct {
		name          string
		parentTimeout time.Duration
	}{
		{name: "two-minute-cap"}, {name: "shorter-caller", parentTimeout: 7 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var cache integrationSchemaSetupCache
				parent := t.Context()
				if tc.parentTimeout != 0 {
					var cancel context.CancelFunc
					parent, cancel = context.WithTimeout(parent, tc.parentTimeout)
					defer cancel()
				}
				wantDuration := integrationSchemaSetupTimeout
				if tc.parentTimeout != 0 {
					wantDuration = tc.parentTimeout
				}
				wantDeadline := time.Now().Add(wantDuration)
				calls := 0
				err := cache.ensure(parent, "same", func(ctx context.Context) error {
					calls++
					if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(wantDeadline) {
						t.Errorf("setup deadline=%v present=%t, want %v", deadline, ok, wantDeadline)
					}
					<-ctx.Done()
					return ctx.Err()
				})
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("setup error=%v, want original deadline failure", err)
				}
				// A fresh caller cannot turn a timed-out attempted baseline green.
				err = cache.ensure(t.Context(), "same", func(context.Context) error { calls++; return nil })
				if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
					t.Fatalf("replay error=%v calls=%d, want sticky timeout and one call", err, calls)
				}
			})
		})
	}
}

func TestIntegrationSchemaSetupCancelledFollowerDoesNotRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var cache integrationSchemaSetupCache
		started, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		var group sync.WaitGroup
		t.Cleanup(func() { unblock(); group.Wait() })
		leader := make(chan error, 1)
		group.Go(func() {
			leader <- cache.ensure(t.Context(), "same", func(context.Context) error { close(started); <-release; return nil })
		})
		<-started
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		err := cache.ensure(ctx, "same", func(context.Context) error { t.Error("follower retried setup"); return nil })
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("follower error=%v, want shorter caller timeout", err)
		}
		unblock()
		group.Wait()
		if err := <-leader; err != nil {
			t.Fatalf("original leader failed: %v", err)
		}
		if err := cache.ensure(t.Context(), "same", func(context.Context) error { t.Error("completed setup retried"); return nil }); err != nil {
			t.Fatalf("cancelled follower poisoned successful baseline: %v", err)
		}
	})
}

func TestIntegrationSchemaSetupCancelledBeforeAttempt(t *testing.T) {
	var cache integrationSchemaSetupCache
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	apply := func(context.Context) error { calls++; return nil }
	if err := cache.ensure(ctx, "same", apply); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller error=%v, want cancellation", err)
	}
	if err := cache.ensure(t.Context(), "same", apply); err != nil {
		t.Fatalf("unattempted baseline poisoned: %v", err)
	}
	if calls != 1 {
		t.Fatalf("setup calls=%d, want first noncancelled attempt only", calls)
	}
}

func TestIntegrationSchemaSetupCancelledLeaderCannotPublishSuccess(t *testing.T) {
	var cache integrationSchemaSetupCache
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	err := cache.ensure(ctx, "same", func(context.Context) error {
		calls++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled completion error=%v, want cancellation rather than success", err)
	}
	err = cache.ensure(t.Context(), "same", func(context.Context) error { calls++; return nil })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancelled baseline replay error=%v calls=%d, want sticky cancellation and no retry", err, calls)
	}
}

func TestIntegrationSchemaSetupMapCapacity(t *testing.T) {
	var cache integrationSchemaSetupCache
	calls := 0
	apply := func(context.Context) error { calls++; return nil }
	for index := range integrationSchemaSetupLimit {
		if err := cache.ensure(t.Context(), fmt.Sprintf("exact-%d", index), apply); err != nil {
			t.Fatalf("key %d setup: %v", index, err)
		}
	}
	if err := cache.ensure(t.Context(), "seventeenth", apply); !errors.Is(err, errIntegrationSchemaSetupCapacity) {
		t.Fatalf("capacity error=%v, want explicit bound", err)
	}
	if err := cache.ensure(t.Context(), "exact-0", apply); err != nil {
		t.Fatalf("existing key at capacity: %v", err)
	}
	if calls != integrationSchemaSetupLimit {
		t.Fatalf("setup calls=%d, want %d", calls, integrationSchemaSetupLimit)
	}
}
