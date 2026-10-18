package ydbstore

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Genuine original Source/issuer/Store/entry; SQL and external funding are
// scripted cooperative fixtures, not native isolation or serving qualification.
func TestLocalWorkerEffectFreshSlotAndObservationOnlyReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		completeLocalEntryFixture(t, f)
		fresh, err := f.bootstrap.ReserveLocalWorkerEffect(f.actual.ctx, f.parent.request)
		if err != nil || fresh.Status != ports.AttemptEffectOwnedV1 || fresh.Grant == nil || f.bootstrap.pendingStart == nil || f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 0 {
			t.Fatalf("fresh original reservation did not install sole pre-entry slot: err=%v inserts=%d burns=%d", err, f.parent.sql.inserts, f.parent.sql.burnUpdates)
		}
		observed, err := f.bootstrap.ReserveLocalWorkerEffect(f.actual.ctx, f.parent.request)
		if err != nil || observed.Status != ports.AttemptEffectReconcileOnlyV1 || observed.Grant != nil || observed.ObservationGrant == nil {
			t.Fatalf("repeat did not remain observation-only: err=%v status=%s", err, observed.Status)
		}
		// This remains structurally valid DATA, but it is not the exact fresh
		// result held by the original slot. Do not consume the slot on mismatch.
		observed = cloneLocalFreshResult(fresh)
		observed.Status = ports.AttemptEffectReplayedV1
		if err := observed.Validate(); err != nil {
			t.Fatal(err)
		}
		entry, err := f.bootstrap.EnterLocalWorkerEffect(f.actual.ctx, observed, &localEntryBlobs{})
		assertLocalEntryDenied(t, entry, err)
		entry, err = f.bootstrap.EnterLocalWorkerEffect(f.actual.ctx, fresh, &localEntryBlobs{})
		if err != nil || entry == nil {
			t.Fatalf("original fresh handoff: %v", err)
		}
		allocation, _ := entry.Allocation()
		entry, err = f.bootstrap.EnterLocalWorkerEffect(f.actual.ctx, cloneLocalFreshResult(fresh), &localEntryBlobs{})
		assertLocalEntryDenied(t, entry, err)
		if f.bridge.VerifyEnteredLocalAllocation(allocation, time.Now()) != nil || f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 1 {
			t.Fatal("copied loser closed winner or repeated reserve/burn")
		}
	})
}

func TestLocalWorkerEffectConcurrentCopiesHaveOneEntryOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		completeLocalEntryFixture(t, f)
		fresh, err := f.bootstrap.ReserveLocalWorkerEffect(f.actual.ctx, f.parent.request)
		if err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		var group sync.WaitGroup
		unblock := func() { once.Do(func() { close(release) }) }
		t.Cleanup(func() { unblock(); group.Wait() })
		f.parent.graph.afterCommit = func() { f.parent.graph.afterCommit = nil; close(entered); <-release }
		type outcome struct {
			entry *LocalInvocationEntryV1
			err   error
		}
		done := make(chan outcome, 1)
		group.Go(func() {
			entry, err := f.bootstrap.EnterLocalWorkerEffect(f.actual.ctx, cloneLocalFreshResult(fresh), &localEntryBlobs{})
			done <- outcome{entry, err}
		})
		synctest.Wait()
		select {
		case <-entered:
		default:
			t.Fatal("original consumer did not reach its SQL barrier")
		}
		entry, err := f.bootstrap.EnterLocalWorkerEffect(f.actual.ctx, cloneLocalFreshResult(fresh), &localEntryBlobs{})
		assertLocalEntryDenied(t, entry, err)
		unblock()
		synctest.Wait()
		select {
		case winner := <-done:
			if winner.err != nil || winner.entry == nil {
				t.Fatalf("winner damaged by copied loser: %v", winner.err)
			}
			allocation, _ := winner.entry.Allocation()
			if f.bridge.VerifyEnteredLocalAllocation(allocation, time.Now()) != nil || f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 1 {
				t.Fatal("copied loser closed winner or repeated preparation")
			}
		default:
			t.Fatal("winning consumer did not finish after releasing owned barrier")
		}
	})
}

func TestLocalWorkerEffectRecoveredGrantCannotCreateFreshSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixture(t)
		completeLocalEntryFixture(t, f)
		original, err := f.store.reserveBoundedOwnedAttemptEffect(f.actual.ctx, f.parent.request)
		if err != nil || original.Grant == nil {
			t.Fatalf("original persisted reservation: %v", err)
		}
		recovered, err := f.store.reserveBoundedOwnedAttemptEffect(f.actual.ctx, f.parent.request)
		if err != nil || recovered.Status != ports.AttemptEffectReplayedV1 || recovered.Grant == nil {
			t.Fatalf("valid recovered ownership grant: %v", err)
		}
		recovered.Status = ports.AttemptEffectOwnedV1
		entry, err := f.bootstrap.EnterLocalWorkerEffect(f.actual.ctx, recovered, &localEntryBlobs{})
		assertLocalEntryDenied(t, entry, err)
		result, err := f.bootstrap.ReserveLocalWorkerEffect(f.actual.ctx, f.parent.request)
		if err != nil || result.Status != ports.AttemptEffectReconcileOnlyV1 || result.Grant != nil || result.ObservationGrant == nil || f.bootstrap.pendingStart != nil || f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 0 {
			t.Fatalf("recovered reserve gained freshness: err=%v status=%s", err, result.Status)
		}
	})
}

func TestLocalWorkerEffectForeignAndCanceledConsumersCannotRecreateEntry(t *testing.T) {
	for _, mode := range []string{"foreign", "source_closed", "caller_canceled"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalBootstrapFixture(t)
				completeLocalEntryFixture(t, f)
				fresh, err := f.bootstrap.ReserveLocalWorkerEffect(f.actual.ctx, f.parent.request)
				if err != nil {
					t.Fatal(err)
				}
				ctx := f.actual.ctx
				owner := f.bootstrap
				switch mode {
				case "foreign":
					other := newLocalBootstrapFixture(t)
					completeLocalEntryFixture(t, other)
					owner = other.bootstrap
				case "source_closed":
					if err := f.actual.source.Close(); err != nil {
						t.Fatal(err)
					}
				case "caller_canceled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				entry, err := owner.EnterLocalWorkerEffect(ctx, fresh, &localEntryBlobs{})
				assertLocalEntryDenied(t, entry, err)
				if f.bootstrap.entry.Load() != nil || f.parent.sql.burnUpdates != 0 || f.parent.sql.inserts != 1 {
					t.Fatal("denied handoff created preparation/effects")
				}
			})
		})
	}
}

func TestLocalWorkerEffectUnknownCommitNeverRecoversFreshness(t *testing.T) {
	for _, mode := range []string{"rollback", "ambiguous"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalBootstrapFixture(t)
				completeLocalEntryFixture(t, f)
				f.parent.graph.commitModes = []string{mode, ""}
				result, err := f.bootstrap.ReserveLocalWorkerEffect(f.actual.ctx, f.parent.request)
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, ports.ReserveAttemptEffectResultV1{}) || f.bootstrap.pendingStart != nil || f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 0 {
					t.Fatalf("uncertain insert gained a handoff: err=%v inserts=%d burns=%d", err, f.parent.sql.inserts, f.parent.sql.burnUpdates)
				}
				result, err = f.bootstrap.ReserveLocalWorkerEffect(f.actual.ctx, f.parent.request)
				if mode == "ambiguous" {
					if err != nil || result.Status != ports.AttemptEffectReconcileOnlyV1 || result.Grant != nil {
						t.Fatalf("persisted uncertainty must be observation-only: %v", err)
					}
				} else if err == nil || !reflect.DeepEqual(result, ports.ReserveAttemptEffectResultV1{}) {
					t.Fatal("absent uncertain insert became fresh/not-found success")
				}
				if f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 0 || f.bootstrap.pendingStart != nil {
					t.Fatal("retry inserted or prepared again")
				}
			})
		})
	}
}

func TestLocalWorkerEffectOriginalInvocationCannotBeRenewedAtHandoff(t *testing.T) {
	for _, mode := range []string{"canceled", "expired", "retained_deadline", "retained_cancel"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalBootstrapFixture(t)
				completeLocalEntryFixture(t, f)
				deadline := time.Now().Add(3 * time.Second)
				original, cancel := context.WithDeadline(f.actual.ctx, deadline)
				defer cancel()
				fresh, err := f.bootstrap.ReserveLocalWorkerEffect(original, f.parent.request)
				if err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "canceled":
					cancel()
				case "expired":
					time.Sleep(time.Until(deadline))
					synctest.Wait()
				}
				// A different live context cannot reset the original invocation.
				entry, err := f.bootstrap.EnterLocalWorkerEffect(f.actual.ctx, fresh, &localEntryBlobs{})
				if mode == "canceled" || mode == "expired" {
					assertLocalEntryDenied(t, entry, err)
					if f.parent.sql.burnUpdates != 0 {
						t.Fatal("dead original invocation reached preparation burn")
					}
					return
				}
				if err != nil || entry == nil {
					t.Fatalf("live original invocation handoff: %v", err)
				}
				if retained, ok := entry.invocationContext.Deadline(); !ok || !retained.Equal(deadline) {
					t.Fatal("handoff renewed or omitted original deadline")
				}
				if mode == "retained_cancel" {
					cancel()
					// Cancellation of the original parent is synchronous, not an
					// AfterFunc callback which may lag a different live handoff.
					if !errors.Is(entry.invocationContext.Err(), context.Canceled) {
						t.Fatal("returned entry escaped synchronous original cancellation")
					}
				} else {
					time.Sleep(time.Until(deadline))
					synctest.Wait() // Join the explicitly fired deadline timer callbacks.
					if !errors.Is(entry.invocationContext.Err(), context.DeadlineExceeded) {
						t.Fatalf("returned entry outlived original invocation deadline: %v", entry.invocationContext.Err())
					}
				}
				if _, finish, err := entry.localCompactionLifetimeContext(f.actual.ctx); err == nil {
					finish()
					t.Fatal("later work revived expired original invocation")
				}
			})
		})
	}
}
