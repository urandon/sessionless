package ydbstore

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// Genuine private-file Source, actual runtime, original Store parent/burn and
// allocation publication. SQL/current external funding remain scripted. These
// tests prove local lifetime/once-resource ownership, NOT a connector/send grant.
func localModelLeaseFixture(t *testing.T) (localRuntimeConnectedStore, domain.RegisteredAllocationV2, domain.AttemptOperationV1) {
	t.Helper()
	f := newLocalRuntimeConnectedStore(t)
	r, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
	if err != nil {
		t.Fatalf("publish genuine local allocation: %v", err)
	}
	if err := f.bridge.EnterOwnedLocalAllocation(f.actual.ctx, f.publication); err != nil {
		t.Fatalf("enter original allocation: %v", err)
	}
	p := f.parent.parent
	cost := p.Authority.AdmissionCostCeiling
	cd, err := cost.Digest()
	if err != nil {
		t.Fatal(err)
	}
	op := domain.AttemptOperationV1{Version: 1, Scope: ownedOperationScope(p), Sequence: 1,
		Kind: domain.AttemptOperationModelV1, CallID: "local-model", RequestRef: "local-request", RequestDigest: strings.Repeat("1", 64),
		Deadline: p.Reservation.ReservedAt.Add(5 * time.Second), CleanupDuration: p.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxCleanupDuration,
		RequestBytes: 64, ResultBytes: 512, ContextBytes: 1, Currency: cost.Currency, PriceRevision: cost.PriceRevision, AdmissionCostCeilingDigest: cd,
		Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 1, OutputTokens: 1, BodyBytes: 576, ResponseBytes: 512, ActiveDuration: time.Second}}
	if err := op.ValidateForParent(p.Authority, p.Reservation); err != nil {
		t.Fatalf("bounded model operation shape: %v", err)
	}
	return f, r.Current, op
}

func TestLocalModelOperationLeaseOwnsOriginalSourceAndAllocationLifetime(t *testing.T) {
	for _, kind := range []string{"source_close", "invocation_cancel", "allocation_close", "operation_expiry"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, allocation, op := localModelLeaseFixture(t)
				lease, err := f.bridge.BeginLocalModelOperation(f.actual.source, allocation, op, f.parent.graph.at)
				if err != nil {
					t.Fatalf("begin retained local operation: %v", err)
				}
				t.Cleanup(func() { _ = lease.Close() })
				if deadline, ok := lease.Context().Deadline(); !ok || !deadline.Equal(op.Deadline) {
					t.Fatalf("lease deadline=%v present=%t want original %v", deadline, ok, op.Deadline)
				}
				switch kind {
				case "source_close":
					if err := f.actual.source.Close(); err != nil {
						t.Fatal(err)
					}
				case "invocation_cancel":
					f.actual.cancel()
				case "allocation_close":
					if err := f.bridge.Close(); err != nil {
						t.Fatal(err)
					}
				case "operation_expiry":
					time.Sleep(time.Until(op.Deadline)) // virtual expiry, not readiness
				}
				synctest.Wait()
				if lease.Context().Err() == nil {
					t.Fatal("original revocation did not cancel retained operation")
				}
				if kind != "invocation_cancel" && f.actual.ctx.Err() != nil {
					t.Fatal("local cleanup cancelled caller invocation context")
				}
				ad, _ := allocation.Digest()
				if lease.Consume(op, ad, time.Now().UTC().Truncate(time.Microsecond)) == nil {
					t.Fatal("revoked operation resource consumed")
				}
				if _, err := f.bridge.BeginLocalModelOperation(f.actual.source, allocation, op, f.parent.graph.at); err == nil {
					t.Fatal("revocation reopened same operation")
				}
			})
		})
	}
}

func TestLocalModelOperationLeaseOneSlotAcrossWrappersAndNoReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, allocation, op := localModelLeaseFixture(t)
		results := make(chan *serverlessharness.LocalModelOperationLeaseV1, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				lease, _ := f.bridge.BeginLocalModelOperation(f.actual.source, allocation, op.Clone(), f.parent.graph.at)
				results <- lease
			})
		}
		wg.Wait()
		close(results)
		var winner *serverlessharness.LocalModelOperationLeaseV1
		for lease := range results {
			if lease != nil {
				if winner != nil {
					t.Fatal("two wrappers acquired original preflight slot")
				}
				winner = lease
			}
		}
		if winner == nil {
			t.Fatal("no original operation slot acquired")
		}
		t.Cleanup(func() { _ = winner.Close() })
		copyLease := new(serverlessharness.LocalModelOperationLeaseV1)
		reflect.ValueOf(copyLease).Elem().Set(reflect.ValueOf(winner).Elem())
		ad, _ := allocation.Digest()
		if copyLease.Context().Err() == nil || copyLease.Consume(op, ad, f.parent.graph.at) == nil || copyLease.Close() == nil {
			t.Fatal("copied private lease remained usable")
		}
		if winner.Context().Err() != nil {
			t.Fatal("copy denial cancelled original lease")
		}
		if err := winner.Consume(op, ad, f.parent.graph.at); err != nil {
			t.Fatalf("original resource consume: %v", err)
		}
		if winner.Consume(op, ad, f.parent.graph.at) == nil {
			t.Fatal("original resource consumed twice")
		}
		if err := winner.Close(); err != nil {
			t.Fatal(err)
		}
		if err := winner.Close(); err != nil {
			t.Fatal("idempotent owned close:", err)
		}
		if _, err := f.bridge.BeginLocalModelOperation(f.actual.source, allocation, op, f.parent.graph.at); err == nil {
			t.Fatal("closing resource refunded same sequence")
		}
	})
}

func TestLocalModelOperationLeaseRejectsSubstitutionAndBurnsFailedConsume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, allocation, op := localModelLeaseFixture(t)
		other := newLocalRuntimeConnectedStore(t)
		if _, err := f.bridge.BeginLocalModelOperation(other.actual.source, allocation, op, f.parent.graph.at); err == nil {
			t.Fatal("foreign actual Source accepted")
		}
		wrong := op.Clone()
		wrong.Scope.SessionID = "other-session"
		if _, err := f.bridge.BeginLocalModelOperation(f.actual.source, allocation, wrong, f.parent.graph.at); err == nil {
			t.Fatal("foreign Session accepted")
		}
		lease, err := f.bridge.BeginLocalModelOperation(f.actual.source, allocation, op, f.parent.graph.at)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = lease.Close() })
		ad, _ := allocation.Digest()
		wrong = op.Clone()
		wrong.RequestDigest = strings.Repeat("2", 64)
		if lease.Consume(wrong, ad, f.parent.graph.at) == nil {
			t.Fatal("changed request consumed original resource")
		}
		if lease.Consume(op, ad, f.parent.graph.at) == nil {
			t.Fatal("failed consume refunded original resource")
		}
		var absent *serverlessharness.LocalModelOperationLeaseV1
		if absent.Context().Err() != context.Canceled || absent.Consume(op, ad, f.parent.graph.at) == nil {
			t.Fatal("nil lease remained live")
		}
	})
}
