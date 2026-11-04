package ydbstore_test

import (
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Explicit PRE-intake policy for the named non-expiry compaction prerequisites,
// not a default for other connected fixtures or a renewal after publication.
const nonExpiryCompactionOperationWindow = 20 * time.Second

func nonExpiryCompactionConnectedLimits(inputTokens uint64) ydbstore.LocalCompactionConnectedLimits {
	return ydbstore.LocalCompactionConnectedLimits{InputTokens: inputTokens, NetworkOperation: nonExpiryCompactionOperationWindow}
}

// Read-only checks of actual authenticated returns and persisted SQL observations.
// The caller declares the required kind sequence independently of the results;
// completing quickly cannot refund an original admitted reservation.
func assertOriginalCompactionActiveBudget(t *testing.T, seed *ydbstore.LocalSessionConnectedSeed, observed []ports.AuthenticatedAttemptOperationObservationV1, kinds []domain.AttemptOperationKindV1) {
	t.Helper()
	const originalActiveLimit = 120 * time.Second
	profile := seed.OriginalProfileReceipt().Profile
	if profile.Limits.MaxActiveDuration != originalActiveLimit || profile.Limits.MaxNetworkOperationDuration != nonExpiryCompactionOperationWindow || seed.Profile.Limits.OperationTimeout != nonExpiryCompactionOperationWindow {
		t.Fatal("non-expiry fixture changed original active allowance or lost its pre-intake duration")
	}
	persisted := seed.PersistedCompactionObservations(t)
	if len(kinds) == 0 || len(kinds) > 5 || len(observed) != len(kinds) || len(persisted) != len(kinds) {
		t.Fatalf("original declared chain incomplete: returned=%d persisted=%d declared=%d", len(observed), len(persisted), len(kinds))
	}
	wantScope := observed[0].Reservation.Operation.Scope
	if wantScope.TenantID != seed.Scope.TenantID || wantScope.SessionID != seed.Scope.SessionID || wantScope.OwnerUserID != seed.Scope.OwnerUserID || wantScope.RunID != seed.Scope.RunID || wantScope.AttemptID != seed.Scope.AttemptID || wantScope.LeaseID != seed.Request.LeaseID || wantScope.FenceToken != seed.Request.FenceToken || wantScope.PhysicalInvocationClaimID != seed.Request.PhysicalInvocationClaimID {
		t.Fatal("returned operation scope does not match the independently constructed original invocation")
	}
	var aggregate domain.AttemptOperationDebitV1
	for i, value := range observed {
		r, o := value.Reservation, value.Observation
		op, kind := r.Operation, kinds[i]
		if kind != domain.AttemptOperationModelV1 && kind != domain.AttemptOperationCompactionV1 {
			t.Fatal("non-expiry prerequisite declared unsupported operation kind", kind)
		}
		if value.Validate() != nil || seed.Operate.VerifyAttemptOperationObservation(value) != nil || r.Validate() != nil || op.Sequence != uint64(i+1) || op.Scope != wantScope || op.Kind != kind || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || !reflect.DeepEqual(o, persisted[i]) || op.Debit.ActiveDuration != nonExpiryCompactionOperationWindow {
			t.Fatalf("original admitted operation mismatch at declared sequence=%d kind=%s actual_kind=%s duration=%s", i+1, kind, op.Kind, op.Debit.ActiveDuration)
		}
		var err error
		aggregate, err = aggregate.Add(op.Debit)
		// Setup grant-body charges are already part of AggregateDebit.BodyBytes;
		// do not erase them by reconstructing unrelated accounting from zero.
		if err != nil || r.AggregateDebit.ActiveDuration != aggregate.ActiveDuration || r.AggregateDebit.Operations != aggregate.Operations || r.AggregateDebit.ModelCalls != aggregate.ModelCalls || r.AggregateDebit.CompactionCalls != aggregate.CompactionCalls || aggregate.ActiveDuration > originalActiveLimit {
			t.Fatalf("non-refundable debit mismatch sequence=%d aggregate_duration=%s summed_duration=%s limit=%s err=%v", op.Sequence, r.AggregateDebit.ActiveDuration, aggregate.ActiveDuration, originalActiveLimit, err)
		}
	}
	if aggregate.ActiveDuration != time.Duration(len(kinds))*nonExpiryCompactionOperationWindow {
		t.Fatalf("original declared chain duration=%s want=%s", aggregate.ActiveDuration, time.Duration(len(kinds))*nonExpiryCompactionOperationWindow)
	}
	t.Logf("non-expiry original chain kinds=%v operations=%d nonrefundable_active=%s original_limit=%s", kinds, aggregate.Operations, aggregate.ActiveDuration, originalActiveLimit)
}
