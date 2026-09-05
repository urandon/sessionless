package domain_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func TestAttemptEffectReconciliationEvidenceSealsExactReservation(t *testing.T) {
	t.Parallel()
	at := time.Unix(20, 0).UTC()
	managed, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(
		"tenant-1", "user-1", "run-1", "attempt-1", "subscription-1", time.Unix(10, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	authority := domain.ServerlessInvocationAuthorityV1{
		Version: domain.ServerlessInvocationAuthorityVersionV1, HarnessBinding: managed.HarnessBinding,
		ExecutionPlacementV2: managed.ExecutionPlacementV2, SubstrateBinding: managed.SubstrateBinding,
		AdmissionCostCeiling:  managed.AdmissionCostCeiling,
		Lease:                 domain.Lease{ID: "lease-1", TenantID: "tenant-1", RunID: "run-1", AttemptID: "attempt-1", WorkerID: "worker-1", FenceToken: 1, AcquiredAt: at, ExpiresAt: at.Add(time.Hour)},
		ContextManifestDigest: strings.Repeat("a", 64), InputManifestDigest: strings.Repeat("b", 64), InvocationDeadline: at.Add(30 * time.Minute),
	}
	reservation, err := domain.BuildAttemptEffectReservationV1(authority, "claim-1", nil, at)
	if err != nil {
		t.Fatal(err)
	}
	authorityDigest, _ := authority.Digest()
	substrateDigest, _ := authority.SubstrateBinding.Digest()
	evidence, err := domain.SealAttemptEffectReconciliationEvidenceV1(authority, reservation, domain.SubstrateOperationObservationV1{
		State: domain.SubstrateOperationObservedV1, InvocationAuthority: authorityDigest,
		SubstrateBinding: substrateDigest, PhysicalInvocationID: "runtime-operation-1", ObservedAt: at.Add(time.Second),
	})
	if err != nil || evidence.ValidateForPersistedAuthority(authority, reservation) != nil {
		t.Fatalf("seal/validate = %v/%v", err, evidence.ValidateForPersistedAuthority(authority, reservation))
	}
	tampered := evidence
	tampered.PhysicalInvocationClaimID = "claim-2"
	if tampered.ValidateForPersistedAuthority(authority, reservation) == nil {
		t.Fatal("tampered physical claim retained reconciliation evidence authority")
	}
}
