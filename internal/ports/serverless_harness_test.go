package ports_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	serverlessruntime "gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func TestReserveAttemptEffectRequestContainsOnlyLocatorsAndClaim(t *testing.T) {
	t.Parallel()
	upstream := strings.Repeat("a", 64)
	base := ports.ReserveAttemptEffectRequestV1{
		TenantID: "tenant-1", RunID: "run-1", AttemptID: "attempt-1", LeaseID: "lease-1",
		FenceToken: 1, PhysicalInvocationClaimID: "physical-claim-1", UpstreamIdempotencyKeyDigest: &upstream,
	}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ports.ReserveAttemptEffectRequestV1){
		"tenant":         func(value *ports.ReserveAttemptEffectRequestV1) { value.TenantID = "" },
		"run":            func(value *ports.ReserveAttemptEffectRequestV1) { value.RunID = "" },
		"attempt":        func(value *ports.ReserveAttemptEffectRequestV1) { value.AttemptID = "" },
		"lease":          func(value *ports.ReserveAttemptEffectRequestV1) { value.LeaseID = "" },
		"fence":          func(value *ports.ReserveAttemptEffectRequestV1) { value.FenceToken = 0 },
		"physical claim": func(value *ports.ReserveAttemptEffectRequestV1) { value.PhysicalInvocationClaimID = "" },
		"upstream digest": func(value *ports.ReserveAttemptEffectRequestV1) {
			invalid := strings.Repeat("A", 64)
			value.UpstreamIdempotencyKeyDigest = &invalid
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("invalid effect request accepted")
			}
		})
	}
}

func TestReserveAttemptEffectResultRequiresExactAuthority(t *testing.T) {
	t.Parallel()
	at := time.Unix(20, 0).UTC()
	managed, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(
		"tenant-1", "user-1", "run-1", "attempt-1", "subscription-1", time.Unix(10, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	authority := domain.ServerlessInvocationAuthorityV1{
		Version:        domain.ServerlessInvocationAuthorityVersionV1,
		HarnessBinding: managed.HarnessBinding, ExecutionPlacementV2: managed.ExecutionPlacementV2,
		SubstrateBinding: managed.SubstrateBinding, AdmissionCostCeiling: managed.AdmissionCostCeiling,
		Lease: domain.Lease{ID: "lease-1", TenantID: "tenant-1", RunID: "run-1", AttemptID: "attempt-1",
			WorkerID: "managed-worker-1", FenceToken: 1, AcquiredAt: at, ExpiresAt: at.Add(time.Hour)},
		ContextManifestDigest: strings.Repeat("b", 64), InputManifestDigest: strings.Repeat("c", 64),
		InvocationDeadline: at.Add(50 * time.Minute),
	}
	reservation, err := domain.BuildAttemptEffectReservationV1(authority, "physical-claim-1", nil, at)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := serverlessruntime.NewCapabilityIssuer(func() time.Time { return at }, strings.NewReader(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, reservation, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []ports.AttemptEffectReservationStatusV1{ports.AttemptEffectOwnedV1, ports.AttemptEffectReplayedV1} {
		result := ports.ReserveAttemptEffectResultV1{Status: status, Reservation: reservation.Clone(), Grant: ptr(grant.Clone())}
		if err := result.Validate(); err != nil {
			t.Fatalf("status %q rejected: %v", status, err)
		}
	}
	reconcile := ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectReconcileOnlyV1, Reservation: reservation.Clone()}
	if err := reconcile.Validate(); err != nil {
		t.Fatalf("reconcile-only result rejected: %v", err)
	}
	invalid := ports.ReserveAttemptEffectResultV1{Status: "unknown", Reservation: reservation}
	if err := invalid.Validate(); err == nil {
		t.Fatal("unknown effect result status accepted")
	}
	divergentGrant := grant.Clone()
	divergentGrant.Reservation.PhysicalInvocationClaimID = "physical-claim-2"
	divergent := ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectOwnedV1, Reservation: reservation.Clone(), Grant: &divergentGrant}
	if err := divergent.Validate(); err == nil {
		t.Fatal("reservation accepted divergent grant")
	}
	if err := (ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectOwnedV1, Reservation: reservation}).Validate(); err == nil {
		t.Fatal("owned effect accepted without grant")
	}
	if err := (ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectReconcileOnlyV1, Reservation: reservation, Grant: &grant}).Validate(); err == nil {
		t.Fatal("reconcile-only effect accepted a grant")
	}
}

func ptr[T any](value T) *T { return &value }
