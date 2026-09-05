package serverlessharness

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	outerharness "gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func capabilityFixture(t testing.TB) (domain.ServerlessInvocationAuthorityV1, domain.AttemptEffectReservationV1, domain.PreparedAllocationV1, time.Time) {
	t.Helper()
	at := time.Unix(20, 0).UTC()
	managed, err := outerharness.NewDeterministicFixtureManagedAuthorityV2("tenant-1", "user-1", "run-1", "attempt-1", "subscription-1", time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	authority := domain.ServerlessInvocationAuthorityV1{
		Version:        domain.ServerlessInvocationAuthorityVersionV1,
		HarnessBinding: managed.HarnessBinding, ExecutionPlacementV2: managed.ExecutionPlacementV2,
		SubstrateBinding: managed.SubstrateBinding, AdmissionCostCeiling: managed.AdmissionCostCeiling,
		Lease:                 domain.Lease{ID: "lease-1", TenantID: "tenant-1", RunID: "run-1", AttemptID: "attempt-1", WorkerID: "managed-worker-1", FenceToken: 1, AcquiredAt: at, ExpiresAt: at.Add(time.Hour)},
		ContextManifestDigest: strings.Repeat("a", 64), InputManifestDigest: strings.Repeat("b", 64), InvocationDeadline: at.Add(50 * time.Minute),
	}
	reservation, err := domain.BuildAttemptEffectReservationV1(authority, "physical-claim-1", nil, at)
	if err != nil {
		t.Fatal(err)
	}
	substrateDigest, _ := authority.SubstrateBinding.Digest()
	allocation := domain.PreparedAllocationV1{
		Version: domain.PreparedAllocationVersionV1, SubstrateBindingDigest: substrateDigest,
		ObservedImageDigest:         authority.SubstrateBinding.ImageDigest,
		ObservedOuterHarnessDigest:  authority.SubstrateBinding.OuterHarnessArtifactDigest,
		ObservedProxyArtifactDigest: authority.SubstrateBinding.EgressProxyArtifactDigest,
		ObservedProxyIdentityDigest: authority.SubstrateBinding.EgressProxyIdentityDigest,
		WorkloadMode:                authority.SubstrateBinding.WorkloadMode,
		InProcess:                   &domain.InProcessAttestationV1{LinkedBackendProfileDigest: authority.HarnessBinding.Backend.BackendProfileDigest},
	}
	return authority, reservation, allocation, at
}

func TestPreparedInvocationRequiresExactProcessLocalAuthenticator(t *testing.T) {
	t.Parallel()
	authority, reservation, allocation, now := capabilityFixture(t)
	clock := now
	issuer, err := NewCapabilityIssuer(func() time.Time { return clock }, bytes.NewReader(bytes.Repeat([]byte{1}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, reservation, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := issuer.Issue(grant, allocation)
	if err != nil {
		t.Fatal(err)
	}
	if err := issuer.Validate(prepared); err != nil {
		t.Fatal(err)
	}

	other, err := NewCapabilityIssuer(func() time.Time { return clock }, bytes.NewReader(bytes.Repeat([]byte{2}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	if other.Validate(prepared) == nil {
		t.Fatal("capability crossed process-local issuer")
	}
	mutated := prepared
	mutated.reservation.PhysicalInvocationClaimID = "physical-claim-2"
	if issuer.Validate(mutated) == nil {
		t.Fatal("mutated physical claim retained capability")
	}

	returned := prepared.Authority()
	returned.Lease.FenceToken++
	if err := issuer.Validate(prepared); err != nil {
		t.Fatal("returned clone mutated capability")
	}
	clock = prepared.ExecuteDeadline()
	if issuer.Validate(prepared) == nil {
		t.Fatal("capability accepted at exclusive deadline")
	}
}

func TestPreparedInvocationIsConsumedExactlyOnceBeforeEffect(t *testing.T) {
	t.Parallel()
	authority, reservation, allocation, now := capabilityFixture(t)
	issuer, err := NewCapabilityIssuer(func() time.Time { return now }, bytes.NewReader(bytes.Repeat([]byte{3}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, reservation, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := issuer.Issue(grant, allocation)
	if err != nil {
		t.Fatal(err)
	}
	const contenders = 32
	results := make(chan error, contenders)
	var wait sync.WaitGroup
	for range contenders {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- issuer.Consume(prepared)
		}()
	}
	wait.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		}
	}
	if success != 1 {
		t.Fatalf("successful consumes = %d, want exactly one", success)
	}
}

func TestObservationGrantIsReadOnlyProcessLocalAndShortLived(t *testing.T) {
	t.Parallel()
	authority, reservation, allocation, now := capabilityFixture(t)
	clock := now
	issuer, err := NewCapabilityIssuer(func() time.Time { return clock }, bytes.NewReader(bytes.Repeat([]byte{4}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectObservationGrant(authority, reservation, now, now.Add(time.Minute))
	if err != nil || issuer.VerifyObservationGrant(grant) != nil {
		t.Fatalf("mint/verify observation grant = %v/%v", err, issuer.VerifyObservationGrant(grant))
	}
	if _, err := issuer.Issue(ports.AttemptEffectOwnershipGrantV1{
		Version: ports.AttemptEffectOwnershipGrantVersionV1, Authority: grant.Authority,
		Reservation: grant.Reservation, GrantExpiresAt: grant.GrantExpiresAt,
		Authenticator: grant.Authenticator,
	}, allocation); err == nil {
		t.Fatal("observation authenticator was accepted as execution ownership")
	}
	other, err := NewCapabilityIssuer(func() time.Time { return clock }, bytes.NewReader(bytes.Repeat([]byte{5}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	if other.VerifyObservationGrant(grant) == nil {
		t.Fatal("observation grant crossed process-local issuer")
	}
	clock = grant.GrantExpiresAt
	if issuer.VerifyObservationGrant(grant) == nil {
		t.Fatal("observation grant accepted at exclusive deadline")
	}
}

func TestPreparedInvocationHasNoJSONConstructionSurface(t *testing.T) {
	t.Parallel()
	valueType := reflect.TypeOf(PreparedInvocation{})
	for index := 0; index < valueType.NumField(); index++ {
		if valueType.Field(index).IsExported() {
			t.Fatalf("exported capability field: %s", valueType.Field(index).Name)
		}
	}
	encoded, err := json.Marshal(PreparedInvocation{})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "{}" {
		t.Fatalf("capability JSON = %s", encoded)
	}
}

func TestCapabilityIssuerRejectsMissingOrZeroEntropy(t *testing.T) {
	t.Parallel()
	if _, err := NewCapabilityIssuer(nil, bytes.NewReader(bytes.Repeat([]byte{1}, 32))); err == nil {
		t.Fatal("nil clock accepted")
	}
	if _, err := NewCapabilityIssuer(time.Now, bytes.NewReader(make([]byte, 32))); err == nil {
		t.Fatal("zero authenticator key accepted")
	}
	if _, err := NewCapabilityIssuer(time.Now, bytes.NewReader([]byte{1})); err == nil {
		t.Fatal("short entropy accepted")
	}
}
