package domain_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func validServerlessInvocationAuthority(t testing.TB) (domain.ServerlessInvocationAuthorityV1, time.Time) {
	t.Helper()
	issuedAt := time.Unix(20, 0).UTC()
	managed := deterministicManagedAuthority(t, "tenant-1", "user-1", "run-1", "attempt-1", time.Unix(10, 0).UTC())
	value := domain.ServerlessInvocationAuthorityV1{
		Version:        domain.ServerlessInvocationAuthorityVersionV1,
		HarnessBinding: managed.HarnessBinding, ExecutionPlacementV2: managed.ExecutionPlacementV2,
		SubstrateBinding: managed.SubstrateBinding, AdmissionCostCeiling: managed.AdmissionCostCeiling,
		Lease: domain.Lease{
			ID: "lease-1", TenantID: "tenant-1", RunID: "run-1", AttemptID: "attempt-1",
			WorkerID: "managed-worker-1", FenceToken: 1, AcquiredAt: issuedAt, ExpiresAt: issuedAt.Add(time.Hour),
		},
		ContextManifestDigest: strings.Repeat("a", 64), InputManifestDigest: strings.Repeat("b", 64),
		InvocationDeadline: issuedAt.Add(50 * time.Minute),
	}
	if err := value.ValidateAt(issuedAt); err != nil {
		t.Fatal(err)
	}
	return value, issuedAt
}

func TestServerlessInvocationAuthorityAndEffectReservationAreExact(t *testing.T) {
	t.Parallel()
	authority, at := validServerlessInvocationAuthority(t)
	digest, err := authority.Digest()
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := domain.BuildAttemptEffectReservationV1(authority, "physical-claim-1", nil, at)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reservation.DigestForAuthority(authority); err != nil {
		t.Fatal(err)
	}
	if reservation.EffectSequence != domain.ServerlessProviderEffectSequenceV1 || reservation.Kind != domain.ProviderEffectTurnV1 {
		t.Fatalf("noncanonical effect reservation: %+v", reservation)
	}

	for name, mutate := range map[string]func(*domain.ServerlessInvocationAuthorityV1){
		"placement": func(value *domain.ServerlessInvocationAuthorityV1) {
			value.ExecutionPlacementV2.SubstrateBindingDigest = strings.Repeat("c", 64)
		},
		"cost": func(value *domain.ServerlessInvocationAuthorityV1) {
			changed := *value.AdmissionCostCeiling.MaxTotalAmountMicrounits + 1
			value.AdmissionCostCeiling.MaxTotalAmountMicrounits = &changed
		},
		"lease fence": func(value *domain.ServerlessInvocationAuthorityV1) { value.Lease.FenceToken++ },
		"context": func(value *domain.ServerlessInvocationAuthorityV1) {
			value.ContextManifestDigest = strings.Repeat("d", 64)
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := authority.Clone()
			mutate(&candidate)
			candidateDigest, candidateErr := candidate.Digest()
			if candidateErr == nil && candidateDigest == digest {
				t.Fatal("authority mutation preserved digest")
			}
			if reservation.ValidateForAuthority(candidate) == nil {
				t.Fatal("effect reservation accepted divergent authority")
			}
		})
	}

	wrongSequence := reservation.Clone()
	wrongSequence.EffectSequence++
	if wrongSequence.ValidateForAuthority(authority) == nil {
		t.Fatal("caller-selected effect sequence accepted")
	}
}

func TestAdmissionCostUnknownAndKnownFreeRemainDistinct(t *testing.T) {
	t.Parallel()
	authority, at := validServerlessInvocationAuthority(t)
	knownFree := authority.AdmissionCostCeiling.Clone()
	if knownFree.ProviderPriceState != domain.ProviderPriceKnownFreeV1 || knownFree.MaxProviderAmountMicrounits == nil || *knownFree.MaxProviderAmountMicrounits != 0 {
		t.Fatalf("fixture does not preserve known-free zero: %+v", knownFree)
	}
	unknown := knownFree.Clone()
	unknown.ProviderPriceState = domain.ProviderPriceUnknownV1
	unknown.MaxProviderAmountMicrounits = nil
	unknown.MaxTotalAmountMicrounits = nil
	if err := unknown.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := unknown.ValidateAt(at); err == nil {
		t.Fatal("unknown provider price authorized execution")
	}
	knownDigest, _ := knownFree.Digest()
	unknownDigest, _ := unknown.Digest()
	if knownDigest == unknownDigest {
		t.Fatal("unknown provider price collapsed into known-free zero")
	}
}

func TestServerlessWorkerJobDigestsBindContextAndManifestContent(t *testing.T) {
	t.Parallel()
	job := attachedContextJob(t)
	managed := deterministicManagedAuthority(t, job.TenantID, job.CredentialOwnerUserID, job.RunID, job.AttemptID, time.Unix(10, 0).UTC())
	job.ExecutionPlacementV2 = managed.ExecutionPlacementV2
	job.HarnessBinding = managed.HarnessBinding
	substrate, cost := managed.SubstrateBinding, managed.AdmissionCostCeiling.Clone()
	job.SubstrateBinding, job.AdmissionCostCeiling = &substrate, &cost
	manifest := attachedContextManifest(job)
	manifest.Artifacts = append(manifest.Artifacts, domain.Artifact{
		Name: "second", MediaType: "text/plain",
		Blob: domain.BlobRef{TenantID: job.TenantID, Key: "tenants/tenant-1/second.txt", Size: 3, SHA256: strings.Repeat("b", 64)},
	})

	contextDigest, inputDigest, err := domain.ServerlessWorkerJobDigestsV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	reordered := manifest
	reordered.Artifacts = []domain.Artifact{manifest.Artifacts[1], manifest.Artifacts[0]}
	reorderedContext, reorderedInput, err := domain.ServerlessWorkerJobDigestsV1(job, reordered)
	if err != nil {
		t.Fatal(err)
	}
	if contextDigest != reorderedContext || inputDigest != reorderedInput {
		t.Fatal("semantic manifest set order changed a canonical digest")
	}

	changedManifest := reordered
	changedManifest.Artifacts = append([]domain.Artifact(nil), reordered.Artifacts...)
	changedManifest.Artifacts[0].Blob.SHA256 = strings.Repeat("c", 64)
	changedContext, changedInput, err := domain.ServerlessWorkerJobDigestsV1(job, changedManifest)
	if err != nil {
		t.Fatal(err)
	}
	if changedInput == inputDigest || changedContext == contextDigest {
		t.Fatal("manifest content mutation did not change both sealed digests")
	}

	changedJob := job
	changedJob.AllowedMCPServers = append([]string(nil), job.AllowedMCPServers...)
	changedJob.AllowedMCPServers[0] = "database"
	changedContext, unchangedInput, err := domain.ServerlessWorkerJobDigestsV1(changedJob, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if changedContext == contextDigest || unchangedInput != inputDigest {
		t.Fatal("context-only mutation was not kept separate from input manifest digest")
	}

	job.ExecutionPlacementV2 = domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker}
	if _, _, err := domain.ServerlessWorkerJobDigestsV1(job, manifest); err == nil {
		t.Fatal("attached-worker placement was accepted as serverless authority")
	}
}

func TestServerlessWorkerJobDigestsNormalizeFiniteLegacyBudgets(t *testing.T) {
	t.Parallel()
	legacy := attachedContextJob(t)
	managed := deterministicManagedAuthority(t, legacy.TenantID, legacy.CredentialOwnerUserID, legacy.RunID, legacy.AttemptID, time.Unix(10, 0).UTC())
	legacy.ExecutionPlacementV2 = managed.ExecutionPlacementV2
	legacy.HarnessBinding = managed.HarnessBinding
	substrate, cost := managed.SubstrateBinding, managed.AdmissionCostCeiling.Clone()
	legacy.SubstrateBinding, legacy.AdmissionCostCeiling = &substrate, &cost
	legacy.Limits.MaxContextEvents = 0
	legacy.Limits.MaxToolEvents = 0
	legacy.Limits.MaxToolEventBytes = 0

	explicit := legacy
	explicit.Limits.MaxContextEvents = legacy.Limits.EffectiveMaxContextEvents()
	explicit.Limits.MaxToolEvents, explicit.Limits.MaxToolEventBytes = legacy.Limits.EffectiveToolEventLimits()
	manifest := attachedContextManifest(legacy)

	legacyContext, legacyInput, err := domain.ServerlessWorkerJobDigestsV1(legacy, manifest)
	if err != nil {
		t.Fatal(err)
	}
	explicitContext, explicitInput, err := domain.ServerlessWorkerJobDigestsV1(explicit, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if legacyContext != explicitContext || legacyInput != explicitInput {
		t.Fatal("legacy zero fields did not normalize to their finite effective budgets")
	}
}

func TestPreparedAllocationExactMatchesSealedInProcessProfile(t *testing.T) {
	t.Parallel()
	authority, _ := validServerlessInvocationAuthority(t)
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
	if _, err := allocation.DigestForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend); err != nil {
		t.Fatal(err)
	}
	mutated := allocation.Clone()
	mutated.ObservedImageDigest = strings.Repeat("e", 64)
	if mutated.ValidateForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend) == nil {
		t.Fatal("image substitution accepted")
	}
}
