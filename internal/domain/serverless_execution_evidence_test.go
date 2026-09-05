package domain_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func validSubstrateEvidenceInputs(t testing.TB) (domain.ServerlessInvocationAuthorityV1, domain.AttemptEffectReservationV1, domain.PreparedAllocationV1, domain.PreparedInvocationDigestV1) {
	t.Helper()
	authority, at := validServerlessInvocationAuthority(t)
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
	if err := allocation.ValidateForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend); err != nil {
		t.Fatal(err)
	}
	return authority, reservation, allocation, domain.PreparedInvocationDigestV1(strings.Repeat("f", 64))
}

func completedProviderEvidence(t testing.TB, binding domain.HarnessBindingV1) domain.ProviderExecutionEvidenceV1 {
	t.Helper()
	value, err := (domain.ProviderExecutionEvidenceV1{
		AcceptanceClass:     domain.ProviderAcceptanceAcceptedV1,
		FinishClass:         domain.ProviderFinishCompletedV1,
		RouteState:          domain.ProviderEvidenceSupportedV1,
		ActualModelVendorID: binding.ModelVendorID,
		ActualModelID:       binding.ModelID,
		TransportKind:       domain.ProviderTransportDirectAPIV1,
		TransportProvider:   "fixture-transport",
		UpstreamProviderID:  "fixture-provider",
		EndpointID:          "fixture-endpoint",
		PolicyVerdict:       domain.ProviderPolicyGoV1,
		UsageProvenance:     domain.ProviderUsageUnknownV1,
	}).SealForBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func unknownSubstrateResources() []domain.SubstrateResourceObservationV1 {
	return []domain.SubstrateResourceObservationV1{
		{Kind: domain.SubstrateResourceCPUTimeV1, State: domain.SubstrateResourceUnknownV1, Provenance: domain.SubstrateResourceProvenanceUnknownV1},
		{Kind: domain.SubstrateResourceEgressBytesV1, State: domain.SubstrateResourceUnknownV1, Provenance: domain.SubstrateResourceProvenanceUnknownV1},
		{Kind: domain.SubstrateResourceEvidenceBytesV1, State: domain.SubstrateResourceUnknownV1, Provenance: domain.SubstrateResourceProvenanceUnknownV1},
		{Kind: domain.SubstrateResourceIngressBytesV1, State: domain.SubstrateResourceUnknownV1, Provenance: domain.SubstrateResourceProvenanceUnknownV1},
		{Kind: domain.SubstrateResourceLogBytesV1, State: domain.SubstrateResourceUnknownV1, Provenance: domain.SubstrateResourceProvenanceUnknownV1},
		{Kind: domain.SubstrateResourceMemoryPeakV1, State: domain.SubstrateResourceUnknownV1, Provenance: domain.SubstrateResourceProvenanceUnknownV1},
		{Kind: domain.SubstrateResourceScratchPeakV1, State: domain.SubstrateResourceUnknownV1, Provenance: domain.SubstrateResourceProvenanceUnknownV1},
	}
}

func validSubstrateEvidence(t testing.TB) (domain.SubstrateExecutionEvidenceV1, domain.ServerlessInvocationAuthorityV1, domain.AttemptEffectReservationV1, domain.PreparedAllocationV1, domain.PreparedInvocationDigestV1) {
	t.Helper()
	authority, reservation, allocation, preparedDigest := validSubstrateEvidenceInputs(t)
	provider := completedProviderEvidence(t, authority.HarnessBinding)
	value, err := (domain.SubstrateExecutionEvidenceV1{
		Allocation:             domain.SubstrateAllocationStartedV1,
		Process:                domain.SubstrateProcessNotApplicableV1,
		CredentialFinalization: domain.CredentialFinalizationNotRequiredV1,
		Cleanup:                domain.SubstrateCleanupVerifiedV1,
		Egress:                 domain.SubstrateEgressPolicyEnforcedV1,
		ImageAttestation:       domain.SubstrateAttestationVerifiedV1,
		BackendAttestation:     domain.SubstrateAttestationVerifiedV1,
		ProxyAttestation:       domain.SubstrateProxyAttestationVerifiedV1,
		Cancellation: domain.SubstrateCancellationEvidenceV1{
			Request: domain.SubstrateCancellationRequestNoneV1, BackendSignal: domain.SubstrateCancellationSignalNotRequiredV1,
		},
		ProviderEvidence:     &provider,
		ResourceObservations: unknownSubstrateResources(),
		FailureCode:          domain.SubstrateExecutionFailureNoneV1,
	}).SealForAuthority(authority, reservation, allocation, preparedDigest)
	if err != nil {
		t.Fatal(err)
	}
	return value, authority, reservation, allocation, preparedDigest
}

func TestSubstrateExecutionEvidenceExactBindingAndMutationResistance(t *testing.T) {
	t.Parallel()
	value, authority, reservation, allocation, preparedDigest := validSubstrateEvidence(t)
	if err := value.ValidateForAuthority(authority, reservation, allocation, preparedDigest); err != nil {
		t.Fatal(err)
	}
	if err := value.ValidateForPersistedAuthority(authority, reservation); err != nil {
		t.Fatalf("validate persisted authority: %v", err)
	}
	digest, err := value.DigestForAuthority(authority, reservation, allocation, preparedDigest)
	if err != nil {
		t.Fatal(err)
	}
	if digest != value.EvidenceDigest {
		t.Fatal("canonical digest differs from sealed evidence digest")
	}

	for name, mutate := range map[string]func(*domain.SubstrateExecutionEvidenceV1){
		"authority": func(candidate *domain.SubstrateExecutionEvidenceV1) {
			candidate.InvocationAuthorityDigest = domain.ServerlessInvocationAuthorityDigestV1(strings.Repeat("a", 64))
		},
		"substrate": func(candidate *domain.SubstrateExecutionEvidenceV1) {
			candidate.SubstrateBindingDigest = domain.SubstrateBindingDigestV1(strings.Repeat("b", 64))
		},
		"cost": func(candidate *domain.SubstrateExecutionEvidenceV1) {
			candidate.AdmissionCostCeilingDigest = domain.AdmissionCostCeilingDigestV1(strings.Repeat("c", 64))
		},
		"prepared": func(candidate *domain.SubstrateExecutionEvidenceV1) {
			candidate.PreparedInvocationDigest = domain.PreparedInvocationDigestV1(strings.Repeat("d", 64))
		},
		"reservation": func(candidate *domain.SubstrateExecutionEvidenceV1) {
			candidate.EffectReservationDigest = domain.AttemptEffectReservationDigestV1(strings.Repeat("e", 64))
		},
		"allocation": func(candidate *domain.SubstrateExecutionEvidenceV1) {
			candidate.PreparedAllocationDigest = domain.PreparedAllocationDigestV1(strings.Repeat("0", 64))
		},
		"claim": func(candidate *domain.SubstrateExecutionEvidenceV1) {
			candidate.PhysicalInvocationClaimID = "other-claim"
		},
		"provider": func(candidate *domain.SubstrateExecutionEvidenceV1) {
			candidate.ProviderEvidence.ActualModelID = "other-model"
		},
		"digest": func(candidate *domain.SubstrateExecutionEvidenceV1) {
			candidate.EvidenceDigest = domain.SubstrateExecutionEvidenceDigestV1(strings.Repeat("1", 64))
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := value.Clone()
			mutate(&candidate)
			if candidate.ValidateForAuthority(authority, reservation, allocation, preparedDigest) == nil {
				t.Fatal("mutated evidence accepted")
			}
		})
	}

	wrongPrepared := domain.PreparedInvocationDigestV1(strings.Repeat("2", 64))
	if value.ValidateForAuthority(authority, reservation, allocation, wrongPrepared) == nil {
		t.Fatal("evidence accepted a different opaque prepared capability")
	}
	mutatedPersisted := value.Clone()
	mutatedPersisted.PhysicalInvocationClaimID = "other-claim"
	if mutatedPersisted.ValidateForPersistedAuthority(authority, reservation) == nil {
		t.Fatal("persisted-authority validation accepted a different physical claim")
	}
}

func TestSubstrateExecutionEvidenceUnknownDoesNotCollapseIntoObservedZero(t *testing.T) {
	t.Parallel()
	unknown, authority, reservation, allocation, preparedDigest := validSubstrateEvidence(t)
	zero := unknown.Clone()
	quantity := uint64(0)
	observedAt := time.Unix(30, 0).UTC()
	zero.ResourceObservations[0] = domain.SubstrateResourceObservationV1{
		Kind: domain.SubstrateResourceCPUTimeV1, State: domain.SubstrateResourceObservedV1,
		Quantity: &quantity, Unit: domain.SubstrateResourceUnitNanosecondsV1,
		Provenance: domain.SubstrateResourceProvenanceHarnessMeasuredV1, ObservedAt: &observedAt,
	}
	zero.EvidenceDigest = ""
	sealed, err := zero.SealForAuthority(authority, reservation, allocation, preparedDigest)
	if err != nil {
		t.Fatal(err)
	}
	if sealed.EvidenceDigest == unknown.EvidenceDigest || sealed.ResourceObservations[0].Quantity == nil || *sealed.ResourceObservations[0].Quantity != 0 {
		t.Fatal("observed zero collapsed into unknown")
	}

	clone := sealed.Clone()
	*clone.ResourceObservations[0].Quantity = 9
	clone.ProviderEvidence.ActualModelID = "mutated"
	if err := sealed.ValidateForAuthority(authority, reservation, allocation, preparedDigest); err != nil {
		t.Fatalf("clone mutation changed original: %v", err)
	}
}

func TestSubstrateExecutionEvidenceRejectsContradictoryCombinations(t *testing.T) {
	t.Parallel()
	base, authority, reservation, allocation, preparedDigest := validSubstrateEvidence(t)

	for name, mutate := range map[string]func(*domain.SubstrateExecutionEvidenceV1){
		"rejected but running": func(value *domain.SubstrateExecutionEvidenceV1) {
			value.Allocation = domain.SubstrateAllocationRejectedV1
			value.Process = domain.SubstrateProcessRunningV1
		},
		"cleanup verified before credential finalization": func(value *domain.SubstrateExecutionEvidenceV1) {
			value.CredentialFinalization = domain.CredentialFinalizationUnknownV1
		},
		"completed provider without enforced egress": func(value *domain.SubstrateExecutionEvidenceV1) {
			value.Egress = domain.SubstrateEgressUnknownV1
		},
		"attestation mismatch after provider effect": func(value *domain.SubstrateExecutionEvidenceV1) {
			value.ImageAttestation = domain.SubstrateAttestationMismatchV1
		},
		"acknowledgement without cancellation request": func(value *domain.SubstrateExecutionEvidenceV1) {
			value.Cancellation.BackendSignal = domain.SubstrateCancellationSignalAcknowledgedV1
		},
		"success with teardown failure": func(value *domain.SubstrateExecutionEvidenceV1) {
			value.SecondaryFailureCodes = []domain.SubstrateExecutionFailureCodeV1{domain.SubstrateExecutionFailureCleanupFailedV1}
		},
		"unsorted secondary failures": func(value *domain.SubstrateExecutionEvidenceV1) {
			value.FailureCode = domain.SubstrateExecutionFailureProcessFailedV1
			value.Process = domain.SubstrateProcessStoppedV1
			value.Cleanup = domain.SubstrateCleanupFailedV1
			value.SecondaryFailureCodes = []domain.SubstrateExecutionFailureCodeV1{domain.SubstrateExecutionFailureCredentialFinalizeFailedV1, domain.SubstrateExecutionFailureCleanupFailedV1}
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			mutate(&candidate)
			candidate.EvidenceDigest = ""
			if _, err := candidate.SealForAuthority(authority, reservation, allocation, preparedDigest); err == nil {
				t.Fatal("contradictory evidence accepted")
			}
		})
	}
}

func TestSubstrateExecutionEvidencePreservesIndependentTerminalFacts(t *testing.T) {
	t.Parallel()
	base, authority, reservation, allocation, preparedDigest := validSubstrateEvidence(t)
	base.FailureCode = domain.SubstrateExecutionFailureCleanupFailedV1
	base.Cleanup = domain.SubstrateCleanupFailedV1
	base.EvidenceDigest = ""
	sealed, err := base.SealForAuthority(authority, reservation, allocation, preparedDigest)
	if err != nil {
		t.Fatal(err)
	}
	if sealed.ProviderEvidence.FinishClass != domain.ProviderFinishCompletedV1 || sealed.Cleanup != domain.SubstrateCleanupFailedV1 {
		t.Fatal("provider completion overwrote independent cleanup failure")
	}
}

func TestSubstrateExecutionEvidenceJSONIsContentFree(t *testing.T) {
	t.Parallel()
	value, _, _, _, _ := validSubstrateEvidence(t)
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(encoded))
	for _, forbidden := range []string{"prompt", "instruction", "provider_response", "raw_body", "stdout", "stderr", "api_key", "bearer", "credential_material", "secret", "terminal_commit"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("public evidence JSON exposes forbidden marker %q", forbidden)
		}
	}

	typeOfEvidence := reflect.TypeOf(domain.SubstrateExecutionEvidenceV1{})
	for index := 0; index < typeOfEvidence.NumField(); index++ {
		field := strings.ToLower(typeOfEvidence.Field(index).Name + " " + typeOfEvidence.Field(index).Tag.Get("json"))
		for _, forbidden := range []string{"prompt", "instruction", "response", "raw", "stdout", "stderr", "secret", "terminalcommit", "terminal_commit"} {
			if strings.Contains(field, forbidden) {
				t.Fatalf("public evidence field exposes forbidden marker %q: %s", forbidden, field)
			}
		}
	}
}
