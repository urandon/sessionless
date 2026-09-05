package serverlessharness

import (
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// InProcessExecutionSubstrateV1 is the reviewed direct-in-process substrate
// used by the credentialless deterministic fixture. The backend descriptor is
// fixed at construction; authenticated invocation authority cannot select a
// different linked implementation.
type InProcessExecutionSubstrateV1 struct {
	now     func() time.Time
	issuer  *CapabilityIssuer
	backend domain.HarnessBackendDescriptorV1
}

func NewInProcessExecutionSubstrateV1(
	now func() time.Time,
	issuer *CapabilityIssuer,
	backend domain.HarnessBackendDescriptorV1,
) (*InProcessExecutionSubstrateV1, error) {
	if now == nil || issuer == nil || backend.Validate() != nil || backend.CredentialDeliveryKind != domain.ProviderCredentialDeliveryNoneV1 {
		return nil, errors.New("in-process substrate requires a clock, capability issuer and valid backend descriptor")
	}
	return &InProcessExecutionSubstrateV1{now: now, issuer: issuer, backend: backend}, nil
}

func (substrate *InProcessExecutionSubstrateV1) Preflight(
	ctx context.Context,
	authority domain.ServerlessInvocationAuthorityV1,
) (domain.PreparedAllocationV1, error) {
	if substrate == nil || substrate.issuer == nil || ctx == nil || ctx.Err() != nil || authority.ValidateAt(substrate.now().UTC()) != nil ||
		authority.HarnessBinding.Backend != substrate.backend || authority.SubstrateBinding.WorkloadMode != domain.SubstrateWorkloadInProcessDirectV1 {
		return domain.PreparedAllocationV1{}, errors.New("in-process substrate authority mismatch")
	}
	binding := authority.SubstrateBinding
	digest, _ := binding.Digest()
	allocation := domain.PreparedAllocationV1{
		Version: domain.PreparedAllocationVersionV1, SubstrateBindingDigest: digest,
		ObservedImageDigest: binding.ImageDigest, ObservedOuterHarnessDigest: binding.OuterHarnessArtifactDigest,
		ObservedProxyArtifactDigest: binding.EgressProxyArtifactDigest, ObservedProxyIdentityDigest: binding.EgressProxyIdentityDigest,
		WorkloadMode: binding.WorkloadMode,
		InProcess:    &domain.InProcessAttestationV1{LinkedBackendProfileDigest: substrate.backend.BackendProfileDigest},
	}
	if err := allocation.ValidateForBinding(binding, substrate.backend); err != nil {
		return domain.PreparedAllocationV1{}, err
	}
	return allocation, nil
}

func (substrate *InProcessExecutionSubstrateV1) Execute(
	ctx context.Context,
	prepared PreparedInvocation,
	request ports.ExecutionRequest,
	sink ports.ExecutionEventSink,
	harness ports.HarnessDriver,
) (ports.ExecutionResult, domain.SubstrateExecutionEvidenceV1, error) {
	if substrate == nil || substrate.issuer == nil || harness == nil || sink == nil {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, errors.New("in-process substrate dependencies are missing")
	}
	if err := substrate.issuer.Consume(prepared); err != nil {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, err
	}
	result, err := harness.Execute(ctx, request, sink)
	if err != nil {
		return result, domain.SubstrateExecutionEvidenceV1{}, err
	}
	evidence, err := sealSuccessfulInProcessEvidence(prepared, result.ProviderEvidence)
	if err != nil {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, err
	}
	return result, evidence, nil
}

func (substrate *InProcessExecutionSubstrateV1) Cancel(
	ctx context.Context,
	authority domain.ServerlessInvocationAuthorityV1,
) (domain.SubstrateOperationObservationV1, error) {
	if ctx == nil || ctx.Err() != nil {
		return domain.SubstrateOperationObservationV1{}, errors.New("in-process substrate cancellation context is unavailable")
	}
	return substrate.observation(authority, domain.SubstrateOperationAcknowledgedV1)
}

func (substrate *InProcessExecutionSubstrateV1) Reconcile(
	ctx context.Context,
	authority domain.ServerlessInvocationAuthorityV1,
) (domain.SubstrateOperationObservationV1, error) {
	if ctx == nil || ctx.Err() != nil {
		return domain.SubstrateOperationObservationV1{}, errors.New("in-process substrate reconciliation context is unavailable")
	}
	return substrate.observation(authority, domain.SubstrateOperationObservedV1)
}

func (substrate *InProcessExecutionSubstrateV1) observation(
	authority domain.ServerlessInvocationAuthorityV1,
	state domain.SubstrateOperationStateV1,
) (domain.SubstrateOperationObservationV1, error) {
	if substrate == nil || substrate.now == nil || authority.Validate() != nil || authority.HarnessBinding.Backend != substrate.backend {
		return domain.SubstrateOperationObservationV1{}, errors.New("in-process substrate authority mismatch")
	}
	authorityDigest, _ := authority.Digest()
	substrateDigest, _ := authority.SubstrateBinding.Digest()
	return domain.SubstrateOperationObservationV1{
		State: state, InvocationAuthority: authorityDigest, SubstrateBinding: substrateDigest,
		PhysicalInvocationID: string(authority.Lease.ID) + "-in-process", ObservedAt: substrate.now().UTC(),
	}, nil
}

func sealSuccessfulInProcessEvidence(
	prepared PreparedInvocation,
	provider *domain.ProviderExecutionEvidenceV1,
) (domain.SubstrateExecutionEvidenceV1, error) {
	if provider == nil {
		return domain.SubstrateExecutionEvidenceV1{}, errors.New("provider evidence is required")
	}
	unknown := func(kind domain.SubstrateResourceKindV1) domain.SubstrateResourceObservationV1 {
		return domain.SubstrateResourceObservationV1{Kind: kind, State: domain.SubstrateResourceUnknownV1, Provenance: domain.SubstrateResourceProvenanceUnknownV1}
	}
	providerClone := provider.Clone()
	evidence := domain.SubstrateExecutionEvidenceV1{
		Allocation: domain.SubstrateAllocationStartedV1, Process: domain.SubstrateProcessNotApplicableV1,
		CredentialFinalization: domain.CredentialFinalizationNotRequiredV1, Cleanup: domain.SubstrateCleanupVerifiedV1,
		Egress: domain.SubstrateEgressPolicyEnforcedV1, ImageAttestation: domain.SubstrateAttestationVerifiedV1,
		BackendAttestation: domain.SubstrateAttestationVerifiedV1, ProxyAttestation: domain.SubstrateProxyAttestationVerifiedV1,
		Cancellation:     domain.SubstrateCancellationEvidenceV1{Request: domain.SubstrateCancellationRequestNoneV1, BackendSignal: domain.SubstrateCancellationSignalNotRequiredV1},
		ProviderEvidence: &providerClone,
		ResourceObservations: []domain.SubstrateResourceObservationV1{
			unknown(domain.SubstrateResourceCPUTimeV1), unknown(domain.SubstrateResourceEgressBytesV1),
			unknown(domain.SubstrateResourceEvidenceBytesV1), unknown(domain.SubstrateResourceIngressBytesV1),
			unknown(domain.SubstrateResourceLogBytesV1), unknown(domain.SubstrateResourceMemoryPeakV1),
			unknown(domain.SubstrateResourceScratchPeakV1),
		},
		FailureCode: domain.SubstrateExecutionFailureNoneV1,
	}
	return evidence.SealForAuthority(prepared.Authority(), prepared.Reservation(), prepared.Allocation(), prepared.Digest())
}

var _ ExecutionSubstrateV1 = (*InProcessExecutionSubstrateV1)(nil)
