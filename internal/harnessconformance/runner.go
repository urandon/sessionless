package harnessconformance

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

type SideEffectObserver interface {
	Snapshot() SideEffectsV1
}

type BackendProtocolObserver interface {
	BackendProtocolState() BackendProtocolStateV1
}

type Runner struct {
	Registry        *sessionlessharness.Registry
	SideEffects     SideEffectObserver
	BackendProtocol BackendProtocolObserver
}

func (runner Runner) Run(ctx context.Context, fixture FixtureV1) (ResultV1, error) {
	if runner.Registry == nil || runner.SideEffects == nil || ctx == nil {
		return ResultV1{}, ErrInvalidFixture
	}
	if err := fixture.Validate(); err != nil {
		return ResultV1{}, ErrInvalidFixture
	}
	fixtureDigest, err := fixture.Digest()
	if err != nil {
		return ResultV1{}, ErrInvalidFixture
	}
	bindingDigest, err := fixture.Binding.Digest()
	if err != nil {
		return ResultV1{}, ErrInvalidFixture
	}

	identity := fixtureIdentity(fixture)
	var executionEvidence *domain.ProviderExecutionEvidenceV1
	var operationErr error
	switch fixture.Operation {
	case OperationPreflightV1:
		operationErr = runner.Registry.Preflight(ctx, identity)
	case OperationCancelV1:
		operationErr = runner.Registry.Cancel(ctx, identity)
	case OperationExecuteV1:
		var executionResult ports.ExecutionResult
		executionResult, operationErr = runner.Registry.Execute(ctx, fixtureRequest(fixture), discardEventSink{})
		executionEvidence = executionResult.ProviderEvidence
		if executionEvidence == nil {
			if operationErr == nil {
				operationErr = &domain.ClassifiedError{Kind: domain.ErrorTerminal, Code: string(sessionlessharness.FailureHarnessBackendFailed), Operation: "provider_conformance.execute"}
			}
		} else if fixture.EvidenceBundle != nil {
			if evidenceErr := fixture.EvidenceBundle.ValidateExecutionEvidence(fixture.Binding, *executionEvidence); evidenceErr != nil {
				operationErr = evidenceErr
			}
		} else if evidenceErr := executionEvidence.ValidateForBinding(fixture.Binding); evidenceErr != nil {
			operationErr = evidenceErr
		}
	default:
		return ResultV1{}, ErrInvalidFixture
	}

	actualContract := RegistryContractPassV1
	failureCode := sessionlessharness.FailureCode("")
	if operationErr != nil {
		actualContract = RegistryContractNoGoV1
		failureCode = closedFailureCode(operationErr)
	}
	backendProtocol := BackendProtocolSkippedV1
	if runner.BackendProtocol != nil {
		candidate := runner.BackendProtocol.BackendProtocolState()
		if candidate == BackendProtocolSupportedV1 || candidate == BackendProtocolUnsupportedV1 || candidate == BackendProtocolSkippedV1 {
			backendProtocol = candidate
		}
	}
	effects := SideEffectsV1{}
	if runner.SideEffects != nil {
		effects = runner.SideEffects.Snapshot()
	}
	checks := []CheckV1{
		{Code: "backend_protocol", Passed: backendProtocol == fixture.Expected.BackendProtocol},
		{Code: "credential_materializations_zero", Passed: effects.CredentialMaterializations == 0},
		{Code: "credential_reads_zero", Passed: effects.CredentialReads == 0},
		{Code: "failure_code", Passed: failureCode == fixture.Expected.FailureCode},
		{Code: "network_starts_zero", Passed: effects.NetworkStarts == 0},
		{Code: "process_starts_zero", Passed: effects.ProcessStarts == 0},
		{Code: "registry_contract", Passed: actualContract == fixture.Expected.RegistryContract},
		{Code: "retry_count_zero", Passed: effects.Retries == 0},
	}
	result := ResultV1{
		Version: VersionV1, FixtureID: fixture.FixtureID, FixtureDigest: fixtureDigest, BindingDigest: bindingDigest,
		RegistryContract: actualContract, BackendProtocol: backendProtocol, FailureCode: failureCode,
		Checks: checks,
	}
	result.SideEffects = effects
	if executionEvidence != nil {
		result.ProviderExecutionEvidenceDigest = executionEvidence.EvidenceDigest
	}
	return result.seal()
}

func fixtureIdentity(fixture FixtureV1) ports.ExecutionIdentity {
	return ports.ExecutionIdentity{
		TenantID: fixture.Binding.TenantID, OwnerUserID: fixture.Binding.OwnerUserID, RunID: fixture.Binding.RunID, AttemptID: fixture.Binding.AttemptID,
		ExecutionPlacementV2: fixture.Placement, HarnessBinding: fixture.Binding.Clone(),
		SubstrateBinding: cloneFixtureSubstrate(fixture.SubstrateBinding), AdmissionCostCeiling: cloneFixtureCost(fixture.AdmissionCostCeiling),
	}
}

func fixtureRequest(fixture FixtureV1) ports.ExecutionRequest {
	binding := fixture.Binding
	request := ports.ExecutionRequest{
		TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, RunID: binding.RunID,
		SessionID: "session-conformance", TriggerEventID: "event-conformance", AttemptID: binding.AttemptID,
		WorkDir: "/sessionless-conformance/work", ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1},
		ExecutionPlacementV2: fixture.Placement, HarnessBinding: binding.Clone(),
		SubstrateBinding: cloneFixtureSubstrate(fixture.SubstrateBinding), AdmissionCostCeiling: cloneFixtureCost(fixture.AdmissionCostCeiling),
	}
	if binding.Resource.CredentialMode == domain.ProviderCredentialInvocationV1 {
		expiresAt := time.Unix(4102444800, 0).UTC()
		if binding.EvidenceExpiresAt != nil {
			expiresAt = binding.EvidenceExpiresAt.UTC()
		}
		request.Credential = ports.ProviderInvocationCredentialV1{
			HandleID: "credential-conformance", TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID,
			RunID: binding.RunID, AttemptID: binding.AttemptID, WorkerID: "worker-conformance",
			LeaseID: "lease-conformance", LeaseFence: 1, ProviderResource: binding.Resource, ExpiresAt: expiresAt,
		}
		switch binding.Backend.CredentialDeliveryKind {
		case domain.ProviderCredentialDeliveryFileV1:
			request.CredentialMaterialization = ports.ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryFileV1, RootDir: "/sessionless-conformance/credential", FilePath: "/sessionless-conformance/credential/provider.json"}
		case domain.ProviderCredentialDeliveryEnvironmentV1:
			request.CredentialMaterialization = ports.ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryEnvironmentV1, EnvironmentName: "SESSIONLESS_PROVIDER_TOKEN"}
		case domain.ProviderCredentialDeliveryDirectV1:
			request.CredentialMaterialization = ports.ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryDirectV1}
		}
	}
	return request
}

func cloneFixtureSubstrate(value domain.SubstrateBindingV1) *domain.SubstrateBindingV1 {
	clone := value
	return &clone
}

func cloneFixtureCost(value domain.AdmissionCostCeilingV1) *domain.AdmissionCostCeilingV1 {
	clone := value.Clone()
	return &clone
}

type discardEventSink struct{}

func (discardEventSink) Emit(context.Context, ports.ExecutionEvent) error { return nil }
