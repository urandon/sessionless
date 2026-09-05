package harnessconformance

import (
	"context"
	"sync"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

// SideEffectRecorder records only bounded conformance counters. It never
// records credentials, prompts, paths, provider payloads, or native errors.
type SideEffectRecorder struct {
	mu      sync.Mutex
	current SideEffectsV1
}

func (recorder *SideEffectRecorder) Snapshot() SideEffectsV1 {
	if recorder == nil {
		return SideEffectsV1{}
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return recorder.current
}

func (recorder *SideEffectRecorder) record(call OperationV1) {
	if recorder == nil {
		return
	}
	recorder.mu.Lock()
	switch call {
	case OperationPreflightV1:
		recorder.current.DriverPreflights++
	case OperationExecuteV1:
		recorder.current.DriverExecutes++
	case OperationCancelV1:
		recorder.current.DriverCancels++
	}
	recorder.mu.Unlock()
}

func (recorder *SideEffectRecorder) recordValidator() {
	if recorder == nil {
		return
	}
	recorder.mu.Lock()
	recorder.current.ValidatorCalls++
	recorder.mu.Unlock()
}

func (recorder *SideEffectRecorder) RecordCredentialRead() {
	recorder.increment(func(value *SideEffectsV1) { value.CredentialReads++ })
}

func (recorder *SideEffectRecorder) RecordCredentialMaterialization() {
	recorder.increment(func(value *SideEffectsV1) { value.CredentialMaterializations++ })
}

func (recorder *SideEffectRecorder) RecordProcessStart() {
	recorder.increment(func(value *SideEffectsV1) { value.ProcessStarts++ })
}

func (recorder *SideEffectRecorder) RecordNetworkStart() {
	recorder.increment(func(value *SideEffectsV1) { value.NetworkStarts++ })
}

func (recorder *SideEffectRecorder) RecordRetry() {
	recorder.increment(func(value *SideEffectsV1) { value.Retries++ })
}

func (recorder *SideEffectRecorder) increment(update func(*SideEffectsV1)) {
	if recorder == nil || update == nil {
		return
	}
	recorder.mu.Lock()
	update(&recorder.current)
	recorder.mu.Unlock()
}

// FixtureDriver proves the provider-neutral registry contract only. It is not
// evidence that a Codex, OpenCode, Pi, or OpenRouter native protocol adapter
// exists or conforms.
type FixtureDriver struct {
	binding    domain.HarnessBindingV1
	bundle     *EvidenceBundleV1
	recorder   *SideEffectRecorder
	executeErr error
}

func RegistrationForFixture(fixture FixtureV1, recorder *SideEffectRecorder) (sessionlessharness.Registration, *FixtureDriver, error) {
	if err := fixture.Validate(); err != nil {
		return sessionlessharness.Registration{}, nil, ErrInvalidFixture
	}
	fixture = fixture.Clone()
	driver := &FixtureDriver{binding: fixture.Binding.Clone(), recorder: recorder}
	if fixture.EvidenceBundle != nil {
		bundle := fixture.EvidenceBundle.Clone()
		driver.bundle = &bundle
	}
	if _, err := fixture.Binding.Digest(); err != nil {
		return sessionlessharness.Registration{}, nil, ErrInvalidFixture
	}
	registration := sessionlessharness.Registration{
		Descriptor: fixture.Binding.Backend,
		Enabled:    true,
		Driver:     driver,
		ValidateBinding: func(candidate domain.HarnessBindingV1) sessionlessharness.FailureCode {
			recorder.recordValidator()
			if err := candidate.Validate(); err != nil {
				return sessionlessharness.FailureHarnessBindingInvalid
			}
			reference := fixture.Binding
			if candidate.TenantID != reference.TenantID || candidate.OwnerUserID != reference.OwnerUserID || candidate.RunID != reference.RunID || candidate.AttemptID != reference.AttemptID {
				return sessionlessharness.FailureHarnessBackendMismatch
			}
			if candidate.Resource.Kind != reference.Resource.Kind || candidate.Resource.ResourceID != reference.Resource.ResourceID || candidate.Resource.OwnerUserID != reference.Resource.OwnerUserID || candidate.Resource.CredentialMode != reference.Resource.CredentialMode {
				return sessionlessharness.FailureProviderResourceMismatch
			}
			if candidate.Resource.Revision != reference.Resource.Revision {
				return sessionlessharness.FailureProviderRevisionMismatch
			}
			if candidate.Resource.CredentialGeneration != reference.Resource.CredentialGeneration {
				return sessionlessharness.FailureCredentialGeneration
			}
			if candidate.ModelVendorID != reference.ModelVendorID || candidate.ModelID != reference.ModelID || candidate.ProviderCatalogDigest != reference.ProviderCatalogDigest {
				return sessionlessharness.FailureProviderCatalogExpired
			}
			if candidate.ProviderRouteDigest != reference.ProviderRouteDigest {
				return sessionlessharness.FailureProviderRouteMismatch
			}
			if candidate.PrivacyPolicyDigest != reference.PrivacyPolicyDigest {
				return sessionlessharness.FailurePrivacyPolicyMismatch
			}
			if candidate.CapabilityEvidenceDigest != reference.CapabilityEvidenceDigest {
				return sessionlessharness.FailureCapabilityMismatch
			}
			if candidate.InputDataClass != reference.InputDataClass || candidate.EffectivePolicyDigest != reference.EffectivePolicyDigest {
				return sessionlessharness.FailureEffectivePolicyMismatch
			}
			if candidate.ExecutionPlacementDigest != reference.ExecutionPlacementDigest {
				return sessionlessharness.FailurePlacementMismatch
			}
			if fixture.EvidenceBundle != nil {
				policy := fixture.EvidenceBundle.Policy
				allowed := false
				for _, class := range policy.AllowedDataClasses {
					allowed = allowed || class == candidate.InputDataClass
				}
				if (policy.Verdict != domain.ProviderPolicyGoV1 && policy.Verdict != domain.ProviderPolicyConditionalV1) || !allowed {
					return sessionlessharness.FailureEffectivePolicyMismatch
				}
			}
			candidateDigest, candidateErr := candidate.Digest()
			referenceDigest, referenceErr := reference.Digest()
			if candidateErr != nil || referenceErr != nil || candidateDigest != referenceDigest {
				return sessionlessharness.FailureHarnessBackendMismatch
			}
			return ""
		},
	}
	return registration, driver, nil
}

func (driver *FixtureDriver) Preflight(_ context.Context, identity ports.ExecutionIdentity) error {
	driver.recorder.record(OperationPreflightV1)
	if err := identity.Validate(); err != nil {
		return err
	}
	candidate, err := identity.HarnessBinding.Digest()
	if err != nil {
		return err
	}
	reference, err := driver.binding.Digest()
	if err != nil || candidate != reference {
		return &domain.ClassifiedError{Kind: domain.ErrorTerminal, Code: string(sessionlessharness.FailureHarnessBackendMismatch), Operation: "provider_conformance.preflight"}
	}
	return nil
}

func (driver *FixtureDriver) Execute(_ context.Context, request ports.ExecutionRequest, _ ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	if err := request.Validate(); err != nil {
		return ports.ExecutionResult{}, err
	}
	driver.recorder.record(OperationExecuteV1)
	input, output := uint64(0), uint64(0)
	evidence := domain.ProviderExecutionEvidenceV1{
		AcceptanceClass: domain.ProviderAcceptanceAcceptedV1,
		FinishClass:     domain.ProviderFinishCompletedV1,
		RouteState:      domain.ProviderEvidenceSupportedV1,
		InputTokens:     &input,
		OutputTokens:    &output,
		UsageProvenance: domain.ProviderUsageHarnessMeasuredV1,
		PolicyVerdict:   domain.ProviderPolicyGoV1,
	}
	if driver.bundle == nil {
		evidence.ActualModelVendorID = "sessionless"
		evidence.ActualModelID = request.HarnessBinding.ModelID
		evidence.TransportKind = domain.ProviderTransportLocalCLIV1
		evidence.TransportProvider = "sessionless"
		evidence.UpstreamProviderID = "local"
		evidence.EndpointID = "deterministic-fixture"
	} else {
		evidence.PolicyVerdict = driver.bundle.Policy.Verdict
		matched := false
		for _, route := range driver.bundle.Route.Routes {
			if route.BackendKind == request.HarnessBinding.Backend.BackendKind && route.ModelID == request.HarnessBinding.ModelID {
				evidence.ActualModelVendorID = route.ModelVendorID
				evidence.ActualModelID = route.ModelID
				evidence.TransportKind = route.TransportKind
				evidence.TransportProvider = route.TransportProvider
				evidence.UpstreamProviderID = route.UpstreamProviderID
				evidence.EndpointID = route.EndpointID
				matched = true
				break
			}
		}
		if !matched {
			return ports.ExecutionResult{}, &domain.ClassifiedError{Kind: domain.ErrorTerminal, Code: string(sessionlessharness.FailureProviderRouteMismatch), Operation: "provider_conformance.execute"}
		}
	}
	sealed, err := evidence.SealForBinding(request.HarnessBinding)
	if err != nil {
		return ports.ExecutionResult{}, err
	}
	return ports.ExecutionResult{Summary: "provider conformance fixture completed", ProviderEvidence: &sealed}, driver.executeErr
}

func (driver *FixtureDriver) Cancel(context.Context, ports.ExecutionIdentity) error {
	driver.recorder.record(OperationCancelV1)
	return nil
}

func (*FixtureDriver) BackendProtocolState() BackendProtocolStateV1 {
	return BackendProtocolSkippedV1
}

var _ ports.HarnessDriver = (*FixtureDriver)(nil)
