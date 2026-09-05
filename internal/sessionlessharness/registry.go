// Package sessionlessharness owns the closed, exact-match registry for the
// Sessionless outer harness. Provider backends are adapters below this layer;
// none may discover or invoke another adapter.
package sessionlessharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	DeterministicHarnessVersionV1    = "1"
	DeterministicNativeProtocolV1    = "sessionless.deterministic.v1"
	DeterministicModelIDV1           = "deterministic-fixture-v1"
	DeterministicFixtureResourceIDV1 = "deterministic-fixture-v1"
)

type FailureCode string

const (
	FailureHarnessBindingInvalid     FailureCode = "harness_binding_invalid"
	FailureHarnessBackendUnsupported FailureCode = "harness_backend_unsupported"
	FailureHarnessBackendDisabled    FailureCode = "harness_backend_disabled"
	FailureHarnessBackendMismatch    FailureCode = "harness_backend_mismatch"
	FailureProviderResourceMismatch  FailureCode = "provider_resource_mismatch"
	FailureProviderRevisionMismatch  FailureCode = "provider_revision_mismatch"
	FailureCredentialGeneration      FailureCode = "credential_generation_mismatch"
	FailureProviderCatalogExpired    FailureCode = "provider_catalog_expired"
	FailureProviderEvidenceExpired   FailureCode = "provider_evidence_expired"
	FailureProviderRouteMismatch     FailureCode = "provider_route_mismatch"
	FailurePrivacyPolicyMismatch     FailureCode = "privacy_policy_mismatch"
	FailureCapabilityMismatch        FailureCode = "capability_mismatch"
	FailureEffectivePolicyMismatch   FailureCode = "effective_policy_mismatch"
	FailurePlacementMismatch         FailureCode = "placement_mismatch"
	FailureHarnessBackendFailed      FailureCode = "harness_backend_failed"
)

type Registration struct {
	Descriptor      domain.HarnessBackendDescriptorV1
	Enabled         bool
	ValidateBinding func(domain.HarnessBindingV1) FailureCode
	Driver          ports.HarnessDriver
}

type Registry struct {
	backends map[string]Registration
	now      func() time.Time
	mu       sync.Mutex
	active   map[string]string
}

func NewRegistry(now func() time.Time, registrations ...Registration) (*Registry, error) {
	if now == nil {
		return nil, errors.New("harness registry clock must not be nil")
	}
	if len(registrations) == 0 {
		return nil, errors.New("at least one explicit harness backend registration is required")
	}
	registry := &Registry{backends: make(map[string]Registration, len(registrations)), active: make(map[string]string), now: now}
	for _, registration := range registrations {
		if err := registration.Descriptor.Validate(); err != nil {
			return nil, err
		}
		if registration.Driver == nil {
			return nil, errors.New("harness backend driver must not be nil")
		}
		if registration.ValidateBinding == nil {
			return nil, errors.New("harness backend binding validator must not be nil")
		}
		key := descriptorKey(registration.Descriptor)
		if _, exists := registry.backends[key]; exists {
			return nil, errors.New("duplicate harness backend registration")
		}
		registry.backends[key] = registration
	}
	return registry, nil
}

func (registry *Registry) Execute(ctx context.Context, request ports.ExecutionRequest, sink ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	if err := request.Validate(); err != nil {
		return ports.ExecutionResult{}, harnessError(FailureHarnessBindingInvalid)
	}
	if err := registry.Preflight(ctx, ports.ExecutionIdentity{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, RunID: request.RunID, AttemptID: request.AttemptID,
		ExecutionPlacementV2: request.ExecutionPlacementV2, HarnessBinding: request.HarnessBinding.Clone(),
		SubstrateBinding: cloneSubstrateBinding(request.SubstrateBinding), AdmissionCostCeiling: cloneAdmissionCostCeiling(request.AdmissionCostCeiling),
	}); err != nil {
		return ports.ExecutionResult{}, err
	}
	registration, key, err := registry.resolve(request.HarnessBinding, true)
	if err != nil {
		return ports.ExecutionResult{}, err
	}
	identityKey := executionKey(request.TenantID, request.RunID, request.AttemptID)
	registry.mu.Lock()
	if _, exists := registry.active[identityKey]; exists {
		registry.mu.Unlock()
		return ports.ExecutionResult{}, harnessError(FailureHarnessBackendMismatch)
	}
	registry.active[identityKey] = key
	registry.mu.Unlock()
	defer func() {
		registry.mu.Lock()
		delete(registry.active, identityKey)
		registry.mu.Unlock()
	}()
	result, executeErr := registration.Driver.Execute(ctx, request, sink)
	if result.ProviderEvidence != nil {
		if result.ProviderEvidence.ValidateForBinding(request.HarnessBinding) != nil {
			return ports.ExecutionResult{}, harnessError(FailureHarnessBackendFailed)
		}
		evidence := result.ProviderEvidence.Clone()
		result.ProviderEvidence = &evidence
	}
	if executeErr != nil {
		bounded := ports.ExecutionResult{}
		if result.ProviderEvidence != nil {
			evidence := result.ProviderEvidence.Clone()
			bounded.ProviderEvidence = &evidence
		}
		return bounded, sanitizeBackendError(executeErr)
	}
	if result.ProviderEvidence == nil {
		return ports.ExecutionResult{}, harnessError(FailureHarnessBackendFailed)
	}
	return result, nil
}

func (registry *Registry) Preflight(ctx context.Context, identity ports.ExecutionIdentity) error {
	if err := identity.Validate(); err != nil {
		return harnessError(FailureHarnessBindingInvalid)
	}
	registration, _, err := registry.resolve(identity.HarnessBinding, true)
	if err != nil {
		return err
	}
	if err := registration.Driver.Preflight(ctx, identity); err != nil {
		return sanitizeBackendError(err)
	}
	return nil
}

func (registry *Registry) Cancel(ctx context.Context, identity ports.ExecutionIdentity) error {
	if err := identity.Validate(); err != nil {
		return harnessError(FailureHarnessBindingInvalid)
	}
	registration, key, err := registry.resolve(identity.HarnessBinding, false)
	if err != nil {
		return err
	}
	identityKey := executionKey(identity.TenantID, identity.RunID, identity.AttemptID)
	registry.mu.Lock()
	activeKey, active := registry.active[identityKey]
	registry.mu.Unlock()
	if active && activeKey != key {
		return harnessError(FailureHarnessBackendMismatch)
	}
	if err := registration.Driver.Cancel(ctx, identity); err != nil {
		return sanitizeBackendError(err)
	}
	return nil
}

func (registry *Registry) resolve(binding domain.HarnessBindingV1, requireFresh bool) (Registration, string, error) {
	if registry == nil {
		return Registration{}, "", harnessError(FailureHarnessBackendUnsupported)
	}
	if err := binding.Validate(); err != nil {
		return Registration{}, "", harnessError(FailureHarnessBindingInvalid)
	}
	if requireFresh {
		if err := binding.ValidateAt(registry.now().UTC()); err != nil {
			return Registration{}, "", harnessError(FailureProviderEvidenceExpired)
		}
	}
	key := descriptorKey(binding.Backend)
	registration, found := registry.backends[key]
	if !found {
		return Registration{}, "", harnessError(FailureHarnessBackendUnsupported)
	}
	if registration.Descriptor != binding.Backend {
		return Registration{}, "", harnessError(FailureHarnessBackendMismatch)
	}
	if requireFresh && !registration.Enabled {
		return Registration{}, "", harnessError(FailureHarnessBackendDisabled)
	}
	if code := registration.ValidateBinding(binding.Clone()); code != "" {
		if !code.validRegistrationResult() {
			code = FailureHarnessBackendMismatch
		}
		return Registration{}, "", harnessError(code)
	}
	return registration, key, nil
}

func (code FailureCode) validRegistrationResult() bool {
	switch code {
	case FailureHarnessBindingInvalid, FailureHarnessBackendUnsupported, FailureHarnessBackendDisabled, FailureHarnessBackendMismatch,
		FailureProviderResourceMismatch, FailureProviderRevisionMismatch, FailureCredentialGeneration,
		FailureProviderCatalogExpired, FailureProviderEvidenceExpired, FailureProviderRouteMismatch, FailurePrivacyPolicyMismatch,
		FailureCapabilityMismatch, FailureEffectivePolicyMismatch, FailurePlacementMismatch:
		return true
	default:
		return false
	}
}

// Valid reports whether code belongs to the closed public-safe harness
// failure taxonomy. Backend-private error text never crosses this boundary.
func (code FailureCode) Valid() bool {
	return code == FailureHarnessBackendFailed || code.validRegistrationResult()
}

func descriptorKey(descriptor domain.HarnessBackendDescriptorV1) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s", descriptor.HarnessKind, descriptor.HarnessVersion,
		descriptor.BackendKind, descriptor.ArtifactKind, descriptor.ArtifactDigest, descriptor.NativeProtocolVersion,
		descriptor.BackendProfileDigest, descriptor.ProviderContractKind, descriptor.CredentialDeliveryKind)
}

func executionKey(tenantID domain.TenantID, runID domain.RunID, attemptID domain.AttemptID) string {
	return string(tenantID) + "\x00" + string(runID) + "\x00" + string(attemptID)
}

func harnessError(code FailureCode) error {
	return harnessErrorWithKind(code, domain.ErrorTerminal)
}

func harnessErrorWithKind(code FailureCode, kind domain.ErrorKind) error {
	if !kind.Valid() {
		kind = domain.ErrorTerminal
	}
	return &domain.ClassifiedError{Kind: kind, Code: string(code), Operation: "sessionless_harness.resolve"}
}

func sanitizeBackendError(err error) error {
	var classified *domain.ClassifiedError
	if errors.As(err, &classified) && classified != nil {
		code := FailureCode(classified.Code)
		if code.Valid() {
			return harnessErrorWithKind(code, classified.Kind)
		}
		// Preserve only the closed domain failure class. Backend-private codes,
		// operations and causes never cross the registry boundary.
		return harnessErrorWithKind(FailureHarnessBackendFailed, classified.Kind)
	}
	return harnessError(FailureHarnessBackendFailed)
}

var _ ports.HarnessDriver = (*Registry)(nil)

type DeterministicFixtureBinder struct {
	descriptor domain.HarnessBackendDescriptorV1
}

func NewDeterministicFixtureBinderV1() *DeterministicFixtureBinder {
	return &DeterministicFixtureBinder{descriptor: DeterministicFixtureDescriptorV1()}
}

func NewDeterministicFixtureManagedAuthorityV2(
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	runID domain.RunID,
	attemptID domain.AttemptID,
	subscriptionConnectionID domain.SubscriptionConnectionID,
	at time.Time,
) (ports.ManagedExecutionAuthorityV2, error) {
	return NewDeterministicFixtureBinderV1().BindHarness(context.Background(), ports.HarnessBindingRequest{
		TenantID: tenantID, OwnerUserID: ownerUserID, RunID: runID, AttemptID: attemptID,
		SubscriptionConnectionID: subscriptionConnectionID, At: at,
	})
}

// ValidateDeterministicFixtureInvocationAuthorityV2 proves that all managed
// placement, harness, substrate and cost fields are exactly those emitted by
// the built-in deterministic binder for the authenticated scope and pinned
// price observation. It does not inspect queue input or discover a backend.
func ValidateDeterministicFixtureInvocationAuthorityV2(authority domain.ServerlessInvocationAuthorityV1) error {
	if err := authority.Validate(); err != nil {
		return err
	}
	expected, err := NewDeterministicFixtureManagedAuthorityV2(
		authority.HarnessBinding.TenantID,
		authority.HarnessBinding.OwnerUserID,
		authority.HarnessBinding.RunID,
		authority.HarnessBinding.AttemptID,
		"deterministic-validation",
		authority.AdmissionCostCeiling.PriceObservedAt,
	)
	if err != nil {
		return err
	}
	expectedHarness, _ := expected.HarnessBinding.Digest()
	actualHarness, _ := authority.HarnessBinding.Digest()
	expectedPlacement, _ := domain.ExecutionPlacementDigest(expected.ExecutionPlacementV2)
	actualPlacement, _ := domain.ExecutionPlacementDigest(authority.ExecutionPlacementV2)
	expectedSubstrate, _ := expected.SubstrateBinding.Digest()
	actualSubstrate, _ := authority.SubstrateBinding.Digest()
	expectedCost, _ := expected.AdmissionCostCeiling.Digest()
	actualCost, _ := authority.AdmissionCostCeiling.Digest()
	if expectedHarness != actualHarness || expectedPlacement != actualPlacement ||
		expectedSubstrate != actualSubstrate || expectedCost != actualCost {
		return errors.New("deterministic fixture invocation authority does not exact-match the built-in profile")
	}
	return nil
}

func DeterministicFixtureDescriptorV1() domain.HarnessBackendDescriptorV1 {
	artifact := sha256.Sum256([]byte(deterministicFixtureProfileArtifactV1))
	return domain.HarnessBackendDescriptorV1{
		HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: DeterministicHarnessVersionV1,
		BackendKind:            domain.HarnessBackendDeterministicFixtureV1,
		ArtifactKind:           domain.HarnessArtifactEmbeddedProfileV1,
		ArtifactDigest:         hex.EncodeToString(artifact[:]),
		NativeProtocolVersion:  DeterministicNativeProtocolV1,
		BackendProfileDigest:   stableDigest("sessionless.deterministic-harness.profile.v1"),
		ProviderContractKind:   domain.ProviderContractCredentiallessFixtureV1,
		CredentialDeliveryKind: domain.ProviderCredentialDeliveryNoneV1,
	}
}

// This exact committed byte string is the complete embedded deterministic
// backend profile artifact. Its digest is evidence of these bytes, not a hash
// of a descriptive label or an installed executable claim.
const deterministicFixtureProfileArtifactV1 = "sessionless.deterministic-fixture-profile.v1\nprotocol=sessionless.deterministic.v1\nmodel=deterministic-fixture-v1\ncredentials=none\nnetwork=denied\n"

func ValidateDeterministicFixtureBindingV1(binding domain.HarnessBindingV1) FailureCode {
	if err := binding.Validate(); err != nil {
		return FailureHarnessBindingInvalid
	}
	if binding.Backend != DeterministicFixtureDescriptorV1() {
		return FailureHarnessBackendMismatch
	}
	resource := binding.Resource
	if resource.Kind != domain.ProviderResourceCredentiallessFixtureV1 ||
		resource.ResourceID != DeterministicFixtureResourceIDV1 || resource.Revision != 1 ||
		resource.CredentialMode != domain.ProviderCredentialNoneV1 || resource.CredentialGeneration != 0 {
		return FailureProviderResourceMismatch
	}
	if binding.ModelID != DeterministicModelIDV1 ||
		binding.InputDataClass != domain.ProviderDataPrivateV1 ||
		binding.ProviderCatalogDigest != stableDigest("sessionless.deterministic-harness.catalog.v1") ||
		binding.ProviderRouteDigest != stableDigest("sessionless.deterministic-harness.route.v1") ||
		binding.PrivacyPolicyDigest != stableDigest("sessionless.deterministic-harness.privacy.v1") ||
		binding.CapabilityEvidenceDigest != stableDigest("sessionless.deterministic-harness.capability.v1") ||
		binding.EffectivePolicyDigest != stableDigest("sessionless.deterministic-harness.policy.v1") ||
		binding.EvidenceExpiresAt != nil {
		return FailureEffectivePolicyMismatch
	}
	return ""
}

func (binder *DeterministicFixtureBinder) BindHarness(_ context.Context, request ports.HarnessBindingRequest) (ports.ManagedExecutionAuthorityV2, error) {
	if binder == nil {
		return ports.ManagedExecutionAuthorityV2{}, errors.New("deterministic harness binder must not be nil")
	}
	for _, validate := range []func() error{request.TenantID.Validate, request.OwnerUserID.Validate, request.RunID.Validate, request.AttemptID.Validate, request.SubscriptionConnectionID.Validate} {
		if err := validate(); err != nil {
			return ports.ManagedExecutionAuthorityV2{}, err
		}
	}
	if request.At.IsZero() {
		return ports.ManagedExecutionAuthorityV2{}, domain.ValidationError{Field: "harness_binding.at", Reason: "must not be zero"}
	}
	zero := uint64(0)
	cost := domain.AdmissionCostCeilingV1{
		Version: domain.AdmissionCostCeilingVersionV1, Currency: "USD", PriceRevision: "deterministic-fixture-v1",
		PriceObservedAt: request.At.UTC(), PriceExpiresAt: request.At.UTC().Add(30 * 24 * time.Hour), MaxDeliveries: 5,
		MaxPreEffectDurationPerDelivery: time.Minute, MaxActiveDuration: 40 * time.Minute, MaxCleanupAndReconcileDuration: 5 * time.Minute,
		ConfiguredMemoryBytes: 256 << 20, ConfiguredVCPUMillis: 1000, MaxIngressBytes: 1 << 20, MaxEgressBytes: 1 << 20,
		MaxLogBytes: 2 << 20, MaxEvidenceBytes: 1 << 20, SubstratePriceState: domain.CostEvidenceKnownV1,
		ProviderPriceState: domain.ProviderPriceKnownFreeV1, MaxSubstrateAmountMicrounits: &zero,
		MaxProviderAmountMicrounits: &zero, MaxTotalAmountMicrounits: &zero,
	}
	costDigest, err := cost.Digest()
	if err != nil {
		return ports.ManagedExecutionAuthorityV2{}, err
	}
	substrate := domain.SubstrateBindingV1{
		Version: domain.SubstrateBindingVersionV1, Kind: domain.SubstrateDeterministicFixtureV1,
		ProfileID: "deterministic-fixture-v1", ProfileRevision: 1,
		ProfileDigest: stableDigest("sessionless.deterministic-substrate.profile.v1"), ProfileEvidenceExpiresAt: request.At.UTC().Add(30 * 24 * time.Hour),
		Region: "local-fixture", ImageDigest: stableDigest("sessionless.deterministic-substrate.image.v1"),
		OuterHarnessArtifactDigest: stableDigest("sessionless.deterministic-substrate.outer-harness.v1"), WorkloadMode: domain.SubstrateWorkloadInProcessDirectV1,
		IsolationProfileDigest: stableDigest("sessionless.deterministic-substrate.isolation.v1"), EgressPolicyDigest: stableDigest("sessionless.deterministic-substrate.egress-denied.v1"),
		CleanupPolicyDigest: stableDigest("sessionless.deterministic-substrate.cleanup.v1"), EgressProxyArtifactDigest: stableDigest("sessionless.deterministic-substrate.no-network-proxy.v1"),
		EgressProxyIdentityDigest: stableDigest("sessionless.deterministic-substrate.no-network-identity.v1"), AdmissionCostCeilingDigest: costDigest,
		Limits: domain.SubstrateLimitsV1{InvocationTimeout: time.Hour, ExecutionTimeout: 40 * time.Minute, CleanupTimeout: 5 * time.Minute,
			CPUMillis: 1000, MemoryBytes: 256 << 20, ScratchBytes: 256 << 20, StdoutBytes: 1 << 20, StderrBytes: 1 << 20, NativeEventCount: 1024, ArtifactBytes: 1 << 20},
	}
	substrateDigest, err := substrate.Digest()
	if err != nil {
		return ports.ManagedExecutionAuthorityV2{}, err
	}
	placement, err := domain.ManagedExecutionPlacementV2(string(substrateDigest))
	if err != nil {
		return ports.ManagedExecutionAuthorityV2{}, err
	}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		return ports.ManagedExecutionAuthorityV2{}, err
	}
	binding := domain.HarnessBindingV1{
		Version:  domain.HarnessBindingVersionV1,
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, RunID: request.RunID, AttemptID: request.AttemptID,
		Backend: binder.descriptor,
		Resource: domain.ProviderResourceBindingV1{
			Kind: domain.ProviderResourceCredentiallessFixtureV1, ResourceID: DeterministicFixtureResourceIDV1,
			OwnerUserID: request.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialNoneV1,
		},
		ModelVendorID:            "sessionless",
		ModelID:                  DeterministicModelIDV1,
		InputDataClass:           domain.ProviderDataPrivateV1,
		ProviderCatalogDigest:    stableDigest("sessionless.deterministic-harness.catalog.v1"),
		ProviderRouteDigest:      stableDigest("sessionless.deterministic-harness.route.v1"),
		PrivacyPolicyDigest:      stableDigest("sessionless.deterministic-harness.privacy.v1"),
		CapabilityEvidenceDigest: stableDigest("sessionless.deterministic-harness.capability.v1"),
		EffectivePolicyDigest:    stableDigest("sessionless.deterministic-harness.policy.v1"),
		ExecutionPlacementDigest: string(placementDigest),
	}
	result := ports.ManagedExecutionAuthorityV2{ExecutionPlacementV2: placement, HarnessBinding: binding, SubstrateBinding: substrate, AdmissionCostCeiling: cost}
	if err := result.ValidateForScope(request); err != nil {
		return ports.ManagedExecutionAuthorityV2{}, err
	}
	return result, nil
}

func stableDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func cloneSubstrateBinding(value *domain.SubstrateBindingV1) *domain.SubstrateBindingV1 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneAdmissionCostCeiling(value *domain.AdmissionCostCeilingV1) *domain.AdmissionCostCeilingV1 {
	if value == nil {
		return nil
	}
	clone := value.Clone()
	return &clone
}

var _ ports.HarnessBinder = (*DeterministicFixtureBinder)(nil)
