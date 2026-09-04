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
	if err := registry.Preflight(ctx, ports.ExecutionIdentity{TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, RunID: request.RunID, AttemptID: request.AttemptID, ExecutionPlacement: request.ExecutionPlacement, HarnessBinding: request.HarnessBinding.Clone()}); err != nil {
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
	case FailureHarnessBindingInvalid, FailureHarnessBackendUnsupported, FailureHarnessBackendMismatch,
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

func NewDeterministicFixtureBindingV1(
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	runID domain.RunID,
	attemptID domain.AttemptID,
	subscriptionConnectionID domain.SubscriptionConnectionID,
	placement domain.ExecutionPlacementV1,
	at time.Time,
) (domain.HarnessBindingV1, error) {
	return NewDeterministicFixtureBinderV1().BindHarness(context.Background(), ports.HarnessBindingRequest{
		TenantID: tenantID, OwnerUserID: ownerUserID, RunID: runID, AttemptID: attemptID,
		SubscriptionConnectionID: subscriptionConnectionID, ExecutionPlacement: placement, At: at,
	})
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

func (binder *DeterministicFixtureBinder) BindHarness(_ context.Context, request ports.HarnessBindingRequest) (domain.HarnessBindingV1, error) {
	if binder == nil {
		return domain.HarnessBindingV1{}, errors.New("deterministic harness binder must not be nil")
	}
	for _, validate := range []func() error{request.TenantID.Validate, request.OwnerUserID.Validate, request.RunID.Validate, request.AttemptID.Validate, request.SubscriptionConnectionID.Validate, request.ExecutionPlacement.Validate} {
		if err := validate(); err != nil {
			return domain.HarnessBindingV1{}, err
		}
	}
	if request.At.IsZero() {
		return domain.HarnessBindingV1{}, domain.ValidationError{Field: "harness_binding.at", Reason: "must not be zero"}
	}
	placementDigest, err := domain.ExecutionPlacementDigestV1(request.ExecutionPlacement)
	if err != nil {
		return domain.HarnessBindingV1{}, err
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
	if err := binding.ValidateForScope(request.TenantID, request.OwnerUserID, request.RunID, request.AttemptID, request.ExecutionPlacement); err != nil {
		return domain.HarnessBindingV1{}, err
	}
	return binding, nil
}

func stableDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

var _ ports.HarnessBinder = (*DeterministicFixtureBinder)(nil)
