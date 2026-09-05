package serverlessharness

import (
	"context"
	"errors"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type SubstrateFailureCodeV1 string

const (
	SubstrateFailureAuthorityInvalidV1 SubstrateFailureCodeV1 = "substrate_authority_invalid"
	SubstrateFailureUnsupportedV1      SubstrateFailureCodeV1 = "substrate_unsupported"
	SubstrateFailureBindingMismatchV1  SubstrateFailureCodeV1 = "substrate_binding_mismatch"
	SubstrateFailureDisabledV1         SubstrateFailureCodeV1 = "substrate_profile_disabled"
	SubstrateFailureExpiredV1          SubstrateFailureCodeV1 = "substrate_profile_expired"
	SubstrateFailurePreflightV1        SubstrateFailureCodeV1 = "substrate_preflight_failed"
	SubstrateFailureExecuteV1          SubstrateFailureCodeV1 = "substrate_execute_failed"
	SubstrateFailureCancelV1           SubstrateFailureCodeV1 = "substrate_cancel_failed"
	SubstrateFailureReconcileV1        SubstrateFailureCodeV1 = "substrate_reconcile_failed"
)

func (code SubstrateFailureCodeV1) Valid() bool {
	switch code {
	case SubstrateFailureAuthorityInvalidV1, SubstrateFailureUnsupportedV1, SubstrateFailureBindingMismatchV1,
		SubstrateFailureDisabledV1, SubstrateFailureExpiredV1, SubstrateFailurePreflightV1,
		SubstrateFailureExecuteV1, SubstrateFailureCancelV1, SubstrateFailureReconcileV1:
		return true
	default:
		return false
	}
}

type substrateErrorV1 struct{ code SubstrateFailureCodeV1 }

func (err substrateErrorV1) Error() string                { return string(err.code) }
func (err substrateErrorV1) Code() SubstrateFailureCodeV1 { return err.code }

type SubstrateOperationStateV1 string

const (
	SubstrateOperationNotFoundV1     SubstrateOperationStateV1 = "not_found"
	SubstrateOperationObservedV1     SubstrateOperationStateV1 = "observed"
	SubstrateOperationAcknowledgedV1 SubstrateOperationStateV1 = "acknowledged"
	SubstrateOperationUnknownV1      SubstrateOperationStateV1 = "unknown"
)

type SubstrateOperationObservationV1 struct {
	State                SubstrateOperationStateV1
	InvocationAuthority  domain.ServerlessInvocationAuthorityDigestV1
	SubstrateBinding     domain.SubstrateBindingDigestV1
	PhysicalInvocationID string
	ObservedAt           time.Time
}

func (value SubstrateOperationObservationV1) ValidateForAuthority(authority domain.ServerlessInvocationAuthorityV1) error {
	if value.State != SubstrateOperationNotFoundV1 && value.State != SubstrateOperationObservedV1 &&
		value.State != SubstrateOperationAcknowledgedV1 && value.State != SubstrateOperationUnknownV1 {
		return domain.ValidationError{Field: "substrate_operation.state", Reason: "is unsupported"}
	}
	authorityDigest, err := authority.Digest()
	if err != nil {
		return err
	}
	substrateDigest, _ := authority.SubstrateBinding.Digest()
	if value.InvocationAuthority != authorityDigest || value.SubstrateBinding != substrateDigest {
		return domain.ValidationError{Field: "substrate_operation.authority", Reason: "must exact-match the invocation authority"}
	}
	if value.PhysicalInvocationID != "" {
		if err := domain.ValidateOpaqueID("substrate_operation.physical_invocation_id", value.PhysicalInvocationID); err != nil {
			return err
		}
	}
	if value.ObservedAt.IsZero() {
		return domain.ValidationError{Field: "substrate_operation.observed_at", Reason: "must not be zero"}
	}
	return nil
}

// ExecutionSubstrateV1 is deliberately not a remote-shell interface. The
// implementation receives only canonical authority or an issuer-authenticated
// PreparedInvocation and cannot select another backend/provider.
type ExecutionSubstrateV1 interface {
	Preflight(context.Context, domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV1, error)
	Execute(context.Context, PreparedInvocation, ports.ExecutionRequest, ports.ExecutionEventSink, ports.HarnessDriver) (ports.ExecutionResult, domain.SubstrateExecutionEvidenceV1, error)
	Cancel(context.Context, domain.ServerlessInvocationAuthorityV1) (SubstrateOperationObservationV1, error)
	Reconcile(context.Context, domain.ServerlessInvocationAuthorityV1) (SubstrateOperationObservationV1, error)
}

type SubstrateRegistrationV1 struct {
	Binding domain.SubstrateBindingV1
	Enabled bool
	Driver  ExecutionSubstrateV1
}

// SubstrateRegistryV1 is an immutable exact-match registry. Disabled and
// expired registrations remain as tombstones for Cancel/Reconcile but can
// never Preflight or Execute new work.
type SubstrateRegistryV1 struct {
	now           func() time.Time
	issuer        *CapabilityIssuer
	registrations map[domain.SubstrateBindingDigestV1]SubstrateRegistrationV1
	mu            sync.Mutex
	active        map[domain.ServerlessInvocationAuthorityDigestV1]domain.SubstrateBindingDigestV1
}

func NewSubstrateRegistryV1(now func() time.Time, issuer *CapabilityIssuer, registrations ...SubstrateRegistrationV1) (*SubstrateRegistryV1, error) {
	if now == nil || issuer == nil || len(registrations) == 0 {
		return nil, errors.New("substrate registry requires a clock, capability issuer and registrations")
	}
	registry := &SubstrateRegistryV1{
		now: now, issuer: issuer,
		registrations: make(map[domain.SubstrateBindingDigestV1]SubstrateRegistrationV1, len(registrations)),
		active:        make(map[domain.ServerlessInvocationAuthorityDigestV1]domain.SubstrateBindingDigestV1),
	}
	for _, registration := range registrations {
		if registration.Driver == nil {
			return nil, errors.New("substrate registration driver must not be nil")
		}
		digest, err := registration.Binding.Digest()
		if err != nil {
			return nil, err
		}
		if _, exists := registry.registrations[digest]; exists {
			return nil, errors.New("duplicate substrate registration")
		}
		registry.registrations[digest] = registration
	}
	return registry, nil
}

// Prepare authenticates one durable effect owner, resolves and preflights its
// exact sealed substrate registration, and issues the non-durable capability
// that downstream process or egress code must consume at its effect boundary.
func (registry *SubstrateRegistryV1) Prepare(
	ctx context.Context,
	result ports.ReserveAttemptEffectResultV1,
) (PreparedInvocation, error) {
	if registry == nil || registry.issuer == nil || ctx == nil || ctx.Err() != nil || result.Validate() != nil {
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	if result.Status == ports.AttemptEffectReconcileOnlyV1 || result.Grant == nil {
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureReconcileV1}
	}
	grant := result.Grant.Clone()
	if registry.issuer.VerifyGrant(grant) != nil {
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	allocation, err := registry.Preflight(ctx, grant.Authority)
	if err != nil {
		return PreparedInvocation{}, err
	}
	prepared, err := registry.issuer.Issue(grant, allocation)
	if err != nil {
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	return prepared, nil
}

func (registry *SubstrateRegistryV1) Preflight(ctx context.Context, authority domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV1, error) {
	registration, _, err := registry.resolve(authority, true)
	if err != nil {
		return domain.PreparedAllocationV1{}, err
	}
	allocation, err := registration.Driver.Preflight(ctx, authority.Clone())
	if err != nil {
		return domain.PreparedAllocationV1{}, substrateErrorV1{code: SubstrateFailurePreflightV1}
	}
	if err := allocation.ValidateForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend); err != nil {
		return domain.PreparedAllocationV1{}, substrateErrorV1{code: SubstrateFailureBindingMismatchV1}
	}
	return allocation.Clone(), nil
}

func (registry *SubstrateRegistryV1) Execute(
	ctx context.Context,
	prepared PreparedInvocation,
	request ports.ExecutionRequest,
	sink ports.ExecutionEventSink,
	harness ports.HarnessDriver,
) (ports.ExecutionResult, domain.SubstrateExecutionEvidenceV1, error) {
	if harness == nil || sink == nil || registry == nil || registry.issuer == nil ||
		registry.issuer.Validate(prepared) != nil || validatePreparedExecutionRequest(prepared, request) != nil {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	authority := prepared.Authority()
	registration, bindingDigest, err := registry.resolve(authority, true)
	if err != nil {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, err
	}
	authorityDigest, _ := authority.Digest()
	registry.mu.Lock()
	if activeBinding, exists := registry.active[authorityDigest]; exists && activeBinding != bindingDigest {
		registry.mu.Unlock()
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, substrateErrorV1{code: SubstrateFailureBindingMismatchV1}
	}
	registry.active[authorityDigest] = bindingDigest
	registry.mu.Unlock()
	defer func() {
		registry.mu.Lock()
		delete(registry.active, authorityDigest)
		registry.mu.Unlock()
	}()
	result, evidence, executeErr := registration.Driver.Execute(ctx, prepared, request, sink, harness)
	if evidence.ValidateForAuthority(authority, prepared.Reservation(), prepared.Allocation(), prepared.Digest()) != nil {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, substrateErrorV1{code: SubstrateFailureExecuteV1}
	}
	if result.ProviderEvidence == nil || evidence.ProviderEvidence == nil ||
		result.ProviderEvidence.EvidenceDigest != evidence.ProviderEvidence.EvidenceDigest {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, substrateErrorV1{code: SubstrateFailureExecuteV1}
	}
	if executeErr != nil {
		return result, evidence.Clone(), substrateErrorV1{code: SubstrateFailureExecuteV1}
	}
	return result, evidence.Clone(), nil
}

func validatePreparedExecutionRequest(prepared PreparedInvocation, request ports.ExecutionRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	authority := prepared.Authority()
	if request.TenantID != authority.HarnessBinding.TenantID || request.OwnerUserID != authority.HarnessBinding.OwnerUserID ||
		request.RunID != authority.HarnessBinding.RunID || request.AttemptID != authority.HarnessBinding.AttemptID {
		return errors.New("prepared execution request scope mismatch")
	}
	harnessDigest, err := request.HarnessBinding.Digest()
	if err != nil {
		return err
	}
	authorityHarnessDigest, _ := authority.HarnessBinding.Digest()
	placementDigest, err := domain.ExecutionPlacementDigest(request.ExecutionPlacementV2)
	if err != nil {
		return err
	}
	authorityPlacementDigest, _ := domain.ExecutionPlacementDigest(authority.ExecutionPlacementV2)
	if request.SubstrateBinding == nil || request.AdmissionCostCeiling == nil {
		return errors.New("prepared execution request authority is incomplete")
	}
	substrateDigest, err := request.SubstrateBinding.Digest()
	if err != nil {
		return err
	}
	authoritySubstrateDigest, _ := authority.SubstrateBinding.Digest()
	costDigest, err := request.AdmissionCostCeiling.Digest()
	if err != nil {
		return err
	}
	authorityCostDigest, _ := authority.AdmissionCostCeiling.Digest()
	if harnessDigest != authorityHarnessDigest || placementDigest != authorityPlacementDigest ||
		substrateDigest != authoritySubstrateDigest || costDigest != authorityCostDigest {
		return errors.New("prepared execution request binding mismatch")
	}
	return nil
}

func (registry *SubstrateRegistryV1) Cancel(ctx context.Context, authority domain.ServerlessInvocationAuthorityV1) (SubstrateOperationObservationV1, error) {
	return registry.observe(ctx, authority, true)
}

func (registry *SubstrateRegistryV1) Reconcile(ctx context.Context, authority domain.ServerlessInvocationAuthorityV1) (SubstrateOperationObservationV1, error) {
	return registry.observe(ctx, authority, false)
}

func (registry *SubstrateRegistryV1) observe(ctx context.Context, authority domain.ServerlessInvocationAuthorityV1, cancel bool) (SubstrateOperationObservationV1, error) {
	registration, _, err := registry.resolve(authority, false)
	if err != nil {
		return SubstrateOperationObservationV1{}, err
	}
	var observation SubstrateOperationObservationV1
	if cancel {
		observation, err = registration.Driver.Cancel(ctx, authority.Clone())
	} else {
		observation, err = registration.Driver.Reconcile(ctx, authority.Clone())
	}
	if err != nil {
		code := SubstrateFailureReconcileV1
		if cancel {
			code = SubstrateFailureCancelV1
		}
		return SubstrateOperationObservationV1{}, substrateErrorV1{code: code}
	}
	if err := observation.ValidateForAuthority(authority); err != nil {
		return SubstrateOperationObservationV1{}, substrateErrorV1{code: SubstrateFailureBindingMismatchV1}
	}
	return observation, nil
}

func (registry *SubstrateRegistryV1) resolve(authority domain.ServerlessInvocationAuthorityV1, requireFresh bool) (SubstrateRegistrationV1, domain.SubstrateBindingDigestV1, error) {
	if registry == nil {
		return SubstrateRegistrationV1{}, "", substrateErrorV1{code: SubstrateFailureUnsupportedV1}
	}
	if err := authority.Validate(); err != nil {
		return SubstrateRegistrationV1{}, "", substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	digest, _ := authority.SubstrateBinding.Digest()
	registration, found := registry.registrations[digest]
	if !found {
		return SubstrateRegistrationV1{}, "", substrateErrorV1{code: SubstrateFailureUnsupportedV1}
	}
	if registration.Binding != authority.SubstrateBinding {
		return SubstrateRegistrationV1{}, "", substrateErrorV1{code: SubstrateFailureBindingMismatchV1}
	}
	if requireFresh {
		if !registration.Enabled {
			return SubstrateRegistrationV1{}, "", substrateErrorV1{code: SubstrateFailureDisabledV1}
		}
		if err := authority.ValidateAt(registry.now().UTC()); err != nil {
			return SubstrateRegistrationV1{}, "", substrateErrorV1{code: SubstrateFailureExpiredV1}
		}
	}
	return registration, digest, nil
}
