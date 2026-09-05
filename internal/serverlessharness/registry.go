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

// ExecutionSubstrateV1 is deliberately not a remote-shell interface. The
// implementation receives only canonical authority or an issuer-authenticated
// PreparedInvocation and cannot select another backend/provider.
type ExecutionSubstrateV1 interface {
	Preflight(context.Context, domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV1, error)
	Execute(context.Context, PreparedInvocation, ports.ExecutionRequest, ports.ExecutionEventSink, ports.HarnessDriver) (ports.ExecutionResult, domain.SubstrateExecutionEvidenceV1, error)
	Cancel(context.Context, domain.ServerlessInvocationAuthorityV1) (domain.SubstrateOperationObservationV1, error)
	Reconcile(context.Context, domain.ServerlessInvocationAuthorityV1) (domain.SubstrateOperationObservationV1, error)
}

// PreparedExecutionV1 is the only managed-work execution surface exposed to
// the worker after it owns a durable effect reservation. The concrete session
// keeps PreparedInvocation process-local and cannot be reconstructed from a
// queue delivery or caller-supplied fields.
type PreparedExecutionV1 interface {
	Execute(context.Context, ports.ExecutionRequest, ports.ExecutionEventSink, ports.HarnessDriver) (ports.ExecutionResult, domain.SubstrateExecutionEvidenceV1, error)
	Cancel(context.Context) (domain.SubstrateOperationObservationV1, error)
	Reconcile(context.Context) (domain.SubstrateOperationObservationV1, error)
}

// PreparedReconciliationV1 is a read-only operation session for a physical
// invocation reserved by another delivery. It intentionally exposes neither
// Execute nor Cancel.
type PreparedReconciliationV1 interface {
	Reconcile(context.Context) (domain.AttemptEffectReconciliationEvidenceV1, error)
}

// ExecutionPreparerV1 authenticates the durable effect owner and returns an
// opaque session already bound to its exact substrate and invocation.
type ExecutionPreparerV1 interface {
	PrepareExecution(context.Context, ports.ReserveAttemptEffectResultV1) (PreparedExecutionV1, error)
	PrepareReconciliation(context.Context, ports.ReserveAttemptEffectResultV1) (PreparedReconciliationV1, error)
}

// SubstrateRegistrationResolverV1 is trusted process composition. It must
// return the one reviewed registration that exact-matches authenticated
// authority; it is never selected from queue or request input.
type SubstrateRegistrationResolverV1 func(domain.ServerlessInvocationAuthorityV1) (SubstrateRegistrationV1, error)

type ExactExecutionPreparerV1 struct {
	now      func() time.Time
	issuer   *CapabilityIssuer
	resolver SubstrateRegistrationResolverV1
}

type preparedExecutionV1 struct {
	registry *SubstrateRegistryV1
	prepared PreparedInvocation
}

type preparedReconciliationV1 struct {
	registry    *SubstrateRegistryV1
	authority   domain.ServerlessInvocationAuthorityV1
	reservation domain.AttemptEffectReservationV1
}

func NewExactExecutionPreparerV1(
	now func() time.Time,
	issuer *CapabilityIssuer,
	resolver SubstrateRegistrationResolverV1,
) (*ExactExecutionPreparerV1, error) {
	if now == nil || issuer == nil || resolver == nil {
		return nil, errors.New("exact execution preparer requires a clock, capability issuer and registration resolver")
	}
	return &ExactExecutionPreparerV1{now: now, issuer: issuer, resolver: resolver}, nil
}

func (preparer *ExactExecutionPreparerV1) PrepareExecution(
	ctx context.Context,
	result ports.ReserveAttemptEffectResultV1,
) (PreparedExecutionV1, error) {
	if preparer == nil || preparer.issuer == nil || preparer.resolver == nil || ctx == nil || ctx.Err() != nil || result.Validate() != nil || result.Grant == nil || result.Status == ports.AttemptEffectReconcileOnlyV1 {
		return nil, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	grant := result.Grant.Clone()
	if preparer.issuer.VerifyGrant(grant) != nil {
		return nil, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	registration, err := preparer.resolver(grant.Authority.Clone())
	if err != nil || registration.Driver == nil {
		return nil, substrateErrorV1{code: SubstrateFailureUnsupportedV1}
	}
	if registration.Binding != grant.Authority.SubstrateBinding {
		return nil, substrateErrorV1{code: SubstrateFailureBindingMismatchV1}
	}
	registry, err := NewSubstrateRegistryV1(preparer.now, preparer.issuer, registration)
	if err != nil {
		return nil, substrateErrorV1{code: SubstrateFailureUnsupportedV1}
	}
	return registry.PrepareExecution(ctx, result)
}

func (preparer *ExactExecutionPreparerV1) PrepareReconciliation(
	ctx context.Context,
	result ports.ReserveAttemptEffectResultV1,
) (PreparedReconciliationV1, error) {
	if preparer == nil || preparer.issuer == nil || preparer.resolver == nil || ctx == nil || ctx.Err() != nil ||
		result.Validate() != nil || result.Status != ports.AttemptEffectReconcileOnlyV1 || result.ObservationGrant == nil {
		return nil, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	grant := result.ObservationGrant.Clone()
	if preparer.issuer.VerifyObservationGrant(grant) != nil {
		return nil, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	registration, err := preparer.resolver(grant.Authority.Clone())
	if err != nil || registration.Driver == nil {
		return nil, substrateErrorV1{code: SubstrateFailureUnsupportedV1}
	}
	if registration.Binding != grant.Authority.SubstrateBinding {
		return nil, substrateErrorV1{code: SubstrateFailureBindingMismatchV1}
	}
	registry, err := NewSubstrateRegistryV1(preparer.now, preparer.issuer, registration)
	if err != nil {
		return nil, substrateErrorV1{code: SubstrateFailureUnsupportedV1}
	}
	return &preparedReconciliationV1{
		registry: registry, authority: grant.Authority.Clone(), reservation: grant.Reservation.Clone(),
	}, nil
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

// PrepareExecution is the worker-facing composition boundary. Callers receive
// operations bound to one authenticated reservation, never the underlying
// PreparedInvocation capability.
func (registry *SubstrateRegistryV1) PrepareExecution(
	ctx context.Context,
	result ports.ReserveAttemptEffectResultV1,
) (PreparedExecutionV1, error) {
	prepared, err := registry.Prepare(ctx, result)
	if err != nil {
		return nil, err
	}
	return &preparedExecutionV1{registry: registry, prepared: prepared}, nil
}

func (execution *preparedExecutionV1) Execute(
	ctx context.Context,
	request ports.ExecutionRequest,
	sink ports.ExecutionEventSink,
	harness ports.HarnessDriver,
) (ports.ExecutionResult, domain.SubstrateExecutionEvidenceV1, error) {
	if execution == nil || execution.registry == nil {
		return ports.ExecutionResult{}, domain.SubstrateExecutionEvidenceV1{}, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	return execution.registry.Execute(ctx, execution.prepared, request, sink, harness)
}

func (execution *preparedExecutionV1) Cancel(ctx context.Context) (domain.SubstrateOperationObservationV1, error) {
	if execution == nil || execution.registry == nil {
		return domain.SubstrateOperationObservationV1{}, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	return execution.registry.Cancel(ctx, execution.prepared.Authority())
}

func (execution *preparedExecutionV1) Reconcile(ctx context.Context) (domain.SubstrateOperationObservationV1, error) {
	if execution == nil || execution.registry == nil {
		return domain.SubstrateOperationObservationV1{}, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	return execution.registry.Reconcile(ctx, execution.prepared.Authority())
}

func (reconciliation *preparedReconciliationV1) Reconcile(ctx context.Context) (domain.AttemptEffectReconciliationEvidenceV1, error) {
	if reconciliation == nil || reconciliation.registry == nil {
		return domain.AttemptEffectReconciliationEvidenceV1{}, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	observation, err := reconciliation.registry.Reconcile(ctx, reconciliation.authority.Clone())
	if err != nil {
		return domain.AttemptEffectReconciliationEvidenceV1{}, err
	}
	evidence, err := domain.SealAttemptEffectReconciliationEvidenceV1(
		reconciliation.authority, reconciliation.reservation, observation,
	)
	if err != nil {
		return domain.AttemptEffectReconciliationEvidenceV1{}, substrateErrorV1{code: SubstrateFailureBindingMismatchV1}
	}
	return evidence, nil
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

func (registry *SubstrateRegistryV1) Cancel(ctx context.Context, authority domain.ServerlessInvocationAuthorityV1) (domain.SubstrateOperationObservationV1, error) {
	return registry.observe(ctx, authority, true)
}

func (registry *SubstrateRegistryV1) Reconcile(ctx context.Context, authority domain.ServerlessInvocationAuthorityV1) (domain.SubstrateOperationObservationV1, error) {
	return registry.observe(ctx, authority, false)
}

func (registry *SubstrateRegistryV1) observe(ctx context.Context, authority domain.ServerlessInvocationAuthorityV1, cancel bool) (domain.SubstrateOperationObservationV1, error) {
	registration, _, err := registry.resolve(authority, false)
	if err != nil {
		return domain.SubstrateOperationObservationV1{}, err
	}
	var observation domain.SubstrateOperationObservationV1
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
		return domain.SubstrateOperationObservationV1{}, substrateErrorV1{code: code}
	}
	if err := observation.ValidateForAuthority(authority); err != nil {
		return domain.SubstrateOperationObservationV1{}, substrateErrorV1{code: SubstrateFailureBindingMismatchV1}
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
