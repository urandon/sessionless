package serverlessharness

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	outerharness "gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

// OwnedAttemptPreparationBurnV1 is supplied ONLY by trusted private composition.
// True means the existing canonical parent made its first null-to-burn transition
// in a confirmed fresh transaction, with all current original authorities checked.
// False means observation/reconciliation only. Any error, including a lost commit
// acknowledgement, MUST NOT authorize preflight. A caller boolean, grant MAC or
// fixture label is not an implementation of this durable boundary.
type OwnedAttemptPreparationBurnV1 interface {
	BeginOwnedAttemptPreparation(context.Context, ports.AttemptEffectOwnershipGrantV1) (bool, error)
}

// OwnedAttemptPreflightV1 exposes no model execution, cleanup or endpoint selector.
// The trusted composer fixes the credential-free fixture driver at construction.
type OwnedAttemptPreflightV1 interface {
	Preflight(context.Context, domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV2, error)
}

// BoundedOwnedAttemptPreparationV1 is a private-composition prerequisite, NOT an
// owned serving registration. Its prepared capability authenticates the actual
// validated preflight result; it is neither durable allocation/proxy publication
// nor sufficient authorization to dispatch any model, tool or search operation.
// It uses the existing PreparedInvocation type, which a trusted substrate can
// consume; that type is not an intrinsic owned-operation gate. Private serving
// composition must remain closed until registration and per-operation gates join.
type BoundedOwnedAttemptPreparationV1 struct {
	issuer  *CapabilityIssuer
	backend domain.HarnessBackendDescriptorV1
	binding domain.SubstrateBindingV1
	driver  OwnedAttemptPreflightV1
	local   *localExecutionAllocationV1
	burn    OwnedAttemptPreparationBurnV1
}

func NewBoundedOwnedAttemptPreparationV1(
	issuer *CapabilityIssuer,
	backend domain.HarnessBackendDescriptorV1,
	binding domain.SubstrateBindingV1,
	driver OwnedAttemptPreflightV1,
	burn OwnedAttemptPreparationBurnV1,
) (*BoundedOwnedAttemptPreparationV1, error) {
	if issuer == nil || issuer.clock == nil || driver == nil || burn == nil ||
		backend != outerharness.DeterministicFixtureDescriptorV1() ||
		binding.Validate() != nil || binding.Kind != domain.SubstrateDeterministicFixtureV1 {
		return nil, substrateErrorV1{code: SubstrateFailureUnsupportedV1}
	}
	return &BoundedOwnedAttemptPreparationV1{issuer: issuer, backend: backend, binding: binding, driver: driver, burn: burn}, nil
}

// Prepare performs one actual preflight only after confirmed durable consumption.
// No failure refunds either fence. Partial/unknown preparation requires bounded
// reconciliation/cleanup of the ORIGINAL allocation by its owning supervisor;
// this bridge cannot resume, remint history, cancel or prepare a replacement.
func (bridge *BoundedOwnedAttemptPreparationV1) Prepare(
	ctx context.Context,
	grant ports.AttemptEffectOwnershipGrantV1,
) (PreparedInvocation, error) {
	if bridge == nil || bridge.issuer == nil || ctx == nil || ctx.Err() != nil {
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	grant = grant.Clone()
	authority := grant.Authority
	fixture := bridge.local == nil && authority.HarnessBinding.Resource.Kind == domain.ProviderResourceCredentiallessFixtureV1 &&
		authority.HarnessBinding.Resource.CredentialMode == domain.ProviderCredentialNoneV1 && authority.HarnessBinding.Resource.CredentialGeneration == 0
	local := bridge.local != nil && bridge.local.source.ValidateExecutionBinding(authority, bridge.issuer.clock().UTC()) == nil
	if bridge.issuer.VerifyGrant(grant) != nil || (!fixture && !local) || authority.HarnessBinding.Profile != domain.HarnessExecutionOwnedAgentV2 ||
		authority.HarnessBinding.AgentLoopPolicy == nil || authority.HarnessBinding.Backend != bridge.backend ||
		authority.SubstrateBinding != bridge.binding {
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	// Never restart the original setup envelope at the time of replay. The
	// durable boundary independently checks the exact persisted parent deadline.
	deadline := grant.Reservation.ReservedAt.Add(authority.AdmissionCostCeiling.MaxPreEffectDurationPerDelivery)
	if grant.GrantExpiresAt.Before(deadline) {
		deadline = grant.GrantExpiresAt
	}
	remaining := deadline.Sub(bridge.issuer.clock().UTC())
	if remaining <= 0 || deadline.Year() > 9999 {
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureExpiredV1}
	}
	bounded, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()
	if bridge.issuer.ConsumePreparationGrant(grant) != nil {
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureReconcileV1}
	}
	fresh, err := bridge.burn.BeginOwnedAttemptPreparation(bounded, grant.Clone())
	if err != nil || !fresh {
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureReconcileV1}
	}
	if bounded.Err() != nil || bridge.issuer.VerifyGrant(grant) != nil || !bridge.issuer.clock().UTC().Before(deadline) {
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureReconcileV1}
	}
	var allocation domain.PreparedAllocationV2
	if bridge.local != nil {
		// The setup context ends when Prepare returns. The retained allocation
		// instead belongs to the original invocation context and fixed deadline.
		allocation, err = bridge.local.preflight(bounded, ctx, authority.Clone(), grant.Reservation.Clone())
	} else {
		allocation, err = bridge.driver.Preflight(bounded, authority.Clone())
	}
	if err != nil || bounded.Err() != nil {
		if bridge.local != nil {
			bridge.local.close()
		}
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailurePreflightV1}
	}
	if allocation.ValidateForBinding(bridge.binding, bridge.backend) != nil {
		if bridge.local != nil {
			bridge.local.close()
		}
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureBindingMismatchV1}
	}
	if bridge.issuer.VerifyGrant(grant) != nil || !bridge.issuer.clock().UTC().Before(deadline) {
		if bridge.local != nil {
			bridge.local.close()
		}
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureExpiredV1}
	}
	prepared, err := bridge.issuer.Issue(grant, allocation.Clone())
	if err != nil {
		if bridge.local != nil {
			bridge.local.close()
		}
		return PreparedInvocation{}, substrateErrorV1{code: SubstrateFailureAuthorityInvalidV1}
	}
	return prepared, nil
}
