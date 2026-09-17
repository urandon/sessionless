package codexexec

import (
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var ErrAuthorityUnavailable = errors.New("codex exec attached-worker authority is unavailable")

// AcceptedAuthorityResolverV1 owns one immutable authority snapshot supplied
// after the attached-worker protocol has accepted the exact attempt. It never
// reads central state, advances protocol state, or creates lease, resource, or
// credential authority on the user-owned worker.
type AcceptedAuthorityResolverV1 struct {
	authority AuthorityV1
	state     domain.AttachedWorkerAttemptState
	now       func() time.Time
}

func NewAcceptedAuthorityResolverV1(
	attempt domain.AttachedWorkerAttemptV1,
	resource domain.ProviderResourceBindingV1,
	now func() time.Time,
) (*AcceptedAuthorityResolverV1, error) {
	if attempt.Validate() != nil || resource.Validate() != nil ||
		resource.Kind != domain.ProviderResourceSubscriptionV1 ||
		resource.OwnerUserID != attempt.OwnerUserID ||
		resource.CredentialMode != domain.ProviderCredentialInvocationV1 {
		return nil, ErrContract
	}
	switch attempt.State {
	case domain.AttachedWorkerAttemptClaimed,
		domain.AttachedWorkerAttemptCancelRequested,
		domain.AttachedWorkerAttemptCancelAcknowledged:
	default:
		return nil, ErrContract
	}
	authority := AuthorityV1{
		Version:  ContractVersionV1,
		TenantID: attempt.TenantID, OwnerUserID: attempt.OwnerUserID,
		WorkerID: attempt.WorkerID, ConnectionID: attempt.ConnectionID,
		EnrollmentGeneration: attempt.EnrollmentGeneration,
		ConnectionGeneration: attempt.ConnectionGeneration,
		RunID:                attempt.RunID, AttemptID: attempt.AttemptID,
		ReservationID: attempt.ReservationID,
		LeaseID:       attempt.LeaseID, LeaseGeneration: attempt.LeaseGeneration,
		FenceToken: attempt.FenceToken, LeaseExpiresAt: attempt.LeaseExpiresAt,
		ContextDigest:    attempt.ContextDigest,
		CapabilityDigest: attempt.CapabilityDigest, PolicyDigest: attempt.PolicyDigest,
		ProviderResource: resource,
	}
	if authority.Validate() != nil {
		return nil, ErrContract
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &AcceptedAuthorityResolverV1{authority: authority, state: attempt.State, now: now}, nil
}

func (resolver *AcceptedAuthorityResolverV1) ResolveExecution(ctx context.Context, identity ports.ExecutionIdentity) (AuthorityV1, error) {
	if resolver == nil || resolver.state != domain.AttachedWorkerAttemptClaimed ||
		!resolver.authority.LeaseExpiresAt.After(resolver.now().UTC()) {
		return AuthorityV1{}, ErrAuthorityUnavailable
	}
	return resolver.resolve(ctx, identity)
}

func (resolver *AcceptedAuthorityResolverV1) ResolveCancellation(ctx context.Context, identity ports.ExecutionIdentity) (AuthorityV1, error) {
	if resolver == nil {
		return AuthorityV1{}, ErrAuthorityUnavailable
	}
	switch resolver.state {
	case domain.AttachedWorkerAttemptClaimed,
		domain.AttachedWorkerAttemptCancelRequested,
		domain.AttachedWorkerAttemptCancelAcknowledged:
		return resolver.resolve(ctx, identity)
	default:
		return AuthorityV1{}, ErrAuthorityUnavailable
	}
}

func (resolver *AcceptedAuthorityResolverV1) resolve(ctx context.Context, identity ports.ExecutionIdentity) (AuthorityV1, error) {
	if ctx == nil || ctx.Err() != nil || identity.Validate() != nil ||
		identity.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker ||
		!authorityMatchesIdentity(resolver.authority, identity) {
		return AuthorityV1{}, ErrAuthorityUnavailable
	}
	return resolver.authority, nil
}

func authorityMatchesIdentity(authority AuthorityV1, identity ports.ExecutionIdentity) bool {
	placement := identity.ExecutionPlacementV2
	return authority.Validate() == nil && authority.TenantID == identity.TenantID &&
		authority.OwnerUserID == identity.OwnerUserID && authority.WorkerID == placement.WorkerID &&
		authority.RunID == identity.RunID && authority.AttemptID == identity.AttemptID &&
		authority.CapabilityDigest == placement.CapabilityDigest &&
		authority.PolicyDigest == placement.PolicyDigest &&
		authority.ProviderResource == identity.HarnessBinding.Resource
}

var _ AuthorityResolverV1 = (*AcceptedAuthorityResolverV1)(nil)
