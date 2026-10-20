package ydbstore

import (
	"bytes"
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func ownedConsentOperationScope(s domain.WebSearchConsentScopeV1) domain.AttemptOperationScopeV1 {
	return domain.AttemptOperationScopeV1{TenantID: s.TenantID, SessionID: s.SessionID, OwnerUserID: s.OwnerUserID, RunID: s.RunID, AttemptID: s.AttemptID, LeaseID: s.LeaseID, FenceToken: s.FenceToken, PhysicalInvocationClaimID: s.PhysicalInvocationClaimID, EffectReservationDigest: domain.AttemptEffectReservationDigestV1(s.EffectReservationDigest), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(s.InvocationAuthorityDigest), HarnessBindingDigest: domain.HarnessBindingDigestV2(s.HarnessBindingDigest), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(s.PolicyDigest)}
}

func findOwnedConsent(v *ownedConsentStates, id domain.WebSearchConsentRequestIDV1) (domain.WebSearchConsentStateV1, bool) {
	if v != nil {
		for _, state := range v.States {
			if state.Request.ID == id {
				return state.Clone(), true
			}
		}
	}
	return domain.WebSearchConsentStateV1{}, false
}

// A locator is never ownership. Resolve the sole canonical parent first, then
// authenticate its original admission and match every locator before returning
// even the private consent projection.
func (store *Store) ownedConsentParent(ctx context.Context, q *ownedAuthorityQuery, l ports.WebSearchConsentLocatorV1) (domain.OwnedAttemptEffectParentV1, domain.AttemptExecutionSourceV1, error) {
	p, err := readOwnedAttemptEffectParent(ctx, q, ports.ReserveAttemptEffectRequestV1{TenantID: l.TenantID, AttemptID: l.AttemptID})
	if err != nil || p == nil || p.Scope.TenantID != l.TenantID || p.Scope.SessionID != l.SessionID || p.Scope.RunID != l.RunID || p.Scope.AttemptID != l.AttemptID {
		return domain.OwnedAttemptEffectParentV1{}, domain.AttemptExecutionSourceV1{}, q.budget.deny()
	}
	parent, source, _, err := store.readOwnedAllocationParent(ctx, q, ownedAllocationParentRequest(p.Reservation))
	return parent, source, err
}

func (store *Store) readOwnedConsentGrant(ctx context.Context, r domain.WebSearchConsentRequestV1) (ports.OwnedFixtureToolGrantV1, error) {
	if store.attemptOperationBodies == nil || r.Validate() != nil {
		return ports.OwnedFixtureToolGrantV1{}, domain.ErrMembershipDenied
	}
	ref, err := ports.OwnedFixtureGrantReferenceV1(r.GrantDigest)
	if err != nil {
		return ports.OwnedFixtureToolGrantV1{}, domain.ErrMembershipDenied
	}
	body, err := store.attemptOperationBodies.ReadAttemptOperationBody(ctx, ownedConsentOperationScope(r.Scope), ref, 4<<10)
	if err != nil || ctx.Err() != nil || len(body) > 4<<10 || ports.OwnedFixtureBytesDigestV1(body) != r.GrantDigest {
		return ports.OwnedFixtureToolGrantV1{}, domain.ErrMembershipDenied
	}
	copyBody := bytes.Clone(body)
	defer clear(copyBody)
	return ports.ParseOwnedFixtureToolGrantV1(copyBody)
}

// The existing private fixture ingress seals its authenticated task user in
// Source.Scope.OwnerUserID/Job.CredentialOwnerUserID. This does NOT introduce a
// resource-owner fallback or claim support for a federated different initiator.
func (store *Store) ownedConsentCurrentAuthority(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, r domain.WebSearchConsentRequestV1, grant ports.OwnedFixtureToolGrantV1) (domain.WebSearchConsentAuthorityV1, error) {
	return store.ownedConsentCurrentAuthorityData(ctx, q, parent, source, r, ownedCurrentConsentGrant{fixture: &grant})
}

// One current SQL gate serves both explicit evidence grammars. Neither a
// structural resource grant nor the fixture wrapper can bypass this owner.
func (store *Store) ownedConsentCurrentAuthorityData(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, r domain.WebSearchConsentRequestV1, grant ownedCurrentConsentGrant) (domain.WebSearchConsentAuthorityV1, error) {
	deny := func() (domain.WebSearchConsentAuthorityV1, error) {
		return domain.WebSearchConsentAuthorityV1{}, q.budget.deny()
	}
	cost := parent.Authority.AdmissionCostCeiling.WebSearchCost
	consentRevision, revocationGeneration, grantExpiry, grantErr := grant.validate(parent, source, r, q.at)
	if source.Validate() != nil || r.Scope != ownedConsentScope(parent) || source.Scope.OwnerUserID != r.InitiatingUserID || cost == nil || grantErr != nil {
		return deny()
	}
	request := ownedAllocationParentRequest(parent.Reservation)
	if store.validateCurrentOwnedAttemptParent(ctx, q, request, parent) != nil {
		return deny()
	}
	loaded, err := loadOwnedEffectWorkerJob(ctx, q, request)
	if err != nil || loaded.Job.CredentialOwnerUserID != r.InitiatingUserID {
		return deny()
	}
	lease, err := readOwnedAttemptEffectLease(ctx, q, request)
	if err != nil {
		return deny()
	}
	burned, burn, err := readOwnedPreparationBurnObservation(ctx, q, parent)
	if err != nil || !burned {
		return deny()
	}
	allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burn)
	if err != nil || allocation == nil || allocation.current.ValidateAt(q.at) != nil {
		return deny()
	}
	policy := parent.Authority.HarnessBinding.AgentLoopPolicy
	a := domain.WebSearchConsentAuthorityV1{Scope: ownedConsentScope(parent), Run: loaded.Run, Attempt: loaded.Attempt, Lease: lease,
		InitiatingUserID: source.Scope.OwnerUserID, GrantDigest: r.GrantDigest, ConsentRevision: consentRevision, RevocationGeneration: revocationGeneration,
		DataClass: parent.Authority.HarnessBinding.InputDataClass, ClaimState: domain.WebSearchConsentClaimActiveV1,
		InvocationDeadline: parent.Authority.InvocationDeadline, ActiveDeadline: parent.Authority.Lease.AcquiredAt.Add(policy.Limits.MaxActiveDuration),
		GrantExpiresAt: grantExpiry, PolicyExpiresAt: policy.ExpiresAt, PriceExpiresAt: cost.PriceExpiresAt, AllocationExpiresAt: allocation.current.Window.ExpiresAt}
	// Never extend a receipt from a later heartbeat or renewed allocation.
	if original := allocation.original.Window.ExpiresAt; original.Before(a.AllocationExpiresAt) {
		a.AllocationExpiresAt = original
	}
	if parent.Authority.Lease.ExpiresAt.Before(a.Lease.ExpiresAt) {
		a.Lease.ExpiresAt = parent.Authority.Lease.ExpiresAt
	}
	if a.ValidateCurrent(r, q.at) != nil {
		return deny()
	}
	return a, nil
}

// Point reads are bounded BEFORE materialization, share the same statement/
// byte budget and transaction clock, and never refresh browser activity.
func loadOwnedConsentActor(ctx context.Context, q *ownedAuthorityQuery, r domain.WebSearchConsentRequestV1, digest domain.SecretDigest, a *domain.WebSearchConsentAuthorityV1) error {
	bucket, err := webBucket(string(digest))
	if err != nil || digest.Validate("owned_consent.web_session_digest") != nil {
		return q.budget.deny()
	}
	raw, err := q.readMetadata(ctx, `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, ownedAuthorityHeadBytes, bucket, digest)
	if err != nil || decodeOwnedEffectCanonicalJSON(raw, &a.WebSession) != nil || a.WebSession.SessionDigest != digest || a.WebSession.UserID != r.InitiatingUserID || a.WebSession.ActiveTenantID != r.Scope.TenantID {
		return q.budget.deny()
	}
	memberBucket, err := webBucket(string(r.InitiatingUserID))
	if err != nil {
		return q.budget.deny()
	}
	for _, v := range []struct {
		statement string
		args      []any
		target    any
	}{
		{"SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3", []any{memberBucket, r.InitiatingUserID, r.Scope.TenantID}, &a.Membership},
		{"SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3", []any{r.Scope.TenantID, r.Scope.SessionID, r.InitiatingUserID}, &a.Participant},
	} {
		raw, err := q.readCanonicalJSON(ctx, v.statement, v.args...)
		if err != nil || decodeOwnedEffectCanonicalJSON(raw, v.target) != nil {
			return q.budget.deny()
		}
	}
	if a.ValidateActor(r, q.at) != nil {
		return q.budget.deny()
	}
	return nil
}

// Called inside the EXISTING reserve and one-use before-send transaction. The
// evidence came from actual keeper bytes outside SQL, not a supplied grant DTO.
func (store *Store) validateOwnedOperationConsent(ctx context.Context, q *ownedAuthorityQuery, op domain.AttemptOperationV1, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, supplied *domain.WebSearchConsentReceiptBindingV1, evidence ownedFixtureToolEvidence) error {
	if op.Kind != domain.AttemptOperationWebSearchV1 || store.validateOwnedFixtureToolEvidence(ctx, q, evidence, parent, source) != nil {
		return q.budget.deny()
	}
	states, err := readOwnedConsentStates(ctx, q, parent)
	state, found := findOwnedConsent(states, domain.WebSearchConsentRequestIDV1(op.ConsentDecisionRef))
	if err != nil || !found || state.Decision == nil {
		return q.budget.deny()
	}
	rd, _ := state.Request.Digest()
	dd, _ := state.Decision.Digest()
	binding := domain.WebSearchConsentReceiptBindingV1{Request: state.Request, RequestDigest: rd, DecisionDigest: dd}
	if supplied != nil && (supplied.Validate() != nil || supplied.RequestDigest != rd || supplied.DecisionDigest != dd) {
		return q.budget.deny()
	}
	if (ports.ReserveAttemptOperationRequestV1{Operation: op, ConsentReceipt: &binding}).Validate() != nil {
		return q.budget.deny()
	}
	a, err := store.ownedConsentCurrentAuthority(ctx, q, parent, source, state.Request, evidence.grant)
	if err != nil || loadOwnedConsentActor(ctx, q, state.Request, state.Decision.WebSessionDigest, &a) != nil || domain.ValidateWebSearchConsentReceiptV1(state, binding, a, q.at) != nil {
		return q.budget.deny()
	}
	return nil
}

func consentCallBound(r domain.WebSearchConsentRequestV1, e ownedFixtureToolEvidence) bool {
	return e.call.Kind == domain.AttemptOperationWebSearchV1 && e.call.CallID == r.CallID && ports.OwnedFixtureBytesDigestV1(e.call.Arguments) == r.ArgumentsDigest && r.ValidateArguments(e.call.Arguments) == nil && uint64(len(e.call.Arguments)) <= e.grant.MaxArgumentBytes && e.grantDigest == r.GrantDigest && e.grant.ConsentRevision == r.ConsentRevision && e.grant.RevocationGeneration == r.RevocationGeneration && !r.ExpiresAt.After(e.grant.ExpiresAt)
}

// All public consent methods share one absolute ten-second call deadline,
// including outside-SQL immutable body reads and fresh transactional rejoin.
func ownedConsentContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return ownedAuthorityCallContext(ctx)
}
