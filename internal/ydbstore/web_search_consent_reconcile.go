package ydbstore

import (
	"context"
	"database/sql"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var _ ports.WebSearchConsentStoreV1 = (*Store)(nil)

// A worker reconciliation call selects only an original immutable request.
// It cannot choose the stop reason, supply browser/approval authority, repair
// missing rows or reopen a stop. No current-permission loader's sticky denial
// is ignored to manufacture a successful stop transaction.
func (store *Store) ReconcileWebSearchConsent(ctx context.Context, input ports.ReconcileWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error) {
	if !store.validOwnedAllocationContext(ctx) || input.Validate() != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	ctx, cancel := ownedConsentContext(ctx)
	defer cancel()
	// Select the negative-only transaction before any grant-body I/O. The
	// immutable request already seals its grant identity and original expiry;
	// only actual canonical stop facts may change its status. This path never
	// catches or ignores a current-authority loader's sticky denial.
	var result domain.WebSearchConsentStateV1
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = domain.WebSearchConsentStateV1{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		parent, source, err := store.ownedConsentParent(ctx, q, input.Locator)
		if err != nil {
			return err
		}
		states, err := readOwnedConsentStates(ctx, q, parent)
		state, found := findOwnedConsent(states, input.Locator.RequestID)
		if err != nil || !found || input.Locator.ValidateForRequest(state.Request) != nil || state.Request.InitiatingUserID != source.Scope.OwnerUserID {
			return q.budget.deny()
		}
		if state.Status != domain.WebSearchConsentPendingV1 && state.Status != domain.WebSearchConsentApprovedV1 {
			// Existing immutable stop/deny evidence, never a repaired request.
			if state.Revision != input.ExpectedRevision+1 && state.Revision != input.ExpectedRevision {
				return q.budget.deny()
			}
			result = state.Clone()
			return nil
		}
		if state.Revision != input.ExpectedRevision {
			return q.budget.deny()
		}
		a, revoked, err := store.loadOwnedConsentStopAuthority(ctx, q, parent, source, state)
		if err != nil {
			return err
		}
		var next domain.WebSearchConsentStateV1
		// Valid structural current heads that no longer equal the original
		// authenticated Source are OBSERVED revocation, not permission. The
		// fixed pure projection has no independent source-state grant field;
		// append only this actually loaded stop, after cancellation/claim loss.
		if revoked && a.Run.CancellationRequestedAt == nil && a.Run.Status == domain.RunRunning && a.Attempt.Status == domain.AttemptRunning && a.ClaimState == domain.WebSearchConsentClaimActiveV1 {
			if !ownedEffectTime(q.at) || q.at.Before(state.UpdatedAt) || q.at.Before(a.Run.UpdatedAt) || q.at.Before(a.Attempt.UpdatedAt) || a.Lease.ValidateForAttempt(a.Run, a.Attempt) != nil {
				return q.budget.deny()
			}
			next = state.Clone()
			next.Status, next.Revision, next.UpdatedAt = domain.WebSearchConsentRevokedV1, state.Revision+1, q.at
		} else {
			next, err = domain.StopWebSearchConsentV1(state, input.ExpectedRevision, a, q.at)
			if err != nil {
				return q.budget.deny()
			}
		}
		projection, err := replaceOwnedConsentState(*states, next, parent, q.at)
		if err != nil || writeOwnedConsentStates(ctx, q, parent, projection) != nil {
			return q.budget.deny()
		}
		actual, err := readOwnedConsentStates(ctx, q, parent)
		result, found = findOwnedConsent(actual, state.Request.ID)
		if err != nil || !found || result.Status != next.Status || result.Revision != next.Revision || !result.UpdatedAt.Equal(next.UpdatedAt) {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	return result, nil
}

func (store *Store) loadOwnedConsentStopAuthority(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, state domain.WebSearchConsentStateV1) (domain.WebSearchConsentAuthorityV1, bool, error) {
	deny := func() (domain.WebSearchConsentAuthorityV1, bool, error) {
		return domain.WebSearchConsentAuthorityV1{}, false, q.budget.deny()
	}
	r := state.Request
	cost := parent.Authority.AdmissionCostCeiling.WebSearchCost
	if r.Scope != ownedConsentScope(parent) || cost == nil || cost.ValidateAt(r.CreatedAt) != nil || cost.Kind != domain.ExternalServiceWebSearchV1 || cost.TenantID != r.Scope.TenantID || cost.OwnerUserID != r.Scope.OwnerUserID || cost.ScopedGrantDigest != r.GrantDigest || cost.RevocationGeneration != r.RevocationGeneration || r.ExpiresAt.After(cost.PriceExpiresAt) || r.ExpiresAt.After(parent.Authority.HarnessBinding.AgentLoopPolicy.ExpiresAt) {
		return deny()
	}
	// These are retained identity facts for StopWebSearchConsentV1 ONLY, not
	// evidence of current grant validity. Original request expiry was bounded
	// at creation; no unavailable grant expiry is invented or renewed here.
	a := domain.WebSearchConsentAuthorityV1{Scope: r.Scope, InitiatingUserID: source.Scope.OwnerUserID, GrantDigest: r.GrantDigest, ConsentRevision: r.ConsentRevision, RevocationGeneration: r.RevocationGeneration, DataClass: parent.Authority.HarnessBinding.InputDataClass, ClaimState: domain.WebSearchConsentClaimActiveV1, InvocationDeadline: parent.Authority.InvocationDeadline, ActiveDeadline: parent.Authority.Lease.AcquiredAt.Add(parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxActiveDuration), PolicyExpiresAt: parent.Authority.HarnessBinding.AgentLoopPolicy.ExpiresAt, PriceExpiresAt: cost.PriceExpiresAt}
	for _, row := range []struct {
		query  string
		args   []any
		target any
	}{
		{"SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2", []any{r.Scope.TenantID, r.Scope.RunID}, &a.Run},
		{"SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2", []any{r.Scope.TenantID, r.Scope.AttemptID}, &a.Attempt},
	} {
		raw, err := q.readCanonicalJSON(ctx, row.query, row.args...)
		if err != nil || decodeOwnedEffectCanonicalJSON(raw, row.target) != nil {
			return deny()
		}
	}
	if a.Run.ID != r.Scope.RunID || a.Run.TenantID != r.Scope.TenantID || a.Run.SessionID != r.Scope.SessionID || a.Attempt.ID != r.Scope.AttemptID || a.Attempt.ValidateForRun(a.Run) != nil {
		return deny()
	}
	raw, err := q.readMetadata(ctx, `SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM leases WHERE tenant_id=$1 AND lease_id=$2`, ownedAuthorityHeadBytes, r.Scope.TenantID, r.Scope.LeaseID)
	if err != nil || decodeOwnedEffectCanonicalJSON(raw, &a.Lease) != nil || a.Lease.ID != r.Scope.LeaseID || a.Lease.FenceToken != r.Scope.FenceToken || a.Lease.ValidateForAttempt(a.Run, a.Attempt) != nil || !a.Lease.AcquiredAt.Equal(parent.Authority.Lease.AcquiredAt) || a.Lease.WorkerID != parent.Authority.Lease.WorkerID || q.at.Before(a.Lease.AcquiredAt) {
		return deny()
	}
	actualLeaseExpiry := a.Lease.ExpiresAt
	if parent.Authority.Lease.ExpiresAt.Before(a.Lease.ExpiresAt) {
		a.Lease.ExpiresAt = parent.Authority.Lease.ExpiresAt
	}
	var leaseID, attemptID, worker string
	var fence uint64
	var expires, updated time.Time
	err = q.scanMetadata(ctx, `SELECT SUBSTRING(CAST(lease_id AS String),0,161),SUBSTRING(CAST(attempt_id AS String),0,161),SUBSTRING(CAST(worker_id AS String),0,161),fence_token,expires_at,updated_at FROM lease_heads WHERE tenant_id=$1 AND run_id=$2`, []any{r.Scope.TenantID, r.Scope.RunID}, &leaseID, &attemptID, &worker, &fence, &expires, &updated)
	if err == sql.ErrNoRows {
		a.ClaimState = domain.WebSearchConsentClaimLostV1
	} else if err != nil || domain.LeaseID(leaseID).Validate() != nil || domain.AttemptID(attemptID).Validate() != nil || domain.ValidateOpaqueID("consent_stop.worker", worker) != nil || fence == 0 || !ownedEffectTime(expires) || !ownedEffectTime(updated) || updated.After(q.at) {
		return deny()
	} else if leaseID != string(r.Scope.LeaseID) || attemptID != string(r.Scope.AttemptID) || worker != a.Lease.WorkerID || fence != r.Scope.FenceToken {
		a.ClaimState = domain.WebSearchConsentClaimLostV1
	} else if !expires.Equal(actualLeaseExpiry) {
		return deny() // malformed head/retained-lease correlation is not a stop
	}
	profile, present, err := loadOwnedProfileProjection(ctx, q, source.Profile.Locator)
	if err != nil || (present && profile.UpdatedAt.After(q.at)) {
		return deny()
	}
	pd, _ := profile.Digest()
	revoked := !present || pd != source.Profile.Digest || profile.RevocationGeneration != source.Profile.RevocationGeneration
	for _, original := range source.Spending {
		current, present, err := loadOwnedSpendingProjection(ctx, q, original.Locator)
		if err != nil || (present && current.UpdatedAt.After(q.at)) {
			return deny()
		}
		cd, _ := current.Digest()
		od, _ := original.Digest()
		if !present || cd != od || current.RevocationGeneration != original.RevocationGeneration {
			revoked = true
		}
	}
	burned, burn, err := readOwnedPreparationBurnObservation(ctx, q, parent)
	if err != nil || !burned {
		return deny()
	}
	allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burn)
	if err != nil || allocation == nil {
		return deny()
	}
	a.AllocationExpiresAt = allocation.original.Window.ExpiresAt
	if allocation.current.Window.ExpiresAt.Before(a.AllocationExpiresAt) {
		a.AllocationExpiresAt = allocation.current.Window.ExpiresAt
	}
	if allocation.current.State != domain.RegisteredPreparationActiveV1 {
		a.ClaimState = domain.WebSearchConsentClaimLostV1
	}
	if err := loadOwnedConsentStopActor(ctx, q, state, &a); err != nil {
		return deny()
	}
	return a, revoked, nil
}

// Structural reads of actual removed/expired actor facts are needed for a stop,
// never for disclosure/approval/send. An absent current actor is revocation;
// malformed or foreign row content denies instead of being treated as removal.
func loadOwnedConsentStopActor(ctx context.Context, q *ownedAuthorityQuery, state domain.WebSearchConsentStateV1, a *domain.WebSearchConsentAuthorityV1) error {
	r := state.Request
	bucket, err := webBucket(string(r.InitiatingUserID))
	if err != nil {
		return q.budget.deny()
	}
	for _, row := range []struct {
		query  string
		args   []any
		target any
	}{
		{"SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3", []any{bucket, r.InitiatingUserID, r.Scope.TenantID}, &a.Membership},
		{"SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3", []any{r.Scope.TenantID, r.Scope.SessionID, r.InitiatingUserID}, &a.Participant},
	} {
		raw, err := q.readCanonicalJSON(ctx, row.query, row.args...)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil || decodeOwnedEffectCanonicalJSON(raw, row.target) != nil {
			return q.budget.deny()
		}
	}
	if a.Membership.UserID != "" && (a.Membership.UserID != r.InitiatingUserID || a.Membership.TenantID != r.Scope.TenantID || a.Membership.Validate() != nil || a.Membership.UpdatedAt.After(q.at)) {
		return q.budget.deny()
	}
	if a.Participant.UserID != "" && (a.Participant.UserID != r.InitiatingUserID || a.Participant.SessionID != r.Scope.SessionID || a.Participant.TenantID != r.Scope.TenantID || a.Participant.Validate() != nil || a.Participant.UpdatedAt.After(q.at)) {
		return q.budget.deny()
	}
	if state.Decision == nil {
		return nil // no browser session exists for a pending worker request
	}
	digest := state.Decision.WebSessionDigest
	bucket, err = webBucket(string(digest))
	if err != nil {
		return q.budget.deny()
	}
	raw, err := q.readMetadata(ctx, `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, ownedAuthorityHeadBytes, bucket, digest)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil || decodeOwnedEffectCanonicalJSON(raw, &a.WebSession) != nil || a.WebSession.SessionDigest != digest || a.WebSession.UserID != r.InitiatingUserID || a.WebSession.ActiveTenantID != r.Scope.TenantID || a.WebSession.Validate() != nil || a.WebSession.LastSeenAt.After(q.at) || (a.WebSession.RevokedAt != nil && a.WebSession.RevokedAt.After(q.at)) {
		return q.budget.deny()
	}
	return nil
}
