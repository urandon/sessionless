package ydbstore

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const ownedAttemptPreparationBurnSQL = `SELECT
 CASE WHEN preparation_started_at IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN preparation_parent_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 COALESCE(preparation_started_at,CAST(0 AS Timestamp)),
 COALESCE(SUBSTRING(CAST(preparation_parent_digest AS String),0,65),CAST('' AS String)),
 CASE WHEN allocation_original_record IS NOT NULL OR allocation_original_digest IS NOT NULL
 OR allocation_current_record IS NOT NULL OR allocation_current_digest IS NOT NULL
 OR allocation_publication_digest IS NOT NULL OR allocation_transition_parent_digest IS NOT NULL
 THEN CAST(1 AS Uint64) ELSE CAST(0 AS Uint64) END
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`

const ownedAttemptPreparationBurnUpdateSQL = `UPDATE attempt_effect_reservations
 SET preparation_started_at=$1,preparation_parent_digest=$2
 WHERE tenant_id=$3 AND attempt_id=$4 AND kind=$5 AND owned_digest=$2
 AND record IS NULL AND preparation_started_at IS NULL AND preparation_parent_digest IS NULL
 AND allocation_original_record IS NULL AND allocation_original_digest IS NULL
 AND allocation_current_record IS NULL AND allocation_current_digest IS NULL
 AND allocation_publication_digest IS NULL AND allocation_transition_parent_digest IS NULL`

// A burn is preserved with its original sole parent, including after expiry or
// cancellation. It is NOT a registration, successful-preparation observation,
// fresh send capability or a new Attempt lifecycle. Do not clear it on failure.
func readOwnedPreparationBurn(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1) (bool, error) {
	burned, _, err := readOwnedPreparationBurnObservation(ctx, q, parent)
	return burned, err
}

// The canonical validator is shared with authentic post-preparation publication,
// which must not precede this exact committed setup burn.
func readOwnedPreparationBurnObservation(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1) (bool, time.Time, error) {
	var timePresent, digestPresent, allocationPresent uint64
	var startedAt time.Time
	var digest string
	r := parent.Reservation
	err := q.scanMetadata(ctx, ownedAttemptPreparationBurnSQL, []any{r.TenantID, r.AttemptID, r.Kind}, &timePresent, &digestPresent, &startedAt, &digest, &allocationPresent)
	if err != nil {
		return false, time.Time{}, q.budget.deny()
	}
	if timePresent == 0 && digestPresent == 0 && digest == "" {
		if allocationPresent != 0 {
			return false, time.Time{}, q.budget.deny()
		}
		return false, time.Time{}, nil
	}
	expected, digestErr := parent.Digest()
	if timePresent != 1 || digestPresent != 1 || digestErr != nil || len(digest) != 64 || digest != expected ||
		!ownedEffectTime(startedAt) || startedAt.Before(r.ReservedAt) || startedAt.After(q.at) || !startedAt.Before(parent.PreparationExpiresAt) {
		return false, time.Time{}, q.budget.deny()
	}
	return true, startedAt.UTC(), nil
}

// Private trusted composition only. true means this logical call confirmed the
// FIRST preparation burn; false means the original parent has already burned.
// Crucially a retry that observes its own committed lost acknowledgement also
// returns false: unknown commit may never authorize a fresh physical preflight.
// No physical I/O or capability minting occurs in the retryable SQL callback.
func (store *Store) beginBoundedOwnedAttemptPreparation(ctx context.Context, grant ports.AttemptEffectOwnershipGrantV1) (bool, error) {
	if store == nil || store.db == nil || ctx == nil || ctx.Err() != nil ||
		store.ownedAttemptEffectSourceVerifier == nil || store.ownedPreparationGrantVerifier == nil ||
		grant.Validate() != nil || store.ownedPreparationGrantVerifier.VerifyGrant(grant.Clone()) != nil {
		return false, domain.ErrMembershipDenied
	}
	grant = grant.Clone()
	r := grant.Reservation
	request := ports.ReserveAttemptEffectRequestV1{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID,
		LeaseID: r.LeaseID, FenceToken: r.FenceToken, PhysicalInvocationClaimID: r.PhysicalInvocationClaimID, UpstreamIdempotencyKeyDigest: r.UpstreamIdempotencyKeyDigest}
	var fresh bool
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		fresh = false
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		parent, err := readOwnedAttemptEffectParent(ctx, q, request)
		if err != nil || parent == nil || parent.Reservation.ReservedAt.After(q.at) || !attemptEffectRequestMatchesRecord(request, attemptEffectRecordV1{Authority: parent.Authority, Reservation: parent.Reservation}) ||
			parent.Reservation.PhysicalInvocationClaimID != r.PhysicalInvocationClaimID || !grant.GrantExpiresAt.Equal(parent.PreparationExpiresAt) {
			return q.budget.deny()
		}
		grantDigest, grantErr := r.DigestForAuthority(grant.Authority)
		parentDigest, parentErr := parent.Reservation.DigestForAuthority(parent.Authority)
		if grantErr != nil || parentErr != nil || grantDigest != parentDigest {
			return q.budget.deny()
		}
		final, err := readOwnedAdmission(ctx, q, parent.Scope)
		if err != nil || final == nil || parent.ValidateForAdmission(*final) != nil || store.ownedAttemptEffectSourceVerifier.VerifyOwnedAttemptEffectSource(final.Source.Clone()) != nil {
			return q.budget.deny()
		}
		burned, err := readOwnedPreparationBurn(ctx, q, *parent)
		if err != nil {
			return err
		}
		if burned {
			return nil // retained historical observation only, never another burn
		}
		if parent.Authority.ValidateAt(q.at) != nil || !q.at.Before(parent.PreparationExpiresAt) || store.validateCurrentOwnedAttemptParent(ctx, q, request, *parent) != nil {
			return q.budget.deny()
		}
		digest, err := parent.Digest()
		if err != nil {
			return q.budget.deny()
		}
		if _, err := q.ExecContext(ctx, ownedAttemptPreparationBurnUpdateSQL, q.at, digest, r.TenantID, r.AttemptID, r.Kind); err != nil {
			return err
		}
		fresh = true
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return false, domain.ErrMembershipDenied
	}
	if fresh && store.ownedPreparationGrantVerifier.VerifyGrant(grant.Clone()) != nil {
		return false, domain.ErrMembershipDenied // preserve burn despite expiry/verification failure
	}
	return fresh, nil
}

func (store *Store) validateCurrentOwnedAttemptParent(ctx context.Context, q *ownedAuthorityQuery, request ports.ReserveAttemptEffectRequestV1, parent domain.OwnedAttemptEffectParentV1) error {
	current, final, err := store.currentOwnedAttemptEffectAuthority(ctx, q, request)
	if err != nil || parent.ValidateForAdmission(final) != nil ||
		!current.Lease.AcquiredAt.Equal(parent.Authority.Lease.AcquiredAt) || current.Lease.WorkerID != parent.Authority.Lease.WorkerID ||
		current.Lease.ExpiresAt.Before(parent.Authority.Lease.ExpiresAt) || current.InvocationDeadline.Before(parent.Authority.InvocationDeadline) {
		return q.budget.deny()
	}
	current.Lease = parent.Authority.Lease
	current.InvocationDeadline = parent.Authority.InvocationDeadline
	digest, err := current.Digest()
	if err != nil || digest != parent.Reservation.InvocationAuthorityDigest || parent.Authority.ValidateAt(q.at) != nil {
		return q.budget.deny()
	}
	return nil
}
