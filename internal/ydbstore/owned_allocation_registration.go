package ydbstore

import (
	"bytes"
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Registration is an attachment on the SAME sole-parent row, not a new
// reservation, claim, send capability or Attempt lifecycle. Null flags preserve
// absence separately from empty/malformed bytes. Both retained snapshots are
// bounded before materialization; the original active snapshot never changes.
const ownedAllocationRegistrationSQL = `SELECT
 CASE WHEN allocation_original_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN allocation_original_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN allocation_current_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN allocation_current_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN allocation_publication_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN allocation_transition_parent_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 COALESCE(SUBSTRING(allocation_original_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(allocation_original_digest AS String),0,65),CAST('' AS String)),
 COALESCE(SUBSTRING(allocation_current_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(allocation_current_digest AS String),0,65),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(allocation_publication_digest AS String),0,65),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(allocation_transition_parent_digest AS String),0,65),CAST('' AS String))
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`

const ownedAllocationRegistrationCreateSQL = `UPDATE attempt_effect_reservations
 SET allocation_original_record=$1,allocation_original_digest=$2,
 allocation_current_record=$1,allocation_current_digest=$2,allocation_publication_digest=$3
 WHERE tenant_id=$4 AND attempt_id=$5 AND kind=$6 AND owned_digest=$7
 AND record IS NULL AND preparation_parent_digest=$7 AND preparation_started_at IS NOT NULL
 AND allocation_original_record IS NULL AND allocation_original_digest IS NULL
 AND allocation_current_record IS NULL AND allocation_current_digest IS NULL
 AND allocation_publication_digest IS NULL AND allocation_transition_parent_digest IS NULL`

const ownedAllocationRegistrationTransitionSQL = `UPDATE attempt_effect_reservations
 SET allocation_current_record=$1,allocation_current_digest=$2,allocation_transition_parent_digest=$3
 WHERE tenant_id=$4 AND attempt_id=$5 AND kind=$6 AND owned_digest=$7
 AND allocation_current_digest=$3`

type ownedAllocationSnapshots struct {
	original, current domain.RegisteredAllocationV2
	intent, previous  string
}

func (v ownedAllocationSnapshots) receipt(replayed bool) ports.OwnedAllocationRegistrationReceiptV2 {
	digest, _ := v.current.Digest()
	return ports.OwnedAllocationRegistrationReceiptV2{Original: v.original.Clone(), Current: v.current.Clone(), Digest: string(digest), Replayed: replayed}
}

// Private trusted-producer composition only. The proof verifier authenticates
// the actual prior preparation observation; neither a caller MAC nor a valid
// structural allocation is accepted in its place. No physical I/O or execution
// capability is produced in this retryable callback or by the returned receipt.
func (store *Store) publishBoundedOwnedAllocation(ctx context.Context, publication ports.OwnedAllocationPublicationV2) (ports.OwnedAllocationRegistrationReceiptV2, error) {
	deny := func() (ports.OwnedAllocationRegistrationReceiptV2, error) {
		return ports.OwnedAllocationRegistrationReceiptV2{}, domain.ErrMembershipDenied
	}
	if !store.validOwnedAllocationContext(ctx) || store.ownedAllocationPublicationVerifier == nil || publication.Validate() != nil {
		return deny()
	}
	publication = publication.Clone()
	if store.ownedAllocationPublicationVerifier.VerifyOwnedAllocationPublication(publication.Clone(), publication.ObservedAt) != nil {
		return deny()
	}
	intent, err := publication.IntentDigest()
	if err != nil {
		return deny()
	}
	r := publication.Reservation
	request := ownedAllocationParentRequest(r)
	var receipt ports.OwnedAllocationRegistrationReceiptV2
	var at time.Time
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		receipt, at = ports.OwnedAllocationRegistrationReceiptV2{}, time.Time{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		parent, source, burnAt, err := store.readOwnedAllocationParent(ctx, q, request)
		if err != nil || publication.ObservedAt.Before(burnAt) || publication.ObservedAt.After(q.at) || store.ownedAllocationPublicationVerifier.VerifyOwnedAllocationPublication(publication.Clone(), q.at) != nil {
			return q.budget.deny()
		}
		pd, pe := r.DigestForAuthority(publication.Authority)
		d, de := parent.Reservation.DigestForAuthority(parent.Authority)
		if pe != nil || de != nil || pd != d || !publication.PreparedInvocationExpiresAt.Equal(parent.PreparationExpiresAt) {
			return q.budget.deny()
		}
		old, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burnAt)
		if err != nil {
			return err
		}
		if old != nil {
			if old.intent != intent || old.original.ValidateForAuthority(publication.Authority, r, publication.Allocation, domain.PreparedInvocationDigestV1(publication.PreparedInvocationDigest), publication.PreparedInvocationExpiresAt, source) != nil || old.original.AllocationID != publication.AllocationID || old.original.AllocationGeneration != publication.AllocationGeneration || !old.original.Window.ObservedAt.Equal(publication.ObservedAt) {
				return q.budget.deny()
			}
			receipt, at = old.receipt(true), q.at
			return nil // exact preserved history; no current permission or renewal
		}
		if !q.at.Before(parent.PreparationExpiresAt) || store.validateCurrentOwnedAttemptParent(ctx, q, request, parent) != nil ||
			store.ownedAllocationPublicationVerifier.VerifyOwnedAllocationPublicationCurrent(publication.Clone(), q.at) != nil {
			return q.budget.deny()
		}
		expires := publication.PreparedInvocationExpiresAt
		if publication.Allocation.LocalRuntime != nil {
			expires, err = domain.LocalAllocationActiveDeadlineV2(parent.Authority, parent.Reservation, source)
			if err != nil {
				return q.budget.deny()
			}
		}
		for _, limit := range []time.Time{source.ExpiresAt, parent.Authority.Lease.ExpiresAt, parent.Authority.InvocationDeadline, parent.Authority.SubstrateBinding.ProfileEvidenceExpiresAt} {
			if limit.Before(expires) {
				expires = limit
			}
		}
		for _, spending := range source.Spending {
			if spending.Window.ExpiresAt.Before(expires) {
				expires = spending.Window.ExpiresAt
			}
		}
		value, err := domain.BuildRegisteredAllocationV2(parent.Authority, parent.Reservation, publication.Allocation, domain.PreparedInvocationDigestV1(publication.PreparedInvocationDigest), publication.PreparedInvocationExpiresAt, source, publication.AllocationID, publication.AllocationGeneration, domain.OwnedAgentAuthorityWindowV1{ObservedAt: publication.ObservedAt, PublishedAt: q.at, ExpiresAt: expires})
		if err != nil {
			return q.budget.deny()
		}
		raw, err := domain.EncodeRegisteredAllocationV2(value)
		digest, digestErr := value.Digest()
		parentDigest, parentErr := parent.Digest()
		if err != nil || digestErr != nil || parentErr != nil {
			return q.budget.deny()
		}
		if _, err := q.ExecContext(ctx, ownedAllocationRegistrationCreateSQL, string(raw), string(digest), intent, r.TenantID, r.AttemptID, r.Kind, parentDigest); err != nil {
			return err
		}
		actual, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burnAt)
		if err != nil || actual == nil || actual.intent != intent || actual.current.State != domain.RegisteredPreparationActiveV1 || actual.receipt(false).Digest != string(digest) {
			return q.budget.deny()
		}
		receipt, at = actual.receipt(false), q.at
		return nil
	})
	if err != nil || ctx.Err() != nil || store.ownedAllocationPublicationVerifier.VerifyOwnedAllocationPublication(publication.Clone(), at) != nil {
		return deny()
	}
	return receipt, nil
}

func (store *Store) validOwnedAllocationContext(ctx context.Context) bool {
	return store != nil && store.localCompositionReady() && store.db != nil && ctx != nil && ctx.Err() == nil && store.ownedAttemptEffectSourceVerifier != nil
}

func ownedAllocationParentRequest(r domain.AttemptEffectReservationV1) ports.ReserveAttemptEffectRequestV1 {
	return ports.ReserveAttemptEffectRequestV1{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID, FenceToken: r.FenceToken, PhysicalInvocationClaimID: r.PhysicalInvocationClaimID, UpstreamIdempotencyKeyDigest: r.UpstreamIdempotencyKeyDigest}
}

func (store *Store) readOwnedAllocationParent(ctx context.Context, q *ownedAuthorityQuery, request ports.ReserveAttemptEffectRequestV1) (domain.OwnedAttemptEffectParentV1, domain.AttemptExecutionSourceV1, time.Time, error) {
	deny := func() (domain.OwnedAttemptEffectParentV1, domain.AttemptExecutionSourceV1, time.Time, error) {
		return domain.OwnedAttemptEffectParentV1{}, domain.AttemptExecutionSourceV1{}, time.Time{}, q.budget.deny()
	}
	parent, err := readOwnedAttemptEffectParent(ctx, q, request)
	if err != nil || parent == nil || parent.Reservation.ReservedAt.After(q.at) || parent.Reservation.PhysicalInvocationClaimID != request.PhysicalInvocationClaimID || !attemptEffectRequestMatchesRecord(request, attemptEffectRecordV1{Authority: parent.Authority, Reservation: parent.Reservation}) {
		return deny()
	}
	final, err := readOwnedAdmission(ctx, q, parent.Scope)
	if err != nil || final == nil || parent.ValidateForAdmission(*final) != nil || store.ownedAttemptEffectSourceVerifier.VerifyOwnedAttemptEffectSource(final.Source.Clone()) != nil {
		return deny()
	}
	burned, burnAt, err := readOwnedPreparationBurnObservation(ctx, q, *parent)
	if err != nil || !burned {
		return deny()
	}
	return parent.Clone(), final.Source.Clone(), burnAt, nil
}

func readOwnedAllocationSnapshots(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, burnAt time.Time) (*ownedAllocationSnapshots, error) {
	var present [6]uint64
	var originalRaw, originalDigest, currentRaw, currentDigest, intent, previous string
	r := parent.Reservation
	if q.scanMetadata(ctx, ownedAllocationRegistrationSQL, []any{r.TenantID, r.AttemptID, r.Kind}, &present[0], &present[1], &present[2], &present[3], &present[4], &present[5], &originalRaw, &originalDigest, &currentRaw, &currentDigest, &intent, &previous) != nil {
		return nil, q.budget.deny()
	}
	if present == [6]uint64{} && originalRaw == "" && originalDigest == "" && currentRaw == "" && currentDigest == "" && intent == "" && previous == "" {
		return nil, nil
	}
	for _, flag := range present[:5] {
		if flag != 1 {
			return nil, q.budget.deny()
		}
	}
	if len(originalRaw) > ownedAuthorityHeadBytes || len(currentRaw) > ownedAuthorityHeadBytes || domain.ValidateSHA256Digest("allocation.intent", intent) != nil {
		return nil, q.budget.deny()
	}
	original, oe := domain.DecodeRegisteredAllocationV2([]byte(originalRaw))
	current, ce := domain.DecodeRegisteredAllocationV2([]byte(currentRaw))
	actualIntent, intentErr := ports.OwnedAllocationRegistrationIntentDigestV2(original)
	od, ode := original.Digest()
	cd, cde := current.Digest()
	if oe != nil || ce != nil || intentErr != nil || actualIntent != intent || ode != nil || cde != nil || string(od) != originalDigest || string(cd) != currentDigest || original.ValidateInitial() != nil || original.Window.ObservedAt.Before(burnAt) || original.Window.PublishedAt.After(q.at) || current.UpdatedAt.After(q.at) || !ownedAllocationOriginalMatches(original, parent, source) {
		return nil, q.budget.deny()
	}
	stable := current.Clone()
	stable.State, stable.RevocationGeneration, stable.UpdatedAt = original.State, original.RevocationGeneration, original.UpdatedAt
	raw, err := domain.EncodeRegisteredAllocationV2(stable)
	if err != nil || !bytes.Equal(raw, []byte(originalRaw)) {
		return nil, q.budget.deny()
	}
	if current.State == domain.RegisteredPreparationActiveV1 {
		if currentDigest != originalDigest || present[5] != 0 || previous != "" {
			return nil, q.budget.deny()
		}
	} else {
		if present[5] != 1 || domain.ValidateSHA256Digest("allocation.transition_parent", previous) != nil || previous == currentDigest || current.RevocationGeneration < 2 || current.RevocationGeneration > 3 || (current.State == domain.RegisteredPreparationRetiredV1 && current.RevocationGeneration != 2) || (current.RevocationGeneration == 2 && previous != originalDigest) || (current.RevocationGeneration == 3 && previous == originalDigest) {
			return nil, q.budget.deny()
		}
	}
	return &ownedAllocationSnapshots{original: original, current: current, intent: intent, previous: previous}, nil
}

func ownedAllocationOriginalMatches(v domain.RegisteredAllocationV2, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) bool {
	expected := v.Clone()
	expected.Scope, expected.Lease, expected.ParentReservation = source.Scope, parent.Authority.Lease, parent.Reservation.Clone()
	expected.ParentReservationDigest, _ = parent.Reservation.DigestForAuthority(parent.Authority)
	expected.Backend, expected.Substrate = parent.Authority.HarnessBinding.Backend, parent.Authority.SubstrateBinding
	expected.InvocationDeadline = parent.Authority.InvocationDeadline
	expected.PreparedInvocationExpiresAt = parent.PreparationExpiresAt
	expected.SourceDigest, _ = source.Digest()
	expected.Profile, expected.SourceIssuedAt, expected.SourceExpiresAt = source.Profile, source.IssuedAt, source.ExpiresAt
	for i, spending := range source.Spending {
		digest, _ := spending.Digest()
		expected.Spending[i] = domain.RegisteredSpendingReferenceV1{Locator: spending.Locator, RevocationGeneration: spending.RevocationGeneration, Digest: digest, Window: spending.Window.Clone()}
	}
	actual, ae := domain.EncodeRegisteredAllocationV2(v)
	want, we := domain.EncodeRegisteredAllocationV2(expected)
	return ae == nil && we == nil && bytes.Equal(actual, want)
}

// A historical read authenticates preserved parent/source bytes even after
// expiry or revocation. current=true additionally joins ALL current canonical
// authority; neither mode is an executable or renewable capability.
func (store *Store) observeBoundedOwnedAllocation(ctx context.Context, request ports.ReserveAttemptEffectRequestV1, current bool) (ports.OwnedAllocationRegistrationReceiptV2, error) {
	var result ports.OwnedAllocationRegistrationReceiptV2
	if !store.validOwnedAllocationContext(ctx) || request.Validate() != nil {
		return result, domain.ErrMembershipDenied
	}
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = ports.OwnedAllocationRegistrationReceiptV2{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		parent, source, burnAt, err := store.readOwnedAllocationParent(ctx, q, request)
		if err != nil {
			return err
		}
		snapshots, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burnAt)
		if err != nil || snapshots == nil {
			return q.budget.deny()
		}
		if current && (snapshots.current.ValidateAt(q.at) != nil || store.validateCurrentOwnedAttemptParent(ctx, q, request, parent) != nil) {
			return q.budget.deny()
		}
		result = snapshots.receipt(true)
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return ports.OwnedAllocationRegistrationReceiptV2{}, domain.ErrMembershipDenied
	}
	return result, nil
}

// Lifecycle transitions preserve the original active snapshot and all sealed
// facts. A lost acknowledgement recovers only its exact prior CAS digest/state;
// stale requests cannot replace or resurrect an allocation. Retirement remains
// possible after permission expiry, for reconciliation and shared cleanup.
func (store *Store) retireBoundedOwnedAllocation(ctx context.Context, request ports.ReserveAttemptEffectRequestV1, expectedDigest string, state domain.RegisteredPreparationStateV1) (ports.OwnedAllocationRegistrationReceiptV2, error) {
	var result ports.OwnedAllocationRegistrationReceiptV2
	if !store.validOwnedAllocationContext(ctx) || request.Validate() != nil || domain.ValidateSHA256Digest("allocation.expected", expectedDigest) != nil || (state != domain.RegisteredPreparationRetiredV1 && state != domain.RegisteredPreparationRevokedV1) {
		return result, domain.ErrMembershipDenied
	}
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = ports.OwnedAllocationRegistrationReceiptV2{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		parent, source, burnAt, err := store.readOwnedAllocationParent(ctx, q, request)
		if err != nil {
			return err
		}
		old, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burnAt)
		if err != nil || old == nil {
			return q.budget.deny()
		}
		if old.current.State == state && old.previous == expectedDigest {
			result = old.receipt(true)
			return nil
		}
		currentDigest, err := old.current.Digest()
		if err != nil || string(currentDigest) != expectedDigest {
			return q.budget.deny()
		}
		next := old.current.Clone()
		next.State, next.RevocationGeneration, next.UpdatedAt = state, next.RevocationGeneration+1, q.at
		if next.ValidateTransitionFrom(old.current) != nil {
			return q.budget.deny()
		}
		raw, err := domain.EncodeRegisteredAllocationV2(next)
		digest, de := next.Digest()
		parentDigest, pe := parent.Digest()
		if err != nil || de != nil || pe != nil {
			return q.budget.deny()
		}
		r := parent.Reservation
		if _, err := q.ExecContext(ctx, ownedAllocationRegistrationTransitionSQL, string(raw), string(digest), expectedDigest, r.TenantID, r.AttemptID, r.Kind, parentDigest); err != nil {
			return err
		}
		actual, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burnAt)
		if err != nil || actual == nil || actual.receipt(false).Digest != string(digest) || actual.previous != expectedDigest {
			return q.budget.deny()
		}
		result = actual.receipt(false)
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return ports.OwnedAllocationRegistrationReceiptV2{}, domain.ErrMembershipDenied
	}
	return result, nil
}
