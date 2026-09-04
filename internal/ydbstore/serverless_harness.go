package ydbstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type attemptEffectRecordV1 struct {
	Authority   domain.ServerlessInvocationAuthorityV1 `json:"authority"`
	Reservation domain.AttemptEffectReservationV1      `json:"reservation"`
}

func (record attemptEffectRecordV1) validate() error {
	if err := record.Authority.Validate(); err != nil {
		return err
	}
	return record.Reservation.ValidateForAuthority(record.Authority)
}

// ReserveAttemptEffect is the durable one-way provider-effect fence. The
// request supplies only locators and a fresh physical claim. All execution
// authority and the transaction timestamp are reconstructed under one
// serializable YDB transaction before the first INSERT.
func (store *Store) ReserveAttemptEffect(ctx context.Context, request ports.ReserveAttemptEffectRequestV1) (result ports.ReserveAttemptEffectResultV1, err error) {
	if err := request.Validate(); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		existing, found, readErr := readAttemptEffectRecordTx(ctx, tx, request.AttemptID)
		if readErr != nil {
			return readErr
		}
		if found {
			if err := existing.validate(); err != nil {
				return fmt.Errorf("invalid persisted attempt effect: %w", err)
			}
			if !attemptEffectRequestMatchesRecord(request, existing) {
				return domain.ValidationError{Field: "reserve_attempt_effect", Reason: "does not match the persisted attempt effect scope"}
			}
			result = ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectReconcileOnlyV1, Reservation: existing.Reservation.Clone()}
			if existing.Reservation.PhysicalInvocationClaimID != request.PhysicalInvocationClaimID {
				return result.Validate()
			}
			current, authorized, err := currentServerlessInvocationAuthorityTx(ctx, tx, request, at)
			if err != nil {
				return err
			}
			currentDigest, digestErr := current.Digest()
			if !authorized || digestErr != nil || currentDigest != existing.Reservation.InvocationAuthorityDigest {
				return result.Validate()
			}
			grant, err := store.mintAttemptEffectGrant(current, existing.Reservation, at)
			if err != nil {
				return err
			}
			result.Status, result.Grant = ports.AttemptEffectReplayedV1, &grant
			return result.Validate()
		}
		authority, authorized, err := currentServerlessInvocationAuthorityTx(ctx, tx, request, at)
		if err != nil {
			return err
		}
		if !authorized {
			return domain.ValidationError{Field: "reserve_attempt_effect", Reason: "canonical authority is not executable"}
		}
		reservation, err := domain.BuildAttemptEffectReservationV1(authority, request.PhysicalInvocationClaimID, request.UpstreamIdempotencyKeyDigest, at)
		if err != nil {
			return err
		}
		record := attemptEffectRecordV1{Authority: authority.Clone(), Reservation: reservation.Clone()}
		payload, err := marshal(record)
		if err != nil {
			return err
		}
		authorityDigest, _ := authority.Digest()
		if _, err := tx.sqlTx.ExecContext(ctx,
			`INSERT INTO attempt_effect_reservations
			 (tenant_id,attempt_id,kind,run_id,lease_id,fence_token,physical_invocation_claim_id,
			  effect_sequence,invocation_authority_digest,reserved_at,record)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CAST($11 AS JsonDocument))`,
			request.TenantID, request.AttemptID, domain.ProviderEffectTurnV1, request.RunID,
			request.LeaseID, request.FenceToken, request.PhysicalInvocationClaimID,
			domain.ServerlessProviderEffectSequenceV1, authorityDigest, reservation.ReservedAt, payload,
		); err != nil {
			return err
		}
		grant, err := store.mintAttemptEffectGrant(authority, reservation, at)
		if err != nil {
			return err
		}
		result = ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectOwnedV1, Reservation: reservation.Clone(), Grant: &grant}
		return result.Validate()
	})
	return result, err
}

func currentServerlessInvocationAuthorityTx(ctx context.Context, tx *stateTx, request ports.ReserveAttemptEffectRequestV1, at time.Time) (domain.ServerlessInvocationAuthorityV1, bool, error) {
	loaded, found, err := loadWorkerJobStateTx(ctx, tx, request.RunID)
	if err != nil || !found {
		return domain.ServerlessInvocationAuthorityV1{}, false, err
	}
	if loaded.Job.AttemptID != request.AttemptID || loaded.Attempt.ID != request.AttemptID ||
		loaded.Run.CancellationRequestedAt != nil || loaded.Run.Status != domain.RunRunning || loaded.Attempt.Status != domain.AttemptRunning {
		return domain.ServerlessInvocationAuthorityV1{}, false, nil
	}
	if loaded.Job.ExecutionPlacementV2.Kind != domain.ExecutionPlacementManaged || loaded.Job.SubstrateBinding == nil || loaded.Job.AdmissionCostCeiling == nil {
		return domain.ServerlessInvocationAuthorityV1{}, false, nil
	}
	if loaded.Job.HarnessBinding.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 ||
		loaded.Job.HarnessBinding.Resource.Kind != domain.ProviderResourceCredentiallessFixtureV1 {
		return domain.ServerlessInvocationAuthorityV1{}, false, nil
	}
	lease, found, err := readCanonicalLeaseHeadTx(ctx, tx, request.RunID)
	if err != nil || !found {
		return domain.ServerlessInvocationAuthorityV1{}, false, err
	}
	if lease.ID != request.LeaseID || lease.AttemptID != request.AttemptID || lease.FenceToken != request.FenceToken {
		return domain.ServerlessInvocationAuthorityV1{}, false, nil
	}
	if err := requireLeaseOwnership(ctx, tx, request.RunID, request.LeaseID, request.FenceToken, at); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			return domain.ServerlessInvocationAuthorityV1{}, false, nil
		}
		return domain.ServerlessInvocationAuthorityV1{}, false, err
	}
	contextDigest, inputDigest, err := domain.ServerlessWorkerJobDigestsV1(loaded.Job, loaded.InputManifest)
	if err != nil {
		return domain.ServerlessInvocationAuthorityV1{}, false, err
	}
	invocationDeadline := lease.AcquiredAt.Add(loaded.Job.SubstrateBinding.Limits.InvocationTimeout)
	if lease.ExpiresAt.Before(invocationDeadline) {
		invocationDeadline = lease.ExpiresAt
	}
	authority := domain.ServerlessInvocationAuthorityV1{
		Version:        domain.ServerlessInvocationAuthorityVersionV1,
		HarnessBinding: loaded.Job.HarnessBinding.Clone(), ExecutionPlacementV2: loaded.Job.ExecutionPlacementV2,
		SubstrateBinding: *loaded.Job.SubstrateBinding, AdmissionCostCeiling: loaded.Job.AdmissionCostCeiling.Clone(),
		Lease: lease, ContextManifestDigest: contextDigest, InputManifestDigest: inputDigest,
		InvocationDeadline: invocationDeadline.UTC(),
	}
	if err := authority.ValidateAt(at); err != nil {
		return domain.ServerlessInvocationAuthorityV1{}, false, nil
	}
	return authority, true, nil
}

func (store *Store) mintAttemptEffectGrant(authority domain.ServerlessInvocationAuthorityV1, reservation domain.AttemptEffectReservationV1, at time.Time) (ports.AttemptEffectOwnershipGrantV1, error) {
	if store.attemptEffectGrantIssuer == nil {
		return ports.AttemptEffectOwnershipGrantV1{}, errors.New("attempt effect ownership grant issuer is not configured")
	}
	expiresAt := at.Add(authority.AdmissionCostCeiling.MaxPreEffectDurationPerDelivery)
	for _, deadline := range []time.Time{authority.InvocationDeadline, authority.SubstrateBinding.ProfileEvidenceExpiresAt, authority.AdmissionCostCeiling.PriceExpiresAt} {
		if deadline.Before(expiresAt) {
			expiresAt = deadline
		}
	}
	if authority.HarnessBinding.EvidenceExpiresAt != nil && authority.HarnessBinding.EvidenceExpiresAt.Before(expiresAt) {
		expiresAt = *authority.HarnessBinding.EvidenceExpiresAt
	}
	if !expiresAt.After(at) {
		return ports.AttemptEffectOwnershipGrantV1{}, errors.New("attempt effect ownership grant has no fresh pre-effect window")
	}
	return store.attemptEffectGrantIssuer.MintAttemptEffectOwnershipGrant(authority, reservation, expiresAt)
}

func readAttemptEffectRecordTx(ctx context.Context, tx *stateTx, attemptID domain.AttemptID) (attemptEffectRecordV1, bool, error) {
	return readJSON[attemptEffectRecordV1](ctx, tx.sqlTx,
		`SELECT record FROM attempt_effect_reservations
		 WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`,
		tx.tenantID, attemptID, domain.ProviderEffectTurnV1)
}

func attemptEffectRequestMatchesRecord(request ports.ReserveAttemptEffectRequestV1, record attemptEffectRecordV1) bool {
	reservation := record.Reservation
	if reservation.TenantID != request.TenantID || reservation.RunID != request.RunID ||
		reservation.AttemptID != request.AttemptID || reservation.LeaseID != request.LeaseID ||
		reservation.FenceToken != request.FenceToken {
		return false
	}
	if (reservation.UpstreamIdempotencyKeyDigest == nil) != (request.UpstreamIdempotencyKeyDigest == nil) {
		return false
	}
	return reservation.UpstreamIdempotencyKeyDigest == nil || *reservation.UpstreamIdempotencyKeyDigest == *request.UpstreamIdempotencyKeyDigest
}

var _ ports.AttemptEffectStoreV1 = (*Store)(nil)
