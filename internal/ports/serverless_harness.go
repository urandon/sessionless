package ports

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type AttemptEffectReservationStatusV1 string

const (
	AttemptEffectOwnedV1         AttemptEffectReservationStatusV1 = "owned"
	AttemptEffectReplayedV1      AttemptEffectReservationStatusV1 = "replayed"
	AttemptEffectReconcileOnlyV1 AttemptEffectReservationStatusV1 = "reconcile_only"
)

func (status AttemptEffectReservationStatusV1) Valid() bool {
	return status == AttemptEffectOwnedV1 || status == AttemptEffectReplayedV1 || status == AttemptEffectReconcileOnlyV1
}

// ReserveAttemptEffectRequestV1 contains locators and a fresh server-generated
// physical claim only. The store reconstructs all execution authority from
// canonical rows and database time inside one transaction.
type ReserveAttemptEffectRequestV1 struct {
	TenantID                     domain.TenantID
	RunID                        domain.RunID
	AttemptID                    domain.AttemptID
	LeaseID                      domain.LeaseID
	FenceToken                   uint64
	PhysicalInvocationClaimID    string
	UpstreamIdempotencyKeyDigest *string
}

func (request ReserveAttemptEffectRequestV1) Validate() error {
	for _, validate := range []func() error{request.TenantID.Validate, request.RunID.Validate, request.AttemptID.Validate, request.LeaseID.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if request.FenceToken == 0 {
		return domain.ValidationError{Field: "reserve_attempt_effect.fence_token", Reason: "must be positive"}
	}
	if err := domain.ValidateOpaqueID("reserve_attempt_effect.physical_invocation_claim_id", request.PhysicalInvocationClaimID); err != nil {
		return err
	}
	if request.UpstreamIdempotencyKeyDigest != nil {
		if err := domain.ValidateSHA256Digest("reserve_attempt_effect.upstream_idempotency_key_digest", *request.UpstreamIdempotencyKeyDigest); err != nil {
			return err
		}
	}
	return nil
}

type ReserveAttemptEffectResultV1 struct {
	Status      AttemptEffectReservationStatusV1
	Authority   domain.ServerlessInvocationAuthorityV1
	Reservation domain.AttemptEffectReservationV1
}

func (result ReserveAttemptEffectResultV1) Validate() error {
	if !result.Status.Valid() {
		return domain.ValidationError{Field: "reserve_attempt_effect.status", Reason: "is unsupported"}
	}
	if err := result.Authority.Validate(); err != nil {
		return err
	}
	return result.Reservation.ValidateForAuthority(result.Authority)
}

type AttemptEffectStoreV1 interface {
	ReserveAttemptEffect(context.Context, ReserveAttemptEffectRequestV1) (ReserveAttemptEffectResultV1, error)
}
