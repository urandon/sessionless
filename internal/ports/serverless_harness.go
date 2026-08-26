package ports

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const AttemptEffectOwnershipGrantVersionV1 uint32 = 1

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
	Reservation domain.AttemptEffectReservationV1
	Grant       *AttemptEffectOwnershipGrantV1
}

func (result ReserveAttemptEffectResultV1) Validate() error {
	if !result.Status.Valid() {
		return domain.ValidationError{Field: "reserve_attempt_effect.status", Reason: "is unsupported"}
	}
	if err := result.Reservation.Validate(); err != nil {
		return err
	}
	if result.Status == AttemptEffectReconcileOnlyV1 {
		if result.Grant != nil {
			return domain.ValidationError{Field: "reserve_attempt_effect.grant", Reason: "must be absent for reconcile-only"}
		}
		return nil
	}
	if result.Grant == nil {
		return domain.ValidationError{Field: "reserve_attempt_effect.grant", Reason: "is required for an owned effect"}
	}
	if err := result.Grant.Validate(); err != nil {
		return err
	}
	if result.Grant.Reservation != result.Reservation {
		return domain.ValidationError{Field: "reserve_attempt_effect.grant", Reason: "must bind the returned reservation"}
	}
	return nil
}

// AttemptEffectOwnershipGrantV1 is a public-shaped but MAC-authenticated
// process-local grant. Its fields are not authority without verification by
// the exact issuer. Reconcile-only results never contain one.
type AttemptEffectOwnershipGrantV1 struct {
	Version        uint32
	Authority      domain.ServerlessInvocationAuthorityV1
	Reservation    domain.AttemptEffectReservationV1
	GrantExpiresAt time.Time
	Authenticator  []byte
}

func (grant AttemptEffectOwnershipGrantV1) Clone() AttemptEffectOwnershipGrantV1 {
	clone := grant
	clone.Authority = grant.Authority.Clone()
	clone.Reservation = grant.Reservation.Clone()
	clone.Authenticator = append([]byte(nil), grant.Authenticator...)
	return clone
}

func (grant AttemptEffectOwnershipGrantV1) Validate() error {
	if grant.Version != AttemptEffectOwnershipGrantVersionV1 {
		return domain.ValidationError{Field: "attempt_effect_ownership_grant.version", Reason: "must equal 1"}
	}
	if err := grant.Reservation.ValidateForAuthority(grant.Authority); err != nil {
		return err
	}
	if grant.GrantExpiresAt.IsZero() || !grant.GrantExpiresAt.After(grant.Reservation.ReservedAt) || grant.GrantExpiresAt.After(grant.Authority.InvocationDeadline) {
		return domain.ValidationError{Field: "attempt_effect_ownership_grant.expires_at", Reason: "must fit inside the invocation authority"}
	}
	if len(grant.Authenticator) != 32 {
		return domain.ValidationError{Field: "attempt_effect_ownership_grant.authenticator", Reason: "must be a process-local MAC"}
	}
	return nil
}

type AttemptEffectOwnershipGrantIssuerV1 interface {
	MintAttemptEffectOwnershipGrant(domain.ServerlessInvocationAuthorityV1, domain.AttemptEffectReservationV1, time.Time) (AttemptEffectOwnershipGrantV1, error)
}

type AttemptEffectStoreV1 interface {
	ReserveAttemptEffect(context.Context, ReserveAttemptEffectRequestV1) (ReserveAttemptEffectResultV1, error)
}
