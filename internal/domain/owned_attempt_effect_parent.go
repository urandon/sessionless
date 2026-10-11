package domain

import (
	"bytes"
	"time"
)

const OwnedAttemptEffectParentVersionV1 uint32 = 1

// OwnedAttemptEffectParentV1 preserves the sole outer physical claim and the
// exact original Final reference. It is not a capability, source approval, a
// fresh permission or proof that preparation/registration actually happened.
// The original Final must be independently retained; duplicating its Source
// here would introduce another mutable authority and exceed the bounded head.
type OwnedAttemptEffectParentV1 struct {
	Version              uint32                          `json:"version"`
	Scope                AttemptExecutionSourceScopeV1   `json:"scope"`
	AdmissionDigest      string                          `json:"admission_digest"`
	Authority            ServerlessInvocationAuthorityV1 `json:"authority"`
	Reservation          AttemptEffectReservationV1      `json:"reservation"`
	PreparationExpiresAt time.Time                       `json:"preparation_expires_at"`
}

func (v OwnedAttemptEffectParentV1) Clone() OwnedAttemptEffectParentV1 {
	v.Authority = v.Authority.Clone()
	v.Authority.Lease.AcquiredAt, v.Authority.Lease.ExpiresAt = v.Authority.Lease.AcquiredAt.UTC(), v.Authority.Lease.ExpiresAt.UTC()
	v.Authority.SubstrateBinding.ProfileEvidenceExpiresAt = v.Authority.SubstrateBinding.ProfileEvidenceExpiresAt.UTC()
	v.Authority.InvocationDeadline = v.Authority.InvocationDeadline.UTC()
	v.Reservation = v.Reservation.Clone()
	v.Reservation.ReservedAt, v.PreparationExpiresAt = v.Reservation.ReservedAt.UTC(), v.PreparationExpiresAt.UTC()
	return v
}

func (v OwnedAttemptEffectParentV1) Validate() error {
	a, r := v.Authority, v.Reservation
	if v.Version != OwnedAttemptEffectParentVersionV1 || v.Scope.Validate() != nil || ValidateSHA256Digest("owned_attempt_effect_parent.admission_digest", v.AdmissionDigest) != nil ||
		a.HarnessBinding.Profile != HarnessExecutionOwnedAgentV2 || a.HarnessBinding.AgentLoopPolicy == nil || a.ValidateAt(r.ReservedAt) != nil || r.ValidateForAuthority(a) != nil ||
		!ownedParentTimes(a, r.ReservedAt, v.PreparationExpiresAt) ||
		v.Scope.TenantID != a.Lease.TenantID || v.Scope.RunID != a.Lease.RunID || v.Scope.AttemptID != a.Lease.AttemptID || v.Scope.OwnerUserID != a.HarnessBinding.OwnerUserID ||
		!v.PreparationExpiresAt.After(r.ReservedAt) || v.PreparationExpiresAt.After(ownedParentPreparationExpiry(a, r.ReservedAt)) {
		return ownedParentError("requires bounded exact original owned authority and sole reservation")
	}
	return nil
}

// ValidateForAdmission correlates retained historical bytes. Current source,
// lease/fence, Session rights and held quota must still be joined atomically by
// the Store before any new effect; a historical match grants no new operation.
func (v OwnedAttemptEffectParentV1) ValidateForAdmission(final AttemptExecutionAdmissionV1) error {
	a := v.Authority
	if v.Validate() != nil || final.ValidateForExecution(a.ExecutionPlacementV2, a.HarnessBinding, a.SubstrateBinding, a.AdmissionCostCeiling) != nil ||
		v.Scope != final.Source.Scope || v.Reservation.ReservedAt.Before(final.AdmittedAt) || a.InvocationDeadline.After(final.Source.ExpiresAt) {
		return ownedParentError("requires exact original Final scope, projection and window")
	}
	d, err := final.Digest()
	expires := ownedParentPreparationExpiry(a, v.Reservation.ReservedAt)
	if final.Source.ExpiresAt.Before(expires) {
		expires = final.Source.ExpiresAt
	}
	if err != nil || v.AdmissionDigest != d || !v.PreparationExpiresAt.Equal(expires) {
		return ownedParentError("requires exact Final digest and original preparation deadline")
	}
	return nil
}

func BuildOwnedAttemptEffectParentV1(final AttemptExecutionAdmissionV1, authority ServerlessInvocationAuthorityV1, claim string, upstream *string, at time.Time) (OwnedAttemptEffectParentV1, error) {
	if !ownedCurrentTime(at) || authority.ValidateAt(at) != nil {
		return OwnedAttemptEffectParentV1{}, ownedParentError("requires finite microsecond reservation time and active original authority")
	}
	r, err := BuildAttemptEffectReservationV1(authority, claim, upstream, at)
	if err != nil {
		return OwnedAttemptEffectParentV1{}, err
	}
	d, err := final.Digest()
	if err != nil {
		return OwnedAttemptEffectParentV1{}, err
	}
	expires := ownedParentPreparationExpiry(authority, at)
	if final.Source.ExpiresAt.Before(expires) {
		expires = final.Source.ExpiresAt
	}
	v := OwnedAttemptEffectParentV1{Version: 1, Scope: final.Source.Scope, AdmissionDigest: d, Authority: authority.Clone(), Reservation: r, PreparationExpiresAt: expires.UTC()}
	if err := v.ValidateForAdmission(final); err != nil {
		return OwnedAttemptEffectParentV1{}, err
	}
	if _, err := EncodeOwnedAttemptEffectParentV1(v); err != nil {
		return OwnedAttemptEffectParentV1{}, err
	}
	return v.Clone(), nil
}

func (v OwnedAttemptEffectParentV1) Digest() (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return ownedCurrentDigest("sessionless.owned-attempt-effect-parent.v1", v.Clone())
}

func EncodeOwnedAttemptEffectParentV1(v OwnedAttemptEffectParentV1) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return encodeOwnedCurrent(v.Clone())
}

func DecodeOwnedAttemptEffectParentV1(raw []byte) (OwnedAttemptEffectParentV1, error) {
	var v OwnedAttemptEffectParentV1
	if err := decodeOwnedCurrent(raw, &v); err != nil {
		return OwnedAttemptEffectParentV1{}, err
	}
	expected, err := EncodeOwnedAttemptEffectParentV1(v)
	if err != nil || !bytes.Equal(raw, expected) {
		return OwnedAttemptEffectParentV1{}, ownedParentError("requires exact bounded canonical producer bytes")
	}
	return v.Clone(), nil
}

func ownedParentPreparationExpiry(a ServerlessInvocationAuthorityV1, at time.Time) time.Time {
	expires := at.Add(a.AdmissionCostCeiling.MaxPreEffectDurationPerDelivery)
	for _, limit := range []time.Time{a.InvocationDeadline, a.SubstrateBinding.ProfileEvidenceExpiresAt, a.AdmissionCostCeiling.PriceExpiresAt} {
		if limit.Before(expires) {
			expires = limit
		}
	}
	if p := a.HarnessBinding.AgentLoopPolicy; p != nil && p.ExpiresAt.Before(expires) {
		expires = p.ExpiresAt
	}
	if limit := a.HarnessBinding.EvidenceExpiresAt; limit != nil && limit.Before(expires) {
		expires = *limit
	}
	for _, service := range []*ExternalServiceCostCeilingV1{a.AdmissionCostCeiling.MCPToolCost, a.AdmissionCostCeiling.WebSearchCost} {
		if service != nil && service.PriceExpiresAt.Before(expires) {
			expires = service.PriceExpiresAt
		}
	}
	return expires
}

func ownedParentTimes(a ServerlessInvocationAuthorityV1, at, expires time.Time) bool {
	for _, value := range []time.Time{at, expires, a.InvocationDeadline, a.Lease.AcquiredAt, a.Lease.ExpiresAt, a.SubstrateBinding.ProfileEvidenceExpiresAt} {
		if !ownedCurrentTime(value) {
			return false
		}
	}
	if value := a.HarnessBinding.EvidenceExpiresAt; value != nil && !ownedCurrentTime(*value) {
		return false
	}
	costs := []AdmissionCostCeilingV2{a.AdmissionCostCeiling}
	if p := a.HarnessBinding.AgentLoopPolicy; p != nil {
		if !ownedCurrentTime(p.IssuedAt) || !ownedCurrentTime(p.ExpiresAt) {
			return false
		}
		costs = append(costs, p.AdmissionCostCeiling)
	}
	for _, cost := range costs {
		if !ownedCurrentTime(cost.PriceObservedAt) || !ownedCurrentTime(cost.PriceExpiresAt) {
			return false
		}
		for _, service := range []*ExternalServiceCostCeilingV1{cost.MCPToolCost, cost.WebSearchCost} {
			if service != nil && (!ownedCurrentTime(service.PriceObservedAt) || !ownedCurrentTime(service.PriceExpiresAt)) {
				return false
			}
		}
	}
	return true
}

func ownedParentError(reason string) error {
	return ValidationError{Field: "owned_attempt_effect_parent", Reason: reason}
}
