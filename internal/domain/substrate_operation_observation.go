package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

const AttemptEffectReconciliationEvidenceVersionV1 uint32 = 1

type (
	SubstrateOperationStateV1                   string
	AttemptEffectReconciliationEvidenceDigestV1 string
)

const (
	SubstrateOperationNotFoundV1     SubstrateOperationStateV1 = "not_found"
	SubstrateOperationObservedV1     SubstrateOperationStateV1 = "observed"
	SubstrateOperationAcknowledgedV1 SubstrateOperationStateV1 = "acknowledged"
	SubstrateOperationUnknownV1      SubstrateOperationStateV1 = "unknown"
)

func (state SubstrateOperationStateV1) Valid() bool {
	return state == SubstrateOperationNotFoundV1 || state == SubstrateOperationObservedV1 ||
		state == SubstrateOperationAcknowledgedV1 || state == SubstrateOperationUnknownV1
}

type SubstrateOperationObservationV1 struct {
	State                SubstrateOperationStateV1
	InvocationAuthority  ServerlessInvocationAuthorityDigestV1
	SubstrateBinding     SubstrateBindingDigestV1
	PhysicalInvocationID string
	ObservedAt           time.Time
}

func (value SubstrateOperationObservationV1) ValidateForAuthority(authority ServerlessInvocationAuthorityV1) error {
	if !value.State.Valid() {
		return ValidationError{Field: "substrate_operation.state", Reason: "is unsupported"}
	}
	authorityDigest, err := authority.Digest()
	if err != nil {
		return err
	}
	substrateDigest, _ := authority.SubstrateBinding.Digest()
	if value.InvocationAuthority != authorityDigest || value.SubstrateBinding != substrateDigest {
		return ValidationError{Field: "substrate_operation.authority", Reason: "must exact-match the invocation authority"}
	}
	if value.PhysicalInvocationID != "" {
		if err := ValidateOpaqueID("substrate_operation.physical_invocation_id", value.PhysicalInvocationID); err != nil {
			return err
		}
	}
	if value.ObservedAt.IsZero() {
		return ValidationError{Field: "substrate_operation.observed_at", Reason: "must not be zero"}
	}
	return nil
}

type AttemptEffectReconciliationEvidenceV1 struct {
	Version                   uint32
	InvocationAuthorityDigest ServerlessInvocationAuthorityDigestV1
	EffectReservationDigest   AttemptEffectReservationDigestV1
	PhysicalInvocationClaimID string
	Observation               SubstrateOperationObservationV1
	EvidenceDigest            AttemptEffectReconciliationEvidenceDigestV1
}

func (value AttemptEffectReconciliationEvidenceV1) Clone() AttemptEffectReconciliationEvidenceV1 {
	return value
}

func (value AttemptEffectReconciliationEvidenceV1) ValidateForPersistedAuthority(
	authority ServerlessInvocationAuthorityV1,
	reservation AttemptEffectReservationV1,
) error {
	if value.Version != AttemptEffectReconciliationEvidenceVersionV1 {
		return ValidationError{Field: "attempt_effect_reconciliation_evidence.version", Reason: "must equal 1"}
	}
	if err := reservation.ValidateForAuthority(authority); err != nil {
		return err
	}
	authorityDigest, _ := authority.Digest()
	reservationDigest, _ := reservation.DigestForAuthority(authority)
	if value.InvocationAuthorityDigest != authorityDigest || value.EffectReservationDigest != reservationDigest ||
		value.PhysicalInvocationClaimID != reservation.PhysicalInvocationClaimID {
		return ValidationError{Field: "attempt_effect_reconciliation_evidence.authority", Reason: "must exact-match persisted authority, reservation, and physical claim"}
	}
	if err := value.Observation.ValidateForAuthority(authority); err != nil {
		return err
	}
	if err := value.EvidenceDigest.Validate(); err != nil {
		return err
	}
	expected, err := value.digest()
	if err != nil {
		return err
	}
	if value.EvidenceDigest != expected {
		return ValidationError{Field: "attempt_effect_reconciliation_evidence.evidence_digest", Reason: "does not match canonical evidence"}
	}
	return nil
}

func (value AttemptEffectReconciliationEvidenceDigestV1) Validate() error {
	return ValidateSHA256Digest("attempt_effect_reconciliation_evidence.evidence_digest", string(value))
}

func SealAttemptEffectReconciliationEvidenceV1(
	authority ServerlessInvocationAuthorityV1,
	reservation AttemptEffectReservationV1,
	observation SubstrateOperationObservationV1,
) (AttemptEffectReconciliationEvidenceV1, error) {
	if err := reservation.ValidateForAuthority(authority); err != nil {
		return AttemptEffectReconciliationEvidenceV1{}, err
	}
	if err := observation.ValidateForAuthority(authority); err != nil {
		return AttemptEffectReconciliationEvidenceV1{}, err
	}
	authorityDigest, _ := authority.Digest()
	reservationDigest, _ := reservation.DigestForAuthority(authority)
	value := AttemptEffectReconciliationEvidenceV1{
		Version:                   AttemptEffectReconciliationEvidenceVersionV1,
		InvocationAuthorityDigest: authorityDigest, EffectReservationDigest: reservationDigest,
		PhysicalInvocationClaimID: reservation.PhysicalInvocationClaimID, Observation: observation,
	}
	digest, err := value.digest()
	if err != nil {
		return AttemptEffectReconciliationEvidenceV1{}, err
	}
	value.EvidenceDigest = digest
	return value, value.ValidateForPersistedAuthority(authority, reservation)
}

func (value AttemptEffectReconciliationEvidenceV1) digest() (AttemptEffectReconciliationEvidenceDigestV1, error) {
	copy := value
	copy.EvidenceDigest = ""
	encoded, err := json.Marshal(copy)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return AttemptEffectReconciliationEvidenceDigestV1(hex.EncodeToString(digest[:])), nil
}
