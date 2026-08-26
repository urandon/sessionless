// Package serverlessharness owns the non-durable execution capability that
// joins durable authority to one attested physical allocation. It is not a
// scheduler, provider router, or second attempt state machine.
package serverlessharness

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

var ErrInvalidPreparedInvocation = errors.New("invalid prepared invocation")

type Clock func() time.Time

type CapabilityIssuer struct {
	key   [32]byte
	clock Clock
}

// PreparedInvocation has no exported fields and no JSON representation. A
// queue payload can copy its type but cannot construct a valid authenticator.
type PreparedInvocation struct {
	authority         domain.ServerlessInvocationAuthorityV1
	reservation       domain.AttemptEffectReservationV1
	allocation        domain.PreparedAllocationV1
	authorityDigest   domain.ServerlessInvocationAuthorityDigestV1
	reservationDigest domain.AttemptEffectReservationDigestV1
	allocationDigest  domain.PreparedAllocationDigestV1
	costDigest        domain.AdmissionCostCeilingDigestV1
	issuedAt          time.Time
	executeDeadline   time.Time
	authenticator     [32]byte
}

func NewCapabilityIssuer(clock Clock, entropy io.Reader) (*CapabilityIssuer, error) {
	if clock == nil {
		return nil, ErrInvalidPreparedInvocation
	}
	if entropy == nil {
		entropy = rand.Reader
	}
	issuer := &CapabilityIssuer{clock: clock}
	if _, err := io.ReadFull(entropy, issuer.key[:]); err != nil {
		return nil, ErrInvalidPreparedInvocation
	}
	var zero [32]byte
	if subtle.ConstantTimeCompare(issuer.key[:], zero[:]) == 1 {
		return nil, ErrInvalidPreparedInvocation
	}
	return issuer, nil
}

func (issuer *CapabilityIssuer) Issue(
	authority domain.ServerlessInvocationAuthorityV1,
	reservation domain.AttemptEffectReservationV1,
	allocation domain.PreparedAllocationV1,
) (PreparedInvocation, error) {
	if issuer == nil || issuer.clock == nil {
		return PreparedInvocation{}, ErrInvalidPreparedInvocation
	}
	now := issuer.clock().UTC()
	if err := authority.ValidateAt(now); err != nil {
		return PreparedInvocation{}, ErrInvalidPreparedInvocation
	}
	if err := reservation.ValidateForAuthority(authority); err != nil || reservation.PhysicalInvocationClaimID == "" || now.Before(reservation.ReservedAt) {
		return PreparedInvocation{}, ErrInvalidPreparedInvocation
	}
	if err := allocation.ValidateForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend); err != nil {
		return PreparedInvocation{}, ErrInvalidPreparedInvocation
	}
	authorityDigest, _ := authority.Digest()
	reservationDigest, _ := reservation.DigestForAuthority(authority)
	allocationDigest, _ := allocation.DigestForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend)
	costDigest, _ := authority.AdmissionCostCeiling.Digest()
	prepared := PreparedInvocation{
		authority: authority.Clone(), reservation: reservation.Clone(), allocation: allocation.Clone(),
		authorityDigest: authorityDigest, reservationDigest: reservationDigest,
		allocationDigest: allocationDigest, costDigest: costDigest,
		issuedAt: now, executeDeadline: authority.InvocationDeadline.UTC(),
	}
	prepared.authenticator = issuer.authenticate(prepared)
	return prepared, nil
}

func (issuer *CapabilityIssuer) Validate(prepared PreparedInvocation) error {
	if issuer == nil || issuer.clock == nil {
		return ErrInvalidPreparedInvocation
	}
	expected := issuer.authenticate(prepared)
	if subtle.ConstantTimeCompare(expected[:], prepared.authenticator[:]) != 1 {
		return ErrInvalidPreparedInvocation
	}
	now := issuer.clock().UTC()
	if prepared.issuedAt.IsZero() || prepared.executeDeadline.IsZero() || now.Before(prepared.issuedAt) || !now.Before(prepared.executeDeadline) {
		return ErrInvalidPreparedInvocation
	}
	if err := prepared.authority.ValidateAt(now); err != nil {
		return ErrInvalidPreparedInvocation
	}
	if err := prepared.reservation.ValidateForAuthority(prepared.authority); err != nil {
		return ErrInvalidPreparedInvocation
	}
	if err := prepared.allocation.ValidateForBinding(prepared.authority.SubstrateBinding, prepared.authority.HarnessBinding.Backend); err != nil {
		return ErrInvalidPreparedInvocation
	}
	authorityDigest, _ := prepared.authority.Digest()
	reservationDigest, _ := prepared.reservation.DigestForAuthority(prepared.authority)
	allocationDigest, _ := prepared.allocation.DigestForBinding(prepared.authority.SubstrateBinding, prepared.authority.HarnessBinding.Backend)
	costDigest, _ := prepared.authority.AdmissionCostCeiling.Digest()
	if authorityDigest != prepared.authorityDigest || reservationDigest != prepared.reservationDigest || allocationDigest != prepared.allocationDigest || costDigest != prepared.costDigest {
		return ErrInvalidPreparedInvocation
	}
	return nil
}

func (prepared PreparedInvocation) Authority() domain.ServerlessInvocationAuthorityV1 {
	return prepared.authority.Clone()
}

func (prepared PreparedInvocation) Reservation() domain.AttemptEffectReservationV1 {
	return prepared.reservation.Clone()
}

func (prepared PreparedInvocation) Allocation() domain.PreparedAllocationV1 {
	return prepared.allocation.Clone()
}

func (prepared PreparedInvocation) ExecuteDeadline() time.Time { return prepared.executeDeadline }

func (issuer *CapabilityIssuer) authenticate(prepared PreparedInvocation) [32]byte {
	mac := hmac.New(sha256.New, issuer.key[:])
	writeFrame(mac, "sessionless.prepared-invocation-capability.v1")
	writeFrame(mac, string(prepared.authorityDigest))
	writeFrame(mac, string(prepared.reservationDigest))
	writeFrame(mac, string(prepared.allocationDigest))
	writeFrame(mac, string(prepared.costDigest))
	writeFrame(mac, prepared.reservation.PhysicalInvocationClaimID)
	writeInstant(mac, prepared.issuedAt)
	writeInstant(mac, prepared.executeDeadline)
	var result [32]byte
	copy(result[:], mac.Sum(nil))
	return result
}

func writeFrame(writer io.Writer, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = io.WriteString(writer, value)
}

func writeInstant(writer io.Writer, value time.Time) {
	writeFrame(writer, value.UTC().Format(time.RFC3339Nano))
}
