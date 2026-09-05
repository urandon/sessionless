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
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var ErrInvalidPreparedInvocation = errors.New("invalid prepared invocation")

type Clock func() time.Time

type CapabilityIssuer struct {
	key      [32]byte
	clock    Clock
	mu       sync.Mutex
	consumed map[domain.AttemptEffectReservationDigestV1]struct{}
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
	issuer := &CapabilityIssuer{clock: clock, consumed: make(map[domain.AttemptEffectReservationDigestV1]struct{})}
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
	grant ports.AttemptEffectOwnershipGrantV1,
	allocation domain.PreparedAllocationV1,
) (PreparedInvocation, error) {
	if issuer == nil || issuer.clock == nil {
		return PreparedInvocation{}, ErrInvalidPreparedInvocation
	}
	now := issuer.clock().UTC()
	if err := issuer.verifyGrant(grant, now); err != nil {
		return PreparedInvocation{}, ErrInvalidPreparedInvocation
	}
	authority := grant.Authority.Clone()
	reservation := grant.Reservation.Clone()
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
		issuedAt: now, executeDeadline: grant.GrantExpiresAt.UTC(),
	}
	prepared.authenticator = issuer.authenticate(prepared)
	return prepared, nil
}

func (issuer *CapabilityIssuer) MintAttemptEffectOwnershipGrant(
	authority domain.ServerlessInvocationAuthorityV1,
	reservation domain.AttemptEffectReservationV1,
	expiresAt time.Time,
) (ports.AttemptEffectOwnershipGrantV1, error) {
	if issuer == nil || issuer.clock == nil || authority.ValidateAt(issuer.clock().UTC()) != nil || reservation.ValidateForAuthority(authority) != nil {
		return ports.AttemptEffectOwnershipGrantV1{}, ErrInvalidPreparedInvocation
	}
	grant := ports.AttemptEffectOwnershipGrantV1{
		Version: ports.AttemptEffectOwnershipGrantVersionV1, Authority: authority.Clone(), Reservation: reservation.Clone(),
		GrantExpiresAt: expiresAt.UTC(), Authenticator: make([]byte, sha256.Size),
	}
	grant.Authenticator = issuer.authenticateGrant(grant)
	if err := grant.Validate(); err != nil {
		return ports.AttemptEffectOwnershipGrantV1{}, ErrInvalidPreparedInvocation
	}
	return grant.Clone(), nil
}

func (issuer *CapabilityIssuer) MintAttemptEffectObservationGrant(
	authority domain.ServerlessInvocationAuthorityV1,
	reservation domain.AttemptEffectReservationV1,
	issuedAt time.Time,
	expiresAt time.Time,
) (ports.AttemptEffectObservationGrantV1, error) {
	if issuer == nil || issuer.clock == nil || authority.Validate() != nil || reservation.ValidateForAuthority(authority) != nil {
		return ports.AttemptEffectObservationGrantV1{}, ErrInvalidPreparedInvocation
	}
	issuedAt = issuedAt.UTC()
	expiresAt = expiresAt.UTC()
	now := issuer.clock().UTC()
	if now.Before(issuedAt) || !now.Before(expiresAt) {
		return ports.AttemptEffectObservationGrantV1{}, ErrInvalidPreparedInvocation
	}
	grant := ports.AttemptEffectObservationGrantV1{
		Version: ports.AttemptEffectObservationGrantVersionV1, Authority: authority.Clone(), Reservation: reservation.Clone(),
		GrantIssuedAt: issuedAt, GrantExpiresAt: expiresAt, Authenticator: make([]byte, sha256.Size),
	}
	grant.Authenticator = issuer.authenticateObservationGrant(grant)
	if err := grant.Validate(); err != nil {
		return ports.AttemptEffectObservationGrantV1{}, ErrInvalidPreparedInvocation
	}
	return grant.Clone(), nil
}

func (issuer *CapabilityIssuer) verifyGrant(grant ports.AttemptEffectOwnershipGrantV1, now time.Time) error {
	if issuer == nil || issuer.clock == nil || grant.Validate() != nil || now.Before(grant.Reservation.ReservedAt) || !now.Before(grant.GrantExpiresAt) {
		return ErrInvalidPreparedInvocation
	}
	expected := issuer.authenticateGrant(grant)
	if subtle.ConstantTimeCompare(expected, grant.Authenticator) != 1 || grant.Authority.ValidateAt(now) != nil {
		return ErrInvalidPreparedInvocation
	}
	return nil
}

// VerifyGrant authenticates a durable reservation result before any substrate
// preflight is delegated. It does not issue or consume an execution capability.
func (issuer *CapabilityIssuer) VerifyGrant(grant ports.AttemptEffectOwnershipGrantV1) error {
	if issuer == nil || issuer.clock == nil {
		return ErrInvalidPreparedInvocation
	}
	return issuer.verifyGrant(grant, issuer.clock().UTC())
}

// VerifyObservationGrant authenticates read-only authority for a historical
// physical invocation. It deliberately uses structural authority validation:
// reconciliation remains possible after the original execution window closes.
func (issuer *CapabilityIssuer) VerifyObservationGrant(grant ports.AttemptEffectObservationGrantV1) error {
	if issuer == nil || issuer.clock == nil || grant.Validate() != nil {
		return ErrInvalidPreparedInvocation
	}
	now := issuer.clock().UTC()
	if now.Before(grant.GrantIssuedAt) || !now.Before(grant.GrantExpiresAt) {
		return ErrInvalidPreparedInvocation
	}
	expected := issuer.authenticateObservationGrant(grant)
	if subtle.ConstantTimeCompare(expected, grant.Authenticator) != 1 {
		return ErrInvalidPreparedInvocation
	}
	return nil
}

// Consume atomically burns a prepared provider-turn capability before the
// caller may start a process, network request, or other provider effect.
func (issuer *CapabilityIssuer) Consume(prepared PreparedInvocation) error {
	if err := issuer.Validate(prepared); err != nil {
		return err
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	if _, exists := issuer.consumed[prepared.reservationDigest]; exists {
		return ErrInvalidPreparedInvocation
	}
	issuer.consumed[prepared.reservationDigest] = struct{}{}
	return nil
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

func (prepared PreparedInvocation) Digest() domain.PreparedInvocationDigestV1 {
	digest := sha256.Sum256(prepared.authenticator[:])
	return domain.PreparedInvocationDigestV1(hex.EncodeToString(digest[:]))
}

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

func (issuer *CapabilityIssuer) authenticateGrant(grant ports.AttemptEffectOwnershipGrantV1) []byte {
	mac := hmac.New(sha256.New, issuer.key[:])
	writeFrame(mac, "sessionless.attempt-effect-ownership-grant.v1")
	authorityDigest, _ := grant.Authority.Digest()
	reservationDigest, _ := grant.Reservation.DigestForAuthority(grant.Authority)
	writeFrame(mac, string(authorityDigest))
	writeFrame(mac, string(reservationDigest))
	writeFrame(mac, grant.Reservation.PhysicalInvocationClaimID)
	writeInstant(mac, grant.GrantExpiresAt)
	return mac.Sum(nil)
}

func (issuer *CapabilityIssuer) authenticateObservationGrant(grant ports.AttemptEffectObservationGrantV1) []byte {
	mac := hmac.New(sha256.New, issuer.key[:])
	writeFrame(mac, "sessionless.attempt-effect-observation-grant.v1")
	authorityDigest, _ := grant.Authority.Digest()
	reservationDigest, _ := grant.Reservation.DigestForAuthority(grant.Authority)
	writeFrame(mac, string(authorityDigest))
	writeFrame(mac, string(reservationDigest))
	writeFrame(mac, grant.Reservation.PhysicalInvocationClaimID)
	writeInstant(mac, grant.GrantIssuedAt)
	writeInstant(mac, grant.GrantExpiresAt)
	return mac.Sum(nil)
}

var _ ports.AttemptEffectOwnershipGrantIssuerV1 = (*CapabilityIssuer)(nil)

func writeFrame(writer io.Writer, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = io.WriteString(writer, value)
}

func writeInstant(writer io.Writer, value time.Time) {
	writeFrame(writer, value.UTC().Format(time.RFC3339Nano))
}
