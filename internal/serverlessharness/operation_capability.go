package serverlessharness

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var ErrInvalidAttemptOperationCapability = errors.New("invalid attempt operation capability")

// OperationCapabilityIssuer authenticates process-local subordinate operations.
// It is not durable admission or a second physical-claim authority. The store
// must mint only after its first confirmed fresh reservation commit and recheck
// durable authority immediately before send; this issuer cannot prove either.
type OperationCapabilityIssuer struct {
	key      [sha256.Size]byte
	clock    Clock
	capacity int
	mu       sync.Mutex
	lastTime time.Time
	entries  map[domain.AttemptOperationReservationDigestV1]operationCapabilityEntry
}

type operationCapabilityEntry struct {
	consumed      bool
	authenticator [sha256.Size]byte
	retainUntil   time.Time
}

// Capacity bounds outstanding permissions and consumed tombstones together.
// Historical observation grants/proofs are stateless and never grant a send.
func NewOperationCapabilityIssuer(clock Clock, entropy io.Reader, capacity int) (*OperationCapabilityIssuer, error) {
	if clock == nil || capacity <= 0 {
		return nil, ErrInvalidAttemptOperationCapability
	}
	if entropy == nil {
		entropy = rand.Reader
	}
	issuer := &OperationCapabilityIssuer{clock: clock, capacity: capacity, entries: make(map[domain.AttemptOperationReservationDigestV1]operationCapabilityEntry)}
	if _, err := io.ReadFull(entropy, issuer.key[:]); err != nil {
		return nil, ErrInvalidAttemptOperationCapability
	}
	var zero [sha256.Size]byte
	if subtle.ConstantTimeCompare(issuer.key[:], zero[:]) == 1 {
		return nil, ErrInvalidAttemptOperationCapability
	}
	return issuer, nil
}

// timeLocked rejects backward time without lowering the high-water mark or
// pruning state. Once a tombstone is pruned, its immutable send deadline is
// already past this mark and cannot become live again through a clock rewind.
func (issuer *OperationCapabilityIssuer) timeLocked() (time.Time, error) {
	now := issuer.clock().UTC()
	if now.IsZero() || now.Year() < 1 || now.Year() > 9999 || now.Before(issuer.lastTime) {
		return time.Time{}, ErrInvalidAttemptOperationCapability
	}
	issuer.lastTime = now
	return now, nil
}

func (issuer *OperationCapabilityIssuer) usable() bool {
	if issuer == nil || issuer.clock == nil || issuer.capacity <= 0 || issuer.entries == nil {
		return false
	}
	var zero [sha256.Size]byte
	return subtle.ConstantTimeCompare(issuer.key[:], zero[:]) != 1
}

func (issuer *OperationCapabilityIssuer) MintAttemptOperationCapability(r domain.AttemptOperationReservationV1, issuedAt, expiresAt time.Time) (ports.AttemptOperationCapabilityV1, error) {
	if !issuer.usable() {
		return ports.AttemptOperationCapabilityV1{}, ErrInvalidAttemptOperationCapability
	}
	v := ports.AttemptOperationCapabilityV1{Version: 1, Reservation: r.Clone(), IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC(), Authenticator: make([]byte, sha256.Size)}
	if v.Validate() != nil || r.Operation.Deadline.Add(r.Operation.CleanupDuration).Year() > 9999 {
		return ports.AttemptOperationCapabilityV1{}, ErrInvalidAttemptOperationCapability
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	now, err := issuer.timeLocked()
	if err != nil || now.Before(v.IssuedAt) || !now.Before(v.ExpiresAt) {
		return ports.AttemptOperationCapabilityV1{}, ErrInvalidAttemptOperationCapability
	}
	for digest, entry := range issuer.entries {
		if !now.Before(entry.retainUntil) {
			delete(issuer.entries, digest)
		}
	}
	digest, _ := v.Reservation.Digest()
	if _, exists := issuer.entries[digest]; exists || len(issuer.entries) >= issuer.capacity {
		return ports.AttemptOperationCapabilityV1{}, ErrInvalidAttemptOperationCapability
	}
	v.Authenticator = issuer.operationMAC("sessionless.attempt-operation-send-capability.v1", v.Version, digest, v.IssuedAt, v.ExpiresAt, "")
	entry := operationCapabilityEntry{retainUntil: r.Operation.Deadline.Add(r.Operation.CleanupDuration)}
	copy(entry.authenticator[:], v.Authenticator)
	issuer.entries[digest] = entry
	return v.Clone(), nil
}

func (issuer *OperationCapabilityIssuer) ConsumeAttemptOperationCapability(v ports.AttemptOperationCapabilityV1) error {
	if !issuer.usable() || v.Validate() != nil {
		return ErrInvalidAttemptOperationCapability
	}
	digest, _ := v.Reservation.Digest()
	expected := issuer.operationMAC("sessionless.attempt-operation-send-capability.v1", v.Version, digest, v.IssuedAt, v.ExpiresAt, "")
	if !hmac.Equal(expected, v.Authenticator) {
		return ErrInvalidAttemptOperationCapability
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	now, err := issuer.timeLocked()
	entry, exists := issuer.entries[digest]
	if err != nil || now.Before(v.IssuedAt) || !now.Before(v.ExpiresAt) || !exists || entry.consumed || !hmac.Equal(entry.authenticator[:], v.Authenticator) {
		return ErrInvalidAttemptOperationCapability
	}
	entry.consumed = true
	issuer.entries[digest] = entry
	return nil
}

// Reconciliation has both a per-grant and an original absolute cleanup bound.
// Reissuing a read-only grant cannot move the original deadline plus cleanup.
func (issuer *OperationCapabilityIssuer) MintAttemptOperationObservationGrant(r domain.AttemptOperationReservationV1, issuedAt, expiresAt time.Time) (ports.AttemptOperationObservationGrantV1, error) {
	if !issuer.usable() {
		return ports.AttemptOperationObservationGrantV1{}, ErrInvalidAttemptOperationCapability
	}
	v := ports.AttemptOperationObservationGrantV1{Version: 1, Reservation: r.Clone(), IssuedAt: issuedAt.UTC(), ExpiresAt: expiresAt.UTC(), Authenticator: make([]byte, sha256.Size)}
	if v.Validate() != nil || v.ExpiresAt.After(r.Operation.Deadline.Add(r.Operation.CleanupDuration)) {
		return ports.AttemptOperationObservationGrantV1{}, ErrInvalidAttemptOperationCapability
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	now, err := issuer.timeLocked()
	if err != nil || now.Before(v.IssuedAt) || !now.Before(v.ExpiresAt) {
		return ports.AttemptOperationObservationGrantV1{}, ErrInvalidAttemptOperationCapability
	}
	digest, _ := v.Reservation.Digest()
	v.Authenticator = issuer.operationMAC("sessionless.attempt-operation-observation-grant.v1", v.Version, digest, v.IssuedAt, v.ExpiresAt, "")
	return v.Clone(), nil
}

func (issuer *OperationCapabilityIssuer) verifyOperationObservationGrantLocked(v ports.AttemptOperationObservationGrantV1, now time.Time) error {
	if v.Validate() != nil || v.ExpiresAt.After(v.Reservation.Operation.Deadline.Add(v.Reservation.Operation.CleanupDuration)) || now.Before(v.IssuedAt) || !now.Before(v.ExpiresAt) {
		return ErrInvalidAttemptOperationCapability
	}
	digest, _ := v.Reservation.Digest()
	expected := issuer.operationMAC("sessionless.attempt-operation-observation-grant.v1", v.Version, digest, v.IssuedAt, v.ExpiresAt, "")
	if !hmac.Equal(expected, v.Authenticator) {
		return ErrInvalidAttemptOperationCapability
	}
	return nil
}

func (issuer *OperationCapabilityIssuer) VerifyAttemptOperationObservationGrant(v ports.AttemptOperationObservationGrantV1) error {
	if !issuer.usable() {
		return ErrInvalidAttemptOperationCapability
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	now, err := issuer.timeLocked()
	if err != nil {
		return err
	}
	return issuer.verifyOperationObservationGrantLocked(v, now)
}

func (issuer *OperationCapabilityIssuer) AuthenticateAttemptOperationObservation(g ports.AttemptOperationObservationGrantV1, o domain.AttemptOperationObservationV1) (ports.AuthenticatedAttemptOperationObservationV1, error) {
	if !issuer.usable() {
		return ports.AuthenticatedAttemptOperationObservationV1{}, ErrInvalidAttemptOperationCapability
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	now, err := issuer.timeLocked()
	if err != nil || issuer.verifyOperationObservationGrantLocked(g, now) != nil || o.ValidateForReservation(g.Reservation) != nil || o.ObservedAt.After(now) {
		return ports.AuthenticatedAttemptOperationObservationV1{}, ErrInvalidAttemptOperationCapability
	}
	v := ports.AuthenticatedAttemptOperationObservationV1{Version: 1, Reservation: g.Reservation.Clone(), Observation: o.Clone()}
	digest, _ := v.Reservation.Digest()
	observed, _ := v.Observation.Digest()
	v.Authenticator = issuer.operationMAC("sessionless.authenticated-attempt-operation-observation.v1", v.Version, digest, time.Time{}, time.Time{}, string(observed))
	return v.Clone(), nil
}

// A verified historical proof is not a live grant or permission to continue.
// Its durability, immutability and retention remain the store's obligations.
func (issuer *OperationCapabilityIssuer) VerifyAttemptOperationObservation(v ports.AuthenticatedAttemptOperationObservationV1) error {
	if !issuer.usable() || v.Validate() != nil {
		return ErrInvalidAttemptOperationCapability
	}
	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	now, err := issuer.timeLocked()
	if err != nil || v.Observation.ObservedAt.After(now) {
		return ErrInvalidAttemptOperationCapability
	}
	digest, _ := v.Reservation.Digest()
	observed, _ := v.Observation.Digest()
	expected := issuer.operationMAC("sessionless.authenticated-attempt-operation-observation.v1", v.Version, digest, time.Time{}, time.Time{}, string(observed))
	if !hmac.Equal(expected, v.Authenticator) {
		return ErrInvalidAttemptOperationCapability
	}
	return nil
}

func (issuer *OperationCapabilityIssuer) operationMAC(separator string, version uint32, digest domain.AttemptOperationReservationDigestV1, issuedAt, expiresAt time.Time, observation string) []byte {
	mac := hmac.New(sha256.New, issuer.key[:])
	writeFrame(mac, separator)
	// Version 1 is closed by each port's validation; bind it explicitly as well.
	writeFrame(mac, strconv.FormatUint(uint64(version), 10))
	writeFrame(mac, string(digest))
	writeInstant(mac, issuedAt)
	writeInstant(mac, expiresAt)
	writeFrame(mac, observation)
	return mac.Sum(nil)
}

var _ ports.AttemptOperationCapabilityIssuerV1 = (*OperationCapabilityIssuer)(nil)
