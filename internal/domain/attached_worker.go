package domain

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	AttachedWorkerAuditEventVersionV1 uint32 = 1
	maxAttachedWorkerDisplayNameBytes        = 128
	maxAttachedWorkerAudienceBytes           = 256
)

type (
	AttachedWorkerID            string
	AttachedWorkerEnrollmentID  string
	WorkerBootstrapDigest       string
	AttachedWorkerDesiredState  string
	AttachedWorkerObservedState string
	AttachedWorkerAuditAction   string
)

func (id AttachedWorkerID) Validate() error {
	return ValidateOpaqueID("attached_worker_id", string(id))
}

func (id AttachedWorkerEnrollmentID) Validate() error {
	return ValidateOpaqueID("attached_worker_enrollment_id", string(id))
}

func DigestWorkerBootstrap(secret []byte) WorkerBootstrapDigest {
	sum := sha256.Sum256(secret)
	return WorkerBootstrapDigest(hex.EncodeToString(sum[:]))
}

func (digest WorkerBootstrapDigest) Validate() error {
	value := string(digest)
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return ValidationError{Field: "worker_bootstrap_digest", Reason: "must be a lowercase SHA-256 digest"}
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return ValidationError{Field: "worker_bootstrap_digest", Reason: "must be a lowercase SHA-256 digest"}
	}
	var nonzero byte
	for _, value := range decoded {
		nonzero |= value
	}
	if nonzero == 0 {
		return ValidationError{Field: "worker_bootstrap_digest", Reason: "must not be the zero digest"}
	}
	return nil
}

const (
	AttachedWorkerDesiredActive  AttachedWorkerDesiredState = "active"
	AttachedWorkerDesiredDrain   AttachedWorkerDesiredState = "drain"
	AttachedWorkerDesiredRevoked AttachedWorkerDesiredState = "revoked"
)

func (state AttachedWorkerDesiredState) Valid() bool {
	switch state {
	case AttachedWorkerDesiredActive, AttachedWorkerDesiredDrain, AttachedWorkerDesiredRevoked:
		return true
	default:
		return false
	}
}

const (
	AttachedWorkerObservedPending  AttachedWorkerObservedState = "pending"
	AttachedWorkerObservedOffline  AttachedWorkerObservedState = "offline"
	AttachedWorkerObservedOnline   AttachedWorkerObservedState = "online"
	AttachedWorkerObservedDraining AttachedWorkerObservedState = "draining"
	AttachedWorkerObservedRevoked  AttachedWorkerObservedState = "revoked"
)

func (state AttachedWorkerObservedState) Valid() bool {
	switch state {
	case AttachedWorkerObservedPending, AttachedWorkerObservedOffline, AttachedWorkerObservedOnline,
		AttachedWorkerObservedDraining, AttachedWorkerObservedRevoked:
		return true
	default:
		return false
	}
}

type AttachedWorkerEnrollment struct {
	TenantID        TenantID                   `json:"tenant_id"`
	OwnerUserID     UserID                     `json:"owner_user_id"`
	ID              AttachedWorkerEnrollmentID `json:"enrollment_id"`
	WorkerID        AttachedWorkerID           `json:"worker_id"`
	DisplayName     string                     `json:"display_name"`
	Audience        string                     `json:"audience"`
	BootstrapDigest WorkerBootstrapDigest      `json:"bootstrap_digest"`
	ExpiresAt       time.Time                  `json:"expires_at"`
	RetainUntil     time.Time                  `json:"retain_until"`
	ConsumedAt      time.Time                  `json:"consumed_at"`
	CreatedAt       time.Time                  `json:"created_at"`
	Revision        uint64                     `json:"revision"`
}

func (enrollment AttachedWorkerEnrollment) Validate() error {
	if err := enrollment.TenantID.Validate(); err != nil {
		return err
	}
	if err := enrollment.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := enrollment.ID.Validate(); err != nil {
		return err
	}
	if err := enrollment.WorkerID.Validate(); err != nil {
		return err
	}
	if err := validateAttachedWorkerDisplayName(enrollment.DisplayName); err != nil {
		return err
	}
	if err := validateAttachedWorkerAudience(enrollment.Audience); err != nil {
		return err
	}
	if err := enrollment.BootstrapDigest.Validate(); err != nil {
		return err
	}
	if enrollment.CreatedAt.IsZero() || !enrollment.ExpiresAt.After(enrollment.CreatedAt) {
		return ValidationError{Field: "attached_worker_enrollment.expires_at", Reason: "must be after created_at"}
	}
	if !enrollment.RetainUntil.After(enrollment.ExpiresAt) {
		return ValidationError{Field: "attached_worker_enrollment.retain_until", Reason: "must be after expires_at"}
	}
	if !enrollment.ConsumedAt.IsZero() && (enrollment.ConsumedAt.Before(enrollment.CreatedAt) || !enrollment.ConsumedAt.Before(enrollment.ExpiresAt)) {
		return ValidationError{Field: "attached_worker_enrollment.consumed_at", Reason: "must be within the enrollment lifetime"}
	}
	if enrollment.Revision == 0 {
		return ValidationError{Field: "attached_worker_enrollment.revision", Reason: "must be positive"}
	}
	return nil
}

type AttachedWorker struct {
	TenantID             TenantID                    `json:"tenant_id"`
	OwnerUserID          UserID                      `json:"owner_user_id"`
	ID                   AttachedWorkerID            `json:"worker_id"`
	DisplayName          string                      `json:"display_name"`
	IdentityPublicKey    []byte                      `json:"identity_public_key"`
	EnrollmentGeneration uint64                      `json:"enrollment_generation"`
	ConnectionGeneration uint64                      `json:"connection_generation"`
	DesiredState         AttachedWorkerDesiredState  `json:"desired_state"`
	ObservedState        AttachedWorkerObservedState `json:"observed_state"`
	Revision             uint64                      `json:"revision"`
	CreatedAt            time.Time                   `json:"created_at"`
	UpdatedAt            time.Time                   `json:"updated_at"`
	RevokedAt            time.Time                   `json:"revoked_at"`
}

func (worker AttachedWorker) Validate() error {
	if err := worker.TenantID.Validate(); err != nil {
		return err
	}
	if err := worker.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := worker.ID.Validate(); err != nil {
		return err
	}
	if err := validateAttachedWorkerDisplayName(worker.DisplayName); err != nil {
		return err
	}
	if len(worker.IdentityPublicKey) != ed25519.PublicKeySize {
		return ValidationError{Field: "attached_worker.identity_public_key", Reason: "must be an Ed25519 public key"}
	}
	if worker.EnrollmentGeneration == 0 {
		return ValidationError{Field: "attached_worker.enrollment_generation", Reason: "must be positive"}
	}
	if !worker.DesiredState.Valid() {
		return ValidationError{Field: "attached_worker.desired_state", Reason: "is unknown"}
	}
	if !worker.ObservedState.Valid() {
		return ValidationError{Field: "attached_worker.observed_state", Reason: "is unknown"}
	}
	if worker.Revision == 0 {
		return ValidationError{Field: "attached_worker.revision", Reason: "must be positive"}
	}
	if worker.CreatedAt.IsZero() || worker.UpdatedAt.Before(worker.CreatedAt) {
		return ValidationError{Field: "attached_worker.updated_at", Reason: "must not precede created_at"}
	}
	if worker.DesiredState == AttachedWorkerDesiredRevoked {
		if worker.RevokedAt.IsZero() || worker.RevokedAt.Before(worker.CreatedAt) || worker.RevokedAt.After(worker.UpdatedAt) {
			return ValidationError{Field: "attached_worker.revoked_at", Reason: "must record the deny-first revocation time"}
		}
	} else if !worker.RevokedAt.IsZero() {
		return ValidationError{Field: "attached_worker.revoked_at", Reason: "must be empty unless desired_state is revoked"}
	}
	return nil
}

const (
	AttachedWorkerAuditEnrollmentCreated            AttachedWorkerAuditAction = "enrollment_created"
	AttachedWorkerAuditEnrollmentClaimed            AttachedWorkerAuditAction = "enrollment_claimed"
	AttachedWorkerAuditWorkerRenamed                AttachedWorkerAuditAction = "worker_renamed"
	AttachedWorkerAuditIdentityRotated              AttachedWorkerAuditAction = "identity_rotated"
	AttachedWorkerAuditConnectionGenerationAdvanced AttachedWorkerAuditAction = "connection_generation_advanced"
	AttachedWorkerAuditWorkerRevoked                AttachedWorkerAuditAction = "worker_revoked"
)

func (action AttachedWorkerAuditAction) Valid() bool {
	switch action {
	case AttachedWorkerAuditEnrollmentCreated, AttachedWorkerAuditEnrollmentClaimed,
		AttachedWorkerAuditWorkerRenamed, AttachedWorkerAuditIdentityRotated,
		AttachedWorkerAuditConnectionGenerationAdvanced, AttachedWorkerAuditWorkerRevoked:
		return true
	default:
		return false
	}
}

// AttachedWorkerAuditEvent is deliberately content-free. In particular it
// cannot carry bootstrap material, identity keys, signatures, capabilities,
// provider data, transport details, or credentials.
type AttachedWorkerAuditEvent struct {
	Version              uint32                     `json:"version"`
	TenantID             TenantID                   `json:"tenant_id"`
	OwnerUserID          UserID                     `json:"owner_user_id"`
	WorkerID             AttachedWorkerID           `json:"worker_id"`
	EnrollmentID         AttachedWorkerEnrollmentID `json:"enrollment_id"`
	Action               AttachedWorkerAuditAction  `json:"action"`
	WorkerRevision       uint64                     `json:"worker_revision"`
	EnrollmentGeneration uint64                     `json:"enrollment_generation"`
	ConnectionGeneration uint64                     `json:"connection_generation"`
	OccurredAt           time.Time                  `json:"occurred_at"`
}

func (event AttachedWorkerAuditEvent) Validate() error {
	if event.Version != AttachedWorkerAuditEventVersionV1 {
		return ValidationError{Field: "attached_worker_audit.version", Reason: "is unsupported"}
	}
	if err := event.TenantID.Validate(); err != nil {
		return err
	}
	if err := event.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := event.WorkerID.Validate(); err != nil {
		return err
	}
	if !event.Action.Valid() {
		return ValidationError{Field: "attached_worker_audit.action", Reason: "is unknown"}
	}
	if event.OccurredAt.IsZero() {
		return ValidationError{Field: "attached_worker_audit.occurred_at", Reason: "must not be zero"}
	}
	switch event.Action {
	case AttachedWorkerAuditEnrollmentCreated:
		if err := event.EnrollmentID.Validate(); err != nil {
			return err
		}
		if event.WorkerRevision != 0 || event.EnrollmentGeneration != 0 || event.ConnectionGeneration != 0 {
			return ValidationError{Field: "attached_worker_audit.generations", Reason: "must be zero for enrollment creation"}
		}
	case AttachedWorkerAuditEnrollmentClaimed:
		if err := event.EnrollmentID.Validate(); err != nil {
			return err
		}
		if event.WorkerRevision != 1 || event.EnrollmentGeneration != 1 || event.ConnectionGeneration != 0 {
			return ValidationError{Field: "attached_worker_audit.generations", Reason: "must describe the initial claimed worker"}
		}
	default:
		if event.EnrollmentID != "" {
			return ValidationError{Field: "attached_worker_audit.enrollment_id", Reason: "must be empty for worker mutations"}
		}
		if event.WorkerRevision < 2 || event.EnrollmentGeneration == 0 {
			return ValidationError{Field: "attached_worker_audit.generations", Reason: "must describe the resulting worker"}
		}
	}
	return nil
}

func validateAttachedWorkerDisplayName(value string) error {
	if value == "" || value != strings.TrimSpace(value) || !utf8.ValidString(value) {
		return ValidationError{Field: "attached_worker.display_name", Reason: "must be non-empty and trimmed"}
	}
	if len(value) > maxAttachedWorkerDisplayNameBytes {
		return ValidationError{Field: "attached_worker.display_name", Reason: "is too long"}
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return ValidationError{Field: "attached_worker.display_name", Reason: "must not contain control characters"}
		}
	}
	return nil
}

func validateAttachedWorkerAudience(value string) error {
	if value == "" || value != strings.TrimSpace(value) || len(value) > maxAttachedWorkerAudienceBytes {
		return ValidationError{Field: "attached_worker_enrollment.audience", Reason: "must be non-empty, trimmed, and bounded"}
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return ValidationError{Field: "attached_worker_enrollment.audience", Reason: "must contain printable ASCII without spaces"}
		}
	}
	return nil
}
