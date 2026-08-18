package domain

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type SessionDeletionState string

const (
	SessionDeletionRequested SessionDeletionState = "requested"
	SessionDeletionDeleting  SessionDeletionState = "deleting"
	SessionDeletionCompleted SessionDeletionState = "completed"
)

func (state SessionDeletionState) Valid() bool {
	switch state {
	case SessionDeletionRequested, SessionDeletionDeleting, SessionDeletionCompleted:
		return true
	default:
		return false
	}
}

// SessionDeletion is the durable tombstone and audit boundary for destructive
// canonical-session removal. It intentionally survives completion.
type SessionDeletion struct {
	TenantID       TenantID             `json:"tenant_id"`
	SessionID      SessionID            `json:"session_id"`
	RequestedBy    UserID               `json:"requested_by"`
	Reason         string               `json:"reason"`
	State          SessionDeletionState `json:"state"`
	RequestedAt    time.Time            `json:"requested_at"`
	StartedAt      *time.Time           `json:"started_at,omitempty"`
	CompletedAt    *time.Time           `json:"completed_at,omitempty"`
	DeletedObjects uint64               `json:"deleted_objects"`
	DeletedBytes   uint64               `json:"deleted_bytes"`
}

func (deletion SessionDeletion) Validate() error {
	if err := deletion.TenantID.Validate(); err != nil {
		return err
	}
	if err := deletion.SessionID.Validate(); err != nil {
		return err
	}
	if err := deletion.RequestedBy.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(deletion.Reason) == "" || len(deletion.Reason) > 1024 {
		return ValidationError{Field: "session_deletion.reason", Reason: "must contain 1 to 1024 characters"}
	}
	if !deletion.State.Valid() {
		return ValidationError{Field: "session_deletion.state", Reason: "is unknown"}
	}
	if deletion.RequestedAt.IsZero() {
		return ValidationError{Field: "session_deletion.requested_at", Reason: "must not be zero"}
	}
	switch deletion.State {
	case SessionDeletionRequested:
		if deletion.StartedAt != nil || deletion.CompletedAt != nil || deletion.DeletedObjects != 0 || deletion.DeletedBytes != 0 {
			return ValidationError{Field: "session_deletion", Reason: "requested deletion must not contain execution results"}
		}
	case SessionDeletionDeleting:
		if deletion.StartedAt == nil || deletion.StartedAt.Before(deletion.RequestedAt) || deletion.CompletedAt != nil {
			return ValidationError{Field: "session_deletion.started_at", Reason: "must follow the request while deleting"}
		}
	case SessionDeletionCompleted:
		if deletion.StartedAt == nil || deletion.CompletedAt == nil ||
			deletion.StartedAt.Before(deletion.RequestedAt) || deletion.CompletedAt.Before(*deletion.StartedAt) {
			return ValidationError{Field: "session_deletion.completed_at", Reason: "must follow deletion start"}
		}
	}
	return nil
}

func (deletion *SessionDeletion) Start(at time.Time) error {
	if deletion == nil {
		return ValidationError{Field: "session_deletion", Reason: "must not be nil"}
	}
	if err := deletion.Validate(); err != nil {
		return err
	}
	if deletion.State == SessionDeletionDeleting || deletion.State == SessionDeletionCompleted {
		return nil
	}
	if at.Before(deletion.RequestedAt) {
		return ValidationError{Field: "session_deletion.started_at", Reason: "must not precede the request"}
	}
	deletion.State, deletion.StartedAt = SessionDeletionDeleting, &at
	return nil
}

func (deletion *SessionDeletion) Complete(at time.Time, deletedObjects, deletedBytes uint64) error {
	if deletion == nil {
		return ValidationError{Field: "session_deletion", Reason: "must not be nil"}
	}
	if err := deletion.Validate(); err != nil {
		return err
	}
	if deletion.State == SessionDeletionCompleted {
		if deletion.DeletedObjects == deletedObjects && deletion.DeletedBytes == deletedBytes {
			return nil
		}
		return fmt.Errorf("session deletion completion conflicts with the durable tombstone")
	}
	if deletion.State != SessionDeletionDeleting || deletion.StartedAt == nil || at.Before(*deletion.StartedAt) {
		return ValidationError{Field: "session_deletion.completed_at", Reason: "requires a started deletion and monotonic time"}
	}
	deletion.State, deletion.CompletedAt = SessionDeletionCompleted, &at
	deletion.DeletedObjects, deletion.DeletedBytes = deletedObjects, deletedBytes
	return nil
}

type SessionLegalHoldState string

const (
	SessionLegalHoldActive   SessionLegalHoldState = "active"
	SessionLegalHoldReleased SessionLegalHoldState = "released"
)

type SessionLegalHold struct {
	TenantID   TenantID              `json:"tenant_id"`
	SessionID  SessionID             `json:"session_id"`
	State      SessionLegalHoldState `json:"state"`
	Reason     string                `json:"reason"`
	SetBy      UserID                `json:"set_by"`
	SetAt      time.Time             `json:"set_at"`
	ReleasedBy *UserID               `json:"released_by,omitempty"`
	ReleasedAt *time.Time            `json:"released_at,omitempty"`
}

func (hold SessionLegalHold) Validate() error {
	if err := hold.TenantID.Validate(); err != nil {
		return err
	}
	if err := hold.SessionID.Validate(); err != nil {
		return err
	}
	if err := hold.SetBy.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(hold.Reason) == "" || len(hold.Reason) > 1024 {
		return ValidationError{Field: "session_legal_hold.reason", Reason: "must contain 1 to 1024 characters"}
	}
	if hold.SetAt.IsZero() {
		return ValidationError{Field: "session_legal_hold.set_at", Reason: "must not be zero"}
	}
	switch hold.State {
	case SessionLegalHoldActive:
		if hold.ReleasedBy != nil || hold.ReleasedAt != nil {
			return ValidationError{Field: "session_legal_hold.release", Reason: "is allowed only for a released hold"}
		}
	case SessionLegalHoldReleased:
		if hold.ReleasedBy == nil || hold.ReleasedAt == nil || hold.ReleasedAt.Before(hold.SetAt) {
			return ValidationError{Field: "session_legal_hold.release", Reason: "must identify a monotonic release"}
		}
		if err := hold.ReleasedBy.Validate(); err != nil {
			return err
		}
	default:
		return ValidationError{Field: "session_legal_hold.state", Reason: "is unknown"}
	}
	return nil
}

func (hold *SessionLegalHold) Release(userID UserID, at time.Time) error {
	if hold == nil {
		return ValidationError{Field: "session_legal_hold", Reason: "must not be nil"}
	}
	if err := hold.Validate(); err != nil {
		return err
	}
	if hold.State == SessionLegalHoldReleased {
		return nil
	}
	if err := userID.Validate(); err != nil {
		return err
	}
	if at.Before(hold.SetAt) {
		return ValidationError{Field: "session_legal_hold.released_at", Reason: "must not precede the hold"}
	}
	hold.State, hold.ReleasedBy, hold.ReleasedAt = SessionLegalHoldReleased, &userID, &at
	return nil
}

type SessionDeletionInventory struct {
	TenantID     TenantID  `json:"tenant_id"`
	SessionID    SessionID `json:"session_id"`
	Objects      []BlobRef `json:"objects"`
	EventRows    uint64    `json:"event_rows"`
	SnapshotRows uint64    `json:"snapshot_rows"`
	RunRows      uint64    `json:"run_rows"`
	ManifestRows uint64    `json:"manifest_rows"`
	DeliveryRows uint64    `json:"delivery_rows"`
	TotalBytes   uint64    `json:"total_bytes"`
}

func (inventory SessionDeletionInventory) Validate(maxObjects uint64) error {
	if err := inventory.TenantID.Validate(); err != nil {
		return err
	}
	if err := inventory.SessionID.Validate(); err != nil {
		return err
	}
	if maxObjects == 0 || uint64(len(inventory.Objects)) > maxObjects {
		return ValidationError{Field: "session_deletion.objects", Reason: "exceeds the configured exact-object bound"}
	}
	seen := make(map[string]struct{}, len(inventory.Objects))
	var bytes uint64
	prefix := SessionObjectPrefix(inventory.TenantID, inventory.SessionID)
	for _, ref := range inventory.Objects {
		if err := ref.Validate(); err != nil {
			return err
		}
		if err := EnsureSameTenant(inventory.TenantID, ref.TenantID); err != nil {
			return err
		}
		if !strings.HasPrefix(ref.Key, prefix) {
			return ValidationError{Field: "session_deletion.objects", Reason: fmt.Sprintf("must remain under %q", prefix)}
		}
		if _, duplicate := seen[ref.Key]; duplicate {
			return ValidationError{Field: "session_deletion.objects", Reason: "contains duplicate object keys"}
		}
		seen[ref.Key] = struct{}{}
		if uint64(ref.Size) > math.MaxUint64-bytes {
			return ValidationError{Field: "session_deletion.total_bytes", Reason: "overflowed"}
		}
		bytes += uint64(ref.Size)
	}
	if bytes != inventory.TotalBytes {
		return ValidationError{Field: "session_deletion.total_bytes", Reason: "does not match exact object metadata"}
	}
	return nil
}
