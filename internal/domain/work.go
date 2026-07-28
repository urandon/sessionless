package domain

import "time"

type AttemptStatus string

const (
	AttemptCreated   AttemptStatus = "created"
	AttemptRunning   AttemptStatus = "running"
	AttemptSucceeded AttemptStatus = "succeeded"
	AttemptFailed    AttemptStatus = "failed"
	AttemptCancelled AttemptStatus = "cancelled"
)

func (status AttemptStatus) Valid() bool {
	switch status {
	case AttemptCreated, AttemptRunning, AttemptSucceeded, AttemptFailed, AttemptCancelled:
		return true
	default:
		return false
	}
}

func (status AttemptStatus) Terminal() bool {
	return status == AttemptSucceeded || status == AttemptFailed || status == AttemptCancelled
}

func CanTransitionAttempt(from, to AttemptStatus) bool {
	switch from {
	case AttemptCreated:
		return to == AttemptRunning || to == AttemptFailed || to == AttemptCancelled
	case AttemptRunning:
		return to == AttemptSucceeded || to == AttemptFailed || to == AttemptCancelled
	default:
		return false
	}
}

type Attempt struct {
	ID         AttemptID     `json:"id"`
	TenantID   TenantID      `json:"tenant_id"`
	RunID      RunID         `json:"run_id"`
	Number     uint32        `json:"number"`
	Status     AttemptStatus `json:"status"`
	WorkerID   string        `json:"worker_id,omitempty"`
	CreatedAt  time.Time     `json:"created_at"`
	UpdatedAt  time.Time     `json:"updated_at"`
	FinishedAt *time.Time    `json:"finished_at,omitempty"`
}

func (attempt Attempt) ValidateForRun(run Run) error {
	if err := run.Validate(); err != nil {
		return err
	}
	if err := attempt.ID.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(run.TenantID, attempt.TenantID); err != nil {
		return err
	}
	if attempt.RunID != run.ID {
		return ValidationError{Field: "attempt.run_id", Reason: "must reference the owning run"}
	}
	if attempt.Number == 0 {
		return ValidationError{Field: "attempt.number", Reason: "must be positive"}
	}
	if !attempt.Status.Valid() {
		return ValidationError{Field: "attempt.status", Reason: "is unknown"}
	}
	if attempt.WorkerID != "" {
		if err := ValidateOpaqueID("attempt.worker_id", attempt.WorkerID); err != nil {
			return err
		}
	}
	if attempt.CreatedAt.IsZero() || attempt.UpdatedAt.Before(attempt.CreatedAt) {
		return ValidationError{Field: "attempt.updated_at", Reason: "must not be before a non-zero created_at"}
	}
	if attempt.Status.Terminal() != (attempt.FinishedAt != nil) {
		return ValidationError{Field: "attempt.finished_at", Reason: "must be present exactly when the attempt is terminal"}
	}
	return nil
}

func (attempt *Attempt) Transition(to AttemptStatus, at time.Time) error {
	if attempt == nil {
		return ValidationError{Field: "attempt", Reason: "must not be nil"}
	}
	if !CanTransitionAttempt(attempt.Status, to) {
		return ValidationError{Field: "attempt.status", Reason: "transition is not allowed"}
	}
	if at.Before(attempt.UpdatedAt) {
		return ValidationError{Field: "attempt.updated_at", Reason: "transition time must not move backwards"}
	}
	attempt.Status = to
	attempt.UpdatedAt = at
	if to.Terminal() {
		finishedAt := at
		attempt.FinishedAt = &finishedAt
	}
	return nil
}

type Lease struct {
	ID         LeaseID   `json:"id"`
	TenantID   TenantID  `json:"tenant_id"`
	RunID      RunID     `json:"run_id"`
	AttemptID  AttemptID `json:"attempt_id"`
	WorkerID   string    `json:"worker_id"`
	FenceToken uint64    `json:"fence_token"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

func (lease Lease) ValidateForAttempt(run Run, attempt Attempt) error {
	if err := attempt.ValidateForRun(run); err != nil {
		return err
	}
	if err := lease.ID.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(run.TenantID, lease.TenantID); err != nil {
		return err
	}
	if lease.RunID != run.ID || lease.AttemptID != attempt.ID {
		return ValidationError{Field: "lease", Reason: "must reference the owning run and attempt"}
	}
	if err := ValidateOpaqueID("lease.worker_id", lease.WorkerID); err != nil {
		return err
	}
	if lease.FenceToken == 0 {
		return ValidationError{Field: "lease.fence_token", Reason: "must be positive"}
	}
	if lease.AcquiredAt.IsZero() || !lease.ExpiresAt.After(lease.AcquiredAt) {
		return ValidationError{Field: "lease.expires_at", Reason: "must be after a non-zero acquired_at"}
	}
	return nil
}

func (lease Lease) ActiveAt(at time.Time) bool {
	return !at.Before(lease.AcquiredAt) && at.Before(lease.ExpiresAt)
}

type Checkpoint struct {
	ID        CheckpointID `json:"id"`
	TenantID  TenantID     `json:"tenant_id"`
	RunID     RunID        `json:"run_id"`
	AttemptID AttemptID    `json:"attempt_id"`
	Sequence  uint64       `json:"sequence"`
	State     BlobRef      `json:"state"`
	CreatedAt time.Time    `json:"created_at"`
}

func (checkpoint Checkpoint) ValidateForAttempt(run Run, attempt Attempt) error {
	if err := attempt.ValidateForRun(run); err != nil {
		return err
	}
	if err := checkpoint.ID.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(run.TenantID, checkpoint.TenantID); err != nil {
		return err
	}
	if checkpoint.RunID != run.ID || checkpoint.AttemptID != attempt.ID {
		return ValidationError{Field: "checkpoint", Reason: "must reference the owning run and attempt"}
	}
	if checkpoint.Sequence == 0 {
		return ValidationError{Field: "checkpoint.sequence", Reason: "must be positive"}
	}
	if err := checkpoint.State.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(run.TenantID, checkpoint.State.TenantID); err != nil {
		return err
	}
	if checkpoint.CreatedAt.IsZero() {
		return ValidationError{Field: "checkpoint.created_at", Reason: "must not be zero"}
	}
	return nil
}
