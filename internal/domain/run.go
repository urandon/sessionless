package domain

import "time"

type RunStatus string

const (
	RunCreated      RunStatus = "created"
	RunAdmitted     RunStatus = "admitted"
	RunQueued       RunStatus = "queued"
	RunRunning      RunStatus = "running"
	RunSucceeded    RunStatus = "succeeded"
	RunFailed       RunStatus = "failed"
	RunCancelled    RunStatus = "cancelled"
	RunQuotaBlocked RunStatus = "quota_blocked"
)

var runTransitions = map[RunStatus]map[RunStatus]struct{}{
	RunCreated: {
		RunAdmitted: {}, RunQuotaBlocked: {}, RunCancelled: {}, RunFailed: {},
	},
	RunAdmitted: {
		RunQueued: {}, RunQuotaBlocked: {}, RunCancelled: {}, RunFailed: {},
	},
	RunQueued: {
		RunRunning: {}, RunQuotaBlocked: {}, RunCancelled: {}, RunFailed: {},
	},
	RunRunning: {
		RunSucceeded: {}, RunQuotaBlocked: {}, RunCancelled: {}, RunFailed: {},
	},
	RunQuotaBlocked: {
		RunAdmitted: {}, RunCancelled: {}, RunFailed: {},
	},
	RunSucceeded: {},
	RunFailed:    {},
	RunCancelled: {},
}

func (status RunStatus) Valid() bool {
	_, ok := runTransitions[status]
	return ok
}

func (status RunStatus) Terminal() bool {
	return status == RunSucceeded || status == RunFailed || status == RunCancelled
}

func CanTransitionRun(from, to RunStatus) bool {
	next, ok := runTransitions[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

type Run struct {
	ID                       RunID                    `json:"id"`
	TenantID                 TenantID                 `json:"tenant_id"`
	SessionID                SessionID                `json:"session_id"`
	TriggerEventID           SessionEventID           `json:"trigger_event_id"`
	SubscriptionConnectionID SubscriptionConnectionID `json:"subscription_connection_id"`
	Status                   RunStatus                `json:"status"`
	IdempotencyKey           IdempotencyKey           `json:"idempotency_key"`
	CancellationRequestedAt  *time.Time               `json:"cancellation_requested_at,omitempty"`
	StartedAt                *time.Time               `json:"started_at,omitempty"`
	FinishedAt               *time.Time               `json:"finished_at,omitempty"`
	CreatedAt                time.Time                `json:"created_at"`
	UpdatedAt                time.Time                `json:"updated_at"`
}

func (run Run) Validate() error {
	if err := run.ID.Validate(); err != nil {
		return err
	}
	if err := run.TenantID.Validate(); err != nil {
		return err
	}
	if err := run.SessionID.Validate(); err != nil {
		return err
	}
	if err := run.TriggerEventID.Validate(); err != nil {
		return err
	}
	if err := run.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	if !run.Status.Valid() {
		return ValidationError{Field: "run.status", Reason: "is unknown"}
	}
	if err := run.IdempotencyKey.Validate(); err != nil {
		return err
	}
	if run.CreatedAt.IsZero() {
		return ValidationError{Field: "run.created_at", Reason: "must not be zero"}
	}
	if run.UpdatedAt.Before(run.CreatedAt) {
		return ValidationError{Field: "run.updated_at", Reason: "must not be before created_at"}
	}
	if run.CancellationRequestedAt != nil && run.CancellationRequestedAt.Before(run.CreatedAt) {
		return ValidationError{Field: "run.cancellation_requested_at", Reason: "must not be before created_at"}
	}
	if run.StartedAt != nil && run.StartedAt.Before(run.CreatedAt) {
		return ValidationError{Field: "run.started_at", Reason: "must not be before created_at"}
	}
	if run.Status.Terminal() {
		if run.FinishedAt == nil {
			return ValidationError{Field: "run.finished_at", Reason: "is required for a terminal run"}
		}
	} else if run.FinishedAt != nil {
		return ValidationError{Field: "run.finished_at", Reason: "is allowed only for a terminal run"}
	}
	return nil
}

func (run *Run) Transition(to RunStatus, at time.Time) error {
	if run == nil {
		return ValidationError{Field: "run", Reason: "must not be nil"}
	}
	if !CanTransitionRun(run.Status, to) {
		return ValidationError{Field: "run.status", Reason: "transition is not allowed"}
	}
	if at.Before(run.UpdatedAt) {
		return ValidationError{Field: "run.updated_at", Reason: "transition time must not move backwards"}
	}
	run.Status = to
	run.UpdatedAt = at
	if to == RunRunning && run.StartedAt == nil {
		startedAt := at
		run.StartedAt = &startedAt
	}
	if to.Terminal() {
		finishedAt := at
		run.FinishedAt = &finishedAt
	}
	return nil
}

func (run *Run) RequestCancellation(at time.Time) error {
	if run == nil {
		return ValidationError{Field: "run", Reason: "must not be nil"}
	}
	if run.Status.Terminal() {
		return ValidationError{Field: "run.cancellation", Reason: "cannot request cancellation for a terminal run"}
	}
	if at.Before(run.CreatedAt) {
		return ValidationError{Field: "run.cancellation_requested_at", Reason: "must not be before created_at"}
	}
	if run.CancellationRequestedAt == nil {
		requestedAt := at
		run.CancellationRequestedAt = &requestedAt
	}
	return nil
}
