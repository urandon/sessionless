package domain

import "time"

// SessionEventDraft is a canonical worker result whose session sequence is
// allocated by the terminal transaction. It deliberately contains no
// transport fields.
type SessionEventDraft struct {
	ID             SessionEventID   `json:"id"`
	Kind           SessionEventKind `json:"kind"`
	IdempotencyKey IdempotencyKey   `json:"idempotency_key"`
	Payload        BlobRef          `json:"payload"`
	DisplayText    string           `json:"display_text,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
}

func (draft SessionEventDraft) ValidateForRun(run Run) error {
	if err := run.Validate(); err != nil {
		return err
	}
	if err := draft.ID.Validate(); err != nil {
		return err
	}
	switch draft.Kind {
	case SessionEventAssistantMessage, SessionEventToolCall, SessionEventToolResult, SessionEventSystemNotice:
	default:
		return ValidationError{Field: "session_event_draft.kind", Reason: "is not a terminal worker event"}
	}
	if err := draft.IdempotencyKey.Validate(); err != nil {
		return err
	}
	if err := ValidateSessionEventBlob(run.TenantID, run.SessionID, draft.ID, draft.Payload); err != nil {
		return err
	}
	if len([]rune(draft.DisplayText)) > 32_000 {
		return ValidationError{Field: "session_event_draft.display_text", Reason: "must not exceed 32000 Unicode characters"}
	}
	if draft.CreatedAt.IsZero() || draft.CreatedAt.Before(run.CreatedAt) {
		return ValidationError{Field: "session_event_draft.created_at", Reason: "must not precede the owning run"}
	}
	return nil
}

func (draft SessionEventDraft) ProjectionEligible() bool {
	return draft.Kind == SessionEventAssistantMessage || draft.Kind == SessionEventSystemNotice
}

type FrontendProjectionStatus string

const FrontendProjectionPending FrontendProjectionStatus = "pending"

func (status FrontendProjectionStatus) Valid() bool {
	return status == FrontendProjectionPending
}

// FrontendProjection is durable, frontend-neutral work referencing canonical
// history. BindingRevision snapshots authorization at finalization; consumers
// must recheck the live binding before projecting.
type FrontendProjection struct {
	ID              FrontendProjectionID     `json:"id"`
	TenantID        TenantID                 `json:"tenant_id"`
	SessionID       SessionID                `json:"session_id"`
	EventID         SessionEventID           `json:"event_id"`
	EventSequence   uint64                   `json:"event_sequence"`
	EventKind       SessionEventKind         `json:"event_kind"`
	BindingID       FrontendBindingID        `json:"binding_id"`
	BindingRevision uint64                   `json:"binding_revision"`
	Frontend        Frontend                 `json:"frontend"`
	Status          FrontendProjectionStatus `json:"status"`
	IdempotencyKey  IdempotencyKey           `json:"idempotency_key"`
	CreatedAt       time.Time                `json:"created_at"`
	UpdatedAt       time.Time                `json:"updated_at"`
}

// TelegramProjectionRef keeps only immutable canonical routing references in
// the transport delivery row. The sender re-resolves every referenced record
// before each physical send; payload content remains owned by SessionEvent.
type TelegramProjectionRef struct {
	ProjectionID    FrontendProjectionID `json:"projection_id"`
	SessionID       SessionID            `json:"session_id"`
	EventID         SessionEventID       `json:"event_id"`
	EventSequence   uint64               `json:"event_sequence"`
	BindingID       FrontendBindingID    `json:"binding_id"`
	BindingRevision uint64               `json:"binding_revision"`
	TriggerEventID  SessionEventID       `json:"trigger_event_id"`
}

func (ref TelegramProjectionRef) Validate() error {
	if err := ref.ProjectionID.Validate(); err != nil {
		return err
	}
	if err := ref.SessionID.Validate(); err != nil {
		return err
	}
	if err := ref.EventID.Validate(); err != nil {
		return err
	}
	if ref.EventSequence == 0 {
		return ValidationError{Field: "telegram_projection.event_sequence", Reason: "must be positive"}
	}
	if err := ref.BindingID.Validate(); err != nil {
		return err
	}
	if ref.BindingRevision == 0 {
		return ValidationError{Field: "telegram_projection.binding_revision", Reason: "must be positive"}
	}
	return ref.TriggerEventID.Validate()
}

func (projection FrontendProjection) ValidateFor(event SessionEvent, binding FrontendBinding) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if event.Kind != SessionEventAssistantMessage && event.Kind != SessionEventSystemNotice {
		return ValidationError{Field: "frontend_projection.event_kind", Reason: "is not frontend-projectable"}
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	if err := projection.ID.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(event.TenantID, projection.TenantID); err != nil {
		return err
	}
	if err := EnsureSameTenant(event.TenantID, binding.TenantID); err != nil {
		return err
	}
	if projection.SessionID != event.SessionID || projection.SessionID != binding.SessionID {
		return ValidationError{Field: "frontend_projection.session_id", Reason: "must reference the event and binding session"}
	}
	if projection.EventID != event.ID || projection.EventSequence != event.Sequence || projection.EventKind != event.Kind {
		return ValidationError{Field: "frontend_projection.event", Reason: "must reference the canonical event"}
	}
	if projection.BindingID != binding.ID || projection.BindingRevision != binding.Revision || projection.Frontend != binding.Frontend {
		return ValidationError{Field: "frontend_projection.binding", Reason: "must snapshot the current binding"}
	}
	if !projection.Status.Valid() {
		return ValidationError{Field: "frontend_projection.status", Reason: "is unknown"}
	}
	if err := projection.IdempotencyKey.Validate(); err != nil {
		return err
	}
	if projection.CreatedAt.IsZero() || projection.UpdatedAt.Before(projection.CreatedAt) {
		return ValidationError{Field: "frontend_projection.updated_at", Reason: "must not be before created_at"}
	}
	return nil
}
