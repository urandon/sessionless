package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type SessionStatus string

const (
	SessionActive   SessionStatus = "active"
	SessionArchived SessionStatus = "archived"
)

func (status SessionStatus) Valid() bool {
	return status == SessionActive || status == SessionArchived
}

// Session is the canonical product conversation. Frontend conversations bind
// to it, but neither a frontend nor a harness owns its identity or lifecycle.
type Session struct {
	ID                SessionID     `json:"id"`
	TenantID          TenantID      `json:"tenant_id"`
	CreatedBy         UserID        `json:"created_by"`
	Status            SessionStatus `json:"status"`
	LastEventSequence uint64        `json:"last_event_sequence"`
	CreatedAt         time.Time     `json:"created_at"`
	UpdatedAt         time.Time     `json:"updated_at"`
	ArchivedAt        *time.Time    `json:"archived_at,omitempty"`
}

func (session Session) Validate() error {
	if err := session.ID.Validate(); err != nil {
		return err
	}
	if err := session.TenantID.Validate(); err != nil {
		return err
	}
	if err := session.CreatedBy.Validate(); err != nil {
		return err
	}
	if !session.Status.Valid() {
		return ValidationError{Field: "session.status", Reason: "is unknown"}
	}
	if session.CreatedAt.IsZero() {
		return ValidationError{Field: "session.created_at", Reason: "must not be zero"}
	}
	if session.UpdatedAt.Before(session.CreatedAt) {
		return ValidationError{Field: "session.updated_at", Reason: "must not be before created_at"}
	}
	if session.Status == SessionArchived && session.ArchivedAt == nil {
		return ValidationError{Field: "session.archived_at", Reason: "is required for an archived session"}
	}
	if session.Status == SessionActive && session.ArchivedAt != nil {
		return ValidationError{Field: "session.archived_at", Reason: "is allowed only for an archived session"}
	}
	return nil
}

func (session *Session) Archive(at time.Time) error {
	if session == nil {
		return ValidationError{Field: "session", Reason: "must not be nil"}
	}
	if err := session.Validate(); err != nil {
		return err
	}
	if session.Status != SessionActive {
		return ValidationError{Field: "session.status", Reason: "only an active session can be archived"}
	}
	if at.Before(session.UpdatedAt) {
		return ValidationError{Field: "session.updated_at", Reason: "transition time must not move backwards"}
	}
	session.Status, session.UpdatedAt, session.ArchivedAt = SessionArchived, at, &at
	return nil
}

func (session *Session) Unarchive(at time.Time) error {
	if session == nil {
		return ValidationError{Field: "session", Reason: "must not be nil"}
	}
	if err := session.Validate(); err != nil {
		return err
	}
	if session.Status != SessionArchived {
		return ValidationError{Field: "session.status", Reason: "only an archived session can be unarchived"}
	}
	if at.Before(session.UpdatedAt) {
		return ValidationError{Field: "session.updated_at", Reason: "transition time must not move backwards"}
	}
	session.Status, session.UpdatedAt, session.ArchivedAt = SessionActive, at, nil
	return nil
}

type SessionParticipantRole string
type SessionParticipantStatus string

const (
	SessionParticipantOwner   SessionParticipantRole   = "owner"
	SessionParticipantMember  SessionParticipantRole   = "member"
	SessionParticipantViewer  SessionParticipantRole   = "viewer"
	SessionParticipantActive  SessionParticipantStatus = "active"
	SessionParticipantRemoved SessionParticipantStatus = "removed"
)

type SessionParticipant struct {
	TenantID  TenantID                 `json:"tenant_id"`
	SessionID SessionID                `json:"session_id"`
	UserID    UserID                   `json:"user_id"`
	Role      SessionParticipantRole   `json:"role"`
	Status    SessionParticipantStatus `json:"status"`
	CreatedAt time.Time                `json:"created_at"`
	UpdatedAt time.Time                `json:"updated_at"`
}

func (participant SessionParticipant) Validate() error {
	if err := participant.TenantID.Validate(); err != nil {
		return err
	}
	if err := participant.SessionID.Validate(); err != nil {
		return err
	}
	if err := participant.UserID.Validate(); err != nil {
		return err
	}
	switch participant.Role {
	case SessionParticipantOwner, SessionParticipantMember, SessionParticipantViewer:
	default:
		return ValidationError{Field: "session_participant.role", Reason: "is unknown"}
	}
	switch participant.Status {
	case SessionParticipantActive, SessionParticipantRemoved:
	default:
		return ValidationError{Field: "session_participant.status", Reason: "is unknown"}
	}
	if participant.CreatedAt.IsZero() || participant.UpdatedAt.Before(participant.CreatedAt) {
		return ValidationError{Field: "session_participant.updated_at", Reason: "must not be before created_at"}
	}
	return nil
}

func (participant SessionParticipant) Authorize(tenantID TenantID, sessionID SessionID, userID UserID, write bool) error {
	if err := participant.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(participant.TenantID, tenantID); err != nil {
		return err
	}
	if participant.SessionID != sessionID || participant.UserID != userID || participant.Status != SessionParticipantActive {
		return ValidationError{Field: "session_participant", Reason: "does not grant access to this user and session"}
	}
	if write && participant.Role == SessionParticipantViewer {
		return ValidationError{Field: "session_participant.role", Reason: "does not grant write access"}
	}
	return nil
}

type FrontendBinding struct {
	ID                     FrontendBindingID `json:"id"`
	TenantID               TenantID          `json:"tenant_id"`
	Frontend               Frontend          `json:"frontend"`
	ExternalConversationID string            `json:"external_conversation_id"`
	SessionID              SessionID         `json:"session_id"`
	Revision               uint64            `json:"revision"`
	CreatedAt              time.Time         `json:"created_at"`
	UpdatedAt              time.Time         `json:"updated_at"`
}

func (binding FrontendBinding) Validate() error {
	if err := binding.ID.Validate(); err != nil {
		return err
	}
	if err := binding.TenantID.Validate(); err != nil {
		return err
	}
	if err := binding.Frontend.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(binding.ExternalConversationID) == "" {
		return ValidationError{Field: "frontend_binding.external_conversation_id", Reason: "must not be empty"}
	}
	if err := binding.SessionID.Validate(); err != nil {
		return err
	}
	if binding.Revision == 0 {
		return ValidationError{Field: "frontend_binding.revision", Reason: "must be positive"}
	}
	if binding.CreatedAt.IsZero() || binding.UpdatedAt.Before(binding.CreatedAt) {
		return ValidationError{Field: "frontend_binding.updated_at", Reason: "must not be before created_at"}
	}
	return nil
}

type StaleBindingError struct{ Expected, Actual uint64 }

func (err StaleBindingError) Error() string {
	return fmt.Sprintf("stale frontend binding revision: expected %d, got %d", err.Expected, err.Actual)
}

func (binding *FrontendBinding) Switch(expected uint64, sessionID SessionID, at time.Time) error {
	if binding == nil {
		return ValidationError{Field: "frontend_binding", Reason: "must not be nil"}
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	if binding.Revision != expected {
		return StaleBindingError{Expected: expected, Actual: binding.Revision}
	}
	if err := sessionID.Validate(); err != nil {
		return err
	}
	if at.Before(binding.UpdatedAt) {
		return ValidationError{Field: "frontend_binding.updated_at", Reason: "switch time must not move backwards"}
	}
	binding.SessionID, binding.Revision, binding.UpdatedAt = sessionID, binding.Revision+1, at
	return nil
}

type SessionEventKind string

const (
	SessionEventUserMessage      SessionEventKind = "user_message"
	SessionEventAssistantMessage SessionEventKind = "assistant_message"
	SessionEventToolCall         SessionEventKind = "tool_call"
	SessionEventToolResult       SessionEventKind = "tool_result"
	SessionEventSystemNotice     SessionEventKind = "system_notice"
)

func (kind SessionEventKind) Valid() bool {
	switch kind {
	case SessionEventUserMessage, SessionEventAssistantMessage, SessionEventToolCall, SessionEventToolResult, SessionEventSystemNotice:
		return true
	default:
		return false
	}
}

type SessionEvent struct {
	ID             SessionEventID   `json:"id"`
	TenantID       TenantID         `json:"tenant_id"`
	SessionID      SessionID        `json:"session_id"`
	Sequence       uint64           `json:"sequence"`
	Kind           SessionEventKind `json:"kind"`
	AuthorUserID   *UserID          `json:"author_user_id,omitempty"`
	RunID          *RunID           `json:"run_id,omitempty"`
	IdempotencyKey IdempotencyKey   `json:"idempotency_key"`
	Payload        BlobRef          `json:"payload"`
	CreatedAt      time.Time        `json:"created_at"`
}

func (event SessionEvent) Validate() error {
	if err := event.ID.Validate(); err != nil {
		return err
	}
	if err := event.TenantID.Validate(); err != nil {
		return err
	}
	if err := event.SessionID.Validate(); err != nil {
		return err
	}
	if event.Sequence == 0 {
		return ValidationError{Field: "session_event.sequence", Reason: "must be positive"}
	}
	if !event.Kind.Valid() {
		return ValidationError{Field: "session_event.kind", Reason: "is unknown"}
	}
	if event.AuthorUserID != nil {
		if err := event.AuthorUserID.Validate(); err != nil {
			return err
		}
	}
	if event.RunID != nil {
		if err := event.RunID.Validate(); err != nil {
			return err
		}
	}
	if event.Kind == SessionEventUserMessage && event.AuthorUserID == nil {
		return ValidationError{Field: "session_event.author_user_id", Reason: "is required for a user message"}
	}
	if (event.Kind == SessionEventAssistantMessage || event.Kind == SessionEventToolCall || event.Kind == SessionEventToolResult) && event.RunID == nil {
		return ValidationError{Field: "session_event.run_id", Reason: "is required for assistant and tool events"}
	}
	if err := event.IdempotencyKey.Validate(); err != nil {
		return err
	}
	if err := event.Payload.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(event.TenantID, event.Payload.TenantID); err != nil {
		return err
	}
	if event.CreatedAt.IsZero() {
		return ValidationError{Field: "session_event.created_at", Reason: "must not be zero"}
	}
	return nil
}

var ErrEventIdempotencyConflict = errors.New("session event idempotency conflict")

// AppendSessionEvent applies the in-memory invariant mirrored by persistence:
// exact retries are no-ops and new events must take the next sequence.
func AppendSessionEvent(session *Session, event SessionEvent, existing *SessionEvent) (bool, error) {
	if session == nil {
		return false, ValidationError{Field: "session", Reason: "must not be nil"}
	}
	if err := session.Validate(); err != nil {
		return false, err
	}
	if err := event.Validate(); err != nil {
		return false, err
	}
	if err := EnsureSameTenant(session.TenantID, event.TenantID); err != nil {
		return false, err
	}
	if session.ID != event.SessionID {
		return false, ValidationError{Field: "session_event.session_id", Reason: "must reference the owning session"}
	}
	if existing != nil {
		if sameSessionEvent(*existing, event) {
			return false, nil
		}
		return false, ErrEventIdempotencyConflict
	}
	if session.Status != SessionActive {
		return false, ValidationError{Field: "session.status", Reason: "must be active to append an event"}
	}
	if event.Sequence != session.LastEventSequence+1 {
		return false, ValidationError{Field: "session_event.sequence", Reason: "must be exactly the next session sequence"}
	}
	if event.CreatedAt.Before(session.UpdatedAt) {
		return false, ValidationError{Field: "session_event.created_at", Reason: "must not move session time backwards"}
	}
	session.LastEventSequence, session.UpdatedAt = event.Sequence, event.CreatedAt
	return true, nil
}

func sameSessionEvent(left, right SessionEvent) bool {
	return left.ID == right.ID &&
		left.TenantID == right.TenantID &&
		left.SessionID == right.SessionID &&
		left.Sequence == right.Sequence &&
		left.Kind == right.Kind &&
		equalUserID(left.AuthorUserID, right.AuthorUserID) &&
		equalRunID(left.RunID, right.RunID) &&
		left.IdempotencyKey == right.IdempotencyKey &&
		left.Payload == right.Payload &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func equalUserID(left, right *UserID) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func equalRunID(left, right *RunID) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

type SessionSnapshot struct {
	ID              SessionSnapshotID `json:"id"`
	TenantID        TenantID          `json:"tenant_id"`
	SessionID       SessionID         `json:"session_id"`
	Version         uint64            `json:"version"`
	ThroughSequence uint64            `json:"through_sequence"`
	Payload         BlobRef           `json:"payload"`
	CreatedAt       time.Time         `json:"created_at"`
}

func (snapshot SessionSnapshot) Validate() error {
	if err := snapshot.ID.Validate(); err != nil {
		return err
	}
	if err := snapshot.TenantID.Validate(); err != nil {
		return err
	}
	if err := snapshot.SessionID.Validate(); err != nil {
		return err
	}
	if snapshot.Version == 0 {
		return ValidationError{Field: "session_snapshot.version", Reason: "must be positive"}
	}
	if err := snapshot.Payload.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(snapshot.TenantID, snapshot.Payload.TenantID); err != nil {
		return err
	}
	if snapshot.CreatedAt.IsZero() {
		return ValidationError{Field: "session_snapshot.created_at", Reason: "must not be zero"}
	}
	return nil
}

type SessionContextInput struct {
	TenantID  TenantID         `json:"tenant_id"`
	SessionID SessionID        `json:"session_id"`
	Snapshot  *SessionSnapshot `json:"snapshot,omitempty"`
	Events    []SessionEvent   `json:"events"`
}

func (input SessionContextInput) Validate() error {
	if err := input.TenantID.Validate(); err != nil {
		return err
	}
	if err := input.SessionID.Validate(); err != nil {
		return err
	}
	next := uint64(1)
	if input.Snapshot != nil {
		if err := input.Snapshot.Validate(); err != nil {
			return err
		}
		if err := EnsureSameTenant(input.TenantID, input.Snapshot.TenantID); err != nil {
			return err
		}
		if input.Snapshot.SessionID != input.SessionID {
			return ValidationError{Field: "session_context.snapshot", Reason: "must belong to the requested session"}
		}
		next = input.Snapshot.ThroughSequence + 1
	}
	for _, event := range input.Events {
		if err := event.Validate(); err != nil {
			return err
		}
		if event.TenantID != input.TenantID || event.SessionID != input.SessionID {
			return ValidationError{Field: "session_context.events", Reason: "must belong to the requested tenant and session"}
		}
		if event.Sequence != next {
			return ValidationError{Field: "session_context.events", Reason: "must form a contiguous ordered range after the snapshot"}
		}
		next++
	}
	return nil
}
