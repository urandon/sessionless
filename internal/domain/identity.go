// Package domain defines harness-neutral business identities, state machines,
// and invariants for the Sessionless control plane.
package domain

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const maxOpaqueIDLength = 160

var opaqueIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)

type (
	TenantID                 string
	UserID                   string
	SessionID                string
	SessionEventID           string
	FrontendBindingID        string
	SessionSnapshotID        string
	TenantInvitationID       string
	UploadIntentID           string
	ConversationID           string
	ActorID                  string
	RunID                    string
	AttemptID                string
	LeaseID                  string
	CheckpointID             string
	QuotaReservationID       string
	UsageObservationID       string
	ArtifactManifestID       string
	DispatchOutboxID         string
	TelegramDeliveryID       string
	SubscriptionConnectionID string
	IdempotencyKey           string
	MessageID                string
)

func (id TenantID) Validate() error       { return ValidateOpaqueID("tenant_id", string(id)) }
func (id UserID) Validate() error         { return ValidateOpaqueID("user_id", string(id)) }
func (id SessionID) Validate() error      { return ValidateOpaqueID("session_id", string(id)) }
func (id SessionEventID) Validate() error { return ValidateOpaqueID("session_event_id", string(id)) }
func (id FrontendBindingID) Validate() error {
	return ValidateOpaqueID("frontend_binding_id", string(id))
}
func (id SessionSnapshotID) Validate() error {
	return ValidateOpaqueID("session_snapshot_id", string(id))
}
func (id TenantInvitationID) Validate() error {
	return ValidateOpaqueID("tenant_invitation_id", string(id))
}
func (id UploadIntentID) Validate() error {
	return ValidateOpaqueID("upload_intent_id", string(id))
}
func (id ConversationID) Validate() error { return ValidateOpaqueID("conversation_id", string(id)) }
func (id ActorID) Validate() error        { return ValidateOpaqueID("actor_id", string(id)) }
func (id RunID) Validate() error          { return ValidateOpaqueID("run_id", string(id)) }
func (id AttemptID) Validate() error      { return ValidateOpaqueID("attempt_id", string(id)) }
func (id LeaseID) Validate() error        { return ValidateOpaqueID("lease_id", string(id)) }
func (id CheckpointID) Validate() error   { return ValidateOpaqueID("checkpoint_id", string(id)) }
func (id QuotaReservationID) Validate() error {
	return ValidateOpaqueID("quota_reservation_id", string(id))
}
func (id UsageObservationID) Validate() error {
	return ValidateOpaqueID("usage_observation_id", string(id))
}
func (id ArtifactManifestID) Validate() error {
	return ValidateOpaqueID("artifact_manifest_id", string(id))
}
func (id DispatchOutboxID) Validate() error {
	return ValidateOpaqueID("dispatch_outbox_id", string(id))
}
func (id TelegramDeliveryID) Validate() error {
	return ValidateOpaqueID("telegram_delivery_id", string(id))
}
func (id SubscriptionConnectionID) Validate() error {
	return ValidateOpaqueID("subscription_connection_id", string(id))
}
func (id IdempotencyKey) Validate() error { return ValidateOpaqueID("idempotency_key", string(id)) }
func (id MessageID) Validate() error      { return ValidateOpaqueID("message_id", string(id)) }

// ValidateOpaqueID enforces the queue-safe identifier grammar shared by domain
// and transport contracts. IDs are deliberately opaque and carry no payload.
func ValidateOpaqueID(field, value string) error {
	if value == "" {
		return ValidationError{Field: field, Reason: "must not be empty"}
	}
	if len(value) > maxOpaqueIDLength {
		return ValidationError{Field: field, Reason: fmt.Sprintf("must not exceed %d bytes", maxOpaqueIDLength)}
	}
	if !opaqueIDPattern.MatchString(value) {
		return ValidationError{Field: field, Reason: "must use only ASCII letters, digits, dot, underscore, colon, or dash"}
	}
	return nil
}

// TenantMismatchError is returned before a cross-tenant reference reaches a
// persistence or execution adapter.
type TenantMismatchError struct {
	Expected TenantID
	Actual   TenantID
}

func (e TenantMismatchError) Error() string {
	return fmt.Sprintf("tenant mismatch: expected %q, got %q", e.Expected, e.Actual)
}

// EnsureSameTenant rejects cross-tenant composition at domain boundaries.
func EnsureSameTenant(expected, actual TenantID) error {
	if err := expected.Validate(); err != nil {
		return err
	}
	if err := actual.Validate(); err != nil {
		return err
	}
	if expected != actual {
		return TenantMismatchError{Expected: expected, Actual: actual}
	}
	return nil
}

type Frontend string

const FrontendTelegram Frontend = "telegram"

func (frontend Frontend) Validate() error {
	return ValidateOpaqueID("frontend", string(frontend))
}

// ConversationRef identifies a frontend-owned conversation without making a
// transport-specific identifier the core scheduling identity.
type ConversationRef struct {
	TenantID   TenantID       `json:"tenant_id"`
	Frontend   Frontend       `json:"frontend"`
	ExternalID string         `json:"external_id"`
	ID         ConversationID `json:"conversation_id"`
}

func (ref ConversationRef) Validate() error {
	if err := ref.TenantID.Validate(); err != nil {
		return err
	}
	if err := ref.Frontend.Validate(); err != nil {
		return err
	}
	if err := ref.ID.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(ref.ExternalID) == "" {
		return ValidationError{Field: "external_id", Reason: "must not be empty"}
	}
	if len(ref.ExternalID) > maxOpaqueIDLength {
		return ValidationError{Field: "external_id", Reason: fmt.Sprintf("must not exceed %d bytes", maxOpaqueIDLength)}
	}
	return nil
}

type ActorRef struct {
	TenantID   TenantID `json:"tenant_id"`
	Frontend   Frontend `json:"frontend"`
	ExternalID string   `json:"external_id"`
	ID         ActorID  `json:"actor_id"`
}

func (ref ActorRef) Validate() error {
	if err := ref.TenantID.Validate(); err != nil {
		return err
	}
	if err := ref.Frontend.Validate(); err != nil {
		return err
	}
	if err := ref.ID.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(ref.ExternalID) == "" {
		return ValidationError{Field: "external_id", Reason: "must not be empty"}
	}
	return nil
}

// TelegramChatRef is the first frontend adapter identity. Negative chat IDs
// are valid for groups and channels; zero is never valid.
type TelegramChatRef struct {
	TenantID TenantID `json:"tenant_id"`
	ChatID   int64    `json:"chat_id"`
}

func (ref TelegramChatRef) Validate() error {
	if err := ref.TenantID.Validate(); err != nil {
		return err
	}
	if ref.ChatID == 0 {
		return ValidationError{Field: "chat_id", Reason: "must not be zero"}
	}
	return nil
}

func (ref TelegramChatRef) Conversation(id ConversationID) ConversationRef {
	return ConversationRef{
		TenantID:   ref.TenantID,
		Frontend:   FrontendTelegram,
		ExternalID: strconv.FormatInt(ref.ChatID, 10),
		ID:         id,
	}
}

type TelegramUserRef struct {
	TenantID TenantID `json:"tenant_id"`
	UserID   int64    `json:"user_id"`
}

func (ref TelegramUserRef) Validate() error {
	if err := ref.TenantID.Validate(); err != nil {
		return err
	}
	if ref.UserID <= 0 {
		return ValidationError{Field: "user_id", Reason: "must be positive"}
	}
	return nil
}

func (ref TelegramUserRef) Actor(id ActorID) ActorRef {
	return ActorRef{
		TenantID:   ref.TenantID,
		Frontend:   FrontendTelegram,
		ExternalID: strconv.FormatInt(ref.UserID, 10),
		ID:         id,
	}
}
