package domain

import "time"

// ContextEpoch identifies an explicit frontend-visible context generation.
// Epoch 1 is the initial context; it changes only through a clean-context event.
type ContextEpoch uint64

const InitialContextEpoch ContextEpoch = 1

func (epoch ContextEpoch) Validate() error {
	if epoch < InitialContextEpoch {
		return ValidationError{Field: "context_epoch", Reason: "must be at least 1"}
	}
	return nil
}

func (epoch ContextEpoch) Next() (ContextEpoch, error) {
	if err := epoch.Validate(); err != nil {
		return 0, err
	}
	if epoch == ^ContextEpoch(0) {
		return 0, ValidationError{Field: "context_epoch", Reason: "cannot overflow"}
	}
	return epoch + 1, nil
}

// CleanContextEvent records the explicit user action that moves a
// conversation to a fresh epoch. It does not delete frontend history.
type CleanContextEvent struct {
	TenantID         TenantID        `json:"tenant_id"`
	Conversation     ConversationRef `json:"conversation"`
	RequestedBy      ActorRef        `json:"requested_by"`
	PreviousEpoch    ContextEpoch    `json:"previous_epoch"`
	NewEpoch         ContextEpoch    `json:"new_epoch"`
	TriggerMessageID string          `json:"trigger_message_id"`
	IdempotencyKey   IdempotencyKey  `json:"idempotency_key"`
	RequestedAt      time.Time       `json:"requested_at"`
}

func (event CleanContextEvent) Validate() error {
	if err := event.TenantID.Validate(); err != nil {
		return err
	}
	if err := event.Conversation.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(event.TenantID, event.Conversation.TenantID); err != nil {
		return err
	}
	if err := event.RequestedBy.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(event.TenantID, event.RequestedBy.TenantID); err != nil {
		return err
	}
	if event.Conversation.Frontend != event.RequestedBy.Frontend {
		return ValidationError{Field: "requested_by.frontend", Reason: "must match the conversation frontend"}
	}
	if err := event.PreviousEpoch.Validate(); err != nil {
		return err
	}
	next, err := event.PreviousEpoch.Next()
	if err != nil {
		return err
	}
	if event.NewEpoch != next {
		return ValidationError{Field: "new_epoch", Reason: "must increment previous_epoch by exactly one"}
	}
	if event.TriggerMessageID == "" {
		return ValidationError{Field: "trigger_message_id", Reason: "must not be empty"}
	}
	if err := event.IdempotencyKey.Validate(); err != nil {
		return err
	}
	if event.RequestedAt.IsZero() {
		return ValidationError{Field: "requested_at", Reason: "must not be zero"}
	}
	return nil
}
