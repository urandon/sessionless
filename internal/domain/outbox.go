package domain

import (
	"strings"
	"time"
	"unicode/utf8"
)

type DispatchStatus string

const (
	DispatchPending   DispatchStatus = "pending"
	DispatchPublished DispatchStatus = "published"
	DispatchCancelled DispatchStatus = "cancelled"
)

func (status DispatchStatus) Valid() bool {
	return status == DispatchPending || status == DispatchPublished || status == DispatchCancelled
}

func CanTransitionDispatch(from, to DispatchStatus) bool {
	return from == DispatchPending && (to == DispatchPublished || to == DispatchCancelled)
}

type DispatchOutbox struct {
	ID             DispatchOutboxID `json:"id"`
	TenantID       TenantID         `json:"tenant_id"`
	RunID          RunID            `json:"run_id"`
	AttemptID      AttemptID        `json:"attempt_id"`
	Status         DispatchStatus   `json:"status"`
	IdempotencyKey IdempotencyKey   `json:"idempotency_key"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
}

func (outbox DispatchOutbox) ValidateForAttempt(run Run, attempt Attempt) error {
	if err := attempt.ValidateForRun(run); err != nil {
		return err
	}
	if err := outbox.ID.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(run.TenantID, outbox.TenantID); err != nil {
		return err
	}
	if outbox.RunID != run.ID || outbox.AttemptID != attempt.ID {
		return ValidationError{Field: "dispatch_outbox", Reason: "must reference the owning run and attempt"}
	}
	if !outbox.Status.Valid() {
		return ValidationError{Field: "dispatch_outbox.status", Reason: "is unknown"}
	}
	if err := outbox.IdempotencyKey.Validate(); err != nil {
		return err
	}
	if outbox.CreatedAt.IsZero() || outbox.UpdatedAt.Before(outbox.CreatedAt) {
		return ValidationError{Field: "dispatch_outbox.updated_at", Reason: "must not be before a non-zero created_at"}
	}
	return nil
}

func (outbox *DispatchOutbox) Transition(to DispatchStatus, at time.Time) error {
	if outbox == nil {
		return ValidationError{Field: "dispatch_outbox", Reason: "must not be nil"}
	}
	if !CanTransitionDispatch(outbox.Status, to) {
		return ValidationError{Field: "dispatch_outbox.status", Reason: "transition is not allowed"}
	}
	if at.Before(outbox.UpdatedAt) {
		return ValidationError{Field: "dispatch_outbox.updated_at", Reason: "transition time must not move backwards"}
	}
	outbox.Status = to
	outbox.UpdatedAt = at
	return nil
}

type DeliveryStatus string

const (
	DeliveryPending   DeliveryStatus = "pending"
	DeliverySending   DeliveryStatus = "sending"
	DeliveryRetryWait DeliveryStatus = "retry_wait"
	DeliverySent      DeliveryStatus = "sent"
	DeliveryFailed    DeliveryStatus = "failed"
	DeliveryCancelled DeliveryStatus = "cancelled"
)

func (status DeliveryStatus) Valid() bool {
	switch status {
	case DeliveryPending, DeliverySending, DeliveryRetryWait, DeliverySent, DeliveryFailed, DeliveryCancelled:
		return true
	default:
		return false
	}
}

func (status DeliveryStatus) Terminal() bool {
	return status == DeliverySent || status == DeliveryFailed || status == DeliveryCancelled
}

func CanTransitionDelivery(from, to DeliveryStatus) bool {
	switch from {
	case DeliveryPending:
		return to == DeliverySending || to == DeliveryCancelled
	case DeliverySending:
		return to == DeliverySent || to == DeliveryRetryWait || to == DeliveryFailed || to == DeliveryCancelled
	case DeliveryRetryWait:
		return to == DeliverySending || to == DeliveryFailed || to == DeliveryCancelled
	default:
		return false
	}
}

type TelegramDeliveryOutbox struct {
	ID                 TelegramDeliveryID  `json:"id"`
	TenantID           TenantID            `json:"tenant_id"`
	RunID              RunID               `json:"run_id"`
	Chat               TelegramChatRef     `json:"chat"`
	ReplyToMessageID   int64               `json:"reply_to_message_id"`
	Payload            BlobRef             `json:"payload"`
	Text               string              `json:"text,omitempty"`
	ArtifactManifestID *ArtifactManifestID `json:"artifact_manifest_id,omitempty"`
	Status             DeliveryStatus      `json:"status"`
	IdempotencyKey     IdempotencyKey      `json:"idempotency_key"`
	AttemptCount       uint32              `json:"attempt_count"`
	NextAttemptAt      *time.Time          `json:"next_attempt_at,omitempty"`
	CreatedAt          time.Time           `json:"created_at"`
	UpdatedAt          time.Time           `json:"updated_at"`
}

func (delivery TelegramDeliveryOutbox) ValidateForRun(run Run) error {
	if err := run.Validate(); err != nil {
		return err
	}
	if err := delivery.ID.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(run.TenantID, delivery.TenantID); err != nil {
		return err
	}
	if delivery.RunID != run.ID {
		return ValidationError{Field: "telegram_delivery.run_id", Reason: "must reference the owning run"}
	}
	if err := delivery.Chat.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(run.TenantID, delivery.Chat.TenantID); err != nil {
		return err
	}
	if delivery.ReplyToMessageID == 0 {
		return ValidationError{Field: "telegram_delivery.reply_to_message_id", Reason: "must not be zero"}
	}
	hasText := strings.TrimSpace(delivery.Text) != ""
	hasPayload := delivery.Payload.Key != ""
	if hasText == hasPayload {
		return ValidationError{
			Field:  "telegram_delivery.content",
			Reason: "must contain exactly one of inline text or a blob payload",
		}
	}
	if hasText {
		if utf8.RuneCountInString(delivery.Text) > 4096 {
			return ValidationError{
				Field:  "telegram_delivery.text",
				Reason: "must not exceed 4096 Unicode characters",
			}
		}
	} else {
		if err := delivery.Payload.Validate(); err != nil {
			return err
		}
		if err := EnsureSameTenant(run.TenantID, delivery.Payload.TenantID); err != nil {
			return err
		}
	}
	if delivery.ArtifactManifestID != nil {
		if err := delivery.ArtifactManifestID.Validate(); err != nil {
			return err
		}
	}
	if !delivery.Status.Valid() {
		return ValidationError{Field: "telegram_delivery.status", Reason: "is unknown"}
	}
	if err := delivery.IdempotencyKey.Validate(); err != nil {
		return err
	}
	if delivery.Status == DeliveryRetryWait && delivery.NextAttemptAt == nil {
		return ValidationError{Field: "telegram_delivery.next_attempt_at", Reason: "is required while waiting to retry"}
	}
	if delivery.Status != DeliveryRetryWait && delivery.NextAttemptAt != nil {
		return ValidationError{Field: "telegram_delivery.next_attempt_at", Reason: "is allowed only while waiting to retry"}
	}
	if delivery.CreatedAt.IsZero() || delivery.UpdatedAt.Before(delivery.CreatedAt) {
		return ValidationError{Field: "telegram_delivery.updated_at", Reason: "must not be before a non-zero created_at"}
	}
	return nil
}

func (delivery *TelegramDeliveryOutbox) Transition(to DeliveryStatus, at time.Time, retryAt *time.Time) error {
	if delivery == nil {
		return ValidationError{Field: "telegram_delivery", Reason: "must not be nil"}
	}
	if !CanTransitionDelivery(delivery.Status, to) {
		return ValidationError{Field: "telegram_delivery.status", Reason: "transition is not allowed"}
	}
	if at.Before(delivery.UpdatedAt) {
		return ValidationError{Field: "telegram_delivery.updated_at", Reason: "transition time must not move backwards"}
	}
	if to == DeliveryRetryWait {
		if retryAt == nil || !retryAt.After(at) {
			return ValidationError{Field: "telegram_delivery.next_attempt_at", Reason: "must be after the transition time"}
		}
		delivery.NextAttemptAt = retryAt
	} else {
		delivery.NextAttemptAt = nil
	}
	if to == DeliverySending {
		delivery.AttemptCount++
	}
	delivery.Status = to
	delivery.UpdatedAt = at
	return nil
}
