// Package telegramdelivery drains the durable Telegram delivery outbox.
package telegramdelivery

import (
	"context"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

type Config struct {
	BatchSize      uint64
	MaxAttempts    uint32
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
	WakeRetryDelay time.Duration
}

type WakeResult struct {
	Outcome string
	Code    string
}

type Sender struct {
	config Config
	clock  ports.Clock
	store  ports.TelegramDeliveryStore
	client ports.TelegramClient
}

func NewSender(
	config Config,
	clock ports.Clock,
	store ports.TelegramDeliveryStore,
	client ports.TelegramClient,
) (*Sender, error) {
	if config.BatchSize == 0 {
		config.BatchSize = 25
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = 5
	}
	if config.BaseBackoff <= 0 {
		config.BaseBackoff = 5 * time.Second
	}
	if config.MaxBackoff <= 0 {
		config.MaxBackoff = 5 * time.Minute
	}
	if config.WakeRetryDelay <= 0 {
		config.WakeRetryDelay = time.Minute
	}
	if clock == nil || store == nil || client == nil {
		return nil, fmt.Errorf("Telegram sender dependencies must not be nil")
	}
	return &Sender{config: config, clock: clock, store: store, client: client}, nil
}

// RunWake resolves one Telegram delivery hint with a tenant/delivery point
// read. Missing and terminal outboxes are acknowledged as duplicate no-ops.
func (sender *Sender) RunWake(ctx context.Context, wakeQueue ports.Queue) (WakeResult, error) {
	message, err := wakeQueue.Receive(ctx)
	if err != nil {
		return WakeResult{}, err
	}
	if message.Envelope.Kind != queuecontract.KindWakeTelegram {
		return WakeResult{}, wakeQueue.DeadLetter(ctx, message.ReceiptHandle, "unexpected_kind")
	}
	deliveryID := domain.TelegramDeliveryID(message.Envelope.SubjectID)
	if err := deliveryID.Validate(); err != nil {
		return WakeResult{}, wakeQueue.DeadLetter(ctx, message.ReceiptHandle, "invalid_delivery_id")
	}
	now := sender.clock.Now().UTC()
	delivery, found, err := sender.store.GetTelegramDelivery(
		ctx, message.Envelope.TenantID, deliveryID,
	)
	if err != nil {
		return WakeResult{}, err
	}
	if !found || delivery.Status.Terminal() {
		if err := wakeQueue.Ack(ctx, message.ReceiptHandle); err != nil {
			return WakeResult{}, err
		}
		return WakeResult{Outcome: "noop", Code: "missing_or_terminal"}, nil
	}
	if delivery.NextAttemptAt != nil && delivery.NextAttemptAt.After(now) {
		if err := wakeQueue.Retry(ctx, message.ReceiptHandle, delivery.NextAttemptAt.Sub(now)); err != nil {
			return WakeResult{}, err
		}
		return WakeResult{Outcome: "retry", Code: "retry_wait"}, nil
	}
	claimed, ok, err := sender.store.ClaimTelegramDelivery(
		ctx, message.Envelope.TenantID, deliveryID, now,
	)
	if err != nil {
		return WakeResult{}, err
	}
	if !ok {
		if err := wakeQueue.Retry(ctx, message.ReceiptHandle, sender.config.WakeRetryDelay); err != nil {
			return WakeResult{}, err
		}
		return WakeResult{Outcome: "retry", Code: "claim_busy"}, nil
	}
	retryAt, err := sender.send(ctx, claimed, now)
	if err != nil {
		return WakeResult{}, err
	}
	if retryAt != nil {
		if err := wakeQueue.Retry(ctx, message.ReceiptHandle, retryAt.Sub(now)); err != nil {
			return WakeResult{}, err
		}
		return WakeResult{Outcome: "retry", Code: "delivery_retry_wait"}, nil
	}
	if err := wakeQueue.Ack(ctx, message.ReceiptHandle); err != nil {
		return WakeResult{}, err
	}
	return WakeResult{Outcome: "sent", Code: "delivered_or_terminal"}, nil
}

func (sender *Sender) RunOnce(ctx context.Context) (processed int, err error) {
	now := sender.clock.Now().UTC()
	for bucket := uint32(0); bucket < ydbpartition.BucketCountV1; bucket++ {
		ready, err := sender.store.ListReadyTelegramDeliveries(
			ctx, bucket, now, sender.config.BatchSize,
		)
		if err != nil {
			return processed, err
		}
		for _, candidate := range ready {
			claimed, ok, err := sender.store.ClaimTelegramDelivery(
				ctx, candidate.TenantID, candidate.DeliveryID, now,
			)
			if err != nil {
				return processed, err
			}
			if !ok {
				continue
			}
			processed++
			if _, err := sender.send(ctx, claimed, now); err != nil {
				return processed, err
			}
		}
	}
	return processed, nil
}

func (sender *Sender) send(
	ctx context.Context,
	delivery domain.TelegramDeliveryOutbox,
	now time.Time,
) (*time.Time, error) {
	var artifacts []domain.Artifact
	if delivery.ArtifactManifestID != nil {
		manifest, found, err := sender.store.GetArtifactManifest(
			ctx, delivery.TenantID, *delivery.ArtifactManifestID,
		)
		if err != nil {
			return sender.fail(ctx, delivery, now, err)
		}
		if !found {
			return sender.fail(ctx, delivery, now, fmt.Errorf("artifact manifest not found"))
		}
		artifacts = manifest.Artifacts
	}
	_, err := sender.client.Send(ctx, ports.TelegramSendRequest{
		TenantID: delivery.TenantID, RunID: delivery.RunID, DeliveryID: delivery.ID,
		Chat: delivery.Chat, ReplyToMessageID: delivery.ReplyToMessageID,
		Payload: delivery.Payload, Text: delivery.Text, Artifacts: artifacts,
		IdempotencyKey: delivery.IdempotencyKey,
	})
	if err != nil {
		return sender.fail(ctx, delivery, now, err)
	}
	if err := sender.store.TransitionTelegramDelivery(
		ctx, delivery.TenantID, delivery.ID, domain.DeliverySent, now, nil,
	); err != nil {
		return nil, err
	}
	return nil, nil
}

func (sender *Sender) fail(
	ctx context.Context,
	delivery domain.TelegramDeliveryOutbox,
	now time.Time,
	cause error,
) (*time.Time, error) {
	if delivery.AttemptCount >= sender.config.MaxAttempts {
		if err := sender.store.TransitionTelegramDelivery(
			ctx, delivery.TenantID, delivery.ID, domain.DeliveryFailed, now, nil,
		); err != nil {
			return nil, err
		}
		return nil, nil
	}
	retryAt := now.Add(sender.backoff(delivery.AttemptCount))
	if err := sender.store.TransitionTelegramDelivery(
		ctx, delivery.TenantID, delivery.ID, domain.DeliveryRetryWait, now, &retryAt,
	); err != nil {
		return nil, err
	}
	_ = cause // content is intentionally left to metadata-only caller logging.
	return &retryAt, nil
}

func (sender *Sender) backoff(attempt uint32) time.Duration {
	delay := sender.config.BaseBackoff
	for current := uint32(1); current < attempt && delay < sender.config.MaxBackoff; current++ {
		delay *= 2
	}
	if delay > sender.config.MaxBackoff {
		return sender.config.MaxBackoff
	}
	return delay
}
