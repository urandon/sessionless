// Package telegramdelivery drains the durable Telegram delivery outbox.
package telegramdelivery

import (
	"context"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

type Config struct {
	BatchSize   uint64
	MaxAttempts uint32
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
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
	if clock == nil || store == nil || client == nil {
		return nil, fmt.Errorf("Telegram sender dependencies must not be nil")
	}
	return &Sender{config: config, clock: clock, store: store, client: client}, nil
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
			if err := sender.send(ctx, claimed, now); err != nil {
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
) error {
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
		TenantID: delivery.TenantID, DeliveryID: delivery.ID,
		Chat: delivery.Chat, ReplyToMessageID: delivery.ReplyToMessageID,
		Payload: delivery.Payload, Artifacts: artifacts,
		IdempotencyKey: delivery.IdempotencyKey,
	})
	if err != nil {
		return sender.fail(ctx, delivery, now, err)
	}
	return sender.store.TransitionTelegramDelivery(
		ctx, delivery.TenantID, delivery.ID, domain.DeliverySent, now, nil,
	)
}

func (sender *Sender) fail(
	ctx context.Context,
	delivery domain.TelegramDeliveryOutbox,
	now time.Time,
	cause error,
) error {
	if delivery.AttemptCount >= sender.config.MaxAttempts {
		if err := sender.store.TransitionTelegramDelivery(
			ctx, delivery.TenantID, delivery.ID, domain.DeliveryFailed, now, nil,
		); err != nil {
			return err
		}
		return nil
	}
	retryAt := now.Add(sender.backoff(delivery.AttemptCount))
	if err := sender.store.TransitionTelegramDelivery(
		ctx, delivery.TenantID, delivery.ID, domain.DeliveryRetryWait, now, &retryAt,
	); err != nil {
		return err
	}
	_ = cause // content is intentionally left to metadata-only caller logging.
	return nil
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
