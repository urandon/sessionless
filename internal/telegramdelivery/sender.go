// Package telegramdelivery drains the durable Telegram delivery outbox.
package telegramdelivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
	blobs  ports.BlobStore
	client ports.TelegramClient
}

func NewSender(
	config Config,
	clock ports.Clock,
	store ports.TelegramDeliveryStore,
	blobs ports.BlobStore,
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
	if clock == nil || store == nil || blobs == nil || client == nil {
		return nil, fmt.Errorf("Telegram sender dependencies must not be nil")
	}
	return &Sender{config: config, clock: clock, store: store, blobs: blobs, client: client}, nil
}

// RunWake resolves one Telegram delivery hint with a tenant/delivery point
// read. Missing and terminal outboxes are acknowledged as duplicate no-ops.
func (sender *Sender) RunWake(ctx context.Context, wakeQueue ports.Queue) (WakeResult, error) {
	message, err := wakeQueue.Receive(ctx)
	if err != nil {
		return WakeResult{}, err
	}
	if message.Envelope.Kind != queuecontract.KindWakeTelegram &&
		message.Envelope.Kind != queuecontract.KindWakeProjection {
		return WakeResult{}, wakeQueue.DeadLetter(ctx, message.ReceiptHandle, "unexpected_kind")
	}
	if message.Envelope.Kind == queuecontract.KindWakeProjection {
		return sender.runProjectionWake(ctx, wakeQueue, message)
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
		if claimed.Status.Terminal() {
			if err := wakeQueue.Ack(ctx, message.ReceiptHandle); err != nil {
				return WakeResult{}, err
			}
			return WakeResult{Outcome: "noop", Code: "delivery_terminal"}, nil
		}
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
	var projectionErr error
	for bucket := uint32(0); bucket < ydbpartition.BucketCountV1; bucket++ {
		ready, err := sender.store.ListReadyTelegramProjections(
			ctx, bucket, now, sender.config.BatchSize,
		)
		if err != nil {
			return processed, err
		}
		for _, candidate := range ready {
			delivery, materialized, err := sender.materializeProjection(
				ctx, candidate.TenantID, candidate.ProjectionID, now,
			)
			if err != nil {
				if projectionErr == nil {
					projectionErr = err
				}
				continue
			}
			if !materialized {
				continue
			}
			claimed, ok, err := sender.store.ClaimTelegramDelivery(
				ctx, candidate.TenantID, delivery, now,
			)
			if err != nil {
				if projectionErr == nil {
					projectionErr = err
				}
				continue
			}
			if !ok {
				continue
			}
			processed++
			if _, err := sender.send(ctx, claimed, now); err != nil {
				if projectionErr == nil {
					projectionErr = err
				}
			}
		}
	}
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
	return processed, projectionErr
}

func (sender *Sender) runProjectionWake(
	ctx context.Context,
	wakeQueue ports.Queue,
	message ports.ReceivedMessage,
) (WakeResult, error) {
	runID := domain.RunID(message.Envelope.SubjectID)
	if err := runID.Validate(); err != nil {
		return WakeResult{}, wakeQueue.DeadLetter(ctx, message.ReceiptHandle, "invalid_run_id")
	}
	now := sender.clock.Now().UTC()
	ready, err := sender.store.ListRunTelegramProjections(
		ctx, message.Envelope.TenantID, runID, sender.config.BatchSize,
	)
	if err != nil {
		return WakeResult{}, err
	}
	processed := 0
	for _, candidate := range ready {
		_, _, err := sender.materializeProjection(
			ctx, candidate.TenantID, candidate.ProjectionID, now,
		)
		if err != nil {
			return WakeResult{}, err
		}
	}
	deliveries, err := sender.store.ListRunTelegramDeliveries(
		ctx, message.Envelope.TenantID, runID, sender.config.BatchSize,
	)
	if err != nil {
		return WakeResult{}, err
	}
	for _, candidate := range deliveries {
		delivery, found, err := sender.store.GetTelegramDelivery(
			ctx, candidate.TenantID, candidate.DeliveryID,
		)
		if err != nil {
			return WakeResult{}, err
		}
		if !found || delivery.Status.Terminal() {
			continue
		}
		if delivery.NextAttemptAt != nil && delivery.NextAttemptAt.After(now) {
			if err := wakeQueue.Retry(ctx, message.ReceiptHandle, delivery.NextAttemptAt.Sub(now)); err != nil {
				return WakeResult{}, err
			}
			return WakeResult{Outcome: "retry", Code: "retry_wait"}, nil
		}
		claimed, ok, err := sender.store.ClaimTelegramDelivery(
			ctx, candidate.TenantID, candidate.DeliveryID, now,
		)
		if err != nil {
			return WakeResult{}, err
		}
		if !ok {
			if claimed.Status.Terminal() {
				continue
			}
			if err := wakeQueue.Retry(ctx, message.ReceiptHandle, sender.config.WakeRetryDelay); err != nil {
				return WakeResult{}, err
			}
			return WakeResult{Outcome: "retry", Code: "claim_busy"}, nil
		}
		processed++
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
	}
	if err := wakeQueue.Ack(ctx, message.ReceiptHandle); err != nil {
		return WakeResult{}, err
	}
	if processed == 0 {
		return WakeResult{Outcome: "noop", Code: "missing_or_terminal"}, nil
	}
	return WakeResult{Outcome: "sent", Code: "projections_delivered"}, nil
}

func (sender *Sender) materializeProjection(
	ctx context.Context,
	tenantID domain.TenantID,
	projectionID domain.FrontendProjectionID,
	now time.Time,
) (domain.TelegramDeliveryID, bool, error) {
	prepared, err := sender.store.MaterializeTelegramProjection(ctx, tenantID, projectionID, nil, now)
	if err != nil {
		return "", false, err
	}
	if prepared.Outcome != ports.TelegramProjectionNeedsContent {
		return prepared.DeliveryID, prepared.Outcome == ports.TelegramProjectionMaterialized, nil
	}
	eventPayload, err := sender.readCanonicalBlob(ctx, tenantID, prepared.EventPayload)
	if err != nil {
		return "", false, err
	}
	triggerPayload, err := sender.readCanonicalBlob(ctx, tenantID, prepared.TriggerPayload)
	if err != nil {
		return "", false, err
	}
	content := ports.TelegramProjectionContent{
		EventPayload: prepared.EventPayload, TriggerPayload: prepared.TriggerPayload,
	}
	switch prepared.EventKind {
	case domain.SessionEventAssistantMessage:
		var envelope struct {
			Schema             string                    `json:"schema"`
			Summary            string                    `json:"summary"`
			ArtifactManifestID domain.ArtifactManifestID `json:"artifact_manifest_id"`
		}
		if err := decodeBoundedJSON(eventPayload, &envelope); err != nil {
			return "", false, fmt.Errorf("decode assistant event: %w", err)
		}
		if envelope.Schema != "sessionless.assistant-message.v1" || strings.TrimSpace(envelope.Summary) == "" ||
			utf8.RuneCountInString(envelope.Summary) > 4096 {
			return "", false, domain.ValidationError{Field: "assistant event", Reason: "has an unsupported schema or unbounded summary"}
		}
		if err := envelope.ArtifactManifestID.Validate(); err != nil {
			return "", false, err
		}
		content.ArtifactManifestID = &envelope.ArtifactManifestID
	case domain.SessionEventSystemNotice:
		var envelope struct {
			Schema    string `json:"schema"`
			Code      string `json:"code"`
			Cancelled bool   `json:"cancelled"`
		}
		if err := decodeBoundedJSON(eventPayload, &envelope); err != nil {
			return "", false, fmt.Errorf("decode terminal event: %w", err)
		}
		if envelope.Schema != "sessionless.run-terminal-notice.v1" || strings.TrimSpace(envelope.Code) == "" {
			return "", false, domain.ValidationError{Field: "terminal event", Reason: "has an unsupported schema or empty code"}
		}
	default:
		return "", false, domain.ValidationError{Field: "Telegram projection event", Reason: "is not supported"}
	}
	var trigger struct {
		Metadata map[string]string `json:"metadata"`
	}
	if err := json.Unmarshal(triggerPayload, &trigger); err != nil {
		return "", false, fmt.Errorf("decode trigger event: %w", err)
	}
	content.TriggerChatID, err = strconv.ParseInt(trigger.Metadata["telegram.chat_id"], 10, 64)
	if err != nil || content.TriggerChatID == 0 {
		return "", false, domain.ValidationError{Field: "trigger metadata telegram.chat_id", Reason: "must not be zero"}
	}
	if value := trigger.Metadata["telegram.message_id"]; value != "" {
		content.ReplyToMessageID, err = strconv.ParseInt(value, 10, 64)
		if err != nil || content.ReplyToMessageID <= 0 {
			return "", false, domain.ValidationError{Field: "trigger metadata telegram.message_id", Reason: "must be positive"}
		}
	}
	materialized, err := sender.store.MaterializeTelegramProjection(ctx, tenantID, projectionID, &content, now)
	if err != nil {
		return "", false, err
	}
	return materialized.DeliveryID, materialized.Outcome == ports.TelegramProjectionMaterialized, nil
}

func (sender *Sender) readCanonicalBlob(
	ctx context.Context,
	tenantID domain.TenantID,
	ref domain.BlobRef,
) ([]byte, error) {
	body, err := sender.blobs.Open(ctx, tenantID, ref)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, maxReplyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != ref.Size || len(data) > maxReplyBytes {
		return nil, domain.ValidationError{Field: "canonical blob size", Reason: "does not match the immutable reference or projection bound"}
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != ref.SHA256 {
		return nil, domain.ValidationError{Field: "canonical blob digest", Reason: "does not match the immutable reference"}
	}
	return data, nil
}

func decodeBoundedJSON(data []byte, value any) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return err
	}
	return nil
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
		if manifest.TenantID != delivery.TenantID || manifest.RunID != delivery.RunID {
			return sender.fail(ctx, delivery, now, fmt.Errorf("artifact manifest ownership mismatch"))
		}
		for _, artifact := range manifest.Artifacts {
			if err := artifact.Validate(); err != nil {
				return sender.fail(ctx, delivery, now, err)
			}
			if err := domain.EnsureSameTenant(delivery.TenantID, artifact.Blob.TenantID); err != nil {
				return sender.fail(ctx, delivery, now, err)
			}
			if delivery.Projection != nil && !strings.HasPrefix(
				artifact.Blob.Key,
				domain.SessionObjectPrefix(delivery.TenantID, delivery.Projection.SessionID),
			) {
				return sender.fail(ctx, delivery, now, fmt.Errorf("artifact is outside the canonical session boundary"))
			}
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
