// Package outboxwake publishes payload-free hints for durable YDB outboxes.
package outboxwake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
)

type Publisher struct {
	queue ports.Queue
	ops   opssignal.Sink
}

func NewPublisher(queue ports.Queue, signals ...opssignal.Sink) (*Publisher, error) {
	if queue == nil {
		return nil, fmt.Errorf("outbox wake queue must not be nil")
	}
	var sink opssignal.Sink
	if len(signals) != 0 {
		sink = signals[0]
	}
	return &Publisher{queue: queue, ops: sink}, nil
}

func (publisher *Publisher) PublishDispatchWake(
	ctx context.Context,
	tenantID domain.TenantID,
	outboxID domain.DispatchOutboxID,
	at time.Time,
) error {
	if err := outboxID.Validate(); err != nil {
		return err
	}
	return publisher.publish(ctx, queuecontract.KindWakeDispatch, tenantID, string(outboxID), at)
}

func (publisher *Publisher) PublishTelegramDeliveryWake(
	ctx context.Context,
	tenantID domain.TenantID,
	deliveryID domain.TelegramDeliveryID,
	at time.Time,
) error {
	if err := deliveryID.Validate(); err != nil {
		return err
	}
	return publisher.publish(ctx, queuecontract.KindWakeTelegram, tenantID, string(deliveryID), at)
}

func (publisher *Publisher) PublishFrontendProjectionWake(
	ctx context.Context,
	tenantID domain.TenantID,
	runID domain.RunID,
	at time.Time,
) error {
	if err := runID.Validate(); err != nil {
		return err
	}
	return publisher.publish(ctx, queuecontract.KindWakeProjection, tenantID, string(runID), at)
}

func (publisher *Publisher) publish(
	ctx context.Context,
	kind queuecontract.Kind,
	tenantID domain.TenantID,
	subjectID string,
	at time.Time,
) error {
	if at.IsZero() {
		return domain.ValidationError{Field: "wake.enqueued_at", Reason: "must not be zero"}
	}
	err := publisher.queue.Publish(ctx, queuecontract.Envelope{
		Schema: queuecontract.SchemaV1, MessageID: wakeMessageID(kind, tenantID, subjectID),
		Kind: kind, TenantID: tenantID, SubjectID: subjectID, EnqueuedAt: at.UTC(),
	})
	if err != nil {
		// This reports a lost hint, never loss of the already committed outbox.
		// Callers retain their existing post-commit success/recovery policy.
		opssignal.Emit(ctx, publisher.ops, opssignal.ControlWakeDeferred, opssignal.WakePublish, opssignal.Queue)
	}
	return err
}

// Legacy Telegram ingress derives operational outbox IDs from the run so a
// replay can republish a lost wake-up after the YDB dedup transaction returns
// the original run ID.
func DispatchOutboxID(runID domain.RunID) domain.DispatchOutboxID {
	return domain.DispatchOutboxID(stableIdentifier("dsp", string(runID)))
}

func TelegramDeliveryID(runID domain.RunID) domain.TelegramDeliveryID {
	return domain.TelegramDeliveryID(stableIdentifier("tdl", string(runID)))
}

func TelegramProjectionDeliveryID(projectionID domain.FrontendProjectionID) domain.TelegramDeliveryID {
	return domain.TelegramDeliveryID(stableIdentifier("tdl", string(projectionID)))
}

func wakeMessageID(kind queuecontract.Kind, tenantID domain.TenantID, subjectID string) domain.MessageID {
	return domain.MessageID(stableIdentifier("wke", string(kind)+"\x00"+string(tenantID)+"\x00"+subjectID))
}

func stableIdentifier(prefix, value string) string {
	digest := sha256.Sum256([]byte(value))
	return prefix + "-" + hex.EncodeToString(digest[:16])
}

var (
	_ ports.DispatchWakePublisher           = (*Publisher)(nil)
	_ ports.TelegramDeliveryWakePublisher   = (*Publisher)(nil)
	_ ports.FrontendProjectionWakePublisher = (*Publisher)(nil)
)
