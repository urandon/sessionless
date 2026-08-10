package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

type Config struct {
	BatchSize            uint64
	ReservationTTL       time.Duration
	WakeRetryDelay       time.Duration
	MaxWakeDeliveryCount uint32
	Limits               domain.ProductLimits
	DefaultWorkload      domain.WorkloadShape
}

type PassResult struct {
	Considered int
	Admitted   int
	Published  int
	Blocked    int
	Expired    int
}

type WakeResult struct {
	Outcome string
	Code    string
}

type Dispatcher struct {
	config Config
	clock  ports.Clock
	store  ports.SchedulerStore
	queue  ports.Queue
}

func NewDispatcher(
	config Config,
	clock ports.Clock,
	store ports.SchedulerStore,
	queue ports.Queue,
) (*Dispatcher, error) {
	if config.BatchSize == 0 {
		config.BatchSize = 25
	}
	if config.ReservationTTL <= 0 {
		config.ReservationTTL = 5 * time.Minute
	}
	if config.WakeRetryDelay <= 0 {
		config.WakeRetryDelay = time.Second
	}
	if config.MaxWakeDeliveryCount == 0 {
		config.MaxWakeDeliveryCount = 5
	}
	if clock == nil || store == nil || queue == nil {
		return nil, fmt.Errorf("scheduler dependencies must not be nil")
	}
	if err := config.Limits.Validate(); err != nil {
		return nil, err
	}
	if err := config.DefaultWorkload.Validate(); err != nil {
		return nil, err
	}
	return &Dispatcher{config: config, clock: clock, store: store, queue: queue}, nil
}

// RunWake resolves one payload-free wake hint with a tenant/outbox point read.
// Missing and already-terminal outboxes are successful idempotent no-ops.
func (dispatcher *Dispatcher) RunWake(ctx context.Context, wakeQueue ports.Queue) (WakeResult, error) {
	message, err := wakeQueue.Receive(ctx)
	if err != nil {
		return WakeResult{}, err
	}
	if message.Envelope.Kind != queuecontract.KindWakeDispatch {
		return WakeResult{}, wakeQueue.DeadLetter(ctx, message.ReceiptHandle, "unexpected_kind")
	}
	outboxID := domain.DispatchOutboxID(message.Envelope.SubjectID)
	if err := outboxID.Validate(); err != nil {
		return WakeResult{}, wakeQueue.DeadLetter(ctx, message.ReceiptHandle, "invalid_outbox_id")
	}
	candidate, status, found, err := dispatcher.store.GetDispatch(
		ctx, message.Envelope.TenantID, outboxID,
	)
	if err != nil {
		return WakeResult{}, err
	}
	if !found || status != domain.DispatchPending {
		if err := wakeQueue.Ack(ctx, message.ReceiptHandle); err != nil {
			return WakeResult{}, err
		}
		return WakeResult{Outcome: "noop", Code: "missing_or_terminal"}, nil
	}
	admission, err := dispatcher.dispatchCandidate(ctx, candidate, dispatcher.clock.Now().UTC())
	if err != nil {
		return WakeResult{}, err
	}
	if !admission.Admitted {
		if admission.Code == "canonical_projection_pending" ||
			admission.Code == "dispatch_not_pending" {
			if err := wakeQueue.Ack(ctx, message.ReceiptHandle); err != nil {
				return WakeResult{}, err
			}
			return WakeResult{Outcome: "blocked", Code: admission.Code}, nil
		}
		if message.DeliveryCount >= dispatcher.config.MaxWakeDeliveryCount {
			if err := wakeQueue.Ack(ctx, message.ReceiptHandle); err != nil {
				return WakeResult{}, err
			}
			return WakeResult{Outcome: "parked", Code: admission.Code}, nil
		}
		delay := dispatcher.config.WakeRetryDelay
		for delivery := uint32(1); delivery < message.DeliveryCount; delivery++ {
			delay *= 2
		}
		if admission.RetryAt != nil {
			untilRetry := admission.RetryAt.Sub(dispatcher.clock.Now().UTC())
			if untilRetry <= 0 {
				delay = 0
			} else if untilRetry < delay {
				delay = untilRetry
			}
		}
		if err := wakeQueue.Retry(ctx, message.ReceiptHandle, delay); err != nil {
			return WakeResult{}, err
		}
		return WakeResult{Outcome: "retry", Code: admission.Code}, nil
	}
	if err := wakeQueue.Ack(ctx, message.ReceiptHandle); err != nil {
		return WakeResult{}, err
	}
	return WakeResult{Outcome: "published", Code: admission.Code}, nil
}

func (dispatcher *Dispatcher) RunOnce(ctx context.Context) (PassResult, error) {
	now := dispatcher.clock.Now().UTC()
	var result PassResult
	for bucket := uint32(0); bucket < ydbpartition.BucketCountV1; bucket++ {
		ready, err := dispatcher.store.ListReadyDispatches(
			ctx, bucket, now, dispatcher.config.BatchSize,
		)
		if err != nil {
			return result, err
		}
		for _, candidate := range ready {
			result.Considered++
			admission, err := dispatcher.dispatchCandidate(ctx, candidate, now)
			if err != nil {
				return result, err
			}
			if !admission.Admitted {
				result.Blocked++
				continue
			}
			result.Admitted++
			result.Published++
		}

		expired, err := dispatcher.store.ListExpiredQuotaReservations(
			ctx, bucket, now, dispatcher.config.BatchSize,
		)
		if err != nil {
			return result, err
		}
		for _, candidate := range expired {
			didExpire, err := dispatcher.store.ExpireQuotaReservation(ctx, candidate, now)
			if err != nil {
				return result, err
			}
			if didExpire {
				result.Expired++
			}
		}
	}
	return result, nil
}

func (dispatcher *Dispatcher) dispatchCandidate(
	ctx context.Context,
	candidate ports.DispatchReady,
	now time.Time,
) (ports.DispatchAdmissionResult, error) {
	admission, err := dispatcher.store.AdmitDispatch(ctx, ports.DispatchAdmissionRequest{
		TenantID: candidate.TenantID, OutboxID: candidate.OutboxID,
		RunID: candidate.RunID, AttemptID: candidate.AttemptID,
		ReservationID: stableReservationID(candidate.OutboxID),
		Now:           now, HoldUntil: now.Add(dispatcher.config.ReservationTTL),
		Limits: dispatcher.config.Limits, Workload: dispatcher.config.DefaultWorkload,
	})
	if err != nil || !admission.Admitted {
		return admission, err
	}
	envelope := queuecontract.Envelope{
		Schema: queuecontract.SchemaV1, MessageID: stableMessageID(candidate.OutboxID),
		Kind: queuecontract.KindDispatchRun, TenantID: candidate.TenantID,
		SubjectID: string(candidate.RunID), EnqueuedAt: now,
	}
	if err := dispatcher.queue.Publish(ctx, envelope); err != nil {
		return ports.DispatchAdmissionResult{}, err
	}
	if err := dispatcher.store.AcknowledgeDispatch(
		ctx, candidate.TenantID, candidate.OutboxID, now,
	); err != nil {
		return ports.DispatchAdmissionResult{}, err
	}
	return admission, nil
}

func stableReservationID(outboxID domain.DispatchOutboxID) domain.QuotaReservationID {
	return domain.QuotaReservationID(stableIdentifier("qrs", string(outboxID)))
}

func stableMessageID(outboxID domain.DispatchOutboxID) domain.MessageID {
	return domain.MessageID(stableIdentifier("msg", string(outboxID)))
}

func stableIdentifier(prefix, value string) string {
	sum := sha256.Sum256([]byte(value))
	return prefix + "-" + hex.EncodeToString(sum[:16])
}
