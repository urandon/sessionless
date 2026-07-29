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
	BatchSize       uint64
	ReservationTTL  time.Duration
	Limits          domain.ProductLimits
	DefaultWorkload domain.WorkloadShape
}

type PassResult struct {
	Considered int
	Admitted   int
	Published  int
	Blocked    int
	Expired    int
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
			admission, err := dispatcher.store.AdmitDispatch(
				ctx,
				ports.DispatchAdmissionRequest{
					TenantID: candidate.TenantID, OutboxID: candidate.OutboxID,
					RunID: candidate.RunID, AttemptID: candidate.AttemptID,
					ReservationID: stableReservationID(candidate.OutboxID),
					Now:           now, HoldUntil: now.Add(dispatcher.config.ReservationTTL),
					Limits:   dispatcher.config.Limits,
					Workload: dispatcher.config.DefaultWorkload,
				},
			)
			if err != nil {
				return result, err
			}
			if !admission.Admitted {
				result.Blocked++
				continue
			}
			result.Admitted++
			envelope := queuecontract.Envelope{
				Schema:     queuecontract.SchemaV1,
				MessageID:  stableMessageID(candidate.OutboxID),
				Kind:       queuecontract.KindDispatchRun,
				TenantID:   candidate.TenantID,
				SubjectID:  string(candidate.RunID),
				EnqueuedAt: now,
			}
			if err := dispatcher.queue.Publish(ctx, envelope); err != nil {
				return result, err
			}
			if err := dispatcher.store.AcknowledgeDispatch(
				ctx, candidate.TenantID, candidate.OutboxID, now,
			); err != nil {
				return result, err
			}
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
