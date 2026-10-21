package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

type opsSinkFunc func(context.Context, opssignal.Record) error

func (f opsSinkFunc) Observe(ctx context.Context, r opssignal.Record) error { return f(ctx, r) }

type opsSchedulerStore struct {
	*memorySchedulerStore
	fail error
	seen context.Context
}

func (s *opsSchedulerStore) ListReadyDispatches(ctx context.Context, _ uint32, _ time.Time, _ uint64) ([]ports.DispatchReady, error) {
	s.seen = ctx
	return nil, s.fail
}

func TestOpsDispatchFailureKeepsOriginalResultAndContext(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "private")
	original := errors.New("private YDB path")
	store := &opsSchedulerStore{memorySchedulerStore: &memorySchedulerStore{}, fail: original}
	observed := 0
	d, err := NewDispatcher(Config{Limits: testLimits(), DefaultWorkload: domain.WorkloadShape{Runtime: time.Minute, Turns: 1}, OpsSignals: opsSinkFunc(func(got context.Context, r opssignal.Record) error {
		observed++
		if got != ctx || r.SignalID != opssignal.ControlDispatchFailure || r.Stage != opssignal.Dispatch {
			t.Errorf("context/record mismatch: %+v", r)
		}
		return errors.New("sink full")
	})}, testkit.NewFakeClock(time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)), store, testkit.NewMemoryQueue())
	if err != nil {
		t.Fatal(err)
	}
	result, err := d.RunOnce(ctx)
	if err != original || result != (PassResult{}) || observed != 1 || store.seen != ctx {
		t.Fatalf("result=%+v err=%v observed=%d original-context=%v", result, err, observed, store.seen == ctx)
	}
}

func TestOpsDispatchExpectedTypedDenialIsNotOutage(t *testing.T) {
	for _, expected := range []error{domain.ErrMembershipDenied, &domain.ClassifiedError{Kind: domain.ErrorPolicyDenied}, &domain.ClassifiedError{Kind: domain.ErrorQuotaBlocked}, &domain.ClassifiedError{Kind: domain.ErrorReauthRequired}} {
		store := &opsSchedulerStore{memorySchedulerStore: &memorySchedulerStore{}, fail: expected}
		d, err := NewDispatcher(Config{Limits: testLimits(), DefaultWorkload: domain.WorkloadShape{Runtime: time.Minute, Turns: 1}, OpsSignals: opsSinkFunc(func(context.Context, opssignal.Record) error {
			t.Errorf("expected denial became outage: %v", expected)
			return nil
		})}, testkit.NewFakeClock(time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)), store, testkit.NewMemoryQueue())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.RunOnce(context.Background()); err != expected {
			t.Fatalf("denial changed: got=%v want=%v", err, expected)
		}
	}
}

type opsWakeQueue struct {
	ports.Queue
	message       ports.ReceivedMessage
	retryErr      error
	retries, acks int
	seen          context.Context
	delay         time.Duration
}

func (q *opsWakeQueue) Receive(ctx context.Context) (ports.ReceivedMessage, error) {
	q.seen = ctx
	return q.message, nil
}
func (q *opsWakeQueue) Retry(ctx context.Context, _ string, d time.Duration) error {
	q.seen = ctx
	q.delay = d
	q.retries++
	return q.retryErr
}
func (q *opsWakeQueue) Ack(ctx context.Context, _ string) error { q.seen = ctx; q.acks++; return nil }

func TestOpsExpectedQuotaRetryAndParkingKeepACKSemantics(t *testing.T) {
	for _, tc := range []struct {
		name          string
		delivery      uint32
		retryErr      error
		want          string
		acks, retries int
	}{
		{"poll_retry", 1, nil, "retry", 0, 1}, {"trigger_retry", 1, ports.ErrQueueRetryRequested, "", 0, 1}, {"park", 3, nil, "parked", 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			candidate := ports.DispatchReady{TenantID: "tenant-a", OutboxID: "dispatch-a", RunID: "run-a", AttemptID: "attempt-a"}
			store := &memorySchedulerStore{ready: map[uint32][]ports.DispatchReady{0: {candidate}}, decisions: map[domain.DispatchOutboxID]ports.DispatchAdmissionResult{candidate.OutboxID: {State: domain.SchedulerPressured, Code: "subscription_slot_busy"}}}
			q := &opsWakeQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Kind: queuecontract.KindWakeDispatch, TenantID: candidate.TenantID, SubjectID: string(candidate.OutboxID)}, ReceiptHandle: "private-receipt", DeliveryCount: tc.delivery}, retryErr: tc.retryErr}
			d, err := NewDispatcher(Config{Limits: testLimits(), DefaultWorkload: domain.WorkloadShape{Runtime: time.Minute, Turns: 1}, MaxWakeDeliveryCount: 3, WakeRetryDelay: 17 * time.Second, OpsSignals: opsSinkFunc(func(context.Context, opssignal.Record) error {
				t.Error("expected policy/parking became outage")
				return nil
			})}, testkit.NewFakeClock(time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)), store, testkit.NewMemoryQueue())
			if err != nil {
				t.Fatal(err)
			}
			result, err := d.RunWake(ctx, q)
			if err != tc.retryErr || result.Outcome != tc.want || q.acks != tc.acks || q.retries != tc.retries || q.seen != ctx {
				t.Fatalf("result=%+v err=%v acks=%d retries=%d context=%v", result, err, q.acks, q.retries, q.seen == ctx)
			}
			if q.retries != 0 && q.delay != 17*time.Second {
				t.Fatalf("retry delay=%v", q.delay)
			}
		})
	}
}
