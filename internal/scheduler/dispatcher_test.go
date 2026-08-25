package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/testkit"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

type memorySchedulerStore struct {
	ready      map[uint32][]ports.DispatchReady
	decisions  map[domain.DispatchOutboxID]ports.DispatchAdmissionResult
	acked      []domain.DispatchOutboxID
	expired    map[uint32][]ports.ExpiredQuotaReservation
	expireSeen []domain.QuotaReservationID
}

type snapshotMaintenanceCall struct {
	tenantID        domain.TenantID
	sessionID       domain.SessionID
	throughSequence uint64
}

type memorySnapshotMaintainer struct {
	calls []snapshotMaintenanceCall
	err   error
}

func (maintainer *memorySnapshotMaintainer) MaybeCreate(
	_ context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	throughSequence uint64,
) (domain.SessionSnapshot, bool, error) {
	maintainer.calls = append(maintainer.calls, snapshotMaintenanceCall{
		tenantID: tenantID, sessionID: sessionID, throughSequence: throughSequence,
	})
	return domain.SessionSnapshot{}, false, maintainer.err
}

func (store *memorySchedulerStore) GetDispatch(
	_ context.Context,
	tenantID domain.TenantID,
	outboxID domain.DispatchOutboxID,
) (ports.DispatchReady, domain.DispatchStatus, bool, error) {
	for _, candidates := range store.ready {
		for _, candidate := range candidates {
			if candidate.TenantID == tenantID && candidate.OutboxID == outboxID {
				return candidate, domain.DispatchPending, true, nil
			}
		}
	}
	return ports.DispatchReady{}, "", false, nil
}

func (store *memorySchedulerStore) ListReadyDispatches(
	_ context.Context,
	bucket uint32,
	_ time.Time,
	_ uint64,
) ([]ports.DispatchReady, error) {
	return append([]ports.DispatchReady(nil), store.ready[bucket]...), nil
}

func (store *memorySchedulerStore) AdmitDispatch(
	_ context.Context,
	request ports.DispatchAdmissionRequest,
) (ports.DispatchAdmissionResult, error) {
	decision, ok := store.decisions[request.OutboxID]
	if !ok {
		return ports.DispatchAdmissionResult{}, errors.New("missing admission decision")
	}
	if request.ReservationID != stableReservationID(request.OutboxID) {
		return ports.DispatchAdmissionResult{}, errors.New("reservation ID is not deterministic")
	}
	return decision, nil
}

func (store *memorySchedulerStore) AcknowledgeDispatch(
	_ context.Context,
	_ domain.TenantID,
	outboxID domain.DispatchOutboxID,
	_ time.Time,
) error {
	store.acked = append(store.acked, outboxID)
	return nil
}

func (store *memorySchedulerStore) ListExpiredQuotaReservations(
	_ context.Context,
	bucket uint32,
	_ time.Time,
	_ uint64,
) ([]ports.ExpiredQuotaReservation, error) {
	return append([]ports.ExpiredQuotaReservation(nil), store.expired[bucket]...), nil
}

func (store *memorySchedulerStore) ExpireQuotaReservation(
	_ context.Context,
	candidate ports.ExpiredQuotaReservation,
	_ time.Time,
) (bool, error) {
	store.expireSeen = append(store.expireSeen, candidate.ReservationID)
	return true, nil
}

func TestDispatcherPublishesOnlyAdmittedRunsAndExpiresReservations(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	admitted := ports.DispatchReady{
		TenantID: "tenant-1", OutboxID: "dispatch-1",
		RunID: "run-1", AttemptID: "attempt-1",
	}
	blocked := ports.DispatchReady{
		TenantID: "tenant-2", OutboxID: "dispatch-2",
		RunID: "run-2", AttemptID: "attempt-2",
	}
	bucket, err := ydbpartition.BucketV1(string(admitted.OutboxID))
	if err != nil {
		t.Fatal(err)
	}
	blockedBucket, err := ydbpartition.BucketV1(string(blocked.OutboxID))
	if err != nil {
		t.Fatal(err)
	}
	expiredBucket, err := ydbpartition.BucketV1("reservation-expired")
	if err != nil {
		t.Fatal(err)
	}
	store := &memorySchedulerStore{
		ready: map[uint32][]ports.DispatchReady{
			bucket:        {admitted},
			blockedBucket: appendIfDifferent(bucket, blockedBucket, blocked),
		},
		decisions: map[domain.DispatchOutboxID]ports.DispatchAdmissionResult{
			admitted.OutboxID: {
				Admitted: true, State: domain.SchedulerReady, Code: "admitted",
				Delivery:  ports.DispatchDeliveryManagedQueue,
				SessionID: "session-1", ThroughSequence: 128,
			},
			blocked.OutboxID: {State: domain.SchedulerPressured, Code: "subscription_slot_busy"},
		},
		expired: map[uint32][]ports.ExpiredQuotaReservation{
			expiredBucket: {{
				TenantID: "tenant-3", ReservationID: "reservation-expired",
				RunID: "run-3", SubscriptionConnectionID: "subscription-3",
				ExpiresAt: now.Add(-time.Second),
			}},
		},
	}
	if bucket == blockedBucket {
		store.ready[bucket] = []ports.DispatchReady{admitted, blocked}
	}
	queue := testkit.NewMemoryQueue()
	maintenanceErr := errors.New("snapshot storage unavailable")
	maintainer := &memorySnapshotMaintainer{err: maintenanceErr}
	var observedMaintenanceErr error
	dispatcher, err := NewDispatcher(Config{
		BatchSize: 10, ReservationTTL: time.Minute,
		Limits: testLimits(), SnapshotMaintainer: maintainer,
		SnapshotObserver: func(err error) {
			observedMaintenanceErr = err
		},
		DefaultWorkload: domain.WorkloadShape{
			Runtime: time.Minute, Turns: 1,
		},
	}, testkit.NewFakeClock(now), store, queue)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Considered != 2 || result.Published != 1 ||
		result.Blocked != 1 || result.Expired != 1 {
		t.Fatalf("pass result = %#v", result)
	}
	message, err := queue.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if message.Envelope.SubjectID != string(admitted.RunID) ||
		message.Envelope.MessageID != stableMessageID(admitted.OutboxID) {
		t.Fatalf("envelope = %#v", message.Envelope)
	}
	if len(store.acked) != 1 || store.acked[0] != admitted.OutboxID {
		t.Fatalf("acked = %#v", store.acked)
	}
	if len(store.expireSeen) != 1 || store.expireSeen[0] != "reservation-expired" {
		t.Fatalf("expired = %#v", store.expireSeen)
	}
	if len(maintainer.calls) != 1 || maintainer.calls[0] != (snapshotMaintenanceCall{
		tenantID: admitted.TenantID, sessionID: "session-1", throughSequence: 128,
	}) {
		t.Fatalf("snapshot maintenance calls = %#v", maintainer.calls)
	}
	if !errors.Is(observedMaintenanceErr, maintenanceErr) {
		t.Fatalf("observed snapshot maintenance error = %v", observedMaintenanceErr)
	}
}

func TestDispatcherWakeUsesPointLookupAndAcknowledgesDuplicate(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	candidate := ports.DispatchReady{
		TenantID: "tenant-1", OutboxID: "dispatch-1",
		RunID: "run-1", AttemptID: "attempt-1",
	}
	store := &memorySchedulerStore{
		ready: map[uint32][]ports.DispatchReady{0: {candidate}},
		decisions: map[domain.DispatchOutboxID]ports.DispatchAdmissionResult{
			candidate.OutboxID: {
				Admitted: true, Code: "admitted", Delivery: ports.DispatchDeliveryManagedQueue,
			},
		},
	}
	dispatchQueue := testkit.NewMemoryQueue()
	dispatcher, err := NewDispatcher(Config{
		Limits:          testLimits(),
		DefaultWorkload: domain.WorkloadShape{Runtime: time.Minute, Turns: 1},
	}, testkit.NewFakeClock(now), store, dispatchQueue)
	if err != nil {
		t.Fatal(err)
	}
	wakeQueue := testkit.NewMemoryQueue()
	publisher, err := outboxwake.NewPublisher(wakeQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishDispatchWake(
		context.Background(), candidate.TenantID, candidate.OutboxID, now,
	); err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.RunWake(context.Background(), wakeQueue)
	if err != nil || result.Outcome != "published" {
		t.Fatalf("wake result = %#v, %v", result, err)
	}
	if len(store.acked) != 1 || store.acked[0] != candidate.OutboxID {
		t.Fatalf("acked = %#v", store.acked)
	}

	missingQueue := testkit.NewMemoryQueue()
	missingPublisher, _ := outboxwake.NewPublisher(missingQueue)
	if err := missingPublisher.PublishDispatchWake(
		context.Background(), candidate.TenantID, "dispatch-missing", now,
	); err != nil {
		t.Fatal(err)
	}
	result, err = dispatcher.RunWake(context.Background(), missingQueue)
	if err != nil || result.Outcome != "noop" {
		t.Fatalf("duplicate result = %#v, %v", result, err)
	}
}

func TestDispatcherAttachedWorkerOfferNeverPublishesManagedQueue(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	candidate := ports.DispatchReady{
		TenantID: "tenant-1", OutboxID: "dispatch-1",
		RunID: "run-1", AttemptID: "attempt-1",
	}
	bucket, err := ydbpartition.BucketV1(string(candidate.OutboxID))
	if err != nil {
		t.Fatal(err)
	}
	store := &memorySchedulerStore{
		ready: map[uint32][]ports.DispatchReady{bucket: {candidate}},
		decisions: map[domain.DispatchOutboxID]ports.DispatchAdmissionResult{
			candidate.OutboxID: {
				Admitted: true, Code: "attached_worker_offered",
				Delivery: ports.DispatchDeliveryAttachedOffer,
			},
		},
	}
	queue := testkit.NewMemoryQueue()
	dispatcher, err := NewDispatcher(Config{
		Limits: testLimits(), DefaultWorkload: domain.WorkloadShape{Runtime: time.Minute, Turns: 1},
	}, testkit.NewFakeClock(now), store, queue)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dispatcher.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Admitted != 1 || result.AttachedOffers != 1 || result.Published != 0 {
		t.Fatalf("pass result = %#v", result)
	}
	if len(store.acked) != 1 || store.acked[0] != candidate.OutboxID {
		t.Fatalf("acked = %#v", store.acked)
	}
	if _, err := queue.Receive(context.Background()); err == nil {
		t.Fatal("attached-worker placement leaked into the managed queue")
	}
}

func TestDispatcherWakeParksPersistentlyBlockedOutboxAfterBoundedRetries(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	candidate := ports.DispatchReady{
		TenantID: "tenant-1", OutboxID: "dispatch-1",
		RunID: "run-1", AttemptID: "attempt-1",
	}
	store := &memorySchedulerStore{
		ready: map[uint32][]ports.DispatchReady{0: {candidate}},
		decisions: map[domain.DispatchOutboxID]ports.DispatchAdmissionResult{
			candidate.OutboxID: {
				State: domain.SchedulerReauthRequired,
				Code:  "subscription_reauthentication_required",
			},
		},
	}
	dispatcher, err := NewDispatcher(Config{
		WakeRetryDelay:       time.Millisecond,
		MaxWakeDeliveryCount: 3,
		Limits:               testLimits(),
		DefaultWorkload:      domain.WorkloadShape{Runtime: time.Minute, Turns: 1},
	}, testkit.NewFakeClock(now), store, testkit.NewMemoryQueue())
	if err != nil {
		t.Fatal(err)
	}
	wakeQueue := testkit.NewMemoryQueue()
	publisher, err := outboxwake.NewPublisher(wakeQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishDispatchWake(
		context.Background(), candidate.TenantID, candidate.OutboxID, now,
	); err != nil {
		t.Fatal(err)
	}

	for delivery := 1; delivery <= 3; delivery++ {
		result, runErr := dispatcher.RunWake(context.Background(), wakeQueue)
		if runErr != nil {
			t.Fatal(runErr)
		}
		wantOutcome := "retry"
		if delivery == 3 {
			wantOutcome = "parked"
		}
		if result.Outcome != wantOutcome || result.Code != "subscription_reauthentication_required" {
			t.Fatalf("delivery %d result = %#v, want outcome %q", delivery, result, wantOutcome)
		}
	}
	if _, err := wakeQueue.Receive(context.Background()); err == nil {
		t.Fatal("parked wake remained queued after the retry budget")
	}
}

func TestDispatcherWakeDeadLettersUnexpectedEnvelopeKind(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	dispatcher, err := NewDispatcher(Config{
		Limits:          testLimits(),
		DefaultWorkload: domain.WorkloadShape{Runtime: time.Minute, Turns: 1},
	}, testkit.NewFakeClock(now), &memorySchedulerStore{}, testkit.NewMemoryQueue())
	if err != nil {
		t.Fatal(err)
	}
	wakeQueue := testkit.NewMemoryQueue()
	if err := wakeQueue.Publish(context.Background(), queuecontract.Envelope{
		Schema: queuecontract.SchemaV1, MessageID: "msg-unexpected",
		Kind: queuecontract.KindWakeTelegram, TenantID: "tenant-1",
		SubjectID: "delivery-1", EnqueuedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.RunWake(context.Background(), wakeQueue); err != nil {
		t.Fatal(err)
	}
	if wakeQueue.DeadLetterCount() != 1 {
		t.Fatalf("dead letters = %d, want 1", wakeQueue.DeadLetterCount())
	}
}

func appendIfDifferent(
	first uint32,
	second uint32,
	value ports.DispatchReady,
) []ports.DispatchReady {
	if first == second {
		return nil
	}
	return []ports.DispatchReady{value}
}

var _ ports.SchedulerStore = (*memorySchedulerStore)(nil)
