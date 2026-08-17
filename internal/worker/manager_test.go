package worker_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/deterministicharness"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/testkit"
	"gitcode.com/urandon/sessionless/internal/worker"
)

var workerTestTime = time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

func TestWorkerCompletesOnceAndCleansReusedScratch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	for _, tenant := range []domain.TenantID{"tenant-a", "tenant-b"} {
		loaded := workerFixture(t, ctx, blobs, tenant, workerTestTime)
		state.jobs[jobKey(tenant, loaded.Run.ID)] = loaded
		publishWorkerMessage(t, ctx, queue, tenant, loaded.Run.ID)
	}
	harness, err := deterministicharness.New(deterministicharness.Config{Turns: 2, Artifacts: 2})
	if err != nil {
		t.Fatal(err)
	}
	scratch := t.TempDir()
	manager, err := worker.New(worker.Config{
		ScratchRoot: scratch, WorkerID: "worker-test",
		LeaseTTL: time.Minute, DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, harness)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		outcome, err := manager.RunOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if outcome != worker.OutcomeCompleted {
			t.Fatalf("outcome = %q, want completed", outcome)
		}
		assertScratchEmpty(t, scratch)
	}
	if state.completions != 2 || len(state.checkpoints) != 4 || len(state.usage) != 4 {
		t.Fatalf(
			"completions/checkpoints/usage = %d/%d/%d, want 2/4/4",
			state.completions, len(state.checkpoints), len(state.usage),
		)
	}
	for _, manifest := range state.manifests {
		for _, artifact := range manifest.Artifacts {
			if !strings.Contains(artifact.Blob.Key, "/artifacts/sha256/") {
				t.Fatalf("artifact key is not content-addressed: %s", artifact.Blob.Key)
			}
			if artifact.Blob.TenantID != manifest.TenantID {
				t.Fatalf("artifact tenant = %s, manifest tenant = %s", artifact.Blob.TenantID, manifest.TenantID)
			}
		}
	}
}

func TestCanonicalWorkerFinalizesToolAndAssistantEventsWithoutTelegramDelivery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	loaded.Job.Origin = &domain.FrontendEventOrigin{
		BindingID: "binding-1", BindingRevision: 1,
		Frontend: domain.Frontend("synthetic"), ExternalConversationID: "conversation-1",
		ExternalEventID: "external-event-1",
	}
	loaded.Job.DeliveryChat = domain.TelegramChatRef{}
	loaded.Job.ReplyToMessageID = 0
	state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-canonical",
		DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, canonicalHarness{})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := manager.RunOnce(ctx)
	if err != nil || outcome != worker.OutcomeCompleted {
		t.Fatalf("outcome/error = %q/%v", outcome, err)
	}
	if len(state.events) != 3 {
		t.Fatalf("canonical events = %d, want tool call/result and assistant", len(state.events))
	}
	if len(state.deliveries) != 0 {
		t.Fatalf("canonical worker created Telegram deliveries: %+v", state.deliveries)
	}
	for index, event := range state.events {
		if event.Payload.TenantID != loaded.Run.TenantID ||
			!strings.HasPrefix(event.Payload.Key, domain.SessionEventObjectPrefix(
				loaded.Run.TenantID, loaded.Run.SessionID, event.ID,
			)) {
			t.Fatalf("event %d has non-canonical payload: %+v", index, event.Payload)
		}
		if _, found := blobs.data[event.Payload.Key]; !found {
			t.Fatalf("event %d payload was not stored", index)
		}
	}
	if state.events[0].Kind != domain.SessionEventToolCall ||
		state.events[1].Kind != domain.SessionEventToolResult ||
		state.events[2].Kind != domain.SessionEventAssistantMessage {
		t.Fatalf("canonical event order = %+v", state.events)
	}
}

func TestCanonicalWorkerFinalizesCancellationNoticeWithoutTelegramDelivery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	loaded.Job.Origin = &domain.FrontendEventOrigin{
		BindingID: "binding-1", BindingRevision: 1,
		Frontend: domain.Frontend("synthetic"), ExternalConversationID: "conversation-1",
		ExternalEventID: "external-event-1",
	}
	loaded.Job.DeliveryChat = domain.TelegramChatRef{}
	loaded.Job.ReplyToMessageID = 0
	key := jobKey(loaded.Run.TenantID, loaded.Run.ID)
	state.jobs[key] = loaded
	state.cancelled[key] = true
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	harness, err := deterministicharness.New(deterministicharness.Config{})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-canonical-cancelled",
		DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, harness)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := manager.RunOnce(ctx)
	if err != nil || outcome != worker.OutcomeCancelled {
		t.Fatalf("outcome/error = %q/%v, want cancelled/nil", outcome, err)
	}
	if len(state.events) != 1 || state.events[0].Kind != domain.SessionEventSystemNotice {
		t.Fatalf("canonical cancellation events = %+v, want one system notice", state.events)
	}
	if len(state.deliveries) != 0 {
		t.Fatalf("canonical cancellation created Telegram deliveries: %+v", state.deliveries)
	}
	if _, found := blobs.data[state.events[0].Payload.Key]; !found {
		t.Fatal("canonical cancellation payload was not stored")
	}
}

func TestWorkerRejectsTraversalAndCrossTenantReferences(t *testing.T) {
	t.Parallel()
	for _, mutate := range []func(*ports.WorkerJobState){
		func(state *ports.WorkerJobState) {
			state.InputManifest.Artifacts[0].Name = "../escape"
		},
		func(state *ports.WorkerJobState) {
			state.Job.ContextSnapshot.TenantID = "tenant-b"
			state.Job.ContextSnapshot.Key = "tenants/tenant-b/context"
		},
	} {
		ctx := context.Background()
		clock := testkit.NewFakeClock(workerTestTime)
		queue := testkit.NewMemoryQueue()
		blobs := newMemoryBlobs()
		state := newWorkerState()
		loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
		mutate(&loaded)
		state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
		publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
		harness, _ := deterministicharness.New(deterministicharness.Config{})
		scratch := t.TempDir()
		manager, err := worker.New(worker.Config{
			ScratchRoot: scratch, WorkerID: "worker-test",
			DeliveryWakePublisher: newDeliveryWakePublisher(t),
		}, clock, queue, state, blobs, harness)
		if err != nil {
			t.Fatal(err)
		}
		outcome, err := manager.RunOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if outcome != worker.OutcomeFailed {
			t.Fatalf("outcome = %q, want failed", outcome)
		}
		assertScratchEmpty(t, scratch)
	}
}

func TestWorkerResumesAfterRetryableFailureAndDeduplicatesDelivery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	key := jobKey(loaded.Run.TenantID, loaded.Run.ID)
	state.jobs[key] = loaded
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	harness, _ := deterministicharness.New(deterministicharness.Config{
		Turns: 2, Artifacts: 1, FailAtTurn: 1, RetryableFail: true,
	})
	var retryCause error
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-test",
		RetryDelay:            time.Millisecond,
		RetryObserver:         func(cause error) { retryCause = cause },
		MaxDeliveryCount:      3,
		DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, harness)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := manager.RunOnce(ctx)
	if err != nil || outcome != worker.OutcomeRetried {
		t.Fatalf("first outcome/error = %q/%v, want retried/nil", outcome, err)
	}
	if retryCause == nil {
		t.Fatal("retry observer did not receive the retry cause")
	}
	if state.jobs[key].Checkpoint == nil || state.jobs[key].Checkpoint.Sequence != 1 {
		t.Fatalf("checkpoint after failure = %#v, want sequence 1", state.jobs[key].Checkpoint)
	}
	outcome, err = manager.RunOnce(ctx)
	if err != nil || outcome != worker.OutcomeCompleted {
		t.Fatalf("second outcome/error = %q/%v, want completed/nil", outcome, err)
	}
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	outcome, err = manager.RunOnce(ctx)
	if err != nil || outcome != worker.OutcomeDuplicate {
		t.Fatalf("duplicate outcome/error = %q/%v, want duplicate/nil", outcome, err)
	}
	if state.completions != 1 {
		t.Fatalf("completion count = %d, want 1", state.completions)
	}
}

func TestWorkerPersistsDurableCancellation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	key := jobKey(loaded.Run.TenantID, loaded.Run.ID)
	state.jobs[key] = loaded
	state.cancelled[key] = true
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	harness, _ := deterministicharness.New(deterministicharness.Config{})
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-test",
		DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, harness)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := manager.RunOnce(ctx)
	if err != nil || outcome != worker.OutcomeCancelled {
		t.Fatalf("outcome/error = %q/%v, want cancelled/nil", outcome, err)
	}
	if state.failures != 1 || state.jobs[key].Run.Status != domain.RunCancelled {
		t.Fatalf("failure count/status = %d/%s, want 1/cancelled", state.failures, state.jobs[key].Run.Status)
	}
	if len(state.deliveries) != 1 || !strings.Contains(state.deliveries[0].Text, "cancelled") {
		t.Fatalf("cancellation delivery = %+v", state.deliveries)
	}
}

func TestWorkerRenewsLeaseAtDurableBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-test", LeaseTTL: time.Minute,
		DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, advancingHarness{clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := manager.RunOnce(ctx)
	if err != nil || outcome != worker.OutcomeCompleted {
		t.Fatalf("outcome/error = %q/%v, want completed/nil", outcome, err)
	}
	if state.renewals != 1 {
		t.Fatalf("lease renewals = %d, want 1", state.renewals)
	}
}

func TestWorkerEnforcesRuntimeLimitAndCancelsHarness(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	loaded.Job.Limits.MaxRuntime = 10 * time.Millisecond
	state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	harness := &blockingHarness{}
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-test",
		DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, harness)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := manager.RunOnce(ctx)
	if err != nil || outcome != worker.OutcomeFailed {
		t.Fatalf("outcome/error = %q/%v, want failed/nil", outcome, err)
	}
	if harness.cancelCalls != 1 || state.failures != 1 ||
		!strings.Contains(state.deliveries[0].Text, "runtime_limit_exceeded") {
		t.Fatalf(
			"cancel calls/failures/delivery = %d/%d/%+v",
			harness.cancelCalls, state.failures, state.deliveries,
		)
	}
}

func newDeliveryWakePublisher(t *testing.T) ports.TelegramDeliveryWakePublisher {
	t.Helper()
	publisher, err := outboxwake.NewPublisher(testkit.NewMemoryQueue())
	if err != nil {
		t.Fatal(err)
	}
	return publisher
}

func workerFixture(
	t *testing.T,
	ctx context.Context,
	blobs *memoryBlobs,
	tenant domain.TenantID,
	at time.Time,
) ports.WorkerJobState {
	t.Helper()
	suffix := strings.TrimPrefix(string(tenant), "tenant-")
	run := domain.Run{
		ID: domain.RunID("run-" + suffix), TenantID: tenant,
		SessionID:                domain.SessionID("session-" + suffix),
		TriggerEventID:           domain.SessionEventID("event-" + suffix),
		SubscriptionConnectionID: domain.SubscriptionConnectionID("subscription-" + suffix),
		Status:                   domain.RunQueued,
		IdempotencyKey:           domain.IdempotencyKey("run-key-" + suffix),
		CreatedAt:                at, UpdatedAt: at,
	}
	attempt := domain.Attempt{
		ID: domain.AttemptID("attempt-" + suffix), TenantID: tenant,
		RunID: run.ID, Number: 1, Status: domain.AttemptCreated,
		CreatedAt: at, UpdatedAt: at,
	}
	contextRef := blobs.seed(t, ctx, tenant, "context", []byte("context-"+suffix))
	inputRef := blobs.seed(t, ctx, tenant, "input", []byte("input-"+suffix))
	manifest := domain.ArtifactManifest{
		ID:       domain.ArtifactManifestID("input-manifest-" + suffix),
		TenantID: tenant, RunID: run.ID, CreatedAt: at,
		Artifacts: []domain.Artifact{{
			Name: "input.txt", MediaType: "text/plain", Blob: inputRef,
		}},
	}
	reservation := domain.QuotaReservation{
		ID:       domain.QuotaReservationID("reservation-" + suffix),
		TenantID: tenant, RunID: run.ID,
		SubscriptionConnectionID: run.SubscriptionConnectionID,
		Status:                   domain.ReservationHeld, CapacityUnits: 1,
		HeldAt: at, ExpiresAt: at.Add(time.Hour), UpdatedAt: at,
	}
	job := domain.WorkerJob{
		TenantID: tenant, RunID: run.ID, AttemptID: attempt.ID,
		SessionID: run.SessionID, TriggerEventID: run.TriggerEventID,
		ReservationID: reservation.ID, InputManifestID: manifest.ID,
		ContextSnapshot:   contextRef,
		AllowedMCPServers: []string{"docs"},
		Limits: domain.ProductLimits{
			MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute,
			MaxTurns: 10, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20,
			MaxArtifacts: 10,
		},
		DeliveryChat: domain.TelegramChatRef{
			TenantID: tenant, ChatID: int64(len(suffix) + 1),
		},
		ReplyToMessageID: 10, CreatedAt: at,
	}
	return ports.WorkerJobState{
		Job: job, Run: run, Attempt: attempt,
		Reservation: reservation, InputManifest: manifest,
	}
}

func publishWorkerMessage(
	t *testing.T,
	ctx context.Context,
	queue ports.Queue,
	tenant domain.TenantID,
	runID domain.RunID,
) {
	t.Helper()
	if err := queue.Publish(ctx, queuecontract.Envelope{
		Schema: queuecontract.SchemaV1, MessageID: domain.MessageID("message-" + string(runID)),
		Kind: queuecontract.KindDispatchRun, TenantID: tenant,
		SubjectID: string(runID), EnqueuedAt: workerTestTime,
	}); err != nil {
		t.Fatal(err)
	}
}

func assertScratchEmpty(t *testing.T, root string) {
	t.Helper()
	entries, err := osReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("scratch root contains %d entries after invocation", len(entries))
	}
}

var osReadDir = func(name string) ([]string, error) {
	entries, err := os.ReadDir(name)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry.Name())
	}
	return result, nil
}

type memoryBlobs struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemoryBlobs() *memoryBlobs {
	return &memoryBlobs{data: make(map[string][]byte)}
}

func (store *memoryBlobs) seed(
	t *testing.T,
	ctx context.Context,
	tenant domain.TenantID,
	key string,
	data []byte,
) domain.BlobRef {
	t.Helper()
	ref, err := store.Put(ctx, tenant, key, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func (store *memoryBlobs) Put(
	_ context.Context,
	tenant domain.TenantID,
	key string,
	body io.Reader,
) (domain.BlobRef, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return domain.BlobRef{}, err
	}
	if strings.HasPrefix(key, "tenants/") && !strings.HasPrefix(key, domain.TenantObjectPrefix(tenant)) {
		return domain.BlobRef{}, domain.TenantMismatchError{Expected: tenant}
	}
	if !strings.HasPrefix(key, domain.TenantObjectPrefix(tenant)) {
		key = domain.TenantObjectPrefix(tenant) + key
	}
	sum := sha256.Sum256(data)
	ref := domain.BlobRef{
		TenantID: tenant, Key: key, Size: int64(len(data)),
		SHA256: hex.EncodeToString(sum[:]),
	}
	store.mu.Lock()
	store.data[key] = append([]byte(nil), data...)
	store.mu.Unlock()
	return ref, nil
}

func (store *memoryBlobs) Open(
	_ context.Context,
	tenant domain.TenantID,
	ref domain.BlobRef,
) (io.ReadCloser, error) {
	if tenant != ref.TenantID {
		return nil, domain.TenantMismatchError{Expected: tenant, Actual: ref.TenantID}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	data, ok := store.data[ref.Key]
	if !ok {
		return nil, errors.New("blob not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (store *memoryBlobs) Delete(_ context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if tenant != ref.TenantID {
		return domain.TenantMismatchError{Expected: tenant, Actual: ref.TenantID}
	}
	store.mu.Lock()
	delete(store.data, ref.Key)
	store.mu.Unlock()
	return nil
}

type workerState struct {
	mu          sync.Mutex
	jobs        map[string]ports.WorkerJobState
	leases      map[string]domain.Lease
	cancelled   map[string]bool
	checkpoints []domain.Checkpoint
	usage       []domain.UsageObservation
	manifests   []domain.ArtifactManifest
	events      []domain.SessionEventDraft
	deliveries  []domain.TelegramDeliveryOutbox
	completions int
	failures    int
	renewals    int
}

func newWorkerState() *workerState {
	return &workerState{
		jobs:      make(map[string]ports.WorkerJobState),
		leases:    make(map[string]domain.Lease),
		cancelled: make(map[string]bool),
	}
}

func (state *workerState) LoadWorkerJob(
	_ context.Context,
	tenant domain.TenantID,
	runID domain.RunID,
) (ports.WorkerJobState, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	loaded, ok := state.jobs[jobKey(tenant, runID)]
	return loaded, ok, nil
}

func (state *workerState) ClaimWorkerLease(
	_ context.Context,
	request ports.WorkerLeaseRequest,
) (domain.Lease, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	key := jobKey(request.TenantID, request.RunID)
	if lease, ok := state.leases[key]; ok {
		if lease.ID == request.LeaseID {
			return lease, nil
		}
		return domain.Lease{}, errors.New("lease held")
	}
	lease := domain.Lease{
		ID: request.LeaseID, TenantID: request.TenantID,
		RunID: request.RunID, AttemptID: request.AttemptID,
		WorkerID: request.WorkerID, FenceToken: 1,
		AcquiredAt: request.Now, ExpiresAt: request.ExpiresAt,
	}
	state.leases[key] = lease
	return lease, nil
}

func (state *workerState) StartWorkerJob(
	_ context.Context,
	loaded ports.WorkerJobState,
	lease domain.Lease,
	at time.Time,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	key := jobKey(loaded.Run.TenantID, loaded.Run.ID)
	current := state.jobs[key]
	if current.Run.Status == domain.RunRunning {
		return nil
	}
	if err := current.Run.Transition(domain.RunRunning, at); err != nil {
		return err
	}
	current.Attempt.WorkerID = lease.WorkerID
	if err := current.Attempt.Transition(domain.AttemptRunning, at); err != nil {
		return err
	}
	state.jobs[key] = current
	return nil
}

func (state *workerState) RenewWorkerLease(
	_ context.Context,
	tenant domain.TenantID,
	leaseID domain.LeaseID,
	fence uint64,
	now time.Time,
	newExpiry time.Time,
) (domain.Lease, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	for key, lease := range state.leases {
		if lease.TenantID == tenant && lease.ID == leaseID && lease.FenceToken == fence {
			lease.ExpiresAt = newExpiry
			state.leases[key] = lease
			state.renewals++
			return lease, nil
		}
	}
	return domain.Lease{}, errors.New("lease lost")
}

func (state *workerState) CommitWorkerEvent(_ context.Context, event ports.WorkerEventCommit) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.checkpoints = append(state.checkpoints, event.Checkpoint)
	if event.Usage != nil {
		state.usage = append(state.usage, *event.Usage)
	}
	key := jobKey(event.Checkpoint.TenantID, event.Checkpoint.RunID)
	current := state.jobs[key]
	checkpoint := event.Checkpoint
	current.Checkpoint = &checkpoint
	state.jobs[key] = current
	return nil
}

func (state *workerState) CompleteWorkerJob(
	_ context.Context,
	completion ports.WorkerCompletion,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	key := jobKey(completion.TenantID, completion.RunID)
	current := state.jobs[key]
	if current.Run.Status == domain.RunSucceeded {
		return nil
	}
	if err := current.Run.Transition(domain.RunSucceeded, completion.At); err != nil {
		return err
	}
	if err := current.Attempt.Transition(domain.AttemptSucceeded, completion.At); err != nil {
		return err
	}
	if err := current.Reservation.Transition(domain.ReservationCommitted, completion.At); err != nil {
		return err
	}
	state.jobs[key] = current
	state.manifests = append(state.manifests, completion.Manifest)
	state.events = append(state.events, completion.Events...)
	state.completions++
	return nil
}

func (state *workerState) CompleteLegacyTelegramWorkerJob(
	_ context.Context,
	completion ports.LegacyTelegramWorkerCompletion,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	key := jobKey(completion.TenantID, completion.RunID)
	current := state.jobs[key]
	if current.Run.Status == domain.RunSucceeded {
		return nil
	}
	if err := current.Run.Transition(domain.RunSucceeded, completion.At); err != nil {
		return err
	}
	if err := current.Attempt.Transition(domain.AttemptSucceeded, completion.At); err != nil {
		return err
	}
	if err := current.Reservation.Transition(domain.ReservationCommitted, completion.At); err != nil {
		return err
	}
	state.jobs[key] = current
	state.manifests = append(state.manifests, completion.Manifest)
	state.deliveries = append(state.deliveries, completion.Delivery)
	state.completions++
	return nil
}

func (state *workerState) FailWorkerJob(_ context.Context, failure ports.WorkerFailure) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	key := jobKey(failure.TenantID, failure.RunID)
	current := state.jobs[key]
	runStatus, attemptStatus := domain.RunFailed, domain.AttemptFailed
	if failure.Cancelled {
		runStatus, attemptStatus = domain.RunCancelled, domain.AttemptCancelled
	}
	if err := current.Run.Transition(runStatus, failure.At); err != nil {
		return err
	}
	if err := current.Attempt.Transition(attemptStatus, failure.At); err != nil {
		return err
	}
	if err := current.Reservation.Transition(domain.ReservationReleased, failure.At); err != nil {
		return err
	}
	state.jobs[key] = current
	state.events = append(state.events, failure.Events...)
	state.failures++
	return nil
}

func (state *workerState) FailLegacyTelegramWorkerJob(
	_ context.Context,
	failure ports.LegacyTelegramWorkerFailure,
) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	key := jobKey(failure.TenantID, failure.RunID)
	current := state.jobs[key]
	runStatus, attemptStatus := domain.RunFailed, domain.AttemptFailed
	if failure.Cancelled {
		runStatus, attemptStatus = domain.RunCancelled, domain.AttemptCancelled
	}
	if err := current.Run.Transition(runStatus, failure.At); err != nil {
		return err
	}
	if err := current.Attempt.Transition(attemptStatus, failure.At); err != nil {
		return err
	}
	if err := current.Reservation.Transition(domain.ReservationReleased, failure.At); err != nil {
		return err
	}
	state.jobs[key] = current
	state.deliveries = append(state.deliveries, failure.Delivery)
	state.failures++
	return nil
}

func (state *workerState) CancellationRequested(
	_ context.Context,
	tenant domain.TenantID,
	runID domain.RunID,
) (bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.cancelled[jobKey(tenant, runID)], nil
}

func jobKey(tenant domain.TenantID, runID domain.RunID) string {
	return string(tenant) + "/" + string(runID)
}

var (
	_ ports.BlobStore                      = (*memoryBlobs)(nil)
	_ ports.WorkerStateStore               = (*workerState)(nil)
	_ ports.LegacyTelegramWorkerStateStore = (*workerState)(nil)
)

type advancingHarness struct {
	clock *testkit.FakeClock
}

func (harness advancingHarness) Execute(
	ctx context.Context,
	request ports.ExecutionRequest,
	sink ports.ExecutionEventSink,
) (ports.ExecutionResult, error) {
	harness.clock.Advance(40 * time.Second)
	inputTokens, outputTokens := uint64(1), uint64(1)
	if err := sink.Emit(ctx, ports.ExecutionEvent{
		Sequence: 1, Boundary: "turn-1", CheckpointState: []byte(`{"turn":1}`),
		InputTokens: &inputTokens, OutputTokens: &outputTokens,
	}); err != nil {
		return ports.ExecutionResult{}, err
	}
	return ports.ExecutionResult{Summary: "renewed"}, nil
}

func (advancingHarness) Cancel(context.Context, ports.ExecutionIdentity) error {
	return nil
}

var _ ports.HarnessDriver = advancingHarness{}

type canonicalHarness struct{}

func (canonicalHarness) Execute(
	ctx context.Context,
	_ ports.ExecutionRequest,
	sink ports.ExecutionEventSink,
) (ports.ExecutionResult, error) {
	if err := sink.Emit(ctx, ports.ExecutionEvent{
		Sequence: 1, Boundary: "tool-boundary", CheckpointState: []byte(`{"turn":1}`),
	}); err != nil {
		return ports.ExecutionResult{}, err
	}
	return ports.ExecutionResult{
		Summary: "canonical result",
		ToolEvents: []ports.ExecutionToolEvent{
			{Kind: domain.SessionEventToolCall, CallID: "call-1", ToolName: "fixture", Payload: []byte(`{"arguments":{"value":1}}`)},
			{Kind: domain.SessionEventToolResult, CallID: "call-1", ToolName: "fixture", Payload: []byte(`{"result":{"value":2}}`)},
		},
	}, nil
}

func (canonicalHarness) Cancel(context.Context, ports.ExecutionIdentity) error { return nil }

var _ ports.HarnessDriver = canonicalHarness{}

type blockingHarness struct {
	cancelCalls int
}

func (*blockingHarness) Execute(
	ctx context.Context,
	_ ports.ExecutionRequest,
	_ ports.ExecutionEventSink,
) (ports.ExecutionResult, error) {
	<-ctx.Done()
	return ports.ExecutionResult{}, ctx.Err()
}

func (harness *blockingHarness) Cancel(context.Context, ports.ExecutionIdentity) error {
	harness.cancelCalls++
	return nil
}

var _ ports.HarnessDriver = (*blockingHarness)(nil)
