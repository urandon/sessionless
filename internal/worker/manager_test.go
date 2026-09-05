package worker_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/deterministicharness"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"gitcode.com/urandon/sessionless/internal/testkit"
	"gitcode.com/urandon/sessionless/internal/worker"
)

var workerTestTime = time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)

func TestWorkerRejectsUnsafeLeaseWatchInterval(t *testing.T) {
	t.Parallel()
	_, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-test", LeaseTTL: time.Minute,
		LeaseWatchdogInterval: 21 * time.Second,
	}, nil, nil, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "one third") {
		t.Fatalf("error = %v, want unsafe lease watch interval rejection", err)
	}
}

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

func TestCanonicalContextReplayMatchesSnapshotTailAndFallsBackFromCorruption(t *testing.T) {
	t.Parallel()
	payloads := [][]byte{
		[]byte(`{"version":1,"text":"first"}`),
		[]byte(`{"version":1,"text":"second"}`),
	}
	fullHistory := runCanonicalContextFixture(t, payloads, false, false)
	snapshotHistory := runCanonicalContextFixture(t, payloads, true, false)
	if !bytes.Equal(fullHistory, snapshotHistory) {
		t.Fatal("snapshot-plus-tail materialization differs from canonical replay")
	}
	fallbackHistory := runCanonicalContextFixture(t, payloads, true, true)
	if !bytes.Equal(fullHistory, fallbackHistory) {
		t.Fatal("corrupt snapshot fallback differs from canonical replay")
	}
}

func runCanonicalContextFixture(
	t *testing.T,
	payloads [][]byte,
	withSnapshot bool,
	corruptSnapshot bool,
) []byte {
	t.Helper()
	ctx := context.Background()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	loaded.Job.Origin = &domain.FrontendEventOrigin{
		BindingID: "binding-1", BindingRevision: 1, Frontend: "synthetic",
		ExternalConversationID: "conversation-1", ExternalEventID: "external-event-1",
	}
	loaded.Job.DeliveryChat = domain.TelegramChatRef{}
	loaded.Job.ReplyToMessageID = 0
	loaded.Job.ContextSnapshot = domain.BlobRef{}
	loaded.Job.Limits.MaxContextEvents = 10
	events := make([]sessioncontext.EventPayload, 0, len(payloads))
	for index, payload := range payloads {
		sequence := uint64(index + 1)
		eventID := domain.SessionEventID(fmt.Sprintf("event-context-%d", sequence))
		if index == 0 {
			attachment := blobs.seed(
				t, ctx, loaded.Run.TenantID,
				domain.SessionEventObjectPrefix(loaded.Run.TenantID, loaded.Run.SessionID, eventID)+"uploads/fixture/attachments/01-image.bin",
				[]byte("image-bytes"),
			)
			var err error
			payload, err = json.Marshal(map[string]any{
				"version": 1, "text": "first",
				"attachments": []map[string]any{{
					"name": "image.bin", "media_type": "application/octet-stream", "blob": attachment,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		ref := blobs.seed(
			t, ctx, loaded.Run.TenantID,
			domain.SessionEventObjectPrefix(loaded.Run.TenantID, loaded.Run.SessionID, eventID)+"payload.json",
			payload,
		)
		author := domain.UserID("user-a")
		event := domain.SessionEvent{
			ID: eventID, TenantID: loaded.Run.TenantID, SessionID: loaded.Run.SessionID,
			Sequence: sequence, Kind: domain.SessionEventUserMessage, AuthorUserID: &author,
			IdempotencyKey: domain.IdempotencyKey(fmt.Sprintf("context-key-%d", sequence)),
			Payload:        ref, CreatedAt: workerTestTime.Add(time.Duration(sequence) * time.Second),
		}
		events = append(events, sessioncontext.EventPayload{Event: event, Payload: payload})
	}
	loaded.Run.TriggerEventID = events[len(events)-1].Event.ID
	loaded.Job.TriggerEventID = loaded.Run.TriggerEventID
	window := &domain.SessionContextWindow{ThroughSequence: uint64(len(events))}
	var snapshot *domain.SessionSnapshot
	if withSnapshot {
		compressed, jsonl, err := sessioncontext.EncodeSnapshot(events[:1])
		if err != nil {
			t.Fatal(err)
		}
		if corruptSnapshot {
			compressed = append([]byte(nil), compressed...)
			compressed[len(compressed)/2] ^= 0xff
		}
		version := uint64(1)
		ref := blobs.seed(
			t, ctx, loaded.Run.TenantID,
			domain.SessionSnapshotObjectKey(loaded.Run.TenantID, loaded.Run.SessionID, version),
			compressed,
		)
		snapshot = &domain.SessionSnapshot{
			ID: "snapshot-context-1", TenantID: loaded.Run.TenantID, SessionID: loaded.Run.SessionID,
			Version: version, ThroughSequence: 1,
			FormatVersion: domain.SessionSnapshotFormatV1,
			Compression:   domain.SessionSnapshotCompressionZstandard,
			EventCount:    1, UncompressedSize: uint64(len(jsonl)), Payload: ref,
			CreatedAt: events[0].Event.CreatedAt,
		}
		window.SnapshotVersion = &version
		window.AfterSequence = 1
	}
	loaded.Job.ContextWindow = window
	state.loadContext = func(request ports.WorkerContextRequest) (domain.SessionContextInput, error) {
		if snapshot != nil && request.AtOrBeforeSnapshotVersion != nil {
			return domain.SessionContextInput{
				TenantID: loaded.Run.TenantID, SessionID: loaded.Run.SessionID,
				Snapshot: snapshot, Events: []domain.SessionEvent{events[1].Event},
			}, nil
		}
		return domain.SessionContextInput{
			TenantID: loaded.Run.TenantID, SessionID: loaded.Run.SessionID,
			Events: []domain.SessionEvent{events[0].Event, events[1].Event},
		}, nil
	}
	state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	var history []byte
	var attachment []byte
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-context",
		DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, captureContextHarness{history: &history, attachment: &attachment})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := manager.RunOnce(ctx)
	if err != nil || outcome != worker.OutcomeCompleted {
		t.Fatalf("outcome/error = %q/%v", outcome, err)
	}
	if string(attachment) != "image-bytes" {
		t.Fatalf("materialized attachment = %q", attachment)
	}
	return history
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

func TestCanonicalWorkerRejectsToolEventsOutsideAdmittedBudgetBeforeUpload(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		maxEvents  uint32
		maxBytes   uint64
		toolEvents []ports.ExecutionToolEvent
	}{
		{
			name: "count", maxEvents: 1, maxBytes: 1 << 20,
			toolEvents: []ports.ExecutionToolEvent{
				{Kind: domain.SessionEventToolCall, CallID: "call-1", ToolName: "lookup", Payload: []byte(`{"query":"one"}`)},
				{Kind: domain.SessionEventToolResult, CallID: "call-1", ToolName: "lookup", Payload: []byte(`{"result":"two"}`)},
			},
		},
		{
			name: "bytes", maxEvents: 2, maxBytes: 8,
			toolEvents: []ports.ExecutionToolEvent{
				{Kind: domain.SessionEventToolCall, CallID: "call-1", ToolName: "lookup", Payload: []byte(`{"query":"too-large"}`)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
			loaded.Job.Limits.MaxToolEvents = test.maxEvents
			loaded.Job.Limits.MaxToolEventBytes = test.maxBytes
			state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
			publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
			manager, err := worker.New(worker.Config{
				ScratchRoot: t.TempDir(), WorkerID: "worker-tool-budget",
				DeliveryWakePublisher: newDeliveryWakePublisher(t),
			}, clock, queue, state, blobs, resultHarness{
				result: ports.ExecutionResult{Summary: "must not complete", ToolEvents: test.toolEvents},
			})
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := manager.RunOnce(ctx)
			if err != nil || outcome != worker.OutcomeFailed {
				t.Fatalf("outcome/error = %q/%v, want failed/nil", outcome, err)
			}
			if state.completions != 0 || state.failures != 1 || len(state.events) != 1 ||
				state.events[0].Kind != domain.SessionEventSystemNotice {
				t.Fatalf("terminal state completions/failures/events = %d/%d/%+v", state.completions, state.failures, state.events)
			}
			eventObjects := 0
			for key := range blobs.data {
				if strings.HasPrefix(key, domain.SessionObjectPrefix(loaded.Run.TenantID, loaded.Run.SessionID)+"events/") {
					eventObjects++
				}
			}
			if eventObjects != 1 {
				t.Fatalf("canonical event objects = %d, want only the terminal notice", eventObjects)
			}
		})
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

func TestWorkerWatchdogRenewsLeaseDuringSilentHarnessCall(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	state.renewalNotify = make(chan error, 1)
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	harness := newWatchdogHarness()
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-test", LeaseTTL: time.Minute,
		LeaseWatchdogInterval: time.Millisecond, DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, harness)
	if err != nil {
		t.Fatal(err)
	}
	done := runWorkerOnce(manager, ctx)
	waitForSignal(t, ctx, harness.started, "harness start")
	clock.Advance(40 * time.Second)
	select {
	case renewalErr := <-state.renewalNotify:
		if renewalErr != nil {
			t.Fatalf("watchdog renewal: %v", renewalErr)
		}
	case <-ctx.Done():
		t.Fatalf("watchdog renewal: %v", ctx.Err())
	}
	close(harness.release)
	result := waitForWorkerResult(t, ctx, done)
	if result.err != nil || result.outcome != worker.OutcomeCompleted {
		t.Fatalf("outcome/error = %q/%v, want completed/nil", result.outcome, result.err)
	}
	if state.renewals == 0 {
		t.Fatal("silent harness call completed without a watchdog lease renewal")
	}
}

func TestWorkerWatchdogRenewsLeaseDuringInputMaterialization(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	state.renewalNotify = make(chan error, 1)
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	materializationStarted, releaseMaterialization := blobs.blockNextOpen()
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-test", LeaseTTL: time.Minute,
		LeaseWatchdogInterval: time.Millisecond, DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, resultHarness{})
	if err != nil {
		t.Fatal(err)
	}
	done := runWorkerOnce(manager, ctx)
	waitForSignal(t, ctx, materializationStarted, "input materialization")
	clock.Advance(40 * time.Second)
	select {
	case renewalErr := <-state.renewalNotify:
		if renewalErr != nil {
			t.Fatalf("watchdog renewal: %v", renewalErr)
		}
	case <-ctx.Done():
		t.Fatalf("watchdog renewal: %v", ctx.Err())
	}
	close(releaseMaterialization)
	result := waitForWorkerResult(t, ctx, done)
	if result.err != nil || result.outcome != worker.OutcomeCompleted {
		t.Fatalf("outcome/error = %q/%v, want completed/nil", result.outcome, result.err)
	}
}

func TestWorkerWatchdogLeaseLossDuringOutputUploadBlocksCompletion(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	key := jobKey(loaded.Run.TenantID, loaded.Run.ID)
	state.jobs[key] = loaded
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	uploadStarted, releaseUpload := blobs.blockNextPut("artifacts/sha256/")
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-test", LeaseTTL: time.Minute,
		LeaseWatchdogInterval: time.Millisecond, DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, outputHarness{})
	if err != nil {
		t.Fatal(err)
	}
	done := runWorkerOnce(manager, ctx)
	waitForSignal(t, ctx, uploadStarted, "output upload")
	state.replaceFence(key)
	result := waitForWorkerResult(t, ctx, done)
	close(releaseUpload)
	if result.err != nil || result.outcome != worker.OutcomeRetried {
		t.Fatalf("outcome/error = %q/%v, want retried/nil", result.outcome, result.err)
	}
	if state.completions != 0 || state.failures != 0 {
		t.Fatalf("completions/failures = %d/%d, want 0/0", state.completions, state.failures)
	}
}

func TestWorkerWatchdogRenewsLeaseDuringCredentialFinalization(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	loaded.Job.CredentialOwnerUserID = "user-a"
	setInvocationHarnessBinding(&loaded.Job, loaded.Run.SubscriptionConnectionID, 1)
	state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	lifecycle := newRecordingCredentialLifecycle(t, "user-a")
	finalizationStarted, releaseFinalization := lifecycle.blockNextWriteBack()
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-credential", LeaseTTL: 2 * time.Minute,
		LeaseWatchdogInterval: time.Millisecond, CredentialMode: worker.CredentialRequired,
		CredentialFinalizeGrace: time.Second, CredentialLifecycle: lifecycle,
		DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, &credentialHarness{})
	if err != nil {
		t.Fatal(err)
	}
	done := runWorkerOnce(manager, ctx)
	waitForSignal(t, ctx, finalizationStarted, "credential finalization")
	renewal := make(chan error, 1)
	state.setRenewalNotify(renewal)
	clock.Advance(40 * time.Second)
	select {
	case renewalErr := <-renewal:
		if renewalErr != nil {
			t.Fatalf("watchdog renewal: %v", renewalErr)
		}
	case <-ctx.Done():
		t.Fatalf("watchdog renewal: %v", ctx.Err())
	}
	close(releaseFinalization)
	result := waitForWorkerResult(t, ctx, done)
	if result.err != nil || result.outcome != worker.OutcomeCompleted {
		t.Fatalf("outcome/error = %q/%v, want completed/nil", result.outcome, result.err)
	}
}

func TestWorkerWatchdogDeliversCancellationToSilentHarness(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	key := jobKey(loaded.Run.TenantID, loaded.Run.ID)
	state.jobs[key] = loaded
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	harness := newWatchdogHarness()
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-test", LeaseTTL: time.Minute,
		LeaseWatchdogInterval: time.Millisecond, DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, harness)
	if err != nil {
		t.Fatal(err)
	}
	done := runWorkerOnce(manager, ctx)
	waitForSignal(t, ctx, harness.started, "harness start")
	state.setCancelled(key)
	waitForSignal(t, ctx, harness.contextCancelled, "harness context cancellation")
	result := waitForWorkerResult(t, ctx, done)
	if result.err != nil || result.outcome != worker.OutcomeCancelled {
		t.Fatalf("outcome/error = %q/%v, want cancelled/nil", result.outcome, result.err)
	}
	if state.completions != 0 || state.failures != 1 || harness.cancelCalls != 1 {
		t.Fatalf("completions/failures/cancel calls = %d/%d/%d, want 0/1/1", state.completions, state.failures, harness.cancelCalls)
	}
}

func TestWorkerWatchdogLeaseLossCancelsAndBlocksSuccess(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clock := testkit.NewFakeClock(workerTestTime)
	queue := testkit.NewMemoryQueue()
	blobs := newMemoryBlobs()
	state := newWorkerState()
	loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
	state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
	publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
	harness := newWatchdogHarness()
	manager, err := worker.New(worker.Config{
		ScratchRoot: t.TempDir(), WorkerID: "worker-test", LeaseTTL: time.Minute,
		LeaseWatchdogInterval: time.Millisecond, DeliveryWakePublisher: newDeliveryWakePublisher(t),
	}, clock, queue, state, blobs, harness)
	if err != nil {
		t.Fatal(err)
	}
	done := runWorkerOnce(manager, ctx)
	waitForSignal(t, ctx, harness.started, "harness start")
	state.replaceFence(jobKey(loaded.Run.TenantID, loaded.Run.ID))
	waitForSignal(t, ctx, harness.contextCancelled, "harness context cancellation")
	result := waitForWorkerResult(t, ctx, done)
	if result.err != nil || result.outcome != worker.OutcomeRetried {
		t.Fatalf("outcome/error = %q/%v, want retried/nil", result.outcome, result.err)
	}
	if state.completions != 0 || state.failures != 0 || harness.cancelCalls != 1 {
		t.Fatalf("completions/failures/cancel calls = %d/%d/%d, want 0/0/1", state.completions, state.failures, harness.cancelCalls)
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
		ScratchRoot: t.TempDir(), WorkerID: "worker-test", LeaseWatchdogInterval: time.Millisecond,
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

func TestRequiredCredentialLifecycleOrchestration(t *testing.T) {
	tests := []struct {
		name           string
		mutateAuth     bool
		harnessError   error
		blockUntilDone bool
		cancelDuring   bool
		writeBackError bool
		blockWriteBack bool
		wantOutcome    worker.Outcome
		wantChanged    bool
	}{
		{name: "success unchanged", wantOutcome: worker.OutcomeCompleted},
		{name: "success changed", mutateAuth: true, wantOutcome: worker.OutcomeCompleted, wantChanged: true},
		{name: "harness error", harnessError: errors.New("private harness detail"), wantOutcome: worker.OutcomeFailed},
		{name: "timeout", blockUntilDone: true, wantOutcome: worker.OutcomeFailed},
		{name: "cancellation", cancelDuring: true, harnessError: context.Canceled, wantOutcome: worker.OutcomeCancelled},
		{name: "writeback failure still releases", writeBackError: true, wantOutcome: worker.OutcomeFailed},
		{name: "writeback deadline still releases", blockWriteBack: true, wantOutcome: worker.OutcomeFailed},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			clock := testkit.NewFakeClock(workerTestTime)
			queue := testkit.NewMemoryQueue()
			blobs := newMemoryBlobs()
			state := newWorkerState()
			loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
			loaded.Job.CredentialOwnerUserID = "user-a"
			setInvocationHarnessBinding(&loaded.Job, loaded.Run.SubscriptionConnectionID, 1)
			if testCase.blockUntilDone {
				loaded.Job.Limits.MaxRuntime = 10 * time.Millisecond
			}
			key := jobKey(loaded.Run.TenantID, loaded.Run.ID)
			state.jobs[key] = loaded
			publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
			lifecycle := newRecordingCredentialLifecycle(t, "user-a")
			lifecycle.writeBackError = testCase.writeBackError
			lifecycle.blockWriteBack = testCase.blockWriteBack
			harness := &credentialHarness{
				mutateAuth: testCase.mutateAuth, executeError: testCase.harnessError,
				blockUntilDone: testCase.blockUntilDone,
			}
			if testCase.cancelDuring {
				harness.beforeReturn = func() {
					state.mu.Lock()
					state.cancelled[key] = true
					state.mu.Unlock()
				}
			}
			finalizeGrace := time.Second
			if testCase.blockWriteBack {
				finalizeGrace = 10 * time.Millisecond
			}
			manager, err := worker.New(worker.Config{
				ScratchRoot: t.TempDir(), WorkerID: "worker-credential",
				LeaseTTL: 2 * time.Minute, CredentialMode: worker.CredentialRequired,
				CredentialFinalizeGrace: finalizeGrace, CredentialLifecycle: lifecycle,
				DeliveryWakePublisher: newDeliveryWakePublisher(t),
			}, clock, queue, state, blobs, harness)
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := manager.RunOnce(ctx)
			if err != nil || outcome != testCase.wantOutcome {
				t.Fatalf("outcome/error = %q/%v, want %q/nil", outcome, err, testCase.wantOutcome)
			}
			if !harness.sawCredential {
				t.Fatal("harness did not receive invocation-only credential fields")
			}
			if got := lifecycle.eventNames(); strings.Join(got, ",") != "issue,materialize,writeback,release" {
				t.Fatalf("lifecycle order = %v", got)
			}
			if lifecycle.changed != testCase.wantChanged {
				t.Fatalf("writeback changed = %t, want %t", lifecycle.changed, testCase.wantChanged)
			}
			if testCase.blockWriteBack && (!lifecycle.writeBackReachedDeadline ||
				!lifecycle.releaseHadDeadline || lifecycle.releaseSawCanceled) {
				t.Fatalf(
					"writeback deadline/release deadline/release cancelled = %t/%t/%t, want true/true/false",
					lifecycle.writeBackReachedDeadline, lifecycle.releaseHadDeadline, lifecycle.releaseSawCanceled,
				)
			}
			if _, err := os.Lstat(lifecycle.materialization.RootDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("credential root remains after finalize: %v", err)
			}
			for _, delivery := range state.deliveries {
				if strings.Contains(delivery.Text, "private") || strings.Contains(delivery.Text, "original-secret") || strings.Contains(delivery.Text, "credential-handle") {
					t.Fatalf("credential detail leaked into durable delivery: %q", delivery.Text)
				}
			}
		})
	}
}

func TestRequiredCredentialFailsClosedBeforeHarness(t *testing.T) {
	tests := []struct {
		name                    string
		configure               func(*ports.WorkerJobState, *recordingCredentialLifecycle, *worker.Config)
		cancelBeforeMaterialize bool
		wantCallerCancellation  bool
		wantEvents              string
	}{
		{name: "missing owner", configure: func(loaded *ports.WorkerJobState, _ *recordingCredentialLifecycle, _ *worker.Config) {
			loaded.Job.CredentialOwnerUserID = ""
		}},
		{name: "forged owner", configure: func(loaded *ports.WorkerJobState, _ *recordingCredentialLifecycle, _ *worker.Config) {
			loaded.Job.CredentialOwnerUserID = "user-forged"
		}},
		{name: "insufficient lease window", configure: func(loaded *ports.WorkerJobState, _ *recordingCredentialLifecycle, config *worker.Config) {
			config.LeaseTTL = loaded.Job.Limits.MaxRuntime + config.CredentialFinalizeGrace
		}},
		{name: "mismatched handle", configure: func(_ *ports.WorkerJobState, lifecycle *recordingCredentialLifecycle, _ *worker.Config) {
			lifecycle.corruptHandle = true
		}, wantEvents: "issue,release"},
		{name: "mismatched materialization", configure: func(_ *ports.WorkerJobState, lifecycle *recordingCredentialLifecycle, _ *worker.Config) {
			lifecycle.corruptMaterialization = true
		}, wantEvents: "issue,materialize,release"},
		{name: "materialize failure with cancelled caller", configure: func(_ *ports.WorkerJobState, lifecycle *recordingCredentialLifecycle, _ *worker.Config) {
			lifecycle.materializeError = true
		}, cancelBeforeMaterialize: true, wantCallerCancellation: true, wantEvents: "issue,materialize,release"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			clock := testkit.NewFakeClock(workerTestTime)
			queue := testkit.NewMemoryQueue()
			blobs := newMemoryBlobs()
			state := newWorkerState()
			loaded := workerFixture(t, ctx, blobs, "tenant-a", workerTestTime)
			loaded.Job.CredentialOwnerUserID = "user-a"
			setInvocationHarnessBinding(&loaded.Job, loaded.Run.SubscriptionConnectionID, 1)
			lifecycle := newRecordingCredentialLifecycle(t, "user-a")
			config := worker.Config{
				ScratchRoot: t.TempDir(), WorkerID: "worker-credential",
				LeaseTTL: 2 * time.Minute, CredentialMode: worker.CredentialRequired,
				CredentialFinalizeGrace: time.Second, CredentialLifecycle: lifecycle,
				DeliveryWakePublisher: newDeliveryWakePublisher(t),
			}
			testCase.configure(&loaded, lifecycle, &config)
			if testCase.cancelBeforeMaterialize {
				lifecycle.cancelOnMaterialize = cancel
			}
			state.jobs[jobKey(loaded.Run.TenantID, loaded.Run.ID)] = loaded
			publishWorkerMessage(t, ctx, queue, loaded.Run.TenantID, loaded.Run.ID)
			harness := &credentialHarness{}
			manager, err := worker.New(config, clock, queue, state, blobs, harness)
			if err != nil {
				t.Fatal(err)
			}
			outcome, err := manager.RunOnce(ctx)
			if testCase.wantCallerCancellation {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("outcome/error = %q/%v, want caller cancellation", outcome, err)
				}
			} else if err != nil || outcome != worker.OutcomeFailed {
				t.Fatalf("outcome/error = %q/%v, want failed/nil", outcome, err)
			}
			if harness.sawCredential {
				t.Fatal("unauthorized credential reached harness")
			}
			if got := strings.Join(lifecycle.eventNames(), ","); got != testCase.wantEvents {
				t.Fatalf("lifecycle events = %q, want %q", got, testCase.wantEvents)
			}
			if strings.Contains(testCase.wantEvents, "release") {
				if !lifecycle.releaseHadDeadline || lifecycle.releaseSawCanceled {
					t.Fatalf(
						"compensating release deadline/cancelled = %t/%t, want true/false",
						lifecycle.releaseHadDeadline, lifecycle.releaseSawCanceled,
					)
				}
			}
			for _, delivery := range state.deliveries {
				if strings.Contains(delivery.Text, "private") || strings.Contains(delivery.Text, "user-forged") || strings.Contains(delivery.Text, "credential-handle") {
					t.Fatalf("credential authorization detail leaked: %q", delivery.Text)
				}
			}
		})
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
	ownerUserID := domain.UserID("user-" + suffix)
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(
		tenant, ownerUserID, run.ID, attempt.ID, run.SubscriptionConnectionID, at,
	)
	if err != nil {
		t.Fatal(err)
	}
	substrateBinding := authority.SubstrateBinding
	admissionCostCeiling := authority.AdmissionCostCeiling
	job := domain.WorkerJob{
		TenantID: tenant, RunID: run.ID, AttemptID: attempt.ID,
		SessionID: run.SessionID, TriggerEventID: run.TriggerEventID,
		ReservationID: reservation.ID, InputManifestID: manifest.ID,
		ContextSnapshot:       contextRef,
		ExecutionPlacementV2:  authority.ExecutionPlacementV2,
		HarnessBinding:        authority.HarnessBinding,
		SubstrateBinding:      &substrateBinding,
		AdmissionCostCeiling:  &admissionCostCeiling,
		CredentialOwnerUserID: ownerUserID,
		AllowedMCPServers:     []string{"docs"},
		Limits: domain.ProductLimits{
			MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute,
			MaxTurns: 10, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20,
			MaxArtifacts: 10, MaxToolEvents: 20, MaxToolEventBytes: 1 << 20,
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

func setInvocationHarnessBinding(job *domain.WorkerJob, resourceID domain.SubscriptionConnectionID, generation uint64) {
	job.HarnessBinding.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
	job.HarnessBinding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
	job.HarnessBinding.Backend.BackendKind = domain.HarnessBackendCodexExecV1
	job.HarnessBinding.Backend.ArtifactKind = domain.HarnessArtifactExecutableV1
	job.HarnessBinding.Resource = domain.ProviderResourceBindingV1{
		Kind: domain.ProviderResourceSubscriptionV1, ResourceID: string(resourceID),
		OwnerUserID: job.CredentialOwnerUserID, Revision: 1,
		CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: generation,
	}
	expires := job.CreatedAt.Add(24 * time.Hour)
	job.HarnessBinding.EvidenceExpiresAt = &expires
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
	mu             sync.Mutex
	data           map[string][]byte
	openStarted    chan struct{}
	openRelease    chan struct{}
	putStarted     chan struct{}
	putRelease     chan struct{}
	blockPutPrefix string
}

func newMemoryBlobs() *memoryBlobs {
	return &memoryBlobs{data: make(map[string][]byte)}
}

func (store *memoryBlobs) blockNextOpen() (<-chan struct{}, chan<- struct{}) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.openStarted = make(chan struct{})
	store.openRelease = make(chan struct{})
	return store.openStarted, store.openRelease
}

func (store *memoryBlobs) blockNextPut(prefix string) (<-chan struct{}, chan<- struct{}) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.putStarted = make(chan struct{})
	store.putRelease = make(chan struct{})
	store.blockPutPrefix = prefix
	return store.putStarted, store.putRelease
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
	ctx context.Context,
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
	started, release := store.putStarted, store.putRelease
	if started == nil || !strings.Contains(key, store.blockPutPrefix) {
		started, release = nil, nil
	} else {
		store.putStarted, store.putRelease, store.blockPutPrefix = nil, nil, ""
	}
	store.mu.Unlock()
	if started != nil {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return domain.BlobRef{}, ctx.Err()
		}
	}
	store.mu.Lock()
	store.data[key] = append([]byte(nil), data...)
	store.mu.Unlock()
	return ref, nil
}

func (store *memoryBlobs) Open(
	ctx context.Context,
	tenant domain.TenantID,
	ref domain.BlobRef,
) (io.ReadCloser, error) {
	if tenant != ref.TenantID {
		return nil, domain.TenantMismatchError{Expected: tenant, Actual: ref.TenantID}
	}
	store.mu.Lock()
	data, ok := store.data[ref.Key]
	data = append([]byte(nil), data...)
	started, release := store.openStarted, store.openRelease
	store.openStarted, store.openRelease = nil, nil
	store.mu.Unlock()
	if !ok {
		return nil, errors.New("blob not found")
	}
	if started != nil {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
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
	mu            sync.Mutex
	jobs          map[string]ports.WorkerJobState
	leases        map[string]domain.Lease
	cancelled     map[string]bool
	checkpoints   []domain.Checkpoint
	usage         []domain.UsageObservation
	manifests     []domain.ArtifactManifest
	events        []domain.SessionEventDraft
	deliveries    []domain.TelegramDeliveryOutbox
	completions   int
	failures      int
	renewals      int
	renewalNotify chan error
	loadContext   func(ports.WorkerContextRequest) (domain.SessionContextInput, error)
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

func (state *workerState) LoadWorkerContext(
	_ context.Context,
	request ports.WorkerContextRequest,
) (domain.SessionContextInput, error) {
	state.mu.Lock()
	loader := state.loadContext
	state.mu.Unlock()
	if loader == nil {
		return domain.SessionContextInput{}, errors.New("worker context fixture is not configured")
	}
	return loader(request)
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

func (state *workerState) LoadWorkerCredentialInvocation(
	_ context.Context,
	tenant domain.TenantID,
	runID domain.RunID,
	attemptID domain.AttemptID,
	leaseID domain.LeaseID,
) (ports.WorkerCredentialInvocationState, bool, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	loaded, found := state.jobs[jobKey(tenant, runID)]
	if !found || loaded.Attempt.ID != attemptID {
		return ports.WorkerCredentialInvocationState{}, false, nil
	}
	lease, found := state.leases[jobKey(tenant, runID)]
	if !found || lease.ID != leaseID {
		return ports.WorkerCredentialInvocationState{}, false, nil
	}
	return ports.WorkerCredentialInvocationState{
		Run: loaded.Run, Attempt: loaded.Attempt, Lease: lease,
	}, true, nil
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
	for key, lease := range state.leases {
		if lease.TenantID == tenant && lease.ID == leaseID && lease.FenceToken == fence {
			lease.ExpiresAt = newExpiry
			state.leases[key] = lease
			state.renewals++
			notify := state.renewalNotify
			state.mu.Unlock()
			notifyRenewal(notify, nil)
			return lease, nil
		}
	}
	notify := state.renewalNotify
	state.mu.Unlock()
	err := errors.New("lease lost")
	notifyRenewal(notify, err)
	return domain.Lease{}, err
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

func (state *workerState) setCancelled(key string) {
	state.mu.Lock()
	state.cancelled[key] = true
	state.mu.Unlock()
}

func (state *workerState) replaceFence(key string) {
	state.mu.Lock()
	lease := state.leases[key]
	lease.FenceToken++
	state.leases[key] = lease
	state.mu.Unlock()
}

func (state *workerState) setRenewalNotify(notify chan error) {
	state.mu.Lock()
	state.renewalNotify = notify
	state.mu.Unlock()
}

func notifyRenewal(channel chan error, err error) {
	if channel == nil {
		return
	}
	select {
	case channel <- err:
	default:
	}
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

func (advancingHarness) Preflight(context.Context, ports.ExecutionIdentity) error {
	return nil
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

func (canonicalHarness) Preflight(context.Context, ports.ExecutionIdentity) error {
	return nil
}

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

type captureContextHarness struct {
	history    *[]byte
	attachment *[]byte
}

func (captureContextHarness) Preflight(context.Context, ports.ExecutionIdentity) error {
	return nil
}

func (harness captureContextHarness) Execute(
	_ context.Context,
	request ports.ExecutionRequest,
	_ ports.ExecutionEventSink,
) (ports.ExecutionResult, error) {
	body, err := os.ReadFile(filepath.Join(request.WorkDir, "context", "history.jsonl"))
	if err != nil {
		return ports.ExecutionResult{}, err
	}
	*harness.history = append([]byte(nil), body...)
	if harness.attachment != nil {
		body, err := os.ReadFile(filepath.Join(
			request.WorkDir, "context", "attachments", "00000000000000000001", "01-image.bin",
		))
		if err != nil {
			return ports.ExecutionResult{}, err
		}
		*harness.attachment = append([]byte(nil), body...)
	}
	return ports.ExecutionResult{Summary: "context captured"}, nil
}

func (captureContextHarness) Cancel(context.Context, ports.ExecutionIdentity) error { return nil }

var _ ports.HarnessDriver = canonicalHarness{}

type resultHarness struct {
	result ports.ExecutionResult
}

func (resultHarness) Preflight(context.Context, ports.ExecutionIdentity) error {
	return nil
}

func (harness resultHarness) Execute(
	context.Context,
	ports.ExecutionRequest,
	ports.ExecutionEventSink,
) (ports.ExecutionResult, error) {
	return harness.result, nil
}

func (resultHarness) Cancel(context.Context, ports.ExecutionIdentity) error { return nil }

var _ ports.HarnessDriver = resultHarness{}

type outputHarness struct{}

func (outputHarness) Preflight(context.Context, ports.ExecutionIdentity) error { return nil }

func (outputHarness) Execute(
	_ context.Context,
	request ports.ExecutionRequest,
	_ ports.ExecutionEventSink,
) (ports.ExecutionResult, error) {
	if err := os.WriteFile(filepath.Join(request.WorkDir, "outputs", "result.txt"), []byte("result"), 0o600); err != nil {
		return ports.ExecutionResult{}, err
	}
	return ports.ExecutionResult{
		Summary: "output fixture completed",
		Outputs: []ports.ExecutionOutput{{
			Name: "result.txt", MediaType: "text/plain", RelativePath: "result.txt",
		}},
	}, nil
}

func (outputHarness) Cancel(context.Context, ports.ExecutionIdentity) error { return nil }

var _ ports.HarnessDriver = outputHarness{}

type blockingHarness struct {
	cancelCalls int
}

func (*blockingHarness) Preflight(context.Context, ports.ExecutionIdentity) error {
	return nil
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

type watchdogHarness struct {
	started          chan struct{}
	release          chan struct{}
	contextCancelled chan struct{}
	cancelCalls      int
}

func newWatchdogHarness() *watchdogHarness {
	return &watchdogHarness{
		started: make(chan struct{}), release: make(chan struct{}), contextCancelled: make(chan struct{}),
	}
}

func (*watchdogHarness) Preflight(context.Context, ports.ExecutionIdentity) error { return nil }

func (harness *watchdogHarness) Execute(
	ctx context.Context,
	_ ports.ExecutionRequest,
	_ ports.ExecutionEventSink,
) (ports.ExecutionResult, error) {
	close(harness.started)
	select {
	case <-harness.release:
		return ports.ExecutionResult{Summary: "watchdog fixture completed"}, nil
	case <-ctx.Done():
		close(harness.contextCancelled)
		return ports.ExecutionResult{}, ctx.Err()
	}
}

func (harness *watchdogHarness) Cancel(context.Context, ports.ExecutionIdentity) error {
	harness.cancelCalls++
	return nil
}

type workerRunResult struct {
	outcome worker.Outcome
	err     error
}

func runWorkerOnce(manager *worker.Manager, ctx context.Context) <-chan workerRunResult {
	done := make(chan workerRunResult, 1)
	go func() {
		outcome, err := manager.RunOnce(ctx)
		done <- workerRunResult{outcome: outcome, err: err}
	}()
	return done
}

func waitForSignal(t *testing.T, ctx context.Context, signal <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-signal:
	case <-ctx.Done():
		t.Fatalf("%s: %v", operation, ctx.Err())
	}
}

func waitForWorkerResult(t *testing.T, ctx context.Context, done <-chan workerRunResult) workerRunResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		t.Fatalf("worker completion: %v", ctx.Err())
		return workerRunResult{}
	}
}

var _ ports.HarnessDriver = (*watchdogHarness)(nil)

type credentialHarness struct {
	mutateAuth     bool
	executeError   error
	blockUntilDone bool
	beforeReturn   func()
	sawCredential  bool
}

func (*credentialHarness) Preflight(context.Context, ports.ExecutionIdentity) error {
	return nil
}

func (harness *credentialHarness) Execute(
	ctx context.Context,
	request ports.ExecutionRequest,
	_ ports.ExecutionEventSink,
) (ports.ExecutionResult, error) {
	if request.Credential.HandleID == "" || request.CredentialMaterialization.FilePath == "" {
		return ports.ExecutionResult{}, errors.New("credential fields are missing")
	}
	harness.sawCredential = true
	if _, err := os.ReadFile(request.CredentialMaterialization.FilePath); err != nil {
		return ports.ExecutionResult{}, err
	}
	if harness.mutateAuth {
		if err := os.WriteFile(request.CredentialMaterialization.FilePath, []byte("changed-secret"), 0o600); err != nil {
			return ports.ExecutionResult{}, err
		}
	}
	if harness.blockUntilDone {
		<-ctx.Done()
		return ports.ExecutionResult{}, ctx.Err()
	}
	if harness.beforeReturn != nil {
		harness.beforeReturn()
	}
	if harness.executeError != nil {
		return ports.ExecutionResult{}, harness.executeError
	}
	return ports.ExecutionResult{Summary: "credential result"}, nil
}

func (*credentialHarness) Cancel(context.Context, ports.ExecutionIdentity) error { return nil }

type recordingCredentialLifecycle struct {
	t                        *testing.T
	root                     string
	expectedOwner            domain.UserID
	mu                       sync.Mutex
	events                   []string
	materialization          ports.CredentialMaterialization
	original                 []byte
	changed                  bool
	writeBackError           bool
	blockWriteBack           bool
	writeBackReachedDeadline bool
	corruptHandle            bool
	corruptMaterialization   bool
	materializeError         bool
	cancelOnMaterialize      func()
	releaseHadDeadline       bool
	releaseSawCanceled       bool
	writeBackStarted         chan struct{}
	writeBackRelease         chan struct{}
}

func newRecordingCredentialLifecycle(t *testing.T, expectedOwner domain.UserID) *recordingCredentialLifecycle {
	t.Helper()
	return &recordingCredentialLifecycle{
		t: t, root: t.TempDir(), expectedOwner: expectedOwner,
		original: []byte("original-secret"),
	}
}

func (lifecycle *recordingCredentialLifecycle) blockNextWriteBack() (<-chan struct{}, chan<- struct{}) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.writeBackStarted = make(chan struct{})
	lifecycle.writeBackRelease = make(chan struct{})
	return lifecycle.writeBackStarted, lifecycle.writeBackRelease
}

func (lifecycle *recordingCredentialLifecycle) Issue(
	_ context.Context,
	request ports.CredentialIssueRequest,
) (ports.CredentialHandle, error) {
	lifecycle.record("issue")
	if request.OwnerUserID != lifecycle.expectedOwner {
		return ports.CredentialHandle{}, errors.New("private credential owner detail")
	}
	handle := ports.CredentialHandle{
		HandleID: "credential-handle", TenantID: request.Run.TenantID,
		SubscriptionConnectionID: request.Run.SubscriptionConnectionID,
		OwnerUserID:              request.OwnerUserID, RunID: request.Run.ID, AttemptID: request.Attempt.ID,
		WorkerID: request.Lease.WorkerID, LeaseID: request.Lease.ID,
		LeaseFence: request.Lease.FenceToken, BindingGeneration: 1, ExpiresAt: request.ExpiresAt,
		ProviderResource: request.ProviderResource,
	}
	if lifecycle.corruptHandle {
		handle.OwnerUserID = "user-other"
	}
	return handle, nil
}

func (lifecycle *recordingCredentialLifecycle) Materialize(
	_ context.Context,
	_ ports.CredentialHandle,
) (ports.CredentialMaterialization, error) {
	lifecycle.record("materialize")
	if lifecycle.cancelOnMaterialize != nil {
		lifecycle.cancelOnMaterialize()
	}
	if lifecycle.materializeError {
		return ports.CredentialMaterialization{}, errors.New("private materialization detail")
	}
	dir, err := os.MkdirTemp(lifecycle.root, "credential-")
	if err != nil {
		return ports.CredentialMaterialization{}, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return ports.CredentialMaterialization{}, err
	}
	materialization := ports.CredentialMaterialization{
		RootDir: dir, AuthFile: filepath.Join(dir, "auth.json"),
	}
	if err := os.WriteFile(materialization.AuthFile, lifecycle.original, 0o600); err != nil {
		return ports.CredentialMaterialization{}, err
	}
	lifecycle.materialization = materialization
	if lifecycle.corruptMaterialization {
		materialization.AuthFile = filepath.Join(dir, "other.json")
	}
	return materialization, nil
}

func (lifecycle *recordingCredentialLifecycle) WriteBack(
	ctx context.Context,
	_ ports.CredentialHandle,
	materialization ports.CredentialMaterialization,
) (ports.CredentialWriteBackResult, error) {
	lifecycle.record("writeback")
	lifecycle.mu.Lock()
	started, release := lifecycle.writeBackStarted, lifecycle.writeBackRelease
	lifecycle.writeBackStarted, lifecycle.writeBackRelease = nil, nil
	lifecycle.mu.Unlock()
	if started != nil {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return ports.CredentialWriteBackResult{}, ctx.Err()
		}
	}
	if lifecycle.blockWriteBack {
		<-ctx.Done()
		lifecycle.writeBackReachedDeadline = errors.Is(ctx.Err(), context.DeadlineExceeded)
		return ports.CredentialWriteBackResult{}, errors.New("private writeback timeout detail")
	}
	content, err := os.ReadFile(materialization.AuthFile)
	if err != nil {
		return ports.CredentialWriteBackResult{}, err
	}
	lifecycle.changed = !bytes.Equal(content, lifecycle.original)
	if lifecycle.writeBackError {
		return ports.CredentialWriteBackResult{}, errors.New("private writeback detail")
	}
	return ports.CredentialWriteBackResult{Changed: lifecycle.changed, Generation: 1}, nil
}

func (lifecycle *recordingCredentialLifecycle) Release(
	ctx context.Context,
	_ ports.CredentialHandle,
) error {
	lifecycle.record("release")
	_, lifecycle.releaseHadDeadline = ctx.Deadline()
	lifecycle.releaseSawCanceled = ctx.Err() != nil
	if lifecycle.materialization.AuthFile != "" {
		_ = os.Remove(lifecycle.materialization.AuthFile)
		_ = os.Remove(lifecycle.materialization.RootDir)
	}
	return nil
}

func (*recordingCredentialLifecycle) RevokeConnection(context.Context, ports.CredentialRevokeRequest) error {
	return nil
}

func (lifecycle *recordingCredentialLifecycle) record(event string) {
	lifecycle.mu.Lock()
	lifecycle.events = append(lifecycle.events, event)
	lifecycle.mu.Unlock()
}

func (lifecycle *recordingCredentialLifecycle) eventNames() []string {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	return append([]string(nil), lifecycle.events...)
}

var _ ports.CredentialLifecycle = (*recordingCredentialLifecycle)(nil)
