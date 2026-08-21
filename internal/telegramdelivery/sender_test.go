package telegramdelivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

type senderStore struct {
	delivery    domain.TelegramDeliveryOutbox
	manifest    domain.ArtifactManifest
	listed      bool
	transitions []domain.DeliveryStatus
}

func (*senderStore) ListRunTelegramProjections(
	context.Context,
	domain.TenantID,
	domain.RunID,
	uint64,
) ([]ports.TelegramProjectionReady, error) {
	return nil, nil
}

func (store *senderStore) ListRunTelegramDeliveries(
	_ context.Context,
	tenantID domain.TenantID,
	runID domain.RunID,
	_ uint64,
) ([]ports.TelegramDeliveryReady, error) {
	if store.delivery.TenantID != tenantID || store.delivery.RunID != runID || store.delivery.ID == "" {
		return nil, nil
	}
	return []ports.TelegramDeliveryReady{{
		TenantID: tenantID, DeliveryID: store.delivery.ID,
	}}, nil
}

func (*senderStore) ListReadyTelegramProjections(
	context.Context,
	uint32,
	time.Time,
	uint64,
) ([]ports.TelegramProjectionReady, error) {
	return nil, nil
}

func (*senderStore) MaterializeTelegramProjection(
	context.Context,
	domain.TenantID,
	domain.FrontendProjectionID,
	*ports.TelegramProjectionContent,
	time.Time,
) (ports.TelegramProjectionResult, error) {
	return ports.TelegramProjectionResult{Outcome: ports.TelegramProjectionNoop}, nil
}

func (store *senderStore) GetTelegramDelivery(
	_ context.Context,
	tenantID domain.TenantID,
	deliveryID domain.TelegramDeliveryID,
) (domain.TelegramDeliveryOutbox, bool, error) {
	if store.delivery.TenantID != tenantID || store.delivery.ID != deliveryID {
		return domain.TelegramDeliveryOutbox{}, false, nil
	}
	return store.delivery, true, nil
}

func (store *senderStore) ListReadyTelegramDeliveries(
	_ context.Context,
	_ uint32,
	before time.Time,
	_ uint64,
) ([]ports.TelegramDeliveryReady, error) {
	if store.listed || (store.delivery.NextAttemptAt != nil && store.delivery.NextAttemptAt.After(before)) {
		return nil, nil
	}
	store.listed = true
	return []ports.TelegramDeliveryReady{{
		TenantID: store.delivery.TenantID, DeliveryID: store.delivery.ID,
	}}, nil
}

func (store *senderStore) ClaimTelegramDelivery(
	_ context.Context,
	_ domain.TenantID,
	_ domain.TelegramDeliveryID,
	at time.Time,
) (domain.TelegramDeliveryOutbox, bool, error) {
	if store.delivery.Status.Terminal() ||
		(store.delivery.NextAttemptAt != nil && store.delivery.NextAttemptAt.After(at)) {
		return domain.TelegramDeliveryOutbox{}, false, nil
	}
	if err := store.delivery.Transition(domain.DeliverySending, at, nil); err != nil {
		return domain.TelegramDeliveryOutbox{}, false, err
	}
	return store.delivery, true, nil
}

func (store *senderStore) TransitionTelegramDelivery(
	_ context.Context,
	_ domain.TenantID,
	_ domain.TelegramDeliveryID,
	to domain.DeliveryStatus,
	at time.Time,
	retryAt *time.Time,
) error {
	if err := store.delivery.Transition(to, at, retryAt); err != nil {
		return err
	}
	store.transitions = append(store.transitions, to)
	return nil
}

func (store *senderStore) GetArtifactManifest(
	_ context.Context,
	tenantID domain.TenantID,
	manifestID domain.ArtifactManifestID,
) (domain.ArtifactManifest, bool, error) {
	if store.manifest.TenantID == tenantID && store.manifest.ID == manifestID {
		return store.manifest, true, nil
	}
	return domain.ArtifactManifest{}, false, nil
}

type senderClient struct {
	err      error
	requests []ports.TelegramSendRequest
}

type pagedSenderStore struct {
	*senderStore
	deliveries map[domain.TelegramDeliveryID]domain.TelegramDeliveryOutbox
}

func (store *pagedSenderStore) ListRunTelegramDeliveries(
	_ context.Context,
	tenantID domain.TenantID,
	runID domain.RunID,
	limit uint64,
) ([]ports.TelegramDeliveryReady, error) {
	ids := make([]string, 0, len(store.deliveries))
	for id, delivery := range store.deliveries {
		if delivery.TenantID == tenantID && delivery.RunID == runID && !delivery.Status.Terminal() {
			ids = append(ids, string(id))
		}
	}
	sort.Strings(ids)
	if uint64(len(ids)) > limit {
		ids = ids[:limit]
	}
	result := make([]ports.TelegramDeliveryReady, 0, len(ids))
	for _, id := range ids {
		result = append(result, ports.TelegramDeliveryReady{
			TenantID: tenantID, DeliveryID: domain.TelegramDeliveryID(id),
		})
	}
	return result, nil
}

func (store *pagedSenderStore) GetTelegramDelivery(
	_ context.Context,
	tenantID domain.TenantID,
	deliveryID domain.TelegramDeliveryID,
) (domain.TelegramDeliveryOutbox, bool, error) {
	delivery, found := store.deliveries[deliveryID]
	if !found || delivery.TenantID != tenantID {
		return domain.TelegramDeliveryOutbox{}, false, nil
	}
	return delivery, true, nil
}

func (store *pagedSenderStore) ClaimTelegramDelivery(
	_ context.Context,
	tenantID domain.TenantID,
	deliveryID domain.TelegramDeliveryID,
	at time.Time,
) (domain.TelegramDeliveryOutbox, bool, error) {
	delivery, found, err := store.GetTelegramDelivery(context.Background(), tenantID, deliveryID)
	if err != nil || !found || delivery.Status.Terminal() {
		return domain.TelegramDeliveryOutbox{}, false, err
	}
	if err := delivery.Transition(domain.DeliverySending, at, nil); err != nil {
		return domain.TelegramDeliveryOutbox{}, false, err
	}
	store.deliveries[deliveryID] = delivery
	return delivery, true, nil
}

func (store *pagedSenderStore) TransitionTelegramDelivery(
	_ context.Context,
	tenantID domain.TenantID,
	deliveryID domain.TelegramDeliveryID,
	to domain.DeliveryStatus,
	at time.Time,
	retryAt *time.Time,
) error {
	delivery, found, err := store.GetTelegramDelivery(context.Background(), tenantID, deliveryID)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("delivery not found")
	}
	if err := delivery.Transition(to, at, retryAt); err != nil {
		return err
	}
	store.deliveries[deliveryID] = delivery
	return nil
}

func (client *senderClient) Send(
	_ context.Context,
	request ports.TelegramSendRequest,
) (ports.TelegramSendResult, error) {
	client.requests = append(client.requests, request)
	return ports.TelegramSendResult{}, client.err
}

type projectionSenderStore struct {
	*senderStore
	prepared   ports.TelegramProjectionResult
	candidate  ports.TelegramProjectionReady
	content    *ports.TelegramProjectionContent
	terminal   bool
	listedRun  bool
	listedScan bool
}

func (store *projectionSenderStore) ListRunTelegramProjections(
	_ context.Context,
	tenantID domain.TenantID,
	runID domain.RunID,
	_ uint64,
) ([]ports.TelegramProjectionReady, error) {
	if store.listedRun || store.candidate.TenantID != tenantID || store.candidate.RunID != runID {
		return nil, nil
	}
	store.listedRun = true
	return []ports.TelegramProjectionReady{store.candidate}, nil
}

func (store *projectionSenderStore) ListReadyTelegramProjections(
	_ context.Context,
	_ uint32,
	_ time.Time,
	_ uint64,
) ([]ports.TelegramProjectionReady, error) {
	if store.listedScan {
		return nil, nil
	}
	store.listedScan = true
	return []ports.TelegramProjectionReady{store.candidate}, nil
}

func (store *projectionSenderStore) MaterializeTelegramProjection(
	_ context.Context,
	tenantID domain.TenantID,
	projectionID domain.FrontendProjectionID,
	content *ports.TelegramProjectionContent,
	at time.Time,
) (ports.TelegramProjectionResult, error) {
	if tenantID != store.candidate.TenantID || projectionID != store.candidate.ProjectionID {
		return ports.TelegramProjectionResult{}, errors.New("unexpected projection")
	}
	if store.terminal {
		return ports.TelegramProjectionResult{Outcome: ports.TelegramProjectionTerminal, Code: "binding_stale"}, nil
	}
	if content == nil {
		return store.prepared, nil
	}
	copy := *content
	store.content = &copy
	store.delivery = domain.TelegramDeliveryOutbox{
		ID: store.prepared.DeliveryID, TenantID: tenantID, RunID: store.prepared.RunID,
		Chat:             domain.TelegramChatRef{TenantID: tenantID, ChatID: content.TriggerChatID},
		ReplyToMessageID: content.ReplyToMessageID, Payload: content.EventPayload,
		ArtifactManifestID: content.ArtifactManifestID,
		Status:             domain.DeliveryPending, IdempotencyKey: "projection-delivery-key",
		CreatedAt: at, UpdatedAt: at,
	}
	return ports.TelegramProjectionResult{
		Outcome: ports.TelegramProjectionMaterialized, DeliveryID: store.delivery.ID,
		RunID: store.delivery.RunID, Created: true,
	}, nil
}

type projectionBlobs struct {
	objects map[string][]byte
	opens   int
}

func (store *projectionBlobs) Put(
	_ context.Context,
	tenantID domain.TenantID,
	key string,
	body io.Reader,
) (domain.BlobRef, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return domain.BlobRef{}, err
	}
	if store.objects == nil {
		store.objects = make(map[string][]byte)
	}
	store.objects[key] = append([]byte(nil), data...)
	digest := sha256.Sum256(data)
	return domain.BlobRef{
		TenantID: tenantID, Key: key, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func (store *projectionBlobs) Open(
	_ context.Context,
	_ domain.TenantID,
	ref domain.BlobRef,
) (io.ReadCloser, error) {
	store.opens++
	data, ok := store.objects[ref.Key]
	if !ok {
		return nil, errors.New("blob not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (*projectionBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	return nil
}

func TestSenderRetriesThenMarksSent(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 29, 13, 0, 0, 0, time.UTC)
	tenantID := domain.TenantID("tenant-a")
	store := &senderStore{delivery: domain.TelegramDeliveryOutbox{
		ID: domain.TelegramDeliveryID("delivery-a"), TenantID: tenantID,
		RunID:            domain.RunID("run-a"),
		Chat:             domain.TelegramChatRef{TenantID: tenantID, ChatID: 10},
		ReplyToMessageID: 20,
		Payload: domain.BlobRef{
			TenantID: tenantID, Key: domain.TenantObjectPrefix(tenantID) + "reply.txt",
			SHA256: "0000000000000000000000000000000000000000000000000000000000000000",
		},
		Status: domain.DeliveryPending, IdempotencyKey: domain.IdempotencyKey("reply-a"),
		CreatedAt: now, UpdatedAt: now,
	}}
	clock := testkit.NewFakeClock(now.Add(time.Second))
	client := &senderClient{err: errors.New("rate limited")}
	sender, err := NewSender(Config{BaseBackoff: time.Second}, clock, store, rejectingBlobStore{}, client)
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := sender.RunOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("first pass = %d, %v", processed, err)
	}
	if store.delivery.Status != domain.DeliveryRetryWait {
		t.Fatalf("status = %s, want retry_wait", store.delivery.Status)
	}
	clock.Advance(2 * time.Second)
	store.listed = false
	client.err = nil
	if processed, err := sender.RunOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("second pass = %d, %v", processed, err)
	}
	if store.delivery.Status != domain.DeliverySent {
		t.Fatalf("status = %s, want sent", store.delivery.Status)
	}
}

func TestSenderWakeTargetsOneDeliveryAndTreatsTerminalDuplicateAsNoop(t *testing.T) {
	now := time.Date(2026, 8, 10, 13, 0, 0, 0, time.UTC)
	tenantID := domain.TenantID("tenant-a")
	store := &senderStore{delivery: domain.TelegramDeliveryOutbox{
		ID: "delivery-a", TenantID: tenantID, RunID: "run-a",
		Chat:             domain.TelegramChatRef{TenantID: tenantID, ChatID: 10},
		ReplyToMessageID: 20, Text: "done", Status: domain.DeliveryPending,
		IdempotencyKey: "reply-a", CreatedAt: now, UpdatedAt: now,
	}}
	wakeQueue := testkit.NewMemoryQueue()
	publisher, err := outboxwake.NewPublisher(wakeQueue)
	if err != nil {
		t.Fatal(err)
	}
	clock := testkit.NewFakeClock(now.Add(time.Second))
	sender, err := NewSender(Config{}, clock, store, rejectingBlobStore{}, &senderClient{})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := publisher.PublishTelegramDeliveryWake(
			context.Background(), tenantID, store.delivery.ID, clock.Now(),
		); err != nil {
			t.Fatal(err)
		}
		result, err := sender.RunWake(context.Background(), wakeQueue)
		if err != nil {
			t.Fatal(err)
		}
		want := "sent"
		if attempt == 1 {
			want = "noop"
		}
		if result.Outcome != want {
			t.Fatalf("wake %d outcome = %q, want %q", attempt, result.Outcome, want)
		}
	}
	if store.delivery.Status != domain.DeliverySent {
		t.Fatalf("delivery status = %s", store.delivery.Status)
	}
}

func TestProjectionWakeContinuesPastFullNonTerminalRunPage(t *testing.T) {
	now := time.Date(2026, 8, 21, 21, 0, 0, 0, time.UTC)
	tenantID := domain.TenantID("tenant-projection-page")
	runID := domain.RunID("run-projection-page")
	deliveries := make(map[domain.TelegramDeliveryID]domain.TelegramDeliveryOutbox, 2)
	for index, id := range []domain.TelegramDeliveryID{"delivery-page-a", "delivery-page-b"} {
		deliveries[id] = domain.TelegramDeliveryOutbox{
			ID: id, TenantID: tenantID, RunID: runID,
			Chat:             domain.TelegramChatRef{TenantID: tenantID, ChatID: int64(7000 + index)},
			ReplyToMessageID: int64(80 + index), Text: "page delivery",
			Status: domain.DeliveryPending, IdempotencyKey: domain.IdempotencyKey("page-key-" + id),
			CreatedAt: now, UpdatedAt: now,
		}
	}
	store := &pagedSenderStore{senderStore: &senderStore{}, deliveries: deliveries}
	client := &senderClient{}
	sender, err := NewSender(
		Config{BatchSize: 1}, testkit.NewFakeClock(now.Add(time.Second)),
		store, rejectingBlobStore{}, client,
	)
	if err != nil {
		t.Fatal(err)
	}
	wakeQueue := testkit.NewMemoryQueue()
	publisher, err := outboxwake.NewPublisher(wakeQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishFrontendProjectionWake(context.Background(), tenantID, runID, now); err != nil {
		t.Fatal(err)
	}
	for page := 0; page < 2; page++ {
		result, runErr := sender.RunWake(context.Background(), wakeQueue)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if result.Outcome != "retry" || result.Code != "run_page_continuation" {
			t.Fatalf("page %d result = %#v", page, result)
		}
	}
	result, err := sender.RunWake(context.Background(), wakeQueue)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "noop" || result.Code != "missing_or_terminal" {
		t.Fatalf("terminal continuation = %#v", result)
	}
	if len(client.requests) != 2 {
		t.Fatalf("Telegram requests = %#v", client.requests)
	}
	for id, delivery := range store.deliveries {
		if delivery.Status != domain.DeliverySent {
			t.Fatalf("delivery %s status = %s", id, delivery.Status)
		}
	}
}

func TestSenderWakeDeadLettersUnexpectedEnvelopeKind(t *testing.T) {
	now := time.Date(2026, 8, 10, 13, 0, 0, 0, time.UTC)
	sender, err := NewSender(
		Config{}, testkit.NewFakeClock(now), &senderStore{}, rejectingBlobStore{}, &senderClient{},
	)
	if err != nil {
		t.Fatal(err)
	}
	wakeQueue := testkit.NewMemoryQueue()
	if err := wakeQueue.Publish(context.Background(), queuecontract.Envelope{
		Schema: queuecontract.SchemaV1, MessageID: "msg-unexpected",
		Kind: queuecontract.KindWakeDispatch, TenantID: "tenant-1",
		SubjectID: "dispatch-1", EnqueuedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := sender.RunWake(context.Background(), wakeQueue); err != nil {
		t.Fatal(err)
	}
	if wakeQueue.DeadLetterCount() != 1 {
		t.Fatalf("dead letters = %d, want 1", wakeQueue.DeadLetterCount())
	}
}

func TestSenderProjectionWakeRetriesMaterializedDeliveryAndRepliesToTrigger(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	tenantID := domain.TenantID("tenant-projection")
	runID := domain.RunID("run-projection")
	projectionID := domain.FrontendProjectionID("projection-a")
	blobs := &projectionBlobs{}
	eventData := []byte(`{"schema":"sessionless.assistant-message.v1","summary":"canonical reply","artifact_manifest_id":"manifest-output"}`)
	eventRef, err := blobs.Put(context.Background(), tenantID,
		domain.SessionEventObjectPrefix(tenantID, "session-a", "event-assistant")+"payload.json",
		bytes.NewReader(eventData))
	if err != nil {
		t.Fatal(err)
	}
	triggerData := []byte(`{"version":1,"metadata":{"telegram.chat_id":"7001","telegram.message_id":"91"}}`)
	triggerRef, err := blobs.Put(context.Background(), tenantID,
		domain.SessionEventObjectPrefix(tenantID, "session-a", "event-trigger")+"message.json",
		bytes.NewReader(triggerData))
	if err != nil {
		t.Fatal(err)
	}
	base := &senderStore{manifest: domain.ArtifactManifest{
		ID: "manifest-output", TenantID: tenantID, RunID: runID, CreatedAt: now,
	}}
	store := &projectionSenderStore{
		senderStore: base,
		candidate: ports.TelegramProjectionReady{
			TenantID: tenantID, ProjectionID: projectionID, RunID: runID,
		},
		prepared: ports.TelegramProjectionResult{
			Outcome: ports.TelegramProjectionNeedsContent, RunID: runID,
			DeliveryID: "delivery-projection", EventKind: domain.SessionEventAssistantMessage,
			EventPayload: eventRef, TriggerPayload: triggerRef,
		},
	}
	client := &senderClient{err: errors.New("retryable Telegram failure")}
	clock := testkit.NewFakeClock(now)
	sender, err := NewSender(Config{BaseBackoff: time.Second}, clock, store, blobs, client)
	if err != nil {
		t.Fatal(err)
	}
	wakeQueue := testkit.NewMemoryQueue()
	publisher, err := outboxwake.NewPublisher(wakeQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishFrontendProjectionWake(context.Background(), tenantID, runID, now); err != nil {
		t.Fatal(err)
	}
	result, err := sender.RunWake(context.Background(), wakeQueue)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "retry" || base.delivery.Status != domain.DeliveryRetryWait {
		t.Fatalf("first projection wake = %#v delivery=%#v", result, base.delivery)
	}
	client.err = nil
	clock.Advance(time.Second)
	result, err = sender.RunWake(context.Background(), wakeQueue)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "sent" || base.delivery.Status != domain.DeliverySent {
		t.Fatalf("retried projection wake = %#v delivery=%#v", result, base.delivery)
	}
	if store.content == nil || store.content.EventPayload != eventRef ||
		store.content.TriggerPayload != triggerRef || store.content.TriggerChatID != 7001 ||
		store.content.ReplyToMessageID != 91 || store.content.ArtifactManifestID == nil ||
		*store.content.ArtifactManifestID != "manifest-output" {
		t.Fatalf("materialized content = %#v", store.content)
	}
	if len(client.requests) != 2 || client.requests[0].Payload != eventRef ||
		client.requests[1].Payload != eventRef || client.requests[1].ReplyToMessageID != 91 {
		t.Fatalf("Telegram requests = %#v", client.requests)
	}
}

func TestSenderTerminalProjectionDoesNotReadCanonicalContent(t *testing.T) {
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	store := &projectionSenderStore{
		senderStore: &senderStore{}, terminal: true,
		candidate: ports.TelegramProjectionReady{
			TenantID: "tenant-a", ProjectionID: "projection-a", RunID: "run-a",
		},
	}
	blobs := &projectionBlobs{}
	sender, err := NewSender(Config{}, testkit.NewFakeClock(now), store, blobs, &senderClient{})
	if err != nil {
		t.Fatal(err)
	}
	wakeQueue := testkit.NewMemoryQueue()
	publisher, err := outboxwake.NewPublisher(wakeQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishFrontendProjectionWake(context.Background(), "tenant-a", "run-a", now); err != nil {
		t.Fatal(err)
	}
	result, err := sender.RunWake(context.Background(), wakeQueue)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "noop" || blobs.opens != 0 {
		t.Fatalf("terminal projection result=%#v blob opens=%d", result, blobs.opens)
	}
}

var _ ports.TelegramDeliveryStore = (*senderStore)(nil)
var _ ports.TelegramDeliveryStore = (*projectionSenderStore)(nil)
var _ ports.TelegramClient = (*senderClient)(nil)
var _ ports.BlobStore = (*projectionBlobs)(nil)
