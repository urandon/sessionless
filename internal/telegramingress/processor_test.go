package telegramingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

type memoryBlobStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemoryBlobStore() *memoryBlobStore {
	return &memoryBlobStore{objects: make(map[string][]byte)}
}

func (store *memoryBlobStore) Put(
	_ context.Context,
	tenantID domain.TenantID,
	key string,
	body io.Reader,
) (domain.BlobRef, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return domain.BlobRef{}, err
	}
	if !strings.HasPrefix(key, domain.TenantObjectPrefix(tenantID)) {
		key = domain.TenantObjectPrefix(tenantID) + key
	}
	sum := sha256.Sum256(data)
	store.mu.Lock()
	store.objects[key] = append([]byte(nil), data...)
	store.mu.Unlock()
	return domain.BlobRef{
		TenantID: tenantID, Key: key, Size: int64(len(data)),
		SHA256: hex.EncodeToString(sum[:]),
	}, nil
}

func (store *memoryBlobStore) Open(
	_ context.Context,
	_ domain.TenantID,
	ref domain.BlobRef,
) (io.ReadCloser, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return io.NopCloser(bytes.NewReader(store.objects[ref.Key])), nil
}

func (*memoryBlobStore) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	return nil
}

type memoryIngressStore struct {
	mu         sync.Mutex
	identities map[domain.TenantID]ports.TelegramIdentityState
	updates    map[string]domain.RunID
	commands   []ports.TelegramCommandRequest
}

func (store *memoryIngressStore) ExecuteTelegramCommand(
	_ context.Context,
	request ports.TelegramCommandRequest,
) (ports.TelegramIngressResult, error) {
	key := fmt.Sprintf("%s:%s:%d", request.TenantID, request.SourceID, request.UpdateID)
	store.mu.Lock()
	defer store.mu.Unlock()
	if existing, ok := store.updates[key]; ok {
		return ports.TelegramIngressResult{RunID: existing, Created: false}, nil
	}
	if request.Kind == ports.TelegramCommandNewContext {
		state := store.identities[request.TenantID]
		state.SessionID = request.SessionID
		state.BindingRevision++
		store.identities[request.TenantID] = state
	}
	store.updates[key] = request.RunID
	store.commands = append(store.commands, request)
	return ports.TelegramIngressResult{RunID: request.RunID, Created: true}, nil
}

func newMemoryIngressStore() *memoryIngressStore {
	return &memoryIngressStore{
		identities: make(map[domain.TenantID]ports.TelegramIdentityState),
		updates:    make(map[string]domain.RunID),
	}
}

func (store *memoryIngressStore) EnsureTelegramIdentity(
	_ context.Context,
	request ports.TelegramIdentityRequest,
) (ports.TelegramIdentityState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	state, ok := store.identities[request.TenantID]
	if !ok {
		state = ports.TelegramIdentityState{
			UserID: "user-memory", SessionID: "session-memory-initial",
			BindingID: "binding-memory", BindingRevision: 1,
		}
		store.identities[request.TenantID] = state
	}
	return state, nil
}

type memoryCanonicalStore struct {
	mu       sync.Mutex
	sessions map[domain.SessionID]domain.Session
	bindings map[string]domain.FrontendBinding
	commits  []ports.CanonicalUserEventCommit
	results  map[string]ports.CanonicalUserEventResult
}

func newMemoryCanonicalStore() *memoryCanonicalStore {
	return &memoryCanonicalStore{
		sessions: make(map[domain.SessionID]domain.Session),
		bindings: make(map[string]domain.FrontendBinding),
		results:  make(map[string]ports.CanonicalUserEventResult),
	}
}

func canonicalBindingKey(tenantID domain.TenantID, frontend domain.Frontend, external string) string {
	return string(tenantID) + "/" + string(frontend) + "/" + external
}

func (store *memoryCanonicalStore) EnsureFrontendSession(
	_ context.Context,
	request ports.FrontendSessionRequest,
) (ports.FrontendSessionState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := canonicalBindingKey(request.TenantID, request.Frontend, request.ExternalConversationID)
	if binding, found := store.bindings[key]; found {
		return ports.FrontendSessionState{Session: store.sessions[binding.SessionID], Binding: binding}, nil
	}
	session := domain.Session{
		ID: request.SessionID, TenantID: request.TenantID, CreatedBy: request.UserID,
		Status: domain.SessionActive, CreatedAt: request.At, UpdatedAt: request.At,
	}
	binding := domain.FrontendBinding{
		ID: request.BindingID, TenantID: request.TenantID, Frontend: request.Frontend,
		ExternalConversationID: request.ExternalConversationID, SessionID: session.ID,
		Revision: 1, CreatedAt: request.At, UpdatedAt: request.At,
	}
	store.sessions[session.ID], store.bindings[key] = session, binding
	return ports.FrontendSessionState{Session: session, Binding: binding}, nil
}

func (store *memoryCanonicalStore) CreateAndSwitchFrontendSession(
	_ context.Context,
	request ports.CanonicalSessionSwitchRequest,
) (ports.FrontendSessionState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for key, binding := range store.bindings {
		if binding.ID != request.BindingID {
			continue
		}
		if err := binding.Switch(request.ExpectedRevision, request.SessionID, request.At); err != nil {
			return ports.FrontendSessionState{}, err
		}
		session := domain.Session{
			ID: request.SessionID, TenantID: request.TenantID, CreatedBy: request.UserID,
			Status: domain.SessionActive, CreatedAt: request.At, UpdatedAt: request.At,
		}
		store.sessions[session.ID], store.bindings[key] = session, binding
		return ports.FrontendSessionState{Session: session, Binding: binding}, nil
	}
	return ports.FrontendSessionState{}, fmt.Errorf("binding %q not found", request.BindingID)
}

func (store *memoryCanonicalStore) LookupCanonicalUserEvent(
	_ context.Context,
	request ports.CanonicalUserEventLookup,
) (ports.CanonicalUserEventLookupResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	result, found := store.results[string(request.BindingID)+"/"+string(request.IdempotencyKey)]
	if found {
		result.Created = false
	}
	return ports.CanonicalUserEventLookupResult{Result: result, Found: found}, nil
}

func (store *memoryCanonicalStore) CommitCanonicalUserEvent(
	_ context.Context,
	request ports.CanonicalUserEventCommit,
) (ports.CanonicalUserEventResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := string(request.BindingID) + "/" + string(request.IdempotencyKey)
	if result, found := store.results[key]; found {
		result.Created = false
		return result, nil
	}
	var sessionID domain.SessionID
	for _, binding := range store.bindings {
		if binding.ID == request.BindingID {
			sessionID = binding.SessionID
			break
		}
	}
	result := ports.CanonicalUserEventResult{
		SessionID: sessionID, EventID: request.EventID, Sequence: 1,
		RunID: request.RunID, Created: true,
	}
	store.commits = append(store.commits, request)
	store.results[key] = result
	return result, nil
}

type staticFileFetcher struct{}

func (staticFileFetcher) Fetch(_ context.Context, fileID string) (File, error) {
	return File{
		Name: "report.txt", MediaType: "text/plain",
		Body: io.NopCloser(strings.NewReader("file:" + fileID)),
	}, nil
}

func TestProcessorPersistsTenantScopedMessageAndDeduplicatesUpdate(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	resolver, err := NewIdentityResolver([]byte(strings.Repeat("i", 32)))
	if err != nil {
		t.Fatal(err)
	}
	blobs := newMemoryBlobStore()
	state := newMemoryIngressStore()
	canonicalStore := newMemoryCanonicalStore()
	dispatchQueue, deliveryQueue := testkit.NewMemoryQueue(), testkit.NewMemoryQueue()
	dispatchPublisher, _ := outboxwake.NewPublisher(dispatchQueue)
	deliveryPublisher, _ := outboxwake.NewPublisher(deliveryQueue)
	canonical, err := sessioningress.New(sessioningress.Config{
		IDKey: []byte(strings.Repeat("i", 32)), DispatchWakePublisher: dispatchPublisher,
		HarnessBinder: sessionlessharness.NewDeterministicFixtureBinderV1(),
	}, canonicalStore, blobs)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := NewProcessor(
		ProcessorConfig{
			SourceID: "bot-primary", Provider: "codex",
			DeliveryWakePublisher: deliveryPublisher,
		},
		resolver, testkit.NewSequenceIDGenerator("test-"),
		testkit.NewFakeClock(now), staticFileFetcher{}, canonical, state,
	)
	if err != nil {
		t.Fatal(err)
	}
	update := Update{
		UpdateID: 77,
		Message: &Message{
			MessageID: 9, From: User{ID: 2001},
			Chat: Chat{ID: 1001, Type: "private"}, Date: now.Unix(),
			Text: "analyze", Document: &Document{FileID: "file-1", FileName: "report.txt"},
		},
	}
	first, err := processor.Process(context.Background(), update)
	if err != nil {
		t.Fatal(err)
	}
	second, err := processor.Process(context.Background(), update)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || second.Created || second.RunID != first.RunID {
		t.Fatalf("dedup results = %#v then %#v", first, second)
	}
	if len(canonicalStore.commits) != 1 {
		t.Fatalf("canonical ingress count = %d, want 1", len(canonicalStore.commits))
	}
	request := canonicalStore.commits[0]
	if request.Origin.Frontend != domain.FrontendTelegram ||
		request.Origin.ExternalConversationID != "1001" ||
		request.Origin.ExternalEventID != "bot-primary:77" {
		t.Fatalf("canonical Telegram origin = %#v", request.Origin)
	}
	var envelope struct {
		Metadata map[string]string `json:"metadata"`
	}
	if err := json.Unmarshal(blobs.objects[request.Payload.Key], &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Metadata["telegram.update_id"] != "77" ||
		envelope.Metadata["telegram.message_id"] != "9" ||
		envelope.Metadata["telegram.attachment.01.file_id"] != "file-1" {
		t.Fatalf("canonical Telegram metadata = %#v", envelope.Metadata)
	}
	for index := 0; index < 2; index++ {
		message, err := dispatchQueue.Receive(context.Background())
		if err != nil || message.Envelope.SubjectID != string(request.DispatchID) {
			t.Fatalf("dispatch wake %d = %#v, %v", index, message, err)
		}
	}
	if len(request.Artifacts) != 2 {
		t.Fatalf("artifact count = %d, want message plus document", len(request.Artifacts))
	}
	if request.Artifacts[1].Name != "attachment-01-report.txt" {
		t.Fatalf(
			"document artifact name = %q, want attachment-01-report.txt",
			request.Artifacts[1].Name,
		)
	}
	for _, artifact := range request.Artifacts {
		if !strings.HasPrefix(artifact.Blob.Key, domain.TenantObjectPrefix(request.TenantID)) {
			t.Fatalf("cross-tenant blob key %q", artifact.Blob.Key)
		}
	}
}

func TestProcessorRoutesCommandsWithoutCreatingAIIngress(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 29, 12, 30, 0, 0, time.UTC)
	resolver, err := NewIdentityResolver([]byte(strings.Repeat("c", 32)))
	if err != nil {
		t.Fatal(err)
	}
	blobs := newMemoryBlobStore()
	state := newMemoryIngressStore()
	canonicalStore := newMemoryCanonicalStore()
	dispatchQueue, deliveryQueue := testkit.NewMemoryQueue(), testkit.NewMemoryQueue()
	dispatchPublisher, _ := outboxwake.NewPublisher(dispatchQueue)
	deliveryPublisher, _ := outboxwake.NewPublisher(deliveryQueue)
	canonical, err := sessioningress.New(sessioningress.Config{
		IDKey: []byte(strings.Repeat("c", 32)), DispatchWakePublisher: dispatchPublisher,
		HarnessBinder: sessionlessharness.NewDeterministicFixtureBinderV1(),
	}, canonicalStore, blobs)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := NewProcessor(
		ProcessorConfig{
			SourceID: "bot-primary", Provider: "hermes",
			DeliveryWakePublisher: deliveryPublisher,
		},
		resolver, testkit.NewSequenceIDGenerator("command-"),
		testkit.NewFakeClock(now), staticFileFetcher{}, canonical, state,
	)
	if err != nil {
		t.Fatal(err)
	}
	update := Update{
		UpdateID: 88,
		Message: &Message{
			MessageID: 10, From: User{ID: 2002},
			Chat: Chat{ID: 1002, Type: "private"}, Date: now.Unix(),
			Text: "/new",
		},
	}
	first, err := processor.Process(context.Background(), update)
	if err != nil {
		t.Fatal(err)
	}
	second, err := processor.Process(context.Background(), update)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || second.Created || first.RunID != second.RunID {
		t.Fatalf("command dedup results = %#v then %#v", first, second)
	}
	connect := update
	connect.UpdateID++
	connect.Message.MessageID++
	connect.Message.Text = "/connect codex"
	if _, err := processor.Process(context.Background(), connect); err != nil {
		t.Fatal(err)
	}
	if len(state.commands) != 2 ||
		state.commands[0].Kind != ports.TelegramCommandNewContext ||
		state.commands[0].Provider != "hermes" ||
		state.commands[1].Kind != ports.TelegramCommandConnectCodex ||
		state.commands[1].Provider != "codex" {
		t.Fatalf("commands = %#v", state.commands)
	}
	if len(canonicalStore.commits) != 0 {
		t.Fatalf("AI ingress count = %d, want 0", len(canonicalStore.commits))
	}
	if len(blobs.objects) != 0 {
		t.Fatalf("command blobs = %d, want 0", len(blobs.objects))
	}
}

var _ ports.BlobStore = (*memoryBlobStore)(nil)
var _ ports.CanonicalIngressStore = (*memoryCanonicalStore)(nil)
var _ ports.TelegramControlStore = (*memoryIngressStore)(nil)
