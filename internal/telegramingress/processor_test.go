package telegramingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
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
	requests   []ports.TelegramIngress
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

func (store *memoryIngressStore) IngestTelegram(
	_ context.Context,
	request ports.TelegramIngress,
) (ports.TelegramIngressResult, error) {
	if err := request.Run.Validate(); err != nil {
		return ports.TelegramIngressResult{}, err
	}
	if err := request.Attempt.ValidateForRun(request.Run); err != nil {
		return ports.TelegramIngressResult{}, err
	}
	if err := request.InputManifest.ValidateForRun(request.Run); err != nil {
		return ports.TelegramIngressResult{}, err
	}
	if err := request.Dispatch.ValidateForAttempt(request.Run, request.Attempt); err != nil {
		return ports.TelegramIngressResult{}, err
	}
	key := fmt.Sprintf("%s:%s:%d", request.TenantID, request.SourceID, request.UpdateID)
	store.mu.Lock()
	defer store.mu.Unlock()
	if existing, ok := store.updates[key]; ok {
		return ports.TelegramIngressResult{RunID: existing, Created: false}, nil
	}
	store.updates[key] = request.Run.ID
	store.requests = append(store.requests, request)
	return ports.TelegramIngressResult{RunID: request.Run.ID, Created: true}, nil
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
	processor, err := NewProcessor(
		ProcessorConfig{SourceID: "bot-primary", Provider: "codex"},
		resolver, testkit.NewSequenceIDGenerator("test-"),
		testkit.NewFakeClock(now), blobs, staticFileFetcher{}, state,
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
	if len(state.requests) != 1 {
		t.Fatalf("committed ingress count = %d, want 1", len(state.requests))
	}
	request := state.requests[0]
	if len(request.InputManifest.Artifacts) != 2 {
		t.Fatalf("artifact count = %d, want message plus document", len(request.InputManifest.Artifacts))
	}
	if request.InputManifest.Artifacts[1].Name != "attachment-01-report.txt" {
		t.Fatalf(
			"document artifact name = %q, want attachment-01-report.txt",
			request.InputManifest.Artifacts[1].Name,
		)
	}
	for _, artifact := range request.InputManifest.Artifacts {
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
	processor, err := NewProcessor(
		ProcessorConfig{SourceID: "bot-primary", Provider: "hermes"},
		resolver, testkit.NewSequenceIDGenerator("command-"),
		testkit.NewFakeClock(now), blobs, staticFileFetcher{}, state,
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
	if len(state.requests) != 0 {
		t.Fatalf("AI ingress count = %d, want 0", len(state.requests))
	}
	if len(blobs.objects) != 0 {
		t.Fatalf("command blobs = %d, want 0", len(blobs.objects))
	}
}

var _ ports.BlobStore = (*memoryBlobStore)(nil)
var _ ports.TelegramIngressStore = (*memoryIngressStore)(nil)
