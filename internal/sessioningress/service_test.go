package sessioningress_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"gitcode.com/urandon/sessionless/internal/syntheticfrontend"
)

func TestSyntheticFrontendCreatesSwitchesAndIngestsCanonicalSessions(t *testing.T) {
	store := newMemoryCanonicalStore()
	blobs := newMemoryBlobs()
	service, err := sessioningress.New(testIngressConfig("k"), store, blobs)
	if err != nil {
		t.Fatal(err)
	}
	adapter := syntheticfrontend.New(service, "tenant-a", "user-a", "conversation-a")
	ctx := context.Background()
	now := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	initial, err := adapter.EnsureSession(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := adapter.NewSession(ctx, initial.Binding.Revision, "reset-1", now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	third, err := adapter.NewSession(ctx, second.Binding.Revision, "reset-2", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if initial.Session.ID == second.Session.ID || second.Session.ID == third.Session.ID {
		t.Fatalf("clean context reused a session: %#v %#v %#v", initial, second, third)
	}
	if len(store.sessions) != 3 {
		t.Fatalf("retained sessions = %d, want 3", len(store.sessions))
	}

	first, err := adapter.Send(ctx, "delivery-1", "hello from synthetic", "subscription-a", now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	objectCount := len(blobs.objects)
	fourth, err := adapter.NewSession(
		ctx, third.Binding.Revision, "reset-after-delivery", now.Add(4*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := adapter.Send(ctx, "delivery-1", "hello from synthetic", "subscription-a", now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || duplicate.Created || first.RunID != duplicate.RunID || first.EventID != duplicate.EventID {
		t.Fatalf("first=%#v duplicate=%#v", first, duplicate)
	}
	if duplicate.SessionID != third.Session.ID || fourth.Session.ID == duplicate.SessionID {
		t.Fatalf("duplicate session=%s current session=%s", duplicate.SessionID, fourth.Session.ID)
	}
	if len(blobs.objects) != objectCount {
		t.Fatalf("duplicate wrote immutable objects: before=%d after=%d", objectCount, len(blobs.objects))
	}
	if len(store.commits) != 1 {
		t.Fatalf("canonical commits = %d, want 1", len(store.commits))
	}
	commit := store.commits[0]
	if commit.Origin.Frontend != syntheticfrontend.Frontend || commit.Origin.ExternalEventID != "delivery-1" {
		t.Fatalf("origin = %#v", commit.Origin)
	}
	prefix := domain.SessionEventObjectPrefix("tenant-a", third.Session.ID, first.EventID)
	if !strings.HasPrefix(commit.Payload.Key, prefix) {
		t.Fatalf("payload key = %q, want prefix %q", commit.Payload.Key, prefix)
	}
	if strings.Contains(strings.ToLower(commit.Payload.Key), "telegram") {
		t.Fatalf("synthetic payload leaked a Telegram path: %q", commit.Payload.Key)
	}
	if err := commit.HarnessBinding.ValidateForScope(
		commit.TenantID, commit.UserID, commit.RunID, commit.AttemptID, commit.ExecutionPlacementV2,
	); err != nil {
		t.Fatalf("server-owned harness binding = %+v: %v", commit.HarnessBinding, err)
	}
	if commit.HarnessBinding.Resource.CredentialMode != domain.ProviderCredentialNoneV1 {
		t.Fatalf("fixture credential mode = %q", commit.HarnessBinding.Resource.CredentialMode)
	}
}

func TestObjectFailureDoesNotReachCanonicalTransaction(t *testing.T) {
	store := newMemoryCanonicalStore()
	blobs := newMemoryBlobs()
	blobs.fail = errors.New("object store unavailable")
	service, err := sessioningress.New(testIngressConfig("k"), store, blobs)
	if err != nil {
		t.Fatal(err)
	}
	adapter := syntheticfrontend.New(service, "tenant-a", "user-a", "conversation-a")
	_, err = adapter.Send(
		context.Background(), "delivery-failed", "must not commit", "subscription-a",
		time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC),
	)
	if err == nil {
		t.Fatal("expected object failure")
	}
	if len(store.commits) != 0 {
		t.Fatalf("canonical commits after object failure = %d", len(store.commits))
	}
}

func TestInvalidServerHarnessBindingFailsBeforeObjectWrite(t *testing.T) {
	store := newMemoryCanonicalStore()
	blobs := newMemoryBlobs()
	config := testIngressConfig("h")
	config.HarnessBinder = invalidHarnessBinder{}
	service, err := sessioningress.New(config, store, blobs)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Ingest(context.Background(), sessioningress.UserInput{
		Actor:           sessioningress.Actor{TenantID: "tenant-a", UserID: "user-a", Frontend: syntheticfrontend.Frontend, ExternalConversationID: "conversation-a"},
		ExternalEventID: "invalid-binding", ReceivedAt: time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC),
		Text: "must not be stored", SubscriptionConnectionID: "subscription-a",
	})
	if err == nil {
		t.Fatal("invalid server-owned binding accepted")
	}
	if len(blobs.objects) != 0 || len(store.commits) != 0 {
		t.Fatalf("invalid binding reached effects: blobs=%d commits=%d", len(blobs.objects), len(store.commits))
	}
}

func TestConcurrentPreflightMissCannotOverwriteCanonicalBlob(t *testing.T) {
	baseStore := newMemoryCanonicalStore()
	store := &alwaysMissCanonicalStore{memoryCanonicalStore: baseStore}
	blobs := newMemoryBlobs()
	service, err := sessioningress.New(
		testIngressConfig("r"), store, blobs,
	)
	if err != nil {
		t.Fatal(err)
	}
	input := sessioningress.UserInput{
		Actor: sessioningress.Actor{
			TenantID: "tenant-a", UserID: "user-a", Frontend: syntheticfrontend.Frontend,
			ExternalConversationID: "conversation-a",
		},
		ExternalEventID:          "racing-delivery",
		ReceivedAt:               time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC),
		Text:                     "winning payload",
		SubscriptionConnectionID: "subscription-a",
	}
	first, err := service.Ingest(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	input.Text = "losing payload"
	input.ReceivedAt = input.ReceivedAt.Add(time.Second)
	second, err := service.Ingest(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || second.Created || first.EventID != second.EventID || first.RunID != second.RunID {
		t.Fatalf("first=%#v second=%#v", first, second)
	}
	if len(baseStore.commits) != 1 || len(blobs.objects) != 2 {
		t.Fatalf("commits=%d objects=%d, want one canonical commit and two isolated uploads", len(baseStore.commits), len(blobs.objects))
	}
	canonical := baseStore.commits[0].Payload
	canonicalBytes := blobs.objects[canonical.Key]
	if !bytes.Contains(canonicalBytes, []byte("winning payload")) || bytes.Contains(canonicalBytes, []byte("losing payload")) {
		t.Fatalf("canonical payload was overwritten: %s", canonicalBytes)
	}
	digest := sha256.Sum256(canonicalBytes)
	if canonical.SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("canonical digest=%s actual=%s", canonical.SHA256, hex.EncodeToString(digest[:]))
	}
	for key := range blobs.objects {
		if !strings.Contains(key, "/uploads/") {
			t.Fatalf("object key %q is outside an immutable upload namespace", key)
		}
	}
}

func TestAttachmentsStayInsideTheCanonicalEventPrefix(t *testing.T) {
	store := newMemoryCanonicalStore()
	blobs := newMemoryBlobs()
	service, err := sessioningress.New(
		testIngressConfig("a"), store, blobs,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Ingest(context.Background(), sessioningress.UserInput{
		Actor: sessioningress.Actor{
			TenantID: "tenant-a", UserID: "user-a", Frontend: syntheticfrontend.Frontend,
			ExternalConversationID: "conversation-a",
		},
		ExternalEventID: "delivery-with-file",
		ReceivedAt:      time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC),
		Text:            "inspect this file",
		Attachments: []sessioningress.Attachment{{
			Name: "../../unsafe name.txt", MediaType: "text/plain", Body: strings.NewReader("payload"),
		}},
		SubscriptionConnectionID: "subscription-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	commit := store.commits[0]
	if len(commit.Artifacts) != 2 {
		t.Fatalf("artifacts = %#v", commit.Artifacts)
	}
	prefix := domain.SessionEventObjectPrefix("tenant-a", result.SessionID, result.EventID)
	for _, artifact := range commit.Artifacts {
		if !strings.HasPrefix(artifact.Blob.Key, prefix) {
			t.Fatalf("artifact key = %q, want prefix %q", artifact.Blob.Key, prefix)
		}
		if strings.Contains(artifact.Blob.Key, "..") || strings.Contains(artifact.Blob.Key, "unsafe name") {
			t.Fatalf("unsafe attachment key = %q", artifact.Blob.Key)
		}
	}
}

func TestBoundIngressAcceptsOnlyPromotedEventAttachments(t *testing.T) {
	store := newMemoryCanonicalStore()
	blobs := newMemoryBlobs()
	service, err := sessioningress.New(
		testIngressConfig("w"), store, blobs,
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	session := domain.Session{
		ID: "session-web", TenantID: "tenant-a", CreatedBy: "user-a",
		Status: domain.SessionActive, CreatedAt: now, UpdatedAt: now,
	}
	binding := domain.FrontendBinding{
		ID: "binding-web", TenantID: session.TenantID, Frontend: "web",
		ExternalConversationID: string(session.ID), SessionID: session.ID,
		Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	store.sessions[session.ID], store.bindings[binding.ID] = session, binding
	input := sessioningress.BoundUserInput{
		Actor: sessioningress.Actor{
			TenantID: session.TenantID, UserID: "user-a", Frontend: "web",
			ExternalConversationID: string(session.ID),
		},
		Binding: binding, ExternalEventID: "message-1", ReceivedAt: now,
		Text: "from web", SubscriptionConnectionID: "subscription-a",
	}
	plan, err := service.PlanBound(input)
	if err != nil {
		t.Fatal(err)
	}
	key, err := plan.AttachmentObjectKey("upl_one", 0, "../photo.png")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("image"))
	input.Attachments = []sessioningress.StoredAttachment{{
		Name: "photo.png", MediaType: "image/png",
		Blob: domain.BlobRef{
			TenantID: session.TenantID, Key: key, Size: 5,
			SHA256: hex.EncodeToString(digest[:]),
		},
	}}
	result, err := service.IngestBound(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if result.EventID != plan.EventID || result.RunID != plan.RunID || result.SessionID != session.ID {
		t.Fatalf("result=%#v plan=%#v", result, plan)
	}
	if len(store.commits) != 1 || len(store.commits[0].Artifacts) != 2 {
		t.Fatalf("commits=%#v", store.commits)
	}
	if store.commits[0].Artifacts[1].Blob.Key != key || strings.Contains(key, "..") {
		t.Fatalf("promoted attachment key=%q", key)
	}

	bad := input
	bad.ExternalEventID = "message-2"
	if _, err := service.IngestBound(context.Background(), bad); err == nil {
		t.Fatal("bound ingress accepted an attachment from another event namespace")
	}
}

func TestBoundIngressRejectsBrowserForgedBinding(t *testing.T) {
	service, err := sessioningress.New(
		testIngressConfig("b"),
		newMemoryCanonicalStore(), newMemoryBlobs(),
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	input := sessioningress.BoundUserInput{
		Actor: sessioningress.Actor{
			TenantID: "tenant-a", UserID: "user-a", Frontend: "web",
			ExternalConversationID: "session-a",
		},
		Binding: domain.FrontendBinding{
			ID: "binding-a", TenantID: "tenant-a", Frontend: domain.FrontendTelegram,
			ExternalConversationID: "telegram-chat", SessionID: "session-a",
			Revision: 1, CreatedAt: now, UpdatedAt: now,
		},
		ExternalEventID: "message-1", ReceivedAt: now, Text: "forged",
		SubscriptionConnectionID: "subscription-a",
	}
	if _, err := service.PlanBound(input); err == nil {
		t.Fatal("forged frontend binding was accepted")
	}
}

func TestBoundIngressNamespacesIdempotencyByUser(t *testing.T) {
	service, err := sessioningress.New(
		testIngressConfig("k"),
		newMemoryCanonicalStore(), newMemoryBlobs(),
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	binding := domain.FrontendBinding{
		ID: "binding-web", TenantID: "tenant-a", Frontend: domain.FrontendWeb,
		ExternalConversationID: "session-a", SessionID: "session-a", Revision: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	input := sessioningress.BoundUserInput{
		Actor: sessioningress.Actor{
			TenantID: "tenant-a", UserID: "user-a", Frontend: domain.FrontendWeb,
			ExternalConversationID: "session-a",
		},
		Binding: binding, ExternalEventID: "request-1", ReceivedAt: now,
		Text: "hello", SubscriptionConnectionID: "connection-a",
	}
	first, err := service.PlanBound(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Actor.UserID = "user-b"
	second, err := service.PlanBound(input)
	if err != nil {
		t.Fatal(err)
	}
	if first.IdempotencyKey == second.IdempotencyKey || first.EventID == second.EventID || first.RunID == second.RunID {
		t.Fatalf("bound idempotency collided across users: first=%+v second=%+v", first, second)
	}
}

func TestServiceRequiresAnOpaqueIDSecret(t *testing.T) {
	if _, err := sessioningress.New(sessioningress.Config{}, newMemoryCanonicalStore(), newMemoryBlobs()); err == nil {
		t.Fatal("service accepted a missing ID HMAC key")
	}
}

func testIngressConfig(key string) sessioningress.Config {
	return sessioningress.Config{
		IDKey:         []byte(strings.Repeat(key, 32)),
		HarnessBinder: sessionlessharness.NewDeterministicFixtureBinderV1(),
	}
}

type invalidHarnessBinder struct{}

func (invalidHarnessBinder) BindHarness(context.Context, ports.HarnessBindingRequest) (ports.ManagedExecutionAuthorityV2, error) {
	return ports.ManagedExecutionAuthorityV2{}, nil
}

type memoryCanonicalStore struct {
	sessions map[domain.SessionID]domain.Session
	bindings map[domain.FrontendBindingID]domain.FrontendBinding
	commits  []ports.CanonicalUserEventCommit
	results  map[string]ports.CanonicalUserEventResult
}

func newMemoryCanonicalStore() *memoryCanonicalStore {
	return &memoryCanonicalStore{
		sessions: make(map[domain.SessionID]domain.Session),
		bindings: make(map[domain.FrontendBindingID]domain.FrontendBinding),
		results:  make(map[string]ports.CanonicalUserEventResult),
	}
}

func (store *memoryCanonicalStore) EnsureFrontendSession(
	_ context.Context,
	request ports.FrontendSessionRequest,
) (ports.FrontendSessionState, error) {
	if binding, found := store.bindings[request.BindingID]; found {
		return ports.FrontendSessionState{Session: store.sessions[binding.SessionID], Binding: binding}, nil
	}
	session := domain.Session{
		ID: request.SessionID, TenantID: request.TenantID, CreatedBy: request.UserID,
		Status: domain.SessionActive, CreatedAt: request.At, UpdatedAt: request.At,
	}
	binding := domain.FrontendBinding{
		ID: request.BindingID, TenantID: request.TenantID, Frontend: request.Frontend,
		ExternalConversationID: request.ExternalConversationID, SessionID: request.SessionID,
		Revision: 1, CreatedAt: request.At, UpdatedAt: request.At,
	}
	store.sessions[session.ID], store.bindings[binding.ID] = session, binding
	return ports.FrontendSessionState{Session: session, Binding: binding}, nil
}

func (store *memoryCanonicalStore) CreateAndSwitchFrontendSession(
	_ context.Context,
	request ports.CanonicalSessionSwitchRequest,
) (ports.FrontendSessionState, error) {
	binding := store.bindings[request.BindingID]
	if binding.Revision != request.ExpectedRevision {
		return ports.FrontendSessionState{}, domain.StaleBindingError{Expected: request.ExpectedRevision, Actual: binding.Revision}
	}
	session := domain.Session{
		ID: request.SessionID, TenantID: request.TenantID, CreatedBy: request.UserID,
		Status: domain.SessionActive, CreatedAt: request.At, UpdatedAt: request.At,
	}
	if err := binding.Switch(request.ExpectedRevision, request.SessionID, request.At); err != nil {
		return ports.FrontendSessionState{}, err
	}
	store.sessions[session.ID], store.bindings[binding.ID] = session, binding
	return ports.FrontendSessionState{Session: session, Binding: binding}, nil
}

func (store *memoryCanonicalStore) CommitCanonicalUserEvent(
	_ context.Context,
	request ports.CanonicalUserEventCommit,
) (ports.CanonicalUserEventResult, error) {
	key := string(request.BindingID) + "/" + string(request.IdempotencyKey)
	if result, found := store.results[key]; found {
		result.Created = false
		return result, nil
	}
	binding := store.bindings[request.BindingID]
	result := ports.CanonicalUserEventResult{
		SessionID: binding.SessionID, EventID: request.EventID, Sequence: 1,
		RunID: request.RunID, Created: true,
	}
	store.commits = append(store.commits, request)
	store.results[key] = result
	return result, nil
}

func (store *memoryCanonicalStore) LookupCanonicalUserEvent(
	_ context.Context,
	request ports.CanonicalUserEventLookup,
) (ports.CanonicalUserEventLookupResult, error) {
	key := string(request.BindingID) + "/" + string(request.IdempotencyKey)
	result, found := store.results[key]
	if !found {
		return ports.CanonicalUserEventLookupResult{}, nil
	}
	result.Created = false
	return ports.CanonicalUserEventLookupResult{Result: result, Found: true}, nil
}

type alwaysMissCanonicalStore struct{ *memoryCanonicalStore }

func (*alwaysMissCanonicalStore) LookupCanonicalUserEvent(
	context.Context,
	ports.CanonicalUserEventLookup,
) (ports.CanonicalUserEventLookupResult, error) {
	return ports.CanonicalUserEventLookupResult{}, nil
}

type memoryBlobs struct {
	objects map[string][]byte
	fail    error
}

func newMemoryBlobs() *memoryBlobs { return &memoryBlobs{objects: make(map[string][]byte)} }

func (store *memoryBlobs) Put(
	_ context.Context,
	tenantID domain.TenantID,
	key string,
	body io.Reader,
) (domain.BlobRef, error) {
	if store.fail != nil {
		return domain.BlobRef{}, store.fail
	}
	payload, err := io.ReadAll(body)
	if err != nil {
		return domain.BlobRef{}, err
	}
	digest := sha256.Sum256(payload)
	store.objects[key] = bytes.Clone(payload)
	return domain.BlobRef{
		TenantID: tenantID, Key: key, Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func (store *memoryBlobs) Open(_ context.Context, _ domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	payload, found := store.objects[ref.Key]
	if !found {
		return nil, errors.New("object not found")
	}
	return io.NopCloser(bytes.NewReader(payload)), nil
}

func (store *memoryBlobs) Delete(_ context.Context, _ domain.TenantID, ref domain.BlobRef) error {
	delete(store.objects, ref.Key)
	return nil
}

var (
	_ ports.CanonicalIngressStore = (*memoryCanonicalStore)(nil)
	_ ports.BlobStore             = (*memoryBlobs)(nil)
)
