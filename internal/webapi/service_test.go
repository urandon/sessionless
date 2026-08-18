package webapi_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/webapi"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

var testNow = time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)

func TestCreateUploadIsIdempotentAndReturnsTheSameCapability(t *testing.T) {
	harness := newHarness(t)
	request := uploadRequest("upload-request-1", "document.pdf", "application/pdf", []byte("pdf"))

	first, created, err := harness.service.CreateUpload(context.Background(), "tenant-a", "user-a", request)
	if err != nil || !created {
		t.Fatalf("first upload = %+v created=%v err=%v", first, created, err)
	}
	second, created, err := harness.service.CreateUpload(context.Background(), "tenant-a", "user-a", request)
	if err != nil || created {
		t.Fatalf("retry upload = %+v created=%v err=%v", second, created, err)
	}
	if first.UploadID != second.UploadID || first.Method != second.Method || first.URL != second.URL ||
		first.ExpiresAt != second.ExpiresAt || first.Headers["x-checksum"] != second.Headers["x-checksum"] {
		t.Fatalf("retry capability changed: first=%+v second=%+v", first, second)
	}
	if len(harness.backend.intents) != 1 {
		t.Fatalf("durable upload intents = %d, want 1", len(harness.backend.intents))
	}
}

func TestCreateUploadRejectsDisallowedTypeAndOversizedObject(t *testing.T) {
	harness := newHarness(t)
	disallowed := uploadRequest("upload-request-1", "page.html", "text/html", []byte("html"))
	if _, _, err := harness.service.CreateUpload(context.Background(), "tenant-a", "user-a", disallowed); err == nil {
		t.Fatal("disallowed media type was accepted")
	}
	oversized := uploadRequest("upload-request-2", "large.txt", "text/plain", []byte("0123456789"))
	oversized.Size = 1025
	if _, _, err := harness.service.CreateUpload(context.Background(), "tenant-a", "user-a", oversized); err == nil {
		t.Fatal("oversized object was accepted")
	}
	if len(harness.backend.intents) != 0 || len(harness.objects.uploads) != 0 {
		t.Fatalf("invalid uploads reached dependencies: intents=%d capabilities=%d", len(harness.backend.intents), len(harness.objects.uploads))
	}
}

func TestCommitUploadUsesAuthoritativeObjectMetadata(t *testing.T) {
	harness := newHarness(t)
	payload := []byte("committed payload")
	response, _, err := harness.service.CreateUpload(
		context.Background(), "tenant-a", "user-a",
		uploadRequest("upload-request-1", "note.txt", "text/plain", payload),
	)
	if err != nil {
		t.Fatal(err)
	}
	intent := harness.backend.intents[response.UploadID]
	harness.objects.put(intent.ObjectKey, "text/plain", payload, "etag-1")

	committed, err := harness.service.CommitUpload(context.Background(), "tenant-a", "user-a", response.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if committed.Status != domain.UploadIntentCommitted || committed.ObservedBlob == nil ||
		committed.ObservedBlob.Key != intent.ObjectKey || committed.ObservedETag != "etag-1" {
		t.Fatalf("committed intent = %+v", committed)
	}
	if harness.backend.lastCommit.Observed != harness.objects.metadata[intent.ObjectKey] {
		t.Fatalf("commit did not receive authoritative HEAD metadata: %+v", harness.backend.lastCommit)
	}
}

func TestSubmitMessagePromotesCommittedUploadAndDeduplicatesCanonicalEvent(t *testing.T) {
	harness := newHarness(t)
	uploadID := prepareCommittedUpload(t, harness, []byte("image"), "photo.png", "image/png")
	request := webcontract.CreateMessageRequest{
		IdempotencyKey: "message-request-1", UploadIDs: []domain.UploadIntentID{uploadID},
	}

	first, err := harness.service.SubmitMessage(context.Background(), "tenant-a", "user-a", "session-a", request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := harness.service.SubmitMessage(context.Background(), "tenant-a", "user-a", "session-a", request)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || second.Created || first.EventID != second.EventID || first.RunID != second.RunID || first.Sequence != second.Sequence {
		t.Fatalf("first=%+v retry=%+v", first, second)
	}
	if len(harness.backend.commits) != 1 || len(harness.backend.events) != 1 || len(harness.backend.runs) != 1 {
		t.Fatalf("commits=%d events=%d runs=%d, want exactly one of each", len(harness.backend.commits), len(harness.backend.events), len(harness.backend.runs))
	}
	if len(harness.objects.promotions) != 1 {
		t.Fatalf("promotion attempts=%d, want retry to return before object work", len(harness.objects.promotions))
	}
	for _, promotion := range harness.objects.promotions {
		prefix := domain.SessionEventObjectPrefix("tenant-a", "session-a", first.EventID)
		if !strings.HasPrefix(promotion.FinalKey, prefix) || strings.Contains(promotion.FinalKey, "../") {
			t.Fatalf("final attachment key %q is outside %q", promotion.FinalKey, prefix)
		}
	}
}

func TestSubmitMessageCommittedRetryDoesNotRequireCurrentComputeConnection(t *testing.T) {
	harness := newHarness(t)
	request := webcontract.CreateMessageRequest{IdempotencyKey: "message-request-1", Text: "hello"}
	first, err := harness.service.SubmitMessage(context.Background(), "tenant-a", "user-a", "session-a", request)
	if err != nil {
		t.Fatal(err)
	}
	harness.backend.connections = nil
	second, err := harness.service.SubmitMessage(context.Background(), "tenant-a", "user-a", "session-a", request)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created || second.Created || first.RunID != second.RunID || harness.backend.resolveCalls != 1 {
		t.Fatalf("first=%+v retry=%+v compute resolutions=%d", first, second, harness.backend.resolveCalls)
	}
	if second.Compute.Provider != "codex" || second.Compute.Entitlement != domain.EntitlementUnknown ||
		second.Compute.Quota != domain.ProviderQuotaUnknown {
		t.Fatalf("retry compute projection = %+v", second.Compute)
	}
}

func TestSubmitMessageRejectsChangedContentForSameIdempotencyKeyBeforeDependencies(t *testing.T) {
	harness := newHarness(t)
	request := webcontract.CreateMessageRequest{IdempotencyKey: "message-request-1", Text: "first"}
	if _, err := harness.service.SubmitMessage(context.Background(), "tenant-a", "user-a", "session-a", request); err != nil {
		t.Fatal(err)
	}
	resolveCalls, commits := harness.backend.resolveCalls, len(harness.backend.commits)
	request.Text = "changed"
	_, err := harness.service.SubmitMessage(context.Background(), "tenant-a", "user-a", "session-a", request)
	if !errors.Is(err, domain.ErrEventIdempotencyConflict) {
		t.Fatalf("changed retry error = %v, want ErrEventIdempotencyConflict", err)
	}
	if harness.backend.resolveCalls != resolveCalls || len(harness.backend.commits) != commits || len(harness.objects.promotions) != 0 {
		t.Fatalf("changed retry reached dependencies: resolutions=%d commits=%d promotions=%d",
			harness.backend.resolveCalls, len(harness.backend.commits), len(harness.objects.promotions))
	}
}

func TestSubmitMessageFailsClosedWhenStagingObjectWasOverwritten(t *testing.T) {
	harness := newHarness(t)
	payload := []byte("original")
	uploadID := prepareCommittedUpload(t, harness, payload, "note.txt", "text/plain")
	intent := harness.backend.intents[uploadID]
	// Even if size and digest still match, an ETag change means the committed
	// source identity was replaced and must not be promoted.
	harness.objects.put(intent.ObjectKey, intent.MediaType, payload, "etag-overwritten")

	_, err := harness.service.SubmitMessage(context.Background(), "tenant-a", "user-a", "session-a", webcontract.CreateMessageRequest{
		IdempotencyKey: "message-request-1", UploadIDs: []domain.UploadIntentID{uploadID},
	})
	if !errors.Is(err, domain.ErrUploadMismatch) {
		t.Fatalf("overwrite error = %v, want ErrUploadMismatch", err)
	}
	if len(harness.objects.promotions) != 0 || len(harness.backend.commits) != 0 {
		t.Fatalf("overwritten staging object progressed: promotions=%d commits=%d", len(harness.objects.promotions), len(harness.backend.commits))
	}
}

func TestSubmitMessageRequiresExactlyOneComputeConnection(t *testing.T) {
	for _, test := range []struct {
		name        string
		connections []ports.ComputeConnectionState
	}{
		{name: "none"},
		{name: "multiple", connections: []ports.ComputeConnectionState{computeConnection("connection-a"), computeConnection("connection-b")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newHarness(t)
			harness.backend.connections = test.connections
			_, err := harness.service.SubmitMessage(context.Background(), "tenant-a", "user-a", "session-a", webcontract.CreateMessageRequest{
				IdempotencyKey: "message-request-1", Text: "hello",
			})
			if !errors.Is(err, webapi.ErrComputeUnavailable) {
				t.Fatalf("compute selection error = %v", err)
			}
			if len(harness.backend.commits) != 0 || len(harness.blobs.values) != 0 {
				t.Fatalf("ambiguous compute selection progressed: commits=%d blobs=%d", len(harness.backend.commits), len(harness.blobs.values))
			}
		})
	}
}

func TestRunAndDownloadSelectorsRemainParticipantScoped(t *testing.T) {
	harness := newHarness(t)
	uploadID := prepareCommittedUpload(t, harness, []byte("secret"), "secret.txt", "text/plain")
	created, err := harness.service.SubmitMessage(context.Background(), "tenant-a", "user-a", "session-a", webcontract.CreateMessageRequest{
		IdempotencyKey: "message-request-1", Text: "private", UploadIDs: []domain.UploadIntentID{uploadID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.service.GetRun(context.Background(), "tenant-a", "user-b", created.RunID); !errors.Is(err, webapi.ErrResourceUnavailable) {
		t.Fatalf("forged run selector error = %v", err)
	}
	if _, err := harness.service.GetRun(context.Background(), "tenant-a", "user-a", "run-missing"); !errors.Is(err, webapi.ErrResourceUnavailable) {
		t.Fatalf("missing run selector error = %v", err)
	}
	for _, selector := range []struct {
		name     string
		tenant   domain.TenantID
		user     domain.UserID
		session  domain.SessionID
		sequence uint64
		index    uint32
	}{
		{name: "other user", tenant: "tenant-a", user: "user-b", session: "session-a", sequence: created.Sequence},
		{name: "other tenant", tenant: "tenant-b", user: "user-a", session: "session-a", sequence: created.Sequence},
		{name: "other session", tenant: "tenant-a", user: "user-a", session: "session-b", sequence: created.Sequence},
		{name: "missing sequence", tenant: "tenant-a", user: "user-a", session: "session-a", sequence: created.Sequence + 1},
		{name: "missing index", tenant: "tenant-a", user: "user-a", session: "session-a", sequence: created.Sequence, index: 1},
	} {
		t.Run(selector.name, func(t *testing.T) {
			before := len(harness.objects.downloads)
			if _, err := harness.service.DownloadAttachment(context.Background(), selector.tenant, selector.user, selector.session, selector.sequence, selector.index); err == nil {
				t.Fatal("forged download selector was accepted")
			}
			if len(harness.objects.downloads) != before {
				t.Fatal("download capability was minted for a forged selector")
			}
		})
	}
	capability, err := harness.service.DownloadAttachment(context.Background(), "tenant-a", "user-a", "session-a", created.Sequence, 0)
	if err != nil || capability.Method != "GET" || len(harness.objects.downloads) != 1 {
		t.Fatalf("authorized download = %+v err=%v", capability, err)
	}
}

func TestWorkerArtifactDownloadIsExactAndParticipantScoped(t *testing.T) {
	harness := newHarness(t)
	runID := domain.RunID("run-worker-output")
	manifestID := domain.ArtifactManifestID("manifest-worker-output")
	finishedAt := testNow
	harness.backend.runs[runID] = ports.RunRecord{Run: domain.Run{
		ID: runID, TenantID: "tenant-a", SessionID: "session-a",
		TriggerEventID: "event-worker-trigger", SubscriptionConnectionID: "connection-a",
		Status: domain.RunSucceeded, IdempotencyKey: "worker-output-run",
		CreatedAt: testNow, UpdatedAt: testNow, FinishedAt: &finishedAt,
	}}
	payload := []byte("worker result")
	key := domain.SessionRunObjectPrefix("tenant-a", "session-a", runID) + "artifacts/sha256/result"
	harness.objects.put(key, "text/plain", payload, "etag-worker-result")
	harness.backend.manifests[manifestID] = domain.ArtifactManifest{
		ID: manifestID, TenantID: "tenant-a", RunID: runID, CreatedAt: testNow,
		Artifacts: []domain.Artifact{{
			Name: "result.txt", MediaType: "text/plain", Blob: harness.objects.metadata[key].Blob,
		}},
	}

	response, err := harness.service.DownloadRunArtifact(
		context.Background(), "tenant-a", "user-a", "session-a", runID, manifestID, 0,
	)
	if err != nil || response.Name != "result.txt" || response.MediaType != "text/plain" ||
		response.Size != int64(len(payload)) || response.Download.Method != "GET" {
		t.Fatalf("worker artifact response = %+v err=%v", response, err)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var public map[string]any
	if err := json.Unmarshal(encoded, &public); err != nil {
		t.Fatal(err)
	}
	if _, exists := public["blob"]; exists || public["key"] != nil || public["sha256"] != nil {
		t.Fatalf("artifact response exposed raw storage fields: %s", encoded)
	}

	for _, selector := range []struct {
		name     string
		tenant   domain.TenantID
		user     domain.UserID
		session  domain.SessionID
		run      domain.RunID
		manifest domain.ArtifactManifestID
		index    uint32
	}{
		{name: "tenant", tenant: "tenant-b", user: "user-a", session: "session-a", run: runID, manifest: manifestID},
		{name: "user", tenant: "tenant-a", user: "user-b", session: "session-a", run: runID, manifest: manifestID},
		{name: "session", tenant: "tenant-a", user: "user-a", session: "session-b", run: runID, manifest: manifestID},
		{name: "run", tenant: "tenant-a", user: "user-a", session: "session-a", run: "run-forged", manifest: manifestID},
		{name: "manifest", tenant: "tenant-a", user: "user-a", session: "session-a", run: runID, manifest: "manifest-forged"},
		{name: "index", tenant: "tenant-a", user: "user-a", session: "session-a", run: runID, manifest: manifestID, index: 1},
	} {
		t.Run(selector.name, func(t *testing.T) {
			before := len(harness.objects.downloads)
			if _, err := harness.service.DownloadRunArtifact(
				context.Background(), selector.tenant, selector.user, selector.session,
				selector.run, selector.manifest, selector.index,
			); !errors.Is(err, webapi.ErrResourceUnavailable) {
				t.Fatalf("forged selector error = %v", err)
			}
			if len(harness.objects.downloads) != before {
				t.Fatal("forged selector minted a download capability")
			}
		})
	}
}

type harness struct {
	service *webapi.Service
	backend *fakeBackend
	objects *fakeObjects
	blobs   *fakeBlobs
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	backend := newFakeBackend()
	blobs := &fakeBlobs{values: make(map[string][]byte)}
	objects := &fakeObjects{metadata: make(map[string]ports.ObjectMetadata)}
	clock := fixedClock{}
	sessions, err := sessionapi.New(sessionapi.Config{
		CursorKey: bytes.Repeat([]byte("c"), 32), IDKey: bytes.Repeat([]byte("s"), 32),
	}, backend, blobs, clock)
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := sessioningress.New(sessioningress.Config{IDKey: bytes.Repeat([]byte("i"), 32)}, backend, blobs)
	if err != nil {
		t.Fatal(err)
	}
	service, err := webapi.New(webapi.Config{
		IDKey: bytes.Repeat([]byte("w"), 32), MaxUploadBytes: 1024,
	}, sessions, ingress, backend, backend, objects, clock)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{service: service, backend: backend, objects: objects, blobs: blobs}
}

func uploadRequest(idempotencyKey, name, mediaType string, payload []byte) webcontract.CreateUploadIntentRequest {
	digest := sha256.Sum256(payload)
	contentMD5 := md5.Sum(payload)
	return webcontract.CreateUploadIntentRequest{
		SessionID: "session-a", IdempotencyKey: domain.IdempotencyKey(idempotencyKey),
		Name: name, MediaType: mediaType, Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
		ContentMD5: base64.StdEncoding.EncodeToString(contentMD5[:]),
	}
}

func prepareCommittedUpload(t *testing.T, harness *harness, payload []byte, name, mediaType string) domain.UploadIntentID {
	t.Helper()
	response, _, err := harness.service.CreateUpload(
		context.Background(), "tenant-a", "user-a", uploadRequest("upload-request-1", name, mediaType, payload),
	)
	if err != nil {
		t.Fatal(err)
	}
	intent := harness.backend.intents[response.UploadID]
	harness.objects.put(intent.ObjectKey, mediaType, payload, "etag-original")
	if _, err := harness.service.CommitUpload(context.Background(), "tenant-a", "user-a", response.UploadID); err != nil {
		t.Fatal(err)
	}
	return response.UploadID
}

func computeConnection(id domain.SubscriptionConnectionID) ports.ComputeConnectionState {
	return ports.ComputeConnectionState{
		ID: id, Provider: "codex", Entitlement: domain.EntitlementActive,
		Quota: domain.ProviderQuotaAvailable, ObservedAt: testNow,
	}
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return testNow }

type fakeBackend struct {
	sessions      map[domain.SessionID]domain.Session
	owners        map[domain.SessionID]domain.UserID
	bindings      map[domain.FrontendBindingID]domain.FrontendBinding
	intents       map[domain.UploadIntentID]domain.UploadIntent
	uploadKeys    map[domain.IdempotencyKey]domain.UploadIntentID
	results       map[string]ports.CanonicalUserEventResult
	resultDigests map[string]string
	events        []domain.SessionEvent
	runs          map[domain.RunID]ports.RunRecord
	manifests     map[domain.ArtifactManifestID]domain.ArtifactManifest
	commits       []ports.CanonicalUserEventCommit
	lastCommit    ports.WebUploadCommitRequest
	connections   []ports.ComputeConnectionState
	resolveCalls  int
}

func newFakeBackend() *fakeBackend {
	session := domain.Session{
		ID: "session-a", TenantID: "tenant-a", CreatedBy: "user-a", Status: domain.SessionActive,
		CreatedAt: testNow, UpdatedAt: testNow,
	}
	return &fakeBackend{
		sessions:      map[domain.SessionID]domain.Session{session.ID: session},
		owners:        map[domain.SessionID]domain.UserID{session.ID: "user-a"},
		bindings:      make(map[domain.FrontendBindingID]domain.FrontendBinding),
		intents:       make(map[domain.UploadIntentID]domain.UploadIntent),
		uploadKeys:    make(map[domain.IdempotencyKey]domain.UploadIntentID),
		results:       make(map[string]ports.CanonicalUserEventResult),
		resultDigests: make(map[string]string),
		runs:          make(map[domain.RunID]ports.RunRecord),
		manifests:     make(map[domain.ArtifactManifestID]domain.ArtifactManifest),
		connections:   []ports.ComputeConnectionState{computeConnection("connection-a")},
	}
}

func (store *fakeBackend) authorized(tenant domain.TenantID, user domain.UserID, session domain.SessionID) bool {
	value, found := store.sessions[session]
	return found && value.TenantID == tenant && store.owners[session] == user
}

func (store *fakeBackend) CreateWebUploadIntent(_ context.Context, request ports.WebUploadCreateRequest) (domain.UploadIntent, bool, error) {
	if id, found := store.uploadKeys[request.IdempotencyKey]; found {
		return store.intents[id], false, nil
	}
	if !store.authorized(request.Intent.TenantID, request.Intent.UserID, request.Intent.SessionID) {
		return domain.UploadIntent{}, false, domain.ErrMembershipDenied
	}
	store.intents[request.Intent.ID] = request.Intent
	store.uploadKeys[request.IdempotencyKey] = request.Intent.ID
	return request.Intent, true, nil
}

func (store *fakeBackend) CommitWebUploadIntent(_ context.Context, request ports.WebUploadCommitRequest) (domain.UploadIntent, error) {
	store.lastCommit = request
	intent, found := store.intents[request.UploadID]
	if !found || intent.TenantID != request.TenantID || intent.UserID != request.UserID ||
		!store.authorized(request.TenantID, request.UserID, intent.SessionID) {
		return domain.UploadIntent{}, domain.ErrMembershipDenied
	}
	if err := intent.RecordObservedMetadata(request.Observed.Blob, request.Observed.MediaType, request.Observed.ETag, request.At); err != nil {
		return domain.UploadIntent{}, err
	}
	store.intents[intent.ID] = intent
	return intent, nil
}

func (store *fakeBackend) ClaimWebUploadIntents(_ context.Context, request ports.WebUploadClaimRequest) ([]domain.UploadIntent, error) {
	claimed := make([]domain.UploadIntent, 0, len(request.UploadIDs))
	for _, id := range request.UploadIDs {
		intent, found := store.intents[id]
		if !found || intent.TenantID != request.TenantID || intent.UserID != request.UserID || intent.SessionID != request.SessionID ||
			!store.authorized(request.TenantID, request.UserID, request.SessionID) {
			return nil, domain.ErrMembershipDenied
		}
		if err := intent.Claim(request.MessageIdempotencyKey, request.At); err != nil {
			return nil, err
		}
		store.intents[id] = intent
		claimed = append(claimed, intent)
	}
	return claimed, nil
}

func (store *fakeBackend) ResolveComputeConnectionsForUser(_ context.Context, request ports.ComputeConnectionResolveRequest) ([]ports.ComputeConnectionState, error) {
	store.resolveCalls++
	if !store.authorized(request.TenantID, request.UserID, request.SessionID) {
		return nil, domain.ErrMembershipDenied
	}
	return append([]ports.ComputeConnectionState(nil), store.connections...), nil
}

func (store *fakeBackend) GetRunForUser(_ context.Context, tenant domain.TenantID, user domain.UserID, runID domain.RunID) (ports.RunRecord, bool, error) {
	record, found := store.runs[runID]
	if !found {
		return ports.RunRecord{}, false, nil
	}
	if !store.authorized(tenant, user, record.Run.SessionID) {
		return ports.RunRecord{}, false, domain.ErrMembershipDenied
	}
	return record, true, nil
}

func (store *fakeBackend) GetRunArtifactForUser(_ context.Context, request ports.WebRunArtifactRequest) (ports.WebRunArtifact, bool, error) {
	run, found := store.runs[request.RunID]
	if !found || run.Run.TenantID != request.TenantID || run.Run.SessionID != request.SessionID {
		return ports.WebRunArtifact{}, false, nil
	}
	if !store.authorized(request.TenantID, request.UserID, request.SessionID) {
		return ports.WebRunArtifact{}, false, domain.ErrMembershipDenied
	}
	manifest, found := store.manifests[request.ManifestID]
	if !found || manifest.TenantID != request.TenantID || manifest.RunID != request.RunID ||
		int(request.Index) >= len(manifest.Artifacts) {
		return ports.WebRunArtifact{}, false, nil
	}
	artifact := manifest.Artifacts[request.Index]
	return ports.WebRunArtifact{Name: artifact.Name, MediaType: artifact.MediaType, Blob: artifact.Blob}, true, nil
}

func (store *fakeBackend) BindOrSwitchFrontendForUser(_ context.Context, request ports.FrontendBindingRequest) (domain.FrontendBinding, error) {
	if !store.authorized(request.TenantID, request.UserID, request.SessionID) {
		return domain.FrontendBinding{}, domain.ErrMembershipDenied
	}
	binding, found := store.bindings[request.BindingID]
	if !found {
		binding = domain.FrontendBinding{
			ID: request.BindingID, TenantID: request.TenantID, Frontend: request.Frontend,
			ExternalConversationID: request.ExternalConversationID, SessionID: request.SessionID,
			Revision: 1, CreatedAt: request.At, UpdatedAt: request.At,
		}
	}
	store.bindings[binding.ID] = binding
	return binding, nil
}

func (store *fakeBackend) LookupCanonicalUserEvent(_ context.Context, request ports.CanonicalUserEventLookup) (ports.CanonicalUserEventLookupResult, error) {
	key := string(request.BindingID) + "/" + string(request.IdempotencyKey)
	result, found := store.results[key]
	if found && request.MutationDigest != "" && store.resultDigests[key] != request.MutationDigest {
		return ports.CanonicalUserEventLookupResult{}, domain.ErrEventIdempotencyConflict
	}
	if found {
		result.Created = false
	}
	return ports.CanonicalUserEventLookupResult{Result: result, Found: found}, nil
}

func (store *fakeBackend) CommitCanonicalUserEvent(_ context.Context, request ports.CanonicalUserEventCommit) (ports.CanonicalUserEventResult, error) {
	key := string(request.BindingID) + "/" + string(request.IdempotencyKey)
	if result, found := store.results[key]; found {
		if request.MutationDigest != "" && store.resultDigests[key] != request.MutationDigest {
			return ports.CanonicalUserEventResult{}, domain.ErrEventIdempotencyConflict
		}
		result.Created = false
		return result, nil
	}
	binding, found := store.bindings[request.BindingID]
	if !found || !store.authorized(request.TenantID, request.UserID, binding.SessionID) {
		return ports.CanonicalUserEventResult{}, domain.ErrMembershipDenied
	}
	sequence := uint64(len(store.events) + 1)
	result := ports.CanonicalUserEventResult{
		SessionID: binding.SessionID, EventID: request.EventID, Sequence: sequence, RunID: request.RunID, Created: true,
	}
	userID, runID := request.UserID, request.RunID
	store.events = append(store.events, domain.SessionEvent{
		ID: request.EventID, TenantID: request.TenantID, SessionID: binding.SessionID,
		Sequence: sequence, Kind: domain.SessionEventUserMessage, AuthorUserID: &userID, RunID: &runID,
		IdempotencyKey: request.IdempotencyKey, Payload: request.Payload, CreatedAt: request.CommittedAt,
	})
	store.runs[request.RunID] = ports.RunRecord{Run: domain.Run{
		ID: request.RunID, TenantID: request.TenantID, SessionID: binding.SessionID,
		TriggerEventID: request.EventID, SubscriptionConnectionID: request.SubscriptionConnectionID,
		Status: domain.RunCreated, IdempotencyKey: request.IdempotencyKey,
		CreatedAt: request.CommittedAt, UpdatedAt: request.CommittedAt,
	}, Provider: "codex"}
	store.commits = append(store.commits, request)
	store.results[key] = result
	store.resultDigests[key] = request.MutationDigest
	return result, nil
}

func (store *fakeBackend) ListSessionHistoryForUser(_ context.Context, tenant domain.TenantID, user domain.UserID, session domain.SessionID, after, limit uint64) ([]domain.SessionEvent, error) {
	if !store.authorized(tenant, user, session) {
		return nil, domain.ErrMembershipDenied
	}
	var result []domain.SessionEvent
	for _, event := range store.events {
		if event.TenantID == tenant && event.SessionID == session && event.Sequence > after {
			result = append(result, event)
		}
	}
	if uint64(len(result)) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (store *fakeBackend) CreateSessionForUser(context.Context, ports.SessionCreateRequest) (domain.Session, bool, error) {
	return domain.Session{}, false, errors.New("not implemented")
}
func (store *fakeBackend) GetSessionForUser(context.Context, domain.TenantID, domain.UserID, domain.SessionID, bool) (ports.SessionRecord, bool, error) {
	return ports.SessionRecord{}, false, errors.New("not implemented")
}
func (store *fakeBackend) ListSessionsForUser(context.Context, ports.SessionListRequest) ([]ports.SessionRecord, error) {
	return nil, errors.New("not implemented")
}
func (store *fakeBackend) ListRunsForUser(context.Context, ports.RunListRequest) ([]ports.RunRecord, error) {
	return nil, errors.New("not implemented")
}
func (store *fakeBackend) SetSessionArchivedForUser(context.Context, domain.TenantID, domain.UserID, domain.SessionID, bool, domain.IdempotencyKey, time.Time) (domain.Session, error) {
	return domain.Session{}, errors.New("not implemented")
}
func (store *fakeBackend) EnsureFrontendSession(context.Context, ports.FrontendSessionRequest) (ports.FrontendSessionState, error) {
	return ports.FrontendSessionState{}, errors.New("not implemented")
}
func (store *fakeBackend) CreateAndSwitchFrontendSession(context.Context, ports.CanonicalSessionSwitchRequest) (ports.FrontendSessionState, error) {
	return ports.FrontendSessionState{}, errors.New("not implemented")
}

type fakeObjects struct {
	metadata   map[string]ports.ObjectMetadata
	uploads    []ports.UploadCapabilityRequest
	promotions []ports.PromoteObjectRequest
	downloads  []domain.BlobRef
}

func (store *fakeObjects) put(key, mediaType string, payload []byte, etag string) {
	digest := sha256.Sum256(payload)
	store.metadata[key] = ports.ObjectMetadata{
		Blob:      domain.BlobRef{TenantID: "tenant-a", Key: key, Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:])},
		MediaType: mediaType, ETag: etag,
	}
}

func (store *fakeObjects) PresignUpload(_ context.Context, request ports.UploadCapabilityRequest) (ports.ObjectCapability, error) {
	store.uploads = append(store.uploads, request)
	return ports.ObjectCapability{
		Method: "PUT", URL: "https://objects.invalid/" + request.ObjectKey,
		Headers: map[string]string{"x-checksum": request.SHA256}, ExpiresAt: testNow.Add(request.ExpiresIn),
	}, nil
}

func (store *fakeObjects) StatObject(_ context.Context, tenant domain.TenantID, key string) (ports.ObjectMetadata, error) {
	metadata, found := store.metadata[key]
	if !found || metadata.Blob.TenantID != tenant {
		return ports.ObjectMetadata{}, webapi.ErrResourceUnavailable
	}
	return metadata, nil
}

func (store *fakeObjects) PromoteObject(_ context.Context, request ports.PromoteObjectRequest) (domain.BlobRef, error) {
	store.promotions = append(store.promotions, request)
	source, found := store.metadata[request.Source.Key]
	if !found || source.Blob != request.Source || source.ETag != request.SourceETag {
		return domain.BlobRef{}, domain.ErrUploadMismatch
	}
	if existing, found := store.metadata[request.FinalKey]; found {
		if existing.Blob.Size == source.Blob.Size && existing.Blob.SHA256 == source.Blob.SHA256 && existing.MediaType == request.MediaType {
			return existing.Blob, nil
		}
		return domain.BlobRef{}, domain.ErrUploadMismatch
	}
	final := source
	final.Blob.Key = request.FinalKey
	store.metadata[request.FinalKey] = final
	return final.Blob, nil
}

func (store *fakeObjects) PresignDownload(_ context.Context, tenant domain.TenantID, ref domain.BlobRef, expires time.Duration) (ports.ObjectCapability, error) {
	metadata, found := store.metadata[ref.Key]
	if !found || tenant != ref.TenantID || metadata.Blob != ref {
		return ports.ObjectCapability{}, webapi.ErrResourceUnavailable
	}
	store.downloads = append(store.downloads, ref)
	return ports.ObjectCapability{Method: "GET", URL: "https://objects.invalid/" + ref.Key, ExpiresAt: testNow.Add(expires)}, nil
}

type fakeBlobs struct{ values map[string][]byte }

func (store *fakeBlobs) Put(_ context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	payload, err := io.ReadAll(body)
	if err != nil {
		return domain.BlobRef{}, err
	}
	digest := sha256.Sum256(payload)
	store.values[key] = bytes.Clone(payload)
	return domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:])}, nil
}

func (store *fakeBlobs) Open(_ context.Context, _ domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	payload, found := store.values[ref.Key]
	if !found {
		return nil, webapi.ErrResourceUnavailable
	}
	return io.NopCloser(bytes.NewReader(payload)), nil
}

func (store *fakeBlobs) Delete(_ context.Context, _ domain.TenantID, ref domain.BlobRef) error {
	delete(store.values, ref.Key)
	return nil
}

var (
	_ ports.WebUploadStore        = (*fakeBackend)(nil)
	_ ports.WebResourceStore      = (*fakeBackend)(nil)
	_ ports.SessionAPIStore       = (*fakeBackend)(nil)
	_ ports.CanonicalIngressStore = (*fakeBackend)(nil)
	_ ports.WebObjectStore        = (*fakeObjects)(nil)
	_ ports.BlobStore             = (*fakeBlobs)(nil)
)
