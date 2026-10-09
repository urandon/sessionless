package attachedworkersealedinput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

type authorizerFixture struct {
	calls          int
	requests       []ports.AttachedWorkerSealedInputAuthorization
	denyAt         int
	revision       uint64
	secondRevision uint64
	bearer         []byte
}

func (fixture *authorizerFixture) AuthorizeSealedInputBearer(_ context.Context, bearer []byte, request ports.AttachedWorkerSealedInputAuthorization) (uint64, error) {
	fixture.calls++
	fixture.requests = append(fixture.requests, request)
	wantBearer := fixture.bearer
	if wantBearer == nil {
		wantBearer = []byte("test-connection-bearer")
	}
	if !bytes.Equal(bearer, wantBearer) || fixture.calls == fixture.denyAt {
		return 0, ErrUnauthorized
	}
	if fixture.calls == 2 && fixture.secondRevision != 0 {
		return fixture.secondRevision, nil
	}
	return fixture.revision, nil
}

type jobFixture struct {
	state ports.WorkerJobState
	calls int
}

func (fixture *jobFixture) LoadWorkerJob(_ context.Context, tenant domain.TenantID, run domain.RunID) (ports.WorkerJobState, bool, error) {
	fixture.calls++
	if tenant != fixture.state.Job.TenantID || run != fixture.state.Job.RunID {
		return ports.WorkerJobState{}, false, nil
	}
	return fixture.state, true, nil
}

type blobFixture struct {
	contents map[string][]byte
	calls    int
}

func (fixture *blobFixture) Open(_ context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	fixture.calls++
	if tenant != ref.TenantID {
		return nil, errors.New("cross tenant")
	}
	body, ok := fixture.contents[ref.Key]
	if !ok {
		return nil, errors.New("missing")
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func fixtureInput(t *testing.T) (*Service, *authorizerFixture, *jobFixture, *blobFixture, attachedworkerdaemontransport.MaterializationRequestV1) {
	t.Helper()
	contextBytes := []byte("synthetic canonical context")
	artifactBytes := []byte("synthetic artifact")
	placement := domain.ExecutionPlacementV2{
		Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: "owner-1", WorkerID: "worker-1",
		CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("synthetic capability")),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("synthetic policy"))),
	}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(
		"tenant-1", "owner-1", "run-1", "attempt-1", "subscription-1", time.Unix(1, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	binding := authority.HarnessBinding.Clone()
	binding.ExecutionPlacementDigest = string(placementDigest)
	job := domain.WorkerJob{
		TenantID: "tenant-1", RunID: "run-1", SessionID: "session-1", TriggerEventID: "event-1",
		AttemptID: "attempt-1", ReservationID: "reservation-1", InputManifestID: "manifest-1",
		ContextSnapshot:       testBlob("tenant-1", "context", contextBytes),
		CredentialOwnerUserID: "owner-1", ExecutionPlacementV2: placement, HarnessBinding: binding,
		Limits: domain.ProductLimits{
			MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 10,
			MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 100,
			MaxArtifacts: 10, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18,
		},
	}
	manifest := domain.ArtifactManifest{ID: job.InputManifestID, TenantID: job.TenantID, RunID: job.RunID,
		CreatedAt: time.Unix(10, 0).UTC(),
		Artifacts: []domain.Artifact{{Name: "alpha", MediaType: "text/plain", Blob: testBlob("tenant-1", "alpha", artifactBytes)}},
	}
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(20, 0).UTC()
	request := attachedworkerdaemontransport.MaterializationRequestV1{
		TenantID: job.TenantID, OwnerUserID: placement.OwnerUserID, WorkerID: placement.WorkerID,
		EnrollmentGeneration: 2, ConnectionGeneration: 3, ConnectionID: "connection-1", AttemptSequence: 1,
		Attempt: attachedworkerprotocol.AttemptBindingV1{
			RunID: string(job.RunID), AttemptID: string(job.AttemptID), LeaseID: "lease-1",
			LeaseGeneration: 4, FenceToken: "fence-1", ExpiresAtUnixMicro: now.Add(time.Minute).UnixMicro(),
			ContextDigest:    decodeDigest(t, string(contextDigest)),
			CapabilityDigest: decodeDigest(t, string(placement.CapabilityDigest)),
			PolicyDigest:     decodeDigest(t, string(placement.PolicyDigest)),
		},
	}
	authorizer := &authorizerFixture{revision: 7}
	jobs := &jobFixture{state: ports.WorkerJobState{Job: job, InputManifest: manifest}}
	blobs := &blobFixture{contents: map[string][]byte{job.ContextSnapshot.Key: contextBytes, manifest.Artifacts[0].Blob.Key: artifactBytes}}
	service, err := NewService(authorizer, jobs, blobs)
	if err != nil {
		t.Fatal(err)
	}
	return service, authorizer, jobs, blobs, request
}

func testBlob(tenant domain.TenantID, name string, body []byte) domain.BlobRef {
	digest := sha256.Sum256(body)
	return domain.BlobRef{TenantID: tenant, Key: "tenants/" + string(tenant) + "/" + name,
		Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}
}

func decodeDigest(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestLoadAuthorizesBeforeAndAfterTenantBlobs(t *testing.T) {
	service, authorizer, jobs, blobs, request := fixtureInput(t)
	input, err := service.Load(context.Background(), []byte("test-connection-bearer"), request)
	if err != nil {
		t.Fatal(err)
	}
	if string(input.Context) != "synthetic canonical context" || len(input.Artifacts) != 1 ||
		string(input.Artifacts[0].Body) != "synthetic artifact" || jobs.calls != 1 || blobs.calls != 2 || authorizer.calls != 2 {
		t.Fatalf("wrong sealed input or reads: jobs=%d blobs=%d auth=%d", jobs.calls, blobs.calls, authorizer.calls)
	}
	if authorizer.requests[0].ExpectedAttemptRevision != 0 || authorizer.requests[1].ExpectedAttemptRevision != 7 ||
		authorizer.requests[1].ContextDigest != domain.AttachedWorkerContextDigest(hex.EncodeToString(request.Attempt.ContextDigest)) {
		t.Fatal("post-read authorization did not bind exact revision and digest")
	}
}

func TestLoadFailsClosedBeforeReadAndAfterRevisionLoss(t *testing.T) {
	for _, test := range []struct {
		name                string
		denyAt              int
		wantJobs, wantBlobs int
	}{
		{name: "pre-read", denyAt: 1, wantJobs: 0, wantBlobs: 0},
		{name: "post-read", denyAt: 2, wantJobs: 1, wantBlobs: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, authorizer, jobs, blobs, request := fixtureInput(t)
			authorizer.denyAt = test.denyAt
			input, err := service.Load(context.Background(), []byte("test-connection-bearer"), request)
			if !errors.Is(err, ErrUnauthorized) || len(input.Context) != 0 || len(input.Artifacts) != 0 ||
				jobs.calls != test.wantJobs || blobs.calls != test.wantBlobs {
				t.Fatalf("failure leaked bytes or read authority: err=%v jobs=%d blobs=%d", err, jobs.calls, blobs.calls)
			}
		})
	}
}

// The job store is tenant/run keyed, not owner keyed. Even if an upstream
// authorizer accidentally approves a forged owner scope, no bytes for the
// other owner's job may pass the service's independent job binding check.
func TestAW07ForeignOwnerCannotReadSameTenantJobOrArtifact(t *testing.T) {
	service, authorizer, jobs, blobs, request := fixtureInput(t)
	request.OwnerUserID = "owner-2"
	input, err := service.Load(context.Background(), []byte("test-connection-bearer"), request)
	if !errors.Is(err, ErrUnauthorized) || len(input.Context) != 0 || len(input.Artifacts) != 0 ||
		authorizer.calls != 1 || jobs.calls != 1 || blobs.calls != 0 {
		t.Fatalf("foreign owner read same-tenant job: err=%v auth=%d jobs=%d blobs=%d input=%+v",
			err, authorizer.calls, jobs.calls, blobs.calls, input)
	}
}

func TestLoadRejectsChangedRevisionAfterBlobRead(t *testing.T) {
	service, authorizer, jobs, blobs, request := fixtureInput(t)
	authorizer.secondRevision = 8
	input, err := service.Load(context.Background(), []byte("test-connection-bearer"), request)
	if !errors.Is(err, ErrUnauthorized) || len(input.Context) != 0 || len(input.Artifacts) != 0 ||
		jobs.calls != 1 || blobs.calls != 2 || authorizer.calls != 2 {
		t.Fatalf("changed head leaked content: err=%v jobs=%d blobs=%d auth=%d", err, jobs.calls, blobs.calls, authorizer.calls)
	}
}

func TestLoadRejectsBlobDriftAndUnsupportedContext(t *testing.T) {
	service, authorizer, _, blobs, request := fixtureInput(t)
	blobs.contents["tenants/tenant-1/alpha"] = []byte("corrupted artifact")
	input, err := service.Load(context.Background(), []byte("test-connection-bearer"), request)
	if !errors.Is(err, ErrInvalid) || len(input.Context) != 0 || authorizer.calls != 1 {
		t.Fatalf("digest drift did not fail before second authorization: %v", err)
	}
	service, _, jobs, blobs, request := fixtureInput(t)
	jobs.state.Job.ContextWindow = &domain.SessionContextWindow{}
	_, err = service.Load(context.Background(), []byte("test-connection-bearer"), request)
	if !errors.Is(err, ErrUnauthorized) || blobs.calls != 0 {
		t.Fatalf("unsupported changed job passed digest gate: %v", err)
	}
}

func TestLoadRejectsCrossTenantArtifactBeforeBlobRead(t *testing.T) {
	service, authorizer, jobs, blobs, request := fixtureInput(t)
	jobs.state.InputManifest.Artifacts[0].Blob.TenantID = "tenant-2"
	jobs.state.InputManifest.Artifacts[0].Blob.Key = "tenants/tenant-2/alpha"
	input, err := service.Load(context.Background(), []byte("test-connection-bearer"), request)
	if !errors.Is(err, ErrUnauthorized) || len(input.Context) != 0 || authorizer.calls != 1 || blobs.calls != 0 {
		t.Fatalf("cross-tenant artifact reached blob store: err=%v auth=%d blobs=%d", err, authorizer.calls, blobs.calls)
	}
}

func TestBlobReaderRejectsValidForeignTenantRefWithoutOpen(t *testing.T) {
	service, _, _, blobs, _ := fixtureInput(t)
	foreign := testBlob("tenant-2", "alpha", []byte("synthetic artifact"))
	if _, err := service.readExact(context.Background(), "tenant-1", foreign, maxInputBytes); !errors.Is(err, ErrInvalid) || blobs.calls != 0 {
		t.Fatalf("valid foreign ref reached blob store: err=%v calls=%d", err, blobs.calls)
	}
}

func TestHTTPSourceToBoundMaterializer(t *testing.T) {
	service, authorizer, _, blobs, request := fixtureInput(t)
	router := http.NewServeMux()
	router.Handle(PathV1, Handler(service))
	server := httptest.NewTLSServer(router)
	defer server.Close()
	source, err := NewClientSource(server.URL+PathV1, server.Client(), []byte("test-connection-bearer"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	materializer, err := attachedworkerdaemontransport.NewBoundMaterializer(source, 4096, func() time.Time { return time.Unix(20, 0).UTC() })
	if err != nil {
		t.Fatal(err)
	}
	result, err := materializer.Materialize(context.Background(), request)
	if err != nil || !bytes.Contains(result.Stdin, []byte("sessionless.attached-worker.synthetic-input.v1")) ||
		authorizer.calls != 2 || blobs.calls != 2 || len(result.ReadRoots) != 0 || result.Credential != nil {
		t.Fatalf("HTTPS materialization failed closed or gained authority: err=%v auth=%d blobs=%d", err, authorizer.calls, blobs.calls)
	}
	if _, err := NewClientSource(strings.Replace(server.URL, "https:", "http:", 1)+PathV1, server.Client(), []byte("bearer")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("plaintext source accepted: %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Load(context.Background(), request); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("closed source remained usable: %v", err)
	}
	requestHTTP, err := http.NewRequest(http.MethodGet, server.URL+PathV1, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(requestHTTP)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("GET status=%d", response.StatusCode)
	}
}

func TestSyntheticCredentialLifecycleCannotIssueOrRelease(t *testing.T) {
	lifecycle := DeniedCredentialLifecycle{}
	if _, err := lifecycle.Issue(context.Background(), ports.CredentialIssueRequest{}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("synthetic lifecycle issued credential: %v", err)
	}
	if err := lifecycle.Release(context.Background(), ports.CredentialHandle{}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("synthetic lifecycle silently released unknown credential: %v", err)
	}
}

func TestHTTPHandlerRejectsUntrustedInputBeforeAuthorization(t *testing.T) {
	service, authorizer, jobs, blobs, _ := fixtureInput(t)
	for _, test := range []struct {
		name, body, bearer string
	}{
		{name: "missing bearer", body: `{}`, bearer: ""},
		{name: "unknown field", body: `{"surprise":1}`, bearer: "test-connection-bearer"},
		{name: "oversized body", body: strings.Repeat("x", maxRequestBytes+1), bearer: "test-connection-bearer"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, PathV1, strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			if test.bearer != "" {
				request.Header.Set("Authorization", "Bearer "+test.bearer)
			}
			recorder := httptest.NewRecorder()
			Handler(service).ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest && recorder.Code != http.StatusUnauthorized {
				t.Fatalf("untrusted request status=%d", recorder.Code)
			}
		})
	}
	if authorizer.calls != 0 || jobs.calls != 0 || blobs.calls != 0 {
		t.Fatalf("untrusted input crossed authority gate: auth=%d jobs=%d blobs=%d", authorizer.calls, jobs.calls, blobs.calls)
	}
}

func TestHTTPSourceNeverForwardsBearerAcrossRedirect(t *testing.T) {
	targetCalls := 0
	target := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetCalls++
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL+PathV1, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	source, err := NewClientSource(redirect.URL+PathV1, redirect.Client(), []byte("test-connection-bearer"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Load(context.Background(), attachedworkerdaemontransport.MaterializationRequestV1{}); !errors.Is(err, ErrUnavailable) || targetCalls != 0 {
		t.Fatalf("redirect was followed: err=%v target calls=%d", err, targetCalls)
	}
}

func TestHTTPSourceAlwaysHasBoundedClientTimeout(t *testing.T) {
	for _, test := range []struct {
		name        string
		input, want time.Duration
	}{
		{name: "negative", input: -time.Second, want: time.Minute},
		{name: "zero", input: 0, want: time.Minute},
		{name: "too large", input: 2 * time.Minute, want: time.Minute},
		{name: "shorter bound", input: 5 * time.Second, want: 5 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &http.Client{Timeout: test.input}
			source, err := NewClientSource("https://example.com"+PathV1, client, []byte("test-connection-bearer"))
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			if source.client.Timeout != test.want || client.Timeout != test.input {
				t.Fatalf("timeout=%s, caller timeout=%s, want %s and unchanged caller", source.client.Timeout, client.Timeout, test.want)
			}
		})
	}
}
