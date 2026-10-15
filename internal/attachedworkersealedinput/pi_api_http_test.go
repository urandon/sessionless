package attachedworkersealedinput

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Synthetic Source collaborators qualify HTTP/bearer wiring, NEVER a current
// database decision, genuine R/M permission, executable profile or native fit.
type piHTTPSource struct {
	ports.AttachedWorkerTransportStore
	mu      sync.Mutex
	calls   int
	ctx     context.Context
	auth    ports.AttachedWorkerSealedInputAuthorization
	binding domain.HarnessBindingDigestV2
	policy  domain.AttachedPiAPIInvocationPolicyMappingDigestV1
	left    uint64
	phase   string
	result  ports.AttachedWorkerPiAPIUseResultV1
	err     error
}

func (s *piHTTPSource) AuthorizeAttachedWorkerPiAPIUse(ctx context.Context, auth ports.AttachedWorkerSealedInputAuthorization,
	binding domain.HarnessBindingDigestV2, policy domain.AttachedPiAPIInvocationPolicyMappingDigestV1,
	left uint64, phase string,
) (ports.AttachedWorkerPiAPIUseResultV1, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.ctx, s.auth, s.binding, s.policy, s.left, s.phase = ctx, auth, binding, policy, left, phase
	return s.result, s.err
}

type piHTTPIDs struct{}

func (piHTTPIDs) NewID(context.Context, ports.IDKind) (string, error) {
	return "", errors.New("read-only phase unexpectedly requested a new ID")
}

func piHTTPService(t *testing.T, store ports.AttachedWorkerTransportStore) *attachedworkertransport.Service {
	t.Helper()
	service, err := attachedworkertransport.NewService(attachedworkertransport.ServiceConfig{
		IDs: piHTTPIDs{}, Audience: "sessionless:attached-worker:v1",
		PlatformOffer:       attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}},
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1},
		ChallengeLifetime:   5 * time.Minute, ChallengeRetention: time.Hour,
		PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: attachedworkertransport.MinimumHeartbeatInterval,
	}, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func piHTTPResult(reply pisdkphasewire.Reply) ports.AttachedWorkerPiAPIUseResultV1 {
	return ports.AttachedWorkerPiAPIUseResultV1{
		Status: reply.Status, AcceptedAttempt: reply.AcceptedAttempt, AcceptedAttemptRevision: reply.AcceptedAttemptRevision,
		HarnessBindingDigest:                domain.HarnessBindingDigestV2(reply.HarnessBindingDigest),
		AttachedPiAPIInvocationPolicyDigest: domain.AttachedPiAPIInvocationPolicyMappingDigestV1(reply.AttachedPiAPIInvocationPolicyDigest),
		CheckedAt:                           reply.CheckedAt, UseExpiresAt: reply.UseExpiresAt,
	}
}

func piHTTPRequest(t *testing.T, request pisdkphasewire.Request, bearer []byte) *http.Request {
	t.Helper()
	body, err := pisdkphasewire.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, PiAPIUsePathV1, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+string(bearer))
	return r
}

func TestPiAPIUseHandlerOriginalBearerAllPhases(t *testing.T) {
	for _, phase := range []pisdkphasewire.Phase{pisdkphasewire.PreIssue, pisdkphasewire.PhysicalSend, pisdkphasewire.ImmutableFinalize} {
		t.Run(string(phase), func(t *testing.T) {
			_, bearer, request, reply := piClientFixture(t)
			request.Phase = phase
			source := &piHTTPSource{result: piHTTPResult(reply)}
			r := piHTTPRequest(t, request, bearer).WithContext(t.Context())
			w := httptest.NewRecorder()
			PiAPIUseHandler(piHTTPService(t, source)).ServeHTTP(w, r)
			if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Content-Type") != "application/json" ||
				!bytes.Equal(w.Body.Bytes(), piReplyBytes(t, reply)) {
				t.Fatalf("phase %s status=%d headers=%v, want exact closed reply", phase, w.Code, w.Header())
			}
			secret, err := attachedworkertransport.ParseConnectionSecret(bytes.Repeat([]byte{7}, 32))
			if err != nil {
				t.Fatal(err)
			}
			wantAuth := request.Authorization
			wantAuth.PresentedSecretDigest = secret.Digest()
			if source.calls != 1 || source.auth != wantAuth || source.ctx != r.Context() || source.phase != string(phase) ||
				string(source.binding) != request.ExpectedHarnessBindingDigest || string(source.policy) != request.ExpectedAttachedPiAPIInvocationPolicyDigest ||
				source.left != request.CallerDeadline.RemainingNanoseconds {
				t.Fatal("original scope, bearer-derived digest, phase, deadline or context changed at source boundary")
			}
		})
	}
}

func TestPiAPIUseHandlerRealClientTLS(t *testing.T) {
	binding, bearer, request, reply := piClientFixture(t)
	source := &piHTTPSource{result: piHTTPResult(reply)}
	server, httpClient := piLoopback(t, PiAPIUseHandler(piHTTPService(t, source)))
	client := piClient(t, server.URL+PiAPIUsePathV1, httpClient, binding, bearer)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	for _, phase := range []pisdkphasewire.Phase{pisdkphasewire.PreIssue, pisdkphasewire.PhysicalSend, pisdkphasewire.ImmutableFinalize} {
		request.Phase = phase
		got, err := client.RequestPiAPIUse(ctx, request)
		if err != nil || got != reply {
			t.Fatalf("phase %s got err=%v, want exact reply through original client/real bearer service", phase, err)
		}
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.calls != 3 {
		t.Fatalf("source calls=%d, want one per original phase", source.calls)
	}
}

func TestPiAPIUseHandlerRejectsBeforeSource(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*http.Request)
		status int
	}{
		{"method", func(r *http.Request) { r.Method = http.MethodGet }, 400},
		{"path", func(r *http.Request) { r.URL.Path += "/" }, 400},
		{"raw path", func(r *http.Request) { r.URL.RawPath = "/attached-worker/v1/pi-api-%75se" }, 400},
		{"query", func(r *http.Request) { r.URL.RawQuery = "phase=physical_send" }, 400},
		{"empty query", func(r *http.Request) { r.URL.ForceQuery = true }, 400},
		{"duplicate media", func(r *http.Request) { r.Header.Add("Content-Type", "application/json") }, 400},
		{"wrong media", func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, 400},
		{"duplicate auth", func(r *http.Request) { r.Header.Add("Authorization", "Bearer private") }, 400},
		{"missing auth", func(r *http.Request) { r.Header.Del("Authorization") }, 400},
		{"wrong auth", func(r *http.Request) { r.Header.Set("Authorization", "Basic private") }, 401},
		{"oversized bearer", func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+strings.Repeat("x", attachedworkertransport.ConnectionBearerMaxBytes+1))
		}, 401},
		{"invalid bearer", func(r *http.Request) { r.Header.Set("Authorization", "Bearer private") }, 401},
		{"encoding", func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, 400},
		{"transfer encoding", func(r *http.Request) { r.TransferEncoding = []string{"chunked"} }, 400},
		{"empty length", func(r *http.Request) { r.ContentLength = 0 }, 400},
		{"oversized length", func(r *http.Request) { r.ContentLength = pisdkphasewire.RequestMaxBytes + 1 }, 400},
		{"wrong length", func(r *http.Request) { r.ContentLength-- }, 400},
		{"nil body", func(r *http.Request) { r.Body = nil }, 400},
		{"trailing body", func(r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(append(b, '\n')))
			r.ContentLength = int64(len(b) + 1)
		}, 400},
		{"duplicate JSON", func(r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader(`{"version":1,"version":1}`))
			r.ContentLength = -1
		}, 400},
		{"unknown phase", func(r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			b = bytes.Replace(b, []byte("physical_send"), []byte("new_phase"), 1)
			r.Body = io.NopCloser(bytes.NewReader(b))
			r.ContentLength = int64(len(b))
		}, 400},
		{"body overflow unknown length", func(r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", pisdkphasewire.RequestMaxBytes+1)))
			r.ContentLength = -1
		}, 400},
		{"canceled", func(r *http.Request) {
			ctx, cancel := context.WithCancel(r.Context())
			cancel()
			*r = *r.WithContext(ctx)
		}, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, bearer, request, reply := piClientFixture(t)
			source := &piHTTPSource{result: piHTTPResult(reply)}
			r := piHTTPRequest(t, request, bearer)
			tc.change(r)
			w := httptest.NewRecorder()
			PiAPIUseHandler(piHTTPService(t, source)).ServeHTTP(w, r)
			if w.Code != tc.status || source.calls != 0 || w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), "private") {
				t.Fatalf("case %s status=%d sourcecalls=%d, want %d/0 with sanitized no-store body", tc.name, w.Code, source.calls, tc.status)
			}
		})
	}
}

func TestPiAPIUseHandlerSourceDenialAndMismatch(t *testing.T) {
	for _, name := range []string{"denied", "backend", "revision", "binding", "policy", "window", "foreign owner"} {
		t.Run(name, func(t *testing.T) {
			_, bearer, request, reply := piClientFixture(t)
			source := &piHTTPSource{result: piHTTPResult(reply)}
			wantStatus := http.StatusUnauthorized
			switch name {
			case "denied":
				source.result = ports.AttachedWorkerPiAPIUseResultV1{Status: ports.AttachedWorkerExecutionDenied}
				wantStatus = http.StatusOK
			case "backend":
				source.err = errors.New("private backend detail")
				wantStatus = http.StatusServiceUnavailable
			case "revision":
				source.result.AcceptedAttemptRevision++
			case "binding":
				source.result.HarnessBindingDigest = domain.HarnessBindingDigestV2(strings.Repeat("1", 64))
			case "policy":
				source.result.AttachedPiAPIInvocationPolicyDigest = domain.AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("1", 64))
			case "window":
				source.result.UseExpiresAt = source.result.CheckedAt.Add(2 * time.Minute)
			case "foreign owner":
				request.Authorization.OwnerUserID = "owner-2"
				a := &request.Authorization
				var err error
				a.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
				if err != nil {
					t.Fatal(err)
				}
			}
			w := httptest.NewRecorder()
			PiAPIUseHandler(piHTTPService(t, source)).ServeHTTP(w, piHTTPRequest(t, request, bearer))
			wantCalls := 1
			if name == "foreign owner" {
				wantCalls = 0
			}
			if w.Code != wantStatus || source.calls != wantCalls || strings.Contains(w.Body.String(), "private") {
				t.Fatalf("case %s status=%d calls=%d, want %d/%d", name, w.Code, source.calls, wantStatus, wantCalls)
			}
			if name == "denied" && w.Body.String() != `{"status":"denied"}` {
				t.Fatal("denial leaked successful head or authority metadata")
			}
		})
	}
}

func TestPiAPIUseHandlerCanonicalSyntaxCeiling(t *testing.T) {
	_, _, request, _ := piClientFixture(t)
	a := &request.Authorization
	a.TenantID, a.OwnerUserID = domain.TenantID(strings.Repeat("t", 160)), domain.UserID(strings.Repeat("o", 160))
	a.WorkerID, a.ConnectionID = domain.AttachedWorkerID(strings.Repeat("w", 160)), domain.AttachedWorkerConnectionID(strings.Repeat("c", 160))
	a.RunID, a.AttemptID, a.LeaseID = domain.RunID(strings.Repeat("r", 128)), domain.AttemptID(strings.Repeat("a", 128)), domain.LeaseID(strings.Repeat("l", 128))
	a.EnrollmentGeneration, a.ConnectionGeneration, a.LeaseGeneration, a.ExpectedAttemptRevision = math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64
	a.LeaseExpiresAtUnixMicro = math.MaxInt64
	var err error
	a.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	request.Phase = pisdkphasewire.ImmutableFinalize
	request.CallerDeadline.RemainingNanoseconds = math.MaxInt64
	secret, err := attachedworkertransport.ParseConnectionSecret(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	token, err := attachedworkertransport.NewConnectionBearer(a.TenantID, a.OwnerUserID, a.WorkerID, a.ConnectionID, secret)
	if err != nil {
		t.Fatal(err)
	}
	for _, knownLength := range []bool{true, false} {
		t.Run(map[bool]string{true: "known length", false: "unknown length"}[knownLength], func(t *testing.T) {
			source := &piHTTPSource{result: ports.AttachedWorkerPiAPIUseResultV1{Status: ports.AttachedWorkerExecutionDenied}}
			r := piHTTPRequest(t, request, token.Bytes())
			if r.ContentLength != pisdkphasewire.RequestMaxBytes {
				t.Fatalf("canonical fixture len=%d, want exact %d", r.ContentLength, pisdkphasewire.RequestMaxBytes)
			}
			if !knownLength {
				r.ContentLength = -1
			}
			w := httptest.NewRecorder()
			PiAPIUseHandler(piHTTPService(t, source)).ServeHTTP(w, r)
			if w.Code != http.StatusOK || source.calls != 1 || w.Body.String() != `{"status":"denied"}` {
				t.Fatalf("full syntax ceiling status=%d calls=%d, want200/1 exact denial", w.Code, source.calls)
			}
		})
	}
}

type piHTTPMissingStore struct {
	ports.AttachedWorkerTransportStore
}
type piHTTPBody struct {
	*bytes.Reader
	err    error
	closed bool
	reads  int
}

func (b *piHTTPBody) Read(p []byte) (int, error) {
	b.reads++
	if b.err != nil {
		return 0, b.err
	}
	return b.Reader.Read(p)
}
func (b *piHTTPBody) Close() error { b.closed = true; return nil }

func TestPiAPIUseHandlerMissingSourceAndBodyFailure(t *testing.T) {
	for _, mode := range []string{"missing Source", "nil service", "read failure", "invalid header"} {
		t.Run(mode, func(t *testing.T) {
			_, bearer, request, reply := piClientFixture(t)
			r := piHTTPRequest(t, request, bearer)
			encoded, err := pisdkphasewire.EncodeRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			body := &piHTTPBody{Reader: bytes.NewReader(encoded)}
			r.Body = body
			source := &piHTTPSource{result: piHTTPResult(reply)}
			service := piHTTPService(t, source)
			status := http.StatusBadRequest
			switch mode {
			case "missing Source":
				service = piHTTPService(t, &piHTTPMissingStore{})
				status = http.StatusServiceUnavailable
			case "nil service":
				service = nil
			case "read failure":
				body.err = errors.New("private body error")
			case "invalid header":
				r.Header.Set("Content-Type", "private")
			}
			w := httptest.NewRecorder()
			PiAPIUseHandler(service).ServeHTTP(w, r)
			if w.Code != status || !body.closed || source.calls != 0 || strings.Contains(w.Body.String(), "private") {
				t.Fatalf("mode %s status=%d closed=%t calls=%d, want %d/true/0 sanitized", mode, w.Code, body.closed, source.calls, status)
			}
			if (mode == "nil service" || mode == "invalid header") && body.reads != 0 {
				t.Fatal("invalid preflight consumed body")
			}
		})
	}
}
