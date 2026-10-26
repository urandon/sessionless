package attachedworkerhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type generationHTTPCoreFixture struct {
	calls, reads   int
	auth           ports.AttachedResourceGenerationAuthorizationV1
	publication    ports.AttachedResourceGenerationPublicationV1
	result         ports.AttachedResourceGenerationResultV1
	err            error
	borrowedBearer []byte
	deadline       time.Time
}

func (f *generationHTTPCoreFixture) PublishResourceGenerationBearer(ctx context.Context, bearer []byte, request ports.AttachedResourceGenerationPublicationV1) (ports.AttachedResourceGenerationResultV1, error) {
	f.calls++
	f.publication = request
	f.auth = request.Authorization
	f.borrowedBearer = bearer
	f.deadline, _ = ctx.Deadline()
	return f.result, f.err
}
func (f *generationHTTPCoreFixture) ReadResourceGenerationBearer(ctx context.Context, bearer []byte, auth ports.AttachedResourceGenerationAuthorizationV1) (ports.AttachedResourceGenerationResultV1, error) {
	f.reads++
	f.auth = auth
	f.borrowedBearer = bearer
	f.deadline, _ = ctx.Deadline()
	return f.result, f.err
}
func generationHTTPFixture(t *testing.T) (*ResourceGenerationHandlerV1, *generationHTTPCoreFixture, ResourceGenerationPublishRequestV1, []byte) {
	t.Helper()
	auth := ports.AttachedResourceGenerationAuthorizationV1{TenantID: "tenant", OwnerUserID: "owner", SubscriptionConnectionID: "subscription", WorkerID: "worker", EnrollmentGeneration: 2, BaselineRevision: 7, ConnectionID: "connection", ConnectionGeneration: 3}
	head := domain.AttachedResourceGenerationHeadV1{Version: 1, OwnerUserID: auth.OwnerUserID, WorkerID: auth.WorkerID, EnrollmentGeneration: auth.EnrollmentGeneration, BaselineRevision: auth.BaselineRevision, CredentialGeneration: 1}
	payload := ResourceGenerationPublishRequestV1{Version: 1, Authorization: auth, Target: head}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	core := &generationHTTPCoreFixture{result: ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionApplied, Head: head}}
	handler, err := NewResourceGenerationHandlerV1(core)
	if err != nil {
		t.Fatal(err)
	}
	return handler, core, payload, raw
}
func generationHTTPRequest(path string, raw []byte) *http.Request {
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer slw1.test-only-bounded-token")
	return request
}

func TestResourceGenerationHTTPBoundedClosedSuccessAndExplicitRead(t *testing.T) {
	t.Parallel()
	handler, core, payload, raw := generationHTTPFixture(t)
	exact := append(append([]byte(nil), raw...), bytes.Repeat([]byte(" "), domain.AttachedResourceGenerationMetadataMaxBytesV1-len(raw))...)
	recorder := httptest.NewRecorder()
	request := generationHTTPRequest(ResourceGenerationPublishPathV1, exact)
	deadline := time.Now().Add(5 * time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	request = request.WithContext(ctx)
	handler.ServeHTTP(recorder, request)
	var response ResourceGenerationResponseV1
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &response) != nil || response.Version != 1 || response.Head != payload.Target || core.calls != 1 || core.reads != 0 || core.auth != payload.Authorization || len(recorder.Body.Bytes()) > 4096 || !core.deadline.Equal(deadline) {
		t.Fatalf("closed publication response: status=%d response=%+v calls/reads=%d/%d", recorder.Code, response, core.calls, core.reads)
	}
	if !bytes.Equal(core.borrowedBearer, make([]byte, len(core.borrowedBearer))) {
		t.Fatal("temporary HTTP bearer bytes retained after original call")
	}
	if recorder.Header().Get("Cache-Control") != "no-store" || recorder.Header().Get("X-Content-Type-Options") != "nosniff" || recorder.Header().Get("Retry-After") != "" {
		t.Fatal("metadata response lacks safe cache policy or encourages mutation retry")
	}
	readBody, err := json.Marshal(ResourceGenerationReadRequestV1{Version: 1, Authorization: payload.Authorization})
	if err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, generationHTTPRequest(ResourceGenerationReadPathV1, readBody))
	if recorder.Code != http.StatusOK || core.reads != 1 || core.calls != 1 {
		t.Fatalf("explicit readback was not one independent read: status=%d calls/reads=%d/%d", recorder.Code, core.calls, core.reads)
	}
}

func TestResourceGenerationHTTPRejectsClosedShapeViolationsBeforeCore(t *testing.T) {
	t.Parallel()
	_, _, _, valid := generationHTTPFixture(t)
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"missing version", bytes.Replace(valid, []byte(`"version":1,`), nil, 1)},
		{"null version", bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":null`), 1)},
		{"wrong version", bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":2`), 1)},
		{"top case alias", bytes.Replace(valid, []byte(`"version":1`), []byte(`"Version":1`), 1)},
		{"nested case alias", bytes.Replace(valid, []byte(`"owner_user_id"`), []byte(`"Owner_User_ID"`), 1)},
		{"duplicate", bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)},
		{"nested escaped duplicate", bytes.Replace(valid, []byte(`"revoked":false`), []byte(`"revoked":false,"\u0072evoked":false`), 1)},
		{"head null scalar", bytes.Replace(valid, []byte(`"revoked":false`), []byte(`"revoked":null`), 1)},
		{"missing head scalar", bytes.Replace(valid, []byte(`,"revoked":false`), nil, 1)},
		{"null authorization", bytes.Replace(valid, valid[bytes.Index(valid, []byte(`{"tenant_id"`)):bytes.Index(valid, []byte(`,"expected_credential_generation"`))], []byte("null"), 1)},
		{"remote digest", bytes.Replace(valid, []byte(`"tenant_id":"tenant"`), []byte(`"tenant_id":"tenant","presented_secret_digest":"must-not-be-accepted"`), 1)},
		{"unknown instruction", bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"instruction":"no-authority"`), 1)},
		{"fraction", bytes.Replace(valid, []byte(`"expected_credential_generation":0`), []byte(`"expected_credential_generation":0.0`), 1)},
		{"overflow", bytes.Replace(valid, []byte(`"credential_generation":1`), []byte(`"credential_generation":18446744073709551616`), 1)},
		{"trailing", append(append([]byte(nil), valid...), []byte(`{}`)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, core, _, _ := generationHTTPFixture(t)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, generationHTTPRequest(ResourceGenerationPublishPathV1, tc.raw))
			if recorder.Code != http.StatusBadRequest || core.calls != 0 || core.reads != 0 || recorder.Body.Len() != 0 {
				t.Fatalf("invalid closed shape reached core: status=%d calls/reads=%d/%d bytes=%d", recorder.Code, core.calls, core.reads, recorder.Body.Len())
			}
		})
	}
}

type generationCountReader struct{ reads int }

func (r *generationCountReader) Read([]byte) (int, error) { r.reads++; return 0, io.EOF }
func (*generationCountReader) Close() error               { return nil }

func TestResourceGenerationHTTPBoundsFramingHeadersBeforeCopyOrBody(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*http.Request)
		want   int
	}{
		{"overlong bearer", func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+strings.Repeat("x", attachedworkertransport.ConnectionBearerMaxBytes+1))
		}, http.StatusUnauthorized},
		{"oversize headers", func(r *http.Request) {
			r.Header.Set("X-Metadata", strings.Repeat("x", resourceGenerationHeaderMaxBytesV1+1))
		}, http.StatusRequestHeaderFieldsTooLarge},
		{"duplicate auth", func(r *http.Request) { r.Header.Add("Authorization", "Bearer another") }, http.StatusUnauthorized},
		{"known oversize body", func(r *http.Request) { r.ContentLength = 4097 }, http.StatusRequestEntityTooLarge},
		{"query", func(r *http.Request) { r.URL.RawQuery = "wait_ms=0" }, http.StatusBadRequest},
		{"media parameters", func(r *http.Request) { r.Header.Set("Content-Type", "application/json; charset=utf-8") }, http.StatusUnsupportedMediaType},
		{"duplicate Accept", func(r *http.Request) { r.Header.Add("Accept", "application/json") }, http.StatusNotAcceptable},
		{"wrong method", func(r *http.Request) { r.Method = http.MethodGet }, http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, core, _, raw := generationHTTPFixture(t)
			request := generationHTTPRequest(ResourceGenerationPublishPathV1, raw)
			body := &generationCountReader{}
			request.Body = body
			tc.mutate(request)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != tc.want || body.reads != 0 || core.calls != 0 {
				t.Fatalf("prospective bound read body/called core: status=%d want=%d reads=%d calls=%d", recorder.Code, tc.want, body.reads, core.calls)
			}
		})
	}
	handler, core, _, _ := generationHTTPFixture(t)
	request := generationHTTPRequest(ResourceGenerationPublishPathV1, bytes.Repeat([]byte(" "), 4097))
	request.ContentLength = -1
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge || core.calls != 0 {
		t.Fatalf("unknown-length body escaped bound: status=%d calls=%d", recorder.Code, core.calls)
	}
}

func TestResourceGenerationHTTPNoFalseACKOrBackendContent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*generationHTTPCoreFixture)
		want   int
	}{
		{"backend", func(f *generationHTTPCoreFixture) { f.err = attachedworkertransport.ErrTransportBackend }, http.StatusServiceUnavailable},
		{"private error", func(f *generationHTTPCoreFixture) { f.err = errors.New("private-backend-marker") }, http.StatusInternalServerError},
		{"unauthorized", func(f *generationHTTPCoreFixture) { f.err = attachedworkertransport.ErrTransportUnauthorized }, http.StatusUnauthorized},
		{"conflict", func(f *generationHTTPCoreFixture) { f.err = attachedworkertransport.ErrTransportConflict }, http.StatusConflict},
		{"foreign head", func(f *generationHTTPCoreFixture) { f.result.Head.OwnerUserID = "foreign" }, http.StatusInternalServerError},
		{"another generation", func(f *generationHTTPCoreFixture) { f.result.Head.CredentialGeneration++ }, http.StatusInternalServerError},
		{"unexpected revoke", func(f *generationHTTPCoreFixture) { f.result.Head.Revoked = true }, http.StatusInternalServerError},
		{"head on denied", func(f *generationHTTPCoreFixture) { f.result.Status = ports.AttachedWorkerExecutionDenied }, http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, core, _, raw := generationHTTPFixture(t)
			tc.mutate(core)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, generationHTTPRequest(ResourceGenerationPublishPathV1, raw))
			if recorder.Code != tc.want || recorder.Body.Len() != 0 || core.calls != 1 || core.reads != 0 || recorder.Header().Get("Retry-After") != "" || recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("backend failure produced ACK/content/retry: status=%d bytes=%d calls/reads=%d/%d", recorder.Code, recorder.Body.Len(), core.calls, core.reads)
			}
		})
	}
	handler, core, payload, _ := generationHTTPFixture(t)
	core.result = ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionNotFound}
	raw, err := json.Marshal(ResourceGenerationReadRequestV1{Version: 1, Authorization: payload.Authorization})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, generationHTTPRequest(ResourceGenerationReadPathV1, raw))
	if recorder.Code != http.StatusNotFound || recorder.Body.Len() != 0 || core.reads != 1 {
		t.Fatalf("missing head was ACK: status=%d reads=%d", recorder.Code, core.reads)
	}
}

func TestResourceGenerationHTTPReadShapeDoesNotAcceptPublishFields(t *testing.T) {
	t.Parallel()
	handler, core, _, publish := generationHTTPFixture(t)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, generationHTTPRequest(ResourceGenerationReadPathV1, publish))
	if recorder.Code != http.StatusBadRequest || core.reads != 0 || !reflect.DeepEqual(core.auth, ports.AttachedResourceGenerationAuthorizationV1{}) {
		t.Fatalf("read endpoint accepted publication envelope: status=%d reads=%d", recorder.Code, core.reads)
	}
}

type generationCancelBody struct {
	cancel context.CancelFunc
	reads  int
}

func (body *generationCancelBody) Read([]byte) (int, error) {
	body.reads++
	body.cancel()
	return 0, io.EOF
}
func (*generationCancelBody) Close() error { return nil }

func TestResourceGenerationHTTPBodyAndCoreShareOriginalCallerBudget(t *testing.T) {
	t.Parallel()
	for _, alreadyCancelled := range []bool{false, true} {
		handler, core, _, raw := generationHTTPFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		body := &generationCancelBody{cancel: cancel}
		request := generationHTTPRequest(ResourceGenerationPublishPathV1, raw).WithContext(ctx)
		request.Body = body
		if alreadyCancelled {
			cancel()
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		cancel()
		if recorder.Code != http.StatusServiceUnavailable || core.calls != 0 || core.reads != 0 || recorder.Body.Len() != 0 || alreadyCancelled && body.reads != 0 {
			t.Fatalf("cancelled body renewed budget or reached core: priorCancel=%t status=%d bodyReads=%d calls=%d", alreadyCancelled, recorder.Code, body.reads, core.calls)
		}
	}
}
