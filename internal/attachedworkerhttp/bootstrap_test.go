package attachedworkerhttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestBootstrapHandlerPassesUntrustedLocatorsToProofBoundCore(t *testing.T) {
	input := ChallengeRequestV1{
		TenantLocator: "tenant-a", OwnerLocator: "owner-a", ExpectedAudience: "sessionless:attached-worker:v1", ExpectedWorkerRevision: 7,
		Purpose: domain.AttachedWorkerAttachInitial, Hello: testBatch(1).Frames[0], Proof: bytes.Repeat([]byte{0x44}, 64),
	}
	wantFrame := testBatch(2).Frames[0]
	var calls atomic.Int32
	core := bootstrapCoreStub{
		issue: func(_ context.Context, tenant domain.TenantID, owner domain.UserID, request attachedworkertransport.IssueChallengeRequest) (attachedworkertransport.ChallengeGrant, error) {
			calls.Add(1)
			if tenant != input.TenantLocator || owner != input.OwnerLocator || request.WorkerID != domain.AttachedWorkerID(input.Hello.WorkerID) ||
				request.ExpectedAudience != input.ExpectedAudience || request.ExpectedWorkerRevision != input.ExpectedWorkerRevision || request.Purpose != input.Purpose ||
				request.Hello.MessageID != input.Hello.MessageID || !bytes.Equal(request.Proof, input.Proof) {
				t.Fatalf("changed core request tenant=%q owner=%q request=%+v", tenant, owner, request)
			}
			request.Proof[0] ^= 0xff
			return attachedworkertransport.ChallengeGrant{Challenge: domain.AttachedWorkerAttachChallenge{
				WorkerProtocolVersions: []uint32{}, PlatformProtocolVersions: []uint32{},
			}, Frame: wantFrame}, nil
		},
	}
	handler := newBootstrapHandler(t, core)
	recorder := serveBootstrap(t, handler, ChallengePathV1, input)
	if recorder.Code != http.StatusCreated || calls.Load() != 1 || input.Proof[0] != 0x44 {
		t.Fatalf("challenge status=%d calls=%d", recorder.Code, calls.Load())
	}
	var response ChallengeResponseV1
	if err := decodeStrictJSON(recorder.Body.Bytes(), &response); err != nil || response.Frame.MessageID != wantFrame.MessageID {
		t.Fatalf("challenge response=%+v err=%v", response, err)
	}
}

func TestBootstrapHandlerActivatesWithDigestOnlyAndReturnsAccepted(t *testing.T) {
	rawSecret := "raw-connection-secret-must-not-cross-http"
	input := ActivateRequestV1{
		TenantLocator: "tenant-a", OwnerLocator: "owner-a", ChallengeID: "challenge-a",
		ConnectionSecretDigest: domain.AttachedWorkerConnectionSecretDigest(strings.Repeat("a", 64)), Attach: testBatch(1).Frames[0],
	}
	wantAccepted := testBatch(2).Frames[0]
	privateSnapshot := "server-private-machine-snapshot"
	core := bootstrapCoreStub{
		activate: func(_ context.Context, tenant domain.TenantID, owner domain.UserID, request attachedworkertransport.ActivateRequest) (attachedworkertransport.ActivationGrant, error) {
			if tenant != input.TenantLocator || owner != input.OwnerLocator || request.ChallengeID != input.ChallengeID ||
				request.ConnectionSecretDigest != input.ConnectionSecretDigest || request.Attach.MessageID != input.Attach.MessageID {
				t.Fatalf("changed activation request tenant=%q owner=%q request=%+v", tenant, owner, request)
			}
			return attachedworkertransport.ActivationGrant{
				Connection: domain.AttachedWorkerConnection{SecretDigest: input.ConnectionSecretDigest, ProtocolSnapshot: []byte(privateSnapshot)}, Accepted: wantAccepted,
			}, nil
		},
	}
	handler := newBootstrapHandler(t, core)
	recorder := serveBootstrap(t, handler, AttachPathV1, input)
	if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), rawSecret) ||
		strings.Contains(recorder.Body.String(), privateSnapshot) || strings.Contains(recorder.Body.String(), "protocol_snapshot") {
		t.Fatalf("activation status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	var response ActivateResponseV1
	if err := decodeStrictJSON(recorder.Body.Bytes(), &response); err != nil || response.Accepted.MessageID != wantAccepted.MessageID ||
		response.Connection.SecretDigest != input.ConnectionSecretDigest {
		t.Fatalf("activation response=%+v err=%v", response, err)
	}
}

func TestBootstrapHandlerStrictlyRejectsAmbiguousOrSecretBearingBodies(t *testing.T) {
	var calls atomic.Int32
	handler := newBootstrapHandler(t, bootstrapCoreStub{issue: func(context.Context, domain.TenantID, domain.UserID, attachedworkertransport.IssueChallengeRequest) (attachedworkertransport.ChallengeGrant, error) {
		calls.Add(1)
		return attachedworkertransport.ChallengeGrant{}, nil
	}, activate: func(context.Context, domain.TenantID, domain.UserID, attachedworkertransport.ActivateRequest) (attachedworkertransport.ActivationGrant, error) {
		calls.Add(1)
		return attachedworkertransport.ActivationGrant{}, nil
	}})
	validChallenge, _ := encodeStrictJSON(ChallengeRequestV1{
		TenantLocator: "tenant-a", OwnerLocator: "owner-a", ExpectedAudience: "sessionless:attached-worker:v1", ExpectedWorkerRevision: 1,
		Purpose: domain.AttachedWorkerAttachInitial, Hello: testBatch(1).Frames[0], Proof: bytes.Repeat([]byte{1}, 64),
	})
	duplicate := bytes.Replace(validChallenge, []byte(`"tenant_locator":"tenant-a"`), []byte(`"tenant_locator":"tenant-a","tenant_locator":"tenant-b"`), 1)
	unknown := bytes.Replace(validChallenge, []byte(`"tenant_locator":"tenant-a"`), []byte(`"tenant_locator":"tenant-a","tenant_id":"tenant-a"`), 1)
	nullValue := bytes.Replace(validChallenge, []byte(`"proof":"`), []byte(`"proof":null,"removed":"`), 1)
	oversized := bytes.Repeat([]byte{'x'}, attachedworkerprotocol.MaxBatchBytes+1)
	activationWithRawSecret := []byte(`{"tenant_locator":"tenant-a","owner_locator":"owner-a","challenge_id":"challenge-a","connection_secret_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","secret":"raw-secret","attach":{}}`)
	for _, test := range []struct {
		name string
		path string
		body []byte
		want int
	}{
		{name: "duplicate", path: ChallengePathV1, body: duplicate, want: http.StatusBadRequest},
		{name: "unknown locator authority", path: ChallengePathV1, body: unknown, want: http.StatusBadRequest},
		{name: "null", path: ChallengePathV1, body: nullValue, want: http.StatusBadRequest},
		{name: "oversized", path: ChallengePathV1, body: oversized, want: http.StatusRequestEntityTooLarge},
		{name: "raw secret", path: AttachPathV1, body: activationWithRawSecret, want: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, test.path, bytes.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json")
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.want {
				t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid bodies reached core %d times", calls.Load())
	}
}

func TestBootstrapHandlerEnforcesExactRouteHeadersAndInitialAttachGate(t *testing.T) {
	var calls atomic.Int32
	handler := newBootstrapHandler(t, bootstrapCoreStub{issue: func(context.Context, domain.TenantID, domain.UserID, attachedworkertransport.IssueChallengeRequest) (attachedworkertransport.ChallengeGrant, error) {
		calls.Add(1)
		return attachedworkertransport.ChallengeGrant{}, nil
	}})
	input := ChallengeRequestV1{
		TenantLocator: "tenant-a", OwnerLocator: "owner-a", ExpectedAudience: "sessionless:attached-worker:v1", ExpectedWorkerRevision: 1,
		Purpose: domain.AttachedWorkerAttachInitial, Hello: testBatch(1).Frames[0], Proof: bytes.Repeat([]byte{1}, 64),
	}
	body, _ := encodeStrictJSON(input)
	tests := []struct {
		name    string
		method  string
		path    string
		headers http.Header
		body    []byte
		status  int
	}{
		{name: "unknown path before media", method: http.MethodPost, path: "/unknown", headers: http.Header{}, body: body, status: http.StatusNotFound},
		{name: "non post", method: http.MethodGet, path: ChallengePathV1, headers: strictBootstrapHeaders(), body: body, status: http.StatusNotFound},
		{name: "query forbidden", method: http.MethodPost, path: ChallengePathV1 + "?wait_ms=0", headers: strictBootstrapHeaders(), body: body, status: http.StatusBadRequest},
		{name: "duplicate content type", method: http.MethodPost, path: ChallengePathV1, headers: http.Header{"Content-Type": []string{"application/json", "application/json"}, "Accept": []string{"application/json"}}, body: body, status: http.StatusUnsupportedMediaType},
		{name: "duplicate accept", method: http.MethodPost, path: ChallengePathV1, headers: http.Header{"Content-Type": []string{"application/json"}, "Accept": []string{"application/json", "application/json"}}, body: body, status: http.StatusNotAcceptable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, test.path, bytes.NewReader(test.body))
			request.Header = test.headers.Clone()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Fatalf("status=%d", recorder.Code)
			}
		})
	}
	input.Purpose = domain.AttachedWorkerAttachReconnect
	recorder := serveBootstrap(t, handler, ChallengePathV1, input)
	if recorder.Code != http.StatusBadRequest || calls.Load() != 0 {
		t.Fatalf("reconnect gate status=%d calls=%d", recorder.Code, calls.Load())
	}
}

func TestStrictJSONUsesAW02BoundsAndCanonicalTokens(t *testing.T) {
	deep := strings.Repeat(`{"a":`, strictJSONMaxDepth+1) + `0` + strings.Repeat(`}`, strictJSONMaxDepth+1)
	items := make([]string, strictJSONMaxArrayItems+1)
	for index := range items {
		items[index] = "0"
	}
	for _, encoded := range []string{
		`{"a":1,"a":2}`,
		`{"A":1}`,
		`{"a":null}`,
		`{"a":-1}`,
		`{"a":1.0}`,
		`{"a":1e1}`,
		deep,
		`[` + strings.Join(items, ",") + `]`,
	} {
		var target any
		if err := decodeStrictJSON([]byte(encoded), &target); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("noncanonical JSON accepted: %s", encoded)
		}
	}
}

func TestBootstrapHandlerCollapsesCoreErrorsWithoutDetails(t *testing.T) {
	for _, test := range []struct {
		name   string
		core   error
		status int
	}{
		{name: "cross owner or bad proof", core: fmt.Errorf("private owner detail: %w", attachedworkertransport.ErrTransportUnauthorized), status: http.StatusUnauthorized},
		{name: "conflict", core: fmt.Errorf("private revision detail: %w", attachedworkertransport.ErrTransportConflict), status: http.StatusConflict},
		{name: "backend", core: fmt.Errorf("private database detail: %w", attachedworkertransport.ErrTransportBackend), status: http.StatusServiceUnavailable},
		{name: "unknown", core: errors.New("private implementation detail"), status: http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := newBootstrapHandler(t, bootstrapCoreStub{issue: func(context.Context, domain.TenantID, domain.UserID, attachedworkertransport.IssueChallengeRequest) (attachedworkertransport.ChallengeGrant, error) {
				return attachedworkertransport.ChallengeGrant{}, test.core
			}})
			recorder := serveBootstrap(t, handler, ChallengePathV1, ChallengeRequestV1{
				TenantLocator: "tenant-a", OwnerLocator: "owner-a", ExpectedAudience: "sessionless:attached-worker:v1", ExpectedWorkerRevision: 1,
				Purpose: domain.AttachedWorkerAttachInitial, Hello: testBatch(1).Frames[0], Proof: bytes.Repeat([]byte{1}, 64),
			})
			if recorder.Code != test.status || strings.Contains(recorder.Body.String(), "private") {
				t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func newBootstrapHandler(t *testing.T, core BootstrapCore) *BootstrapHandler {
	t.Helper()
	handler, err := NewBootstrapHandler(core)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func serveBootstrap(t *testing.T, handler http.Handler, path string, value any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := encodeStrictJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	handler.ServeHTTP(recorder, request)
	return recorder
}

func strictBootstrapHeaders() http.Header {
	return http.Header{"Content-Type": []string{"application/json"}, "Accept": []string{"application/json"}}
}

type bootstrapCoreStub struct {
	issue    func(context.Context, domain.TenantID, domain.UserID, attachedworkertransport.IssueChallengeRequest) (attachedworkertransport.ChallengeGrant, error)
	activate func(context.Context, domain.TenantID, domain.UserID, attachedworkertransport.ActivateRequest) (attachedworkertransport.ActivationGrant, error)
}

func (stub bootstrapCoreStub) IssueChallenge(ctx context.Context, tenant domain.TenantID, owner domain.UserID, request attachedworkertransport.IssueChallengeRequest) (attachedworkertransport.ChallengeGrant, error) {
	if stub.issue == nil {
		return attachedworkertransport.ChallengeGrant{}, errors.New("unexpected challenge")
	}
	return stub.issue(ctx, tenant, owner, request)
}

func (stub bootstrapCoreStub) Activate(ctx context.Context, tenant domain.TenantID, owner domain.UserID, request attachedworkertransport.ActivateRequest) (attachedworkertransport.ActivationGrant, error) {
	if stub.activate == nil {
		return attachedworkertransport.ActivationGrant{}, errors.New("unexpected activation")
	}
	return stub.activate(ctx, tenant, owner, request)
}
