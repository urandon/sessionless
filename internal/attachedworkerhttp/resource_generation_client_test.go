package attachedworkerhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func generationClientInput(t *testing.T) ([]byte, ports.AttachedResourceGenerationPublicationV1) {
	t.Helper()
	_, _, payload, _ := generationHTTPFixture(t)
	secret, err := attachedworkertransport.ParseConnectionSecret([]byte("12345678901234567890123456789012"))
	if err != nil {
		t.Fatal(err)
	}
	bearer, err := attachedworkertransport.NewConnectionBearer(payload.Authorization.TenantID, payload.Authorization.OwnerUserID,
		payload.Authorization.WorkerID, payload.Authorization.ConnectionID, secret)
	if err != nil {
		t.Fatal(err)
	}
	return bearer.Bytes(), ports.AttachedResourceGenerationPublicationV1{Authorization: payload.Authorization, Target: payload.Target}
}

func generationClient(t *testing.T, transport http.RoundTripper) (*ResourceGenerationClientV1, []byte, ports.AttachedResourceGenerationPublicationV1) {
	t.Helper()
	bearer, publication := generationClientInput(t)
	client, err := NewResourceGenerationClientV1("https://generation.invalid", &http.Client{Transport: transport}, bearer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(); clear(bearer) })
	return client, bearer, publication
}

func generationClientJSON(t *testing.T, status ports.AttachedWorkerExecutionStatus, head domain.AttachedResourceGenerationHeadV1) []byte {
	t.Helper()
	raw, err := json.Marshal(ResourceGenerationResponseV1{Version: 1, Status: status, Head: head})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func generationClientReply(raw []byte) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(bytes.NewReader(raw)), ContentLength: int64(len(raw))}
}

func requireGenerationClientError(t *testing.T, result ports.AttachedResourceGenerationResultV1, err error, kind ErrorKind) {
	t.Helper()
	var classified *ExchangeError
	if result != (ports.AttachedResourceGenerationResultV1{}) || !errors.As(err, &classified) || classified.Kind != kind || classified.Retryable() {
		t.Fatalf("client error result=%+v err=%v want nonretryable %s/zero head", result, err, kind)
	}
}

func TestResourceGenerationClientOnePublishAndExplicitReadUseOriginalWire(t *testing.T) {
	t.Parallel()
	var publication ports.AttachedResourceGenerationPublicationV1
	var bearer []byte
	var requests []*http.Request
	calls := 0
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		requests = append(requests, request)
		wantPath := ResourceGenerationPublishPathV1
		if calls == 2 {
			wantPath = ResourceGenerationReadPathV1
		}
		if request.Method != http.MethodPost || request.URL.String() != "https://generation.invalid"+wantPath || request.GetBody != nil ||
			request.Header.Get("Authorization") != "Bearer "+string(bearer) || request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Accept") != "application/json" ||
			request.Header.Get("Idempotency-Key") != "" || request.Header.Get("X-Idempotency-Key") != "" || request.Header.Get("Cookie") != "" {
			t.Fatal("metadata client changed origin/method/auth/wire or enabled replay/ambient state")
		}
		raw, err := io.ReadAll(request.Body)
		if err != nil || len(raw) > 4096 || bytes.Contains(raw, []byte("presented_secret_digest")) {
			t.Fatalf("wire metadata bound/digest: bytes=%d err=%v", len(raw), err)
		}
		if calls == 1 {
			var decoded ResourceGenerationPublishRequestV1
			if decodeResourceGenerationV1(raw, &decoded) != nil || decoded.Version != 1 || decoded.Authorization != publication.Authorization || decoded.Target != publication.Target {
				t.Fatal("publication diverged from frozen server DTO")
			}
		} else {
			var decoded ResourceGenerationReadRequestV1
			if decodeResourceGenerationV1(raw, &decoded) != nil || decoded.Version != 1 || decoded.Authorization != publication.Authorization {
				t.Fatal("read diverged from frozen server DTO")
			}
		}
		return generationClientReply(generationClientJSON(t, ports.AttachedWorkerExecutionApplied, publication.Target)), nil
	})
	client, owned, input := generationClient(t, transport)
	bearer, publication = bytes.Clone(owned), input
	clear(owned) // Constructor must own its original bearer, not the caller slice.
	defer clear(bearer)
	result, err := client.Publish(t.Context(), publication)
	if err != nil || result.Head != publication.Target || calls != 1 {
		t.Fatalf("one publication result=%+v err=%v calls=%d", result, err, calls)
	}
	result, err = client.Read(t.Context(), publication.Authorization)
	if err != nil || result.Head != publication.Target || calls != 2 {
		t.Fatalf("explicit read result=%+v err=%v calls=%d", result, err, calls)
	}
	for _, request := range requests {
		if request.Header.Get("Authorization") != "" || request.GetBody != nil {
			t.Fatal("completed request retained bearer/replay accessor")
		}
	}
}

func TestResourceGenerationClientRejectsRequestScopeBeforeHTTP(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*ports.AttachedResourceGenerationPublicationV1)
		kind   ErrorKind
	}{
		{"tenant", func(v *ports.AttachedResourceGenerationPublicationV1) { v.Authorization.TenantID = "foreign" }, ErrorUnauthorized},
		{"owner", func(v *ports.AttachedResourceGenerationPublicationV1) {
			v.Authorization.OwnerUserID = "foreign"
			v.Target.OwnerUserID = "foreign"
		}, ErrorUnauthorized},
		{"worker", func(v *ports.AttachedResourceGenerationPublicationV1) {
			v.Authorization.WorkerID = "foreign"
			v.Target.WorkerID = "foreign"
		}, ErrorUnauthorized},
		{"connection", func(v *ports.AttachedResourceGenerationPublicationV1) { v.Authorization.ConnectionID = "foreign" }, ErrorUnauthorized},
		{"supplied digest", func(v *ports.AttachedResourceGenerationPublicationV1) {
			v.Authorization.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("forged"))
		}, ErrorUnauthorized},
		{"missing scalar", func(v *ports.AttachedResourceGenerationPublicationV1) { v.Authorization.ConnectionGeneration = 0 }, ErrorUnauthorized},
		{"overlong resource", func(v *ports.AttachedResourceGenerationPublicationV1) {
			v.Authorization.SubscriptionConnectionID = domain.SubscriptionConnectionID(strings.Repeat("x", 4097))
		}, ErrorUnauthorized},
		{"target owner", func(v *ports.AttachedResourceGenerationPublicationV1) { v.Target.OwnerUserID = "foreign" }, ErrorProtocol},
		{"active final generation", func(v *ports.AttachedResourceGenerationPublicationV1) { v.Target.CredentialGeneration = ^uint64(0) }, ErrorProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client, _, publication := generationClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("must not dispatch") }))
			tc.change(&publication)
			result, err := client.Publish(t.Context(), publication)
			requireGenerationClientError(t, result, err, tc.kind)
			if calls != 0 {
				t.Fatalf("invalid publication HTTP calls=%d", calls)
			}
			if tc.kind == ErrorUnauthorized {
				result, err = client.Read(t.Context(), publication.Authorization)
				requireGenerationClientError(t, result, err, ErrorUnauthorized)
				if calls != 0 {
					t.Fatal("invalid read dispatched")
				}
			}
		})
	}
}

func TestResourceGenerationClientRejectsClosedOrForeignResponses(t *testing.T) {
	t.Parallel()
	valid := generationClientJSON(t, ports.AttachedWorkerExecutionApplied, generationHTTPHead(t))
	for _, tc := range []struct {
		name string
		raw  []byte
	}{
		{"missing version", bytes.Replace(valid, []byte(`"version":1,`), nil, 1)},
		{"null version", bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":null`), 1)},
		{"wrong version", bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":2`), 1)},
		{"case alias", bytes.Replace(valid, []byte(`"owner_user_id"`), []byte(`"Owner_User_ID"`), 1)},
		{"unknown", bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"future":true`), 1)},
		{"duplicate", bytes.Replace(valid, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)},
		{"escaped duplicate", bytes.Replace(valid, []byte(`"revoked":false`), []byte(`"revoked":false,"\u0072evoked":false`), 1)},
		{"missing revoked", bytes.Replace(valid, []byte(`,"revoked":false`), nil, 1)},
		{"null revoked", bytes.Replace(valid, []byte(`"revoked":false`), []byte(`"revoked":null`), 1)},
		{"foreign scope", bytes.Replace(valid, []byte(`"owner"`), []byte(`"foreign"`), 1)},
		{"other generation", bytes.Replace(valid, []byte(`"credential_generation":1`), []byte(`"credential_generation":2`), 1)},
		{"unexpected revoke", bytes.Replace(valid, []byte(`"revoked":false`), []byte(`"revoked":true`), 1)},
		{"unknown status", bytes.Replace(valid, []byte(`"applied"`), []byte(`"invented"`), 1)},
		{"denied with head", bytes.Replace(valid, []byte(`"applied"`), []byte(`"denied"`), 1)},
		{"trailing", append(bytes.Clone(valid), []byte(" {}")...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client, _, publication := generationClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) { calls++; return generationClientReply(tc.raw), nil }))
			result, err := client.Publish(t.Context(), publication)
			requireGenerationClientError(t, result, err, ErrorProtocol)
			if calls != 1 {
				t.Fatalf("response violation caused calls=%d", calls)
			}
		})
	}
}

type generationClientBody struct {
	reader        io.Reader
	reads, closed int
}

func (body *generationClientBody) Read(p []byte) (int, error) {
	body.reads++
	return body.reader.Read(p)
}
func (body *generationClientBody) Close() error { body.closed++; return nil }

func TestResourceGenerationClientBoundsResponseBeforeDecode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		size     int
		declared int64
		header   http.Header
		valid    bool
		noRead   bool
	}{
		{"exact", 4096, 4096, http.Header{"Content-Type": {"application/json"}}, true, false},
		{"unknown exact", 4096, -1, http.Header{"Content-Type": {"application/json"}}, true, false},
		{"declared oversize", 4097, 4097, http.Header{"Content-Type": {"application/json"}}, false, true},
		{"unknown oversize", 4097, -1, http.Header{"Content-Type": {"application/json"}}, false, false},
		{"media", 300, -1, http.Header{"Content-Type": {"text/plain"}}, false, true},
		{"duplicate media", 300, -1, http.Header{"Content-Type": {"application/json", "application/json"}}, false, true},
		{"headers", 300, -1, http.Header{"Content-Type": {"application/json"}, "X-Too-Large": {strings.Repeat("x", 8193)}}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body *generationClientBody
			client, _, publication := generationClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
				raw := generationClientJSON(t, ports.AttachedWorkerExecutionApplied, generationHTTPHead(t))
				if len(raw) > tc.size {
					t.Fatal("fixture size smaller than canonical response")
				}
				raw = append(raw, bytes.Repeat([]byte(" "), tc.size-len(raw))...)
				body = &generationClientBody{reader: bytes.NewReader(raw)}
				return &http.Response{StatusCode: 200, Header: tc.header, ContentLength: tc.declared, Body: body}, nil
			}))
			result, err := client.Publish(t.Context(), publication)
			if tc.valid {
				if err != nil || result.Head != publication.Target {
					t.Fatalf("exact response rejected: %v", err)
				}
			} else {
				requireGenerationClientError(t, result, err, ErrorProtocol)
			}
			if body.closed != 1 || tc.noRead && body.reads != 0 {
				t.Fatalf("body lifecycle closed=%d reads=%d noRead=%t", body.closed, body.reads, tc.noRead)
			}
		})
	}
}
func generationHTTPHead(t *testing.T) domain.AttachedResourceGenerationHeadV1 {
	t.Helper()
	_, _, payload, _ := generationHTTPFixture(t)
	return payload.Target
}

func TestResourceGenerationClientUnknownAndHTTPFailuresNeverRetry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		status int
		err    error
		kind   ErrorKind
	}{
		{"lost response", 0, errors.New("PRIVATE_TOKEN https://private.invalid"), ErrorUnavailable},
		{"unauthorized", 401, nil, ErrorUnauthorized}, {"forbidden", 403, nil, ErrorUnauthorized}, {"conflict", 409, nil, ErrorConflict},
		{"busy", 429, nil, ErrorUnavailable}, {"server", 500, nil, ErrorUnavailable}, {"gateway", 502, nil, ErrorUnavailable}, {"unavailable", 503, nil, ErrorUnavailable}, {"timeout", 504, nil, ErrorUnavailable},
		{"publish missing", 404, nil, ErrorProtocol}, {"redirect", 307, nil, ErrorProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client, _, publication := generationClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				if tc.err != nil {
					return nil, tc.err
				}
				return &http.Response{StatusCode: tc.status, Header: http.Header{"Location": {"https://foreign.invalid"}, "Retry-After": {"1"}}, Body: io.NopCloser(strings.NewReader("PRIVATE_TOKEN"))}, nil
			}))
			result, err := client.Publish(t.Context(), publication)
			requireGenerationClientError(t, result, err, tc.kind)
			if calls != 1 || strings.Contains(err.Error(), "PRIVATE_TOKEN") || strings.Contains(err.Error(), "private.invalid") {
				t.Fatalf("failure retried/readback/leaked: calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestResourceGenerationClientReadMissingAndStatusRules(t *testing.T) {
	t.Parallel()
	for _, status := range []ports.AttachedWorkerExecutionStatus{ports.AttachedWorkerExecutionApplied, ports.AttachedWorkerExecutionReplayed} {
		t.Run(string(status), func(t *testing.T) {
			client, _, publication := generationClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
				return generationClientReply(generationClientJSON(t, status, generationHTTPHead(t))), nil
			}))
			result, err := client.Read(t.Context(), publication.Authorization)
			if status == ports.AttachedWorkerExecutionApplied {
				if err != nil || result.Head != publication.Target {
					t.Fatal("read failed", err)
				}
			} else {
				requireGenerationClientError(t, result, err, ErrorProtocol)
			}
		})
	}
	client, _, publication := generationClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("untrusted"))}, nil
	}))
	result, err := client.Read(t.Context(), publication.Authorization)
	if err != nil || result.Status != ports.AttachedWorkerExecutionNotFound || result.Head != (domain.AttachedResourceGenerationHeadV1{}) {
		t.Fatalf("missing read=%+v err=%v", result, err)
	}
}

type generationClientJar struct{ calls atomic.Uint32 }

func (jar *generationClientJar) SetCookies(*url.URL, []*http.Cookie) { jar.calls.Add(1) }
func (jar *generationClientJar) Cookies(*url.URL) []*http.Cookie {
	jar.calls.Add(1)
	return []*http.Cookie{{Name: "ambient", Value: "private"}}
}

func TestResourceGenerationClientConstructorClonesExplicitConfigAndRejectsAliases(t *testing.T) {
	t.Parallel()
	bearer, _ := generationClientInput(t)
	defer clear(bearer)
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) { t.Fatal("constructor dispatched HTTP"); return nil, nil })
	jar := new(generationClientJar)
	original := &http.Client{Transport: transport, Jar: jar, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("original policy") }}
	client, err := NewResourceGenerationClientV1("https://generation.invalid/", original, bearer)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if original.Jar != jar || original.Timeout != time.Second || original.CheckRedirect(nil, nil).Error() != "original policy" || client.http.Jar != nil || client.http.Timeout != time.Second || jar.calls.Load() != 0 {
		t.Fatal("constructor mutated original client or consulted ambient cookies")
	}
	for _, origin := range []string{"http://generation.invalid", "https://user:private@generation.invalid", "https://generation.invalid/path", "https://generation.invalid?", "https://generation.invalid#", "https://generation.invalid/?x=1", "https://generation.invalid/%2f", "https://generation.invalid#fragment", "https://"} {
		if value, err := NewResourceGenerationClientV1(origin, original, bearer); err == nil || value != nil {
			t.Fatalf("invalid/aliased origin accepted: %q", origin)
		}
	}
	for _, raw := range [][]byte{nil, []byte("slw1.invalid"), bytes.Repeat([]byte("x"), 1025)} {
		if value, err := NewResourceGenerationClientV1("https://generation.invalid", original, raw); err == nil || value != nil {
			t.Fatal("malformed/unbounded bearer accepted")
		}
	}
	for _, config := range []*http.Client{nil, {}} {
		if value, err := NewResourceGenerationClientV1("https://generation.invalid", config, bearer); err == nil || value != nil {
			t.Fatal("ambient default HTTP transport accepted")
		}
	}
	if strings.Contains(fmt.Sprintf("%v %#v", client, client), string(bearer)) {
		t.Fatal("client formatting leaked original bearer")
	}
}

func TestResourceGenerationClientPublishAndReadRetainCallerBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var deadlines []time.Time
		client, _, publication := generationClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
			deadline, ok := request.Context().Deadline()
			if !ok {
				t.Fatal("missing call deadline")
			}
			deadlines = append(deadlines, deadline)
			return generationClientReply(generationClientJSON(t, ports.AttachedWorkerExecutionApplied, generationHTTPHead(t))), nil
		}))
		call, cancel := context.WithTimeout(t.Context(), 9*time.Second)
		defer cancel()
		want, _ := call.Deadline()
		if _, err := client.Publish(call, publication); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second) // Explicit virtual time advance, not readiness synchronization.
		if _, err := client.Read(call, publication.Authorization); err != nil {
			t.Fatal(err)
		}
		if len(deadlines) != 2 || !deadlines[0].Equal(want) || !deadlines[1].Equal(want) {
			t.Fatalf("readback renewed original deadline: %v want %v", deadlines, want)
		}
		before := time.Now()
		if _, err := client.Read(context.Background(), publication.Authorization); err != nil {
			t.Fatal(err)
		}
		if !deadlines[2].Equal(before.Add(15 * time.Second)) {
			t.Fatal("unbounded call escaped15s cap")
		}
		cancel()
		result, err := client.Publish(call, publication)
		requireGenerationClientError(t, result, err, ErrorUnavailable)
		if len(deadlines) != 3 {
			t.Fatal("cancelled caller dispatched")
		}
	})
}

func TestResourceGenerationClientCloseCancelsOriginalInFlightAndScrubsBearer(t *testing.T) {
	entered := make(chan struct{})
	client, _, publication := generationClient(t, roundTripFunc(func(request *http.Request) (*http.Response, error) {
		close(entered)
		<-request.Context().Done()
		return nil, errors.New("PRIVATE_TOKEN cancelled")
	}))
	owned := client.bearer
	call, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type outcome struct {
		result ports.AttachedResourceGenerationResultV1
		err    error
	}
	done := make(chan outcome, 1)
	go func() { result, err := client.Publish(call, publication); done <- outcome{result, err} }()
	select {
	case <-entered:
	case <-call.Done():
		t.Fatal("fake transport never entered", call.Err())
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		requireGenerationClientError(t, result.result, result.err, ErrorUnavailable)
	case <-call.Done():
		t.Fatal("Close failed to cancel original operation", call.Err())
	}
	if !bytes.Equal(owned, make([]byte, len(owned))) || len(client.bearer) != 0 {
		t.Fatal("closed client retained bearer")
	}
	result, err := client.Read(call, publication.Authorization)
	requireGenerationClientError(t, result, err, ErrorUnavailable)
	if err := client.Close(); err != nil {
		t.Fatal("repeated Close failed", err)
	}
}

type generationClientFaultBody struct {
	reader io.Reader
	before func()
	err    error
	closed bool
}

func (body *generationClientFaultBody) Read(p []byte) (int, error) {
	if body.before != nil {
		body.before()
		body.before = nil
	}
	n, err := body.reader.Read(p)
	if body.err != nil {
		return n, body.err
	}
	return n, err
}
func (body *generationClientFaultBody) Close() error { body.closed = true; return nil }

func TestResourceGenerationClientLateCancellationOrPartialBodyNeverAcknowledges(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"partial read error", "cancelled body", "closed body", "closed transport"} {
		t.Run(name, func(t *testing.T) {
			call, cancel := context.WithCancel(t.Context())
			defer cancel()
			var client *ResourceGenerationClientV1
			var body *generationClientFaultBody
			calls := 0
			value, _, publication := generationClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				body = &generationClientFaultBody{reader: bytes.NewReader(generationClientJSON(t, ports.AttachedWorkerExecutionApplied, generationHTTPHead(t)))}
				switch name {
				case "partial read error":
					body.err = io.ErrUnexpectedEOF
				case "cancelled body":
					body.before = cancel
				case "closed body":
					body.before = func() { _ = client.Close() }
				case "closed transport":
					_ = client.Close()
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: body, ContentLength: -1}, nil
			}))
			client = value
			result, err := client.Publish(call, publication)
			requireGenerationClientError(t, result, err, ErrorUnavailable)
			if calls != 1 || !body.closed {
				t.Fatalf("late failure retried/readback/leaked body: calls=%d closed=%t", calls, body.closed)
			}
		})
	}
}
