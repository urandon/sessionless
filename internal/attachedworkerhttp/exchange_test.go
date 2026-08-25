package attachedworkerhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
)

func TestBearerTokenIsExplicitAndRedacted(t *testing.T) {
	token, err := ParseBearerToken("worker-token_1.abc==")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(token)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(token) != "[REDACTED]" || fmt.Sprintf("%#v", token) != "[REDACTED]" ||
		string(encoded) != `"[REDACTED]"` || string(token.Bytes()) != "worker-token_1.abc==" {
		t.Fatalf("token boundary leaked or changed: %s / %s / %s", token, encoded, token.Bytes())
	}
	for _, invalid := range []string{"", "=", "space token", "abc=tail", strings.Repeat("a", maxBearerTokenBytes+1)} {
		if _, err := ParseBearerToken(invalid); !errors.Is(err, ErrInvalidBearerToken) {
			t.Fatalf("token %q error = %v", invalid, err)
		}
	}
}

func TestHandlerImmediateExchangeAndIdleResponse(t *testing.T) {
	token, _ := ParseBearerToken("worker-token")
	requestBatch := testBatch(1)
	encoded, err := attachedworkerprotocol.EncodeBatchV1(requestBatch)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	handler, err := NewHandler(exchangeServiceFunc(func(_ context.Context, gotToken BearerToken, gotBatch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		calls.Add(1)
		if string(gotToken.Bytes()) != string(token.Bytes()) || gotBatch.Frames[0].MessageID != requestBatch.Frames[0].MessageID {
			t.Fatal("handler changed authenticated exchange")
		}
		return nil, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, ExchangePathV1+"?wait_ms=0", bytes.NewReader(encoded))
	request.Header.Set("Authorization", token.headerValue())
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent || recorder.Body.Len() != 0 || calls.Load() != 1 ||
		recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("idle exchange status=%d body=%q calls=%d", recorder.Code, recorder.Body.String(), calls.Load())
	}
}

func TestHandlerRejectsWaitAuthMediaTypeAndOversizedBodyBeforeService(t *testing.T) {
	var calls atomic.Int32
	handler, err := NewHandler(exchangeServiceFunc(func(context.Context, BearerToken, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		calls.Add(1)
		return nil, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	valid, _ := attachedworkerprotocol.EncodeBatchV1(testBatch(1))
	tests := []struct {
		name             string
		path             string
		contentType      string
		extraContentType string
		accept           []string
		omitAccept       bool
		auth             []string
		body             []byte
		status           int
	}{
		{name: "nonzero serverless wait", path: ExchangePathV1 + "?wait_ms=1", contentType: "application/json", auth: []string{"Bearer token"}, body: valid, status: http.StatusBadRequest},
		{name: "unknown query", path: ExchangePathV1 + "?wake=1", contentType: "application/json", auth: []string{"Bearer token"}, body: valid, status: http.StatusBadRequest},
		{name: "ambiguous query", path: ExchangePathV1 + "?wait_ms=0;wake=1", contentType: "application/json", auth: []string{"Bearer token"}, body: valid, status: http.StatusBadRequest},
		{name: "missing authorization", path: ExchangePathV1, contentType: "application/json", body: valid, status: http.StatusUnauthorized},
		{name: "multiple authorization", path: ExchangePathV1, contentType: "application/json", auth: []string{"Bearer one", "Bearer two"}, body: valid, status: http.StatusUnauthorized},
		{name: "media parameters", path: ExchangePathV1, contentType: "application/json; charset=utf-8", auth: []string{"Bearer token"}, body: valid, status: http.StatusUnsupportedMediaType},
		{name: "multiple content types", path: ExchangePathV1, contentType: "application/json", extraContentType: "application/json", auth: []string{"Bearer token"}, body: valid, status: http.StatusUnsupportedMediaType},
		{name: "missing accept", path: ExchangePathV1, contentType: "application/json", omitAccept: true, auth: []string{"Bearer token"}, body: valid, status: http.StatusNotAcceptable},
		{name: "multiple accepts", path: ExchangePathV1, contentType: "application/json", accept: []string{"application/json", "application/json"}, auth: []string{"Bearer token"}, body: valid, status: http.StatusNotAcceptable},
		{name: "accept parameters", path: ExchangePathV1, contentType: "application/json", accept: []string{"application/json; q=1"}, auth: []string{"Bearer token"}, body: valid, status: http.StatusNotAcceptable},
		{name: "oversized", path: ExchangePathV1, contentType: "application/json", auth: []string{"Bearer token"}, body: bytes.Repeat([]byte{'x'}, attachedworkerprotocol.MaxBatchBytes+1), status: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, test.path, bytes.NewReader(test.body))
			request.Header.Set("Content-Type", test.contentType)
			if test.extraContentType != "" {
				request.Header.Add("Content-Type", test.extraContentType)
			}
			if !test.omitAccept {
				if len(test.accept) == 0 {
					request.Header.Set("Accept", "application/json")
				} else {
					for _, accept := range test.accept {
						request.Header.Add("Accept", accept)
					}
				}
			}
			for _, authorization := range test.auth {
				request.Header.Add("Authorization", authorization)
			}
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Fatalf("status=%d want=%d", recorder.Code, test.status)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected requests reached service %d times", calls.Load())
	}
}

func TestHandlerReturnsOnlyBoundedProtocolBatchAndSanitizedServiceStatuses(t *testing.T) {
	token, _ := ParseBearerToken("worker-token")
	encoded, _ := attachedworkerprotocol.EncodeBatchV1(testBatch(1))
	for _, test := range []struct {
		name       string
		response   *attachedworkerprotocol.BatchV1
		serviceErr error
		status     int
	}{
		{name: "transactional AW04 batch", response: batchPointer(testBatch(2)), status: http.StatusOK},
		{name: "unauthorized", serviceErr: ErrUnauthorized, status: http.StatusUnauthorized},
		{name: "conflict", serviceErr: ErrConflict, status: http.StatusConflict},
		{name: "unavailable", serviceErr: ErrUnavailable, status: http.StatusServiceUnavailable},
		{name: "unknown", serviceErr: errors.New("private backend detail"), status: http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, err := NewHandler(exchangeServiceFunc(func(context.Context, BearerToken, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
				return test.response, test.serviceErr
			}))
			if err != nil {
				t.Fatal(err)
			}
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, ExchangePathV1, bytes.NewReader(encoded))
			request.Header.Set("Authorization", token.headerValue())
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json")
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status || strings.Contains(recorder.Body.String(), "private") {
				t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
			}
			if test.response != nil {
				decoded, decodeErr := attachedworkerprotocol.DecodeBatchV1(recorder.Body.Bytes())
				if decodeErr != nil || decoded.Frames[0].MessageID != test.response.Frames[0].MessageID || recorder.Header().Get("Content-Type") != "application/json" {
					t.Fatalf("AW04 response=%+v err=%v headers=%v", decoded, decodeErr, recorder.Header())
				}
			}
		})
	}
}

func TestClientUsesHTTPSHeaderOnlyBearerAndRequiresEmpty204(t *testing.T) {
	token, _ := ParseBearerToken("secret-worker-token")
	var calls atomic.Int32
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		if request.URL.Scheme != "https" || request.URL.Path != ExchangePathV1 || request.URL.RawQuery != "" ||
			len(request.Header.Values("Authorization")) != 1 || request.Header.Get("Authorization") != "Bearer secret-worker-token" ||
			len(request.Header.Values("Content-Type")) != 1 || request.Header.Get("Content-Type") != "application/json" ||
			len(request.Header.Values("Accept")) != 1 || request.Header.Get("Accept") != "application/json" {
			t.Fatalf("unsafe request: url=%s headers=%v", request.URL.Redacted(), request.Header)
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := attachedworkerprotocol.DecodeBatchV1(body); err != nil {
			t.Fatalf("request codec: %v", err)
		}
		return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
	})
	client, err := NewClient(ClientConfig{
		BaseURL: "https://control.example", Token: token,
		HTTPClient: &http.Client{Transport: transport}, RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Exchange(context.Background(), testBatch(1))
	if err != nil || response != nil || calls.Load() != 1 {
		t.Fatalf("exchange response=%+v err=%v calls=%d", response, err, calls.Load())
	}
	if _, err := NewClient(ClientConfig{BaseURL: "http://control.example", Token: token}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("plain HTTP accepted: %v", err)
	}
}

func TestClientAcceptsStrictBoundedPlatformBatch(t *testing.T) {
	token, _ := ParseBearerToken("secret-worker-token")
	want := testBatch(2)
	body, _ := attachedworkerprotocol.EncodeBatchV1(want)
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(body))}, nil
	})
	client, err := NewClient(ClientConfig{BaseURL: "https://control.example", Token: token, HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.Exchange(context.Background(), testBatch(1))
	if err != nil || got == nil || got.Frames[0].MessageID != want.Frames[0].MessageID {
		t.Fatalf("response=%+v err=%v", got, err)
	}
}

func TestClientDoesNotFollowRedirectOrExposeBodiesInErrors(t *testing.T) {
	token, _ := ParseBearerToken("secret-worker-token")
	var calls atomic.Int32
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusTemporaryRedirect,
			Header:     http.Header{"Location": []string{"https://other.example/capture"}},
			Body:       io.NopCloser(strings.NewReader("secret provider body")),
		}, nil
	})
	client, err := NewClient(ClientConfig{BaseURL: "https://control.example", Token: token, HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Exchange(context.Background(), testBatch(1))
	if err == nil || calls.Load() != 1 || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "control.example") {
		t.Fatalf("redirect/error handling calls=%d err=%v", calls.Load(), err)
	}
}

func TestClientResponseCapAndRetryClassification(t *testing.T) {
	token, _ := ParseBearerToken("worker-token")
	tests := []struct {
		name      string
		status    int
		body      []byte
		retryable bool
	}{
		{name: "oversized success", status: http.StatusOK, body: bytes.Repeat([]byte{'x'}, attachedworkerprotocol.MaxBatchBytes+1)},
		{name: "unauthorized", status: http.StatusUnauthorized},
		{name: "unavailable", status: http.StatusServiceUnavailable, retryable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: test.status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(test.body))}, nil
			})
			client, err := NewClient(ClientConfig{BaseURL: "https://control.example", Token: token, HTTPClient: &http.Client{Transport: transport}})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Exchange(context.Background(), testBatch(1))
			var exchangeErr *ExchangeError
			if !errors.As(err, &exchangeErr) || exchangeErr.Retryable() != test.retryable {
				t.Fatalf("error=%v retryable=%v", err, exchangeErr != nil && exchangeErr.Retryable())
			}
		})
	}
}

func TestClientRejectsAmbiguousResponseContentType(t *testing.T) {
	token, _ := ParseBearerToken("worker-token")
	responseBody, _ := attachedworkerprotocol.EncodeBatchV1(testBatch(2))
	transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json", "application/json"}},
			Body:       io.NopCloser(bytes.NewReader(responseBody)),
		}, nil
	})
	client, err := NewClient(ClientConfig{BaseURL: "https://control.example", Token: token, HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Exchange(context.Background(), testBatch(1))
	var exchangeErr *ExchangeError
	if !errors.As(err, &exchangeErr) || exchangeErr.Kind != ErrorProtocol || exchangeErr.Retryable() {
		t.Fatalf("ambiguous response error=%v", err)
	}
}

func TestClientIgnoresUntrustedRetryAfterAndKeepsSanitizedClassification(t *testing.T) {
	token, _ := ParseBearerToken("worker-token")
	for _, retryAfter := range [][]string{nil, {"999999999"}, {"1", "999999999"}, {"private backend detail"}} {
		transport := roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     http.Header{"Retry-After": retryAfter},
				Body:       io.NopCloser(strings.NewReader("private backend detail")),
			}, nil
		})
		client, err := NewClient(ClientConfig{BaseURL: "https://control.example", Token: token, HTTPClient: &http.Client{Transport: transport}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Exchange(context.Background(), testBatch(1))
		var exchangeErr *ExchangeError
		if !errors.As(err, &exchangeErr) || exchangeErr.Kind != ErrorUnavailable || !exchangeErr.Retryable() ||
			strings.Contains(err.Error(), "999") || strings.Contains(err.Error(), "private") {
			t.Fatalf("Retry-After=%q error=%v", retryAfter, err)
		}
	}
}

func TestClientTreatsOwnDeadlineAsRetryableButPreservesCallerCancellation(t *testing.T) {
	token, _ := ParseBearerToken("worker-token")
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	})
	client, err := NewClient(ClientConfig{
		BaseURL: "https://control.example", Token: token,
		HTTPClient: &http.Client{Transport: transport}, RequestTimeout: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Exchange(context.Background(), testBatch(1))
	var exchangeErr *ExchangeError
	if !errors.As(err, &exchangeErr) || !exchangeErr.Retryable() {
		t.Fatalf("request timeout error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.Exchange(ctx, testBatch(1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation error=%v", err)
	}
}

type exchangeServiceFunc func(context.Context, BearerToken, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error)

func (function exchangeServiceFunc) Exchange(ctx context.Context, token BearerToken, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	return function(ctx, token, batch)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func testBatch(sequence uint64) attachedworkerprotocol.BatchV1 {
	return attachedworkerprotocol.BatchV1{Version: attachedworkerprotocol.ProtocolVersionV1, Frames: []attachedworkerprotocol.FrameV1{{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, sequence),
		WorkerID: "worker-1", EnrollmentGeneration: 1, ConnectionGeneration: 1,
		Sequence: sequence, Kind: attachedworkerprotocol.MessageHeartbeat,
		Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: 1, Available: true},
	}}}
}

func batchPointer(batch attachedworkerprotocol.BatchV1) *attachedworkerprotocol.BatchV1 {
	return &batch
}
