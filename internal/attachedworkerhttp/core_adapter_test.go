package attachedworkerhttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
)

func TestCoreExchangeAdapterPassesExplicitBearerCopyAndBatch(t *testing.T) {
	token, _ := ParseBearerToken("secret-worker-token")
	wantBatch := testBatch(1)
	core := coreExchangeFunc(func(_ context.Context, rawBearer []byte, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		if string(rawBearer) != "secret-worker-token" || batch.Frames[0].MessageID != wantBatch.Frames[0].MessageID {
			t.Fatalf("core boundary bearer=%q batch=%+v", rawBearer, batch)
		}
		// The boundary owns a copy and cannot mutate the redacted token value.
		rawBearer[0] = 'X'
		return nil, nil
	})
	adapter, err := NewCoreExchangeAdapter(core)
	if err != nil {
		t.Fatal(err)
	}
	response, err := adapter.Exchange(context.Background(), token, wantBatch)
	if err != nil || response != nil {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if string(token.Bytes()) != "secret-worker-token" || fmt.Sprint(token) != "[REDACTED]" {
		t.Fatal("core mutation crossed the redacted bearer boundary")
	}
}

func TestCoreExchangeAdapterPassesValidatedAW04PlatformResponse(t *testing.T) {
	token, _ := ParseBearerToken("secret-worker-token")
	response := testBatch(2)
	adapter, err := NewCoreExchangeAdapter(coreExchangeFunc(func(context.Context, []byte, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		return &response, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := adapter.Exchange(context.Background(), token, testBatch(1)); err != nil || got == nil || got.Frames[0].MessageID != response.Frames[0].MessageID {
		t.Fatalf("AW04 response=%+v err=%v", got, err)
	}
}

func TestCoreExchangeAdapterMapsOnlySanitizedSentinels(t *testing.T) {
	token, _ := ParseBearerToken("secret-worker-token")
	for _, test := range []struct {
		name    string
		coreErr error
		want    error
		status  int
	}{
		{name: "unauthorized", coreErr: fmt.Errorf("private credential detail: %w", attachedworkertransport.ErrTransportUnauthorized), want: ErrUnauthorized, status: http.StatusUnauthorized},
		{name: "conflict", coreErr: fmt.Errorf("private revision detail: %w", attachedworkertransport.ErrTransportConflict), want: ErrConflict, status: http.StatusConflict},
		{name: "backend", coreErr: fmt.Errorf("private database detail: %w", attachedworkertransport.ErrTransportBackend), want: ErrUnavailable, status: http.StatusServiceUnavailable},
		{name: "config is not a public status", coreErr: fmt.Errorf("private config detail: %w", attachedworkertransport.ErrTransportConfig), want: errCoreExchange, status: http.StatusInternalServerError},
		{name: "unknown", coreErr: errors.New("private implementation detail"), want: errCoreExchange, status: http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter, err := NewCoreExchangeAdapter(coreExchangeFunc(func(context.Context, []byte, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
				return nil, test.coreErr
			}))
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.Exchange(context.Background(), token, testBatch(1))
			if !errors.Is(err, test.want) || strings.Contains(err.Error(), "private") || errors.Is(err, test.coreErr) {
				t.Fatalf("mapped error=%v core=%v", err, test.coreErr)
			}

			encoded, encodeErr := attachedworkerprotocol.EncodeBatchV1(testBatch(1))
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			handler, handlerErr := NewHandler(adapter)
			if handlerErr != nil {
				t.Fatal(handlerErr)
			}
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, ExchangePathV1, bytes.NewReader(encoded))
			request.Header.Set("Authorization", token.headerValue())
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json")
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status || strings.Contains(recorder.Body.String(), "private") {
				t.Fatalf("HTTP status=%d body=%q", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestCoreExchangeAdapterRejectsMissingCoreAndInvalidToken(t *testing.T) {
	if _, err := NewCoreExchangeAdapter(nil); !errors.Is(err, errCoreExchange) {
		t.Fatalf("nil core error=%v", err)
	}
	adapter, err := NewCoreExchangeAdapter(coreExchangeFunc(func(context.Context, []byte, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		t.Fatal("invalid token reached core")
		return nil, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.Exchange(context.Background(), BearerToken{}, testBatch(1)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("invalid token error=%v", err)
	}
}

type coreExchangeFunc func(context.Context, []byte, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error)

func (function coreExchangeFunc) ExchangeBearer(ctx context.Context, rawBearer []byte, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	return function(ctx, rawBearer, batch)
}
