package telegramfake

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUpdateSeedIsIdempotentAndVisibleToGetUpdates(t *testing.T) {
	handler := NewHandler("test-token", slog.New(slog.NewTextHandler(io.Discard, nil)))
	update := []byte(`{"update_id":42,"message":{"message_id":7,"text":"synthetic"}}`)

	for range 2 {
		response := performRequest(t, handler, http.MethodPost, "/test/updates", update)
		if response.Code != http.StatusOK {
			t.Fatalf("seed status = %d, body = %s", response.Code, response.Body.String())
		}
	}

	response := performRequest(t, handler, http.MethodGet, "/bottest-token/getUpdates?offset=1", nil)
	var payload struct {
		OK     bool              `json:"ok"`
		Result []json.RawMessage `json:"result"`
	}
	decodeResponse(t, response, &payload)
	if !payload.OK || len(payload.Result) != 1 {
		t.Fatalf("getUpdates result = %#v", payload)
	}
}

func TestSendMessageIsCaptured(t *testing.T) {
	handler := NewHandler("test-token", slog.New(slog.NewTextHandler(io.Discard, nil)))
	response := performRequest(
		t,
		handler,
		http.MethodPost,
		"/bottest-token/sendMessage",
		[]byte(`{"chat_id":123,"text":"hello"}`),
	)
	if response.Code != http.StatusOK {
		t.Fatalf("send status = %d, body = %s", response.Code, response.Body.String())
	}

	response = performRequest(t, handler, http.MethodGet, "/test/captures", nil)
	var payload struct {
		OK     bool      `json:"ok"`
		Result []Capture `json:"result"`
	}
	decodeResponse(t, response, &payload)
	if !payload.OK || len(payload.Result) != 1 {
		t.Fatalf("captures = %#v", payload)
	}
	if payload.Result[0].ChatID != 123 || payload.Result[0].Method != "sendMessage" {
		t.Fatalf("capture = %#v", payload.Result[0])
	}
}

func TestWrongBotTokenIsRejected(t *testing.T) {
	handler := NewHandler("test-token", slog.New(slog.NewTextHandler(io.Discard, nil)))
	response := performRequest(t, handler, http.MethodGet, "/botwrong/getMe", nil)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func performRequest(t *testing.T, handler http.Handler, method, target string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, target, bytes.NewReader(body))
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder, target any) {
	t.Helper()
	if err := json.Unmarshal(response.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response: %v; body = %s", err, response.Body.String())
	}
}
