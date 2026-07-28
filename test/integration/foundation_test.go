//go:build integration

package integration_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"gitcode.com/urandon/sessionless/internal/buildinfo"
	"gitcode.com/urandon/sessionless/internal/controlapi"
)

func TestControlAPIHealthContract(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler := controlapi.NewHandler(logger, buildinfo.Current("control-api"))
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	var body map[string]string
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("status payload = %q, want ok", body["status"])
	}
}
