package controlapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"gitcode.com/urandon/sessionless/internal/buildinfo"
)

func TestHealth(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	NewHandler(discardLogger(), buildinfo.Current("test")).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}
}

func TestVersion(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/version", nil)
	NewHandler(discardLogger(), buildinfo.Current("control-api")).ServeHTTP(recorder, request)

	var got buildinfo.Info
	if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Component != "control-api" {
		t.Fatalf("Component = %q, want control-api", got.Component)
	}
}

func TestUnknownRoute(t *testing.T) {
	t.Parallel()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/missing", nil)
	NewHandler(discardLogger(), buildinfo.Current("test")).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestSealedInputRouteIsDefaultOffAndExplicitlyInjected(t *testing.T) {
	t.Parallel()
	request := httptest.NewRequest(http.MethodPost, "/attached-worker/v1/sealed-input", nil)
	recorder := httptest.NewRecorder()
	NewHandler(discardLogger(), buildinfo.Current("test")).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("default sealed input route status=%d, want 404", recorder.Code)
	}
	calls := 0
	handler := NewHandlerWithOptions(discardLogger(), buildinfo.Current("test"), Options{
		AttachedWorkerSealedInput: http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			calls++
			writer.WriteHeader(http.StatusNoContent)
		}),
	})
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent || calls != 1 {
		t.Fatalf("injected sealed input route status=%d calls=%d", recorder.Code, calls)
	}
	request = httptest.NewRequest(http.MethodGet, "/attached-worker/v1/sealed-input", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound || calls != 1 {
		t.Fatalf("wrong method entered sealed input route: status=%d calls=%d", recorder.Code, calls)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
