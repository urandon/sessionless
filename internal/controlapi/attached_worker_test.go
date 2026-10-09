package controlapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/buildinfo"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func TestAttachedWorkerOptionsRealRoutesWithoutTelegram(t *testing.T) {
	t.Parallel()
	// Construction and rejection before authority do not touch a database or
	// object storage. These are real protocol handlers, not healthy substitutes.
	options, err := AttachedWorkerOptions("sessionless:attached-worker:v1", &ydbstore.Store{}, unusedControlBlobs{})
	if err != nil {
		t.Fatalf("compose attached-only routes: %v", err)
	}
	if options.TelegramWebhook != nil {
		t.Fatal("attached-only composition initialized a Telegram handler")
	}
	handler := NewHandlerWithOptions(discardLogger(), buildinfo.Current("test"), options)
	for _, tc := range []struct {
		path string
		want int
	}{
		{path: "/attached-worker/v1/challenges", want: http.StatusUnsupportedMediaType},
		{path: "/attached-worker/v1/attach", want: http.StatusUnsupportedMediaType},
		{path: "/attached-worker/v1/exchange", want: http.StatusUnsupportedMediaType},
		{path: "/attached-worker/v1/sealed-input", want: http.StatusBadRequest},
		{path: "/attached-worker/v1/output-receipt", want: http.StatusBadRequest},
		{path: "/telegram/webhook", want: http.StatusNotFound},
	} {
		t.Run(tc.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, tc.path, nil))
			if recorder.Code != tc.want {
				t.Errorf("POST %s: status=%d, want %d; body=%s", tc.path, recorder.Code, tc.want, recorder.Body.String())
			}
			recorder = httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if recorder.Code != http.StatusNotFound {
				t.Errorf("GET %s: status=%d, want 404", tc.path, recorder.Code)
			}
		})
	}
}

func TestAttachedWorkerOptionsRejectsMissingDependencies(t *testing.T) {
	t.Parallel()
	if _, err := AttachedWorkerOptions("sessionless:attached-worker:v1", nil, unusedControlBlobs{}); err == nil {
		t.Error("nil state accepted")
	}
	if _, err := AttachedWorkerOptions("sessionless:attached-worker:v1", &ydbstore.Store{}, nil); err == nil {
		t.Error("nil blobs accepted")
	}
	if _, err := AttachedWorkerOptions("", &ydbstore.Store{}, unusedControlBlobs{}); err == nil {
		t.Error("empty audience accepted")
	}
}

func TestAttachedWorkerOptionsRejectsUnauthenticatedContentAndExchange(t *testing.T) {
	t.Parallel()
	options, err := AttachedWorkerOptions("sessionless:attached-worker:v1", &ydbstore.Store{}, unusedControlBlobs{})
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandlerWithOptions(discardLogger(), buildinfo.Current("test"), options)
	for _, path := range []string{"/attached-worker/v1/exchange", "/attached-worker/v1/sealed-input", "/attached-worker/v1/output-receipt"} {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json")
			request.Header.Set("Authorization", "Basic denied")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated POST %s: status=%d, want 401; body=%s", path, recorder.Code, recorder.Body.String())
			}
			if recorder.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("%s: Cache-Control=%q, want no-store", path, recorder.Header().Get("Cache-Control"))
			}
		})
	}
}

func TestAttachedWorkerOptionsTelegramRemainsIndependent(t *testing.T) {
	t.Parallel()
	options, err := AttachedWorkerOptions("sessionless:attached-worker:v1", &ydbstore.Store{}, unusedControlBlobs{})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	options.TelegramWebhook = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	})
	handler := NewHandlerWithOptions(discardLogger(), buildinfo.Current("test"), options)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/telegram/webhook", nil))
	if recorder.Code != http.StatusNoContent || calls != 1 {
		t.Fatalf("optional webhook: status=%d calls=%d, want 204 and one call", recorder.Code, calls)
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/attached-worker/v1/attach", nil))
	if recorder.Code != http.StatusUnsupportedMediaType || calls != 1 {
		t.Fatalf("attached route interfered with webhook: status=%d calls=%d", recorder.Code, calls)
	}
}

type unusedControlBlobs struct{}

func (unusedControlBlobs) Put(context.Context, domain.TenantID, string, io.Reader) (domain.BlobRef, error) {
	panic("unauthenticated route must not write an object")
}
func (unusedControlBlobs) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	panic("unauthenticated route must not read an object")
}
func (unusedControlBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	panic("unauthenticated route must not delete an object")
}
