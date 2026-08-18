package webstatic

import (
	"bytes"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

const testFallback = `<!doctype html><html><head><title>Sessionless</title></head><body><div style="display: contents"><script>start()</script></div></body></html>`

func TestFallbackUsesFreshNonceAndNoStore(t *testing.T) {
	handler := newTestHandler(t, fstest.MapFS{
		fallbackDocument: {Data: []byte(testFallback)},
	})
	request := httptest.NewRequest(http.MethodGet, "https://app.example.test/login", nil)
	request.Header.Set("Accept", "text/html,application/xhtml+xml;q=0.9")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	response := recorder.Result()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	if got := response.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	nonce := "AQEBAQEBAQEBAQEBAQEBAQEB"
	csp := response.Header.Get("Content-Security-Policy")
	for _, wanted := range []string{
		"script-src 'self' 'nonce-" + nonce + "'",
		"style-src 'self' 'nonce-" + nonce + "'",
		"connect-src 'self' https://objects.example.test",
	} {
		if !strings.Contains(csp, wanted) {
			t.Errorf("CSP %q does not contain %q", csp, wanted)
		}
	}
	if strings.Contains(csp, "unsafe-inline") {
		t.Fatalf("CSP unexpectedly permits unsafe-inline: %q", csp)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `<script nonce="`+nonce+`">start()</script>`) {
		t.Fatalf("fallback script has no request nonce: %s", body)
	}
	if !strings.Contains(body, `<meta property="csp-nonce" nonce="`+nonce+`">`) {
		t.Fatalf("fallback has no client nonce meta: %s", body)
	}
	if strings.Contains(body, `style=`) {
		t.Fatalf("fallback retained an inline style attribute: %s", body)
	}
}

func TestFallbackHeadHasHeadersWithoutBody(t *testing.T) {
	handler := newTestHandler(t, fstest.MapFS{
		fallbackDocument: {Data: []byte(testFallback)},
	})
	request := httptest.NewRequest(http.MethodHead, "https://app.example.test/sessions/s_1", nil)
	request.Header.Set("Accept", "text/html")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("HEAD body length = %d, want 0", recorder.Body.Len())
	}
	if recorder.Header().Get("Content-Length") == "" {
		t.Fatal("HEAD fallback has no Content-Length")
	}
}

func TestReservedAndNonNavigationRequestsStayWithBackend(t *testing.T) {
	handler := newTestHandler(t, fstest.MapFS{
		fallbackDocument: {Data: []byte(testFallback)},
	})
	tests := []struct {
		name   string
		method string
		path   string
		accept string
		dest   string
	}{
		{name: "api", method: http.MethodGet, path: "/api/web/v1/unknown", accept: "text/html"},
		{name: "auth", method: http.MethodGet, path: "/auth/unknown", accept: "text/html"},
		{name: "health subtree", method: http.MethodGet, path: "/healthz/unknown", accept: "text/html"},
		{name: "readiness", method: http.MethodGet, path: "/readyz", accept: "text/html"},
		{name: "version", method: http.MethodGet, path: "/version", accept: "text/html"},
		{name: "mutation", method: http.MethodPost, path: "/login", accept: "text/html"},
		{name: "JSON client", method: http.MethodGet, path: "/missing", accept: "application/json"},
		{name: "rejected HTML", method: http.MethodGet, path: "/missing", accept: "text/html;q=0"},
		{name: "subresource", method: http.MethodGet, path: "/missing", accept: "text/html", dest: "script"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, "https://app.example.test"+test.path, nil)
			request.Header.Set("Accept", test.accept)
			if test.dest != "" {
				request.Header.Set("Sec-Fetch-Dest", test.dest)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusTeapot || recorder.Body.String() != "backend" {
				t.Fatalf("response = (%d, %q), want backend", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestImmutableAssetUsesPrecompressionAndLongCache(t *testing.T) {
	handler := newTestHandler(t, fstest.MapFS{
		fallbackDocument:                      {Data: []byte(testFallback)},
		"_app/immutable/entry/app.hash.js":    {Data: []byte("plain")},
		"_app/immutable/entry/app.hash.js.br": {Data: []byte("brotli")},
		"_app/immutable/entry/app.hash.js.gz": {Data: []byte("gzip")},
		"_app/version.json":                   {Data: []byte(`{"version":"1"}`)},
	})
	request := httptest.NewRequest(http.MethodGet, "https://app.example.test/_app/immutable/entry/app.hash.js", nil)
	request.Header.Set("Accept-Encoding", "gzip, br")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK || recorder.Body.String() != "brotli" {
		t.Fatalf("response = (%d, %q), want Brotli asset", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("Content-Encoding = %q, want br", got)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Fatalf("Cache-Control = %q, want immutable", got)
	}
	if got := recorder.Header().Get("Vary"); got != "Accept-Encoding" {
		t.Fatalf("Vary = %q, want Accept-Encoding", got)
	}

	identityRequest := httptest.NewRequest(http.MethodGet, "https://app.example.test/_app/immutable/entry/app.hash.js", nil)
	identityRecorder := httptest.NewRecorder()
	handler.ServeHTTP(identityRecorder, identityRequest)
	if identityRecorder.Body.String() != "plain" {
		t.Fatalf("identity body = %q, want plain", identityRecorder.Body.String())
	}
	if got := identityRecorder.Header().Get("Vary"); got != "Accept-Encoding" {
		t.Fatalf("identity Vary = %q, want Accept-Encoding", got)
	}
}

func TestAssetEncodingHonorsQualityAndExplicitRejection(t *testing.T) {
	handler := newTestHandler(t, fstest.MapFS{
		fallbackDocument:                      {Data: []byte(testFallback)},
		"_app/immutable/entry/app.hash.js":    {Data: []byte("plain")},
		"_app/immutable/entry/app.hash.js.br": {Data: []byte("brotli")},
		"_app/immutable/entry/app.hash.js.gz": {Data: []byte("gzip")},
	})

	request := httptest.NewRequest(http.MethodGet, "https://app.example.test/_app/immutable/entry/app.hash.js", nil)
	request.Header.Set("Accept-Encoding", "br;q=0, gzip;q=0.8, *;q=1")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if got := recorder.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}
	if recorder.Body.String() != "gzip" {
		t.Fatalf("body = %q, want gzip", recorder.Body.String())
	}
}

func TestRawHTMLAssetIsNeverServed(t *testing.T) {
	handler := newTestHandler(t, fstest.MapFS{
		fallbackDocument: {Data: []byte(testFallback)},
		"index.html":     {Data: []byte(`<script>unsafe()</script>`)},
	})
	request := httptest.NewRequest(http.MethodGet, "https://app.example.test/index.html", nil)
	request.Header.Set("Accept", "text/html")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK || strings.Contains(recorder.Body.String(), "unsafe()") {
		t.Fatalf("raw HTML was served: (%d, %q)", recorder.Code, recorder.Body.String())
	}
}

func TestNewRejectsUncontrolledInlineStyle(t *testing.T) {
	_, err := NewFromFS(Config{Backend: backendHandler()}, fstest.MapFS{
		fallbackDocument: {Data: []byte(`<html><head></head><body><div style="color:red"></div></body></html>`)},
	})
	if err == nil || !strings.Contains(err.Error(), "inline style") {
		t.Fatalf("error = %v, want inline style rejection", err)
	}
}

func newTestHandler(t *testing.T, assets fs.FS) *Handler {
	t.Helper()
	handler, err := NewFromFS(Config{
		Backend:             backendHandler(),
		ObjectStorageOrigin: "https://objects.example.test",
		Random:              bytes.NewReader(bytes.Repeat([]byte{1}, 18)),
	}, assets)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func backendHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("backend"))
	})
}
