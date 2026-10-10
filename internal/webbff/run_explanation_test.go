package webbff_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

type ratedExplanationFixture struct {
	call func(context.Context, domain.SecretDigest, string, domain.RunID) (ports.RunExplanationReadOutcomeV1, error)
}

type unreadExplanationBody struct{}

func (unreadExplanationBody) Read([]byte) (int, error) { panic("GET body must not be read") }
func (unreadExplanationBody) Close() error             { return nil }

func (fixture ratedExplanationFixture) ReadRatedRunExplanationV1(ctx context.Context, digest domain.SecretDigest, id string, run domain.RunID) (ports.RunExplanationReadOutcomeV1, error) {
	return fixture.call(ctx, digest, id, run)
}

func explanationHandler(t *testing.T, reader ports.RunExplanationRatedReadStoreV1) http.Handler {
	t.Helper()
	handler, err := webbff.New(webbff.Config{BaseURL: "https://web.dev.sessionless.triborg.dev",
		RedirectURI: "https://web.dev.sessionless.triborg.dev" + webcontract.RouteOIDCCallback,
		OIDCPolicy:  domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}},
		Provider:    fakeProvider{subject: "424242"}, Store: newMemoryAuthStore(), IDs: fixedIDs{}, Clock: fixedClock{now: bffTestTime},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), RunExplanations: reader})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func historicalExplanation(t *testing.T) webcontract.RunExplanationV1 {
	t.Helper()
	value, err := webcontract.NewRunExplanationV1(runexplanation.Snapshot{ReadAt: bffTestTime,
		Run: domain.Run{ID: "public-run", TenantID: "private-tenant", SessionID: "public-session", TriggerEventID: "private-trigger",
			SubscriptionConnectionID: "private-connection", Status: domain.RunCreated, IdempotencyKey: "private-key", CreatedAt: bffTestTime, UpdatedAt: bffTestTime}})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func explanationRequest() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev/api/web/v1/runs/public-run/explanation", nil)
	r.AddCookie(&http.Cookie{Name: webcontract.SessionCookieName, Value: "session-private-cookie"})
	return r
}

func TestExplanationHTTPUsesOnlyCookieDigestAndGeneratedNonce(t *testing.T) {
	value := historicalExplanation(t)
	calls := 0
	reader := ratedExplanationFixture{call: func(ctx context.Context, digest domain.SecretDigest, nonce string, run domain.RunID) (ports.RunExplanationReadOutcomeV1, error) {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 3*time.Second || time.Until(deadline) <= 0 {
			t.Fatal("missing bounded outer deadline")
		}
		if digest != domain.DigestSecret("session-private-cookie") || run != "public-run" || !strings.HasPrefix(nonce, "req_") || nonce == "client-private-nonce" {
			t.Fatal("browser authority or missing generated nonce")
		}
		return ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadSuccessV1, Explanation: &value}, nil
	}}
	r := explanationRequest()
	r.Header.Set("X-Request-ID", "client-private-nonce")
	r.Header.Set("Authorization", "Bearer private-bearer")
	r.Header.Set("If-None-Match", "*")
	w := httptest.NewRecorder()
	explanationHandler(t, reader).ServeHTTP(w, r)
	if w.Code != 200 || calls != 1 || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("ETag") != "" || w.Header().Get("Retry-After") != "" || w.Body.Len() > runexplanation.MaxResponseBytes {
		t.Fatalf("status=%d calls=%d headers=%v", w.Code, calls, w.Header())
	}
	for _, private := range []string{"private-tenant", "private-trigger", "private-connection", "private-key", "session-private-cookie", "private-bearer", "client-private-nonce"} {
		if strings.Contains(w.Body.String(), private) {
			t.Fatal("private source escaped HTTP manifest")
		}
	}
	if !strings.Contains(w.Body.String(), `"run_id":"public-run"`) || !strings.Contains(w.Body.String(), `"session_id":"public-session"`) {
		t.Fatal("exact selector correlation missing")
	}
}

func TestExplanationHTTPClosedOutcomesAndSanitizedFailures(t *testing.T) {
	for _, test := range []struct {
		name   string
		result ports.RunExplanationReadOutcomeV1
		err    error
		status int
		retry  string
	}{
		{"not found", ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadNotFoundV1}, nil, 404, ""},
		{"limited", ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadRateLimitedV1, RetryAfter: 5 * time.Second}, nil, 429, "5"},
		{"revoked", ports.RunExplanationReadOutcomeV1{}, domain.ErrWebSessionRevoked, 401, ""},
		{"expired", ports.RunExplanationReadOutcomeV1{}, domain.ErrWebSessionExpired, 401, ""},
		{"permission", ports.RunExplanationReadOutcomeV1{}, domain.ErrMembershipDenied, 403, ""},
		{"version", ports.RunExplanationReadOutcomeV1{}, domain.ErrMembershipVersionChanged, 403, ""},
		{"backend", ports.RunExplanationReadOutcomeV1{}, errors.New("private-provider-cookie-sql"), 503, ""},
		{"unknown outcome", ports.RunExplanationReadOutcomeV1{Kind: "private-outcome"}, nil, 503, ""},
		{"limited zero", ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadRateLimitedV1}, nil, 503, ""},
		{"limited over cap", ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadRateLimitedV1, RetryAfter: 6 * time.Second}, nil, 503, ""},
		{"limited fraction", ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadRateLimitedV1, RetryAfter: time.Millisecond}, nil, 503, ""},
		{"nil success", ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadSuccessV1}, nil, 503, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			reader := ratedExplanationFixture{call: func(context.Context, domain.SecretDigest, string, domain.RunID) (ports.RunExplanationReadOutcomeV1, error) {
				calls++
				return test.result, test.err
			}}
			w := httptest.NewRecorder()
			explanationHandler(t, reader).ServeHTTP(w, explanationRequest())
			if w.Code != test.status || w.Header().Get("Retry-After") != test.retry || calls != 1 || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("ETag") != "" {
				t.Fatalf("status=%d retry=%q calls=%d", w.Code, w.Header().Get("Retry-After"), calls)
			}
			if strings.Contains(w.Body.String(), "private-") {
				t.Fatal("internal failure content escaped")
			}
		})
	}
	wrong := historicalExplanation(t)
	wrong.RunID = "different-run"
	reader := ratedExplanationFixture{call: func(context.Context, domain.SecretDigest, string, domain.RunID) (ports.RunExplanationReadOutcomeV1, error) {
		return ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadSuccessV1, Explanation: &wrong}, nil
	}}
	w := httptest.NewRecorder()
	explanationHandler(t, reader).ServeHTTP(w, explanationRequest())
	if w.Code != 503 {
		t.Fatal("mismatched Run DTO accepted")
	}
}

func TestExplanationHTTPRejectsBrowserAuthorityAndDefaultIsAbsent(t *testing.T) {
	reader := ratedExplanationFixture{call: func(context.Context, domain.SecretDigest, string, domain.RunID) (ports.RunExplanationReadOutcomeV1, error) {
		t.Fatal("invalid/default request reached store")
		return ports.RunExplanationReadOutcomeV1{}, nil
	}}
	for _, url := range []string{"/api/web/v1/runs/public-run/explanation?tenant_id=private", "/api/web/v1/runs/public-run/explanation?", "/api/web/v1/runs/public-run/explanation?%GG"} {
		r := explanationRequest()
		r.URL, _ = r.URL.Parse(url)
		w := httptest.NewRecorder()
		explanationHandler(t, reader).ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatalf("query status=%d", w.Code)
		}
	}
	r := explanationRequest()
	r.Body = io.NopCloser(strings.NewReader(`{"user_id":"private"}`))
	r.ContentLength = 0
	w := httptest.NewRecorder()
	explanationHandler(t, reader).ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal("unannounced GET body accepted")
	}
	r = explanationRequest()
	r.Header.Del("Cookie")
	r.Header.Set("Authorization", "Bearer private")
	w = httptest.NewRecorder()
	explanationHandler(t, reader).ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("Bearer replaced first-party cookie")
	}
	w = httptest.NewRecorder()
	explanationHandler(t, nil).ServeHTTP(w, explanationRequest())
	if w.Code != 404 {
		t.Fatal("default route mounted")
	}
	r = explanationRequest()
	r.Body = unreadExplanationBody{}
	w = httptest.NewRecorder()
	explanationHandler(t, reader).ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal("potentially blocking body accepted")
	}
	r = explanationRequest()
	r.Method = http.MethodHead
	w = httptest.NewRecorder()
	explanationHandler(t, reader).ServeHTTP(w, r)
	if w.Code != 405 || w.Header().Get("Allow") != "GET" {
		t.Fatal("implicit HEAD debited resource")
	}
}
