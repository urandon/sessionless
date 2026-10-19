package webbff_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

const searchConsentHTTPPath = "/api/web/v1/sessions/search-session/runs/search-run/attempts/search-attempt/search-consents/search-request"

type searchConsentHTTPStore struct {
	discover func(context.Context, ports.DiscoverPrivateWebSearchConsentRequestV1) (*ports.WebSearchConsentPendingV1, error)
	view     ports.WebSearchConsentPrivateViewV1
	read     func(context.Context, ports.ReadPrivateWebSearchConsentRequestV1) (ports.WebSearchConsentPrivateViewV1, error)
	decide   func(context.Context, ports.DecideWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error)
}

func (s *searchConsentHTTPStore) DiscoverPrivateWebSearchConsent(ctx context.Context, r ports.DiscoverPrivateWebSearchConsentRequestV1) (*ports.WebSearchConsentPendingV1, error) {
	if s.discover != nil {
		return s.discover(ctx, r)
	}
	return nil, nil
}

func TestSearchConsentHTTPDiscoveryPrivateOpaqueOrAuthenticatedAbsence(t *testing.T) {
	for _, absent := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "none"}[absent], func(t *testing.T) {
			h, store, auth, cookie, _ := searchConsentHTTPFixture(t, true)
			calls := 0
			store.discover = func(ctx context.Context, r ports.DiscoverPrivateWebSearchConsentRequestV1) (*ports.WebSearchConsentPendingV1, error) {
				calls++
				if r.SessionID != "search-session" || r.RunID != "search-run" || r.WebSessionDigest != domain.DigestSecret(cookie.Value) {
					t.Fatal("discovery did not preserve cookie-only scope")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("unbounded discovery")
				}
				if absent {
					return nil, nil
				}
				rq := store.view.State.Request
				return &ports.WebSearchConsentPendingV1{Locator: ports.WebSearchConsentLocatorV1{TenantID: rq.Scope.TenantID, SessionID: rq.Scope.SessionID, RunID: rq.Scope.RunID, AttemptID: rq.Scope.AttemptID, RequestID: rq.ID}, ExpiresAt: rq.ExpiresAt}, nil
			}
			r := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev/api/web/v1/sessions/search-session/runs/search-run/search-consents/current", nil)
			r.AddCookie(cookie)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 200 || calls != 1 || auth.authorizations != 0 || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("ETag") != "" || w.Body.Len() > 2048 {
				t.Fatalf("discovery status=%d calls=%d authrefresh=%d", w.Code, calls, auth.authorizations)
			}
			var value webcontract.SearchConsentDiscoveryV1
			if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil || value.SessionID != "search-session" || value.RunID != "search-run" || (value.Pending == nil) != absent {
				t.Fatalf("bad opaque response err=%v", err)
			}
			for _, private := range []string{store.view.Query, "ten_alpha", "private-claim", store.view.State.Request.ArgumentsDigest, "query", "receipt"} {
				if strings.Contains(w.Body.String(), private) {
					t.Fatal("discovery leaked private authority/query")
				}
			}
		})
	}
}

func TestSearchConsentHTTPDiscoveryRejectsMalformedAndUnauthorizedRequests(t *testing.T) {
	for _, name := range []string{"head", "query", "empty-query", "body", "bearer-only", "foreign-locator", "cancelled", "disabled"} {
		t.Run(name, func(t *testing.T) {
			h, store, auth, cookie, _ := searchConsentHTTPFixture(t, name != "disabled")
			calls := 0
			store.discover = func(context.Context, ports.DiscoverPrivateWebSearchConsentRequestV1) (*ports.WebSearchConsentPendingV1, error) {
				calls++
				return &ports.WebSearchConsentPendingV1{Locator: ports.WebSearchConsentLocatorV1{TenantID: "ten_alpha", SessionID: "foreign-session", RunID: "search-run", AttemptID: "attempt", RequestID: "request"}, ExpiresAt: bffTestTime}, nil
			}
			r := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev/api/web/v1/sessions/search-session/runs/search-run/search-consents/current", nil)
			r.AddCookie(cookie)
			want := 400
			switch name {
			case "head":
				r.Method = http.MethodHead
				want = 405
			case "query":
				r.URL.RawQuery = "actor=other"
			case "empty-query":
				r.URL.ForceQuery = true
			case "body":
				r.Body = unreadExplanationBody{}
				r.ContentLength = -1
			case "bearer-only":
				r.Header.Del("Cookie")
				r.Header.Set("Authorization", "Bearer ignored")
				want = 401
			case "foreign-locator":
				want = 503
			case "cancelled":
				ctx, cancel := context.WithCancel(r.Context())
				cancel()
				r = r.WithContext(ctx)
				want = 503
			case "disabled":
				want = 404
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != want || auth.authorizations != 0 || name != "foreign-locator" && calls != 0 || strings.Contains(w.Body.String(), "foreign-session") {
				t.Fatalf("discovery status=%d want=%d canonical_calls=%d activity_refreshes=%d", w.Code, want, calls, auth.authorizations)
			}
		})
	}
}

func (s *searchConsentHTTPStore) ReadPrivateWebSearchConsent(ctx context.Context, r ports.ReadPrivateWebSearchConsentRequestV1) (ports.WebSearchConsentPrivateViewV1, error) {
	if s.read != nil {
		return s.read(ctx, r)
	}
	return s.view, nil
}

func (s *searchConsentHTTPStore) DecideWebSearchConsent(ctx context.Context, r ports.DecideWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error) {
	if s.decide != nil {
		return s.decide(ctx, r)
	}
	return domain.WebSearchConsentStateV1{}, domain.ErrWebSearchConsentConflict
}

type searchConsentHTTPAuth struct {
	*memoryAuthStore
	authorizations int
}

func (s *searchConsentHTTPAuth) AuthorizeWebSession(ctx context.Context, d domain.SecretDigest, p domain.TenantPermission, at time.Time) (ports.WebAuthorization, error) {
	s.authorizations++
	return s.memoryAuthStore.AuthorizeWebSession(ctx, d, p, at)
}

func searchConsentHTTPFixture(t *testing.T, enabled bool) (http.Handler, *searchConsentHTTPStore, *searchConsentHTTPAuth, *http.Cookie, *http.Cookie) {
	t.Helper()
	auth := &searchConsentHTTPAuth{memoryAuthStore: newMemoryAuthStore()}
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}
	user := domain.UserID("usr_known_user")
	auth.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: user, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	auth.memberships[user] = []domain.TenantMembership{membership("ten_alpha", user, domain.TenantMembershipOwner)}
	query := " exact <b> public query </b> "
	body := []byte(`{"query":" exact <b> public query </b> ","max_results":5}`)
	d := strings.Repeat("a", 64)
	scope := domain.WebSearchConsentScopeV1{TenantID: "ten_alpha", SessionID: "search-session", OwnerUserID: "private-resource-owner", RunID: "search-run", AttemptID: "search-attempt", LeaseID: "private-lease", FenceToken: 9, PhysicalInvocationClaimID: "private-claim", EffectReservationDigest: d, InvocationAuthorityDigest: d, HarnessBindingDigest: d, PolicyDigest: d}
	r := domain.WebSearchConsentRequestV1{Version: 1, ID: "search-request", Scope: scope, InitiatingUserID: user, ModelResponseDigest: d, CallID: "private-call", ArgumentsDigest: string(domain.DigestSecret(string(body))), QueryDigest: string(domain.DigestSecret(query)), GrantDigest: d, ConsentRevision: 1, RevocationGeneration: 1,
		Arguments: domain.BlobRef{TenantID: scope.TenantID, Key: domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempts/" + string(scope.AttemptID) + "/consent/private-arguments.json", Size: int64(len(body)), SHA256: string(domain.DigestSecret(string(body)))}, CreatedAt: bffTestTime, ExpiresAt: bffTestTime.Add(time.Minute)}
	if err := r.ValidateArguments(body); err != nil {
		t.Fatalf("invalid private-query fixture: %v", err)
	}
	store := &searchConsentHTTPStore{view: ports.WebSearchConsentPrivateViewV1{State: domain.WebSearchConsentStateV1{Request: r, Revision: 1, Status: domain.WebSearchConsentPendingV1, UpdatedAt: bffTestTime}, Query: query}}
	config := webbff.Config{BaseURL: "https://web.dev.sessionless.triborg.dev", RedirectURI: "https://web.dev.sessionless.triborg.dev" + webcontract.RouteOIDCCallback,
		OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}, Provider: fakeProvider{subject: subject.Subject}, Store: auth, IDs: fixedIDs{}, Clock: fixedClock{now: bffTestTime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if enabled {
		config.SearchConsents = store
	}
	h, err := webbff.New(config)
	if err != nil {
		t.Fatal(err)
	}
	session, csrf := performLogin(t, h, "/sessions")
	auth.authorizations = 0
	return h, store, auth, session, csrf
}

func searchConsentReadRequest(cookie *http.Cookie) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+searchConsentHTTPPath, nil)
	r.AddCookie(cookie)
	return r
}

func TestSearchConsentHTTPPrivateReadUsesCanonicalActorWithoutRefreshingAuth(t *testing.T) {
	h, store, auth, cookie, _ := searchConsentHTTPFixture(t, true)
	calls := 0
	store.read = func(ctx context.Context, r ports.ReadPrivateWebSearchConsentRequestV1) (ports.WebSearchConsentPrivateViewV1, error) {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 10*time.Second || ctx.Err() != nil {
			t.Fatal("private read lacks original bounded context")
		}
		if r.WebSessionDigest != domain.DigestSecret(cookie.Value) || r.SessionID != "search-session" || r.RunID != "search-run" || r.AttemptID != "search-attempt" || r.RequestID != "search-request" {
			t.Fatal("browser selectors or cookie digest changed before canonical owner")
		}
		return store.view, nil
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, searchConsentReadRequest(cookie))
	if w.Code != 200 || calls != 1 || auth.authorizations != 0 || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("ETag") != "" || w.Body.Len() > 8<<10 {
		t.Fatalf("status=%d canonical_reads=%d activity_refreshes=%d", w.Code, calls, auth.authorizations)
	}
	var view struct {
		RequestID string `json:"request_id"`
		Revision  uint64 `json:"revision"`
		Status    string `json:"status"`
		Query     string `json:"query"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil || view.Query != store.view.Query || view.RequestID != "search-request" || view.Revision != 1 || view.Status != "pending" {
		t.Fatalf("private view is not the exact original pending query: err=%v", err)
	}
	for _, private := range []string{"ten_alpha", "usr_known_user", "private-resource-owner", "private-lease", "private-claim", "private-call", "private-arguments", cookie.Value, store.view.State.Request.ArgumentsDigest} {
		if strings.Contains(w.Body.String(), private) {
			t.Fatal("private authority metadata escaped the exact-query view")
		}
	}
}

func TestSearchConsentHTTPReadRejectsAuthorityBodiesAndForeignView(t *testing.T) {
	for _, tc := range []string{"query", "empty-query", "body", "no-cookie", "foreign-view", "cancelled"} {
		t.Run(tc, func(t *testing.T) {
			h, store, _, cookie, _ := searchConsentHTTPFixture(t, true)
			calls := 0
			store.read = func(ctx context.Context, _ ports.ReadPrivateWebSearchConsentRequestV1) (ports.WebSearchConsentPrivateViewV1, error) {
				calls++
				if tc == "foreign-view" {
					v := store.view
					v.State.Request.Scope.SessionID = "foreign-session"
					return v, nil
				}
				return store.view, nil
			}
			r := searchConsentReadRequest(cookie)
			want := 400
			switch tc {
			case "query":
				r.URL.RawQuery = "tenant_id=foreign"
			case "empty-query":
				r.URL.ForceQuery = true
			case "body":
				r.Body = unreadExplanationBody{}
				r.ContentLength = -1
			case "no-cookie":
				r.Header.Del("Cookie")
				r.Header.Set("Authorization", "Bearer private")
				want = 401
			case "foreign-view":
				want = 503
			case "cancelled":
				ctx, cancel := context.WithCancel(r.Context())
				cancel()
				r = r.WithContext(ctx)
				want = 503
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != want || strings.Contains(w.Body.String(), "exact") || tc != "foreign-view" && calls != 0 {
				t.Fatalf("status=%d want=%d canonical_reads=%d", w.Code, want, calls)
			}
		})
	}
	h, _, _, cookie, _ := searchConsentHTTPFixture(t, false)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, searchConsentReadRequest(cookie))
	if w.Code != 404 {
		t.Fatal("default-off private consent route was mounted")
	}
}

func TestSearchConsentHTTPDecisionUsesExactCSRFAndClosedCASInput(t *testing.T) {
	for _, tc := range []string{"valid", "csrf", "origin", "extra", "duplicate", "case-alias", "revision", "choice", "query", "content-type", "oversized", "trailing"} {
		t.Run(tc, func(t *testing.T) {
			h, store, _, cookie, csrf := searchConsentHTTPFixture(t, true)
			calls := 0
			store.decide = func(ctx context.Context, r ports.DecideWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error) {
				calls++
				if ctx.Err() != nil || r.Validate() != nil || r.WebSessionDigest != domain.DigestSecret(cookie.Value) || r.CSRFTokenDigest != domain.DigestSecret(csrf.Value) || r.Locator.TenantID != "ten_alpha" || r.Locator.SessionID != "search-session" || r.Locator.RunID != "search-run" || r.Locator.AttemptID != "search-attempt" || r.Locator.RequestID != "search-request" || r.ExpectedRevision != 1 || r.Choice != domain.WebSearchConsentApproveV1 {
					t.Fatal("mutation replaced canonical actor, original locator, CSRF or CAS")
				}
				return domain.WebSearchConsentStateV1{}, domain.ErrWebSearchConsentConflict
			}
			body := `{"expected_revision":1,"choice":"approve"}`
			want := 400
			switch tc {
			case "valid":
				want = 409
			case "csrf", "origin":
				want = 403
			case "extra":
				body = `{"expected_revision":1,"choice":"approve","query":"replacement"}`
			case "duplicate":
				body = `{"expected_revision":1,"choice":"deny","choice":"approve"}`
			case "case-alias":
				body = `{"expected_revision":1,"Choice":"approve"}`
			case "revision":
				body = `{"expected_revision":2,"choice":"approve"}`
			case "choice":
				body = `{"expected_revision":1,"choice":"resume"}`
			case "oversized":
				body = strings.Repeat(" ", 513)
				want = 413
			case "trailing":
				body += `{}`
			}
			r := attachedWorkerMutationRequest(http.MethodPost, searchConsentHTTPPath, body, cookie, csrf, csrf.Value)
			if tc == "csrf" {
				r.Header.Set(webcontract.CSRFHeaderName, "wrong")
			}
			if tc == "origin" {
				r.Header.Set("Origin", "https://foreign.invalid")
			}
			if tc == "query" {
				r.URL.RawQuery = "user_id=foreign"
			}
			if tc == "content-type" {
				r.Header.Set("Content-Type", "text/plain")
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != want || tc == "valid" && calls != 1 || tc != "valid" && calls != 0 {
				t.Fatalf("status=%d want=%d canonical_decisions=%d", w.Code, want, calls)
			}
		})
	}
}

func TestSearchConsentHTTPDecisionReturnsOnlyConfirmedSummary(t *testing.T) {
	for _, choice := range []domain.WebSearchConsentChoiceV1{domain.WebSearchConsentApproveV1, domain.WebSearchConsentDenyV1} {
		t.Run(string(choice), func(t *testing.T) {
			h, store, _, cookie, csrf := searchConsentHTTPFixture(t, true)
			store.decide = func(_ context.Context, input ports.DecideWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error) {
				state := store.view.State
				digest, err := state.Request.Digest()
				if err != nil {
					t.Fatal(err)
				}
				state.Decision = &domain.WebSearchConsentDecisionV1{Version: 1, Request: state.Request, RequestDigest: digest, RequestRevision: 1, ActorUserID: state.Request.InitiatingUserID, WebSessionDigest: input.WebSessionDigest, MembershipSecurityVersion: 1, Choice: input.Choice, ObservedAt: bffTestTime.Add(time.Microsecond)}
				state.Revision, state.UpdatedAt, state.Status = 2, state.Decision.ObservedAt, domain.WebSearchConsentApprovedV1
				if input.Choice == domain.WebSearchConsentDenyV1 {
					state.Status = domain.WebSearchConsentDeniedV1
				}
				if err := state.Validate(); err != nil {
					t.Fatalf("invalid immutable response fixture: %v", err)
				}
				return state, nil
			}
			r := attachedWorkerMutationRequest(http.MethodPost, searchConsentHTTPPath, `{"expected_revision":1,"choice":"`+string(choice)+`"}`, cookie, csrf, csrf.Value)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("confirmed decision status=%d", w.Code)
			}
			var summary webcontract.SearchConsentSummaryV1
			if err := json.Unmarshal(w.Body.Bytes(), &summary); err != nil || summary.RequestID != "search-request" || summary.Revision != 2 {
				t.Fatal("immutable summary correlation missing")
			}
			for _, private := range []string{"query", "private-", "web_session_digest", "request_digest", cookie.Value, "tenant_id"} {
				if strings.Contains(w.Body.String(), private) {
					t.Fatal("POST leaked plaintext query or authority receipt")
				}
			}
		})
	}
}

func TestSearchConsentHTTPReadRejectsHeadAndSanitizesErrors(t *testing.T) {
	h, store, auth, cookie, _ := searchConsentHTTPFixture(t, true)
	calls := 0
	store.read = func(_ context.Context, _ ports.ReadPrivateWebSearchConsentRequestV1) (ports.WebSearchConsentPrivateViewV1, error) {
		calls++
		return ports.WebSearchConsentPrivateViewV1{}, io.ErrUnexpectedEOF
	}
	r := searchConsentReadRequest(cookie)
	r.Method = http.MethodHead
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 405 || calls != 0 || w.Header().Get("Allow") != "GET" {
		t.Fatal("HEAD materialized a private query")
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, searchConsentReadRequest(cookie))
	if w.Code != 503 || calls != 1 || auth.authorizations != 0 || strings.Contains(w.Body.String(), "EOF") {
		t.Fatal("private read failure escaped or refreshed browser activity")
	}
}
