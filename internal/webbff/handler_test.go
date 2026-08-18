package webbff_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/buildinfo"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

var bffTestTime = time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)

func TestLoginTenantSwitchCSRFAndLogout(t *testing.T) {
	store := newMemoryAuthStore()
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}
	userID := domain.UserID("usr_known_user")
	store.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: userID, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	store.memberships[userID] = []domain.TenantMembership{
		membership("ten_alpha", userID, domain.TenantMembershipOwner),
		membership("ten_beta", userID, domain.TenantMembershipMember),
	}
	handler := newTestHandler(t, store, subject.Subject)
	sessionCookie, csrfCookie := performLogin(t, handler, "/sessions")
	if sessionCookie.Name != webcontract.SessionCookieName || !sessionCookie.Secure || !sessionCookie.HttpOnly || sessionCookie.Domain != "" || sessionCookie.Path != "/" {
		t.Fatalf("session cookie = %+v", sessionCookie)
	}
	if csrfCookie.Name != webcontract.CSRFCookieName || !csrfCookie.Secure || csrfCookie.HttpOnly || csrfCookie.SameSite != http.SameSiteStrictMode {
		t.Fatalf("CSRF cookie = %+v", csrfCookie)
	}

	me := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+webcontract.RouteMe, nil)
	me.AddCookie(sessionCookie)
	meResponse := httptest.NewRecorder()
	handler.ServeHTTP(meResponse, me)
	if meResponse.Code != http.StatusOK {
		t.Fatalf("me status = %d body=%s", meResponse.Code, meResponse.Body.String())
	}
	var identity webcontract.Identity
	if err := json.Unmarshal(meResponse.Body.Bytes(), &identity); err != nil {
		t.Fatal(err)
	}
	if identity.UserID != userID || len(identity.Tenants) != 2 || !identity.Tenants[0].Active {
		t.Fatalf("identity = %+v", identity)
	}

	badSwitch := tenantSwitchRequest(sessionCookie, csrfCookie, "ten_beta", "wrong-token")
	badResponse := httptest.NewRecorder()
	handler.ServeHTTP(badResponse, badSwitch)
	if badResponse.Code != http.StatusForbidden {
		t.Fatalf("bad CSRF status = %d", badResponse.Code)
	}
	securityEvents := store.recordedSecurityEvents()
	if len(securityEvents) != 1 || securityEvents[0].Action != domain.WebSecurityCSRFRejected ||
		securityEvents[0].TenantID != "ten_alpha" || securityEvents[0].UserID != userID ||
		securityEvents[0].MembershipSecurityVersion != 1 || securityEvents[0].ReasonCode != "cookie_header_mismatch" {
		t.Fatalf("CSRF audit events = %+v", securityEvents)
	}

	switchRequest := tenantSwitchRequest(sessionCookie, csrfCookie, "ten_beta", csrfCookie.Value)
	switchResponse := httptest.NewRecorder()
	handler.ServeHTTP(switchResponse, switchRequest)
	if switchResponse.Code != http.StatusOK {
		t.Fatalf("switch status = %d body=%s", switchResponse.Code, switchResponse.Body.String())
	}
	nextSession := responseCookie(t, switchResponse.Result(), webcontract.SessionCookieName)
	nextCSRF := responseCookie(t, switchResponse.Result(), webcontract.CSRFCookieName)
	if nextSession.Value == sessionCookie.Value || nextCSRF.Value == csrfCookie.Value {
		t.Fatal("tenant switch did not rotate both opaque secrets")
	}

	oldMe := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+webcontract.RouteMe, nil)
	oldMe.AddCookie(sessionCookie)
	oldResponse := httptest.NewRecorder()
	handler.ServeHTTP(oldResponse, oldMe)
	if oldResponse.Code != http.StatusUnauthorized {
		t.Fatalf("old session status = %d", oldResponse.Code)
	}

	logout := httptest.NewRequest(http.MethodPost, "https://web.dev.sessionless.triborg.dev"+webcontract.RouteLogout, nil)
	logout.Header.Set("Origin", "https://web.dev.sessionless.triborg.dev")
	logout.Header.Set(webcontract.CSRFHeaderName, nextCSRF.Value)
	logout.AddCookie(nextSession)
	logout.AddCookie(nextCSRF)
	logoutResponse := httptest.NewRecorder()
	handler.ServeHTTP(logoutResponse, logout)
	if logoutResponse.Code != http.StatusNoContent {
		t.Fatalf("logout status = %d body=%s", logoutResponse.Code, logoutResponse.Body.String())
	}
}

func TestUnknownTelegramIdentityGetsDeterministicRecoveryWithoutSession(t *testing.T) {
	store := newMemoryAuthStore()
	handler := newTestHandler(t, store, "999999")
	start := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+webcontract.RouteOIDCStart, nil)
	startResponse := httptest.NewRecorder()
	handler.ServeHTTP(startResponse, start)
	location, _ := url.Parse(startResponse.Header().Get("Location"))
	binding := responseCookie(t, startResponse.Result(), webbff.LoginBindingCookieName)
	callback := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteOIDCCallback+"?code=fixture&state="+url.QueryEscape(location.Query().Get("state")), nil)
	callback.AddCookie(binding)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, callback)
	if response.Code != http.StatusForbidden {
		t.Fatalf("unknown identity status = %d body=%s", response.Code, response.Body.String())
	}
	var failure webcontract.ErrorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Error.Code != webcontract.ErrorAccessDenied || failure.Error.Message != "No Sessionless tenant is linked to this Telegram account." {
		t.Fatalf("failure = %+v", failure)
	}
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == webcontract.SessionCookieName && cookie.Value != "" {
			t.Fatal("unknown identity received a web session")
		}
	}
	securityEvents := store.recordedSecurityEvents()
	if len(securityEvents) != 1 || securityEvents[0].Action != domain.WebSecurityLoginFailed ||
		securityEvents[0].ReasonCode != "membership_missing" || securityEvents[0].UserID == "" ||
		securityEvents[0].SubjectFingerprint == "" {
		t.Fatalf("login failure audit events = %+v", securityEvents)
	}
}

func TestCSRFRejectionFailsClosedWhenAuditCannotPersist(t *testing.T) {
	store := newMemoryAuthStore()
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}
	userID := domain.UserID("usr_known_user")
	store.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: userID, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	store.memberships[userID] = []domain.TenantMembership{membership("ten_alpha", userID, domain.TenantMembershipOwner)}
	handler := newTestHandler(t, store, subject.Subject)
	sessionCookie, csrfCookie := performLogin(t, handler, "/sessions")
	store.securityEventErr = errors.New("audit unavailable")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, tenantSwitchRequest(sessionCookie, csrfCookie, "ten_alpha", "wrong-token"))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("audit failure status = %d body=%s", response.Code, response.Body.String())
	}
}

func TestRequestLogsExcludeQueryAndAuthenticationSecrets(t *testing.T) {
	store := newMemoryAuthStore()
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}
	userID := domain.UserID("usr_known_user")
	store.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: userID, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	store.memberships[userID] = []domain.TenantMembership{membership("ten_alpha", userID, domain.TenantMembershipOwner)}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	handler := newTestHandlerWithLogger(t, store, subject.Subject, logger)

	start := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteOIDCStart+"?return_to=%2Fsessions&marker=query-secret", nil)
	startResponse := httptest.NewRecorder()
	handler.ServeHTTP(startResponse, start)
	location, _ := url.Parse(startResponse.Header().Get("Location"))
	state := location.Query().Get("state")
	binding := responseCookie(t, startResponse.Result(), webbff.LoginBindingCookieName)
	callback := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteOIDCCallback+"?code=authorization-code-secret&state="+url.QueryEscape(state), nil)
	callback.AddCookie(binding)
	handler.ServeHTTP(httptest.NewRecorder(), callback)

	for _, secret := range []string{"query-secret", "authorization-code-secret", state, binding.Value} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("request log contains authentication material %q: %s", secret, logs.String())
		}
	}
}

func TestSessionRoutesUseWebAuthorizationCSRFAndSafeSelectors(t *testing.T) {
	auth := newMemoryAuthStore()
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}
	userID := domain.UserID("usr_known_user")
	auth.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: userID, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	auth.memberships[userID] = []domain.TenantMembership{membership("ten_alpha", userID, domain.TenantMembershipOwner)}
	sessions := &memorySessionAPIStore{records: []ports.SessionRecord{{
		Session: domain.Session{
			ID: "session-existing", TenantID: "ten_alpha", CreatedBy: userID,
			Status: domain.SessionActive, CreatedAt: bffTestTime, UpdatedAt: bffTestTime,
		},
		Display: domain.SessionDisplay{
			TenantID: "ten_alpha", SessionID: "session-existing", Title: "Existing",
			Preview: "Bounded preview", UpdatedAt: bffTestTime,
		},
	}}}
	handler := newSessionTestHandler(t, auth, subject.Subject, sessions)
	sessionCookie, csrfCookie := performLogin(t, handler, "/sessions")

	list := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteSessions+"?status=active&limit=1", nil)
	list.AddCookie(sessionCookie)
	listResponse := httptest.NewRecorder()
	handler.ServeHTTP(listResponse, list)
	if listResponse.Code != http.StatusOK || !strings.Contains(listResponse.Body.String(), `"title":"Existing"`) {
		t.Fatalf("session list status=%d body=%s", listResponse.Code, listResponse.Body.String())
	}
	if listResponse.Header().Get("ETag") == "" {
		t.Fatal("session list omitted ETag")
	}
	unchanged := list.Clone(list.Context())
	unchanged.Header.Set("If-None-Match", listResponse.Header().Get("ETag"))
	unchangedResponse := httptest.NewRecorder()
	handler.ServeHTTP(unchangedResponse, unchanged)
	if unchangedResponse.Code != http.StatusNotModified || unchangedResponse.Body.Len() != 0 {
		t.Fatalf("conditional list status=%d body=%s", unchangedResponse.Code, unchangedResponse.Body.String())
	}

	missing := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev/api/web/v1/sessions/session-missing", nil)
	missing.AddCookie(sessionCookie)
	missingResponse := httptest.NewRecorder()
	handler.ServeHTTP(missingResponse, missing)
	if missingResponse.Code != http.StatusNotFound {
		t.Fatalf("missing selector status=%d body=%s", missingResponse.Code, missingResponse.Body.String())
	}

	withoutCSRF := httptest.NewRequest(http.MethodPost,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteSessions,
		strings.NewReader(`{"idempotency_key":"create-1"}`))
	withoutCSRF.Header.Set("Content-Type", "application/json")
	withoutCSRF.AddCookie(sessionCookie)
	withoutCSRFResponse := httptest.NewRecorder()
	handler.ServeHTTP(withoutCSRFResponse, withoutCSRF)
	if withoutCSRFResponse.Code != http.StatusForbidden {
		t.Fatalf("create without CSRF status=%d", withoutCSRFResponse.Code)
	}

	create := httptest.NewRequest(http.MethodPost,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteSessions,
		strings.NewReader(`{"idempotency_key":"create-1"}`))
	create.Header.Set("Content-Type", "application/json")
	create.Header.Set("Origin", "https://web.dev.sessionless.triborg.dev")
	create.Header.Set(webcontract.CSRFHeaderName, csrfCookie.Value)
	create.AddCookie(sessionCookie)
	create.AddCookie(csrfCookie)
	createResponse := httptest.NewRecorder()
	handler.ServeHTTP(createResponse, create)
	if createResponse.Code != http.StatusCreated {
		t.Fatalf("authorized create status=%d body=%s", createResponse.Code, createResponse.Body.String())
	}

	// Browser callers cannot create arbitrary frontend bindings; Web bindings
	// are server-owned by the message submission path.
	bind := httptest.NewRequest(http.MethodPost,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteFrontendBindings,
		strings.NewReader(`{"frontend":"web","external_conversation_id":"browser-1","session_id":"session-existing","expected_revision":1}`))
	bind.Header.Set("Content-Type", "application/json")
	bind.Header.Set("Origin", "https://web.dev.sessionless.triborg.dev")
	bind.Header.Set(webcontract.CSRFHeaderName, csrfCookie.Value)
	bind.AddCookie(sessionCookie)
	bind.AddCookie(csrfCookie)
	bindResponse := httptest.NewRecorder()
	handler.ServeHTTP(bindResponse, bind)
	if bindResponse.Code != http.StatusNotFound {
		t.Fatalf("browser binding route status=%d body=%s", bindResponse.Code, bindResponse.Body.String())
	}
}

func newTestHandler(t *testing.T, store *memoryAuthStore, subject string) http.Handler {
	t.Helper()
	return newTestHandlerWithLogger(t, store, subject, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newTestHandlerWithLogger(t *testing.T, store *memoryAuthStore, subject string, logger *slog.Logger) http.Handler {
	t.Helper()
	handler, err := webbff.New(webbff.Config{
		BaseURL:     "https://web.dev.sessionless.triborg.dev",
		RedirectURI: "https://web.dev.sessionless.triborg.dev" + webcontract.RouteOIDCCallback,
		OIDCPolicy: domain.OIDCVerificationPolicy{
			Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"},
		},
		Provider: fakeProvider{subject: subject}, Store: store, IDs: fixedIDs{},
		Clock: fixedClock{now: bffTestTime}, Logger: logger,
		Build: buildinfo.Current("web-bff-test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func newSessionTestHandler(t *testing.T, auth *memoryAuthStore, subject string, store ports.SessionAPIStore) http.Handler {
	t.Helper()
	sessions, err := sessionapi.New(sessionapi.Config{
		CursorKey: bytes.Repeat([]byte("c"), 32), IDKey: bytes.Repeat([]byte("i"), 32),
	}, store, memorySessionBlobs{}, fixedClock{now: bffTestTime})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := webbff.New(webbff.Config{
		BaseURL:     "https://web.dev.sessionless.triborg.dev",
		RedirectURI: "https://web.dev.sessionless.triborg.dev" + webcontract.RouteOIDCCallback,
		OIDCPolicy: domain.OIDCVerificationPolicy{
			Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"},
		},
		Provider: fakeProvider{subject: subject}, Store: auth, Sessions: sessions, IDs: fixedIDs{},
		Clock: fixedClock{now: bffTestTime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Build: buildinfo.Current("web-bff-session-test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func performLogin(t *testing.T, handler http.Handler, returnTo string) (*http.Cookie, *http.Cookie) {
	t.Helper()
	start := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteOIDCStart+"?return_to="+url.QueryEscape(returnTo), nil)
	startResponse := httptest.NewRecorder()
	handler.ServeHTTP(startResponse, start)
	if startResponse.Code != http.StatusSeeOther {
		t.Fatalf("start status = %d body=%s", startResponse.Code, startResponse.Body.String())
	}
	location, _ := url.Parse(startResponse.Header().Get("Location"))
	binding := responseCookie(t, startResponse.Result(), webbff.LoginBindingCookieName)
	callback := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteOIDCCallback+"?code=fixture&state="+url.QueryEscape(location.Query().Get("state")), nil)
	callback.AddCookie(binding)
	callbackResponse := httptest.NewRecorder()
	handler.ServeHTTP(callbackResponse, callback)
	if callbackResponse.Code != http.StatusSeeOther || callbackResponse.Header().Get("Location") != returnTo {
		t.Fatalf("callback status=%d location=%q body=%s", callbackResponse.Code, callbackResponse.Header().Get("Location"), callbackResponse.Body.String())
	}
	return responseCookie(t, callbackResponse.Result(), webcontract.SessionCookieName),
		responseCookie(t, callbackResponse.Result(), webcontract.CSRFCookieName)
}

func tenantSwitchRequest(session, csrf *http.Cookie, tenantID, token string) *http.Request {
	request := httptest.NewRequest(http.MethodPost,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteActiveTenant,
		strings.NewReader(`{"tenant_id":"`+tenantID+`"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Origin", "https://web.dev.sessionless.triborg.dev")
	request.Header.Set(webcontract.CSRFHeaderName, token)
	request.AddCookie(session)
	request.AddCookie(csrf)
	return request
}

func responseCookie(t *testing.T, response *http.Response, name string) *http.Cookie {
	t.Helper()
	for _, cookie := range response.Cookies() {
		if cookie.Name == name {
			return cookie
		}
	}
	t.Fatalf("response cookie %q is missing", name)
	return nil
}

type fakeProvider struct{ subject string }

func (provider fakeProvider) AuthorizationURL(_ context.Context, request ports.OIDCAuthorizationRequest) (string, error) {
	return "https://oauth.telegram.org/auth?state=" + url.QueryEscape(request.State), nil
}

func (provider fakeProvider) ExchangeAndVerify(_ context.Context, request ports.OIDCTokenRequest) (domain.OIDCIdentityClaims, error) {
	return domain.OIDCIdentityClaims{
		Issuer: request.Policy.Issuer, Audience: []string{request.Policy.Audience}, Subject: provider.subject,
		Nonce: request.ExpectedNonce, IssuedAt: request.Now, ExpiresAt: request.Now.Add(time.Hour),
	}, nil
}

type fixedIDs struct{}

func (fixedIDs) NewID(_ context.Context, kind ports.IDKind) (string, error) {
	if kind != ports.IDUser {
		return "", errors.New("unexpected ID kind")
	}
	return "usr_candidate_user", nil
}

type fixedClock struct{ now time.Time }

func (clock fixedClock) Now() time.Time { return clock.now }

type memoryAuthStore struct {
	mu               sync.Mutex
	challenges       map[domain.SecretDigest]domain.OIDCLoginChallenge
	identities       map[domain.ExternalSubject]domain.ExternalIdentity
	memberships      map[domain.UserID][]domain.TenantMembership
	sessions         map[domain.SecretDigest]domain.WebSession
	securityEvents   []domain.WebSecurityAuditEvent
	securityEventErr error
}

func newMemoryAuthStore() *memoryAuthStore {
	return &memoryAuthStore{
		challenges:  make(map[domain.SecretDigest]domain.OIDCLoginChallenge),
		identities:  make(map[domain.ExternalSubject]domain.ExternalIdentity),
		memberships: make(map[domain.UserID][]domain.TenantMembership),
		sessions:    make(map[domain.SecretDigest]domain.WebSession),
	}
}

func (store *memoryAuthStore) RecordWebSecurityEvent(_ context.Context, event domain.WebSecurityAuditEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.securityEventErr != nil {
		return store.securityEventErr
	}
	store.securityEvents = append(store.securityEvents, event)
	return nil
}

func (store *memoryAuthStore) recordedSecurityEvents() []domain.WebSecurityAuditEvent {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]domain.WebSecurityAuditEvent(nil), store.securityEvents...)
}

func (store *memoryAuthStore) CreateLoginChallenge(_ context.Context, challenge domain.OIDCLoginChallenge) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.challenges[challenge.StateDigest] = challenge
	return nil
}

func (store *memoryAuthStore) ConsumeLoginChallenge(_ context.Context, digest domain.SecretDigest, binding string, at time.Time) (domain.OIDCLoginChallenge, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	challenge, found := store.challenges[digest]
	if !found {
		return challenge, domain.ErrLoginChallengeExpired
	}
	if err := challenge.Consume(binding, at); err != nil {
		return challenge, err
	}
	store.challenges[digest] = challenge
	return challenge, nil
}

func (store *memoryAuthStore) ResolveOrCreateExternalIdentity(_ context.Context, subject domain.ExternalSubject, candidate domain.UserID, at time.Time) (domain.ExternalIdentity, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if identity, found := store.identities[subject]; found {
		return identity, false, nil
	}
	identity := domain.ExternalIdentity{Subject: subject, UserID: candidate, CreatedAt: at, UpdatedAt: at}
	store.identities[subject] = identity
	return identity, true, nil
}

func (store *memoryAuthStore) ListTenantMemberships(_ context.Context, userID domain.UserID, _ uint64) ([]domain.TenantMembership, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]domain.TenantMembership(nil), store.memberships[userID]...), nil
}

func (store *memoryAuthStore) Enroll(context.Context, ports.EnrollmentRequest) (domain.TenantMembership, error) {
	return domain.TenantMembership{}, errors.New("not implemented by test store")
}

func (store *memoryAuthStore) BootstrapDevelopmentMembership(context.Context, domain.DevelopmentBootstrapGrant) (domain.TenantMembership, error) {
	return domain.TenantMembership{}, errors.New("not implemented by test store")
}

func (store *memoryAuthStore) CreateWebSession(_ context.Context, session domain.WebSession) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.sessions[session.SessionDigest] = session
	return nil
}

func (store *memoryAuthStore) AuthorizeWebSession(_ context.Context, digest domain.SecretDigest, permission domain.TenantPermission, at time.Time) (ports.WebAuthorization, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	session, found := store.sessions[digest]
	if !found {
		return ports.WebAuthorization{}, domain.ErrWebSessionRevoked
	}
	membership, found := store.findMembership(session.UserID, session.ActiveTenantID)
	if !found {
		return ports.WebAuthorization{}, domain.ErrMembershipDenied
	}
	if err := session.Authorize(membership, permission, at); err != nil {
		return ports.WebAuthorization{}, err
	}
	return ports.WebAuthorization{Session: session, Membership: membership}, nil
}

func (store *memoryAuthStore) SwitchTenant(_ context.Context, current domain.SecretDigest, next domain.WebSession, selected domain.TenantID, at time.Time) (ports.WebAuthorization, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	previous, found := store.sessions[current]
	if !found {
		return ports.WebAuthorization{}, domain.ErrWebSessionRevoked
	}
	membership, found := store.findMembership(previous.UserID, selected)
	if !found {
		return ports.WebAuthorization{}, domain.ErrMembershipDenied
	}
	if err := domain.ValidateWebSessionRotation(previous, next, membership, at); err != nil {
		return ports.WebAuthorization{}, err
	}
	if err := previous.Revoke(at); err != nil {
		return ports.WebAuthorization{}, err
	}
	store.sessions[current] = previous
	store.sessions[next.SessionDigest] = next
	return ports.WebAuthorization{Session: next, Membership: membership}, nil
}

func (store *memoryAuthStore) RevokeWebSession(_ context.Context, digest domain.SecretDigest, at time.Time) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	session, found := store.sessions[digest]
	if !found {
		return nil
	}
	if err := session.Revoke(at); err != nil {
		return err
	}
	store.sessions[digest] = session
	return nil
}

func (store *memoryAuthStore) findMembership(userID domain.UserID, tenantID domain.TenantID) (domain.TenantMembership, bool) {
	for _, membership := range store.memberships[userID] {
		if membership.TenantID == tenantID {
			return membership, true
		}
	}
	return domain.TenantMembership{}, false
}

func membership(tenantID domain.TenantID, userID domain.UserID, role domain.TenantMembershipRole) domain.TenantMembership {
	return domain.TenantMembership{
		TenantID: tenantID, UserID: userID, Role: role, Status: domain.TenantMembershipActive,
		SecurityVersion: 1, CreatedAt: bffTestTime, UpdatedAt: bffTestTime,
	}
}

type memorySessionAPIStore struct {
	records    []ports.SessionRecord
	created    map[domain.IdempotencyKey]domain.Session
	bindingErr error
}

func (store *memorySessionAPIStore) CreateSessionForUser(_ context.Context, request ports.SessionCreateRequest) (domain.Session, bool, error) {
	if store.created == nil {
		store.created = make(map[domain.IdempotencyKey]domain.Session)
	}
	if session, found := store.created[request.IdempotencyKey]; found {
		return session, false, nil
	}
	store.created[request.IdempotencyKey] = request.Session
	store.records = append(store.records, ports.SessionRecord{
		Session: request.Session,
		Display: domain.SessionDisplay{TenantID: request.Session.TenantID, SessionID: request.Session.ID, UpdatedAt: request.Session.UpdatedAt},
	})
	return request.Session, true, nil
}

func (store *memorySessionAPIStore) GetSessionForUser(_ context.Context, tenantID domain.TenantID, userID domain.UserID, sessionID domain.SessionID, _ bool) (ports.SessionRecord, bool, error) {
	for _, record := range store.records {
		if record.Session.TenantID == tenantID && record.Session.CreatedBy == userID && record.Session.ID == sessionID {
			return record, true, nil
		}
	}
	return ports.SessionRecord{}, false, nil
}

func (store *memorySessionAPIStore) ListSessionsForUser(_ context.Context, request ports.SessionListRequest) ([]ports.SessionRecord, error) {
	var result []ports.SessionRecord
	for _, record := range store.records {
		if record.Session.TenantID == request.TenantID && record.Session.CreatedBy == request.UserID && record.Session.Status == request.Status {
			result = append(result, record)
		}
	}
	if uint64(len(result)) > request.Limit {
		result = result[:request.Limit]
	}
	return result, nil
}

func (*memorySessionAPIStore) ListSessionHistoryForUser(context.Context, domain.TenantID, domain.UserID, domain.SessionID, uint64, uint64) ([]domain.SessionEvent, error) {
	return nil, nil
}

func (*memorySessionAPIStore) ListRunsForUser(context.Context, ports.RunListRequest) ([]ports.RunRecord, error) {
	return nil, nil
}

func (store *memorySessionAPIStore) BindOrSwitchFrontendForUser(_ context.Context, request ports.FrontendBindingRequest) (domain.FrontendBinding, error) {
	if store.bindingErr != nil {
		return domain.FrontendBinding{}, store.bindingErr
	}
	return domain.FrontendBinding{
		ID: request.BindingID, TenantID: request.TenantID, Frontend: request.Frontend,
		ExternalConversationID: request.ExternalConversationID, SessionID: request.SessionID,
		Revision: request.ExpectedRevision + 1, CreatedAt: request.At, UpdatedAt: request.At,
	}, nil
}

func (*memorySessionAPIStore) SetSessionArchivedForUser(context.Context, domain.TenantID, domain.UserID, domain.SessionID, bool, domain.IdempotencyKey, time.Time) (domain.Session, error) {
	return domain.Session{}, errors.New("not implemented")
}

type memorySessionBlobs struct{}

func (memorySessionBlobs) Put(context.Context, domain.TenantID, string, io.Reader) (domain.BlobRef, error) {
	return domain.BlobRef{}, errors.New("not implemented")
}

func (memorySessionBlobs) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}

func (memorySessionBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error { return nil }
