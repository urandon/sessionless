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

	"gitcode.com/urandon/sessionless/internal/attachedworkerux"
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
	if response.Code != http.StatusSeeOther {
		t.Fatalf("unknown identity status = %d body=%s", response.Code, response.Body.String())
	}
	if location := response.Header().Get("Location"); location != "/login?auth_error=access_denied" {
		t.Fatalf("recovery location = %q", location)
	}
	if response.Body.Len() != 0 {
		t.Fatalf("callback failure body = %q", response.Body.String())
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

func TestOIDCProviderDenialRedirectsWithOnlyStableErrorCode(t *testing.T) {
	store := newMemoryAuthStore()
	handler := newTestHandler(t, store, "424242")
	start := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteOIDCStart+"?return_to=%2Fsessions", nil)
	startResponse := httptest.NewRecorder()
	handler.ServeHTTP(startResponse, start)
	providerLocation, _ := url.Parse(startResponse.Header().Get("Location"))
	binding := responseCookie(t, startResponse.Result(), webbff.LoginBindingCookieName)

	callback := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteOIDCCallback+
			"?error=temporarily_unavailable&error_description=provider-secret-detail&state="+
			url.QueryEscape(providerLocation.Query().Get("state")), nil)
	callback.AddCookie(binding)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, callback)

	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/login?auth_error=access_denied" {
		t.Fatalf("provider denial status=%d location=%q body=%q", response.Code, response.Header().Get("Location"), response.Body.String())
	}
	if strings.Contains(response.Header().Get("Location"), "temporarily_unavailable") ||
		strings.Contains(response.Header().Get("Location"), "provider-secret-detail") || response.Body.Len() != 0 {
		t.Fatalf("provider details escaped: location=%q body=%q", response.Header().Get("Location"), response.Body.String())
	}
	cleared := responseCookie(t, response.Result(), webbff.LoginBindingCookieName)
	if cleared.Value != "" || cleared.MaxAge >= 0 {
		t.Fatalf("login binding was not cleared: %+v", cleared)
	}
}

func TestOIDCCallbackFailureRedirectsToStableUnavailableCodeWhenAuditCannotPersist(t *testing.T) {
	store := newMemoryAuthStore()
	handler := newTestHandler(t, store, "424242")
	start := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteOIDCStart+"?return_to=%2Fsessions", nil)
	startResponse := httptest.NewRecorder()
	handler.ServeHTTP(startResponse, start)
	providerLocation, _ := url.Parse(startResponse.Header().Get("Location"))
	binding := responseCookie(t, startResponse.Result(), webbff.LoginBindingCookieName)
	store.securityEventErr = errors.New("audit unavailable")

	callback := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteOIDCCallback+
			"?error=access_denied&error_description=provider-secret-detail&state="+
			url.QueryEscape(providerLocation.Query().Get("state")), nil)
	callback.AddCookie(binding)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, callback)

	if response.Code != http.StatusSeeOther ||
		response.Header().Get("Location") != "/login?auth_error=temporarily_unavailable" {
		t.Fatalf("audit failure status=%d location=%q body=%q",
			response.Code, response.Header().Get("Location"), response.Body.String())
	}
	if strings.Contains(response.Header().Get("Location"), "provider-secret-detail") ||
		response.Body.Len() != 0 {
		t.Fatalf("provider details escaped: location=%q body=%q",
			response.Header().Get("Location"), response.Body.String())
	}
	cleared := responseCookie(t, response.Result(), webbff.LoginBindingCookieName)
	if cleared.Value != "" || cleared.MaxAge >= 0 {
		t.Fatalf("login binding was not cleared: %+v", cleared)
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

func TestContentSecurityPolicyAllowsOnlyConfiguredObjectStorageOrigin(t *testing.T) {
	handler := newTestHandler(t, newMemoryAuthStore(), "424242")
	request := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev/healthz", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	want := "default-src 'self'; connect-src 'self' https://artifact-bucket.storage.yandexcloud.net; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
	if got := response.Header().Get("Content-Security-Policy"); got != want || strings.Contains(got, "*") {
		t.Fatalf("CSP = %q, want %q", got, want)
	}
}

func TestObjectStorageOriginValidationRejectsBroadOrInsecureValues(t *testing.T) {
	base := webbff.Config{
		BaseURL:     "https://web.dev.sessionless.triborg.dev",
		RedirectURI: "https://web.dev.sessionless.triborg.dev" + webcontract.RouteOIDCCallback,
		OIDCPolicy: domain.OIDCVerificationPolicy{
			Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"},
		},
		Provider: fakeProvider{subject: "424242"}, Store: newMemoryAuthStore(), IDs: fixedIDs{},
		Clock: fixedClock{now: bffTestTime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for _, origin := range []string{
		"https://*.storage.yandexcloud.net", "http://storage.yandexcloud.net",
		"https://storage.yandexcloud.net/path", "https://user@storage.yandexcloud.net",
		" https://storage.yandexcloud.net",
	} {
		config := base
		config.ObjectStorageOrigin = origin
		if _, err := webbff.New(config); err == nil {
			t.Fatalf("object storage origin %q was accepted", origin)
		}
	}
	local := base
	local.ObjectStorageOrigin = "http://127.0.0.1:9000"
	local.AllowLoopbackObjectStorage = true
	if _, err := webbff.New(local); err != nil {
		t.Fatalf("local loopback Object Storage origin rejected: %v", err)
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

	for _, test := range []struct {
		name   string
		body   string
		status int
	}{
		{name: "malformed", body: `{`, status: http.StatusBadRequest},
		{name: "unknown field", body: `{"idempotency_key":"create-invalid","unknown":true}`, status: http.StatusBadRequest},
		{name: "multiple values", body: `{"idempotency_key":"create-invalid"} {"idempotency_key":"other"}`, status: http.StatusBadRequest},
		{name: "oversized", body: `{"idempotency_key":"create-invalid","padding":"` + strings.Repeat("x", maxTestRequestBytes) + `"}`, status: http.StatusRequestEntityTooLarge},
	} {
		t.Run("body "+test.name, func(t *testing.T) {
			invalid := httptest.NewRequest(http.MethodPost,
				"https://web.dev.sessionless.triborg.dev"+webcontract.RouteSessions,
				strings.NewReader(test.body))
			invalid.Header.Set("Content-Type", "application/json")
			invalid.Header.Set("Origin", "https://web.dev.sessionless.triborg.dev")
			invalid.Header.Set(webcontract.CSRFHeaderName, csrfCookie.Value)
			invalid.AddCookie(sessionCookie)
			invalid.AddCookie(csrfCookie)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, invalid)
			if response.Code != test.status {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.status, response.Body.String())
			}
		})
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

func TestAttachedWorkerReadRoutesAreOwnerScopedAndReadOnly(t *testing.T) {
	auth := newMemoryAuthStore()
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}
	userID := domain.UserID("usr_known_user")
	auth.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: userID, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	auth.memberships[userID] = []domain.TenantMembership{membership("ten_alpha", userID, domain.TenantMembershipOwner)}
	worker := domain.AttachedWorker{
		TenantID: "ten_alpha", OwnerUserID: userID, ID: "wrk_owner_worker", DisplayName: "Office Mac",
		IdentityPublicKey: bytes.Repeat([]byte{1}, 32), EnrollmentGeneration: 1, ConnectionGeneration: 1,
		DesiredState: domain.AttachedWorkerDesiredActive, ObservedState: domain.AttachedWorkerObservedOffline,
		Revision: 1, CreatedAt: bffTestTime, UpdatedAt: bffTestTime,
	}
	foreign := worker
	foreign.OwnerUserID, foreign.ID, foreign.DisplayName = "usr_other_owner", "wrk_foreign_worker", "Foreign"
	store := &memoryAttachedWorkerUXStore{workers: []domain.AttachedWorker{worker, foreign}}
	var requestLogs bytes.Buffer
	handler := newAttachedWorkerTestHandlerWithLogger(t, auth, subject.Subject, store,
		slog.New(slog.NewTextHandler(&requestLogs, nil)))
	sessionCookie, _ := performLogin(t, handler, "/workers")

	list := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteAttachedWorkers+"?limit=1", nil)
	list.AddCookie(sessionCookie)
	listResponse := httptest.NewRecorder()
	handler.ServeHTTP(listResponse, list)
	if listResponse.Code != http.StatusOK || !strings.Contains(listResponse.Body.String(), `"worker_id":"wrk_owner_worker"`) || strings.Contains(listResponse.Body.String(), "Foreign") {
		t.Fatalf("worker list status=%d body=%s", listResponse.Code, listResponse.Body.String())
	}
	// The canonical reducer loads limit+1 rows to determine has_more.
	if store.lastTenant != "ten_alpha" || store.lastOwner != userID || store.lastLimit != 2 || store.lastAfter != "" {
		t.Fatalf("worker list authority/query mismatch: tenant=%q owner=%q after=%q limit=%d", store.lastTenant, store.lastOwner, store.lastAfter, store.lastLimit)
	}

	detail := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev/api/web/v1/attached-workers/wrk_owner_worker", nil)
	detail.AddCookie(sessionCookie)
	detailResponse := httptest.NewRecorder()
	handler.ServeHTTP(detailResponse, detail)
	if detailResponse.Code != http.StatusOK || !strings.Contains(detailResponse.Body.String(), `"state":"not_evaluated"`) || !strings.Contains(detailResponse.Body.String(), `"enabled":false`) {
		t.Fatalf("worker detail status=%d body=%s", detailResponse.Code, detailResponse.Body.String())
	}

	diagnostics := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev/api/web/v1/attached-workers/wrk_owner_worker/diagnostics", nil)
	diagnostics.AddCookie(sessionCookie)
	diagnosticsResponse := httptest.NewRecorder()
	handler.ServeHTTP(diagnosticsResponse, diagnostics)
	if diagnosticsResponse.Code != http.StatusOK || !strings.Contains(diagnosticsResponse.Body.String(), `"code":"daemon_state","state":"unknown"`) {
		t.Fatalf("worker diagnostics status=%d body=%s", diagnosticsResponse.Code, diagnosticsResponse.Body.String())
	}

	var missingEnvelope, foreignEnvelope webcontract.ErrorEnvelope
	for _, test := range []struct {
		id       string
		envelope *webcontract.ErrorEnvelope
	}{
		{id: "wrk_missing_worker", envelope: &missingEnvelope},
		{id: "wrk_foreign_worker", envelope: &foreignEnvelope},
	} {
		request := httptest.NewRequest(http.MethodGet,
			"https://web.dev.sessionless.triborg.dev/api/web/v1/attached-workers/"+test.id, nil)
		request.AddCookie(sessionCookie)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound || json.Unmarshal(response.Body.Bytes(), test.envelope) != nil {
			t.Fatalf("worker %s status=%d body=%s", test.id, response.Code, response.Body.String())
		}
	}
	if missingEnvelope.Error.Code != foreignEnvelope.Error.Code || missingEnvelope.Error.Message != foreignEnvelope.Error.Message {
		t.Fatalf("missing and foreign worker oracles differ: missing=%+v foreign=%+v", missingEnvelope, foreignEnvelope)
	}

	for _, rawQuery := range []string{
		"limit=51", "limit=0", "limit=abc", "limit=1&limit=2", "unknown=value",
		"after_worker_id=", "after_worker_id=bad%2Fworker",
	} {
		invalid := httptest.NewRequest(http.MethodGet,
			"https://web.dev.sessionless.triborg.dev"+webcontract.RouteAttachedWorkers+"?"+rawQuery, nil)
		invalid.AddCookie(sessionCookie)
		invalidResponse := httptest.NewRecorder()
		handler.ServeHTTP(invalidResponse, invalid)
		if invalidResponse.Code != http.StatusBadRequest {
			t.Fatalf("invalid worker query %q status=%d body=%s", rawQuery, invalidResponse.Code, invalidResponse.Body.String())
		}
	}
	for _, path := range []string{
		"/api/web/v1/attached-workers/wrk_owner_worker?limit=1",
		"/api/web/v1/attached-workers/wrk_owner_worker/diagnostics?extra=1",
	} {
		invalid := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+path, nil)
		invalid.AddCookie(sessionCookie)
		invalidResponse := httptest.NewRecorder()
		handler.ServeHTTP(invalidResponse, invalid)
		if invalidResponse.Code != http.StatusBadRequest {
			t.Fatalf("unexpected selector query %q status=%d body=%s", path, invalidResponse.Code, invalidResponse.Body.String())
		}
	}
	post := httptest.NewRequest(http.MethodPost,
		"https://web.dev.sessionless.triborg.dev/api/web/v1/attached-workers/wrk_owner_worker", nil)
	post.AddCookie(sessionCookie)
	postResponse := httptest.NewRecorder()
	handler.ServeHTTP(postResponse, post)
	if postResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("worker mutation route status=%d body=%s", postResponse.Code, postResponse.Body.String())
	}
	logs := requestLogs.String()
	if strings.Contains(logs, "wrk_owner_worker") || !strings.Contains(logs, webcontract.RouteAttachedWorker) {
		t.Fatalf("request logs must contain the route template and no worker selector: %s", logs)
	}
	if store.mutations != 0 {
		t.Fatalf("read routes caused %d mutations", store.mutations)
	}
}

func TestAttachedWorkerReadRoutesAuthorizeBeforeReadingAndSanitizeBackends(t *testing.T) {
	auth := newMemoryAuthStore()
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "515151"}
	userID := domain.UserID("usr_worker_reader")
	auth.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: userID, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	auth.memberships[userID] = []domain.TenantMembership{membership("ten_alpha", userID, domain.TenantMembershipOwner)}
	store := &memoryAttachedWorkerUXStore{failure: errors.New("provider-token=must-not-escape")}
	handler := newAttachedWorkerTestHandler(t, auth, subject.Subject, store)

	unauthenticated := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteAttachedWorkers, nil)
	unauthenticatedResponse := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticatedResponse, unauthenticated)
	if unauthenticatedResponse.Code != http.StatusUnauthorized || store.reads != 0 {
		t.Fatalf("unauthenticated status=%d reads=%d", unauthenticatedResponse.Code, store.reads)
	}

	sessionCookie, _ := performLogin(t, handler, "/workers")
	backend := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteAttachedWorkers, nil)
	backend.AddCookie(sessionCookie)
	backendResponse := httptest.NewRecorder()
	handler.ServeHTTP(backendResponse, backend)
	if backendResponse.Code != http.StatusServiceUnavailable || strings.Contains(backendResponse.Body.String(), "provider-token") {
		t.Fatalf("backend status=%d body=%s", backendResponse.Code, backendResponse.Body.String())
	}
}

func TestAttachedWorkerReadRoutesRejectOversizedProjection(t *testing.T) {
	auth := newMemoryAuthStore()
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "616161"}
	userID := domain.UserID("usr_worker_reader")
	auth.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: userID, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	auth.memberships[userID] = []domain.TenantMembership{membership("ten_alpha", userID, domain.TenantMembershipOwner)}
	service := oversizedAttachedWorkerReadService{}
	handler := newAttachedWorkerHandlerWithService(t, auth, subject.Subject, service, slog.New(slog.NewTextHandler(io.Discard, nil)))
	sessionCookie, _ := performLogin(t, handler, "/workers")

	request := httptest.NewRequest(http.MethodGet,
		"https://web.dev.sessionless.triborg.dev"+webcontract.RouteAttachedWorkers, nil)
	request.AddCookie(sessionCookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), strings.Repeat("x", 128)) {
		t.Fatalf("oversized projection status=%d body=%s", response.Code, response.Body.String())
	}
}

const maxTestRequestBytes = 64 << 10

func newTestHandler(t *testing.T, store *memoryAuthStore, subject string) http.Handler {
	t.Helper()
	return newTestHandlerWithLogger(t, store, subject, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newTestHandlerWithLogger(t *testing.T, store *memoryAuthStore, subject string, logger *slog.Logger) http.Handler {
	t.Helper()
	handler, err := webbff.New(webbff.Config{
		BaseURL:             "https://web.dev.sessionless.triborg.dev",
		RedirectURI:         "https://web.dev.sessionless.triborg.dev" + webcontract.RouteOIDCCallback,
		ObjectStorageOrigin: "https://artifact-bucket.storage.yandexcloud.net",
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

func newAttachedWorkerTestHandler(t *testing.T, auth *memoryAuthStore, subject string, store ports.AttachedWorkerUXReadStore) http.Handler {
	t.Helper()
	return newAttachedWorkerTestHandlerWithLogger(t, auth, subject, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func newAttachedWorkerTestHandlerWithLogger(t *testing.T, auth *memoryAuthStore, subject string, store ports.AttachedWorkerUXReadStore, logger *slog.Logger) http.Handler {
	t.Helper()
	workers, err := attachedworkerux.NewService(store, func() time.Time { return bffTestTime })
	if err != nil {
		t.Fatal(err)
	}
	return newAttachedWorkerHandlerWithService(t, auth, subject, workers, logger)
}

func newAttachedWorkerHandlerWithService(t *testing.T, auth *memoryAuthStore, subject string, workers webbff.AttachedWorkerReadService, logger *slog.Logger) http.Handler {
	t.Helper()
	handler, err := webbff.New(webbff.Config{
		BaseURL:     "https://web.dev.sessionless.triborg.dev",
		RedirectURI: "https://web.dev.sessionless.triborg.dev" + webcontract.RouteOIDCCallback,
		OIDCPolicy: domain.OIDCVerificationPolicy{
			Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"},
		},
		Provider: fakeProvider{subject: subject}, Store: auth, AttachedWorkers: workers, IDs: fixedIDs{},
		Clock: fixedClock{now: bffTestTime}, Logger: logger,
		Build: buildinfo.Current("web-bff-attached-worker-test"),
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

type memoryAttachedWorkerUXStore struct {
	workers    []domain.AttachedWorker
	mutations  int
	reads      int
	failure    error
	lastTenant domain.TenantID
	lastOwner  domain.UserID
	lastAfter  domain.AttachedWorkerID
	lastLimit  uint64
}

type oversizedAttachedWorkerReadService struct{}

func (oversizedAttachedWorkerReadService) List(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID, uint64) (attachedworkerux.AttachedWorkerListV1, error) {
	return attachedworkerux.AttachedWorkerListV1{
		Version: attachedworkerux.ReadModelVersionV1, EvaluatedAt: bffTestTime,
		Items: []attachedworkerux.AttachedWorkerSummaryV1{{Worker: attachedworkerux.WorkerV1{DisplayName: strings.Repeat("x", 300<<10)}}},
	}, nil
}

func (oversizedAttachedWorkerReadService) Get(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (attachedworkerux.AttachedWorkerUXReadModelV1, error) {
	return attachedworkerux.AttachedWorkerUXReadModelV1{}, attachedworkerux.ErrNotFound
}

func (oversizedAttachedWorkerReadService) Diagnostics(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (attachedworkerux.AttachedWorkerDiagnosticsV1, error) {
	return attachedworkerux.AttachedWorkerDiagnosticsV1{}, attachedworkerux.ErrNotFound
}

func (store *memoryAttachedWorkerUXStore) LoadAttachedWorker(_ context.Context, tenantID domain.TenantID, ownerUserID domain.UserID, workerID domain.AttachedWorkerID) (domain.AttachedWorker, bool, error) {
	store.reads++
	if store.failure != nil {
		return domain.AttachedWorker{}, false, store.failure
	}
	for _, worker := range store.workers {
		if worker.TenantID == tenantID && worker.OwnerUserID == ownerUserID && worker.ID == workerID {
			return worker, true, nil
		}
	}
	return domain.AttachedWorker{}, false, nil
}

func (store *memoryAttachedWorkerUXStore) ListAttachedWorkers(_ context.Context, tenantID domain.TenantID, ownerUserID domain.UserID, after domain.AttachedWorkerID, limit uint64) ([]domain.AttachedWorker, error) {
	store.reads++
	store.lastTenant, store.lastOwner, store.lastAfter, store.lastLimit = tenantID, ownerUserID, after, limit
	if store.failure != nil {
		return nil, store.failure
	}
	result := make([]domain.AttachedWorker, 0, len(store.workers))
	for _, worker := range store.workers {
		if worker.TenantID == tenantID && worker.OwnerUserID == ownerUserID && worker.ID > after {
			result = append(result, worker)
		}
	}
	if uint64(len(result)) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (*memoryAttachedWorkerUXStore) LoadAttachedWorkerConnection(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorkerConnection, bool, error) {
	return domain.AttachedWorkerConnection{}, false, nil
}

func (*memoryAttachedWorkerUXStore) LoadAttachedWorkerCapabilityManifest(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID, domain.AttachedWorkerCapabilityDigest) (domain.AttachedWorkerCapabilityManifest, bool, error) {
	return domain.AttachedWorkerCapabilityManifest{}, false, nil
}

func (*memoryAttachedWorkerUXStore) LoadAttachedWorkerAttempt(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorkerAttemptV1, bool, error) {
	return domain.AttachedWorkerAttemptV1{}, false, nil
}

func (*memoryAttachedWorkerUXStore) ListAttachedWorkerAttemptMessages(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID, domain.AttemptID) ([]domain.AttachedWorkerAttemptMessageV1, error) {
	return nil, nil
}
