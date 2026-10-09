//go:build ydbintegration

package ydbintegration

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/yandexoauth"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// A loopback token/account fixture drives the actual Yandex adapter. This
// test proves the normal BFF and real persisted authorities, not live Yandex
// connectivity or client registration. Every timestamp uses the sampled YDB
// clock; identities and memberships are created through production ports.
func TestYandexLoginYDBFreshIdentityMembershipIsolationReplayAndRevocation(t *testing.T) {
	store, client := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	clock := onboardingDatabaseClock{t: t, ctx: ctx, db: client.DB}
	const origin = "https://web.example"
	var accountID, expectedChallenge atomic.Value
	newAccountID := func() string {
		digest := sha256.Sum256([]byte(uniqueID("yandex-subject")))
		return strconv.FormatUint(binary.BigEndian.Uint64(digest[:8])|1, 10)
	}
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: newAccountID()}
	accountID.Store(subject.Subject)
	var exchanges atomic.Int32
	providerHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			exchanges.Add(1)
			client, secret, ok := r.BasicAuth()
			if !ok || client != "test-yandex-client" || secret != "fixture-secret" {
				t.Error("token endpoint missing private client authentication")
			}
			if err := r.ParseForm(); err != nil {
				t.Errorf("parse token form: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			digest := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
			if r.Method != http.MethodPost || r.PostForm.Get("code") != "test-code" || r.PostForm.Get("redirect_uri") != origin+webcontract.RouteLoginCallback || r.PostForm.Get("grant_type") != "authorization_code" || base64.RawURLEncoding.EncodeToString(digest[:]) != expectedChallenge.Load().(string) {
				t.Error("token exchange lost code/redirect/PKCE binding")
			}
			fmt.Fprint(w, `{"access_token":"fixture-access-token","token_type":"bearer","expires_in":3600}`)
		case "/info":
			if r.Header.Get("Authorization") != "OAuth fixture-access-token" || r.URL.RawQuery != "format=json" {
				t.Error("account token must be header-only")
			}
			fmt.Fprintf(w, `{"id":%q,"client_id":"test-yandex-client","email":"not-link-authority@example.invalid"}`, accountID.Load().(string))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(providerHTTP.Close)
	provider, err := yandexoauth.New(yandexoauth.Config{ClientID: "test-yandex-client", ClientSecret: "fixture-secret", RedirectURI: origin + webcontract.RouteLoginCallback, AuthorizationEndpoint: providerHTTP.URL + "/authorize", TokenEndpoint: providerHTTP.URL + "/token", InfoEndpoint: providerHTTP.URL + "/info", AllowLoopbackProvider: true, HTTPClient: providerHTTP.Client()})
	if err != nil {
		t.Fatalf("compose actual Yandex adapter: %v", err)
	}
	h, err := webbff.New(webbff.Config{BaseURL: origin, RedirectURI: origin + webcontract.RouteLoginCallback, OAuthProvider: provider, OAuthClientID: "test-yandex-client", Store: store, IDs: idgen.New(), Clock: clock, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatalf("compose normal Yandex BFF: %v", err)
	}
	start := func() (*http.Cookie, string) {
		t.Helper()
		r := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, origin+webcontract.RouteLoginStart+"?return_to=%2Fsessions", nil).WithContext(ctx)
		h.ServeHTTP(r, req)
		if r.Code != http.StatusSeeOther {
			t.Fatalf("start status=%d body=%s", r.Code, r.Body.String())
		}
		u, err := url.Parse(r.Header().Get("Location"))
		if err != nil {
			t.Fatal(err)
		}
		if u.Query().Get("nonce") != "" || u.Query().Get("scope") != "login:info" || u.Query().Get("code_challenge_method") != "S256" {
			t.Fatalf("OAuth authorization shape=%q", u.String())
		}
		expectedChallenge.Store(u.Query().Get("code_challenge"))
		return yandexYDBCookie(t, r, webbff.LoginBindingCookieName), u.Query().Get("state")
	}
	finish := func(binding *http.Cookie, state string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, origin+webcontract.RouteLoginCallback+"?code=test-code&state="+url.QueryEscape(state), nil).WithContext(ctx)
		req.AddCookie(binding)
		h.ServeHTTP(r, req)
		return r
	}
	binding, state := start()
	denied := finish(binding, state)
	if denied.Header().Get("Location") != "/login?auth_error=access_denied" {
		t.Fatalf("fresh identity without grant location=%q", denied.Header().Get("Location"))
	}
	auditBucket, err := ydbpartition.BucketV1(denied.Header().Get("X-Request-ID"))
	if err != nil {
		t.Fatal(err)
	}
	var auditProvider, auditReason string
	if err := client.DB.QueryRowContext(ctx, `SELECT provider, reason_code FROM web_security_audit_events WHERE shard_bucket = $1 AND request_id = $2`, auditBucket, denied.Header().Get("X-Request-ID")).Scan(&auditProvider, &auditReason); err != nil {
		t.Fatalf("read content-free fresh-login audit: %v", err)
	}
	if auditProvider != "yandex" || auditReason != "membership_missing" {
		t.Fatalf("actual-provider audit provider=%q reason=%q", auditProvider, auditReason)
	}
	for _, cookie := range denied.Result().Cookies() {
		if cookie.Name == webcontract.SessionCookieName && cookie.Value != "" {
			t.Fatal("fresh identity implicitly granted session")
		}
	}
	identity, created, err := store.ResolveOrCreateExternalIdentity(ctx, subject, domain.UserID(uniqueID("ignored-user")), clock.Now())
	if err != nil || created {
		t.Fatalf("persisted fresh identity created=%t identity=%+v err=%v", created, identity, err)
	}
	tg, _, err := store.ResolveOrCreateExternalIdentity(ctx, domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: subject.Subject}, domain.UserID(uniqueID("telegram-user")), clock.Now())
	if err != nil || tg.UserID == identity.UserID {
		t.Fatalf("provider-scoped raw ID separation Telegram=%+v Yandex=%+v err=%v", tg, identity, err)
	}
	memberships, err := store.ListTenantMemberships(ctx, identity.UserID, 10)
	if err != nil || len(memberships) != 0 {
		t.Fatalf("fresh memberships=%+v err=%v", memberships, err)
	}
	tenantA := domain.TenantID(uniqueID("yandex-tenant-a"))
	tenantB := domain.TenantID(uniqueID("yandex-tenant-b"))
	for _, tenant := range []domain.TenantID{tenantA, tenantB} {
		_, err := store.BootstrapDevelopmentMembership(ctx, domain.DevelopmentBootstrapGrant{TenantID: tenant, UserID: identity.UserID, Role: domain.TenantMembershipOwner, Environment: domain.DevelopmentEnvironment, Operator: "yandex-login-ci", Reason: "explicit membership proof", GrantedAt: clock.Now()})
		if err != nil {
			t.Fatalf("explicit grant tenant=%s: %v", tenant, err)
		}
	}
	binding, state = start()
	bucket, err := ydbpartition.BucketV1(string(domain.DigestSecret(state)))
	if err != nil {
		t.Fatal(err)
	}
	var payload string
	if err := client.DB.QueryRowContext(ctx, `SELECT record FROM oidc_login_challenges WHERE shard_bucket = $1 AND state_digest = $2`, bucket, domain.DigestSecret(state)).Scan(&payload); err != nil {
		t.Fatalf("read persisted challenge: %v", err)
	}
	var challenge domain.OIDCLoginChallenge
	if err := json.Unmarshal([]byte(payload), &challenge); err != nil {
		t.Fatal(err)
	}
	if challenge.Provider != domain.IdentityProviderYandex || challenge.Issuer != domain.YandexOAuthIssuer || challenge.Audience != "test-yandex-client" || challenge.Nonce != "" {
		t.Fatalf("persisted binding provider=%q issuer=%q audience=%q noncePresent=%t", challenge.Provider, challenge.Issuer, challenge.Audience, challenge.Nonce != "")
	}
	r := finish(binding, state)
	if r.Header().Get("Location") != "/sessions" {
		t.Fatalf("enrolled callback status=%d location=%q body=%s", r.Code, r.Header().Get("Location"), r.Body.String())
	}
	session := yandexYDBCookie(t, r, webcontract.SessionCookieName)
	csrf := yandexYDBCookie(t, r, webcontract.CSRFCookieName)
	beforeReplay := exchanges.Load()
	replay := finish(binding, state)
	if replay.Header().Get("Location") != "/login?auth_error=access_denied" || exchanges.Load() != beforeReplay {
		t.Fatalf("single use replay location=%q exchanges=%d want=%d", replay.Header().Get("Location"), exchanges.Load(), beforeReplay)
	}
	me := func(cookie *http.Cookie) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, origin+webcontract.RouteMe, nil).WithContext(ctx)
		req.AddCookie(cookie)
		h.ServeHTTP(r, req)
		return r
	}
	identityRead := me(session)
	if identityRead.Code != http.StatusOK || !strings.Contains(identityRead.Body.String(), `"provider":"yandex"`) {
		t.Fatalf("fresh authenticated me status=%d body=%s", identityRead.Code, identityRead.Body.String())
	}
	var uiIdentity webcontract.Identity
	if err := json.Unmarshal(identityRead.Body.Bytes(), &uiIdentity); err != nil {
		t.Fatal(err)
	}
	if len(uiIdentity.Tenants) != 2 || uiIdentity.UserID != identity.UserID {
		t.Fatalf("identity read=%+v", uiIdentity)
	}
	other := tenantB
	if uiIdentity.Tenants[0].TenantID == tenantB {
		other = tenantA
	}
	switchTenant := func(tenant domain.TenantID) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, origin+webcontract.RouteActiveTenant, strings.NewReader(`{"tenant_id":"`+string(tenant)+`"}`)).WithContext(ctx)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", origin)
		req.Header.Set(webcontract.CSRFHeaderName, csrf.Value)
		req.AddCookie(session)
		req.AddCookie(csrf)
		h.ServeHTTP(r, req)
		return r
	}
	foreign := switchTenant(domain.TenantID(uniqueID("foreign-tenant")))
	if foreign.Code != http.StatusForbidden {
		t.Fatalf("foreign tenant status=%d body=%s", foreign.Code, foreign.Body.String())
	}
	switched := switchTenant(other)
	if switched.Code != http.StatusOK {
		t.Fatalf("own second tenant status=%d body=%s", switched.Code, switched.Body.String())
	}
	next := yandexYDBCookie(t, switched, webcontract.SessionCookieName)
	if stale := me(session); stale.Code != http.StatusUnauthorized {
		t.Fatalf("rotated old session status=%d", stale.Code)
	}
	if active := me(next); active.Code != http.StatusOK {
		t.Fatalf("new tenant session status=%d body=%s", active.Code, active.Body.String())
	}
	if err := store.RevokeWebSession(ctx, domain.DigestSecret(next.Value), clock.Now()); err != nil {
		t.Fatalf("revoke normal Yandex session: %v", err)
	}
	if revoked := me(next); revoked.Code != http.StatusUnauthorized {
		t.Fatalf("revoked session status=%d body=%s", revoked.Code, revoked.Body.String())
	}
	accountID.Store(newAccountID())
	binding, state = start()
	stranger := finish(binding, state)
	if stranger.Header().Get("Location") != "/login?auth_error=access_denied" {
		t.Fatalf("second owner no implicit first tenant grant: location=%q", stranger.Header().Get("Location"))
	}
	// Restore the original owner and obtain a still-live session before revoking
	// membership. Session revocation above is a separate authority boundary.
	accountID.Store(subject.Subject)
	binding, state = start()
	beforeMembershipRevoke := finish(binding, state)
	if beforeMembershipRevoke.Header().Get("Location") != "/sessions" {
		t.Fatalf("original owner's fresh callback location=%q", beforeMembershipRevoke.Header().Get("Location"))
	}
	activeCookie := yandexYDBCookie(t, beforeMembershipRevoke, webcontract.SessionCookieName)
	currentMemberships, err := store.ListTenantMemberships(ctx, identity.UserID, 10)
	if err != nil || len(currentMemberships) != 2 {
		t.Fatalf("memberships before revocation=%+v err=%v", currentMemberships, err)
	}
	userBucket, err := ydbpartition.BucketV1(string(identity.UserID))
	if err != nil {
		t.Fatal(err)
	}
	for _, membership := range currentMemberships {
		membership.Status = domain.TenantMembershipRevoked
		membership.SecurityVersion++
		membership.UpdatedAt = clock.Now()
		payload, err := json.Marshal(membership)
		if err != nil {
			t.Fatal(err)
		}
		// Test-owned security transition fixture; update the indexed columns and
		// canonical record together, using only the exact two seeded keys.
		if _, err := client.DB.ExecContext(ctx, `UPDATE tenant_memberships SET status = $1, security_version = $2, updated_at = $3, record = CAST($4 AS JsonDocument) WHERE user_bucket = $5 AND user_id = $6 AND tenant_id = $7`, membership.Status, membership.SecurityVersion, membership.UpdatedAt, string(payload), userBucket, identity.UserID, membership.TenantID); err != nil {
			t.Fatalf("revoke exact membership tenant=%s: %v", membership.TenantID, err)
		}
	}
	if _, err := store.AuthorizeWebSession(ctx, domain.DigestSecret(activeCookie.Value), domain.TenantPermissionRead, clock.Now()); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("live session after membership revocation err=%v want membership-denied", err)
	}
	if revokedMembership := me(activeCookie); revokedMembership.Code != http.StatusForbidden {
		t.Fatalf("revoked membership me status=%d body=%s", revokedMembership.Code, revokedMembership.Body.String())
	}
	var beforeSessions uint64
	if err := client.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM web_sessions WHERE user_id = $1`, identity.UserID).Scan(&beforeSessions); err != nil {
		t.Fatalf("count test-owner sessions before denied relogin: %v", err)
	}
	binding, state = start()
	deniedAfterMembershipRevoke := finish(binding, state)
	if deniedAfterMembershipRevoke.Header().Get("Location") != "/login?auth_error=access_denied" {
		t.Fatalf("relogin after both membership revocations location=%q", deniedAfterMembershipRevoke.Header().Get("Location"))
	}
	for _, cookie := range deniedAfterMembershipRevoke.Result().Cookies() {
		if cookie.Name == webcontract.SessionCookieName && cookie.Value != "" {
			t.Fatal("revoked membership callback minted browser session")
		}
	}
	var afterSessions uint64
	if err := client.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM web_sessions WHERE user_id = $1`, identity.UserID).Scan(&afterSessions); err != nil {
		t.Fatalf("count test-owner sessions after denied relogin: %v", err)
	}
	if afterSessions != beforeSessions {
		t.Fatalf("revoked membership callback minted durable sessions before=%d after=%d", beforeSessions, afterSessions)
	}
}

func yandexYDBCookie(t *testing.T, r *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	for _, c := range r.Result().Cookies() {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("missing cookie %s status=%d location=%q", name, r.Code, r.Header().Get("Location"))
	return nil
}
