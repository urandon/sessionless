package webbff_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/yandexoauth"
)

type yandexTestProvider struct {
	subject   domain.ExternalSubject
	exchanges int
	request   ports.OAuthTokenRequest
	err       error
}

func (p *yandexTestProvider) AuthorizationURL(_ context.Context, r ports.OAuthAuthorizationRequest) (string, error) {
	return domain.YandexOAuthIssuer + "/authorize?state=" + url.QueryEscape(r.State) + "&code_challenge=" + url.QueryEscape(r.CodeChallenge), nil
}
func (p *yandexTestProvider) ExchangeAndVerify(_ context.Context, r ports.OAuthTokenRequest) (domain.ExternalSubject, error) {
	p.exchanges++
	p.request = r
	return p.subject, p.err
}
func yandexTestConfig(store ports.WebAuthStore, provider ports.OAuthIdentityProvider) webbff.Config {
	return webbff.Config{
		BaseURL: "https://web.dev.sessionless.triborg.dev", RedirectURI: "https://web.dev.sessionless.triborg.dev" + webcontract.RouteLoginCallback,
		OAuthProvider: provider, OAuthClientID: "yandex-client", Store: store, IDs: fixedIDs{}, Clock: fixedClock{now: bffTestTime},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}
func newYandexTestHandler(t *testing.T, store *memoryAuthStore, provider *yandexTestProvider) http.Handler {
	t.Helper()
	h, err := webbff.New(yandexTestConfig(store, provider))
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func startYandexLogin(t *testing.T, h http.Handler) (*http.Cookie, string) {
	t.Helper()
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+webcontract.RouteLoginStart+"?return_to=%2Fsessions", nil))
	if r.Code != http.StatusSeeOther {
		t.Fatalf("start status=%d body=%s", r.Code, r.Body.String())
	}
	u, err := url.Parse(r.Header().Get("Location"))
	if err != nil || u.Host != "oauth.yandex.ru" || u.Query().Get("state") == "" || u.Query().Get("code_challenge") == "" {
		t.Fatalf("authorization location=%q err=%v", r.Header().Get("Location"), err)
	}
	return responseCookie(t, r.Result(), webbff.LoginBindingCookieName), u.Query().Get("state")
}
func finishYandexLogin(h http.Handler, binding *http.Cookie, state string) *httptest.ResponseRecorder {
	r := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+webcontract.RouteLoginCallback+"?code=opaque-code&state="+url.QueryEscape(state), nil)
	req.AddCookie(binding)
	h.ServeHTTP(r, req)
	return r
}
func yandexKnownStore() (*memoryAuthStore, domain.ExternalSubject) {
	s := newMemoryAuthStore()
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "424242"}
	user := domain.UserID("usr_yandex")
	s.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: user, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	s.memberships[user] = []domain.TenantMembership{membership("ten_alpha", user, domain.TenantMembershipOwner), membership("ten_beta", user, domain.TenantMembershipMember)}
	return s, subject
}

func TestYandexLoginBoundChallengeReplayTenantSwitchAndRevoke(t *testing.T) {
	store, subject := yandexKnownStore()
	p := &yandexTestProvider{subject: subject}
	h := newYandexTestHandler(t, store, p)
	for _, path := range []string{webcontract.RouteOIDCStart, webcontract.RouteOIDCCallback} {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+path, nil))
		if r.Code != http.StatusNotFound {
			t.Fatalf("Telegram route %s status=%d", path, r.Code)
		}
	}
	binding, state := startYandexLogin(t, h)
	challenge := store.challenges[domain.DigestSecret(state)]
	if challenge.Provider != subject.Provider || challenge.Issuer != domain.YandexOAuthIssuer || challenge.Audience != "yandex-client" || challenge.Nonce != "" {
		t.Fatalf("challenge binding provider=%q issuer=%q audience=%q noncePresent=%t", challenge.Provider, challenge.Issuer, challenge.Audience, challenge.Nonce != "")
	}
	r := finishYandexLogin(h, binding, state)
	if r.Code != http.StatusSeeOther || r.Header().Get("Location") != "/sessions" || p.exchanges != 1 || p.request.PKCEVerifier != challenge.PKCEVerifier || p.request.RedirectURI != "https://web.dev.sessionless.triborg.dev"+webcontract.RouteLoginCallback {
		t.Fatalf("callback status=%d location=%q exchanges=%d", r.Code, r.Header().Get("Location"), p.exchanges)
	}
	session := responseCookie(t, r.Result(), webcontract.SessionCookieName)
	csrf := responseCookie(t, r.Result(), webcontract.CSRFCookieName)
	replay := finishYandexLogin(h, binding, state)
	if replay.Header().Get("Location") != "/login?auth_error=access_denied" || p.exchanges != 1 {
		t.Fatalf("replay location=%q exchanges=%d", replay.Header().Get("Location"), p.exchanges)
	}
	me := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+webcontract.RouteMe, nil)
	req.AddCookie(session)
	h.ServeHTTP(me, req)
	if me.Code != http.StatusOK || !strings.Contains(me.Body.String(), `"provider":"yandex"`) || strings.Contains(me.Body.String(), "424242") {
		t.Fatalf("me status=%d body=%s", me.Code, me.Body.String())
	}
	denied := httptest.NewRecorder()
	h.ServeHTTP(denied, tenantSwitchRequest(session, csrf, "ten_foreign", csrf.Value))
	if denied.Code != http.StatusForbidden {
		t.Fatalf("foreign tenant switch status=%d body=%s", denied.Code, denied.Body.String())
	}
	switched := httptest.NewRecorder()
	h.ServeHTTP(switched, tenantSwitchRequest(session, csrf, "ten_beta", csrf.Value))
	if switched.Code != http.StatusOK {
		t.Fatalf("own tenant switch status=%d body=%s", switched.Code, switched.Body.String())
	}
	next := responseCookie(t, switched.Result(), webcontract.SessionCookieName)
	nextCSRF := responseCookie(t, switched.Result(), webcontract.CSRFCookieName)
	old := httptest.NewRecorder()
	h.ServeHTTP(old, req)
	if old.Code != http.StatusUnauthorized {
		t.Fatalf("rotated old cookie status=%d", old.Code)
	}
	logout := httptest.NewRequest(http.MethodPost, "https://web.dev.sessionless.triborg.dev"+webcontract.RouteLogout, nil)
	logout.Header.Set("Origin", "https://web.dev.sessionless.triborg.dev")
	logout.Header.Set(webcontract.CSRFHeaderName, nextCSRF.Value)
	logout.AddCookie(next)
	logout.AddCookie(nextCSRF)
	out := httptest.NewRecorder()
	h.ServeHTTP(out, logout)
	if out.Code != http.StatusNoContent {
		t.Fatalf("logout status=%d body=%s", out.Code, out.Body.String())
	}
	after := httptest.NewRecorder()
	afterReq := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+webcontract.RouteMe, nil)
	afterReq.AddCookie(next)
	h.ServeHTTP(after, afterReq)
	if after.Code != http.StatusUnauthorized {
		t.Fatalf("revoked session status=%d", after.Code)
	}
	for _, event := range store.recordedSecurityEvents() {
		if event.Provider != domain.IdentityProviderYandex {
			t.Fatalf("audit provider=%q action=%q", event.Provider, event.Action)
		}
	}
}

func TestYandexChallengeBindingRejectsBeforeExchange(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*domain.OIDCLoginChallenge)
	}{
		{"legacy-unbound", func(c *domain.OIDCLoginChallenge) {
			c.Provider = ""
			c.Issuer = ""
			c.Audience = ""
			c.Nonce = "legacy-nonce"
		}},
		{"Telegram", func(c *domain.OIDCLoginChallenge) {
			c.Provider = domain.IdentityProviderTelegram
			c.Issuer = "https://oauth.telegram.org"
			c.Nonce = "telegram-nonce"
		}},
		{"wrong-issuer", func(c *domain.OIDCLoginChallenge) { c.Issuer = "https://evil.example" }},
		{"wrong-client", func(c *domain.OIDCLoginChallenge) { c.Audience = "another-client" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, subject := yandexKnownStore()
			p := &yandexTestProvider{subject: subject}
			h := newYandexTestHandler(t, s, p)
			binding, state := startYandexLogin(t, h)
			c := s.challenges[domain.DigestSecret(state)]
			tc.mutate(&c)
			s.challenges[c.StateDigest] = c
			r := finishYandexLogin(h, binding, state)
			if r.Header().Get("Location") != "/login?auth_error=access_denied" || p.exchanges != 0 || len(s.sessions) != 0 {
				t.Fatalf("binding rejection location=%q exchanges=%d sessions=%d", r.Header().Get("Location"), p.exchanges, len(s.sessions))
			}
		})
	}
}

func TestLegacyTelegramChallengeAcceptedOnlyByLegacyTelegramCallback(t *testing.T) {
	for _, path := range []string{webcontract.RouteOIDCCallback, webcontract.RouteLoginCallback} {
		t.Run(path, func(t *testing.T) {
			s := newMemoryAuthStore()
			subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}
			s.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: "usr_telegram", CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
			s.memberships["usr_telegram"] = []domain.TenantMembership{membership("ten_alpha", "usr_telegram", domain.TenantMembershipOwner)}
			h := newTestHandler(t, s, subject.Subject)
			start := httptest.NewRecorder()
			h.ServeHTTP(start, httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+webcontract.RouteOIDCStart, nil))
			u, err := url.Parse(start.Header().Get("Location"))
			if err != nil {
				t.Fatal(err)
			}
			binding := responseCookie(t, start.Result(), webbff.LoginBindingCookieName)
			state := u.Query().Get("state")
			challenge := s.challenges[domain.DigestSecret(state)]
			challenge.Provider = ""
			challenge.Issuer = ""
			challenge.Audience = ""
			s.challenges[challenge.StateDigest] = challenge
			request := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+path+"?code=legacy-code&state="+url.QueryEscape(state), nil)
			request.AddCookie(binding)
			r := httptest.NewRecorder()
			h.ServeHTTP(r, request)
			want := "/login?auth_error=access_denied"
			wantSessions := 0
			if path == webcontract.RouteOIDCCallback {
				want = "/"
				wantSessions = 1
			}
			if r.Header().Get("Location") != want || len(s.sessions) != wantSessions {
				t.Fatalf("legacy callback path=%s location=%q sessions=%d wantLocation=%q wantSessions=%d", path, r.Header().Get("Location"), len(s.sessions), want, wantSessions)
			}
		})
	}
}

func TestYandexVerifiedIdentityNeverMergesTelegramOrGrantsMembership(t *testing.T) {
	s := newMemoryAuthStore()
	tg := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}
	s.identities[tg] = domain.ExternalIdentity{Subject: tg, UserID: "usr_telegram", CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	s.memberships["usr_telegram"] = []domain.TenantMembership{membership("ten_private", "usr_telegram", domain.TenantMembershipOwner)}
	p := &yandexTestProvider{subject: domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: tg.Subject}}
	h := newYandexTestHandler(t, s, p)
	binding, state := startYandexLogin(t, h)
	r := finishYandexLogin(h, binding, state)
	if r.Header().Get("Location") != "/login?auth_error=access_denied" || len(s.sessions) != 0 {
		t.Fatalf("unenrolled callback location=%q sessions=%d", r.Header().Get("Location"), len(s.sessions))
	}
	ya := s.identities[p.subject]
	if ya.UserID == tgUser(s, tg) || ya.UserID == "" || len(s.memberships[ya.UserID]) != 0 || len(s.identities) != 2 {
		t.Fatalf("identity separation Yandex=%+v Telegram=%+v memberships=%v", ya, s.identities[tg], s.memberships[ya.UserID])
	}
	events := s.recordedSecurityEvents()
	if len(events) != 1 || events[0].Provider != domain.IdentityProviderYandex || events[0].ReasonCode != "membership_missing" {
		t.Fatalf("failure audit=%+v", events)
	}
}
func tgUser(s *memoryAuthStore, subject domain.ExternalSubject) domain.UserID {
	return s.identities[subject].UserID
}

func TestYandexProviderRejectsUnverifiedOrWrongProviderSubject(t *testing.T) {
	for _, tc := range []struct {
		name    string
		subject domain.ExternalSubject
		err     error
	}{
		{"wrong-provider", domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}, nil},
		{"invalid-subject", domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: ""}, nil},
		{"exchange-failed", domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "424242"}, errors.New("secret-token-detail")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := yandexKnownStore()
			p := &yandexTestProvider{subject: tc.subject, err: tc.err}
			h := newYandexTestHandler(t, s, p)
			b, state := startYandexLogin(t, h)
			r := finishYandexLogin(h, b, state)
			if r.Header().Get("Location") != "/login?auth_error=access_denied" || len(s.sessions) != 0 || r.Body.Len() != 0 {
				t.Fatalf("invalid identity location=%q body=%q sessions=%d", r.Header().Get("Location"), r.Body.String(), len(s.sessions))
			}
		})
	}
}

func TestYandexConfigRejectsAmbiguousProviderAndRedirect(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*webbff.Config)
	}{
		{"both-providers", func(c *webbff.Config) { c.Provider = fakeProvider{subject: "1"} }},
		{"OIDC-policy", func(c *webbff.Config) {
			c.OIDCPolicy = domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "123", AllowedAlgorithms: []string{"RS256"}}
		}},
		{"missing-client", func(c *webbff.Config) { c.OAuthClientID = "" }},
		{"Telegram-callback", func(c *webbff.Config) { c.RedirectURI = c.BaseURL + webcontract.RouteOIDCCallback }},
		{"foreign-callback", func(c *webbff.Config) { c.RedirectURI = "https://evil.example" + webcontract.RouteLoginCallback }},
		{"callback-userinfo", func(c *webbff.Config) {
			c.RedirectURI = "https://secret@web.dev.sessionless.triborg.dev" + webcontract.RouteLoginCallback
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := yandexTestConfig(newMemoryAuthStore(), &yandexTestProvider{})
			tc.mutate(&c)
			if _, err := webbff.New(c); err == nil {
				t.Fatal("unsafe configuration accepted")
			}
		})
	}
}

func TestYandexActualAdapterBFFCodePKCEAndSafeFailure(t *testing.T) {
	store, _ := yandexKnownStore()
	var expectedChallenge atomic.Value
	var invalidClient atomic.Bool
	var tokenCalls atomic.Int32
	const origin = "https://web.dev.sessionless.triborg.dev"
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			tokenCalls.Add(1)
			id, secret, ok := r.BasicAuth()
			if !ok || id != "yandex-client" || secret != "fixture-secret" {
				t.Error("private client binding missing")
			}
			if err := r.ParseForm(); err != nil {
				t.Errorf("token form: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			digest := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
			if base64.RawURLEncoding.EncodeToString(digest[:]) != expectedChallenge.Load().(string) || r.PostForm.Get("code") != "opaque-code" || r.PostForm.Get("redirect_uri") != origin+webcontract.RouteLoginCallback {
				t.Error("PKCE/code/redirect binding missing")
			}
			fmt.Fprint(w, `{"access_token":"private-access-token","token_type":"bearer","expires_in":3600}`)
		case "/info":
			if r.Header.Get("Authorization") != "OAuth private-access-token" || r.URL.RawQuery != "format=json" {
				t.Error("profile bearer must remain header-only")
			}
			client := "yandex-client"
			if invalidClient.Load() {
				client = "another-client"
			}
			fmt.Fprintf(w, `{"id":"424242","client_id":%q,"email":"not-identity@example.invalid"}`, client)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(s.Close)
	p, err := yandexoauth.New(yandexoauth.Config{ClientID: "yandex-client", ClientSecret: "fixture-secret", RedirectURI: origin + webcontract.RouteLoginCallback, AuthorizationEndpoint: s.URL + "/authorize", TokenEndpoint: s.URL + "/token", InfoEndpoint: s.URL + "/info", AllowLoopbackProvider: true, HTTPClient: s.Client()})
	if err != nil {
		t.Fatal(err)
	}
	h, err := webbff.New(yandexTestConfig(store, p))
	if err != nil {
		t.Fatal(err)
	}
	start := func() (*http.Cookie, string) {
		t.Helper()
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, origin+webcontract.RouteLoginStart, nil))
		u, err := url.Parse(r.Header().Get("Location"))
		if err != nil || r.Code != http.StatusSeeOther || u.Query().Get("scope") != "login:info" || u.Query().Get("nonce") != "" {
			t.Fatalf("start status=%d location=%q err=%v", r.Code, r.Header().Get("Location"), err)
		}
		expectedChallenge.Store(u.Query().Get("code_challenge"))
		return responseCookie(t, r.Result(), webbff.LoginBindingCookieName), u.Query().Get("state")
	}
	binding, state := start()
	r := finishYandexLogin(h, binding, state)
	if r.Header().Get("Location") != "/" || tokenCalls.Load() != 1 {
		t.Fatalf("actual adapter login location=%q tokenCalls=%d", r.Header().Get("Location"), tokenCalls.Load())
	}
	session := responseCookie(t, r.Result(), webcontract.SessionCookieName)
	csrf := responseCookie(t, r.Result(), webcontract.CSRFCookieName)
	logout := httptest.NewRequest(http.MethodPost, origin+webcontract.RouteLogout, nil)
	logout.Header.Set("Origin", origin)
	logout.Header.Set(webcontract.CSRFHeaderName, csrf.Value)
	logout.AddCookie(session)
	logout.AddCookie(csrf)
	out := httptest.NewRecorder()
	h.ServeHTTP(out, logout)
	if out.Code != http.StatusNoContent {
		t.Fatalf("logout status=%d body=%s", out.Code, out.Body.String())
	}
	invalidClient.Store(true)
	binding, state = start()
	denied := finishYandexLogin(h, binding, state)
	if denied.Header().Get("Location") != "/login?auth_error=access_denied" || denied.Body.Len() != 0 {
		t.Fatalf("wrong-client profile location=%q body=%q", denied.Header().Get("Location"), denied.Body.String())
	}
}
