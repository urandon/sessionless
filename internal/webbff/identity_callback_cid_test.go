package webbff_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

// Keep the explicit stored LOGIN purpose in this BFF fixture: an arbitrary
// callback state must not become a login merely because a stub returns LOGIN.
type cidCallbackStore struct {
	*memoryAuthStore
	loginRecords map[domain.SecretDigest]domain.IdentityChallengeV2
}

func (s *cidCallbackStore) CreateLoginChallenge(ctx context.Context, challenge domain.OIDCLoginChallenge) error {
	path := webcontract.RouteLoginCallback
	if challenge.Provider == domain.IdentityProviderTelegram {
		path = webcontract.RouteOIDCCallback
	}
	child := domain.IdentityChallengeV2{
		Version: 2, Purpose: domain.IdentityPurposeLoginV2,
		StateDigest: challenge.StateDigest, BrowserBindingDigest: challenge.BrowserBindingDigest,
		Binding:   domain.IdentityProviderBindingV2{Provider: challenge.Provider, Issuer: challenge.Issuer, ClientID: challenge.Audience, CallbackURI: accountTestOrigin + path},
		CreatedAt: challenge.CreatedAt, ExpiresAt: challenge.ExpiresAt, DeleteAfter: challenge.ExpiresAt,
		Login:   &domain.IdentityLoginChallengeV2{ReturnPath: challenge.RedirectPath},
		Secrets: &domain.IdentityChallengeSecretsV2{PKCEVerifier: challenge.PKCEVerifier, Nonce: challenge.Nonce},
	}
	if err := child.Validate(); err != nil {
		return err
	}
	if err := s.memoryAuthStore.CreateLoginChallenge(ctx, challenge); err != nil {
		return err
	}
	s.loginRecords[child.StateDigest] = child
	return nil
}

type cidCallbackFixture struct {
	handler      http.Handler
	store        *cidCallbackStore
	yandex       *yandexTestProvider
	telegram     *callbackBindingProvider
	service      *identityAccountStub
	purposeCalls int
	proofCalls   int
}

func newCIDCallbackFixture(t *testing.T) *cidCallbackFixture {
	t.Helper()
	memory, subject := yandexKnownStore()
	telegramSubject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "777777"}
	memory.identities[telegramSubject] = domain.ExternalIdentity{Subject: telegramSubject, UserID: "usr_telegram_cid", CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	memory.memberships["usr_telegram_cid"] = []domain.TenantMembership{membership("ten_telegram_cid", "usr_telegram_cid", domain.TenantMembershipOwner)}
	f := &cidCallbackFixture{
		store:    &cidCallbackStore{memoryAuthStore: memory, loginRecords: make(map[domain.SecretDigest]domain.IdentityChallengeV2)},
		yandex:   &yandexTestProvider{subject: subject},
		telegram: &callbackBindingProvider{fakeProvider: fakeProvider{subject: telegramSubject.Subject}},
	}
	f.service = &identityAccountStub{
		purpose: func(_ context.Context, request ports.IdentityAccountCallbackPurposeRequestV2) (ports.IdentityAccountCallbackPurposeV2, error) {
			f.purposeCalls++
			child, found := f.store.loginRecords[domain.DigestSecret(request.State)]
			if !found || child.Validate() != nil || child.Purpose != domain.IdentityPurposeLoginV2 || child.Binding.CallbackURI != accountTestOrigin+request.CallbackPath || bffTestTime.Before(child.CreatedAt) || !bffTestTime.Before(child.ExpiresAt) {
				return 0, ports.IdentityAccountAccessDeniedV2
			}
			return ports.IdentityAccountCallbackLoginV2, nil
		},
		callback: func(context.Context, ports.IdentityAccountCallbackRequestV2) (ports.IdentityAccountOperationViewV2, error) {
			f.proofCalls++
			return ports.IdentityAccountOperationViewV2{}, ports.IdentityAccountAccessDeniedV2
		},
	}
	config := yandexTestConfig(f.store, f.yandex)
	config.LoginProviders = append(config.LoginProviders, webbff.LoginProviderConfig{Provider: domain.IdentityProviderTelegram, OIDCProvider: f.telegram, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}})
	config.IdentityAccounts = f.service
	config.Random = strings.NewReader(strings.Repeat("synthetic-cid-entropy-", 32))
	handler, err := webbff.New(config)
	if err != nil {
		t.Fatalf("construct CID callback fixture: %v", err)
	}
	f.handler = handler
	return f
}

func (f *cidCallbackFixture) start(t *testing.T, provider domain.IdentityProvider) (*http.Cookie, string) {
	t.Helper()
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, loginStartRequest(accountTestOrigin+"/auth/login/"+string(provider)+"/start?return_to=%2Fsessions"))
	u, err := url.Parse(w.Header().Get("Location"))
	if err != nil || w.Code != http.StatusSeeOther || u.Query().Get("state") == "" {
		t.Fatalf("start %s: status=%d location=%q err=%v", provider, w.Code, w.Header().Get("Location"), err)
	}
	state := u.Query().Get("state")
	child, found := f.store.loginRecords[domain.DigestSecret(state)]
	if !found || child.Validate() != nil || child.Purpose != domain.IdentityPurposeLoginV2 || child.Binding.Provider != provider {
		t.Fatalf("start %s did not persist its explicit valid LOGIN challenge", provider)
	}
	return responseCookie(t, w.Result(), webbff.LoginBindingCookieName), state
}

func cidCallbackRequest(path, state, cid string) *http.Request {
	query := url.Values{"state": {state}, "code": {"synthetic-authorization-code"}}
	if cid != "" {
		query.Set("cid", cid)
	}
	return httptest.NewRequest(http.MethodGet, accountTestOrigin+path+"?"+query.Encode(), nil)
}

func TestYandexCallbackCIDOrdinaryStoredLogin(t *testing.T) {
	for _, test := range []struct {
		name string
		cid  string
	}{
		{name: "absent"},
		{name: "opaque_not_oauth_client", cid: "j2SyntheticOpaqueMetadata"},
		{name: "not_an_owner_selector", cid: "usr_telegram_cid"},
		{name: "maximum_opaque_length", cid: strings.Repeat("a", 160)},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newCIDCallbackFixture(t)
			binding, state := f.start(t, domain.IdentityProviderYandex)
			challenge := f.store.challenges[domain.DigestSecret(state)]
			request := cidCallbackRequest(webcontract.RouteLoginCallback, state, test.cid)
			request.AddCookie(binding)
			w := httptest.NewRecorder()
			f.handler.ServeHTTP(w, request)
			if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/sessions" || f.purposeCalls != 1 || f.proofCalls != 0 || f.yandex.exchanges != 1 || f.telegram.exchanges != 0 || len(f.store.sessions) != 1 {
				t.Fatalf("ordinary cid callback: status=%d location=%q purpose=%d proof=%d yandex=%d telegram=%d sessions=%d", w.Code, w.Header().Get("Location"), f.purposeCalls, f.proofCalls, f.yandex.exchanges, f.telegram.exchanges, len(f.store.sessions))
			}
			if f.yandex.request.Code != "synthetic-authorization-code" || f.yandex.request.RedirectURI != accountTestOrigin+webcontract.RouteLoginCallback || f.yandex.request.PKCEVerifier != challenge.PKCEVerifier {
				t.Error("supplementary cid changed code, pinned callback or original PKCE verifier")
			}
			for _, session := range f.store.sessions {
				if session.UserID != "usr_yandex" || session.AuthenticatedSubject != f.yandex.subject {
					t.Errorf("cid changed canonical ownership: user=%q subject=%v", session.UserID, session.AuthenticatedSubject)
				}
			}
			responseCookie(t, w.Result(), webcontract.SessionCookieName)
			responseCookie(t, w.Result(), webcontract.CSRFCookieName)
			if challenge := f.store.challenges[domain.DigestSecret(state)]; challenge.ConsumedAt == nil {
				t.Error("ordinary callback did not consume the original browser-bound challenge")
			}
		})
	}
}

func TestYandexCallbackCIDRejectsBeforePurposeLookup(t *testing.T) {
	for _, test := range []struct {
		name     string
		provider domain.IdentityProvider
		change   func(url.Values)
	}{
		{name: "duplicate_same", change: func(q url.Values) { q.Add("cid", q.Get("cid")) }},
		{name: "duplicate_other", change: func(q url.Values) { q.Add("cid", "other-synthetic-cid") }},
		{name: "empty", change: func(q url.Values) { q.Set("cid", "") }},
		{name: "oversized", change: func(q url.Values) { q.Set("cid", strings.Repeat("a", 161)) }},
		{name: "whitespace", change: func(q url.Values) { q.Set("cid", "opaque cid") }},
		{name: "nonopaque", change: func(q url.Values) { q.Set("cid", "opaque/cid") }},
		{name: "unicode", change: func(q url.Values) { q.Set("cid", "opaque\u2603") }},
		{name: "foreign_telegram_callback", provider: domain.IdentityProviderTelegram},
		{name: "other_unknown", change: func(q url.Values) { q.Set("unrecognized", "synthetic") }},
		{name: "case_alias", change: func(q url.Values) { q.Del("cid"); q.Set("CID", "synthetic") }},
		{name: "duplicate_state", change: func(q url.Values) { q.Add("state", "other-state") }},
		{name: "duplicate_code", change: func(q url.Values) { q.Add("code", "other-code") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			provider := test.provider
			if provider == "" {
				provider = domain.IdentityProviderYandex
			}
			f := newCIDCallbackFixture(t)
			binding, state := f.start(t, provider)
			path := webcontract.RouteLoginCallback
			if provider == domain.IdentityProviderTelegram {
				path = webcontract.RouteOIDCCallback
			}
			request := cidCallbackRequest(path, state, "j2SyntheticOpaqueMetadata")
			query := request.URL.Query()
			if test.change != nil {
				test.change(query)
			}
			request.URL.RawQuery = query.Encode()
			request.AddCookie(binding)
			w := httptest.NewRecorder()
			f.handler.ServeHTTP(w, request)
			if w.Code != http.StatusForbidden || f.purposeCalls != 0 || f.proofCalls != 0 || f.yandex.exchanges != 0 || f.telegram.exchanges != 0 || len(f.store.sessions) != 0 || f.store.challenges[domain.DigestSecret(state)].ConsumedAt != nil {
				t.Fatalf("ambiguous cid callback reached authority: status=%d purpose=%d proof=%d yandex=%d telegram=%d sessions=%d", w.Code, f.purposeCalls, f.proofCalls, f.yandex.exchanges, f.telegram.exchanges, len(f.store.sessions))
			}
		})
	}
}

func TestYandexCallbackCIDPreservesStoredPurposeAndBrowserBinding(t *testing.T) {
	for _, name := range []string{"missing_state", "unknown_state", "expired_state", "missing_binding", "wrong_binding", "wrong_callback", "wrong_provider_binding", "replay"} {
		t.Run(name, func(t *testing.T) {
			f := newCIDCallbackFixture(t)
			binding, state := f.start(t, domain.IdentityProviderYandex)
			if name == "replay" {
				initial := cidCallbackRequest(webcontract.RouteLoginCallback, state, "j2SyntheticOpaqueMetadata")
				initial.AddCookie(binding)
				w := httptest.NewRecorder()
				f.handler.ServeHTTP(w, initial)
				if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/sessions" || f.yandex.exchanges != 1 {
					t.Fatalf("establish consumed challenge: status=%d location=%q exchanges=%d", w.Code, w.Header().Get("Location"), f.yandex.exchanges)
				}
			}
			request := cidCallbackRequest(webcontract.RouteLoginCallback, state, "j2SyntheticOpaqueMetadata")
			if name == "wrong_callback" {
				request.URL.Path = webcontract.RouteOIDCCallback
			}
			if name == "missing_state" || name == "unknown_state" {
				q := request.URL.Query()
				if name == "missing_state" {
					q.Del("state")
				} else {
					q.Set("state", "synthetic-unowned-state")
				}
				request.URL.RawQuery = q.Encode()
			}
			if name == "wrong_provider_binding" {
				challenge := f.store.challenges[domain.DigestSecret(state)]
				challenge.Audience = "foreign-oauth-client"
				f.store.challenges[domain.DigestSecret(state)] = challenge
			}
			if name == "expired_state" {
				child := f.store.loginRecords[domain.DigestSecret(state)]
				child.CreatedAt = bffTestTime.Add(-20 * time.Minute)
				child.ExpiresAt = bffTestTime.Add(-10 * time.Minute)
				child.DeleteAfter = child.ExpiresAt
				if err := child.Validate(); err != nil {
					t.Fatalf("expired fixture must retain a valid original lifetime: %v", err)
				}
				f.store.loginRecords[child.StateDigest] = child
			}
			if name != "missing_binding" {
				if name == "wrong_binding" {
					binding.Value = "other-browser-binding"
				}
				request.AddCookie(binding)
			}
			w := httptest.NewRecorder()
			f.handler.ServeHTTP(w, request)
			wantExchanges := 0
			wantLookups := 1
			if name == "replay" {
				wantExchanges = 1
				wantLookups = 2
			}
			wantStatus := http.StatusSeeOther
			if name == "missing_state" || name == "unknown_state" || name == "expired_state" || name == "wrong_callback" {
				wantStatus = http.StatusForbidden
			}
			if name == "missing_state" || name == "wrong_callback" {
				wantLookups = 0
			}
			if w.Code != wantStatus || f.purposeCalls != wantLookups || f.yandex.exchanges != wantExchanges || f.telegram.exchanges != 0 || f.proofCalls != 0 || len(f.store.sessions) != wantExchanges {
				t.Fatalf("cid bypassed %s: status=%d want=%d purpose=%d want=%d yandex=%d want=%d telegram=%d proof=%d sessions=%d", name, w.Code, wantStatus, f.purposeCalls, wantLookups, f.yandex.exchanges, wantExchanges, f.telegram.exchanges, f.proofCalls, len(f.store.sessions))
			}
			if wantStatus == http.StatusSeeOther && w.Header().Get("Location") != "/login?auth_error=access_denied" {
				t.Errorf("denial redirect=%q", w.Header().Get("Location"))
			}
		})
	}
}

func TestYandexCallbackCIDDoesNotAcceptProviderDenial(t *testing.T) {
	f := newCIDCallbackFixture(t)
	binding, state := f.start(t, domain.IdentityProviderYandex)
	request := cidCallbackRequest(webcontract.RouteLoginCallback, state, "j2SyntheticOpaqueMetadata")
	query := request.URL.Query()
	query.Del("code")
	query.Set("error", "access_denied")
	request.URL.RawQuery = query.Encode()
	request.AddCookie(binding)
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, request)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/login?auth_error=access_denied" || f.purposeCalls != 1 || f.proofCalls != 0 || f.yandex.exchanges != 0 || f.telegram.exchanges != 0 || len(f.store.sessions) != 0 {
		t.Fatalf("cid changed provider denial: status=%d location=%q purpose=%d proof=%d yandex=%d telegram=%d sessions=%d", w.Code, w.Header().Get("Location"), f.purposeCalls, f.proofCalls, f.yandex.exchanges, f.telegram.exchanges, len(f.store.sessions))
	}
}

func TestYandexCallbackCIDPreservesSensitiveProofDispatch(t *testing.T) {
	f := newCIDCallbackFixture(t)
	f.service.purpose = func(_ context.Context, request ports.IdentityAccountCallbackPurposeRequestV2) (ports.IdentityAccountCallbackPurposeV2, error) {
		f.purposeCalls++
		if request.CallbackPath != webcontract.RouteLoginCallback || request.State != "synthetic-proof-state" {
			t.Fatal("supplementary cid changed stored proof lookup input")
		}
		return ports.IdentityAccountCallbackProofV2, nil
	}
	f.service.callback = func(_ context.Context, request ports.IdentityAccountCallbackRequestV2) (ports.IdentityAccountOperationViewV2, error) {
		f.proofCalls++
		if request.Auth.SessionCookie != "synthetic-session" || request.Auth.OperationCookie != "synthetic-operation.synthetic-browser" || request.CallbackPath != webcontract.RouteLoginCallback || request.State != "synthetic-proof-state" || request.Code != "synthetic-authorization-code" || request.ProviderDenied {
			t.Fatal("supplementary cid changed original sensitive proof authority")
		}
		return pendingAccountView(), nil
	}
	request := cidCallbackRequest(webcontract.RouteLoginCallback, "synthetic-proof-state", "j2SyntheticOpaqueMetadata")
	request.AddCookie(&http.Cookie{Name: webcontract.SessionCookieName, Value: "synthetic-session"})
	request.AddCookie(&http.Cookie{Name: accountTestOperationCookie, Value: "synthetic-operation.synthetic-browser"})
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, request)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/account/login-methods?operation_id=opaque-operation" || f.purposeCalls != 1 || f.proofCalls != 1 || f.yandex.exchanges != 0 || f.telegram.exchanges != 0 || len(f.store.sessions) != 0 {
		t.Fatalf("cid proof dispatch: status=%d location=%q purpose=%d proof=%d yandex=%d telegram=%d sessions=%d", w.Code, w.Header().Get("Location"), f.purposeCalls, f.proofCalls, f.yandex.exchanges, f.telegram.exchanges, len(f.store.sessions))
	}
}

func TestYandexCallbackCIDDoesNotChangeTelegramLogin(t *testing.T) {
	f := newCIDCallbackFixture(t)
	binding, state := f.start(t, domain.IdentityProviderTelegram)
	request := cidCallbackRequest(webcontract.RouteOIDCCallback, state, "")
	request.AddCookie(binding)
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, request)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/sessions" || f.purposeCalls != 1 || f.proofCalls != 0 || f.yandex.exchanges != 0 || f.telegram.exchanges != 1 || len(f.store.sessions) != 1 {
		t.Fatalf("unchanged Telegram callback: status=%d location=%q purpose=%d proof=%d yandex=%d telegram=%d sessions=%d", w.Code, w.Header().Get("Location"), f.purposeCalls, f.proofCalls, f.yandex.exchanges, f.telegram.exchanges, len(f.store.sessions))
	}
}
