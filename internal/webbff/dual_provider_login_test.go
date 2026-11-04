package webbff_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

// Test-only migration of explicit URL fixture DATA to the canonical POST form;
// the production handler has no query/GET compatibility path.
func loginStartRequest(raw string) *http.Request {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	body := u.Query().Encode()
	u.RawQuery = ""
	r := httptest.NewRequest(http.MethodPost, u.String(), strings.NewReader(body))
	r.Header.Set("Origin", u.Scheme+"://"+u.Host)
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}
func TestDualProviderLoginClosedConfigurationAndReadOnlyMethods(t *testing.T) {
	for _, mode := range []string{"yandex", "telegram", "yandex,telegram"} {
		t.Run(mode, func(t *testing.T) {
			store := newMemoryAuthStore()
			yp := &yandexTestProvider{}
			tp := &callbackBindingProvider{fakeProvider: fakeProvider{subject: "424242"}}
			configs := []webbff.LoginProviderConfig{}
			if strings.Contains(mode, "yandex") {
				configs = append(configs, webbff.LoginProviderConfig{Provider: domain.IdentityProviderYandex, OAuthProvider: yp, OAuthClientID: "yandex-client"})
			}
			if strings.Contains(mode, "telegram") {
				configs = append(configs, webbff.LoginProviderConfig{Provider: domain.IdentityProviderTelegram, OIDCProvider: tp, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}})
			}
			config := webbff.Config{BaseURL: "https://web.dev.sessionless.triborg.dev", LoginProviders: configs, Store: store, IDs: fixedIDs{}, Clock: fixedClock{now: bffTestTime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			h, err := webbff.New(config)
			if err != nil {
				t.Fatal("valid configuration", err)
			}
			// A later caller mutation cannot change the handler's admitted list.
			configs[0].Provider = "foreign"
			response := httptest.NewRecorder()
			h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, config.BaseURL+webcontract.RouteLoginMethods, nil))
			var got webcontract.LoginMethods
			if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &got) != nil || len(got.Methods) != len(configs) || len(store.challenges) != 0 || len(store.sessions) != 0 || yp.exchanges != 0 || tp.exchanges != 0 {
				t.Fatal("metadata performed effect", response.Code)
			}
			for i, m := range got.Methods {
				want := []string{"yandex", "telegram"}
				if mode == "telegram" {
					want = []string{"telegram"}
				}
				if string(m.Provider) != want[i] || m.StartPath != "/auth/login/"+want[i]+"/start" {
					t.Fatalf("wrong closed metadata: %+v", got)
				}
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("cacheable methods")
			}
			for _, old := range []string{"/auth/login/start", "/auth/telegram/start"} {
				r := httptest.NewRecorder()
				h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, config.BaseURL+old, nil))
				if r.Code != 404 {
					t.Fatal("legacy GET start retained", old, r.Code)
				}
			}
		})
	}
}
func TestDualProviderLoginBothRoundTripsAndProviderAttribution(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(provider), func(t *testing.T) {
			store := newMemoryAuthStore()
			for _, p := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
				subject := domain.ExternalSubject{Provider: p, Subject: "424242"}
				user := domain.UserID("owner-" + string(p))
				store.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: user, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
				store.memberships[user] = []domain.TenantMembership{membership(domain.TenantID("tenant-"+string(p)), user, domain.TenantMembershipOwner)}
			}
			yp := &yandexTestProvider{subject: domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "424242"}}
			tp := &callbackBindingProvider{fakeProvider: fakeProvider{subject: "424242"}}
			h, err := webbff.New(webbff.Config{BaseURL: "https://web.dev.sessionless.triborg.dev", LoginProviders: []webbff.LoginProviderConfig{{Provider: domain.IdentityProviderYandex, OAuthProvider: yp, OAuthClientID: "yandex-client"}, {Provider: domain.IdentityProviderTelegram, OIDCProvider: tp, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}}}, Store: store, IDs: fixedIDs{}, Clock: fixedClock{now: bffTestTime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				t.Fatal(err)
			}
			start := httptest.NewRecorder()
			h.ServeHTTP(start, loginStartRequest("https://web.dev.sessionless.triborg.dev/auth/login/"+string(provider)+"/start?return_to=%2Fsessions"))
			if start.Code != 303 {
				t.Fatal("start", start.Code)
			}
			u, _ := url.Parse(start.Header().Get("Location"))
			state := u.Query().Get("state")
			binding := responseCookie(t, start.Result(), webbff.LoginBindingCookieName)
			path := webcontract.RouteLoginCallback
			if provider == domain.IdentityProviderTelegram {
				path = webcontract.RouteOIDCCallback
			}
			request := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+path+"?code=controlled&state="+url.QueryEscape(state), nil)
			request.AddCookie(binding)
			done := httptest.NewRecorder()
			h.ServeHTTP(done, request)
			if done.Code != 303 || done.Header().Get("Location") != "/sessions" || len(store.sessions) != 1 {
				t.Fatal("valid provider roundtrip", done.Code, done.Header().Get("Location"))
			}
			for _, session := range store.sessions {
				if session.UserID != domain.UserID("owner-"+string(provider)) || session.AuthenticatedSubject.Provider != provider {
					t.Fatal("cross-provider numeric subject collision", session.UserID)
				}
			}
			session := responseCookie(t, done.Result(), webcontract.SessionCookieName)
			csrf := responseCookie(t, done.Result(), webcontract.CSRFCookieName)
			bad := tenantSwitchRequest(session, csrf, "tenant-"+string(provider), "wrong")
			denied := httptest.NewRecorder()
			h.ServeHTTP(denied, bad)
			events := store.recordedSecurityEvents()
			if len(events) != 1 || events[0].Provider != provider {
				t.Fatal("audit used global/default provider", events)
			}
		})
	}
}
func TestDualProviderLoginStartRejectsBeforeChallenge(t *testing.T) {
	for _, name := range []string{"missing_origin", "foreign_origin", "duplicate_origin", "duplicate_return", "unknown_field", "query_alias", "json", "oversize"} {
		t.Run(name, func(t *testing.T) {
			store := newMemoryAuthStore()
			h := newYandexTestHandler(t, store, &yandexTestProvider{})
			request := loginStartRequest("https://web.dev.sessionless.triborg.dev/auth/login/yandex/start?return_to=%2Fsessions")
			switch name {
			case "missing_origin":
				request.Header.Del("Origin")
			case "foreign_origin":
				request.Header.Set("Origin", "https://other.example")
			case "duplicate_origin":
				request.Header.Add("Origin", "https://web.dev.sessionless.triborg.dev")
			case "duplicate_return":
				request.Body = io.NopCloser(strings.NewReader("return_to=%2F&return_to=%2Fsessions"))
			case "unknown_field":
				request.Body = io.NopCloser(strings.NewReader("issuer=foreign"))
			case "query_alias":
				request.URL.RawQuery = "return_to=%2F"
			case "json":
				request.Header.Set("Content-Type", "application/json")
			case "oversize":
				request.Body = io.NopCloser(strings.NewReader("return_to=%2F" + strings.Repeat("a", 65<<10)))
			}
			response := httptest.NewRecorder()
			h.ServeHTTP(response, request)
			if response.Code < 400 || len(store.challenges) != 0 || len(store.sessions) != 0 {
				t.Fatal("invalid start produced authority", name, response.Code)
			}
		})
	}
}

func TestDualProviderLoginCrossCallbackWithBothInstalled(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(provider), func(t *testing.T) {
			store := newMemoryAuthStore()
			yp := &yandexTestProvider{}
			tp := &callbackBindingProvider{fakeProvider: fakeProvider{subject: "424242"}}
			h, err := webbff.New(webbff.Config{BaseURL: "https://web.dev.sessionless.triborg.dev", LoginProviders: []webbff.LoginProviderConfig{{Provider: domain.IdentityProviderYandex, OAuthProvider: yp, OAuthClientID: "yandex-client"}, {Provider: domain.IdentityProviderTelegram, OIDCProvider: tp, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}}}, Store: store, IDs: fixedIDs{}, Clock: fixedClock{now: bffTestTime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				t.Fatal(err)
			}
			start := httptest.NewRecorder()
			h.ServeHTTP(start, loginStartRequest("https://web.dev.sessionless.triborg.dev/auth/login/"+string(provider)+"/start"))
			if start.Code != 303 {
				t.Fatal("genuine bound start", start.Code)
			}
			location, _ := url.Parse(start.Header().Get("Location"))
			state := location.Query().Get("state")
			challenge := store.challenges[domain.DigestSecret(state)]
			if challenge.Validate() != nil || challenge.Provider != provider {
				t.Fatal("missing valid source binding")
			}
			binding := responseCookie(t, start.Result(), webbff.LoginBindingCookieName)
			wrong, right := webcontract.RouteOIDCCallback, webcontract.RouteLoginCallback
			if provider == domain.IdentityProviderTelegram {
				wrong, right = right, wrong
			}
			for _, path := range []string{wrong, right} {
				request := httptest.NewRequest(http.MethodGet, "https://web.dev.sessionless.triborg.dev"+path+"?state="+url.QueryEscape(state)+"&code=controlled", nil)
				request.AddCookie(binding)
				response := httptest.NewRecorder()
				h.ServeHTTP(response, request)
				if response.Header().Get("Location") != "/login?auth_error=access_denied" || yp.exchanges != 0 || tp.exchanges != 0 || len(store.sessions) != 0 {
					t.Fatal("cross-callback or replay escaped binding", path, response.Code)
				}
			}
			if store.challenges[domain.DigestSecret(state)].ConsumedAt == nil {
				t.Fatal("wrong callback failed to burn state")
			}
		})
	}
}

func TestDualProviderLoginRejectsInvalidListsWithoutInference(t *testing.T) {
	valid := webbff.LoginProviderConfig{Provider: domain.IdentityProviderYandex, OAuthProvider: &yandexTestProvider{}, OAuthClientID: "yandex-client"}
	for _, providers := range [][]webbff.LoginProviderConfig{nil, {}, {{OAuthProvider: &yandexTestProvider{}, OAuthClientID: "yandex-client"}}, {valid, valid}, {valid, valid, valid}, {{Provider: domain.IdentityProviderTelegram, OIDCProvider: fakeProvider{subject: "1"}, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}}, valid}} {
		config := yandexTestConfig(newMemoryAuthStore(), &yandexTestProvider{})
		config.LoginProviders = providers
		if _, err := webbff.New(config); err == nil {
			t.Fatal("invalid provider list inferred/accepted", len(providers))
		}
	}
}
