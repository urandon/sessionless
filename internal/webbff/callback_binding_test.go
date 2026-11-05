package webbff_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

type callbackBindingProvider struct {
	fakeProvider
	exchanges int
	request   ports.OIDCTokenRequest
}

func (p *callbackBindingProvider) ExchangeAndVerify(ctx context.Context, request ports.OIDCTokenRequest) (domain.OIDCIdentityClaims, error) {
	p.exchanges++
	p.request = request
	return p.fakeProvider.ExchangeAndVerify(ctx, request)
}

func TestTelegramBoundChallengeRequiresExactCallbackBeforeExchange(t *testing.T) {
	for _, test := range []struct {
		name   string
		path   string
		mutate func(*domain.OIDCLoginChallenge)
		accept bool
	}{
		{name: "exact_telegram_callback", path: webcontract.RouteOIDCCallback, accept: true},
		{name: "wrong_generic_callback", path: webcontract.RouteLoginCallback},
		{name: "wrong_client", path: webcontract.RouteOIDCCallback, mutate: func(c *domain.OIDCLoginChallenge) { c.Audience = "other-client" }},
		{name: "wrong_issuer", path: webcontract.RouteOIDCCallback, mutate: func(c *domain.OIDCLoginChallenge) { c.Issuer = "https://other.example" }},
		{name: "wrong_provider", path: webcontract.RouteOIDCCallback, mutate: func(c *domain.OIDCLoginChallenge) {
			c.Provider, c.Issuer, c.Nonce = domain.IdentityProviderYandex, domain.YandexOAuthIssuer, ""
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			const origin = "https://web.dev.sessionless.triborg.dev"
			store := newMemoryAuthStore()
			subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "424242"}
			const user = domain.UserID("usr_existing_telegram")
			store.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: user, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
			store.memberships[user] = []domain.TenantMembership{membership("ten_alpha", user, domain.TenantMembershipOwner)}
			provider := &callbackBindingProvider{fakeProvider: fakeProvider{subject: subject.Subject}}
			handler, err := webbff.New(webbff.Config{LoginProviders: []webbff.LoginProviderConfig{{Provider: domain.IdentityProviderTelegram, OIDCProvider: provider, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}}},
				BaseURL: origin,
				Store:   store,
				IDs:     fixedIDs{},
				Clock:   fixedClock{now: bffTestTime},
				Logger:  slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				t.Fatal(err)
			}
			start := httptest.NewRecorder()
			handler.ServeHTTP(start, loginStartRequest(origin+webcontract.RouteTelegramLoginStart+"?return_to=%2Fsessions"))
			location, err := url.Parse(start.Header().Get("Location"))
			if err != nil || start.Code != http.StatusSeeOther || location.Query().Get("state") == "" {
				t.Fatalf("start did not issue challenge: status=%d err=%v", start.Code, err)
			}
			state := location.Query().Get("state")
			binding := responseCookie(t, start.Result(), webbff.LoginBindingCookieName)
			digest := domain.DigestSecret(state)
			challenge := store.challenges[digest]
			if challenge.Provider != domain.IdentityProviderTelegram || challenge.Audience != "100000" || challenge.Issuer != "https://oauth.telegram.org" {
				t.Fatal("setup lacks genuine explicit Telegram binding")
			}
			if test.mutate != nil {
				test.mutate(&challenge)
				store.challenges[digest] = challenge
			}
			if err := challenge.Validate(); err != nil {
				t.Fatalf("test must reach callback-binding check with valid challenge: %v", err)
			}
			callback := func(path string) *httptest.ResponseRecorder {
				request := httptest.NewRequest(http.MethodGet, origin+path+"?code=synthetic-code&state="+url.QueryEscape(state), nil)
				request.AddCookie(binding)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				return response
			}
			response := callback(test.path)
			wantLocation, wantExchanges := "/login?auth_error=access_denied", 0
			if test.accept {
				wantLocation, wantExchanges = "/sessions", 1
			}
			if response.Code != http.StatusSeeOther || response.Header().Get("Location") != wantLocation || provider.exchanges != wantExchanges || len(store.sessions) != wantExchanges {
				t.Fatalf("callback path=%s status=%d location=%q exchanges=%d sessions=%d wantExchanges=%d", test.path, response.Code, response.Header().Get("Location"), provider.exchanges, len(store.sessions), wantExchanges)
			}
			if store.challenges[digest].ConsumedAt == nil {
				t.Error("callback did not burn one-use state")
			}
			if !test.accept {
				for _, cookie := range response.Result().Cookies() {
					if (cookie.Name == webcontract.SessionCookieName || cookie.Name == webcontract.CSRFCookieName) && cookie.Value != "" {
						t.Error("denial issued authentication cookies")
					}
				}
			} else {
				if provider.request.RedirectURI != origin+webcontract.RouteOIDCCallback || provider.request.PKCEVerifier != challenge.PKCEVerifier || provider.request.ExpectedNonce != challenge.Nonce || provider.request.Policy.Issuer != challenge.Issuer || provider.request.Policy.Audience != challenge.Audience {
					t.Error("correct callback lost exact configured exchange binding")
				}
				for _, session := range store.sessions {
					if session.UserID != user {
						t.Error("login changed canonical existing account")
					}
				}
			}
			if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Error("callback lost existing security headers")
			}
			for _, secret := range []string{"synthetic-code", state, binding.Value, challenge.PKCEVerifier, challenge.Nonce} {
				if secret != "" && strings.Contains(response.Body.String()+response.Header().Get("Location"), secret) {
					t.Error("callback response disclosed exchange input")
				}
			}
			if replay := callback(webcontract.RouteOIDCCallback); replay.Header().Get("Location") != "/login?auth_error=access_denied" || provider.exchanges != wantExchanges || len(store.sessions) != wantExchanges {
				t.Error("correct-route replay reused consumed challenge")
			}
		})
	}
}
