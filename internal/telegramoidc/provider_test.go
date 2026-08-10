package telegramoidc_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/oidcfixture"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
)

func TestLocalFixtureAndProviderAuthorizationCodeFlow(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	issuer := "https://127.0.0.1"
	redirectURI := "https://web.localhost/auth/telegram/callback"
	fixture, err := oidcfixture.New(oidcfixture.Config{
		Environment: "local", Issuer: issuer,
		ClientID: "100000", ClientSecret: "fixture-secret",
		RedirectURI: redirectURI, Subject: "424242", Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	providerClient := &http.Client{Transport: handlerTransport{handler: fixture}}
	provider, err := telegramoidc.New(telegramoidc.Config{
		Issuer: issuer, AuthorizationEndpoint: issuer + "/auth",
		TokenEndpoint: issuer + "/token", JWKSURL: issuer + "/.well-known/jwks.json",
		ClientID: "100000", ClientSecret: "fixture-secret", RedirectURI: redirectURI,
		AllowedAlgorithms: []string{"RS256"}, AllowLoopbackProvider: true,
		HTTPClient: providerClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	verifier := strings.Repeat("a", 43)
	challenge := "Z6g5qV9jYgX0j2NqW8mQnY5QglW3IVbvBz7GNWJ6E1c"
	authorizationURL, err := provider.AuthorizationURL(context.Background(), ports.OIDCAuthorizationRequest{
		Provider: domain.IdentityProviderTelegram, RedirectURI: redirectURI,
		State: "state-value", Nonce: "nonce-value", CodeChallenge: challenge,
		Scopes: []string{"openid", "profile"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Replace the manually supplied challenge with the verifier's actual S256
	// value so the local fixture proves PKCE instead of merely returning a JWT.
	parsed, _ := url.Parse(authorizationURL)
	query := parsed.Query()
	query.Set("code_challenge", pkceChallenge(verifier))
	parsed.RawQuery = query.Encode()
	authorizationRequest := httptest.NewRequest(http.MethodGet, parsed.String(), nil)
	authorizationResponse := httptest.NewRecorder()
	fixture.ServeHTTP(authorizationResponse, authorizationRequest)
	if authorizationResponse.Code != http.StatusSeeOther {
		t.Fatalf("authorization status = %d", authorizationResponse.Code)
	}
	callback, _ := url.Parse(authorizationResponse.Header().Get("Location"))
	claims, err := provider.ExchangeAndVerify(context.Background(), ports.OIDCTokenRequest{
		Provider: domain.IdentityProviderTelegram, Code: callback.Query().Get("code"),
		RedirectURI: redirectURI, PKCEVerifier: verifier, ExpectedNonce: "nonce-value",
		Policy: domain.OIDCVerificationPolicy{
			Issuer: issuer, Audience: "100000", AllowedAlgorithms: []string{"RS256"},
		},
		Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claims.Subject != "424242" || claims.Nonce != "nonce-value" {
		t.Fatalf("claims = %+v", claims)
	}
	_, err = provider.ExchangeAndVerify(context.Background(), ports.OIDCTokenRequest{
		Provider: domain.IdentityProviderTelegram, Code: callback.Query().Get("code"),
		RedirectURI: redirectURI, PKCEVerifier: verifier, ExpectedNonce: "nonce-value",
		Policy: domain.OIDCVerificationPolicy{Issuer: issuer, Audience: "100000", AllowedAlgorithms: []string{"RS256"}},
		Now:    now,
	})
	if !errors.Is(err, telegramoidc.ErrProviderResponse) {
		t.Fatalf("authorization-code replay error = %v", err)
	}
}

func TestFixtureRefusesCloudModes(t *testing.T) {
	_, err := oidcfixture.New(oidcfixture.Config{Environment: "cloud-dev"})
	if err == nil {
		t.Fatal("cloud-dev fixture unexpectedly started")
	}
}

func TestProviderRefusesEndpointOverridesOutsideLocalEnvironment(t *testing.T) {
	_, err := telegramoidc.New(telegramoidc.Config{
		Issuer: "https://provider.invalid", AuthorizationEndpoint: "https://provider.invalid/auth",
		TokenEndpoint: "https://provider.invalid/token", JWKSURL: "https://provider.invalid/jwks",
		ClientID: "100000", ClientSecret: "secret",
		RedirectURI: "https://web.example/auth/telegram/callback", AllowedAlgorithms: []string{"RS256"},
	})
	if err == nil {
		t.Fatal("non-local provider override unexpectedly passed validation")
	}
}

func TestProviderRejectsWrongPKCEAndUntrustedSigningKey(t *testing.T) {
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	issuer := "https://oidc.localhost"
	redirectURI := "https://web.localhost/auth/telegram/callback"
	verifier := strings.Repeat("v", 43)

	newFixture := func(t *testing.T) *oidcfixture.Server {
		t.Helper()
		fixture, err := oidcfixture.New(oidcfixture.Config{
			Environment: "local", Issuer: issuer, ClientID: "100000",
			ClientSecret: "fixture-secret", RedirectURI: redirectURI,
			Subject: "424242", Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		return fixture
	}
	newProvider := func(t *testing.T, transport http.RoundTripper) *telegramoidc.Provider {
		t.Helper()
		provider, err := telegramoidc.New(telegramoidc.Config{
			Issuer: issuer, AuthorizationEndpoint: issuer + "/auth",
			TokenEndpoint: issuer + "/token", JWKSURL: issuer + "/.well-known/jwks.json",
			ClientID: "100000", ClientSecret: "fixture-secret", RedirectURI: redirectURI,
			AllowedAlgorithms: []string{"RS256"}, AllowLoopbackProvider: true,
			HTTPClient: &http.Client{Transport: transport},
		})
		if err != nil {
			t.Fatal(err)
		}
		return provider
	}

	t.Run("wrong PKCE verifier", func(t *testing.T) {
		fixture := newFixture(t)
		provider := newProvider(t, handlerTransport{handler: fixture})
		code := issueCode(t, fixture, issuer, redirectURI, verifier)
		_, err := provider.ExchangeAndVerify(context.Background(), tokenRequest(code, strings.Repeat("x", 43), issuer, redirectURI, now))
		if !errors.Is(err, telegramoidc.ErrProviderResponse) {
			t.Fatalf("wrong PKCE error = %v", err)
		}
	})

	t.Run("JWKS does not trust the signing key", func(t *testing.T) {
		fixture := newFixture(t)
		provider := newProvider(t, filteringTransport{handler: fixture, emptyJWKS: true})
		code := issueCode(t, fixture, issuer, redirectURI, verifier)
		_, err := provider.ExchangeAndVerify(context.Background(), tokenRequest(code, verifier, issuer, redirectURI, now))
		if !errors.Is(err, telegramoidc.ErrProviderResponse) {
			t.Fatalf("untrusted signing key error = %v", err)
		}
	})
}

func issueCode(t *testing.T, fixture http.Handler, issuer, redirectURI, verifier string) string {
	t.Helper()
	authorization, _ := url.Parse(issuer + "/auth")
	query := authorization.Query()
	query.Set("client_id", "100000")
	query.Set("redirect_uri", redirectURI)
	query.Set("response_type", "code")
	query.Set("scope", "openid profile")
	query.Set("state", "state-value")
	query.Set("nonce", "nonce-value")
	query.Set("code_challenge", pkceChallenge(verifier))
	query.Set("code_challenge_method", "S256")
	authorization.RawQuery = query.Encode()
	response := httptest.NewRecorder()
	fixture.ServeHTTP(response, httptest.NewRequest(http.MethodGet, authorization.String(), nil))
	if response.Code != http.StatusSeeOther {
		t.Fatalf("authorization status = %d body=%s", response.Code, response.Body.String())
	}
	callback, _ := url.Parse(response.Header().Get("Location"))
	return callback.Query().Get("code")
}

func tokenRequest(code, verifier, issuer, redirectURI string, now time.Time) ports.OIDCTokenRequest {
	return ports.OIDCTokenRequest{
		Provider: domain.IdentityProviderTelegram, Code: code, RedirectURI: redirectURI,
		PKCEVerifier: verifier, ExpectedNonce: "nonce-value", Now: now,
		Policy: domain.OIDCVerificationPolicy{
			Issuer: issuer, Audience: "100000", AllowedAlgorithms: []string{"RS256"},
		},
	}
}

func pkceChallenge(verifier string) string {
	digest := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

type handlerTransport struct{ handler http.Handler }

func (transport handlerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	transport.handler.ServeHTTP(recorder, request)
	return recorder.Result(), nil
}

type filteringTransport struct {
	handler   http.Handler
	emptyJWKS bool
}

func (transport filteringTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport.emptyJWKS && request.URL.Path == "/.well-known/jwks.json" {
		response := httptest.NewRecorder()
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusOK)
		_, _ = response.WriteString(`{"keys":[]}`)
		return response.Result(), nil
	}
	return handlerTransport{handler: transport.handler}.RoundTrip(request)
}
