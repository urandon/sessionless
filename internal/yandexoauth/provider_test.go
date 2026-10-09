package yandexoauth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const testCallback = "https://web.example.invalid/auth/login/callback"
const testVerifier = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ"

func TestYandexAuthorizationUsesOnlyPinnedCodePKCE(t *testing.T) {
	p, err := New(Config{ClientID: "client-test", ClientSecret: "test-secret", RedirectURI: testCallback})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(testVerifier))
	u, err := p.AuthorizationURL(context.Background(), ports.OAuthAuthorizationRequest{RedirectURI: testCallback, State: "state-test", CodeChallenge: base64.RawURLEncoding.EncodeToString(digest[:])})
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(u)
	q := parsed.Query()
	if parsed.Scheme+"://"+parsed.Host+parsed.Path != DefaultAuthorizationEndpoint || q.Get("scope") != "login:info" || q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" || q.Get("state") != "state-test" || q.Get("redirect_uri") != testCallback || q.Has("client_secret") || q.Has("nonce") {
		t.Fatalf("authorization shape = %s", u)
	}
	if _, err = p.AuthorizationURL(context.Background(), ports.OAuthAuthorizationRequest{RedirectURI: testCallback, State: "state-test", CodeChallenge: "plain"}); err == nil {
		t.Fatal("plain/invalid challenge accepted")
	}
}

func TestYandexExchangeBindsAccountToClientWithoutExposingToken(t *testing.T) {
	var infoCalls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			id, secret, ok := r.BasicAuth()
			if !ok || id != "client-test" || secret != "test-secret" {
				t.Error("missing private client authentication")
			}
			if err := r.ParseForm(); err != nil || r.PostForm.Get("code_verifier") != testVerifier || r.PostForm.Get("code") != "one-code" || r.PostForm.Get("grant_type") != "authorization_code" || r.PostForm.Get("redirect_uri") != testCallback {
				t.Error("token request binding incorrect")
			}
			fmt.Fprint(w, `{"access_token":"private-access-token","token_type":"bearer","expires_in":3600,"refresh_token":"discard-me"}`)
		case "/info":
			infoCalls.Add(1)
			if r.Header.Get("Authorization") != "OAuth private-access-token" || r.URL.RawQuery != "format=json" || r.Method != http.MethodGet {
				t.Error("profile bearer must be header-only")
			}
			fmt.Fprint(w, `{"id":"424242","client_id":"client-test","email":"must-not-link@example.invalid","login":"not-authority"}`)
		default:
			t.Error("unexpected request")
		}
	}))
	t.Cleanup(s.Close)
	p := testProvider(t, s.URL)
	got, err := p.ExchangeAndVerify(context.Background(), ports.OAuthTokenRequest{Code: "one-code", RedirectURI: testCallback, PKCEVerifier: testVerifier})
	if err != nil || got != (domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "424242"}) || infoCalls.Load() != 1 {
		t.Fatalf("identity=%+v infoCalls=%d err=%v", got, infoCalls.Load(), err)
	}
}

func TestYandexExchangeRejectsUntrustedResponses(t *testing.T) {
	validToken := `{"access_token":"private-access-token","token_type":"bearer","expires_in":3600}`
	for _, tc := range []struct {
		name, token, info string
		status            int
	}{
		{"client mismatch", validToken, `{"id":"424242","client_id":"other-client"}`, 200},
		{"missing client", validToken, `{"id":"424242"}`, 200},
		{"numeric id wrong type", validToken, `{"id":424242,"client_id":"client-test"}`, 200},
		{"username id", validToken, `{"id":"alice","client_id":"client-test"}`, 200},
		{"duplicate id", validToken, `{"id":"1","id":"2","client_id":"client-test"}`, 200},
		{"trailing profile", validToken, `{"id":"1","client_id":"client-test"} {}`, 200},
		{"oversized profile", validToken, `{"padding":"` + strings.Repeat("a", maxResponseBytes) + `"}`, 200},
		{"revoked token", validToken, `{"error":"private-detail"}`, 401},
		{"token expired", `{"access_token":"private-access-token","token_type":"bearer","expires_in":0}`, `{}`, 200},
		{"token duplicate", `{"access_token":"one","access_token":"two","token_type":"bearer","expires_in":1}`, `{}`, 200},
		{"wrong token type", `{"access_token":"private-access-token","token_type":"jwt","expires_in":1}`, `{}`, 200},
		{"token whitespace", `{"access_token":"secret\nvalue","token_type":"bearer","expires_in":1}`, `{}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/token" {
					fmt.Fprint(w, tc.token)
				} else {
					w.WriteHeader(tc.status)
					fmt.Fprint(w, tc.info)
				}
			}))
			t.Cleanup(s.Close)
			got, err := testProvider(t, s.URL).ExchangeAndVerify(context.Background(), ports.OAuthTokenRequest{Code: "code", RedirectURI: testCallback, PKCEVerifier: testVerifier})
			if !errors.Is(err, ErrProviderResponse) || got != (domain.ExternalSubject{}) {
				t.Fatalf("accepted untrusted identity=%+v err=%v", got, err)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("error leaked provider payload")
			}
		})
	}
}

func TestYandexNeverFollowsRedirectOrRetriesToken(t *testing.T) {
	var destinationCalls atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { destinationCalls.Add(1) }))
	t.Cleanup(destination.Close)
	for _, path := range []string{"/token", "/info"} {
		t.Run(path, func(t *testing.T) {
			var tokenCalls atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/token" {
					tokenCalls.Add(1)
				}
				if r.URL.Path == path {
					http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
					return
				}
				fmt.Fprint(w, `{"access_token":"token","token_type":"bearer","expires_in":1}`)
			}))
			t.Cleanup(s.Close)
			_, err := testProvider(t, s.URL).ExchangeAndVerify(context.Background(), ports.OAuthTokenRequest{Code: "code", RedirectURI: testCallback, PKCEVerifier: testVerifier})
			if !errors.Is(err, ErrProviderResponse) || tokenCalls.Load() != 1 || destinationCalls.Load() != 0 {
				t.Fatalf("redirect/retry err=%v token=%d destination=%d", err, tokenCalls.Load(), destinationCalls.Load())
			}
		})
	}
}

func TestYandexPinnedConfigAndCanceledExchange(t *testing.T) {
	for _, tc := range []Config{
		{ClientID: "client", ClientSecret: "secret", RedirectURI: testCallback, TokenEndpoint: "https://evil.invalid/token"},
		{ClientID: "client", ClientSecret: "secret", RedirectURI: testCallback, AllowLoopbackProvider: true, InfoEndpoint: "http://evil.invalid/info"},
		{ClientID: "client", ClientSecret: "secret", RedirectURI: "http://web.localhost/auth/login/callback"},
	} {
		if _, err := New(tc); err == nil {
			t.Fatal("unsafe config accepted")
		}
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("canceled callback made network request") }))
	t.Cleanup(s.Close)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := testProvider(t, s.URL).ExchangeAndVerify(ctx, ports.OAuthTokenRequest{Code: "code", RedirectURI: testCallback, PKCEVerifier: testVerifier}); !errors.Is(err, ErrProviderResponse) {
		t.Fatalf("canceled exchange err=%v", err)
	}
}

func testProvider(t *testing.T, origin string) *Provider {
	t.Helper()
	p, err := New(Config{ClientID: "client-test", ClientSecret: "test-secret", RedirectURI: testCallback, AuthorizationEndpoint: origin + "/authorize", TokenEndpoint: origin + "/token", InfoEndpoint: origin + "/info", AllowLoopbackProvider: true})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
