package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const identityRuntimeTestOrigin = "https://web.example.invalid"

func runtimeLoginConfig(t *testing.T, mode string) webLoginConfig {
	t.Helper()
	login, err := webLoginFromEnvironment(loginConfigLookup(completeLoginEnvironment(mode)), identityRuntimeTestOrigin, false)
	if err != nil {
		t.Fatal("construct ordinary provider adapters", err)
	}
	return login
}

func runtimeConfigBindings(config webIdentityConfig) []domain.IdentityProviderBindingV2 {
	bindings := make([]domain.IdentityProviderBindingV2, 0, len(config.providers))
	for _, provider := range config.providers {
		bindings = append(bindings, provider.Binding)
	}
	return bindings
}

func TestWebIdentityRuntimePairsActualOrdinaryAdapters(t *testing.T) {
	for _, mode := range []string{"yandex", "telegram", "yandex,telegram"} {
		t.Run(mode, func(t *testing.T) {
			login := runtimeLoginConfig(t, mode)
			config, err := webIdentityFromEnvironment(loginConfigLookup(map[string]string{"WEB_IDENTITY_VERSION": "2", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "true"}), login, identityRuntimeTestOrigin, []byte(strings.Repeat("k", 32)))
			if err != nil || config.version != "2" || !config.sensitive || len(config.providers) != len(login.providers) {
				t.Fatalf("mode=%s config=%+v err=%v", mode, config, err)
			}
			at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			for i, provider := range config.providers {
				ordinary := login.providers[i]
				if provider.Binding.Provider != ordinary.Provider {
					t.Fatalf("index=%d ordinary=%s proof=%s", i, ordinary.Provider, provider.Binding.Provider)
				}
				// The interface conversion must preserve the exact concrete pointer,
				// not build a separate client with potentially divergent credentials.
				var actual ports.IdentityProviderProverV1
				if ordinary.Provider == domain.IdentityProviderYandex {
					actual = ordinary.OAuthProvider.(ports.IdentityProviderProverV1)
				} else {
					actual = ordinary.OIDCProvider.(ports.IdentityProviderProverV1)
				}
				if provider.Prover != actual {
					t.Fatalf("provider=%s proof adapter is not ordinary adapter", ordinary.Provider)
				}
				nonce := ""
				if ordinary.Provider == domain.IdentityProviderTelegram {
					nonce = "synthetic-original-nonce"
				}
				request := ports.IdentityProviderProofAuthorizationRequestV1{Binding: provider.Binding, State: "synthetic-state", Nonce: nonce, CodeChallenge: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), ChallengeCreatedAt: at, ChallengeExpiresAt: at.Add(10 * time.Minute), Now: at}
				raw, err := provider.Prover.ProofAuthorizationURL(context.Background(), request)
				if err != nil {
					t.Fatalf("same adapter rejected its derived exact binding provider=%s: %v", ordinary.Provider, err)
				}
				u, err := url.Parse(raw)
				if err != nil || u.Query().Get("redirect_uri") != provider.Binding.CallbackURI || u.Query().Get("client_id") != provider.Binding.ClientID {
					t.Fatalf("provider=%s proof URL binding mismatch err=%v", ordinary.Provider, err)
				}
				request.Binding.ClientID = "not-the-ordinary-client"
				if raw, err := provider.Prover.ProofAuthorizationURL(context.Background(), request); err == nil || raw != "" {
					t.Fatalf("provider=%s accepted mismatched ordinary client", ordinary.Provider)
				}
			}
		})
	}
}

func TestWebIdentityRuntimeV2KeepsServiceWhenSensitiveOff(t *testing.T) {
	for _, sensitive := range []string{"false", "true"} {
		t.Run(sensitive, func(t *testing.T) {
			login := runtimeLoginConfig(t, "yandex,telegram")
			key := []byte(strings.Repeat("h", 32))
			config, err := webIdentityFromEnvironment(loginConfigLookup(map[string]string{"WEB_IDENTITY_VERSION": "2", "WEB_IDENTITY_SENSITIVE_OPERATIONS": sensitive}), login, identityRuntimeTestOrigin, key)
			if err != nil {
				t.Fatal("valid V2 runtime configuration", err)
			}
			// A zero-value handle is deliberately unconnected. Constructors must
			// only compose trusted objects, never execute SQL/cutover/repair.
			store, service, err := config.newStore(new(sql.DB), ydbstore.Options{}, key)
			if err != nil || store == nil || service == nil {
				t.Fatalf("sensitive=%s store=%v service=%v err=%v", sensitive, store != nil, service != nil, err)
			}
			if err := service.ValidateLoginBindings(runtimeConfigBindings(config)); err != nil {
				t.Fatal("same immutable ordinary/proof binding", err)
			}
			handler, err := webbff.New(webbff.Config{BaseURL: identityRuntimeTestOrigin, LoginProviders: login.providers, Store: store, IdentityAccounts: service, IDs: idgen.New(), Clock: systemClock{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				t.Fatal("actual BFF paired to same V2 Store/service", err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, identityRuntimeTestOrigin+"/auth/login/methods", nil))
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"yandex"`) || !strings.Contains(response.Body.String(), `"telegram"`) {
				t.Fatalf("sensitive=%s ordinary discovery status=%d body=%s", sensitive, response.Code, response.Body.String())
			}
			response = httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, identityRuntimeTestOrigin+webcontract.RouteAccountLoginMethods, nil))
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("sensitive=%s account service not mounted or missing-cookie guard changed: status=%d body=%s", sensitive, response.Code, response.Body.String())
			}
			bindings := runtimeConfigBindings(config)
			bindings[0].ClientID = "different-client"
			if err := service.ValidateLoginBindings(bindings); err == nil {
				t.Fatal("mismatched service/ordinary client accepted")
			}
		})
	}
}

func TestWebIdentityRuntimeRequiresExplicitModeAndNeverFallsBack(t *testing.T) {
	login := runtimeLoginConfig(t, "yandex,telegram")
	key := []byte(strings.Repeat("h", 32))
	for _, env := range []map[string]string{
		{},
		{"WEB_IDENTITY_VERSION": ""},
		{"WEB_IDENTITY_VERSION": "v2"},
		{"WEB_IDENTITY_VERSION": " 2"},
		{"WEB_IDENTITY_VERSION": "3"},
		{"WEB_IDENTITY_VERSION": "1", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "true"},
		{"WEB_IDENTITY_VERSION": "1", "WEB_IDENTITY_SENSITIVE_OPERATIONS": ""},
		{"WEB_IDENTITY_VERSION": "2"},
		{"WEB_IDENTITY_VERSION": "2", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "TRUE"},
		{"WEB_IDENTITY_VERSION": "2", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "0"},
	} {
		config, err := webIdentityFromEnvironment(loginConfigLookup(env), login, identityRuntimeTestOrigin, key)
		if !errors.Is(err, errWebIdentityConfiguration) || config.version != "" || len(config.providers) != 0 {
			t.Fatalf("invalid explicit mode env=%v produced config=%+v err=%v", env, config, err)
		}
	}
	if _, err := webIdentityFromEnvironment(nil, login, identityRuntimeTestOrigin, key); !errors.Is(err, errWebIdentityConfiguration) {
		t.Fatalf("nil lookup err=%v", err)
	}
	for _, size := range []int{0, 31, 4097} {
		config, err := webIdentityFromEnvironment(loginConfigLookup(map[string]string{"WEB_IDENTITY_VERSION": "2", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "false"}), login, identityRuntimeTestOrigin, []byte(strings.Repeat("k", size)))
		if err == nil || config.version != "" {
			t.Fatalf("invalid existing HMAC length=%d enabled runtime config=%+v err=%v", size, config, err)
		}
	}
	if store, service, err := (webIdentityConfig{}).newStore(new(sql.DB), ydbstore.Options{}, key); err == nil || store != nil || service != nil {
		t.Fatal("missing runtime version silently selected V1")
	}
	valid, err := webIdentityFromEnvironment(loginConfigLookup(map[string]string{"WEB_IDENTITY_VERSION": "2", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "true"}), login, identityRuntimeTestOrigin, key)
	if err != nil {
		t.Fatal("positive V2 configuration", err)
	}
	if store, service, err := valid.newStore(new(sql.DB), ydbstore.Options{}, nil); err == nil || store != nil || service != nil {
		t.Fatal("V2 constructor error fell back to V1 Store")
	}
}

func TestWebIdentityRuntimeV1PreCutoverDoesNotMountPartialService(t *testing.T) {
	for _, env := range []map[string]string{{"WEB_IDENTITY_VERSION": "1"}, {"WEB_IDENTITY_VERSION": "1", "WEB_IDENTITY_SENSITIVE_OPERATIONS": "false"}} {
		config, err := webIdentityFromEnvironment(loginConfigLookup(env), webLoginConfig{}, identityRuntimeTestOrigin, nil)
		if err != nil {
			t.Fatal("explicit V1 pre-cutover configuration", err)
		}
		store, service, err := config.newStore(new(sql.DB), ydbstore.Options{}, nil)
		if err != nil || store == nil || service != nil {
			t.Fatalf("V1 pre-cutover store=%v partial service=%v err=%v", store != nil, service != nil, err)
		}
	}
}

func TestWebIdentityRuntimeInvalidConfigurationStopsBeforeDependencies(t *testing.T) {
	for name, value := range completeLoginEnvironment("yandex,telegram") {
		t.Setenv(name, value)
	}
	t.Setenv("WEB_BASE_URL", identityRuntimeTestOrigin)
	t.Setenv("WEB_LOGIN_PROVIDER", "")
	// t.Setenv has already registered restoration of the original environment;
	// the production rule rejects even a present-empty retired singular value.
	if err := os.Unsetenv("WEB_LOGIN_PROVIDER"); err != nil {
		t.Fatal("unset retired singular test configuration", err)
	}
	for _, name := range []string{"YANDEX_LOGIN_AUTHORIZATION_ENDPOINT", "YANDEX_LOGIN_TOKEN_ENDPOINT", "YANDEX_LOGIN_INFO_ENDPOINT", "TELEGRAM_OIDC_ISSUER", "TELEGRAM_OIDC_AUTHORIZATION_ENDPOINT", "TELEGRAM_OIDC_TOKEN_ENDPOINT", "TELEGRAM_OIDC_JWKS_URL"} {
		t.Setenv(name, "")
	}
	t.Setenv("SESSIONLESS_ENVIRONMENT", "cloud-dev")
	t.Setenv("SESSION_API_ID_HMAC_KEY", strings.Repeat("h", 32))
	t.Setenv("WEB_IDENTITY_VERSION", "not-a-version")
	t.Setenv("WEB_IDENTITY_SENSITIVE_OPERATIONS", "false")
	t.Setenv("YDB_CONNECTION_STRING", "must-not-be-opened")
	handler, closeDependencies, err := buildHandler(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if closeDependencies != nil {
		closeDependencies()
	}
	if handler != nil || !errors.Is(err, errWebIdentityConfiguration) {
		t.Fatalf("invalid identity config reached another dependency: handler=%v err=%v", handler != nil, err)
	}
}
