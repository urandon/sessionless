package main

import (
	"errors"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/yandexoauth"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type webLoginConfig struct{ providers []webbff.LoginProviderConfig }

// Derive proof clients from the very same adapter instances as ordinary login.
// This private deployment composition is never selected by a browser request.
func (config webLoginConfig) identityProvidersV2(baseURL string) ([]ydbstore.IdentityAccountProviderV2, error) {
	providers := make([]ydbstore.IdentityAccountProviderV2, 0, len(config.providers))
	for _, ordinary := range config.providers {
		var provider ydbstore.IdentityAccountProviderV2
		switch ordinary.Provider {
		case domain.IdentityProviderYandex:
			adapter, ok := ordinary.OAuthProvider.(*yandexoauth.Provider)
			if !ok || adapter == nil || ordinary.OIDCProvider != nil {
				return nil, errors.New("identity runtime requires its configured Yandex adapter")
			}
			provider = ydbstore.IdentityAccountProviderV2{Binding: domain.IdentityProviderBindingV2{Provider: ordinary.Provider, Issuer: domain.YandexOAuthIssuer, ClientID: ordinary.OAuthClientID, CallbackURI: baseURL + webcontract.RouteLoginCallback}, Prover: adapter}
		case domain.IdentityProviderTelegram:
			adapter, ok := ordinary.OIDCProvider.(*telegramoidc.Provider)
			if !ok || adapter == nil || ordinary.OAuthProvider != nil {
				return nil, errors.New("identity runtime requires its configured Telegram adapter")
			}
			provider = ydbstore.IdentityAccountProviderV2{Binding: domain.IdentityProviderBindingV2{Provider: ordinary.Provider, Issuer: ordinary.OIDCPolicy.Issuer, ClientID: ordinary.OIDCPolicy.Audience, CallbackURI: baseURL + webcontract.RouteOIDCCallback}, Prover: adapter}
		default:
			return nil, errors.New("identity runtime requires a closed configured provider")
		}
		providers = append(providers, provider)
	}
	bindings := make([]domain.IdentityProviderBindingV2, 0, len(providers))
	for _, provider := range providers {
		bindings = append(bindings, provider.Binding)
	}
	if _, err := ydbstore.WebIdentityManifestDigestV2(bindings); err != nil {
		return nil, err
	}
	return providers, nil
}

// Configuration is deployment authority, not browser input. Presence of the
// retired singular variable (even empty) requires an explicit operator migration.
func webLoginFromEnvironment(lookup func(string) (string, bool), baseURL string, local bool) (webLoginConfig, error) {
	invalid := func() (webLoginConfig, error) {
		return webLoginConfig{}, errors.New("WEB_LOGIN_PROVIDERS must explicitly be yandex, telegram, or yandex,telegram; migrate the singular setting")
	}
	if lookup == nil {
		return invalid()
	}
	if _, present := lookup("WEB_LOGIN_PROVIDER"); present {
		return invalid()
	}
	selected, present := lookup("WEB_LOGIN_PROVIDERS")
	if !present || (selected != "yandex" && selected != "telegram" && selected != "yandex,telegram") {
		return invalid()
	}
	getenv := func(name string) string { value, _ := lookup(name); return value }
	value := func(name, fallback string) string {
		if v := getenv(name); v != "" {
			return v
		}
		return fallback
	}
	config := webLoginConfig{providers: make([]webbff.LoginProviderConfig, 0, 2)}
	for _, provider := range strings.Split(selected, ",") {
		switch provider {
		case "yandex":
			clientID := getenv("YANDEX_LOGIN_CLIENT_ID")
			adapter, err := yandexoauth.New(yandexoauth.Config{
				ClientID: clientID, ClientSecret: getenv("YANDEX_LOGIN_CLIENT_SECRET"), RedirectURI: baseURL + webcontract.RouteLoginCallback,
				AuthorizationEndpoint: getenv("YANDEX_LOGIN_AUTHORIZATION_ENDPOINT"), TokenEndpoint: getenv("YANDEX_LOGIN_TOKEN_ENDPOINT"), InfoEndpoint: getenv("YANDEX_LOGIN_INFO_ENDPOINT"), AllowLoopbackProvider: local,
			})
			if err != nil {
				return webLoginConfig{}, err
			}
			config.providers = append(config.providers, webbff.LoginProviderConfig{Provider: domain.IdentityProviderYandex, OAuthProvider: adapter, OAuthClientID: clientID})
		case "telegram":
			issuer := value("TELEGRAM_OIDC_ISSUER", telegramoidc.DefaultIssuer)
			clientID := getenv("TELEGRAM_OIDC_CLIENT_ID")
			adapter, err := telegramoidc.New(telegramoidc.Config{
				Issuer: issuer, AuthorizationEndpoint: value("TELEGRAM_OIDC_AUTHORIZATION_ENDPOINT", telegramoidc.DefaultAuthorizationEndpoint), TokenEndpoint: value("TELEGRAM_OIDC_TOKEN_ENDPOINT", telegramoidc.DefaultTokenEndpoint), JWKSURL: value("TELEGRAM_OIDC_JWKS_URL", telegramoidc.DefaultJWKSURL), ClientID: clientID, ClientSecret: getenv("TELEGRAM_OIDC_CLIENT_SECRET"), RedirectURI: baseURL + webcontract.RouteOIDCCallback, AllowedAlgorithms: []string{"RS256"}, AllowLoopbackProvider: local,
			})
			if err != nil {
				return webLoginConfig{}, err
			}
			config.providers = append(config.providers, webbff.LoginProviderConfig{Provider: domain.IdentityProviderTelegram, OIDCProvider: adapter, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: issuer, Audience: clientID, AllowedAlgorithms: []string{"RS256"}, MaxClockSkew: 30 * time.Second}})
		}
	}
	return config, nil
}
