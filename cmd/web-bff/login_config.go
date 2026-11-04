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
)

type webLoginConfig struct { providers []webbff.LoginProviderConfig }

// Configuration is deployment authority, not browser input. Presence of the
// retired singular variable (even empty) requires an explicit operator migration.
func webLoginFromEnvironment(lookup func(string) (string, bool), baseURL string, local bool) (webLoginConfig, error) {
	invalid := func() (webLoginConfig, error) { return webLoginConfig{}, errors.New("WEB_LOGIN_PROVIDERS must explicitly be yandex, telegram, or yandex,telegram; migrate the singular setting") }
	if lookup == nil { return invalid() }
	if _, present := lookup("WEB_LOGIN_PROVIDER"); present { return invalid() }
	selected, present := lookup("WEB_LOGIN_PROVIDERS")
	if !present || (selected != "yandex" && selected != "telegram" && selected != "yandex,telegram") { return invalid() }
	getenv := func(name string) string { value, _ := lookup(name); return value }
	value := func(name, fallback string) string { if v := getenv(name); v != "" { return v }; return fallback }
	config := webLoginConfig{providers: make([]webbff.LoginProviderConfig, 0, 2)}
	for _, provider := range strings.Split(selected, ",") {
		switch provider {
		case "yandex":
			clientID := getenv("YANDEX_LOGIN_CLIENT_ID")
			adapter, err := yandexoauth.New(yandexoauth.Config{
				ClientID: clientID, ClientSecret: getenv("YANDEX_LOGIN_CLIENT_SECRET"), RedirectURI: baseURL + webcontract.RouteLoginCallback,
				AuthorizationEndpoint: getenv("YANDEX_LOGIN_AUTHORIZATION_ENDPOINT"), TokenEndpoint: getenv("YANDEX_LOGIN_TOKEN_ENDPOINT"), InfoEndpoint: getenv("YANDEX_LOGIN_INFO_ENDPOINT"), AllowLoopbackProvider: local,
			})
			if err != nil { return webLoginConfig{}, err }
			config.providers = append(config.providers, webbff.LoginProviderConfig{Provider: domain.IdentityProviderYandex, OAuthProvider: adapter, OAuthClientID: clientID})
		case "telegram":
			issuer := value("TELEGRAM_OIDC_ISSUER", telegramoidc.DefaultIssuer)
			clientID := getenv("TELEGRAM_OIDC_CLIENT_ID")
			adapter, err := telegramoidc.New(telegramoidc.Config{
				Issuer: issuer, AuthorizationEndpoint: value("TELEGRAM_OIDC_AUTHORIZATION_ENDPOINT", telegramoidc.DefaultAuthorizationEndpoint), TokenEndpoint: value("TELEGRAM_OIDC_TOKEN_ENDPOINT", telegramoidc.DefaultTokenEndpoint), JWKSURL: value("TELEGRAM_OIDC_JWKS_URL", telegramoidc.DefaultJWKSURL), ClientID: clientID, ClientSecret: getenv("TELEGRAM_OIDC_CLIENT_SECRET"), RedirectURI: baseURL + webcontract.RouteOIDCCallback, AllowedAlgorithms: []string{"RS256"}, AllowLoopbackProvider: local,
			})
			if err != nil { return webLoginConfig{}, err }
			config.providers = append(config.providers, webbff.LoginProviderConfig{Provider: domain.IdentityProviderTelegram, OIDCProvider: adapter, OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: issuer, Audience: clientID, AllowedAlgorithms: []string{"RS256"}, MaxClockSkew: 30 * time.Second}})
		}
	}
	return config, nil
}
