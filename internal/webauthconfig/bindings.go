package webauthconfig

import (
	"errors"
	"net/url"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// ReadBindingsFromEnvironment reads only public deployment pins. It constructs
// no OAuth adapter and never requests a client secret, HMAC or provider token.
func ReadBindingsFromEnvironment(lookup func(string) (string, bool)) ([]domain.IdentityProviderBindingV2, error) {
	invalid := func() ([]domain.IdentityProviderBindingV2, error) {
		return nil, errors.New("identity read bindings require canonical enabled providers, clients and HTTPS Web origin")
	}
	if lookup == nil {
		return invalid()
	}
	if _, present := lookup("WEB_LOGIN_PROVIDER"); present {
		return invalid()
	}
	selected, present := lookup("WEB_LOGIN_PROVIDERS")
	if !present || selected != "yandex" && selected != "telegram" && selected != "yandex,telegram" {
		return invalid()
	}
	base, _ := lookup("WEB_BASE_URL")
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.User != nil || u.Opaque != "" || u.String() != base {
		return invalid()
	}
	out := make([]domain.IdentityProviderBindingV2, 0, 2)
	for _, name := range strings.Split(selected, ",") {
		b := domain.IdentityProviderBindingV2{Provider: domain.IdentityProvider(name), Issuer: domain.YandexOAuthIssuer, CallbackURI: base + "/auth/login/callback"}
		b.ClientID, _ = lookup("YANDEX_LOGIN_CLIENT_ID")
		if b.Provider == domain.IdentityProviderTelegram {
			b.Issuer = "https://oauth.telegram.org"
			if issuer, present := lookup("TELEGRAM_OIDC_ISSUER"); present {
				b.Issuer = issuer
			}
			b.ClientID, _ = lookup("TELEGRAM_OIDC_CLIENT_ID")
			b.CallbackURI = base + "/auth/telegram/callback"
		}
		if b.Validate() != nil {
			return invalid()
		}
		out = append(out, b)
	}
	return out, nil
}
