package webauthconfig

import (
	"gitcode.com/urandon/sessionless/internal/domain"
	"testing"
)

func TestReadBindingsFromEnvironmentClosedNonsecretPins(t *testing.T) {
	for _, selected := range []string{"yandex", "telegram", "yandex,telegram"} {
		t.Run(selected, func(t *testing.T) {
			env := map[string]string{"WEB_LOGIN_PROVIDERS": selected, "WEB_BASE_URL": "https://web.example.com", "YANDEX_LOGIN_CLIENT_ID": "yandex-client", "TELEGRAM_OIDC_CLIENT_ID": "123456"}
			lookup := func(k string) (string, bool) {
				if k == "YANDEX_LOGIN_CLIENT_SECRET" || k == "TELEGRAM_OIDC_CLIENT_SECRET" || k == "WEB_IDENTITY_HMAC_KEY" {
					t.Fatal("reader requested secret", k)
				}
				v, ok := env[k]
				return v, ok
			}
			bindings, err := ReadBindingsFromEnvironment(lookup)
			count := 1
			if selected == "yandex,telegram" {
				count = 2
			}
			if err != nil || len(bindings) != count {
				t.Fatal("valid bindings", err)
			}
			for _, b := range bindings {
				if b.Validate() != nil {
					t.Fatal("invalid binding")
				}
				if b.Provider == domain.IdentityProviderYandex && b.ClientID != "yandex-client" || b.Provider == domain.IdentityProviderTelegram && b.ClientID != "123456" {
					t.Fatal("crossed client")
				}
			}
			for _, mutation := range []struct{ key, value string }{{"WEB_LOGIN_PROVIDERS", ""}, {"WEB_LOGIN_PROVIDERS", "telegram,yandex"}, {"WEB_LOGIN_PROVIDERS", "yandex,yandex"}, {"WEB_LOGIN_PROVIDER", ""}, {"WEB_BASE_URL", "https://web.example.com/"}, {"WEB_BASE_URL", "http://web.example.com"}, {"WEB_BASE_URL", "https://web.example.com?x=1"}} {
				t.Run(mutation.key+mutation.value, func(t *testing.T) {
					old, present := env[mutation.key]
					env[mutation.key] = mutation.value
					defer func() {
						if present {
							env[mutation.key] = old
						} else {
							delete(env, mutation.key)
						}
					}()
					if got, err := ReadBindingsFromEnvironment(lookup); err == nil || got != nil {
						t.Fatal("invalid binding accepted")
					}
				})
			}
		})
	}
	if got, err := ReadBindingsFromEnvironment(nil); err == nil || got != nil {
		t.Fatal("nil lookup accepted")
	}
}
