package domain_test

import (
	"gitcode.com/urandon/sessionless/internal/domain"
	"testing"
	"time"
)

func TestYandexChallengeIsOAuthBoundWithoutOIDCNonce(t *testing.T) {
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	base := domain.OIDCLoginChallenge{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, Audience: "app123", StateDigest: domain.DigestSecret("state"), BrowserBindingDigest: domain.DigestSecret("binding"), PKCEVerifier: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", RedirectPath: "/sessions", CreatedAt: at, ExpiresAt: at.Add(time.Hour)}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*domain.OIDCLoginChallenge)
	}{
		{"nonce", func(c *domain.OIDCLoginChallenge) { c.Nonce = "fake-OIDC-nonce" }},
		{"issuer", func(c *domain.OIDCLoginChallenge) { c.Issuer = "https://evil.example" }},
		{"client", func(c *domain.OIDCLoginChallenge) { c.Audience = "" }},
		{"partial-binding", func(c *domain.OIDCLoginChallenge) { c.Provider = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("invalid binding accepted")
			}
		})
	}
	legacy := base
	legacy.Provider = ""
	legacy.Issuer = ""
	legacy.Audience = ""
	legacy.Nonce = "legacy-telegram-nonce"
	if err := legacy.Validate(); err != nil {
		t.Fatalf("outstanding Telegram record compatibility: %v", err)
	}
}
