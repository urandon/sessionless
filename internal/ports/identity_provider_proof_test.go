package ports_test

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func proofRequests(provider domain.IdentityProvider) (ports.IdentityProviderProofAuthorizationRequestV1, ports.IdentityProviderProofTokenRequestV1) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	binding := domain.IdentityProviderBindingV2{Provider: provider, Issuer: domain.YandexOAuthIssuer, ClientID: "client-test", CallbackURI: "https://web.example.com/auth/login/callback"}
	nonce := ""
	if provider == domain.IdentityProviderTelegram {
		binding.Issuer = "https://oauth.telegram.org"
		binding.ClientID = "100000"
		binding.CallbackURI = "https://web.example.com/auth/telegram/callback"
		nonce = "original-nonce"
	}
	a := ports.IdentityProviderProofAuthorizationRequestV1{Binding: binding, State: "original-state", Nonce: nonce, CodeChallenge: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), ChallengeCreatedAt: now, ChallengeExpiresAt: now.Add(10 * time.Minute), Now: now}
	b := ports.IdentityProviderProofTokenRequestV1{Binding: binding, Code: "one-use-code", PKCEVerifier: strings.Repeat("v", 43), ExpectedNonce: nonce, ChallengeCreatedAt: a.ChallengeCreatedAt, ChallengeExpiresAt: a.ChallengeExpiresAt, Now: now}
	return a, b
}

func TestIdentityProviderProofRequestsV1(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(provider), func(t *testing.T) {
			a, b := proofRequests(provider)
			if err := a.Validate(); err != nil {
				t.Fatal("authorization baseline:", err)
			}
			if err := b.Validate(); err != nil {
				t.Fatal("exchange baseline:", err)
			}
			for _, tc := range []struct {
				name   string
				mutate func(*ports.IdentityProviderProofAuthorizationRequestV1)
			}{
				{"missing-state", func(r *ports.IdentityProviderProofAuthorizationRequestV1) { r.State = "" }},
				{"plain-challenge", func(r *ports.IdentityProviderProofAuthorizationRequestV1) { r.CodeChallenge = "plain" }},
				{"padded-challenge", func(r *ports.IdentityProviderProofAuthorizationRequestV1) { r.CodeChallenge += "=" }},
				{"wrong-issuer", func(r *ports.IdentityProviderProofAuthorizationRequestV1) {
					r.Binding.Issuer = "https://other.example.com"
				}},
				{"wrong-callback", func(r *ports.IdentityProviderProofAuthorizationRequestV1) {
					r.Binding.CallbackURI = "https://web.example.com/other"
				}},
				{"zero-time", func(r *ports.IdentityProviderProofAuthorizationRequestV1) { r.Now = time.Time{} }},
				{"future-created", func(r *ports.IdentityProviderProofAuthorizationRequestV1) {
					r.Now = r.ChallengeCreatedAt.Add(-time.Nanosecond)
				}},
				{"expired-equality", func(r *ports.IdentityProviderProofAuthorizationRequestV1) { r.Now = r.ChallengeExpiresAt }},
				{"empty-interval", func(r *ports.IdentityProviderProofAuthorizationRequestV1) {
					r.ChallengeExpiresAt = r.ChallengeCreatedAt
				}},
				{"over-ten-minutes", func(r *ports.IdentityProviderProofAuthorizationRequestV1) {
					r.ChallengeExpiresAt = r.ChallengeCreatedAt.Add(10*time.Minute + time.Nanosecond)
				}},
				{"wrong-nonce-arm", func(r *ports.IdentityProviderProofAuthorizationRequestV1) {
					if provider == domain.IdentityProviderTelegram {
						r.Nonce = ""
					} else {
						r.Nonce = "invented-nonce"
					}
				}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					r := a
					tc.mutate(&r)
					if r.Validate() == nil {
						t.Error("invalid authorization accepted")
					}
				})
			}
			for _, tc := range []struct {
				name   string
				mutate func(*ports.IdentityProviderProofTokenRequestV1)
			}{
				{"missing-code", func(r *ports.IdentityProviderProofTokenRequestV1) { r.Code = "" }},
				{"oversize-code", func(r *ports.IdentityProviderProofTokenRequestV1) { r.Code = strings.Repeat("c", 4097) }},
				{"control-code", func(r *ports.IdentityProviderProofTokenRequestV1) { r.Code = "code\nprivate" }},
				{"short-verifier", func(r *ports.IdentityProviderProofTokenRequestV1) { r.PKCEVerifier = "short" }},
				{"expired", func(r *ports.IdentityProviderProofTokenRequestV1) { r.Now = r.ChallengeExpiresAt }},
				{"wrong-nonce-arm", func(r *ports.IdentityProviderProofTokenRequestV1) {
					if provider == domain.IdentityProviderTelegram {
						r.ExpectedNonce = ""
					} else {
						r.ExpectedNonce = "invented-nonce"
					}
				}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					r := b
					tc.mutate(&r)
					if r.Validate() == nil {
						t.Error("invalid token request accepted")
					}
				})
			}
		})
	}
}
