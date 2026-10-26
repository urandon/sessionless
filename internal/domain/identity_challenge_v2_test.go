package domain_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func challengeV2Binding(provider domain.IdentityProvider) domain.IdentityProviderBindingV2 {
	binding := domain.IdentityProviderBindingV2{Provider: provider, Issuer: domain.YandexOAuthIssuer, ClientID: "yandex-client", CallbackURI: "https://web.example.invalid/auth/login/callback"}
	if provider == domain.IdentityProviderTelegram {
		binding.Issuer = "https://oauth.telegram.org"
		binding.ClientID = "123456"
		binding.CallbackURI = "https://web.example.invalid/auth/telegram/callback"
	}
	return binding
}

func challengeV2Login(provider domain.IdentityProvider) domain.IdentityChallengeV2 {
	at := operationV2At()
	nonce := ""
	if provider == domain.IdentityProviderTelegram {
		nonce = "login-nonce"
	}
	return domain.IdentityChallengeV2{Version: 2, Purpose: domain.IdentityPurposeLoginV2, StateDigest: domain.DigestSecret("login-state"), BrowserBindingDigest: domain.DigestSecret("login-browser"), Binding: challengeV2Binding(provider), CreatedAt: at, ExpiresAt: at.Add(10 * time.Minute), DeleteAfter: at.Add(10 * time.Minute), Login: &domain.IdentityLoginChallengeV2{ReturnPath: "/sessions"}, Secrets: &domain.IdentityChallengeSecretsV2{PKCEVerifier: strings.Repeat("x", 64), Nonce: nonce}}
}

func TestIdentityChallengeV2LoginConsumeScrubsBeforeExchange(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(provider), func(t *testing.T) {
			challenge := challengeV2Login(provider)
			if err := challenge.Validate(); err != nil {
				t.Fatal(err)
			}
			at := challenge.CreatedAt.Add(time.Second)
			cleared, exchange, err := challenge.Consume(challenge.Binding, "login-browser", nil, at)
			if err != nil {
				t.Fatal(err)
			}
			if cleared.Secrets != nil || cleared.ConsumedAt == nil || !cleared.ConsumedAt.Equal(at) || !cleared.ExpiresAt.Equal(challenge.ExpiresAt) || !cleared.DeleteAfter.Equal(challenge.DeleteAfter) || challenge.Secrets == nil || challenge.ConsumedAt != nil {
				t.Fatalf("consume failed immediate scrub or mutated input: cleared=%+v original=%+v", cleared, challenge)
			}
			if exchange.PKCEVerifier != challenge.Secrets.PKCEVerifier || exchange.Nonce != challenge.Secrets.Nonce || exchange.Binding != challenge.Binding || exchange.StateDigest != challenge.StateDigest {
				t.Error("ephemeral preparation lost exact one-shot exchange values")
			}
			if _, err := json.Marshal(exchange); err == nil {
				t.Error("ephemeral prepared exchange was persistable as JSON")
			}
			if _, err := json.Marshal(&exchange); err == nil {
				t.Error("pointer to ephemeral exchange was persistable as JSON")
			}
			raw, err := json.Marshal(cleared)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "pkce_verifier") || strings.Contains(string(raw), `"nonce"`) || strings.Contains(string(raw), `"secrets"`) {
				t.Errorf("consumed record retained exchange secrets: %s", raw)
			}
			replayed, prepared, err := cleared.Consume(challenge.Binding, "login-browser", nil, at.Add(time.Second))
			if err == nil || !reflect.DeepEqual(replayed, domain.IdentityChallengeV2{}) || !reflect.DeepEqual(prepared, domain.IdentityChallengeExchangeV2{}) {
				t.Errorf("consumed challenge replay=(%+v,%+v,%v)", replayed, prepared, err)
			}
		})
	}
}

func TestIdentityChallengeV2ProofParentAndPurposeBinding(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*domain.IdentityChallengeV2, *domain.IdentityOperationV2)
	}{
		{name: "wrong parent ID", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) {
			c.Proof.ParentOperationDigest = domain.DigestSecret("other")
		}},
		{name: "wrong phase", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) {
			c.Proof.ParentPhase = domain.IdentityPhaseTargetProofV2
			c.Proof.ExpectedSubject = nil
		}},
		{name: "wrong user", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) { c.Proof.Scope.UserID = "other" }},
		{name: "wrong session", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) {
			c.Proof.Scope.SessionDigest = domain.DigestSecret("other")
		}},
		{name: "wrong browser", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) {
			c.BrowserBindingDigest = domain.DigestSecret("other")
		}},
		{name: "wrong configuration", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) {
			c.Proof.Scope.ConfigurationDigest = domain.DigestSecret("other")
		}},
		{name: "wrong expected revision", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) { c.Proof.Scope.ExpectedRevision++ }},
		{name: "wrong source subject", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) {
			c.Proof.ExpectedSubject.Subject = "other"
		}},
		{name: "wrong child state", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) {
			c.StateDigest = domain.DigestSecret("other")
		}},
		{name: "created before parent", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) {
			c.CreatedAt = c.CreatedAt.Add(-time.Hour)
		}},
		{name: "deadline after parent", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) {
			c.ExpiresAt = c.ExpiresAt.Add(time.Second)
			c.DeleteAfter = c.ExpiresAt
		}},
		{name: "login confused with proof", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityOperationV2) {
			c.Purpose = domain.IdentityPurposeLoginV2
			c.Proof = nil
			c.Login = &domain.IdentityLoginChallengeV2{ReturnPath: "/"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			parent := operationV2Fixture(domain.IdentityOperationLinkV2)
			challenge := operationV2ProofChallenge(parent)
			test.mutate(&challenge, &parent)
			if challenge.ValidateForOperation(parent) == nil {
				t.Error("different purpose/parent/scope challenge accepted")
			}
			cleared, prepared, err := challenge.Consume(challenge.Binding, "operation-browser", &parent, parent.CreatedAt.Add(time.Minute))
			if err == nil || !reflect.DeepEqual(cleared, domain.IdentityChallengeV2{}) || !reflect.DeepEqual(prepared, domain.IdentityChallengeExchangeV2{}) {
				t.Errorf("bad proof challenge produced exchange=(%+v,%+v,%v)", cleared, prepared, err)
			}
		})
	}
}

func TestIdentityChallengeV2RejectsMalformedConsumedAndPurposeArms(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*domain.IdentityChallengeV2)
	}{
		{name: "V1", mutate: func(c *domain.IdentityChallengeV2) { c.Version = 1 }},
		{name: "missing purpose", mutate: func(c *domain.IdentityChallengeV2) { c.Purpose = "" }},
		{name: "operation purpose", mutate: func(c *domain.IdentityChallengeV2) { c.Purpose = domain.IdentityPurposeOperationV2 }},
		{name: "missing login arm", mutate: func(c *domain.IdentityChallengeV2) { c.Login = nil }},
		{name: "both arms", mutate: func(c *domain.IdentityChallengeV2) { c.Proof = &domain.IdentityProofChallengeV2{} }},
		{name: "external return", mutate: func(c *domain.IdentityChallengeV2) { c.Login.ReturnPath = "//evil.invalid" }},
		{name: "no verifier", mutate: func(c *domain.IdentityChallengeV2) { c.Secrets = nil }},
		{name: "invalid verifier", mutate: func(c *domain.IdentityChallengeV2) { c.Secrets.PKCEVerifier = "short" }},
		{name: "invented Yandex nonce", mutate: func(c *domain.IdentityChallengeV2) { c.Secrets.Nonce = "not-oidc" }},
		{name: "same browser and state", mutate: func(c *domain.IdentityChallengeV2) { c.BrowserBindingDigest = c.StateDigest }},
		{name: "retention extended", mutate: func(c *domain.IdentityChallengeV2) { c.DeleteAfter = c.CreatedAt.Add(24 * time.Hour) }},
		{name: "lifetime extended", mutate: func(c *domain.IdentityChallengeV2) {
			c.ExpiresAt = c.ExpiresAt.Add(time.Nanosecond)
			c.DeleteAfter = c.ExpiresAt
		}},
		{name: "consumed secrets retained", mutate: func(c *domain.IdentityChallengeV2) { at := c.CreatedAt.Add(time.Second); c.ConsumedAt = &at }},
		{name: "consumed before creation", mutate: func(c *domain.IdentityChallengeV2) {
			at := c.CreatedAt.Add(-time.Nanosecond)
			c.ConsumedAt = &at
			c.Secrets = nil
		}},
		{name: "consumed at expiry", mutate: func(c *domain.IdentityChallengeV2) { at := c.ExpiresAt; c.ConsumedAt = &at; c.Secrets = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			challenge := challengeV2Login(domain.IdentityProviderYandex)
			test.mutate(&challenge)
			if err := challenge.Validate(); err == nil {
				t.Errorf("malformed challenge accepted: %+v", challenge)
			}
		})
	}
	telegram := challengeV2Login(domain.IdentityProviderTelegram)
	telegram.Secrets.Nonce = ""
	if telegram.Validate() == nil {
		t.Error("Telegram challenge accepted without OIDC nonce")
	}
}

func TestIdentityChallengeV2ConsumeRejectsBrowserProviderCallbackAndTimeMixup(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*domain.IdentityProviderBindingV2, *string, *time.Time)
	}{
		{name: "wrong browser", mutate: func(_ *domain.IdentityProviderBindingV2, s *string, _ *time.Time) { *s = "other" }},
		{name: "empty browser", mutate: func(_ *domain.IdentityProviderBindingV2, s *string, _ *time.Time) { *s = "" }},
		{name: "wrong provider", mutate: func(b *domain.IdentityProviderBindingV2, _ *string, _ *time.Time) {
			*b = challengeV2Binding(domain.IdentityProviderTelegram)
		}},
		{name: "wrong client", mutate: func(b *domain.IdentityProviderBindingV2, _ *string, _ *time.Time) { b.ClientID = "other" }},
		{name: "different trusted origin", mutate: func(b *domain.IdentityProviderBindingV2, _ *string, _ *time.Time) {
			b.CallbackURI = "https://other.invalid/auth/login/callback"
		}},
		{name: "wrong callback path", mutate: func(b *domain.IdentityProviderBindingV2, _ *string, _ *time.Time) {
			b.CallbackURI = "https://web.example.invalid/auth/telegram/callback"
		}},
		{name: "future challenge", mutate: func(_ *domain.IdentityProviderBindingV2, _ *string, at *time.Time) {
			*at = operationV2At().Add(-time.Nanosecond)
		}},
		{name: "expiry equality", mutate: func(_ *domain.IdentityProviderBindingV2, _ *string, at *time.Time) {
			*at = operationV2At().Add(10 * time.Minute)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			challenge := challengeV2Login(domain.IdentityProviderYandex)
			binding, secret, at := challenge.Binding, "login-browser", challenge.CreatedAt.Add(time.Second)
			test.mutate(&binding, &secret, &at)
			cleared, prepared, err := challenge.Consume(binding, secret, nil, at)
			if err == nil || !reflect.DeepEqual(cleared, domain.IdentityChallengeV2{}) || !reflect.DeepEqual(prepared, domain.IdentityChallengeExchangeV2{}) || challenge.Secrets == nil {
				t.Errorf("mixup consumed challenge=(%+v,%+v,%v)", cleared, prepared, err)
			}
		})
	}
	parent := operationV2Fixture(domain.IdentityOperationLinkV2)
	proof := operationV2ProofChallenge(parent)
	if _, _, err := proof.Consume(proof.Binding, "operation-browser", nil, proof.CreatedAt.Add(time.Second)); err == nil {
		t.Error("provider proof consumed without original parent")
	}
	login := challengeV2Login(domain.IdentityProviderYandex)
	if _, _, err := login.Consume(login.Binding, "login-browser", &parent, login.CreatedAt.Add(time.Second)); err == nil {
		t.Error("ordinary login accepted identity-operation context")
	}
}

func TestIdentityProviderBindingV2PinsCallbackShape(t *testing.T) {
	for _, callback := range []string{"http://web.example.invalid/auth/login/callback", "https://user@web.example.invalid/auth/login/callback", "https://web.example.invalid/auth/login/callback?x=1", "https://web.example.invalid/auth/login/callback?", "https://web.example.invalid/auth/login/callback#fragment", "https://web.example.invalid/auth/telegram/callback", "https://web.example.invalid/auth%2flogin/callback", "https:///auth/login/callback"} {
		t.Run(callback, func(t *testing.T) {
			binding := challengeV2Binding(domain.IdentityProviderYandex)
			binding.CallbackURI = callback
			if binding.Validate() == nil {
				t.Errorf("invalid configured callback accepted: %q", callback)
			}
		})
	}
	binding := challengeV2Binding(domain.IdentityProviderYandex)
	binding.Issuer = "https://other.invalid"
	if binding.Validate() == nil {
		t.Error("arbitrary Yandex issuer accepted")
	}
	binding = challengeV2Binding(domain.IdentityProviderTelegram)
	binding.Issuer = domain.YandexOAuthIssuer
	if binding.Validate() == nil {
		t.Error("provider issuer namespaces mixed")
	}
}

func TestIdentityChallengeV2CodecRoundTripPendingAndConsumed(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(provider), func(t *testing.T) {
			login := challengeV2Login(provider)
			consumedLogin, _, err := login.Consume(login.Binding, "login-browser", nil, login.CreatedAt.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			kind := domain.IdentityOperationLinkV2
			if provider == domain.IdentityProviderTelegram {
				kind = domain.IdentityOperationUnlinkV2
			}
			operation := operationV2Fixture(kind)
			proof := operationV2ProofChallenge(operation)
			consumedProof, _, err := proof.Consume(proof.Binding, "operation-browser", &operation, proof.CreatedAt.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			for _, challenge := range []domain.IdentityChallengeV2{login, consumedLogin, proof, consumedProof} {
				raw, err := domain.EncodeIdentityChallengeV2(challenge)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := domain.DecodeIdentityChallengeV2(raw)
				if err != nil || !reflect.DeepEqual(decoded, challenge) {
					t.Errorf("%s consumed=%v roundtrip=(%+v,%v), want %+v", challenge.Purpose, challenge.ConsumedAt != nil, decoded, err, challenge)
				}
			}
		})
	}
}
