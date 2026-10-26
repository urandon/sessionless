package domain

import (
	"encoding/json"
	"errors"
	"net/url"
	"time"
)

// Binding is a trusted configured client, not a browser-selected endpoint.
// Validate checks shape/pins; Consume additionally compares the complete value
// with the trusted current manifest binding. The digest is not a global fence.
type IdentityProviderBindingV2 struct {
	Provider    IdentityProvider `json:"provider"`
	Issuer      string           `json:"issuer"`
	ClientID    string           `json:"client_id"`
	CallbackURI string           `json:"callback_uri"`
}

func (binding IdentityProviderBindingV2) Validate() error {
	if !identityProviderV2(binding.Provider) || ValidateOpaqueID("identity_provider_binding_v2.client_id", binding.ClientID) != nil {
		return identityOperationErrorV2("binding", "requires a closed provider and exact configured client")
	}
	issuer, path := YandexOAuthIssuer, "/auth/login/callback"
	if binding.Provider == IdentityProviderTelegram {
		issuer, path = "https://oauth.telegram.org", "/auth/telegram/callback"
	}
	callback, err := url.Parse(binding.CallbackURI)
	if binding.Issuer != issuer || len(binding.CallbackURI) > 2048 || err != nil || callback.Scheme != "https" || callback.Host == "" || callback.Hostname() == "" || callback.User != nil || callback.Opaque != "" || callback.Path != path || callback.RawPath != "" || callback.RawQuery != "" || callback.ForceQuery || callback.Fragment != "" || callback.String() != binding.CallbackURI {
		return identityOperationErrorV2("binding", "requires pinned issuer and exact configured HTTPS provider callback URI")
	}
	return nil
}

type IdentityLoginChallengeV2 struct {
	ReturnPath string `json:"return_path"`
}
type IdentityProofChallengeV2 struct {
	ParentOperationDigest SecretDigest             `json:"parent_operation_digest"`
	ParentPhase           IdentityOperationPhaseV2 `json:"parent_phase"`
	Scope                 IdentityOperationScopeV2 `json:"scope"`
	ExpectedSubject       *ExternalSubject         `json:"expected_subject,omitempty"`
}
type IdentityChallengeSecretsV2 struct {
	PKCEVerifier string `json:"pkce_verifier"`
	Nonce        string `json:"nonce,omitempty"`
}

// Explicit login/proof records share the existing challenge table only. Their
// tags are mandatory; a V1 challenge never acquires a V2 purpose by default.
type IdentityChallengeV2 struct {
	Version              uint32                      `json:"version"`
	Purpose              IdentityRecordPurposeV2     `json:"purpose"`
	StateDigest          SecretDigest                `json:"state_digest"`
	BrowserBindingDigest SecretDigest                `json:"browser_binding_digest"`
	Binding              IdentityProviderBindingV2   `json:"binding"`
	CreatedAt            time.Time                   `json:"created_at"`
	ExpiresAt            time.Time                   `json:"expires_at"`
	DeleteAfter          time.Time                   `json:"delete_after"`
	Login                *IdentityLoginChallengeV2   `json:"login,omitempty"`
	Proof                *IdentityProofChallengeV2   `json:"proof,omitempty"`
	Secrets              *IdentityChallengeSecretsV2 `json:"secrets,omitempty"`
	ConsumedAt           *time.Time                  `json:"consumed_at,omitempty"`
}

func (challenge IdentityChallengeV2) Validate() error {
	if challenge.Version != 2 || challenge.StateDigest.Validate("identity_challenge_v2.state_digest") != nil || challenge.BrowserBindingDigest.Validate("identity_challenge_v2.browser_binding_digest") != nil || challenge.StateDigest == challenge.BrowserBindingDigest || challenge.Binding.Validate() != nil || !identityLifetimeV2(challenge.CreatedAt, challenge.ExpiresAt) || !challenge.DeleteAfter.Equal(challenge.ExpiresAt) {
		return identityOperationErrorV2("challenge", "requires explicit version, distinct digests, binding and original short retention")
	}
	switch challenge.Purpose {
	case IdentityPurposeLoginV2:
		if challenge.Login == nil || challenge.Proof != nil || !safeLocalRedirect(challenge.Login.ReturnPath) {
			return identityOperationErrorV2("challenge.login", "requires only a safe local login-return arm")
		}
	case IdentityPurposeProviderProofV2:
		proof := challenge.Proof
		if challenge.Login != nil || proof == nil || proof.ParentOperationDigest.Validate("identity_challenge_v2.parent_operation_digest") != nil || proof.ParentOperationDigest == challenge.StateDigest || proof.Scope.Validate() != nil || proof.Scope.BrowserBindingDigest != challenge.BrowserBindingDigest || (proof.ParentPhase != IdentityPhaseSourceProofV2 && proof.ParentPhase != IdentityPhaseTargetProofV2) {
			return identityOperationErrorV2("challenge.proof", "requires exact original parent, phase, scope and browser binding")
		}
		if proof.ParentPhase == IdentityPhaseSourceProofV2 {
			if proof.ExpectedSubject == nil || validateIdentitySubjectV2(*proof.ExpectedSubject) != nil || proof.ExpectedSubject.Provider != challenge.Binding.Provider {
				return identityOperationErrorV2("challenge.proof.source", "requires an exact source subject")
			}
		} else if proof.ExpectedSubject != nil {
			return identityOperationErrorV2("challenge.proof.target", "must not preselect an unverified target subject")
		}
	default:
		return identityOperationErrorV2("challenge.purpose", "is not an explicit login or provider-proof purpose")
	}
	if challenge.ConsumedAt == nil {
		if challenge.Secrets == nil || ValidatePKCEVerifier(challenge.Secrets.PKCEVerifier) != nil {
			return identityOperationErrorV2("challenge.secrets", "pending challenge requires its one-use PKCE verifier")
		}
		if challenge.Binding.Provider == IdentityProviderYandex {
			if challenge.Secrets.Nonce != "" {
				return identityOperationErrorV2("challenge.nonce", "Yandex OAuth has no OIDC nonce")
			}
		} else if ValidateOpaqueID("identity_challenge_v2.nonce", challenge.Secrets.Nonce) != nil {
			return identityOperationErrorV2("challenge.nonce", "Telegram requires its one-use nonce")
		}
	} else if challenge.Secrets != nil || challenge.ConsumedAt.Before(challenge.CreatedAt) || !challenge.ConsumedAt.Before(challenge.ExpiresAt) {
		return identityOperationErrorV2("challenge.consumed", "consumed challenge must be timely and immediately scrubbed")
	}
	type wire IdentityChallengeV2
	raw, err := json.Marshal(wire(challenge))
	if err != nil || len(raw) > IdentityAuthMaxRecordBytesV2 {
		return identityOperationErrorV2("challenge", "must fit the original 8 KiB record bound")
	}
	return nil
}

func (challenge IdentityChallengeV2) ValidateForOperation(operation IdentityOperationV2) error {
	if challenge.Validate() != nil || operation.Validate() != nil || challenge.Purpose != IdentityPurposeProviderProofV2 || operation.Pending == nil || challenge.Proof == nil {
		return identityOperationErrorV2("challenge.parent", "requires an exact nonterminal parent operation")
	}
	pending, proof := operation.Pending, challenge.Proof
	if proof.ParentOperationDigest != operation.OperationDigest || proof.ParentPhase != operation.Phase || proof.Scope != pending.Scope || challenge.CreatedAt.Before(operation.CreatedAt) || challenge.ExpiresAt.After(operation.ExpiresAt) {
		return identityOperationErrorV2("challenge.parent", "does not match original parent phase, scope and deadline")
	}
	switch operation.Phase {
	case IdentityPhaseSourceProofV2:
		if challenge.StateDigest != pending.SourceChallengeDigest || proof.ExpectedSubject == nil || *proof.ExpectedSubject != pending.SourceSubject || challenge.Binding.Provider != pending.SourceSubject.Provider {
			return identityOperationErrorV2("challenge.parent.source", "does not match fixed source child and subject")
		}
	case IdentityPhaseTargetProofV2:
		if operation.Kind != IdentityOperationLinkV2 || challenge.StateDigest != pending.TargetChallengeDigest || proof.ExpectedSubject != nil || challenge.Binding.Provider != pending.TargetProvider || pending.SourceProof == nil || challenge.CreatedAt.Before(pending.SourceProof.CompletedAt) {
			return identityOperationErrorV2("challenge.parent.target", "does not match independent target child after source proof")
		}
	default:
		return identityOperationErrorV2("challenge.parent.phase", "cannot exchange another provider challenge in this phase")
	}
	return nil
}

// IdentityChallengeExchangeV2 exists only in memory after successful consume.
// Persist ONLY the returned cleared challenge BEFORE making an external call.
// Crash/ambiguity makes this proof unavailable; no reset/re-exchange is legal.
type IdentityChallengeExchangeV2 struct {
	Binding      IdentityProviderBindingV2
	PKCEVerifier string
	Nonce        string
	StateDigest  SecretDigest
}

func (IdentityChallengeExchangeV2) MarshalJSON() ([]byte, error) {
	return nil, errors.New("ephemeral identity exchange must never be serialized")
}

func (challenge IdentityChallengeV2) Consume(expectedBinding IdentityProviderBindingV2, browserSecret string, parent *IdentityOperationV2, at time.Time) (IdentityChallengeV2, IdentityChallengeExchangeV2, error) {
	deny := func() (IdentityChallengeV2, IdentityChallengeExchangeV2, error) {
		return IdentityChallengeV2{}, IdentityChallengeExchangeV2{}, identityOperationErrorV2("challenge.consume", "requires matching live one-use browser/provider/parent binding")
	}
	if challenge.Validate() != nil || challenge.ConsumedAt != nil || expectedBinding.Validate() != nil || expectedBinding != challenge.Binding || browserSecret == "" || !challenge.BrowserBindingDigest.Matches(browserSecret) || at.Before(challenge.CreatedAt) || !at.Before(challenge.ExpiresAt) {
		return deny()
	}
	if challenge.Purpose == IdentityPurposeProviderProofV2 {
		if parent == nil || challenge.ValidateForOperation(*parent) != nil || at.Before(parent.CreatedAt) || !at.Before(parent.ExpiresAt) {
			return deny()
		}
	} else if parent != nil {
		return deny()
	}
	exchange := IdentityChallengeExchangeV2{Binding: challenge.Binding, PKCEVerifier: challenge.Secrets.PKCEVerifier, Nonce: challenge.Secrets.Nonce, StateDigest: challenge.StateDigest}
	next := challenge.clone()
	consumed := at
	next.ConsumedAt = &consumed
	next.Secrets = nil
	if err := next.Validate(); err != nil {
		return deny()
	}
	return next, exchange, nil
}

func (challenge IdentityChallengeV2) clone() IdentityChallengeV2 {
	if challenge.Login != nil {
		value := *challenge.Login
		challenge.Login = &value
	}
	if challenge.Proof != nil {
		value := *challenge.Proof
		if value.ExpectedSubject != nil {
			subject := *value.ExpectedSubject
			value.ExpectedSubject = &subject
		}
		challenge.Proof = &value
	}
	if challenge.Secrets != nil {
		value := *challenge.Secrets
		challenge.Secrets = &value
	}
	if challenge.ConsumedAt != nil {
		value := *challenge.ConsumedAt
		challenge.ConsumedAt = &value
	}
	return challenge
}
