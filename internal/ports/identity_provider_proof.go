package ports

import (
	"context"
	"encoding/base64"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// IdentityProviderProverV1 returns verified provider DATA only. The original
// service must consume the one-use challenge before exchange and authenticate
// its parent/session/current scope before recording or using a proof.
type IdentityProviderProverV1 interface {
	ProofAuthorizationURL(context.Context, IdentityProviderProofAuthorizationRequestV1) (string, error)
	ExchangeAndVerifyProof(context.Context, IdentityProviderProofTokenRequestV1) (IdentityProviderProofResultV1, error)
}

type IdentityProviderProofAuthorizationRequestV1 struct {
	Binding                                     domain.IdentityProviderBindingV2
	State, Nonce, CodeChallenge                 string
	ChallengeCreatedAt, ChallengeExpiresAt, Now time.Time
}

type IdentityProviderProofTokenRequestV1 struct {
	Binding                                     domain.IdentityProviderBindingV2
	Code, PKCEVerifier, ExpectedNonce           string
	ChallengeCreatedAt, ChallengeExpiresAt, Now time.Time
}

type IdentityProviderProofResultV1 struct {
	Subject        domain.ExternalSubject
	Kind           domain.IdentityProviderProofKindV2
	SignedAuthTime *time.Time
}

func (r IdentityProviderProofAuthorizationRequestV1) Validate() error {
	if err := validateProviderProofRequestV1(r.Binding, r.Nonce, r.ChallengeCreatedAt, r.ChallengeExpiresAt, r.Now); err != nil {
		return err
	}
	if domain.ValidateOpaqueID("identity_proof.state", r.State) != nil {
		return domain.ValidationError{Field: "identity_proof.state", Reason: "requires bounded opaque state"}
	}
	decoded, err := base64.RawURLEncoding.DecodeString(r.CodeChallenge)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != r.CodeChallenge {
		return domain.ValidationError{Field: "identity_proof.code_challenge", Reason: "requires canonical S256 base64url digest"}
	}
	return nil
}

func (r IdentityProviderProofTokenRequestV1) Validate() error {
	if err := validateProviderProofRequestV1(r.Binding, r.ExpectedNonce, r.ChallengeCreatedAt, r.ChallengeExpiresAt, r.Now); err != nil {
		return err
	}
	if len(r.Code) == 0 || len(r.Code) > 4096 {
		return domain.ValidationError{Field: "identity_proof.code", Reason: "requires bounded authorization code"}
	}
	for _, value := range []byte(r.Code) {
		if value < 0x20 || value > 0x7e {
			return domain.ValidationError{Field: "identity_proof.code", Reason: "requires OAuth VSCHAR authorization code"}
		}
	}
	return domain.ValidatePKCEVerifier(r.PKCEVerifier)
}

func validateProviderProofRequestV1(binding domain.IdentityProviderBindingV2, nonce string, created, expires, now time.Time) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	if created.IsZero() || expires.IsZero() || now.IsZero() || !expires.After(created) || expires.Sub(created) > domain.IdentityOperationMaxLifetimeV2 || now.Before(created) || !now.Before(expires) {
		return domain.ValidationError{Field: "identity_proof.time", Reason: "requires original live challenge of at most ten minutes"}
	}
	if binding.Provider == domain.IdentityProviderYandex {
		if nonce != "" {
			return domain.ValidationError{Field: "identity_proof.nonce", Reason: "Yandex OAuth has no nonce"}
		}
	} else if domain.ValidateOpaqueID("identity_proof.nonce", nonce) != nil {
		return domain.ValidationError{Field: "identity_proof.nonce", Reason: "Telegram requires bounded original nonce"}
	}
	return nil
}
