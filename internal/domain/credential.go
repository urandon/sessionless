package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const CredentialBindingVersionV1 uint32 = 1

// CredentialSecretRef is an opaque backend locator. It deliberately redacts
// every formatting and JSON surface. Backends receive and return the opaque
// value as a whole; no public raw-string accessor exists.
type CredentialSecretRef struct {
	raw string
}

func NewCredentialSecretRef(raw string) (CredentialSecretRef, error) {
	if raw != strings.TrimSpace(raw) {
		return CredentialSecretRef{}, ValidationError{Field: "credential.secret_ref", Reason: "must not contain surrounding whitespace"}
	}
	ref := CredentialSecretRef{raw: raw}
	if err := ref.Validate(); err != nil {
		return CredentialSecretRef{}, err
	}
	return ref, nil
}

func (ref CredentialSecretRef) Validate() error {
	if ref.raw == "" || len(ref.raw) > 512 {
		return ValidationError{Field: "credential.secret_ref", Reason: "must be a bounded non-empty backend reference"}
	}
	return nil
}

func (ref CredentialSecretRef) String() string { return "[redacted]" }
func (ref CredentialSecretRef) IsZero() bool   { return ref.raw == "" }
func (ref CredentialSecretRef) GoString() string {
	return "domain.CredentialSecretRef([redacted])"
}
func (ref CredentialSecretRef) MarshalJSON() ([]byte, error) { return json.Marshal("[redacted]") }

type CredentialFingerprint string

func FingerprintCredential(value []byte) CredentialFingerprint {
	digest := sha256.Sum256(value)
	return CredentialFingerprint(hex.EncodeToString(digest[:]))
}

func (fingerprint CredentialFingerprint) Validate() error {
	decoded, err := hex.DecodeString(string(fingerprint))
	if err != nil || len(decoded) != sha256.Size || string(fingerprint) != strings.ToLower(string(fingerprint)) {
		return ValidationError{Field: "credential.secret_fingerprint", Reason: "must be a lowercase SHA-256 digest"}
	}
	return nil
}

type CredentialBinding struct {
	Version                  uint32                   `json:"version"`
	TenantID                 TenantID                 `json:"tenant_id"`
	SubscriptionConnectionID SubscriptionConnectionID `json:"subscription_connection_id"`
	OwnerUserID              UserID                   `json:"owner_user_id"`
	Provider                 string                   `json:"provider"`
	AuthMode                 string                   `json:"auth_mode"`
	SecretRef                CredentialSecretRef      `json:"-"`
	SecretFingerprint        CredentialFingerprint    `json:"secret_fingerprint"`
	Entitlement              EntitlementState         `json:"entitlement"`
	Generation               uint64                   `json:"generation"`
	Revoked                  bool                     `json:"revoked"`
	UpdatedAt                time.Time                `json:"updated_at"`
}

func (binding CredentialBinding) Validate() error {
	if binding.Version != CredentialBindingVersionV1 {
		return ValidationError{Field: "credential_binding.version", Reason: "is unsupported"}
	}
	if err := binding.TenantID.Validate(); err != nil {
		return err
	}
	if err := binding.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	if err := binding.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := ValidateOpaqueID("credential_binding.provider", binding.Provider); err != nil {
		return err
	}
	if err := ValidateOpaqueID("credential_binding.auth_mode", binding.AuthMode); err != nil {
		return err
	}
	if binding.Revoked {
		if !binding.SecretRef.IsZero() || binding.SecretFingerprint != "" || binding.Entitlement != EntitlementDisconnected {
			return ValidationError{Field: "credential_binding.revoked", Reason: "must clear active secret material and entitlement"}
		}
	} else {
		if err := binding.SecretRef.Validate(); err != nil {
			return err
		}
		if err := binding.SecretFingerprint.Validate(); err != nil {
			return err
		}
	}
	if !binding.Entitlement.Valid() {
		return ValidationError{Field: "credential_binding.entitlement", Reason: "is unknown"}
	}
	if binding.Generation == 0 {
		return ValidationError{Field: "credential_binding.generation", Reason: "must be positive"}
	}
	if binding.UpdatedAt.IsZero() {
		return ValidationError{Field: "credential_binding.updated_at", Reason: "must not be zero"}
	}
	return nil
}

func (binding CredentialBinding) String() string {
	return fmt.Sprintf(
		"CredentialBinding{version:%d tenant:%s connection:%s owner:%s provider:%s auth_mode:%s secret_ref:[redacted] fingerprint:%s entitlement:%s generation:%d revoked:%t}",
		binding.Version, binding.TenantID, binding.SubscriptionConnectionID, binding.OwnerUserID,
		binding.Provider, binding.AuthMode, binding.SecretFingerprint, binding.Entitlement,
		binding.Generation, binding.Revoked,
	)
}

func (binding CredentialBinding) GoString() string { return binding.String() }
