package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const ProviderCredentialBindingVersionV1 uint32 = 1
const ProviderCredentialAuditEventVersionV1 uint32 = 1

type ProviderCredentialStateV1 string
type ProviderCredentialAuditActionV1 string
type ProviderCredentialAuditReceiptIDV1 string

const (
	ProviderCredentialActiveV1  ProviderCredentialStateV1 = "active"
	ProviderCredentialRevokedV1 ProviderCredentialStateV1 = "revoked"

	ProviderCredentialAuditIngestedV1 ProviderCredentialAuditActionV1 = "ingested"
	ProviderCredentialAuditRotatedV1  ProviderCredentialAuditActionV1 = "rotated"
	ProviderCredentialAuditRevokedV1  ProviderCredentialAuditActionV1 = "revoked"
)

type ProviderCredentialAuditEventV1 struct {
	Version              uint32                             `json:"version"`
	ReceiptID            ProviderCredentialAuditReceiptIDV1 `json:"receipt_id"`
	TenantID             TenantID                           `json:"tenant_id"`
	OwnerUserID          UserID                             `json:"owner_user_id"`
	ResourceKind         ProviderResourceKindV1             `json:"resource_kind"`
	ResourceID           string                             `json:"resource_id"`
	ResourceRevision     uint64                             `json:"resource_revision"`
	CredentialGeneration uint64                             `json:"credential_generation"`
	CandidateMutationID  string                             `json:"candidate_mutation_id"`
	Action               ProviderCredentialAuditActionV1    `json:"action"`
	OccurredAt           time.Time                          `json:"occurred_at"`
}

func NewProviderCredentialAuditEventV1(binding ProviderCredentialBindingV1, action ProviderCredentialAuditActionV1) (ProviderCredentialAuditEventV1, error) {
	if err := binding.Validate(); err != nil {
		return ProviderCredentialAuditEventV1{}, err
	}
	if action != ProviderCredentialAuditIngestedV1 && action != ProviderCredentialAuditRotatedV1 && action != ProviderCredentialAuditRevokedV1 {
		return ProviderCredentialAuditEventV1{}, ValidationError{Field: "provider_credential_audit.action", Reason: "is unsupported"}
	}
	parts := []string{string(binding.TenantID), string(binding.OwnerUserID), string(binding.ResourceKind), binding.ResourceID,
		strconv.FormatUint(binding.ResourceRevision, 10), strconv.FormatUint(binding.CredentialGeneration, 10), binding.CandidateMutationID, string(action)}
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strconv.Itoa(len(part))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(part))
	}
	event := ProviderCredentialAuditEventV1{
		Version:   ProviderCredentialAuditEventVersionV1,
		ReceiptID: ProviderCredentialAuditReceiptIDV1("pca_" + hex.EncodeToString(hash.Sum(nil))),
		TenantID:  binding.TenantID, OwnerUserID: binding.OwnerUserID, ResourceKind: binding.ResourceKind, ResourceID: binding.ResourceID,
		ResourceRevision: binding.ResourceRevision, CredentialGeneration: binding.CredentialGeneration, CandidateMutationID: binding.CandidateMutationID, Action: action, OccurredAt: binding.UpdatedAt,
	}
	return event, event.Validate()
}

func (event ProviderCredentialAuditEventV1) Validate() error {
	if event.Version != ProviderCredentialAuditEventVersionV1 {
		return ValidationError{Field: "provider_credential_audit.version", Reason: "must equal 1"}
	}
	if !strings.HasPrefix(string(event.ReceiptID), "pca_") || len(event.ReceiptID) != 4+sha256.Size*2 {
		return ValidationError{Field: "provider_credential_audit.receipt_id", Reason: "must be a canonical receipt digest"}
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(string(event.ReceiptID), "pca_")); err != nil {
		return ValidationError{Field: "provider_credential_audit.receipt_id", Reason: "must be a canonical receipt digest"}
	}
	for _, validate := range []func() error{event.TenantID.Validate, event.OwnerUserID.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if event.ResourceKind != ProviderResourceAPIAccountV1 && event.ResourceKind != ProviderResourceRouterAccountV1 {
		return ValidationError{Field: "provider_credential_audit.resource_kind", Reason: "is unsupported"}
	}
	if err := ValidateOpaqueID("provider_credential_audit.resource_id", event.ResourceID); err != nil {
		return err
	}
	if event.ResourceRevision == 0 || event.CredentialGeneration == 0 || event.OccurredAt.IsZero() {
		return ValidationError{Field: "provider_credential_audit.authority", Reason: "requires positive revisions and occurrence time"}
	}
	if err := ValidateOpaqueID("provider_credential_audit.candidate_mutation_id", event.CandidateMutationID); err != nil {
		return err
	}
	if event.Action != ProviderCredentialAuditIngestedV1 && event.Action != ProviderCredentialAuditRotatedV1 && event.Action != ProviderCredentialAuditRevokedV1 {
		return ValidationError{Field: "provider_credential_audit.action", Reason: "is unsupported"}
	}
	return nil
}

// ProviderCredentialBindingV1 is the provider-resource credential authority.
// It is intentionally separate from the subscription connection credential
// lifecycle. SecretRef is an opaque backend locator and never serializes.
type ProviderCredentialBindingV1 struct {
	Version              uint32                    `json:"version"`
	TenantID             TenantID                  `json:"tenant_id"`
	OwnerUserID          UserID                    `json:"owner_user_id"`
	ResourceKind         ProviderResourceKindV1    `json:"resource_kind"`
	ResourceID           string                    `json:"resource_id"`
	ResourceRevision     uint64                    `json:"resource_revision"`
	CredentialGeneration uint64                    `json:"credential_generation"`
	CandidateMutationID  string                    `json:"candidate_mutation_id"`
	State                ProviderCredentialStateV1 `json:"state"`
	SecretRef            CredentialSecretRef       `json:"-"`
	SecretFingerprint    CredentialFingerprint     `json:"secret_fingerprint,omitempty"`
	UpdatedAt            time.Time                 `json:"updated_at"`
}

func (binding ProviderCredentialBindingV1) Clone() ProviderCredentialBindingV1 { return binding }

func (binding ProviderCredentialBindingV1) Validate() error {
	if binding.Version != ProviderCredentialBindingVersionV1 {
		return ValidationError{Field: "provider_credential.version", Reason: "must equal 1"}
	}
	for _, validate := range []func() error{binding.TenantID.Validate, binding.OwnerUserID.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if binding.ResourceKind != ProviderResourceAPIAccountV1 && binding.ResourceKind != ProviderResourceRouterAccountV1 {
		return ValidationError{Field: "provider_credential.resource_kind", Reason: "must be api_account or router_account"}
	}
	if err := ValidateOpaqueID("provider_credential.resource_id", binding.ResourceID); err != nil {
		return err
	}
	if binding.ResourceRevision == 0 || binding.CredentialGeneration == 0 {
		return ValidationError{Field: "provider_credential.revision", Reason: "resource revision and credential generation must be positive"}
	}
	if err := ValidateOpaqueID("provider_credential.candidate_mutation_id", binding.CandidateMutationID); err != nil {
		return err
	}
	switch binding.State {
	case ProviderCredentialActiveV1:
		if err := binding.SecretRef.Validate(); err != nil {
			return err
		}
		if err := binding.SecretFingerprint.Validate(); err != nil {
			return err
		}
	case ProviderCredentialRevokedV1:
		if !binding.SecretRef.IsZero() || binding.SecretFingerprint != "" {
			return ValidationError{Field: "provider_credential.revoked", Reason: "must clear the active secret reference and fingerprint"}
		}
	default:
		return ValidationError{Field: "provider_credential.state", Reason: "is unsupported"}
	}
	if binding.UpdatedAt.IsZero() {
		return ValidationError{Field: "provider_credential.updated_at", Reason: "must not be zero"}
	}
	return nil
}

func (binding ProviderCredentialBindingV1) ResourceBinding() (ProviderResourceBindingV1, error) {
	if err := binding.Validate(); err != nil {
		return ProviderResourceBindingV1{}, err
	}
	if binding.State != ProviderCredentialActiveV1 {
		return ProviderResourceBindingV1{}, ValidationError{Field: "provider_credential.state", Reason: "must be active"}
	}
	return binding.AuthorityResourceBinding()
}

func (binding ProviderCredentialBindingV1) AuthorityResourceBinding() (ProviderResourceBindingV1, error) {
	if err := binding.Validate(); err != nil {
		return ProviderResourceBindingV1{}, err
	}
	resource := ProviderResourceBindingV1{
		Kind: binding.ResourceKind, ResourceID: binding.ResourceID, OwnerUserID: binding.OwnerUserID,
		Revision: binding.ResourceRevision, CredentialMode: ProviderCredentialInvocationV1,
		CredentialGeneration: binding.CredentialGeneration,
	}
	return resource, resource.Validate()
}

func (binding ProviderCredentialBindingV1) String() string {
	return fmt.Sprintf("ProviderCredentialBindingV1{tenant:%s owner:%s kind:%s resource:%s revision:%d generation:%d state:%s secret_ref:[redacted] fingerprint:%s}",
		binding.TenantID, binding.OwnerUserID, binding.ResourceKind, binding.ResourceID,
		binding.ResourceRevision, binding.CredentialGeneration, binding.State, binding.SecretFingerprint)
}

func (binding ProviderCredentialBindingV1) GoString() string { return binding.String() }
