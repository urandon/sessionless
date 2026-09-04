package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"time"
	"unicode/utf8"
)

const HarnessBindingVersionV1 uint32 = 1

type (
	HarnessKindV1                    string
	HarnessBackendKindV1             string
	HarnessArtifactKindV1            string
	HarnessBindingDigestV1           string
	ProviderContractKindV1           string
	ProviderResourceKindV1           string
	ProviderCredentialModeV1         string
	ProviderCredentialDeliveryKindV1 string
)

const (
	HarnessKindSessionlessV1 HarnessKindV1 = "sessionless_harness"

	HarnessBackendDeterministicFixtureV1 HarnessBackendKindV1 = "deterministic_fixture"
	HarnessBackendCodexExecV1            HarnessBackendKindV1 = "codex_exec"
	HarnessBackendOpenCodeV1             HarnessBackendKindV1 = "opencode"
	HarnessBackendPiV1                   HarnessBackendKindV1 = "pi"
	HarnessBackendDirectOpenRouterV1     HarnessBackendKindV1 = "direct_openrouter"

	HarnessArtifactEmbeddedProfileV1 HarnessArtifactKindV1 = "embedded_profile"
	HarnessArtifactExecutableV1      HarnessArtifactKindV1 = "executable"
	HarnessArtifactContainerImageV1  HarnessArtifactKindV1 = "container_image"

	ProviderContractCredentiallessFixtureV1 ProviderContractKindV1 = "credentialless_fixture"
	ProviderContractInvocationV1            ProviderContractKindV1 = "invocation"

	ProviderResourceCredentiallessFixtureV1 ProviderResourceKindV1 = "credentialless_fixture"
	ProviderResourceSubscriptionV1          ProviderResourceKindV1 = "subscription"
	ProviderResourceAPIAccountV1            ProviderResourceKindV1 = "api_account"
	ProviderResourceRouterAccountV1         ProviderResourceKindV1 = "router_account"

	ProviderCredentialNoneV1       ProviderCredentialModeV1 = "none"
	ProviderCredentialInvocationV1 ProviderCredentialModeV1 = "invocation"

	ProviderCredentialDeliveryNoneV1        ProviderCredentialDeliveryKindV1 = "none"
	ProviderCredentialDeliveryFileV1        ProviderCredentialDeliveryKindV1 = "file"
	ProviderCredentialDeliveryEnvironmentV1 ProviderCredentialDeliveryKindV1 = "environment"
	ProviderCredentialDeliveryDirectV1      ProviderCredentialDeliveryKindV1 = "direct"
)

// HarnessBackendDescriptorV1 identifies one immutable backend implementation
// inside the Sessionless-owned outer harness. It is a registry key, not a
// provider lifecycle state and not a request for installed-binary discovery.
type HarnessBackendDescriptorV1 struct {
	HarnessKind            HarnessKindV1                    `json:"harness_kind"`
	HarnessVersion         string                           `json:"harness_version"`
	BackendKind            HarnessBackendKindV1             `json:"backend_kind"`
	ArtifactKind           HarnessArtifactKindV1            `json:"artifact_kind"`
	ArtifactDigest         string                           `json:"artifact_digest"`
	NativeProtocolVersion  string                           `json:"native_protocol_version"`
	BackendProfileDigest   string                           `json:"backend_profile_digest"`
	ProviderContractKind   ProviderContractKindV1           `json:"provider_contract_kind"`
	CredentialDeliveryKind ProviderCredentialDeliveryKindV1 `json:"credential_delivery_kind"`
}

func (descriptor HarnessBackendDescriptorV1) Validate() error {
	if descriptor.HarnessKind != HarnessKindSessionlessV1 {
		return ValidationError{Field: "harness_binding.backend.harness_kind", Reason: "is unsupported"}
	}
	switch descriptor.BackendKind {
	case HarnessBackendDeterministicFixtureV1, HarnessBackendCodexExecV1, HarnessBackendOpenCodeV1, HarnessBackendPiV1, HarnessBackendDirectOpenRouterV1:
	default:
		return ValidationError{Field: "harness_binding.backend.backend_kind", Reason: "is unsupported"}
	}
	switch descriptor.ArtifactKind {
	case HarnessArtifactEmbeddedProfileV1, HarnessArtifactExecutableV1, HarnessArtifactContainerImageV1:
	default:
		return ValidationError{Field: "harness_binding.backend.artifact_kind", Reason: "is unsupported"}
	}
	if descriptor.BackendKind == HarnessBackendDeterministicFixtureV1 && descriptor.ArtifactKind != HarnessArtifactEmbeddedProfileV1 {
		return ValidationError{Field: "harness_binding.backend.artifact_kind", Reason: "must be embedded_profile for the deterministic fixture"}
	}
	for field, value := range map[string]string{
		"harness_binding.backend.harness_version":         descriptor.HarnessVersion,
		"harness_binding.backend.native_protocol_version": descriptor.NativeProtocolVersion,
	} {
		if err := validateProviderToken(field, value, 128); err != nil {
			return err
		}
	}
	if err := validateSHA256("harness_binding.backend.artifact_digest", descriptor.ArtifactDigest); err != nil {
		return err
	}
	if err := validateSHA256("harness_binding.backend.backend_profile_digest", descriptor.BackendProfileDigest); err != nil {
		return err
	}
	switch descriptor.ProviderContractKind {
	case ProviderContractCredentiallessFixtureV1:
		if descriptor.BackendKind != HarnessBackendDeterministicFixtureV1 {
			return ValidationError{Field: "harness_binding.backend", Reason: "credentialless contract is restricted to the deterministic fixture"}
		}
		if descriptor.CredentialDeliveryKind != ProviderCredentialDeliveryNoneV1 {
			return ValidationError{Field: "harness_binding.backend.credential_delivery_kind", Reason: "must be none for the credentialless fixture"}
		}
	case ProviderContractInvocationV1:
		if descriptor.BackendKind == HarnessBackendDeterministicFixtureV1 {
			return ValidationError{Field: "harness_binding.backend", Reason: "deterministic fixture cannot claim the invocation contract"}
		}
		switch descriptor.CredentialDeliveryKind {
		case ProviderCredentialDeliveryFileV1, ProviderCredentialDeliveryEnvironmentV1, ProviderCredentialDeliveryDirectV1:
		default:
			return ValidationError{Field: "harness_binding.backend.credential_delivery_kind", Reason: "is unsupported for provider invocation"}
		}
	default:
		return ValidationError{Field: "harness_binding.backend.provider_contract_kind", Reason: "is unsupported"}
	}
	return nil
}

// ProviderResourceBindingV1 is the exact resource authority selected for an
// invocation. Entitlement, provider, model vendor, credential mode and harness
// identity deliberately remain separate axes.
type ProviderResourceBindingV1 struct {
	Kind                 ProviderResourceKindV1   `json:"kind"`
	ResourceID           string                   `json:"resource_id"`
	OwnerUserID          UserID                   `json:"owner_user_id"`
	Revision             uint64                   `json:"revision"`
	CredentialMode       ProviderCredentialModeV1 `json:"credential_mode"`
	CredentialGeneration uint64                   `json:"credential_generation"`
}

func (resource ProviderResourceBindingV1) Validate() error {
	if err := resource.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := ValidateOpaqueID("harness_binding.resource.resource_id", resource.ResourceID); err != nil {
		return err
	}
	if resource.Revision == 0 {
		return ValidationError{Field: "harness_binding.resource.revision", Reason: "must be positive"}
	}
	switch resource.CredentialMode {
	case ProviderCredentialNoneV1:
		if resource.Kind != ProviderResourceCredentiallessFixtureV1 || resource.CredentialGeneration != 0 {
			return ValidationError{Field: "harness_binding.resource", Reason: "credentialless mode is restricted to the fixture resource with generation zero"}
		}
	case ProviderCredentialInvocationV1:
		if resource.Kind != ProviderResourceSubscriptionV1 && resource.Kind != ProviderResourceAPIAccountV1 && resource.Kind != ProviderResourceRouterAccountV1 {
			return ValidationError{Field: "harness_binding.resource.kind", Reason: "does not support invocation credentials"}
		}
		if resource.CredentialGeneration == 0 {
			return ValidationError{Field: "harness_binding.resource.credential_generation", Reason: "must be positive for invocation credentials"}
		}
	default:
		return ValidationError{Field: "harness_binding.resource.credential_mode", Reason: "is unsupported"}
	}
	return nil
}

func (resource ProviderResourceBindingV1) ValidateForBackend(backend HarnessBackendDescriptorV1) error {
	if err := backend.Validate(); err != nil {
		return err
	}
	if err := resource.Validate(); err != nil {
		return err
	}
	if backend.ProviderContractKind == ProviderContractCredentiallessFixtureV1 {
		if resource.Kind != ProviderResourceCredentiallessFixtureV1 || resource.CredentialMode != ProviderCredentialNoneV1 {
			return ValidationError{Field: "harness_binding", Reason: "credentialless fixture backend requires the credentialless fixture resource"}
		}
		return nil
	}
	if resource.CredentialMode != ProviderCredentialInvocationV1 {
		return ValidationError{Field: "harness_binding", Reason: "provider backend requires invocation credentials"}
	}
	return nil
}

// HarnessBindingV1 is sealed by a server-owned binder before durable dispatch.
// It is immutable execution authority, never a public request DTO or a second
// run/attempt state machine.
type HarnessBindingV1 struct {
	Version                  uint32                     `json:"version"`
	TenantID                 TenantID                   `json:"tenant_id"`
	OwnerUserID              UserID                     `json:"owner_user_id"`
	RunID                    RunID                      `json:"run_id"`
	AttemptID                AttemptID                  `json:"attempt_id"`
	Backend                  HarnessBackendDescriptorV1 `json:"backend"`
	Resource                 ProviderResourceBindingV1  `json:"resource"`
	ModelVendorID            string                     `json:"model_vendor_id"`
	ModelID                  string                     `json:"model_id"`
	InputDataClass           ProviderDataClassV1        `json:"input_data_class"`
	ProviderCatalogDigest    string                     `json:"provider_catalog_digest"`
	ProviderRouteDigest      string                     `json:"provider_route_digest"`
	PrivacyPolicyDigest      string                     `json:"privacy_policy_digest"`
	CapabilityEvidenceDigest string                     `json:"capability_evidence_digest"`
	EffectivePolicyDigest    string                     `json:"effective_policy_digest"`
	ExecutionPlacementDigest string                     `json:"execution_placement_digest"`
	EvidenceExpiresAt        *time.Time                 `json:"evidence_expires_at,omitempty"`
}

func (binding HarnessBindingV1) Clone() HarnessBindingV1 {
	clone := binding
	if binding.EvidenceExpiresAt != nil {
		value := binding.EvidenceExpiresAt.UTC()
		clone.EvidenceExpiresAt = &value
	}
	return clone
}

func (binding HarnessBindingV1) Validate() error {
	if binding.Version != HarnessBindingVersionV1 {
		return ValidationError{Field: "harness_binding.version", Reason: "must equal 1"}
	}
	for _, validate := range []func() error{binding.TenantID.Validate, binding.OwnerUserID.Validate, binding.RunID.Validate, binding.AttemptID.Validate, binding.Backend.Validate, binding.Resource.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if binding.OwnerUserID != binding.Resource.OwnerUserID {
		return ValidationError{Field: "harness_binding.resource.owner_user_id", Reason: "must match the binding owner"}
	}
	if err := validateProviderToken("harness_binding.model_vendor_id", binding.ModelVendorID, 128); err != nil {
		return err
	}
	if err := validateProviderToken("harness_binding.model_id", binding.ModelID, 256); err != nil {
		return err
	}
	if binding.InputDataClass != ProviderDataPublicV1 && binding.InputDataClass != ProviderDataExternallyShareableV1 && binding.InputDataClass != ProviderDataPrivateV1 {
		return ValidationError{Field: "harness_binding.input_data_class", Reason: "is unsupported"}
	}
	for field, value := range map[string]string{
		"harness_binding.provider_catalog_digest":    binding.ProviderCatalogDigest,
		"harness_binding.provider_route_digest":      binding.ProviderRouteDigest,
		"harness_binding.privacy_policy_digest":      binding.PrivacyPolicyDigest,
		"harness_binding.capability_evidence_digest": binding.CapabilityEvidenceDigest,
		"harness_binding.effective_policy_digest":    binding.EffectivePolicyDigest,
		"harness_binding.execution_placement_digest": binding.ExecutionPlacementDigest,
	} {
		if err := validateSHA256(field, value); err != nil {
			return err
		}
	}
	if err := binding.Resource.ValidateForBackend(binding.Backend); err != nil {
		return err
	}
	if binding.Backend.ProviderContractKind == ProviderContractCredentiallessFixtureV1 {
		if binding.EvidenceExpiresAt != nil {
			return ValidationError{Field: "harness_binding.evidence_expires_at", Reason: "must be absent for the closed credentialless fixture"}
		}
	} else if binding.Resource.CredentialMode != ProviderCredentialInvocationV1 {
		return ValidationError{Field: "harness_binding", Reason: "provider backend requires invocation credentials"}
	} else if binding.EvidenceExpiresAt == nil {
		return ValidationError{Field: "harness_binding.evidence_expires_at", Reason: "is required for provider invocation evidence"}
	}
	if binding.EvidenceExpiresAt != nil && binding.EvidenceExpiresAt.IsZero() {
		return ValidationError{Field: "harness_binding.evidence_expires_at", Reason: "must not point to zero time"}
	}
	return nil
}

// ValidateAt applies the time-dependent evidence fence immediately before a
// backend can receive credentials or start a process/network operation.
func (binding HarnessBindingV1) ValidateAt(now time.Time) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	if now.IsZero() {
		return ValidationError{Field: "harness_binding.now", Reason: "must not be zero"}
	}
	if binding.Backend.ProviderContractKind == ProviderContractCredentiallessFixtureV1 {
		return nil
	}
	if binding.EvidenceExpiresAt == nil || !now.UTC().Before(binding.EvidenceExpiresAt.UTC()) {
		return ValidationError{Field: "harness_binding.evidence_expires_at", Reason: "must be present and later than authoritative execution time"}
	}
	return nil
}

func (binding HarnessBindingV1) ValidateForScope(tenantID TenantID, ownerUserID UserID, runID RunID, attemptID AttemptID, placement ExecutionPlacementV2) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	if binding.TenantID != tenantID || binding.OwnerUserID != ownerUserID || binding.RunID != runID || binding.AttemptID != attemptID {
		return ValidationError{Field: "harness_binding.scope", Reason: "must match the owning dispatch scope"}
	}
	placementDigest, err := ExecutionPlacementDigest(placement)
	if err != nil {
		return err
	}
	if binding.ExecutionPlacementDigest != string(placementDigest) {
		return ValidationError{Field: "harness_binding.execution_placement_digest", Reason: "must match the admitted placement"}
	}
	return nil
}

func (binding HarnessBindingV1) Digest() (HarnessBindingDigestV1, error) {
	if err := binding.Validate(); err != nil {
		return "", err
	}
	hash := sha256.New()
	hash.Write([]byte("sessionless.harness-binding.v1\x00"))
	appendString := func(value string) {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		hash.Write(size[:])
		hash.Write([]byte(value))
	}
	appendUint := func(value uint64) {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], value)
		hash.Write(encoded[:])
	}
	appendUint(uint64(binding.Version))
	for _, value := range []string{
		string(binding.TenantID), string(binding.OwnerUserID), string(binding.RunID), string(binding.AttemptID),
		string(binding.Backend.HarnessKind), binding.Backend.HarnessVersion, string(binding.Backend.BackendKind),
		string(binding.Backend.ArtifactKind), binding.Backend.ArtifactDigest,
		binding.Backend.NativeProtocolVersion, binding.Backend.BackendProfileDigest, string(binding.Backend.ProviderContractKind), string(binding.Backend.CredentialDeliveryKind),
		string(binding.Resource.Kind), binding.Resource.ResourceID, string(binding.Resource.OwnerUserID),
	} {
		appendString(value)
	}
	appendUint(binding.Resource.Revision)
	appendString(string(binding.Resource.CredentialMode))
	appendUint(binding.Resource.CredentialGeneration)
	for _, value := range []string{binding.ModelVendorID, binding.ModelID, string(binding.InputDataClass), binding.ProviderCatalogDigest, binding.ProviderRouteDigest, binding.PrivacyPolicyDigest, binding.CapabilityEvidenceDigest, binding.EffectivePolicyDigest, binding.ExecutionPlacementDigest} {
		appendString(value)
	}
	if binding.EvidenceExpiresAt == nil {
		appendUint(0)
	} else {
		appendUint(1)
		appendString(binding.EvidenceExpiresAt.UTC().Format(time.RFC3339Nano))
	}
	return HarnessBindingDigestV1(hex.EncodeToString(hash.Sum(nil))), nil
}

func (digest HarnessBindingDigestV1) Validate() error {
	return validateSHA256("harness_binding_digest", string(digest))
}

func ExecutionPlacementDigest(placement ExecutionPlacementV2) (HarnessBindingDigestV1, error) {
	if err := placement.Validate(); err != nil {
		return "", err
	}
	hash := sha256.New()
	hash.Write([]byte("sessionless.execution-placement.v2\x00"))
	var version [8]byte
	binary.BigEndian.PutUint64(version[:], uint64(placement.Version))
	hash.Write(version[:])
	values := []string{string(placement.Kind), string(placement.FallbackPolicy), string(placement.OwnerUserID), string(placement.WorkerID), string(placement.CapabilityDigest), string(placement.PolicyDigest)}
	values = append(values, placement.SubstrateBindingDigest)
	for _, value := range values {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		hash.Write(size[:])
		hash.Write([]byte(value))
	}
	return HarnessBindingDigestV1(hex.EncodeToString(hash.Sum(nil))), nil
}

func validateProviderToken(field, value string, maxBytes int) error {
	if value == "" || value != strings.TrimSpace(value) || !utf8.ValidString(value) || len(value) > maxBytes {
		return ValidationError{Field: field, Reason: "must be a bounded non-empty canonical UTF-8 token"}
	}
	for _, value := range value {
		if value < 0x21 || value == 0x7f {
			return ValidationError{Field: field, Reason: "must not contain whitespace or control characters"}
		}
	}
	return nil
}
