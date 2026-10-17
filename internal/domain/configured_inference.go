package domain

import (
	"bytes"
	"strconv"
	"strings"
	"time"
)

const (
	ConfiguredInferenceProfileVersionV1          uint32 = 1
	ConfiguredInferenceProtocolChatCompletionsV1 string = "openai-chat-completions.v1"
	ConfiguredInferenceLoopbackPolicyV1                 = "local_loopback_http.v1"
	ConfiguredInferenceMaxCredentialBytesV1      int64  = 4096
)

type ConfiguredInferenceAuthV1 string

const (
	ConfiguredInferenceAuthNoneV1   ConfiguredInferenceAuthV1 = "none"
	ConfiguredInferenceAuthBearerV1 ConfiguredInferenceAuthV1 = "bearer"
)

// ConfiguredInferenceCapabilitiesV1 records explicit evidence declarations.
// Structural validity is not observed model behavior or permission to enable
// these capabilities; admission still verifies the original evidence bodies.
type ConfiguredInferenceCapabilitiesV1 struct {
	Text       ProviderEvidenceStateV1 `json:"text"`
	Image      ProviderEvidenceStateV1 `json:"image"`
	File       ProviderEvidenceStateV1 `json:"file"`
	ToolCalls  ProviderEvidenceStateV1 `json:"tool_calls"`
	JSONSchema ProviderEvidenceStateV1 `json:"json_schema"`
}

// ConfiguredInferenceLimitsV1 is the configured ceiling, never a new budget or
// a measurement. Each operation additionally intersects its original authority.
type ConfiguredInferenceLimitsV1 struct {
	RequestBytes        uint64        `json:"request_bytes"`
	ResponseBytes       uint64        `json:"response_bytes"`
	ResponseHeaderBytes uint64        `json:"response_header_bytes"`
	JSONDepth           uint64        `json:"json_depth"`
	JSONMembers         uint64        `json:"json_members"`
	Choices             uint64        `json:"choices"`
	ToolCalls           uint64        `json:"tool_calls"`
	ToolNameBytes       uint64        `json:"tool_name_bytes"`
	ToolCallIDBytes     uint64        `json:"tool_call_id_bytes"`
	ToolArgumentsBytes  uint64        `json:"tool_arguments_bytes"`
	FinalTextBytes      uint64        `json:"final_text_bytes"`
	Messages            uint64        `json:"messages"`
	Tools               uint64        `json:"tools"`
	MaxInputTokens      uint64        `json:"max_input_tokens"`
	MaxOutputTokens     uint64        `json:"max_output_tokens"`
	OperationTimeout    time.Duration `json:"operation_timeout"`
	CleanupTimeout      time.Duration `json:"cleanup_timeout"`
}

func (v ConfiguredInferenceLimitsV1) Validate() error {
	for _, item := range []struct {
		name           string
		value, maximum uint64
	}{
		{"request_bytes", v.RequestBytes, 1 << 20}, {"response_bytes", v.ResponseBytes, 1 << 20},
		{"response_header_bytes", v.ResponseHeaderBytes, 16 << 10}, {"json_depth", v.JSONDepth, 32},
		{"json_members", v.JSONMembers, 256}, {"choices", v.Choices, 1}, {"tool_calls", v.ToolCalls, 1},
		{"tool_name_bytes", v.ToolNameBytes, 128}, {"tool_call_id_bytes", v.ToolCallIDBytes, 128},
		{"tool_arguments_bytes", v.ToolArgumentsBytes, 4 << 10}, {"final_text_bytes", v.FinalTextBytes, 256 << 10},
		{"messages", v.Messages, 256}, {"tools", v.Tools, 16}, {"max_input_tokens", v.MaxInputTokens, 1 << 20},
		{"max_output_tokens", v.MaxOutputTokens, 2048},
	} {
		if item.value == 0 || item.value > item.maximum {
			return configuredInferenceError("limits."+item.name, "requires a positive bounded ceiling")
		}
	}
	if v.OperationTimeout <= 0 || v.OperationTimeout > 30*time.Second || v.CleanupTimeout <= 0 || v.CleanupTimeout > 30*time.Second {
		return configuredInferenceError("limits.timeout", "operation and cleanup must each fit thirty seconds")
	}
	if v.ToolArgumentsBytes > v.ResponseBytes || v.FinalTextBytes > v.ResponseBytes {
		return configuredInferenceError("limits.response", "individual outputs must fit the response ceiling")
	}
	return nil
}

// ConfiguredInferenceProfileV1 is bounded trusted installation DATA. It contains
// no credential bytes and no self-referential backend/profile digest. A digest,
// declaration or successful Validate does not authenticate an installer, model,
// current source, funding or retained connection, and cannot authorize a send.
type ConfiguredInferenceProfileV1 struct {
	Version                             uint32                            `json:"version"`
	TenantID                            TenantID                          `json:"tenant_id"`
	OwnerUserID                         UserID                            `json:"owner_user_id"`
	ProfileRevision                     uint64                            `json:"profile_revision"`
	Resource                            ProviderResourceBindingV1         `json:"resource"`
	BaseURL                             string                            `json:"base_url"`
	EndpointPolicy                      string                            `json:"endpoint_policy"`
	NativeProtocolVersion               string                            `json:"native_protocol_version"`
	Auth                                ConfiguredInferenceAuthV1         `json:"auth"`
	ModelVendorID                       string                            `json:"model_vendor_id"`
	ModelID                             string                            `json:"model_id"`
	ResponseModelIDs                    []string                          `json:"response_model_ids"`
	ServerID                            string                            `json:"server_id"`
	ServerVersion                       string                            `json:"server_version"`
	ChatTemplateDigest                  string                            `json:"chat_template_digest"`
	Capabilities                        ConfiguredInferenceCapabilitiesV1 `json:"capabilities"`
	CapabilityEvidence                  OwnedAgentAuthorityBodyRefV1      `json:"capability_evidence"`
	ProviderCatalogDigest               string                            `json:"provider_catalog_digest"`
	ProviderRouteDigest                 string                            `json:"provider_route_digest"`
	PrivacyPolicyDigest                 string                            `json:"privacy_policy_digest"`
	EffectivePolicyDigest               string                            `json:"effective_policy_digest"`
	AllowedDataClasses                  []ProviderDataClassV1             `json:"allowed_data_classes"`
	ModelWindowTokens                   uint64                            `json:"model_window_tokens"`
	TokenEstimatorVersion               string                            `json:"token_estimator_version"`
	CalibrationDigest                   string                            `json:"calibration_digest"`
	Limits                              ConfiguredInferenceLimitsV1       `json:"limits"`
	LocalInstallationID                 string                            `json:"local_installation_id"`
	LocalInstallationRevision           uint64                            `json:"local_installation_revision"`
	LocalHostIdentityDigest             string                            `json:"local_host_identity_digest"`
	LocalNetworkNamespaceIdentityDigest string                            `json:"local_network_namespace_identity_digest"`
	WorkerExecutableDigest              string                            `json:"worker_executable_digest"`
	OperatorTerms                       OwnedAgentAuthorityBodyRefV1      `json:"operator_terms"`
	OriginalSource                      OwnedAgentAuthorityBodyRefV1      `json:"original_source"`
	IssuedAt                            time.Time                         `json:"issued_at"`
	ExpiresAt                           time.Time                         `json:"expires_at"`
}

func (ConfiguredInferenceProfileV1) String() string {
	return "ConfiguredInferenceProfileV1{installation:[redacted]}"
}
func (v ConfiguredInferenceProfileV1) GoString() string { return v.String() }

func (v ConfiguredInferenceProfileV1) Clone() ConfiguredInferenceProfileV1 {
	v.ResponseModelIDs = append([]string(nil), v.ResponseModelIDs...)
	v.AllowedDataClasses = append([]ProviderDataClassV1(nil), v.AllowedDataClasses...)
	v.IssuedAt, v.ExpiresAt = v.IssuedAt.UTC(), v.ExpiresAt.UTC()
	return v
}

// ValidateConfiguredInferenceEndpointV1 deliberately recognizes producer text,
// rather than normalizing URLs. There is no DNS, socket or environment access.
func ValidateConfiguredInferenceEndpointV1(value string) error {
	var port string
	for _, host := range []string{"http://127.0.0.1:", "http://[::1]:"} {
		if strings.HasPrefix(value, host) && strings.HasSuffix(value, "/v1") {
			port = strings.TrimSuffix(strings.TrimPrefix(value, host), "/v1")
			break
		}
	}
	if len(port) < 1 || len(port) > 5 || port[0] == '0' {
		return configuredInferenceError("base_url", "requires a canonical literal loopback URL with explicit port and /v1")
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return configuredInferenceError("base_url", "requires a decimal port only")
		}
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return configuredInferenceError("base_url", "requires a nonzero valid port")
	}
	return nil
}

func (v ConfiguredInferenceProfileV1) Validate() error {
	if v.Version != ConfiguredInferenceProfileVersionV1 || v.ProfileRevision == 0 || v.LocalInstallationRevision == 0 {
		return configuredInferenceError("identity", "requires version one and positive revisions")
	}
	for _, validate := range []func() error{v.TenantID.Validate, v.OwnerUserID.Validate, v.Resource.Validate, v.Limits.Validate, v.CapabilityEvidence.Validate, v.OperatorTerms.Validate, v.OriginalSource.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if v.Resource.Kind != ProviderResourceLocalEndpointV1 || v.Resource.OwnerUserID != v.OwnerUserID {
		return configuredInferenceError("resource", "requires the exact owner-scoped local endpoint")
	}
	switch v.Auth {
	case ConfiguredInferenceAuthNoneV1:
		if v.Resource.CredentialMode != ProviderCredentialNoneV1 || v.Resource.CredentialGeneration != 0 {
			return configuredInferenceError("auth", "none requires no credential generation")
		}
	case ConfiguredInferenceAuthBearerV1:
		if v.Resource.CredentialMode != ProviderCredentialInvocationV1 || v.Resource.CredentialGeneration == 0 {
			return configuredInferenceError("auth", "bearer requires positive invocation generation")
		}
	default:
		return configuredInferenceError("auth", "unsupported explicit authentication mode")
	}
	if v.EndpointPolicy != ConfiguredInferenceLoopbackPolicyV1 || v.NativeProtocolVersion != ConfiguredInferenceProtocolChatCompletionsV1 {
		return configuredInferenceError("protocol", "requires explicit local nonstream Chat Completions policy")
	}
	if err := ValidateConfiguredInferenceEndpointV1(v.BaseURL); err != nil {
		return err
	}
	for _, item := range []struct {
		name, value string
		limit       int
	}{
		{"model_vendor_id", v.ModelVendorID, 128}, {"model_id", v.ModelID, 256}, {"server_id", v.ServerID, 128},
		{"server_version", v.ServerVersion, 128}, {"token_estimator_version", v.TokenEstimatorVersion, 128}, {"local_installation_id", v.LocalInstallationID, 160},
	} {
		if err := validateProviderToken("configured_inference."+item.name, item.value, item.limit); err != nil {
			return err
		}
	}
	for _, item := range []struct{ name, value string }{
		{"chat_template", v.ChatTemplateDigest}, {"provider_catalog", v.ProviderCatalogDigest}, {"provider_route", v.ProviderRouteDigest},
		{"privacy", v.PrivacyPolicyDigest}, {"effective_policy", v.EffectivePolicyDigest}, {"calibration", v.CalibrationDigest},
		{"host", v.LocalHostIdentityDigest}, {"network_namespace", v.LocalNetworkNamespaceIdentityDigest}, {"worker_executable", v.WorkerExecutableDigest},
	} {
		if err := ValidateSHA256Digest("configured_inference."+item.name, item.value); err != nil {
			return err
		}
	}
	if len(v.ResponseModelIDs) == 0 || len(v.ResponseModelIDs) > 8 {
		return configuredInferenceError("response_models", "requires one to eight exact reported identities")
	}
	for i, id := range v.ResponseModelIDs {
		if err := validateProviderToken("configured_inference.response_model_id", id, 256); err != nil {
			return err
		}
		if i > 0 && v.ResponseModelIDs[i-1] >= id {
			return configuredInferenceError("response_models", "requires unique lexical producer order")
		}
	}
	if v.Capabilities.Text != ProviderEvidenceSupportedV1 {
		return configuredInferenceError("capabilities.text", "first profile requires explicitly supported text")
	}
	for _, state := range []ProviderEvidenceStateV1{v.Capabilities.Image, v.Capabilities.File, v.Capabilities.ToolCalls, v.Capabilities.JSONSchema} {
		if state != ProviderEvidenceSupportedV1 && state != ProviderEvidenceUnsupportedV1 && state != ProviderEvidenceUnknownV1 {
			return configuredInferenceError("capabilities", "requires explicit supported, unsupported or unknown evidence")
		}
	}
	if len(v.AllowedDataClasses) == 0 || len(v.AllowedDataClasses) > 3 {
		return configuredInferenceError("data_classes", "requires explicit bounded data classes")
	}
	for i, class := range v.AllowedDataClasses {
		if class != ProviderDataPublicV1 && class != ProviderDataExternallyShareableV1 && class != ProviderDataPrivateV1 {
			return configuredInferenceError("data_classes", "unsupported data class")
		}
		if i > 0 && v.AllowedDataClasses[i-1] >= class {
			return configuredInferenceError("data_classes", "requires unique lexical producer order")
		}
	}
	if v.ModelWindowTokens == 0 || v.ModelWindowTokens > 1<<20 || v.Limits.MaxInputTokens+v.Limits.MaxOutputTokens > v.ModelWindowTokens {
		return configuredInferenceError("model_window", "requires exact finite input and output fit")
	}
	if !ownedCurrentTime(v.IssuedAt) || !ownedCurrentTime(v.ExpiresAt) || !v.ExpiresAt.After(v.IssuedAt) || v.ExpiresAt.Sub(v.IssuedAt) > maxServerlessInvocationV1 {
		return configuredInferenceError("window", "requires a finite canonical original window of at most twenty-four hours")
	}
	return nil
}

func (v ConfiguredInferenceProfileV1) ValidateAt(at time.Time) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if !ownedCurrentTime(at) || at.Before(v.IssuedAt) || !at.Before(v.ExpiresAt) {
		return configuredInferenceError("window", "authoritative time must precede exclusive expiry")
	}
	return nil
}

// ValidateForBinding cross-correlates metadata only. It does not read current
// state, authenticate source/funding, attest a host or create a capability.
func (v ConfiguredInferenceProfileV1) ValidateForBinding(binding HarnessBindingV2, substrate SubstrateBindingV1, at time.Time) error {
	if err := v.ValidateAt(at); err != nil {
		return err
	}
	if err := binding.ValidateAt(at); err != nil {
		return err
	}
	if err := substrate.ValidateAt(at); err != nil {
		return err
	}
	digest, err := v.Digest()
	if err != nil {
		return err
	}
	delivery := ProviderCredentialDeliveryNoneV1
	if v.Auth == ConfiguredInferenceAuthBearerV1 {
		delivery = ProviderCredentialDeliveryDirectV1
	}
	b := binding.Backend
	if binding.TenantID != v.TenantID || binding.OwnerUserID != v.OwnerUserID || binding.Resource != v.Resource || binding.ModelVendorID != v.ModelVendorID || binding.ModelID != v.ModelID ||
		b.BackendKind != HarnessBackendDirectOpenAICompatibleV1 || b.ProviderContractKind != ProviderContractConfiguredEndpointV1 || b.NativeProtocolVersion != v.NativeProtocolVersion || b.BackendProfileDigest != digest || b.CredentialDeliveryKind != delivery ||
		binding.ProviderCatalogDigest != v.ProviderCatalogDigest || binding.ProviderRouteDigest != v.ProviderRouteDigest || binding.PrivacyPolicyDigest != v.PrivacyPolicyDigest || binding.CapabilityEvidenceDigest != v.CapabilityEvidence.Digest || binding.EffectivePolicyDigest != v.EffectivePolicyDigest ||
		binding.EvidenceExpiresAt == nil || binding.EvidenceExpiresAt.After(v.ExpiresAt) {
		return configuredInferenceError("binding", "requires the exact sealed profile, resource, model and evidence")
	}
	allowed := false
	for _, class := range v.AllowedDataClasses {
		if class == binding.InputDataClass {
			allowed = true
		}
	}
	if !allowed || substrate.Kind != SubstrateLocalDevelopmentV1 || substrate.WorkloadMode != SubstrateWorkloadInProcessDirectV1 || substrate.ImageDigest != "" || substrate.Region != "" ||
		substrate.OuterHarnessArtifactDigest != v.WorkerExecutableDigest || substrate.LocalInstallationID != v.LocalInstallationID || substrate.LocalInstallationRevision != v.LocalInstallationRevision ||
		substrate.LocalHostIdentityDigest != v.LocalHostIdentityDigest || substrate.LocalNetworkNamespaceIdentityDigest != v.LocalNetworkNamespaceIdentityDigest || substrate.ProfileEvidenceExpiresAt.After(v.ExpiresAt) {
		return configuredInferenceError("placement", "requires the exact configured local host and executable without image or cloud claims")
	}
	return nil
}

func (v ConfiguredInferenceProfileV1) Digest() (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return ownedCurrentDigest("sessionless.configured-inference-profile.v1", v.Clone())
}

func EncodeConfiguredInferenceProfileV1(v ConfiguredInferenceProfileV1) ([]byte, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return encodeOwnedCurrent(v.Clone())
}

func DecodeConfiguredInferenceProfileV1(raw []byte) (ConfiguredInferenceProfileV1, error) {
	var v ConfiguredInferenceProfileV1
	if err := decodeOwnedCurrent(raw, &v); err != nil {
		return ConfiguredInferenceProfileV1{}, err
	}
	expected, err := EncodeConfiguredInferenceProfileV1(v)
	if err != nil || !bytes.Equal(raw, expected) {
		return ConfiguredInferenceProfileV1{}, configuredInferenceError("codec", "requires exact canonical bounded producer bytes")
	}
	return v.Clone(), nil
}

func configuredInferenceError(field, reason string) error {
	return ValidationError{Field: "configured_inference." + field, Reason: reason}
}
