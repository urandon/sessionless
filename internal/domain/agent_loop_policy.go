package domain

import "time"

const AgentLoopPolicyVersionV1 uint32 = 1

type (
	AgentLoopProfileV1      string
	AgentInputModalityV1    string
	AgentLoopPolicyDigestV1 string
)

const (
	AgentLoopOwnedPublicV1 AgentLoopProfileV1   = "owned_agent_public_v1"
	AgentInputTextV1       AgentInputModalityV1 = "text"
	AgentInputImageV1      AgentInputModalityV1 = "image"
	AgentInputFileV1       AgentInputModalityV1 = "file"
)

// AgentLoopLimitsV1 seals the accepted public profile's ceilings. Every field
// is required, including sub-budgets; zero is never an unlimited/default value.
// Model calls include compaction, and tool dispatches include web search.
type AgentLoopLimitsV1 struct {
	MaxConcurrentOperations     uint64        `json:"max_concurrent_operations"`
	MaxModelCalls               uint64        `json:"max_model_calls"`
	MaxCompactionCalls          uint64        `json:"max_compaction_calls"`
	MaxToolDispatches           uint64        `json:"max_tool_dispatches"`
	MaxWebSearches              uint64        `json:"max_web_searches"`
	MaxInputTokensPerCall       uint64        `json:"max_input_tokens_per_call"`
	MaxAggregateInputTokens     uint64        `json:"max_aggregate_input_tokens"`
	MaxOutputTokensPerCall      uint64        `json:"max_output_tokens_per_call"`
	MaxAggregateOutputTokens    uint64        `json:"max_aggregate_output_tokens"`
	MaxActiveDuration           time.Duration `json:"max_active_duration"`
	MaxNetworkOperationDuration time.Duration `json:"max_network_operation_duration"`
	MaxCleanupDuration          time.Duration `json:"max_cleanup_duration"`
	MaxContextViewBytes         uint64        `json:"max_context_view_bytes"`
	MaxSummaryBytes             uint64        `json:"max_summary_bytes"`
	MaxToolArgumentBytes        uint64        `json:"max_tool_argument_bytes"`
	MaxToolResultBytes          uint64        `json:"max_tool_result_bytes"`
	MaxMCPOutboundMessages      uint64        `json:"max_mcp_outbound_messages"`
	MaxMCPCatalogPages          uint64        `json:"max_mcp_catalog_pages"`
	MaxMCPInboundFrames         uint64        `json:"max_mcp_inbound_frames"`
	MaxMCPInboundBytes          uint64        `json:"max_mcp_inbound_bytes"`
	MaxOperations               uint64        `json:"max_operations"`
	MaxAggregateBodyBytes       uint64        `json:"max_aggregate_body_bytes"`
}

func (value AgentLoopLimitsV1) Validate() error {
	quantities := []struct {
		field string
		value uint64
		max   uint64
	}{
		{"max_concurrent_operations", value.MaxConcurrentOperations, 1},
		{"max_model_calls", value.MaxModelCalls, 8},
		{"max_compaction_calls", value.MaxCompactionCalls, 2},
		{"max_tool_dispatches", value.MaxToolDispatches, 4},
		{"max_web_searches", value.MaxWebSearches, 2},
		{"max_input_tokens_per_call", value.MaxInputTokensPerCall, 32768},
		{"max_aggregate_input_tokens", value.MaxAggregateInputTokens, 131072},
		{"max_output_tokens_per_call", value.MaxOutputTokensPerCall, 2048},
		{"max_aggregate_output_tokens", value.MaxAggregateOutputTokens, 16384},
		{"max_context_view_bytes", value.MaxContextViewBytes, 1 << 20},
		{"max_summary_bytes", value.MaxSummaryBytes, 16 << 10},
		{"max_tool_argument_bytes", value.MaxToolArgumentBytes, 4 << 10},
		{"max_tool_result_bytes", value.MaxToolResultBytes, 64 << 10},
		{"max_mcp_outbound_messages", value.MaxMCPOutboundMessages, 12},
		{"max_mcp_catalog_pages", value.MaxMCPCatalogPages, 1},
		{"max_mcp_inbound_frames", value.MaxMCPInboundFrames, 128},
		{"max_mcp_inbound_bytes", value.MaxMCPInboundBytes, 256 << 10},
		{"max_operations", value.MaxOperations, 12},
		{"max_aggregate_body_bytes", value.MaxAggregateBodyBytes, 1 << 20},
	}
	for _, quantity := range quantities {
		if quantity.value == 0 || quantity.value > quantity.max {
			return ValidationError{Field: "agent_loop_policy.limits." + quantity.field, Reason: "must be positive and within the accepted public-profile ceiling"}
		}
	}
	for _, duration := range []struct {
		field string
		value time.Duration
		max   time.Duration
	}{
		{"max_active_duration", value.MaxActiveDuration, 120 * time.Second},
		{"max_network_operation_duration", value.MaxNetworkOperationDuration, 30 * time.Second},
		{"max_cleanup_duration", value.MaxCleanupDuration, 30 * time.Second},
	} {
		if duration.value <= 0 || duration.value > duration.max {
			return ValidationError{Field: "agent_loop_policy.limits." + duration.field, Reason: "must be positive and within the accepted public-profile ceiling"}
		}
	}
	// Validate finite operands before any arithmetic. The accepted maxima make
	// these products and sums bounded, including for malicious uint64 inputs.
	if value.MaxCompactionCalls > value.MaxModelCalls || value.MaxWebSearches > value.MaxToolDispatches || value.MaxOperations > value.MaxModelCalls+value.MaxToolDispatches {
		return ValidationError{Field: "agent_loop_policy.limits", Reason: "sub-budgets must fit their containing operation ceilings"}
	}
	if value.MaxAggregateInputTokens < value.MaxInputTokensPerCall || value.MaxAggregateInputTokens > value.MaxInputTokensPerCall*value.MaxModelCalls || value.MaxAggregateOutputTokens < value.MaxOutputTokensPerCall || value.MaxAggregateOutputTokens > value.MaxOutputTokensPerCall*value.MaxModelCalls {
		return ValidationError{Field: "agent_loop_policy.limits", Reason: "aggregate token ceilings must cover a call and fit the model-call ceiling"}
	}
	if value.MaxSummaryBytes > value.MaxContextViewBytes || value.MaxToolArgumentBytes > value.MaxAggregateBodyBytes || value.MaxToolResultBytes > value.MaxAggregateBodyBytes {
		return ValidationError{Field: "agent_loop_policy.limits", Reason: "per-value byte ceilings must fit their containing byte ceilings"}
	}
	if value.MaxNetworkOperationDuration > value.MaxActiveDuration {
		return ValidationError{Field: "agent_loop_policy.limits.max_network_operation_duration", Reason: "must fit the active duration"}
	}
	return nil
}

// AgentLoopPolicyV1 is immutable server-owned authority, not a request DTO or
// executable capability. Catalog/route/evidence digests identify the completed
// independent provider revisions; no new native model-revision authority is
// inferred. The exact model/profile must separately prove modality and token fit.
// Compaction and grants are completed before this policy: no enclosing policy,
// binding, placement or invocation digest participates in its construction.
type AgentLoopPolicyV1 struct {
	Version                  uint32                     `json:"version"`
	TenantID                 TenantID                   `json:"tenant_id"`
	OwnerUserID              UserID                     `json:"owner_user_id"`
	RunID                    RunID                      `json:"run_id"`
	AttemptID                AttemptID                  `json:"attempt_id"`
	Profile                  AgentLoopProfileV1         `json:"profile"`
	ProfileRevision          uint64                     `json:"profile_revision"`
	Backend                  HarnessBackendDescriptorV1 `json:"backend"`
	Resource                 ProviderResourceBindingV1  `json:"resource"`
	ModelVendorID            string                     `json:"model_vendor_id"`
	ModelID                  string                     `json:"model_id"`
	ProviderCatalogDigest    string                     `json:"provider_catalog_digest"`
	ProviderRouteDigest      string                     `json:"provider_route_digest"`
	CapabilityEvidenceDigest string                     `json:"capability_evidence_digest"`
	PrivacyPolicyDigest      string                     `json:"privacy_policy_digest"`
	EffectivePolicyDigest    string                     `json:"effective_policy_digest"`
	InputModalities          []AgentInputModalityV1     `json:"input_modalities"`
	InputDataClass           ProviderDataClassV1        `json:"input_data_class"`
	InstructionVersion       string                     `json:"instruction_version"`
	CompactionPolicyDigest   string                     `json:"compaction_policy_digest"`
	MCPToolGrantDigest       string                     `json:"mcp_tool_grant_digest"`
	WebSearchGrantDigest     string                     `json:"web_search_grant_digest"`
	Limits                   AgentLoopLimitsV1          `json:"limits"`
	AdmissionCostCeiling     AdmissionCostCeilingV2     `json:"admission_cost_ceiling"`
	IssuedAt                 time.Time                  `json:"issued_at"`
	ExpiresAt                time.Time                  `json:"expires_at"`
}

// BuildAgentLoopPolicyV1 takes an isolated, UTC-normalized snapshot. Callers
// must retain that value rather than mutating a policy after its digest is sealed.
func BuildAgentLoopPolicyV1(value AgentLoopPolicyV1) (AgentLoopPolicyV1, error) {
	clone := value.Clone()
	if err := clone.Validate(); err != nil {
		return AgentLoopPolicyV1{}, err
	}
	return clone, nil
}

func (value AgentLoopPolicyV1) Clone() AgentLoopPolicyV1 {
	clone := value
	clone.InputModalities = append([]AgentInputModalityV1(nil), value.InputModalities...)
	clone.AdmissionCostCeiling = value.AdmissionCostCeiling.Clone()
	clone.AdmissionCostCeiling.PriceObservedAt = value.AdmissionCostCeiling.PriceObservedAt.UTC()
	clone.AdmissionCostCeiling.PriceExpiresAt = value.AdmissionCostCeiling.PriceExpiresAt.UTC()
	clone.IssuedAt, clone.ExpiresAt = value.IssuedAt.UTC(), value.ExpiresAt.UTC()
	return clone
}

// Validate checks immutable structure only. It does not establish current
// resource/revocation, lease, proxy, provider evidence or service-price freshness,
// nor does it allocate MCP/search charges or authorize any physical effect.
func (value AgentLoopPolicyV1) Validate() error {
	if value.Version != AgentLoopPolicyVersionV1 || value.Profile != AgentLoopOwnedPublicV1 || value.ProfileRevision == 0 {
		return ValidationError{Field: "agent_loop_policy.profile", Reason: "requires explicit version 1 owned public profile and positive revision"}
	}
	for _, validate := range []func() error{value.TenantID.Validate, value.OwnerUserID.Validate, value.RunID.Validate, value.AttemptID.Validate, value.Backend.Validate, value.Resource.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if value.Resource.OwnerUserID != value.OwnerUserID {
		return ValidationError{Field: "agent_loop_policy.resource.owner_user_id", Reason: "must match the policy owner"}
	}
	if err := value.Resource.ValidateForBackend(value.Backend); err != nil {
		return err
	}
	for _, token := range []struct {
		field, value string
		max          int
	}{
		{"model_vendor_id", value.ModelVendorID, 128}, {"model_id", value.ModelID, 256}, {"instruction_version", value.InstructionVersion, 128},
	} {
		if err := validateProviderToken("agent_loop_policy."+token.field, token.value, token.max); err != nil {
			return err
		}
	}
	for _, digest := range []struct{ field, value string }{
		{"provider_catalog_digest", value.ProviderCatalogDigest}, {"provider_route_digest", value.ProviderRouteDigest},
		{"capability_evidence_digest", value.CapabilityEvidenceDigest}, {"privacy_policy_digest", value.PrivacyPolicyDigest},
		{"effective_policy_digest", value.EffectivePolicyDigest}, {"compaction_policy_digest", value.CompactionPolicyDigest},
		{"mcp_tool_grant_digest", value.MCPToolGrantDigest}, {"web_search_grant_digest", value.WebSearchGrantDigest},
	} {
		if err := validateSHA256("agent_loop_policy."+digest.field, digest.value); err != nil {
			return err
		}
	}
	if value.InputDataClass != ProviderDataPublicV1 {
		return ValidationError{Field: "agent_loop_policy.input_data_class", Reason: "the first owned profile requires public data"}
	}
	if len(value.InputModalities) == 0 || len(value.InputModalities) > 3 {
		return ValidationError{Field: "agent_loop_policy.input_modalities", Reason: "must be a nonempty bounded canonical set"}
	}
	previous := AgentInputModalityV1("")
	for _, modality := range value.InputModalities {
		if (modality != AgentInputTextV1 && modality != AgentInputImageV1 && modality != AgentInputFileV1) || modality <= previous {
			return ValidationError{Field: "agent_loop_policy.input_modalities", Reason: "must contain supported unique modalities in lexical order"}
		}
		previous = modality
	}
	if err := value.Limits.Validate(); err != nil {
		return err
	}
	if err := value.AdmissionCostCeiling.Validate(); err != nil {
		return err
	}
	// Both services are required by this explicit owned public profile.
	// Single-turn profiles exclude them and carry no tool authority.
	for _, service := range []struct {
		name, grant string
		cost        *ExternalServiceCostCeilingV1
	}{
		{"mcp_tool_cost", value.MCPToolGrantDigest, value.AdmissionCostCeiling.MCPToolCost},
		{"web_search_cost", value.WebSearchGrantDigest, value.AdmissionCostCeiling.WebSearchCost},
	} {
		if service.cost == nil || service.cost.TenantID != value.TenantID ||
			service.cost.OwnerUserID != value.OwnerUserID || service.cost.ScopedGrantDigest != service.grant {
			return ValidationError{Field: "agent_loop_policy.admission_cost_ceiling." + service.name,
				Reason: "requires the exact tenant, owner and independent scoped service grant"}
		}
	}
	if value.IssuedAt.IsZero() || value.IssuedAt.UTC().Year() < 1 || value.ExpiresAt.UTC().Year() > 9999 || !value.ExpiresAt.After(value.IssuedAt) || value.IssuedAt.Before(value.AdmissionCostCeiling.PriceObservedAt) || value.ExpiresAt.After(value.AdmissionCostCeiling.PriceExpiresAt) {
		return ValidationError{Field: "agent_loop_policy.expires_at", Reason: "must bound a nonzero canonical UTC policy window inside the admitted price window"}
	}
	if value.Limits.MaxActiveDuration > value.AdmissionCostCeiling.MaxActiveDuration || value.Limits.MaxCleanupDuration > value.AdmissionCostCeiling.MaxCleanupAndReconcileDuration {
		return ValidationError{Field: "agent_loop_policy.admission_cost_ceiling", Reason: "must cover the sealed active and cleanup durations"}
	}
	return nil
}

// ValidateAt applies only policy and existing monetary freshness fences. The
// operation-admission gate must additionally reload all current authority and
// check the independent grants, evidence, exact model/profile and cost coverage.
func (value AgentLoopPolicyV1) ValidateAt(now time.Time) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if now.IsZero() || now.Before(value.IssuedAt) || !now.Before(value.ExpiresAt) {
		return ValidationError{Field: "agent_loop_policy.expires_at", Reason: "must contain authoritative execution time, excluding expiry"}
	}
	return value.AdmissionCostCeiling.ValidateAt(now)
}

func (value AgentLoopPolicyV1) Digest() (AgentLoopPolicyDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	digest := newServerlessDigest("sessionless.agent-loop-policy.v1")
	digest.uint(uint64(value.Version))
	for _, field := range []string{string(value.TenantID), string(value.OwnerUserID), string(value.RunID), string(value.AttemptID), string(value.Profile)} {
		digest.str(field)
	}
	digest.uint(value.ProfileRevision)
	for _, field := range []string{string(value.Backend.HarnessKind), value.Backend.HarnessVersion, string(value.Backend.BackendKind), string(value.Backend.ArtifactKind), value.Backend.ArtifactDigest, value.Backend.NativeProtocolVersion, value.Backend.BackendProfileDigest, string(value.Backend.ProviderContractKind), string(value.Backend.CredentialDeliveryKind), string(value.Resource.Kind), value.Resource.ResourceID, string(value.Resource.OwnerUserID)} {
		digest.str(field)
	}
	digest.uint(value.Resource.Revision)
	digest.str(string(value.Resource.CredentialMode))
	digest.uint(value.Resource.CredentialGeneration)
	for _, field := range []string{value.ModelVendorID, value.ModelID, value.ProviderCatalogDigest, value.ProviderRouteDigest, value.CapabilityEvidenceDigest, value.PrivacyPolicyDigest, value.EffectivePolicyDigest} {
		digest.str(field)
	}
	digest.uint(uint64(len(value.InputModalities)))
	for _, modality := range value.InputModalities {
		digest.str(string(modality))
	}
	for _, field := range []string{string(value.InputDataClass), value.InstructionVersion, value.CompactionPolicyDigest, value.MCPToolGrantDigest, value.WebSearchGrantDigest} {
		digest.str(field)
	}
	limits := value.Limits
	for _, quantity := range []uint64{limits.MaxConcurrentOperations, limits.MaxModelCalls, limits.MaxCompactionCalls, limits.MaxToolDispatches, limits.MaxWebSearches, limits.MaxInputTokensPerCall, limits.MaxAggregateInputTokens, limits.MaxOutputTokensPerCall, limits.MaxAggregateOutputTokens} {
		digest.uint(quantity)
	}
	digest.duration(limits.MaxActiveDuration)
	digest.duration(limits.MaxNetworkOperationDuration)
	digest.duration(limits.MaxCleanupDuration)
	for _, quantity := range []uint64{limits.MaxContextViewBytes, limits.MaxSummaryBytes, limits.MaxToolArgumentBytes, limits.MaxToolResultBytes, limits.MaxMCPOutboundMessages, limits.MaxMCPCatalogPages, limits.MaxMCPInboundFrames, limits.MaxMCPInboundBytes, limits.MaxOperations, limits.MaxAggregateBodyBytes} {
		digest.uint(quantity)
	}
	costDigest, _ := value.AdmissionCostCeiling.Digest()
	digest.str(string(costDigest))
	digest.instant(value.IssuedAt)
	digest.instant(value.ExpiresAt)
	return AgentLoopPolicyDigestV1(digest.sum()), nil
}

func (digest AgentLoopPolicyDigestV1) Validate() error {
	return validateSHA256("agent_loop_policy_digest", string(digest))
}
