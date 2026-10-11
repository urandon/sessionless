package domain

import "time"

const AdmissionCostCeilingVersionV2 uint32 = 2

type AdmissionCostCeilingDigestV2 string

// AdmissionCostCeilingV2 is the canonical monetary successor, not
// another ledger or live permission. Independent service grant and price/terms
// evidence precede each component, which precedes this ceiling and loop policy.
// An absent service component grants no service authority, not free access.
// Current authority/grant/profile/revision/billing checks remain caller duties;
// matching two component scopes cannot authenticate either scope by itself.
type AdmissionCostCeilingV2 struct {
	Version                         uint32                        `json:"version"`
	Currency                        string                        `json:"currency"`
	PriceRevision                   string                        `json:"price_revision"`
	PriceObservedAt                 time.Time                     `json:"price_observed_at"`
	PriceExpiresAt                  time.Time                     `json:"price_expires_at"`
	MaxDeliveries                   uint32                        `json:"max_deliveries"`
	MaxPreEffectDurationPerDelivery time.Duration                 `json:"max_pre_effect_duration_per_delivery"`
	MaxActiveDuration               time.Duration                 `json:"max_active_duration"`
	MaxCleanupAndReconcileDuration  time.Duration                 `json:"max_cleanup_and_reconcile_duration"`
	ConfiguredMemoryBytes           uint64                        `json:"configured_memory_bytes"`
	ConfiguredVCPUMillis            uint64                        `json:"configured_vcpu_millis"`
	MaxIngressBytes                 uint64                        `json:"max_ingress_bytes"`
	MaxEgressBytes                  uint64                        `json:"max_egress_bytes"`
	MaxLogBytes                     uint64                        `json:"max_log_bytes"`
	MaxEvidenceBytes                uint64                        `json:"max_evidence_bytes"`
	SubstratePriceState             CostEvidenceStateV1           `json:"substrate_price_state"`
	ProviderPriceState              ProviderPriceStateV1          `json:"provider_price_state"`
	MaxSubstrateAmountMicrounits    *uint64                       `json:"max_substrate_amount_microunits,omitempty"`
	MaxProviderAmountMicrounits     *uint64                       `json:"max_provider_amount_microunits,omitempty"`
	MaxTotalAmountMicrounits        *uint64                       `json:"max_total_amount_microunits,omitempty"`
	MCPToolCost                     *ExternalServiceCostCeilingV1 `json:"mcp_tool_cost,omitempty"`
	WebSearchCost                   *ExternalServiceCostCeilingV1 `json:"web_search_cost,omitempty"`
}

func (value AdmissionCostCeilingV2) Clone() AdmissionCostCeilingV2 {
	clone := value
	clone.MaxSubstrateAmountMicrounits = cloneUint64(value.MaxSubstrateAmountMicrounits)
	clone.MaxProviderAmountMicrounits = cloneUint64(value.MaxProviderAmountMicrounits)
	clone.MaxTotalAmountMicrounits = cloneUint64(value.MaxTotalAmountMicrounits)
	clone.PriceObservedAt, clone.PriceExpiresAt = value.PriceObservedAt.UTC(), value.PriceExpiresAt.UTC()
	if value.MCPToolCost != nil {
		component := value.MCPToolCost.Clone()
		clone.MCPToolCost = &component
	}
	if value.WebSearchCost != nil {
		component := value.WebSearchCost.Clone()
		clone.WebSearchCost = &component
	}
	return clone
}

func (value AdmissionCostCeilingV2) Validate() error {
	const prefix = "admission_cost_ceiling."
	if value.Version != AdmissionCostCeilingVersionV2 {
		return ValidationError{Field: prefix + "version", Reason: "must equal 2"}
	}
	if !validCurrencyV1(value.Currency) {
		return ValidationError{Field: prefix + "currency", Reason: "must be three uppercase ASCII letters"}
	}
	if err := validateProviderToken(prefix+"price_revision", value.PriceRevision, 128); err != nil {
		return err
	}
	if value.PriceObservedAt.IsZero() || value.PriceObservedAt.UTC().Year() < 1 || value.PriceExpiresAt.UTC().Year() > 9999 || !value.PriceExpiresAt.After(value.PriceObservedAt) {
		return ValidationError{Field: prefix + "price_expires_at", Reason: "must follow a nonzero observation in the canonical UTC timestamp range"}
	}
	if value.MaxDeliveries == 0 || value.MaxDeliveries > 100 {
		return ValidationError{Field: prefix + "max_deliveries", Reason: "must be positive and bounded"}
	}
	for _, bound := range []struct {
		field string
		value time.Duration
	}{
		{"max_pre_effect_duration_per_delivery", value.MaxPreEffectDurationPerDelivery},
		{"max_active_duration", value.MaxActiveDuration},
		{"max_cleanup_and_reconcile_duration", value.MaxCleanupAndReconcileDuration},
	} {
		if bound.value <= 0 || bound.value > 24*time.Hour {
			return ValidationError{Field: prefix + bound.field, Reason: "must be positive and bounded"}
		}
	}
	for _, bound := range []struct {
		field string
		value uint64
	}{
		{"configured_memory_bytes", value.ConfiguredMemoryBytes}, {"configured_vcpu_millis", value.ConfiguredVCPUMillis},
		{"max_ingress_bytes", value.MaxIngressBytes}, {"max_egress_bytes", value.MaxEgressBytes},
		{"max_log_bytes", value.MaxLogBytes}, {"max_evidence_bytes", value.MaxEvidenceBytes},
	} {
		if bound.value == 0 {
			return ValidationError{Field: prefix + bound.field, Reason: "must be positive"}
		}
	}
	for _, component := range []struct {
		field string
		kind  ExternalServiceKindV1
		value *ExternalServiceCostCeilingV1
	}{
		{"mcp_tool_cost", ExternalServiceMCPToolV1, value.MCPToolCost},
		{"web_search_cost", ExternalServiceWebSearchV1, value.WebSearchCost},
	} {
		if component.value == nil {
			continue
		}
		if err := component.value.Validate(); err != nil {
			return err
		}
		if component.value.Kind != component.kind || component.value.Currency != value.Currency {
			return ValidationError{Field: prefix + component.field, Reason: "must exact-match the fixed service kind and enclosing currency"}
		}
		if value.PriceObservedAt.Before(component.value.PriceObservedAt) || value.PriceExpiresAt.After(component.value.PriceExpiresAt) {
			return ValidationError{Field: prefix + component.field, Reason: "independent service price interval must contain the enclosing executable price interval"}
		}
	}
	if value.MCPToolCost != nil && value.WebSearchCost != nil {
		mcp, search := value.MCPToolCost, value.WebSearchCost
		if mcp.TenantID != search.TenantID || mcp.OwnerUserID != search.OwnerUserID || mcp.BillingOwnerUserID != search.BillingOwnerUserID {
			return ValidationError{Field: prefix + "external_service_cost", Reason: "present components must exact-match tenant, owner and billing-owner scope"}
		}
	}
	return value.validateAmounts()
}

func (value AdmissionCostCeilingV2) validateAmounts() error {
	unknown := false
	switch value.SubstratePriceState {
	case CostEvidenceUnknownV1:
		unknown = true
		if value.MaxSubstrateAmountMicrounits != nil {
			return ValidationError{Field: "admission_cost_ceiling.substrate_price_state", Reason: "unknown price must not carry a numeric ceiling"}
		}
	case CostEvidenceKnownV1:
		if value.MaxSubstrateAmountMicrounits == nil {
			return ValidationError{Field: "admission_cost_ceiling.max_substrate_amount_microunits", Reason: "is required for known substrate price"}
		}
	default:
		return ValidationError{Field: "admission_cost_ceiling.substrate_price_state", Reason: "is unsupported"}
	}
	switch value.ProviderPriceState {
	case ProviderPriceUnknownV1:
		unknown = true
		if value.MaxProviderAmountMicrounits != nil {
			return ValidationError{Field: "admission_cost_ceiling.provider_price_state", Reason: "unknown price must not carry a numeric ceiling"}
		}
	case ProviderPriceKnownFreeV1:
		if value.MaxProviderAmountMicrounits == nil || *value.MaxProviderAmountMicrounits != 0 {
			return ValidationError{Field: "admission_cost_ceiling.max_provider_amount_microunits", Reason: "known free requires an explicit zero"}
		}
	case ProviderPriceKnownV1:
		if value.MaxProviderAmountMicrounits == nil {
			return ValidationError{Field: "admission_cost_ceiling.max_provider_amount_microunits", Reason: "is required for known provider price"}
		}
	default:
		return ValidationError{Field: "admission_cost_ceiling.provider_price_state", Reason: "is unsupported"}
	}
	amounts := []*uint64{value.MaxSubstrateAmountMicrounits, value.MaxProviderAmountMicrounits}
	for _, component := range []*ExternalServiceCostCeilingV1{value.MCPToolCost, value.WebSearchCost} {
		if component != nil {
			unknown = unknown || component.PriceState == ExternalServicePriceUnknownV1
			amounts = append(amounts, component.MaxAmountMicrounits)
		}
	}
	var total uint64
	for _, amount := range amounts {
		if amount == nil {
			continue
		}
		if *amount > ^uint64(0)-total {
			return ValidationError{Field: "admission_cost_ceiling.max_total_amount_microunits", Reason: "known component subtotal must not overflow, including when another price is unknown"}
		}
		total += *amount
	}
	if unknown {
		if value.MaxTotalAmountMicrounits != nil {
			return ValidationError{Field: "admission_cost_ceiling.max_total_amount_microunits", Reason: "unknown component price makes the numeric total unavailable"}
		}
		return nil
	}
	if value.MaxTotalAmountMicrounits == nil || *value.MaxTotalAmountMicrounits != total {
		return ValidationError{Field: "admission_cost_ceiling.max_total_amount_microunits", Reason: "must equal the checked substrate, provider and all present service ceilings"}
	}
	return nil
}

func (value AdmissionCostCeilingV2) ValidateAt(now time.Time) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if now.IsZero() || now.Before(value.PriceObservedAt) || !now.Before(value.PriceExpiresAt) {
		return ValidationError{Field: "admission_cost_ceiling.price_expires_at", Reason: "must contain authoritative execution time, excluding expiry"}
	}
	if value.SubstratePriceState != CostEvidenceKnownV1 || value.ProviderPriceState == ProviderPriceUnknownV1 || value.MaxTotalAmountMicrounits == nil {
		return ValidationError{Field: "admission_cost_ceiling", Reason: "unknown price cannot authorize execution"}
	}
	for _, component := range []*ExternalServiceCostCeilingV1{value.MCPToolCost, value.WebSearchCost} {
		if component != nil {
			if err := component.ValidateAt(now); err != nil {
				return err
			}
		}
	}
	return nil
}

// ValidateForSubstrate retains the delivered exact finite substrate coverage.
// The substrate seals the canonical V2 digest; no legacy decoder is permitted.
func (value AdmissionCostCeilingV2) ValidateForSubstrate(substrate SubstrateBindingV1) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if err := substrate.Validate(); err != nil {
		return err
	}
	digest, _ := value.Digest()
	if substrate.AdmissionCostCeilingDigest != digest {
		return ValidationError{Field: "admission_cost_ceiling", Reason: "must be the exact ceiling sealed by the substrate"}
	}
	limits := substrate.Limits
	if value.ConfiguredMemoryBytes != limits.MemoryBytes || value.ConfiguredVCPUMillis != limits.CPUMillis {
		return ValidationError{Field: "admission_cost_ceiling.configuration", Reason: "memory and CPU must exact-match the substrate profile"}
	}
	if value.MaxActiveDuration != limits.ExecutionTimeout || value.MaxCleanupAndReconcileDuration < limits.CleanupTimeout {
		return ValidationError{Field: "admission_cost_ceiling.duration", Reason: "must cover exact execution and cleanup limits"}
	}
	if value.MaxPreEffectDurationPerDelivery > limits.InvocationTimeout-value.MaxActiveDuration-value.MaxCleanupAndReconcileDuration {
		return ValidationError{Field: "admission_cost_ceiling.duration", Reason: "pre-effect, active and cleanup ceilings must fit inside invocation timeout"}
	}
	if value.MaxIngressBytes < limits.ArtifactBytes || value.MaxEgressBytes < limits.ArtifactBytes {
		return ValidationError{Field: "admission_cost_ceiling.artifact_bytes", Reason: "ingress and egress ceilings must cover the substrate artifact bound"}
	}
	if limits.StdoutBytes > ^uint64(0)-limits.StderrBytes || value.MaxLogBytes < limits.StdoutBytes+limits.StderrBytes {
		return ValidationError{Field: "admission_cost_ceiling.max_log_bytes", Reason: "must cover bounded stdout and stderr"}
	}
	minimumEvidence := limits.StdoutBytes
	if limits.StderrBytes > minimumEvidence {
		minimumEvidence = limits.StderrBytes
	}
	if value.MaxEvidenceBytes < minimumEvidence {
		return ValidationError{Field: "admission_cost_ceiling.max_evidence_bytes", Reason: "must cover one bounded substrate evidence stream"}
	}
	return nil
}

func (value AdmissionCostCeilingV2) Digest() (AdmissionCostCeilingDigestV2, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	digest := newServerlessDigest("sessionless.admission-cost-ceiling.v2")
	digest.uint(uint64(value.Version))
	digest.str(value.Currency)
	digest.str(value.PriceRevision)
	digest.instant(value.PriceObservedAt)
	digest.instant(value.PriceExpiresAt)
	digest.uint(uint64(value.MaxDeliveries))
	digest.duration(value.MaxPreEffectDurationPerDelivery)
	digest.duration(value.MaxActiveDuration)
	digest.duration(value.MaxCleanupAndReconcileDuration)
	for _, quantity := range []uint64{value.ConfiguredMemoryBytes, value.ConfiguredVCPUMillis, value.MaxIngressBytes, value.MaxEgressBytes, value.MaxLogBytes, value.MaxEvidenceBytes} {
		digest.uint(quantity)
	}
	digest.str(string(value.SubstratePriceState))
	digest.str(string(value.ProviderPriceState))
	digest.optionalUint(value.MaxSubstrateAmountMicrounits)
	digest.optionalUint(value.MaxProviderAmountMicrounits)
	digest.optionalUint(value.MaxTotalAmountMicrounits)
	for _, component := range []*ExternalServiceCostCeilingV1{value.MCPToolCost, value.WebSearchCost} {
		if component == nil {
			digest.uint(0)
			continue
		}
		digest.uint(1)
		componentDigest, _ := component.Digest()
		digest.str(string(componentDigest))
	}
	return AdmissionCostCeilingDigestV2(digest.sum()), nil
}

func (digest AdmissionCostCeilingDigestV2) Validate() error {
	return validateSHA256("admission_cost_ceiling_digest", string(digest))
}
