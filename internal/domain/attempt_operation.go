package domain

import (
	"encoding/json"
	"math"
	"time"
)

const AttemptOperationVersionV1 uint32 = 1

type (
	AttemptOperationKindV1              string
	AttemptOperationDigestV1            string
	AttemptOperationReservationDigestV1 string
	AttemptOperationObservationDigestV1 string
	AttemptOperationOutcomeV1           string
	AttemptOperationReasonV1            string
	AttemptOperationUsageStateV1        string
)

const (
	AttemptOperationModelV1                  AttemptOperationKindV1       = "model"
	AttemptOperationCompactionV1             AttemptOperationKindV1       = "compaction"
	AttemptOperationMCPToolV1                AttemptOperationKindV1       = "mcp_tool"
	AttemptOperationWebSearchV1              AttemptOperationKindV1       = "web_search"
	AttemptOperationCompletedV1              AttemptOperationOutcomeV1    = "completed"
	AttemptOperationRejectedBeforeSendV1     AttemptOperationOutcomeV1    = "rejected_before_send"
	AttemptOperationAcceptedOutcomeUnknownV1 AttemptOperationOutcomeV1    = "accepted_outcome_unknown"
	AttemptOperationReasonNoneV1             AttemptOperationReasonV1     = "none"
	AttemptOperationReasonAuthorityDeniedV1  AttemptOperationReasonV1     = "authority_denied"
	AttemptOperationReasonCancelledV1        AttemptOperationReasonV1     = "cancelled"
	AttemptOperationReasonDeadlineV1         AttemptOperationReasonV1     = "deadline"
	AttemptOperationReasonServiceErrorV1     AttemptOperationReasonV1     = "service_error"
	AttemptOperationReasonMalformedV1        AttemptOperationReasonV1     = "malformed"
	AttemptOperationReasonOverflowV1         AttemptOperationReasonV1     = "overflow"
	AttemptOperationReasonTransportUnknownV1 AttemptOperationReasonV1     = "transport_unknown"
	AttemptOperationReasonWorkerLostV1       AttemptOperationReasonV1     = "worker_lost"
	AttemptOperationUsageUnknownV1           AttemptOperationUsageStateV1 = "unknown"
	AttemptOperationUsageObservedV1          AttemptOperationUsageStateV1 = "observed"
)

// AttemptOperationScopeV1 identifies a child of the original physical claim.
// These are structural correlations, never proof of current authority. Session
// participation and the Run's Session must be reread by the resource transaction.
type AttemptOperationScopeV1 struct {
	TenantID                  TenantID                              `json:"tenant_id"`
	SessionID                 SessionID                             `json:"session_id"`
	OwnerUserID               UserID                                `json:"owner_user_id"`
	RunID                     RunID                                 `json:"run_id"`
	AttemptID                 AttemptID                             `json:"attempt_id"`
	LeaseID                   LeaseID                               `json:"lease_id"`
	FenceToken                uint64                                `json:"fence_token"`
	PhysicalInvocationClaimID string                                `json:"physical_invocation_claim_id"`
	EffectReservationDigest   AttemptEffectReservationDigestV1      `json:"effect_reservation_digest"`
	InvocationAuthorityDigest ServerlessInvocationAuthorityDigestV1 `json:"invocation_authority_digest"`
	HarnessBindingDigest      HarnessBindingDigestV2                `json:"harness_binding_digest"`
	AgentLoopPolicyDigest     AgentLoopPolicyDigestV1               `json:"agent_loop_policy_digest"`
}

func (v AttemptOperationScopeV1) Validate() error {
	for _, validate := range []func() error{v.TenantID.Validate, v.SessionID.Validate, v.OwnerUserID.Validate, v.RunID.Validate, v.AttemptID.Validate, v.LeaseID.Validate, v.EffectReservationDigest.Validate, v.InvocationAuthorityDigest.Validate, v.HarnessBindingDigest.Validate, v.AgentLoopPolicyDigest.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if v.FenceToken == 0 {
		return operationError("scope.fence_token", "must be positive")
	}
	return ValidateOpaqueID("attempt_operation.scope.physical_invocation_claim_id", v.PhysicalInvocationClaimID)
}

func (v AttemptOperationScopeV1) ValidateForParent(a ServerlessInvocationAuthorityV1, parent AttemptEffectReservationV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if err := parent.ValidateForAuthority(a); err != nil {
		return err
	}
	if a.HarnessBinding.Profile != HarnessExecutionOwnedAgentV2 || a.HarnessBinding.AgentLoopPolicy == nil {
		return operationError("scope.profile", "requires the explicit owned-agent profile")
	}
	parentDigest, _ := parent.DigestForAuthority(a)
	policyDigest, _ := a.HarnessBinding.AgentLoopPolicy.Digest()
	if v.TenantID != parent.TenantID || v.RunID != parent.RunID || v.AttemptID != parent.AttemptID || v.LeaseID != parent.LeaseID || v.FenceToken != parent.FenceToken || v.PhysicalInvocationClaimID != parent.PhysicalInvocationClaimID || v.OwnerUserID != a.HarnessBinding.OwnerUserID || v.EffectReservationDigest != parentDigest || v.InvocationAuthorityDigest != parent.InvocationAuthorityDigest || v.HarnessBindingDigest != parent.HarnessBindingDigest || v.AgentLoopPolicyDigest != policyDigest {
		return operationError("scope", "must exact-match the original effect claim and completed policy/binding")
	}
	return nil
}

// AttemptOperationDebitV1 is a non-refundable worst-case reservation, not a
// second quota ledger. The Attempt effect store owns its cumulative value.
// Consent bodies/rows also consume BodyBytes in that store before operations.
// Time is conservatively debited; absolute deadlines additionally bound elapsed
// time including consent waits. Observed lower usage never permits another call.
type AttemptOperationDebitV1 struct {
	Operations          uint64        `json:"operations"`
	ModelCalls          uint64        `json:"model_calls"`
	CompactionCalls     uint64        `json:"compaction_calls"`
	ToolDispatches      uint64        `json:"tool_dispatches"`
	WebSearches         uint64        `json:"web_searches"`
	InputTokens         uint64        `json:"input_tokens"`
	OutputTokens        uint64        `json:"output_tokens"`
	BodyBytes           uint64        `json:"body_bytes"`
	MCPOutboundMessages uint64        `json:"mcp_outbound_messages"`
	MCPCatalogPages     uint64        `json:"mcp_catalog_pages"`
	MCPInboundFrames    uint64        `json:"mcp_inbound_frames"`
	MCPInboundBytes     uint64        `json:"mcp_inbound_bytes"`
	ResponseBytes       uint64        `json:"response_bytes"`
	AmountMicrounits    uint64        `json:"amount_microunits"`
	ActiveDuration      time.Duration `json:"active_duration"`
}

// Add checks overflow before addition. Neither operand nor result is mutated.
func (v AttemptOperationDebitV1) Add(other AttemptOperationDebitV1) (AttemptOperationDebitV1, error) {
	out := v
	for _, pair := range []struct {
		dst *uint64
		add uint64
	}{
		{&out.Operations, other.Operations}, {&out.ModelCalls, other.ModelCalls}, {&out.CompactionCalls, other.CompactionCalls}, {&out.ToolDispatches, other.ToolDispatches}, {&out.WebSearches, other.WebSearches}, {&out.InputTokens, other.InputTokens}, {&out.OutputTokens, other.OutputTokens}, {&out.BodyBytes, other.BodyBytes}, {&out.MCPOutboundMessages, other.MCPOutboundMessages}, {&out.MCPCatalogPages, other.MCPCatalogPages}, {&out.MCPInboundFrames, other.MCPInboundFrames}, {&out.MCPInboundBytes, other.MCPInboundBytes}, {&out.ResponseBytes, other.ResponseBytes}, {&out.AmountMicrounits, other.AmountMicrounits},
	} {
		if *pair.dst > math.MaxUint64-pair.add {
			return AttemptOperationDebitV1{}, operationError("debit", "unsigned reservation overflow")
		}
		*pair.dst += pair.add
	}
	if v.ActiveDuration < 0 || other.ActiveDuration < 0 || v.ActiveDuration > time.Duration(math.MaxInt64)-other.ActiveDuration {
		return AttemptOperationDebitV1{}, operationError("debit.active_duration", "invalid or overflowing reservation")
	}
	out.ActiveDuration += other.ActiveDuration
	return out, nil
}

func (v AttemptOperationDebitV1) ValidateForPolicy(p AgentLoopPolicyV1) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if p.AdmissionCostCeiling.MaxTotalAmountMicrounits == nil || p.AdmissionCostCeiling.MaxSubstrateAmountMicrounits == nil || p.AdmissionCostCeiling.ProviderPriceState == ProviderPriceUnknownV1 || p.AdmissionCostCeiling.SubstratePriceState != CostEvidenceKnownV1 {
		return operationError("debit.price", "unknown cost cannot authorize a reservation")
	}
	c := p.Limits
	total, substrate := *p.AdmissionCostCeiling.MaxTotalAmountMicrounits, *p.AdmissionCostCeiling.MaxSubstrateAmountMicrounits
	if substrate > total {
		return operationError("debit.price", "substrate reservation exceeds total admission")
	}
	for _, q := range []struct {
		name         string
		value, limit uint64
	}{
		{"operations", v.Operations, c.MaxOperations}, {"model_calls", v.ModelCalls, c.MaxModelCalls}, {"compaction_calls", v.CompactionCalls, c.MaxCompactionCalls}, {"tool_dispatches", v.ToolDispatches, c.MaxToolDispatches}, {"web_searches", v.WebSearches, c.MaxWebSearches}, {"input_tokens", v.InputTokens, c.MaxAggregateInputTokens}, {"output_tokens", v.OutputTokens, c.MaxAggregateOutputTokens}, {"body_bytes", v.BodyBytes, c.MaxAggregateBodyBytes}, {"mcp_outbound_messages", v.MCPOutboundMessages, c.MaxMCPOutboundMessages}, {"mcp_catalog_pages", v.MCPCatalogPages, c.MaxMCPCatalogPages}, {"mcp_inbound_frames", v.MCPInboundFrames, c.MaxMCPInboundFrames}, {"mcp_inbound_bytes", v.MCPInboundBytes, c.MaxMCPInboundBytes}, {"response_bytes", v.ResponseBytes, p.AdmissionCostCeiling.MaxIngressBytes}, {"amount_microunits", v.AmountMicrounits, total - substrate},
	} {
		if q.value > q.limit {
			return operationError("debit."+q.name, "exceeds the admitted aggregate ceiling")
		}
	}
	if v.ActiveDuration < 0 || v.ActiveDuration > c.MaxActiveDuration {
		return operationError("debit.active_duration", "exceeds the admitted active-time ceiling")
	}
	if v.CompactionCalls > v.ModelCalls || v.WebSearches > v.ToolDispatches || v.ModelCalls > v.Operations || v.ToolDispatches != v.Operations-v.ModelCalls {
		return operationError("debit.calls", "sub-budgets must fit the exact operation count")
	}
	return nil
}

// AttemptOperationV1 seals a request; request contents live in an authorized
// immutable blob. Tool/grant/receipt digests are already completed dependencies.
// A digest here does not attest a grant's approval, price or billing owner.
type AttemptOperationV1 struct {
	Version                      uint32                              `json:"version"`
	Scope                        AttemptOperationScopeV1             `json:"scope"`
	Sequence                     uint64                              `json:"sequence"`
	Kind                         AttemptOperationKindV1              `json:"kind"`
	CallID                       string                              `json:"call_id"`
	RequestRef                   string                              `json:"request_ref"`
	RequestDigest                string                              `json:"request_digest"`
	PredecessorObservationDigest AttemptOperationObservationDigestV1 `json:"predecessor_observation_digest"`
	ModelResponseDigest          string                              `json:"model_response_digest"`
	ArgumentsDigest              string                              `json:"arguments_digest"`
	QueryDigest                  string                              `json:"query_digest"`
	ToolGrantDigest              string                              `json:"tool_grant_digest"`
	ConsentRequestDigest         string                              `json:"consent_request_digest"`
	ConsentDecisionDigest        string                              `json:"consent_decision_digest"`
	ConsentDecisionRef           string                              `json:"consent_decision_ref"`
	Deadline                     time.Time                           `json:"deadline"`
	CleanupDuration              time.Duration                       `json:"cleanup_duration"`
	Debit                        AttemptOperationDebitV1             `json:"debit"`
	RequestBytes                 uint64                              `json:"request_bytes"`
	ResultBytes                  uint64                              `json:"result_bytes"`
	ContextBytes                 uint64                              `json:"context_bytes"`
	SummaryBytes                 uint64                              `json:"summary_bytes"`
	Currency                     string                              `json:"currency"`
	PriceRevision                string                              `json:"price_revision"`
	AdmissionCostCeilingDigest   AdmissionCostCeilingDigestV2        `json:"admission_cost_ceiling_digest"`
	ExternalChargeEvidenceDigest string                              `json:"external_charge_evidence_digest"`
}

func BuildAttemptOperationV1(v AttemptOperationV1) (AttemptOperationV1, error) {
	v = v.Clone()
	return v, v.Validate()
}
func (v AttemptOperationV1) Clone() AttemptOperationV1 { v.Deadline = v.Deadline.UTC(); return v }

func (v AttemptOperationV1) Validate() error {
	if v.Version != AttemptOperationVersionV1 || v.Sequence == 0 || v.Sequence > 12 {
		return operationError("identity", "requires version 1 and bounded positive sequence")
	}
	if err := v.Scope.Validate(); err != nil {
		return err
	}
	for name, id := range map[string]string{"call_id": v.CallID, "request_ref": v.RequestRef} {
		if err := ValidateOpaqueID("attempt_operation."+name, id); err != nil {
			return err
		}
	}
	if err := ValidateSHA256Digest("attempt_operation.request_digest", v.RequestDigest); err != nil {
		return err
	}
	if err := v.AdmissionCostCeilingDigest.Validate(); err != nil {
		return err
	}
	if v.Sequence == 1 {
		if v.PredecessorObservationDigest != "" || (v.Kind != AttemptOperationModelV1 && v.Kind != AttemptOperationCompactionV1) {
			return operationError("predecessor", "the first operation must be a model or pre-model compaction with no predecessor")
		}
	} else if err := v.PredecessorObservationDigest.Validate(); err != nil {
		return err
	}
	if !validCurrencyV1(v.Currency) {
		return operationError("currency", "requires canonical admission currency")
	}
	if err := ValidateOpaqueID("attempt_operation.price_revision", v.PriceRevision); err != nil {
		return err
	}
	if v.Deadline.IsZero() || v.Deadline.Year() < 1 || v.Deadline.Year() > 9999 || v.Debit.ActiveDuration <= 0 || v.Debit.ActiveDuration > 30*time.Second || v.Debit.Operations != 1 || v.RequestBytes == 0 || v.ResultBytes == 0 || v.RequestBytes > 1<<20 || v.ResultBytes > 1<<20 || v.Debit.ResponseBytes == 0 || v.Debit.ResponseBytes > 1<<20 {
		return operationError("limits", "requires finite request/result/network/time reservations")
	}
	if v.CleanupDuration <= 0 || v.CleanupDuration > 30*time.Second {
		return operationError("cleanup_duration", "requires a bounded separately admitted observation window")
	}
	if v.Debit.BodyBytes < v.RequestBytes || v.ResultBytes > v.Debit.BodyBytes-v.RequestBytes || v.Debit.BodyBytes > 1<<20 {
		return operationError("body_bytes", "must cover request and normalized result without overflow")
	}
	tool := v.Kind == AttemptOperationMCPToolV1 || v.Kind == AttemptOperationWebSearchV1
	if tool {
		if v.Debit.ModelCalls != 0 || v.Debit.CompactionCalls != 0 || v.Debit.ToolDispatches != 1 || v.Debit.InputTokens != 0 || v.Debit.OutputTokens != 0 || v.RequestBytes > 4<<10 || v.ResultBytes > 64<<10 || v.ContextBytes != 0 || v.SummaryBytes != 0 || v.Sequence == 1 {
			return operationError("tool", "requires one bounded tool dispatch with independent price evidence")
		}
		for name, digest := range map[string]string{"model_response_digest": v.ModelResponseDigest, "arguments_digest": v.ArgumentsDigest, "tool_grant_digest": v.ToolGrantDigest, "external_charge_evidence_digest": v.ExternalChargeEvidenceDigest} {
			if err := ValidateSHA256Digest("attempt_operation."+name, digest); err != nil {
				return err
			}
		}
	} else {
		if v.Kind != AttemptOperationModelV1 && v.Kind != AttemptOperationCompactionV1 {
			return operationError("kind", "is unsupported")
		}
		if v.Debit.ModelCalls != 1 || v.Debit.ToolDispatches != 0 || v.Debit.WebSearches != 0 || v.Debit.InputTokens == 0 || v.Debit.InputTokens > 32768 || v.Debit.OutputTokens == 0 || v.Debit.OutputTokens > 2048 || v.ContextBytes == 0 || v.ContextBytes > 1<<20 || v.ModelResponseDigest != "" || v.ArgumentsDigest != "" || v.ToolGrantDigest != "" || v.ExternalChargeEvidenceDigest != "" {
			return operationError("model", "requires one bounded model call without tool authority")
		}
		if (v.Kind == AttemptOperationCompactionV1 && (v.Debit.CompactionCalls != 1 || v.SummaryBytes == 0 || v.SummaryBytes > 16<<10)) || (v.Kind == AttemptOperationModelV1 && (v.Debit.CompactionCalls != 0 || v.SummaryBytes != 0)) {
			return operationError("compaction", "must debit a model call and bounded summary only for compaction")
		}
	}
	if v.Kind == AttemptOperationWebSearchV1 {
		if v.Debit.WebSearches != 1 || v.Debit.ActiveDuration > 15*time.Second || v.Debit.ResponseBytes > 256<<10 || v.ResultBytes > 24<<10 {
			return operationError("web_search", "requires the bounded search sub-budget")
		}
		for name, digest := range map[string]string{"query_digest": v.QueryDigest, "consent_request_digest": v.ConsentRequestDigest, "consent_decision_digest": v.ConsentDecisionDigest} {
			if err := ValidateSHA256Digest("attempt_operation."+name, digest); err != nil {
				return err
			}
		}
		if err := ValidateOpaqueID("attempt_operation.consent_decision_ref", v.ConsentDecisionRef); err != nil {
			return err
		}
	} else if v.Debit.WebSearches != 0 || v.QueryDigest != "" || v.ConsentRequestDigest != "" || v.ConsentDecisionDigest != "" || v.ConsentDecisionRef != "" {
		return operationError("consent", "search receipt fields are exclusive to web_search")
	}
	if v.Kind == AttemptOperationMCPToolV1 {
		if v.Debit.MCPOutboundMessages == 0 || v.Debit.MCPOutboundMessages > 12 || v.Debit.MCPCatalogPages > 1 || v.Debit.MCPInboundFrames == 0 || v.Debit.MCPInboundFrames > 128 || v.Debit.MCPInboundBytes == 0 || v.Debit.MCPInboundBytes > 256<<10 || v.Debit.ResponseBytes < v.Debit.MCPInboundBytes {
			return operationError("mcp", "requires bounded protocol reservations")
		}
	} else if v.Debit.MCPOutboundMessages != 0 || v.Debit.MCPCatalogPages != 0 || v.Debit.MCPInboundFrames != 0 || v.Debit.MCPInboundBytes != 0 {
		return operationError("mcp", "protocol debit is exclusive to mcp_tool")
	}
	return nil
}

func (v AttemptOperationV1) ValidateForParent(a ServerlessInvocationAuthorityV1, parent AttemptEffectReservationV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if err := v.Scope.ValidateForParent(a, parent); err != nil {
		return err
	}
	p := *a.HarnessBinding.AgentLoopPolicy
	if v.CleanupDuration != p.Limits.MaxCleanupDuration {
		return operationError("cleanup_duration", "must exact-match the admitted policy's independent cleanup bound")
	}
	costDigest, _ := a.AdmissionCostCeiling.Digest()
	if v.AdmissionCostCeilingDigest != costDigest || v.Currency != a.AdmissionCostCeiling.Currency || v.PriceRevision != a.AdmissionCostCeiling.PriceRevision {
		return operationError("price", "must exact-match completed cost authority")
	}
	if v.Deadline.After(a.InvocationDeadline) || v.Deadline.After(a.Lease.ExpiresAt) || v.Deadline.After(p.ExpiresAt) || v.Deadline.After(a.AdmissionCostCeiling.PriceExpiresAt) || !v.Deadline.After(parent.ReservedAt) || v.Deadline.After(parent.ReservedAt.Add(p.Limits.MaxActiveDuration)) {
		return operationError("deadline", "must fit every parent active authority window")
	}
	if v.Debit.ActiveDuration > p.Limits.MaxNetworkOperationDuration || v.RequestBytes > p.Limits.MaxAggregateBodyBytes || v.ContextBytes > p.Limits.MaxContextViewBytes || v.SummaryBytes > p.Limits.MaxSummaryBytes || v.Debit.InputTokens > p.Limits.MaxInputTokensPerCall || v.Debit.OutputTokens > p.Limits.MaxOutputTokensPerCall {
		return operationError("limits", "must fit the exact policy's per-operation bounds")
	}
	if v.Kind == AttemptOperationMCPToolV1 || v.Kind == AttemptOperationWebSearchV1 {
		expected := p.MCPToolGrantDigest
		component := a.AdmissionCostCeiling.MCPToolCost
		if v.Kind == AttemptOperationWebSearchV1 {
			expected = p.WebSearchGrantDigest
			component = a.AdmissionCostCeiling.WebSearchCost
		}
		if component == nil || component.MaxAmountMicrounits == nil || component.PriceState == ExternalServicePriceUnknownV1 || v.Debit.AmountMicrounits > *component.MaxAmountMicrounits || (v.Debit.AmountMicrounits == 0 && component.PriceState != ExternalServicePriceKnownFreeV1) {
			return operationError("tool.price", "zero requires an explicitly evidenced known-free completed component; unknown never means zero")
		}
		if v.ToolGrantDigest != expected || v.RequestBytes > p.Limits.MaxToolArgumentBytes || v.ResultBytes > p.Limits.MaxToolResultBytes {
			return operationError("tool", "must bind the exact completed grant and tool ceilings")
		}
	}
	return v.Debit.ValidateForPolicy(p)
}

func (v AttemptOperationV1) Digest() (AttemptOperationDigestV1, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return AttemptOperationDigestV1(operationDigest("sessionless.attempt-operation.v1", v.Clone())), nil
}
func (v AttemptOperationDigestV1) Validate() error {
	return ValidateSHA256Digest("attempt_operation_digest", string(v))
}
func (v AttemptOperationReservationDigestV1) Validate() error {
	return ValidateSHA256Digest("attempt_operation_reservation_digest", string(v))
}
func (v AttemptOperationObservationDigestV1) Validate() error {
	return ValidateSHA256Digest("attempt_operation_observation_digest", string(v))
}

type AttemptOperationReservationV1 struct {
	Version                    uint32                             `json:"version"`
	Operation                  AttemptOperationV1                 `json:"operation"`
	OperationDigest            AttemptOperationDigestV1           `json:"operation_digest"`
	ReservedAt                 time.Time                          `json:"reserved_at"`
	AggregateDebit             AttemptOperationDebitV1            `json:"aggregate_debit"`
	RegisteredAllocationDigest RegisteredAllocationDigestV1       `json:"registered_allocation_digest"`
	RegisteredProxyDigest      RegisteredProxyAttestationDigestV2 `json:"registered_proxy_digest"`
	ComponentAmounts           [4]uint64                          `json:"component_amounts"`
}

func (v AttemptOperationReservationV1) Clone() AttemptOperationReservationV1 {
	v.Operation = v.Operation.Clone()
	v.ReservedAt = v.ReservedAt.UTC()
	return v
}
func (v AttemptOperationReservationV1) Validate() error {
	if v.Version != 1 {
		return operationError("reservation.version", "must equal 1")
	}
	if v.RegisteredAllocationDigest.Validate() != nil || v.RegisteredProxyDigest.Validate() != nil {
		return operationError("reservation.registration", "requires the exact registered allocation and proxy observations")
	}
	var amount uint64
	for _, component := range v.ComponentAmounts {
		if component > math.MaxUint64-amount {
			return operationError("reservation.component_amounts", "must not overflow")
		}
		amount += component
	}
	if amount < v.AggregateDebit.AmountMicrounits {
		return operationError("reservation.component_amounts", "cannot omit cumulative monetary reservations")
	}
	digest, err := v.Operation.Digest()
	if err != nil {
		return err
	}
	if v.OperationDigest != digest {
		return operationError("reservation.operation_digest", "must exact-match immutable request")
	}
	if v.ReservedAt.IsZero() || v.ReservedAt.Year() < 1 || v.ReservedAt.Year() > 9999 || !v.ReservedAt.Before(v.Operation.Deadline) || v.Operation.Deadline.After(v.ReservedAt.Add(v.Operation.Debit.ActiveDuration)) {
		return operationError("reservation.window", "must fit the reserved network window")
	}
	if v.AggregateDebit.Operations != v.Operation.Sequence {
		return operationError("reservation.sequence", "must exact-match cumulative operation count")
	}
	return nil
}
func (v AttemptOperationReservationV1) ValidateForParent(a ServerlessInvocationAuthorityV1, parent AttemptEffectReservationV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if err := v.Operation.ValidateForParent(a, parent); err != nil {
		return err
	}
	if v.ReservedAt.Before(parent.ReservedAt) {
		return operationError("reservation.reserved_at", "cannot precede the original claim")
	}
	if err := v.AggregateDebit.ValidateForPolicy(*a.HarnessBinding.AgentLoopPolicy); err != nil {
		return err
	}
	// Require cumulative quantities to cover this debit, without subtraction overflow.
	d := v.Operation.Debit
	for _, q := range []struct{ total, minimum uint64 }{{v.AggregateDebit.ModelCalls, d.ModelCalls}, {v.AggregateDebit.CompactionCalls, d.CompactionCalls}, {v.AggregateDebit.ToolDispatches, d.ToolDispatches}, {v.AggregateDebit.WebSearches, d.WebSearches}, {v.AggregateDebit.InputTokens, d.InputTokens}, {v.AggregateDebit.OutputTokens, d.OutputTokens}, {v.AggregateDebit.BodyBytes, d.BodyBytes}, {v.AggregateDebit.MCPOutboundMessages, d.MCPOutboundMessages}, {v.AggregateDebit.MCPCatalogPages, d.MCPCatalogPages}, {v.AggregateDebit.MCPInboundFrames, d.MCPInboundFrames}, {v.AggregateDebit.MCPInboundBytes, d.MCPInboundBytes}, {v.AggregateDebit.ResponseBytes, d.ResponseBytes}, {v.AggregateDebit.AmountMicrounits, d.AmountMicrounits}} {
		if q.total < q.minimum {
			return operationError("reservation.aggregate_debit", "cannot omit this operation's reservation")
		}
	}
	if v.AggregateDebit.ActiveDuration < d.ActiveDuration {
		return operationError("reservation.active_duration", "cannot omit this operation's reservation")
	}
	return nil
}
func (v AttemptOperationReservationV1) Digest() (AttemptOperationReservationDigestV1, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return AttemptOperationReservationDigestV1(operationDigest("sessionless.attempt-operation-reservation.v1", v.Clone())), nil
}

// Unknown usage has no numeric values; it is never inferred as zero. Usage
// outside the reservation is retained as authenticated evidence and stops work;
// it must not be rejected simply to hide a billing/usage overrun.
type AttemptOperationUsageV1 struct {
	State AttemptOperationUsageStateV1 `json:"state"`
	// Each present metric is measured; nil is explicitly unknown, never zero.
	// Observed permits partial reports so known overruns cannot be discarded
	// merely because another metric (for example billed amount) is absent.
	InputTokens      *uint64 `json:"input_tokens,omitempty"`
	OutputTokens     *uint64 `json:"output_tokens,omitempty"`
	AmountMicrounits *uint64 `json:"amount_microunits,omitempty"`
}

func (v AttemptOperationUsageV1) Clone() AttemptOperationUsageV1 {
	v.InputTokens = cloneUint64(v.InputTokens)
	v.OutputTokens = cloneUint64(v.OutputTokens)
	v.AmountMicrounits = cloneUint64(v.AmountMicrounits)
	return v
}
func (v AttemptOperationUsageV1) Validate() error {
	if v.State == AttemptOperationUsageUnknownV1 && v.InputTokens == nil && v.OutputTokens == nil && v.AmountMicrounits == nil {
		return nil
	}
	if v.State == AttemptOperationUsageObservedV1 && (v.InputTokens != nil || v.OutputTokens != nil || v.AmountMicrounits != nil) {
		return nil
	}
	return operationError("observation.usage", "requires at least one measured metric or wholly absent unknown metrics")
}

type AttemptOperationObservationV1 struct {
	Version           uint32                              `json:"version"`
	OperationDigest   AttemptOperationDigestV1            `json:"operation_digest"`
	ReservationDigest AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	Outcome           AttemptOperationOutcomeV1           `json:"outcome"`
	Reason            AttemptOperationReasonV1            `json:"reason"`
	ResponseRef       string                              `json:"response_ref"`
	ResponseDigest    string                              `json:"response_digest"`
	ResponseBytes     uint64                              `json:"response_bytes"`
	ObservedAt        time.Time                           `json:"observed_at"`
	Usage             AttemptOperationUsageV1             `json:"usage"`
}

func (v AttemptOperationObservationV1) Clone() AttemptOperationObservationV1 {
	v.ObservedAt = v.ObservedAt.UTC()
	v.Usage = v.Usage.Clone()
	return v
}
func (v AttemptOperationObservationV1) Validate() error {
	if v.Version != 1 {
		return operationError("observation.version", "must equal 1")
	}
	for _, validate := range []func() error{v.OperationDigest.Validate, v.ReservationDigest.Validate, v.Usage.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if v.ObservedAt.IsZero() || v.ObservedAt.Year() < 1 || v.ObservedAt.Year() > 9999 {
		return operationError("observation.observed_at", "requires a canonical observed time")
	}
	switch v.Reason {
	case AttemptOperationReasonNoneV1, AttemptOperationReasonAuthorityDeniedV1, AttemptOperationReasonCancelledV1, AttemptOperationReasonDeadlineV1, AttemptOperationReasonServiceErrorV1, AttemptOperationReasonMalformedV1, AttemptOperationReasonOverflowV1, AttemptOperationReasonTransportUnknownV1, AttemptOperationReasonWorkerLostV1:
	default:
		return operationError("observation.reason", "requires a closed sanitized reason")
	}
	switch v.Outcome {
	case AttemptOperationCompletedV1:
		if err := ValidateOpaqueID("attempt_operation.observation.response_ref", v.ResponseRef); err != nil {
			return err
		}
		if err := ValidateSHA256Digest("attempt_operation.observation.response_digest", v.ResponseDigest); err != nil {
			return err
		}
		if v.ResponseBytes == 0 || v.ResponseBytes > 1<<20 || (v.Reason != AttemptOperationReasonNoneV1 && v.Reason != AttemptOperationReasonServiceErrorV1) {
			return operationError("observation.response", "requires a bounded known durable response")
		}
	case AttemptOperationRejectedBeforeSendV1, AttemptOperationAcceptedOutcomeUnknownV1:
		if v.Reason == AttemptOperationReasonNoneV1 || v.ResponseRef != "" || v.ResponseDigest != "" || v.ResponseBytes != 0 {
			return operationError("observation.response", "unknown/rejected operations cannot fabricate a service response")
		}
	default:
		return operationError("observation.outcome", "is unsupported")
	}
	return nil
}
func (v AttemptOperationObservationV1) ValidateForReservation(r AttemptOperationReservationV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	digest, err := r.Digest()
	if err != nil {
		return err
	}
	if v.OperationDigest != r.OperationDigest || v.ReservationDigest != digest || v.ObservedAt.Before(r.ReservedAt) || v.ResponseBytes > r.Operation.ResultBytes {
		return operationError("observation.correlation", "must exact-match the bounded immutable reservation")
	}
	return nil
}
func (v AttemptOperationObservationV1) Digest() (AttemptOperationObservationDigestV1, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return AttemptOperationObservationDigestV1(operationDigest("sessionless.attempt-operation-observation.v1", v.Clone())), nil
}

// ValidatePredecessor is structural only: the store must read this exact durable,
// authenticated observation, ensure it is the sole head, and deny all unresolved
// operations. It also rejects observed overruns; unknown usage is not zero cost.
func (v AttemptOperationV1) ValidatePredecessor(r AttemptOperationReservationV1, o AttemptOperationObservationV1) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if err := o.ValidateForReservation(r); err != nil {
		return err
	}
	digest, _ := o.Digest()
	if v.Scope != r.Operation.Scope || v.Sequence != r.Operation.Sequence+1 || v.PredecessorObservationDigest != digest || o.Outcome != AttemptOperationCompletedV1 {
		return operationError("predecessor", "requires the exact known durable prior operation in the same claim")
	}
	if (o.Usage.InputTokens != nil && *o.Usage.InputTokens > r.Operation.Debit.InputTokens) ||
		(o.Usage.OutputTokens != nil && *o.Usage.OutputTokens > r.Operation.Debit.OutputTokens) ||
		(o.Usage.AmountMicrounits != nil && *o.Usage.AmountMicrounits > r.Operation.Debit.AmountMicrounits) {
		return operationError("predecessor.usage", "observed overrun stops further effects")
	}
	if v.Kind == AttemptOperationMCPToolV1 || v.Kind == AttemptOperationWebSearchV1 {
		if r.Operation.Kind != AttemptOperationModelV1 || v.ModelResponseDigest != o.ResponseDigest {
			return operationError("predecessor.model_response", "tool requires the exact durably completed model response")
		}
	}
	return nil
}

func operationError(field, reason string) error {
	return ValidationError{Field: "attempt_operation." + field, Reason: reason}
}
func operationDigest(separator string, value any) string {
	encoded, _ := json.Marshal(value)
	digest := newServerlessDigest(separator)
	digest.bytes(encoded)
	return digest.sum()
}
