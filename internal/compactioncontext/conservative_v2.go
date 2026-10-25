package compactioncontext

import (
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// MCPConservativeDataV2 is exact historical correlation DATA, NOT financial
// eligibility, an authenticated observation, billing or permission to dispatch.
// The original producer must independently authenticate the SAME Source/Store,
// original spending/terms bodies and full nonrefundable component reservation.
// It must not construct this record from a caller's claimed amount or boolean.
type MCPConservativeDataV2 struct {
	Version           uint32                                     `json:"version"`
	Scope             domain.AttemptOperationScopeV1             `json:"scope"`
	Sequence          uint64                                     `json:"sequence"`
	ReservationDigest domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	ObservationDigest domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
	Reserved          domain.AttemptOperationDebitV1             `json:"reserved"`
	MCPSpendingDigest string                                     `json:"mcp_spending_digest"`
}

// ContextDataV2 explicitly opts into the new DATA grammar. Legacy history
// keeps its measured-money-only grammar, including when mixed with V2 history.
type ContextDataV2 struct {
	Version         uint32
	ConservativeMCP []MCPConservativeDataV2
	Historical      []HistoricalRunV2
	LegacyHistory   []HistoricalRunV1
}

type conservativeOperationKeyV2 struct {
	scope    domain.AttemptOperationScopeV1
	sequence uint64
}

type contextRouteV2 struct {
	conservative map[conservativeOperationKeyV2]MCPConservativeDataV2
	used         map[conservativeOperationKeyV2]bool
	history      []HistoricalRunV2
	projected    map[domain.SessionEventID]projectedOperationV2
}

func newContextRouteV2(data ContextDataV2) (*contextRouteV2, error) {
	if data.Version != 2 || len(data.ConservativeMCP) > 12 || len(data.Historical)+len(data.LegacyHistory) > int(MaxEventsV1) {
		return nil, ErrCanonical
	}
	r := &contextRouteV2{conservative: make(map[conservativeOperationKeyV2]MCPConservativeDataV2), used: make(map[conservativeOperationKeyV2]bool), history: data.Historical, projected: make(map[domain.SessionEventID]projectedOperationV2)}
	for _, row := range data.ConservativeMCP {
		if !row.validShape() {
			return nil, ErrCanonical
		}
		key := conservativeOperationKeyV2{row.Scope, row.Sequence}
		if _, exists := r.conservative[key]; exists {
			return nil, ErrCanonical
		}
		r.conservative[key] = row
	}
	return r, nil
}

func (v MCPConservativeDataV2) validShape() bool {
	return v.Version == 2 && v.Scope.Validate() == nil && v.Sequence > 1 && v.Sequence < scopeSequenceLimit && v.ReservationDigest.Validate() == nil && v.ObservationDigest.Validate() == nil && domain.ValidateSHA256Digest("mcp.spending", v.MCPSpendingDigest) == nil
}

func (v MCPConservativeDataV2) matches(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1) bool {
	rd, re := r.Digest()
	od, oe := o.Digest()
	return v.validShape() && re == nil && oe == nil && o.ValidateForReservation(r) == nil && r.Operation.Kind == domain.AttemptOperationMCPToolV1 &&
		o.Outcome == domain.AttemptOperationCompletedV1 && o.Reason == domain.AttemptOperationReasonNoneV1 && o.Usage.State == domain.AttemptOperationUsageUnknownV1 && !o.Usage.ExceedsReservation(r.Operation.Debit) &&
		v.Scope == r.Operation.Scope && v.Sequence == r.Operation.Sequence && v.ReservationDigest == rd && v.ObservationDigest == od && v.Reserved == r.Operation.Debit
}

func (r *contextRouteV2) current(rsv domain.AttemptOperationReservationV1, obs domain.AttemptOperationObservationV1, policy domain.AgentLoopPolicyV1) *MCPConservativeDataV2 {
	if r == nil {
		return nil
	}
	key := conservativeOperationKeyV2{rsv.Operation.Scope, rsv.Operation.Sequence}
	data, found := r.conservative[key]
	if !found || r.used[key] || !conservativePolicyMatchesV2(data, rsv, obs, policy) {
		return nil
	}
	r.used[key] = true
	return &data
}

func conservativePolicyMatchesV2(data MCPConservativeDataV2, rsv domain.AttemptOperationReservationV1, obs domain.AttemptOperationObservationV1, policy domain.AgentLoopPolicyV1) bool {
	cost := policy.AdmissionCostCeiling.MCPToolCost
	return data.matches(rsv, obs) && cost != nil && cost.MaxAmountMicrounits != nil &&
		rsv.Operation.Debit.AmountMicrounits == *cost.MaxAmountMicrounits && rsv.Operation.ToolGrantDigest == policy.MCPToolGrantDigest &&
		rsv.Operation.ToolGrantDigest == cost.ScopedGrantDigest && rsv.Operation.ExternalChargeEvidenceDigest == cost.PriceEvidenceDigest && rsv.Operation.Currency == cost.Currency &&
		cost.ValidateAt(rsv.ReservedAt) == nil && !rsv.Operation.Deadline.After(cost.PriceExpiresAt)
}

// BuildOrdinaryV2 has the same bounded context/codec behavior as V1, with an
// explicit additional correlated DATA variant. It does not create a send gate.
func BuildOrdinaryV2(profile domain.ConfiguredInferenceProfileV1, limits domain.ConfiguredInferenceLimitsV1, policy domain.AgentLoopPolicyV1,
	sourceDigest string, records []sessioncontext.EventPayload, operations []CompletedOperationV1, parent *ParentV1, tools []configuredinference.ToolV1, data ContextDataV2) (*configuredinference.FrozenRequestV1, error) {
	route, err := newContextRouteV2(data)
	if err != nil {
		return nil, err
	}
	source, err := buildGenerationSource(profile, limits, policy, sourceDigest, records, operations, parent, tools, data.LegacyHistory, route)
	if err != nil {
		return nil, err
	}
	return buildOrdinarySourceV1(source, parent)
}

// BuildGenerationV2 preserves the exact compaction renderer, atomic units,
// summary rules and limits. Any retained artifact must use BuildRetainedV2.
func BuildGenerationV2(profile domain.ConfiguredInferenceProfileV1, limits domain.ConfiguredInferenceLimitsV1, policy domain.AgentLoopPolicyV1,
	sourceDigest string, records []sessioncontext.EventPayload, operations []CompletedOperationV1, parent *ParentV1, tools []configuredinference.ToolV1, data ContextDataV2) (*GenerationV1, error) {
	route, err := newContextRouteV2(data)
	if err != nil {
		return nil, err
	}
	source, err := buildGenerationSource(profile, limits, policy, sourceDigest, records, operations, parent, tools, data.LegacyHistory, route)
	if err != nil {
		return nil, err
	}
	return buildGenerationSourceProgramV1(source, parent)
}
