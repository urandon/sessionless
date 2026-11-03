package ports

import (
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// V4 is a distinct retained failure DATA language. Reservation money remains
// nonrefundable; unknown service USD and reported provider credits are not a bill.
type OwnedOperationFailureRowV4 struct {
	Sequence           uint64                                     `json:"sequence"`
	Kind               domain.AttemptOperationKindV1              `json:"kind"`
	OperationDigest    domain.AttemptOperationDigestV1            `json:"operation_digest"`
	ReservationDigest  domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	ObservationDigest  domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
	Outcome            domain.AttemptOperationOutcomeV1           `json:"outcome"`
	Reason             domain.AttemptOperationReasonV1            `json:"reason"`
	Reserved           domain.AttemptOperationDebitV1             `json:"reserved"`
	Usage              domain.AttemptOperationUsageV1             `json:"usage"`
	ToolSpendingDigest string                                     `json:"tool_spending_digest,omitempty"`
}

type OwnedOperationFailureAccountingV4 struct {
	Version              uint32                         `json:"version"`
	Scope                domain.AttemptOperationScopeV1 `json:"scope"`
	AccountingDigest     string                         `json:"accounting_digest"`
	AggregateDebit       domain.AttemptOperationDebitV1 `json:"aggregate_debit"`
	ComponentAmounts     [4]uint64                      `json:"component_amounts"`
	ConsentStatesDigest  string                         `json:"consent_states_digest"`
	ConsentReservedBytes uint64                         `json:"consent_reserved_bytes"`
	Operations           []OwnedOperationFailureRowV4   `json:"operations"`
}

func FailureSearchRejectedReasonV4(r domain.AttemptOperationReasonV1) bool {
	return domain.SearchFailureRejectedReasonV4(r)
}
func FailureSearchUnknownReasonV4(r domain.AttemptOperationReasonV1) bool {
	return domain.SearchFailureUnknownReasonV4(r)
}

// This checks correlations only; the original Store still authenticates actual
// observation issuance, source and physical custody before exposing history.
func BuildOwnedOperationFailureRowV4(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) (OwnedOperationFailureRowV4, error) {
	deny := func() (OwnedOperationFailureRowV4, error) {
		return OwnedOperationFailureRowV4{}, domain.ErrMembershipDenied
	}
	if !accountingSourceV3(parent, source) || r.ValidateForParent(parent.Authority, parent.Reservation) != nil || r.Operation.Scope != ownedResidualScope(parent) || o.ValidateForReservation(r) != nil {
		return deny()
	}
	op := r.Operation
	spending := ""
	switch op.Kind {
	case domain.AttemptOperationWebSearchV1:
		if !searchFailureReservationV4(op, r.ReservedAt, parent, source) || o.Usage.State != domain.AttemptOperationUsageUnknownV1 || !(o.Outcome == domain.AttemptOperationCompletedV1 && (o.Reason == domain.AttemptOperationReasonNoneV1 || o.Reason == domain.AttemptOperationReasonServiceErrorV1) || o.Outcome == domain.AttemptOperationRejectedBeforeSendV1 && FailureSearchRejectedReasonV4(o.Reason) || o.Outcome == domain.AttemptOperationAcceptedOutcomeUnknownV1 && FailureSearchUnknownReasonV4(o.Reason)) {
			return deny()
		}
		spending, _ = source.Spending[3].Digest()
	case domain.AttemptOperationMCPToolV1:
		if !mcpReservationV3(op, r.ReservedAt, parent, source) || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || o.Usage.State != domain.AttemptOperationUsageUnknownV1 {
			return deny()
		}
		spending, _ = source.Spending[2].Digest()
	case domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1:
		if op.Debit.AmountMicrounits != 0 {
			return deny()
		}
		complete := o.Outcome == domain.AttemptOperationCompletedV1 && o.Reason == domain.AttemptOperationReasonNoneV1 && !o.Usage.ExceedsReservation(op.Debit)
		failed := op.Kind == domain.AttemptOperationModelV1 && (o.Outcome == domain.AttemptOperationRejectedBeforeSendV1 && o.Reason == domain.AttemptOperationReasonTransportUnknownV1 || o.Outcome == domain.AttemptOperationAcceptedOutcomeUnknownV1 && FailureModelUnknownReasonV2(o.Reason))
		if !complete && !failed {
			return deny()
		}
	default:
		return deny()
	}
	rd, _ := r.Digest()
	od, _ := o.Digest()
	return OwnedOperationFailureRowV4{Sequence: op.Sequence, Kind: op.Kind, OperationDigest: r.OperationDigest, ReservationDigest: rd, ObservationDigest: od, Outcome: o.Outcome, Reason: o.Reason, Reserved: op.Debit, Usage: o.Usage.Clone(), ToolSpendingDigest: spending}, nil
}

func searchFailureReservationV4(op domain.AttemptOperationV1, at time.Time, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) bool {
	c, s := parent.Authority.AdmissionCostCeiling.WebSearchCost, source.Spending[3]
	return c != nil && c.ValidateAt(at) == nil && s.ValidateAt(at) == nil && s.SourceKind == domain.AttemptSpendingOwnerLocalSourceV1 && c.Kind == domain.ExternalServiceWebSearchV1 &&
		c.TenantID == source.Scope.TenantID && c.OwnerUserID == source.Scope.OwnerUserID && c.BillingOwnerUserID == s.Locator.BillingOwnerUserID && c.ResourceID == s.ResourceID && c.ResourceRevision == s.ResourceRevision && c.RevocationGeneration == s.ResourceGeneration && c.ProfileID == s.ProfileID && c.ProfileRevision == s.ProfileRevision &&
		c.ScopedGrantDigest == s.ScopedGrantDigest && c.PriceEvidenceDigest == s.PriceEvidenceDigest && c.TermsEvidenceDigest == s.TermsEvidenceDigest && c.Currency == s.Currency && c.PriceRevision == s.PriceRevision && c.PriceState == domain.ExternalServicePriceKnownPaidV1 && s.PriceState == c.PriceState && c.MaxAmountMicrounits != nil && s.MaxAmountMicrounits != nil && *c.MaxAmountMicrounits == 20000 && *s.MaxAmountMicrounits == 20000 &&
		op.SearchWire != nil && op.ToolGrantDigest == s.ScopedGrantDigest && op.ExternalChargeEvidenceDigest == s.PriceEvidenceDigest && op.Currency == s.Currency && op.Debit.AmountMicrounits == 10000 && !op.Deadline.After(c.PriceExpiresAt) && !op.Deadline.After(s.Window.ExpiresAt) && !at.Before(source.IssuedAt) && !op.Deadline.After(source.ExpiresAt)
}

func (v OwnedOperationFailureAccountingV4) contentValid() bool {
	if v.Version != 4 || v.Scope.Validate() != nil || len(v.Operations) < 1 || len(v.Operations) > 12 || uint64(len(v.Operations)) != v.AggregateDebit.Operations || domain.ValidateSHA256Digest("failure.accounting", v.AccountingDigest) != nil || domain.ValidateSHA256Digest("failure.consents", v.ConsentStatesDigest) != nil || v.ConsentReservedBytes < (64<<10)+1 || v.ConsentReservedBytes > 2*((64<<10)+(4<<10)) {
		return false
	}
	for i, r := range v.Operations {
		if r.Sequence != uint64(i+1) || r.OperationDigest.Validate() != nil || r.ReservationDigest.Validate() != nil || r.ObservationDigest.Validate() != nil || r.Usage.Validate() != nil {
			return false
		}
		tool := r.Kind == domain.AttemptOperationMCPToolV1 || r.Kind == domain.AttemptOperationWebSearchV1
		if tool {
			if i == 0 || v.Operations[i-1].Kind != domain.AttemptOperationModelV1 || domain.ValidateSHA256Digest("failure.spending", r.ToolSpendingDigest) != nil || r.Usage.State != domain.AttemptOperationUsageUnknownV1 {
				return false
			}
		} else if (r.Kind != domain.AttemptOperationModelV1 && r.Kind != domain.AttemptOperationCompactionV1) || r.ToolSpendingDigest != "" || r.Reserved.AmountMicrounits != 0 {
			return false
		}
		if i != len(v.Operations)-1 && (r.Outcome != domain.AttemptOperationCompletedV1 || r.Reason != domain.AttemptOperationReasonNoneV1 || r.Usage.ExceedsReservation(r.Reserved)) {
			return false
		}
	}
	return true
}

func (v OwnedOperationFailureAccountingV4) ValidateForStop(parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, stop string) error {
	if !v.contentValid() || !accountingSourceV3(parent, source) || v.Scope != ownedResidualScope(parent) || v.AggregateDebit.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
		return domain.ErrMembershipDenied
	}
	last := v.Operations[len(v.Operations)-1]
	valid := false
	switch stop {
	case domain.SearchConsentStoppedV4:
		valid = last.Kind == domain.AttemptOperationModelV1 && last.Outcome == domain.AttemptOperationCompletedV1 && last.Reason == domain.AttemptOperationReasonNoneV1 && !last.Usage.ExceedsReservation(last.Reserved)
	case domain.SearchRejectedBeforeSendV4:
		valid = last.Kind == domain.AttemptOperationWebSearchV1 && last.Outcome == domain.AttemptOperationRejectedBeforeSendV1 && FailureSearchRejectedReasonV4(last.Reason)
	case domain.SearchServiceErrorV4:
		valid = last.Kind == domain.AttemptOperationWebSearchV1 && last.Outcome == domain.AttemptOperationCompletedV1 && last.Reason == domain.AttemptOperationReasonServiceErrorV1
	case domain.SearchOutcomeUnknownV4:
		valid = last.Kind == domain.AttemptOperationWebSearchV1 && last.Outcome == domain.AttemptOperationAcceptedOutcomeUnknownV1 && FailureSearchUnknownReasonV4(last.Reason)
	case domain.ModelAfterSearchFailureV4:
		for _, r := range v.Operations[:len(v.Operations)-1] {
			if r.Kind == domain.AttemptOperationWebSearchV1 {
				valid = true
			}
		}
		valid = valid && last.Kind == domain.AttemptOperationModelV1 && (last.Outcome == domain.AttemptOperationRejectedBeforeSendV1 && last.Reason == domain.AttemptOperationReasonTransportUnknownV1 || last.Outcome == domain.AttemptOperationAcceptedOutcomeUnknownV1 && FailureModelUnknownReasonV2(last.Reason))
	}
	if !valid {
		return domain.ErrMembershipDenied
	}
	aggregate := domain.AttemptOperationDebitV1{BodyBytes: v.ConsentReservedBytes}
	for _, c := range []*domain.ExternalServiceCostCeilingV1{parent.Authority.AdmissionCostCeiling.MCPToolCost, parent.Authority.AdmissionCostCeiling.WebSearchCost} {
		if c != nil {
			aggregate.BodyBytes += 4 << 10
		}
	}
	amounts := [4]uint64{0, *source.Spending[1].MaxAmountMicrounits, 0, 0}
	for _, r := range v.Operations {
		index := 0
		if r.Kind == domain.AttemptOperationMCPToolV1 {
			index = 2
		} else if r.Kind == domain.AttemptOperationWebSearchV1 {
			index = 3
		}
		if r.Reserved.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
			return domain.ErrMembershipDenied
		}
		if index != 0 {
			sd, _ := source.Spending[index].Digest()
			if sd != r.ToolSpendingDigest || index == 3 && r.Reserved.AmountMicrounits != 10000 || index == 2 && r.Reserved.AmountMicrounits != *source.Spending[2].MaxAmountMicrounits {
				return domain.ErrMembershipDenied
			}
		}
		var err error
		aggregate, err = aggregate.Add(r.Reserved)
		if err != nil || amounts[index] > math.MaxUint64-r.Reserved.AmountMicrounits {
			return domain.ErrMembershipDenied
		}
		amounts[index] += r.Reserved.AmountMicrounits
	}
	var total uint64
	for i, amount := range amounts {
		if source.Spending[i].MaxAmountMicrounits == nil || amount > *source.Spending[i].MaxAmountMicrounits || amount > math.MaxUint64-total {
			return domain.ErrMembershipDenied
		}
		total += amount
	}
	if aggregate != v.AggregateDebit || amounts != v.ComponentAmounts || total > source.MaxTotalAmountMicrounits {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (v OwnedOperationFailureAccountingV4) Digest() (string, error) {
	if !v.contentValid() {
		return "", domain.ErrMembershipDenied
	}
	return residualHashV2("sessionless.failure-accounting.v4\x00", v)
}
