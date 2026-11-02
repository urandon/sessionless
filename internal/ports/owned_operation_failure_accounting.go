package ports

import (
	"math"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Failure accounting is retained DATA. It cannot satisfy successful V2/V3
// accounting, authorize continuation, settle money or attest physical erasure.
type OwnedOperationFailureRowV1 struct {
	Sequence          uint64                                     `json:"sequence"`
	Kind              domain.AttemptOperationKindV1              `json:"kind"`
	OperationDigest   domain.AttemptOperationDigestV1            `json:"operation_digest"`
	ReservationDigest domain.AttemptOperationReservationDigestV1 `json:"reservation_digest"`
	ObservationDigest domain.AttemptOperationObservationDigestV1 `json:"observation_digest"`
	Outcome           domain.AttemptOperationOutcomeV1           `json:"outcome"`
	Reason            domain.AttemptOperationReasonV1            `json:"reason"`
	Reserved          domain.AttemptOperationDebitV1             `json:"reserved"`
	Usage             domain.AttemptOperationUsageV1             `json:"usage"`
	MCPSpendingDigest string                                     `json:"mcp_spending_digest,omitempty"`
}

type OwnedOperationFailureAccountingV1 struct {
	Version          uint32                         `json:"version"`
	Scope            domain.AttemptOperationScopeV1 `json:"scope"`
	AccountingDigest string                         `json:"accounting_digest"`
	AggregateDebit   domain.AttemptOperationDebitV1 `json:"aggregate_debit"`
	ComponentAmounts [4]uint64                      `json:"component_amounts"`
	Operations       []OwnedOperationFailureRowV1   `json:"operations"`
}

func BuildOwnedOperationFailureRowV1(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) (OwnedOperationFailureRowV1, error) {
	deny := func() (OwnedOperationFailureRowV1, error) {
		return OwnedOperationFailureRowV1{}, domain.ErrMembershipDenied
	}
	if !accountingSourceV3(parent, source) || r.ValidateForParent(parent.Authority, parent.Reservation) != nil || r.Operation.Scope != ownedResidualScope(parent) || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Usage.ExceedsReservation(r.Operation.Debit) {
		return deny()
	}
	var spending string
	if r.Operation.Kind == domain.AttemptOperationMCPToolV1 {
		if (o.Reason != domain.AttemptOperationReasonServiceErrorV1 && o.Reason != domain.AttemptOperationReasonNoneV1) || o.Usage.State != domain.AttemptOperationUsageUnknownV1 || !mcpReservationV3(r.Operation, r.ReservedAt, parent, source) {
			return deny()
		}
		spending, _ = source.Spending[2].Digest()
	} else {
		if o.Reason != domain.AttemptOperationReasonNoneV1 || r.Operation.Debit.AmountMicrounits != 0 {
			return deny()
		}
		if _, err := BuildOwnedOperationAccountingRowV2(r, o); err != nil {
			return deny()
		}
	}
	rd, _ := r.Digest()
	od, _ := o.Digest()
	return OwnedOperationFailureRowV1{Sequence: r.Operation.Sequence, Kind: r.Operation.Kind, OperationDigest: r.OperationDigest, ReservationDigest: rd, ObservationDigest: od, Outcome: o.Outcome, Reason: o.Reason, Reserved: r.Operation.Debit, Usage: o.Usage.Clone(), MCPSpendingDigest: spending}, nil
}

func (v OwnedOperationFailureAccountingV1) contentValid() bool {
	if v.Version != 1 || v.Scope.Validate() != nil || len(v.Operations) < 2 || len(v.Operations) > 12 || uint64(len(v.Operations)) != v.AggregateDebit.Operations || domain.ValidateSHA256Digest("failure.accounting", v.AccountingDigest) != nil {
		return false
	}
	for i, r := range v.Operations {
		if r.Sequence != uint64(i+1) || r.OperationDigest.Validate() != nil || r.ReservationDigest.Validate() != nil || r.ObservationDigest.Validate() != nil || r.Usage.Validate() != nil || r.Usage.ExceedsReservation(r.Reserved) || r.Outcome != domain.AttemptOperationCompletedV1 {
			return false
		}
		if i == len(v.Operations)-1 {
			if r.Kind != domain.AttemptOperationMCPToolV1 || v.Operations[i-1].Kind != domain.AttemptOperationModelV1 || r.Reason != domain.AttemptOperationReasonServiceErrorV1 || r.Usage.State != domain.AttemptOperationUsageUnknownV1 || domain.ValidateSHA256Digest("failure.spending", r.MCPSpendingDigest) != nil {
				return false
			}
		} else if r.Kind == domain.AttemptOperationMCPToolV1 {
			if i == 0 || v.Operations[i-1].Kind != domain.AttemptOperationModelV1 || r.Reason != domain.AttemptOperationReasonNoneV1 || r.Usage.State != domain.AttemptOperationUsageUnknownV1 || domain.ValidateSHA256Digest("failure.spending", r.MCPSpendingDigest) != nil {
				return false
			}
		} else if (r.Kind != domain.AttemptOperationModelV1 && r.Kind != domain.AttemptOperationCompactionV1) || r.Reason != domain.AttemptOperationReasonNoneV1 || r.MCPSpendingDigest != "" || r.Reserved.AmountMicrounits != 0 {
			return false
		}
	}
	return true
}

func (v OwnedOperationFailureAccountingV1) ValidateFor(parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	if !v.contentValid() || !accountingSourceV3(parent, source) || v.Scope != ownedResidualScope(parent) || v.AggregateDebit.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
		return domain.ErrMembershipDenied
	}
	return validateFailureAccountingTotals(v, parent, source)
}

// Arithmetic shared by separately validated retained formats. This helper does
// not select a format, authorize continuation or relax either content language.
func validateFailureAccountingTotals(v OwnedOperationFailureAccountingV1, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	var aggregate domain.AttemptOperationDebitV1
	for _, c := range []*domain.ExternalServiceCostCeilingV1{parent.Authority.AdmissionCostCeiling.MCPToolCost, parent.Authority.AdmissionCostCeiling.WebSearchCost} {
		if c != nil {
			aggregate.BodyBytes += 4 << 10
		}
	}
	amounts := [4]uint64{0, *source.Spending[1].MaxAmountMicrounits, 0, 0}
	spending, _ := source.Spending[2].Digest()
	for _, r := range v.Operations {
		if r.Reserved.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
			return domain.ErrMembershipDenied
		}
		i := 0
		if r.Kind == domain.AttemptOperationMCPToolV1 {
			i = 2
			if r.MCPSpendingDigest != spending || r.Reserved.AmountMicrounits != *source.Spending[2].MaxAmountMicrounits {
				return domain.ErrMembershipDenied
			}
		}
		var err error
		aggregate, err = aggregate.Add(r.Reserved)
		if err != nil || r.Reserved.AmountMicrounits > math.MaxUint64-amounts[i] {
			return domain.ErrMembershipDenied
		}
		amounts[i] += r.Reserved.AmountMicrounits
	}
	var total uint64
	for i, a := range amounts {
		if a > *source.Spending[i].MaxAmountMicrounits || a > math.MaxUint64-total {
			return domain.ErrMembershipDenied
		}
		total += a
	}
	if total > source.MaxTotalAmountMicrounits || amounts != v.ComponentAmounts || aggregate != v.AggregateDebit {
		return domain.ErrMembershipDenied
	}
	return nil
}
func (v OwnedOperationFailureAccountingV1) Digest() (string, error) {
	if !v.contentValid() {
		return "", domain.ErrMembershipDenied
	}
	return residualHashV2("sessionless.failure-accounting.v1\x00", v)
}
func (v OwnedOperationFailureAccountingV1) Clone() OwnedOperationFailureAccountingV1 {
	v.Operations = append([]OwnedOperationFailureRowV1(nil), v.Operations...)
	for i := range v.Operations {
		v.Operations[i].Usage = v.Operations[i].Usage.Clone()
	}
	return v
}
