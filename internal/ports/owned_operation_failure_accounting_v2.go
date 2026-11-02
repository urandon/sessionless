package ports

import "gitcode.com/urandon/sessionless/internal/domain"

// V2 retains a complete known prefix followed by an authenticated UNKNOWN MODEL
// tail. It is distinct from V1's known MCP-service-error language. The shared row
// shape is DATA only; neither format is a successful accounting disposition.
type OwnedOperationFailureAccountingV2 OwnedOperationFailureAccountingV1

func BuildOwnedOperationFailureRowV2(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) (OwnedOperationFailureRowV1, error) {
	if o.Outcome == domain.AttemptOperationCompletedV1 && o.Reason == domain.AttemptOperationReasonNoneV1 {
		return BuildOwnedOperationFailureRowV1(r, o, parent, source)
	}
	if !accountingSourceV3(parent, source) || r.ValidateForParent(parent.Authority, parent.Reservation) != nil || r.Operation.Scope != ownedResidualScope(parent) || o.ValidateForReservation(r) != nil || r.Operation.Kind != domain.AttemptOperationModelV1 || o.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || !FailureModelUnknownReasonV2(o.Reason) || o.ResponseRef != "" || o.ResponseDigest != "" || o.ResponseBytes != 0 || r.Operation.Debit.AmountMicrounits != 0 {
		return OwnedOperationFailureRowV1{}, domain.ErrMembershipDenied
	}
	rd, _ := r.Digest()
	od, _ := o.Digest()
	return OwnedOperationFailureRowV1{Sequence: r.Operation.Sequence, Kind: r.Operation.Kind, OperationDigest: r.OperationDigest, ReservationDigest: rd, ObservationDigest: od, Outcome: o.Outcome, Reason: o.Reason, Reserved: r.Operation.Debit, Usage: o.Usage.Clone()}, nil
}

// The closed reason language is DATA validation, never writer eligibility.
// A worker_lost row with a tainted response PUT still cannot pass physical
// retained-body correlation/fencing in the original failure owner.
func FailureModelUnknownReasonV2(reason domain.AttemptOperationReasonV1) bool {
	return reason == domain.AttemptOperationReasonMalformedV1 || reason == domain.AttemptOperationReasonTransportUnknownV1 || reason == domain.AttemptOperationReasonWorkerLostV1
}

func (v OwnedOperationFailureAccountingV2) contentValid() bool {
	if v.Version != 2 || v.Scope.Validate() != nil || len(v.Operations) < 3 || len(v.Operations) > 12 || uint64(len(v.Operations)) != v.AggregateDebit.Operations || domain.ValidateSHA256Digest("failure.accounting", v.AccountingDigest) != nil {
		return false
	}
	tools := 0
	for i, r := range v.Operations {
		if r.Sequence != uint64(i+1) || r.OperationDigest.Validate() != nil || r.ReservationDigest.Validate() != nil || r.ObservationDigest.Validate() != nil || r.Usage.Validate() != nil {
			return false
		}
		if i == len(v.Operations)-1 {
			if tools == 0 || r.Kind != domain.AttemptOperationModelV1 || r.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || !FailureModelUnknownReasonV2(r.Reason) || r.MCPSpendingDigest != "" || r.Reserved.AmountMicrounits != 0 {
				return false
			}
			continue
		}
		if r.Outcome != domain.AttemptOperationCompletedV1 || r.Reason != domain.AttemptOperationReasonNoneV1 || r.Usage.ExceedsReservation(r.Reserved) {
			return false
		}
		if r.Kind == domain.AttemptOperationMCPToolV1 {
			if i == 0 || v.Operations[i-1].Kind != domain.AttemptOperationModelV1 || r.Usage.State != domain.AttemptOperationUsageUnknownV1 || domain.ValidateSHA256Digest("failure.spending", r.MCPSpendingDigest) != nil {
				return false
			}
			tools++
		} else if (r.Kind != domain.AttemptOperationModelV1 && r.Kind != domain.AttemptOperationCompactionV1) || r.MCPSpendingDigest != "" || r.Reserved.AmountMicrounits != 0 {
			return false
		}
	}
	return true
}

func (v OwnedOperationFailureAccountingV2) ValidateFor(parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	if !v.contentValid() || !accountingSourceV3(parent, source) || v.Scope != ownedResidualScope(parent) || v.AggregateDebit.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
		return domain.ErrMembershipDenied
	}
	return validateFailureAccountingTotals(OwnedOperationFailureAccountingV1(v), parent, source)
}
func (v OwnedOperationFailureAccountingV2) Digest() (string, error) {
	if !v.contentValid() {
		return "", domain.ErrMembershipDenied
	}
	return residualHashV2("sessionless.failure-accounting.v2\x00", v)
}
func (v OwnedOperationFailureAccountingV2) Clone() OwnedOperationFailureAccountingV2 {
	return OwnedOperationFailureAccountingV2(OwnedOperationFailureAccountingV1(v).Clone())
}
