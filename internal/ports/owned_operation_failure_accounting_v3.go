package ports

import "gitcode.com/urandon/sessionless/internal/domain"

// V3 retains one authenticated UNKNOWN MCP tail. It is not the completed
// service-error V1 or UNKNOWN MODEL V2 language, successful accounting, a
// measured invoice, continuation permission, refund or erasure evidence.
type OwnedOperationFailureAccountingV3 OwnedOperationFailureAccountingV1

func FailureMCPUnknownReasonV3(reason domain.AttemptOperationReasonV1) bool {
	return reason == domain.AttemptOperationReasonTransportUnknownV1 || reason == domain.AttemptOperationReasonMalformedV1 || reason == domain.AttemptOperationReasonWorkerLostV1
}

func BuildOwnedOperationFailureRowV3(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) (OwnedOperationFailureRowV1, error) {
	if o.Outcome == domain.AttemptOperationCompletedV1 && o.Reason == domain.AttemptOperationReasonNoneV1 {
		return BuildOwnedOperationFailureRowV1(r, o, parent, source)
	}
	if !accountingSourceV3(parent, source) || r.ValidateForParent(parent.Authority, parent.Reservation) != nil || r.Operation.Scope != ownedResidualScope(parent) || o.ValidateForReservation(r) != nil || r.Operation.Kind != domain.AttemptOperationMCPToolV1 || o.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || !FailureMCPUnknownReasonV3(o.Reason) || o.ResponseRef != "" || o.ResponseDigest != "" || o.ResponseBytes != 0 || o.Usage.State != domain.AttemptOperationUsageUnknownV1 || !mcpReservationV3(r.Operation, r.ReservedAt, parent, source) {
		return OwnedOperationFailureRowV1{}, domain.ErrMembershipDenied
	}
	rd, _ := r.Digest()
	od, _ := o.Digest()
	spending, _ := source.Spending[2].Digest()
	return OwnedOperationFailureRowV1{Sequence: r.Operation.Sequence, Kind: r.Operation.Kind, OperationDigest: r.OperationDigest, ReservationDigest: rd, ObservationDigest: od, Outcome: o.Outcome, Reason: o.Reason, Reserved: r.Operation.Debit, Usage: o.Usage.Clone(), MCPSpendingDigest: spending}, nil
}

func (v OwnedOperationFailureAccountingV3) contentValid() bool {
	if v.Version != 3 || v.Scope.Validate() != nil || len(v.Operations) < 2 || len(v.Operations) > 12 || uint64(len(v.Operations)) != v.AggregateDebit.Operations || domain.ValidateSHA256Digest("failure.accounting", v.AccountingDigest) != nil {
		return false
	}
	for i, row := range v.Operations {
		if row.Sequence != uint64(i+1) || row.OperationDigest.Validate() != nil || row.ReservationDigest.Validate() != nil || row.ObservationDigest.Validate() != nil || row.Usage.Validate() != nil {
			return false
		}
		if i == len(v.Operations)-1 {
			if row.Kind != domain.AttemptOperationMCPToolV1 || v.Operations[i-1].Kind != domain.AttemptOperationModelV1 || row.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 || !FailureMCPUnknownReasonV3(row.Reason) || row.Usage.State != domain.AttemptOperationUsageUnknownV1 || domain.ValidateSHA256Digest("failure.spending", row.MCPSpendingDigest) != nil {
				return false
			}
			continue
		}
		if row.Outcome != domain.AttemptOperationCompletedV1 || row.Reason != domain.AttemptOperationReasonNoneV1 || row.Usage.ExceedsReservation(row.Reserved) {
			return false
		}
		if row.Kind == domain.AttemptOperationMCPToolV1 {
			if i == 0 || v.Operations[i-1].Kind != domain.AttemptOperationModelV1 || row.Usage.State != domain.AttemptOperationUsageUnknownV1 || domain.ValidateSHA256Digest("failure.spending", row.MCPSpendingDigest) != nil {
				return false
			}
		} else if (row.Kind != domain.AttemptOperationModelV1 && row.Kind != domain.AttemptOperationCompactionV1) || row.MCPSpendingDigest != "" || row.Reserved.AmountMicrounits != 0 {
			return false
		}
	}
	return true
}

func (v OwnedOperationFailureAccountingV3) ValidateFor(parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	if !v.contentValid() || !accountingSourceV3(parent, source) || v.Scope != ownedResidualScope(parent) || v.AggregateDebit.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
		return domain.ErrMembershipDenied
	}
	return validateFailureAccountingTotals(OwnedOperationFailureAccountingV1(v), parent, source)
}

func (v OwnedOperationFailureAccountingV3) Digest() (string, error) {
	if !v.contentValid() {
		return "", domain.ErrMembershipDenied
	}
	return residualHashV2("sessionless.failure-accounting.v3\x00", v)
}

func (v OwnedOperationFailureAccountingV3) Clone() OwnedOperationFailureAccountingV3 {
	return OwnedOperationFailureAccountingV3(OwnedOperationFailureAccountingV1(v).Clone())
}
