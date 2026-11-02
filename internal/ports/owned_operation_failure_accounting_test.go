package ports_test

import (
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestOwnedOperationFailureAccountingKnownServiceErrorKeepsFullDebit(t *testing.T) {
	for _, amount := range []uint64{0, 19} {
		t.Run(string(rune('a'+amount)), func(t *testing.T) {
			parent, source, operations, _ := accountingV3Fixture(t, amount)
			operations = operations[:2]
			// Structural DATA setup only: this is not an authenticated producer. The
			// successful V3 consumer must continue to reject this changed error arm.
			operations[1].Observation.Reason = domain.AttemptOperationReasonServiceErrorV1
			v := ports.OwnedOperationFailureAccountingV1{Version: 1, Scope: operations[0].Reservation.Operation.Scope, AccountingDigest: strings.Repeat("a", 64), AggregateDebit: operations[1].Reservation.AggregateDebit, ComponentAmounts: operations[1].Reservation.ComponentAmounts}
			for _, op := range operations {
				row, err := ports.BuildOwnedOperationFailureRowV1(op.Reservation, op.Observation, parent, source)
				if err != nil {
					t.Fatal("failure row DATA", err)
				}
				v.Operations = append(v.Operations, row)
			}
			if err := v.ValidateFor(parent, source); err != nil {
				t.Fatal("complete known failure accounting", err)
			}
			if v.ComponentAmounts[2] != amount || v.Operations[1].Reserved.AmountMicrounits != amount || v.Operations[1].Usage.State != domain.AttemptOperationUsageUnknownV1 {
				t.Error("known application error refunded or settled money")
			}
			if _, err := ports.BuildOwnedOperationAccountingRowV3(operations[1].Reservation, operations[1].Observation, parent, source); err == nil {
				t.Error("failure weakened successful V3 language")
			}
			clone := v.Clone()
			clone.Operations[1].Reserved.AmountMicrounits++
			if clone.ValidateFor(parent, source) == nil {
				t.Error("changed full debit accepted")
			}
			clone = v.Clone()
			clone.Operations = clone.Operations[:1]
			if clone.ValidateFor(parent, source) == nil {
				t.Error("incomplete frontier accepted")
			}
			for _, outcome := range []domain.AttemptOperationOutcomeV1{domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationRejectedBeforeSendV1} {
				op := operations[1]
				op.Observation.Outcome = outcome
				if _, err := ports.BuildOwnedOperationFailureRowV1(op.Reservation, op.Observation, parent, source); err == nil {
					t.Error("unknown/rejected relabeled known service error")
				}
			}
		})
	}
}
