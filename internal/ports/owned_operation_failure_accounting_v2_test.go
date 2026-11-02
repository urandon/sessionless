package ports_test

import (
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestOwnedOperationFailureAccountingV2UnknownModelKeepsKnownToolAndDebit(t *testing.T) {
	for _, amount := range []uint64{0, 19} {
		parent, source, operations, _ := accountingV3Fixture(t, amount)
		last := &operations[2].Observation
		last.Outcome, last.Reason = domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonMalformedV1
		last.ResponseRef, last.ResponseDigest, last.ResponseBytes = "", "", 0
		// Reported overrun remains authentic retained DATA, not continuation or
		// refund permission. V1/success consumers must continue to reject it.
		tokens := operations[2].Reservation.Operation.Debit.InputTokens + 1
		last.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: &tokens}
		v := ports.OwnedOperationFailureAccountingV2{Version: 2, Scope: operations[0].Reservation.Operation.Scope, AccountingDigest: strings.Repeat("a", 64), AggregateDebit: operations[2].Reservation.AggregateDebit, ComponentAmounts: operations[2].Reservation.ComponentAmounts}
		for _, op := range operations {
			row, err := ports.BuildOwnedOperationFailureRowV2(op.Reservation, op.Observation, parent, source)
			if err != nil {
				t.Fatal("V2 retained row", err)
			}
			v.Operations = append(v.Operations, row)
		}
		if err := v.ValidateFor(parent, source); err != nil {
			t.Fatal("V2 complete UNKNOWN MODEL tail", err)
		}
		if v.ComponentAmounts[2] != amount || v.Operations[1].Reserved.AmountMicrounits != amount || v.Operations[1].Usage.AmountMicrounits != nil || *v.Operations[2].Usage.InputTokens != tokens {
			t.Fatal("V2 invented settlement/refund or discarded reported overrun")
		}
		if _, err := v.Digest(); err != nil {
			t.Fatal(err)
		}
		if ports.OwnedOperationFailureAccountingV1(v).ValidateFor(parent, source) == nil {
			t.Fatal("V2 reinterpreted as old strict V1")
		}
		if _, err := ports.BuildOwnedOperationAccountingRowV3(operations[2].Reservation, *last, parent, source); err == nil {
			t.Fatal("UNKNOWN MODEL weakened successful V3")
		}
		for _, mode := range []string{"version", "tail_removed", "tail_as_completed", "unknown_tool", "reason", "refunded", "changed_usage"} {
			bad := v.Clone()
			switch mode {
			case "version":
				bad.Version = 1
			case "tail_removed":
				bad.Operations = bad.Operations[:2]
				bad.AggregateDebit = operations[1].Reservation.AggregateDebit
			case "tail_as_completed":
				bad.Operations[2].Outcome = domain.AttemptOperationCompletedV1
			case "unknown_tool":
				bad.Operations[1].Outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
			case "reason":
				bad.Operations[2].Reason = domain.AttemptOperationReasonServiceErrorV1
			case "refunded":
				bad.ComponentAmounts[2]++
			case "changed_usage":
				bad.Operations[2].Usage.State = domain.AttemptOperationUsageUnknownV1
			}
			if bad.ValidateFor(parent, source) == nil {
				t.Fatal("malformed V2 retained accounting accepted", mode)
			}
		}
		imagined := *last
		imagined.ResponseRef, imagined.ResponseDigest, imagined.ResponseBytes = "invented-response", strings.Repeat("a", 64), 1
		if _, err := ports.BuildOwnedOperationFailureRowV2(operations[2].Reservation, imagined, parent, source); err == nil {
			t.Fatal("UNKNOWN MODEL invented response admitted")
		}
	}
}
