package ports_test

import (
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestOwnedOperationFailureAccountingV3UnknownMCPKeepsFullDebit(t *testing.T) {
	for _, amount := range []uint64{0, 19} {
		parent, source, operations, _ := accountingV3Fixture(t, amount)
		operations = operations[:2]
		last := &operations[1].Observation
		last.Outcome, last.Reason = domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonTransportUnknownV1
		last.ResponseRef, last.ResponseDigest, last.ResponseBytes = "", "", 0
		v := ports.OwnedOperationFailureAccountingV3{Version: 3, Scope: operations[0].Reservation.Operation.Scope, AccountingDigest: strings.Repeat("a", 64), AggregateDebit: operations[1].Reservation.AggregateDebit, ComponentAmounts: operations[1].Reservation.ComponentAmounts}
		for _, op := range operations {
			row, err := ports.BuildOwnedOperationFailureRowV3(op.Reservation, op.Observation, parent, source)
			if err != nil {
				t.Fatal("strict failure V3 row DATA", err)
			}
			v.Operations = append(v.Operations, row)
		}
		if v.ValidateFor(parent, source) != nil || v.ComponentAmounts[2] != amount || v.Operations[1].Reserved.AmountMicrounits != amount || v.Operations[1].Usage.State != domain.AttemptOperationUsageUnknownV1 || v.Operations[1].Usage.AmountMicrounits != nil {
			t.Fatal("UNKNOWN MCP lost full debit or invented measured money")
		}
		if _, err := v.Digest(); err != nil {
			t.Fatal(err)
		}
		if ports.OwnedOperationFailureAccountingV1(v).ValidateFor(parent, source) == nil || ports.OwnedOperationFailureAccountingV2(v).ValidateFor(parent, source) == nil {
			t.Fatal("F3 reinterpreted as older failure language")
		}
		if _, err := ports.BuildOwnedOperationAccountingRowV3(operations[1].Reservation, *last, parent, source); err == nil {
			t.Fatal("UNKNOWN MCP became successful completed accounting")
		}
		for _, mode := range []string{"version", "completed", "service_error", "refunded", "measured", "unknown_model", "prefix_unknown", "tail_removed"} {
			bad := v.Clone()
			switch mode {
			case "version":
				bad.Version = 2
			case "completed":
				bad.Operations[1].Outcome = domain.AttemptOperationCompletedV1
			case "service_error":
				bad.Operations[1].Reason = domain.AttemptOperationReasonServiceErrorV1
			case "refunded":
				bad.ComponentAmounts[2]++
			case "measured":
				bad.Operations[1].Usage.State = domain.AttemptOperationUsageObservedV1
			case "unknown_model":
				bad.Operations[1].Kind = domain.AttemptOperationModelV1
			case "prefix_unknown":
				bad.Operations[0].Outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
			case "tail_removed":
				bad.Operations = bad.Operations[:1]
			}
			if bad.ValidateFor(parent, source) == nil {
				t.Fatal("malformed F3 admitted", mode)
			}
		}
		imagined := *last
		imagined.ResponseRef, imagined.ResponseDigest, imagined.ResponseBytes = "imagined-response", strings.Repeat("b", 64), 1
		if _, err := ports.BuildOwnedOperationFailureRowV3(operations[1].Reservation, imagined, parent, source); err == nil {
			t.Fatal("UNKNOWN MCP acquired an invented response")
		}
	}
}
