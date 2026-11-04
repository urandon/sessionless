package ports_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Correlated structural DATA only, not an original installation, authenticated
// consent, current grant, physical transport or cleanup/erasure receipt.
func accountingV4Fixture(t *testing.T, searches int) (domain.OwnedAttemptEffectParentV1, domain.AttemptExecutionSourceV1, []ports.OwnedOperationResidualRowV1, ports.OwnedOperationAccountingDispositionV4) {
	t.Helper()
	if searches < 1 || searches > 2 {
		t.Fatal("bounded fixture search count")
	}
	parent, source, old, previous := accountingV3Fixture(t, 0, 20000)
	v := ports.OwnedOperationAccountingDispositionV4{Version: 4, Scope: previous.Scope, AccountingDigest: previous.AccountingDigest, ProviderInvoice: previous.ProviderInvoice, ConsentStatesDigest: strings.Repeat("c", 64)}
	aggregate := domain.AttemptOperationDebitV1{BodyBytes: 8 << 10}
	amounts := [4]uint64{}
	var rows []ports.OwnedOperationResidualRowV1
	for i := 0; i < 2*searches+1; i++ {
		op := old[0].Reservation.Operation.Clone()
		op.Sequence, op.CallID = uint64(i+1), "call-v4-"+string(rune('a'+i))
		op.Deadline = old[0].Reservation.ReservedAt.Add(time.Duration(i+10) * time.Second)
		if i > 0 {
			op.PredecessorObservationDigest, _ = rows[i-1].Observation.Digest()
		}
		if i%2 == 1 {
			charge := uint64((64 << 10) + 37)
			var err error
			aggregate, err = aggregate.Add(domain.AttemptOperationDebitV1{BodyBytes: charge})
			if err != nil {
				t.Fatal(err)
			}
			v.ConsentReservedBytes += charge
			op.Kind, op.ContextBytes = domain.AttemptOperationWebSearchV1, 0
			op.ModelResponseDigest, op.ArgumentsDigest, op.QueryDigest = rows[i-1].Observation.ResponseDigest, accountingV3Hash("arguments-"+op.CallID), strings.Repeat("e", 64)
			op.ToolGrantDigest, op.ExternalChargeEvidenceDigest = source.Spending[3].ScopedGrantDigest, source.Spending[3].PriceEvidenceDigest
			op.ConsentRequestDigest, op.ConsentDecisionDigest, op.ConsentDecisionRef = strings.Repeat("f", 64), strings.Repeat("b", 64), "consent-v4-"+string(rune('a'+i))
			op.SearchWire = &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: domain.WebSearchAPIProfileV1, ArgumentsBytes: 37, CompilerArtifactDigest: strings.Repeat("a", 64)}
			op.Debit = domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, WebSearches: 1, BodyBytes: 1000, ResponseBytes: 1000, AmountMicrounits: 10000, ActiveDuration: 10 * time.Second}
			amounts[3] += 10000
		}
		var err error
		aggregate, err = aggregate.Add(op.Debit)
		if err != nil {
			t.Fatal(err)
		}
		r := old[0].Reservation.Clone()
		r.Operation, r.AggregateDebit, r.ComponentAmounts = op, aggregate, amounts
		r.ReservedAt = old[0].Reservation.ReservedAt.Add(time.Duration(i) * time.Second)
		r.OperationDigest, err = op.Digest()
		if err != nil {
			t.Fatal("operation DATA", err)
		}
		o := old[0].Observation.Clone()
		o.OperationDigest, o.ObservedAt = r.OperationDigest, r.ReservedAt.Add(time.Millisecond)
		o.ReservationDigest, err = r.Digest()
		if err != nil {
			t.Fatal("reservation DATA", err)
		}
		row, err := ports.BuildOwnedOperationAccountingRowV4(r, o, parent, source)
		if err != nil {
			t.Fatal("correlated V4 row", i, err)
		}
		rows = append(rows, ports.OwnedOperationResidualRowV1{Reservation: r, Observation: o})
		v.Operations = append(v.Operations, row)
	}
	v.AggregateDebit, v.ComponentAmounts = aggregate, amounts
	if err := v.ValidateFor(parent, source); err != nil {
		t.Fatal("valid V4 structural baseline", err)
	}
	return parent, source, rows, v
}

func TestOwnedOperationAccountingV4KnownResultUnknownUSD(t *testing.T) {
	for _, count := range []int{1, 2} {
		parent, source, rows, v := accountingV4Fixture(t, count)
		if v.AggregateDebit.WebSearches != uint64(count) || v.ComponentAmounts[3] != uint64(count)*10000 || v.AggregateDebit.AmountMicrounits != uint64(count)*10000 {
			t.Fatal("exact nonrefundable call count/debit")
		}
		if _, err := v.Digest(); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"version", "refund", "whole_component", "consent_charge", "consent_digest", "measured_USD", "spending", "foreign_scope", "kind", "failed_result", "source_tariff"} {
			t.Run(string(rune('0'+count))+"/"+name, func(t *testing.T) {
				bad, s := v.Clone(), source.Clone()
				switch name {
				case "version":
					bad.Version = 3
				case "refund":
					bad.ComponentAmounts[3]--
				case "whole_component":
					bad.Operations[1].Reserved.AmountMicrounits = 20000
				case "consent_charge":
					bad.ConsentReservedBytes--
				case "consent_digest":
					bad.ConsentStatesDigest = ""
				case "measured_USD":
					zero := uint64(0)
					bad.Operations[1].Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: &zero}
				case "spending":
					bad.Operations[1].ToolSpendingDigest = strings.Repeat("0", 64)
				case "foreign_scope":
					bad.Scope.PhysicalInvocationClaimID = "foreign"
				case "kind":
					bad.Operations[1].Kind = domain.AttemptOperationMCPToolV1
				case "failed_result":
					o := rows[1].Observation.Clone()
					o.Reason = domain.AttemptOperationReasonServiceErrorV1
					if _, err := ports.BuildOwnedOperationAccountingRowV4(rows[1].Reservation, o, parent, source); err == nil {
						t.Fatal("failure row admitted to success")
					}
					return
				case "source_tariff":
					s.Spending[3].MaxAmountMicrounits = accountingV3Amount(10000)
				}
				if bad.ValidateFor(parent, s) == nil {
					t.Fatal("contradiction admitted", name)
				}
			})
		}
		copy := v.Clone()
		copy.Operations[1].ToolSpendingDigest = "changed"
		if reflect.DeepEqual(copy, v) || v.Operations[1].ToolSpendingDigest == "changed" {
			t.Fatal("clone aliases original rows")
		}
	}
}

func TestOwnedOperationAccountingV4DoesNotRetagMCPOnly(t *testing.T) {
	parent, source, rows, _ := accountingV3Fixture(t, 0)
	for _, item := range rows {
		if _, err := ports.BuildOwnedOperationAccountingRowV4(item.Reservation, item.Observation, parent, source); err != nil {
			t.Fatal("unchanged correlated MODEL/MCP row remains usable inside V4", err)
		}
	}
	if _, err := (ports.OwnedOperationAccountingDispositionV4{}).Digest(); err == nil {
		t.Fatal("unpopulated successor admitted")
	}
}
