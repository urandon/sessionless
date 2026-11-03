package ports_test

import (
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestOwnedOperationFailureAccountingV4ConsentTailIsBodyOnly(t *testing.T) {
	parent, source, operations, _ := accountingV3Fixture(t, 0)
	first := operations[0]
	row, err := ports.BuildOwnedOperationFailureRowV4(first.Reservation, first.Observation, parent, source)
	if err != nil {
		t.Fatal("actual correlated MODEL row DATA", err)
	}
	aggregate, err := first.Reservation.AggregateDebit.Add(domain.AttemptOperationDebitV1{BodyBytes: (64 << 10) + 37})
	if err != nil {
		t.Fatal(err)
	}
	v := ports.OwnedOperationFailureAccountingV4{Version: 4, Scope: first.Reservation.Operation.Scope, AccountingDigest: strings.Repeat("a", 64), AggregateDebit: aggregate, ComponentAmounts: first.Reservation.ComponentAmounts, ConsentStatesDigest: strings.Repeat("b", 64), ConsentReservedBytes: (64 << 10) + 37, Operations: []ports.OwnedOperationFailureRowV4{row}}
	if err := v.ValidateForStop(parent, source, domain.SearchConsentStoppedV4); err != nil {
		t.Fatal("one real MODEL + trailing consent debit", err)
	}
	if v.AggregateDebit.Operations != 1 || v.AggregateDebit.WebSearches != 0 || v.AggregateDebit.ToolDispatches != 0 || v.AggregateDebit.AmountMicrounits != 0 {
		t.Fatal("consent invented execution/money", v.AggregateDebit)
	}
	if _, err := v.Digest(); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"version", "refund", "phantom", "digest", "USD", "foreign", "crossed-stop"} {
		bad := v
		bad.Operations = append([]ports.OwnedOperationFailureRowV4(nil), v.Operations...)
		stop := domain.SearchConsentStoppedV4
		switch mutation {
		case "version":
			bad.Version = 3
		case "refund":
			bad.AggregateDebit.BodyBytes--
		case "phantom":
			bad.AggregateDebit.Operations++
		case "digest":
			bad.ConsentStatesDigest = ""
		case "USD":
			bad.ComponentAmounts[3] = 10000
		case "foreign":
			bad.Scope.PhysicalInvocationClaimID = "foreign"
		case "crossed-stop":
			stop = domain.SearchOutcomeUnknownV4
		}
		if bad.ValidateForStop(parent, source, stop) == nil {
			t.Fatalf("mutation %s admitted", mutation)
		}
	}
}

func TestFailureSearchV4ClosedReasonSets(t *testing.T) {
	for _, reason := range []domain.AttemptOperationReasonV1{domain.AttemptOperationReasonAuthorityDeniedV1, domain.AttemptOperationReasonCancelledV1, domain.AttemptOperationReasonDeadlineV1, domain.AttemptOperationReasonTransportUnknownV1, domain.AttemptOperationReasonWorkerLostV1} {
		if !ports.FailureSearchRejectedReasonV4(reason) || !ports.FailureSearchUnknownReasonV4(reason) {
			t.Fatalf("required reason %s", reason)
		}
	}
	for _, reason := range []domain.AttemptOperationReasonV1{domain.AttemptOperationReasonMalformedV1, domain.AttemptOperationReasonOverflowV1} {
		if ports.FailureSearchRejectedReasonV4(reason) || !ports.FailureSearchUnknownReasonV4(reason) {
			t.Fatalf("possible-send-only reason %s", reason)
		}
	}
	for _, reason := range []domain.AttemptOperationReasonV1{domain.AttemptOperationReasonNoneV1, domain.AttemptOperationReasonServiceErrorV1, "arbitrary"} {
		if ports.FailureSearchRejectedReasonV4(reason) || ports.FailureSearchUnknownReasonV4(reason) {
			t.Fatalf("unsupported reason %s", reason)
		}
	}
}
