package ydbstore

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func usageMetric(v uint64) *uint64 { return &v }

// Structural reservation/observation DATA for the I/O-free continuation rule.
// This fixture grants no source approval, price/Store admission or send right.
func usageReservation(t *testing.T, kind domain.AttemptOperationKindV1) (domain.AttemptOperationReservationV1, domain.AttemptOperationObservationV1) {
	t.Helper()
	h := strings.Repeat("a", 64)
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	op := domain.AttemptOperationV1{Version: 1, Scope: domain.AttemptOperationScopeV1{TenantID: "usage-tenant", OwnerUserID: "usage-owner", SessionID: "usage-session", RunID: "usage-run", AttemptID: "usage-attempt", LeaseID: "usage-lease", FenceToken: 1, PhysicalInvocationClaimID: "usage-claim", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(h), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(h), HarnessBindingDigest: domain.HarnessBindingDigestV2(h), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(h)}, Sequence: 1, Kind: kind, CallID: "usage-call", RequestRef: "usage-request", RequestDigest: h, Deadline: at.Add(time.Second), CleanupDuration: time.Second, RequestBytes: 16, ResultBytes: 64, ContextBytes: 16, Currency: "USD", PriceRevision: "usage-price", AdmissionCostCeilingDigest: domain.AdmissionCostCeilingDigestV2(h), Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 10, OutputTokens: 5, BodyBytes: 80, ResponseBytes: 64, AmountMicrounits: 7, ActiveDuration: time.Second}}
	if kind == domain.AttemptOperationCompactionV1 {
		op.SummaryBytes = 16
		op.Debit.CompactionCalls = 1
	}
	if kind == domain.AttemptOperationMCPToolV1 || kind == domain.AttemptOperationWebSearchV1 {
		op.Sequence = 2
		op.PredecessorObservationDigest = domain.AttemptOperationObservationDigestV1(h)
		op.ContextBytes = 0
		op.Debit.ModelCalls = 0
		op.Debit.InputTokens = 0
		op.Debit.OutputTokens = 0
		op.Debit.ToolDispatches = 1
		op.ModelResponseDigest = h
		op.ArgumentsDigest = h
		op.ToolGrantDigest = h
		op.ExternalChargeEvidenceDigest = h
		if kind == domain.AttemptOperationMCPToolV1 {
			op.Debit.MCPOutboundMessages = 1
			op.Debit.MCPInboundFrames = 1
			op.Debit.MCPInboundBytes = 1
		} else {
			op.Debit.WebSearches = 1
			op.QueryDigest = h
			op.ConsentRequestDigest = h
			op.ConsentDecisionDigest = h
			op.ConsentDecisionRef = "usage-consent"
		}
	}
	od, err := op.Digest()
	if err != nil {
		t.Fatal(err)
	}
	aggregate := op.Debit
	aggregate.Operations = op.Sequence
	if op.Sequence == 2 {
		aggregate.ModelCalls = 1
	}
	r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: od, ReservedAt: at, AggregateDebit: aggregate, RegisteredAllocationDigest: domain.RegisteredAllocationDigestV2(h), RegisteredProxyDigest: domain.RegisteredProxyAttestationDigestV2(h), ComponentAmounts: [4]uint64{7}}
	rd, err := r.Digest()
	if err != nil {
		t.Fatal(err)
	}
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: od, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: "usage-response", ResponseDigest: h, ResponseBytes: 32, ObservedAt: at.Add(time.Microsecond), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
	if err := o.ValidateForReservation(r); err != nil {
		t.Fatal(err)
	}
	return r, o
}

func TestOwnedOperationUsageModelContinuationIsConservativeNotMeasuredBilling(t *testing.T) {
	for _, kind := range []domain.AttemptOperationKindV1{domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1} {
		t.Run(string(kind), func(t *testing.T) {
			r, base := usageReservation(t, kind)
			for _, tc := range []struct {
				name  string
				usage domain.AttemptOperationUsageV1
				want  bool
			}{
				{"unknown", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}, true},
				{"partial_measured_input", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: usageMetric(1)}, true},
				{"partial_measured_money", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: usageMetric(1)}, true},
				{"reported_input", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageMetric(1)}, true},
				{"reported_output_zero", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, OutputTokens: usageMetric(0)}, true},
				{"reported_equal", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageMetric(10), OutputTokens: usageMetric(5)}, true},
				{"reported_input_overrun", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageMetric(11)}, false},
				{"reported_output_overrun", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, OutputTokens: usageMetric(6)}, false},
				{"measured_input_overrun", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: usageMetric(11)}, false},
				{"measured_money_overrun", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: usageMetric(8)}, false},
				{"reported_forged_money", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageMetric(1), AmountMicrounits: usageMetric(0)}, false},
				{"unknown_forged_number", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1, InputTokens: usageMetric(0)}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					o := base.Clone()
					o.Usage = tc.usage
					if got := ownedObservationAllowsSuccessor(r, o); got != tc.want {
						t.Fatalf("completed %s usage eligibility=%t want=%t", kind, got, tc.want)
					}
				})
			}
			for _, name := range []string{"service_error", "unknown_outcome", "wrong_operation", "wrong_reservation", "response_overflow"} {
				t.Run(name, func(t *testing.T) {
					o := base.Clone()
					switch name {
					case "service_error":
						o.Reason = domain.AttemptOperationReasonServiceErrorV1
					case "unknown_outcome":
						o.Outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
						o.Reason = domain.AttemptOperationReasonTransportUnknownV1
						o.ResponseRef = ""
						o.ResponseDigest = ""
						o.ResponseBytes = 0
					case "wrong_operation":
						o.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("b", 64))
					case "wrong_reservation":
						o.ReservationDigest = domain.AttemptOperationReservationDigestV1(strings.Repeat("b", 64))
					case "response_overflow":
						o.ResponseBytes = r.Operation.ResultBytes + 1
					}
					if ownedObservationAllowsSuccessor(r, o) {
						t.Fatalf("%s admitted without exact completed durable response", name)
					}
				})
			}
		})
	}
}

func TestOwnedOperationUsageToolsKeepMeasuredMoneyAndAllMetricOverruns(t *testing.T) {
	for _, kind := range []domain.AttemptOperationKindV1{domain.AttemptOperationMCPToolV1, domain.AttemptOperationWebSearchV1} {
		t.Run(string(kind), func(t *testing.T) {
			r, base := usageReservation(t, kind)
			for _, tc := range []struct {
				name  string
				usage domain.AttemptOperationUsageV1
				want  bool
			}{
				{"measured_money", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: usageMetric(7)}, true},
				{"unknown", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}, false},
				{"reported", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageMetric(0)}, false},
				{"missing_money", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: usageMetric(0)}, false},
				{"money_overrun", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: usageMetric(8)}, false},
				{"input_overrun", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: usageMetric(1), AmountMicrounits: usageMetric(7)}, false},
				{"output_overrun", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, OutputTokens: usageMetric(1), AmountMicrounits: usageMetric(7)}, false},
				{"present_zero_tokens", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: usageMetric(0), OutputTokens: usageMetric(0), AmountMicrounits: usageMetric(7)}, true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					o := base.Clone()
					o.Usage = tc.usage
					if got := ownedObservationAllowsSuccessor(r, o); got != tc.want {
						t.Fatalf("completed %s measured money gate=%t want=%t", kind, got, tc.want)
					}
				})
			}
		})
	}
}

func TestOwnedOperationUsageNeverRefundsActualAccountingDebit(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	pd, err := f.parent.Digest()
	if err != nil {
		t.Fatal(err)
	}
	initial := ownedAttemptAccounting{Version: 1, ParentDigest: pd, SetupReservedAt: f.at, UpdatedAt: f.at}
	initial.Amounts[1] = *f.source.Spending[1].MaxAmountMicrounits
	op := domain.AttemptOperationV1{Kind: domain.AttemptOperationModelV1, Sequence: 1, Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 100, OutputTokens: 20, BodyBytes: 1024, ResponseBytes: 512, AmountMicrounits: *f.source.Spending[0].MaxAmountMicrounits, ActiveDuration: time.Second}}
	debited, err := debitOwnedOperation(initial, op, f.parent, f.source, f.at)
	if err != nil {
		t.Fatalf("original full debit: %v", err)
	}
	before, beforeDigest, err := accountingBytes(debited)
	if err != nil {
		t.Fatal(err)
	}
	for _, usage := range []domain.AttemptOperationUsageV1{{State: domain.AttemptOperationUsageUnknownV1}, {State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageMetric(1)}, {State: domain.AttemptOperationUsageObservedV1, InputTokens: usageMetric(0)}} {
		r, o := usageReservation(t, domain.AttemptOperationModelV1)
		o.Usage = usage
		if !ownedObservationAllowsSuccessor(r, o) {
			t.Fatal("valid known complete provider predecessor denied")
		}
		after, digest, err := accountingBytes(debited)
		if err != nil || digest != beforeDigest || !bytes.Equal(before, after) || debited.Aggregate != op.Debit || debited.Amounts[0] != op.Debit.AmountMicrounits {
			t.Fatal("usage gate refunded or re-estimated original full accounting debit")
		}
	}
	second := op.Clone()
	second.Sequence = 2
	next, err := debitOwnedOperation(debited, second, f.parent, f.source, f.at)
	if err != nil {
		t.Fatalf("second independently reserved full debit: %v", err)
	}
	if next.Aggregate.InputTokens != 200 || next.Aggregate.OutputTokens != 40 || next.Aggregate.Operations != 2 || next.Aggregate.BodyBytes != 2048 {
		t.Fatal("lower/unknown report replaced rather than accumulated full reservation")
	}
	var roundtrip ownedAttemptAccounting
	raw, _ := json.Marshal(next)
	if json.Unmarshal(raw, &roundtrip) != nil || !reflect.DeepEqual(next, roundtrip) {
		t.Fatal("accounting round trip changed full debits")
	}
}

func TestOwnedOperationUsageReportedAndUnknownStillDenyResidualDisposal(t *testing.T) {
	fixture := ownedResidualInventoryFixture(t)
	for _, usage := range []domain.AttemptOperationUsageV1{{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageMetric(1)}, {State: domain.AttemptOperationUsageUnknownV1}} {
		raw, err := json.Marshal(fixture)
		if err != nil {
			t.Fatal(err)
		}
		var candidate ports.OwnedOperationResidualInventoryV1
		if json.Unmarshal(raw, &candidate) != nil {
			t.Fatal("isolated inventory clone")
		}
		candidate.Operations[0].Observation.Usage = usage
		if candidate.Operations[0].Observation.ValidateForReservation(candidate.Operations[0].Reservation) != nil {
			t.Fatal("valid reported/unknown observation must remain recordable")
		}
		if _, err := candidate.IntentBytes(); err == nil {
			t.Fatal("continuation provenance silently qualified measured-billing disposal route")
		}
	}
}
