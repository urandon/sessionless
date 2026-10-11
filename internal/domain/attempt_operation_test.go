package domain_test

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func operationFixture() domain.AttemptOperationV1 {
	hash := strings.Repeat("a", 64)
	return domain.AttemptOperationV1{Version: 1, Scope: domain.AttemptOperationScopeV1{
		TenantID: "tenant-operation", SessionID: "session-operation", OwnerUserID: "owner-operation", RunID: "run-operation", AttemptID: "attempt-operation", LeaseID: "lease-operation", FenceToken: 1, PhysicalInvocationClaimID: "claim-operation",
		EffectReservationDigest: domain.AttemptEffectReservationDigestV1(hash), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(hash), HarnessBindingDigest: domain.HarnessBindingDigestV2(hash), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(hash),
	}, Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "model-1", RequestRef: "request-1", RequestDigest: hash, Deadline: time.Date(2026, 10, 10, 12, 0, 10, 0, time.UTC), CleanupDuration: 30 * time.Second,
		Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 100, OutputTokens: 20, BodyBytes: 1000, ResponseBytes: 1000, AmountMicrounits: 1, ActiveDuration: 10 * time.Second}, RequestBytes: 100, ResultBytes: 900, ContextBytes: 100, Currency: "USD", PriceRevision: "offline-price.v1", AdmissionCostCeilingDigest: domain.AdmissionCostCeilingDigestV2(hash)}
}

func operationReservationFixture(t *testing.T, op domain.AttemptOperationV1) domain.AttemptOperationReservationV1 {
	t.Helper()
	digest, err := op.Digest()
	if err != nil {
		t.Fatalf("operation digest: %v", err)
	}
	debit := op.Debit
	debit.Operations = op.Sequence
	if op.Sequence > 1 {
		debit.ModelCalls = op.Sequence - debit.ToolDispatches
	}
	r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: digest, ReservedAt: op.Deadline.Add(-op.Debit.ActiveDuration), AggregateDebit: debit}
	if err := r.Validate(); err != nil {
		t.Fatalf("reservation fixture: %v", err)
	}
	return r
}

func completedOperationFixture(t *testing.T, r domain.AttemptOperationReservationV1) domain.AttemptOperationObservationV1 {
	t.Helper()
	digest, err := r.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return domain.AttemptOperationObservationV1{Version: 1, OperationDigest: r.OperationDigest, ReservationDigest: digest, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: "response-1", ResponseDigest: strings.Repeat("b", 64), ResponseBytes: 100, ObservedAt: r.ReservedAt.Add(time.Second), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
}

func toolOperationFixture() domain.AttemptOperationV1 {
	v := operationFixture()
	v.Sequence = 2
	v.Kind = domain.AttemptOperationWebSearchV1
	v.PredecessorObservationDigest = domain.AttemptOperationObservationDigestV1(strings.Repeat("b", 64))
	v.CallID = "search-1"
	v.Debit.ModelCalls = 0
	v.Debit.InputTokens = 0
	v.Debit.OutputTokens = 0
	v.Debit.ToolDispatches = 1
	v.Debit.WebSearches = 1
	v.ContextBytes = 0
	v.ModelResponseDigest = strings.Repeat("b", 64)
	v.ArgumentsDigest = strings.Repeat("c", 64)
	v.QueryDigest = strings.Repeat("d", 64)
	v.ToolGrantDigest = strings.Repeat("4", 64)
	v.ConsentRequestDigest = strings.Repeat("e", 64)
	v.ConsentDecisionDigest = strings.Repeat("f", 64)
	v.ConsentDecisionRef = "decision-1"
	v.ExternalChargeEvidenceDigest = strings.Repeat("9", 64)
	return v
}

func TestAttemptOperationClosedKindsAndExactReceiptFields(t *testing.T) {
	t.Parallel()
	for _, v := range []domain.AttemptOperationV1{operationFixture(), toolOperationFixture()} {
		if err := v.Validate(); err != nil {
			t.Fatalf("valid %s operation: %v", v.Kind, err)
		}
	}
	for name, mutate := range map[string]func(*domain.AttemptOperationV1){
		"version": func(v *domain.AttemptOperationV1) { v.Version = 0 }, "sequence_zero": func(v *domain.AttemptOperationV1) { v.Sequence = 0 }, "sequence_overflow": func(v *domain.AttemptOperationV1) { v.Sequence = math.MaxUint64 }, "kind": func(v *domain.AttemptOperationV1) { v.Kind = "retry" }, "missing_claim": func(v *domain.AttemptOperationV1) { v.Scope.PhysicalInvocationClaimID = "" }, "fence": func(v *domain.AttemptOperationV1) { v.Scope.FenceToken = 0 }, "unknown_price": func(v *domain.AttemptOperationV1) { v.ExternalChargeEvidenceDigest = "" }, "zero_external_price": func(v *domain.AttemptOperationV1) { v.Debit.AmountMicrounits = 0 }, "missing_receipt": func(v *domain.AttemptOperationV1) { v.ConsentDecisionDigest = "" }, "missing_request": func(v *domain.AttemptOperationV1) { v.ConsentRequestDigest = "" }, "missing_locator": func(v *domain.AttemptOperationV1) { v.ConsentDecisionRef = "" }, "missing_query": func(v *domain.AttemptOperationV1) { v.QueryDigest = "" }, "multiple_calls": func(v *domain.AttemptOperationV1) { v.Debit.ToolDispatches = 2 }, "search_time": func(v *domain.AttemptOperationV1) { v.Debit.ActiveDuration = 16 * time.Second }, "search_response": func(v *domain.AttemptOperationV1) { v.Debit.ResponseBytes = 256<<10 + 1 }, "tool_argument": func(v *domain.AttemptOperationV1) { v.RequestBytes = 4<<10 + 1 }, "result_overflow": func(v *domain.AttemptOperationV1) { v.ResultBytes = math.MaxUint64 }, "missing_body_debit": func(v *domain.AttemptOperationV1) { v.Debit.BodyBytes = v.RequestBytes }, "tool_tokens": func(v *domain.AttemptOperationV1) { v.Debit.InputTokens = 1 }, "forged_mcp": func(v *domain.AttemptOperationV1) { v.Debit.MCPOutboundMessages = 1 }, "future_year": func(v *domain.AttemptOperationV1) { v.Deadline = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
	} {
		t.Run(name, func(t *testing.T) {
			v := toolOperationFixture()
			mutate(&v)
			if err := v.Validate(); err == nil {
				t.Fatalf("invalid operation %s accepted: %+v", name, v)
			}
		})
	}
	model := operationFixture()
	model.ConsentDecisionDigest = toolOperationFixture().ConsentDecisionDigest
	if err := model.Validate(); err == nil {
		t.Fatal("model accepted a search receipt")
	}
	compaction := operationFixture()
	compaction.Kind = domain.AttemptOperationCompactionV1
	compaction.Sequence = 2
	compaction.PredecessorObservationDigest = domain.AttemptOperationObservationDigestV1(strings.Repeat("b", 64))
	compaction.Debit.CompactionCalls = 1
	compaction.SummaryBytes = 100
	if err := compaction.Validate(); err != nil {
		t.Fatalf("compaction containing model debit: %v", err)
	}
	compaction.Debit.ModelCalls = 0
	if err := compaction.Validate(); err == nil {
		t.Fatal("compaction bypassed model-call ceiling")
	}
	mcp := toolOperationFixture()
	mcp.Kind = domain.AttemptOperationMCPToolV1
	mcp.Debit.WebSearches = 0
	mcp.QueryDigest = ""
	mcp.ConsentRequestDigest = ""
	mcp.ConsentDecisionDigest = ""
	mcp.ConsentDecisionRef = ""
	mcp.ToolGrantDigest = strings.Repeat("3", 64)
	mcp.Debit.MCPOutboundMessages = 3
	mcp.Debit.MCPCatalogPages = 1
	mcp.Debit.MCPInboundFrames = 5
	mcp.Debit.MCPInboundBytes = 100
	if err := mcp.Validate(); err != nil {
		t.Fatalf("bounded MCP debit: %v", err)
	}
}

func TestAttemptOperationInitialCompactionAndNoInitialTool(t *testing.T) {
	t.Parallel()
	first := operationFixture()
	first.Kind = domain.AttemptOperationCompactionV1
	first.Debit.CompactionCalls = 1
	first.SummaryBytes = 100
	if err := first.Validate(); err != nil {
		t.Fatalf("accepted pre-model initial compaction rejected: %v", err)
	}
	if err := first.Debit.ValidateForPolicy(agentLoopPolicyFixture()); err != nil {
		t.Fatalf("initial compaction must debit model and compaction ceilings: %v", err)
	}
	for _, kind := range []domain.AttemptOperationKindV1{domain.AttemptOperationMCPToolV1, domain.AttemptOperationWebSearchV1} {
		t.Run(string(kind), func(t *testing.T) {
			tool := toolOperationFixture()
			tool.Kind, tool.Sequence, tool.PredecessorObservationDigest = kind, 1, ""
			if err := tool.Validate(); err == nil {
				t.Fatal("initial tool without durable model predecessor accepted")
			}
		})
	}
}

func TestAttemptOperationDebitEveryDimensionAndOverflow(t *testing.T) {
	t.Parallel()
	policy := agentLoopPolicyFixture()
	base := operationFixture().Debit
	if err := base.ValidateForPolicy(policy); err != nil {
		t.Fatal(err)
	}
	fields := reflect.TypeOf(base)
	for index := range fields.NumField() {
		name := fields.Field(index).Name
		t.Run(name, func(t *testing.T) {
			v := base
			field := reflect.ValueOf(&v).Elem().Field(index)
			if field.Kind() == reflect.Uint64 {
				field.SetUint(math.MaxUint64)
			} else {
				field.SetInt(math.MaxInt64)
			}
			if err := v.ValidateForPolicy(policy); err == nil {
				t.Fatalf("aggregate %s unbounded", name)
			}
			other := domain.AttemptOperationDebitV1{}
			increment := reflect.ValueOf(&other).Elem().Field(index)
			if increment.Kind() == reflect.Uint64 {
				increment.SetUint(1)
			} else {
				increment.SetInt(1)
			}
			if _, err := v.Add(other); err == nil {
				t.Fatalf("overflow %s accepted", name)
			}
			if !reflect.DeepEqual(base, operationFixture().Debit) {
				t.Fatal("Add mutated source debit")
			}
		})
	}
	unknown := policy.Clone()
	unknown.AdmissionCostCeiling.ProviderPriceState = domain.ProviderPriceUnknownV1
	unknown.AdmissionCostCeiling.MaxProviderAmountMicrounits = nil
	unknown.AdmissionCostCeiling.MaxTotalAmountMicrounits = nil
	if err := base.ValidateForPolicy(unknown); err == nil {
		t.Fatal("unknown price became a zero reservation")
	}
	neg := domain.AttemptOperationDebitV1{ActiveDuration: -1}
	if _, err := base.Add(neg); err == nil {
		t.Fatal("negative duration refunded a reservation")
	}
	one := domain.AttemptOperationDebitV1{BodyBytes: 100}
	combined, err := base.Add(one)
	if err != nil || combined.BodyBytes != base.BodyBytes+100 {
		t.Fatalf("consent body accounting got=%+v err=%v", combined, err)
	}
}

func TestAttemptOperationObservationUnknownBlocksContinuationAndForgery(t *testing.T) {
	t.Parallel()
	r := operationReservationFixture(t, operationFixture())
	o := completedOperationFixture(t, r)
	if err := o.ValidateForReservation(r); err != nil {
		t.Fatal(err)
	}
	next := toolOperationFixture()
	next.Scope = r.Operation.Scope
	next.ModelResponseDigest = o.ResponseDigest
	next.PredecessorObservationDigest, _ = o.Digest()
	if err := next.ValidatePredecessor(r, o); err != nil {
		t.Fatalf("durable known model->tool: %v", err)
	}
	for _, outcome := range []domain.AttemptOperationOutcomeV1{domain.AttemptOperationRejectedBeforeSendV1, domain.AttemptOperationAcceptedOutcomeUnknownV1} {
		t.Run(string(outcome), func(t *testing.T) {
			unknown := o.Clone()
			unknown.Outcome = outcome
			unknown.Reason = domain.AttemptOperationReasonTransportUnknownV1
			unknown.ResponseRef = ""
			unknown.ResponseDigest = ""
			unknown.ResponseBytes = 0
			if err := unknown.ValidateForReservation(r); err != nil {
				t.Fatalf("authentic unknown/rejected evidence: %v", err)
			}
			next.PredecessorObservationDigest, _ = unknown.Digest()
			if err := next.ValidatePredecessor(r, unknown); err == nil {
				t.Fatal("unknown/rejected operation authorized continuation")
			}
			unknown.ResponseDigest = o.ResponseDigest
			if err := unknown.Validate(); err == nil {
				t.Fatal("unknown fabricated service response")
			}
		})
	}
	for name, mutate := range map[string]func(*domain.AttemptOperationObservationV1){"operation": func(v *domain.AttemptOperationObservationV1) {
		v.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("f", 64))
	}, "reservation": func(v *domain.AttemptOperationObservationV1) {
		v.ReservationDigest = domain.AttemptOperationReservationDigestV1(strings.Repeat("f", 64))
	}, "before_reserve": func(v *domain.AttemptOperationObservationV1) { v.ObservedAt = r.ReservedAt.Add(-1) }, "oversized": func(v *domain.AttemptOperationObservationV1) { v.ResponseBytes = r.Operation.ResultBytes + 1 }, "private_error": func(v *domain.AttemptOperationObservationV1) { v.Reason = "credential at /private/secret" }, "unknown_as_zero": func(v *domain.AttemptOperationObservationV1) { zero := uint64(0); v.Usage.AmountMicrounits = &zero }} {
		t.Run(name, func(t *testing.T) {
			v := o.Clone()
			mutate(&v)
			if err := v.ValidateForReservation(r); err == nil {
				t.Fatalf("forged observation %s accepted", name)
			}
		})
	}
	o.ObservedAt = r.Operation.Deadline.Add(time.Minute)
	if err := o.ValidateForReservation(r); err != nil {
		t.Fatalf("late authentic observation should survive expiry: %v", err)
	}
	input, output, amount := r.Operation.Debit.InputTokens+1, uint64(0), uint64(1)
	o.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: &input, OutputTokens: &output, AmountMicrounits: &amount}
	if err := o.ValidateForReservation(r); err != nil {
		t.Fatalf("overrun evidence must be retained: %v", err)
	}
	next.PredecessorObservationDigest, _ = o.Digest()
	if err := next.ValidatePredecessor(r, o); err == nil {
		t.Fatal("observed overrun authorized another effect")
	}
}

func TestAttemptOperationPartialObservedUsageRetainedAndOverrunStops(t *testing.T) {
	t.Parallel()
	r := operationReservationFixture(t, operationFixture())
	for _, metric := range []struct {
		name    string
		ceiling uint64
		set     func(*domain.AttemptOperationUsageV1, *uint64)
	}{
		{"input", r.Operation.Debit.InputTokens, func(v *domain.AttemptOperationUsageV1, n *uint64) { v.InputTokens = n }},
		{"output", r.Operation.Debit.OutputTokens, func(v *domain.AttemptOperationUsageV1, n *uint64) { v.OutputTokens = n }},
		{"amount", r.Operation.Debit.AmountMicrounits, func(v *domain.AttemptOperationUsageV1, n *uint64) { v.AmountMicrounits = n }},
	} {
		t.Run(metric.name, func(t *testing.T) {
			for _, overrun := range []bool{false, true} {
				o := completedOperationFixture(t, r)
				o.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1}
				measured := metric.ceiling
				if overrun {
					measured++
				}
				metric.set(&o.Usage, &measured)
				if err := o.ValidateForReservation(r); err != nil {
					t.Fatalf("truthful partial %s usage rejected (overrun=%v): %v", metric.name, overrun, err)
				}
				next := toolOperationFixture()
				next.Scope, next.ModelResponseDigest = r.Operation.Scope, o.ResponseDigest
				next.PredecessorObservationDigest, _ = o.Digest()
				if err := next.ValidatePredecessor(r, o); (err != nil) != overrun {
					t.Fatalf("partial %s overrun=%v continuation error=%v", metric.name, overrun, err)
				}
			}
		})
	}
	if err := (domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1}).Validate(); err == nil {
		t.Fatal("empty observed usage invented measurements")
	}
}

func TestAttemptOperationDigestIsolationAndEveryField(t *testing.T) {
	t.Parallel()
	base := toolOperationFixture()
	original, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	for index := range reflect.TypeOf(base).NumField() {
		name := reflect.TypeOf(base).Field(index).Name
		t.Run(name, func(t *testing.T) {
			v := base.Clone()
			f := reflect.ValueOf(&v).Elem().Field(index)
			switch f.Kind() {
			case reflect.String:
				f.SetString(f.String() + "x")
			case reflect.Uint32, reflect.Uint64:
				f.SetUint(f.Uint() + 1)
			case reflect.Int64:
				f.SetInt(f.Int() - 1)
			case reflect.Struct:
				if name == "Deadline" {
					v.Deadline = v.Deadline.Add(-time.Nanosecond)
				} else if name == "Scope" {
					v.Scope.PhysicalInvocationClaimID = "claim-other"
				} else {
					v.Debit.AmountMicrounits++
				}
			default:
				t.Fatalf("uncovered field %s", name)
			}
			digest, err := v.Digest()
			if err == nil && digest == original {
				t.Fatalf("field %s was not bound", name)
			}
		})
	}
	zone := time.FixedZone("fixture", 3*3600)
	shifted := base.Clone()
	shifted.Deadline = shifted.Deadline.In(zone)
	same, err := shifted.Digest()
	if err != nil || same != original {
		t.Fatalf("same instant different digest got=%s want=%s err=%v", same, original, err)
	}
	r := operationReservationFixture(t, base)
	o := completedOperationFixture(t, r)
	input, output, amount := uint64(1), uint64(2), uint64(3)
	o.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: &input, OutputTokens: &output, AmountMicrounits: &amount}
	digest, _ := o.Digest()
	clone := o.Clone()
	*clone.Usage.AmountMicrounits = 9
	unchanged, _ := o.Digest()
	if unchanged != digest {
		t.Fatal("usage clone mutated immutable observation")
	}
	reservationDigest, _ := r.Digest()
	observationDigest, _ := o.Digest()
	if string(original) == string(reservationDigest) || string(reservationDigest) == string(observationDigest) {
		t.Fatal("digest domain separation collapsed")
	}
}

func TestAttemptOperationExactParentAndAllDeadlines(t *testing.T) {
	t.Parallel()
	a, at := validServerlessInvocationAuthority(t)
	b := ownedAgentBindingFixture(t)
	a.Lease.TenantID = b.TenantID
	a.Lease.RunID = b.RunID
	a.Lease.AttemptID = b.AttemptID
	a.AdmissionCostCeiling.ProviderPriceState = domain.ProviderPriceKnownV1
	amount := uint64(20)
	a.AdmissionCostCeiling.MaxProviderAmountMicrounits = &amount
	total := *a.AdmissionCostCeiling.MaxSubstrateAmountMicrounits + amount
	a.AdmissionCostCeiling.MaxTotalAmountMicrounits = &total
	a.AdmissionCostCeiling = withOwnedServiceCostFixture(t, a.AdmissionCostCeiling, *b.AgentLoopPolicy)
	costDigest, err := a.AdmissionCostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	a.SubstrateBinding.AdmissionCostCeilingDigest = costDigest
	substrateDigest, err := a.SubstrateBinding.Digest()
	if err != nil {
		t.Fatal(err)
	}
	a.ExecutionPlacementV2.SubstrateBindingDigest = string(substrateDigest)
	placementDigest, err := domain.ExecutionPlacementDigest(a.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlacementDigest = string(placementDigest)
	b.AgentLoopPolicy.AdmissionCostCeiling = a.AdmissionCostCeiling.Clone()
	b.AgentLoopPolicy.IssuedAt = at
	b.AgentLoopPolicy.ExpiresAt = at.Add(120 * time.Second)
	b.EvidenceExpiresAt = &b.AgentLoopPolicy.ExpiresAt
	a.HarnessBinding = b
	a.InvocationDeadline = at.Add(120 * time.Second)
	if err := a.ValidateAt(at); err != nil {
		t.Fatalf("owned parent authority fixture: %v", err)
	}
	parent, err := domain.BuildAttemptEffectReservationV1(a, "claim-operation", nil, at)
	if err != nil {
		t.Fatal(err)
	}
	op := operationFixture()
	parentDigest, _ := parent.DigestForAuthority(a)
	policyDigest, _ := b.AgentLoopPolicy.Digest()
	op.Scope = domain.AttemptOperationScopeV1{TenantID: parent.TenantID, SessionID: "session-operation", OwnerUserID: b.OwnerUserID, RunID: parent.RunID, AttemptID: parent.AttemptID, LeaseID: parent.LeaseID, FenceToken: parent.FenceToken, PhysicalInvocationClaimID: parent.PhysicalInvocationClaimID, EffectReservationDigest: parentDigest, InvocationAuthorityDigest: parent.InvocationAuthorityDigest, HarnessBindingDigest: parent.HarnessBindingDigest, AgentLoopPolicyDigest: policyDigest}
	op.AdmissionCostCeilingDigest = costDigest
	op.PriceRevision = a.AdmissionCostCeiling.PriceRevision
	op.Deadline = at.Add(10 * time.Second)
	if err := op.ValidateForParent(a, parent); err != nil {
		t.Fatalf("exact parent operation: %v", err)
	}
	initialCompaction := op.Clone()
	initialCompaction.Kind = domain.AttemptOperationCompactionV1
	initialCompaction.Debit.CompactionCalls = 1
	initialCompaction.SummaryBytes = 100
	if err := initialCompaction.ValidateForParent(a, parent); err != nil {
		t.Fatalf("initial compaction must retain exact parent and model budget: %v", err)
	}
	compactionReservation := operationReservationFixture(t, initialCompaction)
	if err := compactionReservation.ValidateForParent(a, parent); err != nil {
		t.Fatalf("initial compaction aggregate reservation: %v", err)
	}
	compactionReservation.AggregateDebit.CompactionCalls = 0
	if err := compactionReservation.ValidateForParent(a, parent); err == nil {
		t.Fatal("initial compaction omitted its aggregate compaction debit")
	}
	for name, mutate := range map[string]func(*domain.AttemptOperationV1){"foreign_tenant": func(v *domain.AttemptOperationV1) { v.Scope.TenantID = "foreign" }, "foreign_owner": func(v *domain.AttemptOperationV1) { v.Scope.OwnerUserID = "foreign" }, "new_claim": func(v *domain.AttemptOperationV1) { v.Scope.PhysicalInvocationClaimID = "new-claim" }, "new_fence": func(v *domain.AttemptOperationV1) { v.Scope.FenceToken++ }, "new_policy": func(v *domain.AttemptOperationV1) {
		v.Scope.AgentLoopPolicyDigest = domain.AgentLoopPolicyDigestV1(strings.Repeat("9", 64))
	}, "new_price": func(v *domain.AttemptOperationV1) { v.PriceRevision = "another-price" }, "currency": func(v *domain.AttemptOperationV1) { v.Currency = "EUR" }, "past_parent": func(v *domain.AttemptOperationV1) { v.Deadline = parent.ReservedAt }, "extend_deadline": func(v *domain.AttemptOperationV1) { v.Deadline = a.InvocationDeadline.Add(1) }} {
		t.Run(name, func(t *testing.T) {
			v := op.Clone()
			mutate(&v)
			if err := v.ValidateForParent(a, parent); err == nil {
				t.Fatalf("substituted %s authority accepted", name)
			}
		})
	}
	r := operationReservationFixture(t, op)
	if err := r.ValidateForParent(a, parent); err != nil {
		t.Fatalf("exact cumulative reservation: %v", err)
	}
	r.AggregateDebit.InputTokens = 0
	if err := r.ValidateForParent(a, parent); err == nil {
		t.Fatal("cumulative reservation omitted input debit")
	}
	// Structural historical correlation does not make the parent live: the
	// production resource-time resolver must reject this now-expired authority.
	if err := op.ValidateForParent(a, parent); err != nil {
		t.Fatal(err)
	}
	if err := a.ValidateAt(a.InvocationDeadline); err == nil {
		t.Fatal("expired authority accepted")
	}
}
