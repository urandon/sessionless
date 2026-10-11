//go:build ydbintegration

package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessegress"
)

// Complete synthetic bodies and grants are fixed before authenticated profile
// publication. No operation/proxy row or claimed current authority is seeded.
func nativeSequenceGraph(t *testing.T) (nativeOwnedParentGraph, *nativeOperationBodyKeeper, serverlessegress.PolicyV1) {
	return nativeSequenceGraphPrice(t, false)
}
func nativeSequenceGraphPrice(t *testing.T, knownFree bool) (nativeOwnedParentGraph, *nativeOperationBodyKeeper, serverlessegress.PolicyV1) {
	return nativeSequenceGraphPricing(t, knownFree, false)
}
func nativeSequenceGraphPricing(t *testing.T, knownFree, paidProvider bool) (nativeOwnedParentGraph, *nativeOperationBodyKeeper, serverlessegress.PolicyV1) {
	return nativeSequenceGraphPricingLimit(t, knownFree, paidProvider, 20)
}
func nativeSequenceGraphPricingLimit(t *testing.T, knownFree, paidProvider bool, providerCeiling uint64) (nativeOwnedParentGraph, *nativeOperationBodyKeeper, serverlessegress.PolicyV1) {
	t.Helper()
	managed := newNativeManagedAdmissionGraph(t)
	f := managed.f
	if paidProvider {
		installNativeOwnedProviderPrice(t, f, true)
		if providerCeiling != 4 && providerCeiling != 20 {
			t.Fatal("fixed synthetic provider ceiling control only")
		}
		*f.profile.CostCeiling.MaxProviderAmountMicrounits = providerCeiling
		*f.profile.CostCeiling.MaxTotalAmountMicrounits -= 20 - providerCeiling
		*f.spending[0].MaxAmountMicrounits = providerCeiling
		bindReaderSpendingSource(t, &f.spending[0])
	}
	cost := f.profile.CostCeiling.MCPToolCost
	if knownFree {
		old := *cost.MaxAmountMicrounits
		zero := uint64(0)
		cost.PriceState = domain.ExternalServicePriceKnownFreeV1
		cost.MaxAmountMicrounits = &zero
		*f.profile.CostCeiling.MaxTotalAmountMicrounits -= old
		f.spending[2].PriceState = domain.ExternalServicePriceKnownFreeV1
		f.spending[2].MaxAmountMicrounits = new(uint64)
	}
	grant := ports.OwnedFixtureToolGrantV1{Version: 1, TenantID: cost.TenantID, OwnerUserID: cost.OwnerUserID, BillingOwnerUserID: cost.BillingOwnerUserID, Kind: cost.Kind, ResourceID: cost.ResourceID, ResourceRevision: cost.ResourceRevision, RevocationGeneration: cost.RevocationGeneration, ProfileID: cost.ProfileID, ProfileRevision: cost.ProfileRevision, ConsentMode: "none", ConsentRevision: 1, Endpoint: "fixture://owned/mcp", MaxArgumentBytes: 4096, MaxResultBytes: 65536, ExpiresAt: cost.PriceExpiresAt.UTC().Truncate(time.Microsecond)}
	body, err := json.Marshal(grant)
	if err != nil || grant.Validate() != nil {
		t.Fatal("actual fixture grant", err)
	}
	digest := ports.OwnedFixtureBytesDigestV1(body)
	f.profile.MCPToolGrantDigest = digest
	f.profile.CostCeiling.MCPToolCost.ScopedGrantDigest = digest
	f.profile.Substrate.AdmissionCostCeilingDigest, err = f.profile.CostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	f.spending[2].ScopedGrantDigest = digest
	bindReaderSpendingSource(t, &f.spending[2])
	policy, err := serverlessegress.OwnedHTTPSFixturePolicyV1(f.profile)
	if err != nil {
		t.Fatal(err)
	}
	f.profile.Substrate.EgressPolicyDigest, err = policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	g := nativeOwnedParentGraphFromFinal(t, nativeOwnedFinalGraphFrom(t, managed, false))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, table := range []string{"attempt_operation_calls_v1", "attempt_operations_v1", "attempt_operation_proxies_v1"} {
			if _, err := g.f.store.db.ExecContext(ctx, "DELETE FROM "+table+" WHERE tenant_id=$1", g.run.TenantID); err != nil {
				t.Error(err)
			}
		}
	})
	nativeOperationSetup(t, g)
	publication := nativeOwnedAllocationPublication(t, g)
	if _, err := g.f.store.publishBoundedOwnedAllocation(g.ctx, publication); err != nil {
		t.Fatal("actual original allocation", err)
	}
	parent, _ := g.persistedParent(t)
	ref, _ := ports.OwnedFixtureGrantReferenceV1(digest)
	keeper := &nativeOperationBodyKeeper{scope: nativeOwnedOperationScope(t, parent), bodies: map[string][]byte{ref: bytes.Clone(body)}}
	g.f.store.attemptOperationBodies = keeper
	return g, keeper, policy
}

func TestOwnedOperationYDBActualSequentialModelMCPModelCompaction(t *testing.T) {
	for _, knownFree := range []bool{false, true} {
		t.Run(fmt.Sprintf("known_free_%t", knownFree), func(t *testing.T) { testNativeSequentialOperation(t, knownFree) })
	}
}
func testNativeSequentialOperation(t *testing.T, knownFree bool) {
	testNativeSequentialOperationCase(t, knownFree, "")
}

func TestOwnedOperationYDBMCPSemanticDuplicateRegeneratedID(t *testing.T) {
	testNativeSequentialOperationCase(t, true, "same")
}

func TestOwnedOperationYDBMCPSemanticDistinctArguments(t *testing.T) {
	testNativeSequentialOperationCase(t, true, "different")
}

func testNativeSequentialOperationCase(t *testing.T, knownFree bool, semanticCase string) {
	testNativeSequentialOperationPricing(t, knownFree, semanticCase, false)
}
func TestOwnedOperationYDBPaidProviderSequentialModelMCPModelCompaction(t *testing.T) {
	testNativeSequentialOperationPricing(t, false, "", true)
}
func TestOwnedOperationYDBPaidProviderPriceCustodyFaultsNoSend(t *testing.T) {
	testNativeSequentialOperationPricing(t, false, "price-controls", true)
}
func TestOwnedOperationYDBPaidProviderOriginalComponentExhaustion(t *testing.T) {
	testNativeSequentialOperationPricing(t, false, "provider-ceiling", true)
}
func testNativeSequentialOperationPricing(t *testing.T, knownFree bool, semanticCase string, paidProvider bool) {
	t.Helper()
	ceiling := uint64(20)
	if semanticCase == "provider-ceiling" {
		ceiling = 4
	}
	g, keeper, policy := nativeSequenceGraphPricingLimit(t, knownFree, paidProvider, ceiling)
	parent, _ := g.persistedParent(t)
	clock := nativeOperationClock{g.ctx, g.f.store}
	allocation, err := func() (domain.RegisteredAllocationV1, error) {
		var value domain.RegisteredAllocationV1
		err := g.f.store.ownedAuthorityTx(g.ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
			_, p, s, burn, e := g.f.store.operationParent(ctx, q, keeper.scope)
			if e != nil {
				return e
			}
			a, e := readOwnedAllocationSnapshots(ctx, q, p, s, burn)
			if e != nil || a == nil {
				return domain.ErrMembershipDenied
			}
			value = a.original.Clone()
			return nil
		})
		return value, err
	}()
	if err != nil {
		t.Fatal(err)
	}
	requests := [][]byte{[]byte(`{"kind":"model","input":"fixture"}`), []byte(`{"operation":"fixture"}`), nil, nil}
	responses := [][]byte{[]byte(`{"version":1,"kind":"model_response","text":"","calls":[{"call_id":"mcp-call","kind":"mcp_tool","arguments":{"operation":"fixture"}}]}`), []byte(`{"version":1,"kind":"tool_result","text":"fixture result"}`), []byte(`{"version":1,"kind":"model_response","text":"done","calls":[]}`), []byte(`{"version":1,"kind":"model_response","text":"summary","calls":[]}`)}
	kinds := []domain.AttemptOperationKindV1{domain.AttemptOperationModelV1, domain.AttemptOperationMCPToolV1, domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1}
	if semanticCase == "same" || semanticCase == "different" {
		requests[3] = bytes.Clone(requests[1])
		if semanticCase == "different" {
			requests[3] = []byte(`{"operation":"fixture","value":"different"}`)
		}
		responses[2] = []byte(fmt.Sprintf(`{"version":1,"kind":"model_response","text":"","calls":[{"call_id":"regenerated-call-id","kind":"mcp_tool","arguments":%s}]}`, requests[3]))
		responses[3] = []byte(`{"version":1,"kind":"tool_result","text":"different bounded result"}`)
		kinds[3] = domain.AttemptOperationMCPToolV1
	}
	var previous domain.AttemptOperationObservationV1
	for index, kind := range kinds {
		sequence := uint64(index + 1)
		if index >= 2 && kind != domain.AttemptOperationMCPToolV1 {
			requests[index] = []byte(fmt.Sprintf(`{"kind":%q,"input":"fixture","predecessor":%s}`, kind, responses[index-1]))
		}
		requestRef := fmt.Sprintf("sequence-request-%d", sequence)
		keeper.mu.Lock()
		keeper.bodies[requestRef] = bytes.Clone(requests[index])
		keeper.mu.Unlock()
		fixture, err := serverlessegress.NewOwnedHTTPSFixtureForOperationV1(g.ctx, g.profileReceipt.Profile, clock, responses[index], sequence, kind)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(fixture.Close)
		proxy := fixture.Proxy()
		if _, err := proxy.Preflight(g.ctx, policy); err != nil {
			t.Fatal(err)
		}
		proof, err := proxy.ObserveOwnedProxy(allocation, g.final.Source)
		if err != nil {
			t.Fatal(err)
		}
		g.f.store.ownedProxyPublicationVerifier = proxy
		registered, replay, err := g.f.store.PublishOwnedProxy(g.ctx, g.request, proof)
		if err != nil || replay {
			t.Fatalf("sequence%d authentic proxy publish: %v", sequence, err)
		}
		cost := parent.Authority.AdmissionCostCeiling
		cd, _ := cost.Digest()
		deadline := clock.Now().Add(10 * time.Second)
		if registered.Window.ExpiresAt.Before(deadline) {
			deadline = registered.Window.ExpiresAt
		}
		op := domain.AttemptOperationV1{Version: 1, Scope: keeper.scope, Sequence: sequence, Kind: kind, CallID: fmt.Sprintf("model-%d", sequence), RequestRef: requestRef, RequestDigest: operationBodyDigest(requests[index]), Deadline: deadline, CleanupDuration: parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxCleanupDuration, RequestBytes: uint64(len(requests[index])), ResultBytes: 512, ContextBytes: 1, Currency: cost.Currency, PriceRevision: cost.PriceRevision, AdmissionCostCeilingDigest: cd, Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 1, OutputTokens: 1, BodyBytes: uint64(len(requests[index])) + 512, ResponseBytes: 512, AmountMicrounits: *g.final.Source.Spending[0].MaxAmountMicrounits, ActiveDuration: 10 * time.Second}}
		if paidProvider {
			op.Debit.AmountMicrounits = 2
		}
		if index > 0 {
			op.PredecessorObservationDigest, _ = previous.Digest()
		}
		if kind == domain.AttemptOperationMCPToolV1 {
			op.CallID = "mcp-call"
			if sequence == 4 {
				op.CallID = "regenerated-call-id"
			}
			op.ContextBytes = 0
			op.ModelResponseDigest = previous.ResponseDigest
			op.ArgumentsDigest = op.RequestDigest
			op.ToolGrantDigest = cost.MCPToolCost.ScopedGrantDigest
			op.ExternalChargeEvidenceDigest = cost.MCPToolCost.PriceEvidenceDigest
			op.Debit.ModelCalls, op.Debit.InputTokens, op.Debit.OutputTokens = 0, 0, 0
			op.Debit.ToolDispatches = 1
			op.Debit.MCPOutboundMessages = 1
			op.Debit.MCPInboundFrames = 1
			op.Debit.MCPInboundBytes = 512
			op.Debit.AmountMicrounits = *g.final.Source.Spending[2].MaxAmountMicrounits
		}
		if kind == domain.AttemptOperationCompactionV1 {
			op.Debit.CompactionCalls = 1
			op.SummaryBytes = 512
		}
		if sequence == 1 && semanticCase == "price-controls" {
			original, before, _ := nativeAccountingBytes(t, g)
			for _, amount := range []uint64{0, 1, 20} {
				changed := op.Clone()
				changed.Debit.AmountMicrounits = amount
				denied, err := g.f.store.ReserveAttemptOperation(g.ctx, ports.ReserveAttemptOperationRequestV1{Operation: changed})
				current, after, _ := nativeAccountingBytes(t, g)
				if err == nil || denied.Capability != nil || fixture.Requests() != 0 || after != before || current.Amounts != original.Amounts || current.Aggregate.Operations != 0 {
					t.Fatalf("caller zero/underquote/full-envelope%d changed original debit or sent: err=%v cap=%t sends=%d unchanged=%t", amount, err, denied.Capability != nil, fixture.Requests(), before == after)
				}
				g.requireCount(t, "attempt_operations_v1", 0)
				g.requireCount(t, "attempt_operation_calls_v1", 0)
			}
		}
		var originalAccounting string
		if sequence == 4 && (semanticCase == "same" || semanticCase == "provider-ceiling") {
			_, originalAccounting, _ = nativeAccountingBytes(t, g)
		}
		reserved, err := g.f.store.ReserveAttemptOperation(g.ctx, ports.ReserveAttemptOperationRequestV1{Operation: op})
		if sequence == 4 && semanticCase == "provider-ceiling" {
			accounting, currentAccounting, _ := nativeAccountingBytes(t, g)
			if err == nil || reserved.Capability != nil || fixture.Requests() != 0 || currentAccounting != originalAccounting || accounting.Aggregate.Operations != 3 || accounting.Amounts[0] != 4 || accounting.Aggregate.AmountMicrounits != 34 || accounting.Aggregate.AmountMicrounits >= g.final.Source.MaxTotalAmountMicrounits {
				t.Fatalf("independent original provider4 exhaustion must deny compaction2 despite aggregate room: err=%v cap=%t sends=%d provider=%d aggregate=%d unchanged=%t", err, reserved.Capability != nil, fixture.Requests(), accounting.Amounts[0], accounting.Aggregate.AmountMicrounits, currentAccounting == originalAccounting)
			}
			g.requireCount(t, "attempt_operations_v1", 3)
			g.requireCount(t, "attempt_operation_calls_v1", 3)
			return
		}
		if sequence == 4 && semanticCase == "same" {
			accounting, currentAccounting, _ := nativeAccountingBytes(t, g)
			if err == nil || reserved.Capability != nil || fixture.Requests() != 0 || currentAccounting != originalAccounting || accounting.Aggregate.Operations != 3 || accounting.Aggregate.ToolDispatches != 1 || accounting.Amounts[2] != 0 {
				t.Fatalf("regenerated ID must not resend/redebit known-free same args: err=%v cap=%t sends=%d operations=%d tools=%d amount=%d unchanged=%t", err, reserved.Capability != nil, fixture.Requests(), accounting.Aggregate.Operations, accounting.Aggregate.ToolDispatches, accounting.Amounts[2], currentAccounting == originalAccounting)
			}
			g.requireCount(t, "attempt_operations_v1", 3)
			g.requireCount(t, "attempt_operation_calls_v1", 3)
			return // genuine authenticated model3 cannot make this prior effect repeatable
		}
		if err != nil || reserved.Capability == nil {
			t.Fatalf("sequence%d sameTx complete debit reserve: %v", sequence, err)
		}
		if sequence == 1 && semanticCase == "price-controls" {
			custody := g.f.store.ownedProviderPriceCustody.(*nativeOwnedProviderPriceCustody)
			custody.mu.Lock()
			originalBody := bytes.Clone(custody.body)
			changed, err := decodeOwnedProviderPrice(custody.body)
			if err != nil {
				custody.mu.Unlock()
				t.Fatal(err)
			}
			changed.InputMicrounitsPerMTokens++
			custody.body, err = json.Marshal(changed)
			custody.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			_, before, _ := nativeAccountingBytes(t, g)
			_, invokeErr := proxy.InvokeOwnedOperation(g.ctx, registered, *reserved.Capability, g.f.store, requests[index])
			custody.mu.Lock()
			clear(custody.body)
			custody.body = originalBody
			custody.mu.Unlock()
			_, replayErr := proxy.InvokeOwnedOperation(g.ctx, registered, *reserved.Capability, g.f.store, requests[index])
			accounting, after, _ := nativeAccountingBytes(t, g)
			if invokeErr == nil || replayErr == nil || fixture.Requests() != 0 || before != after || accounting.Amounts[0] != 2 || accounting.Aggregate.Operations != 1 {
				t.Fatalf("changed original rate custody must deny physicalsend, never refund/retry: err=%v replay=%v sends=%d unchanged=%t", invokeErr, replayErr, fixture.Requests(), before == after)
			}
			g.requireCount(t, "attempt_operations_v1", 1)
			g.requireCount(t, "attempt_operation_calls_v1", 1)
			return // accepted reservation stays unresolved; no invented response/usage
		}
		result, err := proxy.InvokeOwnedOperation(g.ctx, registered, *reserved.Capability, g.f.store, requests[index])
		if err != nil || fixture.Requests() != 1 || !bytes.Equal(result.Response, responses[index]) {
			t.Fatalf("sequence%d actual one-use response: %v", sequence, err)
		}
		// The local synthetic protocol fixes one measured input/output unit for a
		// model and its independently admitted amount; no live provider usage claim.
		responseRef := fmt.Sprintf("sequence-response-%d", sequence)
		keeper.mu.Lock()
		keeper.bodies[responseRef] = bytes.Clone(result.Response)
		keeper.mu.Unlock()
		rd, _ := reserved.Reservation.Digest()
		amount := op.Debit.AmountMicrounits
		usage := domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: &amount}
		if kind != domain.AttemptOperationMCPToolV1 {
			one := uint64(1)
			usage.InputTokens = &one
			usage.OutputTokens = &one
		}
		previous = domain.AttemptOperationObservationV1{Version: 1, OperationDigest: reserved.Reservation.OperationDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: responseRef, ResponseDigest: operationBodyDigest(result.Response), ResponseBytes: uint64(len(result.Response)), ObservedAt: result.ObservedAt, Usage: usage}
		authenticated, err := g.f.store.attemptOperationIssuer.AuthenticateAttemptOperationObservation(reserved.ObservationGrant, previous)
		if err != nil {
			t.Fatal(err)
		}
		if err := g.f.store.RecordAttemptOperationObservation(g.ctx, ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: reserved.ObservationGrant, Observation: authenticated}); err != nil {
			t.Fatalf("sequence%d immutable actual response custody: %v", sequence, err)
		}
		if _, err := proxy.InvokeOwnedOperation(g.ctx, registered, *reserved.Capability, g.f.store, requests[index]); err == nil || fixture.Requests() != 1 {
			t.Fatal("physical resend")
		}
	}
	g.requireCount(t, "attempt_operations_v1", 4)
	g.requireCount(t, "attempt_operation_calls_v1", 4)
	g.requireCount(t, "attempt_operation_proxies_v1", 3)
	accounting, _, _ := nativeAccountingBytes(t, g)
	models, compactions, tools := uint64(3), uint64(1), uint64(1)
	if semanticCase != "" {
		models, compactions, tools = 2, 0, 2
	}
	if accounting.Aggregate.Operations != 4 || accounting.Aggregate.ModelCalls != models || accounting.Aggregate.CompactionCalls != compactions || accounting.Aggregate.ToolDispatches != tools || accounting.Amounts[2] != *g.final.Source.Spending[2].MaxAmountMicrounits {
		t.Fatal("connected sequence omitted exact component/aggregate debit")
	}
	if paidProvider && (accounting.Amounts[0] != 6 || accounting.Aggregate.AmountMicrounits != 6+*g.final.Source.Spending[2].MaxAmountMicrounits) {
		t.Fatal("paid model+model+compaction must debit three actual upward2 quotes in original provider ceiling20")
	}
}
