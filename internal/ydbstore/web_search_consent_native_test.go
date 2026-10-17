//go:build ydbintegration

package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessegress"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

func nativeConsentGraph(t *testing.T) (nativeOwnedParentGraph, *nativeOperationBodyKeeper, serverlessegress.PolicyV1) {
	t.Helper()
	managed := newNativeManagedAdmissionGraph(t)
	f := managed.f
	cost := f.profile.CostCeiling.WebSearchCost
	grant := ports.OwnedFixtureToolGrantV1{Version: 1, TenantID: cost.TenantID, OwnerUserID: cost.OwnerUserID, BillingOwnerUserID: cost.BillingOwnerUserID, Kind: cost.Kind, ResourceID: cost.ResourceID, ResourceRevision: cost.ResourceRevision, RevocationGeneration: cost.RevocationGeneration, ProfileID: cost.ProfileID, ProfileRevision: cost.ProfileRevision, ConsentMode: "initiating_user_required", ConsentRevision: 1, Endpoint: "fixture://owned/web-search", MaxArgumentBytes: 4096, MaxResultBytes: 24576, ExpiresAt: cost.PriceExpiresAt.UTC().Truncate(time.Microsecond)}
	body, err := json.Marshal(grant)
	if err != nil || grant.Validate() != nil {
		t.Fatal("actual immutable search fixture grant", err)
	}
	digest := ports.OwnedFixtureBytesDigestV1(body)
	f.profile.WebSearchGrantDigest = digest
	f.profile.CostCeiling.WebSearchCost.ScopedGrantDigest = digest
	f.profile.Substrate.AdmissionCostCeilingDigest, err = f.profile.CostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	f.spending[3].ScopedGrantDigest = digest
	bindReaderSpendingSource(t, &f.spending[3])
	policy, err := serverlessegress.OwnedHTTPSFixturePolicyV1(f.profile)
	if err != nil {
		t.Fatal(err)
	}
	f.profile.Substrate.EgressPolicyDigest, err = policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	// All digests precede real private publication and canonical ingress. No
	// consent/operation/proxy/accounting row is fabricated by this graph.
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
	if _, err := g.f.store.publishBoundedOwnedAllocation(g.ctx, nativeOwnedAllocationPublication(t, g)); err != nil {
		t.Fatal("actual preparation allocation", err)
	}
	parent, _ := g.persistedParent(t)
	ref, _ := ports.OwnedFixtureGrantReferenceV1(digest)
	keeper := &nativeOperationBodyKeeper{scope: nativeOwnedOperationScope(t, parent), bodies: map[string][]byte{ref: bytes.Clone(body)}}
	g.f.store.attemptOperationBodies = keeper
	return g, keeper, policy
}

func nativeConsentModel(t *testing.T, g nativeOwnedParentGraph, keeper *nativeOperationBodyKeeper, policy serverlessegress.PolicyV1, response []byte) domain.AttemptOperationObservationV1 {
	t.Helper()
	parent, _ := g.persistedParent(t)
	clock := nativeOperationClock{g.ctx, g.f.store}
	var allocation domain.RegisteredAllocationV2
	if err := g.f.store.ownedAuthorityTx(g.ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		_, p, s, burn, err := g.f.store.operationParent(ctx, q, keeper.scope)
		if err != nil {
			return err
		}
		a, err := readOwnedAllocationSnapshots(ctx, q, p, s, burn)
		if err != nil || a == nil {
			return domain.ErrMembershipDenied
		}
		allocation = a.original.Clone()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	fixture, err := serverlessegress.NewOwnedHTTPSFixtureV1(g.ctx, g.profileReceipt.Profile, clock, response)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fixture.Close)
	proxy := fixture.Proxy()
	preflight, err := proxy.Preflight(g.ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"kind":"model","input":"fixture"}`)
	keeper.mu.Lock()
	keeper.bodies["consent-model-request"] = bytes.Clone(body)
	keeper.mu.Unlock()
	cost := parent.Authority.AdmissionCostCeiling
	cd, _ := cost.Digest()
	deadline := clock.Now().Add(10 * time.Second)
	for _, expiry := range []time.Time{preflight.ExpiresAt, allocation.Window.ExpiresAt} {
		if expiry.Before(deadline) {
			deadline = expiry
		}
	}
	op := domain.AttemptOperationV1{Version: 1, Scope: keeper.scope, Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "consent-model-call", RequestRef: "consent-model-request", RequestDigest: operationBodyDigest(body), Deadline: deadline, CleanupDuration: parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxCleanupDuration, RequestBytes: uint64(len(body)), ResultBytes: 512, ContextBytes: 1, Currency: cost.Currency, PriceRevision: cost.PriceRevision, AdmissionCostCeilingDigest: cd, Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 1, OutputTokens: 1, BodyBytes: uint64(len(body)) + 512, ResponseBytes: 512, AmountMicrounits: *g.final.Source.Spending[0].MaxAmountMicrounits, ActiveDuration: 10 * time.Second}}
	proof, err := proxy.ObserveOwnedProxy(allocation, g.final.Source, op)
	if err != nil {
		t.Fatal(err)
	}
	g.f.store.ownedProxyPublicationVerifier = proxy
	registered, _, err := g.f.store.PublishOwnedProxy(g.ctx, g.request, proof)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := g.f.store.ReserveAttemptOperation(g.ctx, ports.ReserveAttemptOperationRequestV1{Operation: op})
	if err != nil || reserved.Capability == nil {
		t.Fatalf("actual model reserve: %v", err)
	}
	result, err := proxy.InvokeOwnedOperation(g.ctx, registered, *reserved.Capability, g.f.store, body)
	if err != nil || fixture.Requests() != 1 || !bytes.Equal(result.Response, response) {
		t.Fatalf("actual one-use model response: %v", err)
	}
	keeper.mu.Lock()
	keeper.bodies["consent-model-response"] = bytes.Clone(result.Response)
	keeper.mu.Unlock()
	rd, _ := reserved.Reservation.Digest()
	amount, one := op.Debit.AmountMicrounits, uint64(1)
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: reserved.Reservation.OperationDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: "consent-model-response", ResponseDigest: operationBodyDigest(response), ResponseBytes: uint64(len(response)), ObservedAt: result.ObservedAt, Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: &one, OutputTokens: &one, AmountMicrounits: &amount}}
	authenticated, err := g.f.store.attemptOperationIssuer.AuthenticateAttemptOperationObservation(reserved.ObservationGrant, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.f.store.RecordAttemptOperationObservation(g.ctx, ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: reserved.ObservationGrant, Observation: authenticated}); err != nil {
		t.Fatal(err)
	}
	return o
}

func TestOwnedConsentYDBActualModelActorDecisionAndStorageReplay(t *testing.T) {
	for _, revokeBeforeSend := range []bool{false, true} {
		name := "approved_exactly_one_search"
		if revokeBeforeSend {
			name = "revoke_after_reserve_zero_send"
		}
		t.Run(name, func(t *testing.T) { nativeConsentActorScenario(t, revokeBeforeSend) })
	}
}

func nativeConsentPendingRequest(t *testing.T, g nativeOwnedParentGraph, keeper *nativeOperationBodyKeeper, policy serverlessegress.PolicyV1) (domain.WebSearchConsentRequestV1, domain.AttemptOperationObservationV1) {
	t.Helper()
	arguments := []byte(`{"max_results":5,"query":"public compiler documentation"}`)
	response := []byte(`{"version":1,"kind":"model_response","text":"","calls":[{"call_id":"consent-search-call","kind":"web_search","arguments":{"max_results":5,"query":"public compiler documentation"}}]}`)
	model := nativeConsentModel(t, g, keeper, policy, response)
	parent, _ := g.persistedParent(t)
	clock := nativeOperationClock{g.ctx, g.f.store}
	at := clock.Now()
	expires := at.Add(40 * time.Second)
	if parent.Authority.InvocationDeadline.Before(expires) {
		expires = parent.Authority.InvocationDeadline
	}
	s := ownedConsentScope(parent)
	r := domain.WebSearchConsentRequestV1{Version: 1, ID: "native-consent", Scope: s, InitiatingUserID: g.final.Source.Scope.OwnerUserID, ModelResponseDigest: model.ResponseDigest, CallID: "consent-search-call", ArgumentsDigest: operationBodyDigest(arguments), QueryDigest: operationBodyDigest([]byte("public compiler documentation")), GrantDigest: parent.Authority.AdmissionCostCeiling.WebSearchCost.ScopedGrantDigest, ConsentRevision: 1, RevocationGeneration: parent.Authority.AdmissionCostCeiling.WebSearchCost.RevocationGeneration, Arguments: domain.BlobRef{TenantID: s.TenantID, Key: domain.SessionRunObjectPrefix(s.TenantID, s.SessionID, s.RunID) + "attempts/" + string(s.AttemptID) + "/consent/arguments.json", Size: int64(len(arguments)), SHA256: operationBodyDigest(arguments)}, CreatedAt: at, ExpiresAt: expires}
	keeper.mu.Lock()
	keeper.bodies[r.Arguments.Key] = bytes.Clone(arguments)
	keeper.mu.Unlock()
	return r, model
}

func nativeConsentActorScenario(t *testing.T, revokeBeforeSend bool) {
	t.Helper()
	g, keeper, policy := nativeConsentGraph(t)
	r, model := nativeConsentPendingRequest(t, g, keeper, policy)
	s, at := r.Scope, r.CreatedAt
	before, _, _ := nativeAccountingBytes(t, g)
	changed := r
	changed.QueryDigest = strings.Repeat("8", 64)
	if _, err := g.f.store.CreateWebSearchConsent(g.ctx, ports.CreateWebSearchConsentRequestV1{Request: changed}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("changed actual model query created consent")
	}
	state, err := g.f.store.CreateWebSearchConsent(g.ctx, ports.CreateWebSearchConsentRequestV1{Request: r})
	if err != nil || state.Status != domain.WebSearchConsentPendingV1 || state.Decision != nil {
		t.Fatalf("actual model/grant/request creation: %v", err)
	}
	if _, err := g.f.store.ReconcileWebSearchConsent(g.ctx, ports.ReconcileWebSearchConsentRequestV1{Locator: consentLocator(r), ExpectedRevision: 1}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("healthy pending with no browser session falsely stopped", err)
	}
	after, _, _ := nativeAccountingBytes(t, g)
	if after.Aggregate.BodyBytes-before.Aggregate.BodyBytes != ownedConsentMetadataReservationBytes+uint64(r.Arguments.Size) || after.Amounts != before.Amounts || after.Aggregate.Operations != before.Aggregate.Operations {
		t.Fatal("consent did not reserve only shared bounded storage")
	}
	if _, err := g.f.store.CreateWebSearchConsent(g.ctx, ports.CreateWebSearchConsentRequestV1{Request: r}); err != nil {
		t.Fatal("exact create recovery", err)
	}
	replayed, _, _ := nativeAccountingBytes(t, g)
	if replayed.Aggregate != after.Aggregate || replayed.Amounts != after.Amounts {
		t.Fatal("create recovery charged again")
	}
	membership, found, err := readMembershipTx(g.ctx, g.f.store.db, r.InitiatingUserID, s.TenantID)
	if err != nil || !found {
		t.Fatal("actual membership prerequisite", err)
	}
	web := domain.WebSession{SessionDigest: domain.DigestSecret("consent-web-" + string(s.AttemptID)), CSRFTokenDigest: domain.DigestSecret("consent-csrf-" + string(s.AttemptID)), UserID: r.InitiatingUserID, ActiveTenantID: s.TenantID, AuthenticatedSubject: domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "123"}, MembershipSecurityVersion: membership.SecurityVersion, IssuedAt: at, LastSeenAt: at, IdleExpiresAt: at.Add(time.Minute), AbsoluteExpiresAt: at.Add(time.Hour)}
	if err := g.f.store.CreateWebSession(g.ctx, web); err != nil {
		t.Fatal("actual web session prerequisite", err)
	}
	t.Cleanup(func() {
		bucket, _ := webBucket(string(web.SessionDigest))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := g.f.store.db.ExecContext(ctx, `DELETE FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, bucket, web.SessionDigest); err != nil {
			t.Error(err)
		}
	})
	read := ports.ReadWebSearchConsentRequestV1{Locator: consentLocator(r), WebSessionDigest: web.SessionDigest}
	if _, err := g.f.store.ReadWebSearchConsent(g.ctx, read); err != nil {
		t.Fatal("same initiating actor read", err)
	}
	decision := ports.DecideWebSearchConsentRequestV1{Locator: read.Locator, WebSessionDigest: web.SessionDigest, CSRFTokenDigest: domain.DigestSecret("wrong-csrf"), ExpectedRevision: 1, Choice: domain.WebSearchConsentApproveV1}
	if _, err := g.f.store.DecideWebSearchConsent(g.ctx, decision); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("wrong CSRF approved")
	}
	decision.CSRFTokenDigest = web.CSRFTokenDigest
	approved, err := g.f.store.DecideWebSearchConsent(g.ctx, decision)
	if err != nil || approved.Decision == nil || approved.Status != domain.WebSearchConsentApprovedV1 {
		t.Fatalf("actual initiating-user CAS decision: %v", err)
	}
	again, err := g.f.store.DecideWebSearchConsent(g.ctx, decision)
	if err != nil || again.Decision == nil {
		t.Fatal("exact authenticated decision lost ACK", err)
	}
	a, _ := approved.Decision.Digest()
	b, _ := again.Decision.Digest()
	if a != b || !approved.UpdatedAt.Equal(again.UpdatedAt) {
		t.Fatal("decision replay replaced original receipt/time")
	}
	decision.Choice = domain.WebSearchConsentDenyV1
	if _, err := g.f.store.DecideWebSearchConsent(g.ctx, decision); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("conflicting choice replaced receipt")
	}
	nativeConsentSearchSend(t, g, keeper, policy, model, approved, web, revokeBeforeSend)
	if _, err := g.f.store.ReadWebSearchConsent(g.ctx, read); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("revoked actor disclosed consent")
	}
	stopRequest := ports.ReconcileWebSearchConsentRequestV1{Locator: consentLocator(r), ExpectedRevision: 2}
	stopped, err := g.f.store.ReconcileWebSearchConsent(g.ctx, stopRequest)
	if err != nil || stopped.Status != domain.WebSearchConsentRevokedV1 || stopped.Revision != 3 || stopped.Decision == nil {
		t.Fatalf("actual revoked-session reconcile preserves original receipt: %v", err)
	}
	stoppedDigest, _ := stopped.Decision.Digest()
	if stoppedDigest != a {
		t.Fatal("reconciliation replaced human decision")
	}
	recoveredStop, err := g.f.store.ReconcileWebSearchConsent(g.ctx, stopRequest)
	if err != nil || recoveredStop.Status != stopped.Status || recoveredStop.Revision != stopped.Revision || !recoveredStop.UpdatedAt.Equal(stopped.UpdatedAt) {
		t.Fatal("exact stop lost-ack retry mutated stop", err)
	}
	recoveredCreate, err := g.f.store.CreateWebSearchConsent(g.ctx, ports.CreateWebSearchConsentRequestV1{Request: r})
	if err != nil || recoveredCreate.Status != domain.WebSearchConsentRevokedV1 || recoveredCreate.Revision != 3 {
		t.Fatal("create lost-ack replay reopened stopped consent", err)
	}
	g.requireCount(t, "attempt_operations_v1", 2)
}

func TestOwnedConsentYDBPendingStopFromActualCurrentFacts(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, nativeOwnedParentGraph)
		want   domain.WebSearchConsentStatusV1
	}{
		{name: "profile_revocation", mutate: func(t *testing.T, g nativeOwnedParentGraph) { g.revokeProfile(t) }, want: domain.WebSearchConsentRevokedV1},
		{name: "canonical_run_cancellation", mutate: func(t *testing.T, g nativeOwnedParentGraph) {
			t.Helper()
			at := nativeOperationClock{g.ctx, g.f.store}.Now()
			if err := g.f.store.Transact(g.ctx, g.run.TenantID, func(tx ports.StateTx) error {
				run, found, err := tx.GetRun(g.ctx, g.run.ID)
				if err != nil || !found {
					return domain.ErrMembershipDenied
				}
				if err := run.RequestCancellation(at); err != nil {
					return err
				}
				run.UpdatedAt = at
				return tx.PutRun(g.ctx, run)
			}); err != nil {
				t.Fatal("canonical original Run cancellation", err)
			}
		}, want: domain.WebSearchConsentCancelledV1},
		{name: "missing_original_lease_head", mutate: func(t *testing.T, g nativeOwnedParentGraph) {
			t.Helper()
			// Deliberate unique test-owned current-head loss. Retain the real
			// original lease, parent and consent; this is a fault, not permission
			// to repair the claim or a production cleanup operation.
			if _, err := g.f.store.db.ExecContext(g.ctx, `DELETE FROM lease_heads WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID); err != nil {
				t.Fatal("install test-owned missing current lease head", err)
			}
		}, want: domain.WebSearchConsentReconcileOnlyV1},
	} {
		t.Run(test.name, func(t *testing.T) {
			g, keeper, policy := nativeConsentGraph(t)
			r, _ := nativeConsentPendingRequest(t, g, keeper, policy)
			pending, err := g.f.store.CreateWebSearchConsent(g.ctx, ports.CreateWebSearchConsentRequestV1{Request: r})
			if err != nil || pending.Status != domain.WebSearchConsentPendingV1 {
				t.Fatal("actual pending prerequisite", err)
			}
			before, _, _ := nativeAccountingBytes(t, g)
			test.mutate(t, g)
			input := ports.ReconcileWebSearchConsentRequestV1{Locator: consentLocator(r), ExpectedRevision: 1}
			stopped, err := g.f.store.ReconcileWebSearchConsent(g.ctx, input)
			if err != nil || stopped.Status != test.want || stopped.Revision != 2 || stopped.Decision != nil {
				t.Fatalf("actual stop got status=%s revision=%d decision=%t err=%v; want %s/2/no browser", stopped.Status, stopped.Revision, stopped.Decision != nil, err, test.want)
			}
			retry, err := g.f.store.ReconcileWebSearchConsent(g.ctx, input)
			if err != nil || retry.Status != stopped.Status || retry.Revision != stopped.Revision || !retry.UpdatedAt.Equal(stopped.UpdatedAt) {
				t.Fatal("immutable stopped history replay", err)
			}
			after, _, _ := nativeAccountingBytes(t, g)
			if before.Aggregate != after.Aggregate || before.Amounts != after.Amounts {
				t.Fatal("reconcile stop changed original bounded accounting")
			}
			g.requireCount(t, "attempt_operations_v1", 1)
		})
	}
}

func nativeConsentSearchSend(t *testing.T, g nativeOwnedParentGraph, keeper *nativeOperationBodyKeeper, policy serverlessegress.PolicyV1, model domain.AttemptOperationObservationV1, approved domain.WebSearchConsentStateV1, web domain.WebSession, revokeBeforeSend bool) {
	t.Helper()
	parent, _ := g.persistedParent(t)
	clock := nativeOperationClock{g.ctx, g.f.store}
	var allocation domain.RegisteredAllocationV2
	if err := g.f.store.ownedAuthorityTx(g.ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		_, p, s, burn, err := g.f.store.operationParent(ctx, q, keeper.scope)
		if err != nil {
			return err
		}
		a, err := readOwnedAllocationSnapshots(ctx, q, p, s, burn)
		if err != nil || a == nil {
			return domain.ErrMembershipDenied
		}
		allocation = a.original.Clone()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := approved.Request
	keeper.mu.Lock()
	body := bytes.Clone(keeper.bodies[r.Arguments.Key])
	keeper.bodies["consent-search-request"] = bytes.Clone(body)
	keeper.mu.Unlock()
	response := []byte(`{"version":1,"kind":"search_result","results":[{"title":"Compiler documentation","url":"https://example.test/compiler"}]}`)
	fixture, err := serverlessegress.NewOwnedHTTPSFixtureForOperationV1(g.ctx, g.profileReceipt.Profile, clock, response, 2, domain.AttemptOperationWebSearchV1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fixture.Close)
	proxy := fixture.Proxy()
	preflight, err := proxy.Preflight(g.ctx, policy)
	if err != nil {
		t.Fatal(err)
	}
	cost := parent.Authority.AdmissionCostCeiling
	cd, _ := cost.Digest()
	deadline := clock.Now().Add(10 * time.Second)
	for _, expiry := range []time.Time{preflight.ExpiresAt, allocation.Window.ExpiresAt, r.ExpiresAt} {
		if expiry.Before(deadline) {
			deadline = expiry
		}
	}
	rd, _ := r.Digest()
	dd, _ := approved.Decision.Digest()
	previous, _ := model.Digest()
	op := domain.AttemptOperationV1{Version: 1, Scope: keeper.scope, Sequence: 2, Kind: domain.AttemptOperationWebSearchV1, CallID: r.CallID, RequestRef: "consent-search-request", RequestDigest: operationBodyDigest(body), Deadline: deadline, CleanupDuration: parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxCleanupDuration, RequestBytes: uint64(len(body)), ResultBytes: 512, Currency: cost.Currency, PriceRevision: cost.PriceRevision, AdmissionCostCeilingDigest: cd, PredecessorObservationDigest: previous, ModelResponseDigest: model.ResponseDigest, ArgumentsDigest: r.ArgumentsDigest, ToolGrantDigest: r.GrantDigest, ExternalChargeEvidenceDigest: cost.WebSearchCost.PriceEvidenceDigest, QueryDigest: r.QueryDigest, ConsentRequestDigest: string(rd), ConsentDecisionDigest: string(dd), ConsentDecisionRef: string(r.ID), Debit: domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, WebSearches: 1, BodyBytes: uint64(len(body)) + 512, ResponseBytes: 512, AmountMicrounits: *g.final.Source.Spending[3].MaxAmountMicrounits, ActiveDuration: 10 * time.Second}}
	binding := domain.WebSearchConsentReceiptBindingV1{Request: r, RequestDigest: rd, DecisionDigest: dd}
	proof, err := proxy.ObserveOwnedProxy(allocation, g.final.Source, op)
	if err != nil {
		t.Fatal(err)
	}
	g.f.store.ownedProxyPublicationVerifier = proxy
	registered, _, err := g.f.store.PublishOwnedProxy(g.ctx, g.request, proof)
	if err != nil {
		t.Fatal("actual search proxy", err)
	}
	before, _, _ := nativeAccountingBytes(t, g)
	if _, err := g.f.store.ReserveAttemptOperation(g.ctx, ports.ReserveAttemptOperationRequestV1{Operation: op}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("search without typed receipt reserved")
	}
	bad := binding
	bad.DecisionDigest = domain.WebSearchConsentDecisionDigestV1(strings.Repeat("9", 64))
	if _, err := g.f.store.ReserveAttemptOperation(g.ctx, ports.ReserveAttemptOperationRequestV1{Operation: op, ConsentReceipt: &bad}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("changed typed receipt reserved")
	}
	afterDenied, _, _ := nativeAccountingBytes(t, g)
	if before.Aggregate != afterDenied.Aggregate || before.Amounts != afterDenied.Amounts || fixture.Requests() != 0 {
		t.Fatal("denied receipt debited or sent")
	}
	reserved, err := g.f.store.ReserveAttemptOperation(g.ctx, ports.ReserveAttemptOperationRequestV1{Operation: op, ConsentReceipt: &binding})
	if err != nil || reserved.Capability == nil {
		nativeConsentReserveDiagnostic(t, g, op, binding)
		t.Fatalf("actual approved search atomic reserve: %v", err)
	}
	if revokeBeforeSend {
		if err := g.f.store.RevokeWebSession(g.ctx, web.SessionDigest, clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	result, err := proxy.InvokeOwnedOperation(g.ctx, registered, *reserved.Capability, g.f.store, body)
	if revokeBeforeSend {
		if err == nil || fixture.Requests() != 0 {
			t.Fatal("revoked between reserve/send emitted HTTP")
		}
		return
	}
	if err != nil || fixture.Requests() != 1 || !bytes.Equal(result.Response, response) {
		t.Fatalf("approved exact single search: %v, requests=%d", err, fixture.Requests())
	}
	if _, err := proxy.InvokeOwnedOperation(g.ctx, registered, *reserved.Capability, g.f.store, body); err == nil || fixture.Requests() != 1 {
		t.Fatal("approved receipt retransmitted search")
	}
	keeper.mu.Lock()
	keeper.bodies["consent-search-response"] = bytes.Clone(result.Response)
	keeper.mu.Unlock()
	reservationDigest, _ := reserved.Reservation.Digest()
	amount := op.Debit.AmountMicrounits
	observation := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: reserved.Reservation.OperationDigest, ReservationDigest: reservationDigest, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: "consent-search-response", ResponseDigest: operationBodyDigest(result.Response), ResponseBytes: uint64(len(result.Response)), ObservedAt: result.ObservedAt, Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: &amount}}
	authenticated, err := g.f.store.attemptOperationIssuer.AuthenticateAttemptOperationObservation(reserved.ObservationGrant, observation)
	if err != nil {
		t.Fatal("authenticate original synthetic search response", err)
	}
	if err := g.f.store.RecordAttemptOperationObservation(g.ctx, ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: reserved.ObservationGrant, Observation: authenticated}); err != nil {
		t.Fatal("persist actual immutable search response", err)
	}
	issuer, err := serverlessharness.NewOperationCapabilityIssuer(clock.Now, strings.NewReader(strings.Repeat("fresh-search-process-key", 4)), 16)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := New(g.f.store.db, Options{OwnedAttemptEffectSourceVerifier: g.f.store.ownedAttemptEffectSourceVerifier, AttemptOperationIssuer: issuer, AttemptOperationBodies: keeper})
	if err != nil {
		t.Fatal(err)
	}
	beforeRecovery, _, _ := nativeAccountingBytes(t, g)
	recovered, err := resumed.RecoverAttemptOperation(g.ctx, ports.RecoverAttemptOperationRequestV1{Operation: op, ExpectedReservationDigest: reservationDigest})
	if err != nil || recovered.Capability != nil || recovered.Status != ports.AttemptOperationReconcileOnlyV1 {
		t.Fatal("fresh Store recovered a resend capability", err)
	}
	observed, err := resumed.ReadAttemptOperationObservation(g.ctx, recovered.ObservationGrant)
	want, _ := observation.Digest()
	got, _ := observed.Observation.Digest()
	if err != nil || got != want || observed.Observation.Outcome != domain.AttemptOperationCompletedV1 {
		t.Fatal("fresh Store lost exact original completed search response", err)
	}
	afterRecovery, _, _ := nativeAccountingBytes(t, g)
	if beforeRecovery.Aggregate != afterRecovery.Aggregate || beforeRecovery.Amounts != afterRecovery.Amounts || fixture.Requests() != 1 {
		t.Fatal("fresh search recovery changed original debit or sent again")
	}
	if err := g.f.store.RevokeWebSession(g.ctx, web.SessionDigest, clock.Now()); err != nil {
		t.Fatal(err)
	}
}

// Failure-only bounded diagnostics, not an authorization preflight. No writes,
// secret/query dumps, changed budget or substitute production success path.
func nativeConsentReserveDiagnostic(t *testing.T, g nativeOwnedParentGraph, op domain.AttemptOperationV1, binding domain.WebSearchConsentReceiptBindingV1) {
	t.Helper()
	evidence, err := g.f.store.prepareOwnedOperationEvidence(g.ctx, op)
	if err != nil || evidence == nil || evidence.tool == nil {
		t.Logf("reserve diagnostic immutable evidence err=%v", err)
		return
	}
	_ = g.f.store.ownedAuthorityTx(g.ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		report := func(name string, err error) error {
			t.Logf("reserve diagnostic phase=%s statements=%d bytes=%d failed=%t err=%v", name, q.budget.statements, q.budget.bytes, q.budget.failed, err)
			return err
		}
		request, parent, source, burn, err := g.f.store.operationParent(ctx, q, op.Scope)
		if report("parent", err) != nil {
			return err
		}
		a, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burn)
		if report("allocation", err) != nil || a == nil {
			return domain.ErrMembershipDenied
		}
		_, err = readOwnedProxySnapshotAt(ctx, q, parent, source, a.original, op.Sequence)
		if report("proxy", err) != nil {
			return err
		}
		_, err = readOwnedOperation(ctx, q, op)
		if report("absence", err) != nil {
			return err
		}
		err = g.f.store.validateOwnedOperationEvidence(ctx, q, op, evidence, parent, source)
		if report("operation_evidence", err) != nil {
			return err
		}
		err = g.f.store.validateOwnedOperationConsent(ctx, q, op, parent, source, &binding, *evidence.tool)
		if report("consent", err) != nil {
			return err
		}
		err = g.f.store.validateCurrentOwnedAttemptParent(ctx, q, request, parent)
		if report("current_parent", err) != nil {
			return err
		}
		accounting, err := readOwnedAttemptAccounting(ctx, q, parent, source)
		if report("accounting", err) != nil || accounting == nil {
			return domain.ErrMembershipDenied
		}
		_, err = debitOwnedOperation(*accounting, op, parent, source, q.at)
		return report("debit", err)
	})
}
