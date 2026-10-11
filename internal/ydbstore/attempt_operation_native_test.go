//go:build ydbintegration

package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessegress"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type nativeOperationBodyKeeper struct {
	scope  domain.AttemptOperationScopeV1
	mu     sync.Mutex
	bodies map[string][]byte
}

func (k *nativeOperationBodyKeeper) ReadAttemptOperationBody(ctx context.Context, scope domain.AttemptOperationScopeV1, ref string, cap uint64) ([]byte, error) {
	if ctx.Err() != nil || scope != k.scope || cap == 0 || cap > 1<<20 {
		return nil, domain.ErrMembershipDenied
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	body, found := k.bodies[ref]
	if !found || uint64(len(body)) > cap {
		return nil, domain.ErrMembershipDenied
	}
	return bytes.Clone(body), nil
}

type nativeOperationClock struct {
	ctx   context.Context
	store *Store
}

func (c nativeOperationClock) Now() time.Time {
	var at time.Time
	if c.store.db.QueryRowContext(c.ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at) != nil {
		return time.Time{}
	}
	return at.UTC()
}
func operationBodyDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func nativeOwnedOperationScope(t *testing.T, parent domain.OwnedAttemptEffectParentV1) domain.AttemptOperationScopeV1 {
	t.Helper()
	r, a := parent.Reservation, parent.Authority
	rd, err := r.DigestForAuthority(a)
	if err != nil {
		t.Fatal(err)
	}
	pd, err := a.HarnessBinding.AgentLoopPolicy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return domain.AttemptOperationScopeV1{TenantID: r.TenantID, SessionID: parent.Scope.SessionID, OwnerUserID: parent.Scope.OwnerUserID, RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID, FenceToken: r.FenceToken, PhysicalInvocationClaimID: r.PhysicalInvocationClaimID, EffectReservationDigest: rd, InvocationAuthorityDigest: r.InvocationAuthorityDigest, HarnessBindingDigest: r.HarnessBindingDigest, AgentLoopPolicyDigest: pd}
}

func TestOwnedOperationYDBActualTLSRegistrationReservationOneUseAndRecovery(t *testing.T) {
	managed := newNativeManagedAdmissionGraph(t)
	policy, err := serverlessegress.OwnedHTTPSFixturePolicyV1(managed.f.profile)
	if err != nil {
		t.Fatal(err)
	}
	managed.f.profile.Substrate.EgressPolicyDigest, err = policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	// This change precedes actual authenticated publication and canonical ingress;
	// no claimed proxy/operation/accounting record is inserted by this fixture.
	g := nativeOwnedParentGraphFromFinal(t, nativeOwnedFinalGraphFrom(t, managed, false))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, table := range []string{"attempt_operation_calls_v1", "attempt_operations_v1"} {
			if _, err := g.f.store.db.ExecContext(ctx, "DELETE FROM "+table+" WHERE tenant_id=$1", g.run.TenantID); err != nil {
				t.Error(err)
			}
		}
	})
	nativeOperationSetup(t, g)
	publication := nativeOwnedAllocationPublication(t, g)
	receipt, err := g.f.store.publishBoundedOwnedAllocation(g.ctx, publication)
	if err != nil {
		t.Fatalf("actual allocation publication: %v", err)
	}
	parent, _ := g.persistedParent(t)
	scope := nativeOwnedOperationScope(t, parent)
	requestBody := []byte(`{"kind":"model","input":"fixture"}`)
	keeper := &nativeOperationBodyKeeper{scope: scope, bodies: map[string][]byte{"operation-request-1": bytes.Clone(requestBody)}}
	g.f.store.attemptOperationBodies = keeper
	clock := nativeOperationClock{g.ctx, g.f.store}
	fixture, err := serverlessegress.NewOwnedHTTPSFixtureV1(g.ctx, g.profileReceipt.Profile, clock, []byte(`{"version":1,"kind":"model_response","text":"ok","calls":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fixture.Close)
	proxy := fixture.Proxy()
	if _, err := proxy.Preflight(g.ctx, policy); err != nil {
		t.Fatalf("actual original TLS handshake: %v", err)
	}
	proof, err := proxy.ObserveOwnedProxy(receipt.Original, g.final.Source)
	if err != nil {
		t.Fatalf("actual TLS producer observation: %v", err)
	}
	g.f.store.ownedProxyPublicationVerifier = proxy
	registered, replayed, err := g.f.store.PublishOwnedProxy(g.ctx, g.request, proof)
	if err != nil || replayed {
		t.Fatalf("actual native proxy publication: replay=%t err=%v", replayed, err)
	}
	if fixture.Requests() != 0 {
		t.Fatal("TLS registration/preflight sent request payload")
	}
	cost := parent.Authority.AdmissionCostCeiling
	cd, _ := cost.Digest()
	deadline := clock.Now().Add(10 * time.Second)
	if registered.Window.ExpiresAt.Before(deadline) {
		deadline = registered.Window.ExpiresAt
	}
	op := domain.AttemptOperationV1{Version: 1, Scope: scope, Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "model-call-1", RequestRef: "operation-request-1", RequestDigest: operationBodyDigest(requestBody), Deadline: deadline, CleanupDuration: parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxCleanupDuration,
		Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 1, OutputTokens: 1, BodyBytes: uint64(len(requestBody)) + 512, ResponseBytes: 512, AmountMicrounits: *g.final.Source.Spending[0].MaxAmountMicrounits, ActiveDuration: 10 * time.Second}, RequestBytes: uint64(len(requestBody)), ResultBytes: 512, ContextBytes: 1, Currency: cost.Currency, PriceRevision: cost.PriceRevision, AdmissionCostCeilingDigest: cd}
	reserved, err := g.f.store.ReserveAttemptOperation(g.ctx, ports.ReserveAttemptOperationRequestV1{Operation: op})
	if err != nil || reserved.Status != ports.AttemptOperationReservedV1 || reserved.Capability == nil {
		t.Fatalf("actual atomic current registration/debit reserve: err=%v status=%s", err, reserved.Status)
	}
	if reserved.Reservation.ComponentAmounts[1] != *g.final.Source.Spending[1].MaxAmountMicrounits || reserved.Reservation.AggregateDebit.Operations != 1 {
		t.Fatal("reservation omitted original setup or operation debit")
	}
	// Actual payload tampering cannot consume the valid capability or emit HTTP.
	if _, err := proxy.InvokeOwnedOperation(g.ctx, registered, *reserved.Capability, g.f.store, []byte(`{"different":true}`)); err == nil || fixture.Requests() != 0 {
		t.Fatal("changed body emitted an operation")
	}
	result, err := proxy.InvokeOwnedOperation(g.ctx, registered, *reserved.Capability, g.f.store, requestBody)
	if err != nil || result.Acceptance != domain.ProviderAcceptanceUnknownV1 || fixture.Requests() != 1 || !bytes.Equal(fixture.RequestBody(), requestBody) {
		t.Fatalf("actual current-gated one-use TLS request: err=%v count=%d acceptance=%s", err, fixture.Requests(), result.Acceptance)
	}
	if _, err := proxy.InvokeOwnedOperation(g.ctx, registered, *reserved.Capability, g.f.store, requestBody); err == nil || fixture.Requests() != 1 {
		t.Fatal("one-use connection/capability resent request")
	}
	rd, _ := reserved.Reservation.Digest()
	observation := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: reserved.Reservation.OperationDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationAcceptedOutcomeUnknownV1, Reason: domain.AttemptOperationReasonTransportUnknownV1, ObservedAt: result.ObservedAt, Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
	authenticated, err := g.f.store.attemptOperationIssuer.AuthenticateAttemptOperationObservation(reserved.ObservationGrant, observation)
	if err != nil {
		t.Fatal(err)
	}
	record := ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: reserved.ObservationGrant, Observation: authenticated}
	if err := g.f.store.RecordAttemptOperationObservation(g.ctx, record); err != nil {
		t.Fatalf("actual opaque transport remains unknown: %v", err)
	}
	if err := g.f.store.RecordAttemptOperationObservation(g.ctx, record); err != nil {
		t.Fatalf("exact immutable observation retry: %v", err)
	}
	// Unknown acceptance cannot be converted into a new sequence/proxy even
	// when a new synthetic original TLS connection really handshakes.
	nextFixture, err := serverlessegress.NewOwnedHTTPSFixtureForOperationV1(g.ctx, g.profileReceipt.Profile, clock, []byte(`{"version":1,"kind":"model_response","text":"","calls":[]}`), 2, domain.AttemptOperationModelV1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nextFixture.Close)
	if _, err := nextFixture.Proxy().Preflight(g.ctx, policy); err != nil {
		t.Fatal(err)
	}
	nextProof, err := nextFixture.Proxy().ObserveOwnedProxy(receipt.Original, g.final.Source)
	if err != nil {
		t.Fatal(err)
	}
	g.f.store.ownedProxyPublicationVerifier = nextFixture.Proxy()
	if _, _, err := g.f.store.PublishOwnedProxy(g.ctx, g.request, nextProof); err == nil || nextFixture.Requests() != 0 {
		t.Fatal("unknown predecessor became another physical-send registration")
	}
	newIssuer, err := serverlessharness.NewOperationCapabilityIssuer(func() time.Time { return clock.Now() }, strings.NewReader(strings.Repeat("new-process-key", 4)), 16)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := New(g.f.store.db, Options{OwnedAttemptEffectSourceVerifier: g.f.store.ownedAttemptEffectSourceVerifier, AttemptOperationIssuer: newIssuer, AttemptOperationBodies: keeper})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := resumed.RecoverAttemptOperation(g.ctx, ports.RecoverAttemptOperationRequestV1{Operation: op, ExpectedReservationDigest: rd})
	if err != nil || recovered.Status != ports.AttemptOperationReconcileOnlyV1 || recovered.Capability != nil || !reflect.DeepEqual(recovered.Reservation, reserved.Reservation) {
		t.Fatalf("genuine new Store/issuer recovery must never resend/redebit: err=%v status=%s", err, recovered.Status)
	}
	observed, err := resumed.ReadAttemptOperationObservation(g.ctx, recovered.ObservationGrant)
	if err != nil || observed.Observation.Outcome != domain.AttemptOperationAcceptedOutcomeUnknownV1 {
		t.Fatalf("retained unknown under new process issuer: %v", err)
	}
	changed := op.Clone()
	changed.RequestDigest = strings.Repeat("8", 64)
	if _, err := resumed.RecoverAttemptOperation(g.ctx, ports.RecoverAttemptOperationRequestV1{Operation: changed}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("changed original recovery repaired a request")
	}
	g.requireCount(t, "attempt_operations_v1", 1)
	g.requireCount(t, "attempt_operation_calls_v1", 1)
}
