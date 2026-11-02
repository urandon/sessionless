//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Retained DATA only. This cannot establish current permission, original
// producer authenticity, a physical stop/join, erasure or canonical completion.
// The actual Runtime/Store path and the other productive-ACK guards prove those.
type localRuntimeRetirementData struct {
	parent, parentDigest, final, finalDigest                                                                                             string
	originalAllocation, originalAllocationDigest, currentAllocation, currentAllocationDigest, previousAllocationDigest, allocationIntent string
	originalProxy, originalProxyDigest, currentProxy, currentProxyDigest                                                                 string
	operation, reservationDigest, operationDigest, call                                                                                  string
	claim, lease, run                                                                                                                    string
	fence, effectSequence                                                                                                                uint64
	reservedAt                                                                                                                           time.Time
}

func (v localRuntimeRetirementData) validate(tenant domain.TenantID, attempt domain.AttemptID) error {
	parent, pe := domain.DecodeOwnedAttemptEffectParentV1([]byte(v.parent))
	pd, pde := parent.Digest()
	final, fe := domain.DecodeAttemptExecutionAdmissionV1([]byte(v.final))
	fd, fde := final.Digest()
	if pe != nil || pde != nil || pd != v.parentDigest || fe != nil || fde != nil || fd != v.finalDigest || parent.ValidateForAdmission(final) != nil || parent.Scope.TenantID != tenant || parent.Scope.AttemptID != attempt || parent.Reservation.PhysicalInvocationClaimID != v.claim || string(parent.Authority.Lease.ID) != v.lease || string(parent.Scope.RunID) != v.run || parent.Reservation.FenceToken != v.fence || parent.Reservation.EffectSequence != v.effectSequence || !parent.Reservation.ReservedAt.Equal(v.reservedAt) {
		return errors.New("original retirement parent/Final correlation denied")
	}
	a, ae := domain.DecodeRegisteredAllocationV2([]byte(v.originalAllocation))
	r, re := domain.DecodeRegisteredAllocationV2([]byte(v.currentAllocation))
	ad, ade := a.Digest()
	rd, rde := r.Digest()
	intent, ie := ports.OwnedAllocationRegistrationIntentDigestV2(a)
	// This success path calls Retire, not Revoke. A direct original->retired
	// transition has exactly the original digest as its persisted CAS predecessor.
	if ae != nil || re != nil || ade != nil || rde != nil || ie != nil || string(ad) != v.originalAllocationDigest || string(rd) != v.currentAllocationDigest || intent != v.allocationIntent || v.previousAllocationDigest != string(ad) || a.ValidateInitial() != nil || r.State != domain.RegisteredPreparationRetiredV1 || r.ValidateTransitionFrom(a) != nil || a.LocalRuntime == nil {
		return errors.New("original retirement allocation transition/digest/CAS denied")
	}
	// Every local prepared fact is retained; do not normalize the tombstone into
	// an invented active snapshot. Rebuild only from the actual ORIGINAL bytes.
	prepared := domain.PreparedAllocationV2{Version: domain.PreparedAllocationVersionV2, SubstrateBindingDigest: a.ParentReservation.SubstrateBindingDigest,
		ObservedOuterHarnessDigest: a.ObservedOuterHarnessDigest, ObservedProxyArtifactDigest: a.ObservedProxyArtifactDigest,
		ObservedProxyIdentityDigest: a.ObservedProxyIdentityDigest, WorkloadMode: a.WorkloadMode, LocalRuntime: a.LocalRuntime}
	if a.ValidateForAuthority(parent.Authority, parent.Reservation, prepared, a.PreparedInvocationDigest, parent.PreparationExpiresAt, final.Source) != nil {
		return errors.New("original retirement allocation/preparation/Source correlation denied")
	}
	p, pxe := domain.DecodeRegisteredProxyAttestationV2([]byte(v.originalProxy))
	c, ce := domain.DecodeRegisteredProxyAttestationV2([]byte(v.currentProxy))
	pxd, pxde := p.Digest()
	cd, cde := c.Digest()
	if pxe != nil || ce != nil || pxde != nil || cde != nil || string(pxd) != v.originalProxyDigest || string(cd) != v.currentProxyDigest || v.originalProxy != v.currentProxy || p.ValidateInitial() != nil || p.ValidateForAllocation(a, final.Source) != nil || p.TransportKind != domain.RegisteredProxyTransportLocalLoopbackHTTPV2 || p.ServiceKind != domain.RegisteredProxyProviderV1 || p.OperationSequence != 1 || p.PhysicalInvocationClaimID != v.claim {
		return errors.New("original retirement immutable ACTIVE proxy correlation denied")
	}
	var op domain.AttemptOperationReservationV1
	if len(v.operation) > 32<<10 || json.Unmarshal([]byte(v.operation), &op) != nil || op.ValidateForParent(parent.Authority, parent.Reservation) != nil {
		return errors.New("original retirement MODEL reservation DATA denied")
	}
	canonical, me := json.Marshal(op)
	od, ode := op.Digest()
	if me != nil || ode != nil || !bytes.Equal(canonical, []byte(v.operation)) || string(od) != v.reservationDigest || string(op.OperationDigest) != v.operationDigest || op.OperationDigest != p.OperationDigest || op.RegisteredAllocationDigest != ad || op.RegisteredProxyDigest != pxd || op.Operation.CallID != v.call || op.Operation.Sequence != 1 || op.Operation.Kind != domain.AttemptOperationModelV1 || op.Operation.Scope.SessionID != parent.Scope.SessionID || op.ReservedAt.Before(p.Window.PublishedAt) || op.ReservedAt.After(r.UpdatedAt) || op.Operation.Deadline.After(p.Window.ExpiresAt) || op.Operation.Deadline.After(a.Window.ExpiresAt) {
		return errors.New("original retirement MODEL/proxy/allocation links denied")
	}
	// ACTIVE is the immutable original observation, not permission after the
	// allocation's tombstone. Production rejects divergent proxy heads too.
	return nil
}

// Reuse the existing bounded synthetic manifest, not a copied profile/evidence
// fixture. All installed IDs and source declarations below are explicit test
// DATA: no Source, signer, capability issuer, Store, SQL or effect is created.
func localRuntimeRetirementFixture(t *testing.T) localRuntimeRetirementData {
	t.Helper()
	at := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	root, _, _ := newLocalRuntimeOperatorFiles(t, t.Context(), "retirement-tenant", "retirement-owner", "http://127.0.0.1:35421/v1", at)
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	var m localoperator.ManifestV1
	if err != nil || json.Unmarshal(raw, &m) != nil {
		t.Fatal("synthetic manifest DATA", err)
	}
	c, p := m.Configuration, m.Profile
	c.Resource.ResourceID, c.Resource.Revision, c.ProfileRevision = "retirement-resource", 1, 1
	c.LocalInstallationID, c.LocalInstallationRevision = "retirement-installation", 1
	c.OriginalSource = ports.OwnedProxyBodyReferenceV1("manifest", raw)
	p.Resource, p.Locator.ResourceID = c.Resource, c.Resource.ResourceID
	p.ProfileRevision, p.RevocationGeneration, p.State = 1, 1, domain.OwnedAgentCurrentActiveV1
	p.InstallationID, p.InstallationRevision, p.MutationID, p.InstalledManifest = c.LocalInstallationID, 1, "retirement-install", c.OriginalSource
	p.Substrate.LocalInstallationID, p.Substrate.LocalInstallationRevision = c.LocalInstallationID, 1
	p.Backend.BackendProfileDigest, err = c.Digest()
	p.Locator.BackendProfileDigest = p.Backend.BackendProfileDigest
	if err != nil || c.ValidateForOwnedProfile(p) != nil || p.ValidateInitial() != nil {
		t.Fatal("synthetic installed profile DATA", err)
	}
	pd, _ := p.Digest()
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID, SessionID: "retirement-session", RunID: "retirement-run", AttemptID: "retirement-attempt"}
	var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	for i, kind := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
		s := domain.AttemptSpendingAuthorizationCurrentV1{Version: 1, Locator: domain.AttemptSpendingAuthorizationLocatorV1{TenantID: scope.TenantID, AttemptID: scope.AttemptID, ComponentKind: kind, BillingOwnerUserID: p.BillingOwnerUserID}, RunID: scope.RunID, SessionID: scope.SessionID, OwnerUserID: scope.OwnerUserID,
			SourceKind: domain.AttemptSpendingOwnerLocalSourceV1, SourceInstallationID: p.InstallationID, SourceInstallationRevision: 1, SourceProfile: domain.OwnedAgentProfileReferenceV1{Locator: p.Locator, RevocationGeneration: 1, Digest: pd},
			ResourceID: p.Resource.ResourceID, ResourceRevision: 1, ProfileID: string(p.Locator.Profile), ProfileRevision: 1, ScopedGrantDigest: p.EffectivePolicyDigest, Currency: p.CostCeiling.Currency, PriceRevision: p.CostCeiling.PriceRevision, PriceEvidenceDigest: p.ProviderPriceDigest, TermsEvidenceDigest: p.ProviderTermsDigest,
			PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: new(uint64), RevocationGeneration: 1, State: domain.OwnedAgentCurrentActiveV1, Window: p.Window, UpdatedAt: at, MutationID: "retirement-spending-" + string(kind)}
		if i == 1 {
			s.ResourceID, s.ResourceRevision, s.ResourceGeneration, s.ProfileID, s.ProfileRevision, s.ScopedGrantDigest, s.PriceEvidenceDigest, s.TermsEvidenceDigest = p.Substrate.ProfileID, p.Substrate.ProfileRevision, 1, p.Substrate.ProfileID, p.Substrate.ProfileRevision, p.Substrate.ProfileDigest, p.SubstratePriceDigest, p.SubstrateTermsDigest
		}
		if i >= 2 {
			cost := p.CostCeiling.MCPToolCost
			if i == 3 {
				cost = p.CostCeiling.WebSearchCost
			}
			s.ResourceID, s.ResourceRevision, s.ResourceGeneration, s.ProfileID, s.ProfileRevision, s.ScopedGrantDigest = cost.ResourceID, cost.ResourceRevision, cost.RevocationGeneration, cost.ProfileID, cost.ProfileRevision, cost.ScopedGrantDigest
			s.PriceRevision, s.PriceEvidenceDigest, s.TermsEvidenceDigest, s.PriceState, s.MaxAmountMicrounits = cost.PriceRevision, cost.PriceEvidenceDigest, cost.TermsEvidenceDigest, cost.PriceState, cost.MaxAmountMicrounits
		}
		body, e := s.SourceCanonicalBytes()
		if e != nil {
			t.Fatal(e)
		}
		s.SourceAuthorization = ports.OwnedProxyBodyReferenceV1("funding", body)
		if e := s.ValidateAgainstProfile(p, at); e != nil {
			t.Fatal("synthetic spending DATA", e)
		}
		spending[i] = s
	}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, p, spending, at)
	if err != nil {
		t.Fatal(err)
	}
	final, err := domain.BuildAttemptExecutionAdmissionV1(pkg.Source, "retirement-quota", at)
	if err != nil {
		t.Fatal(err)
	}
	h := strings.Repeat("a", 64)
	authority := domain.ServerlessInvocationAuthorityV1{Version: 1, HarnessBinding: pkg.Binding, ExecutionPlacementV2: pkg.Placement, SubstrateBinding: pkg.Substrate, AdmissionCostCeiling: pkg.Cost,
		Lease: domain.Lease{ID: "retirement-lease", TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, WorkerID: "retirement-worker", FenceToken: 1, AcquiredAt: at, ExpiresAt: at.Add(time.Minute)}, ContextManifestDigest: h, InputManifestDigest: h, InvocationDeadline: at.Add(30 * time.Second)}
	parent, err := domain.BuildOwnedAttemptEffectParentV1(final, authority, "retirement-claim", nil, at)
	if err != nil {
		t.Fatal(err)
	}
	prepared := domain.PreparedAllocationV2{Version: domain.PreparedAllocationVersionV2, SubstrateBindingDigest: pkg.Source.SubstrateBindingDigest, ObservedOuterHarnessDigest: p.Substrate.OuterHarnessArtifactDigest, ObservedProxyArtifactDigest: p.Substrate.EgressProxyArtifactDigest, ObservedProxyIdentityDigest: p.Substrate.EgressProxyIdentityDigest, WorkloadMode: p.Substrate.WorkloadMode,
		LocalRuntime: &domain.LocalRuntimeAttestationV2{ConfiguredProfileDigest: p.Backend.BackendProfileDigest, InstallationID: p.InstallationID, InstallationRevision: 1, WorkerExecutableDigest: c.WorkerExecutableDigest, HostIdentityDigest: c.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: c.LocalNetworkNamespaceIdentityDigest, Limits: c.Limits}}
	window := domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at.Add(time.Microsecond), ExpiresAt: at.Add(30 * time.Second)}
	a, err := domain.BuildRegisteredAllocationV2(authority, parent.Reservation, prepared, domain.PreparedInvocationDigestV1(h), parent.PreparationExpiresAt, pkg.Source, "retirement-allocation", 1, window)
	if err != nil {
		t.Fatal(err)
	}
	retired := a.Clone()
	retired.State, retired.RevocationGeneration, retired.UpdatedAt = domain.RegisteredPreparationRetiredV1, 2, at.Add(time.Second)
	reservationDigest, _ := parent.Reservation.DigestForAuthority(authority)
	bd, _ := pkg.Binding.Digest()
	ld, _ := pkg.Binding.AgentLoopPolicy.Digest()
	op := domain.AttemptOperationV1{Version: 1, Scope: domain.AttemptOperationScopeV1{TenantID: scope.TenantID, SessionID: scope.SessionID, OwnerUserID: scope.OwnerUserID, RunID: scope.RunID, AttemptID: scope.AttemptID, LeaseID: authority.Lease.ID, FenceToken: 1, PhysicalInvocationClaimID: parent.Reservation.PhysicalInvocationClaimID, EffectReservationDigest: reservationDigest, InvocationAuthorityDigest: parent.Reservation.InvocationAuthorityDigest, HarnessBindingDigest: bd, AgentLoopPolicyDigest: ld}, Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "retirement-model", RequestRef: "retirement-request", RequestDigest: h, Deadline: at.Add(5 * time.Second), CleanupDuration: 5 * time.Second, RequestBytes: 1, ResultBytes: 1, ContextBytes: 1, Currency: pkg.Cost.Currency, PriceRevision: pkg.Cost.PriceRevision, AdmissionCostCeilingDigest: pkg.Source.AdmissionCostCeilingDigest,
		Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 1, OutputTokens: 1, BodyBytes: 2, ResponseBytes: 1, ActiveDuration: 5 * time.Second}}
	od, err := op.Digest()
	if err != nil {
		t.Fatal(err)
	}
	policy, err := ports.BuildLocalLoopbackEgressPolicyV1(c)
	if err != nil {
		t.Fatal(err)
	}
	transport, _ := policy.TransportDigest()
	peer, _ := policy.LiteralPeerDigest()
	policyDigest, _ := policy.Digest()
	ref := domain.OwnedAgentAuthorityBodyRefV1{Ref: "retirement-structural-body", Digest: h, Bytes: 1}
	proxyWindow := window
	proxyWindow.PublishedAt = at.Add(2 * time.Microsecond)
	proxyWindow.ExpiresAt = op.Deadline
	proxy, err := domain.BuildRegisteredLocalLoopbackProxyAttestationV2(a, pkg.Source, transport, policyDigest, domain.RegisteredLocalLoopbackProxyV2{ConfiguredProfileDigest: p.Backend.BackendProfileDigest, EndpointPolicy: c.EndpointPolicy, ConnectorArtifactDigest: c.WorkerExecutableDigest, InstallationID: p.InstallationID, InstallationRevision: 1, HostIdentityDigest: c.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: c.LocalNetworkNamespaceIdentityDigest, LiteralPeerDigest: peer}, ref, ref, proxyWindow, 1, od)
	if err != nil {
		t.Fatal(err)
	}
	ad, _ := a.Digest()
	pxd, _ := proxy.Digest()
	r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: od, ReservedAt: proxyWindow.PublishedAt, AggregateDebit: op.Debit, RegisteredAllocationDigest: ad, RegisteredProxyDigest: pxd}
	r.ComponentAmounts[2], r.ComponentAmounts[3] = *pkg.Cost.MCPToolCost.MaxAmountMicrounits, *pkg.Cost.WebSearchCost.MaxAmountMicrounits
	if err := r.ValidateForParent(authority, parent.Reservation); err != nil {
		t.Fatal("operation DATA", err)
	}
	v := localRuntimeRetirementData{claim: parent.Reservation.PhysicalInvocationClaimID, lease: string(authority.Lease.ID), previousAllocationDigest: string(ad), operationDigest: string(od), call: op.CallID}
	v.run, v.fence, v.effectSequence, v.reservedAt = string(scope.RunID), parent.Reservation.FenceToken, parent.Reservation.EffectSequence, parent.Reservation.ReservedAt
	encode := func(raw []byte, err error) string {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	v.parent = encode(domain.EncodeOwnedAttemptEffectParentV1(parent))
	v.parentDigest, _ = parent.Digest()
	v.final = encode(domain.EncodeAttemptExecutionAdmissionV1(final))
	v.finalDigest, _ = final.Digest()
	v.originalAllocation = encode(domain.EncodeRegisteredAllocationV2(a))
	v.originalAllocationDigest = string(ad)
	v.currentAllocation = encode(domain.EncodeRegisteredAllocationV2(retired))
	rd, _ := retired.Digest()
	v.currentAllocationDigest = string(rd)
	v.allocationIntent, _ = ports.OwnedAllocationRegistrationIntentDigestV2(a)
	v.originalProxy = encode(domain.EncodeRegisteredProxyAttestationV2(proxy))
	v.currentProxy, v.originalProxyDigest, v.currentProxyDigest = v.originalProxy, string(pxd), string(pxd)
	v.operation = encode(json.Marshal(r))
	dr, _ := r.Digest()
	v.reservationDigest = string(dr)
	return v
}

func TestLocalRuntimeYDBRetirementHistoricalActiveProxy(t *testing.T) {
	v := localRuntimeRetirementFixture(t)
	if err := v.validate("retirement-tenant", "retirement-attempt"); err != nil {
		t.Fatal("valid retired allocation with retained ACTIVE proxy rejected", err)
	}
}

func TestLocalRuntimeYDBRetirementRejectsUncorrelatedHistory(t *testing.T) {
	fixture := localRuntimeRetirementFixture(t)
	encode := func(v any) string {
		t.Helper()
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	allocation := func(v *localRuntimeRetirementData, change func(*domain.RegisteredAllocationV2)) {
		t.Helper()
		a, err := domain.DecodeRegisteredAllocationV2([]byte(v.currentAllocation))
		if err != nil {
			t.Fatal(err)
		}
		change(&a)
		v.currentAllocation = encode(a)
		d, _ := a.Digest()
		v.currentAllocationDigest = string(d)
	}
	proxy := func(v *localRuntimeRetirementData, change func(*domain.RegisteredProxyAttestationV2), both bool) {
		t.Helper()
		p, err := domain.DecodeRegisteredProxyAttestationV2([]byte(v.currentProxy))
		if err != nil {
			t.Fatal(err)
		}
		change(&p)
		v.currentProxy = encode(p)
		d, _ := p.Digest()
		v.currentProxyDigest = string(d)
		if both {
			v.originalProxy, v.originalProxyDigest = v.currentProxy, v.currentProxyDigest
		}
	}
	for _, tc := range []struct {
		name   string
		change func(*localRuntimeRetirementData)
	}{
		{"missing-parent", func(v *localRuntimeRetirementData) { v.parent = "" }},
		{"missing-final", func(v *localRuntimeRetirementData) { v.final = "" }},
		{"missing-original-allocation", func(v *localRuntimeRetirementData) { v.originalAllocation = "" }},
		{"missing-original-proxy", func(v *localRuntimeRetirementData) { v.originalProxy = "" }},
		{"missing-current-proxy", func(v *localRuntimeRetirementData) { v.currentProxy = "" }},
		{"missing-operation", func(v *localRuntimeRetirementData) { v.operation = "" }},
		{"noncanonical-original", func(v *localRuntimeRetirementData) { v.originalAllocation += " " }},
		{"oversized-parent", func(v *localRuntimeRetirementData) { v.parent = strings.Repeat("x", (32<<10)+1) }},
		{"oversized-operation", func(v *localRuntimeRetirementData) { v.operation = strings.Repeat("x", (32<<10)+1) }},
		{"parent-digest", func(v *localRuntimeRetirementData) { v.parentDigest = strings.Repeat("b", 64) }},
		{"final-digest", func(v *localRuntimeRetirementData) { v.finalDigest = strings.Repeat("b", 64) }},
		{"original-allocation-digest", func(v *localRuntimeRetirementData) { v.originalAllocationDigest = strings.Repeat("b", 64) }},
		{"current-allocation-digest", func(v *localRuntimeRetirementData) { v.currentAllocationDigest = strings.Repeat("b", 64) }},
		{"publication-intent", func(v *localRuntimeRetirementData) { v.allocationIntent = strings.Repeat("b", 64) }},
		{"missing-CAS", func(v *localRuntimeRetirementData) { v.previousAllocationDigest = "" }},
		{"stale-CAS", func(v *localRuntimeRetirementData) { v.previousAllocationDigest = v.currentAllocationDigest }},
		{"current-proxy-digest", func(v *localRuntimeRetirementData) { v.currentProxyDigest = strings.Repeat("b", 64) }},
		{"original-proxy-digest", func(v *localRuntimeRetirementData) { v.originalProxyDigest = strings.Repeat("b", 64) }},
		{"active-allocation", func(v *localRuntimeRetirementData) {
			v.currentAllocation, v.currentAllocationDigest = v.originalAllocation, v.originalAllocationDigest
		}},
		{"revoked-not-success-retirement", func(v *localRuntimeRetirementData) {
			allocation(v, func(a *domain.RegisteredAllocationV2) { a.State = domain.RegisteredPreparationRevokedV1 })
		}},
		{"rewritten-allocation", func(v *localRuntimeRetirementData) {
			allocation(v, func(a *domain.RegisteredAllocationV2) { a.AllocationID = "foreign-allocation" })
		}},
		{"divergent-proxy", func(v *localRuntimeRetirementData) {
			proxy(v, func(p *domain.RegisteredProxyAttestationV2) {
				p.State, p.RevocationGeneration, p.UpdatedAt = domain.RegisteredPreparationRetiredV1, 2, p.UpdatedAt.Add(time.Microsecond)
			}, false)
		}},
		{"rewritten-retired-proxy", func(v *localRuntimeRetirementData) {
			proxy(v, func(p *domain.RegisteredProxyAttestationV2) {
				p.State, p.RevocationGeneration, p.UpdatedAt = domain.RegisteredPreparationRetiredV1, 2, p.UpdatedAt.Add(time.Microsecond)
			}, true)
		}},
		{"foreign-proxy-scope", func(v *localRuntimeRetirementData) {
			proxy(v, func(p *domain.RegisteredProxyAttestationV2) { p.Scope.SessionID = "foreign-session" }, true)
		}},
		{"foreign-proxy-claim", func(v *localRuntimeRetirementData) {
			proxy(v, func(p *domain.RegisteredProxyAttestationV2) { p.PhysicalInvocationClaimID = "foreign-claim" }, true)
		}},
		{"foreign-proxy-allocation", func(v *localRuntimeRetirementData) {
			proxy(v, func(p *domain.RegisteredProxyAttestationV2) {
				p.RegisteredAllocationDigest = domain.RegisteredAllocationDigestV2(v.currentAllocationDigest)
			}, true)
		}},
		{"foreign-proxy-source", func(v *localRuntimeRetirementData) {
			proxy(v, func(p *domain.RegisteredProxyAttestationV2) { p.SourceDigest = strings.Repeat("b", 64) }, true)
		}},
		{"foreign-proxy-sequence", func(v *localRuntimeRetirementData) {
			proxy(v, func(p *domain.RegisteredProxyAttestationV2) { p.OperationSequence = 2 }, true)
		}},
		{"foreign-proxy-operation", func(v *localRuntimeRetirementData) {
			proxy(v, func(p *domain.RegisteredProxyAttestationV2) {
				p.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("b", 64))
			}, true)
		}},
		{"foreign-physical-claim", func(v *localRuntimeRetirementData) { v.claim = "foreign-claim" }},
		{"foreign-physical-lease", func(v *localRuntimeRetirementData) { v.lease = "foreign-lease" }},
		{"foreign-run", func(v *localRuntimeRetirementData) { v.run = "foreign-run" }},
		{"foreign-fence", func(v *localRuntimeRetirementData) { v.fence++ }},
		{"foreign-effect-sequence", func(v *localRuntimeRetirementData) { v.effectSequence++ }},
		{"foreign-reservation-time", func(v *localRuntimeRetirementData) { v.reservedAt = v.reservedAt.Add(time.Microsecond) }},
		{"operation-digest", func(v *localRuntimeRetirementData) { v.operationDigest = strings.Repeat("b", 64) }},
		{"operation-reservation-digest", func(v *localRuntimeRetirementData) { v.reservationDigest = strings.Repeat("b", 64) }},
		{"operation-call", func(v *localRuntimeRetirementData) { v.call = "foreign-call" }},
		{"rebound-operation", func(v *localRuntimeRetirementData) {
			var r domain.AttemptOperationReservationV1
			if json.Unmarshal([]byte(v.operation), &r) != nil {
				t.Fatal("operation clone")
			}
			r.Operation.CallID = "foreign-operation"
			r.OperationDigest, _ = r.Operation.Digest()
			d, _ := r.Digest()
			v.operation, v.reservationDigest, v.operationDigest, v.call = encode(r), string(d), string(r.OperationDigest), r.Operation.CallID
		}},
		{"rebound-foreign-source-and-parent", func(v *localRuntimeRetirementData) {
			final, err := domain.DecodeAttemptExecutionAdmissionV1([]byte(v.final))
			if err != nil {
				t.Fatal(err)
			}
			final.Source.Scope.SessionID = "foreign-session"
			for i := range final.Source.Spending {
				s := &final.Source.Spending[i]
				s.SessionID = final.Source.Scope.SessionID
				raw, e := s.SourceCanonicalBytes()
				if e != nil {
					t.Fatal(e)
				}
				s.SourceAuthorization = ports.OwnedProxyBodyReferenceV1("funding", raw)
			}
			if err := final.Validate(); err != nil {
				t.Fatal("foreign Source must be valid DATA", err)
			}
			raw, err := domain.EncodeAttemptExecutionAdmissionV1(final)
			if err != nil {
				t.Fatal(err)
			}
			v.final, v.finalDigest = string(raw), ""
			v.finalDigest, _ = final.Digest()
			parent, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(v.parent))
			if err != nil {
				t.Fatal(err)
			}
			parent.Scope, parent.AdmissionDigest = final.Source.Scope, v.finalDigest
			if err := parent.ValidateForAdmission(final); err != nil {
				t.Fatal("foreign parent/Final must correlate", err)
			}
			raw, err = domain.EncodeOwnedAttemptEffectParentV1(parent)
			if err != nil {
				t.Fatal(err)
			}
			v.parent = string(raw)
			v.parentDigest, _ = parent.Digest()
		}},
	} {
		// Changes are pure setup DATA. Keep setup failures on this parent T;
		// subtests independently assert only their resulting closed oracle input.
		v := fixture
		tc.change(&v)
		t.Run(tc.name, func(t *testing.T) {
			if v.validate("retirement-tenant", "retirement-attempt") == nil {
				t.Fatal("uncorrelated retirement permitted productive ACK")
			}
		})
	}
	for _, scope := range []struct {
		tenant  domain.TenantID
		attempt domain.AttemptID
	}{{"foreign-tenant", "retirement-attempt"}, {"retirement-tenant", "foreign-attempt"}} {
		if fixture.validate(scope.tenant, scope.attempt) == nil {
			t.Fatal("foreign exact-key scope permitted productive ACK")
		}
	}
}
