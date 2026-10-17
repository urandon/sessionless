package domain_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Prepare the synthetic linked connector artifact BEFORE profile/funding/source
// sealing. Older allocation-only fixtures intentionally did not require the
// proxy artifact to be the linked worker, so reusing their sealed graph is wrong.
func localLoopbackAllocationData(t *testing.T) registeredFixture {
	t.Helper()
	p := localSpendingProfileFixture(t, false)
	p.Substrate.EgressProxyArtifactDigest = p.Substrate.OuterHarnessArtifactDigest
	var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	for i, kind := range [4]domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
		spending[i] = localSpendingFixture(t, p, kind)
	}
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID, SessionID: "local-session", RunID: "local-run", AttemptID: "local-attempt"}
	at := p.Window.PublishedAt
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, p, spending, at)
	if err != nil {
		t.Fatal(err)
	}
	a := domain.ServerlessInvocationAuthorityV1{Version: 1, HarnessBinding: pkg.Binding, ExecutionPlacementV2: pkg.Placement, SubstrateBinding: pkg.Substrate, AdmissionCostCeiling: pkg.Cost,
		Lease: domain.Lease{ID: "loopback-lease", TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, WorkerID: "loopback-worker", FenceToken: 3, AcquiredAt: at, ExpiresAt: at.Add(5 * time.Minute)}, ContextManifestDigest: strings.Repeat("1", 64), InputManifestDigest: strings.Repeat("2", 64), InvocationDeadline: at.Add(2 * time.Minute)}
	parent, err := domain.BuildAttemptEffectReservationV1(a, "loopback-claim", nil, at)
	if err != nil {
		t.Fatal(err)
	}
	sd, _ := pkg.Substrate.Digest()
	limits := localInstallationGrantFixture(t).LocalInstallation.Profile.Limits
	prepared := domain.PreparedAllocationV2{Version: domain.PreparedAllocationVersionV2, SubstrateBindingDigest: sd, ObservedOuterHarnessDigest: p.Substrate.OuterHarnessArtifactDigest, ObservedProxyArtifactDigest: p.Substrate.EgressProxyArtifactDigest, ObservedProxyIdentityDigest: p.Substrate.EgressProxyIdentityDigest, WorkloadMode: domain.SubstrateWorkloadInProcessDirectV1,
		LocalRuntime: &domain.LocalRuntimeAttestationV2{ConfiguredProfileDigest: p.Backend.BackendProfileDigest, InstallationID: p.Substrate.LocalInstallationID, InstallationRevision: p.Substrate.LocalInstallationRevision, WorkerExecutableDigest: p.Substrate.OuterHarnessArtifactDigest, HostIdentityDigest: p.Substrate.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: p.Substrate.LocalNetworkNamespaceIdentityDigest, Limits: limits}}
	pd := domain.PreparedInvocationDigestV1(strings.Repeat("8", 64))
	expires := at.Add(time.Minute)
	allocation, err := domain.BuildRegisteredAllocationV2(a, parent, prepared, pd, expires, pkg.Source, "loopback-allocation", 1, domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at.Add(time.Microsecond), ExpiresAt: at.Add(30 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	return registeredFixture{authority: a, parent: parent, prepared: prepared, preparedDigest: pd, preparedExpiresAt: expires, source: pkg.Source, allocation: allocation, at: at}
}

func localProxyData(t *testing.T) (registeredFixture, domain.RegisteredProxyAttestationV2) {
	t.Helper()
	f := localLoopbackAllocationData(t)
	r := f.allocation.LocalRuntime
	hash := strings.Repeat("a", 64)
	o := domain.RegisteredLocalLoopbackProxyV2{ConfiguredProfileDigest: r.ConfiguredProfileDigest, EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, ConnectorArtifactDigest: r.WorkerExecutableDigest, InstallationID: r.InstallationID, InstallationRevision: r.InstallationRevision, HostIdentityDigest: r.HostIdentityDigest, NetworkNamespaceIdentityDigest: r.NetworkNamespaceIdentityDigest, LiteralPeerDigest: hash}
	body := domain.OwnedAgentAuthorityBodyRefV1{Ref: "synthetic-local-body", Digest: hash, Bytes: 100}
	w := domain.OwnedAgentAuthorityWindowV1{ObservedAt: f.allocation.Window.PublishedAt, PublishedAt: f.allocation.Window.PublishedAt.Add(time.Microsecond), ExpiresAt: f.at.Add(10 * time.Second)}
	v, err := domain.BuildRegisteredLocalLoopbackProxyAttestationV2(f.allocation, f.source, hash, f.allocation.Substrate.EgressPolicyDigest, o, body, body, w, 1, domain.AttemptOperationDigestV1(hash))
	if err != nil {
		t.Fatalf("pure local proxy data: %v", err)
	}
	return f, v
}

func TestLocalLoopbackProxyOriginalAllocationCorrelationAndCodec(t *testing.T) {
	f, v := localProxyData(t)
	if err := v.ValidateForAllocation(f.allocation, f.source); err != nil {
		t.Fatal(err)
	}
	if v.GrantDigest != f.source.Spending[0].ScopedGrantDigest || v.ServiceKind != domain.RegisteredProxyProviderV1 || v.PinnedHTTPS != nil {
		t.Fatal("local data inherited external service grant or HTTPS facts")
	}
	raw, err := domain.EncodeRegisteredProxyAttestationV2(v)
	if err != nil {
		t.Fatal(err)
	}
	round, err := domain.DecodeRegisteredProxyAttestationV2(raw)
	if err != nil || round.ValidateForAllocation(f.allocation, f.source) != nil {
		t.Fatalf("local canonical roundtrip: %v", err)
	}
	for _, denied := range []string{"resolution_set_digest", "verified_tls", "proxy_identity_digest"} {
		if bytes.Contains(raw, []byte(denied)) {
			t.Fatalf("fabricated local %s fact", denied)
		}
	}
	clone := v.Clone()
	clone.LocalLoopback.LiteralPeerDigest = strings.Repeat("b", 64)
	if clone.LocalLoopback.LiteralPeerDigest == v.LocalLoopback.LiteralPeerDigest {
		t.Fatal("variant aliases original")
	}
	base, _ := v.Digest()
	changed, _ := clone.Digest()
	if base == changed {
		t.Fatal("literal peer not sealed")
	}
	retired := v.Clone()
	retired.State = domain.RegisteredPreparationRetiredV1
	retired.RevocationGeneration = 2
	retired.UpdatedAt = retired.UpdatedAt.Add(time.Microsecond)
	if retired.ValidateTransitionFrom(v) != nil || retired.ValidateForAllocation(f.allocation, f.source) != nil {
		t.Fatal("historical local retirement lost original correlations")
	}
	retired.LocalLoopback.InstallationRevision++
	if retired.ValidateTransitionFrom(v) == nil {
		t.Fatal("retirement replaced original installation")
	}
	spoof := bytes.Replace(raw, []byte(`"local_loopback":{`), []byte(`"local_loopback":{"verified_tls":true,`), 1)
	if _, err := domain.DecodeRegisteredProxyAttestationV2(spoof); err == nil {
		t.Fatal("unknown local field accepted")
	}
}

func TestLocalLoopbackProxyDeniesWrongRuntimeSourceAndWindow(t *testing.T) {
	f, v := localProxyData(t)
	for name, change := range map[string]func(*domain.RegisteredProxyAttestationV2){
		"config": func(v *domain.RegisteredProxyAttestationV2) {
			v.LocalLoopback.ConfiguredProfileDigest = strings.Repeat("b", 64)
		}, "artifact": func(v *domain.RegisteredProxyAttestationV2) {
			v.LocalLoopback.ConnectorArtifactDigest = strings.Repeat("b", 64)
		}, "installation": func(v *domain.RegisteredProxyAttestationV2) { v.LocalLoopback.InstallationID = "foreign-install" }, "revision": func(v *domain.RegisteredProxyAttestationV2) { v.LocalLoopback.InstallationRevision++ }, "host": func(v *domain.RegisteredProxyAttestationV2) {
			v.LocalLoopback.HostIdentityDigest = strings.Repeat("b", 64)
		}, "net": func(v *domain.RegisteredProxyAttestationV2) {
			v.LocalLoopback.NetworkNamespaceIdentityDigest = strings.Repeat("b", 64)
		}, "scope": func(v *domain.RegisteredProxyAttestationV2) { v.Scope.SessionID = "foreign-session" }, "source": func(v *domain.RegisteredProxyAttestationV2) { v.SourceDigest = strings.Repeat("b", 64) }, "policy": func(v *domain.RegisteredProxyAttestationV2) { v.PolicyDigest = strings.Repeat("b", 64) }, "grant": func(v *domain.RegisteredProxyAttestationV2) { v.GrantDigest = strings.Repeat("b", 64) }, "allocation": func(v *domain.RegisteredProxyAttestationV2) { v.AllocationGeneration++ }, "early": func(v *domain.RegisteredProxyAttestationV2) { v.Window.ObservedAt = f.at.Add(-time.Microsecond) }, "late": func(v *domain.RegisteredProxyAttestationV2) {
			v.Window.ExpiresAt = f.allocation.Window.ExpiresAt.Add(time.Microsecond)
		}, "tool": func(v *domain.RegisteredProxyAttestationV2) { v.ServiceKind = domain.RegisteredProxyMCPToolV1 }, "search": func(v *domain.RegisteredProxyAttestationV2) { v.ServiceKind = domain.RegisteredProxyWebSearchV1 }, "mixed": func(v *domain.RegisteredProxyAttestationV2) { v.PinnedHTTPS = &domain.RegisteredPinnedHTTPSProxyV2{} }, "oversized_ref": func(v *domain.RegisteredProxyAttestationV2) { v.PolicyBody.Bytes = (16 << 10) + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := v.Clone()
			change(&changed)
			if changed.ValidateForAllocation(f.allocation, f.source) == nil {
				t.Fatalf("mismatched local %s accepted", name)
			}
		})
	}
	changedSource := f.source.Clone()
	changedSource.Scope.SessionID = "foreign-session"
	if v.ValidateForAllocation(f.allocation, changedSource) == nil {
		t.Fatal("foreign source accepted")
	}
	image := registeredAllocationFixture(t, false)
	body := v.PolicyBody
	if _, err := domain.BuildRegisteredLocalLoopbackProxyAttestationV2(image.allocation, image.source, v.TransportDigest, image.allocation.Substrate.EgressPolicyDigest, *v.LocalLoopback, body, body, v.Window, 1, v.OperationDigest); err == nil {
		t.Fatal("local observation converted image allocation")
	}
	if _, err := domain.BuildRegisteredPinnedHTTPSProxyAttestationV2(f.allocation, f.source, domain.RegisteredProxyProviderV1, v.TransportDigest, v.PolicyDigest, strings.Repeat("b", 64), body, body, v.Window, 1, v.OperationDigest); err == nil {
		t.Fatal("HTTPS observation converted local allocation")
	}
}
