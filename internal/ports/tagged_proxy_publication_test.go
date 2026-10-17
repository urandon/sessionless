package ports_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func taggedProxyPublicationFixture(t *testing.T) ports.OwnedProxyPublicationV2 {
	t.Helper()
	hash := strings.Repeat("a", 64)
	at := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	policy := ports.ServerlessEgressPolicyV1{Version: 1, RoutePolicyDigest: hash, EffectivePolicyDigest: hash, ProxyArtifactDigest: hash, ProxyIdentityDigest: hash,
		Route:    domain.ProviderRouteV1{BackendKind: domain.HarnessBackendDirectOpenRouterV1, ModelVendorID: "synthetic-vendor", ModelID: "synthetic-model", TransportKind: domain.ProviderTransportRouterAPIV1, TransportProvider: "openrouter", UpstreamProviderID: "synthetic", EndpointID: "synthetic-endpoint", BillingKind: domain.ProviderBillingRouterAccountV1, BillingAuthority: "synthetic-payer"},
		Endpoint: ports.ServerlessEgressEndpointPolicyV1{Scheme: "https", Host: "openrouter.ai", Port: 443, Path: "/api/v1/chat/completions", Method: "POST", DNSClass: ports.ServerlessEgressDNSPublicUnicastOnlyV1, RedirectPolicy: ports.ServerlessEgressRedirectDenyV1, MaxRequestBytes: 4096, MaxResponseBytes: 4096}}
	pd, err := policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	a := ports.ServerlessEgressProxyAttestationV1{PolicyDigest: pd, ProxyArtifactDigest: hash, ProxyIdentityDigest: hash, ResolvedHost: "openrouter.ai", ResolvedPort: 443, ResolvedDNSClass: ports.ServerlessEgressDNSPublicUnicastOnlyV1, ResolutionSetDigest: hash, ConnectionAddressPinned: true, DNSRebindingDenied: true, RedirectsDenied: true, AmbientProxyDenied: true, CertificateValidationRequired: true, ExpiresAt: at.Add(time.Hour)}
	pb, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	ab, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	op := operationPortFixture(t).Reservation.Operation
	op.Deadline = at.Add(time.Minute)
	od, err := op.Digest()
	if err != nil {
		t.Fatal(err)
	}
	p := ports.OwnedProxyPublicationV2{TransportKind: domain.RegisteredProxyTransportPinnedHTTPSV2, OperationSequence: 1, OperationDigest: od, ServiceKind: domain.RegisteredProxyProviderV1, Scope: domain.AttemptExecutionSourceScopeV1{TenantID: "synthetic-tenant", OwnerUserID: "synthetic-owner", SessionID: "synthetic-session", RunID: "synthetic-run", AttemptID: "synthetic-attempt"}, LeaseID: "synthetic-lease", FenceToken: 1, PhysicalInvocationClaimID: "synthetic-claim", RegisteredAllocationDigest: domain.RegisteredAllocationDigestV2(hash), SourceDigest: hash, ObservedAt: at, PolicyBody: pb, AttestationBody: ab, Authenticator: bytes.Repeat([]byte{1}, 32)}
	p.OperationDeadline = op.Deadline
	if err := p.Validate(); err != nil {
		t.Fatalf("scripted HTTPS publication DATA: %v", err)
	}
	return p
}

func TestTaggedProxyPublicationHTTPSBodiesAndExactIntent(t *testing.T) {
	t.Parallel()
	p := taggedProxyPublicationFixture(t)
	raw, err := p.IntentBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte("sessionless.actual-owned-proxy-observation.v2")) || !bytes.Contains(raw, []byte("pinned_https.v1")) || !bytes.Contains(raw, []byte(p.OperationDigest)) {
		t.Fatal("publication intent omitted exact version/tag/whole-operation digest")
	}
	for name, mutate := range map[string]func(*ports.OwnedProxyPublicationV2){
		"sequence": func(v *ports.OwnedProxyPublicationV2) { v.OperationSequence++ },
		"operation": func(v *ports.OwnedProxyPublicationV2) {
			v.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("b", 64))
		},
		"claim": func(v *ports.OwnedProxyPublicationV2) { v.PhysicalInvocationClaimID = "another-claim" },
		"deadline": func(v *ports.OwnedProxyPublicationV2) {
			v.OperationDeadline = v.OperationDeadline.Add(time.Microsecond)
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := p.Clone()
			mutate(&v)
			changed, err := v.IntentBytes()
			if err != nil || bytes.Equal(raw, changed) {
				t.Fatalf("valid %s change unsealed: %v", name, err)
			}
		})
	}
	clone := p.Clone()
	clone.PolicyBody[0] = 'x'
	clone.AttestationBody[0] = 'x'
	clone.Authenticator[0]++
	again, err := p.IntentBytes()
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatalf("publication clone mutated original bodies: %v", err)
	}
	if _, err := json.Marshal(p); err == nil {
		t.Fatal("process-local observation serialized as approval")
	}
	var decoded ports.OwnedProxyPublicationV2
	if err := json.Unmarshal([]byte(`{}`), &decoded); err == nil {
		t.Fatal("caller JSON minted observation")
	}
}

func TestTaggedProxyPublicationRejectsUninstalledLocalOrMixedBodies(t *testing.T) {
	t.Parallel()
	base := taggedProxyPublicationFixture(t)
	for name, mutate := range map[string]func(*ports.OwnedProxyPublicationV2){
		"untagged": func(v *ports.OwnedProxyPublicationV2) { v.TransportKind = "" },
		"local_with_https_bodies": func(v *ports.OwnedProxyPublicationV2) {
			v.TransportKind = domain.RegisteredProxyTransportLocalLoopbackHTTPV2
		},
		"unknown":             func(v *ports.OwnedProxyPublicationV2) { v.TransportKind = "arbitrary-http" },
		"missing_operation":   func(v *ports.OwnedProxyPublicationV2) { v.OperationDigest = "" },
		"missing_deadline":    func(v *ports.OwnedProxyPublicationV2) { v.OperationDeadline = time.Time{} },
		"expired_deadline":    func(v *ports.OwnedProxyPublicationV2) { v.OperationDeadline = v.ObservedAt },
		"nanosecond_deadline": func(v *ports.OwnedProxyPublicationV2) { v.OperationDeadline = v.OperationDeadline.Add(time.Nanosecond) },
		"zero_sequence":       func(v *ports.OwnedProxyPublicationV2) { v.OperationSequence = 0 },
		"overflow_sequence":   func(v *ports.OwnedProxyPublicationV2) { v.OperationSequence = 13 },
		"mixed_local_policy": func(v *ports.OwnedProxyPublicationV2) {
			v.PolicyBody = bytes.Replace(v.PolicyBody, []byte(`"scheme":"https"`), []byte(`"scheme":"http"`), 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := base.Clone()
			mutate(&v)
			if v.Validate() == nil {
				t.Fatal("unsupported tagged publication accepted")
			}
		})
	}
}
