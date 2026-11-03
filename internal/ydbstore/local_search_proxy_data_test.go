package ydbstore

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Pure stored policy/attestation DATA. These synthetic booleans and digests do
// not assert actual DNS/TLS, publication issuance or a live retained connection.
func failureSearchProxyData(t *testing.T, allocation domain.RegisteredAllocationV2, source domain.AttemptExecutionSourceV1, config domain.ConfiguredInferenceProfileV1, grant domain.WebSearchResourceGrantV2, op domain.AttemptOperationV1, at time.Time) (domain.RegisteredProxyAttestationV2, []byte, []byte) {
	t.Helper()
	r := allocation.LocalRuntime
	od, err := op.Digest()
	if err != nil {
		t.Fatal(err)
	}
	w := domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: op.Deadline}
	if op.Kind != domain.AttemptOperationWebSearchV1 {
		p, err := ports.BuildLocalLoopbackEgressPolicyV1(config)
		if err != nil {
			t.Fatal(err)
		}
		pd, _ := p.Digest()
		peer, _ := p.LiteralPeerDigest()
		td, _ := p.TransportDigest()
		a := ports.LocalLoopbackEgressAttestationV1{Version: 1, PolicyDigest: pd, Observation: domain.RegisteredLocalLoopbackProxyV2{ConfiguredProfileDigest: r.ConfiguredProfileDigest, EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, ConnectorArtifactDigest: r.WorkerExecutableDigest, InstallationID: r.InstallationID, InstallationRevision: r.InstallationRevision, HostIdentityDigest: r.HostIdentityDigest, NetworkNamespaceIdentityDigest: r.NetworkNamespaceIdentityDigest, LiteralPeerDigest: peer}, ObservedAt: at, ExpiresAt: op.Deadline}
		pb, _ := json.Marshal(p)
		ab, _ := json.Marshal(a)
		proxy, err := domain.BuildRegisteredLocalLoopbackProxyAttestationV2(allocation, source, td, pd, a.Observation, ports.OwnedProxyBodyReferenceV1("policy", pb), ports.OwnedProxyBodyReferenceV1("attestation", ab), w, op.Sequence, od)
		if err != nil {
			t.Fatal("provider proxy DATA", err)
		}
		return proxy, pb, ab
	}
	p := ports.LocalSearchHTTPSProxyPolicyV1{Version: 1, Grant: grant, ConfiguredProfileDigest: r.ConfiguredProfileDigest, ConnectorArtifactDigest: r.WorkerExecutableDigest, HostIdentityDigest: r.HostIdentityDigest, NetworkNamespaceIdentityDigest: r.NetworkNamespaceIdentityDigest, MaxOperationTimeout: 15 * time.Second, AutomaticRetriesDenied: true, ResumptionDenied: true}
	pd, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	td, _ := p.TransportDigest()
	a := ports.LocalSearchHTTPSProxyAttestationV1{Version: 1, PolicyDigest: pd, Observation: domain.RegisteredLocalSearchHTTPSProxyV1{ConfiguredProfileDigest: r.ConfiguredProfileDigest, EndpointPolicy: string(domain.RegisteredProxyTransportLocalSearchHTTPSV1), ConnectorArtifactDigest: r.WorkerExecutableDigest, InstallationID: r.InstallationID, InstallationRevision: r.InstallationRevision, HostIdentityDigest: r.HostIdentityDigest, NetworkNamespaceIdentityDigest: r.NetworkNamespaceIdentityDigest, ResolutionSetDigest: strings.Repeat("a", 64)}, ConnectionAddressPinned: true, DNSRebindingDenied: true, PublicUnicastOnly: true, RedirectsDenied: true, AmbientProxyDenied: true, CertificateValidationRequired: true, TLSHandshakeVerified: true, VerifiedSNI: "api.tavily.com", ResolvedHost: "api.tavily.com", ResolvedPort: 443, ObservedAt: at, ExpiresAt: op.Deadline}
	pb, _ := json.Marshal(p)
	ab, _ := json.Marshal(a)
	proxy, err := domain.BuildRegisteredLocalSearchHTTPSProxyAttestationV1(allocation, source, td, pd, a.Observation, ports.OwnedProxyBodyReferenceV1("policy", pb), ports.OwnedProxyBodyReferenceV1("attestation", ab), w, op.Sequence, od)
	if err != nil {
		t.Fatal("SEARCH proxy DATA", err)
	}
	return proxy, pb, ab
}

func TestLocalSearchProxyDataCannotPublishThroughOriginalReadyStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		actual, _ := localPreparedModelFixture(t, 0)
		if !actual.store.validOwnedAllocationContext(t.Context()) || actual.store.ownedProxyPublicationVerifier == nil || actual.parent.request.Validate() != nil {
			t.Fatal("original Store readiness/request prerequisite would mask unsupported publication")
		}
		data := failureSearchV4Data(t, domain.SearchOutcomeUnknownV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
		proxy := data.proxies[1]
		if proxy.ValidateForAllocation(data.history.Original, data.history.Admission.Source) != nil {
			t.Fatal("new closed domain proxy prerequisite")
		}
		p := ports.OwnedProxyPublicationV2{TransportKind: proxy.TransportKind, OperationSequence: proxy.OperationSequence, OperationDigest: proxy.OperationDigest, OperationDeadline: proxy.Window.ExpiresAt, ServiceKind: proxy.ServiceKind, Scope: proxy.Scope, LeaseID: data.history.Parent.Reservation.LeaseID, FenceToken: data.history.Parent.Reservation.FenceToken, PhysicalInvocationClaimID: proxy.PhysicalInvocationClaimID, RegisteredAllocationDigest: proxy.RegisteredAllocationDigest, SourceDigest: proxy.SourceDigest, ObservedAt: proxy.Window.ObservedAt, PolicyBody: data.proxyPolicies[1], AttestationBody: data.proxyAttestations[1], Authenticator: bytes.Repeat([]byte{1}, 32)}
		// This failure is the explicit absent serving-tag arm, independently
		// asserted before asking the original ready Store to publish anything.
		if _, err := p.Bodies(); err == nil {
			t.Fatal("historical body decoder widened serving publication")
		}
		queries, writes, blobs := actual.queries.statements, actual.parent.graph.writes, actual.canonical.calls
		out, replayed, err := actual.store.PublishOwnedProxy(t.Context(), actual.parent.request, p)
		if err == nil || replayed || out.Version != 0 || actual.queries.statements != queries || actual.parent.graph.writes != writes || actual.canonical.calls != blobs {
			t.Fatal("unsupported SEARCH DATA reached publication SQL/effects", err, replayed, actual.queries.statements-queries, actual.parent.graph.writes-writes, actual.canonical.calls-blobs)
		}
	})
}

func TestLocalSearchProxyDataOriginalReadAndFreshPublicationDenial(t *testing.T) {
	f := failureSearchV4Data(t, domain.SearchOutcomeUnknownV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
	proxy, pb, ab := f.proxies[1], f.proxyPolicies[1], f.proxyAttestations[1]
	expected, err := readLocalSearchProxyDataV1(proxy, f.history.Parent, f.history.Original, f.history.Admission.Source, pb, ab)
	if err != nil {
		t.Fatal("exact historical body correlation", err)
	}
	got, _ := expected.Digest()
	want, _ := proxy.Digest()
	if got != want {
		t.Fatal("historical reader changed proxy commitment")
	}
	for _, mutation := range []string{"body", "policy", "grant", "allocation", "source"} {
		bad := proxy.Clone()
		badpb, badab := bytes.Clone(pb), bytes.Clone(ab)
		parent := f.history.Parent.Clone()
		allocation := f.history.Original.Clone()
		source := f.history.Admission.Source.Clone()
		switch mutation {
		case "body":
			badab = bytes.Replace(badab, []byte(`"tls_handshake_verified":true`), []byte(`"tls_handshake_verified":false`), 1)
		case "policy":
			bad.PolicyDigest = strings.Repeat("b", 64)
		case "grant":
			bad.GrantDigest = strings.Repeat("b", 64)
		case "allocation":
			allocation.AllocationID = "other-allocation"
		case "source":
			source.Scope.SessionID = "other-session"
		}
		out, e := readLocalSearchProxyDataV1(bad, parent, allocation, source, badpb, badab)
		if e == nil {
			actual, de := out.Digest()
			original, oe := bad.Digest()
			if de == nil && oe == nil && actual == original {
				t.Fatalf("crossed proxy %s accepted", mutation)
			}
		}
	}
	// Public observation DTOs remain unsupported for this tag, even with exact
	// valid historical DATA and a full-sized invented authenticator.
	p := ports.OwnedProxyPublicationV2{TransportKind: proxy.TransportKind, OperationSequence: proxy.OperationSequence, OperationDigest: proxy.OperationDigest, OperationDeadline: proxy.Window.ExpiresAt, ServiceKind: proxy.ServiceKind, Scope: proxy.Scope, LeaseID: f.history.Parent.Reservation.LeaseID, FenceToken: f.history.Parent.Reservation.FenceToken, PhysicalInvocationClaimID: proxy.PhysicalInvocationClaimID, RegisteredAllocationDigest: proxy.RegisteredAllocationDigest, SourceDigest: proxy.SourceDigest, ObservedAt: proxy.Window.ObservedAt, PolicyBody: pb, AttestationBody: ab, Authenticator: bytes.Repeat([]byte{1}, 32)}
	if p.Validate() == nil {
		t.Fatal("historical SEARCH DATA became a serving producer")
	}
	snapshot := &ownedProxySnapshot{original: proxy, current: proxy, policyBody: pb, attestationBody: ab}
	r := f.capsule.Operations[1].Reservation
	if err := validateRetainedSearchProxyOperationV4(snapshot, r); err != nil {
		t.Fatal("actual SEARCH reservation/proxy/grant compiler correlation", err)
	}
	for _, mode := range []string{"compiler", "api_profile", "arguments", "deadline"} {
		bad := r.Clone()
		switch mode {
		case "compiler":
			bad.Operation.SearchWire.CompilerArtifactDigest = strings.Repeat("b", 64)
		case "api_profile":
			bad.Operation.SearchWire.APIProfile = "tavily.extract.v1"
		case "arguments":
			bad.Operation.SearchWire.ArgumentsBytes = domain.WebSearchArgumentsBytesV1 + 1
		case "deadline":
			bad.Operation.Deadline = bad.Operation.Deadline.Add(time.Microsecond)
		}
		if validateRetainedSearchProxyOperationV4(snapshot, bad) == nil {
			t.Fatalf("direct original reservation/proxy correlation %s admitted", mode)
		}
	}
}
