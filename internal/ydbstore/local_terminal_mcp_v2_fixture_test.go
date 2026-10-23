package ydbstore

import (
	"database/sql/driver"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// InstallTerminalMCPV2ProxyData supplies only scripted durable proxy rows for
// the constructor-created original entry. It does not publish a transport
// witness, call the network, replace an owner, or attest physical DNS/TLS.
func (s *LocalTerminalPublicationSessionConnectedSeed) InstallTerminalMCPV2ProxyData(t *testing.T, entry *LocalInvocationEntryV1, op domain.AttemptOperationV1) {
	t.Helper()
	parent, pe := entry.Parent()
	source, se := entry.Source()
	allocation, ae := entry.Allocation()
	if pe != nil || se != nil || ae != nil || op.ValidateForParent(parent.Authority, parent.Reservation) != nil || source.Scope != s.Scope || op.Scope != ownedOperationScope(parent) || allocation.LocalRuntime == nil || s.compaction == nil {
		t.Fatal("scripted proxy row requires SAME original entry/seed operation", pe, se, ae)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	var publication ports.OwnedProxyPublicationV2
	runtime := allocation.LocalRuntime
	switch op.Kind {
	case domain.AttemptOperationModelV1, domain.AttemptOperationCompactionV1:
		policy, err := ports.BuildLocalLoopbackEgressPolicyV1(s.Profile)
		if err != nil {
			t.Fatal(err)
		}
		pd, _ := policy.Digest()
		peer, _ := policy.LiteralPeerDigest()
		att := ports.LocalLoopbackEgressAttestationV1{Version: 1, PolicyDigest: pd, ObservedAt: at, ExpiresAt: op.Deadline, Observation: domain.RegisteredLocalLoopbackProxyV2{ConfiguredProfileDigest: runtime.ConfiguredProfileDigest, EndpointPolicy: policy.EndpointPolicy, ConnectorArtifactDigest: runtime.WorkerExecutableDigest, InstallationID: runtime.InstallationID, InstallationRevision: runtime.InstallationRevision, HostIdentityDigest: runtime.HostIdentityDigest, NetworkNamespaceIdentityDigest: runtime.NetworkNamespaceIdentityDigest, LiteralPeerDigest: peer}}
		pb, _ := json.Marshal(policy)
		ab, _ := json.Marshal(att)
		publication = ports.OwnedProxyPublicationV2{TransportKind: domain.RegisteredProxyTransportLocalLoopbackHTTPV2, ServiceKind: domain.RegisteredProxyProviderV1, ObservedAt: at, PolicyBody: pb, AttestationBody: ab}
	case domain.AttemptOperationMCPToolV1:
		grant, err := s.Source.ReadMCPToolGrant(t.Context(), source.Profile, at)
		if err != nil {
			t.Fatal("original retained grant for scripted MCP proxy DATA", err)
		}
		policy, err := ports.BuildLocalPublicMCPHTTPSProxyPolicyV1(s.Profile, grant)
		if err != nil {
			t.Fatal(err)
		}
		pd, _ := policy.Digest()
		att := ports.LocalPublicMCPHTTPSProxyAttestationV1{Version: 1, PolicyDigest: pd, Observation: domain.RegisteredLocalPublicMCPHTTPSProxyV1{ConfiguredProfileDigest: runtime.ConfiguredProfileDigest, EndpointPolicy: policy.EndpointPolicy, ConnectorArtifactDigest: runtime.WorkerExecutableDigest, InstallationID: runtime.InstallationID, InstallationRevision: runtime.InstallationRevision, HostIdentityDigest: runtime.HostIdentityDigest, NetworkNamespaceIdentityDigest: runtime.NetworkNamespaceIdentityDigest, ResolutionSetDigest: strings.Repeat("9", 64), MCPProfileDigest: policy.MCPProfileDigest, GrantDigest: policy.GrantDigest, DescriptorDigest: policy.DescriptorDigest}, ConnectionAddressPinned: true, DNSRebindingDenied: true, PublicUnicastOnly: true, RedirectsDenied: true, AmbientProxyDenied: true, CertificateValidationRequired: true, TLSHandshakeVerified: true, VerifiedSNI: "developers.openai.com", ResolvedHost: "developers.openai.com", ResolvedPort: 443, ObservedAt: at, ExpiresAt: op.Deadline}
		pb, _ := json.Marshal(policy)
		ab, _ := json.Marshal(att)
		publication = ports.OwnedProxyPublicationV2{TransportKind: domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1, ServiceKind: domain.RegisteredProxyMCPToolV1, ObservedAt: at, PolicyBody: pb, AttestationBody: ab}
	default:
		t.Fatal("only exact MODEL/COMPACTION/MCP proxy DATA is supported")
	}
	bodies, err := publication.Bodies()
	if err != nil {
		t.Fatal(err)
	}
	od, _ := op.Digest()
	proxy, err := buildOwnedProxyRecord(bodies, allocation, source, publication.ServiceKind, publication.PolicyBody, publication.AttestationBody, domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: op.Deadline}, op.Sequence, od)
	if err != nil {
		t.Fatal("scripted bounded original proxy row", err)
	}
	raw, _ := domain.EncodeRegisteredProxyAttestationV2(proxy)
	digest, _ := proxy.Digest()
	row := []driver.Value{uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), uint64(1), string(raw), string(digest), string(raw), string(digest), string(publication.PolicyBody), string(publication.AttestationBody)}
	if op.Sequence == 1 {
		s.sql.row = row
	} else {
		s.compaction.proxies = append(s.compaction.proxies, append(append([]driver.Value{op.Sequence}, row...), string(publication.ServiceKind)))
	}
}
