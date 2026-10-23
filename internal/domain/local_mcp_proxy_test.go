package domain_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestLocalPublicMCPHTTPSProxyBindsOnlyOriginalMCPSpending(t *testing.T) {
	f := localLoopbackAllocationData(t)
	r := f.allocation.LocalRuntime
	hash := strings.Repeat("a", 64)
	obs := domain.RegisteredLocalPublicMCPHTTPSProxyV1{
		ConfiguredProfileDigest: r.ConfiguredProfileDigest, EndpointPolicy: domain.ConfiguredLocalPublicMCPHTTPSPolicyV1,
		ConnectorArtifactDigest: r.WorkerExecutableDigest, InstallationID: r.InstallationID, InstallationRevision: r.InstallationRevision,
		HostIdentityDigest: r.HostIdentityDigest, NetworkNamespaceIdentityDigest: r.NetworkNamespaceIdentityDigest,
		ResolutionSetDigest: hash, MCPProfileDigest: strings.Repeat("b", 64), GrantDigest: f.source.Spending[2].ScopedGrantDigest, DescriptorDigest: strings.Repeat("c", 64),
	}
	body := domain.OwnedAgentAuthorityBodyRefV1{Ref: "local-mcp-body", Digest: hash, Bytes: 128}
	w := domain.OwnedAgentAuthorityWindowV1{ObservedAt: f.allocation.Window.PublishedAt, PublishedAt: f.allocation.Window.PublishedAt.Add(time.Microsecond), ExpiresAt: f.at.Add(10 * time.Second)}
	v, err := domain.BuildRegisteredLocalPublicMCPHTTPSProxyAttestationV1(f.allocation, f.source, hash, strings.Repeat("d", 64), obs, body, body, w, 2, domain.AttemptOperationDigestV1(hash))
	if err != nil {
		t.Fatalf("local MCP observation: %v", err)
	}
	if v.TransportKind != domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1 || v.ServiceKind != domain.RegisteredProxyMCPToolV1 || v.GrantDigest != f.source.Spending[2].ScopedGrantDigest || v.PinnedHTTPS != nil || v.LocalLoopback != nil {
		t.Fatal("local MCP transport did not preserve the closed MCP-only arm")
	}
	if err := v.ValidateForAllocation(f.allocation, f.source); err != nil {
		t.Fatal(err)
	}

	bad := v.Clone()
	bad.LocalPublicMCPHTTPS.GrantDigest = f.source.Spending[0].ScopedGrantDigest
	if err := bad.ValidateForAllocation(f.allocation, f.source); err == nil {
		t.Fatal("provider grant impersonated local MCP")
	}
	bad = v.Clone()
	bad.ServiceKind = domain.RegisteredProxyProviderV1
	if err := bad.Validate(); err == nil {
		t.Fatal("local MCP transport accepted provider service")
	}
	bad = v.Clone()
	bad.TransportKind = domain.RegisteredProxyTransportLocalLoopbackHTTPV2
	if err := bad.Validate(); err == nil {
		t.Fatal("local MCP observation was reinterpreted as provider loopback")
	}
}
