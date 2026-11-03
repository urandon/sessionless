package domain_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestLocalSearchProxyClosedOriginalRuntimeAndSearchSpending(t *testing.T) {
	f := localLoopbackAllocationData(t)
	r := f.allocation.LocalRuntime
	d := strings.Repeat("a", 64)
	o := domain.RegisteredLocalSearchHTTPSProxyV1{ConfiguredProfileDigest: r.ConfiguredProfileDigest, EndpointPolicy: string(domain.RegisteredProxyTransportLocalSearchHTTPSV1), ConnectorArtifactDigest: r.WorkerExecutableDigest, InstallationID: r.InstallationID, InstallationRevision: r.InstallationRevision, HostIdentityDigest: r.HostIdentityDigest, NetworkNamespaceIdentityDigest: r.NetworkNamespaceIdentityDigest, ResolutionSetDigest: d}
	b := domain.OwnedAgentAuthorityBodyRefV1{Ref: "search-proxy-data", Digest: d, Bytes: 128}
	w := domain.OwnedAgentAuthorityWindowV1{ObservedAt: f.allocation.Window.PublishedAt, PublishedAt: f.allocation.Window.PublishedAt, ExpiresAt: f.at.Add(10 * time.Second)}
	v, err := domain.BuildRegisteredLocalSearchHTTPSProxyAttestationV1(f.allocation, f.source, d, d, o, b, b, w, 2, domain.AttemptOperationDigestV1(d))
	if err != nil {
		t.Fatal("SEARCH-only structural DATA", err)
	}
	if v.ValidateForAllocation(f.allocation, f.source) != nil || v.ServiceKind != domain.RegisteredProxyWebSearchV1 || v.GrantDigest != f.source.Spending[3].ScopedGrantDigest {
		t.Fatal("SEARCH lost original runtime/spending correlation")
	}
	raw, err := domain.EncodeRegisteredProxyAttestationV2(v)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := domain.DecodeRegisteredProxyAttestationV2(raw)
	if err != nil || decoded.ValidateForAllocation(f.allocation, f.source) != nil {
		t.Fatal("closed canonical roundtrip", err)
	}
	clone := v.Clone()
	clone.LocalSearchHTTPS.InstallationRevision++
	if v.LocalSearchHTTPS.InstallationRevision == clone.LocalSearchHTTPS.InstallationRevision {
		t.Fatal("clone aliases original SEARCH observation")
	}
	for _, mutation := range []string{"provider", "mcp", "pinned", "loopback", "mixed_mcp", "mixed_https", "grant", "operation", "host", "installation", "window"} {
		bad := v.Clone()
		switch mutation {
		case "provider":
			bad.ServiceKind = domain.RegisteredProxyProviderV1
		case "mcp":
			bad.ServiceKind = domain.RegisteredProxyMCPToolV1
		case "pinned":
			bad.TransportKind = domain.RegisteredProxyTransportPinnedHTTPSV2
		case "loopback":
			bad.TransportKind = domain.RegisteredProxyTransportLocalLoopbackHTTPV2
		case "mixed_mcp":
			bad.LocalPublicMCPHTTPS = &domain.RegisteredLocalPublicMCPHTTPSProxyV1{}
		case "mixed_https":
			bad.PinnedHTTPS = &domain.RegisteredPinnedHTTPSProxyV2{}
		case "grant":
			bad.GrantDigest = f.source.Spending[0].ScopedGrantDigest
		case "operation":
			bad.OperationSequence = 1
		case "host":
			bad.LocalSearchHTTPS.HostIdentityDigest = strings.Repeat("b", 64)
		case "installation":
			bad.LocalSearchHTTPS.InstallationRevision++
		case "window":
			bad.Window.ExpiresAt = f.allocation.Window.ExpiresAt.Add(time.Microsecond)
		}
		if bad.ValidateForAllocation(f.allocation, f.source) == nil {
			t.Fatalf("crossed %s admitted", mutation)
		}
	}
	for _, raw := range [][]byte{bytes.Replace(raw, []byte(`"local_search_https":{`), []byte(`"local_search_https":null,"unused":{`), 1), append(bytes.Clone(raw), ' '), bytes.Replace(raw, []byte(`"endpoint_policy":`), []byte(`"Endpoint_policy":`), 1)} {
		if _, err := domain.DecodeRegisteredProxyAttestationV2(raw); err == nil {
			t.Fatal("noncanonical SEARCH record accepted")
		}
	}
	_, old := localProxyData(t)
	oldBytes, _ := domain.EncodeRegisteredProxyAttestationV2(old)
	old.LocalSearchHTTPS = &o
	if old.Validate() == nil {
		t.Fatal("new field widened old provider arm")
	}
	if bytes.Contains(oldBytes, []byte("local_search_https")) {
		t.Fatal("nil SEARCH field changed old bytes")
	}
}
