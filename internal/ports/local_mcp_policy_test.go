package ports_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func localMCPConfiguredProfileFixture(t *testing.T) domain.ConfiguredInferenceProfileV1 {
	t.Helper()
	h := strings.Repeat("1", 64)
	ref := domain.OwnedAgentAuthorityBodyRefV1{Ref: "synthetic-local-evidence", Digest: h, Bytes: 32}
	p := domain.ConfiguredInferenceProfileV1{
		Version: 1, TenantID: "tenant-synthetic", OwnerUserID: "owner-synthetic", ProfileRevision: 1,
		Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceLocalEndpointV1, ResourceID: "synthetic-local-resource", OwnerUserID: "owner-synthetic", Revision: 1, CredentialMode: domain.ProviderCredentialNoneV1},
		BaseURL:  "http://127.0.0.1:11434/v1", EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, NativeProtocolVersion: domain.ConfiguredInferenceProtocolChatCompletionsV1,
		Auth: domain.ConfiguredInferenceAuthNoneV1, ModelVendorID: "synthetic-vendor", ModelID: "synthetic-model", ResponseModelIDs: []string{"synthetic-model"}, ServerID: "synthetic-server", ServerVersion: "synthetic-version", ChatTemplateDigest: h,
		Capabilities:       domain.ConfiguredInferenceCapabilitiesV1{Text: domain.ProviderEvidenceSupportedV1, Image: domain.ProviderEvidenceUnsupportedV1, File: domain.ProviderEvidenceUnsupportedV1, ToolCalls: domain.ProviderEvidenceSupportedV1, JSONSchema: domain.ProviderEvidenceUnknownV1},
		CapabilityEvidence: ref, ProviderCatalogDigest: h, ProviderRouteDigest: h, PrivacyPolicyDigest: h, EffectivePolicyDigest: h, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPublicV1},
		ModelWindowTokens: 4096, TokenEstimatorVersion: "synthetic-estimator", CalibrationDigest: h,
		Limits:              domain.ConfiguredInferenceLimitsV1{RequestBytes: 1 << 20, ResponseBytes: 1 << 20, ResponseHeaderBytes: 16 << 10, JSONDepth: 32, JSONMembers: 256, Choices: 1, ToolCalls: 1, ToolNameBytes: 128, ToolCallIDBytes: 128, ToolArgumentsBytes: 4 << 10, FinalTextBytes: 256 << 10, Messages: 256, Tools: 16, MaxInputTokens: 1024, MaxOutputTokens: 1024, OperationTimeout: 30 * time.Second, CleanupTimeout: 30 * time.Second},
		LocalInstallationID: "synthetic-installation", LocalInstallationRevision: 1, LocalHostIdentityDigest: h, LocalNetworkNamespaceIdentityDigest: h, WorkerExecutableDigest: h,
		OperatorTerms: ref, OriginalSource: ref, IssuedAt: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC), ExpiresAt: time.Date(2030, 1, 2, 4, 4, 5, 0, time.UTC),
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("configured profile fixture: %v", err)
	}
	return p
}

func localMCPGrantFixture(t *testing.T) mcpadmission.MCPToolGrantV1 {
	t.Helper()
	at := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	g := mcpadmission.MCPToolGrantV1{Version: 1, TenantID: "tenant-synthetic", OwnerUserID: "owner-synthetic", BillingOwnerUserID: "owner-synthetic", ResourceID: "docs-resource", ResourceRevision: 2, RevocationGeneration: 3,
		ProfileID: "docs-profile", ProfileRevision: 4, DataPolicyDigest: strings.Repeat("a", 64), DataClass: "public", ConsentMode: "owner_public_query", ConsentRevision: 5,
		Endpoint: ports.LocalPublicMCPHTTSEndpointV1, Transport: "streamable_http", ProtocolVersion: mcpwire.ProtocolVersionV1, SchemaValidator: domain.ConfiguredToolSchemaValidatorV1,
		ModelDescription: "Synthetic public documentation lookup", Descriptor: json.RawMessage(`{"inputSchema":{"additionalProperties":false,"properties":{"query":{"maxLength":100,"minLength":1,"type":"string"}},"required":["query"],"type":"object"},"name":"search_openai_docs"}`),
		Limits: mcpwire.LimitsV1{MessageBytes: 64 << 10, AggregateBytes: 256 << 10, JSONDepth: 32, JSONMembers: 4096, SSELineBytes: 64 << 10, SSEEventBytes: 64 << 10, SSEEvents: 128, Tools: 1, DescriptorBytes: 32 << 10, ArgumentBytes: 4096, ResultTextBytes: 64 << 10, ContentItems: 16}, IssuedAt: at, ExpiresAt: at.Add(time.Hour)}
	if err := g.Validate(); err != nil {
		t.Fatalf("MCP grant fixture: %v", err)
	}
	return g
}

func localMCPPolicyFixture() ports.LocalPublicMCPHTTPSProxyPolicyV1 {
	h := strings.Repeat("1", 64)
	return ports.LocalPublicMCPHTTPSProxyPolicyV1{
		Version: 1, Endpoint: ports.LocalPublicMCPHTTSEndpointV1, EndpointPolicy: domain.ConfiguredLocalPublicMCPHTTPSPolicyV1,
		ProtocolVersion: mcpwire.ProtocolVersionV1, MCPProfileID: "docs-profile", MCPProfileRevision: 1, MCPProfileDigest: h, GrantDigest: strings.Repeat("2", 64), DescriptorDigest: strings.Repeat("3", 64),
		ConfiguredProfileDigest: strings.Repeat("4", 64), ConnectorArtifactDigest: strings.Repeat("5", 64), HostIdentityDigest: strings.Repeat("6", 64), NetworkNamespaceIdentityDigest: strings.Repeat("7", 64),
		Limits:  mcpwire.LimitsV1{MessageBytes: 64 << 10, AggregateBytes: 256 << 10, JSONDepth: 64, JSONMembers: 4096, SSELineBytes: 64 << 10, SSEEventBytes: 64 << 10, SSEEvents: 128, Tools: 1, DescriptorBytes: 64 << 10, ArgumentBytes: 4 << 10, ResultTextBytes: 64 << 10, ContentItems: 128},
		Methods: []string{"POST"}, StatelessOnly: true, AutomaticRetriesDenied: true, ResumptionDenied: true, MaxOperationTimeout: uint64(30 * time.Second),
	}
}

func TestLocalPublicMCPHTTPSPolicyAndPublicationAreClosedAndBounded(t *testing.T) {
	p := localMCPPolicyFixture()
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	transport, err := p.TransportDigest()
	if err != nil || transport == digest {
		t.Fatal("policy/transport digest separation failed")
	}
	at := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	obs := domain.RegisteredLocalPublicMCPHTTPSProxyV1{
		ConfiguredProfileDigest: p.ConfiguredProfileDigest, EndpointPolicy: p.EndpointPolicy, ConnectorArtifactDigest: p.ConnectorArtifactDigest,
		InstallationID: "synthetic-installation", InstallationRevision: 1, HostIdentityDigest: p.HostIdentityDigest,
		NetworkNamespaceIdentityDigest: p.NetworkNamespaceIdentityDigest, ResolutionSetDigest: strings.Repeat("8", 64), MCPProfileDigest: p.MCPProfileDigest,
		GrantDigest: p.GrantDigest, DescriptorDigest: p.DescriptorDigest,
	}
	att := ports.LocalPublicMCPHTTPSProxyAttestationV1{Version: 1, PolicyDigest: digest, Observation: obs, ConnectionAddressPinned: true, DNSRebindingDenied: true, PublicUnicastOnly: true, RedirectsDenied: true, AmbientProxyDenied: true, CertificateValidationRequired: true, TLSHandshakeVerified: true, VerifiedSNI: "developers.openai.com", ResolvedHost: "developers.openai.com", ResolvedPort: 443, ObservedAt: at, ExpiresAt: at.Add(20 * time.Second)}
	if err := att.ValidateForPolicy(p, at, at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	policyBody, _ := json.Marshal(p)
	attestationBody, _ := json.Marshal(att)
	pub := taggedProxyPublicationFixture(t)
	pub.TransportKind = domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1
	pub.ServiceKind = domain.RegisteredProxyMCPToolV1
	pub.OperationSequence = 2
	pub.ObservedAt = at
	pub.OperationDeadline = at.Add(10 * time.Second)
	pub.PolicyBody, pub.AttestationBody = policyBody, attestationBody
	if _, err := pub.Bodies(); err != nil {
		t.Fatalf("canonical local MCP body readback: %v", err)
	}
	if err := pub.Validate(); err != nil {
		t.Fatalf("local MCP publication: %v", err)
	}

	for name, change := range map[string]func(*ports.LocalPublicMCPHTTPSProxyPolicyV1){
		"endpoint": func(v *ports.LocalPublicMCPHTTPSProxyPolicyV1) { v.Endpoint = "https://example.com/mcp" },
		"method":   func(v *ports.LocalPublicMCPHTTPSProxyPolicyV1) { v.Methods = []string{"GET"} },
		"stateful": func(v *ports.LocalPublicMCPHTTPSProxyPolicyV1) { v.StatelessOnly = false },
		"retry":    func(v *ports.LocalPublicMCPHTTPSProxyPolicyV1) { v.AutomaticRetriesDenied = false },
	} {
		t.Run(name, func(t *testing.T) {
			bad := p
			bad.Methods = append([]string(nil), p.Methods...)
			change(&bad)
			if bad.Validate() == nil {
				t.Fatal("invalid MCP proxy policy accepted")
			}
		})
	}
	badAtt := att
	badAtt.ConnectionAddressPinned = false
	if badAtt.ValidateForPolicy(p, at, at.Add(time.Minute)) == nil {
		t.Fatal("missing pinned-connection evidence accepted")
	}
	badAtt = att
	badAtt.TLSHandshakeVerified = false
	if badAtt.ValidateForPolicy(p, at, at.Add(time.Minute)) == nil {
		t.Fatal("unverified TLS handshake accepted")
	}
	badAtt = att
	badAtt.Observation.GrantDigest = strings.Repeat("9", 64)
	if badAtt.ValidateForPolicy(p, at, at.Add(time.Minute)) == nil {
		t.Fatal("observation with a different grant accepted")
	}
	badPub := pub
	badPub.TransportKind = domain.RegisteredProxyTransportLocalLoopbackHTTPV2
	if _, err := badPub.Bodies(); err == nil {
		t.Fatal("local MCP bodies impersonated provider loopback")
	}
}

func TestLocalPublicMCPHTTPSPolicyBuildsFromOriginalConfiguredProfileAndGrant(t *testing.T) {
	config, grant := localMCPConfiguredProfileFixture(t), localMCPGrantFixture(t)
	p, err := ports.BuildLocalPublicMCPHTTPSProxyPolicyV1(config, grant)
	if err != nil {
		t.Fatal(err)
	}
	configDigest, _ := config.Digest()
	grantDigest, _ := grant.Digest()
	if p.Endpoint != ports.LocalPublicMCPHTTSEndpointV1 || p.ConfiguredProfileDigest != configDigest || p.GrantDigest != grantDigest || p.MCPProfileID != grant.ProfileID || p.MCPProfileRevision != grant.ProfileRevision || p.Methods[0] != "POST" || !p.StatelessOnly || !p.AutomaticRetriesDenied || !p.ResumptionDenied {
		t.Fatal("policy did not preserve the exact configured profile and retained grant as stateless POST-only data")
	}
	wrong := config
	wrong.TenantID = "another-tenant"
	if _, err := ports.BuildLocalPublicMCPHTTPSProxyPolicyV1(wrong, grant); err == nil {
		t.Fatal("cross-tenant configured profile accepted")
	}
	wrongGrant := grant
	wrongGrant.OwnerUserID = "another-owner"
	if _, err := ports.BuildLocalPublicMCPHTTPSProxyPolicyV1(config, wrongGrant); err == nil {
		t.Fatal("cross-owner retained grant accepted")
	}
}
