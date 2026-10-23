package ports

import (
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
)

const LocalPublicMCPHTTSEndpointV1 = "https://developers.openai.com/mcp"

// LocalPublicMCPHTTPSProxyPolicyV1 is installation DATA for one exact public
// MCP endpoint and one original configured local model profile. It does not
// prove that the proxy enforces these limits or authorize a request.
type LocalPublicMCPHTTPSProxyPolicyV1 struct {
	Version                        uint32           `json:"version"`
	Endpoint                       string           `json:"endpoint"`
	EndpointPolicy                 string           `json:"endpoint_policy"`
	ProtocolVersion                string           `json:"protocol_version"`
	MCPProfileID                   string           `json:"mcp_profile_id"`
	MCPProfileRevision             uint64           `json:"mcp_profile_revision"`
	MCPProfileDigest               string           `json:"mcp_profile_digest"`
	GrantDigest                    string           `json:"grant_digest"`
	DescriptorDigest               string           `json:"descriptor_digest"`
	ConfiguredProfileDigest        string           `json:"configured_profile_digest"`
	ConnectorArtifactDigest        string           `json:"connector_artifact_digest"`
	HostIdentityDigest             string           `json:"host_identity_digest"`
	NetworkNamespaceIdentityDigest string           `json:"network_namespace_identity_digest"`
	Limits                         mcpwire.LimitsV1 `json:"limits"`
	Methods                        []string         `json:"methods"`
	StatelessOnly                  bool             `json:"stateless_only"`
	AutomaticRetriesDenied         bool             `json:"automatic_retries_denied"`
	ResumptionDenied               bool             `json:"resumption_denied"`
	MaxOperationTimeout            uint64           `json:"max_operation_timeout_nanos"`
}

func localMCPProfileDigest(grant mcpadmission.MCPToolGrantV1) (string, error) {
	return loopbackDigest("sessionless.local-public-mcp-profile.v1", struct {
		ProfileID string `json:"profile_id"`
		Revision  uint64 `json:"profile_revision"`
	}{grant.ProfileID, grant.ProfileRevision})
}

func BuildLocalPublicMCPHTTPSProxyPolicyV1(config domain.ConfiguredInferenceProfileV1, grant mcpadmission.MCPToolGrantV1) (LocalPublicMCPHTTPSProxyPolicyV1, error) {
	configDigest, configErr := config.Digest()
	grantDigest, grantErr := grant.Digest()
	profileDigest, profileErr := localMCPProfileDigest(grant)
	descriptor, descriptorErr := mcpwire.ParseDescriptorV1(grant.Descriptor, grant.Limits)
	if configErr != nil || grantErr != nil || profileErr != nil || descriptorErr != nil || grant.TenantID != config.TenantID || grant.OwnerUserID != config.OwnerUserID || grant.Limits.Validate() != nil {
		return LocalPublicMCPHTTPSProxyPolicyV1{}, domain.ErrMembershipDenied
	}
	p := LocalPublicMCPHTTPSProxyPolicyV1{
		Version: 1, Endpoint: LocalPublicMCPHTTSEndpointV1, EndpointPolicy: domain.ConfiguredLocalPublicMCPHTTPSPolicyV1,
		ProtocolVersion: grant.ProtocolVersion, MCPProfileID: grant.ProfileID, MCPProfileRevision: grant.ProfileRevision, MCPProfileDigest: profileDigest, GrantDigest: grantDigest, DescriptorDigest: descriptor.Digest(),
		ConfiguredProfileDigest: configDigest, ConnectorArtifactDigest: config.WorkerExecutableDigest, HostIdentityDigest: config.LocalHostIdentityDigest,
		NetworkNamespaceIdentityDigest: config.LocalNetworkNamespaceIdentityDigest, Limits: grant.Limits,
		Methods: []string{"POST"}, StatelessOnly: true, AutomaticRetriesDenied: true, ResumptionDenied: true, MaxOperationTimeout: uint64(config.Limits.OperationTimeout),
	}
	if p.Validate() != nil {
		return LocalPublicMCPHTTPSProxyPolicyV1{}, domain.ErrMembershipDenied
	}
	return p, nil
}

func (p LocalPublicMCPHTTPSProxyPolicyV1) Validate() error {
	if p.Version != 1 || p.Endpoint != LocalPublicMCPHTTSEndpointV1 || p.EndpointPolicy != domain.ConfiguredLocalPublicMCPHTTPSPolicyV1 ||
		p.ProtocolVersion != mcpwire.ProtocolVersionV1 || p.MCPProfileRevision == 0 || domain.ValidateOpaqueID("local_public_mcp.profile", p.MCPProfileID) != nil || p.Limits.Validate() != nil || p.MaxOperationTimeout == 0 || p.MaxOperationTimeout > uint64(30*time.Second) ||
		!p.StatelessOnly || !p.AutomaticRetriesDenied || !p.ResumptionDenied || len(p.Methods) != 1 || p.Methods[0] != "POST" {
		return domain.ErrMembershipDenied
	}
	for _, digest := range []string{p.MCPProfileDigest, p.GrantDigest, p.DescriptorDigest, p.ConfiguredProfileDigest, p.ConnectorArtifactDigest, p.HostIdentityDigest, p.NetworkNamespaceIdentityDigest} {
		if domain.ValidateSHA256Digest("local_public_mcp.policy", digest) != nil {
			return domain.ErrMembershipDenied
		}
	}
	return nil
}

func (p LocalPublicMCPHTTPSProxyPolicyV1) Digest() (string, error) {
	if p.Validate() != nil {
		return "", domain.ErrMembershipDenied
	}
	return loopbackDigest("sessionless.local-public-mcp-https-policy.v1", p)
}

func (p LocalPublicMCPHTTPSProxyPolicyV1) TransportDigest() (string, error) {
	if p.Validate() != nil {
		return "", domain.ErrMembershipDenied
	}
	return loopbackDigest("sessionless.local-public-mcp-https-transport.v1", struct {
		Endpoint   string   `json:"endpoint"`
		Protocol   string   `json:"protocol"`
		Methods    []string `json:"methods"`
		Grant      string   `json:"grant"`
		Descriptor string   `json:"descriptor"`
	}{p.Endpoint, p.ProtocolVersion, p.Methods, p.GrantDigest, p.DescriptorDigest})
}

// LocalPublicMCPHTTPSProxyAttestationV1 is DATA reported by the exact local
// proxy producer; structural validation is not cryptographic or socket proof.
type LocalPublicMCPHTTPSProxyAttestationV1 struct {
	Version                       uint32                                      `json:"version"`
	PolicyDigest                  string                                      `json:"policy_digest"`
	Observation                   domain.RegisteredLocalPublicMCPHTTPSProxyV1 `json:"observation"`
	ConnectionAddressPinned       bool                                        `json:"connection_address_pinned"`
	DNSRebindingDenied            bool                                        `json:"dns_rebinding_denied"`
	PublicUnicastOnly             bool                                        `json:"public_unicast_only"`
	RedirectsDenied               bool                                        `json:"redirects_denied"`
	AmbientProxyDenied            bool                                        `json:"ambient_proxy_denied"`
	CertificateValidationRequired bool                                        `json:"certificate_validation_required"`
	TLSHandshakeVerified          bool                                        `json:"tls_handshake_verified"`
	VerifiedSNI                   string                                      `json:"verified_sni"`
	ResolvedHost                  string                                      `json:"resolved_host"`
	ResolvedPort                  uint16                                      `json:"resolved_port"`
	ObservedAt                    time.Time                                   `json:"observed_at"`
	ExpiresAt                     time.Time                                   `json:"expires_at"`
}

func (a LocalPublicMCPHTTPSProxyAttestationV1) ValidateForPolicy(p LocalPublicMCPHTTPSProxyPolicyV1, now, authorityExpiresAt time.Time) error {
	digest, err := p.Digest()
	o := a.Observation
	window := domain.OwnedAgentAuthorityWindowV1{ObservedAt: a.ObservedAt, PublishedAt: a.ObservedAt, ExpiresAt: a.ExpiresAt}
	if err != nil || a.Version != 1 || a.PolicyDigest != digest || window.ValidateAt(now.UTC()) != nil || authorityExpiresAt.IsZero() || a.ExpiresAt.After(authorityExpiresAt) || a.ExpiresAt.Sub(a.ObservedAt) > time.Duration(p.MaxOperationTimeout) ||
		!a.ConnectionAddressPinned || !a.DNSRebindingDenied || !a.PublicUnicastOnly || !a.RedirectsDenied || !a.AmbientProxyDenied || !a.CertificateValidationRequired || !a.TLSHandshakeVerified ||
		a.VerifiedSNI != "developers.openai.com" || a.ResolvedHost != "developers.openai.com" || a.ResolvedPort != 443 ||
		o.ConfiguredProfileDigest != p.ConfiguredProfileDigest || o.EndpointPolicy != p.EndpointPolicy || o.ConnectorArtifactDigest != p.ConnectorArtifactDigest ||
		o.HostIdentityDigest != p.HostIdentityDigest || o.NetworkNamespaceIdentityDigest != p.NetworkNamespaceIdentityDigest || o.GrantDigest != p.GrantDigest ||
		o.DescriptorDigest != p.DescriptorDigest || o.MCPProfileDigest != p.MCPProfileDigest || o.ResolutionSetDigest == "" ||
		domain.ValidateSHA256Digest("local_public_mcp.resolution", o.ResolutionSetDigest) != nil || domain.ValidateOpaqueID("local_public_mcp.installation", o.InstallationID) != nil || o.InstallationRevision == 0 {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (a LocalPublicMCPHTTPSProxyAttestationV1) MarshalCanonical() ([]byte, error) {
	if a.Version != 1 {
		return nil, domain.ErrMembershipDenied
	}
	return json.Marshal(a)
}
