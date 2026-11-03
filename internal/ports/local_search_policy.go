package ports

import (
	"bytes"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

// Historical DATA only. The original resource grant owns the endpoint, API,
// compiler/schema, resource and payer tuples; no second endpoint selector exists.
type LocalSearchHTTPSProxyPolicyV1 struct {
	Version                        uint32                          `json:"version"`
	Grant                          domain.WebSearchResourceGrantV2 `json:"grant"`
	ConfiguredProfileDigest        string                          `json:"configured_profile_digest"`
	ConnectorArtifactDigest        string                          `json:"connector_artifact_digest"`
	HostIdentityDigest             string                          `json:"host_identity_digest"`
	NetworkNamespaceIdentityDigest string                          `json:"network_namespace_identity_digest"`
	MaxOperationTimeout            time.Duration                   `json:"max_operation_timeout_nanos"`
	AutomaticRetriesDenied         bool                            `json:"automatic_retries_denied"`
	ResumptionDenied               bool                            `json:"resumption_denied"`
}

func (p LocalSearchHTTPSProxyPolicyV1) Validate() error {
	if p.Version != 1 || p.Grant.Validate() != nil || p.MaxOperationTimeout <= 0 || p.MaxOperationTimeout > 15*time.Second || !p.AutomaticRetriesDenied || !p.ResumptionDenied {
		return domain.ErrMembershipDenied
	}
	for _, d := range []string{p.ConfiguredProfileDigest, p.ConnectorArtifactDigest, p.HostIdentityDigest, p.NetworkNamespaceIdentityDigest} {
		if domain.ValidateSHA256Digest("search_proxy.policy", d) != nil {
			return domain.ErrMembershipDenied
		}
	}
	return nil
}
func (p LocalSearchHTTPSProxyPolicyV1) Digest() (string, error) {
	if p.Validate() != nil {
		return "", domain.ErrMembershipDenied
	}
	return loopbackDigest("sessionless.local-search-https-policy.v1", p)
}
func (p LocalSearchHTTPSProxyPolicyV1) TransportDigest() (string, error) {
	if p.Validate() != nil {
		return "", domain.ErrMembershipDenied
	}
	gd, _ := p.Grant.Digest()
	return loopbackDigest("sessionless.local-search-https-transport.v1", struct {
		Grant         string
		APIProfile    string
		Endpoint      string
		Method        string
		RequestBytes  uint64
		ResponseBytes uint64
	}{gd, p.Grant.APIProfile, p.Grant.Endpoint, p.Grant.Method, domain.WebSearchWireBytesV1, domain.WebSearchResponseBytesV1})
}

// Synthetic booleans/hashes authenticate no actual DNS/TLS enforcement. The
// future original transport producer must own these facts before publication.
type LocalSearchHTTPSProxyAttestationV1 struct {
	Version                       uint32                                   `json:"version"`
	PolicyDigest                  string                                   `json:"policy_digest"`
	Observation                   domain.RegisteredLocalSearchHTTPSProxyV1 `json:"observation"`
	ConnectionAddressPinned       bool                                     `json:"connection_address_pinned"`
	DNSRebindingDenied            bool                                     `json:"dns_rebinding_denied"`
	PublicUnicastOnly             bool                                     `json:"public_unicast_only"`
	RedirectsDenied               bool                                     `json:"redirects_denied"`
	AmbientProxyDenied            bool                                     `json:"ambient_proxy_denied"`
	CertificateValidationRequired bool                                     `json:"certificate_validation_required"`
	TLSHandshakeVerified          bool                                     `json:"tls_handshake_verified"`
	VerifiedSNI                   string                                   `json:"verified_sni"`
	ResolvedHost                  string                                   `json:"resolved_host"`
	ResolvedPort                  uint16                                   `json:"resolved_port"`
	ObservedAt                    time.Time                                `json:"observed_at"`
	ExpiresAt                     time.Time                                `json:"expires_at"`
}

func (a LocalSearchHTTPSProxyAttestationV1) ValidateForPolicy(p LocalSearchHTTPSProxyPolicyV1) error {
	pd, err := p.Digest()
	o := a.Observation
	w := domain.OwnedAgentAuthorityWindowV1{ObservedAt: a.ObservedAt, PublishedAt: a.ObservedAt, ExpiresAt: a.ExpiresAt}
	if err != nil || a.Version != 1 || a.PolicyDigest != pd || w.Validate() != nil || a.ObservedAt.Before(p.Grant.IssuedAt) || a.ExpiresAt.After(p.Grant.ExpiresAt) || a.ExpiresAt.Sub(a.ObservedAt) > p.MaxOperationTimeout || !a.ConnectionAddressPinned || !a.DNSRebindingDenied || !a.PublicUnicastOnly || !a.RedirectsDenied || !a.AmbientProxyDenied || !a.CertificateValidationRequired || !a.TLSHandshakeVerified || a.VerifiedSNI != "api.tavily.com" || a.ResolvedHost != "api.tavily.com" || a.ResolvedPort != 443 || o.EndpointPolicy != string(domain.RegisteredProxyTransportLocalSearchHTTPSV1) || o.ConfiguredProfileDigest != p.ConfiguredProfileDigest || o.ConnectorArtifactDigest != p.ConnectorArtifactDigest || o.HostIdentityDigest != p.HostIdentityDigest || o.NetworkNamespaceIdentityDigest != p.NetworkNamespaceIdentityDigest || domain.ValidateOpaqueID("search_proxy.installation", o.InstallationID) != nil || o.InstallationRevision == 0 || domain.ValidateSHA256Digest("search_proxy.resolution", o.ResolutionSetDigest) != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}

// Separate historical decoder: intentionally NOT an arm of the serving
// OwnedProxyPublicationV2 codec/union, which continues to reject this tag.
func DecodeLocalSearchHTTPSProxyDataV1(policyBody, attestationBody []byte) (LocalSearchHTTPSProxyPolicyV1, LocalSearchHTTPSProxyAttestationV1, error) {
	var p LocalSearchHTTPSProxyPolicyV1
	var a LocalSearchHTTPSProxyAttestationV1
	decode := func(raw []byte, v any) bool {
		if _, err := strictjson.Parse(raw, 16<<10, 32, 256); err != nil {
			return false
		}
		d := json.NewDecoder(bytes.NewReader(raw))
		d.DisallowUnknownFields()
		if d.Decode(v) != nil {
			return false
		}
		canonical, err := json.Marshal(v)
		return err == nil && bytes.Equal(canonical, raw)
	}
	if !decode(policyBody, &p) || !decode(attestationBody, &a) || a.ValidateForPolicy(p) != nil {
		return LocalSearchHTTPSProxyPolicyV1{}, LocalSearchHTTPSProxyAttestationV1{}, domain.ErrMembershipDenied
	}
	return p, a, nil
}
