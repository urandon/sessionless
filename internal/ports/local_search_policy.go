package ports

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/strictjson"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// LocalSearchTransportV1 is an actual synchronous owned transport, not a
// readiness probe or approval callback. Construction retains this SAME keeper;
// the original entry/Store still authenticate preparation, receipt and send.
type LocalSearchTransportV1 interface {
	PreflightSearch(context.Context, domain.RegisteredAllocationV2, domain.AttemptExecutionSourceV1, domain.AttemptOperationV1, *websearch.FrozenRequestV2, SearchInvocationCredentialV1, []byte) (LocalSearchConnectionV1, error)
}

// A connection and its mutable key-bearing copies must join before the original
// synchronous materialization callback returns. No connection may escape it.
type LocalSearchConnectionV1 interface {
	Publication() (OwnedProxyPublicationV2, error)
	InvokeSearch(context.Context, domain.RegisteredProxyAttestationV2, AttemptOperationCapabilityV1, []byte) (LocalSearchTransportResultV1, error)
	Close() error
}

type LocalSearchTransportResultV1 struct {
	Body                        []byte
	Metadata                    websearch.HTTPMetadataV1
	RequestBytes, ResponseBytes uint64
	ObservedAt                  time.Time
	SendAttempted               bool
}

// This pure builder describes the fixed existing endpoint/contract. It creates
// no DNS/TLS observation, connection, credential, current grant or send right.
func BuildLocalSearchHTTPSProxyPolicyV1(c domain.ConfiguredInferenceProfileV1, g domain.WebSearchResourceGrantV2) (LocalSearchHTTPSProxyPolicyV1, error) {
	if c.Validate() != nil || g.Validate() != nil || c.TenantID != g.TenantID || c.OwnerUserID != g.OwnerUserID {
		return LocalSearchHTTPSProxyPolicyV1{}, domain.ErrMembershipDenied
	}
	digest, err := c.Digest()
	p := LocalSearchHTTPSProxyPolicyV1{Version: 1, Grant: g, ConfiguredProfileDigest: digest, ConnectorArtifactDigest: c.WorkerExecutableDigest, HostIdentityDigest: c.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: c.LocalNetworkNamespaceIdentityDigest, MaxOperationTimeout: min(c.Limits.OperationTimeout, 15*time.Second), AutomaticRetriesDenied: true, ResumptionDenied: true}
	if err != nil || p.Validate() != nil {
		return LocalSearchHTTPSProxyPolicyV1{}, domain.ErrMembershipDenied
	}
	return p, nil
}

// Immutable policy DATA only. The original resource grant owns the endpoint, API,
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

// One canonical strict DATA decoder shared by historical and current producer
// correspondence. Decoding never authenticates the actual transport owner.
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
