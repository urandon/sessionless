package ports

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// LocalLoopbackEgressPolicyV1 is independent installation DATA, not a socket
// observation or network permission. No freshly minted resource/installation,
// profile digest or expiry may enter this acyclic policy.
type LocalLoopbackEgressPolicyV1 struct {
	Version                        uint32                             `json:"version"`
	BaseURL                        string                             `json:"base_url"`
	EndpointPolicy                 string                             `json:"endpoint_policy"`
	NativeProtocolVersion          string                             `json:"native_protocol_version"`
	Auth                           domain.ConfiguredInferenceAuthV1   `json:"auth"`
	ConnectorArtifactDigest        string                             `json:"connector_artifact_digest"`
	HostIdentityDigest             string                             `json:"host_identity_digest"`
	NetworkNamespaceIdentityDigest string                             `json:"network_namespace_identity_digest"`
	Limits                         domain.ConfiguredInferenceLimitsV1 `json:"limits"`
}

func BuildLocalLoopbackEgressPolicyV1(config domain.ConfiguredInferenceProfileV1) (LocalLoopbackEgressPolicyV1, error) {
	p := LocalLoopbackEgressPolicyV1{Version: 1, BaseURL: config.BaseURL, EndpointPolicy: config.EndpointPolicy, NativeProtocolVersion: config.NativeProtocolVersion, Auth: config.Auth,
		ConnectorArtifactDigest: config.WorkerExecutableDigest, HostIdentityDigest: config.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: config.LocalNetworkNamespaceIdentityDigest, Limits: config.Limits}
	if err := p.Validate(); err != nil {
		return LocalLoopbackEgressPolicyV1{}, err
	}
	return p, nil
}
func (p LocalLoopbackEgressPolicyV1) Validate() error {
	if p.Version != 1 || p.EndpointPolicy != domain.ConfiguredInferenceLoopbackPolicyV1 || p.NativeProtocolVersion != domain.ConfiguredInferenceProtocolChatCompletionsV1 ||
		(p.Auth != domain.ConfiguredInferenceAuthNoneV1 && p.Auth != domain.ConfiguredInferenceAuthBearerV1) || domain.ValidateConfiguredInferenceEndpointV1(p.BaseURL) != nil || p.Limits.Validate() != nil {
		return domain.ErrMembershipDenied
	}
	for _, d := range []string{p.ConnectorArtifactDigest, p.HostIdentityDigest, p.NetworkNamespaceIdentityDigest} {
		if domain.ValidateSHA256Digest("local_loopback.policy", d) != nil {
			return domain.ErrMembershipDenied
		}
	}
	return nil
}
func loopbackDigest(namespace string, v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", domain.ErrMembershipDenied
	}
	h := sha256.New()
	_, _ = h.Write([]byte(namespace))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)), nil
}
func (p LocalLoopbackEgressPolicyV1) Digest() (string, error) {
	if p.Validate() != nil {
		return "", domain.ErrMembershipDenied
	}
	return loopbackDigest("sessionless.local-loopback-egress-policy.v1", p)
}
func (p LocalLoopbackEgressPolicyV1) LiteralPeerDigest() (string, error) {
	if p.Validate() != nil {
		return "", domain.ErrMembershipDenied
	}
	// Validation recognized this exact producer text. No DNS, URL
	// normalization, address aliases or endpoint discovery occurs here.
	peer := strings.TrimSuffix(strings.TrimPrefix(p.BaseURL, "http://"), "/v1")
	return loopbackDigest("sessionless.local-loopback-literal-peer.v1", peer)
}
func (p LocalLoopbackEgressPolicyV1) TransportDigest() (string, error) {
	if p.Validate() != nil {
		return "", domain.ErrMembershipDenied
	}
	return loopbackDigest("sessionless.local-loopback-transport.v1", struct{ Method, Path, BaseURL, NativeProtocolVersion string }{"POST", "/v1/chat/completions", p.BaseURL, p.NativeProtocolVersion})
}

// LocalLoopbackEgressAttestationV1 is canonical observation DATA. Its intrinsic
// correlations do not prove a real retained peer, issuer, source or Store join.
// No DNS or TLS facts are present.
type LocalLoopbackEgressAttestationV1 struct {
	Version      uint32                                `json:"version"`
	PolicyDigest string                                `json:"policy_digest"`
	Observation  domain.RegisteredLocalLoopbackProxyV2 `json:"observation"`
	ObservedAt   time.Time                             `json:"observed_at"`
	ExpiresAt    time.Time                             `json:"expires_at"`
}

func (a LocalLoopbackEgressAttestationV1) ValidateForPolicy(p LocalLoopbackEgressPolicyV1) error {
	digest, err := p.Digest()
	peer, peerErr := p.LiteralPeerDigest()
	o := a.Observation
	window := domain.OwnedAgentAuthorityWindowV1{ObservedAt: a.ObservedAt, PublishedAt: a.ObservedAt, ExpiresAt: a.ExpiresAt}
	if err != nil || peerErr != nil || a.Version != 1 || a.PolicyDigest != digest || window.Validate() != nil || a.ExpiresAt.Sub(a.ObservedAt) > p.Limits.OperationTimeout ||
		o.EndpointPolicy != p.EndpointPolicy || o.ConnectorArtifactDigest != p.ConnectorArtifactDigest || o.HostIdentityDigest != p.HostIdentityDigest || o.NetworkNamespaceIdentityDigest != p.NetworkNamespaceIdentityDigest || o.LiteralPeerDigest != peer ||
		domain.ValidateSHA256Digest("local_loopback.configured_profile", o.ConfiguredProfileDigest) != nil || domain.ValidateOpaqueID("local_loopback.installation", o.InstallationID) != nil || o.InstallationRevision == 0 {
		return domain.ErrMembershipDenied
	}
	return nil
}

type OwnedPinnedHTTPSBodiesV2 struct {
	Policy      ServerlessEgressPolicyV1
	Attestation ServerlessEgressProxyAttestationV1
}
type OwnedLocalLoopbackBodiesV2 struct {
	Policy      LocalLoopbackEgressPolicyV1
	Attestation LocalLoopbackEgressAttestationV1
}
type OwnedLocalPublicMCPHTTPSBodiesV1 struct {
	Policy      LocalPublicMCPHTTPSProxyPolicyV1
	Attestation LocalPublicMCPHTTPSProxyAttestationV1
}

// OwnedProxyBodiesV2 is a closed, exclusive DATA union. A local observation can
// never be interpreted as HTTPS, nor can a transport tag manufacture a producer.
type OwnedProxyBodiesV2 struct {
	TransportKind       domain.RegisteredProxyTransportKindV2
	PinnedHTTPS         *OwnedPinnedHTTPSBodiesV2
	LocalLoopback       *OwnedLocalLoopbackBodiesV2
	LocalPublicMCPHTTPS *OwnedLocalPublicMCPHTTPSBodiesV1
}

func (b OwnedProxyBodiesV2) Validate() error {
	switch b.TransportKind {
	case domain.RegisteredProxyTransportPinnedHTTPSV2:
		if b.PinnedHTTPS == nil || b.LocalLoopback != nil || b.LocalPublicMCPHTTPS != nil {
			return domain.ErrMembershipDenied
		}
		a, p := b.PinnedHTTPS.Attestation, b.PinnedHTTPS.Policy
		// Existing HTTPS validation owns these semantics; no local policy is
		// substituted. Contextual publication time is checked by Bodies().
		if a.ValidateForPolicy(p, a.ExpiresAt.Add(-time.Nanosecond), a.ExpiresAt) != nil {
			return domain.ErrMembershipDenied
		}
	case domain.RegisteredProxyTransportLocalLoopbackHTTPV2:
		if b.LocalLoopback == nil || b.PinnedHTTPS != nil || b.LocalPublicMCPHTTPS != nil || b.LocalLoopback.Attestation.ValidateForPolicy(b.LocalLoopback.Policy) != nil {
			return domain.ErrMembershipDenied
		}
	case domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1:
		if b.LocalPublicMCPHTTPS == nil || b.PinnedHTTPS != nil || b.LocalLoopback != nil || b.LocalPublicMCPHTTPS.Policy.Validate() != nil ||
			b.LocalPublicMCPHTTPS.Attestation.ValidateForPolicy(b.LocalPublicMCPHTTPS.Policy, b.LocalPublicMCPHTTPS.Attestation.ObservedAt, b.LocalPublicMCPHTTPS.Attestation.ExpiresAt) != nil {
			return domain.ErrMembershipDenied
		}
	default:
		return domain.ErrMembershipDenied
	}
	return nil
}
func (b OwnedProxyBodiesV2) PolicyDigest() (string, error) {
	if b.Validate() != nil {
		return "", domain.ErrMembershipDenied
	}
	switch b.TransportKind {
	case domain.RegisteredProxyTransportPinnedHTTPSV2:
		return b.PinnedHTTPS.Policy.Digest()
	case domain.RegisteredProxyTransportLocalLoopbackHTTPV2:
		return b.LocalLoopback.Policy.Digest()
	case domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1:
		return b.LocalPublicMCPHTTPS.Policy.Digest()
	}
	return "", domain.ErrMembershipDenied
}
func (b OwnedProxyBodiesV2) ExpiresAt() time.Time {
	if b.Validate() != nil {
		return time.Time{}
	}
	switch b.TransportKind {
	case domain.RegisteredProxyTransportPinnedHTTPSV2:
		return b.PinnedHTTPS.Attestation.ExpiresAt
	case domain.RegisteredProxyTransportLocalLoopbackHTTPV2:
		return b.LocalLoopback.Attestation.ExpiresAt
	case domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1:
		return b.LocalPublicMCPHTTPS.Attestation.ExpiresAt
	}
	return time.Time{}
}
