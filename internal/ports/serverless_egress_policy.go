// Canonical egress policy and actual connection observation codecs.
// This dependency-neutral contract performs no network I/O or approval.
package ports

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"gitcode.com/urandon/sessionless/internal/domain"
	"net"
	"net/url"
	"strings"
	"time"
)

const ServerlessEgressPolicyVersionV1 uint32 = 1

type (
	ServerlessEgressDNSClassV1       string
	ServerlessEgressRedirectPolicyV1 string
)

const (
	ServerlessEgressDNSPublicUnicastOnlyV1 ServerlessEgressDNSClassV1       = "public_unicast_only"
	ServerlessEgressRedirectDenyV1         ServerlessEgressRedirectPolicyV1 = "deny"
)

type ServerlessEgressEndpointPolicyV1 struct {
	Scheme           string                           `json:"scheme"`
	Host             string                           `json:"host"`
	Port             uint16                           `json:"port"`
	Path             string                           `json:"path"`
	Method           string                           `json:"method"`
	DNSClass         ServerlessEgressDNSClassV1       `json:"dns_class"`
	RedirectPolicy   ServerlessEgressRedirectPolicyV1 `json:"redirect_policy"`
	MaxRequestBytes  uint64                           `json:"max_request_bytes"`
	MaxResponseBytes uint64                           `json:"max_response_bytes"`
}

// ServerlessEgressPolicyV1 maps one already-admitted provider route to one exact proxy and
// HTTPS endpoint. Its digest is the SubstrateBindingV1 egress authority.
type ServerlessEgressPolicyV1 struct {
	Version               uint32                           `json:"version"`
	RoutePolicyDigest     string                           `json:"route_policy_digest"`
	EffectivePolicyDigest string                           `json:"effective_policy_digest"`
	ProxyArtifactDigest   string                           `json:"proxy_artifact_digest"`
	ProxyIdentityDigest   string                           `json:"proxy_identity_digest"`
	Route                 domain.ProviderRouteV1           `json:"route"`
	Endpoint              ServerlessEgressEndpointPolicyV1 `json:"endpoint"`
}

func (policy ServerlessEgressPolicyV1) Digest() (string, error) {
	if err := policy.validateIntrinsic(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(policy)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("sessionless.serverless-egress-policy.v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(encoded)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (policy ServerlessEgressPolicyV1) ValidateForAuthority(
	authority domain.ServerlessInvocationAuthorityV1,
	allocation domain.PreparedAllocationV1,
	routePolicy domain.ProviderRoutePolicyV1,
	effectivePolicy domain.ProviderPolicyEvidenceV1,
	now time.Time,
) error {
	if err := authority.ValidateAt(now.UTC()); err != nil {
		return err
	}
	if err := allocation.ValidateForBinding(authority.SubstrateBinding, authority.HarnessBinding.Backend); err != nil {
		return err
	}
	if err := policy.validateIntrinsic(); err != nil {
		return err
	}
	if err := routePolicy.Validate(); err != nil {
		return err
	}
	if err := effectivePolicy.Validate(); err != nil {
		return err
	}
	binding := authority.HarnessBinding
	expectedScope := domain.ProviderEvidenceScopeV1{
		TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID,
		Resource: binding.Resource, Backend: binding.Backend,
	}
	if routePolicy.Scope != expectedScope || effectivePolicy.Scope != expectedScope {
		return fmt.Errorf("egress evidence scope mismatch")
	}
	routeDigest, _ := routePolicy.Digest()
	effectiveDigest, _ := effectivePolicy.Digest()
	if string(routeDigest) != binding.ProviderRouteDigest || string(effectiveDigest) != binding.EffectivePolicyDigest ||
		policy.RoutePolicyDigest != binding.ProviderRouteDigest || policy.EffectivePolicyDigest != binding.EffectivePolicyDigest ||
		effectivePolicy.RoutePolicyDigest != binding.ProviderRouteDigest ||
		effectivePolicy.CapabilityEvidenceDigest != binding.CapabilityEvidenceDigest ||
		effectivePolicy.PrivacyEvidenceDigest != binding.PrivacyPolicyDigest {
		return fmt.Errorf("egress evidence digest mismatch")
	}
	if routePolicy.State != domain.ProviderEvidenceSupportedV1 || routePolicy.FallbackPolicy != domain.ProviderFallbackDenyV1 ||
		(effectivePolicy.Verdict != domain.ProviderPolicyGoV1 && effectivePolicy.Verdict != domain.ProviderPolicyConditionalV1) {
		return fmt.Errorf("egress policy is not admitted")
	}
	if now.UTC().Before(routePolicy.ObservedAt) || now.UTC().Before(effectivePolicy.ObservedAt) ||
		!now.UTC().Before(routePolicy.ExpiresAt) || !now.UTC().Before(effectivePolicy.ExpiresAt) || binding.EvidenceExpiresAt == nil ||
		binding.EvidenceExpiresAt.After(routePolicy.ExpiresAt) || binding.EvidenceExpiresAt.After(effectivePolicy.ExpiresAt) {
		return fmt.Errorf("egress evidence is expired or mis-scoped")
	}
	allowedData := false
	for _, class := range effectivePolicy.AllowedDataClasses {
		allowedData = allowedData || class == binding.InputDataClass
	}
	if !allowedData {
		return fmt.Errorf("input data class is not admitted")
	}
	if policy.Route.BackendKind != binding.Backend.BackendKind || policy.Route.ModelVendorID != binding.ModelVendorID ||
		policy.Route.ModelID != binding.ModelID || policy.Route.BillingAuthority != binding.Resource.ResourceID {
		return fmt.Errorf("egress route does not match harness binding")
	}
	routeFound := false
	for _, route := range routePolicy.Routes {
		routeFound = routeFound || route == policy.Route
	}
	if !routeFound {
		return fmt.Errorf("egress route is not in the admitted policy")
	}
	if policy.ProxyArtifactDigest != authority.SubstrateBinding.EgressProxyArtifactDigest ||
		policy.ProxyIdentityDigest != authority.SubstrateBinding.EgressProxyIdentityDigest {
		return fmt.Errorf("egress proxy binding mismatch")
	}
	digest, _ := policy.Digest()
	if digest != authority.SubstrateBinding.EgressPolicyDigest {
		return fmt.Errorf("egress substrate policy mismatch")
	}
	if policy.Endpoint.MaxRequestBytes > authority.AdmissionCostCeiling.MaxIngressBytes ||
		policy.Endpoint.MaxResponseBytes > authority.AdmissionCostCeiling.MaxEgressBytes {
		return fmt.Errorf("egress byte ceiling exceeds admission")
	}
	return nil
}

func (policy ServerlessEgressPolicyV1) validateIntrinsic() error {
	if policy.Version != ServerlessEgressPolicyVersionV1 {
		return fmt.Errorf("egress policy version is unsupported")
	}
	for _, digest := range []string{policy.RoutePolicyDigest, policy.EffectivePolicyDigest, policy.ProxyArtifactDigest, policy.ProxyIdentityDigest} {
		if len(digest) != sha256.Size*2 {
			return fmt.Errorf("egress policy digest is invalid")
		}
		if _, err := hex.DecodeString(digest); err != nil {
			return fmt.Errorf("egress policy digest is invalid")
		}
	}
	return policy.Endpoint.Validate()
}

func (endpoint ServerlessEgressEndpointPolicyV1) Validate() error {
	if endpoint.Scheme != "https" || endpoint.Port != 443 || endpoint.Method != "POST" ||
		endpoint.DNSClass != ServerlessEgressDNSPublicUnicastOnlyV1 || endpoint.RedirectPolicy != ServerlessEgressRedirectDenyV1 ||
		endpoint.MaxRequestBytes == 0 || endpoint.MaxResponseBytes == 0 {
		return fmt.Errorf("egress endpoint policy is not deny-by-default")
	}
	if !validPublicDNSName(endpoint.Host) {
		return fmt.Errorf("egress endpoint host is invalid")
	}
	parsed, err := url.ParseRequestURI(endpoint.Path)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
		!strings.HasPrefix(endpoint.Path, "/") || endpoint.Path == "/" || strings.Contains(endpoint.Path, "\\") ||
		strings.Contains(endpoint.Path, "%") || strings.Contains(endpoint.Path, "//") || strings.Contains(endpoint.Path, "/../") || strings.HasSuffix(endpoint.Path, "/..") ||
		strings.Contains(endpoint.Path, "/./") || strings.HasSuffix(endpoint.Path, "/.") {
		return fmt.Errorf("egress endpoint path is invalid")
	}
	return nil
}

func validPublicDNSName(host string) bool {
	if host == "" || len(host) > 253 || host != strings.ToLower(host) || strings.HasSuffix(host, ".") || net.ParseIP(host) != nil {
		return false
	}
	labels := strings.Split(host, ".")
	if len(labels) < 2 {
		return false
	}
	for _, label := range labels {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-') {
				return false
			}
		}
	}
	suffix := labels[len(labels)-1]
	for _, denied := range []string{"local", "localhost", "internal", "lan", "home", "test", "invalid", "example", "onion", "arpa"} {
		if suffix == denied {
			return false
		}
	}
	return true
}

type ServerlessEgressProxyAttestationV1 struct {
	PolicyDigest                  string
	ProxyArtifactDigest           string
	ProxyIdentityDigest           string
	ResolvedHost                  string
	ResolvedPort                  uint16
	ResolvedDNSClass              ServerlessEgressDNSClassV1
	ResolutionSetDigest           string
	ConnectionAddressPinned       bool
	DNSRebindingDenied            bool
	RedirectsDenied               bool
	AmbientProxyDenied            bool
	CertificateValidationRequired bool
	ExpiresAt                     time.Time
}

func (attestation ServerlessEgressProxyAttestationV1) ValidateForPolicy(policy ServerlessEgressPolicyV1, now, authorityExpiresAt time.Time) error {
	digest, err := policy.Digest()
	if err != nil || attestation.PolicyDigest != digest || attestation.ProxyArtifactDigest != policy.ProxyArtifactDigest ||
		attestation.ProxyIdentityDigest != policy.ProxyIdentityDigest || attestation.ResolvedHost != policy.Endpoint.Host ||
		attestation.ResolvedPort != policy.Endpoint.Port || attestation.ResolvedDNSClass != ServerlessEgressDNSPublicUnicastOnlyV1 ||
		len(attestation.ResolutionSetDigest) != sha256.Size*2 || !attestation.ConnectionAddressPinned ||
		!attestation.DNSRebindingDenied || !attestation.RedirectsDenied || !attestation.AmbientProxyDenied ||
		!attestation.CertificateValidationRequired ||
		attestation.ExpiresAt.IsZero() || authorityExpiresAt.IsZero() || !now.UTC().Before(attestation.ExpiresAt.UTC()) ||
		attestation.ExpiresAt.UTC().After(authorityExpiresAt.UTC()) {
		return fmt.Errorf("egress proxy attestation mismatch")
	}
	if _, err := hex.DecodeString(attestation.ResolutionSetDigest); err != nil {
		return fmt.Errorf("egress proxy attestation mismatch")
	}
	return nil
}
