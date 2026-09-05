// Package serverlessegress defines the feature-disabled, deny-by-default
// provider route and invocation-credential boundary for serverless attempts.
package serverlessegress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

const PolicyVersionV1 uint32 = 1

type (
	DNSClassV1       string
	RedirectPolicyV1 string
)

const (
	DNSPublicUnicastOnlyV1 DNSClassV1       = "public_unicast_only"
	RedirectDenyV1         RedirectPolicyV1 = "deny"
)

type EndpointPolicyV1 struct {
	Scheme           string           `json:"scheme"`
	Host             string           `json:"host"`
	Port             uint16           `json:"port"`
	Path             string           `json:"path"`
	Method           string           `json:"method"`
	DNSClass         DNSClassV1       `json:"dns_class"`
	RedirectPolicy   RedirectPolicyV1 `json:"redirect_policy"`
	MaxRequestBytes  uint64           `json:"max_request_bytes"`
	MaxResponseBytes uint64           `json:"max_response_bytes"`
}

// PolicyV1 maps one already-admitted provider route to one exact proxy and
// HTTPS endpoint. Its digest is the SubstrateBindingV1 egress authority.
type PolicyV1 struct {
	Version               uint32                 `json:"version"`
	RoutePolicyDigest     string                 `json:"route_policy_digest"`
	EffectivePolicyDigest string                 `json:"effective_policy_digest"`
	ProxyArtifactDigest   string                 `json:"proxy_artifact_digest"`
	ProxyIdentityDigest   string                 `json:"proxy_identity_digest"`
	Route                 domain.ProviderRouteV1 `json:"route"`
	Endpoint              EndpointPolicyV1       `json:"endpoint"`
}

func (policy PolicyV1) Digest() (string, error) {
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

func (policy PolicyV1) ValidateForAuthority(
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

func (policy PolicyV1) validateIntrinsic() error {
	if policy.Version != PolicyVersionV1 {
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

func (endpoint EndpointPolicyV1) Validate() error {
	if endpoint.Scheme != "https" || endpoint.Port != 443 || endpoint.Method != "POST" ||
		endpoint.DNSClass != DNSPublicUnicastOnlyV1 || endpoint.RedirectPolicy != RedirectDenyV1 ||
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

type ProxyAttestationV1 struct {
	PolicyDigest                  string
	ProxyArtifactDigest           string
	ProxyIdentityDigest           string
	ResolvedHost                  string
	ResolvedPort                  uint16
	ResolvedDNSClass              DNSClassV1
	ResolutionSetDigest           string
	ConnectionAddressPinned       bool
	DNSRebindingDenied            bool
	RedirectsDenied               bool
	AmbientProxyDenied            bool
	CertificateValidationRequired bool
	ExpiresAt                     time.Time
}

func (attestation ProxyAttestationV1) ValidateForPolicy(policy PolicyV1, now, authorityExpiresAt time.Time) error {
	digest, err := policy.Digest()
	if err != nil || attestation.PolicyDigest != digest || attestation.ProxyArtifactDigest != policy.ProxyArtifactDigest ||
		attestation.ProxyIdentityDigest != policy.ProxyIdentityDigest || attestation.ResolvedHost != policy.Endpoint.Host ||
		attestation.ResolvedPort != policy.Endpoint.Port || attestation.ResolvedDNSClass != DNSPublicUnicastOnlyV1 ||
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

type PreparedInvocationGate interface {
	Validate(serverlessharness.PreparedInvocation) error
	Consume(serverlessharness.PreparedInvocation) error
}

type ProxyInvocationV1 struct {
	Policy          PolicyV1
	Attestation     ProxyAttestationV1
	Materialization ports.ProviderCredentialMaterializationV1 `json:"-"`
	Secret          []byte                                    `json:"-"`
	Payload         []byte                                    `json:"-"`
}

func (value ProxyInvocationV1) String() string {
	return fmt.Sprintf("ProxyInvocation{delivery:%s secret:[redacted:%d] payload:[redacted:%d]}", value.Materialization.Kind, len(value.Secret), len(value.Payload))
}

func (value ProxyInvocationV1) GoString() string { return value.String() }

type ProxyResultV1 struct {
	Route         domain.ProviderRouteV1
	Acceptance    domain.ProviderAcceptanceClassV1
	RequestBytes  uint64
	ResponseBytes uint64
	Response      []byte `json:"-"`
	ObservedAt    time.Time
}

type Proxy interface {
	Preflight(context.Context, PolicyV1) (ProxyAttestationV1, error)
	Invoke(context.Context, ProxyInvocationV1) (ProxyResultV1, error)
}

type RequestV1 struct {
	Prepared        serverlessharness.PreparedInvocation
	RoutePolicy     domain.ProviderRoutePolicyV1
	EffectivePolicy domain.ProviderPolicyEvidenceV1
	Policy          PolicyV1
	Payload         []byte `json:"-"`
}

func (value RequestV1) String() string {
	return fmt.Sprintf("ServerlessEgressRequest{payload:[redacted:%d]}", len(value.Payload))
}

func (value RequestV1) GoString() string { return value.String() }

type EvidenceV1 struct {
	Egress                 domain.SubstrateEgressStateV1
	ProxyAttestation       domain.SubstrateProxyAttestationStateV1
	RouteState             domain.ProviderEvidenceStateV1
	ActualModelVendorID    string
	ActualModelID          string
	TransportKind          domain.ProviderTransportKindV1
	TransportProvider      string
	UpstreamProviderID     string
	EndpointID             string
	RequestBytes           uint64
	ResponseBytes          uint64
	Acceptance             domain.ProviderAcceptanceClassV1
	CredentialFinalization domain.CredentialFinalizationStateV1
	ObservedAt             time.Time
}

type ResultV1 struct {
	Evidence EvidenceV1
	Response []byte `json:"-"`
}

func (value ResultV1) String() string {
	return fmt.Sprintf("ServerlessEgressResult{egress:%s proxy:%s route:%s request_bytes:%d response_bytes:%d response:[redacted:%d] credential:%s}",
		value.Evidence.Egress, value.Evidence.ProxyAttestation, value.Evidence.RouteState,
		value.Evidence.RequestBytes, value.Evidence.ResponseBytes, len(value.Response), value.Evidence.CredentialFinalization)
}

func (value ResultV1) GoString() string { return value.String() }
