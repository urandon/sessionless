// Package serverlessegress defines the feature-disabled, deny-by-default
// provider route and invocation-credential boundary for serverless attempts.
package serverlessegress

import (
	"context"
	"fmt"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"time"
)

// The neutral port owns the single canonical codec. These package names are
// the existing transport API, not alternate wire shapes or compatibility reads.
type DNSClassV1 = ports.ServerlessEgressDNSClassV1
type RedirectPolicyV1 = ports.ServerlessEgressRedirectPolicyV1
type EndpointPolicyV1 = ports.ServerlessEgressEndpointPolicyV1
type PolicyV1 = ports.ServerlessEgressPolicyV1
type ProxyAttestationV1 = ports.ServerlessEgressProxyAttestationV1

const PolicyVersionV1 = ports.ServerlessEgressPolicyVersionV1
const DNSPublicUnicastOnlyV1 = ports.ServerlessEgressDNSPublicUnicastOnlyV1
const RedirectDenyV1 = ports.ServerlessEgressRedirectDenyV1

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
