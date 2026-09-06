// Package directopenrouter implements the closed, feature-disabled native
// OpenRouter reference backend below the Sessionless-owned harness registry.
package directopenrouter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	HarnessVersionV1        = "1"
	NativeProtocolVersionV1 = "openrouter-chat-completions.v1"
	EndpointURLV1           = "https://openrouter.ai/api/v1/chat/completions"
	ModelVendorIDV1         = "stealth"
	ModelIDV1               = "ox-alpha"
	WireModelIDV1           = "stealth/ox-alpha"
	ProviderIDV1            = "stealth"
	TransportProviderV1     = "openrouter"
	EndpointIDV1            = "stealth"

	maxPromptBytes     = 1 << 20
	maxRequestBytes    = 2 << 20
	maxResponseBytes   = 16 << 20
	maxFinalBytes      = 256 << 10
	maxJSONDepth       = 32
	maxJSONMembers     = 256
	maxProviderEffects = 1
)

var (
	ErrDisabled = errors.New("direct OpenRouter backend is disabled")
	ErrContract = errors.New("direct OpenRouter backend contract is invalid")
)

// ProfileV1 is a compiled request contract. Enabled is a reversible local
// gate and is deliberately excluded from the immutable profile digest.
type ProfileV1 struct {
	Enabled             bool
	RequestTimeoutMS    uint64
	MaxCompletionTokens uint64
}

func (profile ProfileV1) validate() error {
	if profile.RequestTimeoutMS == 0 || profile.RequestTimeoutMS > 300_000 ||
		profile.MaxCompletionTokens < 16 || profile.MaxCompletionTokens > 65_536 {
		return ErrContract
	}
	return nil
}

func (profile ProfileV1) descriptor() (domain.HarnessBackendDescriptorV1, error) {
	if err := profile.validate(); err != nil {
		return domain.HarnessBackendDescriptorV1{}, err
	}
	artifact := sha256.Sum256([]byte("sessionless.direct-openrouter.native-go.v1"))
	material := struct {
		Schema                 string     `json:"schema"`
		Endpoint               string     `json:"endpoint"`
		Method                 string     `json:"method"`
		Headers                []HeaderV1 `json:"headers"`
		AuthorizationHeader    string     `json:"authorization_header"`
		AuthorizationScheme    string     `json:"authorization_scheme"`
		WireProtocol           string     `json:"wire_protocol"`
		Model                  string     `json:"model"`
		ProviderOnly           []string   `json:"provider_only"`
		AllowFallbacks         bool       `json:"allow_fallbacks"`
		RequireParameters      bool       `json:"require_parameters"`
		Stream                 bool       `json:"stream"`
		RequestTimeoutMS       uint64     `json:"request_timeout_ms"`
		MaxCompletionTokens    uint64     `json:"max_completion_tokens"`
		MaxProviderEffects     uint64     `json:"max_provider_effects"`
		MaxRequestBytes        uint64     `json:"max_request_bytes"`
		MaxResponseBytes       uint64     `json:"max_response_bytes"`
		RequireTLSValidation   bool       `json:"require_tls_validation"`
		DenyAmbientCredentials bool       `json:"deny_ambient_credentials"`
		DenyAmbientProxy       bool       `json:"deny_ambient_proxy"`
		DenyRedirects          bool       `json:"deny_redirects"`
		DenyCookies            bool       `json:"deny_cookies"`
		DenyCompression        bool       `json:"deny_compression"`
	}{
		Schema: "sessionless.direct-openrouter-profile.v1", Endpoint: EndpointURLV1, Method: "POST",
		Headers: requestHeaders(), AuthorizationHeader: "Authorization", AuthorizationScheme: "Bearer",
		WireProtocol: NativeProtocolVersionV1, Model: WireModelIDV1,
		ProviderOnly: []string{ProviderIDV1}, AllowFallbacks: false, RequireParameters: true,
		Stream: false, RequestTimeoutMS: profile.RequestTimeoutMS,
		MaxCompletionTokens: profile.MaxCompletionTokens, MaxProviderEffects: maxProviderEffects,
		MaxRequestBytes: maxRequestBytes, MaxResponseBytes: maxResponseBytes,
		RequireTLSValidation: true, DenyAmbientCredentials: true, DenyAmbientProxy: true,
		DenyRedirects: true, DenyCookies: true, DenyCompression: true,
	}
	encoded, err := json.Marshal(material)
	if err != nil {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	profileDigest := sha256.Sum256(encoded)
	descriptor := domain.HarnessBackendDescriptorV1{
		HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: HarnessVersionV1,
		BackendKind: domain.HarnessBackendDirectOpenRouterV1, ArtifactKind: domain.HarnessArtifactEmbeddedProfileV1,
		ArtifactDigest: hex.EncodeToString(artifact[:]), NativeProtocolVersion: NativeProtocolVersionV1,
		BackendProfileDigest: hex.EncodeToString(profileDigest[:]), ProviderContractKind: domain.ProviderContractInvocationV1,
		CredentialDeliveryKind: domain.ProviderCredentialDeliveryDirectV1,
	}
	if err := descriptor.Validate(); err != nil {
		return domain.HarnessBackendDescriptorV1{}, ErrContract
	}
	return descriptor, nil
}

type HeaderV1 struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func requestHeaders() []HeaderV1 {
	return []HeaderV1{
		{Name: "Accept", Value: "application/json"},
		{Name: "Content-Type", Value: "application/json"},
		{Name: "X-OpenRouter-Metadata", Value: "enabled"},
	}
}

// InvocationV1 carries only the credential handle. The sealed boundary owns
// synchronous secret materialization and Authorization header injection.
type InvocationV1 struct {
	Identity                   ports.ExecutionIdentity                   `json:"-"`
	Credential                 ports.ProviderInvocationCredentialV1      `json:"-"`
	CredentialMaterialization  ports.ProviderCredentialMaterializationV1 `json:"-"`
	URL                        string
	Method                     string
	Headers                    []HeaderV1
	Body                       []byte `json:"-"`
	TimeoutMS                  uint64
	MaxResponseBytes           uint64
	AuthorizationHeaderName    string
	AuthorizationScheme        string
	RequireTLSValidation       bool
	DenyAmbientCredentials     bool
	DenyRedirects              bool
	DenyAmbientProxy           bool
	DenyCookies                bool
	DenyCompression            bool
	RequireProviderEffectFence bool
	MaxProviderEffects         uint64
}

func (value InvocationV1) String() string {
	return fmt.Sprintf("DirectOpenRouterInvocation{tenant:%s owner:%s run:%s attempt:%s url:%s method:%s headers:%d body:[redacted:%d] credential:[redacted]}",
		value.Identity.TenantID, value.Identity.OwnerUserID, value.Identity.RunID, value.Identity.AttemptID,
		value.URL, value.Method, len(value.Headers), len(value.Body))
}

func (value InvocationV1) GoString() string { return value.String() }

type ResultV1 struct {
	StatusCode                   int
	ContentType                  string
	ContentEncoding              string
	AcceptanceClass              domain.ProviderAcceptanceClassV1
	RequestBytesWritten          uint64
	ResponseBytes                uint64
	Redirected                   bool
	CookiesObserved              bool
	Cancelled                    bool
	Deadline                     bool
	CleanupSucceeded             bool
	CredentialFinalized          bool
	ProviderEffectFenceSatisfied bool
	ProviderEffects              uint64
	FailureCode                  string `json:"-"`
	Body                         []byte `json:"-"`
}

func (value ResultV1) String() string {
	return fmt.Sprintf("DirectOpenRouterResult{status:%d type:%s encoding_present:%t acceptance:%s request_bytes:%d response_bytes:%d redirected:%t cookies:%t cancelled:%t deadline:%t cleanup:%t credential_finalized:%t effect_fence:%t provider_effects:%d failure_present:%t body:[redacted:%d]}",
		value.StatusCode, value.ContentType, value.ContentEncoding != "", value.AcceptanceClass,
		value.RequestBytesWritten, value.ResponseBytes, value.Redirected, value.CookiesObserved,
		value.Cancelled, value.Deadline, value.CleanupSucceeded, value.CredentialFinalized,
		value.ProviderEffectFenceSatisfied, value.ProviderEffects, value.FailureCode != "", len(value.Body))
}

func (value ResultV1) GoString() string { return value.String() }

type HTTPBoundaryV1 interface {
	Invoke(context.Context, InvocationV1) (ResultV1, error)
	Cancel(context.Context, ports.ExecutionIdentity) error
}
