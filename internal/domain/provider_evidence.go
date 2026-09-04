package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

const ProviderEvidenceVersionV1 uint32 = 1

type (
	ProviderDataClassV1      string
	ProviderFallbackPolicyV1 string
	ProviderEvidenceStateV1  string
	ProviderPolicyVerdictV1  string
	ProviderTransportKindV1  string
	ProviderBillingKindV1    string
	ProviderEvidenceDigestV1 string
)

const (
	ProviderDataPublicV1              ProviderDataClassV1      = "public"
	ProviderDataExternallyShareableV1 ProviderDataClassV1      = "externally_shareable"
	ProviderDataPrivateV1             ProviderDataClassV1      = "private"
	ProviderFallbackDenyV1            ProviderFallbackPolicyV1 = "deny"
	ProviderEvidenceUnknownV1         ProviderEvidenceStateV1  = "unknown"
	ProviderEvidenceUnsupportedV1     ProviderEvidenceStateV1  = "unsupported"
	ProviderEvidenceSupportedV1       ProviderEvidenceStateV1  = "supported"
	ProviderEvidenceFalseV1           ProviderEvidenceStateV1  = "false"
	ProviderEvidenceTrueV1            ProviderEvidenceStateV1  = "true"
	ProviderPolicyUnknownV1           ProviderPolicyVerdictV1  = "unknown"
	ProviderPolicyGoV1                ProviderPolicyVerdictV1  = "go"
	ProviderPolicyConditionalV1       ProviderPolicyVerdictV1  = "conditional"
	ProviderPolicyNoGoV1              ProviderPolicyVerdictV1  = "no_go"
	ProviderTransportLocalCLIV1       ProviderTransportKindV1  = "local_cli"
	ProviderTransportDirectAPIV1      ProviderTransportKindV1  = "direct_api"
	ProviderTransportRouterAPIV1      ProviderTransportKindV1  = "router_api"
	ProviderBillingSubscriptionV1     ProviderBillingKindV1    = "subscription"
	ProviderBillingAPIAccountV1       ProviderBillingKindV1    = "api_account"
	ProviderBillingRouterAccountV1    ProviderBillingKindV1    = "router_account"
	ProviderBillingNoneV1             ProviderBillingKindV1    = "none"
)

// ProviderEvidenceScopeV1 prevents cross-owner/resource/backend evidence reuse.
type ProviderEvidenceScopeV1 struct {
	TenantID    TenantID                   `json:"tenant_id"`
	OwnerUserID UserID                     `json:"owner_user_id"`
	Resource    ProviderResourceBindingV1  `json:"resource"`
	Backend     HarnessBackendDescriptorV1 `json:"backend"`
}

func (scope ProviderEvidenceScopeV1) Clone() ProviderEvidenceScopeV1 { return scope }

func (scope ProviderEvidenceScopeV1) Validate() error {
	if err := scope.TenantID.Validate(); err != nil {
		return err
	}
	if err := scope.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := scope.Resource.Validate(); err != nil {
		return err
	}
	if err := scope.Backend.Validate(); err != nil {
		return err
	}
	if err := scope.Resource.ValidateForBackend(scope.Backend); err != nil {
		return err
	}
	if scope.Resource.OwnerUserID != scope.OwnerUserID {
		return ValidationError{Field: "provider_evidence.scope", Reason: "resource owner must match evidence owner"}
	}
	return nil
}

type ProviderEvidenceQuantityV1 struct {
	State ProviderEvidenceStateV1 `json:"state"`
	Value uint64                  `json:"value"`
}

func (value ProviderEvidenceQuantityV1) Validate(field string, positive bool) error {
	if value.State == ProviderEvidenceUnknownV1 {
		if value.Value != 0 {
			return ValidationError{Field: field, Reason: "unknown quantity must have zero placeholder"}
		}
		return nil
	}
	if value.State != ProviderEvidenceSupportedV1 || (positive && value.Value == 0) {
		return ValidationError{Field: field, Reason: "must be unknown or a supported canonical quantity"}
	}
	return nil
}

type ProviderRouterResourceV1 struct {
	Version    uint32                  `json:"version"`
	Scope      ProviderEvidenceScopeV1 `json:"scope"`
	RouterID   string                  `json:"router_id"`
	ObservedAt time.Time               `json:"observed_at"`
	ExpiresAt  time.Time               `json:"expires_at"`
}

func (value ProviderRouterResourceV1) Clone() ProviderRouterResourceV1 { return value }

type ProviderRouteV1 struct {
	BackendKind        HarnessBackendKindV1    `json:"backend_kind"`
	ModelVendorID      string                  `json:"model_vendor_id"`
	TransportKind      ProviderTransportKindV1 `json:"transport_kind"`
	TransportProvider  string                  `json:"transport_provider"`
	UpstreamProviderID string                  `json:"upstream_provider_id"`
	EndpointID         string                  `json:"endpoint_id"`
	BillingKind        ProviderBillingKindV1   `json:"billing_kind"`
	BillingAuthority   string                  `json:"billing_authority"`
	ModelID            string                  `json:"model_id"`
}
type ProviderRoutePolicyV1 struct {
	Version        uint32                   `json:"version"`
	Scope          ProviderEvidenceScopeV1  `json:"scope"`
	State          ProviderEvidenceStateV1  `json:"state"`
	PolicyID       string                   `json:"policy_id"`
	Revision       uint64                   `json:"revision"`
	FallbackPolicy ProviderFallbackPolicyV1 `json:"fallback_policy"`
	Routes         []ProviderRouteV1        `json:"routes"`
	ObservedAt     time.Time                `json:"observed_at"`
	ExpiresAt      time.Time                `json:"expires_at"`
}

func (value ProviderRoutePolicyV1) Clone() ProviderRoutePolicyV1 {
	value.Routes = append([]ProviderRouteV1(nil), value.Routes...)
	return value
}

type ProviderCatalogModelV1 struct {
	ModelVendorID string `json:"model_vendor_id"`
	ModelID       string `json:"model_id"`
	CatalogDigest string `json:"catalog_digest"`
}
type ProviderCatalogObservationV1 struct {
	Version         uint32                   `json:"version"`
	Scope           ProviderEvidenceScopeV1  `json:"scope"`
	CatalogRevision string                   `json:"catalog_revision"`
	Models          []ProviderCatalogModelV1 `json:"models"`
	ObservedAt      time.Time                `json:"observed_at"`
	ExpiresAt       time.Time                `json:"expires_at"`
}

func (value ProviderCatalogObservationV1) Clone() ProviderCatalogObservationV1 {
	value.Models = append([]ProviderCatalogModelV1(nil), value.Models...)
	return value
}

type ProviderCapabilityEvidenceV1 struct {
	Version          uint32                     `json:"version"`
	Scope            ProviderEvidenceScopeV1    `json:"scope"`
	ModelVendorID    string                     `json:"model_vendor_id"`
	ModelID          string                     `json:"model_id"`
	MaxContextTokens ProviderEvidenceQuantityV1 `json:"max_context_tokens"`
	ToolCalling      ProviderEvidenceStateV1    `json:"tool_calling"`
	StructuredOutput ProviderEvidenceStateV1    `json:"structured_output"`
	ObservedAt       time.Time                  `json:"observed_at"`
	ExpiresAt        time.Time                  `json:"expires_at"`
}

func (value ProviderCapabilityEvidenceV1) Clone() ProviderCapabilityEvidenceV1 { return value }

type ProviderPrivacyEvidenceV1 struct {
	Version            uint32                     `json:"version"`
	Scope              ProviderEvidenceScopeV1    `json:"scope"`
	ModelVendorID      string                     `json:"model_vendor_id"`
	ModelID            string                     `json:"model_id"`
	AllowedDataClasses []ProviderDataClassV1      `json:"allowed_data_classes"`
	TrainingUse        ProviderEvidenceStateV1    `json:"training_use"`
	RetentionHours     ProviderEvidenceQuantityV1 `json:"retention_hours"`
	PolicyRevision     string                     `json:"policy_revision"`
	ObservedAt         time.Time                  `json:"observed_at"`
	ExpiresAt          time.Time                  `json:"expires_at"`
}

func (value ProviderPrivacyEvidenceV1) Clone() ProviderPrivacyEvidenceV1 {
	value.AllowedDataClasses = append([]ProviderDataClassV1(nil), value.AllowedDataClasses...)
	return value
}

type ProviderPriceObservationV1 struct {
	Version                    uint32                  `json:"version"`
	Scope                      ProviderEvidenceScopeV1 `json:"scope"`
	State                      ProviderEvidenceStateV1 `json:"state"`
	ModelVendorID              string                  `json:"model_vendor_id"`
	ModelID                    string                  `json:"model_id"`
	Currency                   string                  `json:"currency"`
	InputMicrounitsPerMTokens  uint64                  `json:"input_microunits_per_million_tokens"`
	OutputMicrounitsPerMTokens uint64                  `json:"output_microunits_per_million_tokens"`
	ObservedAt                 time.Time               `json:"observed_at"`
	ExpiresAt                  time.Time               `json:"expires_at"`
}

func (value ProviderPriceObservationV1) Clone() ProviderPriceObservationV1 { return value }

type ProviderPolicyEvidenceV1 struct {
	Version                  uint32                  `json:"version"`
	Scope                    ProviderEvidenceScopeV1 `json:"scope"`
	PolicyID                 string                  `json:"policy_id"`
	Revision                 uint64                  `json:"revision"`
	DecisionOwner            string                  `json:"decision_owner"`
	EvidenceSource           string                  `json:"evidence_source"`
	Verdict                  ProviderPolicyVerdictV1 `json:"verdict"`
	AllowedDataClasses       []ProviderDataClassV1   `json:"allowed_data_classes"`
	CapabilityEvidenceDigest string                  `json:"capability_evidence_digest"`
	PrivacyEvidenceDigest    string                  `json:"privacy_evidence_digest"`
	PriceObservationDigest   string                  `json:"price_observation_digest"`
	RoutePolicyDigest        string                  `json:"route_policy_digest"`
	ObservedAt               time.Time               `json:"observed_at"`
	ExpiresAt                time.Time               `json:"expires_at"`
}

func (value ProviderPolicyEvidenceV1) Clone() ProviderPolicyEvidenceV1 {
	value.AllowedDataClasses = append([]ProviderDataClassV1(nil), value.AllowedDataClasses...)
	return value
}

func validateEvidence(version uint32, scope ProviderEvidenceScopeV1, observedAt, expiresAt time.Time) error {
	if version != ProviderEvidenceVersionV1 {
		return ValidationError{Field: "provider_evidence.version", Reason: "must equal 1"}
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	if observedAt.IsZero() || expiresAt.IsZero() || !expiresAt.UTC().After(observedAt.UTC()) {
		return ValidationError{Field: "provider_evidence.window", Reason: "expiry must be later than observation"}
	}
	if observedAt.Location() != time.UTC || expiresAt.Location() != time.UTC {
		return ValidationError{Field: "provider_evidence.window", Reason: "timestamps must use UTC"}
	}
	return nil
}
func validateSupport(field string, state ProviderEvidenceStateV1) error {
	if state != ProviderEvidenceUnknownV1 && state != ProviderEvidenceUnsupportedV1 && state != ProviderEvidenceSupportedV1 {
		return ValidationError{Field: field, Reason: "must preserve unknown, unsupported, or supported"}
	}
	return nil
}
func validateTruth(field string, state ProviderEvidenceStateV1) error {
	if state != ProviderEvidenceUnknownV1 && state != ProviderEvidenceFalseV1 && state != ProviderEvidenceTrueV1 {
		return ValidationError{Field: field, Reason: "must preserve unknown, false, or true"}
	}
	return nil
}
func validateDataClasses(values []ProviderDataClassV1, allowEmpty bool) error {
	if (!allowEmpty && len(values) == 0) || len(values) > 3 {
		return ValidationError{Field: "provider_evidence.allowed_data_classes", Reason: "has invalid cardinality"}
	}
	previous := ProviderDataClassV1("")
	for _, value := range values {
		if value != ProviderDataPublicV1 && value != ProviderDataExternallyShareableV1 && value != ProviderDataPrivateV1 {
			return ValidationError{Field: "provider_evidence.allowed_data_classes", Reason: "contains unsupported value"}
		}
		if value <= previous {
			return ValidationError{Field: "provider_evidence.allowed_data_classes", Reason: "must be sorted and unique"}
		}
		previous = value
	}
	return nil
}
func validateProviderModel(vendor, model string) error {
	if err := validateProviderToken("provider_evidence.model_vendor_id", vendor, 128); err != nil {
		return err
	}
	return validateProviderToken("provider_evidence.model_id", model, 256)
}

func (value ProviderRouterResourceV1) Validate() error {
	if err := validateEvidence(value.Version, value.Scope, value.ObservedAt, value.ExpiresAt); err != nil {
		return err
	}
	if value.Scope.Resource.Kind != ProviderResourceRouterAccountV1 && value.Scope.Resource.Kind != ProviderResourceAPIAccountV1 {
		return ValidationError{Field: "provider_router_resource.scope", Reason: "must bind router or API account"}
	}
	return validateProviderToken("provider_router_resource.router_id", value.RouterID, 128)
}
func (value ProviderRoutePolicyV1) Validate() error {
	if err := validateEvidence(value.Version, value.Scope, value.ObservedAt, value.ExpiresAt); err != nil {
		return err
	}
	if value.FallbackPolicy != ProviderFallbackDenyV1 {
		return ValidationError{Field: "provider_route_policy.fallback_policy", Reason: "must deny fallback"}
	}
	if value.State == ProviderEvidenceUnknownV1 {
		if value.PolicyID != "" || value.Revision != 0 || len(value.Routes) != 0 {
			return ValidationError{Field: "provider_route_policy", Reason: "unknown route must not invent policy or routes"}
		}
		return nil
	}
	if value.State != ProviderEvidenceSupportedV1 {
		return ValidationError{Field: "provider_route_policy.state", Reason: "must be unknown or supported"}
	}
	if err := ValidateOpaqueID("provider_route_policy.policy_id", value.PolicyID); err != nil {
		return err
	}
	if value.Revision == 0 || len(value.Routes) == 0 || len(value.Routes) > 16 {
		return ValidationError{Field: "provider_route_policy", Reason: "requires positive revision, deny fallback, and one to sixteen routes"}
	}
	seen := map[string]struct{}{}
	for _, r := range value.Routes {
		switch r.BackendKind {
		case HarnessBackendCodexExecV1, HarnessBackendOpenCodeV1, HarnessBackendPiV1, HarnessBackendDirectOpenRouterV1, HarnessBackendDeterministicFixtureV1:
		default:
			return ValidationError{Field: "provider_route_policy.routes.backend_kind", Reason: "is unsupported"}
		}
		if r.BackendKind != value.Scope.Backend.BackendKind {
			return ValidationError{Field: "provider_route_policy.routes.backend_kind", Reason: "must match the scoped backend"}
		}
		if err := validateProviderModel(r.ModelVendorID, r.ModelID); err != nil {
			return err
		}
		if err := validateProviderToken("provider_route_policy.routes.transport_provider", r.TransportProvider, 128); err != nil {
			return err
		}
		if err := validateProviderToken("provider_route_policy.routes.upstream_provider_id", r.UpstreamProviderID, 128); err != nil {
			return err
		}
		if err := validateProviderToken("provider_route_policy.routes.endpoint_id", r.EndpointID, 128); err != nil {
			return err
		}
		if err := validateProviderToken("provider_route_policy.routes.billing_authority", r.BillingAuthority, 128); err != nil {
			return err
		}
		switch r.TransportKind {
		case ProviderTransportLocalCLIV1, ProviderTransportDirectAPIV1, ProviderTransportRouterAPIV1:
		default:
			return ValidationError{Field: "provider_route_policy.routes.transport_kind", Reason: "is unsupported"}
		}
		switch r.BillingKind {
		case ProviderBillingSubscriptionV1, ProviderBillingAPIAccountV1, ProviderBillingRouterAccountV1, ProviderBillingNoneV1:
		default:
			return ValidationError{Field: "provider_route_policy.routes.billing_kind", Reason: "is unsupported"}
		}
		expectedBilling := ProviderBillingKindV1("")
		switch value.Scope.Resource.Kind {
		case ProviderResourceSubscriptionV1:
			expectedBilling = ProviderBillingSubscriptionV1
		case ProviderResourceAPIAccountV1:
			expectedBilling = ProviderBillingAPIAccountV1
		case ProviderResourceRouterAccountV1:
			expectedBilling = ProviderBillingRouterAccountV1
		case ProviderResourceCredentiallessFixtureV1:
			expectedBilling = ProviderBillingNoneV1
		}
		if r.BillingKind != expectedBilling || r.BillingAuthority != value.Scope.Resource.ResourceID {
			return ValidationError{Field: "provider_route_policy.routes.billing_authority", Reason: "must match the scoped resource"}
		}
		key := string(r.BackendKind) + "\x00" + r.ModelVendorID + "\x00" + string(r.TransportKind) + "\x00" + r.TransportProvider + "\x00" + r.UpstreamProviderID + "\x00" + r.EndpointID + "\x00" + string(r.BillingKind) + "\x00" + r.BillingAuthority + "\x00" + r.ModelID
		if _, ok := seen[key]; ok {
			return ValidationError{Field: "provider_route_policy.routes", Reason: "must be unique"}
		}
		seen[key] = struct{}{}
	}
	return nil
}
func (value ProviderCatalogObservationV1) Validate() error {
	if err := validateEvidence(value.Version, value.Scope, value.ObservedAt, value.ExpiresAt); err != nil {
		return err
	}
	if err := validateProviderToken("provider_catalog.catalog_revision", value.CatalogRevision, 128); err != nil {
		return err
	}
	if len(value.Models) == 0 || len(value.Models) > 512 {
		return ValidationError{Field: "provider_catalog.models", Reason: "must contain one to 512 models"}
	}
	previous := ""
	for _, m := range value.Models {
		if err := validateProviderModel(m.ModelVendorID, m.ModelID); err != nil {
			return err
		}
		if err := validateSHA256("provider_catalog.models.catalog_digest", m.CatalogDigest); err != nil {
			return err
		}
		key := m.ModelVendorID + "\x00" + m.ModelID
		if key <= previous {
			return ValidationError{Field: "provider_catalog.models", Reason: "must be sorted and unique"}
		}
		previous = key
	}
	return nil
}
func (value ProviderCapabilityEvidenceV1) Validate() error {
	if err := validateEvidence(value.Version, value.Scope, value.ObservedAt, value.ExpiresAt); err != nil {
		return err
	}
	if err := validateProviderModel(value.ModelVendorID, value.ModelID); err != nil {
		return err
	}
	if err := value.MaxContextTokens.Validate("provider_capability.max_context_tokens", true); err != nil {
		return err
	}
	if err := validateSupport("provider_capability.tool_calling", value.ToolCalling); err != nil {
		return err
	}
	return validateSupport("provider_capability.structured_output", value.StructuredOutput)
}
func (value ProviderPrivacyEvidenceV1) Validate() error {
	if err := validateEvidence(value.Version, value.Scope, value.ObservedAt, value.ExpiresAt); err != nil {
		return err
	}
	if err := validateProviderModel(value.ModelVendorID, value.ModelID); err != nil {
		return err
	}
	if err := validateDataClasses(value.AllowedDataClasses, false); err != nil {
		return err
	}
	if err := validateTruth("provider_privacy.training_use", value.TrainingUse); err != nil {
		return err
	}
	if err := value.RetentionHours.Validate("provider_privacy.retention_hours", false); err != nil {
		return err
	}
	return validateProviderToken("provider_privacy.policy_revision", value.PolicyRevision, 128)
}
func (value ProviderPriceObservationV1) Validate() error {
	if err := validateEvidence(value.Version, value.Scope, value.ObservedAt, value.ExpiresAt); err != nil {
		return err
	}
	if value.State == ProviderEvidenceUnknownV1 {
		if err := validateProviderModel(value.ModelVendorID, value.ModelID); err != nil {
			return err
		}
		if value.Currency != "" || value.InputMicrounitsPerMTokens != 0 || value.OutputMicrounitsPerMTokens != 0 {
			return ValidationError{Field: "provider_price", Reason: "unknown price must not invent values"}
		}
		return nil
	}
	if value.State != ProviderEvidenceSupportedV1 {
		return ValidationError{Field: "provider_price.state", Reason: "must be unknown or supported"}
	}
	if err := validateProviderModel(value.ModelVendorID, value.ModelID); err != nil {
		return err
	}
	if len(value.Currency) != 3 || value.Currency != strings.ToUpper(value.Currency) {
		return ValidationError{Field: "provider_price.currency", Reason: "must be uppercase ASCII currency"}
	}
	for _, r := range value.Currency {
		if r < 'A' || r > 'Z' {
			return ValidationError{Field: "provider_price.currency", Reason: "must be uppercase ASCII currency"}
		}
	}
	return nil
}
func (value ProviderPolicyEvidenceV1) Validate() error {
	if err := validateEvidence(value.Version, value.Scope, value.ObservedAt, value.ExpiresAt); err != nil {
		return err
	}
	if err := ValidateOpaqueID("provider_policy.policy_id", value.PolicyID); err != nil {
		return err
	}
	if value.Revision == 0 {
		return ValidationError{Field: "provider_policy.revision", Reason: "must be positive"}
	}
	if err := validateProviderToken("provider_policy.decision_owner", value.DecisionOwner, 128); err != nil {
		return err
	}
	if err := validateProviderToken("provider_policy.evidence_source", value.EvidenceSource, 128); err != nil {
		return err
	}
	switch value.Verdict {
	case ProviderPolicyUnknownV1, ProviderPolicyGoV1, ProviderPolicyConditionalV1, ProviderPolicyNoGoV1:
	default:
		return ValidationError{Field: "provider_policy.verdict", Reason: "is unsupported"}
	}
	if err := validateDataClasses(value.AllowedDataClasses, value.Verdict == ProviderPolicyUnknownV1 || value.Verdict == ProviderPolicyNoGoV1); err != nil {
		return err
	}
	for f, d := range map[string]string{"capability": value.CapabilityEvidenceDigest, "privacy": value.PrivacyEvidenceDigest, "price": value.PriceObservationDigest, "route": value.RoutePolicyDigest} {
		if d == "" && (value.Verdict == ProviderPolicyUnknownV1 || value.Verdict == ProviderPolicyNoGoV1) {
			continue
		}
		if err := validateSHA256("provider_policy."+f+"_digest", d); err != nil {
			return err
		}
	}
	return nil
}

func providerEvidenceDigest(domainName string, value any) (ProviderEvidenceDigestV1, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	h.Write([]byte(domainName))
	h.Write([]byte{0})
	h.Write(encoded)
	return ProviderEvidenceDigestV1(hex.EncodeToString(h.Sum(nil))), nil
}
func (value ProviderRouterResourceV1) Digest() (ProviderEvidenceDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	return providerEvidenceDigest("sessionless.provider-router-resource.v1", value)
}
func (value ProviderRoutePolicyV1) Digest() (ProviderEvidenceDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	return providerEvidenceDigest("sessionless.provider-route-policy.v1", value)
}
func (value ProviderCatalogObservationV1) Digest() (ProviderEvidenceDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	return providerEvidenceDigest("sessionless.provider-catalog.v1", value)
}
func (value ProviderCapabilityEvidenceV1) Digest() (ProviderEvidenceDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	return providerEvidenceDigest("sessionless.provider-capability.v1", value)
}
func (value ProviderPrivacyEvidenceV1) Digest() (ProviderEvidenceDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	return providerEvidenceDigest("sessionless.provider-privacy.v1", value)
}
func (value ProviderPriceObservationV1) Digest() (ProviderEvidenceDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	return providerEvidenceDigest("sessionless.provider-price.v1", value)
}
func (value ProviderPolicyEvidenceV1) Digest() (ProviderEvidenceDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	return providerEvidenceDigest("sessionless.provider-policy.v1", value)
}
