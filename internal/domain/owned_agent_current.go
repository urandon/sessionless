package domain

import (
	"bytes"
	"encoding/json"
	"math"
	"time"
)

const OwnedAgentCurrentVersionV1 uint32 = 1

type OwnedAgentCurrentStateV1 string

const (
	OwnedAgentCurrentActiveV1   OwnedAgentCurrentStateV1 = "active"
	OwnedAgentCurrentDisabledV1 OwnedAgentCurrentStateV1 = "disabled"
	OwnedAgentCurrentRevokedV1  OwnedAgentCurrentStateV1 = "revoked"
)

// These records are structural projections, not installed approval, payer
// permission, current SQL authority or an executable capability. The trusted
// publisher and transactional readers must establish those predicates separately.
type OwnedAgentProfileLocatorV1 struct {
	TenantID             TenantID               `json:"tenant_id"`
	OwnerUserID          UserID                 `json:"owner_user_id"`
	Profile              AgentLoopProfileV1     `json:"profile"`
	BackendProfileDigest string                 `json:"backend_profile_digest"`
	ResourceKind         ProviderResourceKindV1 `json:"resource_kind"`
	ResourceID           string                 `json:"resource_id"`
	ModelVendorID        string                 `json:"model_vendor_id"`
	ModelID              string                 `json:"model_id"`
	InputDataClass       ProviderDataClassV1    `json:"input_data_class"`
}

func (v OwnedAgentProfileLocatorV1) Validate() error {
	for _, validate := range []func() error{v.TenantID.Validate, v.OwnerUserID.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if v.Profile != AgentLoopOwnedPublicV1 || v.InputDataClass != ProviderDataPublicV1 {
		return ownedCurrentError("locator.profile", "requires the explicit owned public profile and public data")
	}
	switch v.ResourceKind {
	case ProviderResourceCredentiallessFixtureV1, ProviderResourceSubscriptionV1, ProviderResourceAPIAccountV1, ProviderResourceRouterAccountV1:
	default:
		return ownedCurrentError("locator.resource_kind", "is unsupported")
	}
	if err := ValidateSHA256Digest("owned_agent_current.locator.backend_profile_digest", v.BackendProfileDigest); err != nil {
		return err
	}
	if err := ValidateOpaqueID("owned_agent_current.locator.resource_id", v.ResourceID); err != nil {
		return err
	}
	if err := validateProviderToken("owned_agent_current.locator.model_vendor_id", v.ModelVendorID, 128); err != nil {
		return err
	}
	return validateProviderToken("owned_agent_current.locator.model_id", v.ModelID, 256)
}

func (v OwnedAgentProfileLocatorV1) Digest() (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return ownedCurrentDigest("sessionless.owned-agent-profile-locator.v1", v)
}

// Body references carry identity and bounds, never proof that the contents were
// read, reviewed or approved. Publishers must authenticate and verify exact
// bounded bytes outside retryable SQL callbacks before signing a publication.
type OwnedAgentAuthorityBodyRefV1 struct {
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
	Bytes  uint64 `json:"bytes"`
}

func (v OwnedAgentAuthorityBodyRefV1) Validate() error {
	if err := ValidateOpaqueID("owned_agent_current.body.ref", v.Ref); err != nil {
		return err
	}
	if err := ValidateSHA256Digest("owned_agent_current.body.digest", v.Digest); err != nil {
		return err
	}
	if v.Bytes == 0 || v.Bytes > 256<<10 {
		return ownedCurrentError("body.bytes", "must be positive and at most 256 KiB")
	}
	return nil
}

// New authority records have microsecond fidelity. Reject finer precision,
// rather than silently rounding away a boundary when storing YDB Timestamp.
type OwnedAgentAuthorityWindowV1 struct {
	ObservedAt  time.Time `json:"observed_at"`
	PublishedAt time.Time `json:"published_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}

func (v OwnedAgentAuthorityWindowV1) Clone() OwnedAgentAuthorityWindowV1 {
	v.ObservedAt, v.PublishedAt, v.ExpiresAt = v.ObservedAt.UTC(), v.PublishedAt.UTC(), v.ExpiresAt.UTC()
	return v
}

func (v OwnedAgentAuthorityWindowV1) Validate() error {
	for _, at := range []time.Time{v.ObservedAt, v.PublishedAt, v.ExpiresAt} {
		if !ownedCurrentTime(at) {
			return ownedCurrentError("window", "requires nonzero canonical-range microsecond timestamps")
		}
	}
	if v.PublishedAt.Before(v.ObservedAt) || !v.ExpiresAt.After(v.PublishedAt) {
		return ownedCurrentError("window", "publication must follow observation and precede exclusive expiry")
	}
	return nil
}

func (v OwnedAgentAuthorityWindowV1) ValidateAt(now time.Time) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if !ownedCurrentTime(now) || now.Before(v.PublishedAt) || !now.Before(v.ExpiresAt) {
		return ownedCurrentError("window", "must contain authoritative time excluding expiry")
	}
	return nil
}

type OwnedAgentProfileCurrentV1 struct {
	Version                  uint32                       `json:"version"`
	Locator                  OwnedAgentProfileLocatorV1   `json:"locator"`
	ProfileRevision          uint64                       `json:"profile_revision"`
	RevocationGeneration     uint64                       `json:"revocation_generation"`
	State                    OwnedAgentCurrentStateV1     `json:"state"`
	Backend                  HarnessBackendDescriptorV1   `json:"backend"`
	Resource                 ProviderResourceBindingV1    `json:"resource"`
	Substrate                SubstrateBindingV1           `json:"substrate"`
	InstallationID           string                       `json:"installation_id"`
	InstallationRevision     uint64                       `json:"installation_revision"`
	InstalledManifest        OwnedAgentAuthorityBodyRefV1 `json:"installed_manifest"`
	ReviewReceipt            OwnedAgentAuthorityBodyRefV1 `json:"review_receipt"`
	ProviderCatalogDigest    string                       `json:"provider_catalog_digest"`
	ProviderRouteDigest      string                       `json:"provider_route_digest"`
	CapabilityEvidenceDigest string                       `json:"capability_evidence_digest"`
	PrivacyPolicyDigest      string                       `json:"privacy_policy_digest"`
	EffectivePolicyDigest    string                       `json:"effective_policy_digest"`
	ProviderPriceDigest      string                       `json:"provider_price_digest"`
	ProviderTermsDigest      string                       `json:"provider_terms_digest"`
	SubstratePriceDigest     string                       `json:"substrate_price_digest"`
	SubstrateTermsDigest     string                       `json:"substrate_terms_digest"`
	SubstratePriceState      ExternalServicePriceStateV1  `json:"substrate_price_state"`
	BillingOwnerUserID       UserID                       `json:"billing_owner_user_id"`
	InputModalities          []AgentInputModalityV1       `json:"input_modalities"`
	ModelWindowTokens        uint64                       `json:"model_window_tokens"`
	TokenEstimatorVersion    string                       `json:"token_estimator_version"`
	CalibrationDigest        string                       `json:"calibration_digest"`
	InstructionVersion       string                       `json:"instruction_version"`
	CompactionPolicyDigest   string                       `json:"compaction_policy_digest"`
	MCPToolGrantDigest       string                       `json:"mcp_tool_grant_digest"`
	WebSearchGrantDigest     string                       `json:"web_search_grant_digest"`
	Limits                   AgentLoopLimitsV1            `json:"limits"`
	CostCeiling              AdmissionCostCeilingV2       `json:"cost_ceiling"`
	Window                   OwnedAgentAuthorityWindowV1  `json:"window"`
	UpdatedAt                time.Time                    `json:"updated_at"`
	MutationID               string                       `json:"mutation_id"`
}

func (v OwnedAgentProfileCurrentV1) Clone() OwnedAgentProfileCurrentV1 {
	v.InputModalities = append([]AgentInputModalityV1(nil), v.InputModalities...)
	v.CostCeiling = v.CostCeiling.Clone()
	v.Substrate.ProfileEvidenceExpiresAt = v.Substrate.ProfileEvidenceExpiresAt.UTC()
	v.Window, v.UpdatedAt = v.Window.Clone(), v.UpdatedAt.UTC()
	return v
}

func (v OwnedAgentProfileCurrentV1) Validate() error {
	if v.Version != OwnedAgentCurrentVersionV1 || v.ProfileRevision == 0 || v.RevocationGeneration == 0 || v.InstallationRevision == 0 {
		return ownedCurrentError("profile.identity", "requires version 1 and positive revisions/generation")
	}
	if v.State != OwnedAgentCurrentActiveV1 && v.State != OwnedAgentCurrentDisabledV1 && v.State != OwnedAgentCurrentRevokedV1 {
		return ownedCurrentError("profile.state", "is unsupported")
	}
	for _, validate := range []func() error{v.Locator.Validate, v.Backend.Validate, v.Resource.Validate, v.Substrate.Validate, v.InstalledManifest.Validate, v.ReviewReceipt.Validate, v.BillingOwnerUserID.Validate, v.Limits.Validate, v.CostCeiling.Validate, v.Window.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if err := v.Resource.ValidateForBackend(v.Backend); err != nil {
		return err
	}
	// The substrate's outer runner and the model backend are independently
	// installed artifacts, not aliases of the same executable. The installed
	// manifest authenticates their association; both descriptors are sealed here.
	if v.Locator.BackendProfileDigest != v.Backend.BackendProfileDigest || v.Locator.OwnerUserID != v.Resource.OwnerUserID || v.Locator.ResourceKind != v.Resource.Kind || v.Locator.ResourceID != v.Resource.ResourceID {
		return ownedCurrentError("profile.scope", "must exact-match locator, backend and resource")
	}
	for _, token := range []struct{ name, value string }{{"installation_id", v.InstallationID}, {"token_estimator_version", v.TokenEstimatorVersion}, {"instruction_version", v.InstructionVersion}, {"mutation_id", v.MutationID}} {
		if err := ValidateOpaqueID("owned_agent_current.profile."+token.name, token.value); err != nil {
			return err
		}
	}
	for _, digest := range []struct{ name, value string }{{"catalog", v.ProviderCatalogDigest}, {"route", v.ProviderRouteDigest}, {"capability", v.CapabilityEvidenceDigest}, {"privacy", v.PrivacyPolicyDigest}, {"policy", v.EffectivePolicyDigest}, {"price", v.ProviderPriceDigest}, {"terms", v.ProviderTermsDigest}, {"substrate_price", v.SubstratePriceDigest}, {"substrate_terms", v.SubstrateTermsDigest}, {"calibration", v.CalibrationDigest}, {"compaction", v.CompactionPolicyDigest}, {"mcp", v.MCPToolGrantDigest}, {"search", v.WebSearchGrantDigest}} {
		if err := ValidateSHA256Digest("owned_agent_current.profile."+digest.name, digest.value); err != nil {
			return err
		}
	}
	if len(v.InputModalities) == 0 || len(v.InputModalities) > 3 {
		return ownedCurrentError("profile.modalities", "requires a bounded nonempty canonical set")
	}
	previous := AgentInputModalityV1("")
	for _, modality := range v.InputModalities {
		if (modality != AgentInputTextV1 && modality != AgentInputImageV1 && modality != AgentInputFileV1) || modality <= previous {
			return ownedCurrentError("profile.modalities", "requires supported unique lexical order")
		}
		previous = modality
	}
	if v.ModelWindowTokens == 0 || v.ModelWindowTokens > 1<<20 || v.ModelWindowTokens < v.Limits.MaxInputTokensPerCall+v.Limits.MaxOutputTokensPerCall {
		return ownedCurrentError("profile.model_window", "must cover the input/output reservation within the finite model window")
	}
	// A zero aggregate ceiling does not establish that the installed substrate
	// is free. Its independent price/terms source must explicitly say so.
	amount := v.CostCeiling.MaxSubstrateAmountMicrounits
	switch v.SubstratePriceState {
	case ExternalServicePriceUnknownV1:
		if v.CostCeiling.SubstratePriceState != CostEvidenceUnknownV1 || amount != nil {
			return ownedCurrentError("profile.substrate_price", "unknown source cannot carry known pricing or a numeric amount")
		}
	case ExternalServicePriceKnownFreeV1:
		if v.CostCeiling.SubstratePriceState != CostEvidenceKnownV1 || amount == nil || *amount != 0 {
			return ownedCurrentError("profile.substrate_price", "explicit known-free source requires explicit zero")
		}
	case ExternalServicePriceKnownPaidV1:
		if v.CostCeiling.SubstratePriceState != CostEvidenceKnownV1 || amount == nil || *amount == 0 {
			return ownedCurrentError("profile.substrate_price", "explicit known-paid source requires a positive finite amount")
		}
	default:
		return ownedCurrentError("profile.substrate_price", "requires independent explicit source pricing state")
	}
	for _, service := range []struct {
		grant string
		cost  *ExternalServiceCostCeilingV1
	}{{v.MCPToolGrantDigest, v.CostCeiling.MCPToolCost}, {v.WebSearchGrantDigest, v.CostCeiling.WebSearchCost}} {
		if service.cost == nil {
			return ownedCurrentError("profile.service", "requires both explicit independently priced service grants")
		}
		if err := service.cost.ValidateForOwnerGrantCurrency(v.Locator.TenantID, v.Locator.OwnerUserID, v.BillingOwnerUserID, service.grant, v.CostCeiling.Currency); err != nil {
			return err
		}
		if !ownedCurrentTime(service.cost.PriceObservedAt) || !ownedCurrentTime(service.cost.PriceExpiresAt) {
			return ownedCurrentError("profile.service_time", "requires exact microsecond price times")
		}
	}
	if err := v.CostCeiling.ValidateForSubstrate(v.Substrate); err != nil {
		return err
	}
	if !ownedCurrentTime(v.UpdatedAt) || v.UpdatedAt.Before(v.Window.PublishedAt) || !ownedCurrentTime(v.CostCeiling.PriceObservedAt) || !ownedCurrentTime(v.CostCeiling.PriceExpiresAt) || !ownedCurrentTime(v.Substrate.ProfileEvidenceExpiresAt) || v.Window.ObservedAt.Before(v.CostCeiling.PriceObservedAt) || v.Window.ExpiresAt.After(v.CostCeiling.PriceExpiresAt) || v.Window.ExpiresAt.After(v.Substrate.ProfileEvidenceExpiresAt) || v.Limits.MaxActiveDuration > v.CostCeiling.MaxActiveDuration || v.Limits.MaxCleanupDuration > v.CostCeiling.MaxCleanupAndReconcileDuration {
		return ownedCurrentError("profile.window", "must preserve source/price/substrate validity and admitted durations")
	}
	return nil
}

func (v OwnedAgentProfileCurrentV1) ValidateAt(now time.Time) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if v.State != OwnedAgentCurrentActiveV1 || now.Before(v.UpdatedAt) {
		return ownedCurrentError("profile.current", "requires active nonfuture current projection")
	}
	if err := v.Window.ValidateAt(now); err != nil {
		return err
	}
	return v.CostCeiling.ValidateAt(now)
}

func (v OwnedAgentProfileCurrentV1) Digest() (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return ownedCurrentDigest("sessionless.owned-agent-profile-current.v1", v.Clone())
}

// Transition validation is not writer authentication or compare-and-swap.
// The Store must reread the exact predecessor and perform the atomic CAS.
func (v OwnedAgentProfileCurrentV1) ValidateTransitionFrom(old OwnedAgentProfileCurrentV1) error {
	if err := old.Validate(); err != nil {
		return err
	}
	if err := v.Validate(); err != nil {
		return err
	}
	if old.Locator != v.Locator || old.RevocationGeneration == math.MaxUint64 || v.RevocationGeneration != old.RevocationGeneration+1 || v.ProfileRevision < old.ProfileRevision || v.InstallationRevision < old.InstallationRevision || v.UpdatedAt.Before(old.UpdatedAt) || v.MutationID == old.MutationID {
		return ownedCurrentError("profile.transition", "requires exact stable locator, next generation and monotonic new intent")
	}
	if old.State == OwnedAgentCurrentRevokedV1 && v.State == OwnedAgentCurrentDisabledV1 {
		return ownedCurrentError("profile.transition", "cannot downgrade a revoked tombstone")
	}
	if v.State == OwnedAgentCurrentActiveV1 && (v.ProfileRevision <= old.ProfileRevision || v.InstallationRevision <= old.InstallationRevision || v.ReviewReceipt.Digest == old.ReviewReceipt.Digest) {
		return ownedCurrentError("profile.transition", "active replacement requires a newly installed reviewed revision")
	}
	if v.State != OwnedAgentCurrentActiveV1 {
		expected := old.Clone()
		expected.State, expected.RevocationGeneration, expected.UpdatedAt, expected.MutationID = v.State, v.RevocationGeneration, v.UpdatedAt, v.MutationID
		left, _ := json.Marshal(expected.Clone())
		right, _ := json.Marshal(v.Clone())
		if !bytes.Equal(left, right) {
			return ownedCurrentError("profile.transition", "disable or revoke must preserve the original installed package")
		}
	}
	return nil
}

func (v OwnedAgentProfileCurrentV1) ValidateInitial() error {
	if err := v.Validate(); err != nil {
		return err
	}
	if v.State != OwnedAgentCurrentActiveV1 || v.RevocationGeneration != 1 || !v.UpdatedAt.Equal(v.Window.PublishedAt) {
		return ownedCurrentError("profile.initial", "requires the original active generation-one publication")
	}
	return nil
}

func ownedCurrentTime(at time.Time) bool {
	return !at.IsZero() && at.UTC().Year() >= 1 && at.UTC().Year() <= 9999 && at.Nanosecond()%1000 == 0
}

func ownedCurrentError(field, reason string) error {
	return ValidationError{Field: "owned_agent_current." + field, Reason: reason}
}

// Canonical initial V1 encoding seals every explicit JSON field and optional
// presence, in declaration order, with UTC microsecond times and exact integers.
// The strict codec accepts only these producer bytes; there is no legacy shape.
func ownedCurrentDigest(kind string, v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", ownedCurrentError("digest", "cannot encode canonical record")
	}
	if len(b) > OwnedAgentCurrentMaxRecordBytesV1 {
		return "", ownedCurrentError("digest", "record exceeds the 32 KiB bound")
	}
	d := newServerlessDigest(kind)
	d.bytes(b)
	return d.sum(), nil
}
