package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"time"
)

type AttemptSpendingComponentKindV1 string

const (
	AttemptSpendingProviderV1      AttemptSpendingComponentKindV1 = "provider"
	AttemptSpendingSubstrateV1     AttemptSpendingComponentKindV1 = "substrate"
	AttemptSpendingMCPToolV1       AttemptSpendingComponentKindV1 = "mcp_tool"
	AttemptSpendingWebSearchV1     AttemptSpendingComponentKindV1 = "web_search"
	AttemptSpendingFixtureSourceV1                                = "fixture_installation"
)

// This is an exact per-Attempt authorization locator, never an account balance
// or reusable quota. Structural validation does not establish payer permission.
type AttemptSpendingAuthorizationLocatorV1 struct {
	TenantID           TenantID                       `json:"tenant_id"`
	AttemptID          AttemptID                      `json:"attempt_id"`
	ComponentKind      AttemptSpendingComponentKindV1 `json:"component_kind"`
	BillingOwnerUserID UserID                         `json:"billing_owner_user_id"`
}

func (v AttemptSpendingAuthorizationLocatorV1) Validate() error {
	for _, validate := range []func() error{v.TenantID.Validate, v.AttemptID.Validate, v.BillingOwnerUserID.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	switch v.ComponentKind {
	case AttemptSpendingProviderV1, AttemptSpendingSubstrateV1, AttemptSpendingMCPToolV1, AttemptSpendingWebSearchV1:
		return nil
	default:
		return ownedCurrentError("spending_locator.component_kind", "is unsupported")
	}
}

func (v AttemptSpendingAuthorizationLocatorV1) Digest() (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return ownedCurrentDigest("sessionless.attempt-spending-authorization-locator.v1", v)
}

type OwnedAgentProfileReferenceV1 struct {
	Locator              OwnedAgentProfileLocatorV1 `json:"locator"`
	RevocationGeneration uint64                     `json:"revocation_generation"`
	Digest               string                     `json:"digest"`
}

func (v OwnedAgentProfileReferenceV1) Validate() error {
	if err := v.Locator.Validate(); err != nil {
		return err
	}
	if v.RevocationGeneration == 0 {
		return ownedCurrentError("profile_reference.generation", "must be positive")
	}
	return ValidateSHA256Digest("owned_agent_current.profile_reference.digest", v.Digest)
}

// There is deliberately no admission, policy, binding, invocation or operation
// digest in this earlier source contract. No balance/spend/refund/transfer or
// process-local capability belongs here. Unknown pricing is representable but
// never passes freshness/composition as executable spending authority.
type AttemptSpendingAuthorizationCurrentV1 struct {
	Version                    uint32                                `json:"version"`
	Locator                    AttemptSpendingAuthorizationLocatorV1 `json:"locator"`
	RunID                      RunID                                 `json:"run_id"`
	SessionID                  SessionID                             `json:"session_id"`
	OwnerUserID                UserID                                `json:"owner_user_id"`
	SourceKind                 string                                `json:"source_kind"`
	SourceInstallationID       string                                `json:"source_installation_id"`
	SourceInstallationRevision uint64                                `json:"source_installation_revision"`
	SourceProfile              OwnedAgentProfileReferenceV1          `json:"source_profile"`
	SourceAuthorization        OwnedAgentAuthorityBodyRefV1          `json:"source_authorization"`
	ResourceID                 string                                `json:"resource_id"`
	ResourceRevision           uint64                                `json:"resource_revision"`
	ResourceGeneration         uint64                                `json:"resource_generation"`
	ProfileID                  string                                `json:"profile_id"`
	ProfileRevision            uint64                                `json:"profile_revision"`
	ScopedGrantDigest          string                                `json:"scoped_grant_digest"`
	Currency                   string                                `json:"currency"`
	PriceRevision              string                                `json:"price_revision"`
	PriceEvidenceDigest        string                                `json:"price_evidence_digest"`
	TermsEvidenceDigest        string                                `json:"terms_evidence_digest"`
	PriceState                 ExternalServicePriceStateV1           `json:"price_state"`
	MaxAmountMicrounits        *uint64                               `json:"max_amount_microunits,omitempty"`
	RevocationGeneration       uint64                                `json:"revocation_generation"`
	State                      OwnedAgentCurrentStateV1              `json:"state"`
	Window                     OwnedAgentAuthorityWindowV1           `json:"window"`
	UpdatedAt                  time.Time                             `json:"updated_at"`
	MutationID                 string                                `json:"mutation_id"`
}

func (v AttemptSpendingAuthorizationCurrentV1) Clone() AttemptSpendingAuthorizationCurrentV1 {
	v.MaxAmountMicrounits = cloneUint64(v.MaxAmountMicrounits)
	v.Window, v.UpdatedAt = v.Window.Clone(), v.UpdatedAt.UTC()
	return v
}

func (v AttemptSpendingAuthorizationCurrentV1) Validate() error {
	if v.Version != OwnedAgentCurrentVersionV1 || v.RevocationGeneration == 0 || v.SourceInstallationRevision == 0 || v.ResourceRevision == 0 || v.ProfileRevision == 0 {
		return ownedCurrentError("spending.identity", "requires version 1 and positive revisions/generation")
	}
	if v.SourceKind != AttemptSpendingFixtureSourceV1 || (v.State != OwnedAgentCurrentActiveV1 && v.State != OwnedAgentCurrentRevokedV1) {
		return ownedCurrentError("spending.source_state", "requires explicit fixture source and active or revoked state")
	}
	for _, validate := range []func() error{v.Locator.Validate, v.RunID.Validate, v.SessionID.Validate, v.OwnerUserID.Validate, v.SourceProfile.Validate, v.SourceAuthorization.Validate, v.Window.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if v.SourceProfile.Locator.TenantID != v.Locator.TenantID || v.SourceProfile.Locator.OwnerUserID != v.OwnerUserID || v.SourceProfile.Locator.ResourceKind != ProviderResourceCredentiallessFixtureV1 {
		return ownedCurrentError("spending.scope", "must exact-match original tenant/principal and fixture source")
	}
	for _, token := range []struct{ name, value string }{{"installation", v.SourceInstallationID}, {"resource_id", v.ResourceID}, {"profile_id", v.ProfileID}, {"price_revision", v.PriceRevision}, {"mutation_id", v.MutationID}} {
		if err := ValidateOpaqueID("owned_agent_current.spending."+token.name, token.value); err != nil {
			return err
		}
	}
	for _, digest := range []struct{ name, value string }{{"grant", v.ScopedGrantDigest}, {"price", v.PriceEvidenceDigest}, {"terms", v.TermsEvidenceDigest}} {
		if err := ValidateSHA256Digest("owned_agent_current.spending."+digest.name, digest.value); err != nil {
			return err
		}
	}
	if !validCurrencyV1(v.Currency) || !ownedCurrentTime(v.UpdatedAt) || v.UpdatedAt.Before(v.Window.PublishedAt) {
		return ownedCurrentError("spending.currency_time", "requires explicit currency and monotonic microsecond update")
	}
	switch v.PriceState {
	case ExternalServicePriceUnknownV1:
		if v.MaxAmountMicrounits != nil {
			return ownedCurrentError("spending.amount", "unknown price cannot carry a numeric authorization")
		}
	case ExternalServicePriceKnownFreeV1:
		if v.MaxAmountMicrounits == nil || *v.MaxAmountMicrounits != 0 {
			return ownedCurrentError("spending.amount", "known free requires explicit zero")
		}
	case ExternalServicePriceKnownPaidV1:
		if v.MaxAmountMicrounits == nil || *v.MaxAmountMicrounits == 0 {
			return ownedCurrentError("spending.amount", "synthetic paid requires explicit positive finite amount")
		}
	default:
		return ownedCurrentError("spending.price_state", "is unsupported")
	}
	if v.ResourceGeneration == 0 && v.Locator.ComponentKind != AttemptSpendingProviderV1 {
		return ownedCurrentError("spending.resource_generation", "zero is exclusive to the credentialless provider fixture")
	}
	source, err := v.SourceCanonicalBytes()
	if err != nil {
		return err
	}
	hash := sha256.Sum256(source)
	if v.SourceAuthorization.Bytes != uint64(len(source)) || v.SourceAuthorization.Digest != hex.EncodeToString(hash[:]) {
		return ownedCurrentError("spending.source_authorization", "must bind the exact immutable source envelope bytes and length")
	}
	return nil
}

// SourceCanonicalBytes is the initial V1 immutable source envelope projection:
// all declaration fields except the source's own body reference, current state,
// generation/update/audit fields and DB publication time. Top-level and window
// members are lexical JSON keys; raw integer values are never decoded as floats.
// Observation/expiry and every original scope/price/amount field remain sealed.
// This identity check is not source authentication: only the trusted publisher
// may verify who approved these bytes and whether that exact source is current.
func (v AttemptSpendingAuthorizationCurrentV1) SourceCanonicalBytes() ([]byte, error) {
	raw, err := encodeOwnedCurrent(v.Clone())
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, ownedCurrentError("spending.source", "cannot encode source envelope")
	}
	for _, key := range []string{"source_authorization", "revocation_generation", "state", "updated_at", "mutation_id"} {
		delete(fields, key)
	}
	var window map[string]json.RawMessage
	if err := json.Unmarshal(fields["window"], &window); err != nil {
		return nil, ownedCurrentError("spending.source", "cannot encode source window")
	}
	delete(window, "published_at")
	fields["window"], err = json.Marshal(window)
	if err != nil {
		return nil, ownedCurrentError("spending.source", "cannot encode source window")
	}
	return encodeOwnedCurrent(fields)
}

func (v AttemptSpendingAuthorizationCurrentV1) ValidateAt(now time.Time) error {
	if err := v.Validate(); err != nil {
		return err
	}
	if v.State != OwnedAgentCurrentActiveV1 || v.PriceState == ExternalServicePriceUnknownV1 || now.Before(v.UpdatedAt) {
		return ownedCurrentError("spending.current", "requires active known-price nonfuture projection")
	}
	return v.Window.ValidateAt(now)
}

// This exact structural comparison must be applied to a freshly read profile
// in the same SQL transaction. Accepting a caller-supplied profile here does not
// authenticate installation, payer intent or a current database snapshot.
func (v AttemptSpendingAuthorizationCurrentV1) ValidateAgainstProfile(p OwnedAgentProfileCurrentV1, now time.Time) error {
	if err := v.ValidateAt(now); err != nil {
		return err
	}
	if err := p.ValidateAt(now); err != nil {
		return err
	}
	digest, err := p.Digest()
	if err != nil {
		return err
	}
	if v.SourceProfile.Locator != p.Locator || v.SourceProfile.RevocationGeneration != p.RevocationGeneration || v.SourceProfile.Digest != digest || v.SourceInstallationID != p.InstallationID || v.SourceInstallationRevision != p.InstallationRevision || v.Locator.BillingOwnerUserID != p.BillingOwnerUserID || v.Window.ObservedAt.Before(p.Window.ObservedAt) || v.Window.PublishedAt.Before(p.Window.PublishedAt) || v.Window.ExpiresAt.After(p.Window.ExpiresAt) || p.Backend.BackendKind != HarnessBackendDeterministicFixtureV1 || p.Backend.ProviderContractKind != ProviderContractCredentiallessFixtureV1 || p.Substrate.Kind != SubstrateDeterministicFixtureV1 {
		return ownedCurrentError("spending.source", "requires the exact original active fixture installation and window")
	}
	resourceID, resourceRev, resourceGen, profileID, profileRev := p.Resource.ResourceID, p.Resource.Revision, p.Resource.CredentialGeneration, string(p.Locator.Profile), p.ProfileRevision
	grant, price, terms, priceRevision, amount := p.EffectivePolicyDigest, p.ProviderPriceDigest, p.ProviderTermsDigest, p.CostCeiling.PriceRevision, p.CostCeiling.MaxProviderAmountMicrounits
	state := ExternalServicePriceKnownPaidV1
	if p.CostCeiling.ProviderPriceState == ProviderPriceKnownFreeV1 {
		state = ExternalServicePriceKnownFreeV1
	}
	switch v.Locator.ComponentKind {
	case AttemptSpendingSubstrateV1:
		resourceID, resourceRev, resourceGen = p.Substrate.ProfileID, p.Substrate.ProfileRevision, p.RevocationGeneration
		profileID, profileRev = p.Substrate.ProfileID, p.Substrate.ProfileRevision
		grant, price, terms, amount = p.Substrate.ProfileDigest, p.SubstratePriceDigest, p.SubstrateTermsDigest, p.CostCeiling.MaxSubstrateAmountMicrounits
		state = p.SubstratePriceState
	case AttemptSpendingMCPToolV1, AttemptSpendingWebSearchV1:
		service := p.CostCeiling.MCPToolCost
		if v.Locator.ComponentKind == AttemptSpendingWebSearchV1 {
			service = p.CostCeiling.WebSearchCost
		}
		resourceID, resourceRev, resourceGen, profileID, profileRev = service.ResourceID, service.ResourceRevision, service.RevocationGeneration, service.ProfileID, service.ProfileRevision
		grant, price, terms, priceRevision, amount, state = service.ScopedGrantDigest, service.PriceEvidenceDigest, service.TermsEvidenceDigest, service.PriceRevision, service.MaxAmountMicrounits, service.PriceState
	}
	if v.ResourceID != resourceID || v.ResourceRevision != resourceRev || v.ResourceGeneration != resourceGen || v.ProfileID != profileID || v.ProfileRevision != profileRev || v.ScopedGrantDigest != grant || v.Currency != p.CostCeiling.Currency || v.PriceRevision != priceRevision || v.PriceEvidenceDigest != price || v.TermsEvidenceDigest != terms || v.PriceState != state || amount == nil || v.MaxAmountMicrounits == nil || *v.MaxAmountMicrounits > *amount {
		return ownedCurrentError("spending.envelope", "must exact-match original component, price, terms, payer and finite ceiling")
	}
	return nil
}

func (v AttemptSpendingAuthorizationCurrentV1) Digest() (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	return ownedCurrentDigest("sessionless.attempt-spending-authorization-current.v1", v.Clone())
}

// Initial shape does not prove row absence, source authentication or admission.
func (v AttemptSpendingAuthorizationCurrentV1) ValidateInitial() error {
	if err := v.Validate(); err != nil {
		return err
	}
	if v.State != OwnedAgentCurrentActiveV1 || v.RevocationGeneration != 1 || !v.UpdatedAt.Equal(v.Window.PublishedAt) {
		return ownedCurrentError("spending.initial", "requires the original active generation-one publication")
	}
	return nil
}

// Revoke is the only update. Exact replay is a writer readback, not a new
// transition. This pure check does not prove initial row absence or CAS.
func (v AttemptSpendingAuthorizationCurrentV1) ValidateTransitionFrom(old AttemptSpendingAuthorizationCurrentV1) error {
	if err := old.Validate(); err != nil {
		return err
	}
	if err := v.Validate(); err != nil {
		return err
	}
	if old.State != OwnedAgentCurrentActiveV1 || v.State != OwnedAgentCurrentRevokedV1 || old.RevocationGeneration == math.MaxUint64 || v.RevocationGeneration != old.RevocationGeneration+1 || v.UpdatedAt.Before(old.UpdatedAt) || v.MutationID == old.MutationID {
		return ownedCurrentError("spending.transition", "permits only original active authorization to next-generation revoked tombstone")
	}
	expected := old.Clone()
	expected.State, expected.RevocationGeneration, expected.UpdatedAt, expected.MutationID = v.State, v.RevocationGeneration, v.UpdatedAt, v.MutationID
	left, _ := json.Marshal(expected.Clone())
	right, _ := json.Marshal(v.Clone())
	if !bytes.Equal(left, right) {
		return ownedCurrentError("spending.transition", "cannot change the immutable amount, scope, source, price or original window")
	}
	return nil
}
