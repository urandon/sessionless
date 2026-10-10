package domain

import "time"

const ExternalServiceCostCeilingVersionV1 uint32 = 1

type (
	ExternalServiceKindV1              string
	ExternalServicePriceStateV1        string
	ExternalServiceCostCeilingDigestV1 string
)

const (
	ExternalServiceMCPToolV1   ExternalServiceKindV1 = "mcp_tool"
	ExternalServiceWebSearchV1 ExternalServiceKindV1 = "web_search"

	ExternalServicePriceUnknownV1   ExternalServicePriceStateV1 = "unknown"
	ExternalServicePriceKnownFreeV1 ExternalServicePriceStateV1 = "known_free"
	ExternalServicePriceKnownPaidV1 ExternalServicePriceStateV1 = "known_paid"
)

// ExternalServiceCostCeilingV1 is a component of the canonical admission cost
// ceiling, not a separate ledger, budget or execution authority. Its amount is
// the aggregate reservation for this exact external service grant. An absent
// component grants no external-tool authority; it does not assert free access.
// The scoped grant and price/terms evidence are independently constructed before
// the enclosing cost ceiling, loop policy and harness binding. No enclosing
// authority digest enters this component, preserving acyclic construction.
type ExternalServiceCostCeilingV1 struct {
	Version              uint32                      `json:"version"`
	Kind                 ExternalServiceKindV1       `json:"kind"`
	TenantID             TenantID                    `json:"tenant_id"`
	OwnerUserID          UserID                      `json:"owner_user_id"`
	BillingOwnerUserID   UserID                      `json:"billing_owner_user_id"`
	ResourceID           string                      `json:"resource_id"`
	ResourceRevision     uint64                      `json:"resource_revision"`
	RevocationGeneration uint64                      `json:"revocation_generation"`
	ProfileID            string                      `json:"profile_id"`
	ProfileRevision      uint64                      `json:"profile_revision"`
	ScopedGrantDigest    string                      `json:"scoped_grant_digest"`
	Currency             string                      `json:"currency"`
	PriceRevision        string                      `json:"price_revision"`
	PriceObservedAt      time.Time                   `json:"price_observed_at"`
	PriceExpiresAt       time.Time                   `json:"price_expires_at"`
	PriceEvidenceDigest  string                      `json:"price_evidence_digest"`
	TermsEvidenceDigest  string                      `json:"terms_evidence_digest"`
	PriceState           ExternalServicePriceStateV1 `json:"price_state"`
	MaxAmountMicrounits  *uint64                     `json:"max_amount_microunits,omitempty"`
}

func (value ExternalServiceCostCeilingV1) Clone() ExternalServiceCostCeilingV1 {
	clone := value
	clone.MaxAmountMicrounits = cloneUint64(value.MaxAmountMicrounits)
	clone.PriceObservedAt = value.PriceObservedAt.UTC()
	clone.PriceExpiresAt = value.PriceExpiresAt.UTC()
	return clone
}

// Validate checks bounded immutable structure, including independent evidence
// locators. It does not prove that evidence is authentic, approved or current.
// Unknown prices are representable only without a numeric reservation.
// Their evidence digests identify unknown-price/terms reports, not numeric
// authorization; even anonymous access requires separate reviewed proof of free
// pricing and acceptable terms before it can be classified as known_free.
func (value ExternalServiceCostCeilingV1) Validate() error {
	const prefix = "external_service_cost_ceiling."
	if value.Version != ExternalServiceCostCeilingVersionV1 {
		return ValidationError{Field: prefix + "version", Reason: "must equal 1"}
	}
	if value.Kind != ExternalServiceMCPToolV1 && value.Kind != ExternalServiceWebSearchV1 {
		return ValidationError{Field: prefix + "kind", Reason: "must be mcp_tool or web_search"}
	}
	for _, field := range []struct{ name, value string }{
		{"tenant_id", string(value.TenantID)}, {"owner_user_id", string(value.OwnerUserID)},
		{"billing_owner_user_id", string(value.BillingOwnerUserID)}, {"resource_id", value.ResourceID},
		{"profile_id", value.ProfileID}, {"price_revision", value.PriceRevision},
	} {
		if err := ValidateOpaqueID(prefix+field.name, field.value); err != nil {
			return err
		}
	}
	for _, field := range []struct {
		name  string
		value uint64
	}{
		{"resource_revision", value.ResourceRevision}, {"revocation_generation", value.RevocationGeneration},
		{"profile_revision", value.ProfileRevision},
	} {
		if field.value == 0 {
			return ValidationError{Field: prefix + field.name, Reason: "must be positive"}
		}
	}
	for _, field := range []struct{ name, value string }{
		{"scoped_grant_digest", value.ScopedGrantDigest}, {"price_evidence_digest", value.PriceEvidenceDigest},
		{"terms_evidence_digest", value.TermsEvidenceDigest},
	} {
		if err := validateDigestToken(prefix+field.name, field.value); err != nil {
			return err
		}
	}
	if !validCurrencyV1(value.Currency) {
		return ValidationError{Field: prefix + "currency", Reason: "must be three uppercase ASCII letters"}
	}
	if value.PriceObservedAt.IsZero() || value.PriceObservedAt.UTC().Year() < 1 || value.PriceExpiresAt.UTC().Year() > 9999 || !value.PriceExpiresAt.After(value.PriceObservedAt) {
		return ValidationError{Field: prefix + "price_expires_at", Reason: "must follow a nonzero observation in the canonical UTC timestamp range"}
	}
	switch value.PriceState {
	case ExternalServicePriceUnknownV1:
		if value.MaxAmountMicrounits != nil {
			return ValidationError{Field: prefix + "max_amount_microunits", Reason: "unknown price must not carry a numeric ceiling"}
		}
	case ExternalServicePriceKnownFreeV1:
		if value.MaxAmountMicrounits == nil || *value.MaxAmountMicrounits != 0 {
			return ValidationError{Field: prefix + "max_amount_microunits", Reason: "known free requires explicit zero and independent price/terms evidence"}
		}
	case ExternalServicePriceKnownPaidV1:
		if value.MaxAmountMicrounits == nil || *value.MaxAmountMicrounits == 0 {
			return ValidationError{Field: prefix + "max_amount_microunits", Reason: "known paid requires a positive bounded reservation"}
		}
	default:
		return ValidationError{Field: prefix + "price_state", Reason: "is unsupported"}
	}
	return nil
}

// ValidateAt applies the executable price window [observation, expiry), but is
// not trusted registry, price or terms approval. The actual store admission and
// before-send gates must reload current owner/billing authority, resource/profile
// revisions, revocation, grant, approved evidence and the enclosing total budget.
func (value ExternalServiceCostCeilingV1) ValidateAt(now time.Time) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if now.IsZero() || now.Before(value.PriceObservedAt) || !now.Before(value.PriceExpiresAt) {
		return ValidationError{Field: "external_service_cost_ceiling.price_expires_at", Reason: "must contain authoritative execution time, excluding expiry"}
	}
	if value.PriceState == ExternalServicePriceUnknownV1 {
		return ValidationError{Field: "external_service_cost_ceiling.price_state", Reason: "unknown price cannot authorize execution"}
	}
	return nil
}

// ValidateForOwnerGrantCurrency checks exact composition, not live authorization.
// The caller must additionally match the service kind and exact resource/profile
// revisions and revocation generation against the independently validated grant.
func (value ExternalServiceCostCeilingV1) ValidateForOwnerGrantCurrency(tenantID TenantID, ownerUserID, billingOwnerUserID UserID, scopedGrantDigest, currency string) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if value.TenantID != tenantID || value.OwnerUserID != ownerUserID || value.BillingOwnerUserID != billingOwnerUserID || value.ScopedGrantDigest != scopedGrantDigest || value.Currency != currency {
		return ValidationError{Field: "external_service_cost_ceiling", Reason: "must exact-match tenant, owner, billing owner, scoped grant and currency"}
	}
	return nil
}

func (value ExternalServiceCostCeilingV1) Digest() (ExternalServiceCostCeilingDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	digest := newServerlessDigest("sessionless.external-service-cost-ceiling.v1")
	digest.uint(uint64(value.Version))
	for _, field := range []string{string(value.Kind), string(value.TenantID), string(value.OwnerUserID), string(value.BillingOwnerUserID), value.ResourceID} {
		digest.str(field)
	}
	digest.uint(value.ResourceRevision)
	digest.uint(value.RevocationGeneration)
	digest.str(value.ProfileID)
	digest.uint(value.ProfileRevision)
	for _, field := range []string{value.ScopedGrantDigest, value.Currency, value.PriceRevision} {
		digest.str(field)
	}
	digest.instant(value.PriceObservedAt)
	digest.instant(value.PriceExpiresAt)
	for _, field := range []string{value.PriceEvidenceDigest, value.TermsEvidenceDigest, string(value.PriceState)} {
		digest.str(field)
	}
	digest.optionalUint(value.MaxAmountMicrounits)
	return ExternalServiceCostCeilingDigestV1(digest.sum()), nil
}

func (digest ExternalServiceCostCeilingDigestV1) Validate() error {
	return validateDigestToken("external_service_cost_ceiling_digest", string(digest))
}
