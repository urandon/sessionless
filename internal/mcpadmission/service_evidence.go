package mcpadmission

import (
	"bytes"
	"encoding/json"
	"net/url"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const ServiceEvidenceBytesV1 uint64 = 8 << 10

// MCPPriceEvidenceV1 is an operator's explicit interpretation of retained
// service documentation, not a caller approval or a measured upstream tariff.
// The finite amount covers the complete admitted grant envelope, not a quote
// calculated per call. Actual document bytes, private Source custody, current
// spending and exact-service qualification are separate requirements.
// Construction is grant -> reports -> cost -> profile; no enclosing digest or
// future Attempt identity enters either report.
type MCPPriceEvidenceV1 struct {
	Version             uint32                              `json:"version"`
	GrantDigest         string                              `json:"grant_digest"`
	Currency            string                              `json:"currency"`
	PriceRevision       string                              `json:"price_revision"`
	PriceState          domain.ExternalServicePriceStateV1  `json:"price_state"`
	MaxAmountMicrounits *uint64                             `json:"max_amount_microunits,omitempty"`
	SourceURL           string                              `json:"source_url"`
	SourceBody          domain.OwnedAgentAuthorityBodyRefV1 `json:"source_body"`
	ObservedAt          time.Time                           `json:"observed_at"`
	ExpiresAt           time.Time                           `json:"expires_at"`
}

// MCPTermsEvidenceV1 declares that the operator reviewed and accepted these
// exact retained terms for this grant's public-data/use boundary (including its
// data policy and consent). It is deliberately
// not a boolean terms-approved DTO. URLs are provenance DATA, never destinations
// that a reader, verifier or SQL retry is allowed to fetch.
type MCPTermsEvidenceV1 struct {
	Version     uint32                              `json:"version"`
	GrantDigest string                              `json:"grant_digest"`
	SourceURL   string                              `json:"source_url"`
	SourceBody  domain.OwnedAgentAuthorityBodyRefV1 `json:"source_body"`
	ObservedAt  time.Time                           `json:"observed_at"`
	ExpiresAt   time.Time                           `json:"expires_at"`
}

func (MCPPriceEvidenceV1) String() string     { return "MCPPriceEvidenceV1{content:[redacted]}" }
func (v MCPPriceEvidenceV1) GoString() string { return v.String() }
func (MCPTermsEvidenceV1) String() string     { return "MCPTermsEvidenceV1{content:[redacted]}" }
func (v MCPTermsEvidenceV1) GoString() string { return v.String() }

func validServiceEvidence(version uint32, grant, sourceURL string, ref domain.OwnedAgentAuthorityBodyRefV1, observed, expires time.Time) bool {
	if version != 1 || domain.ValidateSHA256Digest("mcp.evidence.grant", grant) != nil || ref.Validate() != nil ||
		ref.Bytes == 0 || ref.Bytes > GrantBodyBytesV1 || !canonicalTime(observed) || !canonicalTime(expires) || !expires.After(observed) ||
		len(sourceURL) == 0 || len(sourceURL) > 2048 || !utf8.ValidString(sourceURL) {
		return false
	}
	u, err := url.Parse(sourceURL)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Opaque == "" && u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && u.String() == sourceURL
}

func (v MCPPriceEvidenceV1) Validate() error {
	if !validServiceEvidence(v.Version, v.GrantDigest, v.SourceURL, v.SourceBody, v.ObservedAt, v.ExpiresAt) ||
		len(v.Currency) != 3 || v.Currency[0] < 'A' || v.Currency[0] > 'Z' || v.Currency[1] < 'A' || v.Currency[1] > 'Z' || v.Currency[2] < 'A' || v.Currency[2] > 'Z' ||
		domain.ValidateOpaqueID("mcp.evidence.price_revision", v.PriceRevision) != nil {
		return ErrGrant
	}
	switch v.PriceState {
	case domain.ExternalServicePriceKnownFreeV1:
		if v.MaxAmountMicrounits == nil || *v.MaxAmountMicrounits != 0 {
			return ErrGrant
		}
	case domain.ExternalServicePriceKnownPaidV1:
		if v.MaxAmountMicrounits == nil || *v.MaxAmountMicrounits == 0 {
			return ErrGrant
		}
	default:
		return ErrGrant
	}
	return nil
}

func (v MCPTermsEvidenceV1) Validate() error {
	if !validServiceEvidence(v.Version, v.GrantDigest, v.SourceURL, v.SourceBody, v.ObservedAt, v.ExpiresAt) {
		return ErrGrant
	}
	return nil
}

func parseServiceEvidence(body []byte, value any) error {
	if _, err := strictjson.Parse(body, ServiceEvidenceBytesV1, 8, 32); err != nil || json.Unmarshal(body, value) != nil {
		return ErrGrant
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, body) {
		return ErrGrant
	}
	return nil
}

func ParseMCPPriceEvidenceV1(body []byte) (MCPPriceEvidenceV1, error) {
	var v MCPPriceEvidenceV1
	if parseServiceEvidence(body, &v) != nil || v.Validate() != nil {
		return MCPPriceEvidenceV1{}, ErrGrant
	}
	return v, nil
}

func ParseMCPTermsEvidenceV1(body []byte) (MCPTermsEvidenceV1, error) {
	var v MCPTermsEvidenceV1
	if parseServiceEvidence(body, &v) != nil || v.Validate() != nil {
		return MCPTermsEvidenceV1{}, ErrGrant
	}
	return v, nil
}

// ValidateForCost checks report DATA against the exact grant and cost at the
// authoritative time. The Source must additionally authenticate all four
// retained report/document bodies; metadata/digests alone are insufficient.
func (v MCPPriceEvidenceV1) ValidateForCost(g MCPToolGrantV1, cost domain.ExternalServiceCostCeilingV1, at time.Time) error {
	if v.Validate() != nil || g.ValidateForCost(cost, at) != nil || v.GrantDigest != cost.ScopedGrantDigest ||
		v.Currency != cost.Currency || v.PriceRevision != cost.PriceRevision || v.PriceState != cost.PriceState ||
		cost.MaxAmountMicrounits == nil || *v.MaxAmountMicrounits != *cost.MaxAmountMicrounits ||
		v.ObservedAt != cost.PriceObservedAt || v.ExpiresAt != cost.PriceExpiresAt || at.Before(v.ObservedAt) || !at.Before(v.ExpiresAt) {
		return ErrGrant
	}
	return nil
}

func (v MCPTermsEvidenceV1) ValidateForCost(g MCPToolGrantV1, cost domain.ExternalServiceCostCeilingV1, at time.Time) error {
	if v.Validate() != nil || g.ValidateForCost(cost, at) != nil || v.GrantDigest != cost.ScopedGrantDigest || at.Before(v.ObservedAt) || !at.Before(v.ExpiresAt) {
		return ErrGrant
	}
	return nil
}
