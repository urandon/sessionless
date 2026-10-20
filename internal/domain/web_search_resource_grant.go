package domain

import (
	"bytes"
	"encoding/json"
	"io"
	"time"
	"unicode/utf8"
)

const WebSearchResourceGrantVersionV2 uint32 = 2
const WebSearchResourceGrantMaxBytesV2 = 8 << 10

// WebSearchResourceGrantV2 is reusable resource-scoped DATA. Invocation scope
// belongs to the original policy/spending/Source/parent, not this grant. It is
// neither a credential nor current approval, funding, consent or send authority.
type WebSearchResourceGrantV2 struct {
	Version                        uint32    `json:"version"`
	TenantID                       TenantID  `json:"tenant_id"`
	OwnerUserID                    UserID    `json:"owner_user_id"`
	BillingOwnerUserID             UserID    `json:"billing_owner_user_id"`
	ResourceID                     string    `json:"resource_id"`
	ResourceRevision               uint64    `json:"resource_revision"`
	RevocationGeneration           uint64    `json:"revocation_generation"`
	ConsentRevision                uint64    `json:"consent_revision"`
	ProfileID                      string    `json:"profile_id"`
	ProfileRevision                uint64    `json:"profile_revision"`
	Provider                       string    `json:"provider"`
	APIProfile                     string    `json:"api_profile"`
	Endpoint                       string    `json:"endpoint"`
	Method                         string    `json:"method"`
	CredentialRef                  string    `json:"credential_ref"`
	CredentialAudience             string    `json:"credential_audience"`
	DataPolicyDigest               string    `json:"data_policy_digest"`
	APISchemaDigest                string    `json:"api_schema_digest"`
	AdapterArtifactDigest          string    `json:"adapter_artifact_digest"`
	Currency                       string    `json:"currency"`
	PriceRevision                  string    `json:"price_revision"`
	PriceEvidenceDigest            string    `json:"price_evidence_digest"`
	TermsEvidenceDigest            string    `json:"terms_evidence_digest"`
	PerCallReservationMicrounits   uint64    `json:"per_call_reservation_microunits"`
	AggregateReservationMicrounits uint64    `json:"aggregate_reservation_microunits"`
	IssuedAt                       time.Time `json:"issued_at"`
	ExpiresAt                      time.Time `json:"expires_at"`
}

// WebSearchResourceGrantBodyV2 tags original operator-held content metadata;
// its raw SHA is deliberately not the grant's domain-separated semantic digest.
type WebSearchResourceGrantBodyV2 struct {
	Version uint32                       `json:"version"`
	Body    OwnedAgentAuthorityBodyRefV1 `json:"body"`
}

func (v WebSearchResourceGrantV2) Validate() error {
	if v.Version != WebSearchResourceGrantVersionV2 || v.TenantID.Validate() != nil || v.OwnerUserID.Validate() != nil || v.BillingOwnerUserID.Validate() != nil {
		return ErrMembershipDenied
	}
	for _, id := range []string{v.ResourceID, v.CredentialRef, v.PriceRevision} {
		if ValidateOpaqueID("web_search_resource.id", id) != nil {
			return ErrMembershipDenied
		}
	}
	for _, d := range []string{v.DataPolicyDigest, v.APISchemaDigest, v.AdapterArtifactDigest, v.PriceEvidenceDigest, v.TermsEvidenceDigest} {
		if ValidateSHA256Digest("web_search_resource.digest", d) != nil {
			return ErrMembershipDenied
		}
	}
	if v.ResourceRevision == 0 || v.RevocationGeneration == 0 || v.ConsentRevision == 0 || v.ProfileRevision == 0 ||
		v.ProfileID != WebSearchProfileV1 || v.Provider != "tavily" || v.APIProfile != WebSearchAPIProfileV1 || v.Endpoint != WebSearchEndpointV1 || v.Method != "POST" || v.CredentialAudience != WebSearchCredentialAudienceV1 ||
		v.Currency != "USD" || v.PerCallReservationMicrounits != 10000 || v.AggregateReservationMicrounits != 20000 ||
		!ownedCurrentTime(v.IssuedAt) || !ownedCurrentTime(v.ExpiresAt) || v.IssuedAt.Location() != time.UTC || v.ExpiresAt.Location() != time.UTC || !v.ExpiresAt.After(v.IssuedAt) {
		return ErrMembershipDenied
	}
	return nil
}

func (v WebSearchResourceGrantV2) Digest() (string, error) {
	body, err := EncodeWebSearchResourceGrantV2(v)
	if err != nil {
		return "", ErrMembershipDenied
	}
	return searchDigest("sessionless.web-search-resource-grant.v2", body), nil
}

func (v WebSearchResourceGrantV2) ValidateCostAt(cost ExternalServiceCostCeilingV1, at time.Time) error {
	digest, err := v.Digest()
	if err != nil || cost.ValidateAt(at) != nil || at.Before(v.IssuedAt) || !at.Before(v.ExpiresAt) || cost.Kind != ExternalServiceWebSearchV1 ||
		cost.ValidateForOwnerGrantCurrency(v.TenantID, v.OwnerUserID, v.BillingOwnerUserID, digest, v.Currency) != nil ||
		cost.ResourceID != v.ResourceID || cost.ResourceRevision != v.ResourceRevision || cost.RevocationGeneration != v.RevocationGeneration || cost.ProfileID != v.ProfileID || cost.ProfileRevision != v.ProfileRevision ||
		cost.PriceRevision != v.PriceRevision || cost.PriceEvidenceDigest != v.PriceEvidenceDigest || cost.TermsEvidenceDigest != v.TermsEvidenceDigest || cost.PriceState != ExternalServicePriceKnownPaidV1 || cost.MaxAmountMicrounits == nil || *cost.MaxAmountMicrounits != v.AggregateReservationMicrounits {
		return ErrMembershipDenied
	}
	return nil
}

func (v WebSearchResourceGrantBodyV2) Validate() error {
	if v.Version != WebSearchResourceGrantVersionV2 || v.Body.Validate() != nil || v.Body.Bytes > WebSearchResourceGrantMaxBytesV2 {
		return ErrMembershipDenied
	}
	return nil
}

func EncodeWebSearchResourceGrantV2(v WebSearchResourceGrantV2) ([]byte, error) {
	if v.Validate() != nil {
		return nil, ErrMembershipDenied
	}
	body, err := searchJSON(v)
	if err != nil || len(body) == 0 || len(body) > WebSearchResourceGrantMaxBytesV2 {
		return nil, ErrMembershipDenied
	}
	return body, nil
}

func DecodeWebSearchResourceGrantV2(body []byte) (WebSearchResourceGrantV2, error) {
	deny := func() (WebSearchResourceGrantV2, error) { return WebSearchResourceGrantV2{}, ErrMembershipDenied }
	if len(body) == 0 || len(body) > WebSearchResourceGrantMaxBytesV2 || !utf8.Valid(body) {
		return deny()
	}
	var v WebSearchResourceGrantV2
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&v) != nil {
		return deny()
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		return deny()
	}
	canonical, err := EncodeWebSearchResourceGrantV2(v)
	if err != nil || !bytes.Equal(canonical, body) {
		return deny()
	}
	return v, nil
}
