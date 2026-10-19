package localoperator

import (
	"bytes"
	"context"
	"net/url"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// OwnerLocalSearchExpenditureV2 is original private-manifest expenditure
// acceptance. It is not vendor balance, payment settlement or current HTTP/legal
// truth, and it grants no model/provider call or cloud/platform spending.
type OwnerLocalSearchExpenditureV2 struct {
	Version                        uint32                              `json:"version"`
	TenantID                       domain.TenantID                     `json:"tenant_id"`
	OwnerUserID                    domain.UserID                       `json:"owner_user_id"`
	BillingOwnerUserID             domain.UserID                       `json:"billing_owner_user_id"`
	ResourceID                     string                              `json:"resource_id"`
	ResourceRevision               uint64                              `json:"resource_revision"`
	RevocationGeneration           uint64                              `json:"revocation_generation"`
	GrantDigest                    string                              `json:"grant_digest"`
	GrantBody                      domain.OwnedAgentAuthorityBodyRefV1 `json:"grant_body"`
	PriceEvidence                  domain.OwnedAgentAuthorityBodyRefV1 `json:"price_evidence"`
	TermsEvidence                  domain.OwnedAgentAuthorityBodyRefV1 `json:"terms_evidence"`
	Currency                       string                              `json:"currency"`
	PerCallReservationMicrounits   uint64                              `json:"per_call_reservation_microunits"`
	AggregateReservationMicrounits uint64                              `json:"aggregate_reservation_microunits"`
	IssuedAt                       time.Time                           `json:"issued_at"`
	ExpiresAt                      time.Time                           `json:"expires_at"`
}

// Snapshots authenticate only exact operator-accepted source bytes/metadata.
// Origin and observation are declarations, not network or vendor proof.
type OwnerAcceptedSearchPriceV2 struct {
	Version          uint32                              `json:"version"`
	Provider         string                              `json:"provider"`
	APIProfile       string                              `json:"api_profile"`
	Origin           string                              `json:"origin"`
	ObservedAt       time.Time                           `json:"observed_at"`
	ExpiresAt        time.Time                           `json:"expires_at"`
	Payload          domain.OwnedAgentAuthorityBodyRefV1 `json:"payload"`
	PriceRevision    string                              `json:"price_revision"`
	Currency         string                              `json:"currency"`
	Unit             string                              `json:"unit"`
	TariffMicrounits uint64                              `json:"tariff_microunits"`
}

type OwnerAcceptedSearchTermsV2 struct {
	Version            uint32                              `json:"version"`
	Provider           string                              `json:"provider"`
	APIProfile         string                              `json:"api_profile"`
	Origin             string                              `json:"origin"`
	ObservedAt         time.Time                           `json:"observed_at"`
	ExpiresAt          time.Time                           `json:"expires_at"`
	Payload            domain.OwnedAgentAuthorityBodyRefV1 `json:"payload"`
	DataPolicyDigest   string                              `json:"data_policy_digest"`
	CredentialAudience string                              `json:"credential_audience"`
}

// One immutable projection owned by the original Source, not another approval
// holder. Raw bodies stay in that Source's original bounded evidence list.
type searchAcceptance struct {
	grant       domain.WebSearchResourceGrantV2
	expenditure OwnerLocalSearchExpenditureV2
	price       OwnerAcceptedSearchPriceV2
	terms       OwnerAcceptedSearchTermsV2
	end         time.Time
}

func canonicalSearchTime(at time.Time) bool {
	return !at.IsZero() && at.Year() >= 1 && at.Year() <= 9999 && at.Nanosecond()%1000 == 0 && at.Location() == time.UTC
}

func searchSnapshotOrigin(origin string) bool {
	if len(origin) == 0 || len(origin) > 2048 {
		return false
	}
	u, err := url.Parse(origin)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Fragment == ""
}

// Borrowed original evidence only. The caller owns the retained list (during
// intake or under s.mu); this neither reads a file nor copies an authority DTO.
func exactSearchEvidence(bodies []EvidenceBodyV1, ref domain.OwnedAgentAuthorityBodyRefV1, cap uint64) ([]byte, error) {
	if ref.Validate() != nil || ref.Bytes > cap || len(bodies) == 0 || len(bodies) > 16 {
		return nil, ErrDenied
	}
	var result []byte
	for _, b := range bodies {
		if b.Reference != ref {
			continue
		}
		if result != nil || uint64(len(b.Body)) != ref.Bytes || hash(b.Body) != ref.Digest {
			return nil, ErrDenied
		}
		result = b.Body
	}
	if result == nil {
		return nil, ErrDenied
	}
	return result, nil
}

func acceptSearchEvidence(m ManifestV1, p domain.OwnedAgentProfileCurrentV1, at time.Time) (*searchAcceptance, error) {
	if p.WebSearchResourceGrant == nil && m.SearchExpenditure == nil {
		return nil, nil // exact legacy path, never fresh search authority
	}
	if p.WebSearchResourceGrant == nil || p.WebSearchResourceGrant.Validate() != nil || m.SearchExpenditure == nil || !canonicalSearchTime(at) || p.CostCeiling.WebSearchCost == nil {
		return nil, ErrDenied
	}
	body, err := exactSearchEvidence(m.Evidence, p.WebSearchResourceGrant.Body, domain.WebSearchResourceGrantMaxBytesV2)
	if err != nil {
		return nil, ErrDenied
	}
	g, err := domain.DecodeWebSearchResourceGrantV2(body)
	digest, de := g.Digest()
	e := *m.SearchExpenditure
	cost := p.CostCeiling.WebSearchCost
	if err != nil || de != nil || digest != p.WebSearchGrantDigest || g.ValidateCostAt(*cost, at) != nil ||
		g.TenantID != p.Locator.TenantID || g.OwnerUserID != p.Locator.OwnerUserID || g.BillingOwnerUserID != p.BillingOwnerUserID || g.BillingOwnerUserID != g.OwnerUserID ||
		e.Version != 2 || e.TenantID != g.TenantID || e.OwnerUserID != g.OwnerUserID || e.BillingOwnerUserID != g.BillingOwnerUserID ||
		e.ResourceID != g.ResourceID || e.ResourceRevision != g.ResourceRevision || e.RevocationGeneration != g.RevocationGeneration || e.GrantDigest != digest ||
		e.GrantBody != p.WebSearchResourceGrant.Body || e.PriceEvidence.Digest != g.PriceEvidenceDigest || e.TermsEvidence.Digest != g.TermsEvidenceDigest ||
		e.Currency != "USD" || e.PerCallReservationMicrounits != 10000 || e.AggregateReservationMicrounits != 20000 ||
		!canonicalSearchTime(e.IssuedAt) || !canonicalSearchTime(e.ExpiresAt) || !e.ExpiresAt.After(e.IssuedAt) || e.IssuedAt.Before(g.IssuedAt) || at.Before(e.IssuedAt) || !at.Before(e.ExpiresAt) {
		return nil, ErrDenied
	}
	priceBody, err := exactSearchEvidence(m.Evidence, e.PriceEvidence, 8<<10)
	if err != nil {
		return nil, ErrDenied
	}
	termsBody, err := exactSearchEvidence(m.Evidence, e.TermsEvidence, 8<<10)
	if err != nil {
		return nil, ErrDenied
	}
	var price OwnerAcceptedSearchPriceV2
	var terms OwnerAcceptedSearchTermsV2
	if strictJSON(priceBody, &price) != nil || strictJSON(termsBody, &terms) != nil ||
		price.Version != 2 || terms.Version != 2 || price.Provider != g.Provider || terms.Provider != g.Provider || price.APIProfile != g.APIProfile || terms.APIProfile != g.APIProfile ||
		!searchSnapshotOrigin(price.Origin) || !searchSnapshotOrigin(terms.Origin) ||
		!canonicalSearchTime(price.ObservedAt) || !canonicalSearchTime(price.ExpiresAt) || !price.ExpiresAt.After(price.ObservedAt) ||
		!canonicalSearchTime(terms.ObservedAt) || !canonicalSearchTime(terms.ExpiresAt) || !terms.ExpiresAt.After(terms.ObservedAt) ||
		at.Before(price.ObservedAt) || !at.Before(price.ExpiresAt) || at.Before(terms.ObservedAt) || !at.Before(terms.ExpiresAt) ||
		e.IssuedAt.Before(price.ObservedAt) || e.IssuedAt.Before(terms.ObservedAt) ||
		price.PriceRevision != g.PriceRevision || price.Currency != g.Currency || price.Unit != "request" || price.TariffMicrounits == 0 || price.TariffMicrounits > g.PerCallReservationMicrounits ||
		!cost.PriceObservedAt.Equal(price.ObservedAt) || !cost.PriceExpiresAt.Equal(price.ExpiresAt) ||
		terms.DataPolicyDigest != g.DataPolicyDigest || terms.CredentialAudience != g.CredentialAudience {
		return nil, ErrDenied
	}
	if _, err := exactSearchEvidence(m.Evidence, price.Payload, uint64(maxOperatorManifestBytes)); err != nil {
		return nil, ErrDenied
	}
	if _, err := exactSearchEvidence(m.Evidence, terms.Payload, uint64(maxOperatorManifestBytes)); err != nil {
		return nil, ErrDenied
	}
	end := p.Window.ExpiresAt
	for _, bound := range []time.Time{p.CostCeiling.PriceExpiresAt, cost.PriceExpiresAt, g.ExpiresAt, e.ExpiresAt, price.ExpiresAt, terms.ExpiresAt} {
		if bound.Before(end) {
			end = bound
		}
	}
	if !at.Before(end) {
		return nil, ErrDenied
	}
	return &searchAcceptance{grant: g, expenditure: e, price: price, terms: terms, end: end}, nil
}

// Caller holds s.mu. Rejoin original retained acceptance, receipt/profile and
// lifetime; no SQL, network, mutable external evidence or caller grant is used.
func (s *Source) searchWindowLocked(p domain.OwnedAgentProfileCurrentV1, at time.Time) (time.Time, error) {
	if !s.aliveLocked() || s.receipt == nil || s.search == nil || !s.receiptMatchesLocked(*s.receipt) ||
		p.WebSearchResourceGrant == nil || s.profile.WebSearchResourceGrant == nil || *p.WebSearchResourceGrant != *s.profile.WebSearchResourceGrant {
		return time.Time{}, ErrDenied
	}
	// Revalidate actual retained bytes/acceptance against the exact installed
	// postimage. The manifest commitment and original key bind their provenance.
	m := ManifestV1{Evidence: s.bodies, SearchExpenditure: &s.search.expenditure}
	a, err := acceptSearchEvidence(m, p, at)
	if err != nil || a == nil || p.ValidateAt(at) != nil || s.config.ValidateAt(at) != nil {
		return time.Time{}, ErrDenied
	}
	end := a.end
	for _, bound := range []time.Time{s.config.ExpiresAt, at.Add(p.Limits.MaxActiveDuration + p.Limits.MaxCleanupDuration)} {
		if bound.Before(end) {
			end = bound
		}
	}
	if deadline, ok := s.ctx.Deadline(); ok && deadline.Before(end) {
		end = deadline
	}
	if !at.Before(end) {
		return time.Time{}, ErrDenied
	}
	return end, nil
}

// ReadWebSearchResourceGrant returns original nonsecret content DATA, never an
// expiry extension or spending permission. Current use must still rejoin the
// original profile/resource/spending in the Store. Source Close revokes this
// reader, but cannot zeroize a buffer already transferred to a lawful caller.
func (s *Source) ReadWebSearchResourceGrant(ctx context.Context, ref domain.OwnedAgentProfileReferenceV1, cap uint64) ([]byte, error) {
	if s == nil || ctx == nil || ctx.Err() != nil || cap == 0 || cap > domain.WebSearchResourceGrantMaxBytesV2 || ref.Validate() != nil {
		return nil, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aliveLocked() || s.receipt == nil || s.search == nil || !s.receiptMatchesLocked(*s.receipt) ||
		ref.Locator != s.receipt.Profile.Locator || ref.RevocationGeneration != s.receipt.Profile.RevocationGeneration || ref.Digest != s.receipt.Digest || s.receipt.Profile.WebSearchResourceGrant == nil {
		return nil, ErrDenied
	}
	body, err := exactSearchEvidence(s.bodies, s.receipt.Profile.WebSearchResourceGrant.Body, cap)
	if err != nil {
		return nil, ErrDenied
	}
	result := bytes.Clone(body)
	if ctx.Err() != nil || !s.aliveLocked() {
		clear(result)
		return nil, ErrDenied
	}
	return result, nil
}
