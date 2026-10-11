package ports

import (
	"encoding/json"
	"gitcode.com/urandon/sessionless/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestOwnedFixtureResponseExactSingletonCustody(t *testing.T) {
	valid := []byte(`{"version":1,"kind":"model_response","text":"","calls":[{"call_id":"search-1","kind":"web_search","arguments":{"query":"bounded"}}]}`)
	parsed, err := ParseOwnedFixtureModelResponseV1(valid)
	if err != nil || len(parsed.Calls) != 1 || string(parsed.Calls[0].Arguments) != `{"query":"bounded"}` {
		t.Fatal("exact singleton fixture denied", err)
	}
	valid[0] = 'x'
	if string(parsed.Calls[0].Arguments) != `{"query":"bounded"}` {
		t.Fatal("returned arguments alias input")
	}
	for _, body := range []string{
		`{"version":1,"kind":"model_response","text":"","calls":null}`,
		`{"version":1,"kind":"model_response","text":"","calls":[{"call_id":"a","kind":"mcp_tool","arguments":{}},{"call_id":"b","kind":"web_search","arguments":{}}]}`,
		`{"version":1,"version":1,"kind":"model_response","text":"","calls":[]}`,
		`{"version":1,"kind":"model_response","text":"","extra":true,"calls":[]}`,
		`{"version":1,"kind":"model_response","text":"","calls":[{"call_id":"a","kind":"web_search","arguments":{"query":"first","query":"second"}}]}`,
		`{"version":1,"kind":"model_response","text":"","calls":[{"call_id":"a","kind":"mcp_tool","arguments":{"nested":{"x":1,"x":2}}}]}`,
		`{"version":1,"kind":"model_response","text":"","calls":[]} {}`,
	} {
		if _, err := ParseOwnedFixtureModelResponseV1([]byte(body)); err == nil {
			t.Fatal("malformed/non-singleton fixture accepted")
		}
	}
}

func TestOwnedFixtureGrantExactCurrentCostJoin(t *testing.T) {
	at := time.Date(2026, 10, 11, 14, 0, 0, 0, time.UTC)
	grant := OwnedFixtureToolGrantV1{Version: 1, TenantID: "tenant", OwnerUserID: "owner", BillingOwnerUserID: "payer", Kind: domain.ExternalServiceWebSearchV1, ResourceID: "search", ResourceRevision: 2, RevocationGeneration: 3, ProfileID: "fixture", ProfileRevision: 4, ConsentMode: "initiating_user_required", ConsentRevision: 5, Endpoint: "fixture://owned/web-search", MaxArgumentBytes: 4096, MaxResultBytes: 24576, ExpiresAt: at.Add(time.Minute)}
	body, _ := json.Marshal(grant)
	parsed, err := ParseOwnedFixtureToolGrantV1(body)
	if err != nil || parsed != grant {
		t.Fatal("canonical grant rejected", err)
	}
	digest := OwnedFixtureBytesDigestV1(body)
	zero := uint64(0)
	cost := domain.ExternalServiceCostCeilingV1{Version: 1, Kind: grant.Kind, TenantID: grant.TenantID, OwnerUserID: grant.OwnerUserID, BillingOwnerUserID: grant.BillingOwnerUserID, ResourceID: grant.ResourceID, ResourceRevision: grant.ResourceRevision, RevocationGeneration: grant.RevocationGeneration, ProfileID: grant.ProfileID, ProfileRevision: grant.ProfileRevision, ScopedGrantDigest: digest, Currency: "USD", PriceRevision: "price", PriceObservedAt: at.Add(-time.Second), PriceExpiresAt: at.Add(time.Minute), PriceEvidenceDigest: strings.Repeat("a", 64), TermsEvidenceDigest: strings.Repeat("b", 64), PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: &zero}
	if grant.ValidateForCost(cost, digest, at) != nil {
		t.Fatal("explicit independently evidenced known-free zero denied")
	}
	for _, mutate := range []func(*domain.ExternalServiceCostCeilingV1){func(c *domain.ExternalServiceCostCeilingV1) { c.ResourceRevision++ }, func(c *domain.ExternalServiceCostCeilingV1) { c.RevocationGeneration++ }, func(c *domain.ExternalServiceCostCeilingV1) { c.ProfileRevision++ }, func(c *domain.ExternalServiceCostCeilingV1) { c.BillingOwnerUserID = "other" }, func(c *domain.ExternalServiceCostCeilingV1) {
		c.MaxAmountMicrounits = nil
		c.PriceState = domain.ExternalServicePriceUnknownV1
	}} {
		changed := cost.Clone()
		mutate(&changed)
		if grant.ValidateForCost(changed, digest, at) == nil {
			t.Fatal("changed current cost accepted")
		}
	}
	if grant.ValidateForCost(cost, digest, grant.ExpiresAt) == nil {
		t.Fatal("expired grant accepted")
	}
}
