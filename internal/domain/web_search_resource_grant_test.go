package domain_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func resourceGrantFixture() domain.WebSearchResourceGrantV2 {
	v := searchGrantFixture()
	return domain.WebSearchResourceGrantV2{Version: 2, TenantID: v.TenantID, OwnerUserID: v.OwnerUserID, BillingOwnerUserID: v.BillingOwnerUserID,
		ResourceID: v.ResourceID, ResourceRevision: v.ResourceRevision, RevocationGeneration: v.RevocationGeneration, ConsentRevision: v.ConsentRevision,
		ProfileID: v.ProfileID, ProfileRevision: v.ProfileRevision, Provider: v.Provider, APIProfile: v.APIProfile, Endpoint: v.Endpoint, Method: v.Method,
		CredentialRef: v.CredentialRef, CredentialAudience: v.CredentialAudience, DataPolicyDigest: v.DataPolicyDigest, APISchemaDigest: v.APISchemaDigest, AdapterArtifactDigest: v.AdapterArtifactDigest,
		Currency: v.Currency, PriceRevision: v.PriceRevision, PriceEvidenceDigest: v.PriceEvidenceDigest, TermsEvidenceDigest: v.TermsEvidenceDigest,
		PerCallReservationMicrounits: v.PerCallReservationMicrounits, AggregateReservationMicrounits: v.AggregateReservationMicrounits, IssuedAt: v.IssuedAt, ExpiresAt: v.ExpiresAt}
}

func resourceGrantRaw(t testing.TB, g domain.WebSearchResourceGrantV2) []byte {
	t.Helper()
	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func resourceGrantIndependentDigest(raw []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless.web-search-resource-grant.v2\x00"))
	_, _ = h.Write(raw)
	return hex.EncodeToString(h.Sum(nil))
}

func TestWebSearchResourceGrantV2CanonicalRoundTrip(t *testing.T) {
	g := resourceGrantFixture()
	if err := g.Validate(); err != nil {
		t.Fatalf("valid resource-scoped DATA rejected: %v", err)
	}
	raw, err := domain.EncodeWebSearchResourceGrantV2(g)
	if err != nil || !bytes.Equal(raw, resourceGrantRaw(t, g)) {
		t.Fatalf("closed canonical encoding bytes=%d err=%v", len(raw), err)
	}
	got, err := domain.DecodeWebSearchResourceGrantV2(raw)
	if err != nil || !reflect.DeepEqual(got, g) {
		t.Fatalf("closed decode got=%+v err=%v", got, err)
	}
	d, err := g.Digest()
	sum := sha256.Sum256(raw)
	if err != nil || d != resourceGrantIndependentDigest(raw) || d == hex.EncodeToString(sum[:]) {
		t.Fatalf("semantic/raw identity confused: semantic=%s raw=%x err=%v", d, sum, err)
	}
	for _, forbidden := range []string{`"session_id"`, `"run_id"`, `"attempt_id"`, `"binding_digest"`, `"policy_digest"`} {
		if bytes.Contains(raw, []byte(forbidden)) {
			t.Errorf("reusable grant contains invocation authority field %s", forbidden)
		}
	}
}

func TestWebSearchResourceGrantV2MaximumCanonicalBody(t *testing.T) {
	g := resourceGrantFixture()
	id := strings.Repeat("x", 160)
	g.TenantID, g.OwnerUserID, g.BillingOwnerUserID = domain.TenantID(id), domain.UserID(id), domain.UserID(id)
	g.ResourceID, g.CredentialRef, g.PriceRevision = id, id, id
	g.ResourceRevision, g.RevocationGeneration, g.ConsentRevision, g.ProfileRevision = math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64
	g.IssuedAt = time.Date(1, 1, 1, 0, 0, 0, 1000, time.UTC)
	g.ExpiresAt = time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)
	raw, err := domain.EncodeWebSearchResourceGrantV2(g)
	if err != nil || len(raw) == 0 || len(raw) > domain.WebSearchResourceGrantMaxBytesV2 {
		t.Fatalf("maximum admitted identifiers/integers/times do not fit canonical cap: bytes=%d cap=%d err=%v", len(raw), domain.WebSearchResourceGrantMaxBytesV2, err)
	}
	if _, err := domain.DecodeWebSearchResourceGrantV2(raw); err != nil {
		t.Fatalf("maximum valid encoding rejected: %v", err)
	}
	t.Logf("maximum canonical resource-grant bytes=%d hard-cap=%d", len(raw), domain.WebSearchResourceGrantMaxBytesV2)
}

func TestWebSearchResourceGrantV2ExactCostCorrelation(t *testing.T) {
	g := resourceGrantFixture()
	digest := resourceGrantIndependentDigest(resourceGrantRaw(t, g))
	amount := uint64(20000)
	cost := domain.ExternalServiceCostCeilingV1{Version: 1, Kind: domain.ExternalServiceWebSearchV1, TenantID: g.TenantID, OwnerUserID: g.OwnerUserID, BillingOwnerUserID: g.BillingOwnerUserID,
		ResourceID: g.ResourceID, ResourceRevision: g.ResourceRevision, RevocationGeneration: g.RevocationGeneration, ProfileID: g.ProfileID, ProfileRevision: g.ProfileRevision, ScopedGrantDigest: digest,
		Currency: "USD", PriceRevision: g.PriceRevision, PriceObservedAt: g.IssuedAt, PriceExpiresAt: g.ExpiresAt, PriceEvidenceDigest: g.PriceEvidenceDigest, TermsEvidenceDigest: g.TermsEvidenceDigest,
		PriceState: domain.ExternalServicePriceKnownPaidV1, MaxAmountMicrounits: &amount}
	if err := g.ValidateCostAt(cost, g.IssuedAt); err != nil {
		t.Fatal("exact resource cost rejected before negative controls", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*domain.ExternalServiceCostCeilingV1)
	}{
		{"tenant", func(c *domain.ExternalServiceCostCeilingV1) { c.TenantID = "foreign-tenant" }},
		{"owner", func(c *domain.ExternalServiceCostCeilingV1) { c.OwnerUserID = "foreign-owner" }},
		{"payer", func(c *domain.ExternalServiceCostCeilingV1) { c.BillingOwnerUserID = "foreign-payer" }},
		{"resource", func(c *domain.ExternalServiceCostCeilingV1) { c.ResourceID = "other-resource" }},
		{"generation", func(c *domain.ExternalServiceCostCeilingV1) { c.RevocationGeneration++ }},
		{"profile", func(c *domain.ExternalServiceCostCeilingV1) { c.ProfileRevision++ }},
		{"semantic", func(c *domain.ExternalServiceCostCeilingV1) { c.ScopedGrantDigest = strings.Repeat("f", 64) }},
		{"price", func(c *domain.ExternalServiceCostCeilingV1) { c.PriceRevision = "other-price" }},
		{"terms", func(c *domain.ExternalServiceCostCeilingV1) { c.TermsEvidenceDigest = strings.Repeat("f", 64) }},
		{"free", func(c *domain.ExternalServiceCostCeilingV1) { c.PriceState = domain.ExternalServicePriceKnownFreeV1 }},
		{"reservation", func(c *domain.ExternalServiceCostCeilingV1) { amount := uint64(20001); c.MaxAmountMicrounits = &amount }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := cost.Clone()
			tc.change(&bad)
			if g.ValidateCostAt(bad, g.IssuedAt) == nil {
				t.Fatal("mismatched resource cost accepted")
			}
		})
	}
	if g.ValidateCostAt(cost, g.IssuedAt.Add(-time.Microsecond)) == nil || g.ValidateCostAt(cost, g.ExpiresAt) == nil {
		t.Fatal("original grant/cost window extended")
	}
}

func TestWebSearchResourceGrantV2RejectNoncanonical(t *testing.T) {
	raw := resourceGrantRaw(t, resourceGrantFixture())
	for _, tc := range []struct{ name, raw string }{
		{"unknown", strings.TrimSuffix(string(raw), "}") + `,"run_id":"not-a-resource-grant"}`},
		{"duplicate", strings.Replace(string(raw), `"version":2`, `"version":2,"version":2`, 1)},
		{"alias", strings.Replace(string(raw), `"version":2`, `"Version":2`, 1)},
		{"missing", strings.Replace(string(raw), `"version":2,`, "", 1)},
		{"null", strings.Replace(string(raw), `"version":2`, `"version":null`, 1)},
		{"unsupported", strings.Replace(string(raw), `"version":2`, `"version":3`, 1)},
		{"fraction", strings.Replace(string(raw), `"version":2`, `"version":2.0`, 1)},
		{"nonmicrosecond", strings.Replace(string(raw), "12:00:00Z", "12:00:00.000000001Z", 1)},
		{"offset", strings.Replace(string(raw), "12:00:00Z", "12:00:00+00:00", 1)},
		{"trailing", string(raw) + `{}`},
		{"whitespace", " " + string(raw)},
		{"oversized", strings.Repeat("x", domain.WebSearchResourceGrantMaxBytesV2+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := domain.DecodeWebSearchResourceGrantV2([]byte(tc.raw)); err == nil || got != (domain.WebSearchResourceGrantV2{}) {
				t.Fatalf("noncanonical resource grant returned usable DATA=%+v err=%v", got, err)
			}
		})
	}
	for _, mutate := range []func(*domain.WebSearchResourceGrantV2){
		func(v *domain.WebSearchResourceGrantV2) { v.TenantID = "" },
		func(v *domain.WebSearchResourceGrantV2) { v.Endpoint = "https://other.example/search" },
		func(v *domain.WebSearchResourceGrantV2) { v.CredentialAudience = "openai" },
		func(v *domain.WebSearchResourceGrantV2) { v.PerCallReservationMicrounits = 0 },
		func(v *domain.WebSearchResourceGrantV2) { v.DataPolicyDigest = strings.Repeat("A", 64) },
		func(v *domain.WebSearchResourceGrantV2) { v.ExpiresAt = v.IssuedAt },
	} {
		g := resourceGrantFixture()
		mutate(&g)
		if g.Validate() == nil {
			t.Fatalf("invalid resource grant accepted: %+v", g)
		}
	}
}

func TestWebSearchResourceGrantV2ProfileArmCloneAndCodec(t *testing.T) {
	p, _ := ownedCurrentProfileFixture(t)
	body := resourceGrantRaw(t, resourceGrantFixture())
	sum := sha256.Sum256(body)
	p.WebSearchResourceGrant = &domain.WebSearchResourceGrantBodyV2{Version: 2, Body: domain.OwnedAgentAuthorityBodyRefV1{Ref: "original-search-grant", Digest: hex.EncodeToString(sum[:]), Bytes: uint64(len(body))}}
	clone := p.Clone()
	clone.WebSearchResourceGrant.Body.Ref = "caller-mutation"
	if p.WebSearchResourceGrant.Body.Ref != "original-search-grant" {
		t.Fatal("profile Clone aliases original resource-body arm")
	}
	raw, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
	if err != nil {
		t.Fatalf("valid tagged reference rejected: %v", err)
	}
	got, err := domain.DecodeOwnedAgentProfileCurrentV1(raw)
	if err != nil || !reflect.DeepEqual(got, p) {
		t.Fatalf("closed profile arm decode err=%v", err)
	}
	for _, mutate := range []func(*domain.WebSearchResourceGrantBodyV2){
		func(v *domain.WebSearchResourceGrantBodyV2) { v.Version = 1 },
		func(v *domain.WebSearchResourceGrantBodyV2) { v.Body.Bytes = 8193 },
		func(v *domain.WebSearchResourceGrantBodyV2) { v.Body.Bytes = 0 },
		func(v *domain.WebSearchResourceGrantBodyV2) { v.Body.Digest = "not-a-digest" },
	} {
		bad := p.Clone()
		mutate(bad.WebSearchResourceGrant)
		if bad.Validate() == nil {
			t.Fatal("invalid profile body reference accepted")
		}
	}
}

func TestWebSearchResourceGrantNilProfileRejectExplicitNull(t *testing.T) {
	p, _ := ownedCurrentProfileFixture(t)
	raw, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{`,"web_search_resource_grant":null}`, `,"web_search_resource_grant":{"version":3,"body":{}}}`} {
		if _, err := domain.DecodeOwnedAgentProfileCurrentV1(append(bytes.Clone(raw[:len(raw)-1]), suffix...)); err == nil {
			t.Fatalf("legacy nil arm accepted explicit/unknown value %s", suffix)
		}
	}
}

func TestWebSearchResourceGrantLegacyV1Unchanged(t *testing.T) {
	v := searchGrantFixture()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	digest, err := v.Digest()
	if err != nil || len(raw) != 1126 || hex.EncodeToString(sum[:]) != "7984bd0062cfa75c1eb02e9fedba0c23f9d467387b12e887cdc3424bd4d76665" || digest != "318d2c2123f3e0544f781a9c535dfbc0ff79cc86d6f36d5310e26633e8bb0328" {
		t.Fatalf("unchanged V1 source vector drift: bytes=%d raw=%x semantic=%s err=%v", len(raw), sum, digest, err)
	}
	if _, err := domain.DecodeWebSearchResourceGrantV2(raw); err == nil {
		t.Fatal("invocation V1 was implicitly promoted to reusable V2")
	}
}
