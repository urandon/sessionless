package mcpadmission

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Synthetic operator reports only, not actual service pricing or legal review.
func serviceEvidenceFixture(t *testing.T) (MCPToolGrantV1, domain.ExternalServiceCostCeilingV1, MCPPriceEvidenceV1, MCPTermsEvidenceV1) {
	t.Helper()
	g, c, _ := grantFixture(t)
	price := MCPPriceEvidenceV1{Version: 1, GrantDigest: c.ScopedGrantDigest, Currency: c.Currency, PriceRevision: c.PriceRevision,
		PriceState: c.PriceState, MaxAmountMicrounits: new(uint64), SourceURL: "https://example.org/synthetic-price",
		SourceBody: domain.OwnedAgentAuthorityBodyRefV1{Ref: "synthetic-price-document", Digest: strings.Repeat("d", 64), Bytes: 31},
		ObservedAt: c.PriceObservedAt, ExpiresAt: c.PriceExpiresAt}
	terms := MCPTermsEvidenceV1{Version: 1, GrantDigest: c.ScopedGrantDigest, SourceURL: "https://example.org/synthetic-terms",
		SourceBody: domain.OwnedAgentAuthorityBodyRefV1{Ref: "synthetic-terms-document", Digest: strings.Repeat("e", 64), Bytes: 39},
		ObservedAt: g.IssuedAt.Add(-time.Minute), ExpiresAt: g.ExpiresAt.Add(-time.Minute)}
	return g, c, price, terms
}

func TestMCPServiceEvidenceCanonicalAndIndependentTermsTime(t *testing.T) {
	g, c, price, terms := serviceEvidenceFixture(t)
	pb, _ := json.Marshal(price)
	tb, _ := json.Marshal(terms)
	p, err := ParseMCPPriceEvidenceV1(pb)
	if err != nil || p.ValidateForCost(g, c, g.IssuedAt) != nil {
		t.Fatalf("price exact join: %v", err)
	}
	v, err := ParseMCPTermsEvidenceV1(tb)
	if err != nil || v.ValidateForCost(g, c, g.IssuedAt) != nil {
		t.Fatalf("independent terms observation: %v", err)
	}
	if v.ValidateForCost(g, c, v.ExpiresAt) == nil || v.ValidateForCost(g, c, v.ObservedAt.Add(-time.Microsecond)) == nil {
		t.Fatal("terms boundary authorized outside [observed, expiry)")
	}
	clear(pb)
	if p.Validate() != nil {
		t.Fatal("parsed report aliases caller bytes")
	}
	*p.MaxAmountMicrounits = 9
	if *price.MaxAmountMicrounits != 0 {
		t.Fatal("decoded report amount aliases original DATA")
	}
	for _, report := range []any{price, terms} {
		body, _ := json.Marshal(report)
		for _, key := range []string{"attempt_id", "run_id", "profile_digest", "cost_digest", "approved"} {
			if bytes.Contains(body, []byte(`"`+key+`"`)) {
				t.Fatalf("report introduced enclosing/approval field %s", key)
			}
		}
		if strings.Contains(fmt.Sprintf("%v %#v", report, report), "example.org") {
			t.Fatal("report diagnostic leaked provenance DATA")
		}
	}
	paid := uint64(57)
	price.PriceState, price.MaxAmountMicrounits = domain.ExternalServicePriceKnownPaidV1, &paid
	c.PriceState, c.MaxAmountMicrounits = price.PriceState, &paid
	if price.ValidateForCost(g, c, g.IssuedAt) != nil {
		t.Fatal("explicit bounded paid DATA rejected (no service call)")
	}
}

func TestMCPServiceEvidenceRejectsPriceDrift(t *testing.T) {
	for _, name := range []string{"version", "grant", "currency", "revision", "unknown", "nil-free", "nonzero-free", "zero-paid", "amount", "observation", "expiry", "future", "expired", "url-http", "url-user", "url-query", "url-fragment", "url-size", "document-ref", "document-empty", "document-size", "precision", "window"} {
		t.Run(name, func(t *testing.T) {
			g, c, p, _ := serviceEvidenceFixture(t)
			at := g.IssuedAt
			switch name {
			case "version":
				p.Version++
			case "grant":
				p.GrantDigest = strings.Repeat("f", 64)
			case "currency":
				p.Currency = "USD"
			case "revision":
				p.PriceRevision = "foreign-price"
			case "unknown":
				p.PriceState, p.MaxAmountMicrounits = domain.ExternalServicePriceUnknownV1, nil
			case "nil-free":
				p.MaxAmountMicrounits = nil
			case "nonzero-free":
				*p.MaxAmountMicrounits = 1
			case "zero-paid":
				p.PriceState = domain.ExternalServicePriceKnownPaidV1
			case "amount":
				n := uint64(1)
				p.PriceState, p.MaxAmountMicrounits = domain.ExternalServicePriceKnownPaidV1, &n
				c.PriceState = p.PriceState
				m := uint64(2)
				c.MaxAmountMicrounits = &m
			case "observation":
				p.ObservedAt = p.ObservedAt.Add(-time.Microsecond)
			case "expiry":
				p.ExpiresAt = p.ExpiresAt.Add(time.Microsecond)
			case "future":
				at = p.ObservedAt.Add(-time.Microsecond)
			case "expired":
				at = p.ExpiresAt
			case "url-http":
				p.SourceURL = "http://example.org/price"
			case "url-user":
				p.SourceURL = "https://credential@example.org/price"
			case "url-query":
				p.SourceURL += "?token=secret"
			case "url-fragment":
				p.SourceURL += "#fragment"
			case "url-size":
				p.SourceURL = "https://example.org/" + strings.Repeat("x", 2048)
			case "document-ref":
				p.SourceBody.Digest = "not-a-digest"
			case "document-empty":
				p.SourceBody.Bytes = 0
			case "document-size":
				p.SourceBody.Bytes = GrantBodyBytesV1 + 1
			case "precision":
				p.ObservedAt = p.ObservedAt.Add(time.Nanosecond)
			case "window":
				p.ExpiresAt = p.ObservedAt
			}
			if p.ValidateForCost(g, c, at) == nil {
				t.Fatalf("%s report mismatch authorized", name)
			}
		})
	}
}

func TestMCPServiceEvidenceRejectsTermsDriftAndEncoding(t *testing.T) {
	for _, name := range []string{"grant", "version", "document", "future", "expired"} {
		t.Run(name, func(t *testing.T) {
			g, c, _, v := serviceEvidenceFixture(t)
			at := g.IssuedAt
			switch name {
			case "grant":
				v.GrantDigest = strings.Repeat("f", 64)
			case "version":
				v.Version++
			case "document":
				v.SourceBody.Ref = ""
			case "future":
				v.ObservedAt = at.Add(time.Microsecond)
			case "expired":
				v.ExpiresAt = at
			}
			if v.ValidateForCost(g, c, at) == nil {
				t.Fatalf("%s terms mismatch authorized", name)
			}
		})
	}
	_, _, p, v := serviceEvidenceFixture(t)
	pb, _ := json.Marshal(p)
	tb, _ := json.Marshal(v)
	for _, tc := range []struct {
		name  string
		body  []byte
		parse func([]byte) error
	}{
		{"price", pb, func(b []byte) error { _, err := ParseMCPPriceEvidenceV1(b); return err }},
		{"terms", tb, func(b []byte) error { _, err := ParseMCPTermsEvidenceV1(b); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, b := range [][]byte{nil, bytes.Repeat([]byte("x"), int(ServiceEvidenceBytesV1+1)), append([]byte(" "), tc.body...), append(bytes.Clone(tc.body), 'x'), bytes.Replace(tc.body, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1), append(bytes.Clone(tc.body[:len(tc.body)-1]), []byte(`,"approved":true}`)...)} {
				if tc.parse(b) == nil {
					t.Fatal("malformed/noncanonical/unsupported report accepted")
				}
			}
		})
	}
	if _, err := ParseMCPPriceEvidenceV1(tb); err == nil {
		t.Fatal("terms bytes used as price declaration")
	}
	if _, err := ParseMCPTermsEvidenceV1(pb); err == nil {
		t.Fatal("price bytes used as terms acceptance")
	}
}
