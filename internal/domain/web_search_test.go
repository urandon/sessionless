package domain_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func searchGrantFixture() domain.WebSearchGrantV1 {
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	d := strings.Repeat("a", 64)
	return domain.WebSearchGrantV1{Version: 1, TenantID: "tenant-a", OwnerUserID: "owner-a", RunID: "run-a", AttemptID: "attempt-a", BillingOwnerUserID: "owner-a", ResourceID: "search-a", ResourceRevision: 1,
		RevocationGeneration: 1, ConsentRevision: 1, ProfileID: domain.WebSearchProfileV1, ProfileRevision: 1, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Endpoint: domain.WebSearchEndpointV1, Method: "POST",
		CredentialRef: "search-credential-a", CredentialAudience: domain.WebSearchCredentialAudienceV1, DataPolicyDigest: d, APISchemaDigest: d, AdapterArtifactDigest: d, Currency: "USD", PriceRevision: "price-1", PriceEvidenceDigest: d, TermsEvidenceDigest: d,
		PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: at, ExpiresAt: at.Add(time.Hour)}
}
func TestWebSearchDomainGrantClosedProfile(t *testing.T) {
	g := searchGrantFixture()
	original, e := g.Digest()
	if e != nil {
		t.Fatal(e)
	}
	for _, test := range []struct {
		name   string
		mutate func(*domain.WebSearchGrantV1)
	}{
		{"origin", func(v *domain.WebSearchGrantV1) { v.Endpoint = "https://other.example/search" }},
		{"credential-audience", func(v *domain.WebSearchGrantV1) { v.CredentialAudience = "openai" }},
		{"implicit-free", func(v *domain.WebSearchGrantV1) { v.PerCallReservationMicrounits = 0 }},
		{"profile", func(v *domain.WebSearchGrantV1) { v.APIProfile = "advanced" }},
		{"data-policy", func(v *domain.WebSearchGrantV1) { v.DataPolicyDigest = "" }},
		{"expiry", func(v *domain.WebSearchGrantV1) { v.ExpiresAt = v.IssuedAt }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := g
			test.mutate(&bad)
			if bad.Validate() == nil {
				t.Fatal("invalid grant data accepted")
			}
		})
	}
	changed := g
	changed.RunID = "other-run"
	d, e := changed.Digest()
	if e != nil || d == original {
		t.Fatalf("run association not sealed: digest=%s err=%v", d, e)
	}
}
func TestWebSearchDomainCitationURLSyntaxOnly(t *testing.T) {
	for _, uri := range []string{"https://docs.example.com/a?utm_source=x&tracking=kept", "https://www.example.org/page#section", "https://example.org:443/a%2Fb"} {
		if e := domain.ValidateWebSearchCitationURLV1(uri); e != nil {
			t.Errorf("public syntax %q err=%v", uri, e)
		}
	}
	for _, uri := range []string{"http://example.org", "https://user:pass@example.org/a", "https://localhost/a", "https://a.local/a", "https://metadata.google.internal/a", "https://169.254.169.254/a", "https://[::1]/a", "https://127.1/a", "https://0177.0.0.1/a", "https://0x7f000001/a", "https://2130706433/a", "https://metadata.goog/a", "https://a.internal/a", "https://example.org\\@127.0.0.1/a", "https://example.org/%00 x"} {
		if e := domain.ValidateWebSearchCitationURLV1(uri); e == nil {
			t.Errorf("unsafe citation syntax accepted %q", uri)
		}
	}
}
func TestWebSearchDomainResultCanonicalLineage(t *testing.T) {
	op := toolOperationFixture()
	op.ResultBytes = domain.WebSearchResultBytesV1
	op.Debit.BodyBytes = op.RequestBytes + op.ResultBytes
	d, e := op.Digest()
	if e != nil {
		t.Fatal(e)
	}
	at := searchGrantFixture().IssuedAt
	v := domain.WebSearchResultV1{Version: 1, Scope: op.Scope, OperationDigest: d, Sequence: op.Sequence, CallID: op.CallID, ProfileRevision: 1, GrantDigest: op.ToolGrantDigest, QueryDigest: op.QueryDigest, WireRequestDigest: op.RequestDigest,
		RawResponseDigest: strings.Repeat("f", 64), Provider: "tavily", RetrievedAt: at, Outcome: "known_results", Usage: domain.WebSearchUsageV1{State: "unknown"}, Warnings: []domain.WebSearchWarningV1{},
		Sources: []domain.WebSearchSourceV1{{Rank: 1, URL: "https://example.org/a?utm_x=kept", Title: "Source", Excerpt: "untrusted excerpt", PublishedAt: &at}}}
	v.Sources[0].SourceID = domain.WebSearchSourceIDV1(d, 1, v.Sources[0].URL)
	if e := v.ValidateForOperation(op); e != nil {
		t.Fatal(e)
	}
	body, e := domain.EncodeWebSearchResultV1(v)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := domain.DecodeWebSearchResultV1(body)
	if e != nil || decoded.Sources[0].SourceID != v.Sources[0].SourceID {
		t.Fatalf("canonical roundtrip err=%v", e)
	}
	for _, bad := range [][]byte{append(bytes.Clone(body), []byte(" {}")...), bytes.Replace(body, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1), bytes.Replace(body, []byte(`"provider":"tavily"`), []byte(`"provider":"tavily","extra":true`), 1)} {
		if _, e := domain.DecodeWebSearchResultV1(bad); e == nil {
			t.Fatal("noncanonical result accepted")
		}
	}
	copy := v.Clone()
	copy.Sources[0].URL = "https://other.example.org"
	*copy.Sources[0].PublishedAt = at.Add(time.Hour)
	if copy.Validate() == nil || !v.Sources[0].PublishedAt.Equal(at) {
		t.Fatal("copied result leaked mutation or changed URL retained source ID")
	}
	foreign := op
	foreign.Scope.OwnerUserID = "other-owner"
	if v.ValidateForOperation(foreign) == nil {
		t.Fatal("cross-owner result associated")
	}
	credits := uint64(0)
	copy = v.Clone()
	copy.Usage.Credits = &credits
	if copy.Validate() == nil {
		t.Fatal("unknown usage disguised as zero credits")
	}
}
