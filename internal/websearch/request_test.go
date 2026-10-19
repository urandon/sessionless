package websearch

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func searchFixture() (domain.WebSearchGrantV1, domain.ExternalServiceCostCeilingV1, BindingV1, time.Time) {
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	d := strings.Repeat("a", 64)
	g := domain.WebSearchGrantV1{Version: 1, TenantID: "tenant-a", OwnerUserID: "owner-a", RunID: "run-a", AttemptID: "attempt-a", BillingOwnerUserID: "owner-a", ResourceID: "search-a", ResourceRevision: 1, RevocationGeneration: 1, ConsentRevision: 1,
		ProfileID: domain.WebSearchProfileV1, ProfileRevision: 1, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Endpoint: domain.WebSearchEndpointV1, Method: "POST", CredentialRef: "search-credential", CredentialAudience: domain.WebSearchCredentialAudienceV1,
		DataPolicyDigest: d, APISchemaDigest: d, AdapterArtifactDigest: d, Currency: "USD", PriceRevision: "price-1", PriceEvidenceDigest: d, TermsEvidenceDigest: d, PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: at, ExpiresAt: at.Add(time.Hour)}
	gd, _ := g.Digest()
	amount := uint64(20000)
	c := domain.ExternalServiceCostCeilingV1{Version: 1, Kind: domain.ExternalServiceWebSearchV1, TenantID: g.TenantID, OwnerUserID: g.OwnerUserID, BillingOwnerUserID: g.BillingOwnerUserID, ResourceID: g.ResourceID, ResourceRevision: 1, RevocationGeneration: 1,
		ProfileID: g.ProfileID, ProfileRevision: 1, ScopedGrantDigest: gd, Currency: g.Currency, PriceRevision: g.PriceRevision, PriceObservedAt: at, PriceExpiresAt: g.ExpiresAt, PriceEvidenceDigest: g.PriceEvidenceDigest, TermsEvidenceDigest: g.TermsEvidenceDigest, PriceState: domain.ExternalServicePriceKnownPaidV1, MaxAmountMicrounits: &amount}
	s := domain.AttemptOperationScopeV1{TenantID: g.TenantID, SessionID: "session-a", OwnerUserID: g.OwnerUserID, RunID: g.RunID, AttemptID: g.AttemptID, LeaseID: "lease-a", FenceToken: 1, PhysicalInvocationClaimID: "claim-a",
		EffectReservationDigest: domain.AttemptEffectReservationDigestV1(d), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(d), HarnessBindingDigest: domain.HarnessBindingDigestV2(d), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(d)}
	return g, c, BindingV1{Scope: s, Sequence: 2, CallID: "search-a", ModelResponseDigest: d, InitiatingUserID: g.OwnerUserID,
		ParentCostCeilingDigestV2: domain.AdmissionCostCeilingDigestV2(d), ParentPriceRevision: "price-1"}, at
}
func compileSearch(t *testing.T, args []byte) *FrozenRequestV1 {
	t.Helper()
	g, c, b, at := searchFixture()
	f, e := CompileV1(g, c, b, args, at)
	if e != nil {
		t.Fatal(e)
	}
	return f
}
func searchOperation(f *FrozenRequestV1) domain.AttemptOperationV1 {
	d := strings.Repeat("a", 64)
	deadline := f.compiledAt.Add(15 * time.Second)
	if f.validUntil.Before(deadline) {
		deadline = f.validUntil
	}
	return domain.AttemptOperationV1{Version: 1, Scope: f.binding.Scope, Sequence: 2, Kind: domain.AttemptOperationWebSearchV1, CallID: f.binding.CallID, RequestRef: "search-wire", RequestDigest: f.wireDigest,
		PredecessorObservationDigest: domain.AttemptOperationObservationDigestV1(d), ModelResponseDigest: d, ArgumentsDigest: f.argumentsDigest, QueryDigest: f.queryDigest, ToolGrantDigest: f.grantDigest,
		ConsentRequestDigest: d, ConsentDecisionDigest: d, ConsentDecisionRef: "decision-a", Deadline: deadline, CleanupDuration: time.Second,
		Debit:        domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, WebSearches: 1, BodyBytes: uint64(len(f.wire)) + domain.WebSearchResultBytesV1, ResponseBytes: domain.WebSearchResponseBytesV1, AmountMicrounits: 10000, ActiveDuration: 15 * time.Second},
		RequestBytes: uint64(len(f.wire)), ResultBytes: domain.WebSearchResultBytesV1, Currency: "USD", PriceRevision: f.binding.ParentPriceRevision, AdmissionCostCeilingDigest: f.binding.ParentCostCeilingDigestV2, ExternalChargeEvidenceDigest: f.grant.PriceEvidenceDigest}
}
func boundSearch(t *testing.T) *FrozenRequestV1 {
	t.Helper()
	f := compileSearch(t, []byte(`{"query":" public query "}`))
	bound, e := f.BindOperationV1(searchOperation(f))
	if e != nil {
		t.Fatal(e)
	}
	return bound
}

func TestWebSearchRequestExactBasicWire(t *testing.T) {
	args := []byte(" { \"query\" : \" public <query> & x \" , \"max_results\" : 3 } ")
	f := compileSearch(t, args)
	var wire map[string]any
	if json.Unmarshal(f.Body(), &wire) != nil {
		t.Fatal("invalid compiled JSON")
	}
	want := map[string]any{"query": " public <query> & x ", "max_results": float64(3), "search_depth": "basic", "topic": "general", "language": "en", "filter_by_language": true, "safe_search": true, "auto_parameters": false, "include_usage": true,
		"include_answer": false, "include_raw_content": false, "include_images": false, "include_image_descriptions": false, "include_favicon": false}
	got, _ := json.Marshal(wire)
	expected, _ := json.Marshal(want)
	if !bytes.Equal(got, expected) {
		t.Fatalf("fixed profile wire got=%s want=%s", got, expected)
	}
	ad, qd, wd := f.Digests()
	if ad != digest(args) || qd != digest([]byte(want["query"].(string))) || wd != digest(f.Body()) || ad == wd {
		t.Fatal("args/query/wire commitments conflated")
	}
	args[0] = '!'
	body := f.Body()
	body[0] = '!'
	original := f.Arguments()
	original[0] = '!'
	if !json.Valid(f.Body()) || f.Arguments()[0] != ' ' {
		t.Fatal("caller mutation changed frozen bytes")
	}
	if bytes.Contains(f.Body(), []byte("api_key")) || bytes.Contains(f.Body(), []byte(`\u003c`)) {
		t.Fatal("credential/default HTML escaping entered wire")
	}
}
func TestWebSearchRequestStrictArguments(t *testing.T) {
	for _, raw := range []string{`{}`, `{"query":""}`, `{"query":" "}`, `{"query":null}`, `{"query":"a","query":"b"}`, `{"query":"a","Query":"b"}`, `{"QUERY":"a"}`, `{"query":"a","url":"https://example.org"}`, `{"query":"a","max_results":0}`, `{"query":"a","max_results":6}`, `{"query":"a","max_results":null}`, `{"query":"a","max_results":1.0}`, `{"query":"a","max_results":1e0}`, `{"query":"\ud800"}`, `{"query":"\udc00"}`, `{"query":"a"} {}`, `[]`} {
		t.Run(raw, func(t *testing.T) {
			g, c, b, at := searchFixture()
			if _, e := CompileV1(g, c, b, []byte(raw), at); e == nil {
				t.Fatal("invalid model arguments accepted")
			}
		})
	}
	for _, args := range [][]byte{[]byte(`{"query":"` + strings.Repeat("x", 1025) + `"}`), append([]byte(`{"query":"`), 0xff), []byte(strings.Repeat(" ", 4097))} {
		g, c, b, at := searchFixture()
		if _, e := CompileV1(g, c, b, args, at); e == nil {
			t.Fatal("argument byte/unicode limit ignored")
		}
	}
	if f := compileSearch(t, []byte(`{"query":"\ud83d\ude00"}`)); f.query != "😀" {
		t.Fatal("valid surrogate pair changed")
	}
}
func TestWebSearchRequestPhysicalWireBudgetAndV1Mismatch(t *testing.T) {
	// A legal nearly4KiB original argument body; the fixed supported options
	// make the physical body exceed the old tool-operation RequestBytes cap.
	args, _ := json.Marshal(map[string]string{"query": strings.Repeat("\x00", 675)})
	if len(args) > 4096 {
		t.Fatal("fixture exceeded original args contract")
	}
	f := compileSearch(t, args)
	if len(f.wire) <= 4096 || len(f.wire) > 8192 {
		t.Fatalf("distinct physical bound not exercised: args=%d wire=%d", len(args), len(f.wire))
	}
	if _, e := f.BindOperationV1(searchOperation(f)); e != ErrOperationCompatibility {
		t.Fatalf("old operation cap silently bypassed: err=%v", e)
	}
	empty := compileSearch(t, []byte(`{"query":"x"}`))
	overhead := len(empty.wire) - len(`"x"`)
	if overhead >= 2048 || 6*domain.WebSearchQueryBytesV1+overhead > domain.WebSearchWireBytesV1 {
		t.Fatalf("conservative physical wire arithmetic: fixed=%d worst=%d", overhead, 6*domain.WebSearchQueryBytesV1+overhead)
	}
	query := strings.Repeat("x", 1024)
	exact, _ := json.Marshal(map[string]string{"query": query})
	if compileSearch(t, exact).query != query {
		t.Fatal("legal max query silently shrunk")
	}
}
func TestWebSearchRequestIndependentCostData(t *testing.T) {
	for _, kind := range []string{"unknown", "free", "foreign", "expired", "terms"} {
		t.Run(kind, func(t *testing.T) {
			g, c, b, at := searchFixture()
			switch kind {
			case "unknown":
				c.PriceState = domain.ExternalServicePriceUnknownV1
				c.MaxAmountMicrounits = nil
			case "free":
				c.PriceState = domain.ExternalServicePriceKnownFreeV1
				c.MaxAmountMicrounits = new(uint64)
			case "foreign":
				c.BillingOwnerUserID = "other"
			case "expired":
				at = c.PriceExpiresAt
			case "terms":
				c.TermsEvidenceDigest = strings.Repeat("b", 64)
			}
			if _, e := CompileV1(g, c, b, []byte(`{"query":"a"}`), at); e == nil {
				t.Fatal("unfunded/foreign/drifted structural cost accepted")
			}
		})
	}
}
