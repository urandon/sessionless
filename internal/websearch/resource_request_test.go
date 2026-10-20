package websearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func resourceCompilerFixture(t testing.TB) (domain.WebSearchResourceGrantV2, domain.ExternalServiceCostCeilingV1, BindingV1, time.Time) {
	t.Helper()
	v, cost, b, at := searchFixture()
	g := domain.WebSearchResourceGrantV2{Version: 2, TenantID: v.TenantID, OwnerUserID: v.OwnerUserID, BillingOwnerUserID: v.BillingOwnerUserID,
		ResourceID: v.ResourceID, ResourceRevision: v.ResourceRevision, RevocationGeneration: v.RevocationGeneration, ConsentRevision: v.ConsentRevision,
		ProfileID: v.ProfileID, ProfileRevision: v.ProfileRevision, Provider: v.Provider, APIProfile: v.APIProfile, Endpoint: v.Endpoint, Method: v.Method,
		CredentialRef: v.CredentialRef, CredentialAudience: v.CredentialAudience, DataPolicyDigest: v.DataPolicyDigest, APISchemaDigest: v.APISchemaDigest,
		AdapterArtifactDigest: v.AdapterArtifactDigest, Currency: v.Currency, PriceRevision: v.PriceRevision, PriceEvidenceDigest: v.PriceEvidenceDigest,
		TermsEvidenceDigest: v.TermsEvidenceDigest, PerCallReservationMicrounits: v.PerCallReservationMicrounits, AggregateReservationMicrounits: v.AggregateReservationMicrounits,
		IssuedAt: v.IssuedAt, ExpiresAt: v.ExpiresAt}
	var err error
	cost.ScopedGrantDigest, err = g.Digest()
	if err != nil || g.ValidateCostAt(cost, at) != nil {
		t.Fatalf("invalid reusable compiler DATA fixture: digest=%v", err)
	}
	b.ParentPriceRevision = "distinct-parent-model-price"
	b.InitiatingUserID = "original-task-initiator"
	return g, cost, b, at
}

func compileResourceSearch(t testing.TB, args []byte) (*FrozenRequestV2, domain.WebSearchResourceGrantV2, BindingV1, time.Time) {
	t.Helper()
	g, cost, b, at := resourceCompilerFixture(t)
	f, err := CompileV2(g, cost, b, args, at)
	if err != nil || f == nil {
		t.Fatalf("valid V2 resource compiler DATA denied: %v", err)
	}
	return f, g, b, at
}

func resourceCompilerOperation(f *FrozenRequestV2, g domain.WebSearchResourceGrantV2, b BindingV1, at time.Time) domain.AttemptOperationV1 {
	ad, qd, wd := f.Digests()
	gd, _ := g.Digest()
	d := strings.Repeat("a", 64)
	return domain.AttemptOperationV1{Version: 1, Scope: b.Scope, Sequence: b.Sequence, Kind: domain.AttemptOperationWebSearchV1, CallID: b.CallID,
		RequestRef: "original-physical-search-body", RequestDigest: wd, RequestBytes: uint64(len(f.Body())), ResultBytes: domain.WebSearchResultBytesV1,
		PredecessorObservationDigest: domain.AttemptOperationObservationDigestV1(d), ModelResponseDigest: b.ModelResponseDigest, ArgumentsDigest: ad, QueryDigest: qd, ToolGrantDigest: gd,
		ConsentRequestDigest: d, ConsentDecisionDigest: d, ConsentDecisionRef: "original-consent", Deadline: at.Add(15 * time.Second), CleanupDuration: time.Second,
		Debit: domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, WebSearches: 1, BodyBytes: uint64(len(f.Body())) + domain.WebSearchResultBytesV1,
			ResponseBytes: domain.WebSearchResponseBytesV1, AmountMicrounits: 10000, ActiveDuration: 15 * time.Second}, Currency: g.Currency,
		PriceRevision: b.ParentPriceRevision, AdmissionCostCeilingDigest: b.ParentCostCeilingDigestV2, ExternalChargeEvidenceDigest: g.PriceEvidenceDigest,
		SearchWire: &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: domain.WebSearchAPIProfileV1, ArgumentsBytes: uint64(len(f.Arguments())), CompilerArtifactDigest: g.AdapterArtifactDigest}}
}

func resourceCompilerConsent(f *FrozenRequestV2, g domain.WebSearchResourceGrantV2, b BindingV1, at time.Time) domain.WebSearchConsentRequestV1 {
	ad, qd, _ := f.Digests()
	gd, _ := g.Digest()
	s := b.Scope
	return domain.WebSearchConsentRequestV1{Version: 1, ID: "original-consent", Scope: domain.WebSearchConsentScopeV1{TenantID: s.TenantID, SessionID: s.SessionID, OwnerUserID: s.OwnerUserID,
		RunID: s.RunID, AttemptID: s.AttemptID, LeaseID: s.LeaseID, FenceToken: s.FenceToken, PhysicalInvocationClaimID: s.PhysicalInvocationClaimID,
		EffectReservationDigest: string(s.EffectReservationDigest), InvocationAuthorityDigest: string(s.InvocationAuthorityDigest), HarnessBindingDigest: string(s.HarnessBindingDigest), PolicyDigest: string(s.AgentLoopPolicyDigest)},
		InitiatingUserID: b.InitiatingUserID, ModelResponseDigest: b.ModelResponseDigest, CallID: b.CallID, ArgumentsDigest: ad, QueryDigest: qd, GrantDigest: gd,
		ConsentRevision: g.ConsentRevision, RevocationGeneration: g.RevocationGeneration,
		Arguments: domain.BlobRef{TenantID: s.TenantID, Key: domain.SessionRunObjectPrefix(s.TenantID, s.SessionID, s.RunID) + "attempts/" + string(s.AttemptID) + "/original-consent-arguments", Size: int64(len(f.Arguments())), SHA256: ad},
		CreatedAt: at, ExpiresAt: at.Add(60 * time.Second)}
}

func TestWebSearchResourceCompilerTwoInvocationsOneGrant(t *testing.T) {
	g, cost, a, at := resourceCompilerFixture(t)
	b := a
	b.Scope.SessionID, b.Scope.RunID, b.Scope.AttemptID, b.Scope.LeaseID, b.Scope.PhysicalInvocationClaimID = "session-b", "run-b", "attempt-b", "lease-b", "claim-b"
	args := []byte(` {"query":" public <query> & x ","max_results":3} `)
	fa, err := CompileV2(g, cost, a, args, at)
	if err != nil || fa == nil {
		t.Fatalf("first original invocation denied: %v", err)
	}
	fb, err := CompileV2(g, cost, b, args, at)
	if err != nil || fb == nil {
		t.Fatalf("second original invocation denied: %v", err)
	}
	if fa.compilerProfile != CompilerProfileV2 || fb.compilerProfile != CompilerProfileV2 || !bytes.Equal(fa.Body(), fb.Body()) || fa == fb {
		t.Fatal("explicit V2 profile, independent owners or shared fixed wire lost")
	}
	legacy, legacyCost, _, _ := searchFixture()
	if _, err := CompileV1(legacy, legacyCost, b, args, at); err == nil {
		t.Fatal("V1 invocation-scoped compiler silently widened")
	}
	oa, ob := resourceCompilerOperation(fa, g, a, at), resourceCompilerOperation(fb, g, b, at)
	if oa.Validate() != nil || ob.Validate() != nil {
		t.Fatal("invalid positive operation DATA fixture")
	}
	ba, err := fa.BindOperationV2(oa)
	if err != nil {
		t.Fatal(err)
	}
	bb, err := fb.BindOperationV2(ob)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ba.BindOperationV2(ob); err == nil {
		t.Fatal("cross-Attempt operation accepted")
	}
	ca, cb := resourceCompilerConsent(fa, g, a, at), resourceCompilerConsent(fb, g, b, at)
	if ba.ValidateConsentRequest(ca) != nil || bb.ValidateConsentRequest(cb) != nil || ba.ValidateConsentRequest(cb) == nil {
		t.Fatal("consent scope/reusable grant association lost")
	}
	for _, x := range []struct {
		f  *FrozenRequestV2
		op domain.AttemptOperationV1
	}{{ba, oa}, {bb, ob}} {
		result, err := x.f.DecodeResponseV2(context.Background(), HTTPMetadataV1{StatusCode: 200, ContentType: "application/json"}, strings.NewReader(`{"results":[{"url":"https://example.org/a","title":"source","content":"untrusted source text"}]}`), at.Add(time.Second))
		if err != nil || result.ValidateForOperation(x.op) != nil || result.GrantDigest != cost.ScopedGrantDigest {
			t.Fatalf("genuine V2 operation result association: %+v err=%v", result, err)
		}
	}
}

func TestWebSearchResourceCompilerExactWireOwnership(t *testing.T) {
	args := []byte(` { "query" : " public <query> & x " , "max_results" : 3 } `)
	f, _, _, _ := compileResourceSearch(t, args)
	want := `{"query":" public <query> & x ","max_results":3,"search_depth":"basic","topic":"general","language":"en","filter_by_language":true,"safe_search":true,"auto_parameters":false,"include_usage":true,"include_answer":false,"include_raw_content":false,"include_images":false,"include_image_descriptions":false,"include_favicon":false}`
	if string(f.Body()) != want {
		t.Fatalf("V2 fixed wire got=%s want=%s", f.Body(), want)
	}
	ad, qd, wd := f.Digests()
	if ad != digest(args) || qd != digest([]byte(" public <query> & x ")) || wd != digest([]byte(want)) || ad == wd {
		t.Fatal("original argument/query/physical commitments conflated")
	}
	args[0] = '!'
	body := f.Body()
	body[0] = '!'
	copyArgs := f.Arguments()
	copyArgs[0] = '!'
	if f.Arguments()[0] != ' ' || string(f.Body()) != want {
		t.Fatal("caller mutated frozen buffers")
	}
	if strings.Contains(f.String(), "query>") || strings.Contains(f.GoString(), "query>") {
		t.Fatal("private text leaked in formatter")
	}
	var nilRequest *FrozenRequestV2
	if nilRequest.Body() != nil || nilRequest.Arguments() != nil {
		t.Fatal("nil DATA exposed buffer")
	}
}

func TestWebSearchResourceCompilerClosedArgumentsAndAuthorityData(t *testing.T) {
	g, c, b, at := resourceCompilerFixture(t)
	if _, err := CompileV2(g, c, b, []byte(`{"query":"positive"}`), at); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{}`, `{"query":""}`, `{"query":null}`, `{"query":"a","query":"b"}`, `{"Query":"a"}`, `{"query":"a","url":"https://example.org"}`, `{"query":"a","max_results":1.0}`, `{"query":"a","max_results":6}`, `{"query":"\ud800"}`, `{"query":"a"}{}`} {
		if f, err := CompileV2(g, c, b, []byte(raw), at); err == nil || f != nil {
			t.Fatalf("unsupported arguments accepted %q err=%v", raw, err)
		}
	}
	for _, tc := range []struct {
		name   string
		change func(*domain.WebSearchResourceGrantV2, *domain.ExternalServiceCostCeilingV1, *BindingV1, *time.Time)
	}{
		{"unknown-version", func(g *domain.WebSearchResourceGrantV2, _ *domain.ExternalServiceCostCeilingV1, _ *BindingV1, _ *time.Time) {
			g.Version = 1
		}},
		{"foreign-tenant", func(_ *domain.WebSearchResourceGrantV2, _ *domain.ExternalServiceCostCeilingV1, b *BindingV1, _ *time.Time) {
			b.Scope.TenantID = "foreign-tenant"
		}},
		{"foreign-owner", func(_ *domain.WebSearchResourceGrantV2, _ *domain.ExternalServiceCostCeilingV1, b *BindingV1, _ *time.Time) {
			b.Scope.OwnerUserID = "foreign-owner"
		}},
		{"missing-initiator", func(_ *domain.WebSearchResourceGrantV2, _ *domain.ExternalServiceCostCeilingV1, b *BindingV1, _ *time.Time) {
			b.InitiatingUserID = ""
		}},
		{"missing-parent-cost", func(_ *domain.WebSearchResourceGrantV2, _ *domain.ExternalServiceCostCeilingV1, b *BindingV1, _ *time.Time) {
			b.ParentCostCeilingDigestV2 = ""
		}},
		{"changed-price", func(_ *domain.WebSearchResourceGrantV2, c *domain.ExternalServiceCostCeilingV1, _ *BindingV1, _ *time.Time) {
			c.PriceRevision = "different-price"
		}},
		{"raw-semantic-confusion", func(g *domain.WebSearchResourceGrantV2, c *domain.ExternalServiceCostCeilingV1, _ *BindingV1, _ *time.Time) {
			raw, _ := domain.EncodeWebSearchResourceGrantV2(*g)
			c.ScopedGrantDigest = digest(raw)
		}},
		{"grant-expired", func(g *domain.WebSearchResourceGrantV2, _ *domain.ExternalServiceCostCeilingV1, _ *BindingV1, at *time.Time) {
			*at = g.ExpiresAt
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gg, cc, bb, aa := g, c.Clone(), b, at
			tc.change(&gg, &cc, &bb, &aa)
			if f, err := CompileV2(gg, cc, bb, []byte(`{"query":"positive"}`), aa); err == nil || f != nil {
				t.Fatalf("invalid V2 input accepted err=%v", err)
			}
		})
	}
}

func TestWebSearchResourceCompilerPhysicalWireAndBindingDenials(t *testing.T) {
	args, _ := json.Marshal(map[string]string{"query": strings.Repeat("\x00", 675)})
	f, g, b, at := compileResourceSearch(t, args)
	if len(args) > 4096 || len(f.Body()) <= 4096 || len(f.Body()) > 8192 {
		t.Fatalf("legal args/wire case not exercised args=%d wire=%d", len(args), len(f.Body()))
	}
	op := resourceCompilerOperation(f, g, b, at)
	if op.Validate() != nil || f.ValidateOperation(op) != nil {
		t.Fatal("valid explicit physical wire arm denied")
	}
	for _, tc := range []struct {
		name   string
		change func(*domain.AttemptOperationV1)
	}{
		{"missing-arm", func(o *domain.AttemptOperationV1) { o.SearchWire = nil }},
		{"compiler-artifact", func(o *domain.AttemptOperationV1) { o.SearchWire.CompilerArtifactDigest = strings.Repeat("b", 64) }},
		{"argument-size", func(o *domain.AttemptOperationV1) { o.SearchWire.ArgumentsBytes-- }},
		{"physical-body-digest", func(o *domain.AttemptOperationV1) { o.RequestDigest = o.ArgumentsDigest }},
		{"model-response", func(o *domain.AttemptOperationV1) { o.ModelResponseDigest = strings.Repeat("b", 64) }},
		{"parent-price", func(o *domain.AttemptOperationV1) { o.PriceRevision = g.PriceRevision }},
		{"grant", func(o *domain.AttemptOperationV1) { o.ToolGrantDigest = strings.Repeat("b", 64) }},
		{"initiator-scope", func(o *domain.AttemptOperationV1) { o.Scope.PhysicalInvocationClaimID = "different-claim" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := op.Clone()
			tc.change(&bad)
			if x, err := f.BindOperationV2(bad); err == nil || x != nil {
				t.Fatal("divergent operation accepted")
			}
		})
	}
}

func TestWebSearchResourceCompilerResponseBoundAndCancellation(t *testing.T) {
	f, g, b, at := compileResourceSearch(t, []byte(`{"query":"public query"}`))
	op := resourceCompilerOperation(f, g, b, at)
	bound, err := f.BindOperationV2(op)
	if err != nil {
		t.Fatal(err)
	}
	meta := HTTPMetadataV1{StatusCode: 200, ContentType: "application/json"}
	reader := &searchCountingReader{r: strings.NewReader(`{"results":[]}`)}
	if _, err := f.DecodeResponseV2(context.Background(), meta, reader, at.Add(time.Second)); err == nil || reader.reads != 0 {
		t.Fatal("unbound compiler read result bytes")
	}
	for _, kind := range []string{"known-empty", "known-error", "overflow", "cancel", "deadline", "wrong-compiler"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			x := *bound
			body := `{"results":[]}`
			metadata := meta
			when := at.Add(time.Second)
			if kind == "known-error" {
				metadata.StatusCode = 429
				body = `{"error":"PRIVATE RAW ERROR"}`
			}
			if kind == "overflow" {
				body = strings.Repeat(" ", domain.WebSearchResponseBytesV1+2)
			}
			if kind == "deadline" {
				when = op.Deadline
			}
			if kind == "wrong-compiler" {
				x.compilerProfile = "unknown-compiler"
			}
			r := &searchCountingReader{r: strings.NewReader(body)}
			if kind == "cancel" {
				r.cancel = cancel
			}
			result, err := x.DecodeResponseV2(ctx, metadata, r, when)
			switch kind {
			case "known-empty":
				if err != nil || result.Outcome != "known_empty" || result.Usage.State != "unknown" {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			case "known-error":
				if err != nil || result.Outcome != "known_service_error" || result.Usage.State != "unknown" {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			case "overflow":
				if err != ErrOverflow || r.bytes != domain.WebSearchResponseBytesV1+1 {
					t.Fatalf("overflow bytes=%d err=%v", r.bytes, err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel error=%v", err)
				}
			default:
				if err == nil || r.reads != 0 {
					t.Fatalf("invalid profile/deadline read bytes=%d err=%v", r.reads, err)
				}
			}
			if r.closed {
				t.Fatal("borrowed reader was closed")
			}
			if err != nil && !reflect.DeepEqual(result, domain.WebSearchResultV1{}) {
				t.Fatal("error returned usable result DATA")
			}
		})
	}
}

func TestWebSearchResourceCompilerOriginalMinimaAndPrefilledDigest(t *testing.T) {
	for _, kind := range []string{"grant", "price"} {
		t.Run(kind, func(t *testing.T) {
			g, cost, b, at := resourceCompilerFixture(t)
			stop := at.Add(30 * time.Second)
			if kind == "grant" {
				g.ExpiresAt = stop
				cost.ScopedGrantDigest, _ = g.Digest()
			} else {
				cost.PriceExpiresAt = stop
			}
			f, err := CompileV2(g, cost, b, []byte(`{"query":"original shorter window"}`), at)
			if err != nil {
				t.Fatal(err)
			}
			r := resourceCompilerConsent(f, g, b, at)
			r.ExpiresAt = stop
			if f.ValidateConsentRequest(r) != nil {
				t.Fatal("lawful original minimum denied")
			}
			r.ExpiresAt = stop.Add(time.Microsecond)
			if f.ValidateConsentRequest(r) == nil {
				t.Fatal("original grant/price minimum extended")
			}
			op := resourceCompilerOperation(f, g, b, at)
			bound, err := f.BindOperationV2(op)
			if err != nil {
				t.Fatal(err)
			}
			// A digest supplied to compilation is not evidence that the narrower
			// original operation was bound, even when the digest itself is valid.
			b.OperationDigest, _ = op.Digest()
			prefilled, err := CompileV2(g, cost, b, f.Arguments(), at)
			if err != nil {
				t.Fatal(err)
			}
			reader := &searchCountingReader{r: strings.NewReader(`{"results":[]}`)}
			if _, err := prefilled.DecodeResponseV2(context.Background(), HTTPMetadataV1{StatusCode: 200, ContentType: "application/json"}, reader, at.Add(time.Second)); err == nil || reader.reads != 0 {
				t.Fatal("copied digest fabricated bound operation")
			}
			if _, err := bound.DecodeResponseV2(context.Background(), HTTPMetadataV1{StatusCode: 200, ContentType: "application/json"}, strings.NewReader(`{"results":[]}`), at.Add(time.Second)); err != nil {
				t.Fatalf("actual bound control denied: %v", err)
			}
		})
	}
}
