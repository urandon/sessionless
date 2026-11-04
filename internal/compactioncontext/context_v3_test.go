package compactioncontext_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// Actual MODEL/compiler/result codecs over structural DATA. This supplies no
// original Source, current grant, authenticated decision, transport or send.
func searchContextV3Fixture(t *testing.T) (domain.ConfiguredInferenceProfileV1, domain.AgentLoopPolicyV1, []compactioncontext.CompletedOperationV1, []configuredinference.ToolV1, compactioncontext.ContextDataV3) {
	t.Helper()
	c, p := generationFixture(t)
	h := strings.Repeat("a", 64)
	at := p.IssuedAt
	cost := p.AdmissionCostCeiling.WebSearchCost
	amount := uint64(20000)
	cost.MaxAmountMicrounits, cost.PriceState, cost.ProfileID = &amount, domain.ExternalServicePriceKnownPaidV1, domain.WebSearchProfileV1
	p.AdmissionCostCeiling.MaxTotalAmountMicrounits = &amount
	grant := domain.WebSearchResourceGrantV2{Version: 2, TenantID: c.TenantID, OwnerUserID: c.OwnerUserID, BillingOwnerUserID: cost.BillingOwnerUserID, ResourceID: cost.ResourceID, ResourceRevision: cost.ResourceRevision, RevocationGeneration: cost.RevocationGeneration, ConsentRevision: 1, ProfileID: cost.ProfileID, ProfileRevision: cost.ProfileRevision, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Endpoint: domain.WebSearchEndpointV1, Method: "POST", CredentialRef: "structural-search-credential", CredentialAudience: domain.WebSearchCredentialAudienceV1, DataPolicyDigest: h, APISchemaDigest: h, AdapterArtifactDigest: h, Currency: cost.Currency, PriceRevision: cost.PriceRevision, PriceEvidenceDigest: cost.PriceEvidenceDigest, TermsEvidenceDigest: cost.TermsEvidenceDigest, PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: at, ExpiresAt: cost.PriceExpiresAt}
	var err error
	cost.ScopedGrantDigest, err = grant.Digest()
	if err != nil {
		t.Fatal(err)
	}
	p.WebSearchGrantDigest = cost.ScopedGrantDigest
	if err := p.Validate(); err != nil {
		t.Fatal("valid structural policy", err)
	}
	policyDigest, _ := p.Digest()
	ceiling, _ := p.AdmissionCostCeiling.Digest()
	scope := domain.AttemptOperationScopeV1{TenantID: c.TenantID, OwnerUserID: c.OwnerUserID, SessionID: generationRecords(t)[0].Event.SessionID, RunID: p.RunID, AttemptID: p.AttemptID, LeaseID: "structural-lease", FenceToken: 1, PhysicalInvocationClaimID: "structural-claim", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(h), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(h), HarnessBindingDigest: domain.HarnessBindingDigestV2(h), AgentLoopPolicyDigest: policyDigest}
	tools := []configuredinference.ToolV1{websearch.ModelToolV1()}
	frozen, err := configuredinference.FreezeRequestV1(c, c.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "Find bounded public information."}}, Tools: tools, MaxOutputTokens: 20})
	if err != nil {
		t.Fatal(err)
	}
	request := frozen.Body()
	normalized, err := frozen.NormalizeResponseBodyV1([]byte(`{"model":"model-local","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"search-call","type":"function","function":{"name":"web_search","arguments":"{\"query\":\"bounded public query\"}"}}]},"finish_reason":"tool_calls"}]}`), domain.AttemptOperationModelV1, 8<<10)
	if err != nil {
		t.Fatal("actual configured MODEL normalization", err)
	}
	modelOp := domain.AttemptOperationV1{Version: 1, Scope: scope, Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "structural-model", RequestRef: "model-request", RequestDigest: retainedHash(request), Deadline: at.Add(2 * time.Second), CleanupDuration: time.Second, Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 100, OutputTokens: 20, BodyBytes: uint64(len(request)) + 8<<10, ResponseBytes: 64 << 10, ActiveDuration: time.Second}, RequestBytes: uint64(len(request)), ResultBytes: 8 << 10, ContextBytes: 1, Currency: p.AdmissionCostCeiling.Currency, PriceRevision: p.AdmissionCostCeiling.PriceRevision, AdmissionCostCeilingDigest: ceiling}
	model := compactioncontext.CompletedOperationV1{Request: request, Response: normalized}
	model.Reservation = domain.AttemptOperationReservationV1{Version: 1, Operation: modelOp, ReservedAt: at.Add(time.Second), AggregateDebit: modelOp.Debit, RegisteredAllocationDigest: domain.RegisteredAllocationDigestV2(h), RegisteredProxyDigest: domain.RegisteredProxyAttestationDigestV2(h)}
	model.Reservation.OperationDigest, err = modelOp.Digest()
	if err != nil {
		t.Fatal(err)
	}
	model.Reservation.AggregateDebit.BodyBytes += 8 << 10
	model.Observation = domain.AttemptOperationObservationV1{Version: 1, OperationDigest: model.Reservation.OperationDigest, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: "normalized-model", ResponseDigest: retainedHash(normalized), ResponseBytes: uint64(len(normalized)), ObservedAt: at.Add(time.Second + time.Millisecond), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
	model.Observation.ReservationDigest, err = model.Reservation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	binding := websearch.BindingV1{Scope: scope, Sequence: 2, CallID: "search-call", ModelResponseDigest: model.Observation.ResponseDigest, InitiatingUserID: c.OwnerUserID, ParentCostCeilingDigestV2: ceiling, ParentPriceRevision: p.AdmissionCostCeiling.PriceRevision}
	args := []byte(`{"query":"bounded public query"}`)
	compiled, err := websearch.CompileV2(grant, *cost, binding, args, at.Add(2*time.Second))
	if err != nil {
		t.Fatal("actual pure SEARCH compiler", err)
	}
	ad, qd, wd := compiled.Digests()
	wire := compiled.Body()
	searchOp := domain.AttemptOperationV1{Version: 1, Scope: scope, Sequence: 2, Kind: domain.AttemptOperationWebSearchV1, CallID: binding.CallID, RequestRef: "physical-search-request", RequestDigest: wd, RequestBytes: uint64(len(wire)), ResultBytes: domain.WebSearchResultBytesV1, ModelResponseDigest: binding.ModelResponseDigest, ArgumentsDigest: ad, QueryDigest: qd, ToolGrantDigest: p.WebSearchGrantDigest, ConsentRequestDigest: h, ConsentDecisionDigest: h, ConsentDecisionRef: "structural-approved-consent", Deadline: at.Add(17 * time.Second), CleanupDuration: time.Second, Debit: domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, WebSearches: 1, BodyBytes: uint64(len(wire)) + domain.WebSearchResultBytesV1, ResponseBytes: domain.WebSearchResponseBytesV1, AmountMicrounits: 10000, ActiveDuration: 15 * time.Second}, Currency: grant.Currency, PriceRevision: binding.ParentPriceRevision, AdmissionCostCeilingDigest: ceiling, ExternalChargeEvidenceDigest: grant.PriceEvidenceDigest, SearchWire: &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: grant.APIProfile, ArgumentsBytes: uint64(len(args)), CompilerArtifactDigest: grant.AdapterArtifactDigest}}
	searchOp.PredecessorObservationDigest, _ = model.Observation.Digest()
	bound, err := compiled.BindOperationV2(searchOp)
	if err != nil {
		t.Fatal(err)
	}
	typed, err := bound.DecodeResponseV2(t.Context(), websearch.HTTPMetadataV1{StatusCode: 200, ContentType: "application/json"}, strings.NewReader(`{"results":[{"title":"Bounded source","url":"https://example.com/source","content":"A bounded public excerpt."}]}`), at.Add(3*time.Second))
	if err != nil {
		t.Fatal("actual typed result normalization", err)
	}
	response, err := domain.EncodeWebSearchResultV1(typed)
	if err != nil {
		t.Fatal(err)
	}
	search := compactioncontext.CompletedOperationV1{Request: wire, Response: response}
	search.Reservation = domain.AttemptOperationReservationV1{Version: 1, Operation: searchOp, ReservedAt: at.Add(2 * time.Second), ComponentAmounts: [4]uint64{0, 0, 0, 10000}, RegisteredAllocationDigest: domain.RegisteredAllocationDigestV2(h), RegisteredProxyDigest: domain.RegisteredProxyAttestationDigestV2(h)}
	search.Reservation.OperationDigest, _ = searchOp.Digest()
	aggregate := model.Reservation.AggregateDebit
	aggregate.BodyBytes += (64 << 10) + uint64(len(args))
	search.Reservation.AggregateDebit, err = aggregate.Add(searchOp.Debit)
	if err != nil {
		t.Fatal(err)
	}
	search.Observation = domain.AttemptOperationObservationV1{Version: 1, OperationDigest: search.Reservation.OperationDigest, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: "typed-search-result", ResponseDigest: retainedHash(response), ResponseBytes: uint64(len(response)), ObservedAt: at.Add(3*time.Second + time.Microsecond), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
	search.Observation.ReservationDigest, err = search.Reservation.Digest()
	if err != nil || search.Observation.ValidateForReservation(search.Reservation) != nil {
		t.Fatal("valid structural SEARCH reservation/observation", err)
	}
	rd, _ := search.Reservation.Digest()
	od, _ := search.Observation.Digest()
	row := ports.OwnedOperationAccountingRowV4{Sequence: 2, Kind: domain.AttemptOperationWebSearchV1, OperationDigest: search.Reservation.OperationDigest, ReservationDigest: rd, ObservationDigest: od, Disposition: ports.OwnedOperationUnknownReservedV4, Reserved: searchOp.Debit, Usage: search.Observation.Usage, ToolSpendingDigest: h}
	data := compactioncontext.ContextDataV3{Version: 3, Tools: []compactioncontext.ToolWitnessV3{{Kind: domain.AttemptOperationWebSearchV1, Scope: scope, Sequence: 2, Search: &compactioncontext.SearchToolDataV3{Accounting: row, Result: typed}}}}
	return c, p, []compactioncontext.CompletedOperationV1{model, search}, tools, data
}

func TestContextV3ActualSearchResultReachesNextModel(t *testing.T) {
	c, p, ops, tools, data := searchContextV3Fixture(t)
	actual, err := compactioncontext.BuildOrdinaryV3(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops, nil, tools, data)
	if err != nil {
		t.Fatal("complete actual MODEL→SEARCH context", err)
	}
	_, input := generationWire(t, actual)
	units := input["original_units"].([]any)
	call, result := units[len(units)-2].(map[string]any), units[len(units)-1].(map[string]any)
	if call["original_role"] != "tool_call" || result["original_role"] != "tool_result" || strings.Contains(call["original_payload"].(string), "bounded public query") || !strings.Contains(result["original_payload"].(string), "A bounded public excerpt.") {
		t.Fatal("redacted call/actual typed result missing from next MODEL input")
	}
	if _, err := compactioncontext.BuildOrdinaryV2(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops, nil, tools, compactioncontext.ContextDataV2{Version: 2}); err == nil {
		t.Fatal("SEARCH silently widened V2")
	}
	compactioncontext.InspectContextV3SimultaneousRouteDenial(t, c, p, generationRecords(t), ops, tools, data)
}

func TestContextV3ProspectiveSearchUsesSameQuotingRenderer(t *testing.T) {
	c, p, ops, tools, data := searchContextV3Fixture(t)
	future := ops[1].Reservation.Operation
	upper, err := compactioncontext.BuildSearchSuccessorUpperV3(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops[:1], nil, tools, compactioncontext.ContextDataV3{Version: 3}, future)
	if err != nil {
		t.Fatal("same-renderer prospective upper", err)
	}
	actual, err := compactioncontext.BuildOrdinaryV3(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops, nil, tools, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(upper.Body()) <= len(actual.Body()) {
		t.Fatal("prospective quoting bound smaller than the actual typed-result request")
	}
	bad := future.Clone()
	bad.ArgumentsDigest = strings.Repeat("f", 64)
	if _, err := compactioncontext.BuildSearchSuccessorUpperV3(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops[:1], nil, tools, compactioncontext.ContextDataV3{Version: 3}, bad); err == nil {
		t.Fatal("crossed original call admitted as prospective LENGTH data")
	}
	if _, err := compactioncontext.BuildOrdinaryV3(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops[:1], nil, tools, compactioncontext.ContextDataV3{Version: 3}); err == nil {
		t.Fatal("prospective counting silently widened the actual pending-call renderer")
	}
}

func TestContextV3ProspectiveSearchBoundsHTMLAcrossBothQuotingLayers(t *testing.T) {
	c, p, ops, tools, data := searchContextV3Fixture(t)
	search := &ops[1]
	result := data.Tools[0].Search.Result.Clone()
	result.Sources = nil
	for i := uint32(1); i <= 5; i++ {
		result.Sources = append(result.Sources, domain.WebSearchSourceV1{Rank: i, URL: fmt.Sprintf("https://example.com/source/%d", i), Title: strings.Repeat("<", 512), Excerpt: strings.Repeat(">", 2048)})
		result.Sources[i-1].SourceID = domain.WebSearchSourceIDV1(result.OperationDigest, i, result.Sources[i-1].URL)
	}
	body, err := domain.EncodeWebSearchResultV1(result)
	if err != nil {
		t.Fatal("bounded HTML result DATA", err)
	}
	old := search.Reservation.Operation.ResultBytes
	op := &search.Reservation.Operation
	op.ResultBytes, op.Debit.BodyBytes = uint64(len(body)), op.RequestBytes+uint64(len(body))
	search.Reservation.AggregateDebit.BodyBytes -= old - op.ResultBytes
	search.Reservation.OperationDigest, err = op.Digest()
	if err != nil {
		t.Fatal(err)
	}
	result.OperationDigest = search.Reservation.OperationDigest
	for i := range result.Sources {
		result.Sources[i].SourceID = domain.WebSearchSourceIDV1(result.OperationDigest, result.Sources[i].Rank, result.Sources[i].URL)
	}
	body, err = domain.EncodeWebSearchResultV1(result)
	if err != nil || uint64(len(body)) != op.ResultBytes {
		t.Fatal("fixed-width digest rebound changed typed result size", err)
	}
	search.Response = body
	search.Observation.OperationDigest = search.Reservation.OperationDigest
	search.Observation.ReservationDigest, _ = search.Reservation.Digest()
	search.Observation.ResponseBytes, search.Observation.ResponseDigest = uint64(len(body)), retainedHash(body)
	row := &data.Tools[0].Search.Accounting
	row.OperationDigest, row.ReservationDigest, row.ObservationDigest, row.Reserved = search.Reservation.OperationDigest, search.Observation.ReservationDigest, "", op.Debit
	row.ObservationDigest, _ = search.Observation.Digest()
	data.Tools[0].Search.Result = result
	actual, err := compactioncontext.BuildOrdinaryV3(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops, nil, tools, data)
	if err != nil {
		t.Fatal("actual bounded HTML result renderer", err)
	}
	upper, err := compactioncontext.BuildSearchSuccessorUpperV3(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops[:1], nil, tools, compactioncontext.ContextDataV3{Version: 3}, *op)
	if err != nil {
		t.Fatal("HTML-aware same-renderer upper", err)
	}
	if len(upper.Body()) < len(actual.Body()) {
		t.Fatalf("prospective HTML upper=%d actual=%d", len(upper.Body()), len(actual.Body()))
	}
}

func TestContextV3OriginalCatalogIsExactAndClosed(t *testing.T) {
	c, _, ops, _, _ := searchContextV3Fixture(t)
	catalog, err := compactioncontext.ReadModelCatalogV3(1, ops[0].Request)
	if err != nil {
		t.Fatal("actual original MODEL request", err)
	}
	tools, err := compactioncontext.DecodeModelCatalogV3(catalog.Tools, c.Limits)
	if err != nil || len(tools) != 1 || tools[0].Name != "web_search" {
		t.Fatal("canonical minimum SEARCH surface", err)
	}
	omitted, err := compactioncontext.ReadModelCatalogV3(1, []byte(`{}`))
	if err != nil || omitted.Tools != nil {
		t.Fatal("omission changed", err)
	}
	empty, err := compactioncontext.ReadModelCatalogV3(1, []byte(`{"tools":[]}`))
	if err != nil || !bytes.Equal(empty.Tools, []byte("[]")) {
		t.Fatal("empty array lost", err)
	}
	for _, request := range [][]byte{[]byte(`{"tools":null}`), []byte(`{"tools": null }`), []byte(`{"TOOLS":[]}`), []byte(`{"tools":[],"TOOLS":[]}`), []byte(`{"tools":[],"tools":[]}`)} {
		if _, err := compactioncontext.ReadModelCatalogV3(1, request); err == nil {
			t.Fatal("null/alias/duplicate catalog admitted")
		}
	}
	for _, pair := range [][2]string{{"web_search", "unknown_unused_tool"}, {"Search public web sources for a query.", "different description"}, {`"maxLength":1024`, `"maxLength":200`}} {
		if bytes.Count(catalog.Tools, []byte(pair[0])) != 1 {
			t.Fatal("catalog mutation is vacuous", pair[0])
		}
		bad := bytes.Replace(catalog.Tools, []byte(pair[0]), []byte(pair[1]), 1)
		if _, err := compactioncontext.DecodeModelCatalogV3(bad, c.Limits); err == nil {
			t.Fatal("unsupported catalog admitted", pair[0])
		}
	}
}

func TestContextV3RejectsCrossedAndDriftingWitnesses(t *testing.T) {
	for _, mutation := range []string{"version", "kind", "missing", "duplicate", "both", "scope", "reservation", "result", "debit", "unknown", "body"} {
		t.Run(mutation, func(t *testing.T) {
			c, p, ops, tools, data := searchContextV3Fixture(t)
			if _, err := compactioncontext.BuildOrdinaryV3(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops, nil, tools, data); err != nil {
				t.Fatal("valid baseline", err)
			}
			switch mutation {
			case "version":
				data.Version = 2
			case "kind":
				data.Tools[0].Kind = domain.AttemptOperationMCPToolV1
			case "missing":
				data.Tools = nil
			case "duplicate":
				data.Tools = append(data.Tools, data.Tools[0])
			case "both":
				data.Tools[0].MCP = &compactioncontext.MCPConservativeDataV2{}
			case "scope":
				data.Tools[0].Scope.FenceToken++
			case "reservation":
				data.Tools[0].Search.Accounting.ReservationDigest = domain.AttemptOperationReservationDigestV1(strings.Repeat("f", 64))
			case "result":
				data.Tools[0].Search.Result.QueryDigest = strings.Repeat("f", 64)
			case "debit":
				data.Tools[0].Search.Accounting.Reserved.AmountMicrounits--
			case "unknown":
				ops[1].Observation.Outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
			case "body":
				ops[1].Response = bytes.Clone(ops[1].Response)
				ops[1].Response[0] ^= 1
			}
			if _, err := compactioncontext.BuildOrdinaryV3(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops, nil, tools, data); err == nil {
				t.Fatal("contradiction admitted", mutation)
			}
		})
	}
}
