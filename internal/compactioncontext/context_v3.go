package compactioncontext

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/strictjson"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// ModelCatalogV3 preserves each MODEL's own original request encoding. Omitted
// tools and an authentic empty array differ; null is never a catalog.
type ModelCatalogV3 struct {
	Sequence uint64          `json:"sequence"`
	Tools    json.RawMessage `json:"tools,omitempty"`
}

type modelCatalogToolV3 struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function"`
}

// ReadModelCatalogV3 extracts DATA only. Its owner must first authenticate the
// exact owned request reference, raw digest and length; this is no serving cap.
func ReadModelCatalogV3(sequence uint64, request []byte) (ModelCatalogV3, error) {
	if sequence == 0 || sequence > 12 {
		return ModelCatalogV3{}, ErrCanonical
	}
	if _, err := strictjson.Parse(request, 1<<20, 64, 16384); err != nil {
		return ModelCatalogV3{}, ErrCanonical
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(request, &fields) != nil || fields == nil {
		return ModelCatalogV3{}, ErrCanonical
	}
	for key := range fields {
		if key != "tools" && strings.EqualFold(key, "tools") {
			return ModelCatalogV3{}, ErrCanonical
		}
	}
	raw := fields["tools"]
	if len(raw) > 64<<10 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return ModelCatalogV3{}, ErrCanonical
	}
	return ModelCatalogV3{Sequence: sequence, Tools: bytes.Clone(raw)}, nil
}

// DecodeModelCatalogV3 is the closed minimum catalog DATA language, not a
// latest-catalog comparison or descriptor-hash inversion. Used MCP descriptors
// still come from their original normalized grant; unused advertisements are
// retained from the original request and authenticated terminal seal.
func DecodeModelCatalogV3(raw []byte, limits domain.ConfiguredInferenceLimitsV1) ([]configuredinference.ToolV1, error) {
	if limits.Validate() != nil {
		return nil, ErrCanonical
	}
	if len(raw) == 0 {
		return nil, nil
	}
	if _, err := strictjson.Parse(raw, 64<<10, 32, 4096); err != nil {
		return nil, ErrCanonical
	}
	var wire []modelCatalogToolV3
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&wire) != nil || wire == nil || len(wire) > 2 {
		return nil, ErrCanonical
	}
	canonical, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, ErrCanonical
	}
	search := websearch.ModelToolV1()
	tools := make([]configuredinference.ToolV1, 0, len(wire))
	seen := map[string]bool{}
	for _, tool := range wire {
		f := tool.Function
		if tool.Type != "function" || seen[f.Name] || len(f.Description) > 4096 || configuredinference.ValidateToolSchemaV1(f.Parameters, limits) != nil || f.Name != search.Name && f.Name != "search_openai_docs" || f.Name == search.Name && (f.Description != search.Description || !bytes.Equal(f.Parameters, search.Parameters)) {
			return nil, ErrCanonical
		}
		seen[f.Name] = true
		tools = append(tools, configuredinference.ToolV1{Name: f.Name, Description: f.Description, Parameters: bytes.Clone(f.Parameters)})
	}
	return tools, nil
}

// A closed witness selects unchanged MCP DATA or the exact V4 SEARCH row and
// typed result. It cannot authenticate original funding, observation issuance,
// consent, current source, transport or a send.
type ToolWitnessV3 struct {
	Kind     domain.AttemptOperationKindV1  `json:"kind"`
	Scope    domain.AttemptOperationScopeV1 `json:"scope"`
	Sequence uint64                         `json:"sequence"`
	MCP      *MCPConservativeDataV2         `json:"mcp,omitempty"`
	Search   *SearchToolDataV3              `json:"search,omitempty"`
}

type SearchToolDataV3 struct {
	Accounting ports.OwnedOperationAccountingRowV4 `json:"accounting"`
	Result     domain.WebSearchResultV1            `json:"result"`
}

type ContextDataV3 struct {
	Version       uint32
	Tools         []ToolWitnessV3
	Historical    []HistoricalRunV3
	HistoricalV2  []HistoricalRunV2
	LegacyHistory []HistoricalRunV1
}

type contextRouteV3 struct {
	mcp       *contextRouteV2
	search    map[conservativeOperationKeyV2]ToolWitnessV3
	used      map[conservativeOperationKeyV2]bool
	history   []HistoricalRunV3
	projected map[domain.SessionEventID]projectedOperationV3
}

func newContextRouteV3(data ContextDataV3) (*contextRouteV3, error) {
	if data.Version != 3 || len(data.Tools) > 12 || len(data.Historical)+len(data.HistoricalV2)+len(data.LegacyHistory) > int(MaxEventsV1) {
		return nil, ErrCanonical
	}
	route := &contextRouteV3{search: make(map[conservativeOperationKeyV2]ToolWitnessV3), used: make(map[conservativeOperationKeyV2]bool), history: data.Historical, projected: make(map[domain.SessionEventID]projectedOperationV3)}
	var mcp []MCPConservativeDataV2
	seen := make(map[conservativeOperationKeyV2]bool)
	for _, witness := range data.Tools {
		key := conservativeOperationKeyV2{witness.Scope, witness.Sequence}
		if !witness.validShape() || seen[key] {
			return nil, ErrCanonical
		}
		seen[key] = true
		if witness.Kind == domain.AttemptOperationMCPToolV1 {
			mcp = append(mcp, *witness.MCP)
		} else {
			route.search[key] = witness
		}
	}
	var err error
	route.mcp, err = newContextRouteV2(ContextDataV2{Version: 2, ConservativeMCP: mcp, Historical: data.HistoricalV2, LegacyHistory: data.LegacyHistory})
	if err != nil {
		return nil, err
	}
	return route, nil
}

func (w ToolWitnessV3) validShape() bool {
	if w.Scope.Validate() != nil || w.Sequence < 2 || w.Sequence > 12 {
		return false
	}
	if w.Kind == domain.AttemptOperationMCPToolV1 {
		return w.MCP != nil && w.Search == nil && w.MCP.validShape() && w.MCP.Scope == w.Scope && w.MCP.Sequence == w.Sequence
	}
	if w.Kind != domain.AttemptOperationWebSearchV1 || w.MCP != nil || w.Search == nil {
		return false
	}
	row, result := w.Search.Accounting, w.Search.Result
	return row.Kind == w.Kind && row.Sequence == w.Sequence && row.OperationDigest.Validate() == nil && row.ReservationDigest.Validate() == nil && row.ObservationDigest.Validate() == nil && row.Disposition == ports.OwnedOperationUnknownReservedV4 && row.Usage.State == domain.AttemptOperationUsageUnknownV1 && row.Usage.Validate() == nil && row.Reserved.AmountMicrounits == 10000 && domain.ValidateSHA256Digest("search.spending", row.ToolSpendingDigest) == nil && result.Validate() == nil && result.Scope == w.Scope && result.Sequence == w.Sequence && result.OperationDigest == row.OperationDigest && (result.Outcome == "known_results" || result.Outcome == "known_empty")
}

func (w ToolWitnessV3) matchesSearch(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, normalized []byte) bool {
	if !w.validShape() || w.Kind != domain.AttemptOperationWebSearchV1 || r.Validate() != nil || o.ValidateForReservation(r) != nil || o.Outcome != domain.AttemptOperationCompletedV1 || o.Reason != domain.AttemptOperationReasonNoneV1 || w.Scope != r.Operation.Scope || w.Sequence != r.Operation.Sequence || w.Search.Result.ValidateForOperation(r.Operation) != nil || w.Search.Result.RetrievedAt.Before(r.ReservedAt) || w.Search.Result.RetrievedAt.After(o.ObservedAt) {
		return false
	}
	rd, _ := r.Digest()
	od, _ := o.Digest()
	row := w.Search.Accounting
	body, err := domain.EncodeWebSearchResultV1(w.Search.Result)
	return err == nil && bytes.Equal(body, normalized) && retainedDigest(normalized) == o.ResponseDigest && uint64(len(normalized)) == o.ResponseBytes && row.OperationDigest == r.OperationDigest && row.ReservationDigest == rd && row.ObservationDigest == od && row.Reserved == r.Operation.Debit && reflect.DeepEqual(row.Usage, o.Usage)
}

func (route *contextRouteV3) currentSearch(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1, normalized []byte, policy domain.AgentLoopPolicyV1) *ToolWitnessV3 {
	if route == nil {
		return nil
	}
	key := conservativeOperationKeyV2{r.Operation.Scope, r.Operation.Sequence}
	witness, found := route.search[key]
	cost := policy.AdmissionCostCeiling.WebSearchCost
	if !found || route.used[key] || !witness.matchesSearch(r, o, normalized) || cost == nil || cost.ValidateAt(r.ReservedAt) != nil || cost.MaxAmountMicrounits == nil || *cost.MaxAmountMicrounits != 20000 || r.Operation.SearchWire == nil || r.Operation.ToolGrantDigest != policy.WebSearchGrantDigest || r.Operation.ToolGrantDigest != cost.ScopedGrantDigest || r.Operation.ExternalChargeEvidenceDigest != cost.PriceEvidenceDigest || r.Operation.Currency != cost.Currency || r.Operation.Deadline.After(cost.PriceExpiresAt) {
		return nil
	}
	route.used[key] = true
	return &witness
}

// BuildOrdinaryV3 is the explicit in-fit SEARCH context consumer. There is no
// retained-V3 writer in this slice: old BuildRetainedV1/V2 reject version3.
func BuildOrdinaryV3(profile domain.ConfiguredInferenceProfileV1, limits domain.ConfiguredInferenceLimitsV1, policy domain.AgentLoopPolicyV1,
	sourceDigest string, records []sessioncontext.EventPayload, operations []CompletedOperationV1, parent *ParentV1, tools []configuredinference.ToolV1, data ContextDataV3) (*configuredinference.FrozenRequestV1, error) {
	route, err := newContextRouteV3(data)
	if err != nil {
		return nil, err
	}
	source, err := buildGenerationSource(profile, limits, policy, sourceDigest, records, operations, parent, tools, data.LegacyHistory, nil, route)
	if err != nil {
		return nil, err
	}
	return buildOrdinarySourceV1(source, parent)
}

// BuildSearchSuccessorUpperV3 is LENGTH DATA only. It renders the actual
// completed prefix through the SAME ordinary renderer, then counts the two
// future redacted frames through its exact two JSON quoting layers. The bound
// is never a result, observation, serving request, retained program or witness.
// Actual MODEL2 must independently rebuild and fit its real result afterwards.
func BuildSearchSuccessorUpperV3(profile domain.ConfiguredInferenceProfileV1, limits domain.ConfiguredInferenceLimitsV1, policy domain.AgentLoopPolicyV1,
	sourceDigest string, records []sessioncontext.EventPayload, operations []CompletedOperationV1, parent *ParentV1, tools []configuredinference.ToolV1, data ContextDataV3, future domain.AttemptOperationV1) (*configuredinference.FrozenRequestV1, error) {
	if len(operations) == 0 || len(operations) >= 12 || future.Kind != domain.AttemptOperationWebSearchV1 || future.SearchWire == nil || future.Validate() != nil || future.ResultBytes == 0 || future.ResultBytes > 64<<10 {
		return nil, ErrCanonical
	}
	last := operations[len(operations)-1]
	if last.Reservation.Operation.Kind != domain.AttemptOperationModelV1 || future.ValidatePredecessor(last.Reservation, last.Observation) != nil || future.Sequence != uint64(len(operations)+1) || uint64(len(last.Request)) != last.Reservation.Operation.RequestBytes || retainedDigest(last.Request) != last.Reservation.Operation.RequestDigest || uint64(len(last.Response)) != last.Observation.ResponseBytes || retainedDigest(last.Response) != last.Observation.ResponseDigest {
		return nil, ErrCanonical
	}
	decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(profile, last.Request, last.Response, domain.AttemptOperationModelV1)
	if decoded.Call != nil {
		defer clear(decoded.Call.Arguments)
	}
	if err != nil || decoded.Stop != configuredinference.StopToolV1 || decoded.Call == nil || decoded.Call.Name != "web_search" || decoded.Call.ID != future.CallID || retainedDigest(decoded.Call.Arguments) != future.ArgumentsDigest || uint64(len(decoded.Call.Arguments)) != future.SearchWire.ArgumentsBytes || last.Observation.ResponseDigest != future.ModelResponseDigest {
		return nil, ErrCanonical
	}
	usage, err := decoded.Usage.OperationUsage()
	if err != nil || !reflect.DeepEqual(usage, last.Observation.Usage) {
		return nil, ErrCanonical
	}
	route, err := newContextRouteV3(data)
	if err != nil {
		return nil, err
	}
	source, err := buildGenerationSource(profile, limits, policy, sourceDigest, records, operations[:len(operations)-1], parent, tools, data.LegacyHistory, nil, route)
	if err != nil {
		return nil, err
	}
	key, err := OperationSourceKeyV1(last.Reservation, last.Observation)
	if err != nil {
		return nil, err
	}
	call := searchCallPayloadV3(decoded.Call, future)
	defer clear(call)
	upperPayload, err := searchPayloadLengthUpperV3(future)
	if err != nil {
		return nil, err
	}
	source.frames = append(source.frames,
		historicalFrameV1{Kind: "operation", SourceID: key, Role: "tool_call", RunID: string(future.Scope.RunID), Sequence: last.Reservation.Operation.Sequence, Payload: string(call), Digest: last.Observation.ResponseDigest},
		historicalFrameV1{Kind: "operation", SourceID: strings.Repeat("f", 64), Role: "tool_result", RunID: string(future.Scope.RunID), Sequence: future.Sequence, Payload: upperPayload, Digest: strings.Repeat("f", 64)})
	return buildOrdinarySourceV1(source, parent)
}

func searchPayloadLengthUpperV3(future domain.AttemptOperationV1) (string, error) {
	// Minimum fixed typed-result framing uses its ACTUAL domain field tags and
	// identical no-HTML-escaping JSON encoder. The early year and one-digit
	// revision are LENGTH minima only, not retrieval/grant observations. Actual
	// result metadata cannot be shorter, and every retained variable appears at
	// most once in the canonical subset. No result is validated/issued/returned.
	d := strings.Repeat("0", 64)
	minimum := domain.WebSearchResultV1{Version: 1, Scope: future.Scope, OperationDigest: domain.AttemptOperationDigestV1(d), Sequence: future.Sequence, CallID: future.CallID, ProfileRevision: 1, GrantDigest: future.ToolGrantDigest, QueryDigest: future.QueryDigest, WireRequestDigest: future.RequestDigest, RawResponseDigest: d, Provider: "tavily", RetrievedAt: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), Outcome: "known_empty", Usage: domain.WebSearchUsageV1{State: "unknown"}, Sources: []domain.WebSearchSourceV1{}, Warnings: []domain.WebSearchWarningV1{}}
	var encoded bytes.Buffer
	e := json.NewEncoder(&encoded)
	e.SetEscapeHTML(false)
	if e.Encode(minimum) != nil {
		return "", ErrCanonical
	}
	fixed := uint64(len(bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'})))
	if fixed >= future.ResultBytes {
		return "", configuredinference.ErrContextNoFit
	}
	base, err := json.Marshal(struct {
		Schema    string                          `json:"schema"`
		Outcome   string                          `json:"outcome"`
		Operation domain.AttemptOperationDigestV1 `json:"operation_digest"`
		Sources   []domain.WebSearchSourceV1      `json:"sources"`
		Usage     domain.WebSearchUsageV1         `json:"usage"`
		Warnings  []domain.WebSearchWarningV1     `json:"warnings"`
	}{"sessionless.search-result.v4", minimum.Outcome, minimum.OperationDigest, minimum.Sources, minimum.Usage, minimum.Warnings})
	if err != nil {
		return "", ErrCanonical
	}
	// HTML characters may expand from one byte to six in canonical JSON, then
	// quote twice in history/model wire. Literal backslash-u below costs at
	// least as much under BOTH unchanged renderers. This is not JSON content
	// or an executable tool result: only the resulting request length is used.
	return string(base) + strings.Repeat(`\u0000`, int(future.ResultBytes-fixed)), nil
}
