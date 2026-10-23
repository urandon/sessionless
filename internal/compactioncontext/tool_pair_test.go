package compactioncontext_test

import (
	"bytes"
	"encoding/json"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"strings"
	"testing"
	"time"
)

// This uses real normalized codecs on synthetic DATA, not live MCP evidence.
func pairedOperations(t *testing.T) (domain.ConfiguredInferenceProfileV1, domain.AgentLoopPolicyV1, *compactioncontext.RetainedV1, []configuredinference.ToolV1, []compactioncontext.CompletedOperationV1) {
	t.Helper()
	c, p := generationFixture(t)
	descriptor := []byte(`{"inputSchema":{"additionalProperties":false,"properties":{"query":{"maxLength":100,"minLength":1,"type":"string"}},"required":["query"],"type":"object"},"name":"search_openai_docs"}`)
	g := mcpadmission.MCPToolGrantV1{Version: 1, TenantID: c.TenantID, OwnerUserID: c.OwnerUserID, BillingOwnerUserID: c.OwnerUserID, ResourceID: "mcp_tool-resource", ResourceRevision: 1, RevocationGeneration: 1, ProfileID: "mcp_tool-profile", ProfileRevision: 1, DataPolicyDigest: strings.Repeat("a", 64), DataClass: "public", ConsentMode: "owner_public_query", ConsentRevision: 1, Endpoint: "https://developers.openai.com/mcp", Transport: "streamable_http", ProtocolVersion: mcpwire.ProtocolVersionV1, SchemaValidator: domain.ConfiguredToolSchemaValidatorV1, ModelDescription: "Synthetic public lookup", Descriptor: descriptor, IssuedAt: p.IssuedAt, ExpiresAt: c.ExpiresAt, Limits: mcpwire.LimitsV1{MessageBytes: 65536, AggregateBytes: 262144, JSONDepth: 32, JSONMembers: 4096, SSELineBytes: 65536, SSEEventBytes: 65536, SSEEvents: 128, Tools: 1, DescriptorBytes: 32768, ArgumentBytes: 4096, ResultTextBytes: 65536, ContentItems: 16}}
	gd, e := g.Digest()
	if e != nil {
		t.Fatal(e)
	}
	p.MCPToolGrantDigest = gd
	p.AdmissionCostCeiling.MCPToolCost.ScopedGrantDigest = gd
	d, e := mcpwire.ParseDescriptorV1(descriptor, g.Limits)
	if e != nil {
		t.Fatal(e)
	}
	tools := []configuredinference.ToolV1{{Name: d.Name(), Description: g.ModelDescription, Parameters: d.InputSchema()}}
	initial, e := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), generationRecords(t), nil, nil, tools)
	if e != nil {
		t.Fatal(e)
	}
	firstEvidence := retainedEvidence(t, c, p, initial, 1, nil, nil, nil)
	parent, e := initial.BuildRetainedV1(firstEvidence)
	if e != nil {
		t.Fatal(e)
	}
	first := ordinaryCompletedCompaction(t, c, initial, firstEvidence)
	f, e := configuredinference.FreezeRequestV1(c, c.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "lookup documentation"}}, Tools: tools, MaxOutputTokens: 100})
	if e != nil {
		t.Fatal(e)
	}
	model := retainedModel(t, c, first, 2)
	op := model.Reservation.Operation.Clone()
	op.RequestDigest = retainedHash(f.Body())
	op.RequestBytes = uint64(len(f.Body()))
	op.ContextBytes = op.RequestBytes
	op.Debit.BodyBytes = op.RequestBytes + 65536
	wire := []byte(`{"model":"model-local","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"docs-call","type":"function","function":{"name":"search_openai_docs","arguments":"{\"query\":\"docs\"}"}}]},"finish_reason":"tool_calls"}]}`)
	normalized, e := f.NormalizeResponseBodyV1(wire, op.Kind, op.ResultBytes)
	if e != nil {
		t.Fatal(e)
	}
	model = pairedRecord(t, first, op, f.Body(), normalized, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1})
	call, e := mcpadmission.BindConfiguredCallV1(c, g, model.Reservation, model.Observation, model.Request, model.Response)
	if e != nil {
		t.Fatal(e)
	}
	req, e := call.Request()
	if e != nil {
		t.Fatal(e)
	}
	toolOp := op.Clone()
	toolOp.Sequence = 3
	toolOp.Kind = domain.AttemptOperationMCPToolV1
	toolOp.CallID = call.ID()
	toolOp.RequestBytes = uint64(len(req.Body()))
	toolOp.RequestDigest = retainedHash(req.Body())
	toolOp.ContextBytes = 0
	toolOp.SummaryBytes = 0
	toolOp.PredecessorObservationDigest, _ = model.Observation.Digest()
	toolOp.ModelResponseDigest = call.ModelResponseDigest()
	toolOp.ArgumentsDigest = call.ArgumentsDigest()
	toolOp.ToolGrantDigest = gd
	toolOp.ExternalChargeEvidenceDigest = strings.Repeat("a", 64)
	toolOp.Debit = domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, BodyBytes: toolOp.RequestBytes + 65536, ResponseBytes: 65536, ActiveDuration: time.Second, MCPOutboundMessages: 4, MCPCatalogPages: 1, MCPInboundFrames: 3, MCPInboundBytes: 65536}
	toolOp.Deadline = model.Reservation.ReservedAt.Add(time.Second)
	result, e := req.DecodeToolResultV1([]byte(`{"jsonrpc":"2.0","id":"docs-call","result":{"content":[{"type":"text","text":"Documentation answer"}],"isError":false}}`))
	if e != nil {
		t.Fatal(e)
	}
	amount := uint64(0)
	usage := domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: &amount}
	toolBody, e := mcpadmission.EncodeToolResultV1(call, toolOp, result, usage)
	if e != nil {
		t.Fatal(e)
	}
	tool := pairedRecord(t, model, toolOp, req.Body(), toolBody, usage)
	// A final text operation follows the pair, so compaction must cover BOTH
	// paired source IDs and leave this final complete unit as the suffix.
	finalOp := op.Clone()
	finalOp.Sequence = 4
	finalOp.CallID = "model-4"
	finalOp.PredecessorObservationDigest, _ = tool.Observation.Digest()
	finalWire := []byte(`{"model":"model-local","choices":[{"index":0,"message":{"role":"assistant","content":"final answer"},"finish_reason":"stop"}]}`)
	finalBody, e := f.NormalizeResponseBodyV1(finalWire, finalOp.Kind, finalOp.ResultBytes)
	if e != nil {
		t.Fatal(e)
	}
	final := pairedRecord(t, tool, finalOp, f.Body(), finalBody, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1})
	return c, p, parent, tools, []compactioncontext.CompletedOperationV1{first, model, tool, final}
}
func pairedRecord(t *testing.T, previous compactioncontext.CompletedOperationV1, op domain.AttemptOperationV1, request, response []byte, usage domain.AttemptOperationUsageV1) compactioncontext.CompletedOperationV1 {
	t.Helper()
	od, e := op.Digest()
	if e != nil {
		t.Fatal(e)
	}
	r := previous.Reservation.Clone()
	r.Operation = op
	r.OperationDigest = od
	r.AggregateDebit, e = previous.Reservation.AggregateDebit.Add(op.Debit)
	if e != nil {
		t.Fatal("reservation", e)
	}
	if e = r.Validate(); e != nil {
		t.Fatal("reservation", e)
	}
	rd, _ := r.Digest()
	o := previous.Observation.Clone()
	o.OperationDigest = od
	o.ReservationDigest = rd
	o.ResponseBytes = uint64(len(response))
	o.ResponseDigest = retainedHash(response)
	o.Usage = usage.Clone()
	if e = o.ValidateForReservation(r); e != nil {
		t.Fatal(e)
	}
	return compactioncontext.CompletedOperationV1{Reservation: r, Observation: o, Request: bytes.Clone(request), Response: bytes.Clone(response)}
}
func TestToolPairAtomicGenerationAndRetainedRoundtrip(t *testing.T) {
	c, p, parent, tools, ops := pairedOperations(t)
	f, e := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops[:3], parent.Parent(), tools)
	if e != nil {
		t.Fatal("paired continuation", e)
	}
	if !bytes.Contains(f.Body(), []byte("tool_call")) || !bytes.Contains(f.Body(), []byte("Documentation answer")) {
		t.Fatal("pair lost from actual request")
	}
	g, e := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops, parent.Parent(), tools)
	if e != nil {
		t.Fatal("generation", e)
	}
	coverage := map[string]bool{}
	for _, id := range g.CoverageIDs() {
		coverage[id] = true
	}
	for _, item := range ops[1:3] {
		key, e := compactioncontext.OperationSourceKeyV1(item.Reservation, item.Observation)
		if e != nil || !coverage[key] {
			t.Fatal("split/missing paired coverage", e)
		}
	}
	prev := compactioncontext.RetainedEvidenceV1{Reservation: ops[3].Reservation, Observation: ops[3].Observation}
	evidence := retainedEvidence(t, c, p, g, 5, &prev, nil, parent)
	retained, e := g.BuildRetainedV1(evidence)
	if e != nil {
		t.Fatal("retained tool pair", e)
	}
	if _, e = compactioncontext.DecodeRetainedV1(c, p, retained.Plan(), retained.SummaryBody(), retained.ManifestBody(), parent); e != nil {
		t.Fatal("retained reread", e)
	}
	// Re-key an internally consistent candidate split, so this negative reaches
	// pair atomicity rather than merely failing an outer object digest.
	sb, mb := retained.SummaryBody(), retained.ManifestBody()
	var envelope struct {
		Cut json.RawMessage `json:"cut"`
	}
	if json.Unmarshal(mb, &envelope) != nil {
		t.Fatal("cut")
	}
	var cut struct {
		Sources   []json.RawMessage `json:"sources"`
		Cut       uint32            `json:"cut"`
		Protected []uint32          `json:"protected"`
		Covered   []uint32          `json:"covered"`
		Suffix    []uint32          `json:"suffix"`
	}
	if json.Unmarshal(envelope.Cut, &cut) != nil {
		t.Fatal("cut shape")
	}
	original, _ := json.Marshal(cut)
	if !bytes.Equal(original, envelope.Cut) {
		t.Fatal("fixture not byte-exact")
	}
	toolIndex := uint32(len(cut.Sources) - 2)
	cut.Cut = toolIndex
	filtered := cut.Covered[:0]
	for _, index := range cut.Covered {
		if index != toolIndex {
			filtered = append(filtered, index)
		}
	}
	cut.Covered = filtered
	cut.Suffix = append([]uint32{toolIndex}, cut.Suffix...)
	changed, _ := json.Marshal(cut)
	mb = bytes.Replace(mb, original, changed, 1)
	sb, mb = replaceRetainedCore(t, sb, mb, "cut_digest", retainedHash(changed))
	plan, sb, mb := retainedMutated(t, retained, sb, mb)
	if _, e = compactioncontext.DecodeRetainedV1(c, p, plan, sb, mb, parent); e == nil {
		t.Fatal("retained cut split call/result")
	}
}
func TestToolPairIncompleteOrDriftingCannotContinue(t *testing.T) {
	c, p, parent, tools, ops := pairedOperations(t)
	for _, length := range []int{2} {
		if _, e := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops[:length], parent.Parent(), tools); e == nil {
			t.Fatal("pending call admitted")
		}
	}
	for _, which := range []string{"model_response", "args", "grant", "usage", "result"} {
		t.Run(which, func(t *testing.T) {
			copy := append([]compactioncontext.CompletedOperationV1(nil), ops...)
			copy[2].Response = bytes.Clone(copy[2].Response)
			var v map[string]json.RawMessage
			if json.Unmarshal(copy[2].Response, &v) != nil {
				t.Fatal("fixture")
			}
			var old, replacement []byte
			switch which {
			case "model_response":
				old = append([]byte(`"model_response_digest":`), v["model_response_digest"]...)
				replacement = []byte(`"model_response_digest":"` + strings.Repeat("c", 64) + `"`)
			case "args":
				old = []byte(`"arguments":{"query":"docs"}`)
				replacement = []byte(`"arguments":{"query":"other"}`)
			case "grant":
				old = []byte(`"model_description":"Synthetic public lookup"`)
				replacement = []byte(`"model_description":"changed"`)
			case "usage":
				old = []byte(`"usage":{"state":"observed","amount_microunits":0}`)
				replacement = []byte(`"usage":{"state":"unknown"}`)
				copy[2].Observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
			case "result":
				old = []byte(`"isError":false`)
				replacement = []byte(`"isError":true`)
			}
			if bytes.Count(copy[2].Response, old) != 1 {
				t.Fatal("mutation is not causal")
			}
			copy[2].Response = bytes.Replace(copy[2].Response, old, replacement, 1)
			copy[2].Observation.ResponseDigest = retainedHash(copy[2].Response)
			copy[2].Observation.ResponseBytes = uint64(len(copy[2].Response))
			if _, e := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), copy[:3], parent.Parent(), tools); e == nil {
				t.Fatal("drifting tool DATA admitted")
			}
		})
	}
}

func TestToolPairRetainedHistoricalFramesRemainData(t *testing.T) {
	c, p, parent, tools, ops := pairedOperations(t)
	oldScope := ops[0].Reservation.Operation.Scope
	anchor := assistantRecord(t, 11, string(oldScope.RunID))
	records := append(generationRecords(t), anchor, userRecord(t, 12))
	p.RunID, p.AttemptID = "next-run", "next-attempt"
	history := compactioncontext.HistoricalRunV1{Scope: oldScope, Anchor: anchor.Event.ID}
	for i, item := range ops {
		projection := "retained_only"
		if i == 0 {
			projection = "compaction"
		}
		if i == len(ops)-1 {
			projection = "assistant"
		}
		history.Operations = append(history.Operations, compactioncontext.HistoricalOperationV1{Reservation: item.Reservation, Observation: item.Observation, Normalized: bytes.Clone(item.Response), Projection: projection})
	}
	build := func(h compactioncontext.HistoricalRunV1) (*configuredinference.FrozenRequestV1, error) {
		return compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("2", 64), []sessioncontext.EventPayload(records), nil, parent.Parent(), tools, []compactioncontext.HistoricalRunV1{h})
	}
	f, e := build(history)
	if e != nil {
		t.Fatal("sealed historical pair", e)
	}
	if !bytes.Contains(f.Body(), []byte("Documentation answer")) {
		t.Fatal("historical result dropped")
	}
	broken := history
	broken.Operations = append([]compactioncontext.HistoricalOperationV1(nil), history.Operations...)
	broken.Operations[2].Projection = "assistant"
	if _, e = build(broken); e == nil {
		t.Fatal("tool result projected as final assistant")
	}
	broken = history
	broken.Operations = append([]compactioncontext.HistoricalOperationV1(nil), history.Operations[:2]...)
	if _, e = build(broken); e == nil {
		t.Fatal("historical incomplete tool call accepted")
	}
	broken = history
	broken.Operations = append([]compactioncontext.HistoricalOperationV1(nil), history.Operations[:3]...)
	if _, e = build(broken); e == nil {
		t.Fatal("history cannot end at MCP instead of canonical final MODEL")
	}
}
