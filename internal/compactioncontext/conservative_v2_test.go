package compactioncontext_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// Real codecs over synthetic DATA only. Neither these values nor context
// construction attest Source/Store authority, a tariff, actual money or sends.
func conservativeFixtureV2(t *testing.T) (domain.ConfiguredInferenceProfileV1, domain.AgentLoopPolicyV1, *compactioncontext.RetainedV1, []configuredinference.ToolV1, []compactioncontext.CompletedOperationV1, compactioncontext.ContextDataV2) {
	t.Helper()
	c, p, parent, tools, ops := pairedOperations(t)
	old := []byte(`"usage":{"state":"observed","amount_microunits":0}`)
	if bytes.Count(ops[2].Response, old) != 1 {
		t.Fatal("synthetic observed amount mutation not causal")
	}
	ops[2].Response = bytes.Replace(ops[2].Response, old, []byte(`"usage":{"state":"unknown"}`), 1)
	ops[2].Observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
	ops[2].Observation.ResponseDigest = retainedHash(ops[2].Response)
	ops[2].Observation.ResponseBytes = uint64(len(ops[2].Response))
	last := ops[3].Reservation.Operation.Clone()
	last.PredecessorObservationDigest, _ = ops[2].Observation.Digest()
	ops[3] = pairedRecord(t, ops[2], last, ops[3].Request, ops[3].Response, ops[3].Observation.Usage)
	data := compactioncontext.ContextDataV2{Version: 2, ConservativeMCP: []compactioncontext.MCPConservativeDataV2{conservativeRowV2(t, ops[2])}}
	return c, p, parent, tools, ops, data
}

func conservativeRowV2(t *testing.T, item compactioncontext.CompletedOperationV1) compactioncontext.MCPConservativeDataV2 {
	t.Helper()
	rd, re := item.Reservation.Digest()
	od, oe := item.Observation.Digest()
	if re != nil || oe != nil {
		t.Fatal("valid exact conservative synthetic digests", re, oe)
	}
	return compactioncontext.MCPConservativeDataV2{Version: 2, Scope: item.Reservation.Operation.Scope, Sequence: item.Reservation.Operation.Sequence, ReservationDigest: rd, ObservationDigest: od, Reserved: item.Reservation.Operation.Debit, MCPSpendingDigest: strings.Repeat("a", 64)}
}

func TestConservativeContextV2ExplicitUnknownPairRemainsData(t *testing.T) {
	c, p, parent, tools, ops, data := conservativeFixtureV2(t)
	if _, err := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops[:3], parent.Parent(), tools); err == nil {
		t.Fatal("V1 measured gate accepted unknown MCP")
	}
	f, err := compactioncontext.BuildOrdinaryV2(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops[:3], parent.Parent(), tools, data)
	if err != nil {
		t.Fatal("explicit original conservative DATA route rejected", err)
	}
	_, history := generationWire(t, f)
	units := history["original_units"].([]any)
	call, result := units[len(units)-2].(map[string]any), units[len(units)-1].(map[string]any)
	if call["original_role"] != "tool_call" || result["original_role"] != "tool_result" || !strings.Contains(result["original_payload"].(string), "Documentation answer") {
		t.Fatal("exact pair missing from actual next request", call["original_role"], result["original_role"])
	}
	if ops[2].Observation.Usage.State != domain.AttemptOperationUsageUnknownV1 || ops[2].Observation.Usage.AmountMicrounits != nil || data.ConservativeMCP[0].Reserved != ops[2].Reservation.Operation.Debit {
		t.Error("context route relabelled unknown money or repriced original reservation")
	}
}

func TestConservativeContextV2PositiveFiniteFullReservationIsNotMeasuredMoney(t *testing.T) {
	c, p, _, tools, old, _ := conservativeFixtureV2(t)
	amount := uint64(5)
	p.AdmissionCostCeiling.MCPToolCost.MaxAmountMicrounits = &amount
	p.AdmissionCostCeiling.MCPToolCost.PriceState = domain.ExternalServicePriceKnownPaidV1
	total := *p.AdmissionCostCeiling.MaxTotalAmountMicrounits + amount
	p.AdmissionCostCeiling.MaxTotalAmountMicrounits = &total
	if err := p.Validate(); err != nil {
		t.Fatal("finite synthetic policy", err)
	}
	initial, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), generationRecords(t), nil, nil, tools)
	if err != nil {
		t.Fatal(err)
	}
	e := retainedEvidence(t, c, p, initial, 1, nil, nil, nil)
	parent, err := initial.BuildRetainedV1(e)
	if err != nil {
		t.Fatal(err)
	}
	first := ordinaryCompletedCompaction(t, c, initial, e)
	ops := []compactioncontext.CompletedOperationV1{first}
	cost, _ := p.AdmissionCostCeiling.Digest()
	for i := 1; i < len(old); i++ {
		op := old[i].Reservation.Operation.Clone()
		op.Scope = first.Reservation.Operation.Scope
		op.AdmissionCostCeilingDigest = cost
		op.PredecessorObservationDigest, _ = ops[i-1].Observation.Digest()
		body := bytes.Clone(old[i].Response)
		if op.Kind == domain.AttemptOperationMCPToolV1 {
			op.Debit.AmountMicrounits = amount
			digest, err := op.Digest()
			if err != nil {
				t.Fatal(err)
			}
			oldDigest := []byte(`"operation_digest":"` + string(old[i].Reservation.OperationDigest) + `"`)
			if bytes.Count(body, oldDigest) != 1 {
				t.Fatal("exact normalized operation digest replacement is not causal")
			}
			body = bytes.Replace(body, oldDigest, []byte(`"operation_digest":"`+string(digest)+`"`), 1)
		}
		previous := ops[i-1]
		// pairedRecord's existing zero-money fixture validates its new row
		// immediately. Feed its cloned carry-forward component subtotal, not
		// a changed original predecessor or a claimed observed amount.
		if op.Kind == domain.AttemptOperationMCPToolV1 {
			previous.Reservation = previous.Reservation.Clone()
			previous.Reservation.ComponentAmounts[2] += amount
		}
		ops = append(ops, pairedRecord(t, previous, op, old[i].Request, body, old[i].Observation.Usage))
	}
	data := compactioncontext.ContextDataV2{Version: 2, ConservativeMCP: []compactioncontext.MCPConservativeDataV2{conservativeRowV2(t, ops[2])}}
	if _, err := compactioncontext.BuildOrdinaryV2(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops[:3], parent.Parent(), tools, data); err != nil {
		t.Fatal("positive finite fully reserved original DATA continuation", err)
	}
	if data.ConservativeMCP[0].Reserved.AmountMicrounits != amount || ops[2].Observation.Usage.State != domain.AttemptOperationUsageUnknownV1 || ops[2].Observation.Usage.AmountMicrounits != nil || ops[3].Reservation.AggregateDebit.AmountMicrounits != amount {
		t.Fatal("finite bound was reported as measured money/refunded instead of retained whole")
	}
	if _, err := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops[:3], parent.Parent(), tools); err == nil {
		t.Error("finite full reservation weakened V1 measured-money predicate")
	}
	data.ConservativeMCP[0].Reserved.AmountMicrounits--
	if _, err := compactioncontext.BuildOrdinaryV2(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops[:3], parent.Parent(), tools, data); err == nil {
		t.Error("discounted finite reservation accepted as original full component")
	}
}

func TestConservativeContextV2RejectsMissingDriftingOrDuplicateData(t *testing.T) {
	for _, which := range []string{"version", "missing", "duplicate", "scope", "sequence", "reservation", "observation", "reserved", "spending", "unused", "policy_ceiling", "incomplete_pair", "malformed_result", "service_error", "accepted_unknown"} {
		t.Run(which, func(t *testing.T) {
			c, p, parent, tools, ops, data := conservativeFixtureV2(t)
			switch which {
			case "version":
				data.Version = 1
			case "missing":
				data.ConservativeMCP = nil
			case "duplicate":
				data.ConservativeMCP = append(data.ConservativeMCP, data.ConservativeMCP[0])
			case "scope":
				data.ConservativeMCP[0].Scope.FenceToken++
			case "sequence":
				data.ConservativeMCP[0].Sequence++
			case "reservation":
				data.ConservativeMCP[0].ReservationDigest = domain.AttemptOperationReservationDigestV1(strings.Repeat("b", 64))
			case "observation":
				data.ConservativeMCP[0].ObservationDigest = domain.AttemptOperationObservationDigestV1(strings.Repeat("b", 64))
			case "reserved":
				data.ConservativeMCP[0].Reserved.AmountMicrounits++
			case "spending":
				data.ConservativeMCP[0].MCPSpendingDigest = "not-a-digest"
			case "unused":
				extra := data.ConservativeMCP[0]
				extra.Sequence = 10
				data.ConservativeMCP = append(data.ConservativeMCP, extra)
			case "policy_ceiling":
				p.AdmissionCostCeiling.MCPToolCost.MaxAmountMicrounits = new(uint64(1))
			case "incomplete_pair":
				ops = ops[:2]
			case "malformed_result":
				ops[2].Response[0] ^= 1
			case "service_error":
				ops[2].Observation.Reason = domain.AttemptOperationReasonServiceErrorV1
				data.ConservativeMCP[0] = conservativeRowV2(t, ops[2])
			case "accepted_unknown":
				ops[2].Observation.Outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
			}
			if len(ops) > 3 {
				ops = ops[:3]
			}
			if _, err := compactioncontext.BuildOrdinaryV2(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops, parent.Parent(), tools, data); err == nil {
				t.Fatalf("drifting explicit conservative DATA accepted: %s", which)
			}
		})
	}
}

func TestConservativeRetainedV2RoundtripAndV1Separation(t *testing.T) {
	c, p, parent, tools, ops, data := conservativeFixtureV2(t)
	g, err := compactioncontext.BuildGenerationV2(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops, parent.Parent(), tools, data)
	if err != nil {
		t.Fatal("V2 compaction source", err)
	}
	previous := compactioncontext.RetainedEvidenceV1{Reservation: ops[3].Reservation, Observation: ops[3].Observation}
	e := retainedEvidence(t, c, p, g, 5, &previous, nil, parent)
	if _, err := g.BuildRetainedV1(e); err == nil {
		t.Fatal("V2 source silently emitted V1 retained grammar")
	}
	v, err := g.BuildRetainedV2(e)
	if err != nil {
		t.Fatal("V2 exact retained generation", err)
	}
	if g.RetainedCodecVersion() != 2 || v.CodecVersion() != 2 || parent.CodecVersion() != 1 {
		t.Error("opaque version inspectors do not select exact DATA codec")
	}
	if !bytes.Contains(v.ManifestBody(), []byte(`"conservative":{"version":2`)) || !bytes.Contains(v.ManifestBody(), []byte(`"state":"unknown"`)) || !bytes.Contains(v.ManifestBody(), []byte(`"schema":"sessionless.compaction-manifest.v2"`)) {
		t.Fatal("V2 manifest lost explicit unknown/full-debit DATA variant")
	}
	decoded, err := compactioncontext.DecodeRetainedV2(c, p, v.Plan(), v.SummaryBody(), v.ManifestBody(), parent)
	if err != nil || decoded.ValidateForGeneration(g) != nil {
		t.Fatal("V2 reread fails exact source binding", err)
	}
	pd, _ := c.Digest()
	if _, err := compactioncontext.DecodeSealedRetainedV2(pd, p, v.Plan(), v.SummaryBody(), v.ManifestBody(), parent); err != nil {
		t.Fatal("retired-profile V2 sealed DATA read", err)
	}
	if _, err := compactioncontext.DecodeRetainedV1(c, p, v.Plan(), v.SummaryBody(), v.ManifestBody(), parent); err == nil {
		t.Error("V1 decoder reinterpreted V2 artifact")
	}
	if _, err := compactioncontext.DecodeSealedRetainedV1(pd, p, v.Plan(), v.SummaryBody(), v.ManifestBody(), parent); err == nil {
		t.Error("V1 sealed decoder reinterpreted V2 artifact")
	}
	if _, err := compactioncontext.DecodeRetainedV2(c, p, parent.Plan(), parent.SummaryBody(), parent.ManifestBody(), nil); err == nil {
		t.Error("V2 decoder silently fell back to V1")
	}
}

func TestRetainedCodecVersionInspectorsRejectNilAndZero(t *testing.T) {
	var generation *compactioncontext.GenerationV1
	var retained *compactioncontext.RetainedV1
	if generation.RetainedCodecVersion() != 0 || retained.CodecVersion() != 0 {
		t.Error("nil values claimed a valid DATA codec")
	}
	generation = &compactioncontext.GenerationV1{}
	retained = &compactioncontext.RetainedV1{}
	if generation.RetainedCodecVersion() != 0 || retained.CodecVersion() != 0 {
		t.Error("zero/unbuilt opaque values claimed a valid DATA codec")
	}
	_, _, built, _, value := retainedFixture(t)
	if built.RetainedCodecVersion() != 1 || value.CodecVersion() != 1 {
		t.Error("original V1 producer/reader codec changed")
	}
}

func historicalConservativeFixtureV2(t *testing.T) (domain.ConfiguredInferenceProfileV1, domain.AgentLoopPolicyV1, *compactioncontext.RetainedV1, []configuredinference.ToolV1, []sessioncontext.EventPayload, compactioncontext.ContextDataV2) {
	t.Helper()
	c, p, parent, tools, ops, current := conservativeFixtureV2(t)
	scope := ops[0].Reservation.Operation.Scope
	history := compactioncontext.HistoricalRunV2{Scope: scope, ConservativeMCP: current.ConservativeMCP}
	for i, item := range ops {
		projection := "retained_only"
		switch i {
		case 0:
			projection = "compaction"
		case 1:
			projection = "tool_call"
		case 2:
			projection = "tool_result"
		case 3:
			projection = "assistant"
		}
		history.Operations = append(history.Operations, compactioncontext.HistoricalOperationV1{Reservation: item.Reservation, Observation: item.Observation, Normalized: item.Response, Projection: projection})
	}
	encoded, err := canonicaloutput.EncodeCompletionV1(scope.RunID, domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 16 << 10}, ports.ExecutionResult{Summary: "final answer", ToolEvents: []ports.ExecutionToolEvent{
		{Kind: domain.SessionEventToolCall, CallID: "docs-call", ToolName: "search_openai_docs", Payload: []byte(`{"call_id":"docs-call","name":"search_openai_docs","arguments":{"query":"docs"}}`)},
		{Kind: domain.SessionEventToolResult, CallID: "docs-call", ToolName: "search_openai_docs", Payload: []byte(`{"call_id":"docs-call","is_error":false,"content":["Documentation answer"]}`)},
	}}, canonicaloutput.ManifestIDV1(scope.RunID))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Release()
	records := generationRecords(t)
	for i, event := range encoded.Events() {
		runID := scope.RunID
		id := event.ID
		body := bytes.Clone(event.Payload)
		r := sessioncontext.EventPayload{Payload: body, Event: domain.SessionEvent{ID: id, TenantID: scope.TenantID, SessionID: scope.SessionID, Sequence: uint64(len(records) + 1), Kind: event.Kind, RunID: &runID, IdempotencyKey: domain.IdempotencyKey("canonical-" + string(id)), CreatedAt: records[len(records)-1].Event.CreatedAt.Add(1_000_000)}}
		r.Event.Payload = domain.BlobRef{TenantID: scope.TenantID, Key: domain.SessionEventObjectPrefix(scope.TenantID, scope.SessionID, id) + "payloads/sha256/" + retainedHash(body) + ".json", SHA256: retainedHash(body), Size: int64(len(body))}
		if err := r.Event.Validate(); err != nil {
			t.Fatal(err)
		}
		history.Projections = append(history.Projections, compactioncontext.CanonicalProjectionV2{OperationSequence: uint64(i + 2), EventID: id, Kind: event.Kind, Payload: r.Event.Payload})
		records = append(records, r)
	}
	history.Anchor = records[len(records)-1].Event.ID
	records = append(records, userRecord(t, len(records)+1))
	p.RunID, p.AttemptID = "next-run", "next-attempt"
	return c, p, parent, tools, records, compactioncontext.ContextDataV2{Version: 2, Historical: []compactioncontext.HistoricalRunV2{history}}
}

func TestHistoricalConservativeV2CanonicalPairsOnceAndRetainedProvenance(t *testing.T) {
	c, p, parent, tools, records, data := historicalConservativeFixtureV2(t)
	f, err := compactioncontext.BuildOrdinaryV2(c, c.Limits, p, strings.Repeat("2", 64), records, nil, parent.Parent(), tools, data)
	if err != nil {
		t.Fatal("V2 historical source", err)
	}
	_, history := generationWire(t, f)
	counts := make(map[string]int)
	for _, raw := range history["original_units"].([]any) {
		frame := raw.(map[string]any)
		if frame["run_id"] == string(data.Historical[0].Scope.RunID) {
			counts[frame["original_role"].(string)]++
			if frame["event_id"] == nil {
				t.Error("projected pair frame lost its exact canonical identity")
			}
		}
	}
	if counts["tool_call"] != 1 || counts["tool_result"] != 1 || counts["assistant_message"] != 1 {
		t.Fatalf("historical pair/answer must occur exactly once: counts=%v", counts)
	}
	g, err := compactioncontext.BuildGenerationV2(c, c.Limits, p, strings.Repeat("3", 64), records, nil, parent.Parent(), tools, data)
	if err != nil {
		t.Fatal("V2 historical compaction", err)
	}
	e := retainedEvidence(t, c, p, g, 1, nil, nil, parent)
	v, err := g.BuildRetainedV2(e)
	if err != nil {
		t.Fatal("V2 historical retained correspondence", err)
	}
	if bytes.Count(v.ManifestBody(), []byte(`"projection":{"reservation":`)) != 3 || !bytes.Contains(v.ManifestBody(), []byte(`"mcp_spending_digest":`)) {
		t.Fatal("dedup dropped original pair/answer operation provenance")
	}
	if _, err := compactioncontext.DecodeRetainedV2(c, p, v.Plan(), v.SummaryBody(), v.ManifestBody(), parent); err != nil {
		t.Fatal("V2 projected provenance reread", err)
	}
	if events, _ := v.DependenciesV1(); len(events) != 0 {
		t.Error("V1 dependency entrypoint interpreted V2 grammar")
	}
	dependencies, _ := v.DependenciesV2()
	if len(dependencies) != len(records) {
		t.Errorf("V2 retention lost exact canonical dependencies: got=%d want=%d", len(dependencies), len(records))
	}
	if _, err := compactioncontext.BuildOrdinaryV2(c, c.Limits, p, strings.Repeat("4", 64), records, nil, v.Parent(), tools, data); err == nil {
		// Its original compaction operation is not present in this fixture's
		// current operation history, so parent-closure matching must deny.
		t.Error("new parent restored without its exact compaction lineage")
	}
	completed := ordinaryCompletedCompaction(t, c, g, e)
	refreshed, err := compactioncontext.BuildOrdinaryV2(c, c.Limits, p, strings.Repeat("4", 64), records, []compactioncontext.CompletedOperationV1{completed}, v.Parent(), tools, data)
	if err != nil {
		t.Fatal("V2 next-turn/refresh with exact derived closure", err)
	}
	_, refreshedHistory := generationWire(t, refreshed)
	if refreshedHistory["derived_summary"] == "" {
		t.Error("refresh discarded actual validated derived summary")
	}
	for _, raw := range refreshedHistory["original_units"].([]any) {
		frame := raw.(map[string]any)
		if frame["run_id"] == string(data.Historical[0].Scope.RunID) {
			t.Error("already covered canonical pair appeared again beside its summary")
		}
	}
}

func TestHistoricalConservativeV2RejectsMappingDrift(t *testing.T) {
	for _, which := range []string{"missing_mapping", "duplicate_mapping", "wrong_kind", "wrong_event", "wrong_hash", "wrong_size", "wrong_scope", "reordered_mapping", "reordered_canonical", "missing_canonical", "tampered_content", "duplicate_run", "missing_conservative", "duplicate_conservative", "projection_relabel"} {
		t.Run(which, func(t *testing.T) {
			c, p, parent, tools, records, data := historicalConservativeFixtureV2(t)
			h := &data.Historical[0]
			switch which {
			case "missing_mapping":
				h.Projections = h.Projections[1:]
			case "duplicate_mapping":
				h.Projections[1] = h.Projections[0]
			case "wrong_kind":
				h.Projections[0].Kind = domain.SessionEventToolResult
			case "wrong_event":
				h.Projections[0].EventID = "different-event"
			case "wrong_hash":
				h.Projections[1].Payload.SHA256 = strings.Repeat("b", 64)
			case "wrong_size":
				h.Projections[1].Payload.Size++
			case "wrong_scope":
				h.Scope.FenceToken++
			case "reordered_mapping":
				h.Projections[0], h.Projections[1] = h.Projections[1], h.Projections[0]
			case "reordered_canonical":
				records[10], records[11] = records[11], records[10]
				records[10].Event.Sequence, records[11].Event.Sequence = 11, 12
			case "missing_canonical":
				records = append(records[:11], records[12:]...)
				for i := range records {
					records[i].Event.Sequence = uint64(i + 1)
				}
			case "tampered_content":
				replacePayload(&records[11], strings.Replace(string(records[11].Payload), "Documentation answer", "Different answer", 1))
				h.Projections[1].Payload = records[11].Event.Payload
			case "duplicate_run":
				data.Historical = append(data.Historical, *h)
			case "missing_conservative":
				h.ConservativeMCP = nil
			case "duplicate_conservative":
				h.ConservativeMCP = append(h.ConservativeMCP, h.ConservativeMCP[0])
			case "projection_relabel":
				h.Operations[2].Projection = "retained_only"
			}
			if _, err := compactioncontext.BuildOrdinaryV2(c, c.Limits, p, strings.Repeat("2", 64), records, nil, parent.Parent(), tools, data); err == nil {
				t.Fatalf("invalid historical V2 mapping accepted: %s", which)
			}
		})
	}
}

type conservativeRetainedOperationTestV2 struct {
	Reservation  domain.AttemptOperationReservationV1     `json:"reservation"`
	Observation  domain.AttemptOperationObservationV1     `json:"observation"`
	Conservative *compactioncontext.MCPConservativeDataV2 `json:"conservative,omitempty"`
}

type conservativeRetainedSourceTestV2 struct {
	SourceID   string                               `json:"source_id"`
	Canonical  *domain.SessionEvent                 `json:"canonical"`
	Operation  *conservativeRetainedOperationTestV2 `json:"operation"`
	Projection *conservativeRetainedOperationTestV2 `json:"projection,omitempty"`
}

type conservativeRetainedCutTestV2 struct {
	Sources   []conservativeRetainedSourceTestV2 `json:"sources"`
	Cut       uint32                             `json:"cut"`
	Protected []uint32                           `json:"protected"`
	Covered   []uint32                           `json:"covered"`
	Suffix    []uint32                           `json:"suffix"`
}

func mutateConservativeCutV2(t *testing.T, v *compactioncontext.RetainedV1, mutate func(*conservativeRetainedCutTestV2)) (domain.DerivedContextPlanV1, []byte, []byte) {
	t.Helper()
	sb, mb := v.SummaryBody(), v.ManifestBody()
	var envelope struct {
		Cut json.RawMessage `json:"cut"`
	}
	if json.Unmarshal(mb, &envelope) != nil {
		t.Fatal("decode exact V2 cut")
	}
	var cut conservativeRetainedCutTestV2
	if json.Unmarshal(envelope.Cut, &cut) != nil {
		t.Fatal("decode exact V2 cut shape")
	}
	original, err := json.Marshal(cut)
	if err != nil || !bytes.Equal(original, envelope.Cut) {
		t.Fatal("V2 mutation helper must preserve exact canonical private wire order", err)
	}
	mutate(&cut)
	updated, err := json.Marshal(cut)
	if err != nil || bytes.Equal(original, updated) {
		t.Fatal("V2 mutation is not causal", err)
	}
	mb = bytes.Replace(mb, original, updated, 1)
	sb, mb = replaceRetainedCore(t, sb, mb, "cut_digest", retainedHash(updated))
	return retainedMutated(t, v, sb, mb)
}

func TestConservativeRetainedV2RehashedMetadataStillRequiresExactPair(t *testing.T) {
	c, p, parent, tools, ops, data := conservativeFixtureV2(t)
	g, err := compactioncontext.BuildGenerationV2(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), ops, parent.Parent(), tools, data)
	if err != nil {
		t.Fatal(err)
	}
	previous := compactioncontext.RetainedEvidenceV1{Reservation: ops[3].Reservation, Observation: ops[3].Observation}
	e := retainedEvidence(t, c, p, g, 5, &previous, nil, parent)
	v, err := g.BuildRetainedV2(e)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"missing_conservative", "wrong_reservation", "wrong_observation", "changed_reserved", "bad_spending", "split_pair"} {
		t.Run(mode, func(t *testing.T) {
			plan, sb, mb := mutateConservativeCutV2(t, v, func(cut *conservativeRetainedCutTestV2) {
				for i := range cut.Sources {
					row := cut.Sources[i].Operation
					if row == nil || row.Conservative == nil {
						continue
					}
					switch mode {
					case "missing_conservative":
						row.Conservative = nil
					case "wrong_reservation":
						row.Conservative.ReservationDigest = domain.AttemptOperationReservationDigestV1(strings.Repeat("b", 64))
					case "wrong_observation":
						row.Conservative.ObservationDigest = domain.AttemptOperationObservationDigestV1(strings.Repeat("b", 64))
					case "changed_reserved":
						row.Conservative.Reserved.AmountMicrounits++
					case "bad_spending":
						row.Conservative.MCPSpendingDigest = "invalid"
					case "split_pair":
						cut.Cut = uint32(i)
						var covered []uint32
						for _, index := range cut.Covered {
							if index < uint32(i) {
								covered = append(covered, index)
							}
						}
						cut.Covered = covered
						cut.Suffix = append([]uint32{uint32(i)}, cut.Suffix...)
					}
					return
				}
			})
			if _, err := compactioncontext.DecodeRetainedV2(c, p, plan, sb, mb, parent); err == nil {
				t.Fatalf("rehashed malformed V2 operation metadata accepted: %s", mode)
			}
		})
	}
}

func TestHistoricalRetainedV2RehashedProjectionCannotDropCorrespondence(t *testing.T) {
	c, p, parent, tools, records, data := historicalConservativeFixtureV2(t)
	g, err := compactioncontext.BuildGenerationV2(c, c.Limits, p, strings.Repeat("3", 64), records, nil, parent.Parent(), tools, data)
	if err != nil {
		t.Fatal(err)
	}
	e := retainedEvidence(t, c, p, g, 1, nil, nil, parent)
	v, err := g.BuildRetainedV2(e)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"missing_assistant_projection", "missing_result_conservative", "changed_full_reservation", "different_original_scope"} {
		t.Run(mode, func(t *testing.T) {
			plan, sb, mb := mutateConservativeCutV2(t, v, func(cut *conservativeRetainedCutTestV2) {
				for i := range cut.Sources {
					row := &cut.Sources[i]
					if row.Projection == nil {
						continue
					}
					if mode == "missing_assistant_projection" && row.Canonical.Kind == domain.SessionEventAssistantMessage {
						row.Projection = nil
						return
					}
					if row.Canonical.Kind != domain.SessionEventToolResult {
						continue
					}
					switch mode {
					case "missing_result_conservative":
						row.Projection.Conservative = nil
					case "changed_full_reservation":
						row.Projection.Conservative.Reserved.AmountMicrounits++
					case "different_original_scope":
						row.Projection.Conservative.Scope.FenceToken++
					}
					if mode != "missing_assistant_projection" {
						return
					}
				}
			})
			if _, err := compactioncontext.DecodeRetainedV2(c, p, plan, sb, mb, parent); err == nil {
				t.Fatalf("rehashed incomplete canonical/operation projection accepted: %s", mode)
			}
		})
	}
}

func TestConservativeV2CoveredOperationSourcesSurviveCanonicalProjection(t *testing.T) {
	c, p, _, tools, original, _ := conservativeFixtureV2(t)
	// A root MODEL/tool/final chain has no initial compaction. Rebase exact
	// synthetic reservations/observations and only the MCP operation-digest
	// field; provider request/response/call/grant content stays unchanged.
	var operations []compactioncontext.CompletedOperationV1
	var aggregate domain.AttemptOperationDebitV1
	for i, item := range original[1:] {
		op := item.Reservation.Operation.Clone()
		op.Sequence = uint64(i + 1)
		op.PredecessorObservationDigest = ""
		if i > 0 {
			op.PredecessorObservationDigest, _ = operations[i-1].Observation.Digest()
		}
		digest, err := op.Digest()
		if err != nil {
			t.Fatal(err)
		}
		response := bytes.Clone(item.Response)
		if op.Kind == domain.AttemptOperationMCPToolV1 {
			old := []byte(`"operation_digest":"` + string(item.Reservation.OperationDigest) + `"`)
			if bytes.Count(response, old) != 1 {
				t.Fatal("MCP rebase mutation not causal")
			}
			response = bytes.Replace(response, old, []byte(`"operation_digest":"`+string(digest)+`"`), 1)
		}
		r := item.Reservation.Clone()
		r.Operation, r.OperationDigest = op, digest
		aggregate, err = aggregate.Add(op.Debit)
		if err != nil {
			t.Fatal(err)
		}
		r.AggregateDebit = aggregate
		rd, err := r.Digest()
		if err != nil {
			t.Fatal(err)
		}
		o := item.Observation.Clone()
		o.OperationDigest, o.ReservationDigest = digest, rd
		o.ResponseDigest, o.ResponseBytes = retainedHash(response), uint64(len(response))
		if err := o.ValidateForReservation(r); err != nil {
			t.Fatal(err)
		}
		operations = append(operations, compactioncontext.CompletedOperationV1{Reservation: r, Observation: o, Request: bytes.Clone(item.Request), Response: response})
	}
	current := compactioncontext.ContextDataV2{Version: 2, ConservativeMCP: []compactioncontext.MCPConservativeDataV2{conservativeRowV2(t, operations[1])}}
	root, err := compactioncontext.BuildGenerationV2(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), operations, nil, tools, current)
	if err != nil {
		t.Fatal("root generation", err)
	}
	previous := compactioncontext.RetainedEvidenceV1{Reservation: operations[2].Reservation, Observation: operations[2].Observation}
	e := retainedEvidence(t, c, p, root, 4, &previous, nil, nil)
	retained, err := root.BuildRetainedV2(e)
	if err != nil {
		t.Fatal("root retained", err)
	}
	for _, item := range operations[:2] {
		key, err := compactioncontext.OperationSourceKeyV1(item.Reservation, item.Observation)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, covered := range retained.Parent().CoveredSourceIDs {
			if covered == key {
				found = true
			}
		}
		if !found {
			t.Fatal("fixture must actually compact both original operation source IDs")
		}
	}
	compacted := ordinaryCompletedCompaction(t, c, root, e)
	operations = append(operations, compacted)
	operations = append(operations, retainedModel(t, c, compacted, 5))
	scope := operations[0].Reservation.Operation.Scope
	h := compactioncontext.HistoricalRunV2{Scope: scope, ConservativeMCP: current.ConservativeMCP}
	for i, item := range operations {
		projection := "retained_only"
		switch i {
		case 0:
			projection = "tool_call"
		case 1:
			projection = "tool_result"
		case 3:
			projection = "compaction"
		case 4:
			projection = "assistant"
		}
		h.Operations = append(h.Operations, compactioncontext.HistoricalOperationV1{Reservation: item.Reservation, Observation: item.Observation, Normalized: item.Response, Projection: projection})
	}
	encoded, err := canonicaloutput.EncodeCompletionV1(scope.RunID, domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 16 << 10}, ports.ExecutionResult{Summary: "completed model 5", ToolEvents: []ports.ExecutionToolEvent{
		{Kind: domain.SessionEventToolCall, CallID: "docs-call", ToolName: "search_openai_docs", Payload: []byte(`{"call_id":"docs-call","name":"search_openai_docs","arguments":{"query":"docs"}}`)},
		{Kind: domain.SessionEventToolResult, CallID: "docs-call", ToolName: "search_openai_docs", Payload: []byte(`{"call_id":"docs-call","is_error":false,"content":["Documentation answer"]}`)},
	}}, canonicaloutput.ManifestIDV1(scope.RunID))
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Release()
	records := generationRecords(t)
	sequences := []uint64{1, 2, 5}
	for i, event := range encoded.Events() {
		r := canonicalRecord(t, len(records)+1, event.Kind, string(scope.RunID), string(event.Payload))
		r.Event.ID = event.ID
		r.Event.Payload.Key = domain.SessionEventObjectPrefix(scope.TenantID, scope.SessionID, event.ID) + "payloads/sha256/" + r.Event.Payload.SHA256 + ".json"
		h.Projections = append(h.Projections, compactioncontext.CanonicalProjectionV2{OperationSequence: sequences[i], EventID: event.ID, Kind: event.Kind, Payload: r.Event.Payload})
		records = append(records, r)
	}
	h.Anchor = records[len(records)-1].Event.ID
	records = append(records, userRecord(t, len(records)+1))
	p.RunID, p.AttemptID = "refresh-run", "refresh-attempt"
	data := compactioncontext.ContextDataV2{Version: 2, Historical: []compactioncontext.HistoricalRunV2{h}}
	request, err := compactioncontext.BuildOrdinaryV2(c, c.Limits, p, strings.Repeat("3", 64), records, nil, retained.Parent(), tools, data)
	if err != nil {
		t.Fatal("covered operation citations lost across canonical projection", err)
	}
	_, rendered := generationWire(t, request)
	for _, raw := range rendered["original_units"].([]any) {
		frame := raw.(map[string]any)
		if frame["run_id"] == string(scope.RunID) && (frame["original_role"] == "tool_call" || frame["original_role"] == "tool_result") {
			t.Error("covered pair rendered twice beside its retained summary")
		}
	}
	next, err := compactioncontext.BuildGenerationV2(c, c.Limits, p, strings.Repeat("4", 64), records, nil, retained.Parent(), tools, data)
	if err != nil {
		t.Fatal("refreshed V2 next generation", err)
	}
	nextEvidence := retainedEvidence(t, c, p, next, 1, nil, nil, retained)
	child, err := next.BuildRetainedV2(nextEvidence)
	if err != nil {
		t.Fatal("exact source lineage must survive operation-to-canonical provenance extension", err)
	}
	if _, err := compactioncontext.DecodeRetainedV2(c, p, child.Plan(), child.SummaryBody(), child.ManifestBody(), retained); err != nil {
		t.Fatal("extended retained source reread", err)
	}
}
