package compactioncontext_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// Renderer/lineage DATA checks, not current Store authority or live model proof.
func TestOrdinaryKeepsCompleteCanonicalInputAndCatalog(t *testing.T) {
	c, p := generationFixture(t)
	records := generationRecords(t)
	tools := []configuredinference.ToolV1{{Name: "lookup", Parameters: []byte(`{"type":"object","properties":{},"required":[],"additionalProperties":false}`)}}
	frozen, err := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("1", 64), records, nil, nil, tools)
	if err != nil {
		t.Fatal("complete ordinary input", err)
	}
	wire, history := generationWire(t, frozen)
	if history["schema"] != "sessionless.history.v1" || history["derived_summary"] != "" || history["protected_originals"] != nil {
		t.Fatal("uncompacted input unexpectedly selected a derived view")
	}
	units := history["original_units"].([]any)
	if len(units) != len(records) || len(wire["tools"].([]any)) != 1 {
		t.Fatalf("complete frontier/catalog lost: frames=%d want=%d tools=%v", len(units), len(records), wire["tools"])
	}
	for i, unit := range units {
		frame := unit.(map[string]any)
		if frame["event_id"] != string(records[i].Event.ID) || frame["original_payload"] != string(records[i].Payload) {
			t.Fatalf("original frame %d changed: %v", i, frame["event_id"])
		}
	}
	before := frozen.Body()
	clear(records[0].Payload)
	clear(tools[0].Parameters)
	if !bytes.Equal(before, frozen.Body()) {
		t.Fatal("caller mutation changed frozen ordinary wire")
	}
}

func TestOrdinaryUserOnlyDoesNotRequireCompactionCut(t *testing.T) {
	c, p := generationFixture(t)
	records := []sessioncontext.EventPayload{userRecord(t, 1)}
	frozen, err := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("1", 64), records, nil, nil, nil)
	if err != nil {
		t.Fatal("ordinary first user input must work without a compactable prefix", err)
	}
	_, history := generationWire(t, frozen)
	if len(history["original_units"].([]any)) != 1 {
		t.Fatal("sole original input missing")
	}
	if _, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), records, nil, nil, nil); err == nil {
		t.Fatal("positive ordinary input unexpectedly had a compaction cut")
	}
}

func ordinaryCompletedCompaction(t *testing.T, c domain.ConfiguredInferenceProfileV1, g *compactioncontext.GenerationV1, e compactioncontext.RetainedEvidenceV1) compactioncontext.CompletedOperationV1 {
	t.Helper()
	wire, err := json.Marshal(map[string]any{"model": c.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": string(e.Summary)}, "finish_reason": "stop"}}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := g.FrozenRequest().NormalizeResponseBodyV1(wire, domain.AttemptOperationCompactionV1, e.Reservation.Operation.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	return compactioncontext.CompletedOperationV1{Reservation: e.Reservation, Observation: e.Observation, Request: g.FrozenRequest().Body(), Response: response}
}

func TestOrdinaryRetainedHeadMatchesPublishedWireAcrossTwoGenerations(t *testing.T) {
	c, p, firstProgram, firstEvidence, first := retainedFixture(t)
	op1 := ordinaryCompletedCompaction(t, c, firstProgram, firstEvidence)
	assertWire := func(t *testing.T, program *compactioncontext.GenerationV1, retained *compactioncontext.RetainedV1, operations []compactioncontext.CompletedOperationV1) {
		t.Helper()
		expected, err := program.Rebuild(retained.SummaryText())
		if err != nil {
			t.Fatal("published next view", err)
		}
		actual, err := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("3", 64), generationRecords(t), operations, retained.Parent(), nil)
		if err != nil {
			t.Fatal("ordinary view from retained head and actual closed frontier", err)
		}
		if actual.Digest() != expected.Digest() || !bytes.Equal(actual.Body(), expected.Body()) {
			t.Fatal("ordinary successor differs from actual published next wire")
		}
		_, history := generationWire(t, actual)
		seen := map[string]bool{}
		for _, key := range []string{"protected_originals", "original_units"} {
			for _, value := range history[key].([]any) {
				frame := value.(map[string]any)
				id := frame["source_id"].(string)
				if seen[id] {
					t.Fatal("original appears twice in successor")
				}
				seen[id] = true
				if frame["kind"] == "operation" && frame["original_payload"] == string(retained.SummaryText()) {
					t.Fatal("compaction response became an ordinary assistant frame")
				}
			}
		}
		for _, id := range retained.Parent().CoveredSourceIDs {
			if seen[id] {
				t.Fatal("covered original reinserted")
			}
		}
	}
	t.Run("first", func(t *testing.T) { assertWire(t, firstProgram, first, []compactioncontext.CompletedOperationV1{op1}) })
	op2 := retainedModel(t, c, op1, 2)
	op3 := retainedModel(t, c, op2, 3)
	operations := []compactioncontext.CompletedOperationV1{op1, op2, op3}
	secondProgram, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("2", 64), generationRecords(t), operations, first.Parent(), nil)
	if err != nil {
		t.Fatal("second compactable frontier", err)
	}
	previous := compactioncontext.RetainedEvidenceV1{Reservation: op3.Reservation, Observation: op3.Observation}
	secondEvidence := retainedEvidence(t, c, p, secondProgram, 4, &previous, nil, first)
	second, err := secondProgram.BuildRetainedV1(secondEvidence)
	if err != nil {
		t.Fatal("second retained pair", err)
	}
	operations = append(operations, ordinaryCompletedCompaction(t, c, secondProgram, secondEvidence))
	t.Run("second", func(t *testing.T) { assertWire(t, secondProgram, second, operations) })
	t.Run("missing_parent_plan", func(t *testing.T) {
		parent := second.Parent()
		parent.PriorPlans = nil
		if _, err := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("3", 64), generationRecords(t), operations, parent, nil); err == nil {
			t.Fatal("unbound earlier compaction accepted")
		}
	})
}

func TestOrdinaryClassifiesBoundedNoFitAndRejectsMalformedSource(t *testing.T) {
	for _, name := range []string{"byte_no_fit", "foreign_profile", "invalid_source", "pending_pair"} {
		t.Run(name, func(t *testing.T) {
			c, p := generationFixture(t)
			records := generationRecords(t)
			limits, source := c.Limits, strings.Repeat("1", 64)
			switch name {
			case "byte_no_fit":
				limits.RequestBytes = 128
			case "foreign_profile":
				c.BaseURL = "http://127.0.0.1:11435/v1"
			case "invalid_source":
				source = "untrusted-label"
			case "pending_pair":
				records[3] = toolRecord(t, 4, domain.SessionEventToolResult, "run-a", "missing", "fetch")
			}
			frozen, err := compactioncontext.BuildOrdinaryV1(c, limits, p, source, records, nil, nil, nil)
			if err == nil || frozen != nil {
				t.Fatal("invalid full view produced request", name)
			}
			if errors.Is(err, configuredinference.ErrContextNoFit) != (name == "byte_no_fit") {
				t.Fatalf("error must distinguish no-fit from malformed authority/source: %s: %v", name, err)
			}
		})
	}
}

func TestOrdinaryActualOuterWireLimitNotInnerHistoryLimit(t *testing.T) {
	for _, name := range []string{"plain", "escape_heavy"} {
		t.Run(name, func(t *testing.T) {
			c, p := generationFixture(t)
			records := []sessioncontext.EventPayload{userRecord(t, 1)}
			if name == "escape_heavy" {
				raw, err := json.Marshal(map[string]any{"version": 1, "text": strings.Repeat("\"\\<>&\n", 64)})
				if err != nil {
					t.Fatal(err)
				}
				replacePayload(&records[0], string(raw))
			}
			tools := []configuredinference.ToolV1{{Name: "lookup", Parameters: []byte(`{"type":"object","properties":{},"required":[],"additionalProperties":false}`)}}
			full, err := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("1", 64), records, nil, nil, tools)
			if err != nil {
				t.Fatal(err)
			}
			limit := c.Limits
			limit.RequestBytes = uint64(len(full.Body()))
			atLimit, err := compactioncontext.BuildOrdinaryV1(c, limit, p, strings.Repeat("1", 64), records, nil, nil, tools)
			if err != nil || !bytes.Equal(atLimit.Body(), full.Body()) {
				t.Fatal("exact actual wire limit rejected", err)
			}
			limit.RequestBytes--
			if f, err := compactioncontext.BuildOrdinaryV1(c, limit, p, strings.Repeat("1", 64), records, nil, nil, tools); !errors.Is(err, configuredinference.ErrContextNoFit) || f != nil {
				t.Fatalf("valid actual wire one byte over must be no-fit: %v", err)
			}
			tools[0].Parameters = []byte(`{"not-a-schema":true}`)
			if _, err := compactioncontext.BuildOrdinaryV1(c, limit, p, strings.Repeat("1", 64), records, nil, nil, tools); err == nil || errors.Is(err, configuredinference.ErrContextNoFit) {
				t.Fatalf("malformed catalog promoted to compaction trigger: %v", err)
			}
		})
	}
}

func TestOrdinaryRejectsStructurallyValidNormalizedUsageMismatch(t *testing.T) {
	c, p, g, e, retained := retainedFixture(t)
	compaction := ordinaryCompletedCompaction(t, c, g, e)
	op := retainedModel(t, c, compaction, 2)
	if _, err := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("1", 64), generationRecords(t), []compactioncontext.CompletedOperationV1{compaction, op}, retained.Parent(), nil); err != nil {
		t.Fatal("unchanged normalized usage positive", err)
	}
	zero := uint64(0)
	op.Observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: &zero}
	if err := op.Observation.ValidateForReservation(op.Reservation); err != nil {
		t.Fatal("negative must be structurally valid", err)
	}
	if _, err := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("1", 64), generationRecords(t), []compactioncontext.CompletedOperationV1{compaction, op}, retained.Parent(), nil); err == nil {
		t.Fatal("authenticated usage differing from actual normalized response accepted")
	}
}
