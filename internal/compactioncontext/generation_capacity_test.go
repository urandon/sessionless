package compactioncontext_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
)

func TestGenerationSuccessorMinimumRequiresTypedSummary(t *testing.T) {
	c, p := generationFixture(t)
	g, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), generationRecords(t), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	refs := g.References()
	if len(refs) == 0 {
		t.Fatal("minimum fixture needs actual known source")
	}
	witness := []byte(`{"schema":"sessionless.compaction-summary.v1","task":[{"text":"x","source_ids":["` + refs[0].SourceID + `"]}],"constraints":[],"facts":[],"unresolved":[],"artifacts":[],"omissions":[]}`)
	if _, err := g.ValidateSummary(witness); err != nil {
		t.Fatal("structurally valid DATA witness", err)
	}
	minimum, err := g.MinimumSuccessorRequest()
	if err != nil {
		t.Fatal(err)
	}
	_, history := generationWire(t, minimum)
	summary, ok := history["derived_summary"].(string)
	if !ok || !bytes.Equal([]byte(summary), witness) {
		t.Errorf("minimum lost compulsory typed summary: bytes=%d want=%d", len(summary), len(witness))
	}
	actual, err := g.Rebuild(witness)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(minimum.Body(), actual.Body()) {
		t.Error("minimum does not use actual validated Rebuild wire")
	}
	if _, err := g.SuccessorRequestUpperBytes(uint64(len(witness)-1), c.Limits.RequestBytes); !errors.Is(err, configuredinference.ErrContextNoFit) {
		t.Errorf("summary format first-over accepted: %v", err)
	}
	if _, err := g.SuccessorRequestUpperBytes(uint64(len(witness)), c.Limits.RequestBytes); err != nil {
		t.Error("exact minimum format rejected", err)
	}
}

func TestGenerationSuccessorCapacitySameRenderer(t *testing.T) {
	// The ordinary generation fixture is DATA only, not a current Store owner.
	c, p := generationFixture(t)
	g, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), generationRecords(t), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	upper, err := g.SuccessorRequestUpperBytes(configuredinference.MaxCompactionSummaryBytesV1, 8191)
	if err != nil || upper > 8191 || upper == 0 {
		t.Fatalf("admissible wire bound=%d err=%v", upper, err)
	}
	summary := generationSummary(t, g)
	for _, text := range []string{"Preserve every original condition", "<>&\\u2028\\u2029", "\\\"\\\\\\n"} {
		body := []byte(strings.Replace(string(summary), "Preserve every original condition", text, 1))
		actual, err := g.Rebuild(body)
		if err != nil {
			t.Fatal("valid summary rebuild", err)
		}
		wire := actual.Body()
		if uint64(len(wire)) > upper {
			t.Fatalf("actual wire=%d exceeded upper=%d", len(wire), upper)
		}
		clear(wire)
	}
	if _, err := g.SuccessorRequestUpperBytes(16<<10, 1); !errors.Is(err, configuredinference.ErrContextNoFit) {
		t.Fatalf("known-prefix impossible capacity: %v", err)
	}
	if _, err := g.SuccessorRequestUpperBytes((16<<10)+1, 8191); err == nil {
		t.Fatal("unbounded summary length accepted")
	}
	if _, err := g.Rebuild([]byte("not a summary")); err == nil {
		t.Fatal("capacity estimate waived actual summary validation")
	}
}
