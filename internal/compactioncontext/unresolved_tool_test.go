package compactioncontext_test

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func unresolvedToolRecords(t *testing.T) []sessioncontext.EventPayload {
	t.Helper()
	records := generationRecords(t)[:7] // An unrelated completed Run can compact.
	call := toolRecord(t, 8, domain.SessionEventToolCall, "run-unknown", "unknown-call", "lookup")
	d := strings.Repeat("a", 64)
	outcome, err := canonicaloutput.EncodeMCPUnknownToolOutcomeV1(domain.AttemptOperationDigestV1(d), domain.AttemptOperationReservationDigestV1(d), domain.AttemptOperationObservationDigestV1(d))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(outcome)
	result := toolRecord(t, 9, domain.SessionEventToolResult, "run-unknown", "unknown-call", "lookup")
	body, err := json.Marshal(struct {
		Schema   string          `json:"schema"`
		CallID   string          `json:"call_id"`
		ToolName string          `json:"tool_name"`
		Content  json.RawMessage `json:"content"`
	}{"sessionless.tool-event.v1", "unknown-call", "lookup", outcome})
	if err != nil {
		t.Fatal(err)
	}
	replacePayload(&result, string(body))
	clear(body)
	fixed, err := canonicaloutput.EncodeFailureV1("run-unknown", false, domain.MCPOutcomeUnknownV3)
	if err != nil {
		t.Fatal(err)
	}
	notice := canonicalRecord(t, 10, domain.SessionEventSystemNotice, "run-unknown", string(fixed.Payload))
	clear(fixed.Payload)
	notice.Event.ID = fixed.ID
	notice.Event.Payload.Key = domain.SessionEventObjectPrefix("tenant", "session", fixed.ID) + "message.json"
	return append(records, call, result, notice, userRecord(t, 11))
}

func TestUnresolvedToolPairAndAnchorIntrinsicProtection(t *testing.T) {
	records := unresolvedToolRecords(t)
	defer clearCopies(records)
	view := build(t, records)
	protected := false
	for _, unit := range view.Units() {
		if unit.Start == 7 && unit.End == 10 && unit.Protected {
			protected = true
		}
	}
	if !protected {
		t.Fatal("UNKNOWN call/result was not protected with its anchor")
	}
	c, p := generationFixture(t)
	g, err := compactioncontext.BuildGenerationV1(c, c.Limits, p, strings.Repeat("1", 64), records, nil, nil, nil)
	if err != nil || len(g.CoverageIDs()) == 0 {
		t.Fatal("unrelated completed history must remain compactable", err)
	}
	for _, record := range records[7:10] {
		id, err := compactioncontext.CanonicalSourceKeyV1(record.Event)
		if err != nil {
			t.Fatal(err)
		}
		for _, covered := range g.CoverageIDs() {
			if covered == id {
				t.Fatal("UNKNOWN fact became safe compaction coverage")
			}
		}
	}
	v, err := g.BuildRetainedV1(retainedEvidence(t, c, p, g, 1, nil, nil, nil))
	if err != nil || v.ValidateUnresolvedToolProtection(records) != nil {
		t.Fatal("actual payload/retained protection join", err)
	}
	// Rehash every public artifact/core/reference after moving the pair (but
	// not its separately protected notice) to Covered. The old wire decoder
	// has no payloads: the mandatory actual-source consumer must reject it.
	sb, mb := v.SummaryBody(), v.ManifestBody()
	defer clear(sb)
	defer clear(mb)
	var envelope struct {
		Cut json.RawMessage `json:"cut"`
	}
	if json.Unmarshal(mb, &envelope) != nil {
		t.Fatal("cut DATA")
	}
	var cut struct {
		Sources   []json.RawMessage `json:"sources"`
		Cut       uint32            `json:"cut"`
		Protected []uint32          `json:"protected"`
		Covered   []uint32          `json:"covered"`
		Suffix    []uint32          `json:"suffix"`
	}
	if json.Unmarshal(envelope.Cut, &cut) != nil || cut.Cut < 10 {
		t.Fatal("test requires UNKNOWN component before cut")
	}
	var kept []uint32
	for _, index := range cut.Protected {
		if index == 7 || index == 8 {
			cut.Covered = append(cut.Covered, index)
		} else {
			kept = append(kept, index)
		}
	}
	cut.Protected = kept
	sort.Slice(cut.Covered, func(i, j int) bool { return cut.Covered[i] < cut.Covered[j] })
	changed, err := json.Marshal(cut)
	if err != nil {
		t.Fatal(err)
	}
	mb = bytes.Replace(mb, envelope.Cut, changed, 1)
	sb, mb = replaceRetainedCore(t, sb, mb, "cut_digest", retainedHash(changed))
	plan, sb, mb := retainedMutated(t, v, sb, mb)
	bad, err := compactioncontext.DecodeRetainedV1(c, p, plan, sb, mb, nil)
	if err != nil {
		t.Fatal("mutation must pass hashes/old-wire structural validation before actual-payload check", err)
	}
	if bad.ValidateUnresolvedToolProtection(records) == nil {
		t.Fatal("fully rehashed UNKNOWN pair Covered despite protected notice")
	}
	if _, err := compactioncontext.BuildOrdinaryV1(c, c.Limits, p, strings.Repeat("1", 64), records, nil, bad.Parent(), nil); err == nil {
		t.Fatal("tampered UNKNOWN pair entered ordinary completed context")
	}
}
