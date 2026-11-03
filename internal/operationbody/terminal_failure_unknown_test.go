package operationbody_test

import (
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestTerminalFailureUnknownMCPUsesOriginalBatchAndNoticeOnce(t *testing.T) {
	// Cooperative physical-storage DATA only, not an original Store producer,
	// authenticated failure capsule, retirement or erasure proof.
	b := newBodyBlobs()
	_, owner := terminalOutputStore(t, b, 2, 1<<20)
	run, _, _, custody, capsule := failurePhysicalData(t)
	d := strings.Repeat("a", 64)
	unknown, err := canonicaloutput.EncodeMCPUnknownToolOutcomeV1(domain.AttemptOperationDigestV1(d), domain.AttemptOperationReservationDigestV1(d), domain.AttemptOperationObservationDigestV1(d))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(unknown)
	p, err := canonicaloutput.EncodeMCPOutcomeUnknownFailureV3(run.ID, domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 4096}, []ports.ExecutionToolEvent{{Kind: domain.SessionEventToolCall, CallID: "call-a", ToolName: "lookup", Payload: []byte(`{"q":"<x>"}`)}, {Kind: domain.SessionEventToolResult, CallID: "call-a", ToolName: "lookup", Payload: unknown}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release()
	var drafts []domain.SessionEventDraft
	var bodies [][]byte
	for _, event := range p.Events() {
		draft, err := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, run.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
		drafts = append(drafts, draft)
		bodies = append(bodies, event.Payload)
	}
	if owner.WriteCapsule(t.Context(), custody, capsule) != nil || owner.WriteFailureEventsV1(t.Context(), run, drafts, bodies) != nil {
		t.Fatal("original capsule/batch/notice did not physically publish")
	}
	alias := *owner
	if alias.WriteFailureEventsV1(t.Context(), run, drafts, bodies) == nil || owner.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies) == nil || alias.WriteFailureNotice(t.Context(), drafts[2], bodies[2]) == nil {
		t.Fatal("F3 reopened a batch, success arm or protected notice")
	}
	if puts, opens, closes, deletes, _ := b.counts(); puts != 4 || opens != 4 || closes != 4 || deletes != 0 {
		t.Fatal("F3 physical slots/readbacks repeated or fabricated erasure", puts, opens, closes, deletes)
	}
}
