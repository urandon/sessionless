package operationbody_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// All fixtures below are structural DATA plus original physical ownership.
// They prove no current Store authority, cloud immutability, cleanup or SQL
// correspondence publication. No network/provider/credentials are involved.
func canonicalChainData(t *testing.T) (domain.Run, []domain.SessionEventDraft, [][]byte, domain.TerminalCustodyPlanV1, []byte) {
	t.Helper()
	run, _, _, custody, capsule := canonicalTerminalData(t)
	p, _ := terminalOutputLimits(1, 1<<20)
	encoded, err := canonicaloutput.EncodeCompletionV1(run.ID, p, ports.ExecutionResult{Summary: "final answer", ToolEvents: []ports.ExecutionToolEvent{
		{Kind: domain.SessionEventToolCall, CallID: "call-a", ToolName: "lookup", Payload: []byte(`{"key":"a"}`)},
		{Kind: domain.SessionEventToolResult, CallID: "call-a", ToolName: "lookup", Payload: []byte(`{"value":"b"}`)},
	}}, "")
	if err != nil {
		t.Fatal("encode bounded canonical fixture", err)
	}
	defer encoded.Release()
	var drafts []domain.SessionEventDraft
	var bodies [][]byte
	for _, e := range encoded.Events() {
		draft, err := canonicaloutput.PlanEventDraftV1(run, e.ID, e.Kind, e.Payload, run.CreatedAt)
		if err != nil {
			t.Fatal("plan canonical fixture", err)
		}
		draft.DisplayText = e.DisplayText
		drafts = append(drafts, draft)
		bodies = append(bodies, bytes.Clone(e.Payload))
	}
	t.Cleanup(func() {
		for _, b := range bodies {
			clear(b)
		}
	})
	return run, drafts, bodies, custody, capsule
}

func TestTerminalCanonicalV2CompleteOneUseAndIndependentCapacity(t *testing.T) {
	b := newBodyBlobs()
	s, owner := terminalOutputStore(t, b, 1, 1<<20)
	run, drafts, bodies, custody, capsule := canonicalChainData(t)
	if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
		t.Fatal("capsule", err)
	}
	if err := owner.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies); err != nil {
		t.Fatal("exact ordered original canonical chain", err)
	}
	puts, opens, closes, deletes, _ := b.counts()
	if puts != 4 || opens != 4 || closes != 4 || deletes != 0 {
		t.Fatalf("each owned capsule/event PUT and readback once: put=%d open=%d close=%d delete=%d", puts, opens, closes, deletes)
	}
	for i, draft := range drafts {
		if !bytes.Equal(b.objects[draft.Payload.Key], bodies[i]) {
			t.Errorf("stored canonical event %d differs from sealed payload", i)
		}
	}
	alias := *owner
	if err := alias.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies); err == nil {
		t.Error("holder alias replayed full chain")
	}
	if err := owner.WriteCanonicalAssistant(t.Context(), run, drafts[2], bodies[2]); err == nil {
		t.Error("V1 mixed into used V2 lifetime")
	}
	for range 2 * retainedLimits().MaxOperations {
		if _, err := s.Write(t.Context(), []byte("ordinary")); err != nil {
			t.Fatal("canonical allowance changed independent ordinary write count", err)
		}
	}
	if _, err := s.Write(t.Context(), []byte("first over")); err == nil {
		t.Error("canonical closure enlarged ordinary-body capacity")
	}
}

func TestTerminalCanonicalV2V1CannotBeReopenedAsChain(t *testing.T) {
	b := newBodyBlobs()
	_, owner := terminalOutputStore(t, b, 1, 1<<20)
	run, drafts, bodies, custody, capsule := canonicalChainData(t)
	if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
		t.Fatal(err)
	}
	if err := owner.WriteCanonicalAssistant(t.Context(), run, drafts[2], bodies[2]); err != nil {
		t.Fatal("original V1 remains available", err)
	}
	if err := owner.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies); err == nil {
		t.Error("V2 reopened consumed V1 ownership")
	}
	if puts, _, _, _, _ := b.counts(); puts != 2 {
		t.Errorf("version-mixing caused extra physical writes: puts=%d want=2", puts)
	}
}

func TestTerminalCanonicalV2RejectsWholeInvalidPlanBeforeAnyEventWrite(t *testing.T) {
	for _, mode := range []string{"no_capsule", "wrong_capsule", "wrong_scope", "wrong_run", "cancelled_run", "terminal_run", "missing_final", "mismatched_lengths", "too_many", "wrong_kind", "reordered", "duplicate_id", "duplicate_call", "wrong_call", "wrong_tool", "wrong_key", "wrong_hash", "wrong_size", "wrong_idempotency", "foreign_payload_tenant", "malformed_last", "wrong_schema_last", "tool_display", "old_time", "late_time", "empty_last", "oversize_last", "changed_owned_bytes"} {
		t.Run(mode, func(t *testing.T) {
			b := newBodyBlobs()
			p, substrate := terminalOutputLimits(1, 1<<20)
			if mode == "duplicate_call" {
				p.MaxToolEvents, p.MaxToolEventBytes = 4, 1024
			}
			s := retainedStore(t, b, retainedLimits())
			owner, err := s.AdmitTerminalOutput(t.Context(), p, substrate)
			if err != nil {
				t.Fatal(err)
			}
			run, drafts, bodies, custody, capsule := canonicalChainData(t)
			if mode != "no_capsule" {
				if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
					t.Fatal(err)
				}
			}
			call := t.Context()
			switch mode {
			case "wrong_capsule":
				run.TerminalCustody.Plan = terminalCapsulePlan(t, []byte("different capsule"))
			case "wrong_scope":
				run.TerminalCustody.Plan.Scope.AttemptID = "different-attempt"
				run.TerminalCustody.Plan.Capsule.Key, _ = domain.TerminalCustodyObjectKeyV1(run.TerminalCustody.Plan.Scope, run.TerminalCustody.Plan.Capsule.SHA256)
			case "wrong_run":
				run.ID = "other-run"
			case "cancelled_run":
				at := run.CreatedAt
				run.CancellationRequestedAt = &at
			case "terminal_run":
				run.Status = domain.RunSucceeded
			case "missing_final":
				drafts, bodies = drafts[:2], bodies[:2]
			case "mismatched_lengths":
				bodies = bodies[:1]
			case "too_many":
				drafts = append(drafts, drafts...)
				bodies = append(bodies, bodies...)
			case "wrong_kind":
				drafts[2].Kind = domain.SessionEventSystemNotice
			case "reordered":
				drafts[0], drafts[1] = drafts[1], drafts[0]
				bodies[0], bodies[1] = bodies[1], bodies[0]
			case "duplicate_id":
				drafts[1].ID = drafts[0].ID
			case "duplicate_call":
				drafts = append(append(append([]domain.SessionEventDraft{}, drafts[:2]...), drafts[:2]...), drafts[2])
				bodies = append(append(append([][]byte{}, bodies[:2]...), bodies[:2]...), bodies[2])
				for i := 2; i < 4; i++ {
					drafts[i], _ = canonicaloutput.PlanEventDraftV1(run, domain.SessionEventID("second-"+string(drafts[i].ID)), drafts[i].Kind, bodies[i], run.CreatedAt)
				}
			case "wrong_call", "wrong_tool":
				old, replacement := "call-a", "call-b"
				if mode == "wrong_tool" {
					old, replacement = "lookup", "search"
				}
				bodies[1] = bytes.ReplaceAll(bodies[1], []byte(old), []byte(replacement))
				drafts[1], _ = canonicaloutput.PlanEventDraftV1(run, drafts[1].ID, drafts[1].Kind, bodies[1], run.CreatedAt)
			case "wrong_key":
				drafts[2].Payload.Key += ".redirect"
			case "wrong_hash":
				drafts[2].Payload.SHA256 = strings.Repeat("b", 64)
			case "wrong_size":
				drafts[2].Payload.Size++
			case "wrong_idempotency":
				drafts[2].IdempotencyKey = "other-key"
			case "foreign_payload_tenant":
				drafts[2].Payload.TenantID = "other-tenant"
			case "malformed_last", "wrong_schema_last":
				bodies[2] = []byte("not JSON")
				if mode == "wrong_schema_last" {
					bodies[2] = []byte(`{"schema":"other.v1","summary":"answer"}`)
				}
				drafts[2], _ = canonicaloutput.PlanEventDraftV1(run, drafts[2].ID, drafts[2].Kind, bodies[2], run.CreatedAt)
			case "tool_display":
				drafts[0].DisplayText = "not projectable"
			case "old_time":
				drafts[0].CreatedAt = run.CreatedAt.Add(-1)
			case "late_time":
				drafts[0].CreatedAt = run.CreatedAt.Add(1)
			case "empty_last":
				bodies[2] = nil
			case "oversize_last":
				bodies[2] = bytes.Repeat([]byte{'x'}, (1<<20)+1)
			case "changed_owned_bytes":
				call = &terminalMutatingContext{Context: call, mutate: func() { bodies[2][0] ^= 1 }}
			}
			before, _, _, _, _ := b.counts()
			if err := owner.WriteCanonicalEventsV2(call, run, drafts, bodies); err == nil {
				t.Fatalf("invalid whole plan accepted: mode=%s", mode)
			}
			if after, _, _, _, _ := b.counts(); after != before {
				t.Errorf("invalid final/identity plan partially published: mode=%s put=%d want=%d", mode, after, before)
			}
		})
	}
}

// The test-owned wrapper injects faults at the exact selected canonical event.
// It never substitutes production resolver/authority behavior.
type canonicalFaultBlobs struct {
	*bodyBlobs
	call      int
	faultAt   int
	mode      string
	firstHook func()
	callHook  func(int)
	readers   []*bytes.Reader
}

func (b *canonicalFaultBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	b.call++
	if b.callHook != nil {
		b.callHook(b.call)
	}
	if reader, ok := body.(*bytes.Reader); ok {
		b.readers = append(b.readers, reader)
	}
	if b.call == 2 && b.firstHook != nil {
		b.firstHook()
	}
	if b.call == b.faultAt {
		b.bodyBlobs.mu.Lock()
		switch b.mode {
		case "lost_ack":
			b.putFault = "lost_ack"
		case "put_ref":
			b.forge = "key"
		case "readback":
			b.openFault = "corrupt"
		case "close":
			b.closeFault = true
		}
		b.bodyBlobs.mu.Unlock()
	}
	return b.bodyBlobs.Put(ctx, tenant, key, body)
}

func canonicalFaultStore(t *testing.T, b ports.BlobStore) (*operationbody.StoreV1, *operationbody.TerminalOutputV1) {
	t.Helper()
	s, err := operationbody.NewV1(bodyScope(), retainedLimits(), b)
	if err != nil {
		t.Fatal(err)
	}
	p, substrate := terminalOutputLimits(1, 1<<20)
	owner, err := s.AdmitTerminalOutput(t.Context(), p, substrate)
	if err != nil {
		t.Fatal(err)
	}
	return s, owner
}

func TestTerminalCanonicalV2MiddleUncertaintyStopsNoReplayKeepsFailureSlot(t *testing.T) {
	for _, mode := range []string{"lost_ack", "put_ref", "readback", "close"} {
		t.Run(mode, func(t *testing.T) {
			b := &canonicalFaultBlobs{bodyBlobs: newBodyBlobs(), faultAt: 3, mode: mode}
			s, owner := canonicalFaultStore(t, b)
			run, drafts, bodies, custody, capsule := canonicalChainData(t)
			if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
				t.Fatal(err)
			}
			if err := owner.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies); err == nil {
				t.Fatal("uncertain middle event reported success", mode)
			}
			puts, _, _, _, _ := b.counts()
			if puts != 3 {
				t.Fatalf("must stop at exact uncertain tool result: puts=%d want=3", puts)
			}
			b.putFault, b.forge, b.openFault, b.closeFault = "", "", "", false
			alias := *owner
			if err := alias.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies); err == nil {
				t.Error("alias repeated possible I/O")
			}
			if err := owner.WriteCanonicalAssistant(t.Context(), run, drafts[2], bodies[2]); err == nil {
				t.Error("V1 bypassed incomplete V2")
			}
			if _, err := s.Write(t.Context(), []byte("more work")); err == nil {
				t.Error("ordinary writer bypassed original uncertainty")
			}
			if after, _, _, _, _ := b.counts(); after != puts {
				t.Error("denied replay caused physical PUT")
			}
			notice, err := canonicaloutput.EncodeFailureV1(run.ID, false, "output_unavailable")
			if err != nil {
				t.Fatal(err)
			}
			draft, err := canonicaloutput.PlanEventDraftV1(run, notice.ID, notice.Kind, notice.Payload, run.CreatedAt)
			if err != nil {
				t.Fatal(err)
			}
			if err := owner.WriteFailureNotice(t.Context(), draft, notice.Payload); err != nil {
				t.Fatal("protected original failure slot lost", err)
			}
			if _, err := s.Write(t.Context(), []byte("after notice")); err == nil {
				t.Error("successful failure notice cleared prior canonical uncertainty")
			}
		})
	}
}

func TestTerminalCanonicalV2FreezesWholePlanBeforeFirstPUT(t *testing.T) {
	b := &canonicalFaultBlobs{bodyBlobs: newBodyBlobs()}
	_, owner := canonicalFaultStore(t, b)
	run, drafts, bodies, custody, capsule := canonicalChainData(t)
	if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
		t.Fatal(err)
	}
	expected := bytes.Clone(bodies[2])
	expectedRef := drafts[2].Payload
	b.firstHook = func() {
		bodies[2][0] ^= 1
		drafts[2].ID = "redirected-id"
		run.TerminalCustody.Plan.Capsule.Key += ".redirected"
	}
	b.callHook = func(call int) {
		_, opens, closes, _, _ := b.counts()
		if opens != call-1 || closes != call-1 {
			t.Errorf("event %d started before exact predecessor readback closed: opens=%d closes=%d", call-1, opens, closes)
		}
	}
	if err := owner.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies); err != nil {
		t.Fatal("mutations after first I/O changed already sealed batch", err)
	}
	if !bytes.Equal(b.objects[expectedRef.Key], expected) {
		t.Error("late mutation reached original canonical object")
	}
	for i, reader := range b.readers {
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			t.Fatal("inspect bounded original-reader lifetime", err)
		}
		retired, err := io.ReadAll(reader)
		if err != nil || !bytes.Equal(retired, make([]byte, len(retired))) {
			t.Errorf("owned physical buffer not zeroized after synchronous return: index=%d err=%v", i, err)
		}
	}
}

func TestTerminalCanonicalV2OriginalHardBoundCompletePairs(t *testing.T) {
	for _, pairs := range []int{2, 4, 5} {
		t.Run(string(rune('0'+pairs))+"_pairs", func(t *testing.T) {
			b := newBodyBlobs()
			run, _, _, custody, capsule := canonicalChainData(t)
			p, substrate := terminalOutputLimits(1, 1<<20)
			p.MaxToolEvents, p.MaxToolEventBytes = 10, 16<<10
			s := retainedStore(t, b, retainedLimits())
			owner, err := s.AdmitTerminalOutput(t.Context(), p, substrate)
			if err != nil {
				t.Fatal(err)
			}
			var events []ports.ExecutionToolEvent
			for i := 0; i < pairs; i++ {
				call := "call-" + string(rune('a'+i))
				events = append(events, ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: call, ToolName: "lookup", Payload: []byte(`{}`)}, ports.ExecutionToolEvent{Kind: domain.SessionEventToolResult, CallID: call, ToolName: "lookup", Payload: []byte(`{}`)})
			}
			encoded, err := canonicaloutput.EncodeCompletionV1(run.ID, p, ports.ExecutionResult{Summary: "answer", ToolEvents: events}, "")
			if err != nil {
				t.Fatal(err)
			}
			defer encoded.Release()
			var drafts []domain.SessionEventDraft
			var bodies [][]byte
			for _, e := range encoded.Events() {
				draft, err := canonicaloutput.PlanEventDraftV1(run, e.ID, e.Kind, e.Payload, run.CreatedAt)
				if err != nil {
					t.Fatal(err)
				}
				drafts = append(drafts, draft)
				bodies = append(bodies, e.Payload)
			}
			if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
				t.Fatal(err)
			}
			err = owner.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies)
			want := pairs <= 4
			if (err == nil) != want {
				t.Errorf("unchanged max four-dispatch physical profile: pairs=%d err=%v want_success=%v", pairs, err, want)
			}
			wantPuts := 1
			if want {
				wantPuts += 2*pairs + 1
			}
			if puts, _, _, _, _ := b.counts(); puts != wantPuts {
				t.Errorf("physical ordered batch count: puts=%d want=%d", puts, wantPuts)
			}
		})
	}
}

func TestTerminalCanonicalV2InvalidOwnedPayloadCannotBeRepairedAndRetried(t *testing.T) {
	b := newBodyBlobs()
	s, owner := terminalOutputStore(t, b, 1, 1<<20)
	run, drafts, bodies, custody, capsule := canonicalChainData(t)
	if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
		t.Fatal(err)
	}
	bodies[2][0] ^= 1
	if err := owner.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies); err == nil {
		t.Fatal("mismatched final digest accepted")
	}
	bodies[2][0] ^= 1
	alias := *owner
	if err := alias.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies); err == nil {
		t.Error("repaired caller buffer resurrected consumed plan")
	}
	if _, err := s.Write(t.Context(), []byte("more")); err == nil {
		t.Error("original writer continued after irrevocable batch admission failure")
	}
	if puts, _, _, _, _ := b.counts(); puts != 1 {
		t.Errorf("repair/retry reached physical storage: puts=%d want=1", puts)
	}
}

func TestTerminalCanonicalV2OriginalToolCapacityBoundaries(t *testing.T) {
	for _, mode := range []string{"exact", "bytes_one_short", "events_one_short", "original_capacity_copy"} {
		t.Run(mode, func(t *testing.T) {
			b := newBodyBlobs()
			run, drafts, bodies, custody, capsule := canonicalChainData(t)
			p, substrate := terminalOutputLimits(1, 1<<20)
			p.MaxToolEvents = 2
			p.MaxToolEventBytes = uint64(len(bodies[0]) + len(bodies[1]))
			if mode == "bytes_one_short" {
				p.MaxToolEventBytes--
			}
			if mode == "events_one_short" {
				p.MaxToolEvents--
			}
			s := retainedStore(t, b, retainedLimits())
			owner, err := s.AdmitTerminalOutput(t.Context(), p, substrate)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "original_capacity_copy" {
				// Caller-owned DATA cannot raise captured original bounds.
				p.MaxToolEvents, p.MaxToolEventBytes = 128, 1<<20
			}
			if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
				t.Fatal(err)
			}
			err = owner.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies)
			want := mode == "exact" || mode == "original_capacity_copy"
			if (err == nil) != want {
				t.Errorf("original admitted tool bound: mode=%s err=%v want_success=%v", mode, err, want)
			}
			puts, _, _, _, _ := b.counts()
			wantPuts := 1
			if want {
				wantPuts = 4
			}
			if puts != wantPuts {
				t.Errorf("capacity checked before any partial canonical publication: mode=%s puts=%d want=%d", mode, puts, wantPuts)
			}
		})
	}
}

func TestTerminalCanonicalV2SharesGateAndCancellationJoins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBodyBlobs()
		s, owner := terminalOutputStore(t, b, 2, 1<<20)
		run, drafts, bodies, custody, capsule := canonicalChainData(t)
		if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		done := make(chan error, 1)
		joined := make(chan struct{})
		b.blockPut = release
		ctx, cancel := context.WithCancel(t.Context())
		var once sync.Once
		t.Cleanup(func() {
			cancel()
			once.Do(func() { close(release) })
			synctest.Wait()
			select {
			case <-joined:
			default:
				t.Error("owned chain writer not joined")
			}
		})
		go func() { defer close(joined); done <- owner.WriteCanonicalEventsV2(ctx, run, drafts, bodies) }()
		synctest.Wait()
		alias := *owner
		if err := alias.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies); !errors.Is(err, operationbody.ErrBusy) {
			t.Errorf("canonical alias bypassed same body I/O gate: %v", err)
		}
		if _, err := s.Write(t.Context(), []byte("ordinary")); !errors.Is(err, operationbody.ErrBusy) {
			t.Errorf("ordinary body bypassed canonical gate: %v", err)
		}
		if _, err := owner.ReserveVisibleOutput(t.Context(), 1); !errors.Is(err, operationbody.ErrBusy) {
			t.Errorf("visible output bypassed canonical gate: %v", err)
		}
		if err := owner.WriteCanonicalAssistant(t.Context(), run, drafts[2], bodies[2]); !errors.Is(err, operationbody.ErrBusy) {
			t.Errorf("V1 bypassed active V2 gate: %v", err)
		}
		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("cancelled canonical physical write succeeded")
			}
		default:
			t.Fatal("cancelled synchronous canonical write not joined")
		}
		if err := alias.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies); err == nil {
			t.Error("cancellation restored burned canonical lifetime")
		}
		if puts, _, _, _, _ := b.counts(); puts != 2 {
			t.Errorf("cancelled original PUT retried: puts=%d want=2", puts)
		}
	})
}
