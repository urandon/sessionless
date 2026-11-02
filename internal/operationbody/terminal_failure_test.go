package operationbody_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestTerminalFailureAliasesShareGateWithoutBurningLoser(t *testing.T) {
	for _, failureActive := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary_active", true: "failure_active"}[failureActive], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newBodyBlobs()
				s, owner := terminalOutputStore(t, b, 2, 1<<20)
				run, drafts, bodies, custody, capsule := failurePhysicalData(t)
				if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
					t.Fatal(err)
				}
				release, done, joined := make(chan struct{}), make(chan error, 1), make(chan struct{})
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
						t.Error("original physical writer not joined")
					}
				})
				go func() {
					defer close(joined)
					if failureActive {
						done <- owner.WriteKnownMCPFailureV1(ctx, run, drafts, bodies)
					} else {
						_, err := s.Write(ctx, []byte("original ordinary body"))
						done <- err
					}
				}()
				synctest.Wait()
				alias := *owner
				before, _, _, _, _ := b.counts()
				if err := alias.WriteKnownMCPFailureV1(t.Context(), run, drafts, bodies); !errors.Is(err, operationbody.ErrBusy) {
					t.Error("failure alias inspected/bypassed active slot outside original gate", err)
				}
				if after, _, _, _, _ := b.counts(); after != before {
					t.Error("busy loser performed physical I/O")
				}
				once.Do(func() { close(release) })
				synctest.Wait()
				if err := <-done; err != nil {
					t.Fatal("original writer", err)
				}
				if !failureActive {
					if err := owner.WriteKnownMCPFailureV1(t.Context(), run, drafts, bodies); err != nil {
						t.Fatal("busy loser burned unused batch/notice", err)
					}
				}
				if err := alias.WriteKnownMCPFailureV1(t.Context(), run, drafts, bodies); err == nil {
					t.Error("used shared batch/notice reopened")
				}
				want := 4
				if !failureActive {
					want++
				}
				if puts, opens, closes, deletes, _ := b.counts(); puts != want || opens != want || closes != want || deletes != 0 {
					t.Error("original physical effect counts changed", puts, opens, closes, deletes)
				}
			})
		})
	}
}

func failurePhysicalData(t *testing.T) (domain.Run, []domain.SessionEventDraft, [][]byte, domain.TerminalCustodyPlanV1, []byte) {
	t.Helper()
	run, _, _, custody, capsule := canonicalTerminalData(t)
	limits, _ := terminalOutputLimits(1, 1<<20)
	limits.MaxToolEvents = 2
	limits.MaxToolEventBytes = 4096
	p, err := canonicaloutput.EncodeKnownMCPServiceFailureV1(run.ID, limits, []ports.ExecutionToolEvent{{Kind: domain.SessionEventToolCall, CallID: "call-a", ToolName: "lookup", Payload: []byte(`{"q":"<x>"}`)}, {Kind: domain.SessionEventToolResult, CallID: "call-a", ToolName: "lookup", Payload: []byte(`{"schema":"sessionless.tool-outcome.v1","outcome":"service_error"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release()
	var drafts []domain.SessionEventDraft
	var bodies [][]byte
	for _, e := range p.Events() {
		draft, err := canonicaloutput.PlanEventDraftV1(run, e.ID, e.Kind, e.Payload, run.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
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

func TestTerminalFailureCompletePrefixSharesOriginalEightEventCap(t *testing.T) {
	b := newBodyBlobs()
	s := retainedStore(t, b, retainedLimits())
	product, substrate := terminalOutputLimits(1, 1<<20)
	product.MaxToolEvents = 8
	product.MaxToolEventBytes = 16384
	owner, err := s.AdmitTerminalOutput(t.Context(), product, substrate)
	if err != nil {
		t.Fatal(err)
	}
	run, _, _, custody, capsule := canonicalTerminalData(t)
	var tools []ports.ExecutionToolEvent
	for i := range 4 {
		id := fmt.Sprintf("call-%d", i)
		tools = append(tools, ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: id, ToolName: "lookup", Payload: []byte(`{}`)}, ports.ExecutionToolEvent{Kind: domain.SessionEventToolResult, CallID: id, ToolName: "lookup", Payload: []byte(`{"outcome":"known"}`)})
	}
	p, err := canonicaloutput.EncodeKnownMCPServiceFailureV1(run.ID, product, tools)
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
	if owner.WriteCapsule(t.Context(), custody, capsule) != nil || owner.WriteKnownMCPFailureV1(t.Context(), run, drafts, bodies) != nil {
		t.Fatal("complete bounded DATA batch rejected")
	}
	if puts, opens, closes, deletes, _ := b.counts(); puts != 10 || opens != 10 || closes != 10 || deletes != 0 {
		t.Fatal("multi-pair physical batch repeated/erased", puts, opens, closes, deletes)
	}
	if owner.WriteKnownMCPFailureV1(t.Context(), run, drafts, bodies) == nil {
		t.Fatal("multi-pair original batch replayed")
	}
}

func TestTerminalFailureSameBatchNoticeAndRetainedAliasFence(t *testing.T) {
	b := newBodyBlobs()
	s, owner := terminalOutputStore(t, b, 1, 1<<20)
	ordinary, err := s.Write(t.Context(), []byte("ordinary retained private DATA"))
	if err != nil {
		t.Fatal(err)
	}
	refs, err := owner.SnapshotRetainedBodies(t.Context())
	if err != nil || len(refs) != 1 {
		t.Fatal("actual attempted namespace snapshot", err)
	}
	derived, err := operationbody.RetainedOperationBodyRefV1(bodyScope(), ordinary.Ref, ordinary.Bytes, ordinary.Digest)
	if err != nil || derived != refs[0] || operationbody.ValidateRetainedBodiesV1(bodyScope(), refs) != nil {
		t.Fatal("DATA key derivation differs from actual adapter", err)
	}
	run, drafts, bodies, custody, capsule := failurePhysicalData(t)
	if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
		t.Fatal(err)
	}
	if err := owner.WriteKnownMCPFailureV1(t.Context(), run, drafts, bodies); err != nil {
		t.Fatal("original F1 physical batch", err)
	}
	alias := *owner
	copyStore := *s
	if alias.WriteKnownMCPFailureV1(t.Context(), run, drafts, bodies) == nil || alias.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies) == nil || alias.WriteFailureNotice(t.Context(), drafts[2], bodies[2]) == nil {
		t.Error("batch or protected notice reset across arms/aliases")
	}
	receipt, err := owner.FenceRetainedBodies(t.Context(), refs)
	if err != nil || owner.VerifyRetainedBodies(receipt) != nil {
		t.Fatal("original retained fence", err)
	}
	if _, err := copyStore.Write(t.Context(), []byte("after fence")); err == nil {
		t.Fatal("physical writer alias survived original fence")
	}
	copied := *receipt
	if owner.VerifyRetainedBodies(&copied) == nil {
		t.Error("copied retained receipt accepted")
	}
	if _, err := json.Marshal(receipt); err == nil {
		t.Error("physical receipt serialized")
	}
	if puts, _, _, deletes, _ := b.counts(); puts != 5 || deletes != 0 {
		t.Fatal("retained route fabricated erase or repeated write", puts, deletes)
	}
	if !bytes.Equal(b.objects[refs[0].Key], []byte("ordinary retained private DATA")) {
		t.Error("retained fence erased ordinary bytes")
	}
}

func TestTerminalFailureInvalidNoticeBurnsWithoutExtraEffects(t *testing.T) {
	for _, mode := range []string{"wrong_notice", "missing_capsule", "partial_put"} {
		t.Run(mode, func(t *testing.T) {
			b := newBodyBlobs()
			_, owner := terminalOutputStore(t, b, 1, 1<<20)
			run, drafts, bodies, custody, capsule := failurePhysicalData(t)
			if mode != "missing_capsule" {
				if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
					t.Fatal(err)
				}
			}
			before, _, _, _, _ := b.counts()
			if mode == "wrong_notice" {
				notice, _ := canonicaloutput.EncodeFailureV1(run.ID, true, "cancelled")
				defer clear(notice.Payload)
				bodies[2] = notice.Payload
				draft, err := canonicaloutput.PlanEventDraftV1(run, notice.ID, notice.Kind, notice.Payload, run.CreatedAt)
				if err != nil {
					t.Fatal(err)
				}
				drafts[2] = draft
			}
			if mode == "partial_put" {
				b.putFault = "lost_ack"
			}
			if owner.WriteKnownMCPFailureV1(t.Context(), run, drafts, bodies) == nil {
				t.Fatal("invalid/uncertain F1 batch accepted")
			}
			after, _, _, _, _ := b.counts()
			if mode != "partial_put" && after != before {
				t.Fatal("invalid whole batch wrote events")
			}
			if owner.WriteKnownMCPFailureV1(t.Context(), run, drafts, bodies) == nil {
				t.Error("failed physical publication retried")
			}
		})
	}
}
