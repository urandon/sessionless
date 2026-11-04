package operationbody

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

// Four tool dispatches are the unchanged AgentLoopLimitsV1 hard ceiling.
// Canonical bytes retain their existing independent allowance, not another
// ordinary-body or artifact debit. The original admitted product may be lower.
const maxCanonicalToolEventsV2 = 8

type canonicalPublicationV2 struct {
	draft domain.SessionEventDraft
	body  []byte
	used  bool
}

// WriteCanonicalEventsV2 is the original keeper's one-use physical closure for
// a complete ordered call/result batch and final assistant. It is NOT Store
// registration or publication authority. The original composer must rejoin the
// SAME current Store, registered capsule, permissions, fence and cancellation
// before calling; caller-supplied Run/drafts remain DATA. No BlobStore/path,
// callback, capacity override or generic event writer is exposed here.
//
// The complete bounded plan is privately copied and validated before any PUT.
// A failure burns the whole publication (including every remaining event);
// partial objects remain owned residue, never retryable physical work. V1 and
// V2 share one lifetime, gate and assistant slot; failure-notice custody remains
// separate and cannot clear uncertainty.
func (t *TerminalOutputV1) WriteCanonicalEventsV2(ctx context.Context, run domain.Run, drafts []domain.SessionEventDraft, bodies [][]byte) error {
	return t.writeCanonicalEvents(ctx, run, drafts, bodies, false)
}

// Only the two closed public methods select this encoding arm. The original
// batch/assistant slot and protected notice slot are shared, never replenished.
// A stopped first consent consumes that same batch slot without an assistant
// or a fabricated SEARCH call/result pair.
func (t *TerminalOutputV1) writeCanonicalEvents(ctx context.Context, run domain.Run, drafts []domain.SessionEventDraft, bodies [][]byte, failure bool) error {
	// A first stopped consent has no SEARCH pair to publish. Only its strict
	// V4 notice below can consume a singleton failure batch; every other arm
	// retains the original complete-pair minimum and the same physical slots.
	singleton := failure && len(drafts) == 1
	if t.OperationBodyLimit() == 0 || len(drafts) < 3 && !singleton || len(drafts) > maxCanonicalToolEventsV2+1 || len(drafts)%2 != 1 || len(drafts) != len(bodies) {
		return ErrBody
	}
	s := StoreV1{state: t.state}
	call, finish, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	st := t.state
	if call.Err() != nil || st.writeFailed || t.owner.assistantUsed || (failure && t.owner.failure.used) || !t.owner.capsule.used ||
		run.Validate() != nil || run.Status != domain.RunRunning || run.CancellationRequestedAt != nil ||
		run.ID != st.scope.RunID || run.TenantID != st.scope.TenantID || run.SessionID != st.scope.SessionID ||
		run.TerminalCustody == nil || run.TerminalCustody.Plan.Scope != st.scope ||
		run.TerminalCustody.Plan.Capsule != t.owner.capsule.ref || uint64(len(drafts)-1) > uint64(t.owner.canonicalToolEvents) {
		return ErrBody
	}
	// Burn before copying, authentication or possible I/O. An alias cannot
	// select a different version/plan after any uncertainty.
	t.owner.assistantUsed = true
	st.writeFailed = true
	if failure {
		t.owner.failure.used = true
		t.owner.failure.ref = drafts[len(drafts)-1].Payload
	}
	run = run.Clone()
	plan := make([]canonicalPublicationV2, len(drafts))
	defer func() {
		for i := range plan {
			clear(plan[i].body)
			plan[i] = canonicalPublicationV2{}
		}
	}()
	var toolBytes uint64
	for i, draft := range drafts {
		body := bodies[i]
		if len(body) == 0 || len(body) > sessionmaterial.MaxPayloadBytesV1 || int64(len(body)) != draft.Payload.Size || len(draft.DisplayText) > 4*32_000 {
			return ErrBody
		}
		if i != len(drafts)-1 {
			if uint64(len(body)) > t.owner.canonicalToolBytes-toolBytes || draft.DisplayText != "" {
				return ErrBody
			}
			toolBytes += uint64(len(body))
		} else if failure {
			matches := false
			if uint64(len(body)) > t.owner.failure.limit {
				return ErrBody
			}
			var tag struct {
				Schema string `json:"schema"`
			}
			if json.Unmarshal(body, &tag) != nil {
				return ErrBody
			}
			if tag.Schema == "sessionless.run-terminal-notice.v4" {
				notice, e := domain.DecodeSearchFailureNoticeV4(body)
				if e != nil || draft.DisplayText != "" || singleton && notice.Code != domain.SearchConsentStoppedV4 {
					return ErrBody
				}
				// This derives only the existing stable Run notice identity. The
				// shared V4 codec owns its exact DATA grammar; original producer
				// and capsule/Store checks still authenticate the tail facts.
				identity, e := canonicaloutput.EncodeFailureV1(run.ID, false, notice.Code)
				matches = e == nil && draft.ID == identity.ID
				clear(identity.Payload)
			} else {
				if singleton {
					return ErrBody
				}
				for _, stop := range []string{domain.KnownMCPServiceErrorV1, domain.ModelOutcomeUnknownV2, domain.MCPOutcomeUnknownV3} {
					notice, e := canonicaloutput.EncodeFailureV1(run.ID, false, stop)
					matches = matches || e == nil && bytes.Equal(notice.Payload, body) && draft.ID == notice.ID
					clear(notice.Payload)
				}
			}
			if !matches {
				return ErrBody
			}
		}
		plan[i] = canonicalPublicationV2{draft: draft, body: bytes.Clone(body)}
	}
	seen := make(map[domain.SessionEventID]bool, len(plan))
	seenCalls := make(map[string]bool, len(plan)/2)
	var previousCall, previousName string
	for i := range plan {
		e := &plan[i]
		wantKind := domain.SessionEventToolCall
		if i == len(plan)-1 {
			wantKind = domain.SessionEventAssistantMessage
			if failure {
				wantKind = domain.SessionEventSystemNotice
			}
		} else if i%2 == 1 {
			wantKind = domain.SessionEventToolResult
		}
		if e.draft.Kind != wantKind || seen[e.draft.ID] || (i > 0 && e.draft.CreatedAt.Before(plan[i-1].draft.CreatedAt)) {
			return ErrBody
		}
		seen[e.draft.ID] = true
		expected, err := canonicaloutput.PlanEventDraftV1(run, e.draft.ID, e.draft.Kind, e.body, e.draft.CreatedAt)
		expected.DisplayText = e.draft.DisplayText
		if err != nil || !reflect.DeepEqual(expected, e.draft) {
			return ErrBody
		}
		prepared := ports.PrepareCanonicalDraftV1(run, e.draft, e.body)
		if prepared == nil {
			return ErrBody
		}
		prepared.Release()
		if i != len(plan)-1 {
			var identity struct {
				CallID   string `json:"call_id"`
				ToolName string `json:"tool_name"`
			}
			if json.Unmarshal(e.body, &identity) != nil {
				return ErrBody
			}
			if i%2 == 0 {
				if seenCalls[identity.CallID] {
					return ErrBody
				}
				seenCalls[identity.CallID] = true
				previousCall, previousName = identity.CallID, identity.ToolName
			} else if identity.CallID != previousCall || identity.ToolName != previousName {
				return ErrBody
			}
		}
	}
	for i := range plan {
		e := &plan[i]
		if e.used || call.Err() != nil {
			return ErrBody
		}
		e.used = true // irrevocable BEFORE this event's possible physical I/O
		actual, prepared, err := canonicaloutput.PutEventDraftV1(call, st.blobs, run, e.draft.ID, e.draft.Kind, e.body, e.draft.CreatedAt)
		actual.DisplayText = e.draft.DisplayText
		preparedOK := prepared != nil
		if prepared != nil {
			prepared.Release()
		}
		if err != nil || !preparedOK || !reflect.DeepEqual(actual, e.draft) || call.Err() != nil {
			return ErrBody
		}
		confirmed, err := st.readVerified(call, e.draft.Payload)
		equal := bytes.Equal(confirmed, e.body)
		clear(confirmed)
		if err != nil || !equal || call.Err() != nil {
			return ErrBody
		}
	}
	st.writeFailed = false
	return nil
}
