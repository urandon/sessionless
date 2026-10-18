// Package canonicaloutput factors the worker's canonical output encoding and
// upload preparation. Its plans are DATA: they establish no operation lineage,
// retained custody, cleanup receipt or permission to finalize a Run.
package canonicaloutput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

// EventV1 owns Payload, which must be cleared by its composer after use. These
// exported fields are a pure encoding result, not an authenticated capability.
type EventV1 struct {
	ID          domain.SessionEventID
	Kind        domain.SessionEventKind
	Payload     []byte
	DisplayText string
}

func (EventV1) String() string     { return "CanonicalOutputEventV1{content:[redacted]}" }
func (v EventV1) GoString() string { return v.String() }

// PlanV1 contains one complete, validated tool batch followed by one assistant.
// Release clears only buffers allocated by this encoder, never harness input.
type PlanV1 struct{ events []EventV1 }

// Events returns borrowed payload views in a separate metadata slice. Retargeting
// a view cannot redirect Release onto caller storage or detach an owned buffer.
// Payload bytes remain owned by the plan until Release; they are not authority.
func (p *PlanV1) Events() []EventV1 {
	if p == nil {
		return nil
	}
	return append([]EventV1(nil), p.events...)
}

func (p *PlanV1) Release() {
	if p == nil {
		return
	}
	for i := range p.events {
		clear(p.events[i].Payload)
		p.events[i].Payload = nil
		p.events[i].DisplayText = ""
	}
	p.events = nil
}

func (PlanV1) String() string     { return "CanonicalOutputPlanV1{content:[redacted]}" }
func (p PlanV1) GoString() string { return p.String() }
func (PlanV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("canonical output plan is transient encoding data")
}

type assistantEnvelope struct {
	Schema             string                    `json:"schema"`
	Summary            string                    `json:"summary"`
	ArtifactManifestID domain.ArtifactManifestID `json:"artifact_manifest_id"`
}

type toolEnvelope struct {
	Schema   string          `json:"schema"`
	CallID   string          `json:"call_id"`
	ToolName string          `json:"tool_name"`
	Content  json.RawMessage `json:"content"`
}

type failureEnvelope struct {
	Schema    string `json:"schema"`
	Code      string `json:"code"`
	Cancelled bool   `json:"cancelled"`
}

func toolBytesError() error {
	return domain.ValidationError{Field: "execution_result.tool_events", Reason: "exceeds the admitted byte limit"}
}

// EncodeCompletionV1 preserves the original worker wire, ordering, stable IDs
// and summary normalization. All tools are checked before any publication.
// It performs no blob, SQL, provider or filesystem operation.
func EncodeCompletionV1(runID domain.RunID, limits domain.ProductLimits, result ports.ExecutionResult, manifestID domain.ArtifactManifestID) (_ *PlanV1, err error) {
	maxEvents, maxBytes := limits.EffectiveToolEventLimits()
	if uint64(len(result.ToolEvents)) > uint64(maxEvents) {
		return nil, domain.ValidationError{Field: "execution_result.tool_events", Reason: "exceeds the admitted event count limit"}
	}
	plan := &PlanV1{events: make([]EventV1, 0, len(result.ToolEvents)+1)}
	success := false
	defer func() {
		if !success {
			plan.Release()
		}
	}()
	var rawBytes, encodedBytes uint64
	for index, tool := range result.ToolEvents {
		if uint64(len(tool.Payload)) > maxBytes-rawBytes {
			return nil, toolBytesError()
		}
		rawBytes += uint64(len(tool.Payload))
		if tool.Kind != domain.SessionEventToolCall && tool.Kind != domain.SessionEventToolResult {
			return nil, domain.ValidationError{Field: "execution_tool_event.kind", Reason: "must be tool_call or tool_result"}
		}
		if strings.TrimSpace(tool.CallID) == "" || strings.TrimSpace(tool.ToolName) == "" {
			return nil, domain.ValidationError{Field: "execution_tool_event", Reason: "requires call and tool names"}
		}
		if !json.Valid(tool.Payload) {
			return nil, domain.ValidationError{Field: "execution_tool_event.payload", Reason: "must be valid JSON"}
		}
		// Each encoded name is at least as long as its source. Reject an
		// impossible envelope before copying potentially large metadata.
		remaining := maxBytes - encodedBytes
		if uint64(len(tool.CallID)) > remaining || uint64(len(tool.ToolName)) > remaining-uint64(len(tool.CallID)) {
			return nil, toolBytesError()
		}
		payload, err := json.Marshal(toolEnvelope{Schema: "sessionless.tool-event.v1", CallID: tool.CallID, ToolName: tool.ToolName, Content: json.RawMessage(tool.Payload)})
		if err != nil {
			return nil, err
		}
		if uint64(len(payload)) > remaining {
			clear(payload)
			return nil, toolBytesError()
		}
		encodedBytes += uint64(len(payload))
		plan.events = append(plan.events, EventV1{
			ID:   domain.SessionEventID(stableID("evt", string(runID), fmt.Sprintf("tool-%04d", index+1), string(tool.Kind), tool.CallID)),
			Kind: tool.Kind, Payload: payload,
		})
	}
	summary := NormalizedSummary(result.Summary)
	payload, err := json.Marshal(assistantEnvelope{Schema: "sessionless.assistant-message.v1", Summary: summary, ArtifactManifestID: manifestID})
	if err != nil {
		return nil, err
	}
	plan.events = append(plan.events, EventV1{ID: domain.SessionEventID(stableID("evt", string(runID), "assistant")), Kind: domain.SessionEventAssistantMessage, Payload: payload, DisplayText: summary})
	success = true
	return plan, nil
}

// EncodeFailureV1 produces the existing single SystemNotice, not an assistant
// or a replacement for the worker's cancelled/failed terminal transaction.
func EncodeFailureV1(runID domain.RunID, cancelled bool, code string) (EventV1, error) {
	payload, err := json.Marshal(failureEnvelope{Schema: "sessionless.run-terminal-notice.v1", Code: code, Cancelled: cancelled})
	if err != nil {
		return EventV1{}, err
	}
	return EventV1{ID: domain.SessionEventID(stableID("evt", string(runID), "terminal-notice")), Kind: domain.SessionEventSystemNotice, Payload: payload}, nil
}

// PutEventDraftV1 is the original canonical uploader and bounded material
// preparation, shared with Manager. It does not register a custody plan or
// attest immutable readback/operation provenance. An owned terminal producer
// must establish those independently before erasing original material.
func PutEventDraftV1(ctx context.Context, blobs ports.BlobStore, run domain.Run, eventID domain.SessionEventID, kind domain.SessionEventKind, payload []byte, at time.Time) (domain.SessionEventDraft, *sessionmaterial.PreparedV1, error) {
	draft, err := PlanEventDraftV1(run, eventID, kind, payload, at)
	if err != nil {
		return domain.SessionEventDraft{}, nil, err
	}
	ref, err := blobs.Put(ctx, run.TenantID, draft.Payload.Key, bytes.NewReader(payload))
	if err != nil {
		return domain.SessionEventDraft{}, nil, err
	}
	if err := sessionmaterial.ValidateStoredPayloadV1(run.TenantID, draft.Payload.Key, ref, payload); err != nil {
		return domain.SessionEventDraft{}, nil, err
	}
	return draft, ports.PrepareCanonicalDraftV1(run, draft, payload), nil
}

// PlanEventDraftV1 is the SAME canonical identity/key encoding without I/O.
// A predicted reference is DATA, never evidence of upload or immutable custody.
func PlanEventDraftV1(run domain.Run, eventID domain.SessionEventID, kind domain.SessionEventKind, payload []byte, at time.Time) (domain.SessionEventDraft, error) {
	sum := sha256.Sum256(payload)
	key := domain.SessionEventObjectPrefix(run.TenantID, run.SessionID, eventID) + "payloads/sha256/" + hex.EncodeToString(sum[:]) + ".json"
	ref := domain.BlobRef{TenantID: run.TenantID, Key: key, Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:])}
	draft := domain.SessionEventDraft{ID: eventID, Kind: kind,
		IdempotencyKey: domain.IdempotencyKey(stableID("evtkey", string(run.ID), string(eventID))), Payload: ref, CreatedAt: at}
	if err := draft.ValidateForRun(run); err != nil {
		return domain.SessionEventDraft{}, err
	}
	return draft, nil
}

// ManifestIDV1 is the original worker's stable public output manifest identity.
func ManifestIDV1(runID domain.RunID) domain.ArtifactManifestID {
	return domain.ArtifactManifestID(stableID("art", string(runID)))
}

func NormalizedSummary(summary string) string {
	summary = strings.TrimSpace(summary)
	if summary == "" {
		return "Run completed."
	}
	return summary
}

func stableID(prefix string, values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	return prefix + "_" + hex.EncodeToString(hash.Sum(nil)[:16])
}
