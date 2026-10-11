// Package sessionmaterial prepares bounded canonical source bytes. Preparation
// proves neither access rights nor whole-history, token or execution readiness.
package sessionmaterial

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"

	"gitcode.com/urandon/sessionless/internal/canonicalprompt"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	MaxPayloadBytesV1            = 1 << 20
	MaxIdentityMetadataBytesV1   = 8 << 10
	MaxAttachmentRefsV1          = 64
	MaxAttachmentMetadataBytesV1 = 64 << 10
)

// ErrNotPrepared is content-free unknown projection evidence, not a new
// canonical-write prohibition. Neither failure nor the zero value proves zero.
var ErrNotPrepared = errors.New("canonical material not prepared")

// MeasuresV1 contains source-derived scalars, not a forgeable authority object.
// Consumers must obtain it through a live opaque preparation/final measure.
// Attachment bytes are referenced sizes, NOT verified body bytes or media class.
type MeasuresV1 struct {
	Kind                      domain.SessionEventKind
	PayloadBytes              uint64
	ToolEnvelopeBytes         uint64
	AttachmentCount           uint64
	ReferencedAttachmentBytes uint64
	CodexCompatible           bool
	RenderedFrameBytes        uint64
}

// PreparedV1 owns PRIVATE bounded bytes. Ordinary value copies share the same
// release lifetime; Clone creates independent ownership. It has no serializer
// or accessor exposing mutable owned storage. The zero value is unverified.
type PreparedV1 struct{ state *preparedState }

type preparedState struct {
	mu       sync.RWMutex
	identity domain.SessionEvent
	payload  []byte
	measures MeasuresV1
	live     bool
}

// PrepareV1 seals stable identity, not provisional Sequence/CreatedAt. Uploads
// must be the authorized ordered ingress artifacts EXCLUDING message.json.
// Recognized sources are user-envelope v1, assistant-message v1, tool-event v1
// and run-terminal-notice v1. Actual codec round-trip is checked later, after
// final allocation, by sessioncontext. No I/O, caller classifier or clock runs.
func PrepareV1(event domain.SessionEvent, payload []byte, uploads []domain.Artifact) (*PreparedV1, error) {
	if len(payload) == 0 || len(payload) > MaxPayloadBytesV1 ||
		!boundedIdentity(event) || !boundedUploads(uploads) {
		return nil, ErrNotPrepared
	}
	identity := cloneIdentity(event)
	identity.Sequence = 0
	// Time is deliberately not retained in preparation identity.
	identity.CreatedAt = domain.SessionEvent{}.CreatedAt
	if identity.ValidateMaterialIdentity() != nil {
		return nil, ErrNotPrepared
	}
	body := bytes.Clone(payload)
	owned := false
	defer func() {
		if !owned {
			clear(body)
		}
	}()
	digest := sha256.Sum256(body)
	if identity.Payload.Size != int64(len(body)) || identity.Payload.SHA256 != hex.EncodeToString(digest[:]) {
		return nil, ErrNotPrepared
	}
	// Artifact values contain strings/scalars only; clone the slice so no input
	// slice is retained or consulted after preparation.
	refs := append([]domain.Artifact(nil), uploads...)
	measures, err := inspectEnvelope(identity, body, refs)
	if err != nil {
		return nil, ErrNotPrepared
	}
	frame, err := canonicalprompt.RenderFrameV1(identity.Kind, body)
	if err == nil {
		measures.CodexCompatible = true
		measures.RenderedFrameBytes = uint64(len(frame))
		clear(frame)
	}
	owned = true
	return &PreparedV1{state: &preparedState{identity: identity, payload: body, measures: measures, live: true}}, nil
}

// CopyForFinalV1 atomically matches stable identity against a VALID final event
// and returns caller-owned private bytes plus their source measures. The caller
// must clear this copy. Allocated Sequence/CreatedAt are never replaced here.
func (prepared *PreparedV1) CopyForFinalV1(event domain.SessionEvent) ([]byte, MeasuresV1, error) {
	if prepared == nil || prepared.state == nil || !boundedIdentity(event) || event.Validate() != nil {
		return nil, MeasuresV1{}, ErrNotPrepared
	}
	state := prepared.state
	state.mu.RLock()
	defer state.mu.RUnlock()
	if !state.live || !sameIdentity(state.identity, event) {
		return nil, MeasuresV1{}, ErrNotPrepared
	}
	return bytes.Clone(state.payload), state.measures, nil
}

func (prepared *PreparedV1) Clone() (*PreparedV1, error) {
	if prepared == nil || prepared.state == nil {
		return nil, ErrNotPrepared
	}
	state := prepared.state
	state.mu.RLock()
	defer state.mu.RUnlock()
	if !state.live {
		return nil, ErrNotPrepared
	}
	return &PreparedV1{state: &preparedState{identity: cloneIdentity(state.identity), payload: bytes.Clone(state.payload), measures: state.measures, live: true}}, nil
}

// OwnedBytesV1 is a memory-budget observation, not serialized source proof.
// It never exposes the payload or stable identity, and a released alias cannot
// claim that it still owns a measured buffer.
func (prepared *PreparedV1) OwnedBytesV1() (uint64, bool) {
	if prepared == nil || prepared.state == nil {
		return 0, false
	}
	prepared.state.mu.RLock()
	defer prepared.state.mu.RUnlock()
	if !prepared.state.live {
		return 0, false
	}
	return uint64(len(prepared.state.payload)), true
}

// Release invalidates all ordinary copies/aliases and clears owned bytes.
// Independent explicit clones retain their own lifetime. No implicit reuse or
// refresh can resurrect released preparation. Concurrent copy/release is safe.
func (prepared *PreparedV1) Release() {
	if prepared == nil || prepared.state == nil {
		return
	}
	state := prepared.state
	state.mu.Lock()
	defer state.mu.Unlock()
	clear(state.payload)
	state.payload = nil
	state.identity = domain.SessionEvent{}
	state.measures = MeasuresV1{}
	state.live = false
}

func (PreparedV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("canonical preparation cannot be serialized")
}
func (PreparedV1) Format(state fmt.State, verb rune) {
	_, _ = fmt.Fprint(state, "sessionmaterial.PreparedV1")
}

func boundedIdentity(event domain.SessionEvent) bool {
	remaining := MaxIdentityMetadataBytesV1
	values := []string{string(event.ID), string(event.TenantID), string(event.SessionID), string(event.Kind), string(event.IdempotencyKey), string(event.Payload.TenantID), event.Payload.Key, event.Payload.SHA256}
	if event.AuthorUserID != nil {
		values = append(values, string(*event.AuthorUserID))
	}
	if event.RunID != nil {
		values = append(values, string(*event.RunID))
	}
	for _, v := range values {
		if len(v) > remaining {
			return false
		}
		remaining -= len(v)
	}
	return true
}

func boundedUploads(uploads []domain.Artifact) bool {
	if len(uploads) > MaxAttachmentRefsV1 {
		return false
	}
	remaining := MaxAttachmentMetadataBytesV1
	for _, ref := range uploads {
		if len(ref.Name) > 4096 || len(ref.MediaType) > 4096 || len(ref.Blob.Key) > 4096 {
			return false
		}
		for _, v := range []string{ref.Name, ref.MediaType, string(ref.Blob.TenantID), ref.Blob.Key, ref.Blob.SHA256} {
			if len(v) > remaining {
				return false
			}
			remaining -= len(v)
		}
	}
	return true
}

func cloneIdentity(event domain.SessionEvent) domain.SessionEvent {
	if event.AuthorUserID != nil {
		v := *event.AuthorUserID
		event.AuthorUserID = &v
	}
	if event.RunID != nil {
		v := *event.RunID
		event.RunID = &v
	}
	return event
}

func sameIdentity(a, b domain.SessionEvent) bool {
	return a.ID == b.ID && a.TenantID == b.TenantID && a.SessionID == b.SessionID && a.Kind == b.Kind &&
		a.IdempotencyKey == b.IdempotencyKey && a.Payload == b.Payload &&
		((a.AuthorUserID == nil && b.AuthorUserID == nil) || (a.AuthorUserID != nil && b.AuthorUserID != nil && *a.AuthorUserID == *b.AuthorUserID)) &&
		((a.RunID == nil && b.RunID == nil) || (a.RunID != nil && b.RunID != nil && *a.RunID == *b.RunID))
}

func inspectEnvelope(event domain.SessionEvent, body []byte, uploads []domain.Artifact) (MeasuresV1, error) {
	m := MeasuresV1{Kind: event.Kind, PayloadBytes: uint64(len(body))}
	if event.Kind != domain.SessionEventUserMessage && len(uploads) != 0 {
		return MeasuresV1{}, ErrNotPrepared
	}
	switch event.Kind {
	case domain.SessionEventUserMessage:
		var value struct {
			Version     uint32            `json:"version"`
			Origin      json.RawMessage   `json:"origin"`
			Text        string            `json:"text"`
			Metadata    map[string]string `json:"metadata"`
			Attachments json.RawMessage   `json:"attachments"`
		}
		if decodeEnvelope(body, &value) != nil || value.Version != 1 {
			return MeasuresV1{}, ErrNotPrepared
		}
		descriptors, err := decodeAttachments(value.Attachments)
		if err != nil || len(descriptors) != len(uploads) || !boundedUploads(descriptors) {
			return MeasuresV1{}, ErrNotPrepared
		}
		for i, descriptor := range descriptors {
			ref := uploads[i]
			if descriptor.Name == "" || descriptor.Name == "." || descriptor.Name == ".." || len(descriptor.Name) > 4072 ||
				strings.TrimSpace(descriptor.Name) != descriptor.Name || strings.ContainsAny(descriptor.Name, "/\\\x00") ||
				ref.Name != fmt.Sprintf("attachment-%02d-%s", i+1, descriptor.Name) || ref.MediaType != descriptor.MediaType || ref.Blob != descriptor.Blob ||
				ref.Blob == event.Payload || ref.Validate() != nil || domain.ValidateSessionEventBlob(event.TenantID, event.SessionID, event.ID, ref.Blob) != nil ||
				uint64(ref.Blob.Size) > math.MaxInt64-m.ReferencedAttachmentBytes {
				return MeasuresV1{}, ErrNotPrepared
			}
			m.AttachmentCount++
			m.ReferencedAttachmentBytes += uint64(ref.Blob.Size)
		}
	case domain.SessionEventAssistantMessage:
		var value struct {
			Schema             string `json:"schema"`
			Summary            string `json:"summary"`
			ArtifactManifestID string `json:"artifact_manifest_id"`
		}
		if decodeEnvelope(body, &value) != nil || value.Schema != "sessionless.assistant-message.v1" {
			return MeasuresV1{}, ErrNotPrepared
		}
	case domain.SessionEventToolCall, domain.SessionEventToolResult:
		var value struct {
			Schema   string          `json:"schema"`
			CallID   string          `json:"call_id"`
			ToolName string          `json:"tool_name"`
			Content  json.RawMessage `json:"content"`
		}
		if decodeEnvelope(body, &value) != nil || value.Schema != "sessionless.tool-event.v1" || strings.TrimSpace(value.CallID) == "" || strings.TrimSpace(value.ToolName) == "" || !json.Valid(value.Content) {
			return MeasuresV1{}, ErrNotPrepared
		}
		m.ToolEnvelopeBytes = uint64(len(body))
	case domain.SessionEventSystemNotice:
		var value struct {
			Schema    string `json:"schema"`
			Code      string `json:"code"`
			Cancelled bool   `json:"cancelled"`
		}
		if decodeEnvelope(body, &value) != nil || value.Schema != "sessionless.run-terminal-notice.v1" || strings.TrimSpace(value.Code) == "" {
			return MeasuresV1{}, ErrNotPrepared
		}
	default:
		return MeasuresV1{}, ErrNotPrepared
	}
	return m, nil
}

func decodeEnvelope(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return ErrNotPrepared
	}
	return nil
}

func decodeAttachments(body []byte) ([]domain.Artifact, error) {
	if len(body) == 0 || bytes.Equal(body, []byte("null")) {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('[') {
		return nil, ErrNotPrepared
	}
	var refs []domain.Artifact
	for decoder.More() {
		if len(refs) == MaxAttachmentRefsV1 {
			return nil, ErrNotPrepared
		}
		var ref domain.Artifact
		if decoder.Decode(&ref) != nil {
			return nil, ErrNotPrepared
		}
		refs = append(refs, ref)
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim(']') || decoder.Decode(&struct{}{}) != io.EOF {
		return nil, ErrNotPrepared
	}
	return refs, nil
}
