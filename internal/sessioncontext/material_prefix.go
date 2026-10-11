package sessioncontext

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/canonicalprompt"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const MaxSessionMaterialPrefixBytesV1 = 8 << 10

type SessionMaterialCoverageV1 string

const (
	SessionMaterialCompleteV1 SessionMaterialCoverageV1 = "complete"
	SessionMaterialUnknownV1  SessionMaterialCoverageV1 = "unknown"
)

type SessionMaterialUnknownReasonsV1 uint32

const (
	SessionMaterialLegacyV1 SessionMaterialUnknownReasonsV1 = 1 << iota
	SessionMaterialMissingInvalidV1
	SessionMaterialUnsupportedV1
	SessionMaterialNonRoundTripV1
	SessionMaterialSaturatedV1
	materialReasonMask = SessionMaterialLegacyV1 | SessionMaterialMissingInvalidV1 | SessionMaterialUnsupportedV1 | SessionMaterialNonRoundTripV1 | SessionMaterialSaturatedV1
)

var ErrSessionMaterialPrefix = errors.New("canonical material prefix invalid")

// SessionMaterialContributionV1 is a bounded persisted fact, not a public proof
// of preparation. IdentityDigest seals ALL final metadata, including optional
// presence, BlobRef and the codec's timestamp representation, without retaining
// a potentially huge private object key. Only FoldSessionMaterialV1 obtains a
// verified contribution from the opaque FinalMaterialMeasureV1.
type SessionMaterialContributionV1 struct {
	Version                   uint32                  `json:"version"`
	EventID                   domain.SessionEventID   `json:"event_id"`
	Sequence                  uint64                  `json:"sequence"`
	Kind                      domain.SessionEventKind `json:"kind"`
	IdentityDigest            string                  `json:"identity_digest"`
	RecordDigest              string                  `json:"record_digest"`
	Verified                  bool                    `json:"verified"`
	PayloadBytes              uint64                  `json:"payload_bytes"`
	EncodedRecordBytes        uint64                  `json:"encoded_record_bytes"`
	ToolEnvelopeBytes         uint64                  `json:"tool_envelope_bytes"`
	AttachmentCount           uint64                  `json:"attachment_count"`
	ReferencedAttachmentBytes uint64                  `json:"referenced_attachment_bytes"`
	CodexCompatible           bool                    `json:"codex_compatible"`
	RenderedFrameBytes        uint64                  `json:"rendered_frame_bytes"`
}

// SessionMaterialPrefixV1 is the SAME private common record for current and
// immutable boundary rows. Scalars are source facts, never permission, token,
// cost or eligibility authority. RenderedFrameBytes excludes the fixed header.
// The only records without LastEventID/LastContribution are exact reproducible
// genesis or explicit unknown-legacy seeds; a legacy seed never repairs genesis.
type SessionMaterialPrefixV1 struct {
	Version                   uint32                          `json:"version"`
	TenantID                  domain.TenantID                 `json:"tenant_id"`
	SessionID                 domain.SessionID                `json:"session_id"`
	ThroughSequence           uint64                          `json:"through_sequence"`
	LastEventID               domain.SessionEventID           `json:"last_event_id,omitempty"`
	RecordSchema              string                          `json:"record_schema"`
	RendererContract          string                          `json:"renderer_contract"`
	Coverage                  SessionMaterialCoverageV1       `json:"coverage"`
	UnknownReasons            SessionMaterialUnknownReasonsV1 `json:"unknown_reasons"`
	PrefixDigest              string                          `json:"prefix_digest"`
	LastContribution          *SessionMaterialContributionV1  `json:"last_contribution,omitempty"`
	UserMessageCount          uint64                          `json:"user_message_count"`
	AssistantMessageCount     uint64                          `json:"assistant_message_count"`
	ToolCallCount             uint64                          `json:"tool_call_count"`
	ToolResultCount           uint64                          `json:"tool_result_count"`
	SystemNoticeCount         uint64                          `json:"system_notice_count"`
	VerifiedPayloadBytes      uint64                          `json:"verified_payload_bytes"`
	EncodedJSONLBytes         uint64                          `json:"encoded_jsonl_bytes"`
	MaxEncodedRecordBytes     uint64                          `json:"max_encoded_record_bytes"`
	ToolEnvelopeBytes         uint64                          `json:"tool_envelope_bytes"`
	AttachmentCount           uint64                          `json:"attachment_count"`
	ReferencedAttachmentBytes uint64                          `json:"referenced_attachment_bytes"`
	CodexCompatibleRecords    uint64                          `json:"codex_compatible_records"`
	CodexIncompatibleRecords  uint64                          `json:"codex_incompatible_records"`
	RenderedFrameBytes        uint64                          `json:"rendered_frame_bytes"`
	UnknownRecords            uint64                          `json:"unknown_records"`
}

func (p SessionMaterialPrefixV1) Clone() SessionMaterialPrefixV1 {
	if p.LastContribution != nil {
		c := *p.LastContribution
		p.LastContribution = &c
	}
	return p
}

func (SessionMaterialPrefixV1) Format(s fmt.State, _ rune) {
	_, _ = fmt.Fprint(s, "sessioncontext.SessionMaterialPrefixV1")
}
func (SessionMaterialContributionV1) Format(s fmt.State, _ rune) {
	_, _ = fmt.Fprint(s, "sessioncontext.SessionMaterialContributionV1")
}

func EqualSessionMaterialPrefixV1(a, b SessionMaterialPrefixV1) bool {
	ac, bc := a.LastContribution, b.LastContribution
	a.LastContribution, b.LastContribution = nil, nil
	return a == b && ((ac == nil && bc == nil) || (ac != nil && bc != nil && *ac == *bc))
}

func NewSessionMaterialGenesisV1(tenantID domain.TenantID, sessionID domain.SessionID) (SessionMaterialPrefixV1, error) {
	if tenantID.Validate() != nil || sessionID.Validate() != nil {
		return SessionMaterialPrefixV1{}, ErrSessionMaterialPrefix
	}
	p := materialBase(tenantID, sessionID)
	p.PrefixDigest = materialSeedDigest("sessionless.session-material.genesis.v1", tenantID, sessionID, 0)
	return p, nil
}

func NewUnknownSessionMaterialPrefixV1(tenantID domain.TenantID, sessionID domain.SessionID, priorSequence uint64) (SessionMaterialPrefixV1, error) {
	p, err := NewSessionMaterialGenesisV1(tenantID, sessionID)
	if err != nil {
		return SessionMaterialPrefixV1{}, err
	}
	p.ThroughSequence, p.Coverage, p.UnknownReasons = priorSequence, SessionMaterialUnknownV1, SessionMaterialLegacyV1
	p.UnknownRecords = min(priorSequence, uint64(math.MaxInt64))
	if priorSequence > math.MaxInt64 {
		p.UnknownReasons |= SessionMaterialSaturatedV1
	}
	p.PrefixDigest = materialSeedDigest("sessionless.session-material.unknown-legacy.v1", tenantID, sessionID, priorSequence)
	return p, nil
}

func materialBase(tenantID domain.TenantID, sessionID domain.SessionID) SessionMaterialPrefixV1 {
	return SessionMaterialPrefixV1{Version: 1, TenantID: tenantID, SessionID: sessionID, RecordSchema: RecordSchemaV1, RendererContract: canonicalprompt.ContractV1, Coverage: SessionMaterialCompleteV1}
}

// FoldSessionMaterialV1 never allocates sequence/time or manufactures source
// proof from a public scalar. Nil, zero, mismatched and failed-round-trip
// measures add an explicit unknown contribution. Ordinary valid canonical
// metadata, even beyond preparation's inspection ceiling, can still be hashed
// in fixed-size chunks; it is not retained or newly prohibited by this fold.
func FoldSessionMaterialV1(previous SessionMaterialPrefixV1, event domain.SessionEvent, measure *FinalMaterialMeasureV1) (SessionMaterialPrefixV1, error) {
	if previous.Validate() != nil || event.Validate() != nil || previous.TenantID != event.TenantID || previous.SessionID != event.SessionID || previous.ThroughSequence == math.MaxUint64 || event.Sequence != previous.ThroughSequence+1 {
		return SessionMaterialPrefixV1{}, ErrSessionMaterialPrefix
	}
	c := SessionMaterialContributionV1{Version: 1, EventID: event.ID, Sequence: event.Sequence, Kind: event.Kind, IdentityDigest: materialIdentityDigest(event)}
	if measure != nil && measure.Matches(event) {
		r, recordOK := measure.Record()
		m, sourceOK := measure.SourceMeasures()
		digest, digestOK := r.Digest()
		size, sizeOK := r.EncodedBytes()
		if recordOK && sourceOK && digestOK && sizeOK && size > 0 && m.Kind == event.Kind {
			c.RecordDigest, c.Verified = digest, true
			c.PayloadBytes, c.EncodedRecordBytes, c.ToolEnvelopeBytes = m.PayloadBytes, uint64(size), m.ToolEnvelopeBytes
			c.AttachmentCount, c.ReferencedAttachmentBytes = m.AttachmentCount, m.ReferencedAttachmentBytes
			c.CodexCompatible, c.RenderedFrameBytes = m.CodexCompatible, m.RenderedFrameBytes
		}
	}
	if c.validate() != nil {
		return SessionMaterialPrefixV1{}, ErrSessionMaterialPrefix
	}
	next := previous.Clone()
	next.ThroughSequence, next.LastEventID, next.LastContribution = event.Sequence, event.ID, &c
	add := func(value *uint64, amount uint64) {
		if amount > math.MaxInt64-*value {
			*value = math.MaxInt64
			next.UnknownReasons |= SessionMaterialSaturatedV1
		} else {
			*value += amount
		}
	}
	switch event.Kind {
	case domain.SessionEventUserMessage:
		add(&next.UserMessageCount, 1)
	case domain.SessionEventAssistantMessage:
		add(&next.AssistantMessageCount, 1)
	case domain.SessionEventToolCall:
		add(&next.ToolCallCount, 1)
	case domain.SessionEventToolResult:
		add(&next.ToolResultCount, 1)
	case domain.SessionEventSystemNotice:
		add(&next.SystemNoticeCount, 1)
	}
	if !c.Verified {
		next.UnknownReasons |= SessionMaterialMissingInvalidV1
		add(&next.UnknownRecords, 1)
	} else {
		add(&next.VerifiedPayloadBytes, c.PayloadBytes)
		add(&next.EncodedJSONLBytes, c.EncodedRecordBytes)
		next.MaxEncodedRecordBytes = max(next.MaxEncodedRecordBytes, c.EncodedRecordBytes)
		add(&next.ToolEnvelopeBytes, c.ToolEnvelopeBytes)
		add(&next.AttachmentCount, c.AttachmentCount)
		add(&next.ReferencedAttachmentBytes, c.ReferencedAttachmentBytes)
		if c.CodexCompatible {
			add(&next.CodexCompatibleRecords, 1)
			add(&next.RenderedFrameBytes, c.RenderedFrameBytes)
		} else {
			add(&next.CodexIncompatibleRecords, 1)
		}
	}
	if next.UnknownReasons != 0 {
		next.Coverage = SessionMaterialUnknownV1
	}
	encoded, _ := json.Marshal(c)
	previousEncoded, err := EncodeSessionMaterialPrefixV1(previous)
	if err != nil {
		return SessionMaterialPrefixV1{}, ErrSessionMaterialPrefix
	}
	h := sha256.New()
	// V1 fold frames this exact ordered tuple: domain, previous digest,
	// canonical EncodeSessionMaterialPrefixV1(previous) JSON, final identity
	// digest, record digest (empty for unknown), closed contribution marker,
	// canonical contribution JSON. Each frame is an eight-byte big-endian byte
	// length followed by its UTF-8 bytes. Both current and boundary consumers
	// use this engine; a digest alone is not a proof of source or permission.
	materialFrame(h, "sessionless.session-material.fold.v1")
	materialFrame(h, previous.PrefixDigest)
	materialFrame(h, string(previousEncoded))
	materialFrame(h, c.IdentityDigest)
	materialFrame(h, c.RecordDigest)
	if !c.Verified {
		materialFrame(h, "unknown-contribution.v1")
	} else {
		materialFrame(h, "verified-contribution.v1")
	}
	materialFrame(h, string(encoded))
	next.PrefixDigest = hex.EncodeToString(h.Sum(nil))
	if next.Validate() != nil {
		return SessionMaterialPrefixV1{}, ErrSessionMaterialPrefix
	}
	return next, nil
}

func (c SessionMaterialContributionV1) validate() error {
	if c.Version != 1 || c.EventID.Validate() != nil || c.Sequence == 0 || !c.Kind.Valid() || !materialDigest(c.IdentityDigest) {
		return ErrSessionMaterialPrefix
	}
	if !c.Verified {
		if c.RecordDigest != "" || c.PayloadBytes != 0 || c.EncodedRecordBytes != 0 || c.ToolEnvelopeBytes != 0 || c.AttachmentCount != 0 || c.ReferencedAttachmentBytes != 0 || c.CodexCompatible || c.RenderedFrameBytes != 0 {
			return ErrSessionMaterialPrefix
		}
		return nil
	}
	if !materialDigest(c.RecordDigest) || c.PayloadBytes == 0 || c.PayloadBytes > MaxMeasuredPayloadBytesV1 || c.EncodedRecordBytes < c.PayloadBytes || c.EncodedRecordBytes > MaxMeasuredEncodedRecordBytesV1 || c.AttachmentCount > 64 || c.ReferencedAttachmentBytes > math.MaxInt64 {
		return ErrSessionMaterialPrefix
	}
	isTool := c.Kind == domain.SessionEventToolCall || c.Kind == domain.SessionEventToolResult
	if (isTool && c.ToolEnvelopeBytes != c.PayloadBytes) || (!isTool && c.ToolEnvelopeBytes != 0) || (c.Kind != domain.SessionEventUserMessage && c.AttachmentCount != 0) || (c.AttachmentCount == 0 && c.ReferencedAttachmentBytes != 0) {
		return ErrSessionMaterialPrefix
	}
	if c.CodexCompatible {
		if (c.Kind != domain.SessionEventUserMessage && c.Kind != domain.SessionEventAssistantMessage) || c.AttachmentCount != 0 || c.RenderedFrameBytes == 0 || c.RenderedFrameBytes > canonicalprompt.MaxFrameBytesV1 {
			return ErrSessionMaterialPrefix
		}
	} else if c.RenderedFrameBytes != 0 {
		return ErrSessionMaterialPrefix
	}
	return nil
}

func (p SessionMaterialPrefixV1) Validate() error {
	if p.Version != 1 || p.TenantID.Validate() != nil || p.SessionID.Validate() != nil || p.RecordSchema != RecordSchemaV1 || p.RendererContract != canonicalprompt.ContractV1 || !materialDigest(p.PrefixDigest) || p.UnknownReasons & ^materialReasonMask != 0 ||
		(p.Coverage != SessionMaterialCompleteV1 && p.Coverage != SessionMaterialUnknownV1) || (p.Coverage == SessionMaterialCompleteV1) != (p.UnknownReasons == 0) {
		return ErrSessionMaterialPrefix
	}
	anyCapped := false
	for _, v := range []uint64{p.UserMessageCount, p.AssistantMessageCount, p.ToolCallCount, p.ToolResultCount, p.SystemNoticeCount, p.VerifiedPayloadBytes, p.EncodedJSONLBytes, p.MaxEncodedRecordBytes, p.ToolEnvelopeBytes, p.AttachmentCount, p.ReferencedAttachmentBytes, p.CodexCompatibleRecords, p.CodexIncompatibleRecords, p.RenderedFrameBytes, p.UnknownRecords} {
		if v > math.MaxInt64 {
			return ErrSessionMaterialPrefix
		}
		anyCapped = anyCapped || v == math.MaxInt64
	}
	if p.UnknownReasons&SessionMaterialSaturatedV1 != 0 && !anyCapped {
		return ErrSessionMaterialPrefix
	}
	if p.LastEventID == "" || p.LastContribution == nil {
		var expected SessionMaterialPrefixV1
		if p.Coverage == SessionMaterialCompleteV1 && p.ThroughSequence == 0 {
			expected, _ = NewSessionMaterialGenesisV1(p.TenantID, p.SessionID)
		} else {
			expected, _ = NewUnknownSessionMaterialPrefixV1(p.TenantID, p.SessionID, p.ThroughSequence)
		}
		if !EqualSessionMaterialPrefixV1(p, expected) {
			return ErrSessionMaterialPrefix
		}
		return nil
	}
	if p.ThroughSequence == 0 || p.LastEventID.Validate() != nil || p.LastContribution.validate() != nil || p.LastContribution.EventID != p.LastEventID || p.LastContribution.Sequence != p.ThroughSequence {
		return ErrSessionMaterialPrefix
	}
	kindSum, kindOK := materialSumWithin(p.ThroughSequence, p.UserMessageCount, p.AssistantMessageCount, p.ToolCallCount, p.ToolResultCount, p.SystemNoticeCount)
	renderSum, renderOK := materialSumWithin(p.ThroughSequence, p.CodexCompatibleRecords, p.CodexIncompatibleRecords, p.UnknownRecords)
	if !kindOK || !renderOK || ((p.UnknownReasons&SessionMaterialSaturatedV1) == 0 && renderSum != p.ThroughSequence) || ((p.UnknownReasons&(SessionMaterialLegacyV1|SessionMaterialSaturatedV1)) == 0 && kindSum != p.ThroughSequence) || (p.Coverage == SessionMaterialCompleteV1 && (kindSum != p.ThroughSequence || renderSum != p.ThroughSequence || p.UnknownRecords != 0)) {
		return ErrSessionMaterialPrefix
	}
	if p.MaxEncodedRecordBytes > MaxMeasuredEncodedRecordBytesV1 || p.MaxEncodedRecordBytes > p.EncodedJSONLBytes || (p.EncodedJSONLBytes == 0) != (p.MaxEncodedRecordBytes == 0) || p.VerifiedPayloadBytes > p.EncodedJSONLBytes || p.ToolEnvelopeBytes > p.VerifiedPayloadBytes || (p.AttachmentCount == 0 && p.ReferencedAttachmentBytes != 0) || (p.CodexCompatibleRecords == 0 && p.RenderedFrameBytes != 0) || p.RenderedFrameBytes < p.CodexCompatibleRecords {
		return ErrSessionMaterialPrefix
	}
	if p.CodexCompatibleRecords == 0 && p.CodexIncompatibleRecords == 0 && (p.VerifiedPayloadBytes != 0 || p.EncodedJSONLBytes != 0 || p.ToolEnvelopeBytes != 0 || p.AttachmentCount != 0 || p.ReferencedAttachmentBytes != 0) {
		return ErrSessionMaterialPrefix
	}
	if (p.CodexCompatibleRecords != 0 || p.CodexIncompatibleRecords != 0) && (p.VerifiedPayloadBytes == 0 || p.EncodedJSONLBytes == 0) || p.AttachmentCount != 0 && p.CodexIncompatibleRecords == 0 || p.ToolEnvelopeBytes != 0 && p.ToolCallCount == 0 && p.ToolResultCount == 0 {
		return ErrSessionMaterialPrefix
	}
	// Only user/assistant records can render in this closed compiler; only
	// users can reference attachments, and measured tools are incompatible.
	if p.CodexCompatibleRecords > p.UserMessageCount+p.AssistantMessageCount || p.AttachmentCount != 0 && p.UserMessageCount == 0 || p.ToolEnvelopeBytes != 0 && p.CodexIncompatibleRecords == 0 {
		return ErrSessionMaterialPrefix
	}
	if p.UnknownRecords != 0 && p.UnknownReasons == 0 || p.UnknownReasons&(SessionMaterialMissingInvalidV1|SessionMaterialUnsupportedV1|SessionMaterialNonRoundTripV1) != 0 && p.UnknownRecords == 0 {
		return ErrSessionMaterialPrefix
	}
	c := p.LastContribution
	if !c.Verified && (p.UnknownReasons&(SessionMaterialMissingInvalidV1|SessionMaterialUnsupportedV1|SessionMaterialNonRoundTripV1) == 0 || p.UnknownRecords == 0) {
		return ErrSessionMaterialPrefix
	}
	if p.kindCount(c.Kind) == 0 || c.PayloadBytes > p.VerifiedPayloadBytes || c.EncodedRecordBytes > p.MaxEncodedRecordBytes || c.ToolEnvelopeBytes > p.ToolEnvelopeBytes || c.AttachmentCount > p.AttachmentCount || c.ReferencedAttachmentBytes > p.ReferencedAttachmentBytes || c.RenderedFrameBytes > p.RenderedFrameBytes || (c.Verified && ((c.CodexCompatible && p.CodexCompatibleRecords == 0) || (!c.CodexCompatible && p.CodexIncompatibleRecords == 0))) {
		return ErrSessionMaterialPrefix
	}
	return nil
}

func (p SessionMaterialPrefixV1) kindCount(k domain.SessionEventKind) uint64 {
	switch k {
	case domain.SessionEventUserMessage:
		return p.UserMessageCount
	case domain.SessionEventAssistantMessage:
		return p.AssistantMessageCount
	case domain.SessionEventToolCall:
		return p.ToolCallCount
	case domain.SessionEventToolResult:
		return p.ToolResultCount
	case domain.SessionEventSystemNotice:
		return p.SystemNoticeCount
	}
	return 0
}

func materialSumWithin(ceiling uint64, values ...uint64) (uint64, bool) {
	remaining := ceiling
	for _, v := range values {
		if v > remaining {
			return 0, false
		}
		remaining -= v
	}
	return ceiling - remaining, true
}

func EncodeSessionMaterialPrefixV1(p SessionMaterialPrefixV1) ([]byte, error) {
	if p.Validate() != nil {
		return nil, ErrSessionMaterialPrefix
	}
	encoded, err := json.Marshal(p)
	if err != nil || len(encoded) > MaxSessionMaterialPrefixBytesV1 {
		return nil, ErrSessionMaterialPrefix
	}
	return encoded, nil
}

func DecodeSessionMaterialPrefixV1(data []byte) (SessionMaterialPrefixV1, error) {
	if len(data) == 0 || len(data) > MaxSessionMaterialPrefixBytesV1 || !utf8.Valid(data) {
		return SessionMaterialPrefixV1{}, ErrSessionMaterialPrefix
	}
	fields, err := materialObject(data, prefixRequiredFields, []string{"last_event_id", "last_contribution"})
	if err != nil {
		return SessionMaterialPrefixV1{}, ErrSessionMaterialPrefix
	}
	if raw, ok := fields["last_contribution"]; ok {
		if _, err := materialObject(raw, contributionRequiredFields, nil); err != nil {
			return SessionMaterialPrefixV1{}, ErrSessionMaterialPrefix
		}
	}
	var p SessionMaterialPrefixV1
	if json.Unmarshal(data, &p) != nil || p.Validate() != nil {
		return SessionMaterialPrefixV1{}, ErrSessionMaterialPrefix
	}
	return p, nil
}

var prefixRequiredFields = []string{"version", "tenant_id", "session_id", "through_sequence", "record_schema", "renderer_contract", "coverage", "unknown_reasons", "prefix_digest", "user_message_count", "assistant_message_count", "tool_call_count", "tool_result_count", "system_notice_count", "verified_payload_bytes", "encoded_jsonl_bytes", "max_encoded_record_bytes", "tool_envelope_bytes", "attachment_count", "referenced_attachment_bytes", "codex_compatible_records", "codex_incompatible_records", "rendered_frame_bytes", "unknown_records"}
var contributionRequiredFields = []string{"version", "event_id", "sequence", "kind", "identity_digest", "record_digest", "verified", "payload_bytes", "encoded_record_bytes", "tool_envelope_bytes", "attachment_count", "referenced_attachment_bytes", "codex_compatible", "rendered_frame_bytes"}

// Fixed-shape, bounded fact decoding: exact key spellings, duplicates, missing
// fields and null scalars are rejected BEFORE normal struct decoding. No map
// supplied by a caller controls the schema; only the two closed tables above.
func materialObject(data []byte, required, optional []string) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return nil, ErrSessionMaterialPrefix
	}
	known := make(map[string]bool, len(required)+len(optional))
	for _, k := range required {
		known[k] = true
	}
	for _, k := range optional {
		known[k] = true
	}
	fields := make(map[string]json.RawMessage, len(known))
	for d.More() {
		key, err := d.Token()
		k, ok := key.(string)
		if err != nil || !ok || !known[k] {
			return nil, ErrSessionMaterialPrefix
		}
		if _, exists := fields[k]; exists {
			return nil, ErrSessionMaterialPrefix
		}
		var raw json.RawMessage
		if d.Decode(&raw) != nil || bytes.Equal(raw, []byte("null")) {
			return nil, ErrSessionMaterialPrefix
		}
		fields[k] = raw
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') || d.Decode(&struct{}{}) != io.EOF {
		return nil, ErrSessionMaterialPrefix
	}
	for _, k := range required {
		if _, ok := fields[k]; !ok {
			return nil, ErrSessionMaterialPrefix
		}
	}
	return fields, nil
}

func materialDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range []byte(value) {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func materialSeedDigest(domainName string, tenantID domain.TenantID, sessionID domain.SessionID, sequence uint64) string {
	h := sha256.New()
	materialFrame(h, domainName)
	materialFrame(h, string(tenantID))
	materialFrame(h, string(sessionID))
	materialUint(h, sequence)
	return hex.EncodeToString(h.Sum(nil))
}

func materialIdentityDigest(e domain.SessionEvent) string {
	h := sha256.New()
	materialFrame(h, "sessionless.session-material.final-identity.v1")
	for _, v := range []string{string(e.ID), string(e.TenantID), string(e.SessionID)} {
		materialFrame(h, v)
	}
	materialUint(h, e.Sequence)
	materialFrame(h, string(e.Kind))
	if e.AuthorUserID == nil {
		materialFrame(h, "absent")
	} else {
		materialFrame(h, "present")
		materialFrame(h, string(*e.AuthorUserID))
	}
	if e.RunID == nil {
		materialFrame(h, "absent")
	} else {
		materialFrame(h, "present")
		materialFrame(h, string(*e.RunID))
	}
	for _, v := range []string{string(e.IdempotencyKey), string(e.Payload.TenantID), e.Payload.Key} {
		materialFrame(h, v)
	}
	materialUint(h, uint64(e.Payload.Size))
	materialFrame(h, e.Payload.SHA256)
	// Bind the semantic instant AND the codec representation. RFC3339Nano
	// alone loses sub-minute timezone offsets; UTC here is only a hash frame,
	// never a rewrite of the canonical event or its serialized timestamp.
	materialFrame(h, e.CreatedAt.UTC().Format(time.RFC3339Nano))
	materialFrame(h, e.CreatedAt.Format(time.RFC3339Nano))
	return hex.EncodeToString(h.Sum(nil))
}

func materialUint(h hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	materialFrame(h, string(encoded[:]))
}
func materialFrame(h hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	var chunk [4096]byte
	defer clear(chunk[:])
	for len(value) > 0 {
		n := copy(chunk[:], value)
		_, _ = h.Write(chunk[:n])
		value = value[n:]
	}
}
