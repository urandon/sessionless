package compactioncontext

import (
	"bytes"
	"encoding/json"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// Closed UNKNOWN facts are protected historical DATA, not completed tool
// results. Authentication belongs to the original Store's failure-history
// verifier; conservative protection here never authorizes an effect.
func unresolvedToolSpans(records []sessioncontext.EventPayload) ([]UnitV1, error) {
	var spans []UnitV1
	seen := make(map[domain.RunID]bool)
	for i, record := range records {
		if record.Event.Kind != domain.SessionEventToolResult {
			continue
		}
		var outer struct {
			Content json.RawMessage `json:"content"`
		}
		var tag struct {
			Schema  string `json:"schema"`
			Outcome string `json:"outcome"`
		}
		if json.Unmarshal(record.Payload, &outer) != nil || json.Unmarshal(outer.Content, &tag) != nil || tag.Outcome != "accepted_outcome_unknown" {
			continue
		}
		if tag.Schema == "sessionless.search-operation-status.v4" {
			continue
		}
		if _, err := canonicaloutput.DecodeMCPUnknownToolOutcomeV1(outer.Content); err != nil || i == 0 || i+1 >= len(records) || record.Event.RunID == nil {
			return nil, ErrCanonical
		}
		run := *record.Event.RunID
		call, notice := records[i-1], records[i+1]
		id, name, ok := toolIdentity(record.Payload)
		callID, callName, callOK := toolIdentity(call.Payload)
		if !ok || !callOK || id != callID || name != callName || seen[run] || call.Event.Kind != domain.SessionEventToolCall || call.Event.RunID == nil || *call.Event.RunID != run || notice.Event.Kind != domain.SessionEventSystemNotice || notice.Event.RunID == nil || *notice.Event.RunID != run || call.Event.Sequence+1 != record.Event.Sequence || record.Event.Sequence+1 != notice.Event.Sequence || !validEnvelope(call) || !validEnvelope(record) || !validEnvelope(notice) {
			return nil, ErrCanonical
		}
		expected, err := canonicaloutput.EncodeFailureV1(run, false, domain.MCPOutcomeUnknownV3)
		matches := err == nil && expected.ID == notice.Event.ID && bytes.Equal(expected.Payload, notice.Payload)
		clear(expected.Payload)
		if !matches {
			return nil, ErrCanonical
		}
		seen[run] = true
		spans = append(spans, UnitV1{Start: i - 1, End: i + 2, Protected: true})
	}
	search, err := unresolvedSearchSpansV4(records)
	if err != nil {
		return nil, err
	}
	return append(spans, search...), nil
}

// ValidateUnresolvedToolProtection joins actual canonical payloads to the
// retained cut's exact immutable identities. The retained wire has no trusted
// caller-populated "unresolved" flag. Even a fully rehashed manifest cannot
// move these facts to Covered or split their pair/anchor across a cut.
// Callers MUST first authenticate the original failure capsule/canonical seal;
// this method is DATA validation, not producer or deletion authority.
func (v *RetainedV1) ValidateUnresolvedToolProtection(records []sessioncontext.EventPayload) error {
	if v == nil || v.CodecVersion() == 0 || len(records) > int(MaxEventsV1) {
		return ErrCanonical
	}
	var size uint64
	actual := make(map[domain.SessionEventID]sessioncontext.EventPayload, len(records))
	for _, record := range records {
		if uint64(len(record.Payload)) > MaxBytesV1-size {
			return ErrCanonical
		}
		size += uint64(len(record.Payload))
		if _, exists := actual[record.Event.ID]; exists {
			return ErrCanonical
		}
		actual[record.Event.ID] = record
	}
	spans, err := unresolvedToolSpans(records)
	if err != nil {
		return err
	}
	indices := make(map[domain.SessionEventID]int)
	for i, row := range v.manifest.Cut.Sources {
		if row.Canonical == nil {
			continue
		}
		record, found := actual[row.Canonical.ID]
		if !found || !reflect.DeepEqual(record.Event, *row.Canonical) {
			return ErrCanonical
		}
		indices[row.Canonical.ID] = i
	}
	protected := make(map[uint32]bool, len(v.manifest.Cut.Protected))
	for _, index := range v.manifest.Cut.Protected {
		protected[index] = true
	}
	for _, span := range spans {
		present := 0
		for _, record := range records[span.Start:span.End] {
			if _, found := indices[record.Event.ID]; found {
				present++
			}
		}
		if present == 0 {
			continue // A retained prefix may precede this later producing Run.
		}
		if present != span.End-span.Start {
			return ErrCanonical
		}
		first := indices[records[span.Start].Event.ID]
		for j, record := range records[span.Start:span.End] {
			index := indices[record.Event.ID]
			if index != first+j || index < int(v.manifest.Cut.Cut) && !protected[uint32(index)] || (first < int(v.manifest.Cut.Cut)) != (index < int(v.manifest.Cut.Cut)) {
				return ErrCanonical
			}
		}
	}
	return nil
}
