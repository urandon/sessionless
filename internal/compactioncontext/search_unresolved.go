package compactioncontext

import (
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// Conservative historical STOP protection only. The Source must authenticate
// the original V4 capsule and seal before using this DATA; it grants no send.
func unresolvedSearchSpansV4(records []sessioncontext.EventPayload) ([]UnitV1, error) {
	var spans []UnitV1
	seen := map[domain.RunID]bool{}
	for i, record := range records {
		var outer struct {
			Content json.RawMessage `json:"content"`
		}
		var tag struct {
			Schema string `json:"schema"`
		}
		if record.Event.Kind != domain.SessionEventToolResult || json.Unmarshal(record.Payload, &outer) != nil || json.Unmarshal(outer.Content, &tag) != nil || tag.Schema != "sessionless.search-operation-status.v4" {
			continue
		}
		status, err := canonicaloutput.DecodeSearchOperationStatusV4(outer.Content)
		if err != nil {
			return nil, ErrCanonical
		}
		stop := domain.SearchOutcomeUnknownV4
		if status.Outcome == domain.AttemptOperationRejectedBeforeSendV1 {
			stop = domain.SearchRejectedBeforeSendV4
		}
		if i == 0 || i+1 >= len(records) || record.Event.RunID == nil {
			return nil, ErrCanonical
		}
		run := *record.Event.RunID
		call, notice := records[i-1], records[i+1]
		id, name, ok := toolIdentity(record.Payload)
		callID, callName, callOK := toolIdentity(call.Payload)
		if !ok || !callOK || id != callID || name != "web_search" || callName != name || seen[run] || call.Event.Kind != domain.SessionEventToolCall || call.Event.RunID == nil || *call.Event.RunID != run || notice.Event.Kind != domain.SessionEventSystemNotice || notice.Event.RunID == nil || *notice.Event.RunID != run || call.Event.Sequence+1 != record.Event.Sequence || record.Event.Sequence+1 != notice.Event.Sequence || !validEnvelope(call) || !validEnvelope(record) || !validEnvelope(notice) {
			return nil, ErrCanonical
		}
		closed, err := canonicaloutput.DecodeSearchFailureNoticeV4(notice.Payload)
		if err != nil || closed.Code != stop || closed.Operation != status.Operation || closed.Reservation != status.Reservation || closed.Observation != status.Observation || closed.Outcome != status.Outcome || closed.Reason != status.Reason {
			return nil, ErrCanonical
		}
		seen[run] = true
		spans = append(spans, UnitV1{Start: i - 1, End: i + 2, Protected: true})
	}
	return spans, nil
}
