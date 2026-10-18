// Package compactioncontext groups bounded canonical history for derived
// compaction. Its views are data, not proof of source access, current effects,
// token fit, media visibility, or permission to generate/publish a summary.
package compactioncontext

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

const (
	MaxBytesV1  uint64 = 1 << 20
	MaxEventsV1 uint64 = 4096
)

var ErrCanonical = errors.New("canonical compaction view unavailable")

// UnitV1 is a half-open interval in Records. No cut may split a unit. Protected
// units contain an original user/system record, possibly inside a larger
// correlation component; they are never eligible for lossy summarization.
type UnitV1 struct {
	Start, End int
	Protected  bool
}

// ViewV1 retains private original bytes. Ordinary value copies share a Clear
// lifetime. Accessors return independent copies; callers own and clear them.
type ViewV1 struct{ state *viewState }

type viewState struct {
	mu      sync.RWMutex
	records []sessioncontext.EventPayload
	units   []UnitV1
	digest  string
	live    bool
}

// BuildCanonicalV1 validates the complete canonical range starting at one.
// It does not read attachments or verify current operation observations: the
// original Store must join those separately before using this data. Historical
// system notices remain historical data, not current trusted instructions.
func BuildCanonicalV1(input []sessioncontext.EventPayload, maxBytes, maxEvents uint64) (*ViewV1, error) {
	if maxBytes == 0 || maxBytes > MaxBytesV1 || maxEvents == 0 || maxEvents > MaxEventsV1 || len(input) == 0 || uint64(len(input)) > maxEvents {
		return nil, ErrCanonical
	}
	// Bound all caller-owned strings/payloads before cloning or JSON encoding.
	remaining := maxBytes
	for _, record := range input {
		if !take(&remaining, uint64(len(record.Payload))) || !takeIdentity(&remaining, record.Event) {
			return nil, ErrCanonical
		}
	}
	records := cloneRecords(input)
	owned := false
	defer func() {
		if !owned {
			clearRecords(records)
		}
	}()
	seen := make(map[domain.SessionEventID]bool, len(records))
	type callKey struct {
		run  domain.RunID
		call string
	}
	type callState struct {
		name string
		done bool
	}
	calls := make(map[callKey]callState)
	runs := make(map[domain.RunID]UnitV1)
	intervals := make([]UnitV1, 0, len(records))
	hash := sha256.New()
	_, _ = io.WriteString(hash, "sessionless.compaction-canonical.v1\x00")
	writeUint(hash, uint64(len(records)))
	encodedRemaining := maxBytes
	for i, record := range records {
		event := record.Event
		if event.Sequence != uint64(i+1) || event.TenantID != records[0].Event.TenantID || event.SessionID != records[0].Event.SessionID || seen[event.ID] || !utf8.Valid(record.Payload) {
			return nil, ErrCanonical
		}
		seen[event.ID] = true
		if !validEnvelope(record) {
			return nil, ErrCanonical
		}
		line, err := sessioncontext.EncodeRecord(event, record.Payload)
		if err != nil {
			return nil, ErrCanonical
		}
		fits := take(&encodedRemaining, uint64(len(line)))
		if fits {
			writeUint(hash, uint64(len(line)))
			_, _ = hash.Write(line)
		}
		clear(line)
		if !fits {
			return nil, ErrCanonical
		}
		intervals = append(intervals, UnitV1{Start: i, End: i + 1})
		switch event.Kind {
		case domain.SessionEventAssistantMessage, domain.SessionEventToolCall, domain.SessionEventToolResult:
			// Canonical assistant envelopes do not identify an originating
			// response/call list. Conservatively retain the whole producing
			// Run's span rather than guessing a finer response boundary.
			run := *event.RunID // Prepare and EncodeRecord validated presence.
			span, exists := runs[run]
			if !exists {
				span.Start = i
			}
			span.End = i + 1
			runs[run] = span
			if event.Kind == domain.SessionEventAssistantMessage {
				continue
			}
			callID, name, ok := toolIdentity(record.Payload)
			if !ok {
				return nil, ErrCanonical
			}
			key := callKey{run: run, call: callID}
			call, exists := calls[key]
			if event.Kind == domain.SessionEventToolCall {
				if exists {
					return nil, ErrCanonical
				}
				calls[key] = callState{name: name}
			} else {
				if !exists || call.done || call.name != name {
					return nil, ErrCanonical
				}
				call.done = true
				calls[key] = call
			}
		}
	}
	for _, call := range calls {
		if !call.done {
			return nil, ErrCanonical
		}
	}
	for _, span := range runs {
		intervals = append(intervals, span)
	}
	sort.Slice(intervals, func(i, j int) bool {
		if intervals[i].Start == intervals[j].Start {
			return intervals[i].End > intervals[j].End
		}
		return intervals[i].Start < intervals[j].Start
	})
	units := make([]UnitV1, 0, len(records))
	for _, span := range intervals {
		last := len(units) - 1
		if last >= 0 && span.Start < units[last].End {
			units[last].End = max(units[last].End, span.End)
		} else {
			units = append(units, span)
		}
	}
	writeUint(hash, uint64(len(units)))
	for i := range units {
		for _, record := range records[units[i].Start:units[i].End] {
			if record.Event.Kind == domain.SessionEventUserMessage || record.Event.Kind == domain.SessionEventSystemNotice {
				units[i].Protected = true
			}
		}
		writeUint(hash, uint64(units[i].Start))
		writeUint(hash, uint64(units[i].End))
		var flag byte
		if units[i].Protected {
			flag = 1
		}
		_, _ = hash.Write([]byte{flag})
	}
	owned = true
	return &ViewV1{state: &viewState{records: records, units: units, digest: hex.EncodeToString(hash.Sum(nil)), live: true}}, nil
}

func (view *ViewV1) Digest() string {
	if view == nil || view.state == nil {
		return ""
	}
	view.state.mu.RLock()
	defer view.state.mu.RUnlock()
	return view.state.digest
}

func (view *ViewV1) Records() []sessioncontext.EventPayload {
	if view == nil || view.state == nil {
		return nil
	}
	view.state.mu.RLock()
	defer view.state.mu.RUnlock()
	if !view.state.live {
		return nil
	}
	return cloneRecords(view.state.records)
}

func (view *ViewV1) Units() []UnitV1 {
	if view == nil || view.state == nil {
		return nil
	}
	view.state.mu.RLock()
	defer view.state.mu.RUnlock()
	return append([]UnitV1(nil), view.state.units...)
}

func (view *ViewV1) Clear() {
	if view == nil || view.state == nil {
		return
	}
	view.state.mu.Lock()
	defer view.state.mu.Unlock()
	clearRecords(view.state.records)
	view.state.records, view.state.units = nil, nil
	view.state.digest = ""
	view.state.live = false
}

func (ViewV1) MarshalJSON() ([]byte, error) { return nil, ErrCanonical }
func (ViewV1) Format(state fmt.State, _ rune) {
	_, _ = fmt.Fprint(state, "compactioncontext.ViewV1")
}

func validEnvelope(record sessioncontext.EventPayload) bool {
	var uploads []domain.Artifact
	if record.Event.Kind == domain.SessionEventUserMessage {
		var envelope struct {
			Attachments json.RawMessage `json:"attachments"`
		}
		if json.Unmarshal(record.Payload, &envelope) != nil {
			return false
		}
		defer clear(envelope.Attachments)
		if len(envelope.Attachments) != 0 && !bytes.Equal(envelope.Attachments, []byte("null")) {
			decoder := json.NewDecoder(bytes.NewReader(envelope.Attachments))
			decoder.DisallowUnknownFields()
			start, err := decoder.Token()
			if err != nil || start != json.Delim('[') {
				return false
			}
			for decoder.More() {
				if len(uploads) == sessionmaterial.MaxAttachmentRefsV1 {
					return false
				}
				var ref domain.Artifact
				if decoder.Decode(&ref) != nil {
					return false
				}
				ref.Name = fmt.Sprintf("attachment-%02d-%s", len(uploads)+1, ref.Name)
				uploads = append(uploads, ref)
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') || decoder.Decode(&struct{}{}) != io.EOF {
				return false
			}
		}
	}
	prepared, err := sessionmaterial.PrepareV1(record.Event, record.Payload, uploads)
	if err != nil {
		return false
	}
	prepared.Release()
	return true
}

// Only correlation fields are interpreted; preserve all original content bytes.
// Reject ambiguous outer keys instead of letting JSON's case-insensitive or
// last-duplicate-wins behavior choose a different correlation identity.
func toolIdentity(body []byte) (string, string, bool) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return "", "", false
	}
	seen := make(map[string]bool, 4)
	var call, name string
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] || (key != "schema" && key != "call_id" && key != "tool_name" && key != "content") {
			return "", "", false
		}
		seen[key] = true
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			clear(value)
			return "", "", false
		}
		switch key {
		case "call_id":
			if !validCorrelationString(value) {
				clear(value)
				return "", "", false
			}
			err = json.Unmarshal(value, &call)
		case "tool_name":
			if !validCorrelationString(value) {
				clear(value)
				return "", "", false
			}
			err = json.Unmarshal(value, &name)
		}
		clear(value)
		if err != nil {
			return "", "", false
		}
	}
	end, err := decoder.Token()
	return call, name, err == nil && end == json.Delim('}') && len(seen) == 4 && decoder.Decode(&struct{}{}) == io.EOF
}

// encoding/json replaces unmatched UTF-16 escapes instead of rejecting them.
// That replacement must not make two distinct malformed call/name identities
// compare equal. Valid pairs and equivalent escaped/literal spellings remain
// valid; this predicate neither normalizes nor rewrites the original payload.
func validCorrelationString(raw []byte) bool {
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return false
	}
	for i := 1; i < len(raw)-1; i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw)-1 {
			return false
		}
		if raw[i] != 'u' {
			continue // JSON decoding has already validated ordinary escapes.
		}
		value, ok := hexCodeUnit(raw[i+1:])
		if !ok {
			return false
		}
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value >= 0xd800 && value <= 0xdbff {
			if i+6 >= len(raw)-1 || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, ok := hexCodeUnit(raw[i+3:])
			if !ok || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}

func hexCodeUnit(raw []byte) (uint16, bool) {
	if len(raw) < 4 {
		return 0, false
	}
	var value uint16
	for _, b := range raw[:4] {
		value <<= 4
		switch {
		case b >= '0' && b <= '9':
			value |= uint16(b - '0')
		case b >= 'a' && b <= 'f':
			value |= uint16(b-'a') + 10
		case b >= 'A' && b <= 'F':
			value |= uint16(b-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func cloneRecords(input []sessioncontext.EventPayload) []sessioncontext.EventPayload {
	output := make([]sessioncontext.EventPayload, len(input))
	for i, record := range input {
		output[i] = record
		output[i].Payload = bytes.Clone(record.Payload)
		if record.Event.AuthorUserID != nil {
			v := *record.Event.AuthorUserID
			output[i].Event.AuthorUserID = &v
		}
		if record.Event.RunID != nil {
			v := *record.Event.RunID
			output[i].Event.RunID = &v
		}
	}
	return output
}

func clearRecords(records []sessioncontext.EventPayload) {
	for i := range records {
		clear(records[i].Payload)
		records[i] = sessioncontext.EventPayload{}
	}
}

func take(remaining *uint64, amount uint64) bool {
	if amount > *remaining {
		return false
	}
	*remaining -= amount
	return true
}

func takeIdentity(remaining *uint64, event domain.SessionEvent) bool {
	values := []string{string(event.ID), string(event.TenantID), string(event.SessionID), string(event.Kind), string(event.IdempotencyKey), string(event.Payload.TenantID), event.Payload.Key, event.Payload.SHA256}
	if event.AuthorUserID != nil {
		values = append(values, string(*event.AuthorUserID))
	}
	if event.RunID != nil {
		values = append(values, string(*event.RunID))
	}
	for _, value := range values {
		if !take(remaining, uint64(len(value))) {
			return false
		}
	}
	return true
}

func writeUint(writer io.Writer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}
