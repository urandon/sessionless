package compactioncontext_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func canonicalRecord(t *testing.T, seq int, kind domain.SessionEventKind, run, body string) sessioncontext.EventPayload {
	t.Helper()
	author, runID := domain.UserID("private-user"), domain.RunID(run)
	eventID := domain.SessionEventID(fmt.Sprintf("event-%d", seq))
	event := domain.SessionEvent{ID: eventID, TenantID: "tenant", SessionID: "session", Sequence: uint64(seq), Kind: kind, IdempotencyKey: domain.IdempotencyKey(fmt.Sprintf("key-%d", seq)), CreatedAt: time.Date(2026, 10, 18, 0, 0, seq, 0, time.UTC)}
	if kind == domain.SessionEventUserMessage {
		event.AuthorUserID = &author
	}
	if run != "" {
		event.RunID = &runID
	}
	event.Payload = domain.BlobRef{TenantID: event.TenantID, Key: domain.SessionEventObjectPrefix(event.TenantID, event.SessionID, eventID) + "message.json"}
	r := sessioncontext.EventPayload{Event: event}
	replacePayload(&r, body)
	if err := r.Event.Validate(); err != nil {
		t.Fatalf("fixture event seq=%d kind=%s: %v", seq, kind, err)
	}
	return r
}

func replacePayload(record *sessioncontext.EventPayload, body string) {
	record.Payload = []byte(body)
	sum := sha256.Sum256(record.Payload)
	record.Event.Payload.Size = int64(len(record.Payload))
	record.Event.Payload.SHA256 = hex.EncodeToString(sum[:])
}

func userRecord(t *testing.T, seq int) sessioncontext.EventPayload {
	t.Helper()
	return canonicalRecord(t, seq, domain.SessionEventUserMessage, "", `{"version":1,"text":"private task; preserve every condition"}`)
}

func assistantRecord(t *testing.T, seq int, run string) sessioncontext.EventPayload {
	t.Helper()
	return canonicalRecord(t, seq, domain.SessionEventAssistantMessage, run, `{"schema":"sessionless.assistant-message.v1","summary":"private answer","artifact_manifest_id":"manifest"}`)
}

func toolRecord(t *testing.T, seq int, kind domain.SessionEventKind, run, call, name string) sessioncontext.EventPayload {
	t.Helper()
	body, err := json.Marshal(struct {
		Schema   string          `json:"schema"`
		CallID   string          `json:"call_id"`
		ToolName string          `json:"tool_name"`
		Content  json.RawMessage `json:"content"`
	}{"sessionless.tool-event.v1", call, name, json.RawMessage(`{"private":"tool bytes"}`)})
	if err != nil {
		t.Fatal(err)
	}
	return canonicalRecord(t, seq, kind, run, string(body))
}

func build(t *testing.T, records []sessioncontext.EventPayload) *compactioncontext.ViewV1 {
	t.Helper()
	view, err := compactioncontext.BuildCanonicalV1(records, compactioncontext.MaxBytesV1, compactioncontext.MaxEventsV1)
	if err != nil {
		t.Fatalf("build canonical view (%d events): %v", len(records), err)
	}
	t.Cleanup(view.Clear)
	return view
}

func clearCopies(records []sessioncontext.EventPayload) {
	for i := range records {
		clear(records[i].Payload)
	}
}

func TestCanonicalCompleteMulticallAndProtectedOriginals(t *testing.T) {
	call, result := domain.SessionEventToolCall, domain.SessionEventToolResult
	records := []sessioncontext.EventPayload{
		userRecord(t, 1), toolRecord(t, 2, call, "run-a", "one", "lookup"), toolRecord(t, 3, call, "run-a", "two", "fetch"),
		toolRecord(t, 4, result, "run-a", "two", "fetch"), toolRecord(t, 5, result, "run-a", "one", "lookup"), assistantRecord(t, 6, "run-a"),
		userRecord(t, 7), assistantRecord(t, 8, "run-b"),
		canonicalRecord(t, 9, domain.SessionEventSystemNotice, "", `{"schema":"sessionless.run-terminal-notice.v1","code":"historical_notice","cancelled":false}`), userRecord(t, 10),
	}
	// Valid, multi-call historical material remains eligible for summarization;
	// it is not rejected merely because the initial live model admits one call.
	view := build(t, records)
	want := []compactioncontext.UnitV1{{Start: 0, End: 1, Protected: true}, {Start: 1, End: 6}, {Start: 6, End: 7, Protected: true}, {Start: 7, End: 8}, {Start: 8, End: 9, Protected: true}, {Start: 9, End: 10, Protected: true}}
	if got := view.Units(); !reflect.DeepEqual(got, want) {
		t.Fatalf("correlation/protection units=%v want=%v", got, want)
	}
	got := view.Records()
	defer clearCopies(got)
	if !reflect.DeepEqual(got, records) {
		t.Fatal("view lost or rewrote original identity, role, order or payload")
	}
	var source bytes.Buffer
	for _, r := range records {
		line, err := sessioncontext.EncodeRecord(r.Event, r.Payload)
		if err != nil {
			t.Fatal(err)
		}
		source.Write(line)
		clear(line)
	}
	decoded, err := sessioncontext.DecodeJSONL(source.Bytes(), "tenant", "session")
	if err != nil || !reflect.DeepEqual(decoded, records) {
		t.Fatalf("lossless original codec changed: %v", err)
	}
	clearCopies(decoded)
	clear(source.Bytes())
}

func TestCanonicalInterleavedCorrelationsProtectWholeConnectedSpan(t *testing.T) {
	call, result := domain.SessionEventToolCall, domain.SessionEventToolResult
	records := []sessioncontext.EventPayload{
		userRecord(t, 1), toolRecord(t, 2, call, "run-a", "same", "lookup"), toolRecord(t, 3, call, "run-b", "same", "lookup"),
		userRecord(t, 4), toolRecord(t, 5, result, "run-a", "same", "lookup"), toolRecord(t, 6, result, "run-b", "same", "lookup"),
		assistantRecord(t, 7, "run-a"), assistantRecord(t, 8, "run-b"), assistantRecord(t, 9, "run-c"),
	}
	view := build(t, records)
	want := []compactioncontext.UnitV1{{Start: 0, End: 1, Protected: true}, {Start: 1, End: 8, Protected: true}, {Start: 8, End: 9}}
	if got := view.Units(); !reflect.DeepEqual(got, want) {
		t.Fatalf("interleaved intervals must merge, not reject or split: got=%v want=%v", got, want)
	}
}

func TestCanonicalRejectsIncompleteOrAmbiguousToolHistory(t *testing.T) {
	call, result := domain.SessionEventToolCall, domain.SessionEventToolResult
	for _, tc := range []struct {
		name string
		make func() []sessioncontext.EventPayload
	}{
		{"orphan result", func() []sessioncontext.EventPayload {
			return []sessioncontext.EventPayload{toolRecord(t, 1, result, "run", "c", "t")}
		}},
		{"missing result", func() []sessioncontext.EventPayload {
			return []sessioncontext.EventPayload{toolRecord(t, 1, call, "run", "c", "t"), assistantRecord(t, 2, "run")}
		}},
		{"duplicate call", func() []sessioncontext.EventPayload {
			return []sessioncontext.EventPayload{toolRecord(t, 1, call, "run", "c", "t"), toolRecord(t, 2, call, "run", "c", "t"), toolRecord(t, 3, result, "run", "c", "t")}
		}},
		{"duplicate result", func() []sessioncontext.EventPayload {
			return []sessioncontext.EventPayload{toolRecord(t, 1, call, "run", "c", "t"), toolRecord(t, 2, result, "run", "c", "t"), toolRecord(t, 3, result, "run", "c", "t")}
		}},
		{"wrong tool", func() []sessioncontext.EventPayload {
			return []sessioncontext.EventPayload{toolRecord(t, 1, call, "run", "c", "t"), toolRecord(t, 2, result, "run", "c", "other")}
		}},
		{"wrong run", func() []sessioncontext.EventPayload {
			return []sessioncontext.EventPayload{toolRecord(t, 1, call, "run", "c", "t"), toolRecord(t, 2, result, "other", "c", "t")}
		}},
		{"result first", func() []sessioncontext.EventPayload {
			return []sessioncontext.EventPayload{toolRecord(t, 1, result, "run", "c", "t"), toolRecord(t, 2, call, "run", "c", "t")}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view, err := compactioncontext.BuildCanonicalV1(tc.make(), compactioncontext.MaxBytesV1, compactioncontext.MaxEventsV1)
			if !errors.Is(err, compactioncontext.ErrCanonical) || view != nil {
				t.Fatalf("malformed semantic history accepted: view-present=%t err=%v", view != nil, err)
			}
		})
	}
	for _, tc := range []struct{ name, body string }{
		{"schema", `{"schema":"wrong","call_id":"c","tool_name":"t","content":null}`},
		{"duplicate", `{"schema":"sessionless.tool-event.v1","call_id":"wrong","call_id":"c","tool_name":"t","content":null}`},
		{"escaped duplicate", `{"schema":"sessionless.tool-event.v1","call_id":"wrong","\u0063all_id":"c","tool_name":"t","content":null}`},
		{"case alias", `{"schema":"sessionless.tool-event.v1","Call_ID":"c","tool_name":"t","content":null}`},
		{"missing content", `{"schema":"sessionless.tool-event.v1","call_id":"c","tool_name":"t"}`},
		{"unknown", `{"schema":"sessionless.tool-event.v1","call_id":"c","tool_name":"t","content":null,"extra":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := []sessioncontext.EventPayload{canonicalRecord(t, 1, call, "run", tc.body), toolRecord(t, 2, result, "run", "c", "t")}
			view, err := compactioncontext.BuildCanonicalV1(records, compactioncontext.MaxBytesV1, compactioncontext.MaxEventsV1)
			if !errors.Is(err, compactioncontext.ErrCanonical) || view != nil {
				t.Fatalf("ambiguous tool envelope accepted: %v", err)
			}
		})
	}
}

func TestCanonicalRejectsSurrogateReplacementCorrelation(t *testing.T) {
	for _, field := range []string{"call_id", "tool_name"} {
		t.Run(field, func(t *testing.T) {
			callBody := `{"schema":"sessionless.tool-event.v1","call_id":"c","tool_name":"t","content":null}`
			resultBody := callBody
			key := `"` + field + `":"`
			value := "c"
			if field == "tool_name" {
				value = "t"
			}
			callBody = strings.Replace(callBody, key+value+`"`, key+`\ud800"`, 1)
			resultBody = strings.Replace(resultBody, key+value+`"`, key+`\ud801"`, 1)
			records := []sessioncontext.EventPayload{canonicalRecord(t, 1, domain.SessionEventToolCall, "run", callBody), canonicalRecord(t, 2, domain.SessionEventToolResult, "run", resultBody)}
			view, err := compactioncontext.BuildCanonicalV1(records, compactioncontext.MaxBytesV1, 2)
			if view != nil {
				view.Clear()
			}
			if !errors.Is(err, compactioncontext.ErrCanonical) {
				t.Fatal("distinct malformed Unicode identities collapsed into one complete tool pair")
			}
		})
	}
}

func TestCanonicalValidEscapedCorrelationIdentities(t *testing.T) {
	for _, tc := range []struct{ name, call, result string }{
		{"valid surrogate pair", `"\ud83d\ude00"`, `"😀"`},
		{"escaped equivalent", `"\u0063"`, `"c"`},
		{"literal replacement character", `"�"`, `"\ufffd"`},
		{"escaped slash is not surrogate", `"\\ud800"`, `"\\ud800"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := func(id string) string {
				return `{"schema":"sessionless.tool-event.v1","call_id":` + id + `,"tool_name":` + id + `,"content":null}`
			}
			records := []sessioncontext.EventPayload{canonicalRecord(t, 1, domain.SessionEventToolCall, "run", body(tc.call)), canonicalRecord(t, 2, domain.SessionEventToolResult, "run", body(tc.result))}
			view := build(t, records)
			if got := view.Units(); len(got) != 1 || got[0] != (compactioncontext.UnitV1{Start: 0, End: 2}) {
				t.Fatalf("valid decoded identity pair split: %v", got)
			}
			copy := view.Records()
			defer clearCopies(copy)
			if !reflect.DeepEqual(copy, records) {
				t.Fatal("original escaped spelling was rewritten")
			}
		})
	}
	for _, raw := range []string{`"\udc00"`, `"\ud800"`, `"\ud800\u0061"`, `"\ud800\ud801"`} {
		t.Run(raw, func(t *testing.T) {
			body := `{"schema":"sessionless.tool-event.v1","call_id":` + raw + `,"tool_name":"t","content":null}`
			records := []sessioncontext.EventPayload{canonicalRecord(t, 1, domain.SessionEventToolCall, "run", body), canonicalRecord(t, 2, domain.SessionEventToolResult, "run", body)}
			if view, err := compactioncontext.BuildCanonicalV1(records, compactioncontext.MaxBytesV1, 2); view != nil || err == nil {
				t.Fatal("malformed surrogate pair accepted even when spellings match")
			}
		})
	}
}

func TestCanonicalPreservesAttachmentReferencesAndHistoricalSystem(t *testing.T) {
	record := userRecord(t, 1)
	attachment := domain.Artifact{Name: "photo.png", MediaType: "image/png", Blob: domain.BlobRef{TenantID: "tenant", Key: domain.SessionEventObjectPrefix("tenant", "session", record.Event.ID) + "photo.png", Size: 99, SHA256: strings.Repeat("a", 64)}}
	body, err := json.Marshal(struct {
		Version     int               `json:"version"`
		Text        string            `json:"text"`
		Attachments []domain.Artifact `json:"attachments"`
	}{1, "preserve this photo", []domain.Artifact{attachment}})
	if err != nil {
		t.Fatal(err)
	}
	replacePayload(&record, string(body))
	system := canonicalRecord(t, 2, domain.SessionEventSystemNotice, "", `{"schema":"sessionless.run-terminal-notice.v1","code":"not_current_policy","cancelled":true}`)
	view := build(t, []sessioncontext.EventPayload{record, system})
	got := view.Records()
	defer clearCopies(got)
	if !bytes.Equal(got[0].Payload, body) || !bytes.Equal(got[1].Payload, system.Payload) || !view.Units()[0].Protected || !view.Units()[1].Protected {
		t.Fatal("attachment reference or historical system original lost/rewritten/unprotected")
	}
	// Structural reference validation is not evidence of seeing image content.
	attachment.Blob.Key = "tenants/other/private"
	badBody, _ := json.Marshal(struct {
		Version     int               `json:"version"`
		Attachments []domain.Artifact `json:"attachments"`
	}{1, []domain.Artifact{attachment}})
	replacePayload(&record, string(badBody))
	if v, err := compactioncontext.BuildCanonicalV1([]sessioncontext.EventPayload{record}, compactioncontext.MaxBytesV1, 1); v != nil || err == nil {
		t.Fatal("cross-scope attachment reference accepted")
	}
}

func TestCanonicalBoundsScopeAndExactEncodedCeiling(t *testing.T) {
	records := []sessioncontext.EventPayload{userRecord(t, 1), assistantRecord(t, 2, "run")}
	var exact uint64
	for _, r := range records {
		line, err := sessioncontext.EncodeRecord(r.Event, r.Payload)
		if err != nil {
			t.Fatal(err)
		}
		exact += uint64(len(line))
		clear(line)
	}
	v, err := compactioncontext.BuildCanonicalV1(records, exact, 2)
	if err != nil {
		t.Fatalf("exact encoded ceiling rejected: %v", err)
	}
	v.Clear()
	for _, tc := range []struct {
		name          string
		bytes, events uint64
	}{
		{"one byte short", exact - 1, 2}, {"zero bytes", 0, 2}, {"zero events", exact, 0}, {"one event", exact, 1},
		{"oversize byte authority", compactioncontext.MaxBytesV1 + 1, 2}, {"oversize event authority", exact, compactioncontext.MaxEventsV1 + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if v, err := compactioncontext.BuildCanonicalV1(records, tc.bytes, tc.events); v != nil || err == nil {
				t.Fatal("invalid bound accepted")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func([]sessioncontext.EventPayload)
	}{
		{"gap", func(r []sessioncontext.EventPayload) { r[1].Event.Sequence = 3 }},
		{"not start one", func(r []sessioncontext.EventPayload) { r[0].Event.Sequence = 2 }},
		{"session", func(r []sessioncontext.EventPayload) { r[1].Event.SessionID = "other" }},
		{"tenant", func(r []sessioncontext.EventPayload) { r[1].Event.TenantID = "other" }},
		{"duplicate identity", func(r []sessioncontext.EventPayload) { r[1].Event.ID = r[0].Event.ID }},
		{"wrong immutable hash", func(r []sessioncontext.EventPayload) { r[0].Payload[0] = ' ' }},
		{"missing run", func(r []sessioncontext.EventPayload) { r[1].Event.RunID = nil }},
		{"huge identity", func(r []sessioncontext.EventPayload) {
			r[0].Event.Payload.Key = strings.Repeat("x", int(compactioncontext.MaxBytesV1)+1)
		}},
		{"malformed user", func(r []sessioncontext.EventPayload) { replacePayload(&r[0], `{"version":2,"text":"private"}`) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := []sessioncontext.EventPayload{userRecord(t, 1), assistantRecord(t, 2, "run")}
			tc.change(r)
			if v, err := compactioncontext.BuildCanonicalV1(r, compactioncontext.MaxBytesV1, compactioncontext.MaxEventsV1); v != nil || err == nil {
				t.Fatal("invalid canonical material accepted")
			}
		})
	}
	if v, err := compactioncontext.BuildCanonicalV1(nil, exact, 2); v != nil || err == nil {
		t.Fatal("empty history accepted")
	}
}

func TestCanonicalDigestIndependentFramingAndPrivateOwnership(t *testing.T) {
	records := []sessioncontext.EventPayload{userRecord(t, 1), assistantRecord(t, 2, "run")}
	view := build(t, records)
	// Independent length framing includes every actual codec record and the
	// derived units, without changing the original codec or its digest domain.
	var framed bytes.Buffer
	framed.WriteString("sessionless.compaction-canonical.v1\x00")
	put := func(n uint64) { var b [8]byte; binary.BigEndian.PutUint64(b[:], n); framed.Write(b[:]) }
	put(2)
	for _, r := range records {
		line, err := sessioncontext.EncodeRecord(r.Event, r.Payload)
		if err != nil {
			t.Fatal(err)
		}
		put(uint64(len(line)))
		framed.Write(line)
		clear(line)
	}
	put(2)
	put(0)
	put(1)
	framed.WriteByte(1)
	put(1)
	put(2)
	framed.WriteByte(0)
	want := sha256.Sum256(framed.Bytes())
	clear(framed.Bytes())
	if view.Digest() != hex.EncodeToString(want[:]) {
		t.Fatal("derived digest omitted/reordered exact canonical bytes or unit boundaries")
	}
	originalDigest := view.Digest()
	original := view.Records()
	defer clearCopies(original)
	records[0].Payload[0] = ' '
	*records[0].Event.AuthorUserID = "changed-user"
	*records[1].Event.RunID = "changed-run"
	copyRecords := view.Records()
	copyRecords[0].Payload[0] = ' '
	*copyRecords[0].Event.AuthorUserID = "other-user"
	*copyRecords[1].Event.RunID = "other-run"
	clearCopies(copyRecords)
	units := view.Units()
	units[0].Protected = false
	readback := view.Records()
	defer clearCopies(readback)
	if !reflect.DeepEqual(readback, original) || view.Digest() != originalDigest || !view.Units()[0].Protected {
		t.Fatal("caller or accessor aliases mutated private source view")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%q", "%x"} {
		if got := fmt.Sprintf(format, view); strings.Contains(got, "private") || strings.Contains(got, originalDigest) {
			t.Fatalf("private view formatting leaked content with format=%s", format)
		}
	}
	if body, err := json.Marshal(view); body != nil || err == nil {
		t.Fatal("private source view serialized")
	}
	alias := *view
	alias.Clear()
	view.Clear()
	if view.Digest() != "" || view.Records() != nil || view.Units() != nil || alias.Digest() != "" {
		t.Fatal("clear did not invalidate ordinary value aliases")
	}
	var zero compactioncontext.ViewV1
	var absent *compactioncontext.ViewV1
	zero.Clear()
	absent.Clear()
	if zero.Digest() != "" || absent.Records() != nil || absent.Units() != nil {
		t.Fatal("zero value supplied verified data")
	}
}

func TestCanonicalConcurrentAccessAndClear(t *testing.T) {
	view := build(t, []sessioncontext.EventPayload{userRecord(t, 1), assistantRecord(t, 2, "run")})
	digest := view.Digest()
	start := make(chan struct{})
	var wg sync.WaitGroup
	errors := make(chan string, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 20; j++ {
				if got := view.Digest(); got != "" && got != digest {
					errors <- "torn digest"
					return
				}
				if r := view.Records(); r != nil {
					if len(r) != 2 || r[0].Payload[0] != '{' {
						errors <- "torn record copy"
						return
					}
					clearCopies(r)
				}
				if u := view.Units(); u != nil && len(u) != 2 {
					errors <- "torn units"
					return
				}
			}
		}()
	}
	close(start)
	view.Clear()
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func FuzzCanonicalToolEnvelope(f *testing.F) {
	f.Add(`{"schema":"sessionless.tool-event.v1","call_id":"c","tool_name":"t","content":null}`)
	f.Add(`{"schema":"sessionless.tool-event.v1","call_id":"other","call_id":"c","tool_name":"t","content":null}`)
	f.Fuzz(func(t *testing.T, body string) {
		if len(body) > 4096 {
			t.Skip()
		}
		records := []sessioncontext.EventPayload{canonicalRecord(t, 1, domain.SessionEventToolCall, "run", body), toolRecord(t, 2, domain.SessionEventToolResult, "run", "c", "t")}
		view, err := compactioncontext.BuildCanonicalV1(records, 16<<10, 2)
		if err != nil {
			if view != nil {
				t.Fatal("failure returned a view")
			}
			return
		}
		defer view.Clear()
		got := view.Records()
		defer clearCopies(got)
		if !reflect.DeepEqual(got, records) || len(view.Units()) != 1 || view.Units()[0] != (compactioncontext.UnitV1{Start: 0, End: 2}) {
			t.Fatal("accepted tool pair was rewritten or split")
		}
	})
}
