package sessioncontext_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func TestRecordMeasureUsesActualFinalRecord(t *testing.T) {
	t.Parallel()
	input := contextEvent(t, 2, domain.SessionEventUserMessage, []byte(`{"version":1,"text":"hello"}`))
	input.Event.CreatedAt = time.Date(2026, 8, 17, 12, 0, 2, 123456789, time.UTC)
	before := bytes.Clone(input.Payload)
	measure, err := sessioncontext.MeasureRecordV1(input.Event, input.Payload)
	if err != nil {
		t.Fatal(err)
	}
	// Literal source, not a second encoder or expected value derived through
	// the production codec. This also pins final sequence 2 and its newline.
	literal := `{"schema":"sessionless.context-event.v1","event":{"id":"event-2","tenant_id":"tenant-a","session_id":"session-a","sequence":2,"kind":"user_message","author_user_id":"user-a","idempotency_key":"event-key-2","payload":{"tenant_id":"tenant-a","key":"tenants/tenant-a/sessions/session-a/events/event-2/payload.json","size":28,"sha256":"PAYLOADHASH"},"created_at":"2026-08-17T12:00:02.123456789Z"},"payload":{"version":1,"text":"hello"}}` + "\n"
	literal = strings.Replace(literal, "PAYLOADHASH", "ca499ff825a55abda0411f94dc8c9e7ec938c5d983667aabb5094413338e8344", 1)
	encoded, err := sessioncontext.EncodeRecord(input.Event, input.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, []byte(literal)) {
		t.Fatalf("literal canonical record differs: got %d bytes, want %d", len(encoded), len(literal))
	}
	wantDigest := sha256.Sum256([]byte(literal))
	if got, known := measure.EncodedBytes(); !known || got != int64(len(literal)) {
		t.Errorf("record bytes = %d,%v; want %d,true", got, known, len(literal))
	}
	if got, known := measure.Digest(); !known || got != hex.EncodeToString(wantDigest[:]) {
		t.Errorf("record digest = %q,%v; want literal digest", got, known)
	}
	if !measure.Matches(input.Event) || !bytes.Equal(input.Payload, before) {
		t.Fatal("exact final identity or input ownership lost")
	}
	if _, err := sessioncontext.DecodeJSONL(encoded, input.Event.TenantID, input.Event.SessionID); err == nil {
		t.Fatal("measurement weakened full-history sequence-one rule")
	}
	// No mutable pointer alias from the event input may modify the seal.
	changed := input.Event
	*input.Event.AuthorUserID = "changed-author"
	if measure.Matches(changed) {
		t.Fatal("mutated caller author pointer changed the sealed identity")
	}
	author := domain.UserID("user-a")
	changed.AuthorUserID = &author
	changed.CreatedAt = changed.CreatedAt.In(time.FixedZone("equivalent-zero", 0))
	if !measure.Matches(changed) {
		t.Fatal("location name alone changed canonical timestamp identity")
	}
	changed.CreatedAt = changed.CreatedAt.In(time.FixedZone("different-offset", 3600))
	if measure.Matches(changed) {
		t.Fatal("different encoded timestamp offset reused old record digest")
	}
}

func TestRecordMeasureUnknownAndNonRenderingRecords(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		payload []byte
		known   bool
	}{
		{name: "compact", payload: []byte(`{"version":1,"text":"hello"}`), known: true},
		{name: "notice", payload: []byte(`{"schema":"notice.v1"}`), known: true},
		{name: "attachments", payload: []byte(`{"version":1,"attachments":[{"name":"file"}]}`), known: true},
		{name: "whitespace", payload: []byte(`{ "version":1,"text":"hello"}`)},
		{name: "html-escaping", payload: []byte(`{"text":"<private>"}`)},
		{name: "already-escaped", payload: []byte(`{"text":"\u003cprivate\u003e"}`), known: true},
		{name: "malformed", payload: []byte(`{"text":`)},
		{name: "empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := contextEvent(t, 2, domain.SessionEventSystemNotice, test.payload)
			before := bytes.Clone(test.payload)
			measure, err := sessioncontext.MeasureRecordV1(input.Event, test.payload)
			if test.known {
				if err != nil || !measure.Matches(input.Event) {
					t.Fatalf("expected measurable canonical source: %v", err)
				}
			} else {
				if !errors.Is(err, sessioncontext.ErrRecordNotMeasurable) {
					t.Fatalf("unknown error = %v", err)
				}
				if got, known := measure.EncodedBytes(); known || got != 0 {
					t.Fatalf("unknown size = %d,%v", got, known)
				}
				if got, known := measure.Digest(); known || got != "" {
					t.Fatalf("unknown digest = %q,%v", got, known)
				}
				if measure.Matches(input.Event) {
					t.Fatal("unknown matched source")
				}
			}
			if !bytes.Equal(before, test.payload) {
				t.Fatal("private caller bytes changed")
			}
		})
	}
	var zero sessioncontext.RecordMeasureV1
	if zero.Matches(domain.SessionEvent{}) {
		t.Fatal("zero measure verified")
	}
}

func TestRecordMeasureBoundsAndIdentity(t *testing.T) {
	t.Parallel()
	input := contextEvent(t, 2, domain.SessionEventUserMessage, []byte(`{"version":1,"text":"hello"}`))
	for _, test := range []struct {
		name   string
		mutate func(*domain.SessionEvent)
	}{
		{name: "zero sequence", mutate: func(e *domain.SessionEvent) { e.Sequence = 0 }},
		{name: "payload hash", mutate: func(e *domain.SessionEvent) { e.Payload.SHA256 = strings.Repeat("0", 64) }},
		{name: "payload size", mutate: func(e *domain.SessionEvent) { e.Payload.Size++ }},
		{name: "foreign tenant", mutate: func(e *domain.SessionEvent) { e.TenantID = "foreign" }},
		{name: "foreign session", mutate: func(e *domain.SessionEvent) { e.SessionID = "foreign" }},
		{name: "foreign event", mutate: func(e *domain.SessionEvent) { e.ID = "foreign" }},
		{name: "missing author", mutate: func(e *domain.SessionEvent) { e.AuthorUserID = nil }},
		{name: "empty timestamp", mutate: func(e *domain.SessionEvent) { e.CreatedAt = time.Time{} }},
		{name: "unbounded key", mutate: func(e *domain.SessionEvent) {
			e.Payload.Key += strings.Repeat("k", sessioncontext.MaxMeasuredMetadataBytesV1)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			e := input.Event
			test.mutate(&e)
			if m, err := sessioncontext.MeasureRecordV1(e, input.Payload); !errors.Is(err, sessioncontext.ErrRecordNotMeasurable) || m.Matches(e) {
				t.Fatalf("invalid source measured: %v", err)
			}
		})
	}
	for _, sequence := range []uint64{2, 1 << 63, ^uint64(0)} {
		e := input.Event
		e.Sequence = sequence
		if m, err := sessioncontext.MeasureRecordV1(e, input.Payload); err != nil || !m.Matches(e) {
			t.Fatalf("positive sequence %d changed/rejected: %v", sequence, err)
		}
	}
	for _, size := range []int{sessioncontext.MaxMeasuredPayloadBytesV1, sessioncontext.MaxMeasuredPayloadBytesV1 + 1} {
		payload := []byte(`{"data":"` + strings.Repeat("a", size-len(`{"data":""}`)) + `"}`)
		e := contextEvent(t, 2, domain.SessionEventSystemNotice, payload)
		m, err := sessioncontext.MeasureRecordV1(e.Event, payload)
		if size == sessioncontext.MaxMeasuredPayloadBytesV1 {
			if err != nil || !m.Matches(e.Event) {
				t.Fatalf("exact payload bound rejected: %v", err)
			}
		} else if !errors.Is(err, sessioncontext.ErrRecordNotMeasurable) {
			t.Fatalf("over-bound payload error = %v", err)
		}
	}
}

func TestRecordMeasureDoesNotSerializeOrDiscloseSource(t *testing.T) {
	t.Parallel()
	input := contextEvent(t, 2, domain.SessionEventUserMessage, []byte(`{"version":1,"text":"private-marker"}`))
	m, err := sessioncontext.MeasureRecordV1(input.Event, input.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(m); err == nil {
		t.Fatal("private measure serialized")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%x", "%d"} {
		if got := fmt.Sprintf(format, m); got != "sessioncontext.RecordMeasureV1" {
			t.Fatalf("format %q disclosed measure: %q", format, got)
		}
	}
	input.Event.Payload.Key = "tenants/foreign/private-marker"
	_, err = sessioncontext.MeasureRecordV1(input.Event, input.Payload)
	if err != sessioncontext.ErrRecordNotMeasurable || strings.Contains(err.Error(), "private-marker") {
		t.Fatalf("source error leaked: %v", err)
	}
}

func TestRecordMeasureFinalMetadataChangesCannotReuseEvidence(t *testing.T) {
	t.Parallel()
	input := contextEvent(t, 2, domain.SessionEventUserMessage, []byte(`{"version":1,"text":"hello"}`))
	original, err := sessioncontext.MeasureRecordV1(input.Event, input.Payload)
	if err != nil {
		t.Fatal(err)
	}
	originalDigest, _ := original.Digest()
	for _, test := range []struct {
		name   string
		mutate func(*domain.SessionEvent)
	}{
		{name: "sequence", mutate: func(e *domain.SessionEvent) { e.Sequence++ }},
		{name: "final timestamp", mutate: func(e *domain.SessionEvent) { e.CreatedAt = e.CreatedAt.Add(time.Nanosecond) }},
		{name: "event identity", mutate: func(e *domain.SessionEvent) {
			e.ID = "event-new"
			e.Payload.Key = domain.SessionEventObjectPrefix(e.TenantID, e.SessionID, e.ID) + "payload.json"
		}},
		{name: "tenant", mutate: func(e *domain.SessionEvent) {
			e.TenantID = "tenant-new"
			e.Payload.TenantID = e.TenantID
			e.Payload.Key = domain.SessionEventObjectPrefix(e.TenantID, e.SessionID, e.ID) + "payload.json"
		}},
		{name: "session", mutate: func(e *domain.SessionEvent) {
			e.SessionID = "session-new"
			e.Payload.Key = domain.SessionEventObjectPrefix(e.TenantID, e.SessionID, e.ID) + "payload.json"
		}},
		{name: "author", mutate: func(e *domain.SessionEvent) { value := domain.UserID("user-new"); e.AuthorUserID = &value }},
		{name: "run", mutate: func(e *domain.SessionEvent) { value := domain.RunID("run-new"); e.RunID = &value }},
		{name: "idempotency key", mutate: func(e *domain.SessionEvent) { e.IdempotencyKey = "key-new" }},
		{name: "payload key", mutate: func(e *domain.SessionEvent) { e.Payload.Key += "-new" }},
		{name: "kind", mutate: func(e *domain.SessionEvent) { e.Kind = domain.SessionEventSystemNotice }},
		{name: "encoded timezone", mutate: func(e *domain.SessionEvent) { e.CreatedAt = e.CreatedAt.In(time.FixedZone("other", 3600)) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := input.Event
			test.mutate(&event)
			if original.Matches(event) {
				t.Fatal("changed final record reused source evidence")
			}
			changed, err := sessioncontext.MeasureRecordV1(event, input.Payload)
			if err != nil {
				t.Fatalf("valid changed final record: %v", err)
			}
			if digest, known := changed.Digest(); !known || digest == originalDigest {
				t.Fatal("changed encoded identity retained original digest")
			}
		})
	}
}

func TestRecordMeasureMetadataAndEncodedRecordCeilings(t *testing.T) {
	t.Parallel()
	input := contextEvent(t, 2, domain.SessionEventUserMessage, []byte(`{"version":1,"text":"hello"}`))
	// Derive this fixture's raw metadata sum independently from the public
	// event shape, without invoking the production bound predicate.
	size := len(input.Event.ID) + len(input.Event.TenantID) + len(input.Event.SessionID) + len(input.Event.Kind) + len(input.Event.IdempotencyKey) + len(input.Event.Payload.TenantID) + len(input.Event.Payload.Key) + len(input.Event.Payload.SHA256) + len(*input.Event.AuthorUserID)
	input.Event.Payload.Key += strings.Repeat("k", sessioncontext.MaxMeasuredMetadataBytesV1-size)
	if _, err := sessioncontext.MeasureRecordV1(input.Event, input.Payload); err != nil {
		t.Fatalf("exact raw metadata limit: %v", err)
	}
	extra := input.Event
	extra.Payload.Key += "k"
	if _, err := sessioncontext.MeasureRecordV1(extra, input.Payload); !errors.Is(err, sessioncontext.ErrRecordNotMeasurable) {
		t.Fatalf("metadata limit +1: %v", err)
	}
	// Keep raw metadata at the bound while independently obtaining the fixed
	// canonical framing length, then vary only payload length by one byte.
	base, err := sessioncontext.EncodeRecord(input.Event, input.Payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int{0, 1} {
		payloadSize := sessioncontext.MaxMeasuredEncodedRecordBytesV1 - len(base) + len(input.Payload) + offset
		payload := []byte(`{"data":"` + strings.Repeat("a", payloadSize-len(`{"data":""}`)) + `"}`)
		event := input.Event
		sum := sha256.Sum256(payload)
		event.Payload.Size = int64(len(payload))
		event.Payload.SHA256 = hex.EncodeToString(sum[:])
		// Size decimal width changed from the small fixture; compensate for
		// that actual codec overhead without changing immutable source bytes.
		encoded, err := sessioncontext.EncodeRecord(event, payload)
		if err != nil {
			t.Fatal(err)
		}
		delta := len(encoded) - (sessioncontext.MaxMeasuredEncodedRecordBytesV1 + offset)
		payload = []byte(`{"data":"` + strings.Repeat("a", payloadSize-delta-len(`{"data":""}`)) + `"}`)
		sum = sha256.Sum256(payload)
		event.Payload.Size = int64(len(payload))
		event.Payload.SHA256 = hex.EncodeToString(sum[:])
		encoded, err = sessioncontext.EncodeRecord(event, payload)
		if err != nil || len(encoded) != sessioncontext.MaxMeasuredEncodedRecordBytesV1+offset {
			t.Fatalf("record-bound fixture: bytes %d, offset %d, error %v", len(encoded), offset, err)
		}
		m, err := sessioncontext.MeasureRecordV1(event, payload)
		if offset == 0 {
			if err != nil || !m.Matches(event) {
				t.Fatalf("exact encoded bound: %v", err)
			}
		} else if !errors.Is(err, sessioncontext.ErrRecordNotMeasurable) {
			t.Fatalf("encoded bound +1: %v", err)
		}
	}
	// Standard JSON's finite nesting limit and this helper's input ceiling
	// bound parser work; nested content is not a renderer-compatibility claim.
	for _, depth := range []int{128, 10001} {
		payload := []byte(strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth))
		input := contextEvent(t, 2, domain.SessionEventSystemNotice, payload)
		_, err := sessioncontext.MeasureRecordV1(input.Event, payload)
		if depth == 128 {
			if err != nil {
				t.Fatalf("bounded nesting: %v", err)
			}
		} else if !errors.Is(err, sessioncontext.ErrRecordNotMeasurable) {
			t.Fatalf("over nesting: %v", err)
		}
	}
}

func TestRecordDecoderPreservesOuterJSONSemanticsAndHistoryOrdering(t *testing.T) {
	t.Parallel()
	input := contextEvent(t, 1, domain.SessionEventUserMessage, []byte(`{"version":1,"text":"hello"}`))
	encoded, err := sessioncontext.EncodeRecord(input.Event, input.Payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "unknown outer field", data: bytes.Replace(encoded, []byte(`{"schema":`), []byte(`{"unused":true,"schema":`), 1)},
		{name: "case folded outer field", data: bytes.Replace(encoded, []byte(`"schema":`), []byte(`"SCHEMA":`), 1)},
		{name: "duplicate schema last wins", data: bytes.Replace(encoded, []byte(`{"schema":`), []byte(`{"schema":"old","schema":`), 1)},
		{name: "null first schema", data: bytes.Replace(encoded, []byte(`{"schema":`), []byte(`{"schema":null,"schema":`), 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			decoded, err := sessioncontext.DecodeJSONL(test.data, input.Event.TenantID, input.Event.SessionID)
			if err != nil || len(decoded) != 1 || !bytes.Equal(decoded[0].Payload, input.Payload) {
				t.Fatalf("outer semantics changed: %v", err)
			}
		})
	}
	for _, sequences := range [][]uint64{{2}, {1, 3}, {1, 1}} {
		var history []byte
		for _, sequence := range sequences {
			e := input.Event
			e.Sequence = sequence
			line, err := sessioncontext.EncodeRecord(e, input.Payload)
			if err != nil {
				t.Fatal(err)
			}
			history = append(history, line...)
		}
		if _, err := sessioncontext.DecodeJSONL(history, input.Event.TenantID, input.Event.SessionID); err == nil {
			t.Fatalf("invalid full history %v accepted", sequences)
		}
	}
}
