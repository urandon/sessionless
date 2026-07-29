package queuecontract_test

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/queuecontract"
)

func TestVersionedFixturesRoundTrip(t *testing.T) {
	t.Parallel()

	fixtures := []string{"dispatch-v1.json", "telegram-delivery-v1.json"}
	for _, name := range fixtures {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			data, err := os.ReadFile(filepath.Join("..", "..", "test", "fixtures", "queue", name))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			first, err := queuecontract.Decode(data)
			if err != nil {
				t.Fatalf("Decode(fixture) error = %v", err)
			}
			encoded, err := queuecontract.Encode(first)
			if err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			second, err := queuecontract.Decode(encoded)
			if err != nil {
				t.Fatalf("Decode(encoded) error = %v", err)
			}
			if !reflect.DeepEqual(first, second) {
				t.Fatalf("round trip changed envelope:\nfirst:  %+v\nsecond: %+v", first, second)
			}
		})
	}
}

func TestEnvelopeRejectsPayloadFields(t *testing.T) {
	t.Parallel()

	data := []byte(`{
		"schema":"sessionless.queue.v1",
		"message_id":"msg-1",
		"kind":"dispatch.run",
		"tenant_id":"tenant-a",
		"subject_id":"dispatch-1",
		"enqueued_at":"2026-07-28T12:00:00Z",
		"prompt":"must never enter the queue"
	}`)
	if _, err := queuecontract.Decode(data); err == nil {
		t.Fatal("Decode() accepted a prompt field")
	}
}

func TestEnvelopeRejectsTrailingJSON(t *testing.T) {
	t.Parallel()

	data := bytes.Join([][]byte{
		[]byte(`{"schema":"sessionless.queue.v1","message_id":"msg-1","kind":"dispatch.run","tenant_id":"tenant-a","subject_id":"dispatch-1","enqueued_at":"2026-07-28T12:00:00Z"}`),
		[]byte(`{"extra":true}`),
	}, nil)
	if _, err := queuecontract.Decode(data); err == nil {
		t.Fatal("Decode() accepted a trailing JSON value")
	}
}
