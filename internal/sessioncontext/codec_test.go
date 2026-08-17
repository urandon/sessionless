package sessioncontext_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func TestSnapshotEncodingIsDeterministicAndRoundTrips(t *testing.T) {
	t.Parallel()
	events := []sessioncontext.EventPayload{
		contextEvent(t, 1, domain.SessionEventUserMessage, []byte(`{"version":1,"text":"hello"}`)),
		contextEvent(t, 2, domain.SessionEventSystemNotice, []byte(`{"schema":"notice.v1"}`)),
	}
	compressedA, jsonlA, err := sessioncontext.EncodeSnapshot(events)
	if err != nil {
		t.Fatal(err)
	}
	compressedB, jsonlB, err := sessioncontext.EncodeSnapshot(events)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(compressedA, compressedB) || !bytes.Equal(jsonlA, jsonlB) {
		t.Fatal("same immutable history produced different snapshot bytes")
	}
	digest := sha256.Sum256(compressedA)
	snapshot := domain.SessionSnapshot{
		ID: "snapshot-1", TenantID: "tenant-a", SessionID: "session-a",
		Version: 1, ThroughSequence: 2,
		FormatVersion: domain.SessionSnapshotFormatV1,
		Compression:   domain.SessionSnapshotCompressionZstandard,
		EventCount:    2, UncompressedSize: uint64(len(jsonlA)),
		Payload: domain.BlobRef{
			TenantID: "tenant-a", Key: domain.SessionSnapshotObjectKey("tenant-a", "session-a", 1),
			Size: int64(len(compressedA)), SHA256: hex.EncodeToString(digest[:]),
		},
		CreatedAt: events[1].Event.CreatedAt,
	}
	decoded, decodedJSONL, err := sessioncontext.DecodeSnapshot(compressedA, snapshot, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(events) || !bytes.Equal(decodedJSONL, jsonlA) {
		t.Fatalf("decoded snapshot differs: events=%d bytes_equal=%v", len(decoded), bytes.Equal(decodedJSONL, jsonlA))
	}
}

func TestSnapshotDecoderRejectsCorruptionAndTenantMismatch(t *testing.T) {
	t.Parallel()
	event := contextEvent(t, 1, domain.SessionEventUserMessage, []byte(`{"version":1,"text":"hello"}`))
	compressed, jsonl, err := sessioncontext.EncodeSnapshot([]sessioncontext.EventPayload{event})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(compressed)
	snapshot := domain.SessionSnapshot{
		ID: "snapshot-1", TenantID: "tenant-a", SessionID: "session-a",
		Version: 1, ThroughSequence: 1,
		FormatVersion: domain.SessionSnapshotFormatV1,
		Compression:   domain.SessionSnapshotCompressionZstandard,
		EventCount:    1, UncompressedSize: uint64(len(jsonl)),
		Payload:   domain.BlobRef{TenantID: "tenant-a", Key: domain.SessionSnapshotObjectKey("tenant-a", "session-a", 1), Size: int64(len(compressed)), SHA256: hex.EncodeToString(digest[:])},
		CreatedAt: event.Event.CreatedAt,
	}
	corrupt := append([]byte(nil), compressed...)
	corrupt[len(corrupt)/2] ^= 0xff
	if _, _, err := sessioncontext.DecodeSnapshot(corrupt, snapshot, 1<<20); err == nil {
		t.Fatal("corrupt snapshot was accepted")
	}
	record := append([]byte(nil), jsonl...)
	if _, err := sessioncontext.DecodeJSONL(record, "tenant-b", "session-a"); err == nil {
		t.Fatal("cross-tenant snapshot record was accepted")
	}
}

func contextEvent(t *testing.T, sequence uint64, kind domain.SessionEventKind, payload []byte) sessioncontext.EventPayload {
	t.Helper()
	digest := sha256.Sum256(payload)
	author := domain.UserID("user-a")
	event := domain.SessionEvent{
		ID:       domain.SessionEventID("event-" + string(rune('0'+sequence))),
		TenantID: "tenant-a", SessionID: "session-a", Sequence: sequence,
		Kind: kind, IdempotencyKey: domain.IdempotencyKey("event-key-" + string(rune('0'+sequence))),
		Payload: domain.BlobRef{
			TenantID: "tenant-a",
			Key:      domain.SessionEventObjectPrefix("tenant-a", "session-a", domain.SessionEventID("event-"+string(rune('0'+sequence)))) + "payload.json",
			Size:     int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
		},
		CreatedAt: time.Date(2026, 8, 17, 12, 0, int(sequence), 0, time.UTC),
	}
	if kind == domain.SessionEventUserMessage {
		event.AuthorUserID = &author
	}
	return sessioncontext.EventPayload{Event: event, Payload: payload}
}
