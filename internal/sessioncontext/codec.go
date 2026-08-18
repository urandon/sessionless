// Package sessioncontext defines the immutable, deterministic wire format used
// to materialize canonical session history inside stateless workers.
package sessioncontext

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const RecordSchemaV1 = "sessionless.context-event.v1"

type Record struct {
	Schema  string              `json:"schema"`
	Event   domain.SessionEvent `json:"event"`
	Payload json.RawMessage     `json:"payload"`
}

type EventPayload struct {
	Event   domain.SessionEvent
	Payload []byte
}

func EncodeRecord(event domain.SessionEvent, payload []byte) ([]byte, error) {
	if err := validateEventPayload(event, payload); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(Record{
		Schema: RecordSchemaV1, Event: event, Payload: json.RawMessage(payload),
	})
	if err != nil {
		return nil, fmt.Errorf("encode context record: %w", err)
	}
	return append(encoded, '\n'), nil
}

// EncodeSnapshot returns deterministic Zstandard bytes and the exact JSONL
// representation that workers reconstruct when replaying without a snapshot.
func EncodeSnapshot(events []EventPayload) (compressed []byte, jsonl []byte, err error) {
	var raw bytes.Buffer
	for index, item := range events {
		if item.Event.Sequence != uint64(index+1) {
			return nil, nil, domain.ValidationError{
				Field: "session_snapshot.events", Reason: "must start at one and be contiguous",
			}
		}
		line, err := EncodeRecord(item.Event, item.Payload)
		if err != nil {
			return nil, nil, err
		}
		_, _ = raw.Write(line)
	}
	var output bytes.Buffer
	encoder, err := zstd.NewWriter(
		&output,
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(true),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("create snapshot encoder: %w", err)
	}
	if _, err := encoder.Write(raw.Bytes()); err != nil {
		encoder.Close()
		return nil, nil, fmt.Errorf("compress snapshot: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return nil, nil, fmt.Errorf("close snapshot encoder: %w", err)
	}
	return output.Bytes(), raw.Bytes(), nil
}

func DecodeSnapshot(compressed []byte, snapshot domain.SessionSnapshot, maxBytes uint64) ([]EventPayload, []byte, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, nil, err
	}
	if snapshot.UncompressedSize > maxBytes {
		return nil, nil, domain.ValidationError{
			Field: "session_snapshot.uncompressed_size", Reason: "exceeds the admitted context limit",
		}
	}
	decoder, err := zstd.NewReader(bytes.NewReader(compressed), zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, nil, fmt.Errorf("create snapshot decoder: %w", err)
	}
	decompressed, readErr := io.ReadAll(io.LimitReader(decoder, int64(maxBytes)+1))
	decoder.Close()
	if readErr != nil {
		return nil, nil, fmt.Errorf("decompress snapshot: %w", readErr)
	}
	if uint64(len(decompressed)) > maxBytes || uint64(len(decompressed)) != snapshot.UncompressedSize {
		return nil, nil, fmt.Errorf("snapshot uncompressed size does not match immutable metadata")
	}
	records, err := DecodeJSONL(decompressed, snapshot.TenantID, snapshot.SessionID)
	if err != nil {
		return nil, nil, err
	}
	if uint64(len(records)) != snapshot.EventCount ||
		len(records) == 0 || records[len(records)-1].Event.Sequence != snapshot.ThroughSequence {
		return nil, nil, fmt.Errorf("snapshot coverage does not match immutable metadata")
	}
	return records, decompressed, nil
}

func DecodeJSONL(data []byte, tenantID domain.TenantID, sessionID domain.SessionID) ([]EventPayload, error) {
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return nil, fmt.Errorf("context JSONL must end with a newline")
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	// Individual event payloads remain bounded by the admitted context limit;
	// raise Scanner's default token ceiling without making it unbounded.
	scanner.Buffer(make([]byte, 64<<10), len(data))
	result := make([]EventPayload, 0)
	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode context record: %w", err)
		}
		if record.Schema != RecordSchemaV1 {
			return nil, fmt.Errorf("unsupported context record schema %q", record.Schema)
		}
		if record.Event.TenantID != tenantID || record.Event.SessionID != sessionID {
			return nil, domain.ValidationError{
				Field: "context_record.event", Reason: "crosses the requested tenant or session boundary",
			}
		}
		payload := append([]byte(nil), record.Payload...)
		if err := validateEventPayload(record.Event, payload); err != nil {
			return nil, err
		}
		if record.Event.Sequence != uint64(len(result)+1) {
			return nil, domain.ValidationError{
				Field: "context_record.event.sequence", Reason: "must start at one and be contiguous",
			}
		}
		result = append(result, EventPayload{Event: record.Event, Payload: payload})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan context JSONL: %w", err)
	}
	return result, nil
}

func validateEventPayload(event domain.SessionEvent, payload []byte) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if !json.Valid(payload) {
		return domain.ValidationError{Field: "session_event.payload", Reason: "must be valid JSON"}
	}
	digest := sha256.Sum256(payload)
	if event.Payload.Size != int64(len(payload)) || event.Payload.SHA256 != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("session event payload does not match its immutable reference")
	}
	return nil
}
