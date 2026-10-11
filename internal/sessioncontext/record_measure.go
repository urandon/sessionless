package sessioncontext

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	MaxMeasuredPayloadBytesV1       = 1 << 20
	MaxMeasuredMetadataBytesV1      = 8 << 10
	MaxMeasuredEncodedRecordBytesV1 = MaxMeasuredPayloadBytesV1 + MaxMeasuredMetadataBytesV1
)

// ErrRecordNotMeasurable denotes unknown projection evidence, not a new
// prohibition on otherwise valid canonical history. No private source is
// included in this error. Callers must not replace unknown with measured zero.
var ErrRecordNotMeasurable = errors.New("canonical record not measurable")

// RecordMeasureV1 is an opaque measure of one final canonical record, not a
// prefix, renderer, token, eligibility, authorization or execution verdict.
// It retains no payload or encoded record bytes. The zero value is unverified;
// its private fields cannot be populated by HTTP or other JSON inputs. It
// intentionally refuses serialization and formats without private identities.
type RecordMeasureV1 struct {
	event        domain.SessionEvent
	encodedBytes int64
	digest       string
	verified     bool
}

// MeasureRecordV1 uses the actual canonical codec and final allocated metadata.
// Decode must preserve the immutable payload hash/size and semantic event
// identity. In particular, source JSON is never normalized under its old hash
// and an arbitrary positive sequence is never rewritten to one.
func MeasureRecordV1(event domain.SessionEvent, payload []byte) (RecordMeasureV1, error) {
	if len(payload) == 0 || len(payload) > MaxMeasuredPayloadBytesV1 || !boundedMeasuredMetadata(event) {
		return RecordMeasureV1{}, ErrRecordNotMeasurable
	}
	encoded, err := EncodeRecord(event, payload)
	if err != nil {
		return RecordMeasureV1{}, ErrRecordNotMeasurable
	}
	defer clear(encoded)
	// Payload and metadata have already been bounded before codec allocation.
	// This is the actual encoded-record ceiling, independent of any full-history
	// or rendered-prompt limit applied by a later consumer.
	if len(encoded) > MaxMeasuredEncodedRecordBytesV1 {
		return RecordMeasureV1{}, ErrRecordNotMeasurable
	}
	decoded, err := decodeRecordV1(encoded[:len(encoded)-1], event.TenantID, event.SessionID)
	if err != nil {
		return RecordMeasureV1{}, ErrRecordNotMeasurable
	}
	defer clear(decoded.Payload)
	if !sameMeasuredEvent(event, decoded.Event) {
		return RecordMeasureV1{}, ErrRecordNotMeasurable
	}
	digest := sha256.Sum256(encoded)
	return RecordMeasureV1{
		event: cloneMeasuredEvent(event), encodedBytes: int64(len(encoded)),
		digest: hex.EncodeToString(digest[:]), verified: true,
	}, nil
}

// Matches binds this measure to exact final canonical metadata. Equivalent
// timestamps compare as instants, matching the canonical Session invariant,
// and must also retain the codec's timestamp representation. Losing monotonic
// or location-name metadata is harmless; changing an encoded offset is not.
func (measure RecordMeasureV1) Matches(event domain.SessionEvent) bool {
	return measure.verified && sameMeasuredEvent(measure.event, event) &&
		measure.event.CreatedAt.Format(time.RFC3339Nano) == event.CreatedAt.Format(time.RFC3339Nano)
}

func (measure RecordMeasureV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("canonical record measure cannot be serialized")
}

func (measure RecordMeasureV1) Format(state fmt.State, verb rune) {
	_, _ = fmt.Fprint(state, "sessioncontext.RecordMeasureV1")
}

// EncodedBytes includes the JSONL newline. Unknown never returns a verified 0.
func (measure RecordMeasureV1) EncodedBytes() (int64, bool) {
	return measure.encodedBytes, measure.verified
}

func (measure RecordMeasureV1) Digest() (string, bool) {
	return measure.digest, measure.verified
}

func boundedMeasuredMetadata(event domain.SessionEvent) bool {
	remaining := MaxMeasuredMetadataBytesV1
	strings := []string{
		string(event.ID), string(event.TenantID), string(event.SessionID),
		string(event.Kind), string(event.IdempotencyKey),
		string(event.Payload.TenantID), event.Payload.Key, event.Payload.SHA256,
	}
	if event.AuthorUserID != nil {
		strings = append(strings, string(*event.AuthorUserID))
	}
	if event.RunID != nil {
		strings = append(strings, string(*event.RunID))
	}
	for _, value := range strings {
		if len(value) > remaining {
			return false
		}
		remaining -= len(value)
	}
	return true
}

func cloneMeasuredEvent(event domain.SessionEvent) domain.SessionEvent {
	if event.AuthorUserID != nil {
		value := *event.AuthorUserID
		event.AuthorUserID = &value
	}
	if event.RunID != nil {
		value := *event.RunID
		event.RunID = &value
	}
	return event
}

func sameMeasuredEvent(left, right domain.SessionEvent) bool {
	return left.ID == right.ID && left.TenantID == right.TenantID &&
		left.SessionID == right.SessionID && left.Sequence == right.Sequence &&
		left.Kind == right.Kind && left.IdempotencyKey == right.IdempotencyKey &&
		left.Payload == right.Payload && left.CreatedAt.Equal(right.CreatedAt) &&
		((left.AuthorUserID == nil && right.AuthorUserID == nil) ||
			(left.AuthorUserID != nil && right.AuthorUserID != nil && *left.AuthorUserID == *right.AuthorUserID)) &&
		((left.RunID == nil && right.RunID == nil) ||
			(left.RunID != nil && right.RunID != nil && *left.RunID == *right.RunID))
}
