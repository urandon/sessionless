package sessioncontext

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const snapshotPageSize uint64 = 256

type SnapshotStore interface {
	ListSessionHistory(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, afterSequence uint64, limit uint64) ([]domain.SessionEvent, error)
	PutSessionSnapshot(ctx context.Context, snapshot domain.SessionSnapshot) error
}

type SnapshotBuilder struct {
	store SnapshotStore
	blobs ports.BlobStore
}

type SnapshotRequest struct {
	TenantID        domain.TenantID
	SessionID       domain.SessionID
	Version         uint64
	ThroughSequence uint64
	MaxEvents       uint64
	MaxBytes        uint64
}

func NewSnapshotBuilder(store SnapshotStore, blobs ports.BlobStore) (*SnapshotBuilder, error) {
	if store == nil || blobs == nil {
		return nil, fmt.Errorf("snapshot builder dependencies must not be nil")
	}
	return &SnapshotBuilder{store: store, blobs: blobs}, nil
}

func (builder *SnapshotBuilder) Create(
	ctx context.Context,
	request SnapshotRequest,
) (domain.SessionSnapshot, error) {
	if err := validateSnapshotRequest(request); err != nil {
		return domain.SessionSnapshot{}, err
	}
	events := make([]EventPayload, 0, minInt(request.MaxEvents, request.ThroughSequence))
	after := uint64(0)
	var payloadBytes uint64
	for after < request.ThroughSequence {
		limit := snapshotPageSize
		if remaining := request.ThroughSequence - after; remaining < limit {
			limit = remaining
		}
		page, err := builder.store.ListSessionHistory(
			ctx, request.TenantID, request.SessionID, after, limit,
		)
		if err != nil {
			return domain.SessionSnapshot{}, err
		}
		if len(page) == 0 {
			return domain.SessionSnapshot{}, domain.ValidationError{
				Field: "session_snapshot.events", Reason: "does not reach the requested boundary",
			}
		}
		for _, event := range page {
			if event.Sequence != after+1 || event.Sequence > request.ThroughSequence {
				return domain.SessionSnapshot{}, domain.ValidationError{
					Field: "session_snapshot.events", Reason: "must be contiguous through the requested boundary",
				}
			}
			if uint64(len(events)) >= request.MaxEvents {
				return domain.SessionSnapshot{}, domain.ValidationError{
					Field: "session_snapshot.events", Reason: "exceeds the configured event limit",
				}
			}
			payload, err := readVerifiedBlob(ctx, builder.blobs, request.TenantID, event.Payload, request.MaxBytes-payloadBytes)
			if err != nil {
				return domain.SessionSnapshot{}, err
			}
			payloadBytes += uint64(len(payload))
			events = append(events, EventPayload{Event: event, Payload: payload})
			after = event.Sequence
		}
	}
	compressed, jsonl, err := EncodeSnapshot(events)
	if err != nil {
		return domain.SessionSnapshot{}, err
	}
	if uint64(len(jsonl)) > request.MaxBytes {
		return domain.SessionSnapshot{}, domain.ValidationError{
			Field: "session_snapshot.bytes", Reason: "exceeds the configured byte limit",
		}
	}
	digest := sha256.Sum256(compressed)
	snapshotID := domain.SessionSnapshotID("snapshot-" + hex.EncodeToString(digest[:12]))
	ref := domain.BlobRef{
		TenantID: request.TenantID,
		Key:      domain.SessionSnapshotObjectKey(request.TenantID, request.SessionID, request.Version),
		Size:     int64(len(compressed)), SHA256: hex.EncodeToString(digest[:]),
	}
	snapshot := domain.SessionSnapshot{
		ID: snapshotID, TenantID: request.TenantID, SessionID: request.SessionID,
		Version: request.Version, ThroughSequence: request.ThroughSequence,
		FormatVersion: domain.SessionSnapshotFormatV1,
		Compression:   domain.SessionSnapshotCompressionZstandard,
		EventCount:    uint64(len(events)), UncompressedSize: uint64(len(jsonl)),
		Payload: ref, CreatedAt: events[len(events)-1].Event.CreatedAt,
	}
	if err := builder.store.PutSessionSnapshot(ctx, snapshot); err != nil {
		return domain.SessionSnapshot{}, err
	}
	// The YDB manifest is the uniqueness gate for this session/version. Writing
	// it first prevents a conflicting builder from replacing the stable object
	// key. A crash between these operations is safe: workers fall back to replay,
	// and an exact retry repairs the missing object with identical bytes.
	stored, err := builder.blobs.Put(ctx, request.TenantID, ref.Key, bytes.NewReader(compressed))
	if err != nil {
		return domain.SessionSnapshot{}, err
	}
	if stored != ref {
		return domain.SessionSnapshot{}, fmt.Errorf("snapshot blob store returned mismatched immutable metadata")
	}
	return snapshot, nil
}

func validateSnapshotRequest(request SnapshotRequest) error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.SessionID.Validate(); err != nil {
		return err
	}
	if request.Version == 0 || request.ThroughSequence == 0 {
		return domain.ValidationError{Field: "snapshot_request.boundary", Reason: "version and sequence must be positive"}
	}
	if request.MaxEvents == 0 || request.MaxBytes == 0 {
		return domain.ValidationError{Field: "snapshot_request.limits", Reason: "must be positive"}
	}
	if request.MaxBytes > uint64(^uint64(0)>>1) {
		return domain.ValidationError{Field: "snapshot_request.max_bytes", Reason: "must fit a signed byte count"}
	}
	if request.ThroughSequence > request.MaxEvents {
		return domain.ValidationError{Field: "snapshot_request.max_events", Reason: "is below the requested boundary"}
	}
	return nil
}

func readVerifiedBlob(
	ctx context.Context,
	blobs ports.BlobStore,
	tenantID domain.TenantID,
	ref domain.BlobRef,
	maxBytes uint64,
) ([]byte, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	if err := domain.EnsureSameTenant(tenantID, ref.TenantID); err != nil {
		return nil, err
	}
	if ref.Size < 0 || uint64(ref.Size) > maxBytes {
		return nil, domain.ValidationError{Field: "snapshot_request.bytes", Reason: "exceeds the configured byte limit"}
	}
	reader, err := blobs.Open(ctx, tenantID, ref)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	body, err := io.ReadAll(io.LimitReader(reader, int64(maxBytes)+1))
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(body)
	if int64(len(body)) != ref.Size || hex.EncodeToString(digest[:]) != ref.SHA256 {
		return nil, fmt.Errorf("session event payload does not match its immutable reference")
	}
	return body, nil
}

func minInt(left, right uint64) int {
	if left > right {
		left = right
	}
	maxInt := uint64(^uint(0) >> 1)
	if left > maxInt {
		return int(maxInt)
	}
	return int(left)
}
