package sessioncontext_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func TestSnapshotBuilderIsRetryDeterministic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	items := []sessioncontext.EventPayload{
		contextEvent(t, 1, domain.SessionEventUserMessage, []byte(`{"version":1,"text":"one"}`)),
		contextEvent(t, 2, domain.SessionEventSystemNotice, []byte(`{"schema":"notice.v1"}`)),
	}
	store := &snapshotStore{events: []domain.SessionEvent{items[0].Event, items[1].Event}}
	blobs := &snapshotBlobs{data: make(map[string][]byte)}
	for _, item := range items {
		blobs.data[item.Event.Payload.Key] = append([]byte(nil), item.Payload...)
	}
	builder, err := sessioncontext.NewSnapshotBuilder(store, blobs)
	if err != nil {
		t.Fatal(err)
	}
	request := sessioncontext.SnapshotRequest{
		TenantID: "tenant-a", SessionID: "session-a", Version: 1,
		ThroughSequence: 2, MaxEvents: 10, MaxBytes: 1 << 20,
	}
	first, err := builder.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := builder.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("retry changed snapshot metadata:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if first.Payload.Key != domain.SessionSnapshotObjectKey("tenant-a", "session-a", 1) {
		t.Fatalf("snapshot key = %q", first.Payload.Key)
	}
}

type snapshotStore struct {
	events   []domain.SessionEvent
	snapshot *domain.SessionSnapshot
}

func (store *snapshotStore) ListSessionHistory(
	_ context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	afterSequence uint64,
	limit uint64,
) ([]domain.SessionEvent, error) {
	result := make([]domain.SessionEvent, 0)
	for _, event := range store.events {
		if event.TenantID == tenantID && event.SessionID == sessionID && event.Sequence > afterSequence {
			result = append(result, event)
			if uint64(len(result)) == limit {
				break
			}
		}
	}
	return result, nil
}

func (store *snapshotStore) PutSessionSnapshot(_ context.Context, snapshot domain.SessionSnapshot) error {
	if store.snapshot != nil && *store.snapshot != snapshot {
		return errors.New("snapshot retry conflict")
	}
	copy := snapshot
	store.snapshot = &copy
	return nil
}

type snapshotBlobs struct {
	data map[string][]byte
}

func (store *snapshotBlobs) Put(
	_ context.Context,
	tenantID domain.TenantID,
	key string,
	body io.Reader,
) (domain.BlobRef, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return domain.BlobRef{}, err
	}
	if previous, ok := store.data[key]; ok && !bytes.Equal(previous, data) {
		return domain.BlobRef{}, errors.New("immutable blob conflict")
	}
	store.data[key] = append([]byte(nil), data...)
	digest := sha256.Sum256(data)
	return domain.BlobRef{TenantID: tenantID, Key: key, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}, nil
}

func (store *snapshotBlobs) Open(
	_ context.Context,
	tenantID domain.TenantID,
	ref domain.BlobRef,
) (io.ReadCloser, error) {
	if tenantID != ref.TenantID {
		return nil, domain.TenantMismatchError{Expected: tenantID, Actual: ref.TenantID}
	}
	data, ok := store.data[ref.Key]
	if !ok {
		return nil, errors.New("blob not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (store *snapshotBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	return nil
}
