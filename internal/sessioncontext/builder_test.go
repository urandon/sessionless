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

func TestSnapshotBuilderRejectsCrossSessionEventBeforeOpeningPayload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	item := contextEvent(t, 1, domain.SessionEventUserMessage, []byte(`{"version":1,"text":"wrong session"}`))
	item.Event.SessionID = "session-b"
	item.Event.Payload.Key = domain.SessionEventObjectPrefix(
		item.Event.TenantID, item.Event.SessionID, item.Event.ID,
	) + "payload.json"
	store := &snapshotStore{events: []domain.SessionEvent{item.Event}}
	blobs := &snapshotBlobs{data: map[string][]byte{item.Event.Payload.Key: item.Payload}}
	builder, err := sessioncontext.NewSnapshotBuilder(store, blobs)
	if err != nil {
		t.Fatal(err)
	}
	_, err = builder.Create(ctx, sessioncontext.SnapshotRequest{
		TenantID: "tenant-a", SessionID: "session-a", Version: 1,
		ThroughSequence: 1, MaxEvents: 10, MaxBytes: 1 << 20,
	})
	if err == nil {
		t.Fatal("cross-session event was accepted")
	}
	if len(blobs.opens) != 0 {
		t.Fatalf("cross-session payload was opened before rejection: %v", blobs.opens)
	}
}

func TestSnapshotMaintainerCreatesAtPolicyBoundaryAndSkipsCoveredHistory(t *testing.T) {
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
	maintainer, err := sessioncontext.NewMaintainer(store, builder, sessioncontext.MaintenancePolicy{
		IntervalEvents: 2, MaxEvents: 10, MaxBytes: 1 << 20, MaxVersions: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := maintainer.MaybeCreate(ctx, "tenant-a", "session-a", 1); err != nil || created {
		t.Fatalf("below-boundary maintenance = created=%t err=%v", created, err)
	}
	snapshot, created, err := maintainer.MaybeCreate(ctx, "tenant-a", "session-a", 2)
	if err != nil || !created || snapshot.ThroughSequence != 2 {
		t.Fatalf("boundary maintenance = snapshot=%#v created=%t err=%v", snapshot, created, err)
	}
	if _, created, err := maintainer.MaybeCreate(ctx, "tenant-a", "session-a", 2); err != nil || created {
		t.Fatalf("covered maintenance = created=%t err=%v", created, err)
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

func (store *snapshotStore) ListSessionSnapshots(
	_ context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	afterVersion uint64,
	limit uint64,
) ([]domain.SessionSnapshot, error) {
	if store.snapshot == nil || store.snapshot.TenantID != tenantID || store.snapshot.SessionID != sessionID ||
		store.snapshot.Version <= afterVersion || limit == 0 {
		return nil, nil
	}
	return []domain.SessionSnapshot{*store.snapshot}, nil
}

type snapshotBlobs struct {
	data  map[string][]byte
	opens []string
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
	store.opens = append(store.opens, ref.Key)
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (store *snapshotBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	return nil
}
