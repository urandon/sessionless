package sessionapi_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
)

var apiTestTime = time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)

func TestScopedCursorPaginatesAndRejectsReplay(t *testing.T) {
	store := &memoryStore{records: []ports.SessionRecord{
		sessionRecord("session-3", "tenant-a", "user-a", apiTestTime.Add(3*time.Minute)),
		sessionRecord("session-2", "tenant-a", "user-a", apiTestTime.Add(2*time.Minute)),
		sessionRecord("session-1", "tenant-a", "user-a", apiTestTime.Add(time.Minute)),
	}}
	service := newService(t, store, &memoryBlobs{})
	first, err := service.List(context.Background(), "tenant-a", "user-a", domain.SessionActive, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 2 || first.NextCursor == "" || first.Items[0].Session.ID != "session-3" {
		t.Fatalf("first page = %+v", first)
	}
	second, err := service.List(context.Background(), "tenant-a", "user-a", domain.SessionActive, first.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 1 || second.Items[0].Session.ID != "session-1" || second.NextCursor != "" {
		t.Fatalf("second page = %+v", second)
	}
	for _, scope := range []struct {
		tenant domain.TenantID
		user   domain.UserID
		status domain.SessionStatus
	}{
		{"tenant-b", "user-a", domain.SessionActive},
		{"tenant-a", "user-b", domain.SessionActive},
		{"tenant-a", "user-a", domain.SessionArchived},
	} {
		if _, err := service.List(context.Background(), scope.tenant, scope.user, scope.status, first.NextCursor, 2); err == nil {
			t.Fatalf("cursor replay accepted for %+v", scope)
		}
	}
	tampered := first.NextCursor[:len(first.NextCursor)-1] + "A"
	if _, err := service.List(context.Background(), "tenant-a", "user-a", domain.SessionActive, tampered, 2); err == nil {
		t.Fatal("tampered cursor accepted")
	}
}

func TestCreateUsesStableUserScopedIdempotency(t *testing.T) {
	store := &memoryStore{}
	service := newService(t, store, &memoryBlobs{})
	first, created, err := service.Create(context.Background(), "tenant-a", "user-a", "create-1")
	if err != nil || !created {
		t.Fatalf("first create = %+v, %v, %v", first, created, err)
	}
	second, created, err := service.Create(context.Background(), "tenant-a", "user-a", "create-1")
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("retry create = %+v, %v, %v", second, created, err)
	}
	third, _, err := service.Create(context.Background(), "tenant-a", "user-b", "create-1")
	if err != nil || third.ID == first.ID {
		t.Fatalf("user scope did not change stable ID: first=%q third=%q err=%v", first.ID, third.ID, err)
	}
}

func TestHistoryAuthorizesBeforeOpeningAndVerifiesPayload(t *testing.T) {
	payload := []byte(`{"version":1,"text":"hello"}`)
	digest := sha256.Sum256(payload)
	event := domain.SessionEvent{
		ID: "event-1", TenantID: "tenant-a", SessionID: "session-1", Sequence: 1,
		Kind: domain.SessionEventUserMessage, AuthorUserID: userPtr("user-a"), RunID: runPtr("run-1"),
		IdempotencyKey: "event-key-1", CreatedAt: apiTestTime,
		Payload: domain.BlobRef{
			TenantID: "tenant-a", Key: domain.SessionEventObjectPrefix("tenant-a", "session-1", "event-1") + "payload.json",
			Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
		},
	}
	store := &memoryStore{events: []domain.SessionEvent{event}, denyHistory: true}
	blobs := &memoryBlobs{values: map[string][]byte{event.Payload.Key: payload}}
	service := newService(t, store, blobs)
	if _, err := service.History(context.Background(), "tenant-a", "user-a", "session-1", "", 10); !errors.Is(err, sessionapi.ErrSessionUnavailable) {
		t.Fatalf("denied history error = %v", err)
	}
	if blobs.opens != 0 {
		t.Fatalf("payload opened before authorization: %d", blobs.opens)
	}
	store.denyHistory = false
	page, err := service.History(context.Background(), "tenant-a", "user-a", "session-1", "", 10)
	if err != nil || len(page.Items) != 1 || !bytes.Equal(page.Items[0].Payload, payload) {
		t.Fatalf("authorized history = %+v, %v", page, err)
	}
	blobs.values[event.Payload.Key] = []byte(`{"version":1,"text":"altered"}`)
	if _, err := service.History(context.Background(), "tenant-a", "user-a", "session-1", "", 10); err == nil {
		t.Fatal("digest-mismatched payload accepted")
	}
}

func TestEventAndRunCursorsAreScopedAndOpaque(t *testing.T) {
	payload := []byte(`{"version":1,"text":"hello"}`)
	digest := sha256.Sum256(payload)
	events := make([]domain.SessionEvent, 3)
	for index := range events {
		eventID := domain.SessionEventID("event-" + string(rune('1'+index)))
		events[index] = domain.SessionEvent{
			ID: eventID, TenantID: "tenant-a", SessionID: "session-1", Sequence: uint64(index + 1),
			Kind: domain.SessionEventUserMessage, AuthorUserID: userPtr("user-a"),
			IdempotencyKey: domain.IdempotencyKey("event-key-" + string(rune('1'+index))),
			CreatedAt:      apiTestTime.Add(time.Duration(index) * time.Second),
			Payload: domain.BlobRef{
				TenantID: "tenant-a", Key: domain.SessionEventObjectPrefix("tenant-a", "session-1", eventID) + "payload.json",
				Size: int64(len(payload)), SHA256: hex.EncodeToString(digest[:]),
			},
		}
	}
	runs := make([]ports.RunRecord, 3)
	for index := range runs {
		runs[index] = ports.RunRecord{Run: runFixture(index), Provider: "provider-a"}
	}
	values := make(map[string][]byte)
	for _, event := range events {
		values[event.Payload.Key] = payload
	}
	store := &memoryStore{events: events, runs: runs}
	service := newService(t, store, &memoryBlobs{values: values})

	eventPage, err := service.History(context.Background(), "tenant-a", "user-a", "session-1", "", 2)
	if err != nil || len(eventPage.Items) != 2 || eventPage.NextCursor == "" || eventPage.NextCursor == "2" {
		t.Fatalf("event page = %+v err=%v", eventPage, err)
	}
	nextEvents, err := service.History(context.Background(), "tenant-a", "user-a", "session-1", eventPage.NextCursor, 2)
	if err != nil || len(nextEvents.Items) != 1 || nextEvents.Items[0].Event.Sequence != 3 {
		t.Fatalf("next event page = %+v err=%v", nextEvents, err)
	}
	if _, err := service.History(context.Background(), "tenant-a", "user-a", "session-2", eventPage.NextCursor, 2); err == nil {
		t.Fatal("event cursor replayed for another session")
	}
	afterEvents, err := service.HistoryAfter(context.Background(), "tenant-a", "user-a", "session-1", 1, 2)
	if err != nil || len(afterEvents.Items) != 2 || afterEvents.Items[0].Event.Sequence != 2 ||
		afterEvents.Items[1].Event.Sequence != 3 || afterEvents.NextCursor != "" {
		t.Fatalf("explicit sequence page = %+v err=%v", afterEvents, err)
	}
	if _, err := service.HistoryAfter(context.Background(), "tenant-a", "user-a", "session-1", 0, 101); err == nil {
		t.Fatal("explicit sequence read accepted an unbounded page")
	}

	runPage, err := service.Runs(context.Background(), "tenant-a", "user-a", "session-1", "", 2)
	if err != nil || len(runPage.Items) != 2 || runPage.NextCursor == "" {
		t.Fatalf("run page = %+v err=%v", runPage, err)
	}
	nextRuns, err := service.Runs(context.Background(), "tenant-a", "user-a", "session-1", runPage.NextCursor, 2)
	if err != nil || len(nextRuns.Items) != 1 || nextRuns.Items[0].Run.ID != "run-1" {
		t.Fatalf("next run page = %+v err=%v", nextRuns, err)
	}
	if _, err := service.Runs(context.Background(), "tenant-a", "user-b", "session-1", runPage.NextCursor, 2); err == nil {
		t.Fatal("run cursor replayed for another user")
	}
}

func TestBindFrontendUsesStableIdentityAndMapsAuthorization(t *testing.T) {
	store := &memoryStore{}
	service := newService(t, store, &memoryBlobs{})
	first, err := service.BindFrontend(context.Background(), "tenant-a", "user-a", "web", "browser-1", "session-1", 0)
	if err != nil || first.Revision != 1 {
		t.Fatalf("first binding = %+v err=%v", first, err)
	}
	second, err := service.BindFrontend(context.Background(), "tenant-a", "user-a", "web", "browser-1", "session-2", 1)
	if err != nil || second.ID != first.ID || second.Revision != 2 || second.SessionID != "session-2" {
		t.Fatalf("switched binding = %+v err=%v", second, err)
	}
	store.denyBinding = true
	if _, err := service.BindFrontend(context.Background(), "tenant-a", "user-a", "web", "browser-1", "session-2", 2); !errors.Is(err, sessionapi.ErrSessionUnavailable) {
		t.Fatalf("denied binding error = %v", err)
	}
}

func newService(t *testing.T, store ports.SessionAPIStore, blobs ports.BlobStore) *sessionapi.Service {
	t.Helper()
	service, err := sessionapi.New(sessionapi.Config{
		CursorKey: bytes.Repeat([]byte("c"), 32), IDKey: bytes.Repeat([]byte("i"), 32),
	}, store, blobs, fixedClock{})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func sessionRecord(id domain.SessionID, tenant domain.TenantID, user domain.UserID, at time.Time) ports.SessionRecord {
	return ports.SessionRecord{Session: domain.Session{
		ID: id, TenantID: tenant, CreatedBy: user, Status: domain.SessionActive,
		CreatedAt: apiTestTime, UpdatedAt: at,
	}, Display: domain.SessionDisplay{TenantID: tenant, SessionID: id, UpdatedAt: at}}
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return apiTestTime }

type memoryStore struct {
	records     []ports.SessionRecord
	events      []domain.SessionEvent
	created     map[string]domain.Session
	denyHistory bool
	runs        []ports.RunRecord
	bindings    map[domain.FrontendBindingID]domain.FrontendBinding
	denyBinding bool
}

func (store *memoryStore) CreateSessionForUser(_ context.Context, request ports.SessionCreateRequest) (domain.Session, bool, error) {
	if store.created == nil {
		store.created = make(map[string]domain.Session)
	}
	key := string(request.Session.TenantID) + "/" + string(request.Owner.UserID) + "/" + string(request.IdempotencyKey)
	if existing, found := store.created[key]; found {
		return existing, false, nil
	}
	store.created[key] = request.Session
	return request.Session, true, nil
}

func (store *memoryStore) GetSessionForUser(_ context.Context, tenant domain.TenantID, user domain.UserID, id domain.SessionID, _ bool) (ports.SessionRecord, bool, error) {
	for _, record := range store.records {
		if record.Session.TenantID == tenant && record.Session.CreatedBy == user && record.Session.ID == id {
			return record, true, nil
		}
	}
	return ports.SessionRecord{}, false, nil
}

func (store *memoryStore) ListSessionsForUser(_ context.Context, request ports.SessionListRequest) ([]ports.SessionRecord, error) {
	var records []ports.SessionRecord
	for _, record := range store.records {
		if record.Session.TenantID != request.TenantID || record.Session.CreatedBy != request.UserID || record.Session.Status != request.Status {
			continue
		}
		if request.Before != nil && !(record.Session.UpdatedAt.Before(request.Before.UpdatedAt) ||
			(record.Session.UpdatedAt.Equal(request.Before.UpdatedAt) && record.Session.ID < request.Before.SessionID)) {
			continue
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Session.UpdatedAt.Equal(records[j].Session.UpdatedAt) {
			return records[i].Session.ID > records[j].Session.ID
		}
		return records[i].Session.UpdatedAt.After(records[j].Session.UpdatedAt)
	})
	if uint64(len(records)) > request.Limit {
		records = records[:request.Limit]
	}
	return records, nil
}

func (store *memoryStore) ListSessionHistoryForUser(_ context.Context, _ domain.TenantID, _ domain.UserID, _ domain.SessionID, after uint64, limit uint64) ([]domain.SessionEvent, error) {
	if store.denyHistory {
		return nil, domain.ErrMembershipDenied
	}
	var result []domain.SessionEvent
	for _, event := range store.events {
		if event.Sequence > after {
			result = append(result, event)
		}
	}
	if uint64(len(result)) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (store *memoryStore) ListRunsForUser(_ context.Context, request ports.RunListRequest) ([]ports.RunRecord, error) {
	var result []ports.RunRecord
	for _, record := range store.runs {
		if request.Before != nil && !(record.Run.CreatedAt.Before(request.Before.CreatedAt) ||
			(record.Run.CreatedAt.Equal(request.Before.CreatedAt) && record.Run.ID < request.Before.RunID)) {
			continue
		}
		result = append(result, record)
	}
	if uint64(len(result)) > request.Limit {
		result = result[:request.Limit]
	}
	return result, nil
}

func (store *memoryStore) BindOrSwitchFrontendForUser(_ context.Context, request ports.FrontendBindingRequest) (domain.FrontendBinding, error) {
	if store.denyBinding {
		return domain.FrontendBinding{}, domain.ErrMembershipDenied
	}
	if store.bindings == nil {
		store.bindings = make(map[domain.FrontendBindingID]domain.FrontendBinding)
	}
	binding, found := store.bindings[request.BindingID]
	if !found {
		binding = domain.FrontendBinding{
			ID: request.BindingID, TenantID: request.TenantID, Frontend: request.Frontend,
			ExternalConversationID: request.ExternalConversationID, SessionID: request.SessionID,
			Revision: 1, CreatedAt: request.At, UpdatedAt: request.At,
		}
	} else {
		binding.SessionID, binding.Revision, binding.UpdatedAt = request.SessionID, binding.Revision+1, request.At
	}
	store.bindings[request.BindingID] = binding
	return binding, nil
}

func (store *memoryStore) SetSessionArchivedForUser(context.Context, domain.TenantID, domain.UserID, domain.SessionID, bool, domain.IdempotencyKey, time.Time) (domain.Session, error) {
	return domain.Session{}, nil
}

type memoryBlobs struct {
	values map[string][]byte
	opens  int
}

func (blobs *memoryBlobs) Put(context.Context, domain.TenantID, string, io.Reader) (domain.BlobRef, error) {
	return domain.BlobRef{}, errors.New("not implemented")
}

func (blobs *memoryBlobs) Open(_ context.Context, _ domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	blobs.opens++
	return io.NopCloser(bytes.NewReader(blobs.values[ref.Key])), nil
}

func (blobs *memoryBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error { return nil }

func userPtr(value domain.UserID) *domain.UserID { return &value }
func runPtr(value domain.RunID) *domain.RunID    { return &value }

func runFixture(index int) domain.Run {
	position := 3 - index
	at := apiTestTime.Add(time.Duration(position) * time.Minute)
	return domain.Run{
		ID: domain.RunID("run-" + string(rune('0'+position))), TenantID: "tenant-a", SessionID: "session-1",
		TriggerEventID: "event-1", SubscriptionConnectionID: "connection-1",
		Status: domain.RunSucceeded, IdempotencyKey: domain.IdempotencyKey("run-key-" + string(rune('0'+position))),
		CreatedAt: at, UpdatedAt: at, FinishedAt: &at,
	}
}
