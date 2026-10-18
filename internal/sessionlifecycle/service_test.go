package sessionlifecycle

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestExecuteRequiresExactConfirmation(t *testing.T) {
	service, store, blobs := fixture(t)
	_, err := service.Execute(context.Background(), "tenant-a", "session-a", "wrong", time.Unix(20, 0).UTC())
	if err == nil {
		t.Fatal("expected confirmation failure")
	}
	if store.starts != 0 || len(blobs.deleted) != 0 || store.completions != 0 {
		t.Fatalf("destructive work happened before confirmation: %+v %+v", store, blobs)
	}
}

func TestExecuteDeletesOnlySortedExactInventoryAndCompletes(t *testing.T) {
	service, _, blobs := fixture(t)
	plan, err := service.Plan(context.Background(), "tenant-a", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	completed, err := service.Execute(
		context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(20, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"tenants/tenant-a/sessions/session-a/a", "tenants/tenant-a/sessions/session-a/b"}
	if !reflect.DeepEqual(blobs.deleted, want) {
		t.Fatalf("deleted keys = %v, want %v", blobs.deleted, want)
	}
	if completed.State != domain.SessionDeletionCompleted || completed.DeletedObjects != 2 || completed.DeletedBytes != 7 {
		t.Fatalf("unexpected completion: %+v", completed)
	}
}

func TestExecuteResumesAfterObjectDeleteFailure(t *testing.T) {
	service, store, blobs := fixture(t)
	plan, err := service.Plan(context.Background(), "tenant-a", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	blobs.failOnce = "tenants/tenant-a/sessions/session-a/b"
	if _, err := service.Execute(context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(20, 0).UTC()); err == nil {
		t.Fatal("expected injected object deletion failure")
	}
	if store.deletion.State != domain.SessionDeletionDeleting || store.completions != 0 {
		t.Fatalf("deletion was not left resumable: %+v", store.deletion)
	}
	completed, err := service.Execute(context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(21, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != domain.SessionDeletionCompleted || store.starts != 2 || store.completions != 1 {
		t.Fatalf("unexpected resumed completion: deletion=%+v starts=%d completions=%d", completed, store.starts, store.completions)
	}
	wantDeletes := []string{
		"tenants/tenant-a/sessions/session-a/a",
		"tenants/tenant-a/sessions/session-a/a",
		"tenants/tenant-a/sessions/session-a/b",
	}
	if !reflect.DeepEqual(blobs.deleted, wantDeletes) {
		t.Fatalf("resumed exact deletes = %v, want %v", blobs.deleted, wantDeletes)
	}
}

func TestExecuteRevalidatesInventoryAfterStart(t *testing.T) {
	for _, test := range []struct {
		name  string
		start func(*fakeLifecycleStore)
	}{
		{
			name: "changed_object_identity",
			start: func(store *fakeLifecycleStore) {
				// Keep a valid inventory and identical counts/bytes. Only the
				// exact immutable object identity changes at the Start boundary.
				store.inventory.Objects = append([]domain.BlobRef(nil), store.inventory.Objects...)
				store.inventory.Objects[0].SHA256 = strings.Repeat("3", 64)
			},
		},
		{
			name: "post_start_inventory_unavailable",
			start: func(store *fakeLifecycleStore) {
				store.inventoryErr = errors.New("injected post-Start inventory read failure")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, store, blobs := fixture(t)
			plan, err := service.Plan(context.Background(), "tenant-a", "session-a")
			if err != nil {
				t.Fatalf("initial exact inventory: %v", err)
			}
			store.afterStart = test.start
			_, err = service.Execute(context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(20, 0).UTC())
			if err == nil {
				t.Error("Execute accepted a stale pre-Start inventory after the authoritative Start changed it")
			}
			if store.starts != 1 || store.deletion.State != domain.SessionDeletionDeleting {
				t.Errorf("Start boundary not reached once: starts=%d state=%s", store.starts, store.deletion.State)
			}
			if len(blobs.deleted) != 0 || store.completions != 0 {
				t.Errorf("effects after invalid post-Start inventory: deletes=%v completions=%d, want none", blobs.deleted, store.completions)
			}
		})
	}
}

func TestExecuteResumesWithFreshServiceWithoutBlobReads(t *testing.T) {
	service, store, blobs := fixture(t)
	plan, err := service.Plan(context.Background(), "tenant-a", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	blobs.failOnce = "tenants/tenant-a/sessions/session-a/b"
	if _, err := service.Execute(context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(20, 0).UTC()); err == nil {
		t.Fatal("first Execute did not expose the injected partial deletion failure")
	}
	if store.deletion.State != domain.SessionDeletionDeleting || store.completions != 0 {
		t.Fatalf("partial deletion lost restart state: state=%s completions=%d", store.deletion.State, store.completions)
	}
	// A new coordinator can use only the existing durable store inventory.
	// Blob Open and Put panic in this fixture, so neither can be used to resume.
	restarted, err := New(store, blobs, 100, 100)
	if err != nil {
		t.Fatalf("fresh Service: %v", err)
	}
	completed, err := restarted.Execute(context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(21, 0).UTC())
	if err != nil {
		t.Fatalf("resume exact original inventory: %v", err)
	}
	if completed.State != domain.SessionDeletionCompleted || store.completions != 1 || completed.DeletedObjects != 2 || completed.DeletedBytes != 7 {
		t.Fatalf("resumed completion=%+v completions=%d", completed, store.completions)
	}
	want := []string{"tenants/tenant-a/sessions/session-a/a", "tenants/tenant-a/sessions/session-a/a", "tenants/tenant-a/sessions/session-a/b"}
	if !reflect.DeepEqual(blobs.deleted, want) {
		t.Fatalf("restart exact deletes=%v, want %v", blobs.deleted, want)
	}
	before := append([]string(nil), blobs.deleted...)
	if _, err := restarted.Execute(context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(22, 0).UTC()); err != nil {
		t.Fatalf("completed tombstone replay: %v", err)
	}
	if !reflect.DeepEqual(blobs.deleted, before) || store.completions != 1 || store.starts != 2 {
		t.Fatalf("completed replay repeated effects: deletes=%v starts=%d completions=%d", blobs.deleted, store.starts, store.completions)
	}
}

func fixture(t *testing.T) (*Service, *fakeLifecycleStore, *fakeBlobStore) {
	t.Helper()
	requestedAt := time.Unix(10, 0).UTC()
	store := &fakeLifecycleStore{
		deletion: domain.SessionDeletion{
			TenantID: "tenant-a", SessionID: "session-a", RequestedBy: "user-a", Reason: "user request",
			State: domain.SessionDeletionRequested, RequestedAt: requestedAt,
		},
		inventory: domain.SessionDeletionInventory{
			TenantID: "tenant-a", SessionID: "session-a", EventRows: 2, TotalBytes: 7,
			Objects: []domain.BlobRef{
				{TenantID: "tenant-a", Key: "tenants/tenant-a/sessions/session-a/a", Size: 3, SHA256: strings.Repeat("1", 64)},
				{TenantID: "tenant-a", Key: "tenants/tenant-a/sessions/session-a/b", Size: 4, SHA256: strings.Repeat("2", 64)},
			},
		},
	}
	blobs := &fakeBlobStore{}
	service, err := New(store, blobs, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	return service, store, blobs
}

type fakeLifecycleStore struct {
	deletion     domain.SessionDeletion
	inventory    domain.SessionDeletionInventory
	starts       int
	completions  int
	afterStart   func(*fakeLifecycleStore)
	inventoryErr error
}

func (store *fakeLifecycleStore) PutSessionLegalHold(context.Context, domain.SessionLegalHold) (domain.SessionLegalHold, error) {
	panic("unused")
}
func (store *fakeLifecycleStore) ReleaseSessionLegalHold(context.Context, domain.TenantID, domain.SessionID, domain.UserID, time.Time) (domain.SessionLegalHold, error) {
	panic("unused")
}
func (store *fakeLifecycleStore) GetSessionLegalHold(context.Context, domain.TenantID, domain.SessionID) (domain.SessionLegalHold, bool, error) {
	panic("unused")
}
func (store *fakeLifecycleStore) RequestSessionDeletion(context.Context, domain.SessionDeletion) (domain.SessionDeletion, error) {
	panic("unused")
}
func (store *fakeLifecycleStore) GetSessionDeletion(context.Context, domain.TenantID, domain.SessionID) (domain.SessionDeletion, bool, error) {
	return store.deletion, true, nil
}
func (store *fakeLifecycleStore) StartSessionDeletion(_ context.Context, _ domain.TenantID, _ domain.SessionID, at time.Time) (domain.SessionDeletion, error) {
	store.starts++
	if err := store.deletion.Start(at); err != nil {
		return domain.SessionDeletion{}, err
	}
	if store.afterStart != nil {
		store.afterStart(store)
	}
	return store.deletion, nil
}
func (store *fakeLifecycleStore) BuildSessionDeletionInventory(context.Context, domain.TenantID, domain.SessionID, uint64, uint64) (domain.SessionDeletionInventory, error) {
	return store.inventory, store.inventoryErr
}
func (store *fakeLifecycleStore) CompleteSessionDeletion(_ context.Context, _ domain.TenantID, _ domain.SessionID, at time.Time, objects, bytes uint64) (domain.SessionDeletion, error) {
	store.completions++
	if err := store.deletion.Complete(at, objects, bytes); err != nil {
		return domain.SessionDeletion{}, err
	}
	return store.deletion, nil
}

type fakeBlobStore struct {
	deleted  []string
	failOnce string
}

func (*fakeBlobStore) Put(context.Context, domain.TenantID, string, io.Reader) (domain.BlobRef, error) {
	panic("unused")
}
func (*fakeBlobStore) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	panic("unused")
}
func (store *fakeBlobStore) Delete(_ context.Context, _ domain.TenantID, ref domain.BlobRef) error {
	if store.failOnce == ref.Key {
		store.failOnce = ""
		return errors.New("injected")
	}
	store.deleted = append(store.deleted, ref.Key)
	return nil
}
