package sessionlifecycle

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// These tests exercise the Service's post-Start ordering with a controlled
// application port, not genuine SQL writer verification or physical erasure.
func installRetainedServiceCapture(t *testing.T, store *fakeLifecycleStore) {
	t.Helper()
	if store.deletion.RetainedVerification != nil {
		return // A retry must preserve the original capture and verification time.
	}
	request, err := domain.SessionDeletionRequestDigestV1(store.deletion)
	if err != nil {
		t.Fatalf("request commitment: %v", err)
	}
	inventory, err := domain.SessionDeletionInventoryDigestV1(store.inventory)
	if err != nil {
		t.Fatalf("inventory commitment: %v", err)
	}
	rows, err := domain.SessionDeletionInventoryRowsV1(store.inventory)
	if err != nil || store.deletion.StartedAt == nil {
		t.Fatalf("started capture: rows=%d err=%v started=%v", rows, err, store.deletion.StartedAt)
	}
	store.deletion.RetainedVerification = &domain.RetainedDeletionVerificationV1{
		Schema: "sessionless.retained-deletion.v1", TenantID: store.deletion.TenantID,
		SessionID: store.deletion.SessionID, RequestDigest: request,
		MetadataDigest: strings.Repeat("a", 64), InventoryDigest: inventory,
		Rows: rows, Objects: uint64(len(store.inventory.Objects)), Bytes: store.inventory.TotalBytes,
		VerifiedRuns: 1, VerifiedAt: *store.deletion.StartedAt,
	}
	if err := store.deletion.RetainedVerification.ValidateInventory(store.deletion, store.inventory); err != nil {
		t.Fatalf("valid controlled port capture: %v", err)
	}
}

func TestExecuteChecksRetainedCaptureBeforeAnyDelete(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*domain.RetainedDeletionVerificationV1)
	}{
		{name: "exact"},
		{name: "request", mutate: func(v *domain.RetainedDeletionVerificationV1) { v.RequestDigest = strings.Repeat("b", 64) }},
		{name: "inventory", mutate: func(v *domain.RetainedDeletionVerificationV1) { v.InventoryDigest = strings.Repeat("b", 64) }},
		{name: "rows", mutate: func(v *domain.RetainedDeletionVerificationV1) { v.Rows++ }},
		{name: "objects", mutate: func(v *domain.RetainedDeletionVerificationV1) { v.Objects++ }},
		{name: "bytes", mutate: func(v *domain.RetainedDeletionVerificationV1) { v.Bytes++ }},
		{name: "tenant", mutate: func(v *domain.RetainedDeletionVerificationV1) { v.TenantID = "tenant-b" }},
		{name: "session", mutate: func(v *domain.RetainedDeletionVerificationV1) { v.SessionID = "session-b" }},
		{name: "schema", mutate: func(v *domain.RetainedDeletionVerificationV1) { v.Schema = "unknown" }},
		{name: "verification_time", mutate: func(v *domain.RetainedDeletionVerificationV1) { v.VerifiedAt = v.VerifiedAt.Add(time.Microsecond) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, store, blobs := fixture(t)
			plan, err := service.Plan(context.Background(), "tenant-a", "session-a")
			if err != nil {
				t.Fatalf("initial typed confirmation: %v", err)
			}
			store.afterStart = func(store *fakeLifecycleStore) {
				installRetainedServiceCapture(t, store)
				if test.mutate != nil {
					test.mutate(store.deletion.RetainedVerification)
				}
			}
			_, err = service.Execute(context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(20, 0).UTC())
			if store.starts != 1 {
				t.Fatalf("Start calls=%d, want 1 before testing post-Start capture", store.starts)
			}
			if test.mutate == nil {
				if err != nil || len(blobs.deleted) != 2 || store.completions != 1 {
					t.Fatalf("valid controlled capture rejected: err=%v deletes=%v completions=%d", err, blobs.deleted, store.completions)
				}
				return
			}
			var invalid domain.ValidationError
			if !errors.As(err, &invalid) || invalid.Field != "session_deletion.retained_verification" {
				t.Errorf("capture mutation error=%v, want exact retained-verification rejection", err)
			}
			if len(blobs.deleted) != 0 || store.completions != 0 {
				t.Errorf("effects after invalid capture: deletes=%v completions=%d", blobs.deleted, store.completions)
			}
		})
	}
}

type retainedStartACKStore struct {
	*fakeLifecycleStore
	loseACK bool
}

func (store *retainedStartACKStore) StartSessionDeletion(ctx context.Context, tenant domain.TenantID, session domain.SessionID, at time.Time) (domain.SessionDeletion, error) {
	deletion, err := store.fakeLifecycleStore.StartSessionDeletion(ctx, tenant, session, at)
	if err == nil && store.loseACK {
		store.loseACK = false
		return domain.SessionDeletion{}, errors.New("injected lost Start acknowledgement after persisted capture")
	}
	return deletion, err
}

func TestExecuteRetainedCaptureSurvivesLostStartACKAndPartialDelete(t *testing.T) {
	_, original, blobs := fixture(t)
	original.afterStart = func(store *fakeLifecycleStore) { installRetainedServiceCapture(t, store) }
	store := &retainedStartACKStore{fakeLifecycleStore: original, loseACK: true}
	newService := func() *Service {
		t.Helper()
		service, err := New(store, blobs, 100, 100)
		if err != nil {
			t.Fatalf("fresh coordinator: %v", err)
		}
		return service
	}
	service := newService()
	plan, err := service.Plan(context.Background(), "tenant-a", "session-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Execute(context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(20, 0).UTC()); err == nil {
		t.Fatal("lost Start acknowledgement was hidden")
	}
	if store.deletion.State != domain.SessionDeletionDeleting || store.deletion.RetainedVerification == nil || len(blobs.deleted) != 0 || store.completions != 0 {
		t.Fatalf("unknown Start outcome caused effects or lost original capture: state=%s capture=%v deletes=%v completions=%d", store.deletion.State, store.deletion.RetainedVerification != nil, blobs.deleted, store.completions)
	}
	captured := *store.deletion.RetainedVerification
	blobs.failOnce = store.inventory.Objects[1].Key
	if _, err := newService().Execute(context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(21, 0).UTC()); err == nil {
		t.Fatal("partial exact Delete failure was hidden")
	}
	if len(blobs.deleted) != 1 || store.completions != 0 || *store.deletion.RetainedVerification != captured {
		t.Fatalf("partial delete replaced capture or completed: deletes=%v completions=%d capture=%+v", blobs.deleted, store.completions, store.deletion.RetainedVerification)
	}
	completed, err := newService().Execute(context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(22, 0).UTC())
	if err != nil || completed.State != domain.SessionDeletionCompleted || store.completions != 1 || store.starts != 3 || *completed.RetainedVerification != captured {
		t.Fatalf("fresh Service did not resume original capture: completed=%+v err=%v starts=%d completions=%d", completed, err, store.starts, store.completions)
	}
	want := []string{store.inventory.Objects[0].Key, store.inventory.Objects[0].Key, store.inventory.Objects[1].Key}
	if !reflect.DeepEqual(blobs.deleted, want) {
		t.Fatalf("resumed exact keys=%v, want %v", blobs.deleted, want)
	}
	before := append([]string(nil), blobs.deleted...)
	if _, err := newService().Execute(context.Background(), "tenant-a", "session-a", plan.Confirmation, time.Unix(23, 0).UTC()); err != nil {
		t.Fatalf("completed replay: %v", err)
	}
	if !reflect.DeepEqual(before, blobs.deleted) || store.starts != 3 || store.completions != 1 {
		t.Fatalf("completed replay repeated effects: deletes=%v starts=%d completions=%d", blobs.deleted, store.starts, store.completions)
	}
}
