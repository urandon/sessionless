package attachedworkertransport

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type resourceGenerationStoreFixture struct {
	ports.AttachedWorkerTransportStore
	calls, reads int
	auth         ports.AttachedResourceGenerationAuthorizationV1
	publication  ports.AttachedResourceGenerationPublicationV1
	result       ports.AttachedResourceGenerationResultV1
	err          error
	deadline     time.Time
	afterCall    func()
}

func (f *resourceGenerationStoreFixture) PublishAttachedResourceGenerationV1(ctx context.Context, request ports.AttachedResourceGenerationPublicationV1) (ports.AttachedResourceGenerationResultV1, error) {
	f.calls++
	f.publication = request
	f.auth = request.Authorization
	f.deadline, _ = ctx.Deadline()
	if f.afterCall != nil {
		f.afterCall()
	}
	return f.result, f.err
}
func (f *resourceGenerationStoreFixture) ReadAttachedResourceGenerationV1(ctx context.Context, auth ports.AttachedResourceGenerationAuthorizationV1) (ports.AttachedResourceGenerationResultV1, error) {
	f.reads++
	f.auth = auth
	f.deadline, _ = ctx.Deadline()
	if f.afterCall != nil {
		f.afterCall()
	}
	return f.result, f.err
}
func resourceGenerationServiceFixture(t *testing.T) (*Service, *resourceGenerationStoreFixture, []byte, ports.AttachedResourceGenerationPublicationV1, ConnectionSecret) {
	t.Helper()
	secret, err := ParseConnectionSecret([]byte("12345678901234567890123456789012"))
	if err != nil {
		t.Fatal(err)
	}
	bearer, err := NewConnectionBearer("tenant", "owner", "worker", "connection", secret)
	if err != nil {
		t.Fatal(err)
	}
	auth := ports.AttachedResourceGenerationAuthorizationV1{TenantID: "tenant", OwnerUserID: "owner", SubscriptionConnectionID: "subscription", WorkerID: "worker", EnrollmentGeneration: 3, BaselineRevision: 7, ConnectionID: "connection", ConnectionGeneration: 9}
	head := domain.AttachedResourceGenerationHeadV1{Version: 1, OwnerUserID: auth.OwnerUserID, WorkerID: auth.WorkerID, EnrollmentGeneration: auth.EnrollmentGeneration, BaselineRevision: auth.BaselineRevision, CredentialGeneration: 1}
	store := &resourceGenerationStoreFixture{result: ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionApplied, Head: head}}
	return &Service{store: store}, store, bearer.Bytes(), ports.AttachedResourceGenerationPublicationV1{Authorization: auth, Target: head}, secret
}

func TestResourceGenerationBearerUsesOriginalScopeAndDerivedDigestOnce(t *testing.T) {
	t.Parallel()
	service, store, bearer, request, secret := resourceGenerationServiceFixture(t)
	defer clear(bearer)
	deadline := time.Now().Add(5 * time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()
	result, err := service.PublishResourceGenerationBearer(ctx, bearer, request)
	if err != nil || result.Head != request.Target || store.calls != 1 || store.reads != 0 || store.auth.PresentedSecretDigest != secret.Digest() || !store.deadline.Equal(deadline) {
		t.Fatalf("original publication binding: result=%+v err=%v calls/reads=%d/%d deadline=%s", result, err, store.calls, store.reads, store.deadline)
	}
	if request.Authorization.PresentedSecretDigest != "" {
		t.Fatal("service mutated caller auth value")
	}
	result, err = service.ReadResourceGenerationBearer(ctx, bearer, request.Authorization)
	if err != nil || result.Head != request.Target || store.reads != 1 || store.calls != 1 || store.auth.PresentedSecretDigest != secret.Digest() {
		t.Fatalf("explicit one readback: result=%+v err=%v calls/reads=%d/%d", result, err, store.calls, store.reads)
	}
}

func TestResourceGenerationBearerRejectsForgedLocatorsAndDigestBeforeStore(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*ports.AttachedResourceGenerationPublicationV1)
	}{
		{"tenant", func(r *ports.AttachedResourceGenerationPublicationV1) { r.Authorization.TenantID = "foreign" }},
		{"owner", func(r *ports.AttachedResourceGenerationPublicationV1) { r.Authorization.OwnerUserID = "foreign" }},
		{"worker", func(r *ports.AttachedResourceGenerationPublicationV1) { r.Authorization.WorkerID = "foreign" }},
		{"connection", func(r *ports.AttachedResourceGenerationPublicationV1) { r.Authorization.ConnectionID = "foreign" }},
		{"supplied digest", func(r *ports.AttachedResourceGenerationPublicationV1) {
			r.Authorization.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("forged"))
		}},
		{"missing enrollment", func(r *ports.AttachedResourceGenerationPublicationV1) { r.Authorization.EnrollmentGeneration = 0 }},
		{"target owner", func(r *ports.AttachedResourceGenerationPublicationV1) { r.Target.OwnerUserID = "foreign" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, store, bearer, request, _ := resourceGenerationServiceFixture(t)
			defer clear(bearer)
			tc.mutate(&request)
			result, err := service.PublishResourceGenerationBearer(t.Context(), bearer, request)
			if !errors.Is(err, ErrTransportUnauthorized) || !reflect.DeepEqual(result, ports.AttachedResourceGenerationResultV1{}) || store.calls != 0 || store.reads != 0 {
				t.Fatalf("forged request reached Store/returned ACK: %+v err=%v calls=%d", result, err, store.calls)
			}
		})
	}
}

func TestResourceGenerationBearerRejectsFalseACKAndSanitizesBackend(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*resourceGenerationStoreFixture)
	}{
		{"foreign owner", func(f *resourceGenerationStoreFixture) { f.result.Head.OwnerUserID = "foreign" }},
		{"wrong generation", func(f *resourceGenerationStoreFixture) { f.result.Head.CredentialGeneration++ }},
		{"unexpected revoke", func(f *resourceGenerationStoreFixture) { f.result.Head.Revoked = true }},
		{"wrong baseline", func(f *resourceGenerationStoreFixture) { f.result.Head.BaselineRevision++ }},
		{"invalid version", func(f *resourceGenerationStoreFixture) { f.result.Head.Version++ }},
		{"unknown status", func(f *resourceGenerationStoreFixture) { f.result.Status = "unexpected" }},
		{"head with denial", func(f *resourceGenerationStoreFixture) { f.result.Status = ports.AttachedWorkerExecutionDenied }},
		{"unknown commit", func(f *resourceGenerationStoreFixture) { f.err = errors.New("private-backend-marker") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, store, bearer, request, _ := resourceGenerationServiceFixture(t)
			defer clear(bearer)
			tc.mutate(store)
			result, err := service.PublishResourceGenerationBearer(t.Context(), bearer, request)
			if !errors.Is(err, ErrTransportBackend) || !reflect.DeepEqual(result, ports.AttachedResourceGenerationResultV1{}) || store.calls != 1 || store.reads != 0 || strings.Contains(err.Error(), "private-backend-marker") {
				t.Fatalf("false ACK/backend leak: %+v err=%v calls/reads=%d/%d", result, err, store.calls, store.reads)
			}
		})
	}
}

func TestResourceGenerationBearerReadAndFailureClassifications(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		status ports.AttachedWorkerExecutionStatus
		want   error
	}{{ports.AttachedWorkerExecutionDenied, ErrTransportUnauthorized}, {ports.AttachedWorkerExecutionExpired, ErrTransportUnauthorized}, {ports.AttachedWorkerExecutionFenced, ErrTransportUnauthorized}, {ports.AttachedWorkerExecutionConflict, ErrTransportConflict}} {
		service, store, bearer, request, _ := resourceGenerationServiceFixture(t)
		store.result = ports.AttachedResourceGenerationResultV1{Status: tc.status}
		result, err := service.ReadResourceGenerationBearer(t.Context(), bearer, request.Authorization)
		clear(bearer)
		if !errors.Is(err, tc.want) || !reflect.DeepEqual(result, ports.AttachedResourceGenerationResultV1{}) || store.reads != 1 {
			t.Errorf("read failure classification status=%s err=%v result=%+v", tc.status, err, result)
		}
	}
	service, store, bearer, request, _ := resourceGenerationServiceFixture(t)
	defer clear(bearer)
	store.result = ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionNotFound}
	if result, err := service.ReadResourceGenerationBearer(t.Context(), bearer, request.Authorization); err != nil || result.Status != ports.AttachedWorkerExecutionNotFound {
		t.Fatalf("missing head invented error/ACK: %+v %v", result, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	store.afterCall = cancel
	if result, err := service.ReadResourceGenerationBearer(ctx, bearer, request.Authorization); !errors.Is(err, ErrTransportBackend) || !reflect.DeepEqual(result, ports.AttachedResourceGenerationResultV1{}) {
		t.Fatalf("late backend result escaped original deadline: %+v %v", result, err)
	}
}

func TestResourceGenerationBearerDoesNotRetryFailedPublicationAndBoundsCall(t *testing.T) {
	t.Parallel()
	service, store, bearer, request, _ := resourceGenerationServiceFixture(t)
	defer clear(bearer)
	store.err = errors.New("unknown-private-commit-marker")
	result, err := service.PublishResourceGenerationBearer(t.Context(), bearer, request)
	if !errors.Is(err, ErrTransportBackend) || !reflect.DeepEqual(result, ports.AttachedResourceGenerationResultV1{}) || store.calls != 1 || store.reads != 0 || store.deadline.IsZero() || store.deadline.After(time.Now().Add(ports.AttachedResourceGenerationOperationTimeoutV1)) {
		t.Fatalf("failed publication retried/read/renewed caller budget: %+v err=%v calls/reads=%d/%d", result, err, store.calls, store.reads)
	}
	store.err = nil
	store.result.Status = ports.AttachedWorkerExecutionReplayed
	if result, err := service.ReadResourceGenerationBearer(t.Context(), bearer, request.Authorization); !errors.Is(err, ErrTransportBackend) || !reflect.DeepEqual(result, ports.AttachedResourceGenerationResultV1{}) {
		t.Fatalf("read accepted mutation-replayed authority: %+v %v", result, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	beforeCalls := store.calls
	if _, err := service.PublishResourceGenerationBearer(ctx, bearer, request); !errors.Is(err, ErrTransportUnauthorized) || store.calls != beforeCalls {
		t.Fatalf("cancelled call reached backend: %v", err)
	}
	if _, err := service.PublishResourceGenerationBearer(t.Context(), bytes.Repeat([]byte("a"), ConnectionBearerMaxBytes+1), request); !errors.Is(err, ErrTransportUnauthorized) || store.calls != beforeCalls {
		t.Fatalf("oversize bearer reached backend: %v", err)
	}
}
