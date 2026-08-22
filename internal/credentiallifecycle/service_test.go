package credentiallifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

var credentialTestTime = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

type fakeBindingStore struct {
	mu         sync.Mutex
	bindings   map[string]domain.CredentialBinding
	cleanup    []domain.CredentialSecretRef
	casEntered chan struct{}
	casRelease chan struct{}
}

func newFakeBindingStore(binding domain.CredentialBinding) *fakeBindingStore {
	return &fakeBindingStore{bindings: map[string]domain.CredentialBinding{bindingKey(binding.TenantID, binding.SubscriptionConnectionID): binding}}
}

func (store *fakeBindingStore) LoadCredentialBinding(_ context.Context, tenant domain.TenantID, connection domain.SubscriptionConnectionID) (domain.CredentialBinding, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	binding, found := store.bindings[bindingKey(tenant, connection)]
	return binding, found, nil
}

func (store *fakeBindingStore) CompareAndSwapCredentialBinding(_ context.Context, expected uint64, next domain.CredentialBinding) (bool, error) {
	if store.casEntered != nil {
		close(store.casEntered)
		<-store.casRelease
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	key := bindingKey(next.TenantID, next.SubscriptionConnectionID)
	current, found := store.bindings[key]
	if !found || current.Revoked || current.Generation != expected {
		return false, nil
	}
	if next.Validate() != nil || next.OwnerUserID != current.OwnerUserID || next.Generation != expected+1 {
		return false, errors.New("invalid binding transition")
	}
	store.bindings[key] = next
	store.cleanup = append(store.cleanup, current.SecretRef)
	return true, nil
}

func (store *fakeBindingStore) RevokeCredentialBinding(_ context.Context, request ports.CredentialRevokeRequest, at time.Time) (ports.CredentialRevocationResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	key := bindingKey(request.TenantID, request.SubscriptionConnectionID)
	current, found := store.bindings[key]
	if !found || current.OwnerUserID != request.OwnerUserID {
		return ports.CredentialRevocationResult{}, errors.New("binding unavailable")
	}
	if current.Revoked {
		return ports.CredentialRevocationResult{Binding: current}, nil
	}
	result := ports.CredentialRevocationResult{
		SupersededSecretRef: current.SecretRef, SupersededGeneration: current.Generation,
	}
	current.Revoked = true
	current.Entitlement = domain.EntitlementDisconnected
	current.SecretRef = domain.CredentialSecretRef{}
	current.SecretFingerprint = ""
	current.Generation++
	current.UpdatedAt = at
	if err := current.Validate(); err != nil {
		return ports.CredentialRevocationResult{}, err
	}
	store.bindings[key] = current
	store.cleanup = append(store.cleanup, result.SupersededSecretRef)
	result.Binding = current
	return result, nil
}

func (store *fakeBindingStore) current(binding domain.CredentialBinding) domain.CredentialBinding {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.bindings[bindingKey(binding.TenantID, binding.SubscriptionConnectionID)]
}

type secretRecord struct {
	scope ports.CredentialCandidateScope
	value []byte
}

type fakeSecretStore struct {
	mu         sync.Mutex
	next       uint64
	committed  map[domain.CredentialSecretRef]secretRecord
	candidates map[domain.CredentialSecretRef]secretRecord
	putError   error
	readError  error
}

func newFakeSecretStore(binding domain.CredentialBinding, value []byte) *fakeSecretStore {
	return &fakeSecretStore{
		committed:  map[domain.CredentialSecretRef]secretRecord{binding.SecretRef: {scope: candidateScope(binding), value: append([]byte(nil), value...)}},
		candidates: make(map[domain.CredentialSecretRef]secretRecord),
	}
}

func (store *fakeSecretStore) ReadCredentialSecret(_ context.Context, binding domain.CredentialBinding, max int64) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.readError != nil {
		return nil, store.readError
	}
	record, found := store.committed[binding.SecretRef]
	if !found || record.scope.TenantID != binding.TenantID || record.scope.SubscriptionConnectionID != binding.SubscriptionConnectionID || record.scope.OwnerUserID != binding.OwnerUserID || int64(len(record.value)) > max {
		return nil, errors.New("secret unavailable")
	}
	return append([]byte(nil), record.value...), nil
}

func (store *fakeSecretStore) PutCredentialCandidate(_ context.Context, scope ports.CredentialCandidateScope, value []byte) (ports.CredentialSecretCandidate, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.putError != nil {
		return ports.CredentialSecretCandidate{}, store.putError
	}
	store.next++
	ref, err := domain.NewCredentialSecretRef("candidate-secret")
	if store.next > 1 {
		ref, err = domain.NewCredentialSecretRef("candidate-secret-next")
	}
	if err != nil {
		return ports.CredentialSecretCandidate{}, err
	}
	candidate := ports.CredentialSecretCandidate{Scope: scope, Reference: ref, Fingerprint: domain.FingerprintCredential(value)}
	store.candidates[ref] = secretRecord{scope: scope, value: append([]byte(nil), value...)}
	return candidate, nil
}

func (store *fakeSecretStore) CommitCredentialCandidate(_ context.Context, candidate ports.CredentialSecretCandidate) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	record, found := store.candidates[candidate.Reference]
	if !found || record.scope != candidate.Scope {
		return errors.New("candidate unavailable")
	}
	store.committed[candidate.Reference] = record
	delete(store.candidates, candidate.Reference)
	return nil
}

func (store *fakeSecretStore) DeleteCredentialCandidate(_ context.Context, candidate ports.CredentialSecretCandidate) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.candidates, candidate.Reference)
	return nil
}

func (store *fakeSecretStore) DeleteCredentialSecret(_ context.Context, scope ports.CredentialCandidateScope, ref domain.CredentialSecretRef) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	record, found := store.committed[ref]
	if found && record.scope.TenantID == scope.TenantID && record.scope.SubscriptionConnectionID == scope.SubscriptionConnectionID && record.scope.OwnerUserID == scope.OwnerUserID {
		delete(store.committed, ref)
	}
	return nil
}

func (store *fakeSecretStore) ListUncommittedCredentialCandidates(_ context.Context, scope ports.CredentialCandidateScope, limit uint64) ([]ports.CredentialSecretCandidate, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]ports.CredentialSecretCandidate, 0, limit)
	for ref, record := range store.candidates {
		if record.scope == scope && uint64(len(result)) < limit {
			result = append(result, ports.CredentialSecretCandidate{Scope: scope, Reference: ref, Fingerprint: domain.FingerprintCredential(record.value)})
		}
	}
	return result, nil
}

func (store *fakeSecretStore) RecoverCredentialCandidate(_ context.Context, binding domain.CredentialBinding) (bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, found := store.committed[binding.SecretRef]; found {
		return false, nil
	}
	record, found := store.candidates[binding.SecretRef]
	if !found || binding.Generation < 2 || record.scope != (ports.CredentialCandidateScope{
		TenantID: binding.TenantID, SubscriptionConnectionID: binding.SubscriptionConnectionID,
		OwnerUserID: binding.OwnerUserID, ExpectedGeneration: binding.Generation - 1,
	}) || domain.FingerprintCredential(record.value) != binding.SecretFingerprint {
		return false, errors.New("binding secret unavailable")
	}
	store.committed[binding.SecretRef] = record
	delete(store.candidates, binding.SecretRef)
	return true, nil
}

func (store *fakeSecretStore) candidateCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.candidates)
}

type failureOnce struct {
	point FailurePoint
	mu    sync.Mutex
	used  bool
}

func (failure *failureOnce) Check(point FailurePoint) error {
	failure.mu.Lock()
	defer failure.mu.Unlock()
	if point == failure.point && !failure.used {
		failure.used = true
		return errors.New("injected confidential failure")
	}
	return nil
}

type credentialFixture struct {
	service *Service
	clock   *testkit.FakeClock
	state   *fakeBindingStore
	secrets *fakeSecretStore
	binding domain.CredentialBinding
	request ports.CredentialIssueRequest
	value   []byte
}

func newCredentialFixture(t *testing.T, failures FailureInjector) credentialFixture {
	t.Helper()
	value := []byte(`{"access_token":"original-secret"}`)
	ref, err := domain.NewCredentialSecretRef("vault-secret-original")
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.CredentialBinding{
		Version: domain.CredentialBindingVersionV1, TenantID: "tenant-a",
		SubscriptionConnectionID: "connection-a", OwnerUserID: "user-a",
		Provider: "provider-a", AuthMode: "subscription", SecretRef: ref,
		SecretFingerprint: domain.FingerprintCredential(value), Entitlement: domain.EntitlementActive,
		Generation: 1, UpdatedAt: credentialTestTime.Add(-time.Minute),
	}
	state := newFakeBindingStore(binding)
	secrets := newFakeSecretStore(binding, value)
	clock := testkit.NewFakeClock(credentialTestTime)
	service, err := New(Config{
		ScratchRoot: t.TempDir(), MaxAuthBytes: 128, Clock: clock,
		IDs: testkit.NewSequenceIDGenerator("test-"), Failures: failures,
	}, state, secrets)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	started := credentialTestTime.Add(-time.Minute)
	request := ports.CredentialIssueRequest{
		OwnerUserID: "user-a",
		Run: domain.Run{
			ID: "run-a", TenantID: "tenant-a", SessionID: "session-a", TriggerEventID: "event-a",
			SubscriptionConnectionID: "connection-a", Status: domain.RunRunning,
			IdempotencyKey: "run-key-a", StartedAt: &started,
			CreatedAt: started, UpdatedAt: credentialTestTime,
		},
		Attempt: domain.Attempt{
			ID: "attempt-a", TenantID: "tenant-a", RunID: "run-a", Number: 1,
			Status: domain.AttemptRunning, WorkerID: "worker-a",
			CreatedAt: started, UpdatedAt: credentialTestTime,
		},
		Lease: domain.Lease{
			ID: "lease-a", TenantID: "tenant-a", RunID: "run-a", AttemptID: "attempt-a",
			WorkerID: "worker-a", FenceToken: 9, AcquiredAt: started,
			ExpiresAt: credentialTestTime.Add(10 * time.Minute),
		},
		ExpiresAt: credentialTestTime.Add(5 * time.Minute),
	}
	return credentialFixture{service: service, clock: clock, state: state, secrets: secrets, binding: binding, request: request, value: value}
}

func TestIssueMaterializeReleaseAndExactScope(t *testing.T) {
	fixture := newCredentialFixture(t, nil)
	wrongOwner := fixture.request
	wrongOwner.OwnerUserID = "user-b"
	if _, err := fixture.service.Issue(context.Background(), wrongOwner); !errors.Is(err, ErrCredentialDenied) {
		t.Fatalf("Issue() for wrong owner error = %v, want denied", err)
	}
	notRunning := fixture.request
	notRunning.Run.Status = domain.RunQueued
	notRunning.Run.StartedAt = nil
	if _, err := fixture.service.Issue(context.Background(), notRunning); err == nil {
		t.Fatal("Issue() accepted a non-running run")
	}
	mismatchedLease := fixture.request
	mismatchedLease.Lease.FenceToken = 0
	if _, err := fixture.service.Issue(context.Background(), mismatchedLease); err == nil {
		t.Fatal("Issue() accepted an invalid lease fence")
	}
	handle, err := fixture.service.Issue(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	materialization, err := fixture.service.Materialize(context.Background(), handle)
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	assertMode(t, materialization.RootDir, 0o700)
	assertMode(t, materialization.AuthFile, 0o600)
	content, err := os.ReadFile(materialization.AuthFile)
	if err != nil || string(content) != string(fixture.value) {
		t.Fatalf("materialized content = %q, error = %v", content, err)
	}
	if _, err := fixture.service.Materialize(context.Background(), handle); !errors.Is(err, ErrCredentialConsumed) {
		t.Fatalf("second Materialize() error = %v, want consumed", err)
	}

	mutations := []func(*ports.CredentialHandle){
		func(value *ports.CredentialHandle) { value.TenantID = "tenant-b" },
		func(value *ports.CredentialHandle) { value.OwnerUserID = "user-b" },
		func(value *ports.CredentialHandle) { value.RunID = "run-b" },
		func(value *ports.CredentialHandle) { value.AttemptID = "attempt-b" },
		func(value *ports.CredentialHandle) { value.WorkerID = "worker-b" },
		func(value *ports.CredentialHandle) { value.LeaseID = "lease-b" },
		func(value *ports.CredentialHandle) { value.LeaseFence++ },
		func(value *ports.CredentialHandle) { value.BindingGeneration++ },
	}
	for _, mutate := range mutations {
		wrong := handle
		mutate(&wrong)
		if err := fixture.service.Release(context.Background(), wrong); !errors.Is(err, ErrCredentialDenied) {
			t.Fatalf("wrong-scope Release() error = %v, want denied", err)
		}
	}
	if err := fixture.service.Release(context.Background(), handle); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := fixture.service.Release(context.Background(), handle); err != nil {
		t.Fatalf("idempotent Release() error = %v", err)
	}
	if _, err := os.Lstat(materialization.RootDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("released materialization still exists: %v", err)
	}
}

func TestTenantSecretsRemainIsolatedWithinOneService(t *testing.T) {
	fixture := newCredentialFixture(t, nil)
	secondValue := []byte(`{"access_token":"tenant-b-secret"}`)
	secondRef, err := domain.NewCredentialSecretRef("vault-secret-tenant-b")
	if err != nil {
		t.Fatal(err)
	}
	secondBinding := fixture.binding
	secondBinding.TenantID = "tenant-b"
	secondBinding.SubscriptionConnectionID = "connection-b"
	secondBinding.OwnerUserID = "user-b"
	secondBinding.SecretRef = secondRef
	secondBinding.SecretFingerprint = domain.FingerprintCredential(secondValue)
	fixture.state.mu.Lock()
	fixture.state.bindings[bindingKey(secondBinding.TenantID, secondBinding.SubscriptionConnectionID)] = secondBinding
	fixture.state.mu.Unlock()
	fixture.secrets.mu.Lock()
	fixture.secrets.committed[secondRef] = secretRecord{scope: candidateScope(secondBinding), value: append([]byte(nil), secondValue...)}
	fixture.secrets.mu.Unlock()

	secondRequest := fixture.request
	secondRequest.OwnerUserID = "user-b"
	secondRequest.Run.ID = "run-b"
	secondRequest.Run.TenantID = "tenant-b"
	secondRequest.Run.SessionID = "session-b"
	secondRequest.Run.TriggerEventID = "event-b"
	secondRequest.Run.SubscriptionConnectionID = "connection-b"
	secondRequest.Run.IdempotencyKey = "run-key-b"
	secondRequest.Attempt.ID = "attempt-b"
	secondRequest.Attempt.TenantID = "tenant-b"
	secondRequest.Attempt.RunID = "run-b"
	secondRequest.Lease.ID = "lease-b"
	secondRequest.Lease.TenantID = "tenant-b"
	secondRequest.Lease.RunID = "run-b"
	secondRequest.Lease.AttemptID = "attempt-b"

	firstHandle, _ := fixture.service.Issue(context.Background(), fixture.request)
	secondHandle, err := fixture.service.Issue(context.Background(), secondRequest)
	if err != nil {
		t.Fatalf("Issue(tenant-b) error = %v", err)
	}
	firstMaterialization, _ := fixture.service.Materialize(context.Background(), firstHandle)
	secondMaterialization, err := fixture.service.Materialize(context.Background(), secondHandle)
	if err != nil {
		t.Fatalf("Materialize(tenant-b) error = %v", err)
	}
	firstContent, _ := os.ReadFile(firstMaterialization.AuthFile)
	secondContent, _ := os.ReadFile(secondMaterialization.AuthFile)
	if string(firstContent) != string(fixture.value) || string(secondContent) != string(secondValue) || firstMaterialization.RootDir == secondMaterialization.RootDir {
		t.Fatalf("tenant materializations crossed: first=%q second=%q", firstContent, secondContent)
	}
}

func TestWriteBackCASAndCrashRecovery(t *testing.T) {
	t.Run("unchanged", func(t *testing.T) {
		fixture := newCredentialFixture(t, nil)
		handle, _ := fixture.service.Issue(context.Background(), fixture.request)
		materialization, _ := fixture.service.Materialize(context.Background(), handle)
		result, err := fixture.service.WriteBack(context.Background(), handle, materialization)
		if err != nil || result.Changed || result.Generation != 1 || fixture.secrets.candidateCount() != 0 {
			t.Fatalf("unchanged WriteBack() = %+v, %v, candidates=%d", result, err, fixture.secrets.candidateCount())
		}
	})

	t.Run("changed", func(t *testing.T) {
		fixture := newCredentialFixture(t, nil)
		handle, _ := fixture.service.Issue(context.Background(), fixture.request)
		materialization, _ := fixture.service.Materialize(context.Background(), handle)
		changed := []byte(`{"access_token":"rotated-secret"}`)
		if err := os.WriteFile(materialization.AuthFile, changed, 0o600); err != nil {
			t.Fatal(err)
		}
		result, err := fixture.service.WriteBack(context.Background(), handle, materialization)
		current := fixture.state.current(fixture.binding)
		if err != nil || !result.Changed || result.Generation != 2 || current.Generation != 2 || current.SecretFingerprint != domain.FingerprintCredential(changed) || fixture.secrets.candidateCount() != 0 {
			t.Fatalf("changed WriteBack() = %+v, %v, binding=%+v candidates=%d", result, err, current, fixture.secrets.candidateCount())
		}
	})

	t.Run("after candidate put", func(t *testing.T) {
		fixture := newCredentialFixture(t, &failureOnce{point: FailureAfterCandidatePut})
		handle, _ := fixture.service.Issue(context.Background(), fixture.request)
		materialization, _ := fixture.service.Materialize(context.Background(), handle)
		_ = os.WriteFile(materialization.AuthFile, []byte(`{"access_token":"candidate"}`), 0o600)
		if _, err := fixture.service.WriteBack(context.Background(), handle, materialization); !errors.Is(err, ErrCredentialInterrupted) {
			t.Fatalf("WriteBack() error = %v, want interrupted", err)
		}
		if fixture.state.current(fixture.binding).Generation != 1 || fixture.secrets.candidateCount() != 1 {
			t.Fatal("candidate-put interruption was not durably enumerable before CAS")
		}
	})

	t.Run("after binding CAS", func(t *testing.T) {
		fixture := newCredentialFixture(t, &failureOnce{point: FailureAfterBindingCAS})
		handle, _ := fixture.service.Issue(context.Background(), fixture.request)
		materialization, _ := fixture.service.Materialize(context.Background(), handle)
		_ = os.WriteFile(materialization.AuthFile, []byte(`{"access_token":"candidate"}`), 0o600)
		if _, err := fixture.service.WriteBack(context.Background(), handle, materialization); !errors.Is(err, ErrCredentialInterrupted) {
			t.Fatalf("WriteBack() error = %v, want interrupted", err)
		}
		if fixture.state.current(fixture.binding).Generation != 2 || fixture.secrets.candidateCount() != 1 {
			t.Fatal("CAS interruption did not preserve the binding-to-candidate recovery state")
		}
		restarted, err := New(Config{
			ScratchRoot: t.TempDir(), MaxAuthBytes: 128, Clock: fixture.clock,
			IDs: testkit.NewSequenceIDGenerator("restart-"),
		}, fixture.state, fixture.secrets)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := restarted.Issue(context.Background(), fixture.request); err != nil {
			t.Fatalf("Issue() did not recover exact candidate after CAS: %v", err)
		}
		if fixture.secrets.candidateCount() != 0 {
			t.Fatal("recovered candidate remains uncommitted")
		}
	})
}

func TestRevokeWinsAgainstBlockedWriteBackCAS(t *testing.T) {
	fixture := newCredentialFixture(t, nil)
	fixture.state.casEntered = make(chan struct{})
	fixture.state.casRelease = make(chan struct{})
	handle, _ := fixture.service.Issue(context.Background(), fixture.request)
	materialization, _ := fixture.service.Materialize(context.Background(), handle)
	_ = os.WriteFile(materialization.AuthFile, []byte(`{"access_token":"late"}`), 0o600)

	writeResult := make(chan error, 1)
	go func() {
		_, err := fixture.service.WriteBack(context.Background(), handle, materialization)
		writeResult <- err
	}()
	<-fixture.state.casEntered
	err := fixture.service.RevokeConnection(context.Background(), ports.CredentialRevokeRequest{
		TenantID: "tenant-a", SubscriptionConnectionID: "connection-a", OwnerUserID: "user-a",
	})
	if err != nil {
		t.Fatalf("RevokeConnection() error = %v", err)
	}
	if _, err := os.Lstat(materialization.AuthFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("revoked auth file still exists: %v", err)
	}
	if _, err := os.Lstat(materialization.RootDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("revoked materialization root still exists: %v", err)
	}
	if err := fixture.service.Release(context.Background(), handle); err != nil {
		t.Fatalf("Release() after revoke cleanup is not idempotent: %v", err)
	}
	close(fixture.state.casRelease)
	if err := <-writeResult; !errors.Is(err, ErrCredentialStale) {
		t.Fatalf("late WriteBack() error = %v, want stale", err)
	}
	current := fixture.state.current(fixture.binding)
	if !current.Revoked || current.Generation != 2 || !current.SecretRef.IsZero() || current.SecretFingerprint != "" {
		t.Fatalf("revoked authoritative binding = %+v", current)
	}
	if _, err := fixture.service.Issue(context.Background(), fixture.request); !errors.Is(err, ErrCredentialDenied) {
		t.Fatalf("Issue() after revoke error = %v, want denied", err)
	}
}

func TestMaterializationRejectsUnsafeFilesystemState(t *testing.T) {
	tests := map[string]func(*testing.T, *Service, ports.CredentialHandle, ports.CredentialMaterialization){
		"traversal": func(t *testing.T, service *Service, handle ports.CredentialHandle, value ports.CredentialMaterialization) {
			value.AuthFile = filepath.Join(value.RootDir, "..", "auth.json")
			writeBackMustReject(t, service, handle, value)
		},
		"symlink": func(t *testing.T, service *Service, handle ports.CredentialHandle, value ports.CredentialMaterialization) {
			external := filepath.Join(t.TempDir(), "external.json")
			_ = os.WriteFile(external, []byte("external-secret"), 0o600)
			_ = os.Remove(value.AuthFile)
			if err := os.Symlink(external, value.AuthFile); err != nil {
				t.Fatal(err)
			}
			writeBackMustReject(t, service, handle, value)
			content, _ := os.ReadFile(external)
			if string(content) != "external-secret" {
				t.Fatal("external symlink target was modified")
			}
		},
		"wide file mode": func(t *testing.T, service *Service, handle ports.CredentialHandle, value ports.CredentialMaterialization) {
			_ = os.Chmod(value.AuthFile, 0o644)
			writeBackMustReject(t, service, handle, value)
		},
		"wide directory mode": func(t *testing.T, service *Service, handle ports.CredentialHandle, value ports.CredentialMaterialization) {
			_ = os.Chmod(value.RootDir, 0o755)
			writeBackMustReject(t, service, handle, value)
		},
		"oversize": func(t *testing.T, service *Service, handle ports.CredentialHandle, value ports.CredentialMaterialization) {
			_ = os.WriteFile(value.AuthFile, []byte(strings.Repeat("x", 129)), 0o600)
			writeBackMustReject(t, service, handle, value)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newCredentialFixture(t, nil)
			handle, _ := fixture.service.Issue(context.Background(), fixture.request)
			materialization, _ := fixture.service.Materialize(context.Background(), handle)
			mutate(t, fixture.service, handle, materialization)
		})
	}
}

func writeBackMustReject(t *testing.T, service *Service, handle ports.CredentialHandle, materialization ports.CredentialMaterialization) {
	t.Helper()
	_, err := service.WriteBack(context.Background(), handle, materialization)
	if !errors.Is(err, ErrCredentialMaterialization) {
		t.Fatalf("WriteBack() error = %v, want materialization rejection", err)
	}
}

func TestExpiryBoundaryScratchIsolationAndRedactedErrors(t *testing.T) {
	fixture := newCredentialFixture(t, nil)
	handle, _ := fixture.service.Issue(context.Background(), fixture.request)
	fixture.clock.Advance(5 * time.Minute)
	if _, err := fixture.service.Materialize(context.Background(), handle); !errors.Is(err, ErrCredentialExpired) {
		t.Fatalf("Materialize() at exclusive expiry error = %v", err)
	}

	first := newCredentialFixture(t, nil)
	second := newCredentialFixture(t, nil)
	if first.service.rootDir == second.service.rootDir {
		t.Fatal("services reused a caller-chosen credential root")
	}
	assertMode(t, first.service.rootDir, 0o700)

	scratch := t.TempDir()
	symlink := filepath.Join(t.TempDir(), "scratch-link")
	if err := os.Symlink(scratch, symlink); err != nil {
		t.Fatal(err)
	}
	_, err := New(Config{ScratchRoot: symlink, MaxAuthBytes: 128, Clock: first.clock, IDs: testkit.NewSequenceIDGenerator("bad-")}, first.state, first.secrets)
	if !errors.Is(err, ErrCredentialMaterialization) {
		t.Fatalf("New() through symlink error = %v, want materialization rejection", err)
	}

	unsafe := newCredentialFixture(t, nil)
	const raw = "raw-token-and-vault-reference"
	unsafe.secrets.readError = errors.New(raw)
	handle, _ = unsafe.service.Issue(context.Background(), unsafe.request)
	_, err = unsafe.service.Materialize(context.Background(), handle)
	if !errors.Is(err, ErrCredentialBackend) || strings.Contains(err.Error(), raw) {
		t.Fatalf("backend error was not sanitized: %v", err)
	}
}

func TestReleaseRejectsReplacedInvocationRoot(t *testing.T) {
	fixture := newCredentialFixture(t, nil)
	handle, _ := fixture.service.Issue(context.Background(), fixture.request)
	materialization, _ := fixture.service.Materialize(context.Background(), handle)
	external := t.TempDir()
	externalAuth := filepath.Join(external, "auth.json")
	if err := os.WriteFile(externalAuth, []byte("must-survive"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(materialization.AuthFile); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(materialization.RootDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, materialization.RootDir); err != nil {
		t.Fatal(err)
	}
	if err := fixture.service.Release(context.Background(), handle); !errors.Is(err, ErrCredentialMaterialization) {
		t.Fatalf("Release() through replaced root error = %v, want materialization rejection", err)
	}
	content, err := os.ReadFile(externalAuth)
	if err != nil || string(content) != "must-survive" {
		t.Fatalf("external auth file changed: %q, %v", content, err)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != want {
		t.Fatalf("%s mode = %o, want %o", path, info.Mode().Perm(), want)
	}
}

func bindingKey(tenant domain.TenantID, connection domain.SubscriptionConnectionID) string {
	return string(tenant) + "/" + string(connection)
}

func candidateScope(binding domain.CredentialBinding) ports.CredentialCandidateScope {
	return ports.CredentialCandidateScope{
		TenantID: binding.TenantID, SubscriptionConnectionID: binding.SubscriptionConnectionID,
		OwnerUserID: binding.OwnerUserID, ExpectedGeneration: binding.Generation,
	}
}
