//go:build linux || darwin

package ownerlocalcredential

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

var fixtureTime = time.Date(2026, time.October, 26, 3, 0, 0, 0, time.UTC)

func storeFixture(t *testing.T) (*Store, Config, []byte) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	config := Config{Root: root, Scope: Scope{TenantID: "tenant-a", OwnerUserID: "owner-a", SubscriptionConnectionID: "connection-a"}, KeyEpoch: "synthetic-key-epoch"}
	key := bytes.Repeat([]byte{0x37}, 32)
	s, err := New(t.Context(), config, key)
	if err != nil {
		t.Fatal("new synthetic store", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close synthetic store: %v", err)
		}
		clear(key)
	})
	return s, config, key
}

func initializedStore(t *testing.T) (*Store, Config, []byte, domain.CredentialBinding) {
	t.Helper()
	s, config, key := storeFixture(t)
	if err := s.Initialize(t.Context(), Registration{Provider: "openai", AuthMode: "chatgpt", Entitlement: domain.EntitlementActive, At: fixtureTime}, []byte("synthetic-original-marker")); err != nil {
		t.Fatal("initialize explicit synthetic input", err)
	}
	b, found, err := s.LoadCredentialBinding(t.Context(), config.Scope.TenantID, config.Scope.SubscriptionConnectionID)
	if err != nil || !found {
		t.Fatal("load initial binding", found, err)
	}
	return s, config, key, b
}

func rotate(t *testing.T, s *Store, b domain.CredentialBinding, marker string) domain.CredentialBinding {
	t.Helper()
	c, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(b.Generation), []byte(marker))
	if err != nil {
		t.Fatal("put rotation", err)
	}
	next := b
	next.Generation++
	next.SecretRef = c.Reference
	next.SecretFingerprint = c.Fingerprint
	next.UpdatedAt = next.UpdatedAt.Add(time.Second)
	if swapped, err := s.CompareAndSwapCredentialBinding(t.Context(), b.Generation, next); err != nil || !swapped {
		t.Fatal("rotation CAS", swapped, err)
	}
	if err := s.CommitCredentialCandidate(t.Context(), c); err != nil {
		t.Fatal("commit rotation", err)
	}
	return next
}

func TestOwnerLocalEncryptedPersistenceReopenAndScope(t *testing.T) {
	s, config, key, b := initializedStore(t)
	plain, err := s.ReadCredentialSecret(t.Context(), b, 128)
	if err != nil || string(plain) != "synthetic-original-marker" {
		t.Fatal("read exact initial secret", err)
	}
	clear(plain)
	if err := s.Initialize(t.Context(), Registration{Provider: "openai", AuthMode: "chatgpt", Entitlement: domain.EntitlementActive, At: fixtureTime}, []byte("other")); err != ErrDenied {
		t.Fatal("initialization overwrote binding", err)
	}
	if _, err := New(t.Context(), config, key); err != ErrBusy {
		t.Fatal("second lifetime owner not fenced", err)
	}
	if _, _, err := s.LoadCredentialBinding(t.Context(), "tenant-other", config.Scope.SubscriptionConnectionID); err != ErrDenied {
		t.Fatal("foreign tenant loaded", err)
	}
	foreign := b
	foreign.OwnerUserID = "owner-other"
	if _, err := s.ReadCredentialSecret(t.Context(), foreign, 128); err != ErrDenied {
		t.Fatal("foreign binding read secret", err)
	}
	entries, err := os.ReadDir(config.Root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join(config.Root, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte("synthetic-original-marker")) {
			t.Fatal("plaintext durable marker", entry.Name())
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := bytes.Repeat([]byte{0x38}, 32)
	if _, err := New(t.Context(), config, wrong); err != ErrDenied {
		t.Fatal("wrong key accepted metadata", err)
	}
	reopened, err := New(t.Context(), config, key)
	if err != nil {
		t.Fatal("reopen original durable root", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got, found, err := reopened.LoadCredentialBinding(t.Context(), config.Scope.TenantID, config.Scope.SubscriptionConnectionID)
	if err != nil || !found || got != b || got.SecretRef.StorageValue() != b.SecretRef.StorageValue() {
		t.Fatal("private secret reference/generation lost on reopen", found, err)
	}
	plain, err = reopened.ReadCredentialSecret(t.Context(), got, 128)
	if err != nil || string(plain) != "synthetic-original-marker" {
		t.Fatal("reopened body", err)
	}
	clear(plain)
}

func TestOwnerLocalPrivateObjectRedaction(t *testing.T) {
	s, _, key, b := initializedStore(t)
	life := &Lifecycle{lifecycle: &lifecycle{store: s, handle: &ports.CredentialHandle{HandleID: "private-handle-redaction-marker"}}}
	for _, value := range []any{s, *s, life, *life} {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			out := fmt.Sprintf(format, value)
			for _, marker := range []string{string(key), hex.EncodeToString(key), string(s.macKey), hex.EncodeToString(s.macKey), b.SecretRef.StorageValue(), "synthetic-original-marker", "private-handle-redaction-marker"} {
				if strings.Contains(out, marker) {
					t.Fatalf("private object formatting %s exposed marker", format)
				}
			}
			if !strings.Contains(out, "redacted") {
				t.Fatalf("private object formatting %s did not redact", format)
			}
		}
		if raw, err := json.Marshal(value); err == nil || len(raw) != 0 || strings.Contains(err.Error(), b.SecretRef.StorageValue()) || strings.Contains(err.Error(), "private-handle-redaction-marker") {
			t.Fatal("private object serialization not denied content-free")
		}
	}
	raw, err := json.Marshal(b)
	if err != nil || bytes.Contains(raw, []byte(b.SecretRef.StorageValue())) {
		t.Fatal("public binding ref redaction changed", err)
	}
}

func TestOwnerLocalCandidateCustodyAndReservedRevokeHeadroom(t *testing.T) {
	s, _, _, b := initializedStore(t)
	for i := 0; i < 3; i++ {
		b = rotate(t, s, b, strings.Repeat("synthetic-rotated-", i+1))
	}
	if len(s.state.Cleanups) != 3 {
		t.Fatal("expected three superseded references")
	}
	if _, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(b.Generation), []byte("blocked-rotation")); err != ErrDenied {
		t.Fatal("rotation consumed revoke reservation", err)
	}
	request := ports.CredentialRevokeRequest{TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, SubscriptionConnectionID: b.SubscriptionConnectionID}
	r, err := s.RevokeCredentialBinding(t.Context(), request, b.UpdatedAt.Add(time.Second))
	if err != nil || !r.Binding.Revoked || r.Binding.Generation != b.Generation+1 || len(s.state.Cleanups) != 4 {
		t.Fatal("reserved revoke failed", err)
	}
	if again, err := s.RevokeCredentialBinding(t.Context(), request, r.Binding.UpdatedAt); err != nil || again.Binding != r.Binding {
		t.Fatal("revoke not idempotent", err)
	}
	if swapped, err := s.CompareAndSwapCredentialBinding(t.Context(), b.Generation, b); err != nil || swapped {
		t.Fatal("late CAS resurrected revoked binding", swapped, err)
	}
	if _, err := s.ReadCredentialSecret(t.Context(), b, 128); err != ErrDenied {
		t.Fatal("revoked secret remained readable", err)
	}
}

func TestOwnerLocalActiveCandidateCannotBeDeletedAndExactRecovery(t *testing.T) {
	s, _, _, b := initializedStore(t)
	c, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(b.Generation), []byte("synthetic-new-body"))
	if err != nil {
		t.Fatal(err)
	}
	next := b
	next.Generation++
	next.SecretRef = c.Reference
	next.SecretFingerprint = c.Fingerprint
	next.UpdatedAt = b.UpdatedAt.Add(time.Second)
	if swapped, err := s.CompareAndSwapCredentialBinding(t.Context(), b.Generation, next); err != nil || !swapped {
		t.Fatal(swapped, err)
	}
	if err := s.DeleteCredentialCandidate(t.Context(), c); err != ErrDenied {
		t.Fatal("active uncommitted candidate deleted", err)
	}
	if err := s.DeleteCredentialSecret(t.Context(), s.scopeFor(next.Generation), next.SecretRef); err != ErrDenied {
		t.Fatal("active body deleted", err)
	}
	forged := next
	forged.Generation++
	if _, err := s.RecoverCredentialCandidate(t.Context(), forged); err != ErrDenied {
		t.Fatal("foreign relation recovered", err)
	}
	if recovered, err := s.RecoverCredentialCandidate(t.Context(), next); err != nil || !recovered {
		t.Fatal("exact active candidate recovery", recovered, err)
	}
	if recovered, err := s.RecoverCredentialCandidate(t.Context(), next); err != nil || recovered {
		t.Fatal("already committed recovery changed state", recovered, err)
	}
}

func TestOwnerLocalDurabilityCutsReconcileOriginalImages(t *testing.T) {
	for _, point := range []string{"file-sync", "before-rename", "after-rename", "directory-sync"} {
		t.Run(point, func(t *testing.T) {
			s, _, _, b := initializedStore(t)
			before := bytes.Clone(s.image)
			fired := false
			s.fs.fault = func(p string) error {
				if p == point && !fired {
					fired = true
					return ErrUnavailable
				}
				return nil
			}
			_, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(b.Generation), []byte("synthetic-cut-body"))
			if err == nil || !fired {
				t.Fatal("intended durability cut did not fail", err, fired)
			}
			s.fs.fault = nil
			if err == ErrUnknown {
				if _, _, err := s.LoadCredentialBinding(t.Context(), b.TenantID, b.SubscriptionConnectionID); err != ErrUnknown {
					t.Fatal("unknown publication did not fence reads", err)
				}
				if err := s.Close(); err != ErrUnknown {
					t.Fatal("unknown owner flock dropped", err)
				}
				if err := s.Reconcile(t.Context()); err != nil {
					t.Fatal("exact original image could not reconcile", err)
				}
			} else if !bytes.Equal(before, s.image) {
				t.Fatal("known pre-rename failure changed current image")
			}
			current, found, err := s.LoadCredentialBinding(t.Context(), b.TenantID, b.SubscriptionConnectionID)
			if err != nil || !found || current != b {
				t.Fatal("cut changed binding before original CAS", found, err)
			}
			for _, c := range append([]candidateRecord{}, s.state.Candidates...) {
				if err := s.DeleteCredentialCandidate(t.Context(), candidatePort(s, c)); err != nil {
					t.Fatal("exact abandoned reservation cleanup", err)
				}
			}
		})
	}
}

func TestOwnerLocalBoundsTamperAndGenerationOverflow(t *testing.T) {
	t.Run("candidate-and-key-use", func(t *testing.T) {
		s, _, _, b := initializedStore(t)
		for i := 0; i < maxCandidates; i++ {
			if _, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(b.Generation), []byte{byte(i + 1)}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(b.Generation), []byte("fifth")); err != ErrDenied {
			t.Fatal("unbounded candidates", err)
		}
		if _, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(b.Generation), make([]byte, maxAuthBytes+1)); err != ErrDenied {
			t.Fatal("oversized auth admitted", err)
		}
	})
	t.Run("last-generation-revokes", func(t *testing.T) {
		s, _, _, b := initializedStore(t)
		state := cloneSnapshot(s.state)
		state.Binding.Value.Generation = math.MaxUint64 - 1
		if err := s.saveLocked(t.Context(), state); err != nil {
			t.Fatal("controlled boundary metadata", err)
		}
		if _, err := s.PutCredentialCandidate(t.Context(), s.scopeFor(math.MaxUint64-1), []byte("wrapped")); err != ErrDenied {
			t.Fatal("wrap candidate admitted", err)
		}
		request := ports.CredentialRevokeRequest{TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, SubscriptionConnectionID: b.SubscriptionConnectionID}
		if r, err := s.RevokeCredentialBinding(t.Context(), request, b.UpdatedAt); err != nil || r.Binding.Generation != math.MaxUint64 || !r.Binding.Revoked {
			t.Fatal("last generation not reserved for revoke", err)
		}
	})
	t.Run("ciphertext-tamper", func(t *testing.T) {
		s, config, _, b := initializedStore(t)
		path := filepath.Join(config.Root, bodyName(b.SecretRef.StorageValue()))
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)-1] ^= 1
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		if plain, err := s.ReadCredentialSecret(t.Context(), b, 128); err != ErrDenied || len(plain) != 0 {
			t.Fatal("tampered ciphertext exposed plaintext", err)
		}
	})
}

func lifecycleRequest() ports.CredentialIssueRequest {
	started := fixtureTime.Add(-time.Minute)
	return ports.CredentialIssueRequest{OwnerUserID: "owner-a",
		Run:     domain.Run{ID: "run-a", TenantID: "tenant-a", SessionID: "session-a", TriggerEventID: "event-a", SubscriptionConnectionID: "connection-a", Status: domain.RunRunning, IdempotencyKey: "run-key", StartedAt: &started, CreatedAt: started, UpdatedAt: fixtureTime},
		Attempt: domain.Attempt{ID: "attempt-a", TenantID: "tenant-a", RunID: "run-a", Number: 1, Status: domain.AttemptRunning, WorkerID: "worker-a", CreatedAt: started, UpdatedAt: fixtureTime},
		Lease:   domain.Lease{ID: "lease-a", TenantID: "tenant-a", RunID: "run-a", AttemptID: "attempt-a", WorkerID: "worker-a", FenceToken: 9, AcquiredAt: started, ExpiresAt: fixtureTime.Add(10 * time.Minute)}, ExpiresAt: fixtureTime.Add(5 * time.Minute),
		ProviderResource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "connection-a", OwnerUserID: "owner-a", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}}
}

type controlledFailure struct {
	mu      sync.Mutex
	point   credentiallifecycle.FailurePoint
	once    bool
	entered chan struct{}
	proceed chan struct{}
	allow   bool
}

func (f *controlledFailure) Check(point credentiallifecycle.FailurePoint) error {
	f.mu.Lock()
	if point != f.point || f.once {
		f.mu.Unlock()
		return nil
	}
	f.once = true
	entered, proceed := f.entered, f.proceed
	f.mu.Unlock()
	if entered != nil {
		close(entered)
		<-proceed
	}
	if f.allow {
		return nil
	}
	return errors.New("controlled synthetic interruption")
}

func TestOwnerLocalOriginalLifecycleGuardWritebackAndRelease(t *testing.T) {
	s, _, _, b := initializedStore(t)
	scratch, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	failure := &controlledFailure{point: credentiallifecycle.FailureBeforeCleanup}
	life, err := NewLifecycle(s, credentiallifecycle.Config{ScratchRoot: scratch, MaxAuthBytes: 128, Clock: testkit.NewFakeClock(fixtureTime), IDs: testkit.NewSequenceIDGenerator("synthetic-"), Failures: failure})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := life.Close(); err != nil {
			t.Errorf("close guarded lifecycle: %v", err)
		}
	})
	wrong := lifecycleRequest()
	wrong.OwnerUserID = "other-owner"
	if _, err := life.Issue(t.Context(), wrong); err != ErrDenied {
		t.Fatal("foreign Issue", err)
	}
	handle, err := life.Issue(t.Context(), lifecycleRequest())
	if err != nil {
		t.Fatal("pre-handle denial retained guard", err)
	}
	if err := s.Close(); err != ErrBusy {
		t.Fatal("Close dropped active owner flock", err)
	}
	if _, err := life.Issue(t.Context(), lifecycleRequest()); err != ErrBusy {
		t.Fatal("second refresh owner admitted", err)
	}
	material, err := life.Materialize(t.Context(), handle)
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := life.WriteBack(t.Context(), handle, material)
	if err != nil || unchanged.Changed || unchanged.Generation != b.Generation {
		t.Fatal("unchanged writeback changed generation", unchanged, err)
	}
	if err := life.Release(t.Context(), handle); err == nil {
		t.Fatal("controlled cleanup failure not reported")
	}
	if _, err := life.Issue(t.Context(), lifecycleRequest()); err != ErrBusy {
		t.Fatal("failed Release dropped guard", err)
	}
	if err := life.Close(); err != ErrBusy {
		t.Fatal("failed Release Close lost custody", err)
	}
	if err := life.Release(t.Context(), handle); err != nil {
		t.Fatal("exact original cleanup retry", err)
	}
	if err := life.Release(t.Context(), handle); err != nil {
		t.Fatal("exact Release not idempotent", err)
	}
	second, err := life.Issue(t.Context(), lifecycleRequest())
	if err != nil {
		t.Fatal(err)
	}
	material, err = life.Materialize(t.Context(), second)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(material.AuthFile, []byte("synthetic-refreshed-marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	changed, err := life.WriteBack(t.Context(), second, material)
	if err != nil || !changed.Changed || changed.Generation != 2 {
		t.Fatal("original lifecycle changed writeback", changed, err)
	}
	if err := life.Release(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if _, err := life.Issue(t.Context(), lifecycleRequest()); err == nil {
		t.Fatal("stale sealed generation silently upgraded")
	}
}

func TestOwnerLocalRevokeBypassesGuardAndLateWritebackLoses(t *testing.T) {
	s, _, _, b := initializedStore(t)
	scratch, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	failure := &controlledFailure{point: credentiallifecycle.FailureAfterCandidatePut, entered: make(chan struct{}), proceed: make(chan struct{}), allow: true}
	life, err := NewLifecycle(s, credentiallifecycle.Config{ScratchRoot: scratch, MaxAuthBytes: 128, Clock: testkit.NewFakeClock(fixtureTime), IDs: testkit.NewSequenceIDGenerator("synthetic-race-"), Failures: failure})
	if err != nil {
		t.Fatal(err)
	}
	var handle *ports.CredentialHandle
	var workers []<-chan struct{}
	var barrierOnce sync.Once
	releaseBarrier := func() { barrierOnce.Do(func() { close(failure.proceed) }) }
	// Install before any Fatal or worker launch. Release the test barrier
	// before joining, and join before Close can acquire the lifecycle mutex.
	t.Cleanup(func() {
		releaseBarrier()
		cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, worker := range workers {
			if !waitOwnerLocalWorker(cleanupContext, worker) {
				t.Error("test worker did not exit after barrier release; skipping blocking lifecycle cleanup")
				return
			}
		}
		if handle != nil {
			if err := life.Release(cleanupContext, *handle); err != nil {
				t.Error("exact original cleanup", err)
			}
		}
		if err := life.Close(); err != nil {
			t.Error("lifecycle close", err)
		}
	})
	h, err := life.Issue(t.Context(), lifecycleRequest())
	if err != nil {
		t.Fatal(err)
	}
	handle = &h
	m, err := life.Materialize(t.Context(), h)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(m.AuthFile, []byte("synthetic-racing-refresh"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	writeDone := make(chan error, 1)
	writeExited := make(chan struct{})
	workers = append(workers, writeExited)
	go func() {
		defer close(writeExited)
		_, err := life.WriteBack(ctx, h, m)
		writeDone <- err
	}()
	if !waitOwnerLocalWorker(ctx, failure.entered) {
		t.Fatal("writeback did not reach deterministic candidate barrier")
	}
	revokeDone := make(chan error, 1)
	revokeExited := make(chan struct{})
	workers = append(workers, revokeExited)
	go func() {
		defer close(revokeExited)
		revokeDone <- life.RevokeConnection(ctx, ports.CredentialRevokeRequest{TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, SubscriptionConnectionID: b.SubscriptionConnectionID})
	}()
	// Revoke must actually exit while WriteBack remains at the barrier;
	// releasing the barrier first would erase the independence assertion.
	if !waitOwnerLocalWorker(ctx, revokeExited) {
		t.Fatal("Revoke blocked behind WriteBack guard")
	}
	if err := <-revokeDone; err != nil {
		t.Fatal("Revoke waited on invocation guard or failed", err)
	}
	releaseBarrier()
	if !waitOwnerLocalWorker(ctx, writeExited) {
		t.Fatal("writeback did not finish")
	}
	if err := <-writeDone; err == nil {
		t.Fatal("interrupted late writeback unexpectedly succeeded")
	}
	current, found, err := s.LoadCredentialBinding(t.Context(), b.TenantID, b.SubscriptionConnectionID)
	if err != nil || !found || !current.Revoked || current.Generation != 2 {
		t.Fatal("deny-first revoke not retained", found, err)
	}
	if err := life.Release(t.Context(), h); err != nil {
		t.Fatal("original release after revoke", err)
	}
}

// A deadline and completion may become ready together. Recheck completion at
// the deadline; never turn a cleanup wait into an unbounded channel read.
func waitOwnerLocalWorker(ctx context.Context, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-ctx.Done():
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
}
