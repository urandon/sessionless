package attachedworkerforeground

import (
	"context"
	"crypto/ed25519"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestRunDisabledRetiresObservationAndReleasesOwnership(t *testing.T) {
	store, _, now := initializedStore(t)
	foreground, err := New(store, Config{Now: func() time.Time { return now.Add(time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	result, runErr := foreground.Run(context.Background())
	if !errors.Is(runErr, ErrFeatureDisabled) {
		t.Fatalf("Run error = %v, want ErrFeatureDisabled", runErr)
	}
	if result.Code != CodeFeatureDisabled || result.RuntimeOwnership != "released" ||
		result.ObservationState != "retired" || result.ObservationRevision != 1 ||
		result.DaemonState != "not_started" || result.ServerConnectionState != "unknown" ||
		result.NetworkAction != "not_attempted" || result.ProcessAction != "not_attempted" ||
		result.CredentialAction != "not_attempted" {
		t.Fatalf("Run result = %+v", result)
	}
	status, err := store.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.DaemonObservation != "unknown" || status.ObservationRevision != 0 {
		t.Fatalf("status retained foreground observation: %+v", status)
	}
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatalf("runtime lease was not released: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRunHoldsKernelRuntimeLeaseThroughPreflight(t *testing.T) {
	store, _, now := initializedStore(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	first, err := New(store, Config{Now: func() time.Time {
		once.Do(func() { close(entered) })
		<-release
		return now.Add(time.Second)
	}})
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, runErr := first.Run(context.Background())
		firstDone <- runErr
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first foreground did not reach the owned preflight state")
	}

	second, err := New(store, Config{Now: func() time.Time { return now.Add(2 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	result, runErr := second.Run(context.Background())
	if !errors.Is(runErr, attachedworkerlocal.ErrStateBusy) ||
		result.Code != ResultCode(attachedworkerlocal.CodeBusy) || result.RuntimeOwnership != "not_acquired" {
		t.Fatalf("concurrent Run result=%+v error=%v", result, runErr)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case runErr := <-firstDone:
		if !errors.Is(runErr, ErrFeatureDisabled) {
			t.Fatalf("first Run error = %v", runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first foreground did not finish after release")
	}
}

func TestRunRejectsLoggedOutStateWithoutWritingObservation(t *testing.T) {
	store, _, now := initializedStore(t)
	if _, err := store.Logout(context.Background(), attachedworkerlocal.LogoutInputV1{
		ExpectedRevision: 1, RequestID: "logout-foreground-001",
	}); err != nil {
		t.Fatal(err)
	}
	foreground, err := New(store, Config{Now: func() time.Time { return now.Add(2 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	result, runErr := foreground.Run(context.Background())
	if !errors.Is(runErr, attachedworkerlocal.ErrSecretRetired) ||
		result.Code != ResultCode(attachedworkerlocal.CodeRetired) ||
		result.RuntimeOwnership != "released" || result.ObservationState != "not_written" {
		t.Fatalf("Run result=%+v error=%v", result, runErr)
	}
	status, err := store.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.DaemonObservation != "unknown" {
		t.Fatalf("logged-out status = %+v", status)
	}
}

func TestRunAdvancesValidatedPriorObservationBeforeRetiringIt(t *testing.T) {
	store, _, now := initializedStore(t)
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.PersistObservation(context.Background(), attachedworkerlocal.RuntimeObservationV1{
		Version: 1, Revision: 1, ManifestRevision: 1, State: "running", Active: true,
		Accepted: 3, Completed: 2, Failed: 1, ObservedAt: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	foreground, err := New(store, Config{Now: func() time.Time { return now.Add(2 * time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	result, runErr := foreground.Run(context.Background())
	if !errors.Is(runErr, ErrFeatureDisabled) || result.ObservationRevision != 2 ||
		result.ObservationState != "retired" {
		t.Fatalf("Run result=%+v error=%v", result, runErr)
	}
	status, err := store.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.DaemonObservation != "unknown" {
		t.Fatalf("status retained observation: %+v", status)
	}
}

func TestRunPreservesFailClosedPreflightCodesWithoutObservation(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code ResultCode
	}{
		{name: "missing", err: attachedworkerlocal.ErrStateMissing, code: ResultCode(attachedworkerlocal.CodeMissing)},
		{name: "incomplete", err: attachedworkerlocal.ErrStateIncomplete, code: ResultCode(attachedworkerlocal.CodeIncomplete)},
		{name: "ambiguous", err: attachedworkerlocal.ErrStateAmbiguous, code: ResultCode(attachedworkerlocal.CodeAmbiguous)},
		{name: "invalid", err: attachedworkerlocal.ErrInvalidState, code: ResultCode(attachedworkerlocal.CodeInvalid)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lease := &fakeLease{}
			store := &fakeStore{lease: lease, snapshotErr: test.err}
			foreground, err := newForeground(store, Config{})
			if err != nil {
				t.Fatal(err)
			}
			result, runErr := foreground.Run(context.Background())
			if !errors.Is(runErr, test.err) || result.Code != test.code ||
				result.RuntimeOwnership != "released" || result.ObservationState != "not_written" ||
				lease.persistCalls != 0 || lease.retireCalls != 0 || lease.closeCalls != 1 {
				t.Fatalf("Run result=%+v error=%v lease=%+v", result, runErr, lease)
			}
		})
	}
}

func TestRunCleanupFailureOverridesDisabledCodeAndStillCloses(t *testing.T) {
	manifest := attachedworkerlocal.ManifestV1{Lifecycle: attachedworkerlocal.LifecycleActive, Revision: 7,
		UpdatedAt: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)}
	lease := &fakeLease{retireErr: attachedworkerlocal.ErrLocalIO}
	store := &fakeStore{lease: lease, snapshot: attachedworkerlocal.SnapshotV1{Manifest: manifest}, secret: attachedworkerlocal.SecretRecordV1{}}
	foreground, err := newForeground(store, Config{Now: func() time.Time { return manifest.UpdatedAt }})
	if err != nil {
		t.Fatal(err)
	}
	result, runErr := foreground.Run(context.Background())
	if !errors.Is(runErr, ErrFeatureDisabled) || !errors.Is(runErr, attachedworkerlocal.ErrLocalIO) {
		t.Fatalf("Run error = %v", runErr)
	}
	if result.Code != ResultCode(attachedworkerlocal.CodeIO) || result.ObservationState != "unknown" ||
		result.RuntimeOwnership != "released" || lease.closeCalls != 1 || lease.retireCalls != 1 {
		t.Fatalf("Run result=%+v lease=%+v", result, lease)
	}
}

func TestRunCleanupIgnoresCallerCancellationAfterObservationPersist(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	manifest := attachedworkerlocal.ManifestV1{Lifecycle: attachedworkerlocal.LifecycleActive, Revision: 7,
		UpdatedAt: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)}
	lease := &fakeLease{persistHook: cancel}
	store := &fakeStore{lease: lease, snapshot: attachedworkerlocal.SnapshotV1{Manifest: manifest}}
	foreground, err := newForeground(store, Config{Now: func() time.Time { return manifest.UpdatedAt }})
	if err != nil {
		t.Fatal(err)
	}
	result, runErr := foreground.Run(ctx)
	if !errors.Is(runErr, ErrFeatureDisabled) || result.Code != CodeFeatureDisabled ||
		result.ObservationState != "retired" || result.RuntimeOwnership != "released" ||
		lease.retireContextErr != nil || lease.retireCalls != 1 || lease.closeCalls != 1 {
		t.Fatalf("Run result=%+v error=%v lease=%+v", result, runErr, lease)
	}
}

type fakeStore struct {
	lease       localRuntimeLease
	snapshot    attachedworkerlocal.SnapshotV1
	snapshotErr error
	secret      attachedworkerlocal.SecretRecordV1
}

func (store *fakeStore) AcquireRuntime(context.Context) (localRuntimeLease, error) {
	return store.lease, nil
}

func (store *fakeStore) LoadSnapshot(context.Context) (attachedworkerlocal.SnapshotV1, error) {
	return store.snapshot, store.snapshotErr
}

func (store *fakeStore) LoadSecret(context.Context) (attachedworkerlocal.SecretRecordV1, error) {
	return store.secret, nil
}

type fakeLease struct {
	retireErr        error
	retireContextErr error
	persistHook      func()
	closeCalls       int
	retireCalls      int
	persistCalls     int
}

func (lease *fakeLease) PersistObservation(context.Context, attachedworkerlocal.RuntimeObservationV1) error {
	lease.persistCalls++
	if lease.persistHook != nil {
		lease.persistHook()
	}
	return nil
}

func (lease *fakeLease) RetireObservation(ctx context.Context, _ uint64) error {
	lease.retireCalls++
	lease.retireContextErr = ctx.Err()
	return lease.retireErr
}

func (lease *fakeLease) Close() error {
	lease.closeCalls++
	return nil
}

func initializedStore(t *testing.T) (*attachedworkerlocal.Store, ed25519.PrivateKey, time.Time) {
	t.Helper()
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	boundary := attachedworkerlocal.BoundaryLinuxRootless
	if runtime.GOOS == "darwin" {
		boundary = attachedworkerlocal.BoundaryDarwinVM
	}
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	manifest := attachedworkerlocal.ManifestV1{
		Version: 1, Revision: 1, ControlPlaneOrigin: "https://control.example",
		TenantID: "tenant-001", OwnerUserID: "user-001", WorkerID: "worker-001", EnrollmentGeneration: 1,
		IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(publicKey)),
		OCI: attachedworkerlocal.OCIConfigV1{DockerPath: filepath.Join(parent, "docker"), DockerSHA256: strings.Repeat("a", 64),
			CLIConfigDir: filepath.Join(parent, "docker-config"), Host: "unix:///private/tmp/sessionless-docker.sock",
			EngineID: "engine-001-abcdef", InstallationID: "install-001", Boundary: boundary,
			Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64), UserID: 1000, GroupID: 1000,
			DiskBytes: 1 << 30, CredentialFileBytes: 1024, MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10},
		Harness: attachedworkerlocal.HarnessConfigV1{Executable: filepath.Join(parent, "harness"),
			SHA256: strings.Repeat("c", 64), Arguments: []string{}},
		Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: now, UpdatedAt: now,
	}
	secret := attachedworkerlocal.SecretRecordV1{Version: 1, ManifestRevision: 1, TenantID: manifest.TenantID,
		OwnerUserID: manifest.OwnerUserID, WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, IdentityPrivateKey: privateKey}
	store, err := attachedworkerlocal.NewStore(filepath.Join(parent, "state"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatal(err)
	}
	return store, privateKey, now
}
