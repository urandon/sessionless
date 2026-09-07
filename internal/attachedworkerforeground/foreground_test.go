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

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
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
		result.ObservationState != "retired" || result.ObservationRevision != 3 ||
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
	if !errors.Is(runErr, ErrFeatureDisabled) || result.ObservationRevision != 4 ||
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

func TestSessionDrainAndShutdownAreExplicitAndIdempotent(t *testing.T) {
	store, _, now := initializedStore(t)
	foreground, err := New(store, Config{Now: func() time.Time { return now.Add(time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	result, session, err := foreground.Start(context.Background())
	if err != nil || session == nil || result.ObservationRevision != 1 || result.RuntimeOwnership != "acquired" {
		t.Fatalf("Start result=%+v session=%v error=%v", result, session != nil, err)
	}
	if err := session.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.Drain(context.Background()); err != nil {
		t.Fatalf("repeated Drain error = %v", err)
	}
	status, err := store.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.DaemonObservation != "observed_local" || status.DaemonState != "draining" || status.ObservationRevision != 2 {
		t.Fatalf("draining status = %+v", status)
	}
	if err := session.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.Shutdown(context.Background()); err != nil {
		t.Fatalf("repeated Shutdown error = %v", err)
	}
	result = session.Result()
	if result.ObservationRevision != 3 || result.ObservationState != "retired" ||
		result.RuntimeOwnership != "released" || result.DaemonState != "not_started" {
		t.Fatalf("Shutdown result = %+v", result)
	}
	status, err = store.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.DaemonObservation != "unknown" || status.ObservationRevision != 0 {
		t.Fatalf("shutdown retained observation: %+v", status)
	}
}

func TestSessionConcurrentShutdownRunsCleanupOnce(t *testing.T) {
	store, _, now := initializedStore(t)
	foreground, err := New(store, Config{Now: func() time.Time { return now.Add(time.Second) }})
	if err != nil {
		t.Fatal(err)
	}
	_, session, err := foreground.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	const callers = 12
	errorsSeen := make(chan error, callers)
	var group sync.WaitGroup
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			errorsSeen <- session.Shutdown(context.Background())
		}()
	}
	group.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("concurrent Shutdown error = %v", err)
		}
	}
	if result := session.Result(); result.ObservationRevision != 3 ||
		result.ObservationState != "retired" || result.RuntimeOwnership != "released" {
		t.Fatalf("concurrent Shutdown result = %+v", result)
	}
}

func TestSessionShutdownWaitIsCallerBoundedButCleanupContinues(t *testing.T) {
	manifest := attachedworkerlocal.ManifestV1{Lifecycle: attachedworkerlocal.LifecycleActive, Revision: 7,
		UpdatedAt: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)}
	lease := newBlockingLifecycleLease(2)
	store := &fakeStore{lease: lease, snapshot: attachedworkerlocal.SnapshotV1{Manifest: manifest}}
	foreground, err := newForeground(store, Config{
		Now: func() time.Time { return manifest.UpdatedAt }, CleanupTimeout: time.Second,
	}, ActivationPorts{})
	if err != nil {
		t.Fatal(err)
	}
	_, session, err := foreground.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := session.Shutdown(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded Shutdown error = %v, want deadline", err)
	}
	select {
	case <-lease.entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not continue after caller timeout")
	}
	close(lease.release)
	if err := session.Shutdown(context.Background()); err != nil {
		t.Fatalf("completed Shutdown error = %v", err)
	}
	persistCalls, retireCalls, closeCalls := lease.counts()
	if persistCalls != 3 || retireCalls != 1 || closeCalls != 1 {
		t.Fatalf("cleanup calls persist=%d retire=%d close=%d", persistCalls, retireCalls, closeCalls)
	}
}

func TestConcurrentDrainCancellationIsHonoredWhileShutdownPersists(t *testing.T) {
	manifest := attachedworkerlocal.ManifestV1{Lifecycle: attachedworkerlocal.LifecycleActive, Revision: 7,
		UpdatedAt: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)}
	lease := newBlockingLifecycleLease(2)
	store := &fakeStore{lease: lease, snapshot: attachedworkerlocal.SnapshotV1{Manifest: manifest}}
	foreground, err := newForeground(store, Config{
		Now: func() time.Time { return manifest.UpdatedAt }, CleanupTimeout: time.Second,
	}, ActivationPorts{})
	if err != nil {
		t.Fatal(err)
	}
	_, session, err := foreground.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- session.Shutdown(context.Background()) }()
	select {
	case <-lease.entered:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not enter its draining persist")
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	drainResult := make(chan error, 1)
	go func() { drainResult <- session.Drain(drainCtx) }()
	select {
	case err := <-drainResult:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("concurrent Drain error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("concurrent Drain ignored cancellation while shutdown held the transition")
	}
	close(lease.release)
	select {
	case err := <-shutdownResult:
		if err != nil {
			t.Fatalf("Shutdown error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not complete after persist release")
	}
	persistCalls, retireCalls, closeCalls := lease.counts()
	if persistCalls != 3 || retireCalls != 1 || closeCalls != 1 {
		t.Fatalf("cleanup calls persist=%d retire=%d close=%d", persistCalls, retireCalls, closeCalls)
	}
}

func TestShutdownCancelsInFlightDrainBeforeBoundedCleanup(t *testing.T) {
	manifest := attachedworkerlocal.ManifestV1{Lifecycle: attachedworkerlocal.LifecycleActive, Revision: 7,
		UpdatedAt: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)}
	lease := newBlockingLifecycleLease(2)
	store := &fakeStore{lease: lease, snapshot: attachedworkerlocal.SnapshotV1{Manifest: manifest}}
	foreground, err := newForeground(store, Config{
		Now: func() time.Time { return manifest.UpdatedAt }, CleanupTimeout: time.Second,
	}, ActivationPorts{})
	if err != nil {
		t.Fatal(err)
	}
	_, session, err := foreground.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	drainResult := make(chan error, 1)
	go func() { drainResult <- session.Drain(context.Background()) }()
	select {
	case <-lease.entered:
	case <-time.After(time.Second):
		t.Fatal("drain did not enter its persist")
	}
	if err := session.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown error = %v", err)
	}
	select {
	case err := <-drainResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("in-flight Drain error = %v, want canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight drain was not canceled by shutdown")
	}
	persistCalls, retireCalls, closeCalls := lease.counts()
	if persistCalls != 4 || retireCalls != 1 || closeCalls != 1 {
		t.Fatalf("cleanup calls persist=%d retire=%d close=%d", persistCalls, retireCalls, closeCalls)
	}
}

func TestDisabledForegroundNeverCallsActivationPorts(t *testing.T) {
	manifest := attachedworkerlocal.ManifestV1{Lifecycle: attachedworkerlocal.LifecycleActive, Revision: 7,
		UpdatedAt: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)}
	lease := &fakeLease{}
	store := &fakeStore{lease: lease, snapshot: attachedworkerlocal.SnapshotV1{Manifest: manifest}}
	bootstrap := &bootstrapSpy{}
	source := &sourceSpy{}
	sink := &sinkSpy{}
	runner := &runnerSpy{}
	daemon := &daemonSpy{}
	foreground, err := newForeground(store, Config{Now: func() time.Time { return manifest.UpdatedAt }}, ActivationPorts{
		Bootstrap: bootstrap, Source: source, ResultSink: sink, Runner: runner, Daemon: daemon,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, runErr := foreground.Run(context.Background()); !errors.Is(runErr, ErrFeatureDisabled) {
		t.Fatalf("Run error = %v", runErr)
	}
	if bootstrap.calls != 0 || source.calls != 0 || sink.calls != 0 || runner.calls != 0 || daemon.calls != 0 {
		t.Fatalf("disabled shell called live ports: bootstrap=%d source=%d sink=%d runner=%d daemon=%d",
			bootstrap.calls, source.calls, sink.calls, runner.calls, daemon.calls)
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
			foreground, err := newForeground(store, Config{}, ActivationPorts{})
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
	foreground, err := newForeground(store, Config{Now: func() time.Time { return manifest.UpdatedAt }}, ActivationPorts{})
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
	foreground, err := newForeground(store, Config{Now: func() time.Time { return manifest.UpdatedAt }}, ActivationPorts{})
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

type blockingLifecycleLease struct {
	mu             sync.Mutex
	blockPersistAt int
	persistCalls   int
	retireCalls    int
	closeCalls     int
	entered        chan struct{}
	release        chan struct{}
	enteredOnce    sync.Once
}

func newBlockingLifecycleLease(blockPersistAt int) *blockingLifecycleLease {
	return &blockingLifecycleLease{
		blockPersistAt: blockPersistAt,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
}

func (lease *blockingLifecycleLease) PersistObservation(ctx context.Context, _ attachedworkerlocal.RuntimeObservationV1) error {
	lease.mu.Lock()
	lease.persistCalls++
	call := lease.persistCalls
	lease.mu.Unlock()
	if call != lease.blockPersistAt {
		return nil
	}
	lease.enteredOnce.Do(func() { close(lease.entered) })
	select {
	case <-lease.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (lease *blockingLifecycleLease) RetireObservation(context.Context, uint64) error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	lease.retireCalls++
	return nil
}

func (lease *blockingLifecycleLease) Close() error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	lease.closeCalls++
	return nil
}

func (lease *blockingLifecycleLease) counts() (int, int, int) {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return lease.persistCalls, lease.retireCalls, lease.closeCalls
}

type bootstrapSpy struct{ calls int }

func (spy *bootstrapSpy) IssueChallenge(context.Context, attachedworkerhttp.ChallengeRequestV1) (*attachedworkerhttp.ChallengeResponseV1, error) {
	spy.calls++
	return nil, errors.New("unexpected bootstrap challenge")
}

func (spy *bootstrapSpy) Activate(context.Context, attachedworkerhttp.ActivateClientInputV1) (*attachedworkerhttp.ActivateResponseV1, error) {
	spy.calls++
	return nil, errors.New("unexpected bootstrap activation")
}

type sourceSpy struct{ calls int }

func (spy *sourceSpy) Next(context.Context) (attachedworkerdaemon.Invocation, bool, error) {
	spy.calls++
	return attachedworkerdaemon.Invocation{}, false, errors.New("unexpected transport source")
}

type sinkSpy struct{ calls int }

func (spy *sinkSpy) Complete(context.Context, attachedworkerdaemon.InvocationIdentity, attachedworkerdaemon.InvocationResult, error) error {
	spy.calls++
	return errors.New("unexpected result sink")
}

type runnerSpy struct{ calls int }

func (spy *runnerSpy) Run(context.Context, attachedworkerdaemon.Invocation) (attachedworkerdaemon.InvocationResult, error) {
	spy.calls++
	return attachedworkerdaemon.InvocationResult{}, errors.New("unexpected invocation runner")
}

type daemonSpy struct{ calls int }

func (spy *daemonSpy) Run(context.Context) error {
	spy.calls++
	return errors.New("unexpected daemon run")
}

func (spy *daemonSpy) Drain(context.Context) error {
	spy.calls++
	return errors.New("unexpected daemon drain")
}

func (spy *daemonSpy) Shutdown(context.Context) error {
	spy.calls++
	return errors.New("unexpected daemon shutdown")
}

func (spy *daemonSpy) Status() attachedworkerdaemon.Status {
	spy.calls++
	return attachedworkerdaemon.Status{}
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
