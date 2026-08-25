package attachedworkerdaemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type captureProcessRunner struct {
	started chan struct{}
	mu      sync.Mutex
	spec    AttemptSpec
}

func (runner *captureProcessRunner) Run(ctx context.Context, spec AttemptSpec) (AttemptResult, error) {
	runner.mu.Lock()
	runner.spec = cloneAttemptSpec(spec)
	runner.mu.Unlock()
	close(runner.started)
	<-ctx.Done()
	return AttemptResult{Cancelled: true, DescendantsReaped: true, CleanupSucceeded: true}, nil
}

func (runner *captureProcessRunner) captured() AttemptSpec {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return cloneAttemptSpec(runner.spec)
}

type fakeCredentialLifecycle struct {
	mu              sync.Mutex
	root            string
	base            string
	order           []string
	writeBackActive bool
	releaseActive   bool
	writeBackErr    error
	releaseErr      error
	waitWriteBack   bool
}

func (lifecycle *fakeCredentialLifecycle) Issue(context.Context, ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	lifecycle.record("issue")
	return ports.CredentialHandle{
		HandleID: "handle-a", TenantID: "tenant-a", SubscriptionConnectionID: "connection-a",
		OwnerUserID: "user-a", RunID: "run-a", AttemptID: "attempt-a", WorkerID: "worker-a",
		LeaseID: "lease-a", LeaseFence: 9, BindingGeneration: 1, ExpiresAt: time.Now().Add(time.Hour),
	}, nil
}

func (lifecycle *fakeCredentialLifecycle) Materialize(context.Context, ports.CredentialHandle) (ports.CredentialMaterialization, error) {
	lifecycle.record("materialize")
	base := lifecycle.base
	if base == "" {
		base = "/private/tmp"
	}
	root, err := os.MkdirTemp(base, "sessionless-aw-credential-")
	if err != nil {
		return ports.CredentialMaterialization{}, err
	}
	lifecycle.root = root
	auth := filepath.Join(root, "auth.json")
	if err := os.WriteFile(auth, []byte("{}"), 0o600); err != nil {
		return ports.CredentialMaterialization{}, err
	}
	return ports.CredentialMaterialization{RootDir: root, AuthFile: auth}, nil
}

func (lifecycle *fakeCredentialLifecycle) WriteBack(ctx context.Context, _ ports.CredentialHandle, _ ports.CredentialMaterialization) (ports.CredentialWriteBackResult, error) {
	var waitedErr error
	if lifecycle.waitWriteBack {
		<-ctx.Done()
		waitedErr = ctx.Err()
	}
	lifecycle.mu.Lock()
	lifecycle.order = append(lifecycle.order, "writeback")
	lifecycle.writeBackActive = ctx.Err() == nil
	err := lifecycle.writeBackErr
	if err == nil {
		err = waitedErr
	}
	lifecycle.mu.Unlock()
	return ports.CredentialWriteBackResult{Changed: true, Generation: 2}, err
}

func TestInvocationRunnerReleaseGetsFreshBoundAfterWriteBackUsesItsGrace(t *testing.T) {
	process := &immediateProcessRunner{}
	credentials := &fakeCredentialLifecycle{base: credentialFixtureBase(t), waitWriteBack: true}
	runner, err := NewInvocationRunner(
		InvocationRunnerConfig{CredentialFinalizeGrace: 20 * time.Millisecond},
		process,
		credentials,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), validCredentialInvocation(t))
	if !errors.Is(err, ErrCredentialFinalization) || result.FailureCode != "credential_writeback_failed" {
		t.Fatalf("unexpected finalization result: result=%+v err=%v", result, err)
	}
	credentials.mu.Lock()
	defer credentials.mu.Unlock()
	if !credentials.releaseActive || !equalStrings(credentials.order, []string{"issue", "materialize", "writeback", "release"}) {
		t.Fatalf("release did not receive a fresh bound: order=%#v active=%v", credentials.order, credentials.releaseActive)
	}
}

func (lifecycle *fakeCredentialLifecycle) Release(ctx context.Context, _ ports.CredentialHandle) error {
	lifecycle.mu.Lock()
	lifecycle.order = append(lifecycle.order, "release")
	lifecycle.releaseActive = ctx.Err() == nil
	err := lifecycle.releaseErr
	root := lifecycle.root
	lifecycle.mu.Unlock()
	_ = os.RemoveAll(root)
	return err
}

func (*fakeCredentialLifecycle) RevokeConnection(context.Context, ports.CredentialRevokeRequest) error {
	return nil
}

func (lifecycle *fakeCredentialLifecycle) record(value string) {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	lifecycle.order = append(lifecycle.order, value)
}

func TestInvocationRunnerFinalizesCredentialAfterCancellation(t *testing.T) {
	process := &captureProcessRunner{started: make(chan struct{})}
	credentials := &fakeCredentialLifecycle{base: credentialFixtureBase(t)}
	runner, err := NewInvocationRunner(InvocationRunnerConfig{CredentialFinalizeGrace: time.Second}, process, credentials)
	if err != nil {
		t.Fatalf("new invocation runner: %v", err)
	}
	invocation := validCredentialInvocation(t)
	ctx, cancel := context.WithCancel(context.Background())
	resultChannel := make(chan InvocationResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		result, runErr := runner.Run(ctx, invocation)
		resultChannel <- result
		errorChannel <- runErr
	}()
	select {
	case <-process.started:
	case <-time.After(2 * time.Second):
		t.Fatal("process did not start")
	}
	cancel()
	var result InvocationResult
	select {
	case result = <-resultChannel:
	case <-time.After(2 * time.Second):
		t.Fatal("invocation did not finish after cancellation")
	}
	if err := <-errorChannel; err != nil {
		t.Fatalf("run invocation: %v", err)
	}
	if !result.Process.Cancelled || !result.CredentialChanged || result.CredentialGeneration != 2 || result.FailureCode != "" {
		t.Fatalf("unexpected invocation result: %+v", result)
	}
	credentials.mu.Lock()
	defer credentials.mu.Unlock()
	wantOrder := []string{"issue", "materialize", "writeback", "release"}
	if !equalStrings(credentials.order, wantOrder) || !credentials.writeBackActive || !credentials.releaseActive {
		t.Fatalf("credential finalization order/contexts = %#v, writeback=%v release=%v", credentials.order, credentials.writeBackActive, credentials.releaseActive)
	}
	processSpec := process.captured()
	if len(processSpec.AdditionalReadRoots) != 1 || processSpec.AdditionalReadRoots[0] != credentials.root ||
		len(processSpec.Environment) != 1 || processSpec.Environment[0].Name != "CODEX_HOME" ||
		processSpec.Environment[0].Value != credentials.root {
		t.Fatalf("credential was not scoped into the process spec: %+v", processSpec)
	}
	if _, err := os.Stat(credentials.root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("credential root survived release: %v", err)
	}
}

func TestInvocationRunnerReleaseStillRunsAfterWriteBackFailure(t *testing.T) {
	process := &immediateProcessRunner{}
	credentials := &fakeCredentialLifecycle{
		base: credentialFixtureBase(t), writeBackErr: errors.New("private backend detail"),
	}
	runner, err := NewInvocationRunner(InvocationRunnerConfig{}, process, credentials)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(context.Background(), validCredentialInvocation(t))
	if !errors.Is(err, ErrCredentialFinalization) || result.FailureCode != "credential_writeback_failed" {
		t.Fatalf("unexpected finalization failure: result=%+v err=%v", result, err)
	}
	credentials.mu.Lock()
	defer credentials.mu.Unlock()
	if !equalStrings(credentials.order, []string{"issue", "materialize", "writeback", "release"}) {
		t.Fatalf("release did not follow writeback failure: %#v", credentials.order)
	}
}

func credentialFixtureBase(t *testing.T) string {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize credential fixture base: %v", err)
	}
	return base
}

type immediateProcessRunner struct{}

func (*immediateProcessRunner) Run(context.Context, AttemptSpec) (AttemptResult, error) {
	return AttemptResult{ExitCode: 0, DescendantsReaped: true, CleanupSucceeded: true}, nil
}

func validCredentialInvocation(t *testing.T) Invocation {
	t.Helper()
	started := time.Now().UTC().Add(-time.Minute)
	run := domain.Run{
		ID: "run-a", TenantID: "tenant-a", SessionID: "session-a", TriggerEventID: "event-a",
		SubscriptionConnectionID: "connection-a", Status: domain.RunRunning,
		IdempotencyKey: "run-key-a", StartedAt: &started, CreatedAt: started, UpdatedAt: started,
	}
	attempt := domain.Attempt{
		ID: "attempt-a", TenantID: "tenant-a", RunID: "run-a", Number: 1,
		Status: domain.AttemptRunning, WorkerID: "worker-a", CreatedAt: started, UpdatedAt: started,
	}
	lease := domain.Lease{
		ID: "lease-a", TenantID: "tenant-a", RunID: "run-a", AttemptID: "attempt-a",
		WorkerID: "worker-a", FenceToken: 9, AcquiredAt: started, ExpiresAt: started.Add(time.Hour),
	}
	digest := ExecutableDigest{1}
	return Invocation{
		Identity: InvocationIdentity{
			TenantID: "tenant-a", OwnerUserID: "user-a", WorkerID: "worker-a", RunID: "run-a",
			AttemptID: "attempt-a", LeaseID: "lease-a", FenceToken: 9,
		},
		Process: AttemptSpec{Executable: "/private/tmp/fixture", ExecutableDigest: digest},
		Credential: &CredentialInvocation{
			IssueRequest: ports.CredentialIssueRequest{
				OwnerUserID: "user-a", Run: run, Attempt: attempt, Lease: lease,
				ExpiresAt: started.Add(30 * time.Minute),
			},
			HomeEnvironment: "CODEX_HOME",
		},
	}
}
