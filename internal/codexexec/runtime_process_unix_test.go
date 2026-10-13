//go:build darwin || linux

package codexexec

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestDriverComposesAcceptedAuthorityWithPreparedSupervisorBoundary(t *testing.T) {
	fixture := writePreparedCodexFixture(t, `#!/bin/sh
[ -f "$CODEX_HOME/auth.json" ] || exit 81
[ "$HOME" != "$CODEX_HOME" ] || exit 82
[ -z "$OPENAI_API_KEY" ] || exit 83
while IFS= read -r line; do :; done
printf '%s' '{"rotated":true}' >"$CODEX_HOME/auth.json" || exit 85
printf '%s\n' '{"type":"thread.started","thread_id":"private"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"type":"agent_message","text":"prepared result"}}'
printf '%s\n' '{"type":"turn.completed"}'
`)
	request, authority, driver, launcher, authFile, recording := preparedDriverFixture(t, fixture)
	result, err := driver.Execute(context.Background(), request, &fixtureEventSink{})
	if err != nil || result.Summary != "prepared result" || result.ProviderEvidence == nil ||
		result.ProviderEvidence.FinishClass != domain.ProviderFinishCompletedV1 {
		t.Fatalf("Execute() result=%+v error=%v boundary_observation=%+v", result, err, recording.snapshot())
	}
	if content, err := os.ReadFile(authFile); err != nil || string(content) != `{"rotated":true}` {
		t.Fatalf("prepared credential after Execute = %q, %v", content, err)
	}
	launch := launcher.snapshot()
	if len(launch.WriteFiles) != 1 || launch.WriteFiles[0] != authFile ||
		!sameStrings(launch.Arguments, processArguments("gpt-fixture")) {
		t.Fatalf("launch authority = %+v", launch)
	}
	if resolved, err := driver.authority.ResolveExecution(context.Background(), executionIdentity(request)); err != nil || resolved != authority {
		t.Fatalf("resolved authority = (%+v, %v), want exact accepted authority", resolved, err)
	}
	if _, err := driver.Execute(context.Background(), request, &fixtureEventSink{}); err == nil {
		t.Fatal("replayed Execute() error = nil")
	}
	if got := launcher.prepareCount(); got != 1 {
		t.Fatalf("isolation preparations after replay = %d, want 1", got)
	}
}

func TestPreparedSupervisorBoundaryRechecksExpiryImmediatelyBeforePreparation(t *testing.T) {
	fixture := writePreparedCodexFixture(t, `#!/bin/sh
exit 91
`)
	request, authority, driver, launcher, _, recording := preparedDriverFixture(t, fixture)
	var clockMu sync.Mutex
	now := authority.LeaseExpiresAt.Add(-time.Hour)
	clock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	driver.now = clock
	prepared := recording.delegate.(*PreparedProcessBoundaryV1)
	prepared.now = clock
	recording.beforeRun = func() {
		clockMu.Lock()
		now = authority.LeaseExpiresAt.Add(time.Second)
		clockMu.Unlock()
	}
	if _, err := driver.Execute(context.Background(), request, &fixtureEventSink{}); err == nil {
		t.Fatal("Execute() after authority expired before process preparation = nil")
	}
	if got := launcher.prepareCount(); got != 0 {
		t.Fatalf("expired authority reached isolation preparation %d times", got)
	}
}

func TestPreparedSupervisorBoundaryPreservesCancellationOnFinalExpiryFailure(t *testing.T) {
	fixture := writePreparedCodexFixture(t, `#!/bin/sh
exit 91
`)
	request, authority, driver, launcher, _, recording := preparedDriverFixture(t, fixture)
	prepared := recording.delegate.(*PreparedProcessBoundaryV1)
	secondCheck := make(chan struct{})
	continueCheck := make(chan struct{})
	var callsMu sync.Mutex
	calls := 0
	prepared.now = func() time.Time {
		callsMu.Lock()
		calls++
		call := calls
		callsMu.Unlock()
		if call == 2 {
			close(secondCheck)
			<-continueCheck
			return authority.LeaseExpiresAt.Add(time.Second)
		}
		return authority.LeaseExpiresAt.Add(-time.Hour)
	}
	executed := make(chan error, 1)
	go func() {
		_, err := driver.Execute(context.Background(), request, &fixtureEventSink{})
		executed <- err
	}()
	select {
	case <-secondCheck:
	case <-time.After(5 * time.Second):
		t.Fatal("final authority check was not reached")
	}
	if err := driver.Cancel(context.Background(), executionIdentity(request)); err != nil {
		t.Fatalf("Cancel() during final authority check = %v", err)
	}
	close(continueCheck)
	select {
	case err := <-executed:
		if err == nil {
			t.Fatal("Execute() after cancellation and expiry error = nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Execute() did not return after final authority check")
	}
	observed := recording.snapshot()
	if !observed.Returned || !observed.Process.Cancelled || !errors.Is(observed.Err, context.Canceled) {
		t.Fatalf("early-exit arbitration observation=%+v", observed)
	}
	if got := launcher.prepareCount(); got != 0 {
		t.Fatalf("expired authority reached isolation preparation %d times", got)
	}
}

func TestPreparedSupervisorBoundaryArbitratesFinishAndCancel(t *testing.T) {
	_, authority := validDriverRequest(t)
	for iteration := 0; iteration < 200; iteration++ {
		_, cancel := context.WithCancel(context.Background())
		active := &activePreparedProcess{authority: authority, cancel: cancel}
		boundary := &PreparedProcessBoundaryV1{authority: authority, active: active, consumed: true}
		start := make(chan struct{})
		cancelled := make(chan error, 1)
		finished := make(chan bool, 1)
		go func() {
			<-start
			cancelled <- boundary.Cancel(context.Background(), authority)
		}()
		go func() {
			<-start
			finished <- boundary.finish(active)
		}()
		close(start)
		cancelErr, cancellationWon := <-cancelled, <-finished
		switch {
		case cancelErr == nil && cancellationWon:
		case errors.Is(cancelErr, ErrProcessNotActive) && !cancellationWon:
		default:
			t.Fatalf("iteration %d: cancel_error=%v finish_cancelled=%t", iteration, cancelErr, cancellationWon)
		}
	}
}

func TestPreparedSupervisorBoundaryRoutesExactCancellation(t *testing.T) {
	fixture := writePreparedCodexFixture(t, `#!/bin/sh
while IFS= read -r line; do :; done
printf '%s\n' '{"type":"thread.started","thread_id":"private"}'
printf '%s\n' '{"type":"turn.started"}'
: >"$CODEX_HOME/started"
while :; do :; done
`)
	request, authority, driver, launcher, authFile, recording := preparedDriverFixture(t, fixture)
	runCtx, stopRun := context.WithCancel(context.Background())
	result := make(chan error, 1)
	executeDone := make(chan struct{})
	// Registered after fixture TempDirs and BEFORE launch: one callback cancels
	// then joins, so even Fatal paths cannot skip the owned execution join.
	t.Cleanup(func() {
		if err := cancelAndJoinPreparedExecution(stopRun, executeDone, 10*time.Second); err != nil {
			t.Errorf("test-owned Execute cleanup not joined/quiescent: %v observation=%+v", err, recording.snapshot())
		}
	})
	var executeResult ports.ExecutionResult
	go func() {
		defer close(executeDone)
		var err error
		executeResult, err = driver.Execute(runCtx, request, &fixtureEventSink{})
		result <- err
	}()
	waitForPathOrResult(t, filepath.Join(filepath.Dir(authFile), "started"), result, recording, 5*time.Second)
	if err := driver.Cancel(context.Background(), executionIdentity(request)); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	returned := time.NewTimer(10 * time.Second)
	defer returned.Stop()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled Execute() error = nil")
		}
	case <-returned.C:
		t.Fatal("cancelled Execute() did not stop within 10s")
	}
	// Same original 10s window, not a second result/grace deadline. This channel
	// is independent of result, which readiness failures may already consume.
	select {
	case <-executeDone:
	case <-returned.C:
		// When both channels are already ready, select can choose the timer.
		// Recheck this SAME completion, without extending the original bound.
		select {
		case <-executeDone:
		default:
			t.Fatal("cancelled Execute goroutine did not join within original 10s window")
		}
	}
	observed := recording.snapshot()
	process := observed.Process
	if !observed.Returned || !process.Cancelled || !errors.Is(observed.Err, context.Canceled) ||
		errors.Is(observed.Err, context.DeadlineExceeded) || process.Deadline || process.OutputLimitExceeded ||
		!process.ProcessStopped || !process.DescendantsStopped || !process.CleanupSucceeded ||
		!process.PrivateStateRemoved || !process.CredentialStateQuiesced {
		t.Fatalf("exact cancellation lacks authentic joined teardown: observation=%+v", observed)
	}
	prepared := recording.delegate.(*PreparedProcessBoundaryV1)
	if process.original == nil || process.original.boundary != prepared || process.original.authority != authority ||
		!process.original.process.Cancelled || process.original.process.Deadline ||
		!process.original.process.DescendantsReaped || !process.original.process.BoundaryReleased ||
		!process.original.process.CleanupSucceeded ||
		(!process.original.process.TermSent && !process.original.process.KillSent) {
		t.Fatalf("exact cancellation original same-run witness missing: observation=%+v", observed)
	}
	if executeResult.ProviderEvidence == nil || executeResult.ProviderEvidence.FinishClass != domain.ProviderFinishCancelledV1 ||
		executeResult.ProviderEvidence.FailureCode != domain.ProviderExecutionFailureCancelledV1 {
		t.Fatalf("Execute cancellation evidence=%+v", executeResult.ProviderEvidence)
	}
	launch := launcher.snapshot()
	if _, err := os.Stat(filepath.Dir(launch.Directory)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("actual private attempt root remains after joined cancellation: %v", err)
	}
	if data, err := os.ReadFile(authFile); err != nil || string(data) != `{"fixture":true}` {
		t.Fatalf("public prepared credential changed after cancellation: bytes=%q error=%v", data, err)
	}
}

func preparedDriverFixture(
	t *testing.T,
	executable string,
) (ports.ExecutionRequest, AuthorityV1, *Driver, *fixtureIsolationLauncher, string, *recordingPreparedBoundary) {
	t.Helper()
	digest, err := attachedworkerdaemon.DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	credentialBase := canonicalTempDir(t)
	credentialRoot, err := os.MkdirTemp(credentialBase, "credential-")
	if err != nil {
		t.Fatal(err)
	}
	credentialRoot, err = filepath.EvalSymlinks(credentialRoot)
	if err != nil {
		t.Fatal(err)
	}
	authFile := filepath.Join(credentialRoot, "auth.json")
	if err := os.WriteFile(authFile, []byte(`{"fixture":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	launcher := &fixtureIsolationLauncher{}
	supervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{
		ScratchRoot: canonicalTempDir(t), Launcher: launcher,
		Timeout: 20 * time.Second, TerminationGrace: time.Second,
		MaxStdoutBytes: maxJSONLOutputBytes, MaxStderrBytes: maxProcessStderrBytesV1,
		AllowedEnvironmentNames: []string{CredentialHomeEnvironmentV1},
		AllowedReadRoots:        []string{credentialBase},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, authority := validDriverRequest(t)
	runtimeNow := time.Now().UTC()
	leaseExpiresAt := runtimeNow.Add(time.Hour)
	credentialExpiresAt := runtimeNow.Add(30 * time.Minute)
	authority.LeaseExpiresAt = leaseExpiresAt
	request.HarnessBinding.EvidenceExpiresAt = &leaseExpiresAt
	request.Credential.ExpiresAt = credentialExpiresAt
	config := Config{
		Enabled: true, Executable: executable, ExecutableVersion: "fixture-v1",
		ExecutableDigest: digest, Model: "gpt-fixture", Now: func() time.Time { return runtimeNow },
	}
	descriptor, err := prepareConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	request.HarnessBinding.Backend = descriptor
	request.CredentialMaterialization.RootDir = credentialRoot
	request.CredentialMaterialization.FilePath = authFile
	attempt := attemptForAuthority(authority, domain.AttachedWorkerAttemptClaimed)
	driver, err := NewPreparedAttachedWorkerDriverV1(config, attempt, authority.ProviderResource, supervisor)
	if err != nil {
		t.Fatal(err)
	}
	recording := &recordingPreparedBoundary{delegate: driver.boundary}
	driver.boundary = recording
	if err := request.Validate(); err != nil {
		t.Fatalf("prepared request invalid: %v", err)
	}
	return request, authority, driver, launcher, authFile, recording
}

type recordingPreparedBoundary struct {
	delegate  ProcessBoundaryV1
	beforeRun func()
	mu        sync.Mutex
	observed  preparedBoundaryObservation
}

// Returned describes delegate.Run completion, NOT preparation/process start.
// Result/error/completion are read atomically as this one canonical test tuple.
type preparedBoundaryObservation struct {
	Returned bool
	Process  ProcessResultV1
	Err      error
}

func (boundary *recordingPreparedBoundary) Run(ctx context.Context, invocation ProcessInvocationV1) (ProcessResultV1, error) {
	if boundary.beforeRun != nil {
		boundary.beforeRun()
	}
	result, err := boundary.delegate.Run(ctx, invocation)
	recorded := result
	recorded.Stdout = append([]byte(nil), result.Stdout...)
	boundary.mu.Lock()
	boundary.observed = preparedBoundaryObservation{Returned: true, Process: recorded, Err: err}
	boundary.mu.Unlock()
	return result, err
}

func (boundary *recordingPreparedBoundary) Cancel(ctx context.Context, authority AuthorityV1) error {
	return boundary.delegate.Cancel(ctx, authority)
}

func (boundary *recordingPreparedBoundary) snapshot() preparedBoundaryObservation {
	boundary.mu.Lock()
	defer boundary.mu.Unlock()
	observed := boundary.observed
	observed.Process.Stdout = append([]byte(nil), observed.Process.Stdout...)
	return observed
}

func writePreparedCodexFixture(t *testing.T, source string) string {
	t.Helper()
	path := filepath.Join(canonicalTempDir(t), "codex-fixture")
	if err := os.WriteFile(path, []byte(source), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func waitForPathOrResult(t *testing.T, path string, result <-chan error, recording *recordingPreparedBoundary, timeout time.Duration) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("observe process readiness %q: %v", path, err)
		}
		select {
		case err := <-result:
			t.Fatalf("Execute returned before readiness: execute_error=%v boundary_observation=%+v", err, recording.snapshot())
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("process readiness marker %q missing after %s: boundary_observation=%+v", path, timeout, recording.snapshot())
		}
	}
}
