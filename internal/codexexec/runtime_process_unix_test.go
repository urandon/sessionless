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
		process, processErr := recording.snapshot()
		t.Fatalf("Execute() result=%+v error=%v process=%+v process_error=%v", result, err, process, processErr)
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
	request, _, driver, launcher, _, recording := preparedDriverFixture(t, fixture)
	var clockMu sync.Mutex
	now := driverNow
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
		now = driverNow.Add(2 * time.Hour)
		clockMu.Unlock()
	}
	if _, err := driver.Execute(context.Background(), request, &fixtureEventSink{}); err == nil {
		t.Fatal("Execute() after authority expired before process preparation = nil")
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
	request, _, driver, _, authFile, recording := preparedDriverFixture(t, fixture)
	runCtx, stopRun := context.WithCancel(context.Background())
	t.Cleanup(stopRun)
	result := make(chan error, 1)
	go func() {
		_, err := driver.Execute(runCtx, request, &fixtureEventSink{})
		result <- err
	}()
	waitForPathOrResult(t, filepath.Join(filepath.Dir(authFile), "started"), result, recording, 5*time.Second)
	if err := driver.Cancel(context.Background(), executionIdentity(request)); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled Execute() error = nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled Execute() did not stop within 10s")
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
	config := Config{
		Enabled: true, Executable: executable, ExecutableVersion: "fixture-v1",
		ExecutableDigest: digest, Model: "gpt-fixture", Now: func() time.Time { return driverNow },
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
	result    ProcessResultV1
	err       error
}

func (boundary *recordingPreparedBoundary) Run(ctx context.Context, invocation ProcessInvocationV1) (ProcessResultV1, error) {
	if boundary.beforeRun != nil {
		boundary.beforeRun()
	}
	result, err := boundary.delegate.Run(ctx, invocation)
	boundary.mu.Lock()
	boundary.result, boundary.err = result, err
	boundary.mu.Unlock()
	return result, err
}

func (boundary *recordingPreparedBoundary) Cancel(ctx context.Context, authority AuthorityV1) error {
	return boundary.delegate.Cancel(ctx, authority)
}

func (boundary *recordingPreparedBoundary) snapshot() (ProcessResultV1, error) {
	boundary.mu.Lock()
	defer boundary.mu.Unlock()
	return boundary.result, boundary.err
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
			process, processErr := recording.snapshot()
			t.Fatalf("process exited before readiness: execute_error=%v process=%+v process_error=%v", err, process, processErr)
		case <-ticker.C:
		case <-deadline.C:
			process, processErr := recording.snapshot()
			t.Fatalf("process readiness marker %q missing after %s: process=%+v process_error=%v", path, timeout, process, processErr)
		}
	}
}
