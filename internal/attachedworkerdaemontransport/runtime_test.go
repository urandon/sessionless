package attachedworkerdaemontransport

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestForegroundRuntimeComposesRemoteDrainAfterActiveTerminal(t *testing.T) {
	source := newRuntimeSource()
	runner := &runtimeRunner{started: make(chan struct{}, 1), release: make(chan struct{})}
	sink := &runtimeSink{completed: make(chan struct{}, 1)}
	watcher := &runtimeWatcher{started: make(chan struct{}, 1), control: ActiveControlDraining}
	closer := &runtimeCloser{}
	runtime, err := newForegroundRuntime(source, sink, watcher, closer, runner, RuntimeConfig{
		Daemon: attachedworkerdaemon.DaemonConfig{IdleBackoff: time.Second}, CleanupTimeout: time.Second,
		ActiveControlInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	first := runtimeInvocation("attempt-active-drain")
	source.invocations <- first
	source.invocations <- runtimeInvocation("attempt-must-not-start")
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(context.Background()) }()
	<-runner.started
	<-watcher.started
	waitForRuntimeState(t, runtime, attachedworkerdaemon.DaemonDraining)
	if err := runtime.Wake(); err != nil {
		t.Fatalf("wake: %v", err)
	}
	close(runner.release)
	select {
	case <-sink.completed:
	case <-time.After(time.Second):
		t.Fatal("terminal result was not reported")
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
	if closer.calls != 1 || source.wakes != 1 || runner.calls != 1 || sink.calls != 1 {
		t.Fatalf("close=%d wakes=%d runs=%d completes=%d", closer.calls, source.wakes, runner.calls, sink.calls)
	}
	if sink.identity != first.Identity {
		t.Fatalf("completed identity=%+v want=%+v", sink.identity, first.Identity)
	}
	if status := runtime.Status(); status.State != attachedworkerdaemon.DaemonStopped || status.Active || status.Completed != 1 {
		t.Fatalf("status=%+v", status)
	}
	select {
	case <-runner.started:
		t.Fatal("remote drain admitted a second invocation")
	default:
	}
	if err := runtime.Run(context.Background()); !errors.Is(err, ErrRuntimeAlreadyUsed) {
		t.Fatalf("second run error=%v", err)
	}
}

func TestForegroundRuntimeComposesExactRemoteCancellation(t *testing.T) {
	source := newRuntimeSource()
	runner := &runtimeRunner{started: make(chan struct{}, 1), release: make(chan struct{})}
	sink := &runtimeSink{completed: make(chan struct{}, 1)}
	watcher := &runtimeWatcher{started: make(chan struct{}, 1), control: ActiveControlCancelled}
	closer := &runtimeCloser{}
	runtime, err := newForegroundRuntime(source, sink, watcher, closer, runner, RuntimeConfig{
		CleanupTimeout: time.Second, ActiveControlInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	invocation := runtimeInvocation("attempt-active-cancel")
	source.invocations <- invocation
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(context.Background()) }()
	<-runner.started
	<-watcher.started
	select {
	case <-sink.completed:
	case <-time.After(time.Second):
		t.Fatal("cancelled result was not reported")
	}
	if err := runtime.Drain(context.Background()); err != nil {
		t.Fatalf("drain after cancellation: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
	if !sink.result.Process.Cancelled || sink.identity != invocation.Identity || watcher.identity != invocation.Identity {
		t.Fatalf("sink=%+v watcher_identity=%+v", sink.result, watcher.identity)
	}
	if closer.calls != 1 {
		t.Fatalf("close calls=%d", closer.calls)
	}
}

func TestForegroundRuntimeFailsClosedOnActiveControlAmbiguity(t *testing.T) {
	source := newRuntimeSource()
	runner := &runtimeRunner{started: make(chan struct{}, 1), release: make(chan struct{})}
	sink := &runtimeSink{completed: make(chan struct{}, 1)}
	watcher := &runtimeWatcher{started: make(chan struct{}, 1), err: errors.New("ambiguous control exchange")}
	closer := &runtimeCloser{}
	runtime, err := newForegroundRuntime(source, sink, watcher, closer, runner, RuntimeConfig{
		CleanupTimeout: time.Second, ActiveControlInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	source.invocations <- runtimeInvocation("attempt-ambiguous-control")
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(context.Background()) }()
	<-runner.started
	<-watcher.started
	err = <-runDone
	if !errors.Is(err, ErrReconciliationRequired) || !errors.Is(err, watcher.err) {
		t.Fatalf("run error=%v", err)
	}
	if !sink.result.Process.Cancelled || sink.calls != 1 || closer.calls != 1 {
		t.Fatalf("result=%+v sink_calls=%d close_calls=%d", sink.result, sink.calls, closer.calls)
	}
}

func TestForegroundRuntimeTurnsIdleRemoteDrainIntoGracefulStop(t *testing.T) {
	source := newRuntimeSource()
	source.errs <- ErrDrainRequested
	runner := &runtimeRunner{started: make(chan struct{}, 1), release: make(chan struct{})}
	sink := &runtimeSink{completed: make(chan struct{}, 1)}
	watcher := &runtimeWatcher{started: make(chan struct{}, 1)}
	closer := &runtimeCloser{}
	runtime, err := newForegroundRuntime(source, sink, watcher, closer, runner, RuntimeConfig{
		CleanupTimeout: time.Second, ActiveControlInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if source.calls != 1 || runner.calls != 0 || sink.calls != 0 || closer.calls != 1 {
		t.Fatalf("source=%d runner=%d sink=%d close=%d", source.calls, runner.calls, sink.calls, closer.calls)
	}
}

type runtimeSource struct {
	invocations chan attachedworkerdaemon.Invocation
	errs        chan error
	mu          sync.Mutex
	calls       int
	wakes       int
}

func newRuntimeSource() *runtimeSource {
	return &runtimeSource{
		invocations: make(chan attachedworkerdaemon.Invocation, 2),
		errs:        make(chan error, 1),
	}
}

func (source *runtimeSource) Next(ctx context.Context) (attachedworkerdaemon.Invocation, bool, error) {
	source.mu.Lock()
	source.calls++
	source.mu.Unlock()
	select {
	case err := <-source.errs:
		return attachedworkerdaemon.Invocation{}, false, err
	case invocation := <-source.invocations:
		return invocation, true, nil
	case <-ctx.Done():
		return attachedworkerdaemon.Invocation{}, false, ctx.Err()
	}
}

func (source *runtimeSource) Wake() error {
	source.mu.Lock()
	source.wakes++
	source.mu.Unlock()
	return nil
}

type runtimeRunner struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   int
}

func (runner *runtimeRunner) Run(ctx context.Context, _ attachedworkerdaemon.Invocation) (attachedworkerdaemon.InvocationResult, error) {
	runner.mu.Lock()
	runner.calls++
	runner.mu.Unlock()
	select {
	case runner.started <- struct{}{}:
	default:
	}
	select {
	case <-runner.release:
		return runtimeSuccessfulResult(false), nil
	case <-ctx.Done():
		return runtimeSuccessfulResult(true), nil
	}
}

type runtimeSink struct {
	completed chan struct{}
	mu        sync.Mutex
	calls     int
	identity  attachedworkerdaemon.InvocationIdentity
	result    attachedworkerdaemon.InvocationResult
}

func (sink *runtimeSink) Complete(
	_ context.Context,
	identity attachedworkerdaemon.InvocationIdentity,
	result attachedworkerdaemon.InvocationResult,
	_ error,
) error {
	sink.mu.Lock()
	sink.calls++
	sink.identity = identity
	sink.result = result
	sink.mu.Unlock()
	select {
	case sink.completed <- struct{}{}:
	default:
	}
	return nil
}

type runtimeWatcher struct {
	started  chan struct{}
	control  ActiveControl
	err      error
	identity attachedworkerdaemon.InvocationIdentity
}

func (watcher *runtimeWatcher) WatchActiveControl(
	ctx context.Context,
	identity attachedworkerdaemon.InvocationIdentity,
	target ActiveAttemptController,
	_ time.Duration,
) (ActiveControl, error) {
	watcher.identity = identity
	select {
	case watcher.started <- struct{}{}:
	default:
	}
	if watcher.err != nil {
		return "", watcher.err
	}
	switch watcher.control {
	case ActiveControlCancelled:
		return watcher.control, target.CancelActive(ctx, identity)
	case ActiveControlDraining:
		return watcher.control, target.RequestDrain(ctx)
	default:
		<-ctx.Done()
		return "", ctx.Err()
	}
}

type runtimeCloser struct {
	calls int
	err   error
}

func (closer *runtimeCloser) Close(context.Context) error {
	closer.calls++
	return closer.err
}

func runtimeInvocation(attempt string) attachedworkerdaemon.Invocation {
	digest := attachedworkerdaemon.ExecutableDigest(sha256.Sum256([]byte("runtime fixture")))
	return attachedworkerdaemon.Invocation{
		Identity: attachedworkerdaemon.InvocationIdentity{
			TenantID: "tenant-001", OwnerUserID: "user-001", WorkerID: "worker-001",
			RunID: "run-001", AttemptID: domain.AttemptID(attempt),
			LeaseID: "lease-001", FenceToken: 7,
		},
		Process: attachedworkerdaemon.AttemptSpec{Executable: "/fixture/harness", ExecutableDigest: digest},
	}
}

func runtimeSuccessfulResult(cancelled bool) attachedworkerdaemon.InvocationResult {
	return attachedworkerdaemon.InvocationResult{Process: attachedworkerdaemon.AttemptResult{
		ExitCode: 0, Cancelled: cancelled, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true,
	}}
}

func waitForRuntimeState(t *testing.T, runtime *ForegroundRuntime, want attachedworkerdaemon.DaemonState) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if runtime.Status().State == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("runtime state=%s want=%s status=%+v", runtime.Status().State, want, runtime.Status())
		case <-ticker.C:
		}
	}
}
