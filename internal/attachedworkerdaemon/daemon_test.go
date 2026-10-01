package attachedworkerdaemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type channelSource struct {
	invocations chan Invocation
	calls       chan struct{}
}

func (source *channelSource) Next(ctx context.Context) (Invocation, bool, error) {
	select {
	case source.calls <- struct{}{}:
	default:
	}
	select {
	case invocation := <-source.invocations:
		return invocation, true, nil
	case <-ctx.Done():
		return Invocation{}, false, ctx.Err()
	}
}

type blockingRunner struct {
	started chan InvocationIdentity
	release chan struct{}
	mu      sync.Mutex
	active  int
	peak    int
}

type cancelBarrierRunner struct {
	started   chan InvocationIdentity
	cancelled chan struct{}
	release   chan struct{}
}

func (runner *cancelBarrierRunner) Run(ctx context.Context, invocation Invocation) (InvocationResult, error) {
	runner.started <- invocation.Identity
	<-ctx.Done()
	close(runner.cancelled)
	<-runner.release
	return InvocationResult{Process: AttemptResult{
		Cancelled: true, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true,
	}}, nil
}

func (runner *blockingRunner) Run(ctx context.Context, invocation Invocation) (InvocationResult, error) {
	runner.mu.Lock()
	runner.active++
	if runner.active > runner.peak {
		runner.peak = runner.active
	}
	runner.mu.Unlock()
	runner.started <- invocation.Identity
	select {
	case <-runner.release:
	case <-ctx.Done():
	}
	runner.mu.Lock()
	runner.active--
	runner.mu.Unlock()
	return InvocationResult{Process: AttemptResult{Cancelled: ctx.Err() != nil, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}}, nil
}

type recordingSink struct {
	mu      sync.Mutex
	results []InvocationResult
	done    chan struct{}
}

func (sink *recordingSink) Complete(_ context.Context, _ InvocationIdentity, result InvocationResult, _ error) error {
	sink.mu.Lock()
	sink.results = append(sink.results, result)
	sink.mu.Unlock()
	select {
	case sink.done <- struct{}{}:
	default:
	}
	return nil
}

func TestDaemonDrainLetsActiveAttemptFinishAndStopsAdmission(t *testing.T) {
	source := &channelSource{invocations: make(chan Invocation, 2), calls: make(chan struct{}, 8)}
	runner := &blockingRunner{started: make(chan InvocationIdentity, 2), release: make(chan struct{}, 2)}
	sink := &recordingSink{done: make(chan struct{}, 2)}
	daemon, err := NewDaemon(DaemonConfig{IdleBackoff: time.Second}, source, runner, sink)
	if err != nil {
		t.Fatal(err)
	}
	first := validDaemonInvocation("attempt-a")
	second := validDaemonInvocation("attempt-b")
	source.invocations <- first
	source.invocations <- second
	runDone := make(chan error, 1)
	go func() { runDone <- daemon.Run(context.Background()) }()
	<-runner.started
	drainDone := make(chan error, 1)
	go func() { drainDone <- daemon.Drain(context.Background()) }()
	waitForDaemonState(t, daemon, DaemonDraining)
	status := daemon.Status()
	if !status.Active || status.ActiveAttempt.AttemptID != first.Identity.AttemptID {
		t.Fatalf("draining status lost active attempt: %+v", status)
	}
	runner.release <- struct{}{}
	if err := <-drainDone; err != nil {
		t.Fatalf("drain: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run: %v", err)
	}
	status = daemon.Status()
	if status.State != DaemonStopped || status.Accepted != 1 || status.Completed != 1 || status.Committed != 1 || status.Failed != 0 || status.Active {
		t.Fatalf("unexpected stopped status: %+v", status)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.peak != 1 {
		t.Fatalf("daemon exceeded concurrency one: %d", runner.peak)
	}
}

type failingObservationRunner struct{ err error }

func (runner failingObservationRunner) Run(context.Context, Invocation) (InvocationResult, error) {
	return InvocationResult{FailureCode: "synthetic_failed"}, runner.err
}

func TestDaemonStatusCountsFailedAttemptWithoutInventingSuccess(t *testing.T) {
	source := &channelSource{invocations: make(chan Invocation, 1), calls: make(chan struct{}, 1)}
	sink := &recordingSink{done: make(chan struct{}, 1)}
	runFailure := errors.New("synthetic runner failure")
	daemon, err := NewDaemon(DaemonConfig{}, source, failingObservationRunner{err: runFailure}, sink)
	if err != nil {
		t.Fatal(err)
	}
	source.invocations <- validDaemonInvocation("attempt-failed-observation")
	runCtx, cancelRun := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRun()
	if err := daemon.Run(runCtx); !errors.Is(err, runFailure) {
		t.Fatalf("run failure=%v", err)
	}
	status := daemon.Status()
	if status.State != DaemonStopped || status.Accepted != 1 || status.Completed != 1 || status.Committed != 0 ||
		status.Failed != 1 || status.LastFailureCode != "synthetic_failed" {
		t.Fatalf("failed status=%+v", status)
	}
	select {
	case update := <-daemon.Updates():
		if update != status {
			t.Fatalf("latest coalesced update=%+v want=%+v", update, status)
		}
	default:
		t.Fatal("stopped status update is missing")
	}
}

type processObservationRunner struct{ result InvocationResult }

func (runner processObservationRunner) Run(context.Context, Invocation) (InvocationResult, error) {
	return runner.result, nil
}

type failingTerminalSink struct{ err error }

func (sink failingTerminalSink) Complete(context.Context, InvocationIdentity, InvocationResult, error) error {
	return sink.err
}

func TestDaemonStatusCountsProcessFailureAsFailedTerminal(t *testing.T) {
	source := &channelSource{invocations: make(chan Invocation, 1), calls: make(chan struct{}, 1)}
	result := InvocationResult{Process: AttemptResult{ExitCode: 42, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}}
	sink := &recordingSink{done: make(chan struct{}, 1)}
	daemon, err := NewDaemon(DaemonConfig{}, source, processObservationRunner{result: result}, sink)
	if err != nil {
		t.Fatal(err)
	}
	source.invocations <- validDaemonInvocation("attempt-process-failed")
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	runStopped := make(chan struct{})
	go func() {
		defer close(runStopped)
		runDone <- daemon.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancelRun()
		select {
		case <-runStopped:
		case <-time.After(time.Second):
			t.Error("daemon did not stop during cleanup")
		}
	})
	select {
	case <-sink.done:
	case <-time.After(time.Second):
		t.Fatal("process terminal was not reported")
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := daemon.Drain(drainCtx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("daemon did not stop after drain")
	}
	status := daemon.Status()
	if status.Accepted != 1 || status.Completed != 1 || status.Committed != 0 || status.Failed != 1 ||
		status.LastFailureCode != "invocation_process_failed" {
		t.Fatalf("process failure status=%+v", status)
	}
}

func TestDaemonStatusDoesNotCommitUnacknowledgedTerminal(t *testing.T) {
	source := &channelSource{invocations: make(chan Invocation, 1), calls: make(chan struct{}, 1)}
	terminalErr := errors.New("ambiguous terminal acknowledgement")
	result := InvocationResult{Process: AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}}
	daemon, err := NewDaemon(DaemonConfig{}, source, processObservationRunner{result: result}, failingTerminalSink{err: terminalErr})
	if err != nil {
		t.Fatal(err)
	}
	source.invocations <- validDaemonInvocation("attempt-terminal-unconfirmed")
	runCtx, cancelRun := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRun()
	if err := daemon.Run(runCtx); !errors.Is(err, terminalErr) {
		t.Fatalf("terminal error=%v", err)
	}
	status := daemon.Status()
	if status.Accepted != 1 || status.Completed != 1 || status.Committed != 0 || status.Failed != 0 ||
		status.LastFailureCode != "terminal_unconfirmed" {
		t.Fatalf("unconfirmed status=%+v", status)
	}
}

func TestDaemonRequestDrainClosesAdmissionWithoutWaitingForActiveAttempt(t *testing.T) {
	source := &channelSource{invocations: make(chan Invocation, 2), calls: make(chan struct{}, 8)}
	runner := &blockingRunner{started: make(chan InvocationIdentity, 2), release: make(chan struct{}, 1)}
	sink := &recordingSink{done: make(chan struct{}, 1)}
	daemon, err := NewDaemon(DaemonConfig{IdleBackoff: time.Second}, source, runner, sink)
	if err != nil {
		t.Fatal(err)
	}
	first := validDaemonInvocation("attempt-drain-active")
	source.invocations <- first
	source.invocations <- validDaemonInvocation("attempt-must-not-start")
	runDone := make(chan error, 1)
	go func() { runDone <- daemon.Run(context.Background()) }()
	if started := <-runner.started; started != first.Identity {
		t.Fatalf("started identity=%+v want=%+v", started, first.Identity)
	}

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	if err := daemon.RequestDrain(requestCtx); err != nil {
		cancelRequest()
		t.Fatalf("request drain: %v", err)
	}
	cancelRequest()
	status := daemon.Status()
	if status.State != DaemonDraining || !status.Active || status.ActiveAttempt != first.Identity {
		t.Fatalf("request-drain status=%+v", status)
	}
	select {
	case started := <-runner.started:
		t.Fatalf("request drain admitted another attempt: %+v", started)
	default:
	}

	runner.release <- struct{}{}
	if err := <-runDone; err != nil {
		t.Fatalf("run after request drain: %v", err)
	}
	if status := daemon.Status(); status.State != DaemonStopped || status.Active || status.Completed != 1 {
		t.Fatalf("stopped status=%+v", status)
	}
	if err := daemon.RequestDrain(context.Background()); !errors.Is(err, ErrDaemonNotRunning) {
		t.Fatalf("request drain after stop error=%v", err)
	}
}

func TestDaemonShutdownCancelsActiveAttemptWithinBound(t *testing.T) {
	source := &channelSource{invocations: make(chan Invocation, 1), calls: make(chan struct{}, 8)}
	runner := &blockingRunner{started: make(chan InvocationIdentity, 1), release: make(chan struct{})}
	sink := &recordingSink{done: make(chan struct{}, 1)}
	daemon, err := NewDaemon(DaemonConfig{ShutdownGrace: time.Second}, source, runner, sink)
	if err != nil {
		t.Fatal(err)
	}
	source.invocations <- validDaemonInvocation("attempt-a")
	runDone := make(chan error, 1)
	go func() { runDone <- daemon.Run(context.Background()) }()
	<-runner.started
	if err := daemon.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("run after shutdown: %v", err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.results) != 1 || !sink.results[0].Process.Cancelled {
		t.Fatalf("shutdown did not report cancelled attempt: %#v", sink.results)
	}
}

func TestDaemonCancelActiveRequiresExactIdentityAndIsIdempotent(t *testing.T) {
	source := &channelSource{invocations: make(chan Invocation, 1), calls: make(chan struct{}, 8)}
	runner := &cancelBarrierRunner{
		started: make(chan InvocationIdentity, 1), cancelled: make(chan struct{}), release: make(chan struct{}),
	}
	sink := &recordingSink{done: make(chan struct{}, 1)}
	daemon, err := NewDaemon(DaemonConfig{ShutdownGrace: time.Second}, source, runner, sink)
	if err != nil {
		t.Fatal(err)
	}
	invocation := validDaemonInvocation("attempt-exact")
	source.invocations <- invocation
	runDone := make(chan error, 1)
	go func() { runDone <- daemon.Run(context.Background()) }()
	if started := <-runner.started; started != invocation.Identity {
		t.Fatalf("started identity=%+v want=%+v", started, invocation.Identity)
	}
	mismatch := invocation.Identity
	mismatch.AttemptID = "attempt-replacement"
	if err := daemon.CancelActive(context.Background(), mismatch); !errors.Is(err, ErrActiveAttemptMismatch) {
		t.Fatalf("mismatched cancel error=%v", err)
	}
	select {
	case <-runner.cancelled:
		t.Fatal("mismatched identity cancelled the active invocation")
	default:
	}
	if err := daemon.CancelActive(context.Background(), invocation.Identity); err != nil {
		t.Fatalf("exact cancel error=%v", err)
	}
	<-runner.cancelled
	if err := daemon.CancelActive(context.Background(), invocation.Identity); err != nil {
		t.Fatalf("exact replay error=%v", err)
	}
	close(runner.release)
	<-sink.done
	if err := daemon.CancelActive(context.Background(), invocation.Identity); !errors.Is(err, ErrActiveAttemptMissing) {
		t.Fatalf("completed cancel error=%v", err)
	}
	if err := daemon.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}

func TestDaemonShutdownCancellationSurvivesPreInstallRace(t *testing.T) {
	identity := validDaemonInvocation("attempt-shutdown-race").Identity
	daemon := &Daemon{state: DaemonRunning, active: true, activeID: identity}
	if !daemon.startDrain(true) {
		t.Fatal("shutdown drain was not started")
	}
	cancelled := make(chan struct{})
	daemon.setActiveCancel(func() { close(cancelled) })
	select {
	case <-cancelled:
	default:
		t.Fatal("late-installed active cancel did not inherit shutdown")
	}
	if err := daemon.CancelActive(context.Background(), identity); err != nil {
		t.Fatalf("exact replay after shutdown error=%v", err)
	}
}

func TestDaemonPollCancellationSurvivesDrainBeforeInstall(t *testing.T) {
	daemon := &Daemon{state: DaemonDraining}
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	daemon.setPollCancel(cancelPoll)
	select {
	case <-pollCtx.Done():
	default:
		t.Fatal("poll cancellation installed after drain was not cancelled")
	}
}

func TestDaemonCancelActiveRejectsIdleAndInvalidIdentity(t *testing.T) {
	daemon, err := NewDaemon(DaemonConfig{}, &channelSource{}, &blockingRunner{}, &recordingSink{})
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.CancelActive(context.Background(), validDaemonInvocation("attempt-idle").Identity); !errors.Is(err, ErrActiveAttemptMissing) {
		t.Fatalf("idle cancel error=%v", err)
	}
	if err := daemon.CancelActive(context.Background(), InvocationIdentity{}); !errors.Is(err, ErrInvocationInvalid) {
		t.Fatalf("invalid cancel error=%v", err)
	}
}

func TestDaemonRejectsConcurrentRun(t *testing.T) {
	source := &channelSource{invocations: make(chan Invocation), calls: make(chan struct{}, 8)}
	runner := &blockingRunner{started: make(chan InvocationIdentity), release: make(chan struct{})}
	sink := &recordingSink{done: make(chan struct{}, 1)}
	daemon, err := NewDaemon(DaemonConfig{}, source, runner, sink)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- daemon.Run(context.Background()) }()
	<-source.calls
	if err := daemon.Run(context.Background()); !errors.Is(err, ErrDaemonAlreadyRunning) {
		t.Fatalf("second Run error = %v", err)
	}
	if err := daemon.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}

func validDaemonInvocation(attemptID string) Invocation {
	return Invocation{
		Identity: InvocationIdentity{
			TenantID: "tenant-a", OwnerUserID: "user-a", WorkerID: "worker-a", RunID: "run-a",
			AttemptID: domainAttemptID(attemptID), LeaseID: "lease-a", FenceToken: 1,
		},
		Process: AttemptSpec{Executable: "/fixture", ExecutableDigest: ExecutableDigest{1}},
	}
}

func domainAttemptID(value string) domain.AttemptID { return domain.AttemptID(value) }

func waitForDaemonState(t *testing.T, daemon *Daemon, state DaemonState) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if daemon.Status().State == state {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("daemon did not reach state %s: %+v", state, daemon.Status())
}
