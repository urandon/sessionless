package attachedworkerdaemon

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrDaemonAlreadyRunning  = errors.New("attached worker daemon is already running")
	ErrDaemonNotRunning      = errors.New("attached worker daemon is not running")
	ErrDaemonShutdown        = errors.New("attached worker daemon shutdown exceeded its bound")
	ErrActiveAttemptMissing  = errors.New("attached worker daemon active attempt is unavailable")
	ErrActiveAttemptMismatch = errors.New("attached worker daemon active attempt identity does not match")
)

type DaemonState string

const (
	DaemonStopped  DaemonState = "stopped"
	DaemonRunning  DaemonState = "running"
	DaemonDraining DaemonState = "draining"
)

type Source interface {
	Next(context.Context) (Invocation, bool, error)
}

type ResultSink interface {
	Complete(context.Context, InvocationIdentity, InvocationResult, error) error
}

type Runner interface {
	Run(context.Context, Invocation) (InvocationResult, error)
}

type DaemonConfig struct {
	IdleBackoff   time.Duration
	ShutdownGrace time.Duration
	ReportGrace   time.Duration
}

type Status struct {
	State           DaemonState
	Active          bool
	ActiveAttempt   InvocationIdentity
	StartedAt       time.Time
	Completed       uint64
	LastFailureCode string
}

type Daemon struct {
	config DaemonConfig
	source Source
	runner Runner
	sink   ResultSink

	mu                    sync.Mutex
	state                 DaemonState
	active                bool
	activeID              InvocationIdentity
	activeCancel          context.CancelFunc
	activeCancelIssued    bool
	cancelActiveOnInstall bool
	pollCancel            context.CancelFunc
	done                  chan struct{}
	wake                  chan struct{}
	startedAt             time.Time
	completed             uint64
	lastFailure           string
}

func NewDaemon(config DaemonConfig, source Source, runner Runner, sink ResultSink) (*Daemon, error) {
	if source == nil || runner == nil || sink == nil {
		return nil, ErrInvocationInvalid
	}
	if config.IdleBackoff <= 0 {
		config.IdleBackoff = time.Second
	}
	if config.ShutdownGrace <= 0 {
		config.ShutdownGrace = 30 * time.Second
	}
	if config.ReportGrace <= 0 {
		config.ReportGrace = 15 * time.Second
	}
	if config.ShutdownGrace > 2*time.Minute || config.ReportGrace > time.Minute {
		return nil, ErrInvocationInvalid
	}
	return &Daemon{config: config, source: source, runner: runner, sink: sink, state: DaemonStopped}, nil
}

func (daemon *Daemon) Run(parent context.Context) error {
	if parent == nil || parent.Err() != nil {
		return ErrInvocationInvalid
	}
	if !daemon.beginRun() {
		return ErrDaemonAlreadyRunning
	}
	defer daemon.finishRun()
	for {
		if daemon.draining() {
			return nil
		}
		pollCtx, pollCancel := context.WithCancel(parent)
		daemon.setPollCancel(pollCancel)
		invocation, available, err := daemon.source.Next(pollCtx)
		pollCancel()
		daemon.setPollCancel(nil)
		if err != nil {
			if daemon.draining() || parent.Err() != nil {
				return nil
			}
			return err
		}
		if !available {
			if !daemon.waitIdle(parent) {
				return nil
			}
			continue
		}
		if invocation.Validate() != nil {
			return ErrInvocationInvalid
		}
		if !daemon.beginAttempt(invocation.Identity) {
			return nil
		}
		attemptCtx, attemptCancel := context.WithCancel(parent)
		daemon.setActiveCancel(attemptCancel)
		result, runErr := daemon.runner.Run(attemptCtx, invocation)
		attemptCancel()
		daemon.setActiveCancel(nil)
		reportCtx, reportCancel := context.WithTimeout(context.WithoutCancel(parent), daemon.config.ReportGrace)
		reportErr := daemon.sink.Complete(reportCtx, invocation.Identity, result, runErr)
		reportCancel()
		daemon.finishAttempt(result, runErr)
		if reportErr != nil {
			return reportErr
		}
		if runErr != nil {
			return runErr
		}
	}
}

func (daemon *Daemon) Drain(ctx context.Context) error {
	if ctx == nil {
		return ErrInvocationInvalid
	}
	if !daemon.startDrain(false) {
		return ErrDaemonNotRunning
	}
	return daemon.waitDone(ctx, false)
}

func (daemon *Daemon) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrInvocationInvalid
	}
	if !daemon.startDrain(true) {
		return ErrDaemonNotRunning
	}
	return daemon.waitDone(ctx, true)
}

func (daemon *Daemon) Status() Status {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	return Status{
		State: daemon.state, Active: daemon.active, ActiveAttempt: daemon.activeID,
		StartedAt: daemon.startedAt, Completed: daemon.completed, LastFailureCode: daemon.lastFailure,
	}
}

func (daemon *Daemon) beginRun() bool {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	if daemon.state != DaemonStopped {
		return false
	}
	daemon.state = DaemonRunning
	daemon.done = make(chan struct{})
	daemon.wake = make(chan struct{}, 1)
	daemon.startedAt = time.Now().UTC()
	return true
}

func (daemon *Daemon) finishRun() {
	daemon.mu.Lock()
	daemon.state = DaemonStopped
	daemon.active = false
	daemon.activeID = InvocationIdentity{}
	daemon.activeCancel = nil
	daemon.activeCancelIssued = false
	daemon.cancelActiveOnInstall = false
	daemon.pollCancel = nil
	done := daemon.done
	daemon.done = nil
	daemon.wake = nil
	daemon.mu.Unlock()
	if done != nil {
		close(done)
	}
}

func (daemon *Daemon) draining() bool {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	return daemon.state == DaemonDraining
}

func (daemon *Daemon) setPollCancel(cancel context.CancelFunc) {
	daemon.mu.Lock()
	daemon.pollCancel = cancel
	shouldCancel := cancel != nil && daemon.state == DaemonDraining
	daemon.mu.Unlock()
	if shouldCancel {
		cancel()
	}
}

func (daemon *Daemon) beginAttempt(identity InvocationIdentity) bool {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	if daemon.state != DaemonRunning || daemon.active {
		return false
	}
	daemon.active = true
	daemon.activeID = identity
	daemon.activeCancelIssued = false
	return true
}

func (daemon *Daemon) setActiveCancel(cancel context.CancelFunc) {
	daemon.mu.Lock()
	daemon.activeCancel = cancel
	shouldCancel := cancel != nil && daemon.cancelActiveOnInstall && !daemon.activeCancelIssued
	if shouldCancel {
		daemon.activeCancelIssued = true
	}
	daemon.mu.Unlock()
	if shouldCancel {
		cancel()
	}
}

// CancelActive cancels only the exact currently active invocation. Repeating
// the same accepted authority is idempotent and never invokes the local cancel
// function twice. Callers must still reconcile their own remote acknowledgement
// before treating cancellation as committed.
func (daemon *Daemon) CancelActive(ctx context.Context, identity InvocationIdentity) error {
	if daemon == nil || ctx == nil || identity.Validate() != nil {
		return ErrInvocationInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	daemon.mu.Lock()
	if !daemon.active {
		daemon.mu.Unlock()
		return ErrActiveAttemptMissing
	}
	if daemon.activeID != identity {
		daemon.mu.Unlock()
		return ErrActiveAttemptMismatch
	}
	if daemon.activeCancelIssued {
		daemon.mu.Unlock()
		return nil
	}
	cancel := daemon.activeCancel
	if cancel == nil {
		daemon.mu.Unlock()
		return ErrActiveAttemptMissing
	}
	daemon.activeCancelIssued = true
	daemon.mu.Unlock()
	cancel()
	return nil
}

func (daemon *Daemon) finishAttempt(result InvocationResult, runErr error) {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	daemon.active = false
	daemon.activeID = InvocationIdentity{}
	daemon.activeCancel = nil
	daemon.activeCancelIssued = false
	daemon.completed++
	daemon.lastFailure = result.FailureCode
	if daemon.lastFailure == "" && runErr != nil {
		daemon.lastFailure = "invocation_runner_failed"
	}
}

func (daemon *Daemon) startDrain(cancelActive bool) bool {
	daemon.mu.Lock()
	if daemon.state == DaemonStopped {
		daemon.mu.Unlock()
		return false
	}
	daemon.state = DaemonDraining
	if cancelActive {
		daemon.cancelActiveOnInstall = true
	}
	if daemon.pollCancel != nil {
		daemon.pollCancel()
	}
	activeCancel := context.CancelFunc(nil)
	if cancelActive && daemon.activeCancel != nil && !daemon.activeCancelIssued {
		daemon.activeCancelIssued = true
		activeCancel = daemon.activeCancel
	}
	if daemon.wake != nil {
		select {
		case daemon.wake <- struct{}{}:
		default:
		}
	}
	daemon.mu.Unlock()
	if activeCancel != nil {
		activeCancel()
	}
	return true
}

func (daemon *Daemon) waitDone(ctx context.Context, bounded bool) error {
	daemon.mu.Lock()
	done := daemon.done
	daemon.mu.Unlock()
	if done == nil {
		return nil
	}
	if !bounded {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	timer := time.NewTimer(daemon.config.ShutdownGrace)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ErrDaemonShutdown
	}
}

func (daemon *Daemon) waitIdle(parent context.Context) bool {
	daemon.mu.Lock()
	wake := daemon.wake
	daemon.mu.Unlock()
	timer := time.NewTimer(daemon.config.IdleBackoff)
	defer timer.Stop()
	select {
	case <-parent.Done():
		return false
	case <-wake:
		return false
	case <-timer.C:
		return !daemon.draining()
	}
}

var _ Runner = (*InvocationRunner)(nil)
