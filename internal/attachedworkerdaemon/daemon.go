package attachedworkerdaemon

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrDaemonAlreadyRunning = errors.New("attached worker daemon is already running")
	ErrDaemonNotRunning     = errors.New("attached worker daemon is not running")
	ErrDaemonShutdown       = errors.New("attached worker daemon shutdown exceeded its bound")
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

	mu           sync.Mutex
	state        DaemonState
	active       bool
	activeID     InvocationIdentity
	activeCancel context.CancelFunc
	pollCancel   context.CancelFunc
	done         chan struct{}
	wake         chan struct{}
	startedAt    time.Time
	completed    uint64
	lastFailure  string
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
	daemon.mu.Unlock()
}

func (daemon *Daemon) beginAttempt(identity InvocationIdentity) bool {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	if daemon.state != DaemonRunning || daemon.active {
		return false
	}
	daemon.active = true
	daemon.activeID = identity
	return true
}

func (daemon *Daemon) setActiveCancel(cancel context.CancelFunc) {
	daemon.mu.Lock()
	daemon.activeCancel = cancel
	daemon.mu.Unlock()
}

func (daemon *Daemon) finishAttempt(result InvocationResult, runErr error) {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	daemon.active = false
	daemon.activeID = InvocationIdentity{}
	daemon.completed++
	daemon.lastFailure = result.FailureCode
	if daemon.lastFailure == "" && runErr != nil {
		daemon.lastFailure = "invocation_runner_failed"
	}
}

func (daemon *Daemon) startDrain(cancelActive bool) bool {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	if daemon.state == DaemonStopped {
		return false
	}
	daemon.state = DaemonDraining
	if daemon.pollCancel != nil {
		daemon.pollCancel()
	}
	if cancelActive && daemon.activeCancel != nil {
		daemon.activeCancel()
	}
	if daemon.wake != nil {
		select {
		case daemon.wake <- struct{}{}:
		default:
		}
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
