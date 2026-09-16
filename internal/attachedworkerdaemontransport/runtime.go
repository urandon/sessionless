package attachedworkerdaemontransport

import (
	"context"
	"errors"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
)

const (
	defaultActiveControlInterval = 5 * time.Second
	defaultRuntimeCleanupTimeout = 15 * time.Second
)

var ErrRuntimeAlreadyUsed = errors.New("attached worker foreground runtime has already been used")

// RuntimeConfig bounds the feature-disabled foreground composition. The
// shipped attached-worker command does not construct this runtime.
type RuntimeConfig struct {
	Daemon                attachedworkerdaemon.DaemonConfig
	ActiveControlInterval time.Duration
	CleanupTimeout        time.Duration
}

type activeControlWatcher interface {
	WatchActiveControl(
		context.Context,
		attachedworkerdaemon.InvocationIdentity,
		ActiveAttemptController,
		time.Duration,
	) (ActiveControl, error)
}

type sessionCloser interface {
	Close(context.Context) error
}

type wakeSource interface {
	attachedworkerdaemon.Source
	Wake() error
}

// ForegroundRuntime is the one-owner composition of cadence, fenced dispatch,
// active control, daemon execution, terminal reporting, and session cleanup.
// It remains feature-disabled because no product constructor or command path
// can create it.
type ForegroundRuntime struct {
	daemon  *attachedworkerdaemon.Daemon
	source  wakeSource
	closer  sessionCloser
	cleanup time.Duration

	mu   sync.Mutex
	used bool
}

// ReconnectForegroundRuntime performs the reviewed durable idle reconnect and
// then constructs one foreground runtime. Any construction failure closes the
// reconciled session before returning.
func ReconnectForegroundRuntime(
	ctx context.Context,
	connector *attachedworkersession.Connector,
	input attachedworkersession.ReconnectInputV1,
	materializer Materializer,
	adapterConfig Config,
	pollConfig attachedworkertransport.Config,
	runner attachedworkerdaemon.Runner,
	config RuntimeConfig,
) (*ForegroundRuntime, error) {
	cadence, err := ReconnectIdleCadence(ctx, connector, input, materializer, adapterConfig, pollConfig)
	if err != nil {
		return nil, err
	}
	runtime, err := NewForegroundRuntime(cadence, runner, config)
	if err == nil {
		return runtime, nil
	}
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), preparedRuntimeConfig(config).CleanupTimeout)
	defer cancel()
	return nil, errors.Join(err, cadence.Close(closeCtx))
}

// NewForegroundRuntime composes an already reconciled idle cadence. The
// cadence's adapter remains the only source, result sink, and active-control
// authority; the daemon remains the only process and drain authority.
func NewForegroundRuntime(
	cadence *IdleRecoveredCadence,
	runner attachedworkerdaemon.Runner,
	config RuntimeConfig,
) (*ForegroundRuntime, error) {
	if cadence == nil || cadence.Source == nil || cadence.Adapter == nil || cadence.session == nil || runner == nil {
		return nil, ErrInvalidConfiguration
	}
	return newForegroundRuntime(cadence.Source, cadence.Adapter, cadence.Adapter, cadence, runner, config)
}

func newForegroundRuntime(
	source attachedworkerdaemon.Source,
	sink attachedworkerdaemon.ResultSink,
	watcher activeControlWatcher,
	closer sessionCloser,
	runner attachedworkerdaemon.Runner,
	config RuntimeConfig,
) (*ForegroundRuntime, error) {
	config, err := validateRuntimeConfig(config)
	wakeable, wakeableOK := source.(wakeSource)
	if err != nil || source == nil || sink == nil || watcher == nil || closer == nil || runner == nil || !wakeableOK {
		return nil, ErrInvalidConfiguration
	}
	proxy := &activeControllerProxy{}
	drainSource := &drainAwareSource{source: source, target: proxy, timeout: config.CleanupTimeout}
	controlled := &activeControlRunner{
		delegate: runner, watcher: watcher, target: proxy,
		interval: config.ActiveControlInterval, cleanup: config.CleanupTimeout,
	}
	daemon, err := attachedworkerdaemon.NewDaemon(config.Daemon, drainSource, controlled, sink)
	if err != nil {
		return nil, err
	}
	proxy.bind(daemon)
	return &ForegroundRuntime{daemon: daemon, source: wakeable, closer: closer, cleanup: config.CleanupTimeout}, nil
}

func preparedRuntimeConfig(config RuntimeConfig) RuntimeConfig {
	if config.ActiveControlInterval == 0 {
		config.ActiveControlInterval = defaultActiveControlInterval
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = defaultRuntimeCleanupTimeout
	}
	return config
}

func validateRuntimeConfig(config RuntimeConfig) (RuntimeConfig, error) {
	config = preparedRuntimeConfig(config)
	if config.ActiveControlInterval <= 0 || config.ActiveControlInterval > time.Minute ||
		config.CleanupTimeout <= 0 || config.CleanupTimeout > time.Minute {
		return RuntimeConfig{}, ErrInvalidConfiguration
	}
	return config, nil
}

// Run owns the daemon and reconciled session exactly once. Session close runs
// under an independent cleanup bound even when the caller cancels the daemon.
func (runtime *ForegroundRuntime) Run(ctx context.Context) error {
	if runtime == nil || runtime.daemon == nil || runtime.closer == nil || ctx == nil || ctx.Err() != nil {
		return ErrInvalidConfiguration
	}
	runtime.mu.Lock()
	if runtime.used {
		runtime.mu.Unlock()
		return ErrRuntimeAlreadyUsed
	}
	runtime.used = true
	runtime.mu.Unlock()

	runErr := runtime.daemon.Run(ctx)
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runtime.cleanup)
	closeErr := runtime.closer.Close(closeCtx)
	cancel()
	if closeErr != nil {
		return errors.Join(runErr, ErrReconciliationRequired, closeErr)
	}
	return runErr
}

func (runtime *ForegroundRuntime) Drain(ctx context.Context) error {
	if runtime == nil || runtime.daemon == nil {
		return ErrInvalidConfiguration
	}
	return runtime.daemon.Drain(ctx)
}

func (runtime *ForegroundRuntime) Shutdown(ctx context.Context) error {
	if runtime == nil || runtime.daemon == nil {
		return ErrInvalidConfiguration
	}
	return runtime.daemon.Shutdown(ctx)
}

func (runtime *ForegroundRuntime) Wake() error {
	if runtime == nil || runtime.source == nil {
		return ErrInvalidConfiguration
	}
	return runtime.source.Wake()
}

func (runtime *ForegroundRuntime) Status() attachedworkerdaemon.Status {
	if runtime == nil || runtime.daemon == nil {
		return attachedworkerdaemon.Status{}
	}
	return runtime.daemon.Status()
}

type activeControllerProxy struct {
	mu     sync.RWMutex
	target ActiveAttemptController
}

func (proxy *activeControllerProxy) bind(target ActiveAttemptController) {
	proxy.mu.Lock()
	proxy.target = target
	proxy.mu.Unlock()
}

func (proxy *activeControllerProxy) CancelActive(ctx context.Context, identity attachedworkerdaemon.InvocationIdentity) error {
	proxy.mu.RLock()
	target := proxy.target
	proxy.mu.RUnlock()
	if target == nil {
		return ErrInvalidConfiguration
	}
	return target.CancelActive(ctx, identity)
}

func (proxy *activeControllerProxy) RequestDrain(ctx context.Context) error {
	proxy.mu.RLock()
	target := proxy.target
	proxy.mu.RUnlock()
	if target == nil {
		return ErrInvalidConfiguration
	}
	return target.RequestDrain(ctx)
}

type drainAwareSource struct {
	source  attachedworkerdaemon.Source
	target  ActiveAttemptController
	timeout time.Duration
}

func (source *drainAwareSource) Next(ctx context.Context) (attachedworkerdaemon.Invocation, bool, error) {
	invocation, available, err := source.source.Next(ctx)
	if !errors.Is(err, ErrDrainRequested) {
		return invocation, available, err
	}
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), source.timeout)
	drainErr := source.target.RequestDrain(drainCtx)
	cancel()
	if drainErr != nil {
		return attachedworkerdaemon.Invocation{}, false, errors.Join(ErrReconciliationRequired, drainErr)
	}
	return attachedworkerdaemon.Invocation{}, false, nil
}

type activeControlRunner struct {
	delegate attachedworkerdaemon.Runner
	watcher  activeControlWatcher
	target   ActiveAttemptController
	interval time.Duration
	cleanup  time.Duration
}

type runnerOutcome struct {
	result attachedworkerdaemon.InvocationResult
	err    error
}

type controlOutcome struct {
	control ActiveControl
	err     error
}

func (runner *activeControlRunner) Run(
	ctx context.Context,
	invocation attachedworkerdaemon.Invocation,
) (attachedworkerdaemon.InvocationResult, error) {
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	runDone := make(chan runnerOutcome, 1)
	watchDone := make(chan controlOutcome, 1)
	go func() {
		result, err := runner.delegate.Run(ctx, invocation)
		runDone <- runnerOutcome{result: result, err: err}
	}()
	go func() {
		control, err := runner.watcher.WatchActiveControl(watchCtx, invocation.Identity, runner.target, runner.interval)
		watchDone <- controlOutcome{control: control, err: err}
	}()

	select {
	case outcome := <-runDone:
		cancelWatch()
		control := runner.waitControl(watchDone)
		if control.err != nil && !errors.Is(control.err, context.Canceled) {
			return outcome.result, errors.Join(outcome.err, ErrReconciliationRequired, control.err)
		}
		return outcome.result, outcome.err
	case control := <-watchDone:
		if control.err != nil {
			cancelErr := runner.cancelExact(invocation.Identity)
			outcome, waitErr := runner.waitRunner(runDone)
			return outcome.result, errors.Join(outcome.err, ErrReconciliationRequired, control.err, cancelErr, waitErr)
		}
		if control.control != ActiveControlCancelled && control.control != ActiveControlDraining {
			cancelErr := runner.cancelExact(invocation.Identity)
			outcome, waitErr := runner.waitRunner(runDone)
			return outcome.result, errors.Join(outcome.err, ErrReconciliationRequired, ErrInvalidAuthority, cancelErr, waitErr)
		}
		if control.control == ActiveControlDraining {
			select {
			case outcome := <-runDone:
				return outcome.result, outcome.err
			case <-ctx.Done():
				outcome, waitErr := runner.waitRunner(runDone)
				return outcome.result, errors.Join(outcome.err, ctx.Err(), waitErr)
			}
		}
		outcome, waitErr := runner.waitRunner(runDone)
		return outcome.result, errors.Join(outcome.err, waitErr)
	}
}

func (runner *activeControlRunner) cancelExact(identity attachedworkerdaemon.InvocationIdentity) error {
	ctx, cancel := context.WithTimeout(context.Background(), runner.cleanup)
	defer cancel()
	return runner.target.CancelActive(ctx, identity)
}

func (runner *activeControlRunner) waitRunner(done <-chan runnerOutcome) (runnerOutcome, error) {
	timer := time.NewTimer(runner.cleanup)
	defer timer.Stop()
	select {
	case outcome := <-done:
		return outcome, nil
	case <-timer.C:
		return runnerOutcome{}, context.DeadlineExceeded
	}
}

func (runner *activeControlRunner) waitControl(done <-chan controlOutcome) controlOutcome {
	timer := time.NewTimer(runner.cleanup)
	defer timer.Stop()
	select {
	case outcome := <-done:
		return outcome
	case <-timer.C:
		return controlOutcome{err: context.DeadlineExceeded}
	}
}

var _ attachedworkerdaemon.Runner = (*activeControlRunner)(nil)
var _ ActiveAttemptController = (*activeControllerProxy)(nil)
