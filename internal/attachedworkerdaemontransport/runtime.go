package attachedworkerdaemontransport

import (
	"context"
	"encoding/hex"
	"errors"
	"slices"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
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

type initialConnectPort interface {
	Connect(context.Context, attachedworkersession.ConnectInputV1) (connectedSession, error)
}

type connectedSession interface {
	SessionPort
	Close(context.Context) error
}

type concreteInitialConnectPort struct {
	connector *attachedworkersession.Connector
}

func (port concreteInitialConnectPort) Connect(ctx context.Context, input attachedworkersession.ConnectInputV1) (connectedSession, error) {
	return port.connector.Connect(ctx, input)
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

type initialConnectorFactory func(*attachedworkerlocal.Store, attachedworkersession.BootstrapPort, attachedworkersession.ExchangeFactory, attachedworkersession.Config) (initialConnectPort, error)

type pinnedStackFactory func(context.Context, attachedworkerlocal.ManifestV1, attachedworkerstack.Config, ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error)

// ConnectPinnedForegroundRuntime constructs the initial session and pinned
// runner under the same local runtime lease. The pinned stack is verified and
// its installation-owned OCI residue reconciled before Connect advances a
// generation or performs network I/O. This remains a default-off library path;
// the shipped command does not call it or authorize a provider turn.
func ConnectPinnedForegroundRuntime(
	ctx context.Context,
	store *attachedworkerlocal.Store,
	bootstrap attachedworkersession.BootstrapPort,
	exchange attachedworkersession.ExchangeFactory,
	sessionConfig attachedworkersession.Config,
	input attachedworkersession.ConnectInputV1,
	materializer Materializer,
	adapterConfig Config,
	pollConfig attachedworkertransport.Config,
	stackConfig attachedworkerstack.Config,
	credentials ports.CredentialLifecycle,
	config RuntimeConfig,
) (*ForegroundRuntime, error) {
	if store == nil {
		return nil, ErrInvalidConfiguration
	}
	return connectPinnedForegroundRuntime(ctx, store, bootstrap, exchange, sessionConfig, input, materializer,
		adapterConfig, pollConfig, stackConfig, credentials, config,
		func(store *attachedworkerlocal.Store, bootstrap attachedworkersession.BootstrapPort, exchange attachedworkersession.ExchangeFactory, config attachedworkersession.Config) (initialConnectPort, error) {
			connector, err := attachedworkersession.New(store, bootstrap, exchange, config)
			if err != nil {
				return nil, err
			}
			return concreteInitialConnectPort{connector: connector}, nil
		},
		func(ctx context.Context, manifest attachedworkerlocal.ManifestV1, config attachedworkerstack.Config, credentials ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
			return attachedworkerstack.New(ctx, manifest, config, credentials)
		})
}

func connectPinnedForegroundRuntime(
	ctx context.Context,
	store *attachedworkerlocal.Store,
	bootstrap attachedworkersession.BootstrapPort,
	exchange attachedworkersession.ExchangeFactory,
	sessionConfig attachedworkersession.Config,
	input attachedworkersession.ConnectInputV1,
	materializer Materializer,
	adapterConfig Config,
	pollConfig attachedworkertransport.Config,
	stackConfig attachedworkerstack.Config,
	credentials ports.CredentialLifecycle,
	config RuntimeConfig,
	newConnector initialConnectorFactory,
	newStack pinnedStackFactory,
) (*ForegroundRuntime, error) {
	if ctx == nil || newConnector == nil || newStack == nil || credentials == nil || sessionConfig.RuntimePreflight != nil {
		return nil, ErrInvalidConfiguration
	}
	// The selected capability and the local invocation profile must already
	// agree before acquiring the local lease or constructing a connector.
	digest, err := attachedworkerprotocol.ManifestDigestV1(input.CapabilityManifest)
	if err != nil || adapterConfig.Profile.CapabilityDigest != domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digest)) {
		return nil, ErrInvalidAuthority
	}
	for _, variable := range adapterConfig.Profile.Environment {
		if !slices.Contains(stackConfig.AllowedEnvironmentNames, variable.Name) {
			return nil, ErrInvalidConfiguration
		}
	}
	capabilityExecutableDigest := slices.Clone(input.CapabilityManifest.HarnessExecutableDigest)
	adapterConfig.Profile = cloneProfile(adapterConfig.Profile)
	var runner attachedworkerdaemon.Runner
	sessionConfig.RuntimePreflight = func(ctx context.Context, manifest attachedworkerlocal.ManifestV1) error {
		if !profileMatchesManifest(adapterConfig.Profile, manifest) ||
			!slices.Equal(capabilityExecutableDigest, decodeDigest(manifest.Harness.SHA256)) {
			return ErrInvalidAuthority
		}
		pinned, err := newStack(ctx, manifest, stackConfig, credentials)
		if err != nil {
			return err
		}
		if pinned == nil {
			return ErrInvalidConfiguration
		}
		runner = pinned
		return nil
	}
	connector, err := newConnector(store, bootstrap, exchange, sessionConfig)
	if err != nil {
		return nil, err
	}
	return connectForegroundRuntime(ctx, connector, input, materializer, adapterConfig, pollConfig, &preflightRunner{current: &runner}, config)
}

// preflightRunner only exists so local configuration can be validated before
// Connect. It never dispatches unless the lease-held preflight installed a
// fully verified runner. A failed preflight cannot leave process authority.
type preflightRunner struct{ current *attachedworkerdaemon.Runner }

func (runner *preflightRunner) ready() bool {
	return runner != nil && runner.current != nil && *runner.current != nil
}

func (runner *preflightRunner) Run(ctx context.Context, invocation attachedworkerdaemon.Invocation) (attachedworkerdaemon.InvocationResult, error) {
	if !runner.ready() {
		return attachedworkerdaemon.InvocationResult{}, ErrReconciliationRequired
	}
	return (*runner.current).Run(ctx, invocation)
}

func profileMatchesManifest(profile LocalProfileV1, manifest attachedworkerlocal.ManifestV1) bool {
	digest := decodeDigest(manifest.Harness.SHA256)
	return len(digest) == len(profile.ExecutableDigest) && slices.Equal(digest, profile.ExecutableDigest[:]) &&
		profile.Executable == manifest.Harness.Executable && slices.Equal(profile.Arguments, manifest.Harness.Arguments)
}

func decodeDigest(encoded string) []byte {
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return nil
	}
	return decoded
}

func connectForegroundRuntime(
	ctx context.Context,
	port initialConnectPort,
	input attachedworkersession.ConnectInputV1,
	materializer Materializer,
	adapterConfig Config,
	pollConfig attachedworkertransport.Config,
	runner attachedworkerdaemon.Runner,
	config RuntimeConfig,
) (*ForegroundRuntime, error) {
	if ctx == nil || port == nil || materializer == nil || runner == nil {
		return nil, ErrInvalidConfiguration
	}
	if !pollConfig.Enabled {
		return nil, attachedworkertransport.ErrPollingDisabled
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Validate every locally available bound before Connect advances the
	// generation or sends a challenge/Manifest. A successful Manifest starts
	// with cooldown; it never causes an immediate extra heartbeat.
	if _, err := prepareAdapterConfig(adapterConfig); err != nil {
		return nil, err
	}
	pollConfig.StartWithCooldown = true
	preparedPoll, err := attachedworkertransport.PrepareConfig(pollConfig)
	if err != nil {
		return nil, err
	}
	if _, err := validateRuntimeConfig(config); err != nil {
		return nil, err
	}
	session, err := port.Connect(ctx, input)
	if session == nil {
		if err != nil {
			return nil, err
		}
		return nil, ErrReconciliationRequired
	}
	cleanup := func(prior error) (*ForegroundRuntime, error) {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), safeRuntimeCleanupTimeout(config.CleanupTimeout))
		defer cancel()
		return nil, errors.Join(prior, session.Close(closeCtx))
	}
	if err != nil || ctx.Err() != nil {
		return cleanup(errors.Join(ErrReconciliationRequired, err, ctx.Err()))
	}
	if prepared, ok := runner.(*preflightRunner); ok && !prepared.ready() {
		return cleanup(ErrReconciliationRequired)
	}
	adapter, err := New(session, materializer, adapterConfig)
	if err != nil {
		return cleanup(ErrReconciliationRequired)
	}
	if _, err := adapter.readySnapshot(); err != nil {
		return cleanup(errors.Join(ErrReconciliationRequired, err))
	}
	source, err := NewPreparedCadencedSource(adapter, preparedPoll)
	if err != nil {
		return cleanup(errors.Join(ErrReconciliationRequired, err))
	}
	runtime, err := newForegroundRuntime(source, adapter, adapter, session, runner, config)
	if err != nil {
		return cleanup(errors.Join(ErrReconciliationRequired, err))
	}
	if err := ctx.Err(); err != nil {
		return cleanup(errors.Join(ErrReconciliationRequired, err))
	}
	return runtime, nil
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
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), safeRuntimeCleanupTimeout(config.CleanupTimeout))
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
		config.CleanupTimeout <= 0 || config.CleanupTimeout > time.Minute ||
		config.Daemon.ShutdownGrace > 2*time.Minute || config.Daemon.ReportGrace > time.Minute {
		return RuntimeConfig{}, ErrInvalidConfiguration
	}
	return config, nil
}

func safeRuntimeCleanupTimeout(timeout time.Duration) time.Duration {
	if timeout <= 0 || timeout > time.Minute {
		return defaultRuntimeCleanupTimeout
	}
	return timeout
}

// Run owns the daemon and reconciled session exactly once. Session close runs
// under an independent cleanup bound even when the caller cancels the daemon.
func (runtime *ForegroundRuntime) Run(ctx context.Context) error {
	if runtime == nil || runtime.daemon == nil || runtime.closer == nil {
		return ErrInvalidConfiguration
	}
	runtime.mu.Lock()
	if runtime.used {
		runtime.mu.Unlock()
		return ErrRuntimeAlreadyUsed
	}
	runtime.used = true
	runtime.mu.Unlock()

	// Once constructed, this runtime owns an acquired session and lease even if
	// the caller cancels before Run starts. Closing them must not depend on the
	// caller's context being live (or even non-nil).
	cleanupParent := context.Background()
	var runErr error
	if ctx == nil {
		runErr = ErrInvalidConfiguration
	} else {
		cleanupParent = context.WithoutCancel(ctx)
		if err := ctx.Err(); err != nil {
			runErr = err
		} else {
			runErr = runtime.daemon.Run(ctx)
		}
	}
	closeCtx, cancel := context.WithTimeout(cleanupParent, runtime.cleanup)
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
			if ctx.Err() != nil && errors.Is(control.err, ctx.Err()) {
				outcome, waitErr := runner.waitRunner(runDone)
				return outcome.result, errors.Join(outcome.err, waitErr)
			}
			cancelErr := runner.cancelExact(invocation.Identity)
			outcome, waitErr := runner.waitRunner(runDone)
			return outcome.result, errors.Join(outcome.err, ErrReconciliationRequired, control.err, cancelErr, waitErr)
		}
		if control.control != ActiveControlCancelled {
			cancelErr := runner.cancelExact(invocation.Identity)
			outcome, waitErr := runner.waitRunner(runDone)
			return outcome.result, errors.Join(outcome.err, ErrReconciliationRequired, ErrInvalidAuthority, cancelErr, waitErr)
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
