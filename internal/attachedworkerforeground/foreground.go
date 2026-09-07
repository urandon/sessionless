// Package attachedworkerforeground owns the feature-disabled foreground
// preflight and local-runtime lifecycle. It deliberately does not own
// enrollment, transport, attempt, provider, or isolation authority.
package attachedworkerforeground

import (
	"context"
	"errors"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

const resultVersionV1 = uint32(1)

var (
	ErrInvalidConfiguration = errors.New("attached worker foreground configuration is invalid")
	ErrFeatureDisabled      = errors.New("attached worker foreground activation is disabled")
)

type ResultCode string

const (
	CodeFeatureDisabled ResultCode = "feature_disabled"
	CodeInvalid         ResultCode = "state_invalid"
)

// ResultV1 contains bounded local lifecycle evidence only. Constant unknown
// and not_attempted values are intentional: preflight cannot upgrade them to
// remote or daemon authority.
type ResultV1 struct {
	Version               uint32                             `json:"version"`
	Code                  ResultCode                         `json:"code"`
	Lifecycle             attachedworkerlocal.LifecycleState `json:"lifecycle,omitempty"`
	ManifestRevision      uint64                             `json:"manifest_revision,omitempty"`
	ObservationRevision   uint64                             `json:"observation_revision,omitempty"`
	FeatureState          string                             `json:"feature_state"`
	RuntimeOwnership      string                             `json:"runtime_ownership"`
	ObservationState      string                             `json:"observation_state"`
	DaemonState           string                             `json:"daemon_state"`
	ServerConnectionState string                             `json:"server_connection_state"`
	NetworkAction         string                             `json:"network_action"`
	ProcessAction         string                             `json:"process_action"`
	CredentialAction      string                             `json:"credential_action"`
}

type Config struct {
	CleanupTimeout time.Duration
	Now            func() time.Time
}

// BootstrapPort is the existing bootstrap/enrollment authority surface. The
// disabled shell retains the seam but never calls it.
type BootstrapPort interface {
	IssueChallenge(context.Context, attachedworkerhttp.ChallengeRequestV1) (*attachedworkerhttp.ChallengeResponseV1, error)
	Activate(context.Context, attachedworkerhttp.ActivateClientInputV1) (*attachedworkerhttp.ActivateResponseV1, error)
}

// DaemonPort is the existing daemon lifecycle authority surface. The disabled
// shell owns only its local lifecycle and never starts or controls a daemon.
type DaemonPort interface {
	Run(context.Context) error
	Drain(context.Context) error
	Shutdown(context.Context) error
	Status() attachedworkerdaemon.Status
}

// ActivationPorts names every live composition authority deferred by this
// feature-disabled slice. It is accepted only by the package-private test
// constructor; the shipped product constructor cannot inject live authority.
type ActivationPorts struct {
	Bootstrap  BootstrapPort
	Source     attachedworkerdaemon.Source
	ResultSink attachedworkerdaemon.ResultSink
	Runner     attachedworkerdaemon.Runner
	Daemon     DaemonPort
}

type localRuntimeLease interface {
	PersistObservation(context.Context, attachedworkerlocal.RuntimeObservationV1) error
	RetireObservation(context.Context, uint64) error
	Close() error
}

type localState interface {
	AcquireRuntime(context.Context) (localRuntimeLease, error)
	LoadSnapshot(context.Context) (attachedworkerlocal.SnapshotV1, error)
	LoadSecret(context.Context) (attachedworkerlocal.SecretRecordV1, error)
}

type storeAdapter struct{ store *attachedworkerlocal.Store }

func (adapter storeAdapter) AcquireRuntime(ctx context.Context) (localRuntimeLease, error) {
	return adapter.store.AcquireRuntime(ctx)
}

func (adapter storeAdapter) LoadSnapshot(ctx context.Context) (attachedworkerlocal.SnapshotV1, error) {
	return adapter.store.LoadSnapshot(ctx)
}

func (adapter storeAdapter) LoadSecret(ctx context.Context) (attachedworkerlocal.SecretRecordV1, error) {
	return adapter.store.LoadSecret(ctx)
}

type Foreground struct {
	store          localState
	now            func() time.Time
	cleanupTimeout time.Duration
	ports          ActivationPorts
}

func New(store *attachedworkerlocal.Store, config Config) (*Foreground, error) {
	if store == nil {
		return nil, ErrInvalidConfiguration
	}
	return newForeground(storeAdapter{store: store}, config, ActivationPorts{})
}

func newForeground(store localState, config Config, ports ActivationPorts) (*Foreground, error) {
	if store == nil || config.CleanupTimeout < 0 || config.CleanupTimeout > time.Minute {
		return nil, ErrInvalidConfiguration
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = 5 * time.Second
	}
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Foreground{store: store, now: config.Now, cleanupTimeout: config.CleanupTimeout, ports: ports}, nil
}

// Session owns the kernel runtime lease between successful preflight and
// bounded shutdown. Drain and Shutdown are idempotent and safe for concurrent
// callers; caller cancellation never cancels the one cleanup attempt.
type Session struct {
	mu             sync.Mutex
	lease          localRuntimeLease
	now            func() time.Time
	cleanupTimeout time.Duration
	observation    attachedworkerlocal.RuntimeObservationV1
	result         ResultV1
	state          string
	shutdownOnce   sync.Once
	shutdownDone   chan struct{}
	shutdownErr    error
}

// Start performs fail-closed local preflight and returns ownership to the
// caller without activating any live authority port.
func (foreground *Foreground) Start(ctx context.Context) (result ResultV1, session *Session, resultErr error) {
	result = emptyResult()
	if ctx == nil || ctx.Err() != nil || foreground == nil || foreground.store == nil || foreground.now == nil {
		result.Code = CodeInvalid
		return result, nil, ErrInvalidConfiguration
	}
	lease, err := foreground.store.AcquireRuntime(ctx)
	if err != nil {
		result.Code = localCode(err)
		return result, nil, err
	}
	result.RuntimeOwnership = "acquired"
	cleanupLease := func(prior error) error {
		closeErr := lease.Close()
		if closeErr == nil {
			result.RuntimeOwnership = "released"
		} else {
			result.RuntimeOwnership = "unknown"
			result.Code = localCode(closeErr)
		}
		return errors.Join(prior, closeErr)
	}

	snapshot, err := foreground.store.LoadSnapshot(ctx)
	if err != nil {
		result.Code = localCode(err)
		return result, nil, cleanupLease(err)
	}
	result.Lifecycle = snapshot.Manifest.Lifecycle
	result.ManifestRevision = snapshot.Manifest.Revision
	if snapshot.Manifest.Lifecycle != attachedworkerlocal.LifecycleActive {
		result.Code = localCode(attachedworkerlocal.ErrSecretRetired)
		return result, nil, cleanupLease(attachedworkerlocal.ErrSecretRetired)
	}
	secret, err := foreground.store.LoadSecret(ctx)
	if err != nil {
		result.Code = localCode(err)
		return result, nil, cleanupLease(err)
	}
	clearSecret(&secret)

	observedAt := foreground.now().UTC()
	if observedAt.Before(snapshot.Manifest.UpdatedAt) ||
		(snapshot.ObservationPresent && observedAt.Before(snapshot.Observation.ObservedAt)) {
		result.Code = localCode(attachedworkerlocal.ErrStateConflict)
		return result, nil, cleanupLease(attachedworkerlocal.ErrStateConflict)
	}
	observation := attachedworkerlocal.RuntimeObservationV1{
		Version: 1, Revision: 1, ManifestRevision: snapshot.Manifest.Revision,
		State: attachedworkerdaemon.DaemonStopped, Active: false, ObservedAt: observedAt,
		LastFailureCode: "feature_disabled",
	}
	if snapshot.ObservationPresent {
		if snapshot.Observation.Revision == ^uint64(0) {
			result.Code = localCode(attachedworkerlocal.ErrStateConflict)
			return result, nil, cleanupLease(attachedworkerlocal.ErrStateConflict)
		}
		observation.Revision = snapshot.Observation.Revision + 1
		observation.Accepted = snapshot.Observation.Accepted
		observation.Completed = snapshot.Observation.Completed
		observation.Failed = snapshot.Observation.Failed
	}
	if err := lease.PersistObservation(ctx, observation); err != nil {
		result.Code = localCode(err)
		return result, nil, cleanupLease(err)
	}
	result.ObservationRevision = observation.Revision
	result.ObservationState = "observed_local"
	result.Code = CodeFeatureDisabled
	session = &Session{
		lease: lease, now: foreground.now, cleanupTimeout: foreground.cleanupTimeout,
		observation: observation, result: result, state: "owned", shutdownDone: make(chan struct{}),
	}
	return result, session, nil
}

// Run is the current command path: preflight, then bounded drain/shutdown,
// returning the stable feature-disabled result only after ownership release.
func (foreground *Foreground) Run(ctx context.Context) (ResultV1, error) {
	result, session, err := foreground.Start(ctx)
	if err != nil {
		return result, err
	}
	shutdownErr := session.Shutdown(context.Background())
	result = session.Result()
	if shutdownErr != nil {
		return result, errors.Join(ErrFeatureDisabled, shutdownErr)
	}
	return result, ErrFeatureDisabled
}

// Drain persists the explicit local draining transition once. It never calls
// a daemon port and retains runtime ownership for Shutdown.
func (session *Session) Drain(ctx context.Context) error {
	if session == nil || ctx == nil || ctx.Err() != nil {
		return ErrInvalidConfiguration
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.state == "stopped" {
		return session.shutdownErr
	}
	if session.state == "draining" {
		return nil
	}
	return session.transitionLocked(ctx, attachedworkerdaemon.DaemonDraining, "draining")
}

// Shutdown starts exactly one cleanup attempt under its own timeout. Each
// caller may bound only how long it waits; timing out cannot cancel cleanup.
func (session *Session) Shutdown(ctx context.Context) error {
	if session == nil || ctx == nil {
		return ErrInvalidConfiguration
	}
	session.shutdownOnce.Do(func() { go session.shutdown() })
	select {
	case <-session.shutdownDone:
		session.mu.Lock()
		err := session.shutdownErr
		session.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (session *Session) shutdown() {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), session.cleanupTimeout)
	defer cancel()

	session.mu.Lock()
	var cleanupErr error
	if session.state == "owned" {
		cleanupErr = session.transitionLocked(cleanupCtx, attachedworkerdaemon.DaemonDraining, "draining")
	}
	if cleanupErr == nil && session.state == "draining" {
		cleanupErr = session.transitionLocked(cleanupCtx, attachedworkerdaemon.DaemonStopped, "stopped")
	}
	if cleanupErr == nil {
		cleanupErr = session.lease.RetireObservation(cleanupCtx, session.observation.Revision)
		if cleanupErr == nil {
			session.result.ObservationState = "retired"
		} else {
			session.result.ObservationState = "unknown"
		}
	}
	closeErr := session.lease.Close()
	if closeErr == nil {
		session.result.RuntimeOwnership = "released"
	} else {
		session.result.RuntimeOwnership = "unknown"
	}
	if cleanupErr != nil || closeErr != nil {
		session.shutdownErr = errors.Join(cleanupErr, closeErr)
		if cleanupErr != nil {
			session.result.Code = localCode(cleanupErr)
		} else {
			session.result.Code = localCode(closeErr)
		}
	}
	session.state = "stopped"
	session.mu.Unlock()
	close(session.shutdownDone)
}

func (session *Session) transitionLocked(ctx context.Context, daemonState attachedworkerdaemon.DaemonState, state string) error {
	if session.observation.Revision == ^uint64(0) {
		session.result.Code = localCode(attachedworkerlocal.ErrStateConflict)
		session.result.ObservationState = "unknown"
		return attachedworkerlocal.ErrStateConflict
	}
	observedAt := session.now().UTC()
	if observedAt.Before(session.observation.ObservedAt) {
		session.result.Code = localCode(attachedworkerlocal.ErrStateConflict)
		session.result.ObservationState = "unknown"
		return attachedworkerlocal.ErrStateConflict
	}
	next := session.observation
	next.Revision++
	next.State = daemonState
	next.Active = false
	next.ObservedAt = observedAt
	if err := session.lease.PersistObservation(ctx, next); err != nil {
		session.result.Code = localCode(err)
		session.result.ObservationState = "unknown"
		return err
	}
	session.observation = next
	session.result.ObservationRevision = next.Revision
	session.result.ObservationState = "observed_local"
	if daemonState == attachedworkerdaemon.DaemonDraining {
		session.result.DaemonState = "draining"
	} else {
		session.result.DaemonState = "not_started"
	}
	session.state = state
	return nil
}

// Result returns a race-free snapshot of bounded local lifecycle evidence.
func (session *Session) Result() ResultV1 {
	if session == nil {
		return emptyResult()
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.result
}

func emptyResult() ResultV1 {
	return ResultV1{
		Version: resultVersionV1, Code: CodeInvalid, FeatureState: "disabled",
		RuntimeOwnership: "not_acquired", ObservationState: "not_written",
		DaemonState: "not_started", ServerConnectionState: "unknown",
		NetworkAction: "not_attempted", ProcessAction: "not_attempted",
		CredentialAction: "not_attempted",
	}
}

func localCode(err error) ResultCode {
	return ResultCode(attachedworkerlocal.Code(err))
}

func clearSecret(secret *attachedworkerlocal.SecretRecordV1) {
	if secret == nil {
		return
	}
	for index := range secret.IdentityPrivateKey {
		secret.IdentityPrivateKey[index] = 0
	}
	for index := range secret.ConnectionSecret {
		secret.ConnectionSecret[index] = 0
	}
	*secret = attachedworkerlocal.SecretRecordV1{}
}
