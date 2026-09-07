// Package attachedworkerforeground owns the feature-disabled foreground
// preflight and local-runtime lifecycle. It deliberately does not own
// enrollment, transport, attempt, provider, or isolation authority.
package attachedworkerforeground

import (
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
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

// Activation is the only future live-runtime seam visible to this package.
// The product constructor does not accept an implementation: the shipped
// foreground remains disabled until a later reviewed slice defines the
// connection-session and AW-03/AW-04 composition contracts.
type Activation interface {
	Run(context.Context, attachedworkerlocal.SnapshotV1) error
}

type localRuntimeLease interface {
	PersistObservation(context.Context, attachedworkerlocal.RuntimeObservationV1) error
	RetireObservation(context.Context) error
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
}

func New(store *attachedworkerlocal.Store, config Config) (*Foreground, error) {
	if store == nil {
		return nil, ErrInvalidConfiguration
	}
	return newForeground(storeAdapter{store: store}, config)
}

func newForeground(store localState, config Config) (*Foreground, error) {
	if store == nil || config.CleanupTimeout < 0 || config.CleanupTimeout > time.Minute {
		return nil, ErrInvalidConfiguration
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = 5 * time.Second
	}
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Foreground{store: store, now: config.Now, cleanupTimeout: config.CleanupTimeout}, nil
}

func (foreground *Foreground) Run(ctx context.Context) (result ResultV1, resultErr error) {
	result = emptyResult()
	if ctx == nil || ctx.Err() != nil || foreground == nil || foreground.store == nil || foreground.now == nil {
		result.Code = CodeInvalid
		return result, ErrInvalidConfiguration
	}
	lease, err := foreground.store.AcquireRuntime(ctx)
	if err != nil {
		result.Code = localCode(err)
		return result, err
	}
	result.RuntimeOwnership = "acquired"
	observationPersisted := false
	defer func() {
		var cleanupErr error
		if observationPersisted {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), foreground.cleanupTimeout)
			cleanupErr = lease.RetireObservation(cleanupCtx)
			cancel()
			if cleanupErr == nil {
				result.ObservationState = "retired"
			} else {
				result.ObservationState = "unknown"
			}
		}
		closeErr := lease.Close()
		if closeErr == nil {
			result.RuntimeOwnership = "released"
		} else {
			result.RuntimeOwnership = "unknown"
		}
		if cleanupErr != nil || closeErr != nil {
			resultErr = errors.Join(resultErr, cleanupErr, closeErr)
			if cleanupErr != nil {
				result.Code = localCode(cleanupErr)
			} else {
				result.Code = localCode(closeErr)
			}
		}
	}()

	snapshot, err := foreground.store.LoadSnapshot(ctx)
	if err != nil {
		result.Code = localCode(err)
		return result, err
	}
	result.Lifecycle = snapshot.Manifest.Lifecycle
	result.ManifestRevision = snapshot.Manifest.Revision
	if snapshot.Manifest.Lifecycle != attachedworkerlocal.LifecycleActive {
		result.Code = localCode(attachedworkerlocal.ErrSecretRetired)
		return result, attachedworkerlocal.ErrSecretRetired
	}
	secret, err := foreground.store.LoadSecret(ctx)
	if err != nil {
		result.Code = localCode(err)
		return result, err
	}
	clearSecret(&secret)

	observedAt := foreground.now().UTC()
	if observedAt.Before(snapshot.Manifest.UpdatedAt) ||
		(snapshot.ObservationPresent && observedAt.Before(snapshot.Observation.ObservedAt)) {
		result.Code = localCode(attachedworkerlocal.ErrStateConflict)
		return result, attachedworkerlocal.ErrStateConflict
	}
	observation := attachedworkerlocal.RuntimeObservationV1{
		Version: 1, Revision: 1, ManifestRevision: snapshot.Manifest.Revision,
		State: attachedworkerdaemon.DaemonStopped, Active: false, ObservedAt: observedAt,
		LastFailureCode: "feature_disabled",
	}
	if snapshot.ObservationPresent {
		if snapshot.Observation.Revision == ^uint64(0) {
			result.Code = localCode(attachedworkerlocal.ErrStateConflict)
			return result, attachedworkerlocal.ErrStateConflict
		}
		observation.Revision = snapshot.Observation.Revision + 1
		observation.Accepted = snapshot.Observation.Accepted
		observation.Completed = snapshot.Observation.Completed
		observation.Failed = snapshot.Observation.Failed
	}
	if err := lease.PersistObservation(ctx, observation); err != nil {
		result.Code = localCode(err)
		return result, err
	}
	observationPersisted = true
	result.ObservationRevision = observation.Revision
	result.ObservationState = "observed_local"
	result.Code = CodeFeatureDisabled
	return result, ErrFeatureDisabled
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
