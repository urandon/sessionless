package worker

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

var ErrLocalRuntime = errors.New("local worker runtime unavailable")

// LocalRuntimeConfig is trusted startup composition. Source must have been
// opened from the original installation/receipt. All dependencies are borrowed;
// there is no installer, resource discovery or model permission in this DATA.
type LocalRuntimeConfig struct {
	Source  *localoperator.Source
	Receipt ports.OwnedProfilePublicationReceiptV1
	DB      *sql.DB
	Blobs   ports.BlobStore
	Worker  Config
}

// LocalRuntime retains installation-level dependencies only. It never retains
// a message context, session, issuer, keeper, Manager or invocation state.
type LocalRuntime struct {
	config   LocalRuntimeConfig
	clock    ports.Clock
	capacity int
}

func nilLocalRuntimeDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

// NewLocalRuntime validates the historical installation DATA and constructor
// owners without SQL, sockets or preparing an invocation. Current authority is
// still established by the original Manager/Store at each admitted effect.
func NewLocalRuntime(c LocalRuntimeConfig, clock ports.Clock) (*LocalRuntime, error) {
	if c.Source == nil || c.DB == nil || nilLocalRuntimeDependency(c.Blobs) || nilLocalRuntimeDependency(clock) ||
		c.Worker.LocalOwnedSession != nil || c.Worker.ExecutionPreparer != nil || c.Worker.CredentialMode != CredentialDisabled || c.Worker.CredentialLifecycle != nil {
		return nil, ErrLocalRuntime
	}
	p := c.Receipt.Profile.Clone()
	digest, err := p.Digest()
	if err != nil || digest != c.Receipt.Digest || p.ValidateInitial() != nil || p.Resource.Kind != domain.ProviderResourceLocalEndpointV1 {
		return nil, ErrLocalRuntime
	}
	profile, err := c.Source.Configuration()
	if err != nil || profile.Auth != domain.ConfiguredInferenceAuthNoneV1 || profile.ValidateForOwnedProfile(p) != nil {
		return nil, ErrLocalRuntime
	}
	capacity := int(p.Limits.MaxOperations)
	if capacity <= 0 || uint64(capacity) != p.Limits.MaxOperations {
		return nil, ErrLocalRuntime
	}
	c.Receipt.Profile = p
	return &LocalRuntime{config: c, clock: clock, capacity: capacity}, nil
}

// RunOnce creates one fresh original factory tuple for exactly one ordinary
// Manager invocation, including early no-message/duplicate/preflight failures.
// Every return attempts the original bounded close/join; a cleanup error is
// surfaced, never evidence of successful joining or permission to replay work.
// The borrowed Source/DB/blob are never closed, renewed or replaced here.
func (r *LocalRuntime) RunOnce(ctx context.Context, queue ports.Queue) (outcome Outcome, resultErr error) {
	if r == nil || r.capacity <= 0 || nilLocalRuntimeDependency(r.clock) || nilLocalRuntimeDependency(ctx) || ctx.Err() != nil || nilLocalRuntimeDependency(queue) {
		return "", ErrLocalRuntime
	}
	prepare, err := serverlessharness.NewCapabilityIssuer(r.clock.Now, nil)
	if err != nil {
		return "", ErrLocalRuntime
	}
	operate, err := serverlessharness.NewOperationCapabilityIssuer(r.clock.Now, nil, r.capacity)
	if err != nil {
		return "", ErrLocalRuntime
	}
	c := r.config
	session, err := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{
		Source: c.Source, DB: c.DB, Blobs: c.Blobs, Prepare: prepare, Operate: operate,
		Backend: c.Receipt.Profile.Backend, Substrate: c.Receipt.Profile.Substrate,
		StoreOptions: ydbstore.Options{OwnedProfilePublicationVerifier: c.Source,
			AttemptSpendingPublicationVerifier: c.Source, OwnedAttemptEffectSourceVerifier: c.Source},
	})
	if err != nil {
		return "", ErrLocalRuntime
	}
	defer func() {
		if err := session.Close(); err != nil {
			outcome = ""
			resultErr = errors.Join(resultErr, ErrLocalRuntime)
		}
	}()
	state, err := session.WorkerState()
	if err != nil {
		return "", ErrLocalRuntime
	}
	config := c.Worker // never mutate shared startup/default configuration
	config.LocalOwnedSession = session
	manager, err := New(config, r.clock, queue, state, c.Blobs, nil)
	if err != nil {
		return "", ErrLocalRuntime
	}
	return manager.RunOnce(ctx)
}

func (*LocalRuntime) String() string     { return "LocalRuntime{private:[redacted]}" }
func (r *LocalRuntime) GoString() string { return r.String() }
