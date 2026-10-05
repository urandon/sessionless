package attachedworkeractivation

import (
	"context"
	"errors"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerforeground"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
)

var ErrAlreadyRun = errors.New("attached worker activation owner has already run")

// Owner adapts the one synthetic runtime to the existing permission-bound
// local control surface. It never creates another daemon or process owner.
type Owner struct {
	runtime runtimePort
	profile ProfileV1

	mu      sync.Mutex
	started bool
	done    bool
	err     error
}

type runtimePort interface {
	Run(context.Context) error
	Drain(context.Context) error
	Close(context.Context) error
	Status() attachedworkerdaemon.Status
}

func NewOwner(runtime *attachedworkersealedinput.SyntheticRuntime, profile ProfileV1) (*Owner, error) {
	if runtime == nil {
		return nil, ErrInvalidProfile
	}
	return newOwner(runtime, profile)
}

func newOwner(runtime runtimePort, profile ProfileV1) (*Owner, error) {
	if runtime == nil || profile.Version != 1 || profile.Mode != "synthetic-denied" || profile.ManifestRevision == 0 {
		return nil, ErrInvalidProfile
	}
	return &Owner{runtime: runtime, profile: profile}, nil
}

func (owner *Owner) Run(ctx context.Context) error {
	if owner == nil || ctx == nil {
		return ErrInvalidProfile
	}
	owner.mu.Lock()
	if owner.started {
		owner.mu.Unlock()
		return ErrAlreadyRun
	}
	owner.started = true
	owner.mu.Unlock()
	err := owner.runtime.Run(ctx)
	owner.mu.Lock()
	owner.done, owner.err = true, err
	owner.mu.Unlock()
	return err
}

// WaitReady prevents the local control socket from becoming visible until
// the sole daemon has actually entered its running state. An early exit is
// failure, never a briefly live endpoint with no runtime behind it.
func (owner *Owner) WaitReady(ctx context.Context) error {
	if owner == nil || ctx == nil {
		return ErrInvalidProfile
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		owner.mu.Lock()
		started, done, prior := owner.started, owner.done, owner.err
		owner.mu.Unlock()
		if done {
			return errors.Join(ErrInvalidProfile, prior)
		}
		if started && owner.runtime.Status().State == attachedworkerdaemon.DaemonRunning {
			owner.mu.Lock()
			done, prior = owner.done, owner.err
			owner.mu.Unlock()
			if done {
				return errors.Join(ErrInvalidProfile, prior)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (owner *Owner) Drain(ctx context.Context) error {
	if owner == nil {
		return ErrInvalidProfile
	}
	owner.mu.Lock()
	done, started, prior := owner.done, owner.started, owner.err
	owner.mu.Unlock()
	if done {
		return prior
	}
	if started && owner.runtime.Status().State == attachedworkerdaemon.DaemonStopped {
		// Run has not begun or has already closed admission. The service
		// readiness barrier prevents exposure during the former case.
		return nil
	}
	return owner.runtime.Drain(ctx)
}

func (owner *Owner) Shutdown(ctx context.Context) error {
	if owner == nil {
		return ErrInvalidProfile
	}
	owner.mu.Lock()
	done, prior := owner.done, owner.err
	owner.mu.Unlock()
	if done {
		return prior
	}
	return owner.runtime.Close(ctx)
}

func (owner *Owner) Result() attachedworkerforeground.ResultV1 {
	if owner == nil {
		return attachedworkerforeground.ResultV1{Version: 1, Code: attachedworkerforeground.CodeInvalid}
	}
	result := attachedworkerforeground.ResultV1{
		Version: 1, Code: "synthetic_active", Lifecycle: attachedworkerlocal.LifecycleActive,
		ManifestRevision: owner.profile.ManifestRevision,
		RuntimeOwnership: "acquired", ObservationState: "unknown",
		ServerConnectionState: "authenticated", NetworkAction: "outbound_authenticated",
		ProcessAction: "pinned_only", CredentialAction: "denied",
		FeatureState: "synthetic_denied",
	}
	status := owner.runtime.Status()
	result.DaemonState = string(status.State)
	owner.mu.Lock()
	if owner.done {
		if owner.err != nil && !errors.Is(owner.err, context.Canceled) {
			result.Code = "runtime_failed"
			result.RuntimeOwnership = "unknown"
			result.ServerConnectionState = "unknown"
		} else {
			result.Code = "synthetic_stopped"
			result.RuntimeOwnership = "released"
			result.ServerConnectionState = "closed"
		}
	}
	owner.mu.Unlock()
	return result
}
