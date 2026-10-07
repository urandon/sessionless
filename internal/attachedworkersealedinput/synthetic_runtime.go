package attachedworkersealedinput

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
)

// SyntheticRuntimeConfig is an explicit, default-off composition. It requires
// an already enrolled installation and operator-supplied HTTP, OCI, and
// transport configuration; no value is discovered from environment or remote
// frames. The shipped command does not call this constructor.
type SyntheticRuntimeConfig struct {
	Store          *attachedworkerlocal.Store
	Bootstrap      attachedworkersession.BootstrapPort
	Exchange       attachedworkersession.ExchangeFactory
	Session        attachedworkersession.Config
	Connect        attachedworkersession.ConnectInputV1
	SealedEndpoint string
	SealedClient   *http.Client
	MaxInputBytes  int
	Now            func() time.Time
	Adapter        attachedworkerdaemontransport.Config
	Poll           attachedworkertransport.Config
	Stack          attachedworkerstack.Config
	Runtime        attachedworkerdaemontransport.RuntimeConfig
}

type syntheticRuntimePort interface {
	Run(context.Context) error
	Drain(context.Context) error
	Shutdown(context.Context) error
	Wake() error
	Status() attachedworkerdaemon.Status
}

// SyntheticRuntime owns the authenticated exchange, its sealed-input bearer,
// and the single foreground lifecycle. Close can retire a constructed runtime
// before Run, or stop and wait for an active Run without taking a second daemon
// or process owner.
type SyntheticRuntime struct {
	runtime syntheticRuntimePort
	source  runtimeSource

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
	err     error
}

type runtimeSource interface {
	revokeSource() error
	Close() error
}

// ConnectSyntheticPinnedRuntime links the session-generated bearer to the
// bounded sealed-input materializer and the manifest-pinned runtime. Provider
// credential operations are denied; this cannot enable a real provider turn.
func ConnectSyntheticPinnedRuntime(ctx context.Context, config SyntheticRuntimeConfig) (*SyntheticRuntime, error) {
	return connectSyntheticPinnedRuntime(ctx, config, func(ctx context.Context, source *SessionSourceFactory, materializer *attachedworkerdaemontransport.BoundMaterializer) (syntheticRuntimePort, error) {
		runtime, err := attachedworkerdaemontransport.ConnectPinnedForegroundRuntime(
			ctx, config.Store, config.Bootstrap, source, config.Session, config.Connect,
			materializer, config.Adapter, config.Poll, config.Stack, DeniedCredentialLifecycle{}, config.Runtime,
		)
		if err != nil || runtime == nil {
			return nil, err
		}
		return runtime, nil
	})
}

// ReconnectSyntheticPinnedRuntime resumes only a durably reconciled idle
// installation. The underlying pinned connector compares the exact local
// checkpoint with the server head before it permits another poll. Active,
// draining, terminal, or ambiguous recovery never becomes process authority.
// Like the initial constructor, this entrypoint is library-only and default-off.
func ReconnectSyntheticPinnedRuntime(ctx context.Context, config SyntheticRuntimeConfig) (*SyntheticRuntime, error) {
	return connectSyntheticPinnedRuntime(ctx, config, func(ctx context.Context, source *SessionSourceFactory, materializer *attachedworkerdaemontransport.BoundMaterializer) (syntheticRuntimePort, error) {
		runtime, err := attachedworkerdaemontransport.ReconnectPinnedForegroundRuntime(
			ctx, config.Store, config.Bootstrap, source, config.Session,
			attachedworkersession.ReconnectInputV1(config.Connect), materializer, config.Adapter,
			config.Poll, config.Stack, DeniedCredentialLifecycle{}, config.Runtime,
		)
		if err != nil || runtime == nil {
			return nil, err
		}
		return runtime, nil
	})
}

func connectSyntheticPinnedRuntime(
	ctx context.Context,
	config SyntheticRuntimeConfig,
	connect func(context.Context, *SessionSourceFactory, *attachedworkerdaemontransport.BoundMaterializer) (syntheticRuntimePort, error),
) (*SyntheticRuntime, error) {
	if ctx == nil || ctx.Err() != nil || config.Store == nil || config.Bootstrap == nil || connect == nil {
		return nil, ErrInvalid
	}
	source, err := NewSessionSourceFactory(config.SealedEndpoint, config.SealedClient, config.Exchange)
	if err != nil {
		return nil, err
	}
	materializer, err := attachedworkerdaemontransport.NewBoundMaterializer(source, config.MaxInputBytes, config.Now)
	if err != nil {
		_ = source.Close()
		return nil, err
	}
	runtime, err := connect(ctx, source, materializer)
	if err != nil || runtime == nil {
		_ = source.Close()
		return nil, errors.Join(ErrUnavailable, err)
	}
	return &SyntheticRuntime{runtime: runtime, source: source, done: make(chan struct{})}, nil
}

func (owner *SyntheticRuntime) Run(ctx context.Context) error {
	if owner == nil {
		return ErrInvalid
	}
	if ctx == nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		return errors.Join(ErrInvalid, owner.Close(cleanupCtx))
	}
	owner.mu.Lock()
	if owner.started {
		owner.mu.Unlock()
		return attachedworkerdaemontransport.ErrRuntimeAlreadyUsed
	}
	runCtx, cancel := context.WithCancel(ctx)
	owner.started, owner.cancel = true, cancel
	owner.mu.Unlock()
	err := owner.runtime.Run(runCtx)
	cancel()
	err = errors.Join(err, owner.source.Close())
	owner.mu.Lock()
	owner.err = err
	close(owner.done)
	owner.mu.Unlock()
	return err
}

func (owner *SyntheticRuntime) Close(ctx context.Context) error {
	if owner == nil || ctx == nil {
		return ErrInvalid
	}
	owner.mu.Lock()
	if !owner.started {
		owner.started = true
		owner.mu.Unlock()
		go owner.retireBeforeRun()
		return owner.waitClosed(ctx)
	}
	cancel, done := owner.cancel, owner.done
	owner.mu.Unlock()
	select {
	case <-done:
		owner.mu.Lock()
		err := owner.err
		owner.mu.Unlock()
		return err
	default:
	}
	if cancel != nil {
		cancel()
	}
	// Cancelling the one Run owner stops admission and active execution. The
	// Run path performs daemon shutdown, observation retirement, session close,
	// and bearer scrubbing. No second shutdown goroutine may overtake it.
	return owner.waitClosed(ctx)
}

func (owner *SyntheticRuntime) retireBeforeRun() {
	// Revoke bearer/read authority first, even if the underlying runtime's
	// bounded session cleanup later stalls. This goroutine is the sole late
	// finalizer when a Close caller's wait deadline expires.
	sourceErr := owner.source.revokeSource()
	closedCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err := owner.runtime.Run(closedCtx)
	if err == context.Canceled {
		err = nil
	}
	err = errors.Join(err, sourceErr, owner.source.Close())
	owner.mu.Lock()
	owner.err = err
	close(owner.done)
	owner.mu.Unlock()
}

func (owner *SyntheticRuntime) waitClosed(ctx context.Context) error {
	select {
	case <-owner.done:
		owner.mu.Lock()
		err := owner.err
		owner.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (owner *SyntheticRuntime) Drain(ctx context.Context) error {
	if owner == nil {
		return ErrInvalid
	}
	return owner.runtime.Drain(ctx)
}

func (owner *SyntheticRuntime) Wake() error {
	if owner == nil {
		return ErrInvalid
	}
	return owner.runtime.Wake()
}

func (owner *SyntheticRuntime) Status() attachedworkerdaemon.Status {
	if owner == nil {
		return attachedworkerdaemon.Status{}
	}
	return owner.runtime.Status()
}
