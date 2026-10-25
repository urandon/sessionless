package providerloopsession

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// CredentialedConfigV1 is trusted composition of the same original local
// Source/Store/bootstrap/operation owners and the existing credential lifecycle.
// It accepts neither an engine callback nor an endpoint/model/key override.
// It is not worker registration or permission to enable a real profile.
type CredentialedConfigV1 struct {
	Local       LocalConfigV1
	Credentials ports.ProviderResourceCredentialLifecycleV1
}

// Copies share the same one-use original invocation. The returning noauth
// Start/RunModelStep API is deliberately not exposed by this closed wrapper.
type CredentialedV1 struct{ state *credentialedState }
type credentialedState struct {
	session   *LocalV1
	lifecycle ports.ProviderResourceCredentialLifecycleV1
	claimed   atomic.Bool
}

func NewCredentialedV1(ctx context.Context, c CredentialedConfigV1) (*CredentialedV1, error) {
	if c.Credentials == nil {
		return nil, ErrSession
	}
	v := reflect.ValueOf(c.Credentials)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return nil, ErrSession
		}
	}
	s, err := newLocalSession(ctx, c.Local, true)
	if err != nil {
		return nil, ErrSession
	}
	return &CredentialedV1{state: &credentialedState{session: s, lifecycle: c.Credentials}}, nil
}

// Run consumes genuine original prepared authority before Issue, then executes
// the SAME fixed finite model/compaction engine inside one synchronous V1
// materialization. It returns the original secret-free terminal owner only
// AFTER actual transport disposal and credential Release. Canonical success
// still requires that owner's original output/cleanup/transactional completion.
// Original typed MCP continuation keeps unknown service money and its complete
// reservation unchanged. Real profile qualification and managed MVP acceptance
// remain separate from this bounded source implementation.
func (v *CredentialedV1) Run(ctx context.Context, request ports.ReserveAttemptEffectRequestV1) (result ModelStepResultV1, terminal *PreparedLocalTerminalV1, err error) {
	if v == nil || v.state == nil || ctx == nil || ctx.Err() != nil || !v.state.claimed.CompareAndSwap(false, true) {
		return result, nil, ErrSession
	}
	c := v.state
	if _, err := c.session.Start(ctx, request); err != nil {
		_ = c.session.Close()
		return result, nil, ErrSession
	}
	result, err = c.run(ctx)
	if err != nil {
		_ = c.session.Close()
		return result, nil, ErrIncomplete
	}
	terminal, err = c.session.PrepareTerminal(ctx)
	if err != nil {
		_ = c.session.Close()
		return result, nil, ErrIncomplete
	}
	return result, terminal, nil
}

func (v *CredentialedV1) Close() error {
	if v == nil || v.state == nil {
		return nil
	}
	return v.state.session.Close()
}

func (*CredentialedV1) String() string     { return "CredentialedV1{private:[redacted]}" }
func (v *CredentialedV1) GoString() string { return v.String() }

func ownedCredentialHandle(h ports.ProviderInvocationCredentialV1, r ports.ProviderCredentialIssueRequestV1) bool {
	// A foreign result is not a safely owned handle and must never be released
	// on this invocation's behalf. Expiry is validated separately, AFTER the
	// guard is installed, so invalid/narrowed lifetime cannot skip owned release.
	b := r.HarnessBinding
	return domain.ValidateOpaqueID("provider_credential.handle_id", h.HandleID) == nil && h.TenantID == b.TenantID && h.OwnerUserID == b.OwnerUserID &&
		h.RunID == b.RunID && h.AttemptID == b.AttemptID && h.WorkerID == r.WorkerID &&
		h.LeaseID == r.LeaseID && h.LeaseFence == r.LeaseFence && h.ProviderResource == b.Resource
}

func (c *credentialedState) run(ctx context.Context) (result ModelStepResultV1, err error) {
	s, call, finish, err := c.session.begin(ctx)
	if err != nil {
		return result, ErrSession
	}
	defer finish()
	ok := false
	defer func() {
		if !ok {
			s.runtimeFailed = true
			s.cancel()
		}
	}()
	if !s.credentialed || s.entry == nil || s.runtimeStarted || s.attempted {
		return result, ErrSession
	}
	active, err := s.entry.PrepareCredentialedExecution(call)
	if err != nil {
		return result, ErrSession
	}
	request := ports.ProviderCredentialIssueRequestV1{HarnessBinding: s.parent.Authority.HarnessBinding.Clone(), WorkerID: s.allocation.Lease.WorkerID,
		LeaseID: s.allocation.Lease.ID, LeaseFence: s.allocation.Lease.FenceToken, ExpiresAt: active}
	if request.Validate() != nil {
		return result, ErrSession
	}
	handle, issueErr := c.lifecycle.IssueProviderCredential(call, request)
	if !ownedCredentialHandle(handle, request) {
		return result, ErrIncomplete
	}
	// Install the one owned-handle guard BEFORE expiry checks, Materialize,
	// delivery validation or engine branches. It runs outside the callback,
	// after the materializer has cleared its bytes and released its mutex.
	defer func() {
		d, e := s.entry.BeginCredentialedCleanup()
		cleanup, cancel := context.WithDeadline(context.WithoutCancel(ctx), d)
		defer cancel()
		releaseErr := c.lifecycle.ReleaseProviderCredential(cleanup, handle)
		if e != nil || releaseErr != nil || cleanup.Err() != nil || !time.Now().Before(d) {
			ok = false
			err = ErrIncomplete
			return
		}
		if err == nil && ok {
			s.mu.Lock()
			s.credentialReleased = true
			s.mu.Unlock()
		}
	}()
	if issueErr != nil || handle.Validate() != nil || !handle.ExpiresAt.Equal(request.ExpiresAt) || call.Err() != nil {
		return result, ErrIncomplete
	}

	var callbackMu sync.Mutex
	accepting, called, invalid, running := true, false, false, false
	done := make(chan struct{})
	var callbackErr error
	materializeErr := c.lifecycle.MaterializeProviderCredential(call, handle, func(plan ports.ProviderCredentialMaterializationV1, borrowed []byte) (e error) {
		callbackMu.Lock()
		if !accepting || called {
			invalid = true
			callbackMu.Unlock()
			return ErrSession
		}
		called, running = true, true
		callbackMu.Unlock()
		began := false
		defer func() {
			d, cleanupErr := s.entry.BeginCredentialedCleanup()
			if cleanupErr != nil {
				e = ErrIncomplete
			}
			if began {
				cleanup, cancel := context.WithDeadline(context.WithoutCancel(ctx), d)
				endErr := s.keeper.EndCredentialScope(cleanup, d)
				if cleanupErr != nil || endErr != nil || cleanup.Err() != nil {
					e = ErrIncomplete
				}
				cancel()
			}
			callbackMu.Lock()
			callbackErr, running = e, false
			close(done)
			callbackMu.Unlock()
		}()
		if plan.Validate() != nil || plan.Kind != domain.ProviderCredentialDeliveryDirectV1 || plan.FilePath != "" || plan.EnvironmentName != "" || call.Err() != nil {
			return ErrSession
		}
		if s.keeper.BeginCredentialScope(call, handle, borrowed) != nil {
			return ErrSession
		}
		began = true
		result, e = s.runBoundedEngine(ctx, call)
		return e
	})
	callbackMu.Lock()
	accepting = false
	stillRunning := running
	if stillRunning {
		invalid = true
	}
	callbackMu.Unlock()
	if stillRunning {
		// A nonconforming asynchronous lifecycle is ineligible. Cancel and join
		// our actual engine before returning; never detach credential-bearing I/O.
		s.cancel()
		<-done
	}
	callbackMu.Lock()
	valid := called && !invalid && callbackErr == nil
	callbackMu.Unlock()
	if materializeErr != nil || !valid || call.Err() != nil {
		return result, ErrIncomplete
	}
	ok = true
	return result, nil
}
