package ownerlocalcredential

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Lifecycle is the admission guard around the original lifecycle, not another
// issuer, generation authority or durable lease. Only its exact original handle
// may own the one refresh window. Revoke remains independently callable.
type Lifecycle struct{ *lifecycle }

type lifecycle struct {
	mu      sync.Mutex
	store   *Store
	service *credentiallifecycle.Service
	handle  *ports.CredentialHandle
	retired *ports.CredentialHandle
	closed  atomic.Bool
}

func NewLifecycle(store *Store, config credentiallifecycle.Config) (*Lifecycle, error) {
	if store == nil || store.store == nil {
		return nil, ErrDenied
	}
	if config.MaxAuthBytes < 0 || config.MaxAuthBytes > maxAuthBytes {
		return nil, ErrDenied
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed || store.unknown || store.wrapper || store.guard.Load() {
		return nil, ErrBusy
	}
	rel, err := filepath.Rel(store.config.Root, config.ScratchRoot)
	if err != nil || rel == "." || !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, ErrDenied
	}
	service, err := credentiallifecycle.New(config, store, store)
	if err != nil {
		return nil, ErrDenied
	}
	store.wrapper = true
	return &Lifecycle{lifecycle: &lifecycle{store: store, service: service}}, nil
}

func (v *Lifecycle) Issue(ctx context.Context, request ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	if v == nil || v.lifecycle == nil {
		return ports.CredentialHandle{}, ErrDenied
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed.Load() || !v.store.guard.CompareAndSwap(false, true) {
		return ports.CredentialHandle{}, ErrBusy
	}
	scope := v.store.config.Scope
	if request.OwnerUserID != scope.OwnerUserID || request.Run.TenantID != scope.TenantID || request.Run.SubscriptionConnectionID != scope.SubscriptionConnectionID {
		v.store.guard.Store(false)
		return ports.CredentialHandle{}, ErrDenied
	}
	handle, err := v.service.Issue(ctx, request)
	if err != nil {
		v.store.guard.Store(false)
		return ports.CredentialHandle{}, err
	}
	v.handle = &handle
	return handle, nil
}

func (v *Lifecycle) exactLocked(handle ports.CredentialHandle) bool {
	return !v.closed.Load() && v.store.guard.Load() && v.handle != nil && *v.handle == handle
}

func (v *Lifecycle) Materialize(ctx context.Context, handle ports.CredentialHandle) (ports.CredentialMaterialization, error) {
	if v == nil || v.lifecycle == nil {
		return ports.CredentialMaterialization{}, ErrDenied
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.exactLocked(handle) {
		return ports.CredentialMaterialization{}, ErrDenied
	}
	return v.service.Materialize(ctx, handle)
}
func (v *Lifecycle) WriteBack(ctx context.Context, handle ports.CredentialHandle, materialization ports.CredentialMaterialization) (ports.CredentialWriteBackResult, error) {
	if v == nil || v.lifecycle == nil {
		return ports.CredentialWriteBackResult{}, ErrDenied
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.exactLocked(handle) {
		return ports.CredentialWriteBackResult{}, ErrDenied
	}
	return v.service.WriteBack(ctx, handle, materialization)
}
func (v *Lifecycle) Release(ctx context.Context, handle ports.CredentialHandle) error {
	if v == nil || v.lifecycle == nil {
		return ErrDenied
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed.Load() {
		return ErrDenied
	}
	if v.retired != nil && *v.retired == handle {
		return nil
	}
	if v.handle == nil || *v.handle != handle || !v.store.guard.Load() {
		return ErrDenied
	}
	if err := v.service.Release(ctx, handle); err != nil {
		return err
	}
	if err := v.store.released(ctx, handle); err != nil {
		return err
	}
	if err := v.store.Reconcile(ctx); err != nil {
		return err
	}
	if v.handle != nil && *v.handle == handle {
		v.retired = v.handle
		v.handle = nil
		v.store.guard.Store(false)
	}
	return nil
}
func (v *Lifecycle) RevokeConnection(ctx context.Context, request ports.CredentialRevokeRequest) error {
	if v == nil || v.lifecycle == nil {
		return ErrDenied
	}
	if v.closed.Load() {
		return ErrDenied
	}
	// The service's deny-first CAS and original materialization cleanup own
	// revocation. It neither waits for nor retires this invocation guard.
	return v.service.RevokeConnection(ctx, request)
}
func (v *Lifecycle) Close() error {
	if v == nil || v.lifecycle == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed.Load() {
		return v.store.Close()
	}
	if v.store.guard.Load() {
		return ErrBusy
	}
	if err := v.service.Close(); err != nil {
		return ErrUnavailable
	}
	v.closed.Store(true)
	v.store.mu.Lock()
	v.store.wrapper = false
	v.store.mu.Unlock()
	if err := v.store.Close(); err != nil {
		return err
	}
	return nil
}

var _ ports.CredentialLifecycle = (*Lifecycle)(nil)
