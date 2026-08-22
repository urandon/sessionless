// Package credentiallifecycle implements the provider-neutral local Phase B0
// credential state machine. It is intentionally not wired into worker runtime.
package credentiallifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrCredentialDenied          = errors.New("credential invocation is not authorized")
	ErrCredentialExpired         = errors.New("credential invocation has expired")
	ErrCredentialConsumed        = errors.New("credential invocation has already been consumed")
	ErrCredentialStale           = errors.New("credential binding generation is stale")
	ErrCredentialBackend         = errors.New("credential backend operation failed")
	ErrCredentialMaterialization = errors.New("credential materialization is invalid")
	ErrCredentialInterrupted     = errors.New("credential lifecycle operation interrupted")
)

type FailurePoint string

const (
	FailureAfterCandidatePut FailurePoint = "after_candidate_put"
	FailureAfterBindingCAS   FailurePoint = "after_binding_cas"
	FailureBeforeAuthRead    FailurePoint = "before_auth_read"
	FailureBeforeCleanup     FailurePoint = "before_cleanup"
)

type FailureInjector interface {
	Check(FailurePoint) error
}

type Config struct {
	ScratchRoot  string
	MaxAuthBytes int64
	Clock        ports.Clock
	IDs          ports.IDGenerator
	Failures     FailureInjector
}

type Service struct {
	rootDir  string
	fs       *secureCredentialFS
	maxBytes int64
	clock    ports.Clock
	ids      ports.IDGenerator
	bindings ports.CredentialBindingStore
	secrets  ports.CredentialSecretStore
	failures FailureInjector

	mu      sync.Mutex
	handles map[string]*handleState
}

type handleState struct {
	handle            ports.CredentialHandle
	binding           domain.CredentialBinding
	materialization   *pinnedMaterialization
	materialized      bool
	writeBackStarted  bool
	revoked           bool
	released          bool
	cleanupDone       bool
	cleanupInProgress bool
	cleanupWait       chan struct{}
	cleanupWaiterSeen chan struct{}
	cleanupWaiters    int
	cleanupErr        error
}

func New(
	config Config,
	bindings ports.CredentialBindingStore,
	secrets ports.CredentialSecretStore,
) (*Service, error) {
	if config.Clock == nil || config.IDs == nil || bindings == nil || secrets == nil {
		return nil, errors.New("credential lifecycle dependencies must not be nil")
	}
	if config.MaxAuthBytes == 0 {
		config.MaxAuthBytes = 1 << 20
	}
	if config.MaxAuthBytes < 1 {
		return nil, errors.New("credential auth byte bound must be positive")
	}
	scratchRoot := filepath.Clean(config.ScratchRoot)
	if !filepath.IsAbs(scratchRoot) || scratchRoot != config.ScratchRoot {
		return nil, errors.New("credential scratch root must be a normalized absolute path")
	}
	scratchInfo, err := os.Lstat(scratchRoot)
	if err != nil || !scratchInfo.IsDir() || scratchInfo.Mode()&os.ModeSymlink != 0 {
		return nil, ErrCredentialMaterialization
	}
	canonicalScratchRoot, err := filepath.EvalSymlinks(scratchRoot)
	if err != nil || !filepath.IsAbs(canonicalScratchRoot) {
		return nil, ErrCredentialMaterialization
	}
	filesystem, err := newSecureCredentialFS(canonicalScratchRoot)
	if err != nil {
		return nil, ErrCredentialMaterialization
	}
	return &Service{
		rootDir: filesystem.rootPath, fs: filesystem, maxBytes: config.MaxAuthBytes, clock: config.Clock,
		ids: config.IDs, bindings: bindings, secrets: secrets, failures: config.Failures,
		handles: make(map[string]*handleState),
	}, nil
}

func (service *Service) Issue(
	ctx context.Context,
	request ports.CredentialIssueRequest,
) (ports.CredentialHandle, error) {
	now := service.clock.Now().UTC()
	if err := request.ValidateAt(now); err != nil {
		return ports.CredentialHandle{}, err
	}
	binding, found, err := service.bindings.LoadCredentialBinding(
		ctx, request.Run.TenantID, request.Run.SubscriptionConnectionID,
	)
	if err != nil {
		return ports.CredentialHandle{}, ErrCredentialBackend
	}
	if !found || binding.Validate() != nil || binding.Revoked ||
		binding.Entitlement != domain.EntitlementActive ||
		binding.TenantID != request.Run.TenantID ||
		binding.SubscriptionConnectionID != request.Run.SubscriptionConnectionID ||
		binding.OwnerUserID != request.OwnerUserID {
		return ports.CredentialHandle{}, ErrCredentialDenied
	}
	if _, err := service.secrets.RecoverCredentialCandidate(ctx, binding); err != nil {
		return ports.CredentialHandle{}, ErrCredentialBackend
	}
	handleID, err := service.ids.NewID(ctx, ports.IDCredentialHandle)
	if err != nil || domain.ValidateOpaqueID("credential.handle_id", handleID) != nil {
		return ports.CredentialHandle{}, ErrCredentialBackend
	}
	handle := ports.CredentialHandle{
		HandleID: handleID, TenantID: binding.TenantID,
		SubscriptionConnectionID: binding.SubscriptionConnectionID,
		OwnerUserID:              request.OwnerUserID, RunID: request.Run.ID, AttemptID: request.Attempt.ID,
		WorkerID: request.Lease.WorkerID, LeaseID: request.Lease.ID,
		LeaseFence: request.Lease.FenceToken, BindingGeneration: binding.Generation,
		ExpiresAt: request.ExpiresAt,
	}
	if err := handle.Validate(); err != nil {
		return ports.CredentialHandle{}, ErrCredentialBackend
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if _, exists := service.handles[handle.HandleID]; exists {
		return ports.CredentialHandle{}, ErrCredentialBackend
	}
	service.handles[handle.HandleID] = &handleState{handle: handle, binding: binding}
	return handle, nil
}

func (service *Service) Materialize(
	ctx context.Context,
	handle ports.CredentialHandle,
) (ports.CredentialMaterialization, error) {
	state, err := service.beginMaterialize(handle)
	if err != nil {
		return ports.CredentialMaterialization{}, err
	}
	binding, err := service.loadAuthorizedBinding(ctx, handle)
	if err != nil {
		return ports.CredentialMaterialization{}, err
	}
	secret, err := service.secrets.ReadCredentialSecret(ctx, binding, service.maxBytes)
	if err != nil {
		return ports.CredentialMaterialization{}, ErrCredentialBackend
	}
	defer zero(secret)
	if int64(len(secret)) > service.maxBytes || len(secret) == 0 ||
		domain.FingerprintCredential(secret) != binding.SecretFingerprint {
		return ports.CredentialMaterialization{}, ErrCredentialDenied
	}
	materialization, err := service.fs.create(secret, service.maxBytes)
	if err != nil {
		return ports.CredentialMaterialization{}, err
	}
	service.mu.Lock()
	current, exists := service.handles[handle.HandleID]
	if !exists || current != state || current.revoked || current.released || current.handle != handle {
		service.mu.Unlock()
		_ = service.fs.cleanup(materialization)
		return ports.CredentialMaterialization{}, ErrCredentialDenied
	}
	current.materialization = materialization
	service.mu.Unlock()
	return materialization.public, nil
}

func (service *Service) WriteBack(
	ctx context.Context,
	handle ports.CredentialHandle,
	materialization ports.CredentialMaterialization,
) (ports.CredentialWriteBackResult, error) {
	state, err := service.beginWriteBack(handle, materialization)
	if err != nil {
		return ports.CredentialWriteBackResult{}, err
	}
	binding, err := service.loadAuthorizedBinding(ctx, handle)
	if err != nil {
		return ports.CredentialWriteBackResult{}, err
	}
	if service.interrupted(FailureBeforeAuthRead) {
		return ports.CredentialWriteBackResult{}, ErrCredentialInterrupted
	}
	content, err := service.fs.read(state.materialization, service.maxBytes)
	if err != nil {
		return ports.CredentialWriteBackResult{}, err
	}
	defer zero(content)
	fingerprint := domain.FingerprintCredential(content)
	if fingerprint == binding.SecretFingerprint {
		return ports.CredentialWriteBackResult{Generation: binding.Generation}, nil
	}
	scope := ports.CredentialCandidateScope{
		TenantID: binding.TenantID, SubscriptionConnectionID: binding.SubscriptionConnectionID,
		OwnerUserID: binding.OwnerUserID, ExpectedGeneration: binding.Generation,
	}
	candidate, err := service.secrets.PutCredentialCandidate(ctx, scope, content)
	if err != nil {
		return ports.CredentialWriteBackResult{}, ErrCredentialBackend
	}
	if candidate.Scope != scope || candidate.Reference.Validate() != nil ||
		candidate.Fingerprint != fingerprint {
		return ports.CredentialWriteBackResult{}, ErrCredentialBackend
	}
	if service.interrupted(FailureAfterCandidatePut) {
		return ports.CredentialWriteBackResult{}, ErrCredentialInterrupted
	}
	next := binding
	next.SecretRef = candidate.Reference
	next.SecretFingerprint = candidate.Fingerprint
	next.Generation++
	next.UpdatedAt = service.clock.Now().UTC()
	swapped, err := service.bindings.CompareAndSwapCredentialBinding(ctx, binding.Generation, next)
	if err != nil {
		return ports.CredentialWriteBackResult{}, ErrCredentialBackend
	}
	if !swapped {
		_ = service.secrets.DeleteCredentialCandidate(ctx, candidate)
		return ports.CredentialWriteBackResult{}, ErrCredentialStale
	}
	if service.interrupted(FailureAfterBindingCAS) {
		return ports.CredentialWriteBackResult{}, ErrCredentialInterrupted
	}
	if err := service.secrets.CommitCredentialCandidate(ctx, candidate); err != nil {
		return ports.CredentialWriteBackResult{}, ErrCredentialBackend
	}
	return ports.CredentialWriteBackResult{Changed: true, Generation: next.Generation}, nil
}

func (service *Service) Release(_ context.Context, handle ports.CredentialHandle) error {
	service.mu.Lock()
	state, exists := service.handles[handle.HandleID]
	if !exists || state.handle != handle {
		service.mu.Unlock()
		return ErrCredentialDenied
	}
	service.mu.Unlock()
	return service.cleanupHandle(state)
}

func (service *Service) RevokeConnection(
	ctx context.Context,
	request ports.CredentialRevokeRequest,
) error {
	if err := request.Validate(); err != nil {
		return err
	}
	revocation, err := service.bindings.RevokeCredentialBinding(ctx, request, service.clock.Now().UTC())
	if err != nil {
		return ErrCredentialBackend
	}
	if err := revocation.Binding.Validate(); err != nil || !revocation.Binding.Revoked ||
		revocation.Binding.TenantID != request.TenantID ||
		revocation.Binding.SubscriptionConnectionID != request.SubscriptionConnectionID ||
		revocation.Binding.OwnerUserID != request.OwnerUserID {
		return ErrCredentialBackend
	}
	cleanups := make([]*handleState, 0)
	service.mu.Lock()
	for _, state := range service.handles {
		if state.handle.TenantID == request.TenantID &&
			state.handle.SubscriptionConnectionID == request.SubscriptionConnectionID &&
			state.handle.OwnerUserID == request.OwnerUserID {
			state.revoked = true
			cleanups = append(cleanups, state)
		}
	}
	service.mu.Unlock()
	cleanupFailed := false
	for _, state := range cleanups {
		if err := service.cleanupHandle(state); err != nil {
			cleanupFailed = true
		}
	}
	secretDeleteFailed := false
	if !revocation.SupersededSecretRef.IsZero() {
		scope := ports.CredentialCandidateScope{
			TenantID: request.TenantID, SubscriptionConnectionID: request.SubscriptionConnectionID,
			OwnerUserID: request.OwnerUserID, ExpectedGeneration: revocation.SupersededGeneration,
		}
		if err := service.secrets.DeleteCredentialSecret(ctx, scope, revocation.SupersededSecretRef); err != nil {
			secretDeleteFailed = true
		}
	}
	if cleanupFailed {
		return ErrCredentialMaterialization
	}
	if secretDeleteFailed {
		return ErrCredentialBackend
	}
	return nil
}

func (service *Service) beginMaterialize(handle ports.CredentialHandle) (*handleState, error) {
	if err := handle.Validate(); err != nil {
		return nil, ErrCredentialDenied
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	state, exists := service.handles[handle.HandleID]
	if !exists || state.handle != handle || state.revoked || state.released {
		return nil, ErrCredentialDenied
	}
	if !service.clock.Now().UTC().Before(handle.ExpiresAt) {
		return nil, ErrCredentialExpired
	}
	if state.materialized {
		return nil, ErrCredentialConsumed
	}
	state.materialized = true
	return state, nil
}

func (service *Service) beginWriteBack(
	handle ports.CredentialHandle,
	materialization ports.CredentialMaterialization,
) (*handleState, error) {
	if err := handle.Validate(); err != nil {
		return nil, ErrCredentialDenied
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	state, exists := service.handles[handle.HandleID]
	if !exists || state.handle != handle || state.revoked || state.released {
		return nil, ErrCredentialDenied
	}
	if !service.clock.Now().UTC().Before(handle.ExpiresAt) {
		return nil, ErrCredentialExpired
	}
	if !state.materialized || state.materialization == nil || state.materialization.public != materialization {
		return nil, ErrCredentialMaterialization
	}
	if state.writeBackStarted {
		return nil, ErrCredentialConsumed
	}
	state.writeBackStarted = true
	return state, nil
}

func (service *Service) loadAuthorizedBinding(
	ctx context.Context,
	handle ports.CredentialHandle,
) (domain.CredentialBinding, error) {
	binding, found, err := service.bindings.LoadCredentialBinding(
		ctx, handle.TenantID, handle.SubscriptionConnectionID,
	)
	if err != nil {
		return domain.CredentialBinding{}, ErrCredentialBackend
	}
	if !found || binding.Validate() != nil || binding.Revoked ||
		binding.TenantID != handle.TenantID ||
		binding.SubscriptionConnectionID != handle.SubscriptionConnectionID ||
		binding.OwnerUserID != handle.OwnerUserID ||
		binding.Generation != handle.BindingGeneration {
		return domain.CredentialBinding{}, ErrCredentialStale
	}
	return binding, nil
}

func (service *Service) cleanupHandle(state *handleState) error {
	service.mu.Lock()
	if state.cleanupDone {
		service.mu.Unlock()
		return nil
	}
	if state.cleanupInProgress {
		wait := state.cleanupWait
		state.cleanupWaiters++
		if state.cleanupWaiters == 1 {
			close(state.cleanupWaiterSeen)
		}
		service.mu.Unlock()
		<-wait
		service.mu.Lock()
		state.cleanupWaiters--
		done := state.cleanupDone
		err := state.cleanupErr
		service.mu.Unlock()
		if done {
			return nil
		}
		return err
	}
	state.released = true
	state.cleanupInProgress = true
	state.cleanupWait = make(chan struct{})
	state.cleanupWaiterSeen = make(chan struct{})
	materialization := state.materialization
	service.mu.Unlock()

	var cleanupErr error
	if materialization != nil {
		if service.interrupted(FailureBeforeCleanup) {
			cleanupErr = ErrCredentialInterrupted
		} else if err := service.fs.cleanup(materialization); err != nil {
			cleanupErr = ErrCredentialMaterialization
		}
	}
	service.mu.Lock()
	state.cleanupInProgress = false
	state.cleanupErr = cleanupErr
	state.cleanupDone = cleanupErr == nil
	close(state.cleanupWait)
	service.mu.Unlock()
	return cleanupErr
}

// Close releases the service-root descriptor. Invocation cleanup remains the
// caller's responsibility through Release or RevokeConnection.
func (service *Service) Close() error {
	if service == nil || service.fs == nil {
		return nil
	}
	return service.fs.close()
}

func (service *Service) interrupted(point FailurePoint) bool {
	return service.failures != nil && service.failures.Check(point) != nil
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

var _ ports.CredentialLifecycle = (*Service)(nil)
