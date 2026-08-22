// Package credentiallifecycle implements the provider-neutral local Phase B0
// credential state machine. It is intentionally not wired into worker runtime.
package credentiallifecycle

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	handle           ports.CredentialHandle
	binding          domain.CredentialBinding
	materialization  ports.CredentialMaterialization
	materialized     bool
	writeBackStarted bool
	revoked          bool
	released         bool
	cleanupDone      bool
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
	resolvedRoot, err := filepath.EvalSymlinks(scratchRoot)
	if err != nil || resolvedRoot != scratchRoot {
		return nil, ErrCredentialMaterialization
	}
	scratchInfo, err := os.Lstat(scratchRoot)
	if err != nil || !scratchInfo.IsDir() || scratchInfo.Mode()&os.ModeSymlink != 0 {
		return nil, ErrCredentialMaterialization
	}
	rootDir, err := os.MkdirTemp(scratchRoot, "sessionless-credentials-")
	if err != nil {
		return nil, ErrCredentialMaterialization
	}
	if err := os.Chmod(rootDir, 0o700); err != nil {
		_ = os.Remove(rootDir)
		return nil, ErrCredentialMaterialization
	}
	return &Service{
		rootDir: rootDir, maxBytes: config.MaxAuthBytes, clock: config.Clock,
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
	materialization, err := service.writeMaterialization(secret)
	if err != nil {
		return ports.CredentialMaterialization{}, err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	current, exists := service.handles[handle.HandleID]
	if !exists || current != state || current.revoked || current.released || current.handle != handle {
		_ = service.cleanupMaterialization(materialization)
		return ports.CredentialMaterialization{}, ErrCredentialDenied
	}
	current.materialization = materialization
	return materialization, nil
}

func (service *Service) WriteBack(
	ctx context.Context,
	handle ports.CredentialHandle,
	materialization ports.CredentialMaterialization,
) (ports.CredentialWriteBackResult, error) {
	if err := service.beginWriteBack(handle, materialization); err != nil {
		return ports.CredentialWriteBackResult{}, err
	}
	binding, err := service.loadAuthorizedBinding(ctx, handle)
	if err != nil {
		return ports.CredentialWriteBackResult{}, err
	}
	content, err := readRegularBounded(materialization, service.maxBytes)
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
	if state.released && state.cleanupDone {
		service.mu.Unlock()
		return nil
	}
	state.released = true
	materialization := state.materialization
	materialized := state.materialized
	service.mu.Unlock()
	if materialized && materialization.RootDir != "" {
		if err := service.cleanupMaterialization(materialization); err != nil {
			return ErrCredentialMaterialization
		}
	}
	service.mu.Lock()
	state.cleanupDone = true
	service.mu.Unlock()
	return nil
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
	type pendingCleanup struct {
		state           *handleState
		materialization ports.CredentialMaterialization
	}
	cleanups := make([]pendingCleanup, 0)
	service.mu.Lock()
	for _, state := range service.handles {
		if state.handle.TenantID == request.TenantID &&
			state.handle.SubscriptionConnectionID == request.SubscriptionConnectionID &&
			state.handle.OwnerUserID == request.OwnerUserID {
			state.revoked = true
			state.released = true
			if state.materialized && !state.cleanupDone && state.materialization.RootDir != "" {
				cleanups = append(cleanups, pendingCleanup{state: state, materialization: state.materialization})
			}
		}
	}
	service.mu.Unlock()
	cleanupFailed := false
	for _, cleanup := range cleanups {
		if err := service.cleanupMaterialization(cleanup.materialization); err != nil {
			cleanupFailed = true
			continue
		}
		service.mu.Lock()
		cleanup.state.cleanupDone = true
		service.mu.Unlock()
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
) error {
	if err := handle.Validate(); err != nil {
		return ErrCredentialDenied
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	state, exists := service.handles[handle.HandleID]
	if !exists || state.handle != handle || state.revoked || state.released {
		return ErrCredentialDenied
	}
	if !service.clock.Now().UTC().Before(handle.ExpiresAt) {
		return ErrCredentialExpired
	}
	if !state.materialized || state.materialization != materialization {
		return ErrCredentialMaterialization
	}
	if state.writeBackStarted {
		return ErrCredentialConsumed
	}
	state.writeBackStarted = true
	return nil
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

func (service *Service) writeMaterialization(secret []byte) (ports.CredentialMaterialization, error) {
	root, err := os.MkdirTemp(service.rootDir, "invocation-")
	if err != nil {
		return ports.CredentialMaterialization{}, ErrCredentialMaterialization
	}
	if err := os.Chmod(root, 0o700); err != nil {
		_ = os.Remove(root)
		return ports.CredentialMaterialization{}, ErrCredentialMaterialization
	}
	authFile := filepath.Join(root, "auth.json")
	file, err := os.OpenFile(authFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		_ = os.Remove(root)
		return ports.CredentialMaterialization{}, ErrCredentialMaterialization
	}
	_, writeErr := file.Write(secret)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil || os.Chmod(authFile, 0o600) != nil {
		_ = os.Remove(authFile)
		_ = os.Remove(root)
		return ports.CredentialMaterialization{}, ErrCredentialMaterialization
	}
	materialization := ports.CredentialMaterialization{RootDir: root, AuthFile: authFile}
	verified, err := readRegularBounded(materialization, service.maxBytes)
	if err != nil {
		_ = service.cleanupMaterialization(materialization)
		return ports.CredentialMaterialization{}, err
	}
	zero(verified)
	return materialization, nil
}

func readRegularBounded(materialization ports.CredentialMaterialization, maxBytes int64) ([]byte, error) {
	root := filepath.Clean(materialization.RootDir)
	authFile := filepath.Clean(materialization.AuthFile)
	if !filepath.IsAbs(root) || !filepath.IsAbs(authFile) || root != materialization.RootDir ||
		authFile != materialization.AuthFile || filepath.Dir(authFile) != root || filepath.Base(authFile) != "auth.json" {
		return nil, ErrCredentialMaterialization
	}
	if err := validateSecureDirectory(root); err != nil {
		return nil, err
	}
	before, err := os.Lstat(authFile)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm() != 0o600 {
		return nil, ErrCredentialMaterialization
	}
	file, err := os.Open(authFile)
	if err != nil {
		return nil, ErrCredentialMaterialization
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return nil, ErrCredentialMaterialization
	}
	content, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || int64(len(content)) > maxBytes || len(content) == 0 {
		zero(content)
		return nil, ErrCredentialMaterialization
	}
	return content, nil
}

func validateSecureDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return ErrCredentialMaterialization
	}
	return nil
}

func (service *Service) cleanupMaterialization(materialization ports.CredentialMaterialization) error {
	root := filepath.Clean(materialization.RootDir)
	authFile := filepath.Clean(materialization.AuthFile)
	if root != materialization.RootDir || authFile != materialization.AuthFile ||
		filepath.Dir(root) != service.rootDir || !strings.HasPrefix(filepath.Base(root), "invocation-") ||
		filepath.Dir(authFile) != root || filepath.Base(authFile) != "auth.json" {
		return ErrCredentialMaterialization
	}
	if err := validateSecureDirectory(root); err != nil {
		return err
	}
	info, err := os.Lstat(authFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return ErrCredentialMaterialization
	}
	if err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			return ErrCredentialMaterialization
		}
		if err := os.Remove(authFile); err != nil {
			return err
		}
	}
	if err := os.Remove(root); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
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
