package providercredential

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrExpired          = &Failure{Code: FailureExpiredV1}
	ErrConsumed         = &Failure{Code: FailureConsumedV1}
	ErrDeliveryMismatch = &Failure{Code: FailureDeliveryMismatchV1}
)

type InvocationConfig struct {
	Clock          ports.Clock
	IDs            ports.IDGenerator
	ScratchRoot    string
	MaxSecretBytes int64
}

type InvocationService struct {
	clock    ports.Clock
	ids      ports.IDGenerator
	root     string
	maxBytes int64
	planner  ports.ProviderCredentialDeliveryPlannerV1
	bindings ports.ProviderCredentialBindingStore
	secrets  ports.ProviderCredentialSecretStore
	files    *credentiallifecycle.PinnedSecretFileSystem
	mu       sync.Mutex
	handles  map[string]*providerInvocationState
}

type providerInvocationState struct {
	mu       sync.Mutex
	handle   ports.ProviderInvocationCredentialV1
	template ports.ProviderCredentialDeliveryTemplateV1
	file     *credentiallifecycle.PinnedSecretFile
	consumed bool
	revoked  bool
	released bool
	// These values describe local immutable materialization, not owner, Run or
	// remote-phase permission. CandidateMutationID is only mutation metadata.
	binding           domain.ProviderCredentialBindingV1
	plan              ports.ProviderCredentialMaterializationV1
	callbackSucceeded bool
	finalizeAttempted bool
	finalized         bool
}

func NewInvocationService(config InvocationConfig, planner ports.ProviderCredentialDeliveryPlannerV1, bindings ports.ProviderCredentialBindingStore, secrets ports.ProviderCredentialSecretStore) (*InvocationService, error) {
	if config.Clock == nil || config.IDs == nil || planner == nil || bindings == nil || secrets == nil {
		return nil, errors.New("provider credential invocation dependencies must not be nil")
	}
	if config.MaxSecretBytes == 0 {
		config.MaxSecretBytes = 8 << 10
	}
	if config.MaxSecretBytes < 1 || config.MaxSecretBytes > 64<<10 {
		return nil, errors.New("provider credential invocation byte bound is invalid")
	}
	return &InvocationService{clock: config.Clock, ids: config.IDs, root: config.ScratchRoot, maxBytes: config.MaxSecretBytes, planner: planner, bindings: bindings, secrets: secrets, handles: make(map[string]*providerInvocationState)}, nil
}

func (service *InvocationService) IssueProviderCredential(ctx context.Context, request ports.ProviderCredentialIssueRequestV1) (ports.ProviderInvocationCredentialV1, error) {
	if ctx == nil || request.Validate() != nil {
		return ports.ProviderInvocationCredentialV1{}, ErrInvalid
	}
	now := service.clock.Now().UTC()
	if !now.Before(request.ExpiresAt) || request.HarnessBinding.EvidenceExpiresAt == nil || !now.Before(*request.HarnessBinding.EvidenceExpiresAt) {
		return ports.ProviderInvocationCredentialV1{}, ErrExpired
	}
	template, err := service.planner.PlanProviderCredentialDelivery(ctx, request.HarnessBinding.Clone())
	if err != nil {
		return ports.ProviderInvocationCredentialV1{}, ErrBackend
	}
	if err := template.ValidateForBinding(request.HarnessBinding); err != nil {
		return ports.ProviderInvocationCredentialV1{}, ErrDeliveryMismatch
	}
	// File delivery must establish and recover its pinned scratch namespace
	// before any credential backend operation. Environment and direct delivery
	// never create a file and remain portable without a scratch root.
	if template.Kind == domain.ProviderCredentialDeliveryFileV1 {
		if err := service.ensureFileSystem(); err != nil {
			return ports.ProviderInvocationCredentialV1{}, ErrDeliveryMismatch
		}
	}
	locator := ports.ProviderCredentialLocatorV1{TenantID: request.HarnessBinding.TenantID, OwnerUserID: request.HarnessBinding.OwnerUserID, ResourceKind: request.HarnessBinding.Resource.Kind, ResourceID: request.HarnessBinding.Resource.ResourceID}
	binding, found, err := service.bindings.LoadProviderCredential(ctx, locator)
	if err != nil {
		return ports.ProviderInvocationCredentialV1{}, ErrBackend
	}
	if !found || !providerCredentialBindingMatchesResource(binding, request.HarnessBinding.Resource) {
		return ports.ProviderInvocationCredentialV1{}, ErrNotFound
	}
	recovered, err := service.secrets.RecoverProviderCredentialCandidate(ctx, binding)
	if err != nil {
		return ports.ProviderInvocationCredentialV1{}, ErrBackend
	}
	if !recovered {
		return ports.ProviderInvocationCredentialV1{}, ErrNotFound
	}
	handleID, err := service.ids.NewID(ctx, ports.IDCredentialHandle)
	if err != nil || domain.ValidateOpaqueID("provider_credential.handle_id", handleID) != nil {
		return ports.ProviderInvocationCredentialV1{}, ErrBackend
	}
	handle := ports.ProviderInvocationCredentialV1{
		HandleID: handleID, TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID,
		RunID: request.HarnessBinding.RunID, AttemptID: request.HarnessBinding.AttemptID,
		WorkerID: request.WorkerID, LeaseID: request.LeaseID, LeaseFence: request.LeaseFence,
		ProviderResource: request.HarnessBinding.Resource, ExpiresAt: canonicalTime(request.ExpiresAt),
	}
	if err := handle.Validate(); err != nil {
		return ports.ProviderInvocationCredentialV1{}, ErrBackend
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if _, exists := service.handles[handle.HandleID]; exists {
		return ports.ProviderInvocationCredentialV1{}, ErrBackend
	}
	service.handles[handle.HandleID] = &providerInvocationState{handle: handle, template: template}
	return handle, nil
}

func (service *InvocationService) MaterializeProviderCredential(ctx context.Context, handle ports.ProviderInvocationCredentialV1, consume ports.ProviderCredentialConsumerV1) error {
	if ctx == nil || consume == nil || handle.Validate() != nil {
		return ErrInvalid
	}
	service.mu.Lock()
	state, found := service.handles[handle.HandleID]
	service.mu.Unlock()
	if !found {
		return ErrNotFound
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.handle != handle || state.revoked || state.released {
		return ErrNotFound
	}
	if state.consumed {
		return ErrConsumed
	}
	if !service.clock.Now().UTC().Before(handle.ExpiresAt) {
		return ErrExpired
	}
	state.consumed = true

	locator := ports.ProviderCredentialLocatorV1{TenantID: handle.TenantID, OwnerUserID: handle.OwnerUserID, ResourceKind: handle.ProviderResource.Kind, ResourceID: handle.ProviderResource.ResourceID}
	binding, found, err := service.bindings.LoadProviderCredential(ctx, locator)
	if err != nil {
		return ErrBackend
	}
	if !found || !providerCredentialBindingMatchesResource(binding, handle.ProviderResource) {
		return ErrNotFound
	}
	secret, err := service.secrets.ReadProviderCredentialSecret(ctx, binding, service.maxBytes)
	if err != nil {
		return ErrBackend
	}
	defer zero(secret)
	if len(secret) == 0 || int64(len(secret)) > service.maxBytes || domain.FingerprintCredential(secret) != binding.SecretFingerprint {
		return ErrNotFound
	}
	plan, err := service.materializationPlanLocked(state, secret)
	if err != nil {
		return err
	}
	if handle.ProviderResource.Kind == domain.ProviderResourceAPIAccountV1 && plan.Kind == domain.ProviderCredentialDeliveryFileV1 {
		state.binding, state.plan = binding, plan
	}
	callbackSecret := secret
	if plan.Kind == domain.ProviderCredentialDeliveryFileV1 {
		callbackSecret = nil
	}
	if err := consume(plan, callbackSecret); err != nil {
		return ErrBackend
	}
	state.callbackSucceeded = true
	return nil
}

// FinalizeProviderCredentialFile verifies local immutable integrity only.
// The last binding read is a current-generation snapshot, not an atomic FS/DB
// observation or a fresh remote immutable_finalize/owner/head authorization.
func (service *InvocationService) FinalizeProviderCredentialFile(ctx context.Context, handle ports.ProviderInvocationCredentialV1, plan ports.ProviderCredentialMaterializationV1) error {
	if ctx == nil || handle.Validate() != nil || plan.Validate() != nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	service.mu.Lock()
	state, found := service.handles[handle.HandleID]
	// The original handle is immutable. Compare its complete tuple before
	// checking busy state so a foreign tuple cannot observe callback activity.
	if !found || state.handle != handle {
		service.mu.Unlock()
		return ErrNotFound
	}
	service.mu.Unlock()
	if !state.mu.TryLock() {
		return ErrConsumed
	}
	defer state.mu.Unlock()
	if handle.ProviderResource.Kind != domain.ProviderResourceAPIAccountV1 || state.template.Kind != domain.ProviderCredentialDeliveryFileV1 || plan != state.plan {
		return ErrDeliveryMismatch
	}
	if state.revoked || state.released || !state.callbackSucceeded || state.file == nil {
		return ErrNotFound
	}
	if state.finalizeAttempted {
		return ErrConsumed
	}
	// Every begun attempt is irreversible, including cancelled, stale or
	// ambiguous work. A failure cannot be retried or become cached permission.
	state.finalizeAttempted = true
	if err := service.immutableFinalizeLive(ctx, handle); err != nil {
		return err
	}
	if err := service.immutableFinalizeCurrentBinding(ctx, state); err != nil {
		return err
	}
	if err := service.immutableFinalizeLive(ctx, handle); err != nil {
		return err
	}
	// Lock order: per-handle state -> provider wrapper -> pinned file. No
	// lifecycle reentry or remote callback occurs while these locks are held.
	err := service.files.VerifyImmutable(ctx, state.file, state.binding.SecretFingerprint, service.maxBytes)
	if liveErr := service.immutableFinalizeLive(ctx, handle); liveErr != nil {
		return liveErr
	}
	if err != nil {
		return ErrDeliveryMismatch
	}
	if err := service.immutableFinalizeCurrentBinding(ctx, state); err != nil {
		return err
	}
	if err := service.immutableFinalizeLive(ctx, handle); err != nil {
		return err
	}
	state.finalized = true
	return nil
}

func (service *InvocationService) immutableFinalizeLive(ctx context.Context, handle ports.ProviderInvocationCredentialV1) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !service.clock.Now().UTC().Before(handle.ExpiresAt) {
		return ErrExpired
	}
	return nil
}

func (service *InvocationService) immutableFinalizeCurrentBinding(ctx context.Context, state *providerInvocationState) error {
	if err := service.immutableFinalizeLive(ctx, state.handle); err != nil {
		return err
	}
	locator := ports.ProviderCredentialLocatorV1{TenantID: state.handle.TenantID, OwnerUserID: state.handle.OwnerUserID, ResourceKind: state.handle.ProviderResource.Kind, ResourceID: state.handle.ProviderResource.ResourceID}
	current, found, err := service.bindings.LoadProviderCredential(ctx, locator)
	if liveErr := service.immutableFinalizeLive(ctx, state.handle); liveErr != nil {
		return liveErr
	}
	if err != nil {
		return ErrBackend
	}
	if !found || !providerCredentialBindingMatchesResource(current, state.handle.ProviderResource) || !current.UpdatedAt.Equal(state.binding.UpdatedAt) {
		return ErrNotFound
	}
	// Compare every actual canonical field, using instant equality for time
	// rather than representation equality (location/monotonic metadata).
	current.UpdatedAt = state.binding.UpdatedAt
	if current != state.binding {
		return ErrNotFound
	}
	return nil
}

func (service *InvocationService) ReleaseProviderCredential(ctx context.Context, handle ports.ProviderInvocationCredentialV1) error {
	if ctx == nil || handle.Validate() != nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// No-Done callers retain the original wait-after-callback ordering.
	// Cancellable cleanup instead refuses contention without a detached
	// waiter. The registry error is generic: no handle was inspected yet.
	finite := ctx.Done() != nil
	if finite {
		if !service.mu.TryLock() {
			return ErrBackend
		}
		defer service.mu.Unlock()
	} else {
		service.mu.Lock()
	}
	state, found := service.handles[handle.HandleID]
	if !finite {
		service.mu.Unlock()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !found {
		return nil
	}
	// The stored handle is immutable. Compare its entire tuple before
	// disclosing per-handle contention, just as immutable Finalize does.
	if state.handle != handle {
		return ErrNotFound
	}
	if finite {
		if !state.mu.TryLock() {
			return ErrConsumed
		}
	} else {
		state.mu.Lock()
	}
	if err := ctx.Err(); err != nil {
		state.mu.Unlock()
		return err
	}
	// Context checks bound admission, not the interruptibility of filesystem
	// syscalls. Preserve the original irreversible revocation tombstone
	// before cleanup, including failures with partial residue. The tombstone
	// is not successful cleanup evidence: an error retains state/file custody.
	state.released = true
	if err := service.cleanupProviderCredentialFileLocked(state); err != nil {
		state.mu.Unlock()
		return ErrDeliveryMismatch
	}
	state.mu.Unlock()
	if !finite {
		service.mu.Lock()
		defer service.mu.Unlock()
	}
	if service.handles[handle.HandleID] == state {
		delete(service.handles, handle.HandleID)
	}
	return nil
}

func (service *InvocationService) FenceProviderCredentialInvocations(_ context.Context, locator ports.ProviderCredentialLocatorV1, beforeGeneration uint64) error {
	if err := locator.Validate(); err != nil || beforeGeneration == 0 {
		return ErrInvalid
	}
	service.mu.Lock()
	states := make([]*providerInvocationState, 0)
	for _, state := range service.handles {
		resource := state.handle.ProviderResource
		if state.handle.TenantID == locator.TenantID && state.handle.OwnerUserID == locator.OwnerUserID && resource.Kind == locator.ResourceKind && resource.ResourceID == locator.ResourceID && resource.CredentialGeneration < beforeGeneration {
			states = append(states, state)
		}
	}
	service.mu.Unlock()
	for _, state := range states {
		state.mu.Lock()
		state.revoked = true
		if err := service.cleanupProviderCredentialFileLocked(state); err != nil {
			state.mu.Unlock()
			return ErrDeliveryMismatch
		}
		state.mu.Unlock()
	}
	return nil
}

// materializationPlanLocked requires state.mu. The lock remains held through
// the consumer callback, so a successful fence waits for any already-started
// local secret use and prevents every later callback for that handle.
func (service *InvocationService) materializationPlanLocked(state *providerInvocationState, secret []byte) (ports.ProviderCredentialMaterializationV1, error) {
	switch state.template.Kind {
	case domain.ProviderCredentialDeliveryFileV1:
		var file *credentiallifecycle.PinnedSecretFile
		var err error
		if state.handle.ProviderResource.Kind == domain.ProviderResourceAPIAccountV1 {
			file, err = service.files.CreateImmutable(state.template.FileName, secret, service.maxBytes)
		} else {
			file, err = service.files.Create(state.template.FileName, secret, service.maxBytes)
		}
		if err != nil {
			return ports.ProviderCredentialMaterializationV1{}, ErrDeliveryMismatch
		}
		state.file = file
		plan := ports.ProviderCredentialMaterializationV1{Kind: state.template.Kind, RootDir: file.RootDir, FilePath: file.FilePath}
		if plan.Validate() != nil {
			_ = service.files.Cleanup(file)
			state.file = nil
			return ports.ProviderCredentialMaterializationV1{}, ErrDeliveryMismatch
		}
		return plan, nil
	case domain.ProviderCredentialDeliveryEnvironmentV1:
		plan := ports.ProviderCredentialMaterializationV1{Kind: state.template.Kind, EnvironmentName: state.template.EnvironmentName}
		if plan.Validate() != nil {
			return ports.ProviderCredentialMaterializationV1{}, ErrDeliveryMismatch
		}
		return plan, nil
	case domain.ProviderCredentialDeliveryDirectV1:
		return ports.ProviderCredentialMaterializationV1{Kind: state.template.Kind}, nil
	default:
		return ports.ProviderCredentialMaterializationV1{}, ErrDeliveryMismatch
	}
}

func (service *InvocationService) cleanupProviderCredentialFileLocked(state *providerInvocationState) error {
	if state.file == nil {
		return nil
	}
	if err := service.files.Cleanup(state.file); err != nil {
		return err
	}
	state.file = nil
	return nil
}

func (service *InvocationService) ensureFileSystem() error {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.files != nil {
		return nil
	}
	root := filepath.Clean(service.root)
	if service.root == "" || !filepath.IsAbs(root) || root != service.root {
		return ErrDeliveryMismatch
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return ErrDeliveryMismatch
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root {
		return ErrDeliveryMismatch
	}
	files, err := credentiallifecycle.NewPinnedSecretFileSystem(root)
	if err != nil {
		return ErrDeliveryMismatch
	}
	service.files = files
	return nil
}

func providerCredentialBindingMatchesResource(binding domain.ProviderCredentialBindingV1, resource domain.ProviderResourceBindingV1) bool {
	if binding.Validate() != nil || binding.State != domain.ProviderCredentialActiveV1 {
		return false
	}
	projected, err := binding.ResourceBinding()
	return err == nil && projected == resource
}

var _ ports.ProviderResourceCredentialLifecycleV1 = (*InvocationService)(nil)
var _ ports.ProviderCredentialImmutableFileFinalizerV1 = (*InvocationService)(nil)
var _ ports.ProviderCredentialInvocationRevokerV1 = (*InvocationService)(nil)
