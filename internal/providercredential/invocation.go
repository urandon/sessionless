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
	callbackSecret := secret
	if plan.Kind == domain.ProviderCredentialDeliveryFileV1 {
		callbackSecret = nil
	}
	if err := consume(plan, callbackSecret); err != nil {
		return ErrBackend
	}
	return nil
}

func (service *InvocationService) ReleaseProviderCredential(_ context.Context, handle ports.ProviderInvocationCredentialV1) error {
	if handle.Validate() != nil {
		return ErrInvalid
	}
	service.mu.Lock()
	state, found := service.handles[handle.HandleID]
	service.mu.Unlock()
	if !found {
		return nil
	}
	state.mu.Lock()
	if state.handle != handle {
		state.mu.Unlock()
		return ErrNotFound
	}
	state.released = true
	if err := service.cleanupProviderCredentialFileLocked(state); err != nil {
		state.mu.Unlock()
		return ErrDeliveryMismatch
	}
	state.mu.Unlock()
	service.mu.Lock()
	if service.handles[handle.HandleID] == state {
		delete(service.handles, handle.HandleID)
	}
	service.mu.Unlock()
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
		file, err := service.files.Create(state.template.FileName, secret, service.maxBytes)
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
var _ ports.ProviderCredentialInvocationRevokerV1 = (*InvocationService)(nil)
