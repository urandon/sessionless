package providercredential

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type invocationCredentialPurpose uint8

const (
	invocationCredentialModel invocationCredentialPurpose = iota + 1
	invocationCredentialSearch
)

// Preserve the MODEL arm's existing contract. SEARCH never borrows after its
// original absolute expiry/cancellation, including a late custody read; a
// failed materialization stays consumed, and owned bytes are still cleared.
func (service *InvocationService) searchBorrowCurrent(ctx context.Context, handle ports.ProviderInvocationCredentialV1, purpose invocationCredentialPurpose) error {
	if purpose != invocationCredentialSearch {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !service.clock.Now().UTC().Before(handle.ExpiresAt) {
		return ErrExpired
	}
	return nil
}

// IssueSearchCredential consumes only closed correlation DATA. The original
// prepared entry must separately authenticate the call/consent/Source/Store;
// this service owns current credential binding and one-shot key custody only.
// No MODEL binding/planner, new signing key or parallel handle map is created.
func (service *InvocationService) IssueSearchCredential(ctx context.Context, request ports.SearchCredentialIssueRequestV1) (ports.SearchInvocationCredentialV1, error) {
	if ctx == nil || ctx.Err() != nil || request.Validate() != nil {
		return ports.SearchInvocationCredentialV1{}, ErrInvalid
	}
	if !service.clock.Now().UTC().Before(request.ExpiresAt) {
		return ports.SearchInvocationCredentialV1{}, ErrExpired
	}
	d, s := request.Declaration, request.Correlation.Scope
	locator := ports.ProviderCredentialLocatorV1{TenantID: d.TenantID, OwnerUserID: d.Resource.OwnerUserID, ResourceKind: d.Resource.Kind, ResourceID: d.Resource.ResourceID}
	binding, found, err := service.bindings.LoadProviderCredential(ctx, locator)
	if err != nil {
		return ports.SearchInvocationCredentialV1{}, ErrBackend
	}
	if !found || binding.TenantID != d.TenantID || !providerCredentialBindingMatchesResource(binding, d.Resource) {
		return ports.SearchInvocationCredentialV1{}, ErrNotFound
	}
	recovered, err := service.secrets.RecoverProviderCredentialCandidate(ctx, binding)
	if err != nil {
		return ports.SearchInvocationCredentialV1{}, ErrBackend
	}
	if !recovered {
		return ports.SearchInvocationCredentialV1{}, ErrNotFound
	}
	id, err := service.ids.NewID(ctx, ports.IDCredentialHandle)
	if err != nil {
		return ports.SearchInvocationCredentialV1{}, ErrBackend
	}
	h := ports.SearchInvocationCredentialV1{Credential: ports.ProviderInvocationCredentialV1{HandleID: id, TenantID: d.TenantID, OwnerUserID: d.Resource.OwnerUserID, RunID: s.RunID, AttemptID: s.AttemptID, WorkerID: request.WorkerID, LeaseID: s.LeaseID, LeaseFence: s.FenceToken, ProviderResource: d.Resource, ExpiresAt: request.ExpiresAt}, Declaration: d, Correlation: request.Correlation}
	if h.Validate() != nil || ctx.Err() != nil || !service.clock.Now().UTC().Before(h.Credential.ExpiresAt) {
		return ports.SearchInvocationCredentialV1{}, ErrBackend
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if _, exists := service.handles[id]; exists {
		return ports.SearchInvocationCredentialV1{}, ErrBackend
	}
	service.handles[id] = &providerInvocationState{handle: h.Credential, purpose: invocationCredentialSearch, search: h, template: ports.ProviderCredentialDeliveryTemplateV1{Kind: domain.ProviderCredentialDeliveryDirectV1}}
	return h, nil
}

func (service *InvocationService) MaterializeSearchCredential(ctx context.Context, handle ports.SearchInvocationCredentialV1, consume ports.ProviderCredentialConsumerV1) error {
	if handle.Validate() != nil {
		return ErrInvalid
	}
	return service.materializeInvocationCredential(ctx, handle.Credential, invocationCredentialSearch, handle, consume)
}

func (service *InvocationService) ReleaseSearchCredential(ctx context.Context, handle ports.SearchInvocationCredentialV1) error {
	if handle.Validate() != nil {
		return ErrInvalid
	}
	return service.releaseInvocationCredential(ctx, handle.Credential, invocationCredentialSearch, handle)
}

var _ ports.SearchResourceCredentialLifecycleV1 = (*InvocationService)(nil)
