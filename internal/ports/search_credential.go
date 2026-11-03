package ports

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// SearchCredentialDeclarationV1 is Source-pinned nonsecret DATA, not approval,
// a secret locator, a current binding or permission to send. Its credential
// revisions are independent of the search grant's resource revisions.
type SearchCredentialDeclarationV1 struct {
	Version      uint32                                  `json:"version"`
	TenantID     domain.TenantID                         `json:"tenant_id"`
	Resource     domain.ProviderResourceBindingV1        `json:"resource"`
	Audience     string                                  `json:"audience"`
	DeliveryKind domain.ProviderCredentialDeliveryKindV1 `json:"delivery_kind"`
}

func (v SearchCredentialDeclarationV1) Validate() error {
	if v.Version != 1 || v.TenantID.Validate() != nil || v.Resource.Validate() != nil || v.Resource.Kind != domain.ProviderResourceWebSearchAccountV1 || v.Resource.CredentialMode != domain.ProviderCredentialInvocationV1 || v.Audience != domain.WebSearchCredentialAudienceV1 || v.DeliveryKind != domain.ProviderCredentialDeliveryDirectV1 {
		return domain.ValidationError{Field: "search_credential.declaration", Reason: "requires the closed Tavily direct invocation credential declaration"}
	}
	return nil
}

// Correlation is comparable immutable DATA. The original entry/Source/Store
// must authenticate the durable MODEL, canonical actor, exact consent, grant,
// spending and current claim before calling Issue and again before send. This
// lifecycle validates current credential custody, never those public digests.
type SearchCredentialCorrelationV1 struct {
	Scope                 domain.AttemptOperationScopeV1
	OperationDigest       domain.AttemptOperationDigestV1
	ConsentRequestDigest  domain.WebSearchConsentRequestDigestV1
	ConsentDecisionDigest domain.WebSearchConsentDecisionDigestV1
}

func (v SearchCredentialCorrelationV1) Validate() error {
	if v.Scope.Validate() != nil || v.OperationDigest.Validate() != nil || v.ConsentRequestDigest.Validate() != nil || v.ConsentDecisionDigest.Validate() != nil {
		return domain.ValidationError{Field: "search_credential.correlation", Reason: "requires exact original scope, operation and receipt digests"}
	}
	return nil
}

type SearchCredentialIssueRequestV1 struct {
	Declaration SearchCredentialDeclarationV1
	Correlation SearchCredentialCorrelationV1
	WorkerID    string
	ExpiresAt   time.Time
}

func (v SearchCredentialIssueRequestV1) Validate() error {
	s := v.Correlation.Scope
	if v.Declaration.Validate() != nil || v.Correlation.Validate() != nil || v.Declaration.TenantID != s.TenantID || v.Declaration.Resource.OwnerUserID != s.OwnerUserID || domain.ValidateOpaqueID("search_credential.worker_id", v.WorkerID) != nil || v.ExpiresAt.IsZero() || v.ExpiresAt != v.ExpiresAt.UTC().Truncate(time.Microsecond) {
		return domain.ValidationError{Field: "search_credential.issue", Reason: "requires exact tenant/owner scope, worker and canonical absolute expiry"}
	}
	return nil
}

// SearchInvocationCredentialV1 is a distinct comparable handle language. The
// unchanged MODEL handle is embedded as correlation only; its consumer cannot
// materialize or release this SEARCH handle through the MODEL API.
type SearchInvocationCredentialV1 struct {
	Credential  ProviderInvocationCredentialV1
	Declaration SearchCredentialDeclarationV1
	Correlation SearchCredentialCorrelationV1
}

func (v SearchInvocationCredentialV1) Validate() error {
	h, s := v.Credential, v.Correlation.Scope
	if h.Validate() != nil || (SearchCredentialIssueRequestV1{Declaration: v.Declaration, Correlation: v.Correlation, WorkerID: h.WorkerID, ExpiresAt: h.ExpiresAt}).Validate() != nil || h.TenantID != s.TenantID || h.OwnerUserID != s.OwnerUserID || h.RunID != s.RunID || h.AttemptID != s.AttemptID || h.LeaseID != s.LeaseID || h.LeaseFence != s.FenceToken || h.ProviderResource != v.Declaration.Resource {
		return domain.ValidationError{Field: "search_credential.handle", Reason: "requires exact declared credential and original physical claim correlation"}
	}
	return nil
}

type SearchResourceCredentialLifecycleV1 interface {
	IssueSearchCredential(context.Context, SearchCredentialIssueRequestV1) (SearchInvocationCredentialV1, error)
	// The synchronous callback borrows bytes only until it returns. Successful
	// release/fencing joins that callback through the same invocation registry.
	MaterializeSearchCredential(context.Context, SearchInvocationCredentialV1, ProviderCredentialConsumerV1) error
	ReleaseSearchCredential(context.Context, SearchInvocationCredentialV1) error
}
