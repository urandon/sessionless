package ports

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type ProviderCredentialLocatorV1 struct {
	TenantID     domain.TenantID
	OwnerUserID  domain.UserID
	ResourceKind domain.ProviderResourceKindV1
	ResourceID   string
}

func (locator ProviderCredentialLocatorV1) Validate() error {
	if err := locator.TenantID.Validate(); err != nil {
		return err
	}
	if err := locator.OwnerUserID.Validate(); err != nil {
		return err
	}
	if locator.ResourceKind != domain.ProviderResourceAPIAccountV1 && locator.ResourceKind != domain.ProviderResourceRouterAccountV1 {
		return domain.ValidationError{Field: "provider_credential.resource_kind", Reason: "must be api_account or router_account"}
	}
	return domain.ValidateOpaqueID("provider_credential.resource_id", locator.ResourceID)
}

type ProviderCredentialMutationChannelV1 string
type ProviderCredentialMutationOperationV1 string

const (
	ProviderCredentialChannelLocalOperatorV1 ProviderCredentialMutationChannelV1   = "local_operator"
	ProviderCredentialOperationIngestV1      ProviderCredentialMutationOperationV1 = "ingest"
	ProviderCredentialOperationRevokeV1      ProviderCredentialMutationOperationV1 = "revoke"
)

type ProviderCredentialPrincipalV1 struct {
	TenantID    domain.TenantID
	OwnerUserID domain.UserID
	Channel     ProviderCredentialMutationChannelV1
}

func (principal ProviderCredentialPrincipalV1) Validate() error {
	if err := principal.TenantID.Validate(); err != nil {
		return err
	}
	if err := principal.OwnerUserID.Validate(); err != nil {
		return err
	}
	if principal.Channel != ProviderCredentialChannelLocalOperatorV1 {
		return domain.ValidationError{Field: "provider_credential.channel", Reason: "is unsupported"}
	}
	return nil
}

type ProviderCredentialIngestRequestV1 struct {
	Principal    ProviderCredentialPrincipalV1
	ResourceKind domain.ProviderResourceKindV1
	ResourceID   string
}

type ProviderCredentialRevokeRequestV1 struct {
	Principal    ProviderCredentialPrincipalV1
	ResourceKind domain.ProviderResourceKindV1
	ResourceID   string
}

func (request ProviderCredentialIngestRequestV1) Locator() ProviderCredentialLocatorV1 {
	return ProviderCredentialLocatorV1{TenantID: request.Principal.TenantID, OwnerUserID: request.Principal.OwnerUserID, ResourceKind: request.ResourceKind, ResourceID: request.ResourceID}
}

func (request ProviderCredentialRevokeRequestV1) Locator() ProviderCredentialLocatorV1 {
	return ProviderCredentialLocatorV1{TenantID: request.Principal.TenantID, OwnerUserID: request.Principal.OwnerUserID, ResourceKind: request.ResourceKind, ResourceID: request.ResourceID}
}

func (request ProviderCredentialIngestRequestV1) Validate() error {
	if err := request.Principal.Validate(); err != nil {
		return err
	}
	return request.Locator().Validate()
}

func (request ProviderCredentialRevokeRequestV1) Validate() error {
	if err := request.Principal.Validate(); err != nil {
		return err
	}
	return request.Locator().Validate()
}

type ProviderCredentialMutationStatusV1 string

const (
	ProviderCredentialAppliedV1  ProviderCredentialMutationStatusV1 = "applied"
	ProviderCredentialReplayedV1 ProviderCredentialMutationStatusV1 = "replayed"
)

type ProviderCredentialReceiptV1 struct {
	Status         ProviderCredentialMutationStatusV1
	Resource       domain.ProviderResourceBindingV1
	Fingerprint    domain.CredentialFingerprint
	Revoked        bool
	UpdatedAt      time.Time
	AuditReceiptID domain.ProviderCredentialAuditReceiptIDV1
}

type ProviderCredentialCandidateScopeV1 struct {
	Locator              ProviderCredentialLocatorV1
	ResourceRevision     uint64
	CredentialGeneration uint64
	MutationID           string
}

type ProviderCredentialSecretCandidateV1 struct {
	Scope       ProviderCredentialCandidateScopeV1
	Reference   domain.CredentialSecretRef
	Fingerprint domain.CredentialFingerprint
	CreatedAt   time.Time
}

type ProviderCredentialCandidateCursorV1 struct {
	Present              bool
	CreatedAt            time.Time
	TenantID             domain.TenantID
	OwnerUserID          domain.UserID
	ResourceKind         domain.ProviderResourceKindV1
	ResourceID           string
	ResourceRevision     uint64
	CredentialGeneration uint64
	MutationID           string
	Reference            domain.CredentialSecretRef
}

type ProviderCredentialCandidatePageV1 struct {
	Items      []ProviderCredentialSecretCandidateV1
	NextCursor ProviderCredentialCandidateCursorV1
	HasMore    bool
}

type ProviderCredentialCleanupV1 struct {
	Locator              ProviderCredentialLocatorV1
	CredentialGeneration uint64
	Reference            domain.CredentialSecretRef
}

type ProviderCredentialCleanupCursorV1 struct {
	Present              bool
	CreatedAt            time.Time
	TenantID             domain.TenantID
	OwnerUserID          domain.UserID
	ResourceKind         domain.ProviderResourceKindV1
	ResourceID           string
	CredentialGeneration uint64
	Reference            domain.CredentialSecretRef
}

type ProviderCredentialCleanupItemV1 struct {
	Cleanup   ProviderCredentialCleanupV1
	CreatedAt time.Time
}

type ProviderCredentialCleanupPageV1 struct {
	Items          []ProviderCredentialCleanupItemV1
	NextCursor     ProviderCredentialCleanupCursorV1
	HasMore        bool
	SkippedInvalid uint64
}

type ProviderCredentialIssueRequestV1 struct {
	HarnessBinding domain.HarnessBindingV1
	WorkerID       string
	LeaseID        domain.LeaseID
	LeaseFence     uint64
	ExpiresAt      time.Time
}

func (request ProviderCredentialIssueRequestV1) Validate() error {
	if err := request.HarnessBinding.Validate(); err != nil {
		return err
	}
	if request.HarnessBinding.Resource.Kind != domain.ProviderResourceAPIAccountV1 && request.HarnessBinding.Resource.Kind != domain.ProviderResourceRouterAccountV1 {
		return domain.ValidationError{Field: "provider_credential_issue.resource", Reason: "must be an API or router account"}
	}
	if err := domain.ValidateOpaqueID("provider_credential_issue.worker_id", request.WorkerID); err != nil {
		return err
	}
	if err := request.LeaseID.Validate(); err != nil {
		return err
	}
	if request.LeaseFence == 0 || request.ExpiresAt.IsZero() {
		return domain.ValidationError{Field: "provider_credential_issue", Reason: "requires a positive fence and expiry"}
	}
	if request.HarnessBinding.EvidenceExpiresAt == nil || request.ExpiresAt.After(*request.HarnessBinding.EvidenceExpiresAt) {
		return domain.ValidationError{Field: "provider_credential_issue.expires_at", Reason: "must not outlive sealed provider evidence"}
	}
	return nil
}

type ProviderCredentialDeliveryTemplateV1 struct {
	Kind            domain.ProviderCredentialDeliveryKindV1
	FileName        string
	EnvironmentName string
}

func (template ProviderCredentialDeliveryTemplateV1) ValidateForBinding(binding domain.HarnessBindingV1) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	if template.Kind != binding.Backend.CredentialDeliveryKind {
		return domain.ValidationError{Field: "provider_credential_delivery.kind", Reason: "must match the sealed backend"}
	}
	switch template.Kind {
	case domain.ProviderCredentialDeliveryFileV1:
		if template.EnvironmentName != "" {
			return domain.ValidationError{Field: "provider_credential_delivery", Reason: "file delivery cannot set an environment name"}
		}
		return domain.ValidateOpaqueID("provider_credential_delivery.file_name", template.FileName)
	case domain.ProviderCredentialDeliveryEnvironmentV1:
		return (ProviderCredentialMaterializationV1{Kind: template.Kind, EnvironmentName: template.EnvironmentName}).Validate()
	case domain.ProviderCredentialDeliveryDirectV1:
		if template.FileName != "" || template.EnvironmentName != "" {
			return domain.ValidationError{Field: "provider_credential_delivery", Reason: "direct delivery has no locator"}
		}
		return nil
	default:
		return domain.ValidationError{Field: "provider_credential_delivery.kind", Reason: "is unsupported"}
	}
}

type ProviderCredentialDeliveryPlannerV1 interface {
	PlanProviderCredentialDelivery(context.Context, domain.HarnessBindingV1) (ProviderCredentialDeliveryTemplateV1, error)
}

type ProviderCredentialConsumerV1 func(ProviderCredentialMaterializationV1, []byte) error

type ProviderResourceCredentialLifecycleV1 interface {
	IssueProviderCredential(context.Context, ProviderCredentialIssueRequestV1) (ProviderInvocationCredentialV1, error)
	MaterializeProviderCredential(context.Context, ProviderInvocationCredentialV1, ProviderCredentialConsumerV1) error
	ReleaseProviderCredential(context.Context, ProviderInvocationCredentialV1) error
}

type ProviderCredentialInvocationRevokerV1 interface {
	// FenceProviderCredentialInvocations waits for active local consumers from
	// every generation lower than beforeGeneration and prevents their later
	// materialization. Handles already issued for beforeGeneration or newer are
	// not invalidated by an older rotation replay.
	FenceProviderCredentialInvocations(context.Context, ProviderCredentialLocatorV1, uint64) error
}

type ProviderCredentialSwapV1 struct {
	Applied        bool
	Found          bool
	Binding        domain.ProviderCredentialBindingV1
	AuditReceiptID domain.ProviderCredentialAuditReceiptIDV1
}

type ProviderCredentialCandidateFenceV1 struct {
	Authoritative bool
	Binding       domain.ProviderCredentialBindingV1
}

type ProviderCredentialMutationAuthorizer interface {
	AuthorizeProviderCredentialMutation(context.Context, ProviderCredentialPrincipalV1, ProviderCredentialMutationOperationV1) (bool, error)
}

// ProviderCredentialBindingStore is the sole resource/generation authority.
// A successful rotation or revoke must enqueue the superseded secret reference
// durably in the same transaction as the binding mutation.
type ProviderCredentialBindingStore interface {
	LoadProviderCredential(context.Context, ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error)
	CompareAndSwapProviderCredential(context.Context, uint64, domain.ProviderCredentialBindingV1) (ProviderCredentialSwapV1, error)
	RevokeProviderCredential(context.Context, ProviderCredentialLocatorV1, time.Time) (ProviderCredentialSwapV1, error)
	FenceProviderCredentialCandidate(context.Context, ProviderCredentialSecretCandidateV1, time.Time) (ProviderCredentialCandidateFenceV1, error)
	ListProviderCredentialCleanups(context.Context, ProviderCredentialLocatorV1, uint64) ([]ProviderCredentialCleanupV1, error)
	ListDueProviderCredentialCleanups(context.Context, uint32, time.Time, ProviderCredentialCleanupCursorV1, uint64) (ProviderCredentialCleanupPageV1, error)
	AcknowledgeProviderCredentialCleanup(context.Context, ProviderCredentialCleanupV1) error
}

// ProviderCredentialSecretStore owns plaintext and candidate promotion. No
// implementation may put secret bytes into its reference or error surfaces.
type ProviderCredentialSecretStore interface {
	ReadProviderCredentialSecret(context.Context, domain.ProviderCredentialBindingV1, int64) ([]byte, error)
	PutProviderCredentialCandidate(context.Context, ProviderCredentialCandidateScopeV1, []byte) (ProviderCredentialSecretCandidateV1, error)
	CommitProviderCredentialCandidate(context.Context, ProviderCredentialSecretCandidateV1) error
	DeleteProviderCredentialCandidate(context.Context, ProviderCredentialSecretCandidateV1) error
	DeleteProviderCredentialSecret(context.Context, ProviderCredentialCleanupV1) error
	ListUncommittedProviderCredentialCandidates(context.Context, ProviderCredentialLocatorV1, uint64) ([]ProviderCredentialSecretCandidateV1, error)
	ListAbandonedProviderCredentialCandidates(context.Context, time.Time, ProviderCredentialCandidateCursorV1, uint64) (ProviderCredentialCandidatePageV1, error)
	// RecoverProviderCredentialCandidate proves that the exact authoritative
	// secret is available, promoting a matching uncommitted candidate when
	// necessary. It returns false when neither committed secret nor exact
	// recoverable candidate exists; callers must fail closed in that case.
	RecoverProviderCredentialCandidate(context.Context, domain.ProviderCredentialBindingV1) (bool, error)
}
