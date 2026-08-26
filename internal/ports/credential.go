package ports

import (
	"context"
	"path/filepath"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type CredentialIssueRequest struct {
	OwnerUserID      domain.UserID
	Run              domain.Run
	Attempt          domain.Attempt
	Lease            domain.Lease
	ExpiresAt        time.Time
	ProviderResource domain.ProviderResourceBindingV1
}

// CredentialHandle is the existing subscription-backed lifecycle handle. It
// remains private to credential orchestration and is never the provider-neutral
// harness contract.
type CredentialHandle struct {
	HandleID                 string
	TenantID                 domain.TenantID
	SubscriptionConnectionID domain.SubscriptionConnectionID
	OwnerUserID              domain.UserID
	RunID                    domain.RunID
	AttemptID                domain.AttemptID
	WorkerID                 string
	LeaseID                  domain.LeaseID
	LeaseFence               uint64
	BindingGeneration        uint64
	ProviderResource         domain.ProviderResourceBindingV1
	ExpiresAt                time.Time
}

// ProviderInvocationCredentialV1 is the provider-neutral, invocation-only
// projection passed to a harness backend. It contains no secret and does not
// model API/router resources as subscription connections.
type ProviderInvocationCredentialV1 struct {
	HandleID         string
	TenantID         domain.TenantID
	OwnerUserID      domain.UserID
	RunID            domain.RunID
	AttemptID        domain.AttemptID
	WorkerID         string
	LeaseID          domain.LeaseID
	LeaseFence       uint64
	ProviderResource domain.ProviderResourceBindingV1
	ExpiresAt        time.Time
}

func (handle CredentialHandle) ProviderInvocationCredential() ProviderInvocationCredentialV1 {
	return ProviderInvocationCredentialV1{HandleID: handle.HandleID, TenantID: handle.TenantID, OwnerUserID: handle.OwnerUserID, RunID: handle.RunID, AttemptID: handle.AttemptID, WorkerID: handle.WorkerID, LeaseID: handle.LeaseID, LeaseFence: handle.LeaseFence, ProviderResource: handle.ProviderResource, ExpiresAt: handle.ExpiresAt}
}

type CredentialMaterialization struct {
	RootDir  string
	AuthFile string
}

// ProviderCredentialMaterializationV1 describes only the delivery boundary;
// it never contains credential bytes. Exact delivery kind remains backend-
// profile authority and is validated again by that backend's preflight.
type ProviderCredentialMaterializationV1 struct {
	Kind            domain.ProviderCredentialDeliveryKindV1
	RootDir         string
	FilePath        string
	EnvironmentName string
}

func (materialization CredentialMaterialization) ProviderMaterialization() ProviderCredentialMaterializationV1 {
	return ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryFileV1, RootDir: materialization.RootDir, FilePath: materialization.AuthFile}
}

func (materialization CredentialMaterialization) Validate() error {
	root := filepath.Clean(materialization.RootDir)
	authFile := filepath.Clean(materialization.AuthFile)
	if !filepath.IsAbs(root) || !filepath.IsAbs(authFile) ||
		root != materialization.RootDir || authFile != materialization.AuthFile ||
		filepath.Dir(authFile) != root || filepath.Base(authFile) != "auth.json" {
		return domain.ValidationError{Field: "credential.materialization", Reason: "must be an exact normalized auth.json direct child"}
	}
	return nil
}

type CredentialWriteBackResult struct {
	Changed    bool
	Generation uint64
}

type CredentialRevokeRequest struct {
	TenantID                 domain.TenantID
	SubscriptionConnectionID domain.SubscriptionConnectionID
	OwnerUserID              domain.UserID
}

type CredentialLifecycle interface {
	Issue(context.Context, CredentialIssueRequest) (CredentialHandle, error)
	Materialize(context.Context, CredentialHandle) (CredentialMaterialization, error)
	WriteBack(context.Context, CredentialHandle, CredentialMaterialization) (CredentialWriteBackResult, error)
	Release(context.Context, CredentialHandle) error
	RevokeConnection(context.Context, CredentialRevokeRequest) error
}

// CredentialBindingStore owns the authoritative generation/revocation fence.
// CompareAndSwapCredentialBinding must atomically publish Next and enqueue the
// superseded secret reference for durable cleanup.
type CredentialBindingStore interface {
	LoadCredentialBinding(
		context.Context,
		domain.TenantID,
		domain.SubscriptionConnectionID,
	) (domain.CredentialBinding, bool, error)
	CompareAndSwapCredentialBinding(
		context.Context,
		uint64,
		domain.CredentialBinding,
	) (bool, error)
	RevokeCredentialBinding(
		context.Context,
		CredentialRevokeRequest,
		time.Time,
	) (CredentialRevocationResult, error)
}

type CredentialCandidateScope struct {
	TenantID                 domain.TenantID
	SubscriptionConnectionID domain.SubscriptionConnectionID
	OwnerUserID              domain.UserID
	ExpectedGeneration       uint64
}

type CredentialSecretCandidate struct {
	Scope       CredentialCandidateScope
	Reference   domain.CredentialSecretRef
	Fingerprint domain.CredentialFingerprint
}

type CredentialRevocationResult struct {
	Binding              domain.CredentialBinding
	SupersededSecretRef  domain.CredentialSecretRef
	SupersededGeneration uint64
}

// CredentialSecretStore keeps PutCredentialCandidate results durably
// enumerable until CommitCredentialCandidate or DeleteCredentialSecret. This
// makes a process stop on either side of the binding CAS recoverable.
type CredentialSecretStore interface {
	ReadCredentialSecret(context.Context, domain.CredentialBinding, int64) ([]byte, error)
	PutCredentialCandidate(context.Context, CredentialCandidateScope, []byte) (CredentialSecretCandidate, error)
	CommitCredentialCandidate(context.Context, CredentialSecretCandidate) error
	DeleteCredentialCandidate(context.Context, CredentialSecretCandidate) error
	DeleteCredentialSecret(context.Context, CredentialCandidateScope, domain.CredentialSecretRef) error
	ListUncommittedCredentialCandidates(
		context.Context,
		CredentialCandidateScope,
		uint64,
	) ([]CredentialSecretCandidate, error)
	// RecoverCredentialCandidate atomically promotes the exact uncommitted
	// candidate referenced by binding. It returns false when the referenced
	// secret is already committed and no recovery was needed.
	RecoverCredentialCandidate(context.Context, domain.CredentialBinding) (bool, error)
}
