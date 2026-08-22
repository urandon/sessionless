package ports

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type CredentialIssueRequest struct {
	OwnerUserID domain.UserID
	Run         domain.Run
	Attempt     domain.Attempt
	Lease       domain.Lease
	ExpiresAt   time.Time
}

// CredentialHandle is invocation-scoped and must never be placed in a queue,
// checkpoint, artifact, log, or persisted session event.
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
	ExpiresAt                time.Time
}

type CredentialMaterialization struct {
	RootDir  string
	AuthFile string
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
