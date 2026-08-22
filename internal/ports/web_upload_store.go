package ports

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const MaxWebMessageUploads = 8

// MaxWebRunArtifacts is the hard public read bound for one worker output
// manifest. Product plans may admit fewer artifacts; the Web API refuses to
// index or serve a manifest beyond this independent transport bound.
const MaxWebRunArtifacts = 100

type WebUploadCreateRequest struct {
	Intent         domain.UploadIntent
	IdempotencyKey domain.IdempotencyKey
}

type WebUploadCommitRequest struct {
	TenantID domain.TenantID
	UserID   domain.UserID
	UploadID domain.UploadIntentID
	Observed ObjectMetadata
	At       time.Time
}

type WebUploadClaimRequest struct {
	TenantID              domain.TenantID
	UserID                domain.UserID
	SessionID             domain.SessionID
	UploadIDs             []domain.UploadIntentID
	MessageIdempotencyKey domain.IdempotencyKey
	At                    time.Time
}

// WebUploadStore owns only durable intent state. Object existence and metadata
// are observed by WebObjectStore before CommitWebUploadIntent is called.
type WebUploadStore interface {
	CreateWebUploadIntent(context.Context, WebUploadCreateRequest) (domain.UploadIntent, bool, error)
	CommitWebUploadIntent(context.Context, WebUploadCommitRequest) (domain.UploadIntent, error)
	ClaimWebUploadIntents(context.Context, WebUploadClaimRequest) ([]domain.UploadIntent, error)
}

type ComputeConnectionState struct {
	ID          domain.SubscriptionConnectionID
	Provider    string
	Entitlement domain.EntitlementState
	Quota       domain.ProviderQuotaState
	ObservedAt  time.Time
}

type ComputeConnectionResolveRequest struct {
	TenantID  domain.TenantID
	UserID    domain.UserID
	SessionID domain.SessionID
}

type WebRunArtifactRequest struct {
	TenantID   domain.TenantID
	UserID     domain.UserID
	SessionID  domain.SessionID
	RunID      domain.RunID
	ManifestID domain.ArtifactManifestID
	Index      uint32
}

// WebRunArtifact contains the minimum internal object identity required to
// mint an exact-object capability. Transport adapters must not serialize Blob.
type WebRunArtifact struct {
	Name      string
	MediaType string
	Blob      domain.BlobRef
}

// WebResourceStore combines participant-authorized point run reads with a
// bounded, user-owned compute resolver. ComputeConnectionState deliberately
// has no field capable of carrying credential_ref.
type WebResourceStore interface {
	GetRunForUser(context.Context, domain.TenantID, domain.UserID, domain.RunID) (RunRecord, bool, error)
	GetRunArtifactForUser(context.Context, WebRunArtifactRequest) (WebRunArtifact, bool, error)
	ResolveComputeConnectionsForUser(context.Context, ComputeConnectionResolveRequest) ([]ComputeConnectionState, error)
}
