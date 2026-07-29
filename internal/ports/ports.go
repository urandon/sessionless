// Package ports defines infrastructure and runtime interfaces used by the
// harness-neutral control plane.
package ports

import (
	"context"
	"io"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
)

type Clock interface {
	Now() time.Time
}

type IDKind string

const (
	IDRun              IDKind = "run"
	IDAttempt          IDKind = "attempt"
	IDLease            IDKind = "lease"
	IDCheckpoint       IDKind = "checkpoint"
	IDQuotaReservation IDKind = "quota_reservation"
	IDUsageObservation IDKind = "usage_observation"
	IDArtifactManifest IDKind = "artifact_manifest"
	IDDispatchOutbox   IDKind = "dispatch_outbox"
	IDTelegramDelivery IDKind = "telegram_delivery"
	IDQueueMessage     IDKind = "queue_message"
)

type IDGenerator interface {
	NewID(ctx context.Context, kind IDKind) (string, error)
}

// StateStore provides the transaction boundary required for atomic state and
// outbox writes. Adapters must reject tenant mismatches before mutation.
type StateStore interface {
	Transact(ctx context.Context, tenantID domain.TenantID, fn func(StateTx) error) error
}

type StateTx interface {
	GetRun(ctx context.Context, id domain.RunID) (domain.Run, bool, error)
	FindRunByIdempotencyKey(ctx context.Context, key domain.IdempotencyKey) (domain.Run, bool, error)
	PutRun(ctx context.Context, run domain.Run) error

	GetAttempt(ctx context.Context, id domain.AttemptID) (domain.Attempt, bool, error)
	PutAttempt(ctx context.Context, attempt domain.Attempt) error
	PutLease(ctx context.Context, lease domain.Lease) error
	PutCheckpoint(ctx context.Context, checkpoint domain.Checkpoint) error

	PutQuotaReservation(ctx context.Context, reservation domain.QuotaReservation) error
	AppendUsageObservation(ctx context.Context, observation domain.UsageObservation) error
	PutArtifactManifest(ctx context.Context, manifest domain.ArtifactManifest) error

	PutDispatchOutbox(ctx context.Context, outbox domain.DispatchOutbox) error
	PutTelegramDeliveryOutbox(ctx context.Context, outbox domain.TelegramDeliveryOutbox) error
}

type ReceivedMessage struct {
	Envelope      queuecontract.Envelope
	ReceiptHandle string
	DeliveryCount uint32
}

// Queue is explicitly at-least-once. Consumers acknowledge only after the
// corresponding idempotent state transition commits.
type Queue interface {
	Publish(ctx context.Context, envelope queuecontract.Envelope) error
	Receive(ctx context.Context) (ReceivedMessage, error)
	Ack(ctx context.Context, receiptHandle string) error
	Retry(ctx context.Context, receiptHandle string, delay time.Duration) error
	DeadLetter(ctx context.Context, receiptHandle, reasonCode string) error
}

type BlobStore interface {
	Put(ctx context.Context, tenantID domain.TenantID, key string, body io.Reader) (domain.BlobRef, error)
	Open(ctx context.Context, tenantID domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error)
	Delete(ctx context.Context, tenantID domain.TenantID, ref domain.BlobRef) error
}

type TelegramSendRequest struct {
	TenantID         domain.TenantID
	DeliveryID       domain.TelegramDeliveryID
	Chat             domain.TelegramChatRef
	ReplyToMessageID int64
	Payload          domain.BlobRef
	Artifacts        []domain.Artifact
	IdempotencyKey   domain.IdempotencyKey
}

type TelegramSendResult struct {
	MessageID int64
	SentAt    time.Time
}

// TelegramClient is a frontend adapter port, not a core session abstraction.
type TelegramClient interface {
	Send(ctx context.Context, request TelegramSendRequest) (TelegramSendResult, error)
}

type CredentialRequest struct {
	TenantID                 domain.TenantID
	SubscriptionConnectionID domain.SubscriptionConnectionID
	RunID                    domain.RunID
	AttemptID                domain.AttemptID
	WorkerID                 string
}

// CredentialHandle is an opaque vault/runtime reference. It is safe to pass to
// an isolated worker control channel but never to a queue envelope.
type CredentialHandle struct {
	TenantID                 domain.TenantID
	SubscriptionConnectionID domain.SubscriptionConnectionID
	Handle                   string
	ExpiresAt                time.Time
}

type CredentialVault interface {
	IssueWorkerCredential(ctx context.Context, request CredentialRequest) (CredentialHandle, error)
	RevokeWorkerCredential(ctx context.Context, handle CredentialHandle) error
}

type ExecutionRequest struct {
	TenantID          domain.TenantID
	RunID             domain.RunID
	AttemptID         domain.AttemptID
	ContextSnapshot   domain.BlobRef
	InputArtifacts    []domain.Artifact
	Credential        CredentialHandle
	AllowedMCPServers []string
}

type ExecutionEvent struct {
	Checkpoint *domain.Checkpoint
	Usage      *domain.UsageObservation
}

type ExecutionEventSink interface {
	Emit(ctx context.Context, event ExecutionEvent) error
}

type ExecutionResult struct {
	Manifest domain.ArtifactManifest
	Usage    []domain.UsageObservation
}

type ExecutionIdentity struct {
	TenantID  domain.TenantID
	RunID     domain.RunID
	AttemptID domain.AttemptID
}

// HarnessDriver is implemented inside an isolated worker adapter. Core
// scheduling types never name or assume a concrete harness.
type HarnessDriver interface {
	Execute(ctx context.Context, request ExecutionRequest, sink ExecutionEventSink) (ExecutionResult, error)
	Cancel(ctx context.Context, identity ExecutionIdentity) error
}

type EntitlementObserver interface {
	ObserveEntitlement(
		ctx context.Context,
		tenantID domain.TenantID,
		connectionID domain.SubscriptionConnectionID,
	) (domain.EntitlementSnapshot, error)
}

type QuotaObserver interface {
	ObserveQuota(
		ctx context.Context,
		tenantID domain.TenantID,
		connectionID domain.SubscriptionConnectionID,
	) (domain.ProviderQuotaSnapshot, error)
}

// CancellationObserver reads durable cancellation state. context cancellation
// is still required for in-process shutdown and deadlines.
type CancellationObserver interface {
	CancellationRequested(ctx context.Context, tenantID domain.TenantID, runID domain.RunID) (bool, error)
}
