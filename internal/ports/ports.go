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
	IDTenant                 IDKind = "tenant"
	IDUser                   IDKind = "user"
	IDSession                IDKind = "session"
	IDSessionEvent           IDKind = "session_event"
	IDFrontendBinding        IDKind = "frontend_binding"
	IDSessionSnapshot        IDKind = "session_snapshot"
	IDActor                  IDKind = "actor"
	IDConversation           IDKind = "conversation"
	IDSubscriptionConnection IDKind = "subscription_connection"
	IDRun                    IDKind = "run"
	IDAttempt                IDKind = "attempt"
	IDLease                  IDKind = "lease"
	IDCheckpoint             IDKind = "checkpoint"
	IDQuotaReservation       IDKind = "quota_reservation"
	IDUsageObservation       IDKind = "usage_observation"
	IDArtifactManifest       IDKind = "artifact_manifest"
	IDDispatchOutbox         IDKind = "dispatch_outbox"
	IDTelegramDelivery       IDKind = "telegram_delivery"
	IDQueueMessage           IDKind = "queue_message"
)

type IDGenerator interface {
	NewID(ctx context.Context, kind IDKind) (string, error)
}

type TelegramIdentityRequest struct {
	TenantID                 domain.TenantID
	Actor                    domain.ActorRef
	Conversation             domain.ConversationRef
	SubscriptionConnectionID domain.SubscriptionConnectionID
	Provider                 string
	ObservedAt               time.Time
}

type TelegramIdentityState struct {
	UserID          domain.UserID
	SessionID       domain.SessionID
	BindingID       domain.FrontendBindingID
	BindingRevision uint64
}

type TelegramIngress struct {
	TenantID      domain.TenantID
	SourceID      string
	UpdateID      int64
	ExpireAt      time.Time
	Run           domain.Run
	Attempt       domain.Attempt
	InputManifest domain.ArtifactManifest
	Dispatch      domain.DispatchOutbox
}

type TelegramIngressResult struct {
	RunID   domain.RunID
	Created bool
}

type TelegramCommandKind string

const (
	TelegramCommandConnectCodex    TelegramCommandKind = "connect_codex"
	TelegramCommandComputeStatus   TelegramCommandKind = "compute_status"
	TelegramCommandDisconnectCodex TelegramCommandKind = "disconnect_codex"
	TelegramCommandNewContext      TelegramCommandKind = "new_context"
	TelegramCommandHelp            TelegramCommandKind = "help"
)

func (kind TelegramCommandKind) Valid() bool {
	switch kind {
	case TelegramCommandConnectCodex, TelegramCommandComputeStatus,
		TelegramCommandDisconnectCodex, TelegramCommandNewContext,
		TelegramCommandHelp:
		return true
	default:
		return false
	}
}

type TelegramCommandRequest struct {
	TenantID                 domain.TenantID
	SourceID                 string
	UpdateID                 int64
	ExpireAt                 time.Time
	Kind                     TelegramCommandKind
	Provider                 string
	Actor                    domain.ActorRef
	Conversation             domain.ConversationRef
	SubscriptionConnectionID domain.SubscriptionConnectionID
	RunID                    domain.RunID
	SessionID                domain.SessionID
	TriggerEventID           domain.SessionEventID
	DeliveryID               domain.TelegramDeliveryID
	Chat                     domain.TelegramChatRef
	ReplyToMessageID         int64
	IdempotencyKey           domain.IdempotencyKey
	RequestedAt              time.Time
}

// TelegramIngressStore owns the frontend-specific identity and idempotent
// ingress transactions while keeping Telegram types out of the core domain.
type TelegramIngressStore interface {
	EnsureTelegramIdentity(
		ctx context.Context,
		request TelegramIdentityRequest,
	) (TelegramIdentityState, error)
	IngestTelegram(ctx context.Context, request TelegramIngress) (TelegramIngressResult, error)
	ExecuteTelegramCommand(
		ctx context.Context,
		request TelegramCommandRequest,
	) (TelegramIngressResult, error)
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
	PutWorkerJob(ctx context.Context, job domain.WorkerJob) error

	PutDispatchOutbox(ctx context.Context, outbox domain.DispatchOutbox) error
	PutTelegramDeliveryOutbox(ctx context.Context, outbox domain.TelegramDeliveryOutbox) error
}

// SessionStore is the frontend-neutral canonical conversation boundary.
// Implementations must make append and binding switches transactional.
type SessionStore interface {
	CreateSession(ctx context.Context, session domain.Session, owner domain.SessionParticipant) error
	CreateAndSwitchSession(ctx context.Context, session domain.Session, owner domain.SessionParticipant, bindingID domain.FrontendBindingID, expectedRevision uint64, at time.Time) (domain.FrontendBinding, error)
	GetSession(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID) (domain.Session, bool, error)
	BindFrontend(ctx context.Context, binding domain.FrontendBinding) error
	ResolveFrontendBinding(ctx context.Context, tenantID domain.TenantID, frontend domain.Frontend, externalConversationID string) (domain.FrontendBinding, bool, error)
	SwitchFrontendBinding(ctx context.Context, tenantID domain.TenantID, bindingID domain.FrontendBindingID, expectedRevision uint64, sessionID domain.SessionID, at time.Time) (domain.FrontendBinding, error)
	AppendSessionEvent(ctx context.Context, event domain.SessionEvent) (created bool, err error)
	PutSessionSnapshot(ctx context.Context, snapshot domain.SessionSnapshot) error
	ListSessionSnapshots(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, afterVersion uint64, limit uint64) ([]domain.SessionSnapshot, error)
	ArchiveSession(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, at time.Time) error
	UnarchiveSession(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, at time.Time) error
	ListSessions(ctx context.Context, tenantID domain.TenantID, userID domain.UserID, limit uint64) ([]domain.Session, error)
	ListSessionHistory(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, afterSequence uint64, limit uint64) ([]domain.SessionEvent, error)
	ListRunsBySession(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, limit uint64) ([]domain.Run, error)
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
	RunID            domain.RunID
	DeliveryID       domain.TelegramDeliveryID
	Chat             domain.TelegramChatRef
	ReplyToMessageID int64
	Payload          domain.BlobRef
	Text             string
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

type TelegramDeliveryReady struct {
	TenantID   domain.TenantID
	DeliveryID domain.TelegramDeliveryID
}

type TelegramDeliveryStore interface {
	ListReadyTelegramDeliveries(
		ctx context.Context,
		bucket uint32,
		before time.Time,
		limit uint64,
	) ([]TelegramDeliveryReady, error)
	ClaimTelegramDelivery(
		ctx context.Context,
		tenantID domain.TenantID,
		deliveryID domain.TelegramDeliveryID,
		at time.Time,
	) (domain.TelegramDeliveryOutbox, bool, error)
	TransitionTelegramDelivery(
		ctx context.Context,
		tenantID domain.TenantID,
		deliveryID domain.TelegramDeliveryID,
		to domain.DeliveryStatus,
		at time.Time,
		retryAt *time.Time,
	) error
	GetArtifactManifest(
		ctx context.Context,
		tenantID domain.TenantID,
		manifestID domain.ArtifactManifestID,
	) (domain.ArtifactManifest, bool, error)
}

type DispatchReady struct {
	TenantID  domain.TenantID
	OutboxID  domain.DispatchOutboxID
	RunID     domain.RunID
	AttemptID domain.AttemptID
}

type DispatchAdmissionRequest struct {
	TenantID      domain.TenantID
	OutboxID      domain.DispatchOutboxID
	RunID         domain.RunID
	AttemptID     domain.AttemptID
	ReservationID domain.QuotaReservationID
	Now           time.Time
	HoldUntil     time.Time
	Limits        domain.ProductLimits
	Workload      domain.WorkloadShape
}

type DispatchAdmissionResult struct {
	Admitted  bool
	State     domain.SchedulerState
	Code      string
	RetryAt   *time.Time
	RunID     domain.RunID
	AttemptID domain.AttemptID
}

type ExpiredQuotaReservation struct {
	TenantID                 domain.TenantID
	ReservationID            domain.QuotaReservationID
	RunID                    domain.RunID
	SubscriptionConnectionID domain.SubscriptionConnectionID
	ExpiresAt                time.Time
}

// SchedulerStore exposes only bounded ready/expiry traversals and atomic
// admission mutations. The queue publisher never reconstructs state by
// scanning tenant payloads.
type SchedulerStore interface {
	ListReadyDispatches(
		ctx context.Context,
		bucket uint32,
		before time.Time,
		limit uint64,
	) ([]DispatchReady, error)
	AdmitDispatch(
		ctx context.Context,
		request DispatchAdmissionRequest,
	) (DispatchAdmissionResult, error)
	AcknowledgeDispatch(
		ctx context.Context,
		tenantID domain.TenantID,
		outboxID domain.DispatchOutboxID,
		at time.Time,
	) error
	ListExpiredQuotaReservations(
		ctx context.Context,
		bucket uint32,
		before time.Time,
		limit uint64,
	) ([]ExpiredQuotaReservation, error)
	ExpireQuotaReservation(
		ctx context.Context,
		candidate ExpiredQuotaReservation,
		at time.Time,
	) (bool, error)
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
	SessionID         domain.SessionID
	TriggerEventID    domain.SessionEventID
	AttemptID         domain.AttemptID
	WorkDir           string
	ContextSnapshot   domain.BlobRef
	InputArtifacts    []domain.Artifact
	ResumeCheckpoint  *domain.Checkpoint
	Credential        CredentialHandle
	AllowedMCPServers []string
}

type ExecutionEvent struct {
	Sequence        uint64
	Boundary        string
	CheckpointState []byte
	InputTokens     *uint64
	OutputTokens    *uint64
}

type ExecutionEventSink interface {
	Emit(ctx context.Context, event ExecutionEvent) error
}

type ExecutionResult struct {
	Summary string
	Outputs []ExecutionOutput
}

type ExecutionOutput struct {
	Name         string
	MediaType    string
	RelativePath string
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

type WorkerJobState struct {
	Job           domain.WorkerJob
	Run           domain.Run
	Attempt       domain.Attempt
	Reservation   domain.QuotaReservation
	InputManifest domain.ArtifactManifest
	Checkpoint    *domain.Checkpoint
}

type WorkerLeaseRequest struct {
	TenantID  domain.TenantID
	RunID     domain.RunID
	AttemptID domain.AttemptID
	LeaseID   domain.LeaseID
	WorkerID  string
	Now       time.Time
	ExpiresAt time.Time
}

type WorkerEventCommit struct {
	Checkpoint domain.Checkpoint
	Usage      *domain.UsageObservation
	LeaseID    domain.LeaseID
	Fence      uint64
	At         time.Time
}

type WorkerCompletion struct {
	TenantID      domain.TenantID
	RunID         domain.RunID
	AttemptID     domain.AttemptID
	ReservationID domain.QuotaReservationID
	LeaseID       domain.LeaseID
	Fence         uint64
	At            time.Time
	Manifest      domain.ArtifactManifest
	Delivery      domain.TelegramDeliveryOutbox
	Usage         []domain.UsageObservation
}

type WorkerFailure struct {
	TenantID      domain.TenantID
	RunID         domain.RunID
	AttemptID     domain.AttemptID
	ReservationID domain.QuotaReservationID
	LeaseID       domain.LeaseID
	Fence         uint64
	At            time.Time
	Cancelled     bool
	Code          string
	Delivery      domain.TelegramDeliveryOutbox
}

// WorkerStateStore exposes the durable lifecycle boundary required by one
// isolated, concurrency-one worker invocation.
type WorkerStateStore interface {
	LoadWorkerJob(ctx context.Context, tenantID domain.TenantID, runID domain.RunID) (WorkerJobState, bool, error)
	ClaimWorkerLease(ctx context.Context, request WorkerLeaseRequest) (domain.Lease, error)
	StartWorkerJob(ctx context.Context, state WorkerJobState, lease domain.Lease, at time.Time) error
	RenewWorkerLease(
		ctx context.Context,
		tenantID domain.TenantID,
		leaseID domain.LeaseID,
		fence uint64,
		now time.Time,
		newExpiry time.Time,
	) (domain.Lease, error)
	CommitWorkerEvent(ctx context.Context, event WorkerEventCommit) error
	CompleteWorkerJob(ctx context.Context, completion WorkerCompletion) error
	FailWorkerJob(ctx context.Context, failure WorkerFailure) error
	CancellationRequested(ctx context.Context, tenantID domain.TenantID, runID domain.RunID) (bool, error)
}
