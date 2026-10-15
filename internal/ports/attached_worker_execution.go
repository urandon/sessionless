package ports

import (
	"context"
	"encoding/hex"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type AttachedWorkerExecutionStatus string

const (
	AttachedWorkerExecutionApplied  AttachedWorkerExecutionStatus = "applied"
	AttachedWorkerExecutionReplayed AttachedWorkerExecutionStatus = "replayed"
	AttachedWorkerExecutionConflict AttachedWorkerExecutionStatus = "conflict"
	AttachedWorkerExecutionDenied   AttachedWorkerExecutionStatus = "denied"
	AttachedWorkerExecutionExpired  AttachedWorkerExecutionStatus = "expired"
	AttachedWorkerExecutionFenced   AttachedWorkerExecutionStatus = "fenced"
	AttachedWorkerExecutionNotFound AttachedWorkerExecutionStatus = "not_found"
)

// AttachedWorkerAttemptOffer contains only point locators, stable idempotency
// IDs, and the requested fixed lifetime. It atomically creates the owner-scoped
// durable head, deadline, and first platform-to-worker message. The store loads
// the authoritative WorkerJob, worker, connection and protocol snapshot in the
// same transaction; it exact-matches the attached placement and derives the
// context/capability/policy digests, generations, revisions and connection ID.
// Transaction time determines CreatedAt/UpdatedAt/deadline.
type AttachedWorkerAttemptOffer struct {
	TenantID      domain.TenantID
	OwnerUserID   domain.UserID
	WorkerID      domain.AttachedWorkerID
	RunID         domain.RunID
	AttemptID     domain.AttemptID
	ReservationID domain.QuotaReservationID
	LeaseID       domain.LeaseID
	LeaseTTL      time.Duration
}

type AttachedWorkerAttemptExchange struct {
	TenantID              domain.TenantID
	OwnerUserID           domain.UserID
	WorkerID              domain.AttachedWorkerID
	ConnectionID          domain.AttachedWorkerConnectionID
	AttemptID             domain.AttemptID
	LeaseGeneration       uint64
	PresentedSecretDigest domain.AttachedWorkerConnectionSecretDigest
	// InboundFrame is untrusted transient input. The store validates it against
	// the authoritative connection snapshot, clones any retained bytes, applies
	// the protocol reducer, and derives the durable message record itself.
	InboundFrame attachedworkerprotocol.FrameV1
}

// AttachedWorkerAttemptPoll reauthorizes the current connection after a
// heartbeat checkpoint and returns at most one already-durable platform frame.
// A LeaseOffer is delivery evidence only; execution still requires the atomic
// LeaseClaim -> LeaseAccepted transition.
type AttachedWorkerAttemptPoll struct {
	TenantID              domain.TenantID
	OwnerUserID           domain.UserID
	WorkerID              domain.AttachedWorkerID
	ConnectionID          domain.AttachedWorkerConnectionID
	PresentedSecretDigest domain.AttachedWorkerConnectionSecretDigest
}

// AttachedWorkerSealedInputAuthorization is a read-only, exact-head gate for
// materialization after LeaseAccepted. The store must check the presented
// connection secret, current connection, claimed attempt, lease time, and all
// immutable bindings in one authoritative transaction before any input bytes
// are fetched. ExpectedAttemptRevision lets a caller repeat the gate after
// reading blobs and reject a head changed during that read.
type AttachedWorkerSealedInputAuthorization struct {
	TenantID                domain.TenantID
	OwnerUserID             domain.UserID
	WorkerID                domain.AttachedWorkerID
	ConnectionID            domain.AttachedWorkerConnectionID
	PresentedSecretDigest   domain.AttachedWorkerConnectionSecretDigest
	EnrollmentGeneration    uint64
	ConnectionGeneration    uint64
	RunID                   domain.RunID
	AttemptID               domain.AttemptID
	AttemptSequence         uint64
	LeaseID                 domain.LeaseID
	LeaseGeneration         uint64
	FenceToken              domain.AttachedWorkerFenceToken
	LeaseExpiresAtUnixMicro int64
	ContextDigest           domain.AttachedWorkerContextDigest
	CapabilityDigest        domain.AttachedWorkerCapabilityDigest
	PolicyDigest            domain.AttachedWorkerPolicyDigest
	ExpectedAttemptRevision uint64
}

type AttachedWorkerSealedInputAuthorizationResult struct {
	Status          AttachedWorkerExecutionStatus
	AttemptRevision uint64
	// Attempt is the actual claimed head read in the authorization transaction,
	// not a reconstruction from the worker's wire binding.
	Attempt domain.AttachedWorkerAttemptV1
}

// Matches checks the complete immutable materialization scope. A matching
// value is not an issuer: only an authenticated authorizer may produce it.
func (result AttachedWorkerSealedInputAuthorizationResult) Matches(request AttachedWorkerSealedInputAuthorization) bool {
	a := result.Attempt
	return result.Status == AttachedWorkerExecutionApplied && result.AttemptRevision != 0 &&
		a.Validate() == nil && a.Revision == result.AttemptRevision && a.State == domain.AttachedWorkerAttemptClaimed &&
		a.TenantID == request.TenantID && a.OwnerUserID == request.OwnerUserID && a.WorkerID == request.WorkerID &&
		a.ConnectionID == request.ConnectionID && a.EnrollmentGeneration == request.EnrollmentGeneration &&
		a.ConnectionGeneration == request.ConnectionGeneration && a.RunID == request.RunID && a.AttemptID == request.AttemptID &&
		a.LeaseID == request.LeaseID && a.LeaseGeneration == request.LeaseGeneration && a.FenceToken == request.FenceToken &&
		a.LeaseExpiresAt.UnixMicro() == request.LeaseExpiresAtUnixMicro && a.ContextDigest == request.ContextDigest &&
		a.CapabilityDigest == request.CapabilityDigest && a.PolicyDigest == request.PolicyDigest &&
		request.AttemptSequence == 1 &&
		(request.ExpectedAttemptRevision == 0 || a.Revision == request.ExpectedAttemptRevision)
}

// AuthorizationForAttachedInput derives only comparison fields from an already
// authenticated local request. It never constructs or authorizes an attempt.
func AuthorizationForAttachedInput(tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID,
	connection domain.AttachedWorkerConnectionID, enrollment, generation uint64,
	binding attachedworkerprotocol.AttemptBindingV1, sequence uint64,
) AttachedWorkerSealedInputAuthorization {
	return AttachedWorkerSealedInputAuthorization{
		TenantID: tenant, OwnerUserID: owner, WorkerID: worker, ConnectionID: connection,
		EnrollmentGeneration: enrollment, ConnectionGeneration: generation,
		RunID: domain.RunID(binding.RunID), AttemptID: domain.AttemptID(binding.AttemptID), AttemptSequence: sequence,
		LeaseID: domain.LeaseID(binding.LeaseID), LeaseGeneration: binding.LeaseGeneration,
		FenceToken: domain.AttachedWorkerFenceToken(binding.FenceToken), LeaseExpiresAtUnixMicro: binding.ExpiresAtUnixMicro,
		ContextDigest:    domain.AttachedWorkerContextDigest(hex.EncodeToString(binding.ContextDigest)),
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(binding.CapabilityDigest)),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(hex.EncodeToString(binding.PolicyDigest)),
	}
}

type AttachedWorkerSealedInputAuthorizer interface {
	AuthorizeAttachedWorkerSealedInput(context.Context, AttachedWorkerSealedInputAuthorization) (AttachedWorkerSealedInputAuthorizationResult, error)
}

// This result is produced only by the actual same-transaction current-source
// authorizer. Its metadata is not a local send grant: the genuine original
// owner still checks the matching invocation/RPC, retained monotonic L/R/S/F
// and the actual same-controller physical boundary without renewing a permit.
type AttachedWorkerPiAPIUseResultV1 struct {
	Status                              AttachedWorkerExecutionStatus
	AcceptedAttempt                     domain.AttachedWorkerAttemptV1
	AcceptedAttemptRevision             uint64
	HarnessBindingDigest                domain.HarnessBindingDigestV2
	AttachedPiAPIInvocationPolicyDigest domain.AttachedPiAPIInvocationPolicyMappingDigestV1
	CheckedAt, UseExpiresAt             time.Time
}

// Scalar arguments preserve the existing closed phase wire contract without
// adding an alternative public DTO, token, new source issuer or Boolean permit.
// The bearer transport derives PresentedSecretDigest; the store reads actual
// Job/Run/Attempt/Lease, principal and current API/P-E-T-R-M in ONE transaction.
type AttachedWorkerPiAPIUseAuthorizerV1 interface {
	AuthorizeAttachedWorkerPiAPIUse(context.Context, AttachedWorkerSealedInputAuthorization,
		domain.HarnessBindingDigestV2, domain.AttachedPiAPIInvocationPolicyMappingDigestV1,
		uint64, string) (AttachedWorkerPiAPIUseResultV1, error)
}

type AttachedWorkerAttemptResult struct {
	Status   AttachedWorkerExecutionStatus
	Attempt  domain.AttachedWorkerAttemptV1
	Outbound *domain.AttachedWorkerAttemptMessageV1
	// Historical is true only when an exact retained-ledger replay no longer
	// has a current singleton attempt head. Attempt is then intentionally zero;
	// Outbound is the canonical retained response and remains fully scoped.
	Historical bool
}

// AttachedWorkerTerminalMaterialization contains canonical server-side result
// inputs. Exactly one of Completion or Failure is present; terminal evidence
// alone is never treated as enough to reconstruct canonical worker output.
type AttachedWorkerTerminalMaterialization struct {
	EvidenceDigest domain.AttachedWorkerTerminalEvidenceDigest
	Completion     *WorkerCompletion
	Failure        *WorkerFailure
}

type AttachedWorkerTerminalCommit struct {
	TenantID        domain.TenantID
	OwnerUserID     domain.UserID
	WorkerID        domain.AttachedWorkerID
	AttemptID       domain.AttemptID
	LeaseGeneration uint64
	Materialization AttachedWorkerTerminalMaterialization
}

type AttachedWorkerCancellationRequest struct {
	TenantID        domain.TenantID
	OwnerUserID     domain.UserID
	WorkerID        domain.AttachedWorkerID
	AttemptID       domain.AttemptID
	LeaseGeneration uint64
	AckTimeout      time.Duration
}

type AttachedWorkerFenceReason string

const (
	AttachedWorkerFenceLeaseExpired     AttachedWorkerFenceReason = "lease_expired"
	AttachedWorkerFenceCancelAckUnknown AttachedWorkerFenceReason = "cancel_ack_unknown"
)

type AttachedWorkerAttemptFence struct {
	TenantID        domain.TenantID
	OwnerUserID     domain.UserID
	WorkerID        domain.AttachedWorkerID
	AttemptID       domain.AttemptID
	LeaseGeneration uint64
	// CandidateAttemptRevision comes from the durable deadline row. It is a
	// stale-work guard for the internal poller, not authority supplied by a
	// worker, broker, or HTTP request.
	CandidateAttemptRevision uint64
	Reason                   AttachedWorkerFenceReason
	DeadlineAt               time.Time
}

// AttachedWorkerAttemptDeadlineCursor is exclusive and mirrors every primary
// key component after bucket. It remains lossless when many deadlines share
// YDB's microsecond timestamp precision.
type AttachedWorkerAttemptDeadlineCursor struct {
	Present     bool
	DeadlineAt  time.Time
	TenantID    domain.TenantID
	OwnerUserID domain.UserID
	WorkerID    domain.AttachedWorkerID
	AttemptID   domain.AttemptID
	Kind        domain.AttachedWorkerAttemptDeadlineKind
}

// AttachedWorkerAttemptDeadlinePage advances by the raw physical key even
// when a legacy/corrupt row cannot become a domain deadline. Callers must keep
// paging while HasMore is true; SkippedInvalid is an operational quarantine
// signal and never turns malformed index data into execution authority.
type AttachedWorkerAttemptDeadlinePage struct {
	Items          []domain.AttachedWorkerAttemptDeadlineV1
	NextCursor     AttachedWorkerAttemptDeadlineCursor
	HasMore        bool
	SkippedInvalid uint64
}

// AttachedWorkerExecutionStore is deny-first and owner scoped. All mutation
// methods are single authoritative transactions: exact replay may reconcile to
// Replayed, while a divergent payload/fingerprint or stale generation never
// mutates durable state. Store transaction time, not a caller clock, decides
// lease/cancellation expiry. Attached-worker leases have fixed expiry and no
// renewal operation.
type AttachedWorkerExecutionStore interface {
	OfferAttachedWorkerAttempt(context.Context, AttachedWorkerAttemptOffer) (AttachedWorkerAttemptResult, error)
	PollAttachedWorkerAttempt(context.Context, AttachedWorkerAttemptPoll) (AttachedWorkerAttemptResult, error)
	ExchangeAttachedWorkerAttempt(context.Context, AttachedWorkerAttemptExchange) (AttachedWorkerAttemptResult, error)
	CommitAttachedWorkerTerminal(context.Context, AttachedWorkerTerminalCommit) (AttachedWorkerAttemptResult, error)
	RequestAttachedWorkerCancellation(context.Context, AttachedWorkerCancellationRequest) (AttachedWorkerAttemptResult, error)
	ListDueAttachedWorkerAttemptDeadlines(context.Context, uint32, time.Time, AttachedWorkerAttemptDeadlineCursor, uint64) (AttachedWorkerAttemptDeadlinePage, error)
	FenceAttachedWorkerAttempt(context.Context, AttachedWorkerAttemptFence) (AttachedWorkerAttemptResult, error)
}
