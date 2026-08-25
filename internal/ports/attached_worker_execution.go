package ports

import (
	"context"
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

type AttachedWorkerAttemptResult struct {
	Status   AttachedWorkerExecutionStatus
	Attempt  domain.AttachedWorkerAttemptV1
	Outbound *domain.AttachedWorkerAttemptMessageV1
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
	DeadlineAt  time.Time
	TenantID    domain.TenantID
	OwnerUserID domain.UserID
	WorkerID    domain.AttachedWorkerID
	AttemptID   domain.AttemptID
	Kind        domain.AttachedWorkerAttemptDeadlineKind
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
	ListDueAttachedWorkerAttemptDeadlines(context.Context, uint32, time.Time, AttachedWorkerAttemptDeadlineCursor, uint64) ([]domain.AttachedWorkerAttemptDeadlineV1, error)
	FenceAttachedWorkerAttempt(context.Context, AttachedWorkerAttemptFence) (AttachedWorkerAttemptResult, error)
}
