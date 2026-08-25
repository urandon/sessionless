package ports

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type AttachedWorkerClaimStatus string

const (
	AttachedWorkerClaimed  AttachedWorkerClaimStatus = "claimed"
	AttachedWorkerDenied   AttachedWorkerClaimStatus = "denied"
	AttachedWorkerExpired  AttachedWorkerClaimStatus = "expired"
	AttachedWorkerConsumed AttachedWorkerClaimStatus = "consumed"
	AttachedWorkerConflict AttachedWorkerClaimStatus = "conflict"
)

func (status AttachedWorkerClaimStatus) Valid() bool {
	switch status {
	case AttachedWorkerClaimed, AttachedWorkerDenied, AttachedWorkerExpired,
		AttachedWorkerConsumed, AttachedWorkerConflict:
		return true
	default:
		return false
	}
}

type AttachedWorkerClaimMutation struct {
	TenantID                   domain.TenantID
	OwnerUserID                domain.UserID
	EnrollmentID               domain.AttachedWorkerEnrollmentID
	ExpectedEnrollmentRevision uint64
	PresentedAudience          string
	PresentedDigest            domain.WorkerBootstrapDigest
	IdentityPublicKey          []byte
}

type AttachedWorkerClaimResult struct {
	Status AttachedWorkerClaimStatus
	Worker domain.AttachedWorker
}

type AttachedWorkerCASMutation struct {
	ExpectedRevision uint64
	Next             domain.AttachedWorker
	Audit            domain.AttachedWorkerAuditEvent
	At               time.Time
}

type AttachedWorkerRevokeMutation struct {
	TenantID         domain.TenantID
	OwnerUserID      domain.UserID
	WorkerID         domain.AttachedWorkerID
	ExpectedRevision uint64
	Next             domain.AttachedWorker
	Audit            domain.AttachedWorkerAuditEvent
	At               time.Time
}

// AttachedWorkerStore is owner scoped. Implementations must put tenant_id and
// owner_user_id first in every lookup and mutation predicate; a cross-owner
// record is indistinguishable from a missing record.
//
// Each mutating method atomically writes its supplied, validated V1 audit
// event with the state mutation. Audit payloads are canonical and content-free.
type AttachedWorkerStore interface {
	CreateAttachedWorkerEnrollment(
		context.Context,
		domain.AttachedWorkerEnrollment,
		domain.AttachedWorkerAuditEvent,
	) error
	LoadAttachedWorkerEnrollment(
		context.Context,
		domain.TenantID,
		domain.UserID,
		domain.AttachedWorkerEnrollmentID,
	) (domain.AttachedWorkerEnrollment, bool, error)
	// ClaimAttachedWorkerEnrollment atomically checks exact scope, revision,
	// digest, expiry and single-use state. The store uses its authoritative
	// transaction time to mark the enrollment consumed and construct the
	// pristine Worker plus V1 Audit; it never receives the raw secret or proof.
	// An exact replay may return Claimed only while that pristine target and
	// audit still match; any later worker mutation makes the replay Consumed.
	ClaimAttachedWorkerEnrollment(
		context.Context,
		AttachedWorkerClaimMutation,
	) (AttachedWorkerClaimResult, error)
	LoadAttachedWorker(
		context.Context,
		domain.TenantID,
		domain.UserID,
		domain.AttachedWorkerID,
	) (domain.AttachedWorker, bool, error)
	ListAttachedWorkers(
		context.Context,
		domain.TenantID,
		domain.UserID,
		domain.AttachedWorkerID,
		uint64,
	) ([]domain.AttachedWorker, error)
	CompareAndSwapAttachedWorker(
		context.Context,
		AttachedWorkerCASMutation,
	) (bool, error)
	// RevokeAttachedWorker is a dedicated deny-first CAS. It atomically sets
	// desired_state=revoked, advances both fences, and appends Audit. It must
	// not claim remote acknowledgement by changing observed_state.
	RevokeAttachedWorker(
		context.Context,
		AttachedWorkerRevokeMutation,
	) (bool, error)
	// ListAttachedWorkerAuditEvents uses an inclusive fromWorkerRevision.
	// Passing zero returns enrollment_created at revision zero. The next page
	// starts at last.WorkerRevision+1; callers must guard MaxUint64 overflow.
	ListAttachedWorkerAuditEvents(
		ctx context.Context,
		tenantID domain.TenantID,
		ownerUserID domain.UserID,
		workerID domain.AttachedWorkerID,
		fromWorkerRevision uint64,
		limit uint64,
	) ([]domain.AttachedWorkerAuditEvent, error)
}
