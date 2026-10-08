package ydbstore

import (
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// attachedWorkerOutputReceiptStatusAdmissible is the state/status part of the
// future mutating receipt transaction. It is deliberately insufficient as an
// authorization check: the transaction must also verify the owner-scoped
// worker, connection bearer, generations, exact attempt binding, and cleanup
// observation before storing anything.
func attachedWorkerOutputReceiptStatusAdmissible(
	attempt domain.AttachedWorkerAttemptV1,
	status domain.AttachedWorkerTerminalStatus,
	at time.Time,
) bool {
	at = canonicalAttachedWorkerTime(at)
	if at.IsZero() || !status.Valid() || !at.Before(attempt.LeaseExpiresAt) {
		return false
	}
	switch attempt.State {
	case domain.AttachedWorkerAttemptClaimed:
		return attempt.CancelRevision == 0 && attempt.CancelDeadline.IsZero() &&
			(status == domain.AttachedWorkerTerminalSucceeded || status == domain.AttachedWorkerTerminalFailed)
	case domain.AttachedWorkerAttemptCancelRequested:
		return attempt.CancelRevision > 0 && !attempt.CancelDeadline.IsZero() &&
			status == domain.AttachedWorkerTerminalCancelled &&
			!attachedWorkerCancellationFrameExpired(attempt, at)
	case domain.AttachedWorkerAttemptCancelAcknowledged:
		// CancelAck has already satisfied its acknowledgement deadline. Bounded
		// teardown may legitimately finish after that deadline, but never after
		// the lease/fence ceases to be current.
		return attempt.CancelRevision > 0 && !attempt.CancelDeadline.IsZero() &&
			status == domain.AttachedWorkerTerminalCancelled
	default:
		return false
	}
}

// attachedWorkerOutputReceiptAuthorized is the full current-head predicate
// used by the mutating receipt path. It deliberately differs from the
// read-only sealed-input gate: cancellation can admit only cancelled output,
// while every binding and the presented bearer remain exact.
func attachedWorkerOutputReceiptAuthorized(
	request ports.AttachedWorkerSealedInputAuthorization,
	status domain.AttachedWorkerTerminalStatus,
	at time.Time,
	worker domain.AttachedWorker,
	connection domain.AttachedWorkerConnection,
	attempt domain.AttachedWorkerAttemptV1,
) bool {
	poll := ports.AttachedWorkerAttemptPoll{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		ConnectionID: request.ConnectionID, PresentedSecretDigest: request.PresentedSecretDigest,
	}
	return attachedWorkerAttemptPollAuthorized(poll, at, worker, connection) &&
		attachedWorkerTerminalCommitAuthorityCurrent(worker, connection, attempt) &&
		attachedWorkerOutputReceiptStatusAdmissible(attempt, status, at) &&
		request.AttemptSequence == 1 &&
		worker.EnrollmentGeneration == request.EnrollmentGeneration &&
		connection.ConnectionGeneration == request.ConnectionGeneration &&
		attempt.TenantID == request.TenantID && attempt.OwnerUserID == request.OwnerUserID &&
		attempt.WorkerID == request.WorkerID && attempt.ConnectionID == request.ConnectionID &&
		attempt.ExecutionConnectionID == request.ConnectionID &&
		attempt.ExecutionConnectionGeneration == request.ConnectionGeneration &&
		attempt.EnrollmentGeneration == request.EnrollmentGeneration &&
		attempt.ConnectionGeneration == request.ConnectionGeneration &&
		attempt.RunID == request.RunID && attempt.AttemptID == request.AttemptID &&
		attempt.LeaseID == request.LeaseID && attempt.LeaseGeneration == request.LeaseGeneration &&
		attempt.FenceToken == request.FenceToken &&
		attempt.LeaseExpiresAt.UnixMicro() == request.LeaseExpiresAtUnixMicro &&
		attempt.ContextDigest == request.ContextDigest &&
		attempt.CapabilityDigest == request.CapabilityDigest &&
		attempt.PolicyDigest == request.PolicyDigest &&
		(request.ExpectedAttemptRevision == 0 || attempt.Revision == request.ExpectedAttemptRevision)
}
