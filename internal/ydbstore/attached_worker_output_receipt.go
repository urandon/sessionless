package ydbstore

import (
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
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
