package attachedworkerdaemon

import (
	"errors"
	"time"
)

var errAttachedPiAPIUseDeadlineInvalid = errors.New("attached Pi API use deadline is invalid")

// attachedPiAPIUseDeadlineV1 translates one server snapshot's remaining lifetime
// into the original parent's monotonic clock. It is timing arithmetic, NOT an
// authorization decision or a send permit. The caller must first authenticate
// and match the reply's status, original head, binding, policy and phase, and
// still check cancellation and this deadline immediately before use.
//
// sentAt and receivedAt must be the original parent's monotonic samples around
// the entire authorization RPC. earlierDeadline must already be the minimum of
// every required existing local caller/run/process deadline. A serialized time
// cannot replace any of these local samples. Presence checks cannot establish
// their provenance, which remains the caller's responsibility. safetySlack is
// an explicitly supplied positive conservative margin, never a default grant.
// Server timestamps belong to one server clock; they are never compared with
// local wall time. This helper neither samples a clock nor renews authority.
func attachedPiAPIUseDeadlineV1(checkedAt, useExpiresAt, sentAt, receivedAt, earlierDeadline time.Time, safetySlack time.Duration) (time.Time, error) {
	if checkedAt.IsZero() || useExpiresAt.IsZero() || sentAt.IsZero() || receivedAt.IsZero() || earlierDeadline.IsZero() ||
		safetySlack <= 0 ||
		sentAt == sentAt.Round(0) || receivedAt == receivedAt.Round(0) || earlierDeadline == earlierDeadline.Round(0) {
		return time.Time{}, errAttachedPiAPIUseDeadlineInvalid
	}

	// Wire timestamps carry server wall time, not this process's monotonic clock.
	checkedAt = checkedAt.Round(0)
	useExpiresAt = useExpiresAt.Round(0)
	window := useExpiresAt.Sub(checkedAt)
	if window <= 0 || !checkedAt.Add(window).Equal(useExpiresAt) {
		return time.Time{}, errAttachedPiAPIUseDeadlineInvalid
	}

	roundTrip := receivedAt.Sub(sentAt)
	measuredReceive := sentAt.Add(roundTrip)
	if roundTrip < 0 || measuredReceive == measuredReceive.Round(0) || !measuredReceive.Equal(receivedAt) {
		return time.Time{}, errAttachedPiAPIUseDeadlineInvalid
	}
	// Subtract separately: adding roundTrip and slack first can overflow.
	if roundTrip >= window {
		return time.Time{}, errAttachedPiAPIUseDeadlineInvalid
	}
	remaining := window - roundTrip
	if safetySlack >= remaining {
		return time.Time{}, errAttachedPiAPIUseDeadlineInvalid
	}
	remaining -= safetySlack

	localWindow := earlierDeadline.Sub(receivedAt)
	localEnd := receivedAt.Add(localWindow)
	if localWindow <= 0 || localEnd == localEnd.Round(0) || !localEnd.Equal(earlierDeadline) {
		return time.Time{}, errAttachedPiAPIUseDeadlineInvalid
	}
	// Intersect before Add so a safe earlier bound never requires constructing
	// an out-of-range, wall-only endpoint for a much longer server interval.
	if localWindow < remaining {
		remaining = localWindow
	}
	deadline := receivedAt.Add(remaining)
	if deadline == deadline.Round(0) || deadline.Sub(receivedAt) != remaining || !deadline.Add(-remaining).Equal(receivedAt) {
		return time.Time{}, errAttachedPiAPIUseDeadlineInvalid
	}
	return deadline, nil
}
