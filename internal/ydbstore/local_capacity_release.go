package ydbstore

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Called only inside the original terminal/queued-expiry transaction. Retained
// ownership, not current funding or subscription entitlement, permits release.
// No missing/foreign slot is recreated or repaired; capacity alone never makes
// the resource ready again. Fresh admission rechecks its actual current Source.
func releaseLocalCapacitySlotTx(ctx context.Context, tx *stateTx, run domain.Run, reservation domain.QuotaReservation, at time.Time) error {
	if run.LocalCapacityAssociation == nil || reservation.ValidateForRun(run) != nil || at.IsZero() {
		return domain.ErrMembershipDenied
	}
	slot, err := capacitySchedulerSlot(ctx, tx, "", run.LocalCapacityAssociation, false, at, false)
	if err != nil {
		return err
	}
	if slot.ActiveRunID != run.ID || slot.ActiveReservationID != reservation.ID || at.Before(slot.UpdatedAt) {
		return domain.ErrMembershipDenied
	}
	slot.ActiveRunID, slot.ActiveReservationID = "", ""
	slot.BlockedUntil = nil
	slot.State, slot.UpdatedAt = domain.SchedulerReauthRequired, at
	return writeSchedulerSlot(ctx, tx, slot)
}
