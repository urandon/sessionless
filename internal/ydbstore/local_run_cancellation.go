package ydbstore

import (
	"context"
	"database/sql"
	"errors"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// The original managed cancellation writer. It uses canonical Run/Attempt/
// outbox and the existing reservation/release transaction, not a cleanup loop
// or a new cancellation ledger. Running/possibly-active work retains its hold
// for the original worker's cleanup and fenced terminal finalizer.
func (store *Store) RequestManagedRunCancellationV1(ctx context.Context, request ports.ManagedRunCancellationRequestV1) (result ports.ManagedRunCancellationResultV1, err error) {
	if ctx == nil || ctx.Err() != nil || store == nil || store.db == nil {
		return result, domain.ErrMembershipDenied
	}
	for _, validate := range []func() error{request.TenantID.Validate, request.UserID.Validate, request.SessionID.Validate, request.RunID.Validate, request.DispatchID.Validate} {
		if err := validate(); err != nil {
			return result, err
		}
	}
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = ports.ManagedRunCancellationResultV1{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		tx := q.canonicalState(store, request.TenantID)
		run, found, err := tx.GetRun(ctx, request.RunID)
		if err != nil || !found || run.Validate() != nil || run.ID != request.RunID || run.TenantID != request.TenantID || run.SessionID != request.SessionID || run.UpdatedAt.After(q.at) ||
			run.LocalCapacityAssociation == nil || run.LocalCapacityAssociation.OwnerUserID != request.UserID {
			return q.budget.deny()
		}
		if _, _, err := readOwnedSessionPrincipal(ctx, q, request.TenantID, request.SessionID, request.UserID); err != nil {
			return err
		}
		outbox, found, err := readJSON[domain.DispatchOutbox](ctx, q, `SELECT payload FROM dispatch_outbox WHERE tenant_id = $1 AND dispatch_outbox_id = $2`, request.TenantID, request.DispatchID)
		if err != nil || !found || outbox.ID != request.DispatchID || outbox.RunID != run.ID || outbox.UpdatedAt.After(q.at) {
			return q.budget.deny()
		}
		attempt, found, err := tx.GetAttempt(ctx, outbox.AttemptID)
		if err != nil || !found || outbox.ValidateForAttempt(run, attempt) != nil || attempt.UpdatedAt.After(q.at) {
			return q.budget.deny()
		}
		if run.Status.Terminal() {
			result = ports.ManagedRunCancellationResultV1{RunID: run.ID, Status: run.Status, Replayed: true}
			return nil
		}
		if run.CancellationRequestedAt != nil {
			result = ports.ManagedRunCancellationResultV1{RunID: run.ID, Status: run.Status, Replayed: true}
			return nil
		}
		// Observing any lease, including an expired one, is not proof that its
		// external effect stopped. Cancellation only marks intent in that case.
		var lease string
		leaseErr := q.scanMetadata(ctx, `SELECT SUBSTRING(CAST(lease_id AS String),0,161) FROM lease_heads WHERE tenant_id=$1 AND run_id=$2`, []any{run.TenantID, run.ID}, &lease)
		if leaseErr != nil && !errors.Is(leaseErr, sql.ErrNoRows) || leaseErr == nil && domain.LeaseID(lease).Validate() != nil {
			return q.budget.deny()
		}
		if run.Status == domain.RunRunning || leaseErr == nil {
			if err := run.RequestCancellation(q.at); err != nil {
				return err
			}
			run.UpdatedAt = q.at
			if err := tx.PutRun(ctx, run); err != nil {
				return err
			}
			result = ports.ManagedRunCancellationResultV1{RunID: run.ID, Status: run.Status}
			return nil
		}
		job, hasJob, err := readJSON[domain.WorkerJob](ctx, q, `SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, run.TenantID, run.ID)
		if err != nil {
			return err
		}
		source, err := domain.DecodeAttemptExecutionSourceV1([]byte(outbox.OwnedExecutionSourceRecord))
		if err != nil {
			return q.budget.deny()
		}
		final, err := readOwnedAdmission(ctx, q, source.Scope)
		if err != nil {
			return err
		}
		if !hasJob {
			// An unadmitted Run has no reservation to release. Still inspect its
			// exact contention key so an inconsistent held slot cannot be hidden.
			if final != nil || run.Status != domain.RunCreated || attempt.Status != domain.AttemptCreated || outbox.Status != domain.DispatchPending {
				return q.budget.deny()
			}
			slot, slotErr := capacitySchedulerSlot(ctx, tx, "", run.LocalCapacityAssociation, false, q.at, false)
			if slotErr != nil && !errors.Is(slotErr, sql.ErrNoRows) || slotErr == nil && slot.ActiveRunID == run.ID {
				return q.budget.deny()
			}
		} else {
			if run.Status != domain.RunQueued || attempt.Status != domain.AttemptCreated || job.ValidateForRun(run) != nil || job.AttemptID != attempt.ID || job.OwnedExecutionSourceRecord != outbox.OwnedExecutionSourceRecord {
				return q.budget.deny()
			}
			reservation, found, err := readQuotaReservationStorageV1(ctx, tx, run, job.ReservationID)
			if err != nil || !found || reservation.Status != domain.ReservationHeld || !ownedAdmissionMatches(final, source, outbox.OwnedExecutionSourceRecord, reservation, job) {
				return q.budget.deny()
			}
			if err := reservation.Transition(domain.ReservationReleased, q.at); err != nil {
				return err
			}
			if err := tx.PutQuotaReservation(ctx, reservation); err != nil {
				return err
			}
			if err := releaseLocalCapacitySlotTx(ctx, tx, run, reservation, q.at); err != nil {
				return err
			}
			depth, active, err := readSchedulerCounters(ctx, tx, q.at)
			if err != nil || depth == 0 {
				return q.budget.deny()
			}
			if _, err := q.ExecContext(ctx, `UPDATE tenant_scheduler_counters SET queue_depth=$1,active_runs=$2,updated_at=$3 WHERE tenant_id=$4`, depth-1, active, q.at, run.TenantID); err != nil {
				return err
			}
		}
		if err := run.RequestCancellation(q.at); err != nil {
			return err
		}
		if err := run.Transition(domain.RunCancelled, q.at); err != nil {
			return err
		}
		if err := attempt.Transition(domain.AttemptCancelled, q.at); err != nil {
			return err
		}
		if err := tx.PutRun(ctx, run); err != nil {
			return err
		}
		if err := tx.PutAttempt(ctx, attempt); err != nil {
			return err
		}
		if outbox.Status == domain.DispatchPending {
			if err := outbox.Transition(domain.DispatchCancelled, q.at); err != nil {
				return err
			}
			if err := tx.PutDispatchOutbox(ctx, outbox); err != nil {
				return err
			}
		}
		result = ports.ManagedRunCancellationResultV1{RunID: run.ID, Status: run.Status, NoHold: !hasJob}
		return nil
	})
	if err != nil {
		return ports.ManagedRunCancellationResultV1{}, err
	}
	return result, nil
}

var _ ports.ManagedRunCancellationStoreV1 = (*Store)(nil)
