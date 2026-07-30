package ydbstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

func (store *Store) LoadWorkerJob(
	ctx context.Context,
	tenantID domain.TenantID,
	runID domain.RunID,
) (result ports.WorkerJobState, found bool, err error) {
	if err := tenantID.Validate(); err != nil {
		return result, false, err
	}
	if err := runID.Validate(); err != nil {
		return result, false, err
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		result.Run, found, err = state.GetRun(ctx, runID)
		if err != nil || !found {
			return err
		}
		result.Job, found, err = readJSON[domain.WorkerJob](
			ctx, tx.sqlTx,
			`SELECT payload FROM worker_jobs WHERE tenant_id = $1 AND run_id = $2`,
			tenantID, runID,
		)
		if err != nil || !found {
			return err
		}
		if err := result.Job.ValidateForRun(result.Run); err != nil {
			return err
		}
		result.Attempt, found, err = state.GetAttempt(ctx, result.Job.AttemptID)
		if err != nil || !found {
			return err
		}
		result.Reservation, found, err = readJSON[domain.QuotaReservation](
			ctx, tx.sqlTx,
			`SELECT payload FROM quota_reservations
			 WHERE tenant_id = $1 AND quota_reservation_id = $2`,
			tenantID, result.Job.ReservationID,
		)
		if err != nil || !found {
			return err
		}
		result.InputManifest, found, err = readJSON[domain.ArtifactManifest](
			ctx, tx.sqlTx,
			`SELECT payload FROM artifact_manifests
			 WHERE tenant_id = $1 AND artifact_manifest_id = $2`,
			tenantID, result.Job.InputManifestID,
		)
		if err != nil || !found {
			return err
		}
		var checkpoint domain.Checkpoint
		var checkpointFound bool
		checkpoint, checkpointFound, err = readJSON[domain.Checkpoint](
			ctx, tx.sqlTx,
			`SELECT payload FROM checkpoints
			 WHERE tenant_id = $1 AND attempt_id = $2
			 ORDER BY sequence DESC LIMIT 1`,
			tenantID, result.Job.AttemptID,
		)
		if err != nil {
			return err
		}
		if checkpointFound {
			result.Checkpoint = &checkpoint
		}
		return validateLoadedWorkerJob(result)
	})
	return result, found, err
}

func validateLoadedWorkerJob(state ports.WorkerJobState) error {
	if err := state.Attempt.ValidateForRun(state.Run); err != nil {
		return err
	}
	if state.Attempt.ID != state.Job.AttemptID {
		return domain.ValidationError{Field: "worker_job.attempt_id", Reason: "does not match stored attempt"}
	}
	if err := state.Reservation.ValidateForRun(state.Run); err != nil {
		return err
	}
	if state.Reservation.ID != state.Job.ReservationID {
		return domain.ValidationError{Field: "worker_job.reservation_id", Reason: "does not match stored reservation"}
	}
	if err := state.InputManifest.ValidateForRun(state.Run); err != nil {
		return err
	}
	if state.InputManifest.ID != state.Job.InputManifestID {
		return domain.ValidationError{Field: "worker_job.input_manifest_id", Reason: "does not match stored manifest"}
	}
	return nil
}

func (store *Store) ClaimWorkerLease(
	ctx context.Context,
	request ports.WorkerLeaseRequest,
) (domain.Lease, error) {
	return store.ClaimLease(ctx, LeaseClaim{
		TenantID: request.TenantID, RunID: request.RunID,
		AttemptID: request.AttemptID, LeaseID: request.LeaseID,
		WorkerID: request.WorkerID, Now: request.Now, ExpiresAt: request.ExpiresAt,
	})
}

func (store *Store) StartWorkerJob(
	ctx context.Context,
	loaded ports.WorkerJobState,
	lease domain.Lease,
	at time.Time,
) error {
	return store.Transact(ctx, loaded.Run.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := requireLeaseOwnership(ctx, tx, loaded.Run.ID, lease.ID, lease.FenceToken, at); err != nil {
			return err
		}
		run, found, err := state.GetRun(ctx, loaded.Run.ID)
		if err != nil || !found {
			return err
		}
		attempt, found, err := state.GetAttempt(ctx, loaded.Attempt.ID)
		if err != nil || !found {
			return err
		}
		if run.Status == domain.RunRunning && attempt.Status == domain.AttemptRunning &&
			attempt.WorkerID == lease.WorkerID {
			return nil
		}
		if run.Status != domain.RunQueued || attempt.Status != domain.AttemptCreated {
			return domain.ValidationError{Field: "worker start", Reason: "run and attempt are not claimable"}
		}
		reservation, found, err := readJSON[domain.QuotaReservation](
			ctx, tx.sqlTx,
			`SELECT payload FROM quota_reservations
			 WHERE tenant_id = $1 AND quota_reservation_id = $2`,
			run.TenantID, loaded.Reservation.ID,
		)
		if err != nil || !found {
			return err
		}
		if reservation.Status != domain.ReservationHeld ||
			!reservation.ExpiresAt.After(at) {
			return domain.ValidationError{Field: "worker reservation", Reason: "must be held and unexpired"}
		}
		if err := run.Transition(domain.RunRunning, at); err != nil {
			return err
		}
		attempt.WorkerID = lease.WorkerID
		if err := attempt.Transition(domain.AttemptRunning, at); err != nil {
			return err
		}
		if err := state.PutRun(ctx, run); err != nil {
			return err
		}
		if err := state.PutAttempt(ctx, attempt); err != nil {
			return err
		}
		queueDepth, activeRuns, err := readSchedulerCounters(ctx, tx, at)
		if err != nil {
			return err
		}
		if queueDepth > 0 {
			queueDepth--
		}
		activeRuns++
		_, err = tx.sqlTx.ExecContext(ctx,
			`UPDATE tenant_scheduler_counters
			 SET queue_depth = $1, active_runs = $2, updated_at = $3
			 WHERE tenant_id = $4`,
			queueDepth, activeRuns, at, run.TenantID,
		)
		return err
	})
}

func (store *Store) RenewWorkerLease(
	ctx context.Context,
	tenantID domain.TenantID,
	leaseID domain.LeaseID,
	fence uint64,
	now time.Time,
	newExpiry time.Time,
) (domain.Lease, error) {
	return store.RenewLease(ctx, tenantID, leaseID, fence, now, newExpiry)
}

func (store *Store) CommitWorkerEvent(ctx context.Context, event ports.WorkerEventCommit) error {
	return store.Transact(ctx, event.Checkpoint.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := requireLeaseOwnership(
			ctx, tx, event.Checkpoint.RunID, event.LeaseID, event.Fence, event.At,
		); err != nil {
			return err
		}
		if err := state.PutCheckpoint(ctx, event.Checkpoint); err != nil {
			return err
		}
		if event.Usage != nil {
			return state.AppendUsageObservation(ctx, *event.Usage)
		}
		return nil
	})
}

func (store *Store) CompleteWorkerJob(
	ctx context.Context,
	completion ports.WorkerCompletion,
) error {
	return store.Transact(ctx, completion.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		run, attempt, reservation, err := loadWorkerTerminalState(
			ctx, state, tx, completion.RunID, completion.AttemptID, completion.ReservationID,
		)
		if err != nil {
			return err
		}
		if run.Status == domain.RunSucceeded {
			return nil
		}
		if err := requireLeaseOwnership(
			ctx, tx, completion.RunID, completion.LeaseID, completion.Fence, completion.At,
		); err != nil {
			return err
		}
		if err := run.Transition(domain.RunSucceeded, completion.At); err != nil {
			return err
		}
		if err := attempt.Transition(domain.AttemptSucceeded, completion.At); err != nil {
			return err
		}
		if err := reservation.Transition(domain.ReservationCommitted, completion.At); err != nil {
			return err
		}
		if err := state.PutRun(ctx, run); err != nil {
			return err
		}
		if err := state.PutAttempt(ctx, attempt); err != nil {
			return err
		}
		if err := state.PutQuotaReservation(ctx, reservation); err != nil {
			return err
		}
		for _, usage := range completion.Usage {
			if err := state.AppendUsageObservation(ctx, usage); err != nil {
				return err
			}
		}
		if err := state.PutArtifactManifest(ctx, completion.Manifest); err != nil {
			return err
		}
		if err := state.PutTelegramDeliveryOutbox(ctx, completion.Delivery); err != nil {
			return err
		}
		if err := finishWorkerScheduling(
			ctx, tx, run, reservation, completion.LeaseID, completion.Fence, completion.At,
		); err != nil {
			return err
		}
		return appendSchedulerAudit(
			ctx, tx, completion.At, "worker.succeeded",
			"run", string(run.ID), "succeeded",
			map[string]any{"attempt_id": attempt.ID, "manifest_id": completion.Manifest.ID},
		)
	})
}

func (store *Store) FailWorkerJob(ctx context.Context, failure ports.WorkerFailure) error {
	return store.Transact(ctx, failure.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		run, attempt, reservation, err := loadWorkerTerminalState(
			ctx, state, tx, failure.RunID, failure.AttemptID, failure.ReservationID,
		)
		if err != nil {
			return err
		}
		if run.Status.Terminal() {
			return nil
		}
		if err := requireLeaseOwnership(
			ctx, tx, failure.RunID, failure.LeaseID, failure.Fence, failure.At,
		); err != nil {
			return err
		}
		runStatus, attemptStatus := domain.RunFailed, domain.AttemptFailed
		if failure.Cancelled {
			runStatus, attemptStatus = domain.RunCancelled, domain.AttemptCancelled
		}
		if err := run.Transition(runStatus, failure.At); err != nil {
			return err
		}
		if err := attempt.Transition(attemptStatus, failure.At); err != nil {
			return err
		}
		if err := reservation.Transition(domain.ReservationReleased, failure.At); err != nil {
			return err
		}
		if err := state.PutRun(ctx, run); err != nil {
			return err
		}
		if err := state.PutAttempt(ctx, attempt); err != nil {
			return err
		}
		if err := state.PutQuotaReservation(ctx, reservation); err != nil {
			return err
		}
		if err := state.PutTelegramDeliveryOutbox(ctx, failure.Delivery); err != nil {
			return err
		}
		if err := finishWorkerScheduling(
			ctx, tx, run, reservation, failure.LeaseID, failure.Fence, failure.At,
		); err != nil {
			return err
		}
		return appendSchedulerAudit(
			ctx, tx, failure.At, "worker."+string(runStatus),
			"run", string(run.ID), failure.Code,
			map[string]any{"attempt_id": attempt.ID},
		)
	})
}

func loadWorkerTerminalState(
	ctx context.Context,
	state ports.StateTx,
	tx *stateTx,
	runID domain.RunID,
	attemptID domain.AttemptID,
	reservationID domain.QuotaReservationID,
) (domain.Run, domain.Attempt, domain.QuotaReservation, error) {
	run, found, err := state.GetRun(ctx, runID)
	if err != nil {
		return run, domain.Attempt{}, domain.QuotaReservation{}, err
	}
	if !found {
		return run, domain.Attempt{}, domain.QuotaReservation{}, fmt.Errorf("run %q not found", runID)
	}
	attempt, found, err := state.GetAttempt(ctx, attemptID)
	if err != nil {
		return run, attempt, domain.QuotaReservation{}, err
	}
	if !found {
		return run, attempt, domain.QuotaReservation{}, fmt.Errorf("attempt %q not found", attemptID)
	}
	reservation, found, err := readJSON[domain.QuotaReservation](
		ctx, tx.sqlTx,
		`SELECT payload FROM quota_reservations
		 WHERE tenant_id = $1 AND quota_reservation_id = $2`,
		tx.tenantID, reservationID,
	)
	if err != nil {
		return run, attempt, reservation, err
	}
	if !found {
		return run, attempt, reservation, fmt.Errorf("reservation %q not found", reservationID)
	}
	return run, attempt, reservation, nil
}

func finishWorkerScheduling(
	ctx context.Context,
	tx *stateTx,
	run domain.Run,
	reservation domain.QuotaReservation,
	leaseID domain.LeaseID,
	fence uint64,
	at time.Time,
) error {
	var entitlement string
	if err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT entitlement_state FROM subscription_connections
		 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
		run.TenantID, run.SubscriptionConnectionID,
	).Scan(&entitlement); err != nil {
		return err
	}
	slot, err := ensureSchedulerSlot(
		ctx, tx, run.SubscriptionConnectionID, domain.EntitlementState(entitlement), at,
	)
	if err != nil {
		return err
	}
	if slot.ActiveRunID == run.ID && slot.ActiveReservationID == reservation.ID {
		slot.ActiveRunID = ""
		slot.ActiveReservationID = ""
		slot.State = domain.SchedulerReady
		slot.BlockedUntil = nil
		slot.UpdatedAt = at
		if err := writeSchedulerSlot(ctx, tx, slot); err != nil {
			return err
		}
	}
	queueDepth, activeRuns, err := readSchedulerCounters(ctx, tx, at)
	if err != nil {
		return err
	}
	if activeRuns > 0 {
		activeRuns--
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`UPDATE tenant_scheduler_counters
		 SET queue_depth = $1, active_runs = $2, updated_at = $3
		 WHERE tenant_id = $4`,
		queueDepth, activeRuns, at, run.TenantID,
	); err != nil {
		return err
	}
	var currentLeaseID string
	var currentFence uint64
	var expiresAt time.Time
	if err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT lease_id, fence_token, expires_at
		 FROM lease_heads WHERE tenant_id = $1 AND run_id = $2`,
		run.TenantID, run.ID,
	).Scan(&currentLeaseID, &currentFence, &expiresAt); err != nil {
		return err
	}
	if currentLeaseID != string(leaseID) || currentFence != fence {
		return ErrLeaseLost
	}
	bucket, err := ydbpartition.BucketV1(string(run.ID))
	if err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`DELETE FROM lease_expiry
		 WHERE tenant_id = $1 AND expires_at = $2 AND run_id = $3`,
		run.TenantID, expiresAt, run.ID,
	); err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`DELETE FROM lease_expiry_v2
		 WHERE shard_bucket = $1 AND expires_at = $2
		 AND tenant_id = $3 AND run_id = $4`,
		bucket, expiresAt, run.TenantID, run.ID,
	); err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`DELETE FROM lease_heads WHERE tenant_id = $1 AND run_id = $2`,
		run.TenantID, run.ID,
	)
	return err
}

func (store *Store) CancellationRequested(
	ctx context.Context,
	tenantID domain.TenantID,
	runID domain.RunID,
) (requested bool, err error) {
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		run, found, err := state.GetRun(ctx, runID)
		if err != nil {
			return err
		}
		if !found {
			return sql.ErrNoRows
		}
		requested = run.CancellationRequestedAt != nil
		return nil
	})
	return requested, err
}

var _ ports.WorkerStateStore = (*Store)(nil)
