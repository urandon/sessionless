package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

const workerSnapshotCandidateLimit = 32

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

func (store *Store) LoadWorkerContext(
	ctx context.Context,
	request ports.WorkerContextRequest,
) (domain.SessionContextInput, error) {
	if err := request.Validate(); err != nil {
		return domain.SessionContextInput{}, err
	}
	result := domain.SessionContextInput{
		TenantID: request.TenantID, SessionID: request.SessionID,
	}
	afterSequence := uint64(0)
	if request.AtOrBeforeSnapshotVersion != nil {
		rows, err := store.db.QueryContext(ctx,
			`SELECT record FROM session_snapshots
			 WHERE tenant_id = $1 AND session_id = $2
			   AND version <= $3 AND through_sequence <= $4
			 ORDER BY version DESC LIMIT $5`,
			request.TenantID, request.SessionID,
			*request.AtOrBeforeSnapshotVersion, request.ThroughSequence,
			workerSnapshotCandidateLimit,
		)
		if err != nil {
			return result, err
		}
		for rows.Next() {
			var record string
			if err := rows.Scan(&record); err != nil {
				rows.Close()
				return result, err
			}
			var snapshot domain.SessionSnapshot
			if err := json.Unmarshal([]byte(record), &snapshot); err != nil || snapshot.Validate() != nil {
				continue
			}
			if snapshot.TenantID != request.TenantID || snapshot.SessionID != request.SessionID {
				continue
			}
			result.Snapshot = &snapshot
			afterSequence = snapshot.ThroughSequence
			break
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return result, err
		}
		if err := rows.Close(); err != nil {
			return result, err
		}
	}
	covered := afterSequence
	if result.Snapshot != nil {
		covered = result.Snapshot.EventCount
	}
	if covered > request.MaxEvents {
		return result, domain.ValidationError{
			Field: "worker_context.events", Reason: "snapshot exceeds the admitted event limit",
		}
	}
	remaining := request.MaxEvents - covered
	queryLimit := remaining
	if queryLimit < ^uint64(0) {
		queryLimit++
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT record FROM session_events
		 WHERE tenant_id = $1 AND session_id = $2
		   AND sequence > $3 AND sequence <= $4
		 ORDER BY sequence ASC LIMIT $5`,
		request.TenantID, request.SessionID, afterSequence,
		request.ThroughSequence, queryLimit,
	)
	if err != nil {
		return result, err
	}
	result.Events, err = decodeRows[domain.SessionEvent](rows)
	closeErr := rows.Close()
	if err != nil {
		return result, err
	}
	if closeErr != nil {
		return result, closeErr
	}
	if uint64(len(result.Events)) > remaining {
		return result, domain.ValidationError{
			Field: "worker_context.events", Reason: "exceeds the admitted event limit",
		}
	}
	if err := result.Validate(); err != nil {
		return result, err
	}
	if len(result.Events) == 0 {
		if afterSequence != request.ThroughSequence {
			return result, domain.ValidationError{Field: "worker_context.events", Reason: "does not reach the pinned boundary"}
		}
	} else {
		last := result.Events[len(result.Events)-1]
		if last.Sequence != request.ThroughSequence {
			return result, domain.ValidationError{Field: "worker_context.events", Reason: "does not reach the pinned boundary"}
		}
		if last.ID != request.TriggerEventID {
			return result, domain.ValidationError{Field: "worker_context.trigger_event_id", Reason: "does not match the pinned boundary event"}
		}
	}
	return result, nil
}

func (store *Store) LoadWorkerCredentialInvocation(
	ctx context.Context,
	tenantID domain.TenantID,
	runID domain.RunID,
	attemptID domain.AttemptID,
	leaseID domain.LeaseID,
) (result ports.WorkerCredentialInvocationState, found bool, err error) {
	if err := tenantID.Validate(); err != nil {
		return result, false, err
	}
	if err := runID.Validate(); err != nil {
		return result, false, err
	}
	if err := attemptID.Validate(); err != nil {
		return result, false, err
	}
	if err := leaseID.Validate(); err != nil {
		return result, false, err
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		result = ports.WorkerCredentialInvocationState{}
		found = false
		var currentFound bool
		result.Run, currentFound, err = state.GetRun(ctx, runID)
		if err != nil || !currentFound {
			return err
		}
		result.Attempt, currentFound, err = state.GetAttempt(ctx, attemptID)
		if err != nil || !currentFound {
			return err
		}
		result.Lease, currentFound, err = readJSON[domain.Lease](
			ctx, state.(*stateTx).sqlTx,
			`SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2`,
			tenantID, leaseID,
		)
		if err != nil || !currentFound {
			return err
		}
		found = true
		return nil
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
		return startWorkerJobTx(ctx, state, loaded, lease, at)
	})
}

// startWorkerJobTx is the canonical queued-to-running transition. Remote
// attached-worker claim composes this helper with its durable LeaseAccepted
// record in the same serializable transaction; it must not call StartWorkerJob
// as a second transaction.
func startWorkerJobTx(
	ctx context.Context,
	state ports.StateTx,
	loaded ports.WorkerJobState,
	lease domain.Lease,
	at time.Time,
) error {
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
	if reservation.Status != domain.ReservationHeld || !reservation.ExpiresAt.After(at) {
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
	// Attached offers are durable direct delivery and never increment the
	// managed queue counter during admission. Starting one must therefore not
	// consume an unrelated managed queue slot for the same tenant.
	if loaded.Job.ExecutionPlacement.Kind == domain.ExecutionPlacementManaged && queueDepth > 0 {
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
	if len(completion.Events) == 0 {
		return domain.ValidationError{
			Field: "worker_completion.events", Reason: "must not be empty for canonical finalization",
		}
	}
	return store.Transact(ctx, completion.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		run, attempt, reservation, err := loadWorkerTerminalState(
			ctx, state, tx, completion.RunID, completion.AttemptID, completion.ReservationID,
		)
		if err != nil {
			return err
		}
		if err := validateCanonicalFinalizationEvents(domain.RunSucceeded, completion.Events); err != nil {
			return err
		}
		if err := completion.Manifest.ValidateForRun(run); err != nil {
			return err
		}
		finalizationDigest, err := runFinalizationDigest(
			domain.RunSucceeded, &completion.Manifest, completion.Events,
		)
		if err != nil {
			return err
		}
		matched, err := matchingRunFinalizationTx(
			ctx, tx, run.ID, domain.RunSucceeded, finalizationDigest,
		)
		if err != nil {
			return err
		}
		if matched {
			return nil
		}
		if run.Status.Terminal() {
			return ErrRunFinalizationConflict
		}
		return completeWorkerSuccessTx(
			ctx, state, tx, run, attempt, reservation,
			completion.LeaseID, completion.Fence, completion.At,
			completion.Manifest, completion.Usage,
			func(run domain.Run) error {
				return appendCanonicalFinalizationTx(
					ctx, tx, run, domain.RunSucceeded, finalizationDigest,
					completion.Events, completion.At,
				)
			},
		)
	})
}

func (store *Store) CompleteLegacyTelegramWorkerJob(
	ctx context.Context,
	completion ports.LegacyTelegramWorkerCompletion,
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
		return completeWorkerSuccessTx(
			ctx, state, tx, run, attempt, reservation,
			completion.LeaseID, completion.Fence, completion.At,
			completion.Manifest, completion.Usage,
			func(domain.Run) error {
				return state.PutTelegramDeliveryOutbox(ctx, completion.Delivery)
			},
		)
	})
}

func completeWorkerSuccessTx(
	ctx context.Context,
	state ports.StateTx,
	tx *stateTx,
	run domain.Run,
	attempt domain.Attempt,
	reservation domain.QuotaReservation,
	leaseID domain.LeaseID,
	fence uint64,
	at time.Time,
	manifest domain.ArtifactManifest,
	usage []domain.UsageObservation,
	finalize func(domain.Run) error,
) error {
	if err := requireLeaseOwnership(ctx, tx, run.ID, leaseID, fence, at); err != nil {
		return err
	}
	if err := run.Transition(domain.RunSucceeded, at); err != nil {
		return err
	}
	if err := attempt.Transition(domain.AttemptSucceeded, at); err != nil {
		return err
	}
	if err := reservation.Transition(domain.ReservationCommitted, at); err != nil {
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
	for _, observation := range usage {
		if err := state.AppendUsageObservation(ctx, observation); err != nil {
			return err
		}
	}
	if err := state.PutArtifactManifest(ctx, manifest); err != nil {
		return err
	}
	if err := finalize(run); err != nil {
		return err
	}
	if err := finishWorkerScheduling(ctx, tx, run, reservation, leaseID, fence, at); err != nil {
		return err
	}
	return appendSchedulerAudit(
		ctx, tx, at, "worker.succeeded", "run", string(run.ID), "succeeded",
		map[string]any{"attempt_id": attempt.ID, "manifest_id": manifest.ID},
	)
}

func (store *Store) FailWorkerJob(ctx context.Context, failure ports.WorkerFailure) error {
	if len(failure.Events) == 0 {
		return domain.ValidationError{
			Field: "worker_failure.events", Reason: "must not be empty for canonical finalization",
		}
	}
	return store.Transact(ctx, failure.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		run, attempt, reservation, err := loadWorkerTerminalState(
			ctx, state, tx, failure.RunID, failure.AttemptID, failure.ReservationID,
		)
		if err != nil {
			return err
		}
		runStatus, attemptStatus := domain.RunFailed, domain.AttemptFailed
		if failure.Cancelled {
			runStatus, attemptStatus = domain.RunCancelled, domain.AttemptCancelled
		}
		if err := validateCanonicalFinalizationEvents(runStatus, failure.Events); err != nil {
			return err
		}
		finalizationDigest, err := runFinalizationDigest(runStatus, nil, failure.Events)
		if err != nil {
			return err
		}
		matched, err := matchingRunFinalizationTx(ctx, tx, run.ID, runStatus, finalizationDigest)
		if err != nil {
			return err
		}
		if matched {
			return nil
		}
		if run.Status.Terminal() {
			return ErrRunFinalizationConflict
		}
		return failWorkerTx(
			ctx, state, tx, run, attempt, reservation, runStatus, attemptStatus,
			failure.LeaseID, failure.Fence, failure.At, failure.Code,
			func(run domain.Run) error {
				return appendCanonicalFinalizationTx(
					ctx, tx, run, runStatus, finalizationDigest, failure.Events, failure.At,
				)
			},
		)
	})
}

func (store *Store) FailLegacyTelegramWorkerJob(
	ctx context.Context,
	failure ports.LegacyTelegramWorkerFailure,
) error {
	return store.Transact(ctx, failure.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		run, attempt, reservation, err := loadWorkerTerminalState(
			ctx, state, tx, failure.RunID, failure.AttemptID, failure.ReservationID,
		)
		if err != nil {
			return err
		}
		runStatus, attemptStatus := domain.RunFailed, domain.AttemptFailed
		if failure.Cancelled {
			runStatus, attemptStatus = domain.RunCancelled, domain.AttemptCancelled
		}
		if run.Status.Terminal() {
			return nil
		}
		return failWorkerTx(
			ctx, state, tx, run, attempt, reservation, runStatus, attemptStatus,
			failure.LeaseID, failure.Fence, failure.At, failure.Code,
			func(domain.Run) error {
				return state.PutTelegramDeliveryOutbox(ctx, failure.Delivery)
			},
		)
	})
}

func failWorkerTx(
	ctx context.Context,
	state ports.StateTx,
	tx *stateTx,
	run domain.Run,
	attempt domain.Attempt,
	reservation domain.QuotaReservation,
	runStatus domain.RunStatus,
	attemptStatus domain.AttemptStatus,
	leaseID domain.LeaseID,
	fence uint64,
	at time.Time,
	code string,
	finalize func(domain.Run) error,
) error {
	if err := requireLeaseOwnership(ctx, tx, run.ID, leaseID, fence, at); err != nil {
		return err
	}
	if err := run.Transition(runStatus, at); err != nil {
		return err
	}
	if err := attempt.Transition(attemptStatus, at); err != nil {
		return err
	}
	if err := reservation.Transition(domain.ReservationReleased, at); err != nil {
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
	if err := finalize(run); err != nil {
		return err
	}
	if err := finishWorkerScheduling(ctx, tx, run, reservation, leaseID, fence, at); err != nil {
		return err
	}
	return appendSchedulerAudit(
		ctx, tx, at, "worker."+string(runStatus), "run", string(run.ID), code,
		map[string]any{"attempt_id": attempt.ID},
	)
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

var (
	_ ports.WorkerStateStore               = (*Store)(nil)
	_ ports.LegacyTelegramWorkerStateStore = (*Store)(nil)
)
