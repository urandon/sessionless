package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/scheduler"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

func (store *Store) GetDispatch(
	ctx context.Context,
	tenantID domain.TenantID,
	outboxID domain.DispatchOutboxID,
) (result ports.DispatchReady, status domain.DispatchStatus, found bool, err error) {
	if err := tenantID.Validate(); err != nil {
		return result, status, false, err
	}
	if err := outboxID.Validate(); err != nil {
		return result, status, false, err
	}
	outbox, found, err := readJSON[domain.DispatchOutbox](ctx, store.db,
		`SELECT payload FROM dispatch_outbox
		 WHERE tenant_id = $1 AND dispatch_outbox_id = $2`,
		tenantID, outboxID,
	)
	if err != nil || !found {
		return result, status, found, err
	}
	return ports.DispatchReady{
		TenantID: tenantID, OutboxID: outboxID,
		RunID: outbox.RunID, AttemptID: outbox.AttemptID,
	}, outbox.Status, true, nil
}

func (store *Store) ListReadyDispatches(
	ctx context.Context,
	bucket uint32,
	before time.Time,
	limit uint64,
) (result []ports.DispatchReady, err error) {
	if bucket >= ydbpartition.BucketCountV1 {
		return nil, domain.ValidationError{
			Field: "dispatch bucket", Reason: "must be within the v1 bucket range",
		}
	}
	if before.IsZero() || limit == 0 {
		return nil, domain.ValidationError{
			Field: "dispatch listing", Reason: "requires a non-zero time and positive limit",
		}
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT tenant_id, dispatch_outbox_id, run_id, attempt_id
		 FROM dispatch_ready_v2
		 WHERE shard_bucket = $1 AND available_at <= $2
		 ORDER BY available_at, tenant_id, dispatch_outbox_id
		 LIMIT $3`,
		bucket, before, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item ports.DispatchReady
		if err := rows.Scan(
			&item.TenantID, &item.OutboxID, &item.RunID, &item.AttemptID,
		); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (store *Store) AdmitDispatch(
	ctx context.Context,
	request ports.DispatchAdmissionRequest,
) (result ports.DispatchAdmissionResult, err error) {
	if err := validateAdmissionRequest(request); err != nil {
		return result, err
	}
	result.RunID = request.RunID
	result.AttemptID = request.AttemptID
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		outbox, found, err := readJSON[domain.DispatchOutbox](ctx, tx.sqlTx,
			`SELECT payload FROM dispatch_outbox
			 WHERE tenant_id = $1 AND dispatch_outbox_id = $2`,
			request.TenantID, request.OutboxID,
		)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("dispatch outbox %q not found", request.OutboxID)
		}
		if outbox.RunID != request.RunID || outbox.AttemptID != request.AttemptID {
			return domain.ValidationError{
				Field: "dispatch candidate", Reason: "does not match the stored outbox",
			}
		}
		switch outbox.ExecutionPlacement.Kind {
		case domain.ExecutionPlacementManaged:
			result.Delivery = ports.DispatchDeliveryManagedQueue
		case domain.ExecutionPlacementAttachedWorker:
			result.Delivery = ports.DispatchDeliveryAttachedOffer
		default:
			return domain.ValidationError{Field: "dispatch_outbox.execution_placement", Reason: "is unknown"}
		}
		if outbox.Status != domain.DispatchPending {
			result.Code = "dispatch_not_pending"
			return nil
		}
		run, found, err := state.GetRun(ctx, request.RunID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("run %q not found", request.RunID)
		}
		result.SessionID = run.SessionID
		if outbox.ContextWindow != nil {
			result.ThroughSequence = outbox.ContextWindow.ThroughSequence
		}
		if _, found, err := state.GetAttempt(ctx, request.AttemptID); err != nil {
			return err
		} else if !found {
			return fmt.Errorf("attempt %q not found", request.AttemptID)
		}

		var entitlementValue, quotaValue string
		if err := tx.sqlTx.QueryRowContext(ctx,
			`SELECT entitlement_state, quota_state
			 FROM subscription_connections
			 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
			request.TenantID, run.SubscriptionConnectionID,
		).Scan(&entitlementValue, &quotaValue); err != nil {
			return err
		}
		slot, err := ensureSchedulerSlot(
			ctx, tx, run.SubscriptionConnectionID,
			domain.EntitlementState(entitlementValue), request.Now,
		)
		if err != nil {
			return err
		}
		if slot.ActiveRunID == request.RunID &&
			slot.ActiveReservationID == request.ReservationID {
			reservation, found, err := readJSON[domain.QuotaReservation](ctx, tx.sqlTx,
				`SELECT payload FROM quota_reservations
				 WHERE tenant_id = $1 AND quota_reservation_id = $2`,
				request.TenantID, request.ReservationID,
			)
			if err != nil {
				return err
			}
			if found && reservation.Status == domain.ReservationHeld {
				result.Admitted = true
				result.State = slot.State
				result.Code = "already_admitted"
				return nil
			}
		}

		queueDepth, activeRuns, err := readSchedulerCounters(ctx, tx, request.Now)
		if err != nil {
			return err
		}
		decision, err := scheduler.Evaluate(
			request.Now,
			request.Limits,
			request.Workload,
			scheduler.Snapshot{
				Entitlement: domain.EntitlementState(entitlementValue),
				Quota:       domain.ProviderQuotaState(quotaValue),
				Slot:        slot,
				QueueDepth:  queueDepth,
				ActiveRuns:  activeRuns,
			},
		)
		if err != nil {
			return err
		}
		result.Admitted = decision.Admit
		result.State = decision.State
		result.Code = decision.Code
		result.RetryAt = decision.RetryAt
		previousState := slot.State
		previousBlockedUntil := slot.BlockedUntil
		slot.State = decision.State
		slot.BlockedUntil = decision.RetryAt
		slot.UpdatedAt = request.Now

		if !decision.Admit {
			if run.Status == domain.RunQuotaBlocked &&
				previousState == decision.State &&
				equalOptionalTime(previousBlockedUntil, decision.RetryAt) {
				return nil
			}
			if run.Status != domain.RunQuotaBlocked && !run.Status.Terminal() {
				if err := run.Transition(domain.RunQuotaBlocked, request.Now); err != nil {
					return err
				}
				if err := state.PutRun(ctx, run); err != nil {
					return err
				}
			}
			if err := writeSchedulerSlot(ctx, tx, slot); err != nil {
				return err
			}
			return appendSchedulerAudit(
				ctx, tx, request.Now, "scheduler.admission_blocked",
				"run", string(request.RunID), decision.Code,
				map[string]any{"scheduler_state": decision.State},
			)
		}

		switch run.Status {
		case domain.RunCreated, domain.RunQuotaBlocked:
			if err := run.Transition(domain.RunAdmitted, request.Now); err != nil {
				return err
			}
			if err := run.Transition(domain.RunQueued, request.Now); err != nil {
				return err
			}
		case domain.RunQueued:
		default:
			return domain.ValidationError{
				Field: "run.status", Reason: "is not eligible for scheduler admission",
			}
		}
		if err := state.PutRun(ctx, run); err != nil {
			return err
		}
		reservation := domain.QuotaReservation{
			ID: request.ReservationID, TenantID: request.TenantID,
			RunID:                    request.RunID,
			SubscriptionConnectionID: run.SubscriptionConnectionID,
			Status:                   domain.ReservationHeld, CapacityUnits: 1,
			HeldAt: request.Now, ExpiresAt: request.HoldUntil,
			UpdatedAt: request.Now,
		}
		if err := state.PutQuotaReservation(ctx, reservation); err != nil {
			return err
		}
		contextWindow, err := selectAdmittedContextWindow(ctx, tx, run.SessionID, outbox.ContextWindow)
		if err != nil {
			return err
		}
		job := domain.WorkerJob{
			TenantID: request.TenantID, RunID: request.RunID,
			SessionID: run.SessionID, TriggerEventID: run.TriggerEventID,
			AttemptID: request.AttemptID, ReservationID: request.ReservationID,
			InputManifestID:       outbox.InputManifestID,
			ContextSnapshot:       outbox.ContextSnapshot,
			ContextWindow:         contextWindow,
			WorkspaceSnapshot:     outbox.WorkspaceSnapshot,
			SkillBundle:           outbox.SkillBundle,
			AllowedMCPServers:     append([]string(nil), outbox.AllowedMCPServers...),
			CredentialOwnerUserID: outbox.CredentialOwnerUserID,
			ExecutionPlacement:    outbox.ExecutionPlacement,
			Limits:                request.Limits,
			Origin:                outbox.Origin,
			DeliveryChat:          outbox.DeliveryChat,
			ReplyToMessageID:      outbox.ReplyToMessageID,
			CreatedAt:             request.Now,
		}
		if err := state.PutWorkerJob(ctx, job); err != nil {
			return err
		}
		if result.Delivery == ports.DispatchDeliveryAttachedOffer {
			leaseID, err := domain.NewAttachedWorkerLeaseIDV1(request.TenantID, request.RunID, request.AttemptID)
			if err != nil {
				return err
			}
			leaseTTL, err := domain.AttachedWorkerLeaseTTLForLimitsV1(job.Limits)
			if err != nil {
				return err
			}
			offerRequest := ports.AttachedWorkerAttemptOffer{
				TenantID: request.TenantID, OwnerUserID: job.ExecutionPlacement.OwnerUserID,
				WorkerID: job.ExecutionPlacement.WorkerID, RunID: request.RunID,
				AttemptID: request.AttemptID, ReservationID: request.ReservationID,
				LeaseID: leaseID, LeaseTTL: leaseTTL,
			}
			if err := validateAttachedWorkerAttemptOffer(offerRequest); err != nil {
				return err
			}
			var offer ports.AttachedWorkerAttemptResult
			if err := store.offerAttachedWorkerAttemptTx(ctx, tx, offerRequest, &offer); err != nil {
				return err
			}
			if offer.Status != ports.AttachedWorkerExecutionApplied && offer.Status != ports.AttachedWorkerExecutionReplayed {
				return ErrAttachedWorkerAttemptConflict
			}
			// The canonical attached-worker lease may outlive the ordinary queue
			// reservation hold. Keep quota fenced through the exact fixed lease;
			// otherwise the expiry reconciler could release capacity before a late
			// but still-authorized claim.
			if reservation.ExpiresAt.Before(offer.Attempt.LeaseExpiresAt) {
				reservation.ExpiresAt = offer.Attempt.LeaseExpiresAt
				reservation.UpdatedAt = request.Now
				if err := state.PutQuotaReservation(ctx, reservation); err != nil {
					return err
				}
			}
		}
		slot.State = domain.SchedulerPressured
		slot.ActiveRunID = request.RunID
		slot.ActiveReservationID = request.ReservationID
		slot.BlockedUntil = nil
		if err := writeSchedulerSlot(ctx, tx, slot); err != nil {
			return err
		}
		if result.Delivery == ports.DispatchDeliveryManagedQueue {
			if _, err := tx.sqlTx.ExecContext(ctx,
				`UPDATE tenant_scheduler_counters
				 SET queue_depth = queue_depth + $1, updated_at = $2
				 WHERE tenant_id = $3`,
				uint32(1), request.Now, request.TenantID,
			); err != nil {
				return err
			}
		}
		return appendSchedulerAudit(
			ctx, tx, request.Now, "scheduler.admitted",
			"run", string(request.RunID), "held",
			map[string]any{
				"reservation_id": request.ReservationID,
				"capacity_units": uint32(1),
				"delivery":       result.Delivery,
			},
		)
	})
	return result, err
}

func selectAdmittedContextWindow(
	ctx context.Context,
	tx *stateTx,
	sessionID domain.SessionID,
	requested *domain.SessionContextWindow,
) (*domain.SessionContextWindow, error) {
	if requested == nil {
		return nil, nil
	}
	window := *requested
	if err := window.Validate(); err != nil {
		return nil, err
	}
	// Snapshot selection belongs to admission; callers may only pin the trigger
	// boundary, not smuggle a preselected snapshot into the durable job.
	window.SnapshotVersion = nil
	window.AfterSequence = 0
	rows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT record FROM session_snapshots
		 WHERE tenant_id = $1 AND session_id = $2 AND through_sequence <= $3
		 ORDER BY version DESC LIMIT $4`,
		tx.tenantID, sessionID, window.ThroughSequence, uint64(16),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var snapshot domain.SessionSnapshot
		if err := json.Unmarshal(payload, &snapshot); err != nil || snapshot.Validate() != nil {
			continue
		}
		if snapshot.TenantID != tx.tenantID || snapshot.SessionID != sessionID {
			continue
		}
		version := snapshot.Version
		window.SnapshotVersion = &version
		window.AfterSequence = snapshot.ThroughSequence
		return &window, nil
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &window, nil
}

func (store *Store) ListExpiredQuotaReservations(
	ctx context.Context,
	bucket uint32,
	before time.Time,
	limit uint64,
) (result []ports.ExpiredQuotaReservation, err error) {
	if bucket >= ydbpartition.BucketCountV1 {
		return nil, domain.ValidationError{
			Field: "quota expiry bucket", Reason: "must be within the v1 bucket range",
		}
	}
	if before.IsZero() || limit == 0 {
		return nil, domain.ValidationError{
			Field: "quota expiry listing", Reason: "requires a non-zero time and positive limit",
		}
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT tenant_id, quota_reservation_id, run_id,
		        subscription_connection_id, expires_at
		 FROM quota_expiry_v2
		 WHERE shard_bucket = $1 AND expires_at <= $2
		 ORDER BY expires_at, tenant_id, quota_reservation_id
		 LIMIT $3`,
		bucket, before, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item ports.ExpiredQuotaReservation
		if err := rows.Scan(
			&item.TenantID, &item.ReservationID, &item.RunID,
			&item.SubscriptionConnectionID, &item.ExpiresAt,
		); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (store *Store) ExpireQuotaReservation(
	ctx context.Context,
	candidate ports.ExpiredQuotaReservation,
	at time.Time,
) (expired bool, err error) {
	if err := validateExpiredReservation(candidate, at); err != nil {
		return false, err
	}
	err = store.Transact(ctx, candidate.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		reservation, found, err := readJSON[domain.QuotaReservation](ctx, tx.sqlTx,
			`SELECT payload FROM quota_reservations
			 WHERE tenant_id = $1 AND quota_reservation_id = $2`,
			candidate.TenantID, candidate.ReservationID,
		)
		if err != nil || !found {
			return err
		}
		if reservation.RunID != candidate.RunID ||
			reservation.SubscriptionConnectionID != candidate.SubscriptionConnectionID {
			return domain.ValidationError{
				Field: "quota expiry candidate", Reason: "does not match the stored reservation",
			}
		}
		if reservation.Status != domain.ReservationHeld || reservation.ExpiresAt.After(at) {
			return nil
		}
		run, found, err := state.GetRun(ctx, reservation.RunID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("run %q not found", reservation.RunID)
		}
		if run.Status == domain.RunRunning {
			return nil
		}
		if err := reservation.Transition(domain.ReservationExpired, at); err != nil {
			return err
		}
		if err := state.PutQuotaReservation(ctx, reservation); err != nil {
			return err
		}
		var entitlementValue string
		if err := tx.sqlTx.QueryRowContext(ctx,
			`SELECT entitlement_state FROM subscription_connections
			 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
			candidate.TenantID, reservation.SubscriptionConnectionID,
		).Scan(&entitlementValue); err != nil {
			return err
		}
		slot, err := ensureSchedulerSlot(
			ctx, tx, reservation.SubscriptionConnectionID,
			domain.EntitlementState(entitlementValue), at,
		)
		if err != nil {
			return err
		}
		if slot.ActiveRunID == reservation.RunID &&
			slot.ActiveReservationID == reservation.ID {
			slot.ActiveRunID = ""
			slot.ActiveReservationID = ""
			slot.State = domain.SchedulerReady
			slot.BlockedUntil = nil
			slot.UpdatedAt = at
			if err := writeSchedulerSlot(ctx, tx, slot); err != nil {
				return err
			}
		}
		if run.Status == domain.RunQueued {
			loaded, found, err := loadWorkerJobStateTx(ctx, tx, run.ID)
			if err != nil {
				return err
			}
			if !found {
				return domain.ValidationError{Field: "quota expiry worker job", Reason: "is missing"}
			}
			if err := run.Transition(domain.RunQuotaBlocked, at); err != nil {
				return err
			}
			if err := state.PutRun(ctx, run); err != nil {
				return err
			}
			queueDepth, activeRuns, err := readSchedulerCounters(ctx, tx, at)
			if err != nil {
				return err
			}
			// Attached-worker admission is delivered directly and never increments
			// the managed queue counter. Expiring its reservation must not consume
			// an unrelated managed queue slot for the tenant.
			if loaded.Job.ExecutionPlacement.Kind == domain.ExecutionPlacementManaged && queueDepth > 0 {
				queueDepth--
			}
			if _, err := tx.sqlTx.ExecContext(ctx,
				`UPDATE tenant_scheduler_counters
				 SET queue_depth = $1, active_runs = $2, updated_at = $3
				 WHERE tenant_id = $4`,
				queueDepth, activeRuns, at, candidate.TenantID,
			); err != nil {
				return err
			}
		}
		expired = true
		return appendSchedulerAudit(
			ctx, tx, at, "scheduler.reservation_expired",
			"quota_reservation", string(reservation.ID), "expired",
			map[string]any{"run_id": reservation.RunID},
		)
	})
	return expired, err
}

func equalOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func ensureSchedulerSlot(
	ctx context.Context,
	tx *stateTx,
	connectionID domain.SubscriptionConnectionID,
	entitlement domain.EntitlementState,
	at time.Time,
) (domain.SubscriptionSchedulerSlot, error) {
	slot := domain.SubscriptionSchedulerSlot{
		TenantID: tx.tenantID, SubscriptionConnectionID: connectionID,
	}
	var state, activeRun, activeReservation string
	var blockedUntil, updatedAt time.Time
	err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT state, active_run_id, active_reservation_id, blocked_until, updated_at
		 FROM subscription_scheduler_slots
		 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
		tx.tenantID, connectionID,
	).Scan(&state, &activeRun, &activeReservation, &blockedUntil, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		slot.State = domain.SchedulerReauthRequired
		if entitlement == domain.EntitlementActive {
			slot.State = domain.SchedulerReady
		}
		slot.UpdatedAt = at
		if err := writeSchedulerSlot(ctx, tx, slot); err != nil {
			return slot, err
		}
		return slot, nil
	}
	if err != nil {
		return slot, err
	}
	slot.State = domain.SchedulerState(state)
	slot.ActiveRunID = domain.RunID(activeRun)
	slot.ActiveReservationID = domain.QuotaReservationID(activeReservation)
	slot.UpdatedAt = updatedAt
	if slot.State == domain.SchedulerBlockedUntilReset {
		blocked := blockedUntil
		slot.BlockedUntil = &blocked
	}
	return slot, slot.Validate()
}

func writeSchedulerSlot(
	ctx context.Context,
	tx *stateTx,
	slot domain.SubscriptionSchedulerSlot,
) error {
	if err := slot.Validate(); err != nil {
		return err
	}
	blocked := time.Unix(0, 0).UTC()
	if slot.BlockedUntil != nil {
		blocked = *slot.BlockedUntil
	}
	_, err := tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO subscription_scheduler_slots
		 (tenant_id, subscription_connection_id, state, active_run_id,
		  active_reservation_id, blocked_until, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		slot.TenantID, slot.SubscriptionConnectionID, slot.State,
		slot.ActiveRunID, slot.ActiveReservationID, blocked, slot.UpdatedAt,
	)
	return err
}

func readSchedulerCounters(
	ctx context.Context,
	tx *stateTx,
	initializeAt time.Time,
) (queueDepth uint32, activeRuns uint32, err error) {
	err = tx.sqlTx.QueryRowContext(ctx,
		`SELECT queue_depth, active_runs FROM tenant_scheduler_counters
		 WHERE tenant_id = $1`,
		tx.tenantID,
	).Scan(&queueDepth, &activeRuns)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.sqlTx.ExecContext(ctx,
			`INSERT INTO tenant_scheduler_counters
			 (tenant_id, queue_depth, active_runs, updated_at)
			 VALUES ($1, $2, $3, $4)`,
			tx.tenantID, uint32(0), uint32(0), initializeAt,
		)
		return 0, 0, err
	}
	return queueDepth, activeRuns, err
}

func appendSchedulerAudit(
	ctx context.Context,
	tx *stateTx,
	at time.Time,
	action, subjectKind, subjectID, outcome string,
	metadata map[string]any,
) error {
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(action + ":" + subjectKind + ":" + subjectID))
	auditID := "audit-" + hex.EncodeToString(sum[:12])
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO audit_events
		 (tenant_id, occurred_at, audit_event_id, actor_id, action,
		  subject_kind, subject_id, outcome, metadata, expire_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
		         CAST($9 AS JsonDocument), $10)`,
		tx.tenantID, at, auditID, "system-scheduler", action,
		subjectKind, subjectID, outcome, string(encoded),
		at.Add(tx.store.operationalRetention),
	)
	return err
}

func validateAdmissionRequest(request ports.DispatchAdmissionRequest) error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.OutboxID.Validate(); err != nil {
		return err
	}
	if err := request.RunID.Validate(); err != nil {
		return err
	}
	if err := request.AttemptID.Validate(); err != nil {
		return err
	}
	if err := request.ReservationID.Validate(); err != nil {
		return err
	}
	if request.Now.IsZero() || !request.HoldUntil.After(request.Now) {
		return domain.ValidationError{
			Field: "dispatch admission hold", Reason: "must end after a non-zero admission time",
		}
	}
	if err := request.Limits.ValidateForAdmission(); err != nil {
		return err
	}
	return request.Workload.Validate()
}

func validateExpiredReservation(
	candidate ports.ExpiredQuotaReservation,
	at time.Time,
) error {
	if err := candidate.TenantID.Validate(); err != nil {
		return err
	}
	if err := candidate.ReservationID.Validate(); err != nil {
		return err
	}
	if err := candidate.RunID.Validate(); err != nil {
		return err
	}
	if err := candidate.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	if candidate.ExpiresAt.IsZero() || at.IsZero() {
		return domain.ValidationError{
			Field: "quota expiry", Reason: "candidate and observation times must be non-zero",
		}
	}
	return nil
}

var _ ports.SchedulerStore = (*Store)(nil)
