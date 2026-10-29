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
	"gitcode.com/urandon/sessionless/internal/runexplanation"
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
		result = ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
		return store.admitDispatchTx(ctx, state.(*stateTx), request, &result)
	})
	if err != nil {
		result = ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
	}
	return result, err
}

// Compose the complete existing managed graph into one callback/clock/budget.
// This is private groundwork, not a serving port; the owned gate remains closed.
func (store *Store) admitBoundedManagedDispatch(ctx context.Context, request ports.DispatchAdmissionRequest) (result ports.DispatchAdmissionResult, err error) {
	return store.admitBoundedDispatch(ctx, request, false)
}

// Still private: neither serving admission nor either registry is enabled.
// Trusted installation/funding publication must be explicitly composed first.
func (store *Store) admitBoundedOwnedDispatch(ctx context.Context, request ports.DispatchAdmissionRequest) (ports.DispatchAdmissionResult, error) {
	if store.ownedProfilePublicationVerifier == nil || store.attemptSpendingPublicationVerifier == nil {
		return ports.DispatchAdmissionResult{}, domain.ErrMembershipDenied
	}
	return store.admitBoundedDispatch(ctx, request, true)
}

func (store *Store) admitBoundedDispatch(ctx context.Context, request ports.DispatchAdmissionRequest, owned bool) (result ports.DispatchAdmissionResult, err error) {
	if err = validateAdmissionRequest(request); err != nil {
		return result, err
	}
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
		return store.admitBoundedDispatchCallback(ctx, q, request, owned, &result)
	})
	if err != nil {
		result = ports.DispatchAdmissionResult{RunID: request.RunID, AttemptID: request.AttemptID}
	}
	return result, err
}

// The complete production callback is also directly measurable by native
// tests; there is no alternative test scheduler or extra observer authority.
func (store *Store) admitBoundedDispatchCallback(ctx context.Context, q *ownedAuthorityQuery, request ports.DispatchAdmissionRequest, owned bool, result *ports.DispatchAdmissionResult) error {
	q.metadataCache = make(map[string]ownedMetadataCacheEntry)
	q.ownedAdmission = owned
	q.explanationPending = make(map[domain.RunID]runexplanation.HeadV1)
	current := request
	current.Now = q.at
	if !current.HoldUntil.After(q.at) {
		return q.budget.deny()
	}
	if err := store.admitDispatchTx(ctx, q.canonicalState(store, request.TenantID), current, result); err != nil {
		return err
	}
	return q.flushAdmissionExplanation(ctx, request.RunID, request.TenantID)
}

func (store *Store) admitDispatchTx(ctx context.Context, tx *stateTx, request ports.DispatchAdmissionRequest, result *ports.DispatchAdmissionResult) error {
	var state ports.StateTx = tx
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
	if q, bounded := tx.sqlTx.(*ownedAuthorityQuery); bounded && (outbox.ID != request.OutboxID || outbox.TenantID != request.TenantID) {
		return q.budget.deny()
	}
	if outbox.RunID != request.RunID || outbox.AttemptID != request.AttemptID {
		return domain.ValidationError{
			Field: "dispatch candidate", Reason: "does not match the stored outbox",
		}
	}
	if err := outbox.HarnessBinding.ValidateForScope(
		outbox.TenantID, outbox.CredentialOwnerUserID, outbox.RunID, outbox.AttemptID, outbox.ExecutionPlacementV2,
	); err != nil {
		return err
	}
	q, bounded := tx.sqlTx.(*ownedAuthorityQuery)
	owned := outbox.HarnessBinding.Profile == domain.HarnessExecutionOwnedAgentV2
	// The public/legacy path cannot enable owned admission from queue bytes.
	if owned && (!bounded || !q.ownedAdmission) || bounded && q.ownedAdmission && !owned {
		return domain.ErrMembershipDenied
	}
	if _, bounded := tx.sqlTx.(*ownedAuthorityQuery); bounded && outbox.ExecutionPlacementV2.Kind != domain.ExecutionPlacementManaged {
		return domain.ErrMembershipDenied
	}
	switch outbox.ExecutionPlacementV2.Kind {
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
	attempt, found, err := state.GetAttempt(ctx, request.AttemptID)
	if err != nil {
		return err
	} else if !found {
		return fmt.Errorf("attempt %q not found", request.AttemptID)
	}
	var ownedSource domain.AttemptExecutionSourceV1
	var final *domain.AttemptExecutionAdmissionV1
	if q, bounded := tx.sqlTx.(*ownedAuthorityQuery); bounded {
		if run.ID != request.RunID || run.TenantID != request.TenantID || attempt.ID != request.AttemptID ||
			run.CancellationRequestedAt != nil || run.Status.Terminal() || run.UpdatedAt.After(q.at) || attempt.UpdatedAt.After(q.at) || outbox.UpdatedAt.After(q.at) || outbox.ValidateForAttempt(run, attempt) != nil {
			return q.budget.deny()
		}
		if owned {
			ownedSource, err = domain.DecodeAttemptExecutionSourceV1([]byte(outbox.OwnedExecutionSourceRecord))
			if err != nil || outbox.SubstrateBinding == nil || outbox.AdmissionCostCeiling == nil ||
				ownedSource.Scope != (domain.AttemptExecutionSourceScopeV1{TenantID: run.TenantID, SessionID: run.SessionID, OwnerUserID: outbox.CredentialOwnerUserID, RunID: run.ID, AttemptID: attempt.ID}) ||
				ownedSource.ValidateForExecution(outbox.ExecutionPlacementV2, outbox.HarnessBinding, *outbox.SubstrateBinding, *outbox.AdmissionCostCeiling) != nil ||
				outbox.HarnessBinding.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 ||
				currentServerlessProviderResourceTx(ctx, q, outbox.HarnessBinding, &ownedSource.Profile, q.at) != nil {
				return q.budget.deny()
			}
			if err := readOwnedAdmissionPrincipal(ctx, q, ownedSource.Scope); err != nil {
				return err
			}
			if _, err := readOwnedAdmissionSources(ctx, q, ownedSource); err != nil {
				return err
			}
			final, err = readOwnedAdmission(ctx, q, ownedSource.Scope)
			if err != nil {
				return err
			}
		} else if err := ensureSessionWritableTx(ctx, tx, run.SessionID); err != nil {
			return q.budget.deny()
		}
	}

	var entitlementValue, quotaValue string
	var entitlementErr error
	if q, bounded := tx.sqlTx.(*ownedAuthorityQuery); bounded {
		entitlementErr = q.scanMetadata(ctx,
			`SELECT SUBSTRING(CAST(entitlement_state AS String),0,33), SUBSTRING(CAST(quota_state AS String),0,33)
				 FROM subscription_connections WHERE tenant_id=$1 AND subscription_connection_id=$2`,
			[]any{request.TenantID, run.SubscriptionConnectionID}, &entitlementValue, &quotaValue)
		if entitlementErr == nil && (!domain.EntitlementState(entitlementValue).Valid() || !domain.ProviderQuotaState(quotaValue).Valid()) {
			entitlementErr = q.budget.deny()
		}
	} else {
		entitlementErr = tx.sqlTx.QueryRowContext(ctx,
			`SELECT entitlement_state, quota_state
			 FROM subscription_connections
			 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
			request.TenantID, run.SubscriptionConnectionID,
		).Scan(&entitlementValue, &quotaValue)
	}
	if entitlementErr != nil {
		return entitlementErr
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
			job, jobFound, err := readJSON[domain.WorkerJob](ctx, tx.sqlTx,
				`SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`,
				request.TenantID, request.RunID,
			)
			if err != nil {
				return err
			}
			outboxBindingDigest, outboxErr := outbox.HarnessBinding.Digest()
			jobBindingDigest, jobErr := job.HarnessBinding.Digest()
			outboxPlacementDigest, outboxPlacementErr := domain.ExecutionPlacementDigest(outbox.ExecutionPlacementV2)
			jobPlacementDigest, jobPlacementErr := domain.ExecutionPlacementDigest(job.ExecutionPlacementV2)
			authorityMatches := outbox.SubstrateBinding == nil && job.SubstrateBinding == nil && outbox.AdmissionCostCeiling == nil && job.AdmissionCostCeiling == nil
			if outbox.SubstrateBinding != nil && job.SubstrateBinding != nil && outbox.AdmissionCostCeiling != nil && job.AdmissionCostCeiling != nil {
				outboxSubstrateDigest, outboxSubstrateErr := outbox.SubstrateBinding.Digest()
				jobSubstrateDigest, jobSubstrateErr := job.SubstrateBinding.Digest()
				outboxCostDigest, outboxCostErr := outbox.AdmissionCostCeiling.Digest()
				jobCostDigest, jobCostErr := job.AdmissionCostCeiling.Digest()
				authorityMatches = outboxSubstrateErr == nil && jobSubstrateErr == nil && outboxCostErr == nil && jobCostErr == nil &&
					outboxSubstrateDigest == jobSubstrateDigest && outboxCostDigest == jobCostDigest
			}
			if !jobFound || outboxErr != nil || jobErr != nil || outboxBindingDigest != jobBindingDigest ||
				outboxPlacementErr != nil || jobPlacementErr != nil || outboxPlacementDigest != jobPlacementDigest || !authorityMatches ||
				job.AttemptID != request.AttemptID || job.ReservationID != request.ReservationID || job.OwnedExecutionSourceRecord != outbox.OwnedExecutionSourceRecord {
				return domain.ValidationError{Field: "worker_job.harness_binding", Reason: "does not match the admitted outbox authority"}
			}
			if q, bounded := tx.sqlTx.(*ownedAuthorityQuery); bounded {
				if reservation.ID != request.ReservationID || reservation.ValidateForRun(run) != nil ||
					job.ValidateForRun(run) != nil || job.InputManifestID != outbox.InputManifestID ||
					job.CreatedAt.After(q.at) || reservation.HeldAt.After(q.at) || reservation.UpdatedAt.After(q.at) ||
					!reservation.ExpiresAt.After(q.at) {
					return q.budget.deny()
				}
				if owned && !ownedAdmissionMatches(final, ownedSource, outbox.OwnedExecutionSourceRecord, reservation, job) {
					return q.budget.deny()
				}
			}
			result.Admitted = true
			result.State = slot.State
			result.Code = "already_admitted"
			// Exact reservation/job replay observes the existing canonical
			// decision, not a new admission. Preserve its original projection
			// fingerprint/time/revision; historical missing heads stay unknown.
			return nil
		}
	}
	if owned && final != nil {
		return q.budget.deny()
	} // never replace orphan/conflicting history
	if outbox.ExecutionPlacementV2.Kind == domain.ExecutionPlacementAttachedWorker {
		if err := store.validateAttachedResourceGenerationAdmissionTx(ctx, tx, outbox.CredentialOwnerUserID,
			run.SubscriptionConnectionID, outbox.ExecutionPlacementV2, outbox.HarnessBinding); err != nil {
			return err
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
			if q, bounded := tx.sqlTx.(*ownedAuthorityQuery); bounded && q.ownedAdmission && q.schedulerSlotAbsent {
				if err := writeSchedulerSlot(ctx, tx, slot); err != nil {
					return err
				}
			}
			return recordAdmissionExplanationTx(ctx, tx, run, attempt, outbox.ExecutionPlacementV2, decision.Code, request.Now)
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
		if err := recordAdmissionExplanationTx(ctx, tx, run, attempt, outbox.ExecutionPlacementV2, decision.Code, request.Now); err != nil {
			return err
		}
		return appendSchedulerAudit(
			ctx, tx, request.Now, "scheduler.admission_blocked",
			"run", string(request.RunID), decision.Code,
			map[string]any{"scheduler_state": decision.State},
		)
	}
	if q, bounded := tx.sqlTx.(*ownedAuthorityQuery); bounded {
		// Only the exact slot/reservation/job branch above can recover a hold.
		// A colliding historical ID cannot be overwritten to renew capacity.
		_, found, err := readJSON[domain.QuotaReservation](ctx, q,
			`SELECT payload FROM quota_reservations
			 WHERE tenant_id = $1 AND quota_reservation_id = $2`, request.TenantID, request.ReservationID)
		if err != nil || found {
			return q.budget.deny()
		}
		if owned {
			admission, err := domain.BuildAttemptExecutionAdmissionV1(ownedSource, request.ReservationID, q.at)
			if err != nil {
				return q.budget.deny()
			}
			if err := writeOwnedAdmission(ctx, q, admission); err != nil {
				return err
			}
		}
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
	if err := recordAdmissionExplanationTx(ctx, tx, run, attempt, outbox.ExecutionPlacementV2, decision.Code, request.Now); err != nil {
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
		InputManifestID:            outbox.InputManifestID,
		ContextSnapshot:            outbox.ContextSnapshot,
		ContextWindow:              contextWindow,
		WorkspaceSnapshot:          outbox.WorkspaceSnapshot,
		SkillBundle:                outbox.SkillBundle,
		AllowedMCPServers:          append([]string(nil), outbox.AllowedMCPServers...),
		CredentialOwnerUserID:      outbox.CredentialOwnerUserID,
		ExecutionPlacementV2:       outbox.ExecutionPlacementV2,
		HarnessBinding:             outbox.HarnessBinding.Clone(),
		Limits:                     request.Limits,
		Origin:                     outbox.Origin,
		DeliveryChat:               outbox.DeliveryChat,
		ReplyToMessageID:           outbox.ReplyToMessageID,
		CreatedAt:                  request.Now,
		OwnedExecutionSourceRecord: outbox.OwnedExecutionSourceRecord,
	}
	if outbox.SubstrateBinding != nil {
		substrate := *outbox.SubstrateBinding
		job.SubstrateBinding = &substrate
	}
	if outbox.AdmissionCostCeiling != nil {
		cost := outbox.AdmissionCostCeiling.Clone()
		job.AdmissionCostCeiling = &cost
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
			TenantID: request.TenantID, OwnerUserID: job.ExecutionPlacementV2.OwnerUserID,
			WorkerID: job.ExecutionPlacementV2.WorkerID, RunID: request.RunID,
			AttemptID: request.AttemptID, ReservationID: request.ReservationID,
			LeaseID: leaseID, LeaseTTL: leaseTTL,
		}
		if err := validateAttachedWorkerAttemptOffer(offerRequest); err != nil {
			return err
		}
		var offer ports.AttachedWorkerAttemptResult
		var diagnostic attachedWorkerOfferConflictStage
		if err := store.offerAttachedWorkerAttemptTx(ctx, tx, offerRequest, &offer, &diagnostic); err != nil {
			return err
		}
		if offer.Status != ports.AttachedWorkerExecutionApplied && offer.Status != ports.AttachedWorkerExecutionReplayed {
			if offer.Status == ports.AttachedWorkerExecutionDenied && diagnostic != "" {
				return attachedWorkerOfferConflictAt(diagnostic)
			}
			return attachedWorkerOfferConflictAt(attachedWorkerOfferStatusStage(offer.Status))
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
}

const admittedSnapshotMetadataBytes = 32 << 10

const admittedSnapshotMetadataSQL = `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_snapshots
		 WHERE tenant_id = $1 AND session_id = $2 AND through_sequence <= $3
		 ORDER BY version DESC LIMIT $4`

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
		admittedSnapshotMetadataSQL,
		tx.tenantID, sessionID, window.ThroughSequence, uint64(16),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	bounded, owned := tx.sqlTx.(*ownedAuthorityQuery)
	var selected *domain.SessionContextWindow
	count := 0
	for rows.Next() {
		count++
		var payload string
		if owned {
			returned, scanErr := scanOwnedCurrentRow(rows, &payload)
			if bounded.budget.materialized(bounded.callback, returned) != nil {
				return nil, domain.ErrMembershipDenied
			}
			if scanErr != nil || len(payload) > admittedSnapshotMetadataBytes || count > 16 {
				return nil, bounded.budget.deny()
			}
		} else {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				return nil, err
			}
			payload = string(raw)
		}
		if len(payload) > admittedSnapshotMetadataBytes {
			// Legacy selection already ignores malformed snapshot metadata. Do
			// not transfer an oversized cell merely to discover it cannot decode.
			continue
		}
		var snapshot domain.SessionSnapshot
		if err := json.Unmarshal([]byte(payload), &snapshot); err != nil || snapshot.Validate() != nil {
			continue
		}
		if snapshot.TenantID != tx.tenantID || snapshot.SessionID != sessionID || snapshot.ThroughSequence > window.ThroughSequence {
			continue
		}
		version := snapshot.Version
		window.SnapshotVersion = &version
		window.AfterSequence = snapshot.ThroughSequence
		if !owned {
			return &window, nil
		}
		if selected == nil {
			copy := window
			selected = &copy
		}
		// database/sql may have materialized the whole bounded result set.
		// Drain and charge it even after selecting the first valid snapshot.
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if selected != nil {
		return selected, nil
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
			if loaded.Job.ExecutionPlacementV2.Kind == domain.ExecutionPlacementManaged && queueDepth > 0 {
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
	var err error
	if q, bounded := tx.sqlTx.(*ownedAuthorityQuery); bounded {
		err = q.scanMetadata(ctx,
			`SELECT SUBSTRING(CAST(state AS String),0,33), SUBSTRING(CAST(active_run_id AS String),0,161), SUBSTRING(CAST(active_reservation_id AS String),0,161), blocked_until, updated_at
			 FROM subscription_scheduler_slots WHERE tenant_id=$1 AND subscription_connection_id=$2`,
			[]any{tx.tenantID, connectionID}, &state, &activeRun, &activeReservation, &blockedUntil, &updatedAt)
		if err == nil && (len(state) > 32 || len(activeRun) > 160 || len(activeReservation) > 160 || updatedAt.After(q.at)) {
			err = q.budget.deny()
		}
	} else {
		err = tx.sqlTx.QueryRowContext(ctx,
			`SELECT state, active_run_id, active_reservation_id, blocked_until, updated_at
		 FROM subscription_scheduler_slots
		 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
			tx.tenantID, connectionID,
		).Scan(&state, &activeRun, &activeReservation, &blockedUntil, &updatedAt)
	}
	if errors.Is(err, sql.ErrNoRows) {
		slot.State = domain.SchedulerReauthRequired
		if entitlement == domain.EntitlementActive {
			slot.State = domain.SchedulerReady
		}
		slot.UpdatedAt = at
		if q, bounded := tx.sqlTx.(*ownedAuthorityQuery); bounded && q.ownedAdmission {
			// The absent key is already part of this serializable read set.
			// Do not publish a redundant provisional Ready slot before the
			// actual admission/denial decision. Every successful cold branch
			// still writes that final canonical slot, including an unchanged
			// quota-blocked denial; any failure rolls the whole callback back.
			q.schedulerSlotAbsent = true
			return slot, slot.Validate()
		}
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
	if q, bounded := tx.sqlTx.(*ownedAuthorityQuery); bounded {
		err = q.scanMetadata(ctx,
			`SELECT queue_depth, active_runs FROM tenant_scheduler_counters
			 WHERE tenant_id = $1`, []any{tx.tenantID}, &queueDepth, &activeRuns)
	} else {
		err = tx.sqlTx.QueryRowContext(ctx,
			`SELECT queue_depth, active_runs FROM tenant_scheduler_counters
		 WHERE tenant_id = $1`,
			tx.tenantID,
		).Scan(&queueDepth, &activeRuns)
	}
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
