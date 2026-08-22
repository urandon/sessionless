package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

const maxSessionDeletionRows = uint64(10_000)

const sessionLifecycleBackfillID = "session-lifecycle-indexes-v1"

var ErrSessionLifecycleConflict = errors.New("session lifecycle request conflicts with durable state")

func sessionLifecycleIndexesReady(ctx context.Context, query rowQuery) (bool, error) {
	var completedAt time.Time
	err := query.QueryRowContext(ctx,
		`SELECT completed_at FROM session_lifecycle_backfill_state WHERE backfill_id = $1`,
		sessionLifecycleBackfillID,
	).Scan(&completedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return !completedAt.IsZero(), nil
}

func readLifecycleDelivery(
	ctx context.Context,
	query rowQuery,
	tenantID domain.TenantID,
	runID domain.RunID,
	deliveryID domain.TelegramDeliveryID,
) (domain.TelegramDeliveryOutbox, bool, error) {
	delivery, found, err := readJSON[domain.TelegramDeliveryOutbox](ctx, query,
		`SELECT record FROM telegram_deliveries_by_run
		 WHERE tenant_id = $1 AND run_id = $2 AND telegram_delivery_id = $3`,
		tenantID, runID, deliveryID,
	)
	if err != nil || found {
		return delivery, found, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, query)
	if err != nil || ready {
		return delivery, false, err
	}
	return readJSON[domain.TelegramDeliveryOutbox](ctx, query,
		`SELECT payload FROM telegram_delivery_outbox
		 WHERE tenant_id = $1 AND telegram_delivery_id = $2`, tenantID, deliveryID)
}

func readLifecycleCheckpoint(
	ctx context.Context,
	query rowQuery,
	tenantID domain.TenantID,
	runID domain.RunID,
	checkpointID domain.CheckpointID,
) (domain.Checkpoint, bool, error) {
	checkpoint, found, err := readJSON[domain.Checkpoint](ctx, query,
		`SELECT record FROM checkpoint_objects_by_run
		 WHERE tenant_id = $1 AND run_id = $2 AND checkpoint_id = $3`,
		tenantID, runID, checkpointID,
	)
	if err != nil || found {
		return checkpoint, found, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, query)
	if err != nil || ready {
		return checkpoint, false, err
	}
	return readJSON[domain.Checkpoint](ctx, query,
		`SELECT payload FROM checkpoints
		 WHERE tenant_id = $1 AND run_id = $2 AND checkpoint_id = $3`,
		tenantID, runID, checkpointID)
}

func (store *Store) PutSessionLegalHold(
	ctx context.Context,
	hold domain.SessionLegalHold,
) (result domain.SessionLegalHold, err error) {
	if err := hold.Validate(); err != nil {
		return result, err
	}
	if hold.State != domain.SessionLegalHoldActive {
		return result, domain.ValidationError{Field: "session_legal_hold.state", Reason: "new holds must be active"}
	}
	err = store.Transact(ctx, hold.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if _, found, err := readSessionTx(ctx, tx, hold.SessionID); err != nil {
			return err
		} else if !found {
			return fmt.Errorf("session %q not found", hold.SessionID)
		}
		deletion, deleting, err := readSessionDeletionTx(ctx, tx, hold.SessionID)
		if err != nil {
			return err
		}
		if deleting && (deletion.State == domain.SessionDeletionDeleting || deletion.State == domain.SessionDeletionCompleted) {
			return domain.ValidationError{Field: "session_legal_hold", Reason: "cannot establish a hold after destructive cleanup starts"}
		}
		existing, found, err := readSessionLegalHoldTx(ctx, tx, hold.SessionID)
		if err != nil {
			return err
		}
		if found && existing.State == domain.SessionLegalHoldActive {
			if existing.SetBy == hold.SetBy && existing.Reason == hold.Reason {
				result = existing
				return nil
			}
			return ErrSessionLifecycleConflict
		}
		if found && existing.ReleasedAt != nil && hold.SetAt.Before(*existing.ReleasedAt) {
			return domain.ValidationError{Field: "session_legal_hold.set_at", Reason: "must follow the prior release"}
		}
		if err := writeSessionLegalHoldTx(ctx, tx, hold); err != nil {
			return err
		}
		if err := appendSessionLifecycleAuditTx(ctx, tx, hold.SetAt, hold.SetBy,
			"session.legal_hold.set", hold.SessionID, map[string]any{"reason": hold.Reason}); err != nil {
			return err
		}
		result = hold
		return nil
	})
	return result, err
}

func (store *Store) ReleaseSessionLegalHold(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	releasedBy domain.UserID,
	at time.Time,
) (result domain.SessionLegalHold, err error) {
	if err := validateLifecycleIdentity(tenantID, sessionID, releasedBy, at); err != nil {
		return result, err
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		hold, found, err := readSessionLegalHoldTx(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("legal hold for session %q not found", sessionID)
		}
		if hold.State == domain.SessionLegalHoldReleased {
			result = hold
			return nil
		}
		if err := hold.Release(releasedBy, at); err != nil {
			return err
		}
		if err := writeSessionLegalHoldTx(ctx, tx, hold); err != nil {
			return err
		}
		if err := appendSessionLifecycleAuditTx(ctx, tx, at, releasedBy,
			"session.legal_hold.release", sessionID, nil); err != nil {
			return err
		}
		result = hold
		return nil
	})
	return result, err
}

func (store *Store) GetSessionLegalHold(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
) (domain.SessionLegalHold, bool, error) {
	if err := tenantID.Validate(); err != nil {
		return domain.SessionLegalHold{}, false, err
	}
	if err := sessionID.Validate(); err != nil {
		return domain.SessionLegalHold{}, false, err
	}
	return readJSON[domain.SessionLegalHold](ctx, store.db,
		`SELECT record FROM session_legal_holds WHERE tenant_id = $1 AND session_id = $2`,
		tenantID, sessionID,
	)
}

func (store *Store) RequestSessionDeletion(
	ctx context.Context,
	deletion domain.SessionDeletion,
) (result domain.SessionDeletion, err error) {
	if err := deletion.Validate(); err != nil {
		return result, err
	}
	if deletion.State != domain.SessionDeletionRequested {
		return result, domain.ValidationError{Field: "session_deletion.state", Reason: "new deletions must be requested"}
	}
	err = store.Transact(ctx, deletion.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		existing, found, err := readSessionDeletionTx(ctx, tx, deletion.SessionID)
		if err != nil {
			return err
		}
		if found {
			if existing.RequestedBy == deletion.RequestedBy && existing.Reason == deletion.Reason {
				result = existing
				return nil
			}
			return ErrSessionLifecycleConflict
		}
		if _, found, err := readSessionTx(ctx, tx, deletion.SessionID); err != nil {
			return err
		} else if !found {
			return fmt.Errorf("session %q not found", deletion.SessionID)
		}
		if err := authorizeTenantWriteTx(ctx, tx, deletion.RequestedBy); err != nil {
			return err
		}
		if err := authorizeSessionOwnerTx(ctx, tx, deletion.SessionID, deletion.RequestedBy); err != nil {
			return err
		}
		if err := ensureNoActiveLegalHoldTx(ctx, tx, deletion.SessionID); err != nil {
			return err
		}
		hold, heldBefore, err := readSessionLegalHoldTx(ctx, tx, deletion.SessionID)
		if err != nil {
			return err
		}
		if heldBefore && hold.ReleasedAt != nil && deletion.RequestedAt.Before(*hold.ReleasedAt) {
			return domain.ValidationError{Field: "session_deletion.requested_at", Reason: "must not precede the legal-hold release"}
		}
		if err := ensureSessionRunsTerminalTx(ctx, tx, deletion.SessionID); err != nil {
			return err
		}
		if err := writeSessionDeletionTx(ctx, tx, deletion); err != nil {
			return err
		}
		if err := appendSessionLifecycleAuditTx(ctx, tx, deletion.RequestedAt, deletion.RequestedBy,
			"session.deletion.request", deletion.SessionID, map[string]any{"reason": deletion.Reason}); err != nil {
			return err
		}
		result = deletion
		return nil
	})
	return result, err
}

func (store *Store) GetSessionDeletion(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
) (domain.SessionDeletion, bool, error) {
	if err := tenantID.Validate(); err != nil {
		return domain.SessionDeletion{}, false, err
	}
	if err := sessionID.Validate(); err != nil {
		return domain.SessionDeletion{}, false, err
	}
	return readJSON[domain.SessionDeletion](ctx, store.db,
		`SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`,
		tenantID, sessionID,
	)
}

func (store *Store) StartSessionDeletion(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	at time.Time,
) (result domain.SessionDeletion, err error) {
	if err := tenantID.Validate(); err != nil {
		return result, err
	}
	if err := sessionID.Validate(); err != nil {
		return result, err
	}
	if at.IsZero() {
		return result, domain.ValidationError{Field: "session_deletion.started_at", Reason: "must not be zero"}
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		deletion, found, err := readSessionDeletionTx(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("deletion for session %q not found", sessionID)
		}
		if deletion.State == domain.SessionDeletionCompleted {
			result = deletion
			return nil
		}
		if err := ensureNoActiveLegalHoldTx(ctx, tx, sessionID); err != nil {
			return err
		}
		if deletion.State == domain.SessionDeletionDeleting {
			result = deletion
			return nil
		}
		if err := ensureSessionRunsTerminalTx(ctx, tx, sessionID); err != nil {
			return err
		}
		if err := deletion.Start(at); err != nil {
			return err
		}
		if err := writeSessionDeletionTx(ctx, tx, deletion); err != nil {
			return err
		}
		if err := appendSessionLifecycleAuditTx(ctx, tx, at, deletion.RequestedBy,
			"session.deletion.start", sessionID, nil); err != nil {
			return err
		}
		result = deletion
		return nil
	})
	return result, err
}

func (store *Store) BuildSessionDeletionInventory(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	maxRows uint64,
	maxObjects uint64,
) (domain.SessionDeletionInventory, error) {
	inventory := domain.SessionDeletionInventory{TenantID: tenantID, SessionID: sessionID}
	if err := tenantID.Validate(); err != nil {
		return inventory, err
	}
	if err := sessionID.Validate(); err != nil {
		return inventory, err
	}
	if maxRows == 0 || maxRows > maxSessionDeletionRows || maxObjects == 0 || maxObjects > maxSessionDeletionRows {
		return inventory, domain.ValidationError{Field: "session_deletion.inventory_limit", Reason: "must be between 1 and 10000"}
	}
	deletion, found, err := store.GetSessionDeletion(ctx, tenantID, sessionID)
	if err != nil {
		return inventory, err
	}
	if !found || deletion.State == domain.SessionDeletionCompleted {
		return inventory, domain.ValidationError{Field: "session_deletion", Reason: "must be requested and incomplete before inventory"}
	}

	objects := make(map[string]domain.BlobRef)
	var rowsUsed uint64
	addRow := func() error {
		rowsUsed++
		if rowsUsed > maxRows {
			return domain.ValidationError{Field: "session_deletion.rows", Reason: "exceeds the configured inventory bound"}
		}
		return nil
	}
	addObject := func(ref domain.BlobRef) error {
		if err := ref.Validate(); err != nil {
			return err
		}
		if err := domain.EnsureSameTenant(tenantID, ref.TenantID); err != nil {
			return err
		}
		if previous, exists := objects[ref.Key]; exists {
			if previous != ref {
				return fmt.Errorf("object %q has conflicting immutable metadata", ref.Key)
			}
			return nil
		}
		if uint64(len(objects)) >= maxObjects {
			return domain.ValidationError{Field: "session_deletion.objects", Reason: "exceeds the configured exact-object bound"}
		}
		if uint64(ref.Size) > math.MaxUint64-inventory.TotalBytes {
			return domain.ValidationError{Field: "session_deletion.total_bytes", Reason: "overflowed"}
		}
		objects[ref.Key] = ref
		inventory.TotalBytes += uint64(ref.Size)
		return nil
	}

	var afterSequence uint64
	for {
		pageLimit := lifecyclePageLimit(maxRows - rowsUsed)
		events, err := store.ListSessionHistory(ctx, tenantID, sessionID, afterSequence, pageLimit)
		if err != nil {
			return inventory, err
		}
		for _, event := range events {
			if err := addRow(); err != nil {
				return inventory, err
			}
			inventory.EventRows++
			if err := addObject(event.Payload); err != nil {
				return inventory, err
			}
			afterSequence = event.Sequence
		}
		if len(events) == 0 || uint64(len(events)) < pageLimit {
			break
		}
	}

	var afterVersion uint64
	for {
		pageLimit := lifecyclePageLimit(maxRows - rowsUsed)
		snapshots, err := store.ListSessionSnapshots(ctx, tenantID, sessionID, afterVersion, pageLimit)
		if err != nil {
			return inventory, err
		}
		for _, snapshot := range snapshots {
			if err := addRow(); err != nil {
				return inventory, err
			}
			inventory.SnapshotRows++
			if err := addObject(snapshot.Payload); err != nil {
				return inventory, err
			}
			afterVersion = snapshot.Version
		}
		if len(snapshots) == 0 || uint64(len(snapshots)) < pageLimit {
			break
		}
	}

	participantCount, err := store.countSessionParticipantRows(ctx, tenantID, sessionID, maxRows-rowsUsed)
	if err != nil {
		return inventory, err
	}
	for range participantCount {
		if err := addRow(); err != nil {
			return inventory, err
		}
		inventory.ParticipantRows++
	}
	bindingIDs, err := store.listSessionBindingIDs(ctx, tenantID, sessionID, maxRows-rowsUsed+1)
	if err != nil {
		return inventory, err
	}
	for range bindingIDs {
		if err := addRow(); err != nil {
			return inventory, err
		}
		inventory.BindingRows++
	}
	projectionIDs, err := store.listSessionProjectionIDs(ctx, tenantID, sessionID, maxRows-rowsUsed+1)
	if err != nil {
		return inventory, err
	}
	for range projectionIDs {
		if err := addRow(); err != nil {
			return inventory, err
		}
		inventory.ProjectionRows++
	}

	runIDs, err := store.listSessionRunIDs(ctx, tenantID, sessionID, maxRows-rowsUsed+1)
	if err != nil {
		return inventory, err
	}
	for _, runID := range runIDs {
		if err := addRow(); err != nil {
			return inventory, err
		}
		inventory.RunRows++
		inventory.RunIDs = append(inventory.RunIDs, runID)
		manifestIDs, err := store.listRunManifestIDs(ctx, tenantID, runID, maxRows-rowsUsed+1)
		if err != nil {
			return inventory, err
		}
		for _, manifestID := range manifestIDs {
			if err := addRow(); err != nil {
				return inventory, err
			}
			manifest, found, err := store.GetArtifactManifest(ctx, tenantID, manifestID)
			if err != nil {
				return inventory, err
			}
			if !found || manifest.RunID != runID {
				return inventory, fmt.Errorf("manifest index references inconsistent manifest %q", manifestID)
			}
			inventory.ManifestRows++
			for _, artifact := range manifest.Artifacts {
				if err := addObject(artifact.Blob); err != nil {
					return inventory, err
				}
			}
		}
		deliveryIDs, err := store.listRunDeliveryIDs(ctx, tenantID, runID, maxRows-rowsUsed+1)
		if err != nil {
			return inventory, err
		}
		for _, deliveryID := range deliveryIDs {
			if err := addRow(); err != nil {
				return inventory, err
			}
			delivery, found, err := readLifecycleDelivery(ctx, store.db, tenantID, runID, deliveryID)
			if err != nil {
				return inventory, err
			}
			if !found || delivery.RunID != runID {
				return inventory, fmt.Errorf("delivery index references inconsistent delivery %q", deliveryID)
			}
			inventory.DeliveryRows++
			if delivery.Payload.Key != "" {
				if err := addObject(delivery.Payload); err != nil {
					return inventory, err
				}
			}
		}
		checkpointIDs, err := store.listRunCheckpointIDs(ctx, tenantID, runID, maxRows-rowsUsed+1)
		if err != nil {
			return inventory, err
		}
		for _, checkpointID := range checkpointIDs {
			if err := addRow(); err != nil {
				return inventory, err
			}
			checkpoint, found, err := readLifecycleCheckpoint(ctx, store.db, tenantID, runID, checkpointID)
			if err != nil {
				return inventory, err
			}
			if !found || checkpoint.RunID != runID {
				return inventory, fmt.Errorf("checkpoint index references inconsistent checkpoint %q", checkpointID)
			}
			inventory.CheckpointRows++
			if err := addObject(checkpoint.State); err != nil {
				return inventory, err
			}
		}
	}
	objectKeys := make([]string, 0, len(objects))
	for key := range objects {
		objectKeys = append(objectKeys, key)
	}
	sort.Strings(objectKeys)
	for _, key := range objectKeys {
		ref := objects[key]
		inventory.Objects = append(inventory.Objects, ref)
	}
	if err := inventory.Validate(maxObjects); err != nil {
		return inventory, err
	}
	return inventory, nil
}

func (store *Store) CompleteSessionDeletion(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	at time.Time,
	deletedObjects uint64,
	deletedBytes uint64,
) (result domain.SessionDeletion, err error) {
	if err := tenantID.Validate(); err != nil {
		return result, err
	}
	if err := sessionID.Validate(); err != nil {
		return result, err
	}
	if at.IsZero() {
		return result, domain.ValidationError{Field: "session_deletion.completed_at", Reason: "must not be zero"}
	}
	stored, found, err := store.GetSessionDeletion(ctx, tenantID, sessionID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, fmt.Errorf("deletion for session %q not found", sessionID)
	}
	if stored.State != domain.SessionDeletionCompleted {
		inventory, err := store.BuildSessionDeletionInventory(
			ctx, tenantID, sessionID, maxSessionDeletionRows, maxSessionDeletionRows,
		)
		if err != nil {
			return result, err
		}
		if deletedObjects != uint64(len(inventory.Objects)) || deletedBytes != inventory.TotalBytes {
			return result, domain.ValidationError{Field: "session_deletion.completion", Reason: "does not match the durable exact-object inventory"}
		}
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		deletion, found, err := readSessionDeletionTx(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("deletion for session %q not found", sessionID)
		}
		if deletion.State == domain.SessionDeletionCompleted {
			if err := deletion.Complete(at, deletedObjects, deletedBytes); err != nil {
				return err
			}
			result = deletion
			return nil
		}
		if err := ensureNoActiveLegalHoldTx(ctx, tx, sessionID); err != nil {
			return err
		}
		if err := ensureSessionRunsTerminalTx(ctx, tx, sessionID); err != nil {
			return err
		}
		if err := deleteSessionRowsTx(ctx, tx, sessionID); err != nil {
			return err
		}
		if err := deletion.Complete(at, deletedObjects, deletedBytes); err != nil {
			return err
		}
		if err := writeSessionDeletionTx(ctx, tx, deletion); err != nil {
			return err
		}
		if err := appendSessionLifecycleAuditTx(ctx, tx, at, deletion.RequestedBy,
			"session.deletion.complete", sessionID, map[string]any{
				"deleted_objects": deletedObjects, "deleted_bytes": deletedBytes,
			}); err != nil {
			return err
		}
		result = deletion
		return nil
	})
	return result, err
}

func ensureSessionWritableTx(ctx context.Context, tx *stateTx, sessionID domain.SessionID) error {
	deletion, found, err := readSessionDeletionTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	if found {
		return domain.ValidationError{
			Field: "session.lifecycle", Reason: fmt.Sprintf("writes are blocked by deletion state %q", deletion.State),
		}
	}
	return nil
}

func authorizeSessionOwnerTx(
	ctx context.Context,
	tx *stateTx,
	sessionID domain.SessionID,
	userID domain.UserID,
) error {
	participant, found, err := readJSON[domain.SessionParticipant](ctx, tx.sqlTx,
		`SELECT record FROM session_participants
		 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3`,
		tx.tenantID, sessionID, userID,
	)
	if err != nil {
		return err
	}
	if !found || participant.Status != domain.SessionParticipantActive ||
		participant.Role != domain.SessionParticipantOwner {
		return domain.ErrMembershipDenied
	}
	return participant.Authorize(tx.tenantID, sessionID, userID, true)
}

func readSessionDeletionTx(ctx context.Context, tx *stateTx, sessionID domain.SessionID) (domain.SessionDeletion, bool, error) {
	return readJSON[domain.SessionDeletion](ctx, tx.sqlTx,
		`SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`,
		tx.tenantID, sessionID,
	)
}

func readSessionLegalHoldTx(ctx context.Context, tx *stateTx, sessionID domain.SessionID) (domain.SessionLegalHold, bool, error) {
	return readJSON[domain.SessionLegalHold](ctx, tx.sqlTx,
		`SELECT record FROM session_legal_holds WHERE tenant_id = $1 AND session_id = $2`,
		tx.tenantID, sessionID,
	)
}

func writeSessionDeletionTx(ctx context.Context, tx *stateTx, deletion domain.SessionDeletion) error {
	if err := deletion.Validate(); err != nil {
		return err
	}
	record, err := marshal(deletion)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO session_deletions
		 (tenant_id, session_id, state, requested_at, started_at, completed_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, CAST($7 AS JsonDocument))`,
		deletion.TenantID, deletion.SessionID, deletion.State, deletion.RequestedAt,
		nullableTime(deletion.StartedAt), nullableTime(deletion.CompletedAt), record,
	)
	return err
}

func writeSessionLegalHoldTx(ctx context.Context, tx *stateTx, hold domain.SessionLegalHold) error {
	if err := hold.Validate(); err != nil {
		return err
	}
	record, err := marshal(hold)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO session_legal_holds
		 (tenant_id, session_id, state, set_at, released_at, record)
		 VALUES ($1, $2, $3, $4, $5, CAST($6 AS JsonDocument))`,
		hold.TenantID, hold.SessionID, hold.State, hold.SetAt, nullableTime(hold.ReleasedAt), record,
	)
	return err
}

func ensureNoActiveLegalHoldTx(ctx context.Context, tx *stateTx, sessionID domain.SessionID) error {
	hold, found, err := readSessionLegalHoldTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	if found && hold.State == domain.SessionLegalHoldActive {
		return domain.ValidationError{Field: "session_deletion.legal_hold", Reason: "active legal hold blocks destructive cleanup"}
	}
	return nil
}

func ensureSessionRunsTerminalTx(ctx context.Context, tx *stateTx, sessionID domain.SessionID) error {
	rows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT run_id, status FROM runs_by_session
		 WHERE tenant_id = $1 AND session_id = $2 LIMIT $3`,
		tx.tenantID, sessionID, maxSessionDeletionRows+1,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	var count uint64
	for rows.Next() {
		count++
		if count > maxSessionDeletionRows {
			return domain.ValidationError{Field: "session_deletion.runs", Reason: "exceeds the hard deletion bound"}
		}
		var runID domain.RunID
		var status domain.RunStatus
		if err := rows.Scan(&runID, &status); err != nil {
			return err
		}
		if !status.Terminal() {
			return domain.ValidationError{Field: "session_deletion.runs", Reason: fmt.Sprintf("run %q is not terminal", runID)}
		}
	}
	return rows.Err()
}

func deleteSessionRowsTx(ctx context.Context, tx *stateTx, sessionID domain.SessionID) error {
	session, found, err := readSessionTx(ctx, tx, sessionID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("session %q not found before deletion completion", sessionID)
	}
	participants, err := activeSessionParticipants(ctx, tx, sessionID, maxSessionDeletionRows)
	if err != nil {
		return err
	}
	for _, participant := range participants {
		if err := deleteActivityTx(ctx, tx, session, participant.UserID); err != nil {
			return err
		}
	}

	bindings, err := currentSessionBindingsTx(ctx, tx, sessionID, maxSessionDeletionRows)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		statements := []struct {
			query string
			args  []any
		}{
			{`DELETE FROM frontend_binding_keys WHERE tenant_id = $1 AND frontend = $2 AND external_conversation_id = $3`, []any{tx.tenantID, binding.Frontend, binding.ExternalConversationID}},
			{`DELETE FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2`, []any{tx.tenantID, binding.ID}},
			{`DELETE FROM frontend_bindings_by_session WHERE tenant_id = $1 AND session_id = $2 AND binding_id = $3`, []any{tx.tenantID, sessionID, binding.ID}},
		}
		for _, statement := range statements {
			if _, err := tx.sqlTx.ExecContext(ctx, statement.query, statement.args...); err != nil {
				return err
			}
		}
	}

	projectionIDs, err := listSessionProjectionIDsTx(ctx, tx, sessionID, maxSessionDeletionRows+1)
	if err != nil {
		return err
	}
	if uint64(len(projectionIDs)) > maxSessionDeletionRows {
		return domain.ValidationError{Field: "session_deletion.projections", Reason: "exceeds the hard deletion bound"}
	}
	for _, id := range projectionIDs {
		projection, found, err := readJSON[domain.FrontendProjection](ctx, tx.sqlTx,
			`SELECT record FROM frontend_projection_outbox
			 WHERE tenant_id = $1 AND frontend_projection_id = $2`, tx.tenantID, id)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		event, found, err := readJSON[domain.SessionEvent](ctx, tx.sqlTx,
			`SELECT record FROM session_events
			 WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3`,
			tx.tenantID, projection.SessionID, projection.EventSequence)
		if err != nil {
			return err
		}
		if !found || event.RunID == nil {
			return fmt.Errorf("projection %q has no canonical run during deletion", id)
		}
		if err := deleteFrontendProjectionTx(ctx, tx, telegramProjectionContext{
			projection: projection,
			run:        domain.Run{ID: *event.RunID},
		}); err != nil {
			return err
		}
	}

	runIDs, err := listSessionRunIDsTx(ctx, tx, sessionID, maxSessionDeletionRows+1)
	if err != nil {
		return err
	}
	for _, runID := range runIDs {
		manifestIDs, err := listRunManifestIDsTx(ctx, tx, runID, maxSessionDeletionRows+1)
		if err != nil {
			return err
		}
		for _, manifestID := range manifestIDs {
			if _, err := tx.sqlTx.ExecContext(ctx,
				`DELETE FROM artifact_manifests WHERE tenant_id = $1 AND artifact_manifest_id = $2`,
				tx.tenantID, manifestID,
			); err != nil {
				return err
			}
		}
		deliveryIDs, err := listRunDeliveryIDsTx(ctx, tx, runID, maxSessionDeletionRows+1)
		if err != nil {
			return err
		}
		for _, deliveryID := range deliveryIDs {
			delivery, found, err := readLifecycleDelivery(ctx, tx.sqlTx, tx.tenantID, runID, deliveryID)
			if err != nil {
				return err
			}
			if !found || delivery.RunID != runID {
				return fmt.Errorf("delivery index references inconsistent delivery %q", deliveryID)
			}
			availableAt := telegramDeliveryAvailableAt(delivery)
			bucket, err := ydbpartition.BucketV1(string(delivery.ID))
			if err != nil {
				return err
			}
			for _, statement := range []struct {
				query string
				args  []any
			}{
				{`DELETE FROM telegram_delivery_ready WHERE tenant_id = $1 AND available_at = $2 AND telegram_delivery_id = $3`, []any{tx.tenantID, availableAt, delivery.ID}},
				{`DELETE FROM telegram_delivery_ready_v2 WHERE shard_bucket = $1 AND available_at = $2 AND tenant_id = $3 AND telegram_delivery_id = $4`, []any{bucket, availableAt, tx.tenantID, delivery.ID}},
				{`DELETE FROM telegram_delivery_outbox WHERE tenant_id = $1 AND telegram_delivery_id = $2`, []any{tx.tenantID, delivery.ID}},
			} {
				if _, err := tx.sqlTx.ExecContext(ctx, statement.query, statement.args...); err != nil {
					return err
				}
			}
		}
		checkpointIDs, err := listRunCheckpointIDsTx(ctx, tx, runID, maxSessionDeletionRows+1)
		if err != nil {
			return err
		}
		for _, checkpointID := range checkpointIDs {
			checkpoint, found, err := readLifecycleCheckpoint(ctx, tx.sqlTx, tx.tenantID, runID, checkpointID)
			if err != nil {
				return err
			}
			if !found || checkpoint.RunID != runID {
				return fmt.Errorf("checkpoint index references inconsistent checkpoint %q", checkpointID)
			}
			if _, err := tx.sqlTx.ExecContext(ctx,
				`DELETE FROM checkpoints WHERE tenant_id = $1 AND attempt_id = $2 AND sequence = $3`,
				tx.tenantID, checkpoint.AttemptID, checkpoint.Sequence,
			); err != nil {
				return err
			}
		}
		run, found, err := readJSON[domain.Run](ctx, tx.sqlTx,
			`SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`, tx.tenantID, runID)
		if err != nil {
			return err
		}
		if found {
			if _, err := tx.sqlTx.ExecContext(ctx,
				`DELETE FROM run_idempotency WHERE tenant_id = $1 AND idempotency_key = $2`,
				tx.tenantID, run.IdempotencyKey,
			); err != nil {
				return err
			}
		}
		for _, query := range []string{
			`DELETE FROM artifact_manifests_by_run WHERE tenant_id = $1 AND run_id = $2`,
			`DELETE FROM frontend_projections_by_run WHERE tenant_id = $1 AND run_id = $2`,
			`DELETE FROM telegram_deliveries_by_run WHERE tenant_id = $1 AND run_id = $2`,
			`DELETE FROM checkpoint_objects_by_run WHERE tenant_id = $1 AND run_id = $2`,
			`DELETE FROM run_finalizations WHERE tenant_id = $1 AND run_id = $2`,
			`DELETE FROM worker_jobs WHERE tenant_id = $1 AND run_id = $2`,
			`DELETE FROM runs WHERE tenant_id = $1 AND run_id = $2`,
		} {
			if _, err := tx.sqlTx.ExecContext(ctx, query, tx.tenantID, runID); err != nil {
				return err
			}
		}
	}

	for _, query := range []string{
		`DELETE FROM frontend_projections_by_session WHERE tenant_id = $1 AND session_id = $2`,
		`DELETE FROM runs_by_session WHERE tenant_id = $1 AND session_id = $2`,
		`DELETE FROM session_snapshots WHERE tenant_id = $1 AND session_id = $2`,
		`DELETE FROM session_event_idempotency WHERE tenant_id = $1 AND session_id = $2`,
		`DELETE FROM session_events WHERE tenant_id = $1 AND session_id = $2`,
		`DELETE FROM session_participants WHERE tenant_id = $1 AND session_id = $2`,
		`DELETE FROM session_displays WHERE tenant_id = $1 AND session_id = $2`,
		`DELETE FROM sessions WHERE tenant_id = $1 AND session_id = $2`,
	} {
		if _, err := tx.sqlTx.ExecContext(ctx, query, tx.tenantID, sessionID); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) listSessionRunIDs(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, limit uint64) ([]domain.RunID, error) {
	rows, err := store.db.QueryContext(ctx,
		`SELECT run_id FROM runs_by_session WHERE tenant_id = $1 AND session_id = $2 ORDER BY created_at ASC, run_id ASC LIMIT $3`,
		tenantID, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []domain.RunID
	for rows.Next() {
		var id domain.RunID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (store *Store) countSessionParticipantRows(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, maxRows uint64) (uint64, error) {
	rows, err := store.db.QueryContext(ctx,
		`SELECT user_id FROM session_participants
		 WHERE tenant_id = $1 AND session_id = $2 ORDER BY user_id ASC LIMIT $3`,
		tenantID, sessionID, maxRows+1)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var count uint64
	for rows.Next() {
		count++
		if count > maxRows {
			return 0, domain.ValidationError{Field: "session_deletion.participants", Reason: "exceeds the configured inventory bound"}
		}
		var userID domain.UserID
		if err := rows.Scan(&userID); err != nil {
			return 0, err
		}
	}
	return count, rows.Err()
}

func (store *Store) listSessionBindingIDs(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, limit uint64) ([]domain.FrontendBindingID, error) {
	rows, err := store.db.QueryContext(ctx,
		`SELECT binding_id FROM frontend_bindings_by_session
		 WHERE tenant_id = $1 AND session_id = $2 ORDER BY binding_id ASC LIMIT $3`,
		tenantID, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []domain.FrontendBindingID
	for rows.Next() {
		var id domain.FrontendBindingID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, store.db)
	if err != nil || ready {
		return ids, err
	}
	legacyRows, err := store.db.QueryContext(ctx,
		`SELECT binding_id FROM frontend_bindings
		 WHERE tenant_id = $1 AND session_id = $2 LIMIT $3`, tenantID, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer legacyRows.Close()
	var legacy []domain.FrontendBindingID
	for legacyRows.Next() {
		var id domain.FrontendBindingID
		if err := legacyRows.Scan(&id); err != nil {
			return nil, err
		}
		legacy = append(legacy, id)
	}
	if err := legacyRows.Err(); err != nil {
		return nil, err
	}
	return mergeBoundedStringIDs(ids, legacy, limit), nil
}

func (store *Store) listSessionProjectionIDs(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, limit uint64) ([]domain.FrontendProjectionID, error) {
	rows, err := store.db.QueryContext(ctx,
		`SELECT frontend_projection_id FROM frontend_projections_by_session
		 WHERE tenant_id = $1 AND session_id = $2 ORDER BY frontend_projection_id ASC LIMIT $3`,
		tenantID, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []domain.FrontendProjectionID
	for rows.Next() {
		var id domain.FrontendProjectionID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, store.db)
	if err != nil || ready {
		return ids, err
	}
	legacyRows, err := store.db.QueryContext(ctx,
		`SELECT frontend_projection_id FROM frontend_projection_outbox
		 WHERE tenant_id = $1 AND session_id = $2 LIMIT $3`, tenantID, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer legacyRows.Close()
	var legacy []domain.FrontendProjectionID
	for legacyRows.Next() {
		var id domain.FrontendProjectionID
		if err := legacyRows.Scan(&id); err != nil {
			return nil, err
		}
		legacy = append(legacy, id)
	}
	if err := legacyRows.Err(); err != nil {
		return nil, err
	}
	return mergeBoundedStringIDs(ids, legacy, limit), nil
}

func listSessionRunIDsTx(ctx context.Context, tx *stateTx, sessionID domain.SessionID, limit uint64) ([]domain.RunID, error) {
	rows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT run_id FROM runs_by_session WHERE tenant_id = $1 AND session_id = $2 ORDER BY created_at ASC, run_id ASC LIMIT $3`,
		tx.tenantID, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []domain.RunID
	for rows.Next() {
		if uint64(len(ids)) >= maxSessionDeletionRows {
			return nil, domain.ValidationError{Field: "session_deletion.runs", Reason: "exceeds the hard deletion bound"}
		}
		var id domain.RunID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func listSessionProjectionIDsTx(ctx context.Context, tx *stateTx, sessionID domain.SessionID, limit uint64) ([]domain.FrontendProjectionID, error) {
	rows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT frontend_projection_id FROM frontend_projections_by_session
		 WHERE tenant_id = $1 AND session_id = $2 LIMIT $3`, tx.tenantID, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []domain.FrontendProjectionID
	for rows.Next() {
		var id domain.FrontendProjectionID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, tx.sqlTx)
	if err != nil || ready {
		return ids, err
	}
	legacyRows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT frontend_projection_id FROM frontend_projection_outbox
		 WHERE tenant_id = $1 AND session_id = $2 LIMIT $3`, tx.tenantID, sessionID, limit)
	if err != nil {
		return nil, err
	}
	defer legacyRows.Close()
	var legacy []domain.FrontendProjectionID
	for legacyRows.Next() {
		var id domain.FrontendProjectionID
		if err := legacyRows.Scan(&id); err != nil {
			return nil, err
		}
		legacy = append(legacy, id)
	}
	if err := legacyRows.Err(); err != nil {
		return nil, err
	}
	return mergeBoundedStringIDs(ids, legacy, limit), nil
}

func (store *Store) listRunManifestIDs(ctx context.Context, tenantID domain.TenantID, runID domain.RunID, limit uint64) ([]domain.ArtifactManifestID, error) {
	rows, err := store.db.QueryContext(ctx,
		`SELECT artifact_manifest_id FROM artifact_manifests_by_run WHERE tenant_id = $1 AND run_id = $2 ORDER BY artifact_manifest_id ASC LIMIT $3`,
		tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []domain.ArtifactManifestID
	for rows.Next() {
		var id domain.ArtifactManifestID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, store.db)
	if err != nil || ready {
		return ids, err
	}
	legacyRows, err := store.db.QueryContext(ctx,
		`SELECT artifact_manifest_id FROM artifact_manifests
		 WHERE tenant_id = $1 AND run_id = $2 LIMIT $3`, tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer legacyRows.Close()
	var legacy []domain.ArtifactManifestID
	for legacyRows.Next() {
		var id domain.ArtifactManifestID
		if err := legacyRows.Scan(&id); err != nil {
			return nil, err
		}
		legacy = append(legacy, id)
	}
	if err := legacyRows.Err(); err != nil {
		return nil, err
	}
	return mergeBoundedStringIDs(ids, legacy, limit), nil
}

func (store *Store) listRunDeliveryIDs(ctx context.Context, tenantID domain.TenantID, runID domain.RunID, limit uint64) ([]domain.TelegramDeliveryID, error) {
	rows, err := store.db.QueryContext(ctx,
		`SELECT telegram_delivery_id FROM telegram_deliveries_by_run
		 WHERE tenant_id = $1 AND run_id = $2 ORDER BY telegram_delivery_id ASC LIMIT $3`,
		tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []domain.TelegramDeliveryID
	for rows.Next() {
		var id domain.TelegramDeliveryID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, store.db)
	if err != nil || ready {
		return ids, err
	}
	legacyRows, err := store.db.QueryContext(ctx,
		`SELECT telegram_delivery_id FROM telegram_delivery_outbox
		 WHERE tenant_id = $1 AND run_id = $2 LIMIT $3`, tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer legacyRows.Close()
	var legacy []domain.TelegramDeliveryID
	for legacyRows.Next() {
		var id domain.TelegramDeliveryID
		if err := legacyRows.Scan(&id); err != nil {
			return nil, err
		}
		legacy = append(legacy, id)
	}
	if err := legacyRows.Err(); err != nil {
		return nil, err
	}
	return mergeBoundedStringIDs(ids, legacy, limit), nil
}

func (store *Store) listRunCheckpointIDs(ctx context.Context, tenantID domain.TenantID, runID domain.RunID, limit uint64) ([]domain.CheckpointID, error) {
	rows, err := store.db.QueryContext(ctx,
		`SELECT checkpoint_id FROM checkpoint_objects_by_run
		 WHERE tenant_id = $1 AND run_id = $2 ORDER BY checkpoint_id ASC LIMIT $3`,
		tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []domain.CheckpointID
	for rows.Next() {
		var id domain.CheckpointID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, store.db)
	if err != nil || ready {
		return ids, err
	}
	legacyRows, err := store.db.QueryContext(ctx,
		`SELECT checkpoint_id FROM checkpoints
		 WHERE tenant_id = $1 AND run_id = $2 LIMIT $3`, tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer legacyRows.Close()
	var legacy []domain.CheckpointID
	for legacyRows.Next() {
		var id domain.CheckpointID
		if err := legacyRows.Scan(&id); err != nil {
			return nil, err
		}
		legacy = append(legacy, id)
	}
	if err := legacyRows.Err(); err != nil {
		return nil, err
	}
	return mergeBoundedStringIDs(ids, legacy, limit), nil
}

func listRunManifestIDsTx(ctx context.Context, tx *stateTx, runID domain.RunID, limit uint64) ([]domain.ArtifactManifestID, error) {
	rows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT artifact_manifest_id FROM artifact_manifests_by_run WHERE tenant_id = $1 AND run_id = $2 ORDER BY artifact_manifest_id ASC LIMIT $3`,
		tx.tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []domain.ArtifactManifestID
	for rows.Next() {
		if uint64(len(ids)) >= maxSessionDeletionRows {
			return nil, domain.ValidationError{Field: "session_deletion.manifests", Reason: "exceeds the hard deletion bound"}
		}
		var id domain.ArtifactManifestID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, tx.sqlTx)
	if err != nil || ready {
		return ids, err
	}
	legacyRows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT artifact_manifest_id FROM artifact_manifests
		 WHERE tenant_id = $1 AND run_id = $2 LIMIT $3`, tx.tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer legacyRows.Close()
	var legacy []domain.ArtifactManifestID
	for legacyRows.Next() {
		var id domain.ArtifactManifestID
		if err := legacyRows.Scan(&id); err != nil {
			return nil, err
		}
		legacy = append(legacy, id)
	}
	if err := legacyRows.Err(); err != nil {
		return nil, err
	}
	return mergeBoundedStringIDs(ids, legacy, limit), nil
}

func listRunDeliveryIDsTx(ctx context.Context, tx *stateTx, runID domain.RunID, limit uint64) ([]domain.TelegramDeliveryID, error) {
	rows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT telegram_delivery_id FROM telegram_deliveries_by_run
		 WHERE tenant_id = $1 AND run_id = $2 ORDER BY telegram_delivery_id ASC LIMIT $3`,
		tx.tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []domain.TelegramDeliveryID
	for rows.Next() {
		if uint64(len(ids)) >= maxSessionDeletionRows {
			return nil, domain.ValidationError{Field: "session_deletion.deliveries", Reason: "exceeds the hard deletion bound"}
		}
		var id domain.TelegramDeliveryID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, tx.sqlTx)
	if err != nil || ready {
		return ids, err
	}
	legacyRows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT telegram_delivery_id FROM telegram_delivery_outbox
		 WHERE tenant_id = $1 AND run_id = $2 LIMIT $3`, tx.tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer legacyRows.Close()
	var legacy []domain.TelegramDeliveryID
	for legacyRows.Next() {
		var id domain.TelegramDeliveryID
		if err := legacyRows.Scan(&id); err != nil {
			return nil, err
		}
		legacy = append(legacy, id)
	}
	if err := legacyRows.Err(); err != nil {
		return nil, err
	}
	return mergeBoundedStringIDs(ids, legacy, limit), nil
}

func listRunCheckpointIDsTx(ctx context.Context, tx *stateTx, runID domain.RunID, limit uint64) ([]domain.CheckpointID, error) {
	rows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT checkpoint_id FROM checkpoint_objects_by_run
		 WHERE tenant_id = $1 AND run_id = $2 ORDER BY checkpoint_id ASC LIMIT $3`,
		tx.tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []domain.CheckpointID
	for rows.Next() {
		if uint64(len(ids)) >= maxSessionDeletionRows {
			return nil, domain.ValidationError{Field: "session_deletion.checkpoints", Reason: "exceeds the hard deletion bound"}
		}
		var id domain.CheckpointID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, tx.sqlTx)
	if err != nil || ready {
		return ids, err
	}
	legacyRows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT checkpoint_id FROM checkpoints
		 WHERE tenant_id = $1 AND run_id = $2 LIMIT $3`, tx.tenantID, runID, limit)
	if err != nil {
		return nil, err
	}
	defer legacyRows.Close()
	var legacy []domain.CheckpointID
	for legacyRows.Next() {
		var id domain.CheckpointID
		if err := legacyRows.Scan(&id); err != nil {
			return nil, err
		}
		legacy = append(legacy, id)
	}
	if err := legacyRows.Err(); err != nil {
		return nil, err
	}
	return mergeBoundedStringIDs(ids, legacy, limit), nil
}

func appendSessionLifecycleAuditTx(
	ctx context.Context,
	tx *stateTx,
	at time.Time,
	actorID domain.UserID,
	action string,
	sessionID domain.SessionID,
	metadata map[string]any,
) error {
	if metadata == nil {
		metadata = map[string]any{}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(action + ":" + string(sessionID) + ":" + at.UTC().Format(time.RFC3339Nano)))
	auditID := "audit-lifecycle-" + hex.EncodeToString(sum[:12])
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO audit_events
		 (tenant_id, occurred_at, audit_event_id, actor_id, action,
		  subject_kind, subject_id, outcome, metadata, expire_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument), $10)`,
		tx.tenantID, at, auditID, actorID, action, "session", sessionID,
		"succeeded", string(encoded), nullableTime(nil),
	)
	return err
}

func validateLifecycleIdentity(tenantID domain.TenantID, sessionID domain.SessionID, userID domain.UserID, at time.Time) error {
	if err := tenantID.Validate(); err != nil {
		return err
	}
	if err := sessionID.Validate(); err != nil {
		return err
	}
	if err := userID.Validate(); err != nil {
		return err
	}
	if at.IsZero() {
		return domain.ValidationError{Field: "session_lifecycle.at", Reason: "must not be zero"}
	}
	return nil
}

func lifecyclePageLimit(remaining uint64) uint64 {
	if remaining == 0 {
		return 1
	}
	if remaining >= maxSessionPageSize {
		return maxSessionPageSize
	}
	return remaining
}

func mergeBoundedStringIDs[T ~string](primary []T, fallback []T, limit uint64) []T {
	seen := make(map[T]struct{}, len(primary)+len(fallback))
	merged := make([]T, 0, min(int(limit), len(primary)+len(fallback)))
	for _, ids := range [][]T{primary, fallback} {
		for _, id := range ids {
			if _, exists := seen[id]; exists {
				continue
			}
			seen[id] = struct{}{}
			merged = append(merged, id)
			if uint64(len(merged)) == limit {
				sort.Slice(merged, func(i, j int) bool { return merged[i] < merged[j] })
				return merged
			}
		}
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i] < merged[j] })
	return merged
}

var _ ports.SessionLifecycleStore = (*Store)(nil)
