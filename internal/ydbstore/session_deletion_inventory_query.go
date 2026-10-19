package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Private read-only composition over EITHER the installed DB OR the original
// transaction executor. There is no Store pointer, transaction constructor,
// callback selected by a request, external I/O or ability to commit here.
type sessionDeletionInventoryQuery struct{ query stateSQLExecutor }

func (r *sessionDeletionInventoryQuery) GetSessionDeletion(ctx context.Context, tenant domain.TenantID, session domain.SessionID) (domain.SessionDeletion, bool, error) {
	return readJSON[domain.SessionDeletion](ctx, r.query, `SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`, tenant, session)
}

func deletionInventoryJSONRows[T any](ctx context.Context, query stateSQLExecutor, statement string, limit uint64, args ...any) (result []T, err error) {
	if limit == 0 || limit > maxSessionDeletionRows+1 {
		return nil, domain.ErrMembershipDenied
	}
	rows, err := query.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	for rows.Next() {
		if uint64(len(result)) >= limit {
			return nil, domain.ErrMembershipDenied
		}
		var raw string
		var v T
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}

func (r *sessionDeletionInventoryQuery) ListSessionHistory(ctx context.Context, tenant domain.TenantID, session domain.SessionID, after, limit uint64) ([]domain.SessionEvent, error) {
	return deletionInventoryJSONRows[domain.SessionEvent](ctx, r.query,
		`SELECT record FROM session_events
 WHERE tenant_id = $1 AND session_id = $2 AND sequence > $3
 ORDER BY sequence ASC LIMIT $4`, limit, tenant, session, after, limit)
}

func (r *sessionDeletionInventoryQuery) ListSessionSnapshots(ctx context.Context, tenant domain.TenantID, session domain.SessionID, after, limit uint64) ([]domain.SessionSnapshot, error) {
	return deletionInventoryJSONRows[domain.SessionSnapshot](ctx, r.query,
		`SELECT record FROM session_snapshots
 WHERE tenant_id = $1 AND session_id = $2 AND version > $3
 ORDER BY version ASC LIMIT $4`, limit, tenant, session, after, limit)
}

func deletionInventoryIDs[T ~string](ctx context.Context, query stateSQLExecutor, statement string, limit uint64, args ...any) (ids []T, err error) {
	if limit == 0 || limit > maxSessionDeletionRows+1 {
		return nil, domain.ErrMembershipDenied
	}
	rows, err := query.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	for rows.Next() {
		if uint64(len(ids)) >= limit {
			return nil, domain.ErrMembershipDenied
		}
		var id T
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// All SQL identifiers below are compile-time-owned call sites, not caller
// input. Preserve the original lifecycle backfill boundary and exact union.
func deletionInventoryIndexedIDs[T ~string](ctx context.Context, query stateSQLExecutor, column, index, canonical, ownerColumn string, tenant domain.TenantID, owner any, limit uint64) ([]T, error) {
	primary, err := deletionInventoryIDs[T](ctx, query, fmt.Sprintf("SELECT %s FROM %s WHERE tenant_id = $1 AND %s = $2 ORDER BY %s ASC LIMIT $3", column, index, ownerColumn, column), limit, tenant, owner, limit)
	if err != nil {
		return nil, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, query)
	if err != nil || ready {
		return primary, err
	}
	legacy, err := deletionInventoryIDs[T](ctx, query, fmt.Sprintf("SELECT %s FROM %s WHERE tenant_id = $1 AND %s = $2 LIMIT $3", column, canonical, ownerColumn), limit, tenant, owner, limit)
	if err != nil {
		return nil, err
	}
	return mergeBoundedStringIDs(primary, legacy, limit), nil
}

func (r *sessionDeletionInventoryQuery) countSessionParticipantRows(ctx context.Context, tenant domain.TenantID, session domain.SessionID, remaining uint64) (uint64, error) {
	ids, err := deletionInventoryIDs[domain.UserID](ctx, r.query, `SELECT user_id FROM session_participants WHERE tenant_id = $1 AND session_id = $2 ORDER BY user_id ASC LIMIT $3`, remaining+1, tenant, session, remaining+1)
	if err != nil {
		return 0, err
	}
	if uint64(len(ids)) > remaining {
		return 0, domain.ErrMembershipDenied
	}
	return uint64(len(ids)), nil
}

func (r *sessionDeletionInventoryQuery) listSessionBindingIDs(ctx context.Context, tenant domain.TenantID, session domain.SessionID, limit uint64) ([]domain.FrontendBindingID, error) {
	return deletionInventoryIndexedIDs[domain.FrontendBindingID](ctx, r.query, "binding_id", "frontend_bindings_by_session", "frontend_bindings", "session_id", tenant, session, limit)
}
func (r *sessionDeletionInventoryQuery) listSessionProjectionIDs(ctx context.Context, tenant domain.TenantID, session domain.SessionID, limit uint64) ([]domain.FrontendProjectionID, error) {
	return deletionInventoryIndexedIDs[domain.FrontendProjectionID](ctx, r.query, "frontend_projection_id", "frontend_projections_by_session", "frontend_projection_outbox", "session_id", tenant, session, limit)
}
func (r *sessionDeletionInventoryQuery) listSessionRunIDs(ctx context.Context, tenant domain.TenantID, session domain.SessionID, limit uint64) ([]domain.RunID, error) {
	return deletionInventoryIDs[domain.RunID](ctx, r.query, `SELECT run_id FROM runs_by_session WHERE tenant_id = $1 AND session_id = $2 ORDER BY created_at ASC, run_id ASC LIMIT $3`, limit, tenant, session, limit)
}
func (r *sessionDeletionInventoryQuery) listRunManifestIDs(ctx context.Context, tenant domain.TenantID, run domain.RunID, limit uint64) ([]domain.ArtifactManifestID, error) {
	return deletionInventoryIndexedIDs[domain.ArtifactManifestID](ctx, r.query, "artifact_manifest_id", "artifact_manifests_by_run", "artifact_manifests", "run_id", tenant, run, limit)
}
func (r *sessionDeletionInventoryQuery) listRunDeliveryIDs(ctx context.Context, tenant domain.TenantID, run domain.RunID, limit uint64) ([]domain.TelegramDeliveryID, error) {
	return deletionInventoryIndexedIDs[domain.TelegramDeliveryID](ctx, r.query, "telegram_delivery_id", "telegram_deliveries_by_run", "telegram_delivery_outbox", "run_id", tenant, run, limit)
}
func (r *sessionDeletionInventoryQuery) listRunCheckpointIDs(ctx context.Context, tenant domain.TenantID, run domain.RunID, limit uint64) ([]domain.CheckpointID, error) {
	return deletionInventoryIndexedIDs[domain.CheckpointID](ctx, r.query, "checkpoint_id", "checkpoint_objects_by_run", "checkpoints", "run_id", tenant, run, limit)
}
func (r *sessionDeletionInventoryQuery) GetArtifactManifest(ctx context.Context, tenant domain.TenantID, manifest domain.ArtifactManifestID) (domain.ArtifactManifest, bool, error) {
	if err := tenant.Validate(); err != nil {
		return domain.ArtifactManifest{}, false, err
	}
	if err := manifest.Validate(); err != nil {
		return domain.ArtifactManifest{}, false, err
	}
	return readJSON[domain.ArtifactManifest](ctx, r.query, `SELECT payload FROM artifact_manifests WHERE tenant_id = $1 AND artifact_manifest_id = $2`, tenant, manifest)
}

func (r *sessionDeletionInventoryQuery) countSessionMaterialRows(ctx context.Context, statement string, tenant domain.TenantID, session domain.SessionID, remaining uint64) (count uint64, err error) {
	rows, err := r.query.QueryContext(ctx, statement, tenant, session, remaining+1)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	for rows.Next() {
		if count >= remaining {
			return 0, domain.ErrMembershipDenied
		}
		var key any
		if err := rows.Scan(&key); err != nil {
			return 0, err
		}
		count++
	}
	return count, rows.Err()
}

func (r *sessionDeletionInventoryQuery) listRunOutputReceipts(ctx context.Context, tenant domain.TenantID, run domain.RunID, limit uint64) (receipts []AttachedWorkerOutputReceiptV1, err error) {
	if limit == 0 || limit > maxSessionDeletionRows+1 {
		return nil, domain.ErrMembershipDenied
	}
	rows, err := r.query.QueryContext(ctx, `SELECT owner_user_id,worker_id,attempt_id,lease_generation,payload
 FROM attached_worker_output_receipts WHERE tenant_id=$1 AND run_id=$2
 ORDER BY owner_user_id,worker_id,attempt_id,lease_generation LIMIT $3`, tenant, run, limit)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	for rows.Next() {
		if uint64(len(receipts)) >= limit {
			return nil, domain.ErrMembershipDenied
		}
		var owner domain.UserID
		var worker domain.AttachedWorkerID
		var attempt domain.AttemptID
		var generation uint64
		var raw string
		if err := rows.Scan(&owner, &worker, &attempt, &generation, &raw); err != nil {
			return nil, err
		}
		var receipt AttachedWorkerOutputReceiptV1
		if err := json.Unmarshal([]byte(raw), &receipt); err != nil {
			return nil, err
		}
		if err := validateAttachedWorkerReceiptRecord(receipt, tenant, run, owner, worker, attempt, generation); err != nil {
			return nil, err
		}
		receipts = append(receipts, receipt)
	}
	return receipts, rows.Err()
}

// Inventory identity is NOT the retained metadata commitment. It is useful
// only for an exact SAME-transaction inventory rejoin; full writer/metadata
// proof remains separately mandatory before any retained guard is relaxed.
func rejoinSessionDeletionInventoryTx(ctx context.Context, tx *stateTx, expected domain.SessionDeletionInventory) error {
	actual, err := buildSessionDeletionInventory(ctx, &sessionDeletionInventoryQuery{query: tx.sqlTx}, tx.tenantID, expected.SessionID, maxSessionDeletionRows, maxSessionDeletionRows)
	if err != nil {
		return err
	}
	before, be := domain.SessionDeletionInventoryDigestV1(expected)
	after, ae := domain.SessionDeletionInventoryDigestV1(actual)
	if be != nil || ae != nil || before != after {
		return ErrSessionLifecycleConflict
	}
	return nil
}

var _ stateSQLExecutor = (*sql.DB)(nil)
