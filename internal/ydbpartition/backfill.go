package ydbpartition

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type BackfillResult struct {
	Table string `json:"table"`
	Rows  uint64 `json:"rows"`
}

// BackfillReadyExpiryV2 copies the four legacy ready/expiry tables into the
// bucketed v2 layout. UPSERT makes the operation restart-safe. This command is
// a deployment migration tool; full legacy-table reads are forbidden in
// serving paths.
func BackfillReadyExpiryV2(
	ctx context.Context,
	db *sql.DB,
	dryRun bool,
) ([]BackfillResult, error) {
	if db == nil {
		return nil, errors.New("YDB database must not be nil")
	}
	operations := []func(context.Context, *sql.DB, bool) (BackfillResult, error){
		backfillLeaseExpiry,
		backfillDispatchReady,
		backfillTelegramDeliveryReady,
		backfillQuotaExpiry,
	}
	results := make([]BackfillResult, 0, len(operations))
	for _, operation := range operations {
		result, err := operation(ctx, db, dryRun)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

// BackfillSchemaIndexes performs the complete deployment backfill. Serving
// code dual-writes these tables; the completion marker is written only after
// every legacy row visible to this run has been copied.
func BackfillSchemaIndexes(ctx context.Context, db *sql.DB, dryRun bool) ([]BackfillResult, error) {
	cutover, err := verifyExecutionPlacementCutover(ctx, db, dryRun)
	if err != nil {
		return nil, err
	}
	results, err := BackfillReadyExpiryV2(ctx, db, dryRun)
	if err != nil {
		return nil, err
	}
	results = append([]BackfillResult{cutover}, results...)
	operations := []func(context.Context, *sql.DB, bool) (BackfillResult, error){
		backfillArtifactManifestsByRun,
		backfillFrontendBindingsBySession,
		backfillFrontendProjectionsBySession,
		backfillFrontendProjectionsByRun,
		backfillFrontendProjectionReady,
		backfillTelegramDeliveriesByRun,
		backfillCheckpointObjectsByRun,
	}
	for _, operation := range operations {
		result, err := operation(ctx, db, dryRun)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if !dryRun {
		if _, err := db.ExecContext(ctx,
			`UPSERT INTO session_lifecycle_backfill_state (backfill_id, completed_at) VALUES ($1, $2)`,
			"session-lifecycle-indexes-v1", time.Now().UTC(),
		); err != nil {
			return nil, fmt.Errorf("mark session lifecycle backfill complete: %w", err)
		}
	}
	return results, nil
}

// verifyExecutionPlacementCutover is the enforced no-production-data gate for
// the first explicit placement rollout. Serving code never interprets a
// missing placement as managed. Before the marker exists, both legacy payload
// tables must be empty; operators must drain or use the typed preprod reset.
// Once marked, all writers persist canonical placement and later backfill runs
// do not require an empty live system.
func verifyExecutionPlacementCutover(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	const cutoverID = "execution-placement-v1-empty-cutover"
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return BackfillResult{}, fmt.Errorf("begin execution placement cutover: %w", err)
	}
	defer tx.Rollback()
	var completedAt time.Time
	err = tx.QueryRowContext(ctx,
		`SELECT completed_at FROM execution_placement_cutover_state WHERE cutover_id=$1`, cutoverID,
	).Scan(&completedAt)
	if err == nil {
		return BackfillResult{Table: "execution_placement_cutover_v1"}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return BackfillResult{}, fmt.Errorf("read execution placement cutover marker: %w", err)
	}
	for _, table := range []string{"dispatch_outbox", "worker_jobs"} {
		var tenantID string
		err := tx.QueryRowContext(ctx, "SELECT tenant_id FROM "+table+" LIMIT 1").Scan(&tenantID)
		if err == nil {
			return BackfillResult{}, fmt.Errorf("execution placement cutover requires empty %s; drain or reset before deployment", table)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return BackfillResult{}, fmt.Errorf("verify empty %s: %w", table, err)
		}
	}
	if !dryRun {
		if _, err := tx.ExecContext(ctx,
			`UPSERT INTO execution_placement_cutover_state (cutover_id,completed_at) VALUES ($1,$2)`,
			cutoverID, time.Now().UTC()); err != nil {
			return BackfillResult{}, fmt.Errorf("mark execution placement cutover: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return BackfillResult{}, fmt.Errorf("commit execution placement cutover: %w", err)
		}
	}
	return BackfillResult{Table: "execution_placement_cutover_v1"}, nil
}

func backfillArtifactManifestsByRun(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	return backfillTriples(ctx, db, dryRun, "artifact_manifests_by_run",
		`SELECT tenant_id, run_id, artifact_manifest_id FROM artifact_manifests`,
		`UPSERT INTO artifact_manifests_by_run (tenant_id, run_id, artifact_manifest_id) VALUES ($1, $2, $3)`)
}

func backfillFrontendBindingsBySession(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	return backfillTriples(ctx, db, dryRun, "frontend_bindings_by_session",
		`SELECT tenant_id, session_id, binding_id FROM frontend_bindings`,
		`UPSERT INTO frontend_bindings_by_session (tenant_id, session_id, binding_id) VALUES ($1, $2, $3)`)
}

func backfillFrontendProjectionsBySession(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	return backfillTriples(ctx, db, dryRun, "frontend_projections_by_session",
		`SELECT tenant_id, session_id, frontend_projection_id FROM frontend_projection_outbox`,
		`UPSERT INTO frontend_projections_by_session (tenant_id, session_id, frontend_projection_id) VALUES ($1, $2, $3)`)
}

type projectionBackfillRow struct {
	tenantID, projectionID, frontend, sessionID, runID string
	sequence                                           uint64
	createdAt                                          time.Time
}

func frontendProjectionBackfillRows(ctx context.Context, db *sql.DB) ([]projectionBackfillRow, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT tenant_id, frontend_projection_id, frontend, session_id,
		        event_sequence, created_at
		 FROM frontend_projection_outbox`,
	)
	if err != nil {
		return nil, err
	}
	var values []projectionBackfillRow
	for rows.Next() {
		var value projectionBackfillRow
		if err := rows.Scan(
			&value.tenantID, &value.projectionID, &value.frontend,
			&value.sessionID, &value.sequence, &value.createdAt,
		); err != nil {
			rows.Close()
			return nil, err
		}
		values = append(values, value)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	resolved := values[:0]
	for index := range values {
		if err := db.QueryRowContext(ctx,
			`SELECT run_id FROM session_events
			 WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3`,
			values[index].tenantID, values[index].sessionID, values[index].sequence,
		).Scan(&values[index].runID); errors.Is(err, sql.ErrNoRows) {
			// Pre-index deployments can contain operational orphans after their
			// canonical session history has already been deleted. They cannot be
			// recovered into a run index and must not block unrelated tenants.
			continue
		} else if err != nil {
			return nil, fmt.Errorf("resolve projection %s run: %w", values[index].projectionID, err)
		}
		if values[index].runID == "" {
			continue
		}
		resolved = append(resolved, values[index])
	}
	return resolved, nil
}

func backfillFrontendProjectionsByRun(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	values, err := frontendProjectionBackfillRows(ctx, db)
	if err != nil {
		return BackfillResult{}, err
	}
	for _, value := range values {
		if dryRun {
			continue
		}
		if _, err := db.ExecContext(ctx,
			`UPSERT INTO frontend_projections_by_run
			 (tenant_id, run_id, frontend, frontend_projection_id) VALUES ($1, $2, $3, $4)`,
			value.tenantID, value.runID, value.frontend, value.projectionID,
		); err != nil {
			return BackfillResult{}, fmt.Errorf("backfill frontend_projections_by_run: %w", err)
		}
	}
	return BackfillResult{Table: "frontend_projections_by_run", Rows: uint64(len(values))}, nil
}

func backfillFrontendProjectionReady(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	values, err := frontendProjectionBackfillRows(ctx, db)
	if err != nil {
		return BackfillResult{}, err
	}
	for _, value := range values {
		bucket, err := BucketV1(value.projectionID)
		if err != nil {
			return BackfillResult{}, err
		}
		if dryRun {
			continue
		}
		if _, err := db.ExecContext(ctx,
			`UPSERT INTO frontend_projection_ready_v1
			 (frontend, shard_bucket, created_at, tenant_id, frontend_projection_id, run_id)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			value.frontend, bucket, value.createdAt, value.tenantID, value.projectionID, value.runID,
		); err != nil {
			return BackfillResult{}, fmt.Errorf("backfill frontend_projection_ready_v1: %w", err)
		}
	}
	return BackfillResult{Table: "frontend_projection_ready_v1", Rows: uint64(len(values))}, nil
}

func backfillTelegramDeliveriesByRun(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	return backfillJSONRows(ctx, db, dryRun, "telegram_deliveries_by_run",
		`SELECT tenant_id, run_id, telegram_delivery_id, payload FROM telegram_delivery_outbox`,
		`UPSERT INTO telegram_deliveries_by_run
		 (tenant_id, run_id, telegram_delivery_id, record)
		 VALUES ($1, $2, $3, CAST($4 AS JsonDocument))`)
}

func backfillCheckpointObjectsByRun(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	return backfillJSONRows(ctx, db, dryRun, "checkpoint_objects_by_run",
		`SELECT tenant_id, run_id, checkpoint_id, payload FROM checkpoints`,
		`UPSERT INTO checkpoint_objects_by_run
		 (tenant_id, run_id, checkpoint_id, record)
		 VALUES ($1, $2, $3, CAST($4 AS JsonDocument))`)
}

func backfillTriples(
	ctx context.Context,
	db *sql.DB,
	dryRun bool,
	table string,
	selectSQL string,
	upsertSQL string,
) (BackfillResult, error) {
	rows, err := db.QueryContext(ctx, selectSQL)
	if err != nil {
		return BackfillResult{}, fmt.Errorf("read %s source: %w", table, err)
	}
	defer rows.Close()
	type triple struct{ first, second, third string }
	var values []triple
	for rows.Next() {
		var value triple
		if err := rows.Scan(&value.first, &value.second, &value.third); err != nil {
			return BackfillResult{}, err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return BackfillResult{}, err
	}
	for _, value := range values {
		if dryRun {
			continue
		}
		if _, err := db.ExecContext(ctx, upsertSQL, value.first, value.second, value.third); err != nil {
			return BackfillResult{}, fmt.Errorf("backfill %s: %w", table, err)
		}
	}
	return BackfillResult{Table: table, Rows: uint64(len(values))}, nil
}

func backfillJSONRows(
	ctx context.Context,
	db *sql.DB,
	dryRun bool,
	table string,
	selectSQL string,
	upsertSQL string,
) (BackfillResult, error) {
	rows, err := db.QueryContext(ctx, selectSQL)
	if err != nil {
		return BackfillResult{}, fmt.Errorf("read %s source: %w", table, err)
	}
	defer rows.Close()
	type value struct{ first, second, third, record string }
	var values []value
	for rows.Next() {
		var item value
		if err := rows.Scan(&item.first, &item.second, &item.third, &item.record); err != nil {
			return BackfillResult{}, err
		}
		values = append(values, item)
	}
	if err := rows.Err(); err != nil {
		return BackfillResult{}, err
	}
	for _, item := range values {
		if dryRun {
			continue
		}
		if _, err := db.ExecContext(ctx, upsertSQL, item.first, item.second, item.third, item.record); err != nil {
			return BackfillResult{}, fmt.Errorf("backfill %s: %w", table, err)
		}
	}
	return BackfillResult{Table: table, Rows: uint64(len(values))}, nil
}

func backfillLeaseExpiry(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	type row struct {
		tenantID, runID, leaseID string
		expiresAt                time.Time
		fence                    uint64
	}
	rows, err := db.QueryContext(ctx,
		`SELECT tenant_id, expires_at, run_id, lease_id, fence_token FROM lease_expiry`,
	)
	if err != nil {
		return BackfillResult{}, err
	}
	var values []row
	for rows.Next() {
		var value row
		if err := rows.Scan(
			&value.tenantID, &value.expiresAt, &value.runID, &value.leaseID, &value.fence,
		); err != nil {
			rows.Close()
			return BackfillResult{}, err
		}
		values = append(values, value)
	}
	if err := rows.Close(); err != nil {
		return BackfillResult{}, err
	}
	for _, value := range values {
		bucket, err := BucketV1(value.runID)
		if err != nil {
			return BackfillResult{}, err
		}
		if dryRun {
			continue
		}
		if _, err := db.ExecContext(ctx,
			`UPSERT INTO lease_expiry_v2
			 (shard_bucket, expires_at, tenant_id, run_id, lease_id, fence_token)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			bucket, value.expiresAt, value.tenantID, value.runID, value.leaseID, value.fence,
		); err != nil {
			return BackfillResult{}, err
		}
	}
	return BackfillResult{Table: "lease_expiry_v2", Rows: uint64(len(values))}, rows.Err()
}

func backfillDispatchReady(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	type row struct {
		tenantID, outboxID, runID, attemptID string
		availableAt                          time.Time
	}
	rows, err := db.QueryContext(ctx,
		`SELECT tenant_id, available_at, dispatch_outbox_id, run_id, attempt_id
		 FROM dispatch_ready`,
	)
	if err != nil {
		return BackfillResult{}, err
	}
	var values []row
	for rows.Next() {
		var value row
		if err := rows.Scan(
			&value.tenantID, &value.availableAt, &value.outboxID, &value.runID, &value.attemptID,
		); err != nil {
			rows.Close()
			return BackfillResult{}, err
		}
		values = append(values, value)
	}
	if err := rows.Close(); err != nil {
		return BackfillResult{}, err
	}
	for _, value := range values {
		bucket, err := BucketV1(value.outboxID)
		if err != nil {
			return BackfillResult{}, err
		}
		if dryRun {
			continue
		}
		if _, err := db.ExecContext(ctx,
			`UPSERT INTO dispatch_ready_v2
			 (shard_bucket, available_at, tenant_id, dispatch_outbox_id, run_id, attempt_id)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			bucket, value.availableAt, value.tenantID, value.outboxID, value.runID, value.attemptID,
		); err != nil {
			return BackfillResult{}, err
		}
	}
	return BackfillResult{Table: "dispatch_ready_v2", Rows: uint64(len(values))}, rows.Err()
}

func backfillTelegramDeliveryReady(
	ctx context.Context,
	db *sql.DB,
	dryRun bool,
) (BackfillResult, error) {
	type row struct {
		tenantID, deliveryID, runID string
		availableAt                 time.Time
	}
	rows, err := db.QueryContext(ctx,
		`SELECT tenant_id, available_at, telegram_delivery_id, run_id
		 FROM telegram_delivery_ready`,
	)
	if err != nil {
		return BackfillResult{}, err
	}
	var values []row
	for rows.Next() {
		var value row
		if err := rows.Scan(&value.tenantID, &value.availableAt, &value.deliveryID, &value.runID); err != nil {
			rows.Close()
			return BackfillResult{}, err
		}
		values = append(values, value)
	}
	if err := rows.Close(); err != nil {
		return BackfillResult{}, err
	}
	for _, value := range values {
		bucket, err := BucketV1(value.deliveryID)
		if err != nil {
			return BackfillResult{}, err
		}
		if dryRun {
			continue
		}
		if _, err := db.ExecContext(ctx,
			`UPSERT INTO telegram_delivery_ready_v2
			 (shard_bucket, available_at, tenant_id, telegram_delivery_id, run_id)
			 VALUES ($1, $2, $3, $4, $5)`,
			bucket, value.availableAt, value.tenantID, value.deliveryID, value.runID,
		); err != nil {
			return BackfillResult{}, err
		}
	}
	return BackfillResult{Table: "telegram_delivery_ready_v2", Rows: uint64(len(values))}, rows.Err()
}

func backfillQuotaExpiry(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	type row struct {
		tenantID, reservationID, runID, connectionID string
		expiresAt                                    time.Time
	}
	rows, err := db.QueryContext(ctx,
		`SELECT tenant_id, expires_at, quota_reservation_id, run_id, subscription_connection_id
		 FROM quota_expiry`,
	)
	if err != nil {
		return BackfillResult{}, err
	}
	var values []row
	for rows.Next() {
		var value row
		if err := rows.Scan(
			&value.tenantID, &value.expiresAt, &value.reservationID,
			&value.runID, &value.connectionID,
		); err != nil {
			rows.Close()
			return BackfillResult{}, err
		}
		values = append(values, value)
	}
	if err := rows.Close(); err != nil {
		return BackfillResult{}, err
	}
	for _, value := range values {
		bucket, err := BucketV1(value.reservationID)
		if err != nil {
			return BackfillResult{}, err
		}
		if dryRun {
			continue
		}
		if _, err := db.ExecContext(ctx,
			`UPSERT INTO quota_expiry_v2
			 (shard_bucket, expires_at, tenant_id, quota_reservation_id,
			  run_id, subscription_connection_id)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			bucket, value.expiresAt, value.tenantID, value.reservationID,
			value.runID, value.connectionID,
		); err != nil {
			return BackfillResult{}, fmt.Errorf("backfill quota_expiry_v2: %w", err)
		}
	}
	return BackfillResult{Table: "quota_expiry_v2", Rows: uint64(len(values))}, rows.Err()
}
