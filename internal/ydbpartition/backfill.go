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
