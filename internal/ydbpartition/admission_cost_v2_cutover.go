package ydbpartition

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// AdmissionCostV2CutoverID is distinct from the earlier placement, binding and
// managed-authority markers. Those do not certify a V2 monetary payload.
const AdmissionCostV2CutoverID = "admission-cost-v2-empty-cutover"

// verifyAdmissionCostV2Cutover runs only in the deployment backfill, with old
// writers/readers stopped. No conversion or replay of a physical claim is
// permitted. Historical digest-only receipts are not new admission authority.
func verifyAdmissionCostV2Cutover(ctx context.Context, db *sql.DB, dryRun bool) (BackfillResult, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return BackfillResult{}, fmt.Errorf("begin admission cost v2 cutover: %w", err)
	}
	defer tx.Rollback()
	var completedAt time.Time
	err = tx.QueryRowContext(ctx,
		`SELECT completed_at FROM managed_execution_authority_v2_cutover_state WHERE cutover_id=$1`,
		AdmissionCostV2CutoverID,
	).Scan(&completedAt)
	if err == nil {
		if completedAt.IsZero() {
			return BackfillResult{}, errors.New("admission cost v2 cutover marker has no completion time")
		}
		return BackfillResult{Table: "admission_cost_cutover_v2"}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return BackfillResult{}, fmt.Errorf("read admission cost v2 cutover marker: %w", err)
	}
	for _, table := range []string{"dispatch_outbox", "worker_jobs", "attempt_effect_reservations"} {
		var tenantID string
		err := tx.QueryRowContext(ctx, "SELECT tenant_id FROM "+table+" LIMIT 1").Scan(&tenantID)
		if err == nil {
			return BackfillResult{}, fmt.Errorf("admission cost v2 cutover requires empty %s; drain before deployment", table)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return BackfillResult{}, fmt.Errorf("verify empty %s for admission cost v2: %w", table, err)
		}
	}
	if !dryRun {
		// The transaction's database clock, not the host clock, stamps the receipt.
		if err := tx.QueryRowContext(ctx, "SELECT CurrentUtcTimestamp()").Scan(&completedAt); err != nil {
			return BackfillResult{}, fmt.Errorf("read admission cost v2 cutover clock: %w", err)
		}
		if completedAt.IsZero() {
			return BackfillResult{}, errors.New("admission cost v2 cutover clock unavailable")
		}
		if _, err := tx.ExecContext(ctx,
			`UPSERT INTO managed_execution_authority_v2_cutover_state (cutover_id,completed_at) VALUES ($1,$2)`,
			AdmissionCostV2CutoverID, completedAt.UTC()); err != nil {
			return BackfillResult{}, fmt.Errorf("mark admission cost v2 cutover: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return BackfillResult{}, fmt.Errorf("commit admission cost v2 cutover: %w", err)
		}
	}
	return BackfillResult{Table: "admission_cost_cutover_v2"}, nil
}
