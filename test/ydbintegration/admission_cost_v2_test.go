//go:build ydbintegration

package ydbintegration

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// This test exercises the actual deployment transaction, not an operator-made
// marker or a fake scripting transaction. Run only against a disposable DB:
// it temporarily replaces exact schema receipts, never resets tenant tables.
func TestAdmissionCostV2YDBCutover(t *testing.T) {
	requireExplanationTrueTransactions(t)
	store, client := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	db := client.DB
	clock := func() time.Time {
		t.Helper()
		var at time.Time
		if err := db.QueryRowContext(ctx, "SELECT CurrentUtcTimestamp()").Scan(&at); err != nil || at.IsZero() {
			t.Fatalf("database clock=%v error=%v", at, err)
		}
		return at.UTC()
	}
	// The full entry point can backfill these sources too. Refuse unrelated
	// inventory before replacing any receipt; this is a disposable DB proof.
	for _, table := range []string{
		"dispatch_outbox", "worker_jobs", "attempt_effect_reservations",
		"artifact_manifests", "frontend_bindings", "frontend_projection_outbox",
		"telegram_delivery_outbox", "checkpoints",
	} {
		var tenant string
		if err := db.QueryRowContext(ctx, "SELECT tenant_id FROM "+table+" LIMIT 1").Scan(&tenant); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("disposable empty %s precondition: tenant=%q error=%v; no existing data will be removed", table, tenant, err)
		}
	}
	preserveAdmissionCostFixtureReceipt(t, ctx, db, "session_lifecycle_backfill_state", "backfill_id", "session-lifecycle-indexes-v1")
	markers := []struct{ table, id string }{
		{"execution_placement_cutover_state", "execution-placement-v1-empty-cutover"},
		{"harness_binding_cutover_state", "harness-binding-v1-empty-cutover"},
		{"harness_binding_cutover_state", "harness-binding-v2-empty-cutover"},
		{"managed_execution_authority_v2_cutover_state", "managed-execution-authority-v2-empty-cutover"},
		{"managed_execution_authority_v2_cutover_state", ydbpartition.AdmissionCostV2CutoverID},
	}
	for _, marker := range markers {
		preserveAdmissionCostFixtureReceipt(t, ctx, db, marker.table, "cutover_id", marker.id)
		if marker.id == ydbpartition.AdmissionCostV2CutoverID {
			if _, err := db.ExecContext(ctx, "DELETE FROM "+marker.table+" WHERE cutover_id=$1", marker.id); err != nil {
				t.Fatal(err)
			}
		} else if _, err := db.ExecContext(ctx, "UPSERT INTO "+marker.table+" (cutover_id,completed_at) VALUES ($1,$2)", marker.id, clock()); err != nil {
			t.Fatal(err)
		}
	}
	assertMissing := func(t *testing.T) {
		t.Helper()
		if err := store.RequireAdmissionCostV2Cutover(ctx); err == nil {
			t.Fatal("monetary serving gate accepted absent receipt despite earlier committed markers")
		}
		var at time.Time
		if err := db.QueryRowContext(ctx, `SELECT completed_at FROM managed_execution_authority_v2_cutover_state WHERE cutover_id=$1`, ydbpartition.AdmissionCostV2CutoverID).Scan(&at); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("missing monetary receipt: value=%v error=%v", at, err)
		}
	}
	assertMissing(t)
	t.Run("dry_run", func(t *testing.T) {
		if _, err := ydbpartition.BackfillSchemaIndexes(ctx, db, true); err != nil {
			t.Fatalf("actual dry-run backfill: %v", err)
		}
		assertMissing(t)
	})
	for _, fixture := range []struct{ table, insert, remove string }{
		{"dispatch_outbox", `INSERT INTO dispatch_outbox (tenant_id,dispatch_outbox_id) VALUES ($1,$2)`, `DELETE FROM dispatch_outbox WHERE tenant_id=$1 AND dispatch_outbox_id=$2`},
		{"worker_jobs", `INSERT INTO worker_jobs (tenant_id,run_id) VALUES ($1,$2)`, `DELETE FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`},
		{"attempt_effect_reservations", `INSERT INTO attempt_effect_reservations (tenant_id,attempt_id,kind) VALUES ($1,$2,"provider_turn")`, `DELETE FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind="provider_turn"`},
	} {
		t.Run("deny_"+fixture.table, func(t *testing.T) {
			nonce := rand.Text()
			tenant, id := "cost-cutover-tenant-"+nonce, "cost-cutover-row-"+nonce
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				if _, err := db.ExecContext(cleanupCtx, fixture.remove, tenant, id); err != nil {
					t.Errorf("remove exact owned %s backlog row: %v", fixture.table, err)
				}
			})
			if _, err := db.ExecContext(ctx, fixture.insert, tenant, id); err != nil {
				t.Fatal(err)
			}
			if _, err := ydbpartition.BackfillSchemaIndexes(ctx, db, false); err == nil || !strings.Contains(err.Error(), "admission cost v2 cutover requires empty "+fixture.table) {
				t.Fatalf("actual %s backlog must fail at monetary cutover, got %v", fixture.table, err)
			}
			assertMissing(t)
		})
	}
	t.Run("commit_and_idempotency", func(t *testing.T) {
		before := clock()
		results, err := ydbpartition.BackfillSchemaIndexes(ctx, db, false)
		if err != nil {
			t.Fatalf("actual empty-backlog commit: %v", err)
		}
		var receipt time.Time
		if err := db.QueryRowContext(ctx, `SELECT completed_at FROM managed_execution_authority_v2_cutover_state WHERE cutover_id=$1`, ydbpartition.AdmissionCostV2CutoverID).Scan(&receipt); err != nil {
			t.Fatal(err)
		}
		after := clock()
		if receipt.IsZero() || receipt.Before(before) || receipt.After(after) {
			t.Fatalf("receipt=%v outside authoritative database bracket [%v,%v]", receipt, before, after)
		}
		costResults := 0
		for _, result := range results {
			if result.Table == "admission_cost_cutover_v2" {
				costResults++
			}
		}
		if costResults != 1 {
			t.Fatalf("monetary backfill results=%d want1: %+v", costResults, results)
		}
		if err := store.RequireAdmissionCostV2Cutover(ctx); err != nil {
			t.Fatalf("serving gate rejected actual committed receipt: %v", err)
		}
		// Once the writer-first cutover committed, unrelated new backlog does
		// not invalidate it. Never rewrite its timestamp or legacy payload.
		nonce := rand.Text()
		tenant, run := "cost-repeat-tenant-"+nonce, "cost-repeat-run-"+nonce
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			if _, err := db.ExecContext(cleanupCtx, `DELETE FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, tenant, run); err != nil {
				t.Errorf("cleanup exact idempotency row: %v", err)
			}
		})
		if _, err := db.ExecContext(ctx, `INSERT INTO worker_jobs (tenant_id,run_id) VALUES ($1,$2)`, tenant, run); err != nil {
			t.Fatal(err)
		}
		if _, err := ydbpartition.BackfillSchemaIndexes(ctx, db, false); err != nil {
			t.Fatalf("repeat actual committed cutover with live backlog: %v", err)
		}
		var repeated time.Time
		if err := db.QueryRowContext(ctx, `SELECT completed_at FROM managed_execution_authority_v2_cutover_state WHERE cutover_id=$1`, ydbpartition.AdmissionCostV2CutoverID).Scan(&repeated); err != nil || !repeated.Equal(receipt) {
			t.Fatalf("immutable committed receipt=%v repeat=%v error=%v", receipt, repeated, err)
		}
	})
}

// Only compile-time fixture table/key constants may reach this helper. Register
// before mutation; cleanup restores exact timestamp or absence, never an
// invented deployment receipt. It runs before the enclosing DB cleanup.
func preserveAdmissionCostFixtureReceipt(t *testing.T, ctx context.Context, db *sql.DB, table, key, id string) {
	t.Helper()
	var previous time.Time
	err := db.QueryRowContext(ctx, "SELECT completed_at FROM "+table+" WHERE "+key+"=$1", id).Scan(&previous)
	found := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("read original receipt %s/%s: %v", table, id, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		var err error
		if found {
			_, err = db.ExecContext(cleanupCtx, "UPSERT INTO "+table+" ("+key+",completed_at) VALUES ($1,$2)", id, previous)
		} else {
			_, err = db.ExecContext(cleanupCtx, "DELETE FROM "+table+" WHERE "+key+"=$1", id)
		}
		if err != nil {
			t.Errorf("restore exact receipt %s/%s: %v", table, id, err)
		}
	})
}

// The deployment entry point also writes the lifecycle index receipt. The
// cutover fixture must leave it exactly as it was, including prior absence.
func TestAdmissionCostV2YDBCutoverRestoresLifecycleReceipt(t *testing.T) {
	requireExplanationTrueTransactions(t)
	_, client := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	const id = "session-lifecycle-indexes-v1"
	const read = `SELECT completed_at FROM session_lifecycle_backfill_state WHERE backfill_id=$1`
	const remove = `DELETE FROM session_lifecycle_backfill_state WHERE backfill_id=$1`
	const put = `UPSERT INTO session_lifecycle_backfill_state (backfill_id,completed_at) VALUES ($1,$2)`
	var original time.Time
	err := client.DB.QueryRowContext(ctx, read, id).Scan(&original)
	originalFound := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		var err error
		if originalFound {
			_, err = client.DB.ExecContext(cleanupCtx, put, id, original)
		} else {
			_, err = client.DB.ExecContext(cleanupCtx, remove, id)
		}
		if err != nil {
			t.Errorf("restore outer lifecycle receipt: %v", err)
		}
	})
	for _, found := range []bool{false, true} {
		name := "original_absence"
		if found {
			name = "original_timestamp"
		}
		t.Run(name, func(t *testing.T) {
			var before time.Time
			if found {
				if err := client.DB.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&before); err != nil {
					t.Fatal(err)
				}
				if _, err := client.DB.ExecContext(ctx, put, id, before); err != nil {
					t.Fatal(err)
				}
			} else if _, err := client.DB.ExecContext(ctx, remove, id); err != nil {
				t.Fatal(err)
			}
			if !t.Run("cutover", TestAdmissionCostV2YDBCutover) {
				t.Fatal("nested deployment cutover failed")
			}
			var after time.Time
			err := client.DB.QueryRowContext(ctx, read, id).Scan(&after)
			if found {
				if err != nil || !after.Equal(before) {
					t.Fatalf("prior lifecycle timestamp changed: before=%v after=%v error=%v", before, after, err)
				}
			} else if !errors.Is(err, sql.ErrNoRows) {
				t.Fatalf("fixture left lifecycle receipt after original absence: after=%v error=%v", after, err)
			}
		})
	}
}
