package ydbstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestOwnedCanonicalExecutorYDBWritesRollbackAndBoundedRead(t *testing.T) {
	f, ctx := nativeOwnedCurrentFixture(t)
	store := f.store
	store.idempotencyRetention = time.Hour
	run := domain.Run{TenantID: f.scope.tenant, SessionID: f.scope.session, ID: f.scope.run, TriggerEventID: "event-native", SubscriptionConnectionID: "subscription-native", IdempotencyKey: "idempotency-native", Status: domain.RunCreated, CreatedAt: f.driver.at, UpdatedAt: f.driver.at}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, statement := range []string{"DELETE FROM runs WHERE tenant_id=$1 AND run_id=$2", "DELETE FROM runs_by_session WHERE tenant_id=$1 AND run_id=$2", "DELETE FROM run_idempotency WHERE tenant_id=$1 AND run_id=$2"} {
			if _, err := store.db.ExecContext(cleanup, statement, run.TenantID, run.ID); err != nil {
				t.Error(err)
			}
		}
	})
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		if err := q.canonicalState(store, run.TenantID).PutRun(ctx, run); err != nil {
			return err
		}
		_ = q.budget.materialized(q.callback, ownedAuthorityMaxBytes+1)
		return nil // Actual speculative writes must roll back despite ignored error.
	})
	if !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("ignored exhaustion=%v", err)
	}
	for _, table := range []string{"runs", "runs_by_session", "run_idempotency"} {
		var count uint64
		if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE tenant_id=$1 AND run_id=$2", run.TenantID, run.ID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("rollback table=%s count=%d err=%v", table, count, err)
		}
	}
	var b *ownedAuthorityBudget
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		b = q.budget
		return q.canonicalState(store, run.TenantID).PutRun(ctx, run)
	})
	if err != nil || b.statements != 8 {
		t.Fatalf("native canonical PutRun err=%v statements=%d", err, b.statements)
	}
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		got, found, err := q.canonicalState(store, run.TenantID).GetRun(ctx, run.ID)
		if err != nil || !found || got.ID != run.ID || !got.CreatedAt.Equal(run.CreatedAt) {
			return errors.New("native canonical readback mismatch")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	oversized := strings.TrimSuffix(raw, "}") + `,"test_owned_padding":"` + strings.Repeat("x", ownedAuthorityHeadBytes) + `"}`
	if _, err := store.db.ExecContext(ctx, `UPDATE runs SET payload=CAST($1 AS JsonDocument) WHERE tenant_id=$2 AND run_id=$3`, oversized, run.TenantID, run.ID); err != nil {
		t.Fatal(err)
	}
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		b = q.budget
		_, _, err := q.canonicalState(store, run.TenantID).GetRun(ctx, run.ID)
		return err
	})
	if !errors.Is(err, domain.ErrMembershipDenied) || b.bytes != 8+ownedAuthorityHeadBytes+1 {
		t.Fatalf("SQL cap+1 err=%v bytes=%d", err, b.bytes)
	}
}

func TestOwnedCanonicalExecutorYDBSnapshotResultSet(t *testing.T) {
	f, ctx := nativeOwnedCurrentFixture(t)
	s := currentWorkerAuthorityFixture(t)
	s.Run.TenantID, s.Run.SessionID, s.Job.CreatedAt = f.scope.tenant, f.scope.session, f.driver.at
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := f.store.db.ExecContext(cleanup, `DELETE FROM session_snapshots WHERE tenant_id=$1 AND session_id=$2`, s.Run.TenantID, s.Run.SessionID); err != nil {
			t.Error(err)
		}
	})
	raws := []string{canonicalSnapshot(t, s, 3), canonicalSnapshot(t, s, 2)}
	put := func(version uint64, raw string) {
		t.Helper()
		if _, err := f.store.db.ExecContext(ctx, `UPSERT INTO session_snapshots (tenant_id,session_id,version,through_sequence,record) VALUES ($1,$2,$3,$4,CAST($5 AS JsonDocument))`, s.Run.TenantID, s.Run.SessionID, version, uint64(1), raw); err != nil {
			t.Fatal(err)
		}
	}
	put(3, raws[0])
	put(2, raws[1])
	var b *ownedAuthorityBudget
	err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		b = q.budget
		got, err := selectAdmittedContextWindow(ctx, q.canonicalState(f.store, s.Run.TenantID), s.Run.SessionID, &domain.SessionContextWindow{ThroughSequence: 1})
		if err != nil {
			return err
		}
		if got.SnapshotVersion == nil || *got.SnapshotVersion != 3 {
			return errors.New("native snapshot ordering lost")
		}
		return nil
	})
	// JsonDocument serialization may change whitespace/order. Count actual
	// materialized bytes, not the pre-storage Go JSON length.
	if err != nil || b.statements != 2 || b.bytes <= 8+uint64(len(raws[0])) {
		t.Fatalf("native snapshot err=%v statements=%d bytes=%d", err, b.statements, b.bytes)
	}
	put(2, strings.TrimSuffix(raws[1], "}")+`,"test_owned_padding":"`+strings.Repeat("x", admittedSnapshotMetadataBytes)+`"}`)
	err = f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		_, err := selectAdmittedContextWindow(ctx, q.canonicalState(f.store, s.Run.TenantID), s.Run.SessionID, &domain.SessionContextWindow{ThroughSequence: 1})
		return err
	})
	if !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("oversize row after valid first=%v", err)
	}
}
