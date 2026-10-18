package ydbstore

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Actual migrated disposable YDB, exact production payload-only UPDATE and
// bounded Run codec. Seeded metadata is DATA, not generation/current authority,
// source closure, head publication or physical PUT evidence. CI explicitly sets
// the same opt-ins as nativeOwnedCurrentFixture; no local native run is required.
func TestLocalRetainedCompactionYDBPayloadOnlyUpdate(t *testing.T) {
	f, ctx := nativeOwnedCurrentFixture(t)
	at := f.driver.at
	started, cancelled := at.Add(-time.Second), at.Add(-time.Millisecond)
	run := domain.Run{TenantID: f.scope.tenant, SessionID: f.scope.session, ID: f.scope.run, TriggerEventID: "event-retained-sql", SubscriptionConnectionID: "subscription-retained-sql", IdempotencyKey: "idempotency-retained-sql", Status: domain.RunRunning, CreatedAt: at.Add(-2 * time.Second), UpdatedAt: at, StartedAt: &started, CancellationRequestedAt: &cancelled}
	metadata := runPayloadMetadata(t, run)
	second := metadata.Inventory[1]
	metadata.Inventory = metadata.Inventory[:1]
	digest, err := metadata.Inventory[0].Digest()
	if err != nil {
		t.Fatal(err)
	}
	metadata.Head = &domain.DerivedContextHeadV1{Version: 1, PlanDigest: digest}
	run.DerivedContext = metadata
	payload, err := marshalRunPayload(run)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := f.store.db.ExecContext(cleanup, `DELETE FROM runs WHERE tenant_id=$1 AND run_id=$2`, run.TenantID, run.ID); err != nil {
			t.Error(err)
		}
	})
	if _, err := f.store.db.ExecContext(ctx, `UPSERT INTO runs (tenant_id,run_id,session_id,trigger_event_id,subscription_connection_id,status,created_at,updated_at,payload) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS JsonDocument))`, run.TenantID, run.ID, run.SessionID, run.TriggerEventID, run.SubscriptionConnectionID, run.Status, run.CreatedAt, run.UpdatedAt, payload); err != nil {
		t.Fatal(err)
	}
	write := func(rollback bool) error {
		return f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
			current, found, err := readRunPayload(ctx, q, run.TenantID, run.ID)
			if err != nil || !found || !reflect.DeepEqual(current, run) {
				return errors.New("native bounded original Run mismatch")
			}
			current.DerivedContext.Inventory = append(current.DerivedContext.Inventory, second)
			if err := current.Validate(); err != nil {
				return err
			}
			encoded, err := marshalRunPayload(current)
			if err != nil {
				return err
			}
			if _, err := q.ExecContext(ctx, localRetainedRunUpdateSQL, encoded, current.TenantID, current.ID); err != nil {
				return err
			}
			if rollback {
				return errors.New("owned test requested rollback")
			}
			return nil
		})
	}
	if err := write(true); err == nil {
		t.Fatal("native deliberate rollback succeeded")
	}
	rolled, found, err := readRunPayload(ctx, f.store.db, run.TenantID, run.ID)
	if err != nil || !found || !reflect.DeepEqual(rolled, run) {
		t.Fatalf("native rollback changed original Run: found=%t err=%v", found, err)
	}
	if err := write(false); err != nil {
		t.Fatalf("exact production JsonDocument UPDATE: %v", err)
	}
	want := run.Clone()
	want.DerivedContext.Inventory = append(want.DerivedContext.Inventory, second)
	actual, found, err := readRunPayload(ctx, f.store.db, run.TenantID, run.ID)
	if err != nil || !found || !reflect.DeepEqual(actual, want) {
		t.Fatalf("native payload-only update lost base/old inventory/head: found=%t err=%v", found, err)
	}
	var status string
	var created, updated time.Time
	if err := f.store.db.QueryRowContext(ctx, `SELECT status,created_at,updated_at FROM runs WHERE tenant_id=$1 AND run_id=$2`, run.TenantID, run.ID).Scan(&status, &created, &updated); err != nil || status != string(run.Status) || !created.Equal(run.CreatedAt) || !updated.Equal(run.UpdatedAt) {
		t.Fatalf("native payload update mutated scalar status/times: %v", err)
	}
}
