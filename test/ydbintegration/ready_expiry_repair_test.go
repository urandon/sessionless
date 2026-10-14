//go:build ydbintegration

package ydbintegration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// These tests are for the existing remote integration gate only. Local author
// validation compiles this package; it must not execute TestMain or openStore.
type expiryRepairCI struct {
	store                                                  *ydbstore.Store
	db                                                     *sql.DB
	ctx                                                    context.Context
	request                                                ydbstore.ReadyExpiryRepairRequest
	sourceTable, sourceKey, indexTable, indexID, dueColumn string
	due                                                    time.Time
	run                                                    domain.Run
	attempt                                                domain.Attempt
	fence                                                  uint64
}

func newExpiryRepairCI(t *testing.T, kind string) *expiryRepairCI {
	t.Helper()
	store, client := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	tenant := domain.TenantID(uniqueID("tenant-repair"))
	// Register test-owned deletion before the first write. No shared marker reset,
	// new schema, public traffic, or external replay is part of this fixture.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		for _, table := range []string{"lease_expiry_v2", "dispatch_ready_v2", "quota_expiry_v2", "telegram_delivery_ready_v2", "lease_heads", "leases", "dispatch_outbox", "quota_reservations", "telegram_delivery_outbox", "telegram_deliveries_by_run", "run_explanation_heads_v1", "attempts", "runs_by_session", "run_idempotency", "runs", "session_deletions"} {
			if _, err := client.DB.ExecContext(cleanup, "DELETE FROM "+table+" WHERE tenant_id=$1", tenant); err != nil {
				t.Errorf("repair fixture cleanup %s: %v", table, err)
			}
		}
	})
	var databaseNow time.Time
	if err := client.DB.QueryRowContext(ctx, "SELECT CurrentUtcTimestamp()").Scan(&databaseNow); err != nil || databaseNow.IsZero() {
		t.Fatalf("repair database clock: %v", err)
	}
	// Current lease/Held quota are deliberately already due, but retention TTL
	// remains future. All source instants come from the SAME database clock.
	at := databaseNow.UTC().Add(-2 * time.Minute)
	ingress := ingressFixture(tenant, uniqueID("run-repair"), 7123, at)
	if kind == "telegram" {
		finished := at.Add(time.Second)
		ingress.Run.Status = domain.RunSucceeded
		ingress.Run.FinishedAt = &finished
		ingress.Run.UpdatedAt = finished
	}
	lease := domain.Lease{ID: domain.LeaseID(uniqueID("lease-repair")), TenantID: tenant, RunID: ingress.Run.ID, AttemptID: ingress.Attempt.ID, WorkerID: "repair-worker", FenceToken: 7, AcquiredAt: at, ExpiresAt: at.Add(time.Minute)}
	quota := domain.QuotaReservation{ID: domain.QuotaReservationID(uniqueID("quota-repair")), TenantID: tenant, RunID: ingress.Run.ID, SubscriptionConnectionID: ingress.Run.SubscriptionConnectionID, Status: domain.ReservationHeld, CapacityUnits: 1, HeldAt: at, ExpiresAt: at.Add(time.Minute), UpdatedAt: at}
	delivery := domain.TelegramDeliveryOutbox{ID: domain.TelegramDeliveryID(uniqueID("delivery-repair")), TenantID: tenant, RunID: ingress.Run.ID, Chat: domain.TelegramChatRef{TenantID: tenant, ChatID: 7788}, Text: "Private fixture Привет 🌍", Status: domain.DeliveryPending, IdempotencyKey: domain.IdempotencyKey(uniqueID("delivery-key")), CreatedAt: at, UpdatedAt: at}
	f := &expiryRepairCI{store: store, db: client.DB, ctx: ctx, run: ingress.Run, attempt: ingress.Attempt, fence: lease.FenceToken, request: ydbstore.ReadyExpiryRepairRequest{Mode: "plan", Kind: kind, TenantID: tenant, RunID: ingress.Run.ID, MaxSourceBytes: ydbstore.ReadyExpiryRepairMaxSourceBytes, MaxOutputBytes: ydbstore.ReadyExpiryRepairMaxOutputBytes}}
	switch kind {
	case "lease":
		f.sourceTable = "leases"
		f.sourceKey = "lease_id"
		f.indexTable = "lease_expiry_v2"
		f.indexID = "run_id"
		f.dueColumn = "expires_at"
		f.request.ID = string(lease.ID)
		f.due = lease.ExpiresAt
	case "dispatch":
		f.sourceTable = "dispatch_outbox"
		f.sourceKey = "dispatch_outbox_id"
		f.indexTable = "dispatch_ready_v2"
		f.indexID = "dispatch_outbox_id"
		f.dueColumn = "available_at"
		f.request.ID = string(ingress.Dispatch.ID)
		f.due = ingress.Dispatch.CreatedAt
	case "quota":
		f.sourceTable = "quota_reservations"
		f.sourceKey = "quota_reservation_id"
		f.indexTable = "quota_expiry_v2"
		f.indexID = "quota_reservation_id"
		f.dueColumn = "expires_at"
		f.request.ID = string(quota.ID)
		f.due = quota.ExpiresAt
	case "telegram":
		f.sourceTable = "telegram_delivery_outbox"
		f.sourceKey = "telegram_delivery_id"
		f.indexTable = "telegram_delivery_ready_v2"
		f.indexID = "telegram_delivery_id"
		f.dueColumn = "available_at"
		f.request.ID = string(delivery.ID)
		f.due = delivery.UpdatedAt
	default:
		t.Fatal("invalid fixture family")
	}
	if err := store.Transact(ctx, tenant, func(tx ports.StateTx) error {
		if err := tx.PutRun(ctx, ingress.Run); err != nil {
			return err
		}
		if err := tx.PutAttempt(ctx, ingress.Attempt); err != nil {
			return err
		}
		switch kind {
		case "lease":
			return tx.PutLease(ctx, lease)
		case "dispatch":
			return tx.PutDispatchOutbox(ctx, ingress.Dispatch)
		case "quota":
			return tx.PutQuotaReservation(ctx, quota)
		case "telegram":
			return tx.PutTelegramDeliveryOutbox(ctx, delivery)
		}
		return errors.New("unreachable family")
	}); err != nil {
		t.Fatal(err)
	}
	if kind == "lease" {
		if _, err := client.DB.ExecContext(ctx, `UPSERT INTO lease_heads (tenant_id,run_id,lease_id,attempt_id,worker_id,fence_token,expires_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, tenant, lease.RunID, lease.ID, lease.AttemptID, lease.WorkerID, lease.FenceToken, lease.ExpiresAt, at); err != nil {
			t.Fatal(err)
		}
	}
	f.removeIndex(t)
	return f
}

func (f *expiryRepairCI) indexLocator() string {
	if f.request.Kind == "lease" {
		return string(f.request.RunID)
	}
	return f.request.ID
}
func (f *expiryRepairCI) removeIndex(t *testing.T) {
	t.Helper()
	bucket, err := ydbpartition.BucketV1(f.indexLocator())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.db.ExecContext(f.ctx, "DELETE FROM "+f.indexTable+" WHERE shard_bucket=$1 AND "+f.dueColumn+"=$2 AND tenant_id=$3 AND "+f.indexID+"=$4", bucket, f.due, f.request.TenantID, f.indexLocator()); err != nil {
		t.Fatal(err)
	}
}
func (f *expiryRepairCI) requireIndex(t *testing.T, want int) {
	t.Helper()
	var count uint64
	if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+f.indexTable+" WHERE tenant_id=$1", f.request.TenantID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != uint64(want) {
		t.Fatalf("repair index count=%d want=%d", count, want)
	}
	if want == 0 {
		return
	}
	bucket, err := ydbpartition.BucketV1(f.indexLocator())
	if err != nil {
		t.Fatal(err)
	}
	var due time.Time
	var run string
	cols := f.dueColumn + ",run_id"
	if f.request.Kind == "lease" {
		cols += ",lease_id,fence_token"
	}
	if f.request.Kind == "dispatch" {
		cols += ",attempt_id"
	}
	if f.request.Kind == "quota" {
		cols += ",subscription_connection_id"
	}
	scan := []any{&due, &run}
	var association string
	var fence uint64
	if f.request.Kind != "telegram" {
		scan = append(scan, &association)
	}
	if f.request.Kind == "lease" {
		scan = append(scan, &fence)
	}
	if err = f.db.QueryRowContext(f.ctx, "SELECT "+cols+" FROM "+f.indexTable+" WHERE shard_bucket=$1 AND "+f.dueColumn+"=$2 AND tenant_id=$3 AND "+f.indexID+"=$4", bucket, f.due, f.request.TenantID, f.indexLocator()).Scan(scan...); err != nil {
		t.Fatal(err)
	}
	if !due.Equal(f.due) || run != string(f.run.ID) {
		t.Fatal("original due/run changed")
	}
	expected := map[string]string{"lease": f.request.ID, "dispatch": string(f.attempt.ID), "quota": string(f.run.SubscriptionConnectionID)}[f.request.Kind]
	if f.request.Kind != "telegram" && association != expected {
		t.Fatal("original association changed")
	}
	if f.request.Kind == "lease" && fence != f.fence {
		t.Fatal("original fence changed")
	}
}

func (f *expiryRepairCI) sourceSnapshot(t *testing.T) []string {
	t.Helper()
	result := []string{}
	for _, q := range []struct {
		table, key, id string
		column         string
	}{{"runs", "run_id", string(f.run.ID), "payload"}, {"attempts", "attempt_id", string(f.attempt.ID), "payload"}, {f.sourceTable, f.sourceKey, f.request.ID, "payload"}, {"run_explanation_heads_v1", "run_id", string(f.run.ID), "record"}} {
		var raw string
		err := f.db.QueryRowContext(f.ctx, "SELECT SUBSTRING(CAST("+q.column+" AS String),0,131073) FROM "+q.table+" WHERE tenant_id=$1 AND "+q.key+"=$2", f.request.TenantID, q.id).Scan(&raw)
		if errors.Is(err, sql.ErrNoRows) {
			result = append(result, "ABSENT")
			continue
		}
		if err != nil || len(raw) > 131072 {
			t.Fatalf("snapshot %s: %v", q.table, err)
		}
		result = append(result, raw)
	}
	// Actual head, acquiredAt and retention TTLs are immutable too, not merely
	// decoded JSON equality. Every query is a fixed test-owned point locator.
	if f.request.Kind == "lease" {
		var id, attempt, worker string
		var fence uint64
		var expiry, updated, acquired, retention time.Time
		if err := f.db.QueryRowContext(f.ctx, "SELECT lease_id,attempt_id,worker_id,fence_token,expires_at,updated_at FROM lease_heads WHERE tenant_id=$1 AND run_id=$2", f.request.TenantID, f.run.ID).Scan(&id, &attempt, &worker, &fence, &expiry, &updated); err != nil {
			t.Fatal(err)
		}
		if err := f.db.QueryRowContext(f.ctx, "SELECT acquired_at,retention_expire_at FROM leases WHERE tenant_id=$1 AND lease_id=$2", f.request.TenantID, f.request.ID).Scan(&acquired, &retention); err != nil {
			t.Fatal(err)
		}
		result = append(result, fmt.Sprint(id, attempt, worker, fence, expiry, updated, acquired, retention))
	} else {
		var retention time.Time
		if err := f.db.QueryRowContext(f.ctx, "SELECT retention_expire_at FROM "+f.sourceTable+" WHERE tenant_id=$1 AND "+f.sourceKey+"=$2", f.request.TenantID, f.request.ID).Scan(&retention); err != nil {
			t.Fatal(err)
		}
		result = append(result, retention.String())
	}
	return result
}

func TestReadyExpiryRepairYDBAllFamiliesPlanApplyRepeat(t *testing.T) {
	for _, kind := range []string{"lease", "dispatch", "quota", "telegram"} {
		t.Run(kind, func(t *testing.T) {
			f := newExpiryRepairCI(t, kind)
			before := f.sourceSnapshot(t)
			for _, mode := range []string{"plan", "apply", "apply"} {
				f.request.Mode = mode
				out, err := f.store.RepairReadyExpiryV2(f.ctx, f.request)
				if err != nil {
					t.Fatal(err)
				}
				var metadata struct {
					Outcome string    `json:"outcome"`
					DueAt   time.Time `json:"due_at"`
				}
				if len(out) > f.request.MaxOutputBytes || json.Unmarshal(out, &metadata) != nil || !metadata.DueAt.Equal(f.due) {
					t.Fatal("invalid bounded repair metadata")
				}
				want := 1
				if mode == "plan" {
					want = 0
				}
				f.requireIndex(t, want)
				expected := "applied"
				if mode == "plan" {
					expected = "planned"
				}
				if metadata.Outcome != expected {
					t.Fatal("repair outcome")
				}
				if !reflect.DeepEqual(before, f.sourceSnapshot(t)) {
					t.Fatal("repair changed canonical payload/head/acquiredAt/retention")
				}
			}
		})
	}
}

func TestReadyExpiryRepairYDBSourceCellsAndDeletionDeny(t *testing.T) {
	for _, kind := range []string{"lease", "dispatch", "quota", "telegram"} {
		for _, bad := range []string{"deletion", "physical-trigger", "physical-source", "missing", "null", "sentinel", "foreign", "tiny-output"} {
			t.Run(kind+"/"+bad, func(t *testing.T) {
				f := newExpiryRepairCI(t, kind)
				f.request.Mode = "apply"
				exec := func(q string, args ...any) {
					t.Helper()
					if _, err := f.db.ExecContext(f.ctx, q, args...); err != nil {
						t.Fatal(err)
					}
				}
				switch bad {
				case "deletion":
					marker := domain.SessionDeletion{TenantID: f.run.TenantID, SessionID: f.run.SessionID, State: domain.SessionDeletionRequested, RequestedAt: f.run.UpdatedAt}
					raw, err := json.Marshal(marker)
					if err != nil {
						t.Fatal(err)
					}
					exec("UPSERT INTO session_deletions (tenant_id,session_id,state,requested_at,record) VALUES ($1,$2,$3,$4,CAST($5 AS JsonDocument))", marker.TenantID, marker.SessionID, marker.State, marker.RequestedAt, string(raw))
				case "physical-trigger":
					exec("UPDATE runs SET trigger_event_id=$1 WHERE tenant_id=$2 AND run_id=$3", "foreign-trigger", f.request.TenantID, f.run.ID)
				case "physical-source":
					exec("UPDATE "+f.sourceTable+" SET run_id=$1 WHERE tenant_id=$2 AND "+f.sourceKey+"=$3", "foreign-run", f.request.TenantID, f.request.ID)
				case "missing":
					exec("DELETE FROM "+f.sourceTable+" WHERE tenant_id=$1 AND "+f.sourceKey+"=$2", f.request.TenantID, f.request.ID)
				case "null":
					exec("UPDATE "+f.sourceTable+" SET payload=NULL WHERE tenant_id=$1 AND "+f.sourceKey+"=$2", f.request.TenantID, f.request.ID)
				case "sentinel":
					exec("UPDATE "+f.sourceTable+" SET payload=CAST($1 AS JsonDocument) WHERE tenant_id=$2 AND "+f.sourceKey+"=$3", `{"large":"`+strings.Repeat("x", ydbstore.ReadyExpiryRepairMaxSourceBytes)+`"}`, f.request.TenantID, f.request.ID)
				case "foreign":
					f.request.TenantID = domain.TenantID(uniqueID("foreign-repair"))
				case "tiny-output":
					f.request.MaxOutputBytes = 1
				}
				out, err := f.store.RepairReadyExpiryV2(f.ctx, f.request)
				if err == nil || len(out) != 0 {
					t.Fatal("invalid source produced repair")
				}
				// The request may be foreign, but assertion remains on original OWNED tenant.
				f.request.TenantID = f.run.TenantID
				f.requireIndex(t, 0)
			})
		}
	}
}
