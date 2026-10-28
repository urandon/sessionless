package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

// Drives the actual allocating callbacks through database/sql. This is an
// explicit finite SQL fault fixture, not native YDB isolation/syntax evidence.
type generationAllocationSQL struct {
	*generationSQLFixture
	aliases      map[string][]string
	written      map[string][]driver.Value
	exactScope   map[string]driver.Value
	staged       int
	committed    int
	denyHeadRead bool
	leaseReads   int
}

func (f *generationAllocationSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func cloneGenerationAllocationRows(rows map[string][]driver.Value) map[string][]driver.Value {
	result := make(map[string][]driver.Value, len(rows))
	for key, row := range rows {
		result[key] = append([]driver.Value(nil), row...)
	}
	return result
}
func (f *generationAllocationSQL) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	inner, err := f.generationSQLFixture.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	f.staged = 0
	return generationAllocationTx{f, inner, cloneGenerationAllocationRows(f.rows), cloneGenerationAllocationRows(f.written)}, nil
}

type generationAllocationTx struct {
	f             *generationAllocationSQL
	inner         driver.Tx
	rows, written map[string][]driver.Value
}

func (tx generationAllocationTx) Commit() error {
	if err := tx.inner.Commit(); err != nil {
		return err
	}
	tx.f.committed += tx.f.staged
	tx.f.staged = 0
	return nil
}
func (tx generationAllocationTx) Rollback() error {
	tx.f.rows, tx.f.written, tx.f.staged = tx.rows, tx.written, 0
	return tx.inner.Rollback()
}
func (f *generationAllocationSQL) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "attached_resource_generation_v1") && f.denyHeadRead {
		return nil, errors.New("historical replay unexpectedly consulted generation authority")
	}
	if strings.Contains(query, "FROM lease_heads") {
		f.leaseReads++
	}
	return f.generationSQLFixture.QueryContext(ctx, query, args)
}

type generationAllocationWrite struct {
	operation, table, columns string
	jsonColumn                string
}

// Columns, operation, argument count and every placeholder are checked, not a
// permissive "accept any UPSERT" driver. Production owns all state transitions.
var generationAllocationWrites = []generationAllocationWrite{
	{"UPSERT", "subscription_scheduler_slots", "tenant_id,subscription_connection_id,state,active_run_id,active_reservation_id,blocked_until,updated_at", ""},
	{"UPSERT", "runs", "tenant_id,run_id,session_id,trigger_event_id,subscription_connection_id,status,created_at,updated_at,payload", "payload"},
	{"UPSERT", "runs_by_session", "tenant_id,session_id,created_at,run_id,trigger_event_id,status", ""},
	{"UPSERT", "run_idempotency", "tenant_id,idempotency_key,run_id,created_at,expire_at", ""},
	{"UPSERT", "run_explanation_heads_v1", "tenant_id,run_id,revision,record", "record"},
	{"UPSERT", "quota_reservations", "tenant_id,quota_reservation_id,run_id,subscription_connection_id,status,capacity_units,held_at,expires_at,retention_expire_at,payload", "payload"},
	{"UPSERT", "quota_expiry_v2", "shard_bucket,expires_at,tenant_id,quota_reservation_id,run_id,subscription_connection_id", ""},
	{"UPSERT", "worker_jobs", "tenant_id,run_id,attempt_id,reservation_id,created_at,retention_expire_at,payload", "payload"},
	{"UPSERT", "leases", "tenant_id,lease_id,run_id,attempt_id,worker_id,fence_token,acquired_at,expires_at,retention_expire_at,payload", "payload"},
	{"UPSERT", "lease_expiry_v2", "shard_bucket,expires_at,tenant_id,run_id,lease_id,fence_token", ""},
	{"UPSERT", "lease_heads", "tenant_id,run_id,lease_id,attempt_id,worker_id,fence_token,expires_at,updated_at", ""},
	{"UPSERT", "attached_worker_connections", "tenant_id,owner_user_id,worker_id,connection_id,activation_challenge_id,enrollment_generation,connection_generation,protocol_version,capability_digest,channel_binding_digest,secret_digest,manifest_revision,manifest_identity_key_digest,manifest_signature,manifest_observed_at,state,platform_sequence,worker_sequence,platform_ack,worker_ack,connected_at,last_checkpoint_at,presence_expires_at,auth_expires_at,revision,record", "record"},
	{"UPSERT", "attached_worker_presence_expiry_v1", "shard_bucket,presence_expires_at,tenant_id,owner_user_id,worker_id,connection_id,connection_generation,connection_revision", ""},
	{"UPSERT", "attached_worker_attempt_heads", "tenant_id,owner_user_id,worker_id,connection_id,attempt_id,run_id,lease_id,lease_generation,fence_token,enrollment_generation,connection_generation,state,lease_expires_at,cancel_deadline,updated_at,revision,retention_expire_at,payload", "payload"},
	{"INSERT", "attached_worker_attempt_messages", "tenant_id,owner_user_id,worker_id,attempt_id,direction,attempt_sequence,connection_generation,envelope_sequence,kind,fingerprint,created_at,retention_expire_at,payload", "payload"},
	{"UPSERT", "attached_worker_attempt_deadlines_v1", "shard_bucket,deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind,lease_generation,attempt_revision", ""},
	{"UPSERT", "audit_events", "tenant_id,occurred_at,audit_event_id,actor_id,action,subject_kind,subject_id,outcome,metadata,expire_at", "metadata"},
}

func generationCompactSQL(s string) string { return strings.Join(strings.Fields(s), "") }
func (f *generationAllocationSQL) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("allocation write has no deadline")
	}
	// The sole deletion in the fresh Offer path removes the prior presence
	// index; it must preserve the exact same tenant/owner/worker point key.
	if generationCompactSQL(query) == generationCompactSQL(`DELETE FROM attached_worker_presence_expiry_v1 WHERE shard_bucket=$1 AND presence_expires_at=$2 AND tenant_id=$3 AND owner_user_id=$4 AND worker_id=$5`) {
		if len(args) != 5 || args[2].Value != f.scope("tenant_id") || args[3].Value != f.scope("owner_user_id") || args[4].Value != f.scope("worker_id") {
			return nil, errors.New("foreign presence delete")
		}
		f.writes++
		f.staged++
		return driver.RowsAffected(1), nil
	}
	for _, spec := range generationAllocationWrites {
		columns := strings.Split(spec.columns, ",")
		values := make([]string, len(columns))
		for i, column := range columns {
			values[i] = fmt.Sprintf("$%d", i+1)
			if column == spec.jsonColumn {
				values[i] = "CAST(" + values[i] + " AS JsonDocument)"
			}
		}
		expected := spec.operation + " INTO " + spec.table + " (" + spec.columns + ") VALUES (" + strings.Join(values, ",") + ")"
		if generationCompactSQL(query) != generationCompactSQL(expected) {
			continue
		}
		if len(args) != len(columns) {
			return nil, fmt.Errorf("%s arguments=%d want=%d", spec.table, len(args), len(columns))
		}
		row := make([]driver.Value, len(args))
		for i, column := range columns {
			row[i] = args[i].Value
			if want := f.scope(column); want != nil && row[i] != want {
				return nil, fmt.Errorf("foreign %s.%s got=%v want=%v", spec.table, column, row[i], want)
			}
		}
		f.written[spec.table] = row
		if spec.jsonColumn != "" && spec.jsonColumn != "metadata" {
			for i, column := range columns {
				if column == spec.jsonColumn {
					f.setTable(spec.table, []driver.Value{row[i]})
				}
			}
		}
		if spec.table == "subscription_scheduler_slots" {
			f.setTable(spec.table, append([]driver.Value(nil), row[2:]...))
		}
		if spec.table == "lease_heads" {
			f.setTable(spec.table, append([]driver.Value(nil), row[2:7]...))
		}
		f.writes++
		f.staged++
		return driver.RowsAffected(1), nil
	}
	return nil, fmt.Errorf("allocation fixture forbids SQL write: %s", query)
}
func (f *generationAllocationSQL) scope(column string) driver.Value {
	if value, ok := f.exactScope[column]; ok {
		return value
	}
	keys := f.keys[generationSQLKey(attachedGenerationResourceSQL)]
	identity := f.keys[generationSQLKey(`SELECT record FROM attached_workers WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3`)]
	switch column {
	case "tenant_id":
		return keys[0]
	case "subscription_connection_id":
		return keys[1]
	case "owner_user_id":
		return identity[1]
	case "worker_id":
		return identity[2]
	}
	return nil
}
func (f *generationAllocationSQL) setTable(table string, row []driver.Value) {
	for _, query := range f.aliases[table] {
		f.rows[query] = append([]driver.Value(nil), row...)
	}
}
func (f *generationAllocationSQL) add(table, query string, row []driver.Value, keys ...driver.Value) {
	key := generationSQLKey(query)
	f.aliases[table] = append(f.aliases[table], key)
	f.rows[key], f.keys[key] = append([]driver.Value(nil), row...), keys
}

type generationAllocationFixture struct {
	*generationStoreFixture
	db       *generationAllocationSQL
	dispatch ports.DispatchAdmissionRequest
	offer    ports.AttachedWorkerAttemptOffer
}

func newGenerationAllocationFixture(t *testing.T, freshScheduler bool) *generationAllocationFixture {
	t.Helper()
	g := newGenerationStoreFixture(t)
	binding, placement := generationAdmissionBinding(t, g)
	s := currentWorkerAuthorityFixture(t)
	s.Run.TenantID, s.Run.ID, s.Run.SubscriptionConnectionID, s.Run.Status = g.auth.TenantID, binding.RunID, g.auth.SubscriptionConnectionID, domain.RunQueued
	s.Run.CreatedAt, s.Run.UpdatedAt = g.at, g.at
	s.Attempt.TenantID, s.Attempt.RunID, s.Attempt.ID, s.Attempt.Status, s.Attempt.WorkerID = g.auth.TenantID, s.Run.ID, binding.AttemptID, domain.AttemptCreated, ""
	s.Attempt.CreatedAt, s.Attempt.UpdatedAt = g.at, g.at
	s.Job.TenantID, s.Job.RunID, s.Job.AttemptID, s.Job.CredentialOwnerUserID = g.auth.TenantID, s.Run.ID, s.Attempt.ID, g.auth.OwnerUserID
	s.Job.HarnessBinding, s.Job.ExecutionPlacementV2, s.Job.SubstrateBinding, s.Job.AdmissionCostCeiling = binding, placement, nil, nil
	s.Job.CreatedAt, s.Job.ContextSnapshot.TenantID, s.Job.DeliveryChat.TenantID = g.at, g.auth.TenantID, g.auth.TenantID
	s.Job.ContextSnapshot.Key = domain.SessionObjectPrefix(g.auth.TenantID, s.Run.SessionID) + "context"
	s.Job.Limits.MaxContextEvents, s.Job.Limits.MaxToolEvents, s.Job.Limits.MaxToolEventBytes = 16, 8, 4096
	s.Reservation.TenantID, s.Reservation.RunID, s.Reservation.SubscriptionConnectionID = g.auth.TenantID, s.Run.ID, g.auth.SubscriptionConnectionID
	s.Reservation.HeldAt, s.Reservation.UpdatedAt, s.Reservation.ExpiresAt = g.at, g.at, g.at.Add(time.Hour)
	s.InputManifest.TenantID, s.InputManifest.RunID, s.InputManifest.CreatedAt = g.auth.TenantID, s.Run.ID, g.at
	if err := validateLoadedWorkerJob(s); err != nil {
		t.Fatalf("complete generation allocation fixture: %v", err)
	}
	leaseID, err := domain.NewAttachedWorkerLeaseIDV1(g.auth.TenantID, s.Run.ID, s.Attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	ttl, err := domain.AttachedWorkerLeaseTTLForLimitsV1(s.Job.Limits)
	if err != nil {
		t.Fatal(err)
	}
	f := &generationAllocationFixture{generationStoreFixture: g, db: &generationAllocationSQL{generationSQLFixture: g.sql, aliases: make(map[string][]string), written: make(map[string][]driver.Value), exactScope: map[string]driver.Value{
		"run_id": string(s.Run.ID), "attempt_id": string(s.Attempt.ID), "session_id": string(s.Run.SessionID),
		"trigger_event_id": string(s.Run.TriggerEventID), "reservation_id": string(s.Reservation.ID), "quota_reservation_id": string(s.Reservation.ID),
		"lease_id": string(leaseID), "connection_id": string(g.auth.ConnectionID), "idempotency_key": string(s.Run.IdempotencyKey),
	}}}
	f.offer = ports.AttachedWorkerAttemptOffer{TenantID: g.auth.TenantID, OwnerUserID: g.auth.OwnerUserID, WorkerID: g.auth.WorkerID, RunID: s.Run.ID, AttemptID: s.Attempt.ID, ReservationID: s.Reservation.ID, LeaseID: leaseID, LeaseTTL: ttl}
	f.dispatch = ports.DispatchAdmissionRequest{TenantID: g.auth.TenantID, OutboxID: "generation-outbox", RunID: s.Run.ID, AttemptID: s.Attempt.ID, ReservationID: s.Reservation.ID, Now: g.at, HoldUntil: s.Reservation.ExpiresAt, Limits: s.Job.Limits, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
	q := f.db
	q.aliases["attached_worker_connections"] = []string{generationSQLKey(`SELECT record FROM attached_worker_connections WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`)}
	put := func(table, query string, value any, keys ...driver.Value) {
		q.add(table, query, []driver.Value{generationJSON(t, value)}, keys...)
	}
	put("runs", boundedRunPayloadSQL, s.Run, string(g.auth.TenantID), string(s.Run.ID))
	put("attempts", `SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2`, s.Attempt, string(g.auth.TenantID), string(s.Attempt.ID))
	put("worker_jobs", `SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, s.Job, string(g.auth.TenantID), string(s.Run.ID))
	for _, query := range []string{`SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2`, `SELECT payload FROM quota_reservations WHERE tenant_id = $1 AND quota_reservation_id = $2`} {
		put("quota_reservations", query, s.Reservation, string(g.auth.TenantID), string(s.Reservation.ID))
	}
	for _, query := range []string{`SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2`, `SELECT payload FROM artifact_manifests WHERE tenant_id = $1 AND artifact_manifest_id = $2`} {
		put("artifact_manifests", query, s.InputManifest, string(g.auth.TenantID), string(s.InputManifest.ID))
	}
	q.add("subscription", `SELECT entitlement_state, quota_state FROM subscription_connections WHERE tenant_id = $1 AND subscription_connection_id = $2`, []driver.Value{"active", "available"}, string(g.auth.TenantID), string(g.auth.SubscriptionConnectionID))
	q.add("subscription_scheduler_slots", `SELECT state, active_run_id, active_reservation_id, blocked_until, updated_at FROM subscription_scheduler_slots WHERE tenant_id = $1 AND subscription_connection_id = $2`, []driver.Value{string(domain.SchedulerReady), "", "", time.Unix(0, 0).UTC(), g.at}, string(g.auth.TenantID), string(g.auth.SubscriptionConnectionID))
	q.add("tenant_scheduler_counters", `SELECT queue_depth, active_runs FROM tenant_scheduler_counters WHERE tenant_id = $1`, []driver.Value{int64(0), int64(0)}, string(g.auth.TenantID))
	q.add("attached_worker_attempt_heads", `SELECT payload FROM attached_worker_attempt_heads WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`, nil, string(g.auth.TenantID), string(g.auth.OwnerUserID), string(g.auth.WorkerID))
	q.add("lease_heads", `SELECT lease_id,attempt_id,worker_id,fence_token,expires_at FROM lease_heads WHERE tenant_id=$1 AND run_id=$2`, nil, string(g.auth.TenantID), string(s.Run.ID))
	q.add("leases", `SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2`, nil, string(g.auth.TenantID), string(leaseID))
	q.add("attached_worker_attempt_messages", `SELECT payload FROM attached_worker_attempt_messages WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND attempt_id=$4 AND direction=$5 AND attempt_sequence=$6`, nil, string(g.auth.TenantID), string(g.auth.OwnerUserID), string(g.auth.WorkerID), string(s.Attempt.ID), string(domain.AttachedWorkerAttemptPlatformToWorker), uint64(1))
	q.add("session_deletions", `SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`, nil, string(g.auth.TenantID), string(s.Run.SessionID))
	q.add("run_idempotency", `SELECT run_id FROM run_idempotency WHERE tenant_id = $1 AND idempotency_key = $2`, []driver.Value{string(s.Run.ID)}, string(g.auth.TenantID), string(s.Run.IdempotencyKey))
	q.add("run_explanation_heads_v1", `SELECT SUBSTRING(CAST(record AS String), 0, 8193) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, nil, string(g.auth.TenantID), string(s.Run.ID))
	outbox := domain.DispatchOutbox{ID: f.dispatch.OutboxID, TenantID: g.auth.TenantID, RunID: s.Run.ID, AttemptID: s.Attempt.ID, InputManifestID: s.InputManifest.ID, ContextSnapshot: s.Job.ContextSnapshot, CredentialOwnerUserID: g.auth.OwnerUserID, ExecutionPlacementV2: placement, HarnessBinding: binding, DeliveryChat: s.Job.DeliveryChat, ReplyToMessageID: s.Job.ReplyToMessageID, Status: domain.DispatchPending, IdempotencyKey: "generation-dispatch", CreatedAt: g.at, UpdatedAt: g.at}
	if err := outbox.ValidateForAttempt(s.Run, s.Attempt); err != nil {
		t.Fatalf("complete generation dispatch fixture: %v", err)
	}
	put("dispatch_outbox", `SELECT payload FROM dispatch_outbox WHERE tenant_id = $1 AND dispatch_outbox_id = $2`, outbox, string(g.auth.TenantID), string(outbox.ID))
	if freshScheduler {
		s.Run.Status = domain.RunCreated
		q.setTable("runs", []driver.Value{generationJSON(t, s.Run)})
		q.setTable("worker_jobs", nil)
		q.setTable("quota_reservations", nil)
	}
	db := sql.OpenDB(q)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	g.store.db, g.store.operationalRetention, g.store.idempotencyRetention = db, time.Hour, time.Hour
	return f
}

func (f *generationAllocationFixture) transact(t *testing.T, callback func(context.Context, *stateTx) error) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tx, err := f.store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := callback(ctx, &stateTx{store: f.store, sqlTx: tx, tenantID: f.auth.TenantID}); err != nil {
		return err
	}
	return tx.Commit()
}
func (f *generationAllocationFixture) admit(t *testing.T) (ports.DispatchAdmissionResult, error) {
	t.Helper()
	var result ports.DispatchAdmissionResult
	err := f.transact(t, func(ctx context.Context, tx *stateTx) error {
		return f.store.admitDispatchTx(ctx, tx, f.dispatch, &result)
	})
	return result, err
}
func (f *generationAllocationFixture) offerAttempt(t *testing.T) (ports.AttachedWorkerAttemptResult, error) {
	t.Helper()
	var result ports.AttachedWorkerAttemptResult
	err := f.transact(t, func(ctx context.Context, tx *stateTx) error {
		return f.store.offerAttachedWorkerAttemptTx(ctx, tx, f.offer, &result)
	})
	return result, err
}

func TestAttachedGenerationActualSchedulerRejectsStaleHeadWithoutCommittedAllocation(t *testing.T) {
	for _, cold := range []bool{false, true} {
		for _, revoked := range []bool{false, true} {
			t.Run(fmt.Sprintf("cold=%t/revoked=%t", cold, revoked), func(t *testing.T) {
				f := newGenerationAllocationFixture(t, true)
				if cold {
					f.db.setTable("subscription_scheduler_slots", nil)
				}
				f.db.rows[generationSQLKey(attachedGenerationResourceSQL)][3] = generationJSON(t, f.head(5, revoked))
				before := cloneGenerationAllocationRows(f.db.rows)
				result, err := f.admit(t)
				if !errors.Is(err, domain.ErrMembershipDenied) || result.Admitted || f.db.commits != 0 || f.db.rollbacks != 1 || f.db.committed != 0 || len(f.db.written) != 0 || !reflect.DeepEqual(before, f.db.rows) {
					t.Fatalf("stale admission result=%+v err=%v begin/commit/rollback=%d/%d/%d committed=%d writes=%v", result, err, f.db.begins, f.db.commits, f.db.rollbacks, f.db.committed, f.db.written)
				}
				if cold && f.db.writes != 1 || !cold && f.db.writes != 0 {
					t.Fatalf("provisional slot write count=%d cold=%t", f.db.writes, cold)
				}
			})
		}
	}
}

func TestAttachedGenerationActualDirectOfferRejectsStaleBeforeLeaseAllocation(t *testing.T) {
	for _, revoked := range []bool{false, true} {
		t.Run(fmt.Sprint(revoked), func(t *testing.T) {
			f := newGenerationAllocationFixture(t, false)
			f.db.rows[generationSQLKey(attachedGenerationResourceSQL)][3] = generationJSON(t, f.head(5, revoked))
			before := cloneGenerationAllocationRows(f.db.rows)
			result, err := f.offerAttempt(t)
			if !errors.Is(err, domain.ErrMembershipDenied) || result.Status == ports.AttachedWorkerExecutionApplied || f.db.leaseReads != 0 || f.db.writes != 0 || f.db.commits != 0 || f.db.rollbacks != 1 || !reflect.DeepEqual(before, f.db.rows) {
				t.Fatalf("stale offer result=%+v err=%v leaseReads=%d writes=%d commits=%d rollbacks=%d", result, err, f.db.leaseReads, f.db.writes, f.db.commits, f.db.rollbacks)
			}
		})
	}
}

func TestAttachedGenerationActualSchedulerAndDirectOfferPositiveThenHistoricalReplay(t *testing.T) {
	for _, scheduler := range []bool{false, true} {
		t.Run(fmt.Sprintf("scheduler=%t", scheduler), func(t *testing.T) {
			f := newGenerationAllocationFixture(t, scheduler)
			if scheduler {
				result, err := f.admit(t)
				if err != nil || !result.Admitted || result.Code != "admitted" {
					t.Fatalf("exact-N scheduler: result=%+v err=%v", result, err)
				}
			} else {
				result, err := f.offerAttempt(t)
				if err != nil || result.Status != ports.AttachedWorkerExecutionApplied {
					t.Fatalf("exact-N Offer: result=%+v err=%v", result, err)
				}
			}
			if f.db.commits != 1 || f.db.committed == 0 || f.db.leaseReads != 1 {
				t.Fatalf("positive path did not allocate: commits=%d committed=%d leaseReads=%d", f.db.commits, f.db.committed, f.db.leaseReads)
			}
			for _, table := range []string{"leases", "lease_heads", "attached_worker_connections", "attached_worker_attempt_heads", "attached_worker_attempt_messages", "attached_worker_attempt_deadlines_v1"} {
				if len(f.db.written[table]) == 0 {
					t.Fatalf("positive allocation missing %s", table)
				}
			}
			var original domain.AttachedWorkerAttemptV1
			if err := json.Unmarshal([]byte(f.db.rows[f.db.aliases["attached_worker_attempt_heads"][0]][0].(string)), &original); err != nil {
				t.Fatal(err)
			}
			var originalMessage domain.AttachedWorkerAttemptMessageV1
			if err := json.Unmarshal([]byte(f.db.rows[f.db.aliases["attached_worker_attempt_messages"][0]][0].(string)), &originalMessage); err != nil {
				t.Fatal(err)
			}
			f.db.rows[generationSQLKey(attachedGenerationResourceSQL)][3] = generationJSON(t, f.head(5, true))
			before, writes, committed := cloneGenerationAllocationRows(f.db.rows), f.db.writes, f.db.committed
			f.db.denyHeadRead = true
			if scheduler {
				result, err := f.admit(t)
				if err != nil || !result.Admitted || result.Code != "already_admitted" {
					t.Fatalf("historical scheduler result=%+v err=%v", result, err)
				}
			} else {
				result, err := f.offerAttempt(t)
				if err != nil || result.Status != ports.AttachedWorkerExecutionReplayed || !reflect.DeepEqual(result.Attempt, original) || result.Outbound == nil || !reflect.DeepEqual(*result.Outbound, originalMessage) {
					t.Fatalf("historical Offer result=%+v err=%v", result, err)
				}
			}
			if f.db.commits != 2 || f.db.writes != writes || f.db.committed != committed || !reflect.DeepEqual(before, f.db.rows) {
				t.Fatalf("historical replay mutated authority: commits=%d writes=%d/%d committed=%d/%d", f.db.commits, f.db.writes, writes, f.db.committed, committed)
			}
		})
	}
}

func TestAttachedGenerationActualLegacyCredentiallessResourceKeepsSubscriptionFence(t *testing.T) {
	for _, scheduler := range []bool{false, true} {
		for _, mode := range []string{"legacy NULL head", "published without registry", "configured resource"} {
			t.Run(fmt.Sprintf("scheduler=%t/%s", scheduler, mode), func(t *testing.T) {
				f := newGenerationAllocationFixture(t, scheduler)
				// The same canonical credentialless binding used by AW07 Offer
				// has its own provider resource, distinct from the Run's capacity
				// subscription. Neither identifier may be rewritten to hide that.
				authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(f.auth.TenantID,
					f.auth.OwnerUserID, f.offer.RunID, f.offer.AttemptID, f.auth.SubscriptionConnectionID, f.at)
				if err != nil {
					t.Fatal(err)
				}
				binding := authority.HarnessBinding
				if binding.Resource.Kind != domain.ProviderResourceCredentiallessFixtureV1 || binding.Resource.ResourceID == string(f.auth.SubscriptionConnectionID) {
					t.Fatal("fixture no longer represents independent legacy resource identity")
				}
				var outbox domain.DispatchOutbox
				if err := json.Unmarshal([]byte(f.db.rows[f.db.aliases["dispatch_outbox"][0]][0].(string)), &outbox); err != nil {
					t.Fatal(err)
				}
				placement := outbox.ExecutionPlacementV2
				placement.PolicyDigest = domain.AttachedWorkerPolicyDigest(binding.EffectivePolicyDigest)
				digest, err := domain.ExecutionPlacementDigest(placement)
				if err != nil {
					t.Fatal(err)
				}
				binding.ExecutionPlacementDigest = string(digest)
				if err := binding.ValidateForScope(f.auth.TenantID, f.auth.OwnerUserID, f.offer.RunID, f.offer.AttemptID, placement); err != nil {
					t.Fatal("canonical legacy binding rejected", err)
				}
				outbox.HarnessBinding, outbox.ExecutionPlacementV2 = binding, placement
				f.db.setTable("dispatch_outbox", []driver.Value{generationJSON(t, outbox)})
				if !scheduler {
					var job domain.WorkerJob
					if err := json.Unmarshal([]byte(f.db.rows[f.db.aliases["worker_jobs"][0]][0].(string)), &job); err != nil {
						t.Fatal(err)
					}
					job.HarnessBinding, job.ExecutionPlacementV2 = binding, placement
					f.db.setTable("worker_jobs", []driver.Value{generationJSON(t, job)})
				}
				var persisted driver.Value
				if mode == "published without registry" {
					persisted = generationJSON(t, f.head(4, false))
				}
				f.db.add("generation_presence", attachedGenerationPresenceSQL, []driver.Value{persisted}, string(f.auth.TenantID), string(f.auth.SubscriptionConnectionID))
				if mode != "configured resource" {
					f.store.attachedResourceGenerations = nil
				}
				if scheduler {
					result, callErr := f.admit(t)
					err = callErr
					if mode == "legacy NULL head" && (err != nil || !result.Admitted || result.Code != "admitted") {
						t.Fatalf("legacy scheduler changed: result=%+v err=%v", result, err)
					}
				} else {
					result, callErr := f.offerAttempt(t)
					err = callErr
					if mode == "legacy NULL head" && (err != nil || result.Status != ports.AttachedWorkerExecutionApplied) {
						t.Fatalf("legacy Offer changed: result=%+v err=%v", result, err)
					}
				}
				if mode == "legacy NULL head" {
					if f.db.commits != 1 || f.db.committed == 0 || f.db.leaseReads != 1 {
						t.Fatal("legacy positive did not exercise actual allocation")
					}
					var original domain.WorkerJob
					if err := json.Unmarshal([]byte(f.db.rows[f.db.aliases["worker_jobs"][0]][0].(string)), &original); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(original.HarnessBinding, binding) || original.ExecutionPlacementV2 != placement {
						t.Fatal("legacy allocation rewrote canonical binding/placement")
					}
				} else if !errors.Is(err, domain.ErrMembershipDenied) || f.db.commits != 0 || f.db.committed != 0 || f.db.writes != 0 || f.db.rollbacks != 1 {
					t.Fatalf("resource-kind substitution bypassed generation authority: err=%v commits=%d committed=%d writes=%d rollbacks=%d", err, f.db.commits, f.db.committed, f.db.writes, f.db.rollbacks)
				}
				reads := 0
				for _, query := range f.db.queries {
					if query == generationSQLKey(attachedGenerationPresenceSQL) {
						reads++
					}
				}
				wantReads := 1
				if scheduler && mode == "legacy NULL head" {
					// Successful scheduler admission reaches the nested direct
					// Offer too; BOTH fresh gates use this same transaction.
					wantReads = 2
				}
				if mode == "configured resource" {
					wantReads = 0 // The full current-authority/projector path applies.
				}
				if reads != wantReads {
					t.Fatalf("legacy resource escaped original subscription head fence: reads=%d want=%d", reads, wantReads)
				}
			})
		}
	}
}
