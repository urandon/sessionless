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
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// Extend the real canonical/admission DATA peer only. No Lease, Run, Attempt,
// Job, source grant, or model result is fabricated for the successful path.
// This is adapter/SDK lifecycle evidence, not native isolation or an invocation.
type localWorkerLifecycleSQL struct {
	*localCanonicalJoinSQL
	leaseID                                                domain.LeaseID
	worker                                                 string
	phase                                                  string
	callbacks, callbackStatements, statements, phaseWrites int
	failTable                                              string
}

func (f *localWorkerLifecycleSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }

func (f *localWorkerLifecycleSQL) beginPhase(name string) {
	f.phase, f.callbacks, f.callbackStatements, f.statements, f.phaseWrites = name, 0, 0, 0, 0
}

func (f *localWorkerLifecycleSQL) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	f.callbacks++
	f.callbackStatements = 0
	if f.callbacks > 3 || options.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("lifecycle peer requires at most three serializable callbacks")
	}
	return f.localCanonicalJoinSQL.BeginTx(ctx, options)
}

func (f *localWorkerLifecycleSQL) observe(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("lifecycle peer requires a caller deadline")
	}
	f.statements++
	f.callbackStatements++
	if f.statements > 96 || f.callbackStatements > 32 {
		return fmt.Errorf("lifecycle peer SQL bound: phase=%s callback=%d statements=%d", f.phase, f.callbackStatements, f.statements)
	}
	return nil
}

func (f *localWorkerLifecycleSQL) exactArgs(args []driver.NamedValue, want ...driver.Value) bool {
	values := make([]driver.Value, len(args))
	for i := range args {
		values[i] = args[i].Value
	}
	return reflect.DeepEqual(values, want)
}

func (f *localWorkerLifecycleSQL) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if err := f.observe(ctx); err != nil {
		return nil, err
	}
	flat := canonicalJoinSQL(statement)
	tenant, run, attempt, session := string(f.request.TenantID), string(f.request.RunID), string(f.request.AttemptID), string(f.request.SessionID)
	row := func(values []driver.Value) (driver.Rows, error) {
		f.recordQuery(statement, args)
		return &explanationFixtureRows{values: append([]driver.Value(nil), values...)}, nil
	}
	var table string
	var key []driver.Value
	switch flat {
	case "SELECT lease_id, attempt_id, worker_id, fence_token, expires_at FROM lease_heads WHERE tenant_id = $1 AND run_id = $2":
		if !f.exactArgs(args, tenant, run) {
			return nil, errors.New("lifecycle claim foreign head key")
		}
		v := f.rows["lease_heads"]
		if len(v) == 0 {
			return row(nil)
		}
		return row([]driver.Value{v[2], v[3], v[4], v[5], v[6]})
	case "SELECT lease_id, fence_token, expires_at FROM lease_heads WHERE tenant_id = $1 AND run_id = $2":
		if !f.exactArgs(args, tenant, run) {
			return nil, errors.New("lifecycle start foreign head key")
		}
		v := f.rows["lease_heads"]
		if len(v) == 0 {
			return row(nil)
		}
		return row([]driver.Value{v[2], v[5], v[6]})
	case "SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2":
		table, key = "leases", []driver.Value{tenant, string(f.leaseID)}
	case "SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2", "SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM runs WHERE tenant_id = $1 AND run_id = $2":
		table, key = "runs", []driver.Value{tenant, run}
	case "SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2":
		table, key = "attempts", []driver.Value{tenant, attempt}
	case "SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2":
		table, key = "session_deletions", []driver.Value{tenant, session}
	case "SELECT run_id FROM run_idempotency WHERE tenant_id = $1 AND idempotency_key = $2":
		table, key = "run_idempotency", []driver.Value{tenant, string(f.request.IdempotencyKey)}
	case "SELECT SUBSTRING(CAST(record AS String), 0, 8193) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2":
		table, key = "run_explanation_heads_v1", []driver.Value{tenant, run}
	case "SELECT payload FROM worker_jobs WHERE tenant_id = $1 AND run_id = $2":
		table, key = "worker_jobs", []driver.Value{tenant, run}
	case "SELECT payload FROM artifact_manifests WHERE tenant_id = $1 AND artifact_manifest_id = $2":
		table, key = "artifact_manifests", []driver.Value{tenant, string(f.request.ManifestID)}
	case "SELECT payload FROM checkpoints WHERE tenant_id = $1 AND attempt_id = $2 ORDER BY sequence DESC LIMIT 1":
		table, key = "checkpoints", []driver.Value{tenant, attempt}
	case "SELECT queue_depth, active_runs FROM tenant_scheduler_counters WHERE tenant_id = $1":
		if !f.exactArgs(args, tenant) {
			return nil, errors.New("lifecycle foreign counter key")
		}
		return row(f.counters)
	case canonicalJoinSQL(quotaReservationStorageSQLV1):
		if !f.exactArgs(args, tenant, "reservation-local-lifecycle") {
			return nil, errors.New("lifecycle foreign reservation key")
		}
		return f.localCanonicalJoinSQL.QueryContext(ctx, statement, args)
	case canonicalJoinSQL(quotaExpiryStorageSQLV1):
		if len(f.expiry) != 8 || !f.exactArgs(args, f.expiry[0], f.expiry[1], f.expiry[2], f.expiry[3]) {
			return nil, errors.New("lifecycle foreign expiry key")
		}
		return f.localCanonicalJoinSQL.QueryContext(ctx, statement, args)
	default:
		return nil, fmt.Errorf("lifecycle peer unexpected query: %s", flat)
	}
	if !f.exactArgs(args, key...) {
		return nil, fmt.Errorf("lifecycle peer foreign key: %s", flat)
	}
	return row(f.rows[table])
}

const localLifecycleLeaseWrite = "UPSERT INTO leases (tenant_id, lease_id, run_id, attempt_id, worker_id, fence_token, acquired_at, expires_at, retention_expire_at, payload) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10 AS JsonDocument))"
const localLifecycleExpiryWrite = "UPSERT INTO lease_expiry_v2 (shard_bucket, expires_at, tenant_id, run_id, lease_id, fence_token) VALUES ($1, $2, $3, $4, $5, $6)"
const localLifecycleHeadWrite = "UPSERT INTO lease_heads (tenant_id, run_id, lease_id, attempt_id, worker_id, fence_token, expires_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
const localLifecycleCountersWrite = "UPDATE tenant_scheduler_counters SET queue_depth = $1, active_runs = $2, updated_at = $3 WHERE tenant_id = $4"

func (f *localWorkerLifecycleSQL) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if err := f.observe(ctx); err != nil {
		return nil, err
	}
	flat := canonicalJoinSQL(statement)
	fields := strings.Fields(flat)
	table := ""
	if len(fields) > 2 && fields[0] == "UPSERT" {
		table = fields[2]
	}
	if flat == localLifecycleCountersWrite {
		table = "tenant_scheduler_counters"
	}
	if table == f.failTable {
		return nil, errors.New("controlled lifecycle write failure")
	}
	tenant, run, attempt := string(f.request.TenantID), string(f.request.RunID), string(f.request.AttemptID)
	switch flat {
	case localLifecycleLeaseWrite:
		var lease domain.Lease
		if len(args) != 10 || json.Unmarshal([]byte(args[9].Value.(string)), &lease) != nil || lease.ID != f.leaseID || lease.TenantID != f.request.TenantID || lease.RunID != f.request.RunID || lease.AttemptID != f.request.AttemptID || lease.WorkerID != f.worker || !f.exactArgs(args[:8], tenant, string(lease.ID), run, attempt, lease.WorkerID, int64(lease.FenceToken), lease.AcquiredAt, lease.ExpiresAt) {
			return nil, errors.New("lifecycle incomplete physical lease")
		}
	case localLifecycleExpiryWrite, localLifecycleHeadWrite:
		var lease domain.Lease
		if len(f.rows["leases"]) != 1 || json.Unmarshal([]byte(f.rows["leases"][0].(string)), &lease) != nil {
			return nil, errors.New("lifecycle projection precedes lease producer")
		}
		bucket, _ := ydbpartition.BucketV1(string(lease.RunID))
		want := []driver.Value{int64(bucket), lease.ExpiresAt, tenant, run, string(lease.ID), int64(lease.FenceToken)}
		if flat == localLifecycleHeadWrite {
			want = []driver.Value{tenant, run, string(lease.ID), attempt, lease.WorkerID, int64(lease.FenceToken), lease.ExpiresAt, lease.AcquiredAt}
		}
		if !f.exactArgs(args, want...) {
			return nil, errors.New("lifecycle physical lease projection mismatch")
		}
	case localLifecycleCountersWrite:
		if len(args) != 4 || args[3].Value != tenant {
			return nil, errors.New("lifecycle foreign counter write")
		}
	default:
		entry, ok := localCanonicalJoinWrites[flat]
		if !ok || (entry.table != "runs" && entry.table != "attempts" && entry.table != "run_explanation_heads_v1" && !strings.HasPrefix(flat, "UPSERT INTO runs_by_session ") && !strings.HasPrefix(flat, "UPSERT INTO run_idempotency ")) {
			return nil, fmt.Errorf("lifecycle peer unexpected write: %s", flat)
		}
		if len(args) < 2 || args[0].Value != tenant {
			return nil, errors.New("lifecycle foreign canonical write")
		}
	}
	result, err := f.localCanonicalJoinSQL.ExecContext(ctx, statement, args)
	if err != nil {
		return result, err
	}
	f.phaseWrites++
	switch flat {
	case localLifecycleLeaseWrite:
		f.rows["leases"] = []driver.Value{args[9].Value}
	case localLifecycleHeadWrite, localLifecycleExpiryWrite:
		f.rows[table] = make([]driver.Value, len(args))
		for i := range args {
			f.rows[table][i] = args[i].Value
		}
	case localLifecycleCountersWrite:
		f.counters = []driver.Value{args[0].Value, args[1].Value}
	}
	return result, nil
}

func newLocalWorkerLifecycle(t *testing.T) (*Store, *localWorkerLifecycleSQL, *originalLocalRecoveryFixture, ports.WorkerJobState, ports.WorkerLeaseRequest) {
	t.Helper()
	store, canonical, original, request := newLocalCanonicalJoin(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	committed, err := store.CommitCanonicalUserEvent(ctx, request)
	if err != nil || !committed.Created {
		t.Fatalf("actual canonical lifecycle prefix: created=%t err=%v", committed.Created, err)
	}
	canonical.canonical = false
	admission := ports.DispatchAdmissionRequest{TenantID: request.TenantID, RunID: request.RunID, AttemptID: request.AttemptID, OutboxID: request.DispatchID, ReservationID: "reservation-local-lifecycle", Now: canonical.at, HoldUntil: canonical.at.Add(30 * time.Second), Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 8, MaxContextEvents: 100, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18}, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
	admitted, err := store.AdmitDispatch(ctx, admission)
	if err != nil || !admitted.Admitted || admitted.Delivery != ports.DispatchDeliveryManagedQueue {
		t.Fatalf("actual lifecycle admission: result=%+v err=%v", admitted, err)
	}
	f := &localWorkerLifecycleSQL{localCanonicalJoinSQL: canonical, leaseID: "lease-local-lifecycle", worker: "worker-local-lifecycle"}
	f.beginPhase("initial load")
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store.db = db
	loaded, found, err := store.LoadWorkerJob(ctx, request.TenantID, request.RunID)
	if err != nil || !found || loaded.Run.Status != domain.RunQueued || loaded.Attempt.Status != domain.AttemptCreated {
		t.Fatalf("actual lifecycle load: found=%t err=%v", found, err)
	}
	for _, table := range []string{"leases", "lease_heads", "lease_expiry_v2"} {
		if len(f.rows[table]) != 0 {
			t.Fatalf("preseeded lifecycle lease: %s", table)
		}
	}
	claim := ports.WorkerLeaseRequest{TenantID: request.TenantID, RunID: request.RunID, AttemptID: request.AttemptID, LeaseID: f.leaseID, WorkerID: f.worker, Now: f.at, ExpiresAt: f.at.Add(20 * time.Second)}
	return store, f, original, loaded, claim
}

func lifecycleContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestLocalCapacityWorkerLifecycleActualClaimThenStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, f, original, loaded, claim := newLocalWorkerLifecycle(t)
		ctx := lifecycleContext(t)
		heads, audits := cloneLocalInstallationRows(original.sql.fundingHeads), cloneLocalInstallationRows(original.sql.fundingAudits)
		f.beginPhase("claim")
		lease, err := store.ClaimWorkerLease(ctx, claim)
		if err != nil || lease.ID != claim.LeaseID || lease.FenceToken != 1 || lease.ValidateForAttempt(loaded.Run, loaded.Attempt) != nil || !lease.AcquiredAt.Equal(claim.Now) || !lease.ExpiresAt.Equal(claim.ExpiresAt) || f.phaseWrites != 3 || len(f.rows["lease_heads"]) != 8 || len(f.rows["lease_expiry_v2"]) != 6 {
			t.Fatalf("real local claim: lease=%+v err=%v SQL=%d writes=%d", lease, err, f.statements, f.phaseWrites)
		}
		claimStatements := f.statements
		before := cloneLocalInstallationRows(f.rows)
		f.beginPhase("claim replay")
		replayed, err := store.ClaimWorkerLease(ctx, claim)
		if err != nil || replayed != lease || f.phaseWrites != 0 || !reflect.DeepEqual(before, f.rows) {
			t.Fatalf("original claim replay: err=%v writes=%d", err, f.phaseWrites)
		}
		f.beginPhase("start")
		if err := store.StartWorkerJob(ctx, loaded, lease, f.at); err != nil {
			t.Fatalf("real claimed local start: err=%v SQL=%d writes=%d", err, f.statements, f.phaseWrites)
		}
		startStatements := f.statements
		f.beginPhase("running load")
		running, found, err := store.LoadWorkerJob(ctx, claim.TenantID, claim.RunID)
		if err != nil || !found || running.Run.Status != domain.RunRunning || running.Attempt.Status != domain.AttemptRunning || running.Attempt.WorkerID != lease.WorkerID || running.Reservation.Status != domain.ReservationHeld || !reflect.DeepEqual(running.Reservation, loaded.Reservation) || running.Job.OwnedExecutionSourceRecord != loaded.Job.OwnedExecutionSourceRecord || running.Run.SubscriptionConnectionID != "" || running.Reservation.SubscriptionConnectionID != "" || !equalLocalCapacityV1(running.Run.LocalCapacityAssociation, loaded.Run.LocalCapacityAssociation) || !reflect.DeepEqual(f.counters, []driver.Value{int64(0), int64(1)}) {
			t.Fatalf("actual running local graph: found=%t err=%v run=%s attempt=%s counters=%v", found, err, running.Run.Status, running.Attempt.Status, f.counters)
		}
		before, counters := cloneLocalInstallationRows(f.rows), append([]driver.Value(nil), f.counters...)
		f.beginPhase("start replay")
		if err := store.StartWorkerJob(ctx, loaded, lease, f.at); err != nil || f.phaseWrites != 0 || !reflect.DeepEqual(before, f.rows) || !reflect.DeepEqual(counters, f.counters) {
			t.Fatalf("real local start replay: err=%v writes=%d", err, f.phaseWrites)
		}
		if !reflect.DeepEqual(heads, original.sql.fundingHeads) || !reflect.DeepEqual(audits, original.sql.fundingAudits) {
			t.Fatal("claim/start republished funding")
		}
		for _, statement := range f.statementsForLifecycle() {
			if strings.Contains(statement, "subscription_connections") {
				t.Fatal("local lifecycle queried fabricated subscription authority")
			}
		}
		t.Logf("real canonical/admission/load → ClaimWorkerLease=%d SQL → StartWorkerJob=%d SQL; original Source/funding/hold unchanged", claimStatements, startStatements)
	})
}

func (f *localWorkerLifecycleSQL) statementsForLifecycle() []string {
	return f.localCanonicalJoinSQL.statements
}

func TestLocalCapacityWorkerLifecycleClaimRollbackAndLostACK(t *testing.T) {
	for _, mode := range []string{"known rollback", "committed lost ACK", "lease head write failure"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store, f, _, loaded, claim := newLocalWorkerLifecycle(t)
				before := cloneLocalInstallationRows(f.rows)
				f.beginPhase(mode)
				switch mode {
				case "known rollback":
					f.commitModes = []string{"rollback"}
				case "committed lost ACK":
					f.commitModes = []string{"ambiguous"}
				case "lease head write failure":
					f.failTable = "lease_heads"
				}
				lease, err := store.ClaimWorkerLease(lifecycleContext(t), claim)
				if mode == "lease head write failure" {
					if err == nil || !reflect.DeepEqual(before, f.rows) {
						t.Fatalf("failed lease projection did not roll back: err=%v rowsEqual=%t", err, reflect.DeepEqual(before, f.rows))
					}
					return
				}
				if err != nil || lease.FenceToken != 1 || lease.ValidateForAttempt(loaded.Run, loaded.Attempt) != nil || f.callbacks != 2 || len(f.rows["leases"]) != 1 || len(f.rows["lease_heads"]) != 8 || len(f.rows["lease_expiry_v2"]) != 6 {
					t.Fatalf("real lease retry: mode=%s err=%v callbacks=%d SQL=%d", mode, err, f.callbacks, f.statements)
				}
				wantWrites := 6
				if mode == "committed lost ACK" {
					wantWrites = 3
				}
				if f.phaseWrites != wantWrites {
					t.Fatalf("lease retry changed original hold: mode=%s writes=%d want=%d", mode, f.phaseWrites, wantWrites)
				}
			})
		})
	}
}

func TestLocalCapacityWorkerLifecycleStartRollbackAndFenceDenial(t *testing.T) {
	for _, fault := range []string{"attempt write failure", "wrong fence", "distinct active claim"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store, f, _, loaded, claim := newLocalWorkerLifecycle(t)
				ctx := lifecycleContext(t)
				f.beginPhase("claim")
				lease, err := store.ClaimWorkerLease(ctx, claim)
				if err != nil {
					t.Fatal("actual prerequisite claim", err)
				}
				before, counters := cloneLocalInstallationRows(f.rows), append([]driver.Value(nil), f.counters...)
				f.beginPhase(fault)
				switch fault {
				case "attempt write failure":
					f.failTable = "attempts"
					err = store.StartWorkerJob(ctx, loaded, lease, f.at)
				case "wrong fence":
					lease.FenceToken++
					err = store.StartWorkerJob(ctx, loaded, lease, f.at)
					if !errors.Is(err, ErrLeaseLost) {
						t.Fatalf("wrong fence disposition: %v", err)
					}
				case "distinct active claim":
					claim.LeaseID = "distinct-lifecycle-lease"
					_, err = store.ClaimWorkerLease(ctx, claim)
					if !errors.Is(err, ErrLeaseHeld) {
						t.Fatalf("active claim disposition: %v", err)
					}
				}
				if err == nil || !reflect.DeepEqual(before, f.rows) || !reflect.DeepEqual(counters, f.counters) {
					t.Fatalf("lifecycle denial changed canonical/capacity state: fault=%s err=%v rowsEqual=%t countersEqual=%t", fault, err, reflect.DeepEqual(before, f.rows), reflect.DeepEqual(counters, f.counters))
				}
			})
		})
	}
}

func TestLocalCapacityWorkerLifecycleStartCommitRecovery(t *testing.T) {
	for _, mode := range []string{"rollback", "ambiguous"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store, f, _, loaded, claim := newLocalWorkerLifecycle(t)
				ctx := lifecycleContext(t)
				f.beginPhase("claim")
				lease, err := store.ClaimWorkerLease(ctx, claim)
				if err != nil {
					t.Fatal("actual prerequisite claim", err)
				}
				leaseRows, expiryRows := append([]driver.Value(nil), f.rows["leases"]...), append([]driver.Value(nil), f.rows["lease_expiry_v2"]...)
				f.beginPhase("start " + mode)
				f.commitModes = []string{mode}
				firstWrites := 0
				f.afterCommit = func() {
					if firstWrites == 0 {
						firstWrites = f.phaseWrites
					}
				}
				if err := store.StartWorkerJob(ctx, loaded, lease, f.at); err != nil {
					t.Fatal("actual start commit recovery", err)
				}
				wantWrites := firstWrites
				if mode == "rollback" {
					wantWrites *= 2
				}
				if firstWrites == 0 || f.callbacks != 2 || f.phaseWrites != wantWrites || !reflect.DeepEqual(f.counters, []driver.Value{int64(0), int64(1)}) || !reflect.DeepEqual(leaseRows, f.rows["leases"]) || !reflect.DeepEqual(expiryRows, f.rows["lease_expiry_v2"]) {
					t.Fatalf("actual start retry changed lease/counters: mode=%s callbacks=%d writes=%d want=%d counters=%v", mode, f.callbacks, f.phaseWrites, wantWrites, f.counters)
				}
			})
		})
	}
}
