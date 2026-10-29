package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Corrupt only returned physical DATA, after the actual first admission. The
// original Source, payloads, Job/Final and held slot are never replaced.
type localAdmissionReplaySQL struct {
	*localCanonicalJoinSQL
	physicalStatus string
}

func (f *localAdmissionReplaySQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *localAdmissionReplaySQL) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := f.localCanonicalJoinSQL.QueryContext(ctx, statement, args)
	if err != nil || statement != quotaReservationStorageSQLV1 || f.physicalStatus == "" {
		return rows, err
	}
	defer rows.Close()
	values := make([]driver.Value, 10)
	if err := rows.Next(values); err != nil {
		return nil, err
	}
	values[5] = f.physicalStatus
	return &explanationFixtureRows{values: values}, nil
}

func TestLocalCapacityAdmissionReplayRequiresPhysicalHold(t *testing.T) {
	for _, fault := range []string{"intact", "missing expiry", "foreign expiry", "physical status"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store, base, _, request := newLocalCanonicalJoin(t)
				peer := &localAdmissionReplaySQL{localCanonicalJoinSQL: base}
				db := sql.OpenDB(peer)
				db.SetMaxOpenConns(1)
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				store.db = db
				if result, err := store.CommitCanonicalUserEvent(t.Context(), request); err != nil || !result.Created {
					t.Fatalf("actual canonical setup: result=%+v err=%v", result, err)
				}
				base.canonical = false
				admission := ports.DispatchAdmissionRequest{TenantID: request.TenantID, RunID: request.RunID, AttemptID: request.AttemptID, OutboxID: request.DispatchID, ReservationID: "reservation-local-replay", Now: base.at, HoldUntil: base.at.Add(30 * time.Second), Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 8, MaxContextEvents: 100, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18}, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
				if first, err := store.AdmitDispatch(t.Context(), admission); err != nil || !first.Admitted {
					t.Fatalf("actual first admission: result=%+v err=%v", first, err)
				}
				switch fault {
				case "missing expiry":
					base.expiry = nil
				case "foreign expiry":
					base.expiry[4] = "foreign-run"
				case "physical status":
					peer.physicalStatus = string(domain.ReservationReleased)
				}
				beforeWrites := base.writes
				beforeRows := make(map[string][]driver.Value, len(base.rows))
				for table, row := range base.rows {
					beforeRows[table] = append([]driver.Value(nil), row...)
				}
				beforeExpiry := append([]driver.Value(nil), base.expiry...)
				beforeSlot := append([]driver.Value(nil), base.slot...)
				beforeCounters := append([]driver.Value(nil), base.counters...)
				beforeStatements := len(base.statements)
				result, err := store.AdmitDispatch(t.Context(), admission)
				if fault == "intact" {
					if err != nil || !result.Admitted || result.Code != "already_admitted" {
						t.Fatalf("intact original hold replay denied: result=%+v err=%v", result, err)
					}
				} else if err == nil || result.Admitted {
					t.Errorf("corrupt physical hold accepted: fault=%s result=%+v err=%v", fault, result, err)
				}
				if base.writes != beforeWrites || !reflect.DeepEqual(base.rows, beforeRows) || !reflect.DeepEqual(base.expiry, beforeExpiry) || !reflect.DeepEqual(base.slot, beforeSlot) || !reflect.DeepEqual(base.counters, beforeCounters) {
					t.Fatal("hold replay repaired/replaced original capacity facts")
				}
				if statements := len(base.statements) - beforeStatements; statements > 32 {
					t.Fatalf("physical replay exceeded original callback bound: SQL=%d", statements)
				}
			})
		})
	}
}

func TestLocalCapacityAdmissionRecoversAfterTenantPressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, f, _, request := newLocalCanonicalJoin(t)
		if committed, err := store.CommitCanonicalUserEvent(t.Context(), request); err != nil || !committed.Created {
			t.Fatalf("actual canonical setup: result=%+v err=%v", committed, err)
		}
		f.canonical = false
		admission := ports.DispatchAdmissionRequest{TenantID: request.TenantID, RunID: request.RunID, AttemptID: request.AttemptID, OutboxID: request.DispatchID, ReservationID: "reservation-local-pressure", Now: f.at, HoldUntil: f.at.Add(30 * time.Second), Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 8, MaxContextEvents: 100, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18}, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
		// Another resource currently occupies the tenant's sole active slot.
		f.counters = []driver.Value{int64(0), int64(1)}
		denied, err := store.AdmitDispatch(t.Context(), admission)
		if err != nil || denied.Admitted || denied.Code != "tenant_active_run_limit" || len(f.slot) != 7 || f.slot[0] != string(domain.SchedulerPressured) || f.slot[1] != "" || f.slot[2] != "" || len(f.rows["quota_reservations"]) != 0 || len(f.final) != 0 {
			t.Fatalf("first pressure did not retain an empty denied resource: result=%+v slot=%+v err=%v", denied, f.slot, err)
		}
		originalSource := request.OwnedExecutionSourceRecord
		// The other resource finishes; no mutation repairs this resource slot.
		f.counters = []driver.Value{int64(0), int64(0)}
		before := len(f.statements)
		admitted, err := store.AdmitDispatch(t.Context(), admission)
		if err != nil || !admitted.Admitted || admitted.Code != "admitted" || admitted.State != domain.SchedulerReady || len(f.expiry) != 8 || len(f.final) == 0 || len(f.statements)-before > 32 {
			t.Fatalf("cleared tenant pressure stranded resource: result=%+v SQL=%d err=%v", admitted, len(f.statements)-before, err)
		}
		var job domain.WorkerJob
		if len(f.rows["worker_jobs"]) != 1 || json.Unmarshal([]byte(f.rows["worker_jobs"][0].(string)), &job) != nil || job.OwnedExecutionSourceRecord != originalSource || !reflect.DeepEqual(f.counters, []driver.Value{int64(1), int64(0)}) {
			t.Fatal("pressure recovery changed original Source or tenant counter ownership")
		}
	})
}
