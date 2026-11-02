package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Uses the actual synthetic installation Source and actual admission graph.
// Only SQL row DATA is controlled; this does not prove native YDB isolation.
type localCapacityConsumerSQL struct {
	*localCancellationSQL
	corruption string
	queries    int
}

func (f *localCapacityConsumerSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *localCapacityConsumerSQL) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f.queries++
	if f.queries > 32 {
		return nil, errors.New("consumer fixture point-read bound exceeded")
	}
	flat := strings.Join(strings.Fields(statement), " ")
	if flat == "SELECT lease_id, fence_token, expires_at FROM lease_heads WHERE tenant_id = $1 AND run_id = $2" {
		f.recordQuery(statement, args)
		return &explanationFixtureRows{values: append([]driver.Value(nil), f.lease...)}, nil
	}
	if flat == "SELECT payload FROM worker_jobs WHERE tenant_id = $1 AND run_id = $2" {
		var job domain.WorkerJob
		stored := f.rows["worker_jobs"]
		if len(stored) != 0 && (json.Unmarshal([]byte(stored[0].(string)), &job) != nil || len(args) != 2 || args[0].Value != string(job.TenantID) || args[1].Value != string(job.RunID)) {
			return nil, errors.New("consumer job key differs from original")
		}
		f.recordQuery(statement, args)
		return &explanationFixtureRows{values: stored}, nil
	}
	if flat == "SELECT lease_id FROM lease_heads WHERE tenant_id=$1 AND run_id=$2" || flat == "SELECT SUBSTRING(CAST(lease_id AS String),0,161) FROM lease_heads WHERE tenant_id=$1 AND run_id=$2" {
		f.recordQuery(statement, args)
		return &explanationFixtureRows{values: append([]driver.Value(nil), f.lease...)}, nil
	}
	if flat == "SELECT payload FROM checkpoints WHERE tenant_id = $1 AND attempt_id = $2 ORDER BY sequence DESC LIMIT 1" {
		f.recordQuery(statement, args)
		return &explanationFixtureRows{}, nil
	}
	if statement == quotaReservationStorageSQLV1 && len(f.rows["quota_reservations"]) != 0 {
		var r domain.QuotaReservation
		if len(args) != 2 || json.Unmarshal([]byte(f.rows["quota_reservations"][0].(string)), &r) != nil || args[0].Value != string(r.TenantID) || args[1].Value != string(r.ID) {
			return nil, errors.New("consumer quota key differs from original")
		}
		f.recordQuery(statement, args)
		row := []driver.Value{f.rows["quota_reservations"][0], string(r.ID), string(r.TenantID), string(r.RunID), string(r.SubscriptionConnectionID), string(r.Status), int64(r.CapacityUnits), r.HeldAt, r.ExpiresAt, r.ExpiresAt.Add(fixtureRetention)}
		switch f.corruption {
		case "scalar_run":
			row[3] = "foreign-run"
		case "scalar_subscription":
			row[4] = "foreign-subscription"
		case "scalar_units":
			row[6] = int64(2)
		case "scalar_expiry":
			row[8] = r.ExpiresAt.Add(time.Second)
		}
		return &explanationFixtureRows{values: row}, nil
	}
	return f.localCancellationSQL.QueryContext(ctx, statement, args)
}

func (f *localCapacityConsumerSQL) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	result, err := f.localCancellationSQL.ExecContext(ctx, statement, args)
	if err == nil && strings.Join(strings.Fields(statement), " ") == "UPDATE tenant_scheduler_counters SET queue_depth = $1, active_runs = $2, updated_at = $3 WHERE tenant_id = $4" {
		f.counters = []driver.Value{args[0].Value, args[1].Value}
	}
	return result, err
}

func localCapacityConsumerFixture(t *testing.T) (*Store, *localCapacityConsumerSQL, ports.ExpiredQuotaReservation, domain.WorkerJob) {
	t.Helper()
	store, base, original, admission, _ := localCancellationFixture(t)
	got, err := store.AdmitDispatch(t.Context(), admission)
	if err != nil || !got.Admitted {
		t.Fatalf("actual original Source admission: admitted=%t err=%v", got.Admitted, err)
	}
	if err := original.source.Close(); err != nil {
		t.Fatal(err)
	}
	var r domain.QuotaReservation
	var job domain.WorkerJob
	if json.Unmarshal([]byte(base.rows["quota_reservations"][0].(string)), &r) != nil || json.Unmarshal([]byte(base.rows["worker_jobs"][0].(string)), &job) != nil {
		t.Fatal("actual admission did not produce readable original records")
	}
	f := &localCapacityConsumerSQL{localCancellationSQL: base}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store.db = db
	association := *r.LocalCapacityAssociation
	return store, f, ports.ExpiredQuotaReservation{TenantID: r.TenantID, ReservationID: r.ID, RunID: r.RunID, LocalCapacityAssociation: &association, ExpiresAt: r.ExpiresAt}, job
}

func localCapacityConsumerUnchanged(t *testing.T, f *localCapacityConsumerSQL, writes int, expiry, slot, counters []driver.Value) {
	t.Helper()
	if f.writes != writes || !reflect.DeepEqual(f.expiry, expiry) || !reflect.DeepEqual(f.slot, slot) || !reflect.DeepEqual(f.counters, counters) {
		t.Fatalf("consumer changed retained custody: writes=%d want=%d", f.writes, writes)
	}
	for _, statement := range f.statements {
		if strings.Contains(statement, "subscription_connections") {
			t.Fatal("explicit local consumer queried a subscription")
		}
	}
}

func TestLocalCapacityConsumerWorkerLoadPhysicalCorrespondence(t *testing.T) {
	for _, path := range []string{"public", "shared", "terminal"} {
		for _, corruption := range []string{"none", "scalar_run", "scalar_subscription", "scalar_units", "scalar_expiry", "expiry_owner", "expiry_partial", "expiry_missing", "payload_owner", "payload_run", "run_owner"} {
			t.Run(path+"/"+corruption, func(t *testing.T) {
				store, f, candidate, job := localCapacityConsumerFixture(t)
				f.corruption = corruption
				switch corruption {
				case "expiry_owner":
					f.expiry[6] = strings.Replace(f.expiry[6].(string), `"owner_id":"`+string(candidate.LocalCapacityAssociation.OwnerUserID)+`"`, `"owner_id":"foreign-owner"`, 1)
				case "expiry_partial":
					f.expiry[7] = nil
				case "expiry_missing":
					f.expiry = nil
				case "payload_owner", "payload_run":
					var r domain.QuotaReservation
					_ = json.Unmarshal([]byte(f.rows["quota_reservations"][0].(string)), &r)
					if corruption == "payload_owner" {
						r.LocalCapacityAssociation.OwnerUserID = "foreign-owner"
					} else {
						r.RunID = "foreign-run"
					}
					ratedFixturePut(t, f.ratedSQLFixture, "quota_reservations", r)
				case "run_owner":
					var run domain.Run
					_ = json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run)
					run.LocalCapacityAssociation.OwnerUserID = "foreign-owner"
					ratedFixturePut(t, f.ratedSQLFixture, "runs", run)
				}
				writes := f.writes
				expiry, slot, counters := append([]driver.Value(nil), f.expiry...), append([]driver.Value(nil), f.slot...), append([]driver.Value(nil), f.counters...)
				var err error
				var found bool
				if path == "public" {
					_, found, err = store.LoadWorkerJob(t.Context(), candidate.TenantID, candidate.RunID)
				} else {
					err = store.Transact(t.Context(), candidate.TenantID, func(state ports.StateTx) error {
						tx := state.(*stateTx)
						if path == "terminal" {
							_, _, _, e := loadWorkerTerminalState(t.Context(), state, tx, candidate.RunID, job.AttemptID, candidate.ReservationID)
							found = e == nil
							return e
						}
						_, found, err = loadWorkerJobStateTx(t.Context(), tx, candidate.RunID)
						return err
					})
				}
				if corruption == "none" && (err != nil || !found) || corruption != "none" && err == nil {
					t.Fatalf("physical correspondence path=%s corruption=%s found=%t err=%v", path, corruption, found, err)
				}
				localCapacityConsumerUnchanged(t, f, writes, expiry, slot, counters)
			})
		}
	}
}

func TestLocalCapacityConsumerExpiryRetainsAnyLeaseAndOriginalDeadline(t *testing.T) {
	for _, lease := range []string{"original-lease", ""} {
		t.Run("lease="+lease, func(t *testing.T) {
			store, f, candidate, _ := localCapacityConsumerFixture(t)
			// Even an empty/unknown persisted lease row is custody, not absence.
			f.lease = []driver.Value{lease}
			writes := f.writes
			expiry, slot, counters := append([]driver.Value(nil), f.expiry...), append([]driver.Value(nil), f.slot...), append([]driver.Value(nil), f.counters...)
			for _, at := range []time.Time{candidate.ExpiresAt.Add(-time.Nanosecond), candidate.ExpiresAt, candidate.ExpiresAt.Add(time.Hour)} {
				expired, err := store.ExpireQuotaReservation(t.Context(), candidate, at)
				if expired || err != nil {
					t.Fatalf("lease custody lost: expired=%t err=%v", expired, err)
				}
				localCapacityConsumerUnchanged(t, f, writes, expiry, slot, counters)
			}
		})
	}
}

func TestLocalCapacityConsumerExpiryRejectsPhysicalMismatchBeforeLeaseNoop(t *testing.T) {
	for _, corruption := range []string{"scalar_run", "expiry_partial"} {
		t.Run(corruption, func(t *testing.T) {
			store, f, candidate, _ := localCapacityConsumerFixture(t)
			f.corruption = corruption
			if corruption == "expiry_partial" {
				f.expiry[7] = nil
			}
			f.lease = []driver.Value{"retained-lease"}
			writes := f.writes
			expiry, slot, counters := append([]driver.Value(nil), f.expiry...), append([]driver.Value(nil), f.slot...), append([]driver.Value(nil), f.counters...)
			expired, err := store.ExpireQuotaReservation(t.Context(), candidate, candidate.ExpiresAt)
			if expired || err == nil {
				t.Fatalf("malformed retained projection accepted: expired=%t err=%v", expired, err)
			}
			localCapacityConsumerUnchanged(t, f, writes, expiry, slot, counters)
		})
	}
}

func TestLocalCapacityConsumerQueuedExpiryReleasesOriginalHoldOnce(t *testing.T) {
	store, f, candidate, _ := localCapacityConsumerFixture(t)
	before, err := store.ExpireQuotaReservation(t.Context(), candidate, candidate.ExpiresAt.Add(-time.Nanosecond))
	if before || err != nil {
		t.Fatalf("original hold expired early: expired=%t err=%v", before, err)
	}
	expired, err := store.ExpireQuotaReservation(t.Context(), candidate, candidate.ExpiresAt)
	if !expired || err != nil {
		logLocalCancellationSQLTail(t, f.localCancellationSQL)
		t.Fatalf("original queued hold expiry: expired=%t err=%v", expired, err)
	}
	var r domain.QuotaReservation
	var run domain.Run
	if json.Unmarshal([]byte(f.rows["quota_reservations"][0].(string)), &r) != nil || json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run) != nil || r.Status != domain.ReservationExpired || run.Status != domain.RunQuotaBlocked || len(f.expiry) != 0 || f.slot[0] != string(domain.SchedulerReauthRequired) || f.slot[1] != "" || f.slot[2] != "" || f.counters[0] != int64(0) || f.counters[1] != int64(0) || !r.ExpiresAt.Equal(candidate.ExpiresAt) {
		t.Fatal("expiry did not preserve original deadline and release exactly original queued ownership")
	}
	writes := f.writes
	expired, err = store.ExpireQuotaReservation(t.Context(), candidate, candidate.ExpiresAt.Add(time.Second))
	if expired || err != nil || f.writes != writes {
		t.Fatalf("expiry replay mutated custody: expired=%t writes=%d/%d err=%v", expired, f.writes, writes, err)
	}
}

func TestLocalCapacityConsumerMissingRunFallbackHasNoEffects(t *testing.T) {
	for _, disposition := range []string{"missing_quota", "terminal", "not_expired", "held_expired"} {
		t.Run(disposition, func(t *testing.T) {
			store, f, candidate, _ := localCapacityConsumerFixture(t)
			f.rows["runs"] = nil
			at := candidate.ExpiresAt
			switch disposition {
			case "missing_quota":
				f.rows["quota_reservations"] = nil
			case "terminal":
				var r domain.QuotaReservation
				_ = json.Unmarshal([]byte(f.rows["quota_reservations"][0].(string)), &r)
				r.Status = domain.ReservationReleased
				ratedFixturePut(t, f.ratedSQLFixture, "quota_reservations", r)
			case "not_expired":
				at = at.Add(-time.Nanosecond)
			}
			writes := f.writes
			expiry, slot, counters := append([]driver.Value(nil), f.expiry...), append([]driver.Value(nil), f.slot...), append([]driver.Value(nil), f.counters...)
			expired, err := store.ExpireQuotaReservation(t.Context(), candidate, at)
			if expired || (disposition == "held_expired") != (err != nil) {
				t.Fatalf("missing Run disposition=%s expired=%t err=%v", disposition, expired, err)
			}
			localCapacityConsumerUnchanged(t, f, writes, expiry, slot, counters)
		})
	}
}

func TestLocalCapacityConsumerWorkerStartRechecksPhysicalHold(t *testing.T) {
	for _, corruption := range []string{"none", "scalar_run", "expiry_missing", "expiry_partial", "quota_missing", "run_missing", "attempt_missing"} {
		t.Run(corruption, func(t *testing.T) {
			store, f, candidate, _ := localCapacityConsumerFixture(t)
			loaded, found, err := store.LoadWorkerJob(t.Context(), candidate.TenantID, candidate.RunID)
			if err != nil || !found {
				t.Fatalf("original worker load: found=%t err=%v", found, err)
			}
			lease := domain.Lease{ID: "original-worker-lease", TenantID: candidate.TenantID, RunID: candidate.RunID, AttemptID: loaded.Attempt.ID, WorkerID: "original-worker", FenceToken: 1, AcquiredAt: loaded.Run.UpdatedAt, ExpiresAt: loaded.Run.UpdatedAt.Add(time.Hour)}
			f.lease = []driver.Value{string(lease.ID), int64(lease.FenceToken), lease.ExpiresAt}
			f.corruption = corruption
			if corruption == "expiry_missing" {
				f.expiry = nil
			} else if corruption == "expiry_partial" {
				f.expiry[7] = nil
			} else if corruption == "quota_missing" {
				f.rows["quota_reservations"] = nil
			} else if corruption == "run_missing" {
				f.rows["runs"] = nil
			} else if corruption == "attempt_missing" {
				f.rows["attempts"] = nil
			}
			f.queries = 0
			writes := f.writes
			expiry, slot, counters := append([]driver.Value(nil), f.expiry...), append([]driver.Value(nil), f.slot...), append([]driver.Value(nil), f.counters...)
			err = store.StartWorkerJob(t.Context(), loaded, lease, loaded.Run.UpdatedAt)
			if corruption != "none" {
				if err == nil {
					t.Fatal("worker start accepted a changed original physical hold")
				}
				localCapacityConsumerUnchanged(t, f, writes, expiry, slot, counters)
				return
			}
			var run domain.Run
			if json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run) != nil || err != nil || run.Status != domain.RunRunning || f.counters[0] != int64(0) || f.counters[1] != int64(1) || !reflect.DeepEqual(f.expiry, expiry) || !reflect.DeepEqual(f.slot, slot) {
				t.Fatalf("canonical worker start did not preserve original held capacity: err=%v", err)
			}
			writes = f.writes
			f.queries = 0
			// Running replay retains the original held deadline, not a renewed
			// queue hold; the original live lease still governs this replay.
			err = store.StartWorkerJob(t.Context(), loaded, lease, candidate.ExpiresAt.Add(time.Second))
			if err != nil || f.writes != writes {
				t.Fatalf("exact running replay changed original hold: err=%v writes=%d/%d", err, f.writes, writes)
			}
			f.corruption, f.queries = "scalar_run", 0
			err = store.StartWorkerJob(t.Context(), loaded, lease, candidate.ExpiresAt.Add(time.Second))
			if err == nil || f.writes != writes {
				t.Fatalf("running replay accepted changed physical custody: err=%v writes=%d/%d", err, f.writes, writes)
			}
			// A malformed replacement cannot move the original local worker
			// through the legacy running-replay branch without its hold check.
			f.corruption, f.queries = "none", 0
			run.LocalCapacityAssociation, run.SubscriptionConnectionID = nil, "foreign-subscription"
			ratedFixturePut(t, f.ratedSQLFixture, "runs", run)
			err = store.StartWorkerJob(t.Context(), loaded, lease, candidate.ExpiresAt.Add(time.Second))
			if err == nil || f.writes != writes {
				t.Fatalf("original local worker replay accepted an arm downgrade: err=%v writes=%d/%d", err, f.writes, writes)
			}
		})
	}
}
