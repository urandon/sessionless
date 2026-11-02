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
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// This exact-query transactional model exercises physical correspondence only,
// not admission/current funding, native YDB isolation or execution authority.
type quotaCapacitySQLFixture struct {
	run                      domain.Run
	quota, pendingQuota      []driver.Value
	expiry, pendingExpiry    map[string][]driver.Value
	queries, writes          int
	commits, rollbacks       int
	queriesThisTx, failWrite int
	oldPayloadReads          int
	sdkTimestampPrecision    bool
}

// Match the pinned SDK's time.Time -> Timestamp bind conversion, not a reader
// tolerance. JSON string arguments remain unchanged and retain nanoseconds.
func (f *quotaCapacitySQLFixture) CheckNamedValue(value *driver.NamedValue) error {
	if at, ok := value.Value.(time.Time); ok && f.sdkTimestampPrecision {
		value.Value = at.UTC().Truncate(time.Microsecond)
		return nil
	}
	return driver.ErrSkip
}

func (f *quotaCapacitySQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (*quotaCapacitySQLFixture) Driver() driver.Driver                          { return explanationFixtureDriver{} }
func (*quotaCapacitySQLFixture) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare forbidden")
}
func (*quotaCapacitySQLFixture) Close() error { return nil }
func (*quotaCapacitySQLFixture) Begin() (driver.Tx, error) {
	return nil, errors.New("explicit transaction required")
}
func (f *quotaCapacitySQLFixture) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if ctx.Err() != nil || options.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("serializable required")
	}
	f.pendingQuota = append([]driver.Value(nil), f.quota...)
	f.pendingExpiry = cloneLocalInstallationRows(f.expiry)
	f.queriesThisTx = 0
	return quotaCapacitySQLTx{f}, nil
}

type quotaCapacitySQLTx struct{ f *quotaCapacitySQLFixture }

func (tx quotaCapacitySQLTx) Commit() error {
	tx.f.quota = append([]driver.Value(nil), tx.f.pendingQuota...)
	tx.f.expiry = cloneLocalInstallationRows(tx.f.pendingExpiry)
	tx.f.commits++
	return nil
}
func (tx quotaCapacitySQLTx) Rollback() error { tx.f.rollbacks++; return nil }
func quotaCapacityFlat(s string) string       { return strings.Join(strings.Fields(s), " ") }
func quotaCapacityExpiryKey(bucket any, expires any, tenant any, id any) string {
	return fmt.Sprintf("%v/%s/%v/%v", bucket, expires.(time.Time).Format(time.RFC3339Nano), tenant, id)
}
func (f *quotaCapacitySQLFixture) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	f.queries++
	f.queriesThisTx++
	if ctx.Err() != nil || f.queriesThisTx > 8 {
		return nil, errors.New("bounded query count exceeded")
	}
	flat := quotaCapacityFlat(query)
	var row []driver.Value
	switch flat {
	case quotaCapacityFlat(boundedRunPayloadSQL):
		if len(args) != 2 || args[0].Value != string(f.run.TenantID) || args[1].Value != string(f.run.ID) {
			return nil, errors.New("foreign Run read")
		}
		raw, _ := json.Marshal(f.run)
		row = []driver.Value{string(raw)}
	case "SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2", "SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_deletions WHERE tenant_id = $1 AND session_id = $2":
		if len(args) != 2 || args[0].Value != string(f.run.TenantID) || args[1].Value != string(f.run.SessionID) {
			return nil, errors.New("foreign deletion read")
		}
	case "SELECT payload FROM quota_reservations WHERE tenant_id = $1 AND quota_reservation_id = $2":
		f.oldPayloadReads++
		if len(args) != 2 || args[0].Value != string(f.run.TenantID) || args[1].Value != "reservation" {
			return nil, errors.New("foreign quota read")
		}
		if f.pendingQuota != nil {
			row = []driver.Value{f.pendingQuota[0]}
		}
	case quotaCapacityFlat(quotaReservationStorageSQLV1):
		if len(args) != 2 || args[0].Value != string(f.run.TenantID) || args[1].Value != "reservation" || !strings.Contains(query, "SUBSTRING(CAST(payload AS String),0,32769)") {
			return nil, errors.New("unbounded or foreign physical quota read")
		}
		row = f.pendingQuota
	case quotaCapacityFlat(quotaExpiryStorageSQLV1):
		if len(args) != 4 || args[2].Value != string(f.run.TenantID) || args[3].Value != "reservation" || !strings.Contains(query, "WHERE shard_bucket=$1 AND expires_at=$2 AND tenant_id=$3 AND quota_reservation_id=$4") {
			return nil, errors.New("unscoped expiry read")
		}
		row = f.pendingExpiry[quotaCapacityExpiryKey(args[0].Value, args[1].Value, args[2].Value, args[3].Value)]
	default:
		return nil, errors.New("unexpected query (subscription/discovery forbidden)")
	}
	return &explanationFixtureRows{values: row}, nil
}
func (f *quotaCapacitySQLFixture) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f.writes++
	if f.failWrite != 0 && f.writes == f.failWrite {
		return nil, errors.New("controlled write failure")
	}
	v := func(i int) driver.Value { return args[i].Value }
	switch quotaCapacityFlat(query) {
	case "DELETE FROM quota_expiry_v2 WHERE shard_bucket = $1 AND expires_at = $2 AND tenant_id = $3 AND quota_reservation_id = $4":
		if len(args) != 4 || v(2) != string(f.run.TenantID) || v(3) != "reservation" {
			return nil, errors.New("foreign expiry delete")
		}
		delete(f.pendingExpiry, quotaCapacityExpiryKey(v(0), v(1), v(2), v(3)))
	case "UPSERT INTO quota_reservations (tenant_id, quota_reservation_id, run_id, subscription_connection_id, status, capacity_units, held_at, expires_at, retention_expire_at, payload) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10 AS JsonDocument))":
		if len(args) != 10 || v(0) != string(f.run.TenantID) || v(1) != "reservation" || v(2) != string(f.run.ID) {
			return nil, errors.New("foreign quota upsert")
		}
		f.pendingQuota = []driver.Value{v(9), v(1), v(0), v(2), v(3), v(4), v(5), v(6), v(7), v(8)}
	case "UPSERT INTO quota_expiry_v2 (shard_bucket, expires_at, tenant_id, quota_reservation_id, run_id, subscription_connection_id, local_capacity_association, local_capacity_digest) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)":
		if len(args) != 8 || v(2) != string(f.run.TenantID) || v(3) != "reservation" || v(4) != string(f.run.ID) {
			return nil, errors.New("foreign expiry upsert")
		}
		f.pendingExpiry[quotaCapacityExpiryKey(v(0), v(1), v(2), v(3))] = []driver.Value{v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7)}
	default:
		return nil, errors.New("unexpected mutation")
	}
	return driver.RowsAffected(1), nil
}

func quotaCapacityFixture(t *testing.T, local bool) (*quotaCapacitySQLFixture, *Store, domain.QuotaReservation) {
	t.Helper()
	at := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	run := domain.Run{ID: "run", TenantID: "tenant-quota", SessionID: "session", TriggerEventID: "event", SubscriptionConnectionID: "subscription", Status: domain.RunQueued, IdempotencyKey: "run-intent", CreatedAt: at, UpdatedAt: at}
	if local {
		run.SubscriptionConnectionID = ""
		run.LocalCapacityAssociation = &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: run.TenantID, OwnerUserID: "owner", ResourceID: "resource"}
	}
	reservation := domain.QuotaReservation{ID: "reservation", TenantID: run.TenantID, RunID: run.ID, SubscriptionConnectionID: run.SubscriptionConnectionID, LocalCapacityAssociation: run.LocalCapacityAssociation, Status: domain.ReservationHeld, CapacityUnits: 1, HeldAt: at, ExpiresAt: at.Add(time.Hour), UpdatedAt: at}
	f := &quotaCapacitySQLFixture{run: run, expiry: make(map[string][]driver.Value)}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := New(db, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return f, store, reservation
}
func quotaCapacitySeed(t *testing.T, f *quotaCapacitySQLFixture, r domain.QuotaReservation, projection bool) {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	f.quota = []driver.Value{string(raw), string(r.ID), string(r.TenantID), string(r.RunID), string(r.SubscriptionConnectionID), string(r.Status), int64(r.CapacityUnits), r.HeldAt, r.ExpiresAt, r.ExpiresAt.Add(defaultOperationalRetention)}
	if projection {
		quotaCapacitySeedExpiry(t, f, r)
	}
}
func quotaCapacitySeedExpiry(t *testing.T, f *quotaCapacitySQLFixture, r domain.QuotaReservation) string {
	t.Helper()
	p, err := encodeCapacityStorageV1(r.TenantID, r.SubscriptionConnectionID, r.LocalCapacityAssociation)
	if err != nil {
		t.Fatal(err)
	}
	bucket, _ := ydbpartition.BucketV1(string(r.ID))
	key := quotaCapacityExpiryKey(int64(bucket), r.ExpiresAt, string(r.TenantID), string(r.ID))
	var association, digest driver.Value
	if p.association.Valid {
		association = p.association.String
	}
	if p.digest.Valid {
		digest = p.digest.String
	}
	f.expiry[key] = []driver.Value{int64(bucket), r.ExpiresAt, string(r.TenantID), string(r.ID), string(r.RunID), p.key, association, digest}
	return key
}
func quotaCapacityApply(t *testing.T, store *Store, r domain.QuotaReservation) error {
	t.Helper()
	tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	state := &stateTx{store: store, sqlTx: tx, tenantID: r.TenantID}
	if err := state.PutQuotaReservation(t.Context(), r); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func TestLocalCapacityQuotaWriterRejectsProjectionCollisionBeforeWrite(t *testing.T) {
	for _, fault := range []string{"orphan", "foreign run", "partial NULL", "digest", "key", "noncanonical"} {
		t.Run(fault, func(t *testing.T) {
			f, store, r := quotaCapacityFixture(t, true)
			key := quotaCapacitySeedExpiry(t, f, r)
			if fault != "orphan" {
				quotaCapacitySeed(t, f, r, false)
			}
			switch fault {
			case "foreign run":
				f.expiry[key][4] = "foreign"
			case "partial NULL":
				f.expiry[key][7] = nil
			case "digest":
				f.expiry[key][7] = strings.Repeat("0", 64)
			case "key":
				f.expiry[key][5] = "subscription"
			case "noncanonical":
				f.expiry[key][6] = " " + f.expiry[key][6].(string)
			}
			before := cloneLocalInstallationRows(f.expiry)
			beforeQuota := append([]driver.Value(nil), f.quota...)
			err := quotaCapacityApply(t, store, r)
			if !errors.Is(err, domain.ErrMembershipDenied) || f.writes != 0 || f.commits != 0 || !reflect.DeepEqual(before, f.expiry) || !reflect.DeepEqual(beforeQuota, f.quota) {
				t.Fatalf("physical collision overwritten: fault=%s denied=%t writes=%d commits=%d", fault, errors.Is(err, domain.ErrMembershipDenied), f.writes, f.commits)
			}
		})
	}
}

func TestLocalCapacityQuotaWriterHeldReleaseAndReplay(t *testing.T) {
	f, store, r := quotaCapacityFixture(t, true)
	if err := quotaCapacityApply(t, store, r); err != nil {
		t.Fatal("fresh exact hold", err)
	}
	if f.writes != 2 || len(f.expiry) != 1 || f.quota[4] != "" || f.oldPayloadReads != 0 {
		t.Fatalf("fresh local hold corrupted physical arm: writes=%d expiry=%d oldReads=%d", f.writes, len(f.expiry), f.oldPayloadReads)
	}
	for _, row := range f.expiry {
		if row[5] == "" || row[6] == nil || row[7] == nil {
			t.Fatal("local expiry missing strict physical arm")
		}
	}
	writes := f.writes
	if err := quotaCapacityApply(t, store, r); err != nil || f.writes != writes {
		t.Fatalf("held replay rewrote hold: writes=%d/%d err=%v", f.writes, writes, err)
	}
	r.Status, r.UpdatedAt = domain.ReservationReleased, r.UpdatedAt.Add(time.Second)
	if err := quotaCapacityApply(t, store, r); err != nil || f.writes-writes != 2 || len(f.expiry) != 0 {
		t.Fatalf("owned release failed: writes=%d expiry=%d err=%v", f.writes-writes, len(f.expiry), err)
	}
	writes = f.writes
	if err := quotaCapacityApply(t, store, r); err != nil || f.writes != writes {
		t.Fatalf("terminal replay rewrote custody: writes=%d/%d err=%v", f.writes, writes, err)
	}
}

func TestLocalCapacityQuotaTimestampPrecisionRoundTrip(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprintf("local=%t", local), func(t *testing.T) {
			f, store, original := quotaCapacityFixture(t, local)
			f.sdkTimestampPrecision = true
			original.HeldAt = original.HeldAt.Add(581 * time.Nanosecond)
			original.ExpiresAt = original.ExpiresAt.Add(777 * time.Nanosecond)
			original.UpdatedAt = original.HeldAt
			if err := quotaCapacityApply(t, store, original); err != nil {
				t.Fatal("original writer", err)
			}
			raw := f.quota[0]
			if !f.quota[7].(time.Time).Equal(original.HeldAt.UTC().Truncate(time.Microsecond)) || !f.quota[8].(time.Time).Equal(original.ExpiresAt.UTC().Truncate(time.Microsecond)) {
				t.Fatal("fixture did not model the SDK physical timestamp projection")
			}
			tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelSerializable})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tx.Rollback() })
			got, found, err := readWorkerQuotaReservationStorageV1(t.Context(), &stateTx{store: store, sqlTx: tx, tenantID: original.TenantID}, f.run, original.ID)
			if err != nil || !found || !reflect.DeepEqual(got, original) {
				t.Fatalf("authentic nanos JSON/micros columns rejected or rewritten: found=%t err=%v", found, err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			writes := f.writes
			if err := quotaCapacityApply(t, store, original); err != nil || local && f.writes != writes || f.quota[0] != raw {
				t.Fatal("held replay rewrote original nanosecond history", err)
			}
			released := original.Clone()
			released.Status, released.UpdatedAt = domain.ReservationReleased, original.UpdatedAt.Add(time.Second)
			if err := quotaCapacityApply(t, store, released); err != nil || len(f.expiry) != 0 {
				t.Fatal("release lost original physical expiry key", err)
			}
			var retained domain.QuotaReservation
			if err := json.Unmarshal([]byte(f.quota[0].(string)), &retained); err != nil || !reflect.DeepEqual(retained, released) {
				t.Fatal("release normalized canonical JSON timestamps", err)
			}
			writes = f.writes
			if err := quotaCapacityApply(t, store, released); err != nil || local && f.writes != writes {
				t.Fatal("terminal replay rewrote history", err)
			}
		})
	}
}

func TestLocalCapacityQuotaTimestampPrecisionRejectsPhysicalMismatch(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, fault := range []string{"held-before", "held-after", "expiry-before", "expiry-after", "index-before", "index-after", "physical-submicro"} {
			t.Run(fmt.Sprintf("local=%t/%s", local, fault), func(t *testing.T) {
				f, store, r := quotaCapacityFixture(t, local)
				f.sdkTimestampPrecision = true
				r.HeldAt, r.ExpiresAt, r.UpdatedAt = r.HeldAt.Add(581*time.Nanosecond), r.ExpiresAt.Add(777*time.Nanosecond), r.UpdatedAt.Add(581*time.Nanosecond)
				if err := quotaCapacityApply(t, store, r); err != nil {
					t.Fatal(err)
				}
				delta := time.Microsecond
				if strings.HasSuffix(fault, "before") {
					delta = -delta
				}
				switch {
				case strings.HasPrefix(fault, "held-"):
					f.quota[7] = f.quota[7].(time.Time).Add(delta)
				case strings.HasPrefix(fault, "expiry-"):
					f.quota[8] = f.quota[8].(time.Time).Add(delta)
				case strings.HasPrefix(fault, "index-"):
					for _, row := range f.expiry {
						row[1] = row[1].(time.Time).Add(delta)
					}
				default:
					f.quota[7] = r.HeldAt
				}
				beforeQuota, beforeExpiry, writes := append([]driver.Value(nil), f.quota...), cloneLocalInstallationRows(f.expiry), f.writes
				tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelSerializable})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = tx.Rollback() })
				got, found, err := readWorkerQuotaReservationStorageV1(t.Context(), &stateTx{store: store, sqlTx: tx, tenantID: r.TenantID}, f.run, r.ID)
				if !errors.Is(err, domain.ErrMembershipDenied) || found || !reflect.DeepEqual(got, domain.QuotaReservation{}) {
					t.Fatalf("physical timestamp corruption accepted: found=%t err=%v", found, err)
				}
				if f.writes != writes || !reflect.DeepEqual(beforeQuota, f.quota) || !reflect.DeepEqual(beforeExpiry, f.expiry) {
					t.Fatal("reader repaired corrupted storage")
				}
			})
		}
	}
}

func TestLocalCapacityQuotaTimestampPrecisionDoesNotExpireEarly(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprintf("local=%t", local), func(t *testing.T) {
			f, store, r := quotaCapacityFixture(t, local)
			f.sdkTimestampPrecision = true
			r.ExpiresAt = r.ExpiresAt.Add(777 * time.Nanosecond)
			if err := quotaCapacityApply(t, store, r); err != nil {
				t.Fatal(err)
			}
			at := r.ExpiresAt.UTC().Truncate(time.Microsecond)
			candidate := ports.ExpiredQuotaReservation{TenantID: r.TenantID, ReservationID: r.ID, RunID: r.RunID, SubscriptionConnectionID: r.SubscriptionConnectionID, LocalCapacityAssociation: r.LocalCapacityAssociation, ExpiresAt: at}
			beforeQuota, beforeExpiry, writes := append([]driver.Value(nil), f.quota...), cloneLocalInstallationRows(f.expiry), f.writes
			expired, err := store.ExpireQuotaReservation(t.Context(), candidate, at)
			if err != nil || expired {
				t.Fatalf("projected expiry rejected or ran before original JSON deadline: expired=%t err=%v", expired, err)
			}
			if f.writes != writes || !reflect.DeepEqual(beforeQuota, f.quota) || !reflect.DeepEqual(beforeExpiry, f.expiry) {
				t.Fatal("early expiry changed original hold")
			}
			for _, check := range []struct {
				name   string
				expiry time.Time
			}{
				{name: "candidate-before", expiry: at.Add(-time.Microsecond)},
				{name: "candidate-after", expiry: at.Add(time.Microsecond)},
				{name: "candidate-submicro", expiry: at.Add(time.Nanosecond)},
			} {
				t.Run(check.name, func(t *testing.T) {
					changed := candidate
					changed.ExpiresAt = check.expiry
					expired, err := store.ExpireQuotaReservation(t.Context(), changed, r.ExpiresAt.Add(time.Second))
					var mismatch domain.ValidationError
					if expired || !errors.As(err, &mismatch) || mismatch.Field != "quota expiry candidate" {
						t.Fatalf("forged physical candidate did not fail its exact correlation: expired=%t err=%v", expired, err)
					}
					if f.writes != writes || !reflect.DeepEqual(beforeQuota, f.quota) || !reflect.DeepEqual(beforeExpiry, f.expiry) {
						t.Fatal("forged expiry candidate changed original hold")
					}
				})
			}
		})
	}
}

func TestLocalCapacityQuotaWriterMissingAndImmutableHoldDenials(t *testing.T) {
	for _, fault := range []string{"missing projection", "fresh terminal", "renewed expiry", "changed heldAt", "changed units", "changed updatedAt", "terminal revival", "terminal projection collision"} {
		t.Run(fault, func(t *testing.T) {
			f, store, r := quotaCapacityFixture(t, true)
			if fault != "fresh terminal" {
				quotaCapacitySeed(t, f, r, fault != "missing projection")
			}
			switch fault {
			case "missing projection", "fresh terminal":
				r.Status = domain.ReservationReleased
			case "renewed expiry":
				r.ExpiresAt = r.ExpiresAt.Add(time.Minute)
			case "changed heldAt":
				r.HeldAt = r.HeldAt.Add(-time.Second)
			case "changed units":
				r.CapacityUnits++
			case "changed updatedAt":
				r.UpdatedAt = r.UpdatedAt.Add(time.Second)
			case "terminal revival":
				prior := r
				prior.Status = domain.ReservationReleased
				quotaCapacitySeed(t, f, prior, false)
				f.expiry = make(map[string][]driver.Value)
			case "terminal projection collision":
				r.Status = domain.ReservationReleased
				quotaCapacitySeed(t, f, r, true)
			}
			if err := quotaCapacityApply(t, store, r); !errors.Is(err, domain.ErrMembershipDenied) || f.writes != 0 {
				t.Fatalf("local original hold recreated/mutated: fault=%s denied=%t writes=%d", fault, errors.Is(err, domain.ErrMembershipDenied), f.writes)
			}
		})
	}
}

func TestLocalCapacityQuotaWriterPreservesLegacyHistory(t *testing.T) {
	for _, mode := range []string{"fresh held", "fresh terminal", "missing projection release", "extension"} {
		t.Run(mode, func(t *testing.T) {
			f, store, r := quotaCapacityFixture(t, false)
			switch mode {
			case "fresh terminal":
				r.Status = domain.ReservationCommitted
			case "missing projection release":
				quotaCapacitySeed(t, f, r, false)
				r.Status = domain.ReservationReleased
			case "extension":
				quotaCapacitySeed(t, f, r, true)
				r.ExpiresAt = r.ExpiresAt.Add(time.Minute)
			}
			if err := quotaCapacityApply(t, store, r); err != nil {
				t.Fatal("genuine legacy history changed", err)
			}
			if f.quota[4] != "subscription" {
				t.Fatal("legacy physical subscription changed")
			}
			for _, row := range f.expiry {
				if row[5] != "subscription" || row[6] != nil || row[7] != nil {
					t.Fatal("legacy NULL arm changed")
				}
			}
		})
	}
}

func TestLocalCapacityQuotaWriterRejectsPhysicalQuotaCorruption(t *testing.T) {
	for _, column := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9} {
		t.Run(fmt.Sprint(column), func(t *testing.T) {
			f, store, r := quotaCapacityFixture(t, true)
			quotaCapacitySeed(t, f, r, true)
			switch column {
			case 6:
				f.quota[column] = int64(2)
			case 7, 8:
				f.quota[column] = r.ExpiresAt.Add(time.Minute)
			case 9:
				f.quota[column] = r.HeldAt
			default:
				f.quota[column] = "foreign"
			}
			r.Status = domain.ReservationReleased
			if err := quotaCapacityApply(t, store, r); !errors.Is(err, domain.ErrMembershipDenied) || f.writes != 0 {
				t.Fatalf("corrupt scalar overwritten: column=%d denied=%t writes=%d", column, errors.Is(err, domain.ErrMembershipDenied), f.writes)
			}
		})
	}
}

func TestLocalCapacityQuotaWriterLegacyPresentCollisionDenial(t *testing.T) {
	for _, fault := range []string{"orphan", "foreign run", "partial NULL", "foreign key", "extension target collision"} {
		t.Run(fault, func(t *testing.T) {
			f, store, r := quotaCapacityFixture(t, false)
			key := quotaCapacitySeedExpiry(t, f, r)
			if fault != "orphan" {
				quotaCapacitySeed(t, f, r, false)
			}
			switch fault {
			case "foreign run":
				f.expiry[key][4] = "foreign"
			case "partial NULL":
				f.expiry[key][7] = strings.Repeat("0", 64)
			case "foreign key":
				f.expiry[key][5] = "foreign-subscription"
			case "extension target collision":
				r.ExpiresAt = r.ExpiresAt.Add(time.Minute)
				quotaCapacitySeedExpiry(t, f, r)
			}
			before := cloneLocalInstallationRows(f.expiry)
			if err := quotaCapacityApply(t, store, r); !errors.Is(err, domain.ErrMembershipDenied) || f.writes != 0 || !reflect.DeepEqual(before, f.expiry) {
				t.Fatalf("legacy collision changed original projections: fault=%s denied=%t writes=%d", fault, errors.Is(err, domain.ErrMembershipDenied), f.writes)
			}
		})
	}
}

func TestLocalCapacityQuotaReaderNativeUint32(t *testing.T) {
	for _, arm := range []string{"subscription", "local"} {
		for _, representation := range []string{"units", "bucket", "both", "wrong units", "wrong bucket"} {
			t.Run(arm+"/"+representation, func(t *testing.T) {
				f, store, r := quotaCapacityFixture(t, arm == "local")
				quotaCapacitySeed(t, f, r, true)
				bucket, err := ydbpartition.BucketV1(string(r.ID))
				if err != nil {
					t.Fatal(err)
				}
				key := quotaCapacityExpiryKey(bucket, r.ExpiresAt, string(r.TenantID), string(r.ID))
				if representation != "bucket" && representation != "wrong bucket" {
					f.quota[6] = uint32(r.CapacityUnits)
				}
				if representation != "units" {
					f.expiry[key][0] = uint32(bucket)
				}
				deny := representation == "wrong units" || representation == "wrong bucket"
				if representation == "wrong units" {
					f.quota[6] = uint32(r.CapacityUnits + 1)
				} else if representation == "wrong bucket" {
					f.expiry[key][0] = uint32(bucket ^ 1)
				}
				beforeQuota, beforeExpiry := append([]driver.Value(nil), f.quota...), cloneLocalInstallationRows(f.expiry)
				tx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelSerializable})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = tx.Rollback() })
				state := &stateTx{store: store, sqlTx: tx, tenantID: r.TenantID}
				got, found, err := readWorkerQuotaReservationStorageV1(t.Context(), state, f.run, r.ID)
				if deny {
					if !errors.Is(err, domain.ErrMembershipDenied) || found || !reflect.DeepEqual(got, domain.QuotaReservation{}) {
						t.Fatalf("mismatched native Uint32 accepted: found=%t err=%v", found, err)
					}
				} else if err != nil || !found || !reflect.DeepEqual(got, r) {
					t.Fatalf("native Uint32 quota/projection rejected: found=%t err=%v", found, err)
				}
				wantQueries := 2
				if representation == "wrong units" {
					wantQueries = 1
				}
				if f.queries != wantQueries || f.oldPayloadReads != 0 || f.writes != 0 || f.commits != 0 || !reflect.DeepEqual(beforeQuota, f.quota) || !reflect.DeepEqual(beforeExpiry, f.expiry) {
					t.Fatalf("reader changed query/write contract: queries=%d/%d payloadReads=%d writes=%d commits=%d", f.queries, wantQueries, f.oldPayloadReads, f.writes, f.commits)
				}
			})
		}
	}
}

func TestLocalCapacityQuotaReaderUsesOriginalCallbackBudget(t *testing.T) {
	for _, fault := range []string{"normalized JSONDocument", "invalid first scalar", "oversize payload", "exhausted original budget"} {
		t.Run(fault, func(t *testing.T) {
			f, store, r := quotaCapacityFixture(t, true)
			quotaCapacitySeed(t, f, r, true)
			if fault == "normalized JSONDocument" {
				var document map[string]json.RawMessage
				if err := json.Unmarshal([]byte(f.quota[0].(string)), &document); err != nil {
					t.Fatal(err)
				}
				var association map[string]json.RawMessage
				if err := json.Unmarshal(document["local_capacity_association"], &association); err != nil {
					t.Fatal(err)
				}
				document["local_capacity_association"], _ = json.Marshal(association)
				raw, err := json.MarshalIndent(document, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				f.quota[0] = string(raw)
			} else if fault == "invalid first scalar" {
				f.quota[0] = true
			} else if fault == "oversize payload" {
				f.quota[0] = strings.Repeat("x", ownedAuthorityHeadBytes+1)
			}
			sqlTx, err := store.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelSerializable})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sqlTx.Rollback() })
			budget := &ownedAuthorityBudget{}
			callback, err := budget.begin()
			if err != nil {
				t.Fatal(err)
			}
			defer budget.finish(callback)
			before := uint64(0)
			if fault == "exhausted original budget" {
				before = ownedAuthorityMaxBytes - 1
				if err := budget.materialized(callback, before); err != nil {
					t.Fatal(err)
				}
			}
			q := &ownedAuthorityQuery{tx: sqlTx, budget: budget, callback: callback, at: r.HeldAt}
			state := q.canonicalState(store, r.TenantID)
			got, found, err := readQuotaReservationStorageV1(t.Context(), state, f.run, r.ID)
			var bytes uint64
			for _, v := range f.quota {
				if text, ok := v.(string); ok {
					bytes += uint64(len(text))
				} else {
					bytes += 8
				}
			}
			if fault == "normalized JSONDocument" {
				if err != nil || !found || !reflect.DeepEqual(got, r) {
					t.Fatalf("normalized physical quota lost original correlation: found=%t err=%v", found, err)
				}
			} else if !errors.Is(err, domain.ErrMembershipDenied) || found || !reflect.DeepEqual(got, domain.QuotaReservation{}) {
				t.Fatalf("corrupt/exhausted reader returned partial quota: fault=%s denied=%t found=%t", fault, errors.Is(err, domain.ErrMembershipDenied), found)
			}
			if budget.callbacks != 1 || budget.statements != 1 || f.queries != 1 || f.writes != 0 {
				t.Fatal("reader reset/expanded original callback or performed writes")
			}
			if fault != "exhausted original budget" && budget.bytes != before+bytes {
				t.Fatalf("not all physical columns charged: bytes=%d want=%d", budget.bytes, before+bytes)
			}
			if fault == "exhausted original budget" && !budget.failed {
				t.Fatal("original byte exhaustion was not sticky")
			}
		})
	}
}

func TestLocalCapacityQuotaReaderRejectsForbiddenRawArmPresence(t *testing.T) {
	for _, tc := range []struct {
		name, member string
		local        bool
	}{
		{name: "local_null_subscription", local: true, member: `"subscription_connection_id":null`},
		{name: "local_empty_subscription", local: true, member: `"subscription_connection_id":""`},
		{name: "legacy_null_local", member: `"local_capacity_association":null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, store, reservation := quotaCapacityFixture(t, tc.local)
			quotaCapacitySeed(t, f, reservation, true)
			f.quota[0] = strings.TrimSuffix(f.quota[0].(string), "}") + "," + tc.member + "}"
			beforeQuota, beforeExpiry := append([]driver.Value(nil), f.quota...), cloneLocalInstallationRows(f.expiry)
			if err := quotaCapacityApply(t, store, reservation); !errors.Is(err, domain.ErrMembershipDenied) || f.writes != 0 || f.commits != 0 || !reflect.DeepEqual(beforeQuota, f.quota) || !reflect.DeepEqual(beforeExpiry, f.expiry) {
				t.Fatalf("forbidden selected-arm member accepted: err=%v writes=%d commits=%d", err, f.writes, f.commits)
			}
		})
	}
}

func TestLocalCapacityQuotaWriterRollbackPreservesOriginalProjection(t *testing.T) {
	f, store, r := quotaCapacityFixture(t, true)
	quotaCapacitySeed(t, f, r, true)
	beforeQuota, beforeExpiry := append([]driver.Value(nil), f.quota...), cloneLocalInstallationRows(f.expiry)
	f.failWrite = 2
	r.Status = domain.ReservationReleased
	if err := quotaCapacityApply(t, store, r); err == nil || f.commits != 0 || f.rollbacks != 1 || !reflect.DeepEqual(beforeQuota, f.quota) || !reflect.DeepEqual(beforeExpiry, f.expiry) {
		t.Fatalf("failed write changed original transaction: denied=%t commits=%d rollbacks=%d", err != nil, f.commits, f.rollbacks)
	}
}
