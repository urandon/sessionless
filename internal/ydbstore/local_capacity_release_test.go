package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// The real common terminal scheduling hook over a strict scripted SQL peer.
// No current Source/subscription/funding query is supported: retained exact
// original slot ownership must suffice for release, not for a fresh admission.
type localCapacityReleaseSQL struct {
	key       string
	slot      []driver.Value
	writes    []string
	arguments [][]driver.NamedValue
	queue     int64
	active    int64
	at        time.Time
}

func (f *localCapacityReleaseSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (*localCapacityReleaseSQL) Driver() driver.Driver                          { return explanationFixtureDriver{} }
func (*localCapacityReleaseSQL) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare forbidden")
}
func (*localCapacityReleaseSQL) Close() error { return nil }
func (*localCapacityReleaseSQL) Begin() (driver.Tx, error) {
	return nil, errors.New("explicit transaction required")
}
func (*localCapacityReleaseSQL) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return legacyCapacitySQLTx{}, nil
}
func (f *localCapacityReleaseSQL) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	var row []driver.Value
	switch {
	case strings.Contains(statement, "FROM subscription_scheduler_slots"):
		if len(args) != 2 || args[0].Value != "tenant-release" || args[1].Value != f.key || !strings.Contains(statement, "local_capacity_association") || !strings.Contains(statement, "local_capacity_digest") {
			return nil, errors.New("wrong physical local slot point read")
		}
		row = f.slot
	case strings.Contains(statement, "FROM tenant_scheduler_counters"):
		row = []driver.Value{f.queue, f.active}
	case strings.Contains(statement, "FROM lease_heads"):
		row = []driver.Value{"lease-release", int64(7), f.at.Add(time.Minute)}
	default:
		return nil, errors.New("unexpected authority/subscription query during retained release")
	}
	return &explanationFixtureRows{values: row}, nil
}
func (f *localCapacityReleaseSQL) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f.writes = append(f.writes, strings.Join(strings.Fields(statement), " "))
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	if strings.Contains(statement, "UPSERT INTO subscription_scheduler_slots") {
		if len(args) != 9 || args[0].Value != "tenant-release" || args[1].Value != f.key {
			return nil, errors.New("wrong physical local slot write")
		}
		f.slot = []driver.Value{args[2].Value, args[3].Value, args[4].Value, args[5].Value, args[6].Value, args[7].Value, args[8].Value}
	}
	return driver.RowsAffected(1), nil
}

func localCapacityReleaseFixture(t *testing.T) (*stateTx, *localCapacityReleaseSQL, domain.Run, domain.QuotaReservation) {
	t.Helper()
	at := time.Date(2026, 10, 20, 1, 0, 0, 0, time.UTC)
	local := domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: "tenant-release", OwnerUserID: "owner-release", ResourceID: "resource-release"}
	physical, err := encodeCapacityStorageV1(local.TenantID, "", &local)
	if err != nil {
		t.Fatal(err)
	}
	run := domain.Run{ID: "run-release", TenantID: local.TenantID, SessionID: "session-release", TriggerEventID: "event-release", LocalCapacityAssociation: &local, Status: domain.RunSucceeded, IdempotencyKey: "intent-release", CreatedAt: at.Add(-time.Minute), UpdatedAt: at, FinishedAt: &at}
	reservation := domain.QuotaReservation{ID: "reservation-release", TenantID: run.TenantID, RunID: run.ID, LocalCapacityAssociation: &local, Status: domain.ReservationCommitted, CapacityUnits: 1, HeldAt: run.CreatedAt, ExpiresAt: at.Add(time.Minute), UpdatedAt: at}
	if err := reservation.ValidateForRun(run); err != nil {
		t.Fatalf("invalid controlled terminal fixture: %v", err)
	}
	f := &localCapacityReleaseSQL{key: physical.key, slot: []driver.Value{string(domain.SchedulerPressured), string(run.ID), string(reservation.ID), time.Unix(0, 0).UTC(), run.CreatedAt, physical.association.String, physical.digest.String}, queue: 2, active: 1, at: at}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return &stateTx{sqlTx: tx, tenantID: run.TenantID, store: &Store{db: db}}, f, run, reservation
}

func TestLocalCapacityCommonTerminalReleaseUsesRetainedOwnership(t *testing.T) {
	tx, f, run, reservation := localCapacityReleaseFixture(t)
	if err := finishWorkerScheduling(t.Context(), tx, run, reservation, "lease-release", 7, f.at); err != nil {
		t.Fatalf("common local terminal release: %v", err)
	}
	if len(f.writes) != 4 || !strings.HasPrefix(f.writes[0], "UPSERT INTO subscription_scheduler_slots") || !strings.HasPrefix(f.writes[1], "UPDATE tenant_scheduler_counters") || !strings.HasPrefix(f.writes[2], "DELETE FROM lease_expiry_v2") || !strings.HasPrefix(f.writes[3], "DELETE FROM lease_heads") {
		t.Fatalf("common terminal scheduling graph: %v", f.writes)
	}
	if f.slot[0] != string(domain.SchedulerReauthRequired) || f.slot[1] != "" || f.slot[2] != "" || f.arguments[1][0].Value != int64(2) && f.arguments[1][0].Value != uint32(2) || f.arguments[1][1].Value != int64(0) && f.arguments[1][1].Value != uint32(0) {
		t.Fatal("released local capacity changed queued ownership or became ready without current Source")
	}
	// A duplicate helper call is not an idempotent new release: original terminal
	// replay must return before it, rather than decrementing counters again.
	writes := len(f.writes)
	if err := releaseLocalCapacitySlotTx(t.Context(), tx, run, reservation, f.at); !errors.Is(err, domain.ErrMembershipDenied) || len(f.writes) != writes {
		t.Fatalf("unowned repeat release: denied=%t writes=%d/%d", errors.Is(err, domain.ErrMembershipDenied), len(f.writes), writes)
	}
}

func TestLocalCapacityCommonTerminalRejectsForeignOrMissingSlot(t *testing.T) {
	for _, fault := range []string{"missing", "run", "reservation", "association", "digest", "partial-null"} {
		t.Run(fault, func(t *testing.T) {
			tx, f, run, reservation := localCapacityReleaseFixture(t)
			switch fault {
			case "missing":
				f.slot = nil
			case "run":
				f.slot[1] = "run-foreign"
			case "reservation":
				f.slot[2] = "reservation-foreign"
			case "association":
				f.slot[5] = strings.Replace(f.slot[5].(string), "owner-release", "owner-foreign", 1)
			case "digest":
				f.slot[6] = strings.Repeat("0", 64)
			case "partial-null":
				f.slot[5] = nil
			}
			if err := finishWorkerScheduling(t.Context(), tx, run, reservation, "lease-release", 7, f.at); err == nil || len(f.writes) != 0 {
				t.Fatalf("foreign/missing slot mutated release graph: rejected=%t writes=%d", err != nil, len(f.writes))
			}
		})
	}
}
