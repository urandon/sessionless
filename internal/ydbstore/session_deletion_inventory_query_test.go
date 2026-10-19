package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Strict SQL-only fixture: no objects, issuer, writer or erasure receipt exists.
// Tests exercise the SAME production inventory through DB and transaction, not
// native-YDB snapshot isolation or retained-writer/destructive eligibility.
type deletionInventorySQL struct {
	deletion                domain.SessionDeletion
	event                   domain.SessionEvent
	active                  bool
	mode                    string
	begins, outside, inside int
}
type deletionInventoryConn struct{ f *deletionInventorySQL }

func (f *deletionInventorySQL) Connect(context.Context) (driver.Conn, error) {
	return &deletionInventoryConn{f}, nil
}
func (*deletionInventorySQL) Driver() driver.Driver { return ownedAdmissionJoinDriver{} }
func (*deletionInventoryConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("inventory fixture forbids Prepare")
}
func (*deletionInventoryConn) Close() error { return nil }
func (*deletionInventoryConn) Begin() (driver.Tx, error) {
	return nil, errors.New("explicit BeginTx required")
}
func (c *deletionInventoryConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if c.f.active {
		return nil, errors.New("nested transaction forbidden")
	}
	c.f.active = true
	c.f.begins++
	return &deletionInventoryTx{f: c.f}, nil
}
func (*deletionInventoryConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("inventory fixture forbids all mutations")
}

type deletionInventoryTx struct{ f *deletionInventorySQL }

func (tx *deletionInventoryTx) Commit() error   { tx.f.active = false; return nil }
func (tx *deletionInventoryTx) Rollback() error { tx.f.active = false; return nil }

type deletionInventoryRows struct {
	values   [][]driver.Value
	columns  int
	closeErr error
}

func (r *deletionInventoryRows) Columns() []string {
	names := make([]string, r.columns)
	for i := range names {
		names[i] = "cell"
	}
	return names
}
func (r *deletionInventoryRows) Close() error { return r.closeErr }
func (r *deletionInventoryRows) Next(dst []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(dst, r.values[0])
	r.values = r.values[1:]
	return nil
}
func (c *deletionInventoryConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f := c.f
	if f.active {
		f.inside++
	} else {
		f.outside++
	}
	s := strings.Join(strings.Fields(statement), " ")
	r := &deletionInventoryRows{columns: 1}
	switch {
	case strings.HasPrefix(s, "SELECT record FROM session_deletions "):
		if f.active && f.mode == "missing_request" {
			return r, nil
		}
		b, _ := json.Marshal(f.deletion)
		r.values = [][]driver.Value{{string(b)}}
	case strings.HasPrefix(s, "SELECT completed_at FROM session_lifecycle_backfill_state "):
		r.values = [][]driver.Value{{f.deletion.RequestedAt}}
	case strings.HasPrefix(s, "SELECT record FROM session_events "):
		e := f.event
		if f.active && f.mode == "same_count_changed_ref" {
			e.Payload.SHA256 = strings.Repeat("a", 64)
		}
		b, _ := json.Marshal(e)
		r.values = [][]driver.Value{{string(b)}}
		if f.active && f.mode == "close_error" {
			r.closeErr = errors.New("injected range Close failure")
		}
	case strings.HasPrefix(s, "SELECT SUBSTRING(CAST(attempt_id AS String)"):
		r.columns = 2
		if f.active && f.mode == "extra_admission" {
			r.values = [][]driver.Value{{"extra-attempt", "extra-run"}}
		}
	case strings.HasPrefix(s, "SELECT record FROM session_snapshots "),
		strings.HasPrefix(s, "SELECT session_id FROM session_material_current_v1 "),
		strings.HasPrefix(s, "SELECT sequence FROM session_material_boundaries_v1 "),
		strings.HasPrefix(s, "SELECT snapshot_version FROM session_snapshot_material_v1 "),
		strings.HasPrefix(s, "SELECT user_id FROM session_participants "),
		strings.HasPrefix(s, "SELECT binding_id FROM frontend_bindings_by_session "),
		strings.HasPrefix(s, "SELECT frontend_projection_id FROM frontend_projections_by_session "),
		strings.HasPrefix(s, "SELECT run_id FROM runs_by_session "):
	default:
		return nil, errors.New("unexpected inventory query: " + s)
	}
	return r, nil
}

func TestSessionDeletionInventorySameQueryTransactionRejoin(t *testing.T) {
	run := currentWorkerAuthorityFixture(t).Run
	if err := run.Transition(domain.RunSucceeded, run.CreatedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	records, _, _, _ := markerLegacyComposerData(t, run, "query-inventory-manifest")
	for _, mode := range []string{"same", "same_count_changed_ref", "extra_admission", "missing_request", "close_error"} {
		t.Run(mode, func(t *testing.T) {
			f := &deletionInventorySQL{event: records[len(records)-1].Event, mode: mode, deletion: domain.SessionDeletion{TenantID: run.TenantID, SessionID: run.SessionID, RequestedBy: "user-delete-query", Reason: "exact SQL inventory", State: domain.SessionDeletionRequested, RequestedAt: run.CreatedAt}}
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
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			inventory, err := store.BuildSessionDeletionInventory(ctx, run.TenantID, run.SessionID, maxSessionDeletionRows, maxSessionDeletionRows)
			if err != nil || len(inventory.Objects) != 1 {
				t.Fatal("original Plan inventory", err, len(inventory.Objects))
			}
			outside := f.outside
			tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			err = rejoinSessionDeletionInventoryTx(ctx, &stateTx{store: store, sqlTx: tx, tenantID: run.TenantID}, inventory)
			if (err == nil) != (mode == "same") {
				t.Fatal("exact transaction rejoin", mode, err)
			}
			if f.outside != outside || f.inside == 0 || f.begins != 1 {
				t.Fatalf("escaped SAME transaction: outside=%d/%d inside=%d begins=%d", f.outside, outside, f.inside, f.begins)
			}
		})
	}
}
