package ydbpartition

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// A test-owned SQL connector exercises the real transaction/query sequence
// without a mutable schema, credentials, external clock, or database process.
type harnessV2CutoverSQL struct {
	markers                         map[string]time.Time
	markerID, markerTable           string
	clock                           time.Time
	failQuery, failExec, failCommit bool
	filled                          string
	queries                         []string
	beginOptions                    driver.TxOptions
	writes, commits, rollbacks      int
}

func (fixture *harnessV2CutoverSQL) Connect(context.Context) (driver.Conn, error) {
	return fixture, nil
}
func (fixture *harnessV2CutoverSQL) Driver() driver.Driver { return harnessV2CutoverDriver{} }
func (fixture *harnessV2CutoverSQL) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (fixture *harnessV2CutoverSQL) Close() error { return nil }
func (fixture *harnessV2CutoverSQL) Begin() (driver.Tx, error) {
	return nil, errors.New("expected explicit serializable BeginTx")
}
func (fixture *harnessV2CutoverSQL) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	fixture.beginOptions = options
	return fixture, nil
}
func (fixture *harnessV2CutoverSQL) Commit() error {
	fixture.commits++
	if fixture.failCommit {
		return errors.New("scripted commit failure")
	}
	return nil
}
func (fixture *harnessV2CutoverSQL) Rollback() error { fixture.rollbacks++; return nil }
func (fixture *harnessV2CutoverSQL) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	fixture.queries = append(fixture.queries, query)
	if fixture.failQuery {
		return nil, errors.New("scripted query failure")
	}
	if query == "SELECT CurrentUtcTimestamp()" {
		return &harnessV2CutoverRows{column: "clock", value: fixture.clock}, nil
	}
	if strings.HasPrefix(query, "SELECT completed_at") {
		if query != "SELECT completed_at FROM "+fixture.expectedTable()+" WHERE cutover_id=$1" ||
			len(args) != 1 || args[0].Value != fixture.expectedMarker() {
			return nil, errors.New("successor did not select its exact additive marker")
		}
		if at, found := fixture.markers[args[0].Value.(string)]; found {
			return &harnessV2CutoverRows{column: "completed_at", value: at}, nil
		}
		return &harnessV2CutoverRows{column: "completed_at", done: true}, nil
	}
	for _, table := range []string{"dispatch_outbox", "worker_jobs", "attempt_effect_reservations"} {
		if query == "SELECT tenant_id FROM "+table+" LIMIT 1" {
			return &harnessV2CutoverRows{column: "tenant_id", value: "test-tenant", done: fixture.filled != table}, nil
		}
	}
	return nil, errors.New("unexpected successor query: " + query)
}
func (fixture *harnessV2CutoverSQL) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if query != "UPSERT INTO "+fixture.expectedTable()+" (cutover_id,completed_at) VALUES ($1,$2)" || len(args) != 2 || args[0].Value != fixture.expectedMarker() {
		return nil, errors.New("unexpected successor marker write")
	}
	if fixture.failExec {
		return nil, errors.New("scripted write failure")
	}
	if fixture.markerID != "" && (args[1].Value != fixture.clock.UTC() || fixture.clock.IsZero()) {
		return nil, errors.New("wrong database clock")
	}
	fixture.writes++
	return driver.RowsAffected(1), nil
}

func (fixture *harnessV2CutoverSQL) expectedMarker() string {
	if fixture.markerID != "" {
		return fixture.markerID
	}
	return "harness-binding-v2-empty-cutover"
}
func (fixture *harnessV2CutoverSQL) expectedTable() string {
	if fixture.markerTable != "" {
		return fixture.markerTable
	}
	return "harness_binding_cutover_state"
}

type harnessV2CutoverDriver struct{}

func (harnessV2CutoverDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("connector required")
}

type harnessV2CutoverRows struct {
	column string
	value  driver.Value
	done   bool
}

func (rows *harnessV2CutoverRows) Columns() []string { return []string{rows.column} }
func (rows *harnessV2CutoverRows) Close() error      { return nil }
func (rows *harnessV2CutoverRows) Next(values []driver.Value) error {
	if rows.done {
		return io.EOF
	}
	rows.done = true
	values[0] = rows.value
	return nil
}

func TestHarnessBindingV2CutoverRequiresEmptyAuthorityBacklog(t *testing.T) {
	t.Parallel()
	for _, table := range []string{"dispatch_outbox", "worker_jobs", "attempt_effect_reservations"} {
		t.Run(table, func(t *testing.T) {
			fixture := &harnessV2CutoverSQL{filled: table, markers: map[string]time.Time{"harness-binding-v1-empty-cutover": time.Unix(10, 0).UTC()}}
			db := sql.OpenDB(fixture)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("close fixture DB: %v", err)
				}
			})
			if _, err := verifyHarnessBindingV2Cutover(context.Background(), db, false); err == nil || !strings.Contains(err.Error(), table) {
				t.Fatalf("filled %s successor cutover error=%v, want bounded table-specific denial", table, err)
			}
			if fixture.beginOptions.Isolation != driver.IsolationLevel(sql.LevelSerializable) || fixture.writes != 0 || fixture.commits != 0 || fixture.rollbacks != 1 {
				t.Fatalf("denied cutover mutated authority: isolation=%v writes=%d commits=%d rollbacks=%d", fixture.beginOptions.Isolation, fixture.writes, fixture.commits, fixture.rollbacks)
			}
		})
	}
}

func TestHarnessBindingV2CutoverCommitsOnlySuccessorMarkerAfterAllChecks(t *testing.T) {
	t.Parallel()
	for _, dryRun := range []bool{true, false} {
		name := "commit"
		if dryRun {
			name = "dry_run"
		}
		t.Run(name, func(t *testing.T) {
			fixture := &harnessV2CutoverSQL{markers: map[string]time.Time{"harness-binding-v1-empty-cutover": time.Unix(10, 0).UTC()}}
			db := sql.OpenDB(fixture)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("close fixture DB: %v", err)
				}
			})
			result, err := verifyHarnessBindingV2Cutover(context.Background(), db, dryRun)
			if err != nil || result.Table != "harness_binding_cutover_v2" {
				t.Fatalf("successor result=%+v err=%v", result, err)
			}
			wantWrites := 1
			if dryRun {
				wantWrites = 0
			}
			if len(fixture.queries) != 4 || fixture.beginOptions.Isolation != driver.IsolationLevel(sql.LevelSerializable) || fixture.writes != wantWrites || fixture.commits != wantWrites {
				t.Fatalf("successor checks=%v isolation=%v writes=%d commits=%d, want all 4 checks and %d writes/commits", fixture.queries, fixture.beginOptions.Isolation, fixture.writes, fixture.commits, wantWrites)
			}
		})
	}
}

func TestHarnessBindingV2CutoverExistingMarkerIsRequiredAndIdempotent(t *testing.T) {
	t.Parallel()
	for _, at := range []time.Time{time.Unix(10, 0).UTC(), {}} {
		fixture := &harnessV2CutoverSQL{markers: map[string]time.Time{"harness-binding-v2-empty-cutover": at}, filled: "worker_jobs"}
		db := sql.OpenDB(fixture)
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Errorf("close fixture DB: %v", err)
			}
		})
		_, err := verifyHarnessBindingV2Cutover(context.Background(), db, false)
		if (err != nil) != at.IsZero() {
			t.Errorf("existing marker at=%v error=%v, want error iff zero", at, err)
		}
		if len(fixture.queries) != 1 || fixture.writes != 0 || fixture.commits != 0 {
			t.Errorf("existing marker mutated or rechecked live backlog: queries=%v writes=%d commits=%d", fixture.queries, fixture.writes, fixture.commits)
		}
	}
}
