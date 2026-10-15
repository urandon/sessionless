package ydbpartition

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"
)

// Exercises the real remaining bootstrap path. No native database, schema or
// deployment drain proof is supplied by this test-owned connector.
type v2OnlyBackfillSQL struct {
	queries, writes []string
	failSource      bool
}

func (f *v2OnlyBackfillSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (*v2OnlyBackfillSQL) Driver() driver.Driver                          { return harnessV2CutoverDriver{} }
func (*v2OnlyBackfillSQL) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare forbidden")
}
func (*v2OnlyBackfillSQL) Close() error { return nil }
func (*v2OnlyBackfillSQL) Begin() (driver.Tx, error) {
	return nil, errors.New("explicit transaction required")
}
func (f *v2OnlyBackfillSQL) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if opts.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("serializable required")
	}
	return v2OnlyBackfillTx{}, nil
}

type v2OnlyBackfillTx struct{}

func (v2OnlyBackfillTx) Commit() error   { return nil }
func (v2OnlyBackfillTx) Rollback() error { return nil }
func (f *v2OnlyBackfillSQL) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f.queries = append(f.queries, q)
	for _, field := range strings.Fields(q) {
		switch field {
		case "lease_expiry", "dispatch_ready", "quota_expiry", "telegram_delivery_ready":
			return nil, errors.New("legacy index read forbidden")
		}
	}
	if strings.HasPrefix(q, "SELECT completed_at") {
		return &harnessV2CutoverRows{column: "completed_at", value: time.Unix(10, 0).UTC()}, nil
	}
	if f.failSource {
		return nil, errors.New("source scan failed")
	}
	return &harnessV2CutoverRows{column: "empty", done: true}, nil
}
func (f *v2OnlyBackfillSQL) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f.writes = append(f.writes, q)
	if q != "UPSERT INTO session_lifecycle_backfill_state (backfill_id, completed_at) VALUES ($1, $2)" || len(args) != 2 || args[0].Value != "session-lifecycle-indexes-v1" {
		return nil, errors.New("unexpected bootstrap mutation")
	}
	return driver.RowsAffected(1), nil
}
func TestSchemaBackfillDoesNotReadRetiredReadyExpiry(t *testing.T) {
	for _, dry := range []bool{true, false} {
		name := "apply"
		if dry {
			name = "dry_run"
		}
		t.Run(name, func(t *testing.T) {
			f := &v2OnlyBackfillSQL{}
			db := sql.OpenDB(f)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			results, err := BackfillSchemaIndexes(t.Context(), db, dry)
			if err != nil {
				t.Fatal(err)
			}
			if len(results) != 12 {
				t.Fatalf("remaining backfill results=%d, want five cutovers and seven lifecycle projections", len(results))
			}
			for _, r := range results {
				switch r.Table {
				case "lease_expiry_v2", "dispatch_ready_v2", "quota_expiry_v2", "telegram_delivery_ready_v2":
					t.Errorf("implicit ready/expiry backfill survived: %s", r.Table)
				}
			}
			wantWrites := 1
			if dry {
				wantWrites = 0
			}
			if len(f.writes) != wantWrites {
				t.Fatalf("writes=%v, want %d", f.writes, wantWrites)
			}
		})
	}
}
func TestSchemaBackfillSourceFailureDoesNotWriteCompletion(t *testing.T) {
	f := &v2OnlyBackfillSQL{failSource: true}
	db := sql.OpenDB(f)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := BackfillSchemaIndexes(t.Context(), db, false); err == nil || len(f.writes) != 0 {
		t.Fatalf("error=%v completion writes=%v", err, f.writes)
	}
}
