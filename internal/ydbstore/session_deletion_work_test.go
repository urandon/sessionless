package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Real ordinary *sql.Tx + original ownedAuthorityBudget. These tests exercise
// the private bounded carrier only, never a deletion or lifecycle permission.
func TestRetainedDeletionWorkOriginalRunBudgets(t *testing.T) {
	const statement = `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`
	for _, name := range []string{"callbacks", "material", "cancelled", "same_run_set"} {
		t.Run(name, func(t *testing.T) {
			f := &deletionCaptureFixture{statement: statement, columns: 1, point: true}
			tx := deletionCaptureOrdinaryTx(t, f)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			w := newRetainedDeletionWork(ctx)
			defer w.close()
			if _, err := w.forRun("run-a"); err == nil {
				t.Fatal("no SQL transaction/Run admission accepted")
			}
			if err := w.admitRuns([]domain.RunID{"run-a", "run-b"}); err != nil {
				t.Fatal(err)
			}
			w.tx = tx
			if _, err := w.forRun("foreign-run"); err == nil {
				t.Fatal("unadmitted original Run accepted")
			}
			q, err := w.forRun("run-a")
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "callbacks":
				for i := 0; i < ownedAuthorityMaxCallbacks; i++ {
					if i > 0 {
						w.tx = tx
						q, err = w.forRun("run-a")
						if err != nil {
							t.Fatal(err)
						}
					}
					rows, err := q.QueryContext(ctx, statement, "tenant-capture", "session-capture", "owner-capture")
					if err != nil || rows.Close() != nil || w.finishSQL() != nil {
						t.Fatal("original callback", i, err)
					}
				}
				w.tx = tx
				if _, err := w.forRun("run-a"); err == nil || w.runs["run-a"].budget.callbacks != ownedAuthorityMaxCallbacks || f.queries != ownedAuthorityMaxCallbacks {
					t.Fatal("fourth callback reset original budget", err, f.queries)
				}
			case "material":
				if chargeDeletionCaptureSQL(&deletionCaptureQuery{query: q, budget: w.readBudget}, ownedAuthorityMaxBytes-8) != nil {
					t.Fatal("original Run's remaining envelope denied")
				}
				if w.readBudget.bytes != 0 || w.runs["run-a"].budget.bytes != ownedAuthorityMaxBytes {
					t.Fatal("common and original Run material budgets conflated")
				}
				if chargeDeletionCaptureSQL(&deletionCaptureQuery{query: q, budget: w.readBudget}, 1) == nil {
					t.Fatal("first over original Run envelope accepted")
				}
				other, err := w.forRun("run-b")
				if err != nil || other.run.budget.bytes != 8 {
					t.Fatal("Run allowance transferred/reset", err)
				}
				_ = w.finishSQL()
				w.tx = tx
				if _, err := w.forRun("run-a"); err == nil {
					t.Fatal("failed original Run budget reset between SQL phases")
				}
			case "cancelled":
				cancel()
				rows, err := q.QueryContext(context.Background(), statement, "tenant-capture", "session-capture", "owner-capture")
				if rows != nil {
					_ = rows.Close()
				}
				if err == nil || f.queries != 0 || w.finishSQL() == nil {
					t.Fatal("query relay ignored original cancellation", err, f.queries)
				}
			case "same_run_set":
				if w.admitRuns([]domain.RunID{"run-b", "run-a"}) != nil || w.admitRuns([]domain.RunID{"run-a", "run-c"}) == nil {
					t.Fatal("original discovery identity changed across capture phases")
				}
			}
		})
	}
}

type retainedDeletionHeldCommit struct {
	ctx              context.Context
	entered, release chan struct{}
	cooperative      bool
}

func (f *retainedDeletionHeldCommit) Driver() driver.Driver                        { return ownedAdmissionJoinDriver{} }
func (f *retainedDeletionHeldCommit) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *retainedDeletionHeldCommit) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("held Commit fixture forbids Prepare")
}
func (f *retainedDeletionHeldCommit) Close() error { return nil }
func (f *retainedDeletionHeldCommit) Begin() (driver.Tx, error) {
	return nil, errors.New("held Commit fixture requires BeginTx")
}
func (f *retainedDeletionHeldCommit) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	f.ctx = ctx
	return &retainedDeletionHeldTx{f}, nil
}
func (f *retainedDeletionHeldCommit) QueryContext(_ context.Context, s string, _ []driver.NamedValue) (driver.Rows, error) {
	if s != `SELECT CurrentUtcTimestamp()` {
		return nil, errors.New("held Commit exact clock only")
	}
	return &retainedDeletionWriterRows{rows: [][]driver.Value{{time.Now().UTC().Truncate(time.Microsecond)}}, columns: 1}, nil
}

type retainedDeletionHeldTx struct{ f *retainedDeletionHeldCommit }

func (tx *retainedDeletionHeldTx) Commit() error {
	close(tx.f.entered)
	if tx.f.cooperative {
		<-tx.f.ctx.Done()
		return tx.f.ctx.Err()
	}
	<-tx.f.release
	return nil
}
func (*retainedDeletionHeldTx) Rollback() error { return nil }

func TestRetainedDeletionWorkFirstCaptureCommitDeadline(t *testing.T) {
	for _, cooperative := range []bool{false, true} {
		name := "late_driver_nil"
		if cooperative {
			name = "context_linked_driver"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := &retainedDeletionHeldCommit{entered: make(chan struct{}), release: make(chan struct{}), cooperative: cooperative}
				db := sql.OpenDB(f)
				defer db.Close()
				callctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				w := newRetainedDeletionWork(callctx)
				defer w.close()
				done := make(chan error, 1)
				go func() {
					done <- w.transaction(&Store{db: db}, "tenant-capture", func(*stateTx, time.Time) error {
						if err := w.admitRuns([]domain.RunID{"run-a"}); err != nil {
							return err
						}
						_, err := w.forRun("run-a")
						return err
					})
				}()
				<-f.entered
				if f.ctx.Err() != nil {
					t.Fatal("first Commit already cancelled")
				}
				time.Sleep(ownedAuthorityDeadline + time.Second) // Virtual synctest clock, no wall sleep.
				synctest.Wait()
				linked := f.ctx.Err() != nil
				if !linked {
					cancel()
				} // Release even a regressed cooperative driver before failing.
				close(f.release)
				err := <-done
				if !linked {
					t.Fatal("late-discovered Run deadline did not cancel the SAME BeginTx context")
				}
				if err == nil {
					t.Fatal("Commit returning after original deadline reported verifier success")
				}
			})
		})
	}
}

type retainedDeletionStatementValue struct{}
type retainedDeletionHeldStatement struct {
	retainedDeletionHeldCommit
	statementContext chan context.Context
}

func (f *retainedDeletionHeldStatement) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *retainedDeletionHeldStatement) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	if s == `SELECT CurrentUtcTimestamp()` {
		return f.retainedDeletionHeldCommit.QueryContext(ctx, s, args)
	}
	if s != "held query" {
		return nil, errors.New("unexpected held statement")
	}
	f.statementContext <- ctx
	<-ctx.Done()
	return nil, ctx.Err()
}
func (f *retainedDeletionHeldStatement) ExecContext(ctx context.Context, s string, _ []driver.NamedValue) (driver.Result, error) {
	if s != "held exec" {
		return nil, errors.New("unexpected held write")
	}
	f.statementContext <- ctx
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestRetainedDeletionWorkCommonStatementCancellation(t *testing.T) {
	for _, operation := range []string{"query", "exec", "row"} {
		t.Run(operation, func(t *testing.T) {
			func() {
				f := &retainedDeletionHeldStatement{statementContext: make(chan context.Context, 1)}
				db := sql.OpenDB(f)
				defer db.Close()
				outer, cancel := context.WithCancel(t.Context())
				defer cancel()
				w := newRetainedDeletionWork(outer)
				defer w.close()
				done := make(chan error, 1)
				go func() {
					done <- w.transaction(&Store{db: db}, "tenant-capture", func(tx *stateTx, _ time.Time) error {
						if err := w.admitRuns([]domain.RunID{"run-a"}); err != nil {
							return err
						}
						if _, err := w.forRun("run-a"); err != nil {
							return err
						}
						statement := context.WithValue(outer, retainedDeletionStatementValue{}, "original value")
						switch operation {
						case "query":
							_, err := tx.sqlTx.QueryContext(statement, "held query")
							return err
						case "row":
							var value string
							return tx.sqlTx.QueryRowContext(statement, "held query").Scan(&value)
						default:
							_, err := tx.sqlTx.ExecContext(statement, "held exec")
							return err
						}
					})
				}()
				statement := <-f.statementContext
				if statement.Value(retainedDeletionStatementValue{}) != "original value" || statement.Err() != nil {
					t.Fatal("statement values/lifetime lost before original deadline")
				}
				// Explicitly cancel the admitted ORIGINAL Run. The separate
				// synctest Commit test proves its deadline triggers this same
				// cancellation relay. No sleep decides the result here.
				w.runs["run-a"].cancel()
				select {
				case <-statement.Done():
				case <-time.After(5 * time.Second): // Deadlock watchdog, not an elapsed-time assertion.
					cancel()
					<-done
					t.Fatal("original Run cancellation did not cancel in-flight common SQL statement")
				}
				cancel()
				if err := <-done; err == nil {
					t.Fatal("cancelled statement accepted")
				}
			}()
		})
	}
}
