package ydbstore

import (
	"context"
	"database/sql"
	"reflect"
	"sort"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// One logical retained-deletion verification. The same original per-Run
// budgets survive metadata capture, historical writer/body validation and the
// final Start transaction. No active Source, lease or send authority is made.
type retainedDeletionWork struct {
	ctx          context.Context
	runs         map[domain.RunID]*retainedDeletionRunWork
	readBudget   *deletionCaptureReadBudget
	originalRuns []domain.RunID
	admitted     bool
	tx           *sql.Tx
	at           time.Time
	txCancel     context.CancelFunc
	txStops      []func() bool
}
type retainedDeletionRunWork struct {
	ctx    context.Context
	cancel context.CancelFunc
	budget *ownedAuthorityBudget
	query  *ownedAuthorityQuery
}

func newRetainedDeletionWork(ctx context.Context) *retainedDeletionWork {
	return &retainedDeletionWork{ctx: ctx, runs: map[domain.RunID]*retainedDeletionRunWork{}, readBudget: &deletionCaptureReadBudget{ceiling: ownedAuthorityMaxBytes}}
}
func (w *retainedDeletionWork) close() {
	for _, run := range w.runs {
		run.cancel()
	}
}
func (w *retainedDeletionWork) admitRuns(ids []domain.RunID) error {
	ordered := append([]domain.RunID(nil), ids...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	if w.admitted {
		if !reflect.DeepEqual(ordered, w.originalRuns) {
			return domain.ErrMembershipDenied
		}
		return nil
	}
	// Common Session inventory keeps its original 4MiB envelope. Run-specific
	// reads/body copies spend their OWN original 4MiB budget; unused Run credit
	// may not silently enlarge common discovery or another producing Run.
	if len(ordered) > int(maxSessionDeletionRows) || w.readBudget.failed {
		return domain.ErrMembershipDenied
	}
	w.readBudget.admitted = true
	w.originalRuns = ordered
	w.admitted = true
	return nil
}

// Query relay binds the SQL operation itself to the original 10-second Run
// context. Merely checking a timeout after an unbounded DB call is insufficient.
type retainedDeletionRunQuery struct{ run *retainedDeletionRunWork }

func (q *retainedDeletionRunQuery) QueryContext(_ context.Context, s string, args ...any) (*sql.Rows, error) {
	return q.run.query.QueryContext(q.run.ctx, s, args...)
}
func (q *retainedDeletionRunQuery) QueryRowContext(_ context.Context, s string, args ...any) *sql.Row {
	return q.run.query.QueryRowContext(q.run.ctx, s, args...)
}
func (q *retainedDeletionRunQuery) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, domain.ErrMembershipDenied
}

func (w *retainedDeletionWork) forRun(id domain.RunID) (*retainedDeletionRunQuery, error) {
	if w.tx == nil || id.Validate() != nil || !w.admitted {
		return nil, domain.ErrMembershipDenied
	}
	found := false
	for _, original := range w.originalRuns {
		if id == original {
			found = true
			break
		}
	}
	if !found {
		return nil, domain.ErrMembershipDenied
	}
	run := w.runs[id]
	if run == nil {
		ctx, cancel := ownedAuthorityCallContext(w.ctx)
		run = &retainedDeletionRunWork{ctx: ctx, cancel: cancel, budget: ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)}
		w.runs[id] = run
	}
	if run.ctx.Err() != nil || run.budget.check() != nil {
		return nil, domain.ErrMembershipDenied
	}
	if run.query == nil {
		// Runs in first capture are discovered AFTER BeginTx. Their original
		// deadlines must cancel this already-open transaction, including Commit.
		if w.txCancel != nil {
			w.txStops = append(w.txStops, context.AfterFunc(run.ctx, w.txCancel))
		}
		callback, err := run.budget.begin()
		if err != nil {
			return nil, err
		}
		run.query = &ownedAuthorityQuery{tx: w.tx, budget: run.budget, callback: callback, at: w.at}
		// The callback uses this SAME transaction's shared DB clock sample.
		// Account its statement/material even though SQL fetched it only once.
		if run.budget.statement(run.ctx, callback) != nil || run.budget.materialized(callback, 8) != nil {
			return nil, domain.ErrMembershipDenied
		}
	}
	return &retainedDeletionRunQuery{run: run}, nil
}

func (w *retainedDeletionWork) finishSQL() error {
	var err error
	for _, run := range w.runs {
		if run.query != nil {
			run.budget.finish(run.query.callback)
			run.query = nil
		}
		if run.ctx.Err() != nil || run.budget.check() != nil {
			err = domain.ErrMembershipDenied
		}
	}
	w.tx = nil
	return err
}

// No hidden SDK retry resets a logical budget. Unknown commit acknowledgement
// is returned to the existing lifecycle caller, which reads the same durable
// tombstone before deciding whether body verification or SQL-only resume applies.
func (w *retainedDeletionWork) transaction(store *Store, tenant domain.TenantID, fn func(*stateTx, time.Time) error) (err error) {
	if w == nil || w.ctx == nil || w.ctx.Err() != nil || w.tx != nil || store == nil || store.db == nil || fn == nil {
		return domain.ErrMembershipDenied
	}
	// A final transaction/commit cannot outlive a producing Run's ORIGINAL
	// deadline merely because its common Session work uses an outer context.
	txctx, txcancel := context.WithCancel(w.ctx)
	w.txCancel = txcancel
	defer func() {
		for _, stop := range w.txStops {
			stop()
		}
		w.txStops = nil
		w.txCancel = nil
		txcancel()
	}()
	var earliest time.Time
	for _, run := range w.runs {
		if deadline, ok := run.ctx.Deadline(); ok && (earliest.IsZero() || deadline.Before(earliest)) {
			earliest = deadline
		}
	}
	if !earliest.IsZero() {
		var cancel context.CancelFunc
		txctx, cancel = context.WithDeadline(txctx, earliest)
		defer cancel()
	}
	tx, err := store.db.BeginTx(txctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var at time.Time
	if tx.QueryRowContext(txctx, `SELECT CurrentUtcTimestamp()`).Scan(&at) != nil || w.readBudget.charge(8) != nil || at.IsZero() || at.Year() < 1 || at.Year() > 9999 || at.Nanosecond()%1000 != 0 {
		return domain.ErrMembershipDenied
	}
	w.tx, w.at = tx, at.UTC()
	defer func() {
		if w.tx != nil {
			_ = w.finishSQL()
		}
	}()
	err = fn(&stateTx{store: store, sqlTx: tx, tenantID: tenant}, w.at)
	finished := w.finishSQL()
	if err != nil {
		return err
	}
	if finished != nil {
		return finished
	}
	if w.ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	commitErr := tx.Commit()
	// driver.Tx.Commit has no context argument. A non-cooperative/late driver
	// can return nil AFTER cancellation; that is not a successful verifier call.
	// Preserve ambiguity: do not claim rollback or automatically replay writes.
	if commitErr != nil {
		return commitErr
	}
	if txctx.Err() != nil || w.ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	for _, run := range w.runs {
		if run.ctx.Err() != nil || run.budget.check() != nil {
			return domain.ErrMembershipDenied
		}
	}
	return nil
}
