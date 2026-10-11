package ydbstore

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

const (
	ownedAuthorityDeadline                  = 10 * time.Second
	ownedAuthorityMaxCallbacks              = 3
	ownedAuthorityMaxStatements             = 32
	ownedAuthorityMaxTotalStatements        = 96
	ownedAuthorityMaxBytes           uint64 = 4 << 20
	ownedAuthorityHeadBytes                 = 32 << 10
)

// One budget belongs to one logical call, including SDK retries and commit-loss
// recovery. It is not a monetary balance, quota or transferable permission.
type ownedAuthorityBudget struct {
	mu                                                 sync.Mutex
	retries, callbacks, statements, callbackStatements int
	bytes                                              uint64
	active                                             bool
	failed                                             bool
}

// Acquire is the SDK's second/subsequent-attempt hook. Bound connection/Begin
// failures too: no fourth transaction callback or unbounded pre-callback retry.
func (b *ownedAuthorityBudget) Acquire(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ctx.Err() != nil || b.failed || b.retries >= ownedAuthorityMaxCallbacks-1 {
		b.failed = true
		return domain.ErrMembershipDenied
	}
	b.retries++
	return nil
}

func (b *ownedAuthorityBudget) begin() (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failed || b.active || b.callbacks >= ownedAuthorityMaxCallbacks {
		b.failed = true
		return 0, domain.ErrMembershipDenied
	}
	b.callbacks++
	b.callbackStatements, b.active = 0, true
	return b.callbacks, nil
}

func (b *ownedAuthorityBudget) finish(callback int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active && callback == b.callbacks {
		b.active = false
	}
}

func (b *ownedAuthorityBudget) statement(ctx context.Context, callback int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ctx.Err() != nil || b.failed || !b.active || callback != b.callbacks || b.callbackStatements >= ownedAuthorityMaxStatements || b.statements >= ownedAuthorityMaxTotalStatements {
		b.failed = true
		return domain.ErrMembershipDenied
	}
	b.callbackStatements++
	b.statements++
	return nil
}

func (b *ownedAuthorityBudget) materialized(callback int, lengths ...uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failed || !b.active || callback != b.callbacks {
		b.failed = true
		return domain.ErrMembershipDenied
	}
	for _, n := range lengths {
		// Subtraction, not unchecked addition: even MaxUint64 cannot wrap.
		if n > ownedAuthorityMaxBytes-b.bytes {
			b.failed = true
			return domain.ErrMembershipDenied
		}
		b.bytes += n
	}
	return nil
}

func (b *ownedAuthorityBudget) check() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failed {
		return domain.ErrMembershipDenied
	}
	return nil
}

type ownedAuthorityQuery struct {
	tx       *sql.Tx
	budget   *ownedAuthorityBudget
	callback int
	at       time.Time
}

func (q *ownedAuthorityQuery) QueryRowContext(ctx context.Context, statement string, args ...any) *sql.Row {
	if q.budget.statement(ctx, q.callback) != nil {
		// database/sql returns a Row only; a cancelled context prevents issuing
		// the over-budget statement. The sticky budget error also prevents commit
		// if an enclosing callback mistakenly ignores Scan's error.
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		return q.tx.QueryRowContext(cancelled, statement, args...)
	}
	return q.tx.QueryRowContext(ctx, statement, args...)
}

func (q *ownedAuthorityQuery) ExecContext(ctx context.Context, statement string, args ...any) (sql.Result, error) {
	if err := q.budget.statement(ctx, q.callback); err != nil {
		return nil, err
	}
	return q.tx.ExecContext(ctx, statement, args...)
}

func (q *ownedAuthorityQuery) QueryContext(ctx context.Context, statement string, args ...any) (*sql.Rows, error) {
	if err := q.budget.statement(ctx, q.callback); err != nil {
		return nil, err
	}
	return q.tx.QueryContext(ctx, statement, args...)
}

// Compose canonical helpers into THIS callback, clock and logical-call budget.
// This is not an admission port, another transaction, or a standalone preflight.
func (q *ownedAuthorityQuery) canonicalState(store *Store, tenant domain.TenantID) *stateTx {
	return &stateTx{store: store, sqlTx: q, tenantID: tenant}
}

// This private composition primitive establishes the actual serializable DB
// clock and shared budget. It exports no standalone preflight permission. A
// future reservation MUST compose all its authority/debit reads here and clear
// speculative outputs on every callback; only a confirmed result may escape.
// The installed publisher/reserver is intentionally not wired by this slice.
func (store *Store) ownedAuthorityTx(ctx context.Context, fn func(context.Context, *ownedAuthorityQuery) error) error {
	ctx, cancel := context.WithTimeout(ctx, ownedAuthorityDeadline)
	defer cancel()
	b := &ownedAuthorityBudget{}
	err := retry.DoTx(ctx, store.db, func(ctx context.Context, tx *sql.Tx) error {
		callback, err := b.begin()
		if err != nil {
			return callbackError{err: err}
		}
		defer b.finish(callback)
		q := &ownedAuthorityQuery{tx: tx, budget: b, callback: callback}
		scanErr := q.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&q.at)
		if b.materialized(callback, 8) != nil || scanErr != nil || q.at.IsZero() || q.at.Year() < 1 || q.at.Year() > 9999 || q.at.Nanosecond()%1000 != 0 {
			return callbackError{err: domain.ErrMembershipDenied}
		}
		q.at = q.at.UTC()
		if err := fn(ctx, q); err != nil {
			return callbackError{err: err}
		}
		// Do not leave deadline enforcement solely to database/sql's async
		// rollback or report denial only AFTER the SDK has attempted commit.
		if ctx.Err() != nil {
			return callbackError{err: domain.ErrMembershipDenied}
		}
		return b.check()
	}, retry.WithIdempotent(true), retry.WithBudget(b), retry.WithTxOptions(&sql.TxOptions{Isolation: sql.LevelSerializable}))
	if err != nil || ctx.Err() != nil || b.check() != nil {
		// Fixed, content-free denial, including ambiguous commit or backend error.
		return domain.ErrMembershipDenied
	}
	return nil
}
