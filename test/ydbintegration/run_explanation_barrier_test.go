//go:build ydbintegration

package ydbintegration

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// This fixture-only connector delegates to the real YDB data connector and
// retains its native transaction/rows. The one-shot barrier runs after the
// selected query's rows close, while the reader's serializable tx is still open.
// It introduces neither a production hook nor a synthetic transaction result.
type explanationReadBarrier struct {
	statement string
	entered   chan struct{}
	release   chan struct{}
	once      sync.Once
	unblock   sync.Once
}

func (barrier *explanationReadBarrier) releaseReader() {
	barrier.unblock.Do(func() { close(barrier.release) })
}

type explanationBarrierConnector struct {
	driver.Connector
	barrier *explanationReadBarrier
}

func (connector explanationBarrierConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := connector.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return explanationBarrierConn{Conn: conn, barrier: connector.barrier}, nil
}

type explanationBarrierConn struct {
	driver.Conn
	barrier *explanationReadBarrier
}

func (conn explanationBarrierConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if options.Isolation != driver.IsolationLevel(sql.LevelSerializable) || options.ReadOnly {
		return nil, errors.New("explanation fixture requires native serializable transaction")
	}
	return conn.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}

func (conn explanationBarrierConn) CheckNamedValue(value *driver.NamedValue) error {
	return conn.Conn.(driver.NamedValueChecker).CheckNamedValue(value)
}

func (conn explanationBarrierConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := conn.Conn.(driver.QueryerContext).QueryContext(ctx, statement, args)
	if err != nil || !strings.Contains(statement, conn.barrier.statement) {
		return rows, err
	}
	return explanationBarrierRows{Rows: rows, ctx: ctx, barrier: conn.barrier}, nil
}

type explanationBarrierRows struct {
	driver.Rows
	ctx     context.Context
	barrier *explanationReadBarrier
}

func (rows explanationBarrierRows) Close() error {
	err := rows.Rows.Close()
	rows.barrier.once.Do(func() {
		close(rows.barrier.entered)
		select {
		case <-rows.barrier.release:
		case <-rows.ctx.Done():
			err = rows.ctx.Err()
		}
	})
	return err
}

func explanationBarrierStore(t *testing.T, f explanationYDBFixture, statement string) (*ydbstore.Store, *explanationReadBarrier) {
	t.Helper()
	connector, ok := f.client.DB.Driver().(driver.Connector)
	if !ok {
		t.Fatal("native YDB SQL driver does not expose its connector")
	}
	barrier := &explanationReadBarrier{statement: statement, entered: make(chan struct{}), release: make(chan struct{})}
	db := sql.OpenDB(explanationBarrierConnector{Connector: connector, barrier: barrier})
	t.Cleanup(func() {
		barrier.releaseReader()
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := ydbstore.New(db, ydbstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return store, barrier
}

func awaitExplanationReadBarrier(t *testing.T, f explanationYDBFixture, barrier *explanationReadBarrier) {
	t.Helper()
	select {
	case <-barrier.entered:
	case <-f.ctx.Done():
		t.Fatal("reader did not reach the deterministic transaction query barrier")
	}
}
