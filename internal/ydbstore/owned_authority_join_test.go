package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type ownedAdmissionJoinConnector struct {
	statement string
	args      []driver.Value
	values    []driver.Value
	noRows    bool
	calls     int
	badQuery  bool
}

func (c *ownedAdmissionJoinConnector) Connect(context.Context) (driver.Conn, error) {
	return &ownedAdmissionJoinConn{state: c}, nil
}
func (c *ownedAdmissionJoinConnector) Driver() driver.Driver { return ownedAdmissionJoinDriver{} }

type ownedAdmissionJoinDriver struct{}

func (ownedAdmissionJoinDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("connector only")
}

type ownedAdmissionJoinConn struct{ state *ownedAdmissionJoinConnector }

func (c *ownedAdmissionJoinConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepared statements forbidden")
}
func (c *ownedAdmissionJoinConn) Close() error { return nil }
func (c *ownedAdmissionJoinConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *ownedAdmissionJoinConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return ownedAdmissionJoinTx{}, nil
}
func (c *ownedAdmissionJoinConn) QueryContext(_ context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	c.state.calls++
	got := make([]driver.Value, len(args))
	for i := range args {
		got[i] = args[i].Value
	}
	if statement != c.state.statement || !reflect.DeepEqual(got, c.state.args) {
		c.state.badQuery = true
		return nil, errors.New("unexpected owned admission join query")
	}
	if c.state.noRows {
		return &ownedAdmissionJoinRows{columns: make([]string, 55)}, nil
	}
	return &ownedAdmissionJoinRows{columns: make([]string, 55), values: append([]driver.Value(nil), c.state.values...)}, nil
}

type ownedAdmissionJoinTx struct{}

func (ownedAdmissionJoinTx) Commit() error   { return nil }
func (ownedAdmissionJoinTx) Rollback() error { return nil }

type ownedAdmissionJoinRows struct {
	columns []string
	values  []driver.Value
	done    bool
}

func (r *ownedAdmissionJoinRows) Columns() []string { return r.columns }
func (r *ownedAdmissionJoinRows) Close() error      { return nil }
func (r *ownedAdmissionJoinRows) Next(dest []driver.Value) error {
	if r.done || r.values == nil {
		return io.EOF
	}
	r.done = true
	copy(dest, r.values)
	return nil
}

func ownedAdmissionJoinFixture(t *testing.T) (*ownedReaderFixture, domain.AttemptExecutionSourceV1, []driver.Value, []driver.Value) {
	t.Helper()
	f := newOwnedReaderFixture(t)
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: f.scope.tenant, SessionID: f.scope.session, OwnerUserID: f.scope.owner, RunID: f.scope.run, AttemptID: f.scope.attempt}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, f.profile, f.spending, f.driver.at)
	if err != nil {
		t.Fatalf("build fixture source: %v", err)
	}
	args := []driver.Value{string(f.ref.Locator.TenantID), string(f.ref.Locator.OwnerUserID), string(f.ref.Locator.Profile), f.ref.Locator.BackendProfileDigest, string(f.ref.Locator.ResourceKind), f.ref.Locator.ResourceID, f.ref.Locator.ModelVendorID, f.ref.Locator.ModelID, string(f.ref.Locator.InputDataClass)}
	values := append([]driver.Value(nil), f.driver.profileRow...)
	for i, spending := range f.spending {
		l := spending.Locator
		args = append(args, string(l.TenantID), string(l.AttemptID), string(l.ComponentKind), string(l.BillingOwnerUserID))
		values = append(values, f.driver.spendingRows[i]...)
	}
	return f, pkg.Source, args, values
}

func runOwnedAdmissionJoin(t *testing.T, source domain.AttemptExecutionSourceV1, args, values []driver.Value, noRows bool) (ownedCurrentSnapshot, *ownedAuthorityBudget, *ownedAdmissionJoinConnector, error) {
	t.Helper()
	state := &ownedAdmissionJoinConnector{statement: ownedAdmissionSourcesJoinSQL, args: args, values: values, noRows: noRows}
	db := sql.OpenDB(state)
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	b := &ownedAuthorityBudget{}
	callback, err := b.begin()
	if err != nil {
		t.Fatal(err)
	}
	q := &ownedAuthorityQuery{tx: tx, budget: b, callback: callback, at: source.IssuedAt}
	got, callErr := readOwnedAdmissionSources(context.Background(), q, source)
	b.finish(callback)
	if callErr != nil {
		_ = tx.Rollback()
	} else if err := b.check(); err != nil {
		_ = tx.Rollback()
		callErr = err
	} else if err := tx.Commit(); err != nil {
		callErr = err
	}
	return got, b, state, callErr
}

func TestReadOwnedAdmissionSourcesUsesOneExactJoinedStatement(t *testing.T) {
	f, source, args, values := ownedAdmissionJoinFixture(t)
	got, budget, state, err := runOwnedAdmissionJoin(t, source, args, values, false)
	if err != nil || !reflect.DeepEqual(got.profile, f.profile) || !reflect.DeepEqual(got.spending, f.spending) {
		t.Fatalf("joined source correlation failed: err=%v", err)
	}
	if state.calls != 1 || state.badQuery || budget.statements != 1 || budget.bytes == 0 {
		t.Fatalf("calls=%d badQuery=%t statements=%d bytes=%d", state.calls, state.badQuery, budget.statements, budget.bytes)
	}
	if len(args) != 25 || len(values) != 55 {
		t.Fatalf("exact keys/columns args=%d columns=%d", len(args), len(values))
	}
}

func TestReadOwnedAdmissionSourcesMissingHeadFailsClosed(t *testing.T) {
	_, source, args, values := ownedAdmissionJoinFixture(t)
	got, budget, state, err := runOwnedAdmissionJoin(t, source, args, values, true)
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ownedCurrentSnapshot{}) || state.calls != 1 || budget.statements != 1 {
		t.Fatalf("missing joined row result=%+v err=%v calls=%d statements=%d", got, err, state.calls, budget.statements)
	}
}

func TestReadOwnedAdmissionSourcesChargesAllColumnsBeforeRejecting(t *testing.T) {
	_, source, args, values := ownedAdmissionJoinFixture(t)
	// A malformed early scalar must not prevent charging later returned columns.
	values[3] = nil
	var want uint64
	for _, value := range values {
		switch v := value.(type) {
		case string:
			want += uint64(len(v))
		case []byte:
			want += uint64(len(v))
		case nil:
		default:
			want += 8
		}
	}
	got, budget, state, err := runOwnedAdmissionJoin(t, source, args, values, false)
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ownedCurrentSnapshot{}) || state.calls != 1 || budget.statements != 1 || budget.bytes != want {
		t.Fatalf("malformed joined row err=%v calls=%d statements=%d charged=%d want=%d", err, state.calls, budget.statements, budget.bytes, want)
	}
}

func TestReadOwnedAdmissionSourcesRejectsInvalidOriginalAttachmentBeforeQuery(t *testing.T) {
	_, source, args, values := ownedAdmissionJoinFixture(t)
	source.Spending[3].Locator = source.Spending[2].Locator
	got, budget, state, err := runOwnedAdmissionJoin(t, source, args, values, false)
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ownedCurrentSnapshot{}) || state.calls != 0 || budget.statements != 0 {
		t.Fatalf("invalid attachment result=%+v err=%v calls=%d statements=%d", got, err, state.calls, budget.statements)
	}
}
