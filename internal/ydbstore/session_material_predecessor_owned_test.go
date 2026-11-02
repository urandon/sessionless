package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// Closed point-reader DATA only, not a Source, producer or admission verifier.
// The separate canonical-commit regression exercises the actual installed
// Source/binder and complete ordinary transaction at the 32-statement limit.
type ownedMaterialPairFixture struct {
	*materialSQLFixture
	rows       [][]driver.Value
	queries    int
	readError  bool
	closeError bool
}

func (f *ownedMaterialPairFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *ownedMaterialPairFixture) QueryContext(_ context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f.queries++
	if statement != ownedSessionMaterialPredecessorSQL || len(args) != 3 || args[0].Value != "writer-tenant" || args[1].Value != "writer-session" || args[2].Value != int64(1) {
		return nil, errors.New("unexpected material query or exact point key")
	}
	return &ownedMaterialPairRows{rows: f.rows, readError: f.readError, closeError: f.closeError}, nil
}

type ownedMaterialPairRows struct {
	rows       [][]driver.Value
	next       int
	readError  bool
	closeError bool
}

func (*ownedMaterialPairRows) Columns() []string {
	return []string{"prefix_kind", "prefix_record", "prefix_version", "prefix_through", "prefix_tenant", "prefix_session", "prefix_last", "prefix_digest"}
}
func (r *ownedMaterialPairRows) Close() error {
	if r.closeError {
		return errors.New("controlled rows close failure")
	}
	return nil
}
func (r *ownedMaterialPairRows) Next(dest []driver.Value) error {
	if r.next == len(r.rows) {
		if r.readError {
			return errors.New("controlled rows read failure")
		}
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

func ownedMaterialPrefix(t *testing.T) sessioncontext.SessionMaterialPrefixV1 {
	t.Helper()
	genesis, err := sessioncontext.NewSessionMaterialGenesisV1("writer-tenant", "writer-session")
	if err != nil {
		t.Fatal(err)
	}
	event, prepared := materialWriterEvent(t, 1)
	measure, err := sessioncontext.MeasurePreparedRecordV1(event, prepared)
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := sessioncontext.FoldSessionMaterialV1(genesis, event, &measure)
	if err != nil {
		t.Fatal(err)
	}
	return prefix
}

func ownedMaterialRows(t *testing.T, prefix sessioncontext.SessionMaterialPrefixV1) [][]driver.Value {
	t.Helper()
	return [][]driver.Value{
		append([]driver.Value{int64(0)}, materialRow(t, prefix)...),
		append([]driver.Value{int64(1)}, materialRow(t, prefix)...),
	}
}

func readOwnedMaterialPairFixture(t *testing.T, f *ownedMaterialPairFixture, used uint64) (sessioncontext.SessionMaterialPrefixV1, *ownedAuthorityBudget, error) {
	t.Helper()
	f.materialSQLFixture = &materialSQLFixture{committed: map[string][]driver.Value{}}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	budget := &ownedAuthorityBudget{bytes: used}
	callback, err := budget.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer budget.finish(callback)
	q := &ownedAuthorityQuery{tx: tx, budget: budget, callback: callback}
	prefix, err := loadSessionMaterialPredecessorTx(t.Context(), &stateTx{sqlTx: q, tenantID: "writer-tenant"}, "writer-session", 1)
	return prefix, budget, err
}

func TestSessionMaterialOwnedPairPresenceOrderAndUnknown(t *testing.T) {
	for _, name := range []string{"complete", "reversed", "both absent", "unknown preserved", "allowed whitespace"} {
		t.Run(name, func(t *testing.T) {
			want := ownedMaterialPrefix(t)
			if name == "unknown preserved" || name == "both absent" {
				var err error
				want, err = sessioncontext.NewUnknownSessionMaterialPrefixV1("writer-tenant", "writer-session", 1)
				if err != nil {
					t.Fatal(err)
				}
			}
			rows := ownedMaterialRows(t, want)
			if name == "reversed" {
				rows[0], rows[1] = rows[1], rows[0]
			}
			if name == "both absent" {
				rows = nil
			}
			if name == "allowed whitespace" {
				rows[1][1] = rows[1][1].(string) + " "
			}
			f := &ownedMaterialPairFixture{rows: rows}
			got, budget, err := readOwnedMaterialPairFixture(t, f, 0)
			if err != nil || !sessioncontext.EqualSessionMaterialPrefixV1(got, want) || f.queries != 1 || f.writes != 0 || budget.callbackStatements != 1 || budget.failed {
				t.Fatalf("pair result equal=%t err=%v queries=%d writes=%d statements=%d failed=%t", sessioncontext.EqualSessionMaterialPrefixV1(got, want), err, f.queries, f.writes, budget.callbackStatements, budget.failed)
			}
		})
	}
}

func TestSessionMaterialOwnedPairRejectsCorruptionWithoutFallback(t *testing.T) {
	for _, name := range []string{
		"missing current", "missing boundary", "duplicate current", "duplicate boundary", "extra row", "unknown tag",
		"record too large", "version", "sequence", "tenant", "session", "last event", "digest", "digest too large", "null", "trailing value",
		"different valid measure", "rows read error", "rows close error", "aggregate bytes",
	} {
		t.Run(name, func(t *testing.T) {
			prefix := ownedMaterialPrefix(t)
			rows := ownedMaterialRows(t, prefix)
			f := &ownedMaterialPairFixture{}
			var used uint64
			switch name {
			case "missing current":
				rows = rows[1:]
			case "missing boundary":
				rows = rows[:1]
			case "duplicate current":
				rows[1][0] = int64(0)
			case "duplicate boundary":
				rows[0][0] = int64(1)
			case "extra row":
				rows = append(rows, append([]driver.Value(nil), rows[1]...))
			case "unknown tag":
				rows[1][0] = int64(2)
			case "record too large":
				rows[1][1] = strings.Repeat("x", 8193)
			case "version":
				rows[1][2] = int64(2)
			case "sequence":
				rows[1][3] = int64(2)
			case "tenant":
				rows[1][4] = "foreign-tenant"
			case "session":
				rows[1][5] = "foreign-session"
			case "last event":
				rows[1][6] = "foreign-event"
			case "digest":
				rows[1][7] = strings.Repeat("f", 64)
			case "digest too large":
				rows[1][7] = strings.Repeat("f", 65)
			case "null":
				rows[1][1] = nil
			case "trailing value":
				rows[1][1] = rows[1][1].(string) + " {}"
			case "different valid measure":
				prefix.VerifiedPayloadBytes++
				rows[1] = append([]driver.Value{int64(1)}, materialRow(t, prefix)...)
			case "rows read error":
				f.readError = true
			case "rows close error":
				f.closeError = true
			case "aggregate bytes":
				used = ownedAuthorityMaxBytes - 1
			}
			f.rows = rows
			_, _, err := readOwnedMaterialPairFixture(t, f, used)
			if err == nil || f.queries != 1 || f.writes != 0 {
				t.Fatalf("bad pair accepted/fallback written: err=%v queries=%d writes=%d", err, f.queries, f.writes)
			}
		})
	}
}

func TestSessionMaterialOwnedPairChargesBothRowsBeforeParsing(t *testing.T) {
	rows := ownedMaterialRows(t, ownedMaterialPrefix(t))
	rows[0][1] = "{"
	var want uint64
	for _, row := range rows {
		for _, value := range row {
			switch value := value.(type) {
			case string:
				want += uint64(len(value))
			case int64:
				want += 8
			default:
				t.Fatal("fixture type", reflect.TypeOf(value))
			}
		}
	}
	f := &ownedMaterialPairFixture{rows: rows}
	_, budget, err := readOwnedMaterialPairFixture(t, f, 0)
	if err == nil || !budget.failed || budget.bytes != want || f.queries != 1 {
		t.Fatalf("malformed JSON uncharged: err=%v failed=%t bytes=%d want=%d queries=%d", err, budget.failed, budget.bytes, want, f.queries)
	}
}
