package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

// A deterministic SQL fault connector exercises real readers/folds/inserts
// and SDK callback retry. It is NOT native YDB isolation/query-plan evidence.
type materialSQLFixture struct {
	committed       map[string][]driver.Value
	pending         map[string][]driver.Value
	reads, writes   int
	failWrite       int
	commitConflicts int
	begins          int
}

func (f *materialSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (*materialSQLFixture) Driver() driver.Driver                          { return explanationFixtureDriver{} }
func (*materialSQLFixture) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fixture forbids prepare")
}
func (*materialSQLFixture) Close() error { return nil }
func (*materialSQLFixture) Begin() (driver.Tx, error) {
	return nil, errors.New("fixture requires BeginTx")
}
func (f *materialSQLFixture) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("fixture requires Serializable")
	}
	f.begins++
	f.pending = maps.Clone(f.committed)
	return materialFixtureTx{f}, nil
}

type materialFixtureTx struct{ f *materialSQLFixture }

func (tx materialFixtureTx) Commit() error {
	if tx.f.commitConflicts > 0 {
		tx.f.commitConflicts--
		tx.f.pending = nil
		return retry.RetryableError(errors.New("fixture serializable conflict"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	tx.f.committed, tx.f.pending = tx.f.pending, nil
	return nil
}
func (tx materialFixtureTx) Rollback() error { tx.f.pending = nil; return nil }

func (f *materialSQLFixture) QueryContext(_ context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f.reads++
	key := "current"
	switch statement {
	case sessionMaterialCurrentSQL:
		if len(args) != 2 || args[0].Value != "writer-tenant" || args[1].Value != "writer-session" {
			return nil, errors.New("fixture rejects foreign current key")
		}
	case sessionMaterialBoundarySQL:
		if len(args) != 3 || args[0].Value != "writer-tenant" || args[1].Value != "writer-session" {
			return nil, errors.New("fixture rejects foreign boundary key")
		}
		key = fmt.Sprintf("boundary-%v", args[2].Value)
	default:
		return nil, errors.New("fixture rejects unbounded/unrelated query")
	}
	rows := f.committed
	if f.pending != nil {
		rows = f.pending
	}
	return &explanationFixtureRows{values: rows[key]}, nil
}

func (f *materialSQLFixture) ExecContext(_ context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	f.writes++
	if f.failWrite == f.writes {
		return nil, errors.New("fixture write failed")
	}
	if f.pending == nil {
		return nil, errors.New("fixture write outside transaction")
	}
	values := make([]driver.Value, len(args))
	for i := range args {
		values[i] = args[i].Value
	}
	switch {
	case strings.HasPrefix(statement, "INSERT INTO session_events"):
		f.pending[fmt.Sprintf("event-%v", values[2])] = values
	case strings.HasPrefix(statement, "INSERT INTO session_event_idempotency"):
		f.pending[fmt.Sprintf("idem-%v", values[2])] = values
	case strings.HasPrefix(statement, "INSERT INTO session_material_boundaries_v1"):
		key := fmt.Sprintf("boundary-%v", values[2])
		if _, exists := f.pending[key]; exists {
			return nil, errors.New("fixture immutable boundary conflict")
		}
		f.pending[key] = []driver.Value{values[7], values[3], values[4], values[0], values[1], values[5], values[6]}
	case strings.HasPrefix(statement, "UPSERT INTO session_material_current_v1"), strings.HasPrefix(statement, "INSERT INTO session_material_current_v1"):
		if strings.HasPrefix(statement, "INSERT") {
			if _, exists := f.pending["current"]; exists {
				return nil, errors.New("fixture genesis conflict")
			}
		}
		f.pending["current"] = []driver.Value{values[6], values[2], values[3], values[0], values[1], values[4], values[5]}
	default:
		return nil, errors.New("fixture rejects unrelated mutation")
	}
	return driver.RowsAffected(1), nil
}

func materialWriterFixture(t *testing.T) (*Store, *materialSQLFixture, context.Context) {
	t.Helper()
	f := &materialSQLFixture{committed: make(map[string][]driver.Value)}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	return &Store{db: db}, f, ctx
}

func materialWriterEvent(t *testing.T, sequence uint64) (domain.SessionEvent, *sessionmaterial.PreparedV1) {
	t.Helper()
	body := []byte(`{"version":1,"text":"verified source"}`)
	hash := sha256.Sum256(body)
	user, run := domain.UserID("writer-user"), domain.RunID("writer-run")
	id := domain.SessionEventID(fmt.Sprintf("writer-event-%d", sequence))
	event := domain.SessionEvent{ID: id, TenantID: "writer-tenant", SessionID: "writer-session", Kind: domain.SessionEventUserMessage,
		Sequence: sequence, AuthorUserID: &user, RunID: &run, IdempotencyKey: domain.IdempotencyKey(id),
		Payload:   domain.BlobRef{TenantID: "writer-tenant", Key: domain.SessionEventObjectPrefix("writer-tenant", "writer-session", id) + "uploads/source/message.json", Size: int64(len(body)), SHA256: hex.EncodeToString(hash[:])},
		CreatedAt: time.Date(2026, 10, 11, 1, 2, 3, 0, time.UTC)}
	p, err := sessionmaterial.PrepareV1(event, body, nil)
	clear(body)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Release)
	return event, p
}

func materialRow(t *testing.T, p sessioncontext.SessionMaterialPrefixV1) []driver.Value {
	t.Helper()
	b, err := sessioncontext.EncodeSessionMaterialPrefixV1(p)
	if err != nil {
		t.Fatal(err)
	}
	return []driver.Value{string(b), int64(p.Version), int64(p.ThroughSequence), string(p.TenantID), string(p.SessionID), string(p.LastEventID), p.PrefixDigest}
}

func TestSessionMaterialActualInsertBatchUnknownAndCallbackRetry(t *testing.T) {
	store, f, ctx := materialWriterFixture(t)
	genesis, _ := sessioncontext.NewSessionMaterialGenesisV1("writer-tenant", "writer-session")
	f.committed["current"] = materialRow(t, genesis)
	f.commitConflicts = 1
	one, prepared := materialWriterEvent(t, 1)
	two, _ := materialWriterEvent(t, 2)
	err := store.Transact(ctx, one.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := insertSessionEventTx(ctx, tx, one, prepared); err != nil {
			return err
		}
		return insertSessionEventTx(ctx, tx, two, nil)
	})
	if err != nil {
		t.Fatalf("actual insert retry failed: %v", err)
	}
	if f.begins != 2 || f.reads != 2 || f.writes != 16 {
		t.Fatalf("callback-local batch reads/writes/retry: begins=%d reads=%d writes=%d want2/2/16", f.begins, f.reads, f.writes)
	}
	current, found, err := readSessionMaterialPrefixV1(ctx, store.db, sessionMaterialCurrentSQL, one.TenantID, one.SessionID, 2, nil)
	if err != nil || !found || current.ThroughSequence != 2 || current.UserMessageCount != 2 || current.UnknownRecords != 1 || current.CodexCompatibleRecords != 1 || current.Coverage != sessioncontext.SessionMaterialUnknownV1 {
		t.Fatalf("actual ordered complete→unknown fold: found=%v through=%d user=%d unknown=%d compatible=%d coverage=%s err=%v", found, current.ThroughSequence, current.UserMessageCount, current.UnknownRecords, current.CodexCompatibleRecords, current.Coverage, err)
	}
	for _, key := range []string{"event-1", "event-2", "boundary-1", "boundary-2", "idem-writer-event-1", "idem-writer-event-2"} {
		if _, found := f.committed[key]; !found {
			t.Errorf("committed batch omitted %s", key)
		}
	}
}

func TestSessionMaterialActualInsertRollbackAndLegacyDoNotRepair(t *testing.T) {
	t.Run("rollback", func(t *testing.T) {
		store, f, ctx := materialWriterFixture(t)
		genesis, _ := sessioncontext.NewSessionMaterialGenesisV1("writer-tenant", "writer-session")
		f.committed["current"] = materialRow(t, genesis)
		f.failWrite = 8 // second event's current UPSERT, after speculative boundary
		err := store.Transact(ctx, "writer-tenant", func(state ports.StateTx) error {
			for sequence := uint64(1); sequence <= 2; sequence++ {
				e, p := materialWriterEvent(t, sequence)
				if err := insertSessionEventTx(ctx, state.(*stateTx), e, p); err != nil {
					return err
				}
			}
			return nil
		})
		if err == nil || len(f.committed) != 1 {
			t.Fatalf("failed batch published partial facts: rows=%d err=%v", len(f.committed), err)
		}
		got, _, err := readSessionMaterialPrefixV1(ctx, store.db, sessionMaterialCurrentSQL, "writer-tenant", "writer-session", 0, nil)
		if err != nil || !sessioncontext.EqualSessionMaterialPrefixV1(got, genesis) {
			t.Fatalf("rollback altered genesis: %v", err)
		}
	})
	t.Run("legacy", func(t *testing.T) {
		store, _, ctx := materialWriterFixture(t)
		e, p := materialWriterEvent(t, 7)
		if err := store.Transact(ctx, e.TenantID, func(state ports.StateTx) error { return insertSessionEventTx(ctx, state.(*stateTx), e, p) }); err != nil {
			t.Fatal(err)
		}
		got, found, err := readSessionMaterialPrefixV1(ctx, store.db, sessionMaterialCurrentSQL, e.TenantID, e.SessionID, 7, nil)
		if err != nil || !found || got.Coverage != sessioncontext.SessionMaterialUnknownV1 || got.UnknownRecords != 6 || got.UnknownReasons&sessioncontext.SessionMaterialLegacyV1 == 0 {
			t.Fatalf("legacy was repaired by new text: unknown=%d coverage=%s err=%v", got.UnknownRecords, got.Coverage, err)
		}
	})
}

func TestSessionMaterialReaderChargesBeforeRejectingAllScalarMismatch(t *testing.T) {
	genesis, _ := sessioncontext.NewSessionMaterialGenesisV1("writer-tenant", "writer-session")
	cases := []struct {
		name   string
		mutate func([]driver.Value)
	}{
		{"record too large", func(r []driver.Value) { r[0] = strings.Repeat("x", 8193) }},
		{"version", func(r []driver.Value) { r[1] = int64(2) }},
		{"sequence", func(r []driver.Value) { r[2] = int64(1) }},
		{"tenant", func(r []driver.Value) { r[3] = "foreign-tenant" }},
		{"session", func(r []driver.Value) { r[4] = "foreign-session" }},
		{"last ID", func(r []driver.Value) { r[5] = "foreign-event" }},
		{"digest", func(r []driver.Value) { r[6] = strings.Repeat("f", 64) }},
		{"all detection prefixes", func(r []driver.Value) {
			r[0] = strings.Repeat("x", 8193)
			r[3], r[4], r[5] = strings.Repeat("x", 161), strings.Repeat("x", 161), strings.Repeat("x", 161)
			r[6] = strings.Repeat("f", 65)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store, f, ctx := materialWriterFixture(t)
			row := materialRow(t, genesis)
			test.mutate(row)
			f.committed["current"] = row
			want := int64(16)
			for _, index := range []int{0, 3, 4, 5, 6} {
				want += int64(len(row[index].(string)))
			}
			var charged int64
			_, found, err := readSessionMaterialPrefixV1(ctx, store.db, sessionMaterialCurrentSQL, "writer-tenant", "writer-session", 0, func(n int64) error { charged += n; return nil })
			if !errors.Is(err, errSessionMaterialSource) || found || charged != want || charged > 8757 {
				t.Fatalf("bad row accepted or uncharged: found=%v charge=%d want=%d err=%v", found, charged, want, err)
			}
			if test.name == "all detection prefixes" && charged != 8757 {
				t.Fatalf("full row charged=%d want8757", charged)
			}
		})
	}
}

func TestSessionMaterialCurrentBoundaryWholeParityAndGenesis(t *testing.T) {
	for _, name := range []string{"missing boundary", "orphan boundary", "divergent common measure", "fake zero genesis"} {
		t.Run(name, func(t *testing.T) {
			store, f, ctx := materialWriterFixture(t)
			genesis, _ := sessioncontext.NewSessionMaterialGenesisV1("writer-tenant", "writer-session")
			e, p := materialWriterEvent(t, 1)
			m, err := sessioncontext.MeasurePreparedRecordV1(e, p)
			if err != nil {
				t.Fatal(err)
			}
			prefix, err := sessioncontext.FoldSessionMaterialV1(genesis, e, &m)
			if err != nil {
				t.Fatal(err)
			}
			sequence := uint64(1)
			f.committed["current"], f.committed["boundary-1"] = materialRow(t, prefix), materialRow(t, prefix)
			switch name {
			case "missing boundary":
				delete(f.committed, "boundary-1")
			case "orphan boundary":
				delete(f.committed, "current")
			case "divergent common measure":
				prefix.VerifiedPayloadBytes++
				f.committed["boundary-1"] = materialRow(t, prefix)
			case "fake zero genesis":
				sequence = 0
				unknown, _ := sessioncontext.NewUnknownSessionMaterialPrefixV1(e.TenantID, e.SessionID, 0)
				f.committed["current"] = materialRow(t, unknown)
			}
			err = store.Transact(ctx, e.TenantID, func(state ports.StateTx) error {
				_, err := loadSessionMaterialPredecessorTx(ctx, state.(*stateTx), e.SessionID, sequence)
				return err
			})
			if !errors.Is(err, errSessionMaterialSource) || f.writes != 0 {
				t.Fatalf("inconsistent predecessor repaired/written: writes=%d err=%v", f.writes, err)
			}
		})
	}
}
