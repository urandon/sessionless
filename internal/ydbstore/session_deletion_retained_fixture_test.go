package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// InspectRetainedDeletionBodiesV1 is a test-only bridge to the historical DATA
// verifier. Input is the durable snapshot of a GENUINE original Complete, not
// a fabricated success marker. SQL/storage are controlled fixtures, not native
// YDB/S3 proof, and this helper grants no lifecycle/destructive authority.
func InspectRetainedDeletionBodiesV1(t *testing.T, seed *LocalTerminalPublicationSessionConnectedSeed, completed *LocalTerminalCompletionSQLFixture, reader SessionDeletionBlobReader, mutation string) ([]domain.BlobRef, error) {
	t.Helper()
	if strings.HasPrefix(mutation, "service_") {
		if e := inspectGenuineRetainedDeletionService(t, seed, completed, reader, mutation); e != nil {
			return nil, e
		}
		return retainedDeletionRunRefsData(completed.Snapshot(t).Run)
	}
	if strings.HasPrefix(mutation, "start_") || strings.HasPrefix(mutation, "resume_") || strings.HasPrefix(mutation, "complete_") {
		if err := inspectGenuineRetainedDeletionStart(t, seed, completed, reader, mutation); err != nil {
			return nil, err
		}
		return retainedDeletionRunRefsData(completed.Snapshot(t).Run)
	}
	if mutation == "capture_positive" {
		if err := inspectGenuineRetainedDeletionCapture(t, seed, completed); err != nil {
			return nil, err
		}
		return retainedDeletionRunRefsData(completed.Snapshot(t).Run)
	}
	state := completed.Snapshot(t)
	history := localFreshCommittedHistory(t, seed, state, nil)
	db := sql.OpenDB(retainedDeletionWriterConnector{base: freshRunHistoryConnector{base: terminalCompletionConnector{f: completed}, history: history}, seed: seed, mutation: mutation})
	db.SetMaxOpenConns(1)
	defer func() {
		if err := db.Close(); err != nil {
			t.Error("historical DATA fixture close", err)
		}
	}()
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	callback, err := b.begin()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		b.finish(callback)
		t.Fatal(err)
	}
	q := &ownedAuthorityQuery{tx: tx, budget: b, callback: callback, at: time.Now().UTC().Truncate(time.Microsecond)}
	if len(state.Events) != 1 {
		t.Fatal("genuine one-assistant completion required")
	}
	producer, err := readRetainedDeletionProducer(ctx, q, state.Run, state.Events[0])
	var writer *retainedDeletionWriterData
	if strings.HasPrefix(mutation, "writer_") && err == nil {
		counts := retainedDeletionAttemptRows{parents: 1, evidence: 1, operations: uint64(len(seed.compaction.operations)), calls: uint64(len(seed.compaction.operations)), proxies: uint64(len(seed.compaction.proxies))}
		if mutation == "writer_extra_counts" {
			counts.operations++
		}
		v, e := readRetainedDeletionWriter(ctx, q, state.Run, state.Events[0], counts)
		err = e
		writer = &v
		if err == nil {
			producer = v.producer
		}
	}
	rollbackErr := tx.Rollback()
	b.finish(callback)
	if strings.HasPrefix(mutation, "writer_") && err != nil {
		return nil, err
	}
	if err != nil || rollbackErr != nil {
		t.Fatalf("original historical SQL snapshot: read=%v rollback=%v", err, rollbackErr)
	}
	snapshot := producer.terminal
	events := append(seed.CanonicalSourceEvents(t), state.Events...)
	ownership, err := newRetainedDeletionOwnershipData(state.Run.TenantID, state.Run.SessionID)
	if err != nil {
		t.Fatal("original SQL-owned inventory DATA", err)
	}
	for _, event := range events {
		if err := ownership.addEvent(event); err != nil {
			t.Fatal("original canonical owning SQL row", err)
		}
	}
	if err := ownership.addRun(state.Run); err != nil {
		t.Fatal("original Run owning SQL row", err)
	}
	switch mutation {
	case "missing_anchor_owner":
		delete(ownership.events, snapshot.Anchor.ID)
	case "mismatched_anchor_owner":
		event := ownership.events[snapshot.Anchor.ID]
		event.Payload.SHA256 = strings.Repeat("a", 64)
		ownership.events[event.ID] = event
	case "missing_retained_owner":
		if state.Run.DerivedContext == nil || len(state.Run.DerivedContext.Inventory) != 2 {
			t.Fatal("missing retained owner needs genuine A2")
		}
		digest, _ := state.Run.DerivedContext.Inventory[0].Digest()
		delete(ownership.plans, digest)
	case "changed_seal":
		digest, err := decodeLocalFinalizationMarkerV2(snapshot.Seal)
		if err != nil {
			t.Fatal(err)
		}
		first := "a"
		if strings.HasPrefix(digest, first) {
			first = "b"
		}
		snapshot.Seal, _ = encodeLocalFinalizationMarkerV2(first + digest[1:])
	case "positive":
	default:
		if !strings.HasPrefix(mutation, "writer_") {
			t.Fatal("unknown retained deletion DATA test mutation", mutation)
		}
	}
	if mutation == "writer_capsule_mismatch" && writer != nil && len(writer.operations) > 0 {
		writer.operations[0].reservation.Operation.CallID = "foreign-call"
	}
	_, err = verifyRetainedDeletionBodiesWithWriter(ctx, reader, snapshot, ownership, nil, writer)
	if err != nil {
		return nil, err
	}
	return retainedDeletionRunRefsData(snapshot.Run)
}

// This adapter projects the ACTUAL original producer's durable child rows.
// It never supplies a parent/evidence/seal/receipt not written by that producer.
type retainedDeletionWriterConnector struct {
	base     driver.Connector
	seed     *LocalTerminalPublicationSessionConnectedSeed
	mutation string
}

func (c retainedDeletionWriterConnector) Driver() driver.Driver { return c.base.Driver() }
func (c retainedDeletionWriterConnector) Connect(ctx context.Context) (driver.Conn, error) {
	v, e := c.base.Connect(ctx)
	if e != nil {
		return nil, e
	}
	return &retainedDeletionWriterConn{Conn: v, fixture: c}, nil
}

type retainedDeletionWriterConn struct {
	driver.Conn
	fixture retainedDeletionWriterConnector
}

func (c *retainedDeletionWriterConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
}
func (c *retainedDeletionWriterConn) ExecContext(ctx context.Context, s string, a []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, s, a)
}
func (c *retainedDeletionWriterConn) QueryContext(ctx context.Context, s string, a []driver.NamedValue) (driver.Rows, error) {
	f := c.fixture
	if s == retainedDeletionCallsSQL || s == retainedDeletionReconciliationSQL {
		if len(a) != 2 || a[0].Value != string(f.seed.Scope.TenantID) || a[1].Value != string(f.seed.Scope.AttemptID) {
			return nil, errors.New("retained writer child scope")
		}
		if f.mutation == "writer_query_error" && s == retainedDeletionCallsSQL {
			return nil, errors.New("controlled original child query failure")
		}
		var rows [][]driver.Value
		if s == retainedDeletionCallsSQL {
			for _, o := range f.seed.compaction.operations {
				rows = append(rows, []driver.Value{o[4], o[0], o[3]})
			}
			if f.mutation == "writer_extra_call" {
				rows = append(rows, []driver.Value{"orphan-extra-call", int64(1), strings.Repeat("a", 64)})
			}
			if f.mutation == "writer_changed_call" && len(rows) > 0 {
				rows[0][2] = strings.Repeat("a", 64)
			}
			sort.Slice(rows, func(i, j int) bool { return rows[i][0].(string) < rows[j][0].(string) })
		}
		return &retainedDeletionWriterRows{rows: rows, columns: map[bool]int{true: 3, false: 6}[s == retainedDeletionCallsSQL], failClose: f.mutation == "writer_close_error" && s == retainedDeletionCallsSQL}, nil
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, s, a)
}

type retainedDeletionWriterRows struct {
	rows          [][]driver.Value
	columns, next int
	failClose     bool
}

func (r *retainedDeletionWriterRows) Columns() []string { return make([]string, r.columns) }
func (r *retainedDeletionWriterRows) Next(v []driver.Value) error {
	if r.next == len(r.rows) {
		return io.EOF
	}
	copy(v, r.rows[r.next])
	r.next++
	return nil
}
func (r *retainedDeletionWriterRows) Close() error {
	if r.failClose {
		return errors.New("controlled original child Close failure")
	}
	return nil
}
