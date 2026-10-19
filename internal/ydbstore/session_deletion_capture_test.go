package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// These tests prove only the bounded SQL capture codec. They are not a
// successful-writer fixture and cannot authorize any lifecycle transition.
type deletionCaptureFixture struct {
	spec                   *deletionCaptureSpec
	values                 [][]driver.Value
	closeErr               error
	queries                int
	statement              string
	columns                int
	queryErr               error
	allowTx, active, point bool
	calls, pulls           int
	rowsBySQL              map[string][][]driver.Value
	columnsBySQL           map[string]int
	pullsBySQL             map[string]int
}
type deletionCaptureConn struct{ f *deletionCaptureFixture }

func (f *deletionCaptureFixture) Connect(context.Context) (driver.Conn, error) {
	return &deletionCaptureConn{f}, nil
}
func (*deletionCaptureFixture) Driver() driver.Driver { return ownedAdmissionJoinDriver{} }
func (*deletionCaptureConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("capture forbids Prepare")
}
func (*deletionCaptureConn) Close() error { return nil }
func (*deletionCaptureConn) Begin() (driver.Tx, error) {
	return nil, errors.New("capture forbids all transactions")
}
func (c *deletionCaptureConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if !c.f.allowTx || c.f.active {
		return nil, errors.New("unexpected capture transaction")
	}
	c.f.active = true
	return &deletionCaptureFixtureTx{c.f}, nil
}

type deletionCaptureFixtureTx struct{ f *deletionCaptureFixture }

func (tx *deletionCaptureFixtureTx) Commit() error   { tx.f.active = false; return nil }
func (tx *deletionCaptureFixtureTx) Rollback() error { tx.f.active = false; return nil }

type deletionCaptureCountedRows struct {
	*deletionInventoryRows
	f         *deletionCaptureFixture
	statement string
}

func (r *deletionCaptureCountedRows) Next(dst []driver.Value) error {
	if len(r.values) > 0 {
		r.f.pulls++
		if r.f.pullsBySQL != nil {
			r.f.pullsBySQL[r.statement]++
		}
	}
	return r.deletionInventoryRows.Next(dst)
}
func (c *deletionCaptureConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	c.f.calls++
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if c.f.rowsBySQL != nil {
		values, ok := c.f.rowsBySQL[statement]
		if !ok {
			return nil, fmt.Errorf("unhandled actual capture SQL: %s", statement)
		}
		rows := make([][]driver.Value, len(values))
		for i, v := range values {
			rows[i] = append([]driver.Value(nil), v...)
		}
		c.f.queries++
		return &deletionCaptureCountedRows{deletionInventoryRows: &deletionInventoryRows{columns: c.f.columnsBySQL[statement], values: rows}, f: c.f, statement: statement}, nil
	}
	want, columns := c.f.statement, c.f.columns
	if want == "" {
		var err error
		want, err = c.f.spec.sql(len(args) - 1)
		if err != nil {
			return nil, err
		}
		columns = len(c.f.spec.columns)
	}
	if statement != want || !strings.Contains(statement, "SUBSTRING(") || (!c.f.point && (!strings.Contains(statement, " ORDER BY ") || !strings.Contains(statement, " LIMIT $"))) {
		return nil, errors.New("capture requires exact bounded schema-owned SQL")
	}
	c.f.queries++
	if c.f.queryErr != nil {
		return nil, c.f.queryErr
	}
	values := make([][]driver.Value, len(c.f.values))
	for i, row := range c.f.values {
		values[i] = append([]driver.Value(nil), row...)
	}
	return &deletionCaptureCountedRows{deletionInventoryRows: &deletionInventoryRows{columns: columns, values: values, closeErr: c.f.closeErr}, f: c.f, statement: statement}, nil
}

func deletionCaptureTestGraph(t *testing.T, q stateSQLExecutor) *deletionCaptureGraph {
	t.Helper()
	ownership, err := newRetainedDeletionOwnershipData("tenant-capture", "session-capture")
	if err != nil {
		t.Fatal(err)
	}
	if q != nil {
		if _, ok := q.(*deletionCaptureQuery); !ok {
			q = newDeletionCaptureQuery(q)
		}
	}
	return &deletionCaptureGraph{sessionDeletionCapture: &sessionDeletionCapture{inventory: domain.SessionDeletionInventory{TenantID: "tenant-capture", SessionID: "session-capture"}, ownership: ownership}, query: q, seen: map[string]bool{}, objects: map[string]domain.BlobRef{}, runDigests: map[domain.RunID]string{"run-capture": "original-run-digest"}, required: map[string]bool{}, expected: map[string]map[string]json.RawMessage{}, optionalExpected: map[string]bool{}}
}

func deletionCaptureTestCell(t *testing.T, kind string, value any) deletionCaptureCell {
	t.Helper()
	if at, ok := value.(time.Time); ok {
		value = at.UTC().Format("2006-01-02T15:04:05.000000Z")
	}
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return deletionCaptureCell{Kind: kind, Present: true, Value: b}
}

func TestRetainedDeletionCaptureRequiredScalarCorrespondence(t *testing.T) {
	at := time.Date(2026, 10, 19, 1, 0, 0, 0, time.UTC)
	p := domain.SessionParticipant{TenantID: "tenant-capture", SessionID: "session-capture", UserID: "owner-capture", Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: at, UpdatedAt: at}
	body, err := json.Marshal(p)
	if err != nil || p.Validate() != nil {
		t.Fatal("invalid original participant fixture", err)
	}
	cells := map[string]deletionCaptureCell{"tenant_id": deletionCaptureTestCell(t, "s", p.TenantID), "session_id": deletionCaptureTestCell(t, "s", p.SessionID), "user_id": deletionCaptureTestCell(t, "s", p.UserID), "role": deletionCaptureTestCell(t, "s", p.Role), "status": deletionCaptureTestCell(t, "s", p.Status), "created_at": deletionCaptureTestCell(t, "t", at), "updated_at": deletionCaptureTestCell(t, "t", at)}
	if err := validateDeletionCaptureScalars("session_participants", cells, body); err != nil {
		t.Fatal("original typed scalar correspondence", err)
	}
	for _, name := range []string{"missing_required_json", "missing_required_scalar", "null_required_scalar", "same_count_scalar", "null_json_required", "case_folded_json"} {
		t.Run(name, func(t *testing.T) {
			var record map[string]json.RawMessage
			if json.Unmarshal(body, &record) != nil {
				t.Fatal("fixture record")
			}
			candidate := make(map[string]deletionCaptureCell, len(cells))
			for k, v := range cells {
				candidate[k] = v
			}
			switch name {
			case "missing_required_json":
				delete(record, "created_at")
			case "missing_required_scalar":
				delete(candidate, "created_at")
			case "null_required_scalar":
				candidate["created_at"] = deletionCaptureCell{Kind: "t", Value: json.RawMessage("null")}
			case "same_count_scalar":
				candidate["role"] = deletionCaptureTestCell(t, "s", domain.SessionParticipantViewer)
			case "null_json_required":
				record["user_id"] = json.RawMessage("null")
			case "case_folded_json":
				record["User_ID"] = record["user_id"]
				delete(record, "user_id")
			}
			b, _ := json.Marshal(record)
			if err := validateDeletionCaptureScalars("session_participants", candidate, b); err == nil {
				t.Fatal("malformed initial scalar/JSON correspondence accepted")
			}
		})
	}
}

const deletionCaptureParticipantTestSQL = `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_participants WHERE tenant_id=$1 AND session_id=$2 ORDER BY user_id LIMIT $3`
const deletionCaptureAdmissionTestSQL = `SELECT SUBSTRING(CAST(attempt_id AS String),0,161),SUBSTRING(CAST(run_id AS String),0,161) FROM attempt_execution_admissions_by_session_v1 WHERE tenant_id=$1 AND session_id=$2 ORDER BY tenant_id,session_id,attempt_id LIMIT $3`

func TestRetainedDeletionCaptureBoundedDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name               string
		values             [][]driver.Value
		queryErr, closeErr error
		wantError          bool
	}{
		{name: "two_original_admissions", values: [][]driver.Value{{"attempt-a", "run-capture"}, {"attempt-b", "run-capture"}}},
		{name: "oversize_sentinel", values: [][]driver.Value{{strings.Repeat("a", 161), "run-capture"}}, wantError: true},
		{name: "duplicate_identity", values: [][]driver.Value{{"attempt-a", "run-capture"}, {"attempt-a", "run-capture"}}, wantError: true},
		{name: "empty_identity", values: [][]driver.Value{{"", "run-capture"}}, wantError: true},
		{name: "null_identity", values: [][]driver.Value{{nil, "run-capture"}}, wantError: true},
		{name: "query_error", queryErr: errors.New("injected discovery query"), wantError: true},
		{name: "close_error", values: [][]driver.Value{{"attempt-a", "run-capture"}}, closeErr: errors.New("injected discovery Close"), wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &deletionCaptureFixture{statement: deletionCaptureAdmissionTestSQL, columns: 2, values: tc.values, queryErr: tc.queryErr, closeErr: tc.closeErr}
			db := captureFixtureDB(t, f)
			got, err := discoverDeletionCaptureIDs(t.Context(), newDeletionCaptureQuery(db), "attempt_execution_admissions_by_session_v1", "attempt_id,run_id", "tenant-capture", "session-capture")
			if (err != nil) != tc.wantError || (!tc.wantError && len(got) != 2) || f.queries != 1 {
				t.Fatalf("bounded discovery rows=%d queries=%d err=%v wantError=%v", len(got), f.queries, err, tc.wantError)
			}
		})
	}
	t.Run("aggregate_row_overflow", func(t *testing.T) {
		rows := make([][]driver.Value, maxSessionDeletionRows+1)
		for i := range rows {
			rows[i] = []driver.Value{fmt.Sprintf("attempt-%05d", i), "run-capture"}
		}
		f := &deletionCaptureFixture{statement: deletionCaptureAdmissionTestSQL, columns: 2, values: rows}
		db := captureFixtureDB(t, f)
		if _, err := discoverDeletionCaptureIDs(t.Context(), newDeletionCaptureQuery(db), "attempt_execution_admissions_by_session_v1", "attempt_id,run_id", "tenant-capture", "session-capture"); err == nil {
			t.Fatal("10001st discovery row accepted")
		}
	})
}

func TestRetainedDeletionCaptureStrictParticipants(t *testing.T) {
	at := time.Date(2026, 10, 19, 1, 0, 0, 0, time.UTC)
	p := domain.SessionParticipant{TenantID: "tenant-capture", SessionID: "session-capture", UserID: "owner-capture", Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: at, UpdatedAt: at}
	raw, _ := json.Marshal(p)
	for _, tc := range []struct {
		name, raw          string
		queryErr, closeErr error
		wantError          bool
	}{
		{name: "original", raw: string(raw)},
		{name: "missing_created_at", raw: `{"tenant_id":"tenant-capture","session_id":"session-capture","user_id":"owner-capture","role":"owner","status":"active","updated_at":"2026-10-19T01:00:00Z"}`, wantError: true},
		{name: "unknown_member", raw: strings.TrimSuffix(string(raw), "}") + `,"extra":true}`, wantError: true},
		{name: "duplicate_member", raw: strings.TrimSuffix(string(raw), "}") + `,"role":"owner"}`, wantError: true},
		{name: "overflow_before_decode", raw: strings.Repeat(" ", ownedAuthorityHeadBytes+1), wantError: true},
		{name: "query_error", queryErr: errors.New("injected participant query"), wantError: true},
		{name: "close_error", raw: string(raw), closeErr: errors.New("injected participant Close"), wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &deletionCaptureFixture{statement: deletionCaptureParticipantTestSQL, columns: 1, values: [][]driver.Value{{tc.raw}}, queryErr: tc.queryErr, closeErr: tc.closeErr}
			db := captureFixtureDB(t, f)
			got, err := deletionCaptureJSONRows[domain.SessionParticipant](t.Context(), newDeletionCaptureQuery(db), deletionCaptureParticipantTestSQL, maxSessionDeletionRows+1, "tenant-capture", "session-capture", maxSessionDeletionRows+1)
			if (err != nil) != tc.wantError || (!tc.wantError && (len(got) != 1 || got[0] != p)) {
				t.Fatalf("strict participant rows=%d err=%v wantError=%v", len(got), err, tc.wantError)
			}
		})
	}
}

func TestRetainedDeletionCaptureAllAdmissionAndKindRanges(t *testing.T) {
	f := &deletionCaptureFixture{statement: deletionCaptureAdmissionTestSQL, columns: 2, values: [][]driver.Value{{"attempt-a", "run-capture"}, {"attempt-b", "run-capture"}}}
	db := captureFixtureDB(t, f)
	g := deletionCaptureTestGraph(t, db)
	if err := g.addAdmissionScopes(t.Context(), "tenant-capture", "session-capture"); err != nil {
		t.Fatal("original reverse admission inventory", err)
	}
	if len(g.admissions) != 2 || len(g.scopes) != 14 {
		t.Fatalf("every original admission needs canonical point plus six complete child ranges: admissions=%d scopes=%d", len(g.admissions), len(g.scopes))
	}
	for _, attempt := range []domain.AttemptID{"attempt-a", "attempt-b"} {
		for _, table := range []string{"attempt_execution_admissions_v1", "attempt_effect_reservations", "substrate_execution_evidence", "attempt_operations_v1", "attempt_operation_calls_v1", "attempt_operation_proxies_v1", "attempt_effect_reconciliation_evidence"} {
			found := false
			for _, s := range g.scopes {
				if s.spec.table == table && len(s.prefix) == 2 && s.prefix[0] == domain.TenantID("tenant-capture") && s.prefix[1] == attempt {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing complete original scope: attempt=%s table=%s", attempt, table)
			}
		}
	}
	t.Run("extra_unknown_parent_kind", func(t *testing.T) {
		spec := deletionCaptureSpecs["attempt_effect_reservations"]
		row := make([]driver.Value, len(spec.columns))
		row[0], row[1], row[2] = "tenant-capture", "attempt-b", "unknown-new-kind"
		f := &deletionCaptureFixture{spec: spec, values: [][]driver.Value{row}}
		db := captureFixtureDB(t, f)
		candidate := deletionCaptureTestGraph(t, db)
		if _, _, err := captureDeletionMetadata(t.Context(), []deletionCaptureScope{{spec: spec, prefix: []any{"tenant-capture", "attempt-b"}, query: db, observe: candidate.observeRow}}, ownedAuthorityMaxBytes); err == nil {
			t.Fatal("extra parent KIND hidden behind selected provider-turn point")
		}
	})
	t.Run("foreign_reverse_run", func(t *testing.T) {
		f := &deletionCaptureFixture{statement: deletionCaptureAdmissionTestSQL, columns: 2, values: [][]driver.Value{{"attempt-extra", "foreign-run"}}}
		db := captureFixtureDB(t, f)
		candidate := deletionCaptureTestGraph(t, db)
		if err := candidate.addAdmissionScopes(t.Context(), "tenant-capture", "session-capture"); err == nil {
			t.Fatal("reverse admission outside original Session Run owners accepted")
		}
	})
}

func TestRetainedDeletionCaptureMaterialScalarCorrespondence(t *testing.T) {
	p, err := sessioncontext.NewSessionMaterialGenesisV1("tenant-capture", "session-capture")
	if err != nil {
		t.Fatal(err)
	}
	body, err := sessioncontext.EncodeSessionMaterialPrefixV1(p)
	if err != nil {
		t.Fatal(err)
	}
	cells := map[string]deletionCaptureCell{"tenant_id": deletionCaptureTestCell(t, "s", p.TenantID), "session_id": deletionCaptureTestCell(t, "s", p.SessionID), "version": deletionCaptureTestCell(t, "u", uint64(p.Version)), "through_sequence": deletionCaptureTestCell(t, "u", p.ThroughSequence), "last_event_id": deletionCaptureTestCell(t, "s", ""), "prefix_digest": deletionCaptureTestCell(t, "s", p.PrefixDigest)}
	if err := validateDeletionCaptureScalars("session_material_current_v1", cells, body); err != nil {
		t.Fatal("original material seed correspondence", err)
	}
	cells["through_sequence"] = deletionCaptureTestCell(t, "u", uint64(1))
	if err := validateDeletionCaptureScalars("session_material_current_v1", cells, body); err == nil {
		t.Fatal("same-count material scalar mutation accepted")
	}
	if err := validateDeletionCaptureScalars("session_snapshot_material_v1", cells, body); err == nil {
		t.Fatal("unsupported snapshot-material historical codec accepted")
	}
}

func TestRetainedDeletionCaptureRequiredIndexCorrespondence(t *testing.T) {
	at := time.Date(2026, 10, 19, 1, 0, 0, 0, time.UTC)
	spec := deletionCaptureSpecs["runs_by_session"]
	for _, name := range []string{"original", "same_count_scalar_mutation", "missing_original_index", "unexpected_index_row"} {
		t.Run(name, func(t *testing.T) {
			rows := [][]driver.Value{{"tenant-capture", "session-capture", at, "run-capture", "event-trigger", "succeeded"}}
			if name == "same_count_scalar_mutation" {
				rows[0][5] = "failed"
			}
			if name == "missing_original_index" {
				rows = nil
			}
			f := &deletionCaptureFixture{spec: spec, values: rows}
			db := captureFixtureDB(t, f)
			g := deletionCaptureTestGraph(t, db)
			if name != "unexpected_index_row" {
				if err := g.expect("runs_by_session", []any{domain.TenantID("tenant-capture"), domain.SessionID("session-capture"), at, domain.RunID("run-capture")}, map[string]any{"trigger_event_id": domain.SessionEventID("event-trigger"), "status": domain.RunSucceeded}); err != nil {
					t.Fatal(err)
				}
			}
			_, _, err := captureDeletionMetadata(t.Context(), []deletionCaptureScope{{spec: spec, prefix: []any{"tenant-capture", "session-capture"}, query: db, observe: g.observeRow}}, ownedAuthorityMaxBytes)
			if err == nil {
				err = g.requiredRowsCaptured()
			}
			if (err != nil) != (name != "original") {
				t.Fatalf("index correspondence case=%s err=%v", name, err)
			}
		})
	}
}

func TestRetainedDeletionCaptureSharesExistingMaterialBudget(t *testing.T) {
	b := &ownedAuthorityBudget{}
	callback, err := b.begin()
	if err != nil {
		t.Fatal(err)
	}
	defer b.finish(callback)
	q := &ownedAuthorityQuery{budget: b, callback: callback}
	if err := chargeDeletionCaptureSQL(q, ownedAuthorityMaxBytes-1); err != nil {
		t.Fatal("original shared envelope", err)
	}
	if err := chargeDeletionCaptureSQL(q, 2); err == nil {
		t.Fatal("capture created a fresh material allowance instead of using original logical budget")
	}
	if err := chargeDeletionCaptureSQL(q, 0); err == nil {
		t.Fatal("sticky original material-budget denial was reset")
	}
}

func TestRetainedDeletionCaptureConflictingOriginalOwners(t *testing.T) {
	g := deletionCaptureTestGraph(t, nil)
	key := []any{domain.TenantID("tenant-capture"), domain.ArtifactManifestID("manifest-capture")}
	for i := 0; i < 2; i++ {
		if err := g.expect("artifact_manifests", key, map[string]any{"run_id": domain.RunID("run-a")}); err != nil {
			t.Fatalf("identical original owner replay %d: %v", i, err)
		}
	}
	if err := g.expect("artifact_manifests", key, map[string]any{"run_id": domain.RunID("run-b")}); err == nil {
		t.Fatal("second reverse index replaced the canonical manifest's original Run owner")
	}
}

func deletionCaptureOrdinaryTx(t *testing.T, f *deletionCaptureFixture) *sql.Tx {
	t.Helper()
	f.allowTx = true
	db := captureFixtureDB(t, f)
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Error(err)
		}
	})
	return tx
}

func TestRetainedDeletionCaptureOrdinaryTxProspectiveBudget(t *testing.T) {
	type record struct {
		Value string `json:"value"`
	}
	body, err := json.Marshal(record{Value: strings.Repeat("x", ownedAuthorityHeadBytes-128)})
	if err != nil {
		t.Fatal(err)
	}
	admitted := int(ownedAuthorityMaxBytes / uint64(len(body)))
	f := &deletionCaptureFixture{statement: deletionCaptureParticipantTestSQL, columns: 1, values: [][]driver.Value{{string(body)}}}
	tx := deletionCaptureOrdinaryTx(t, f)
	q := newDeletionCaptureQuery(tx)
	if err := q.budget.admitOriginalRuns(0); err != nil {
		t.Fatal(err)
	}
	// The first separate SQL projection already spends from the SAME ledger.
	if got, err := deletionCaptureJSONRows[record](t.Context(), q, deletionCaptureParticipantTestSQL, maxSessionDeletionRows+1, "tenant-capture", "session-capture", maxSessionDeletionRows+1); err != nil || len(got) != 1 {
		t.Fatalf("initial ordinary transaction read: rows=%d err=%v", len(got), err)
	}
	f.values = make([][]driver.Value, admitted+1)
	for i := range f.values {
		f.values[i] = []driver.Value{string(body)}
	}
	if got, err := deletionCaptureJSONRows[record](t.Context(), q, deletionCaptureParticipantTestSQL, maxSessionDeletionRows+1, "tenant-capture", "session-capture", maxSessionDeletionRows+1); err == nil || got != nil {
		t.Fatalf("first over-budget row decoded/appended: rows=%d err=%v", len(got), err)
	}
	if f.pulls != admitted+1 || q.budget.rows != uint64(admitted) || q.budget.bytes != uint64(admitted)*uint64(len(body)) || !q.budget.failed {
		t.Fatalf("prospective first-over bound: pulls=%d admittedRows=%d bytes=%d failed=%v; want pulls=%d", f.pulls, q.budget.rows, q.budget.bytes, q.budget.failed, admitted+1)
	}
	if err := q.budget.admitOriginalRuns(1); err == nil {
		t.Fatal("failed ordinary-Tx ledger was reset by later envelope admission")
	}
}

func TestRetainedDeletionCaptureOrdinaryTxParticipantStreaming(t *testing.T) {
	at := time.Date(2026, 10, 19, 1, 0, 0, 0, time.UTC)
	session := domain.Session{ID: "session-capture", TenantID: "tenant-capture", CreatedBy: "owner-capture", Status: domain.SessionActive, CreatedAt: at, UpdatedAt: at}
	for _, name := range []string{"original", "oversized_first_user_id", "foreign_first_scope", "close_error"} {
		t.Run(name, func(t *testing.T) {
			first := domain.SessionParticipant{TenantID: session.TenantID, SessionID: session.ID, UserID: "owner-a", Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: at, UpdatedAt: at}
			if name == "oversized_first_user_id" {
				first.UserID = domain.UserID(strings.Repeat("a", ownedAuthorityHeadBytes-512))
			}
			if name == "foreign_first_scope" {
				first.SessionID = "foreign-session"
			}
			second := first
			second.TenantID = session.TenantID
			second.SessionID = session.ID
			second.UserID = "owner-b"
			a, _ := json.Marshal(first)
			b, _ := json.Marshal(second)
			f := &deletionCaptureFixture{statement: deletionCaptureParticipantTestSQL, columns: 1, values: [][]driver.Value{{string(a)}, {string(b)}}}
			if name == "close_error" {
				f.closeErr = errors.New("injected participant stream Close")
			}
			tx := deletionCaptureOrdinaryTx(t, f)
			g := deletionCaptureTestGraph(t, tx)
			err := g.addParticipantScopes(t.Context(), session)
			if (err != nil) != (name != "original") {
				t.Fatalf("stream participant case=%s err=%v", name, err)
			}
			if name == "oversized_first_user_id" || name == "foreign_first_scope" {
				if f.pulls != 1 || len(g.expected) != 0 {
					t.Fatalf("invalid first participant retained or later row read: pulls=%d owners=%d", f.pulls, len(g.expected))
				}
			} else if f.pulls != 2 {
				t.Fatalf("expected complete two-row stream/EOF, got pulls=%d", f.pulls)
			}
		})
	}
}

func TestRetainedDeletionCaptureActualRequestScopeRead(t *testing.T) {
	at := time.Date(2026, 10, 19, 1, 0, 0, 0, time.UTC)
	for _, name := range []string{"original_reaches_next_query", "foreign_tenant", "foreign_session"} {
		t.Run(name, func(t *testing.T) {
			d := domain.SessionDeletion{TenantID: "tenant-capture", SessionID: "session-capture", RequestedBy: "owner-capture", Reason: "controlled deletion", RequestedAt: at, State: domain.SessionDeletionRequested}
			if name == "foreign_tenant" {
				d.TenantID = "tenant-foreign"
			}
			if name == "foreign_session" {
				d.SessionID = "session-foreign"
			}
			if err := d.Validate(); err != nil {
				t.Fatal("foreign request must remain structurally valid", err)
			}
			raw, _ := json.Marshal(d)
			f := &deletionCaptureFixture{statement: `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_deletions WHERE tenant_id=$1 AND session_id=$2`, columns: 1, point: true, values: [][]driver.Value{{string(raw)}}}
			tx := deletionCaptureOrdinaryTx(t, f)
			got, err := buildRetainedDeletionCapture(t.Context(), &stateTx{sqlTx: tx, tenantID: "tenant-capture"}, "session-capture")
			if err == nil || got != nil {
				t.Fatalf("incomplete controlled capture unexpectedly accepted: result=%v err=%v", got, err)
			}
			wantCalls := 1
			if name == "original_reaches_next_query" {
				wantCalls = 2
			} // The fixture intentionally does not implement the next index-marker read.
			if f.calls != wantCalls {
				t.Fatalf("actual request scope guard: case=%s SQL calls=%d want=%d", name, f.calls, wantCalls)
			}
		})
	}
}

func TestRetainedDeletionCaptureActualOrdinaryTxDiscoveryBudget(t *testing.T) {
	at := time.Date(2026, 10, 19, 1, 0, 0, 0, time.UTC)
	for _, name := range []string{"first_over_existing_inventory_envelope", "first_invalid_participant"} {
		t.Run(name, func(t *testing.T) {
			tenant := domain.TenantID(strings.Repeat("t", 160))
			sessionID := domain.SessionID(strings.Repeat("s", 160))
			session := domain.Session{ID: sessionID, TenantID: tenant, CreatedBy: "owner-capture", Status: domain.SessionActive, CreatedAt: at, UpdatedAt: at}
			if err := session.Validate(); err != nil {
				t.Fatal(err)
			}
			d := domain.SessionDeletion{TenantID: tenant, SessionID: sessionID, RequestedBy: "owner-capture", Reason: "controlled deletion", RequestedAt: at, State: domain.SessionDeletionRequested}
			requestRaw, _ := json.Marshal(d)
			sessionRaw, _ := json.Marshal(session)
			requestSQL := `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_deletions WHERE tenant_id=$1 AND session_id=$2`
			markerSQL := `SELECT completed_at FROM session_lifecycle_backfill_state WHERE backfill_id = $1`
			runSQL := `SELECT SUBSTRING(CAST(run_id AS String),0,161) FROM runs_by_session WHERE tenant_id=$1 AND session_id=$2 ORDER BY tenant_id,session_id,created_at,run_id LIMIT $3`
			eventSQL := `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_events WHERE tenant_id=$1 AND session_id=$2 AND sequence>$3 ORDER BY sequence LIMIT $4`
			bindingSQL := `SELECT SUBSTRING(CAST(binding_id AS String),0,161) FROM frontend_bindings_by_session WHERE tenant_id=$1 AND session_id=$2 ORDER BY tenant_id,session_id,binding_id LIMIT $3`
			projectionSQL := `SELECT SUBSTRING(CAST(frontend_projection_id AS String),0,161) FROM frontend_projections_by_session WHERE tenant_id=$1 AND session_id=$2 ORDER BY tenant_id,session_id,frontend_projection_id LIMIT $3`
			sessionSQL := `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM sessions WHERE tenant_id=$1 AND session_id=$2`
			rows := make([][]driver.Value, maxSessionDeletionRows)
			spent := uint64(len(requestRaw)+len(sessionRaw)) + 8
			wantParticipantPulls := 0
			for i := range rows {
				id := domain.UserID(strings.Repeat("u", 150) + fmt.Sprintf("%010d", i))
				p := domain.SessionParticipant{TenantID: tenant, SessionID: sessionID, UserID: id, Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: at, UpdatedAt: at}
				if name == "first_invalid_participant" && i == 0 {
					p.UserID = domain.UserID(strings.Repeat("u", ownedAuthorityHeadBytes-512))
				} else if err := p.Validate(); err != nil {
					t.Fatalf("participant %d must be entirely valid: %v", i, err)
				}
				raw, _ := json.Marshal(p)
				rows[i] = []driver.Value{string(raw)}
				if wantParticipantPulls == 0 {
					if uint64(len(raw)) > ownedAuthorityMaxBytes-spent {
						wantParticipantPulls = i + 1
					} else {
						spent += uint64(len(raw))
					}
				}
			}
			if name == "first_invalid_participant" {
				wantParticipantPulls = 1
			}
			if wantParticipantPulls <= 0 || wantParticipantPulls >= int(maxSessionDeletionRows) {
				t.Fatal("fixture must cross the original byte envelope before row overflow", wantParticipantPulls)
			}
			f := &deletionCaptureFixture{rowsBySQL: map[string][][]driver.Value{requestSQL: {{string(requestRaw)}}, markerSQL: {{at}}, runSQL: nil, eventSQL: nil, deletionCaptureAdmissionTestSQL: nil, bindingSQL: nil, projectionSQL: nil, sessionSQL: {{string(sessionRaw)}}, deletionCaptureParticipantTestSQL: rows}, columnsBySQL: map[string]int{requestSQL: 1, markerSQL: 1, runSQL: 1, eventSQL: 1, deletionCaptureAdmissionTestSQL: 2, bindingSQL: 1, projectionSQL: 1, sessionSQL: 1, deletionCaptureParticipantTestSQL: 1}, pullsBySQL: map[string]int{}}
			tx := deletionCaptureOrdinaryTx(t, f)
			capture, err := buildRetainedDeletionCapture(t.Context(), &stateTx{sqlTx: tx, tenantID: tenant}, sessionID)
			if err == nil || capture != nil {
				t.Fatalf("actual ordinary-Tx capture admitted invalid/oversized discovery: result=%v err=%v", capture, err)
			}
			if got := f.pullsBySQL[deletionCaptureParticipantTestSQL]; got != wantParticipantPulls {
				t.Fatalf("actual ordinary-Tx first-over/invalid row: pulls=%d want=%d; totalSQL=%d err=%v", got, wantParticipantPulls, f.calls, err)
			}
			if f.calls != 9 {
				t.Fatalf("capture ran additional SQL after participant denial: calls=%d want=9", f.calls)
			}
		})
	}
}
func captureFixtureDB(t *testing.T, f *deletionCaptureFixture) *sql.DB {
	t.Helper()
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestRetainedDeletionMetadataExactPostimages(t *testing.T) {
	at := time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC)
	spec := deletionCaptureSchema("test_owned", "tenant_id,sequence", "tenant_id:s,sequence:u,at:t,payload:j,nullable:s")
	base := [][]driver.Value{{"tenant-a", uint64(2), at, `{"b":2,"a":1}`, nil}, {"tenant-a", uint64(math.MaxUint64), at, `{"a":3}`, ""}}
	digest := func(t *testing.T, rows [][]driver.Value) (string, error) {
		t.Helper()
		f := &deletionCaptureFixture{spec: spec, values: rows}
		db := captureFixtureDB(t, f)
		d, n, e := captureDeletionMetadata(t.Context(), []deletionCaptureScope{{spec: spec, prefix: []any{"tenant-a"}, query: db}}, 1<<20)
		if e == nil && (n != 2 || f.queries != 1) {
			t.Fatalf("rows=%d queries=%d", n, f.queries)
		}
		return d, e
	}
	want, err := digest(t, base)
	if err != nil || len(want) != 64 {
		t.Fatalf("original complete typed capture: digest=%s err=%v", want, err)
	}
	for _, tc := range []struct {
		name   string
		column int
		value  driver.Value
	}{
		{"same_count_scalar", 2, at.Add(time.Microsecond)},
		{"same_count_json", 3, `{"a":4}`},
		{"null_to_present_empty", 4, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := [][]driver.Value{append([]driver.Value(nil), base[0]...), append([]driver.Value(nil), base[1]...)}
			rows[0][tc.column] = tc.value
			got, err := digest(t, rows)
			if err != nil || got == want {
				t.Fatalf("changed exact physical postimage: got=%s original=%s err=%v", got, want, err)
			}
		})
	}
	t.Run("json_key_order", func(t *testing.T) {
		rows := [][]driver.Value{append([]driver.Value(nil), base[0]...), append([]driver.Value(nil), base[1]...)}
		rows[0][3] = `{"a":1,"b":2}`
		got, err := digest(t, rows)
		if err != nil || got != want {
			t.Fatalf("canonical JSON object key order: got=%s want=%s err=%v", got, want, err)
		}
	})
}

func TestRetainedDeletionMetadataCausalBounds(t *testing.T) {
	spec := deletionCaptureSchema("test_owned", "tenant_id,sequence", "tenant_id:s,sequence:u,payload:j")
	for _, tc := range []struct {
		name     string
		rows     [][]driver.Value
		closeErr error
		bytes    uint64
	}{
		{"foreign_prefix", [][]driver.Value{{"tenant-b", uint64(1), `{}`}}, nil, 1 << 20},
		{"duplicate_key", [][]driver.Value{{"tenant-a", uint64(1), `{}`}, {"tenant-a", uint64(1), `{}`}}, nil, 1 << 20},
		{"reordered_numeric_key", [][]driver.Value{{"tenant-a", uint64(10), `{}`}, {"tenant-a", uint64(2), `{}`}}, nil, 1 << 20},
		{"null_primary_key", [][]driver.Value{{"tenant-a", nil, `{}`}}, nil, 1 << 20},
		{"json_duplicate_member", [][]driver.Value{{"tenant-a", uint64(1), `{"a":1,"a":2}`}}, nil, 1 << 20},
		{"json_trailing_value", [][]driver.Value{{"tenant-a", uint64(1), `{} {}`}}, nil, 1 << 20},
		{"text_overflow_sentinel", [][]driver.Value{{strings.Repeat("a", ownedAuthorityHeadBytes+1), uint64(1), `{}`}}, nil, 1 << 20},
		{"close_failure", [][]driver.Value{{"tenant-a", uint64(1), `{}`}}, errors.New("injected Close"), 1 << 20},
		{"aggregate_material_overflow", [][]driver.Value{{"tenant-a", uint64(1), `{}`}}, nil, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &deletionCaptureFixture{spec: spec, values: tc.rows, closeErr: tc.closeErr}
			db := captureFixtureDB(t, f)
			if d, n, err := captureDeletionMetadata(t.Context(), []deletionCaptureScope{{spec: spec, prefix: []any{"tenant-a"}, query: db}}, tc.bytes); err == nil {
				t.Fatalf("invalid capture accepted digest=%s rows=%d", d, n)
			}
		})
	}
	t.Run("aggregate_row_overflow", func(t *testing.T) {
		rows := make([][]driver.Value, maxSessionDeletionRows+1)
		for i := range rows {
			rows[i] = []driver.Value{"tenant-a", uint64(i + 1), `{}`}
		}
		f := &deletionCaptureFixture{spec: spec, values: rows}
		db := captureFixtureDB(t, f)
		if _, _, err := captureDeletionMetadata(t.Context(), []deletionCaptureScope{{spec: spec, prefix: []any{"tenant-a"}, query: db}}, 16<<20); err == nil {
			t.Fatal("10001st physical row accepted")
		}
	})
}

func TestRetainedDeletionMetadataAbsentRangeBinding(t *testing.T) {
	spec := deletionCaptureSchema("test_owned", "tenant_id,sequence", "tenant_id:s,sequence:u,payload:j")
	f := &deletionCaptureFixture{spec: spec}
	db := captureFixtureDB(t, f)
	capture := func(prefix []any) (string, uint64, error) {
		return captureDeletionMetadata(t.Context(), []deletionCaptureScope{{spec: spec, prefix: prefix, query: db}}, 1<<20)
	}
	empty, n, err := capture([]any{"tenant-a"})
	if err != nil || n != 0 {
		t.Fatalf("empty range capture rows=%d err=%v", n, err)
	}
	absent, n, err := capture([]any{"tenant-a", uint64(1)})
	if err != nil || n != 0 || absent == empty {
		t.Fatalf("point absence must differ from range EOF rows=%d err=%v", n, err)
	}
	f.values = [][]driver.Value{{"tenant-a", uint64(1), `{}`}}
	present, n, err := capture([]any{"tenant-a", uint64(1)})
	if err != nil || n != 1 || present == absent {
		t.Fatalf("absent-to-present must change capture rows=%d err=%v", n, err)
	}
}
