package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Scripted storage only. These tests prove the actual lifecycle methods reach
// their same-transaction custody guard, not native YDB concurrency/isolation.
type terminalDeletionSQL struct {
	rows              [][]driver.Value
	deletion          *domain.SessionDeletion
	guards, mutations int
	failClose         bool
}
type terminalDeletionConn struct{ f *terminalDeletionSQL }

func (f *terminalDeletionSQL) Connect(context.Context) (driver.Conn, error) {
	return &terminalDeletionConn{f}, nil
}
func (*terminalDeletionSQL) Driver() driver.Driver { return ownedAdmissionJoinDriver{} }
func (*terminalDeletionConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("no prepared statement")
}
func (*terminalDeletionConn) Close() error                { return nil }
func (c *terminalDeletionConn) Begin() (driver.Tx, error) { return ownedAdmissionJoinTx{}, nil }
func (c *terminalDeletionConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return ownedAdmissionJoinTx{}, nil
}
func (c *terminalDeletionConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.f.mutations++
	return nil, errors.New("unexpected deletion mutation")
}
func (c *terminalDeletionConn) QueryContext(_ context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if statement == sessionDeletionRunsSQL {
		c.f.guards++
		if len(args) != 3 || args[0].Value != "tenant-test" || args[1].Value != "session-test" || args[2].Value != int64(maxSessionDeletionRows+1) {
			return nil, errors.New("wrong deletion tenant/session/hard bound")
		}
		return &compactionSourceRows{rows: c.f.rows, failClose: c.f.failClose}, nil
	}
	if strings.Contains(statement, "FROM session_deletions") {
		if c.f.deletion == nil {
			return &compactionSourceRows{}, nil
		}
		raw, _ := json.Marshal(c.f.deletion)
		return &compactionSourceRows{rows: [][]driver.Value{{string(raw)}}}, nil
	}
	if strings.Contains(statement, "FROM session_lifecycle_backfill_state") {
		return &compactionSourceRows{rows: [][]driver.Value{{time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)}}}, nil
	}
	// The inventory is deliberately empty to isolate the final transaction's
	// guard. It is not exact-object inventory or physical erasure evidence.
	for _, table := range []string{"session_legal_holds", "session_events", "session_snapshots", "session_material_current_v1", "session_material_boundaries_v1", "session_snapshot_material_v1", "session_participants", "frontend_bindings_by_session", "frontend_projections_by_session", "runs_by_session", "attempt_execution_admissions_by_session_v1"} {
		if strings.Contains(statement, "FROM "+table) {
			return &compactionSourceRows{}, nil
		}
	}
	return nil, errors.New("unexpected lifecycle query: " + statement)
}
func terminalDeletionStore(t *testing.T, f *terminalDeletionSQL) *Store {
	t.Helper()
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return &Store{db: db}
}
func terminalDeletionRun(t *testing.T) (domain.Run, *domain.RunTerminalCustodyV1) {
	t.Helper()
	_, _, run := runPayloadFixture(t)
	at := run.UpdatedAt.Add(time.Second)
	run.Status = domain.RunFailed
	run.UpdatedAt = at
	run.FinishedAt = &at
	h := strings.Repeat("a", 64)
	scope := runPayloadMetadata(t, run).Inventory[0].Scope
	key, err := domain.TerminalCustodyObjectKeyV1(scope, h)
	if err != nil {
		t.Fatal(err)
	}
	custody := &domain.RunTerminalCustodyV1{Version: 1, Plan: domain.TerminalCustodyPlanV1{Scope: scope, Capsule: domain.BlobRef{TenantID: run.TenantID, Key: key, Size: 512, SHA256: h}, FrontierDigest: h}}
	if run.ID != "run-test" || run.TenantID != "tenant-test" || run.SessionID != "session-test" || run.Validate() != nil {
		t.Fatal("unexpected canonical fixture Run", run.ID, run.TenantID, run.SessionID, run.Validate())
	}
	return run, custody
}
func terminalDeletionRow(t *testing.T, run domain.Run) []driver.Value {
	t.Helper()
	raw, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	return []driver.Value{string(run.ID), string(run.Status), string(raw)}
}

func TestLocalTerminalDeletionRequiresOriginalReconciliation(t *testing.T) {
	base, custody := terminalDeletionRun(t)
	for _, name := range []string{"terminal_without_plan", "registered_failed_plan", "registered_succeeded_plan", "apparent_ready_evidence", "missing", "malformed", "oversized", "foreign", "index_status_mismatch", "oversized_index", "close_error", "row_bound"} {
		t.Run(name, func(t *testing.T) {
			run := base.Clone()
			f := &terminalDeletionSQL{}
			switch name {
			case "registered_failed_plan", "registered_succeeded_plan", "apparent_ready_evidence":
				v := custody.Clone()
				run.TerminalCustody = &v
				if name != "registered_failed_plan" {
					run.Status = domain.RunSucceeded
				}
				if name == "apparent_ready_evidence" {
					// Structurally valid DATA is intentionally not an authentic
					// producer or writer-stop/retirement/erasure receipt.
					h := strings.Repeat("b", 64)
					derived, err := domain.TerminalDerivedContextDigestV1(nil)
					if err != nil {
						t.Fatal(err)
					}
					v.Correspondence = &domain.TerminalCorrespondenceV1{Version: 1, CapsuleDigest: v.Plan.Capsule.SHA256, TerminalAssistantEventID: "assistant-test", CanonicalEventsDigest: h, DerivedContextDigest: derived, AccountingDispositionDigest: h}
					e := domain.LocalExecutionEvidenceV1{Version: 1, Scope: v.Plan.Scope, ParentDigest: h, SourceDigest: h, ProfileDigest: h, OriginalAllocationDigest: h, RetiredAllocationDigest: strings.Repeat("c", 64), FrontierDigest: v.Plan.FrontierDigest, Capsule: v.Plan.Capsule, ResidualInventoryDigest: h, ResidualReceiptDigest: h, AccountingDispositionDigest: h, StoppedAt: run.CreatedAt, RetiredAt: run.CreatedAt, ErasedAt: run.CreatedAt}
					e.EvidenceDigest, err = e.Digest()
					if err != nil {
						t.Fatal(err)
					}
					v.LocalEvidence = &e
					if err := run.Validate(); err != nil {
						t.Fatal("valid non-authoritative ready DATA", err)
					}
				}
			case "foreign":
				run.SessionID = "foreign-session"
			}
			f.rows = [][]driver.Value{terminalDeletionRow(t, run)}
			switch name {
			case "missing":
				f.rows[0][2] = nil
			case "malformed":
				f.rows[0][2] = "{"
			case "oversized":
				f.rows[0][2] = strings.Repeat(" ", 32769)
			case "foreign":
				f.rows[0][0] = "run-test"
			case "index_status_mismatch":
				f.rows[0][1] = string(domain.RunSucceeded)
			case "oversized_index":
				f.rows[0][0] = strings.Repeat("a", 161)
			case "close_error":
				f.failClose = true
			case "row_bound":
				f.rows = make([][]driver.Value, maxSessionDeletionRows+1)
				for i := range f.rows {
					f.rows[i] = terminalDeletionRow(t, run)
				}
			}
			store := terminalDeletionStore(t, f)
			err := store.Transact(t.Context(), base.TenantID, func(tx ports.StateTx) error {
				return ensureSessionRunsTerminalTx(t.Context(), tx.(*stateTx), base.SessionID)
			})
			if (err == nil) != (name == "terminal_without_plan") || f.guards != 1 || f.mutations != 0 {
				t.Fatalf("deletion guard name=%s err=%v queries=%d mutations=%d", name, err, f.guards, f.mutations)
			}
		})
	}
}

func TestLocalTerminalDeletionGuardsActualLifecycleRetries(t *testing.T) {
	run, custody := terminalDeletionRun(t)
	run.TerminalCustody = custody
	for _, phase := range []string{"request_requested", "request_deleting", "start_requested", "start_deleting", "complete_deleting"} {
		t.Run(phase, func(t *testing.T) {
			deletion := domain.SessionDeletion{TenantID: run.TenantID, SessionID: run.SessionID, RequestedBy: "user-test", Reason: "test owned custody", State: domain.SessionDeletionRequested, RequestedAt: run.CreatedAt}
			if strings.HasSuffix(phase, "deleting") {
				at := run.CreatedAt.Add(time.Second)
				if err := deletion.Start(at); err != nil {
					t.Fatal(err)
				}
			}
			f := &terminalDeletionSQL{rows: [][]driver.Value{terminalDeletionRow(t, run)}, deletion: &deletion}
			store := terminalDeletionStore(t, f)
			before := deletion
			var err error
			switch {
			case strings.HasPrefix(phase, "request"):
				request := deletion
				request.State = domain.SessionDeletionRequested
				request.StartedAt = nil
				_, err = store.RequestSessionDeletion(t.Context(), request)
			case strings.HasPrefix(phase, "start"):
				_, err = store.StartSessionDeletion(t.Context(), run.TenantID, run.SessionID, run.UpdatedAt)
			default:
				_, err = store.CompleteSessionDeletion(t.Context(), run.TenantID, run.SessionID, run.UpdatedAt, 0, 0)
			}
			var validation domain.ValidationError
			if !errors.As(err, &validation) || validation.Field != "session_deletion.terminal_custody" || f.guards != 1 || f.mutations != 0 || !reflect.DeepEqual(deletion, before) {
				t.Fatalf("phase=%s guard err=%v queries=%d mutations=%d", phase, err, f.guards, f.mutations)
			}
		})
	}
}
