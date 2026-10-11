package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Actual SDK/canonical helpers, not a native isolation or admission fixture.
type canonicalBudgetFixture struct {
	*ratedSQLFixture
	at                                                               time.Time
	snapshots                                                        [][]driver.Value
	rowsClosed, rowsRead, queryCalls, persistedWrites, pendingWrites int
	rowError                                                         error
}

func (f *canonicalBudgetFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *canonicalBudgetFixture) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	inner, err := f.ratedSQLFixture.BeginTx(ctx, options)
	f.pendingWrites = 0
	return canonicalBudgetTx{f, inner}, err
}

type canonicalBudgetTx struct {
	f     *canonicalBudgetFixture
	inner driver.Tx
}

func (tx canonicalBudgetTx) Commit() error {
	mode := ""
	if len(tx.f.commitModes) > 0 {
		mode = tx.f.commitModes[0]
	}
	err := tx.inner.Commit()
	if mode != "rollback" {
		tx.f.persistedWrites += tx.f.pendingWrites
	}
	tx.f.pendingWrites = 0
	return err
}
func (tx canonicalBudgetTx) Rollback() error { tx.f.pendingWrites = 0; return tx.inner.Rollback() }
func (f *canonicalBudgetFixture) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f.queryCalls++
	f.statements = append(f.statements, statement)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	if _, ok := ctx.Deadline(); !ok {
		f.deadlineMissing = true
	}
	if statement == `SELECT CurrentUtcTimestamp()` {
		return &explanationFixtureRows{values: []driver.Value{f.at}}, nil
	}
	if statement == admittedSnapshotMetadataSQL {
		if len(args) != 4 || args[3].Value != int64(16) {
			return nil, errors.New("snapshot must use explicit row bound")
		}
		return &canonicalBudgetRows{f: f, rows: f.snapshots, fail: f.rowError}, nil
	}
	for original, limit := range ownedCanonicalJSONReads {
		bounded, _, _ := ownedCanonicalJSONSelect(original)
		if statement != bounded {
			continue
		}
		fields := strings.Fields(original)
		row := append([]driver.Value(nil), f.rows[fields[3]]...)
		if len(row) > 0 {
			if raw, ok := row[0].(string); ok && len(raw) > limit+1 {
				row[0] = raw[:limit+1]
			}
		}
		return &explanationFixtureRows{values: row}, nil
	}
	if strings.Contains(statement, "FROM run_idempotency") && strings.Contains(statement, "SUBSTRING(CAST(run_id AS String),0,161)") {
		return &explanationFixtureRows{values: f.rows["run_idempotency"]}, nil
	}
	if strings.Contains(statement, "FROM run_explanation_heads_v1") && strings.Contains(statement, "8193") {
		return &explanationFixtureRows{values: f.rows["run_explanation_heads_v1"]}, nil
	}
	return nil, errors.New("fixture rejects unbounded or unknown SQL")
}
func (f *canonicalBudgetFixture) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	f.statements = append(f.statements, statement)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	if _, ok := ctx.Deadline(); !ok {
		f.deadlineMissing = true
	}
	if !strings.HasPrefix(strings.TrimSpace(statement), "UPSERT INTO ") && !strings.HasPrefix(strings.TrimSpace(statement), "DELETE FROM quota_expiry") {
		return nil, errors.New("fixture expected canonical write")
	}
	f.writes++
	f.pendingWrites++
	return driver.RowsAffected(1), nil
}

type canonicalBudgetRows struct {
	f    *canonicalBudgetFixture
	rows [][]driver.Value
	fail error
}

func (*canonicalBudgetRows) Columns() []string { return []string{"record"} }
func (r *canonicalBudgetRows) Close() error    { r.f.rowsClosed++; return nil }
func (r *canonicalBudgetRows) Next(dest []driver.Value) error {
	if len(r.rows) == 0 {
		if r.fail != nil {
			err := r.fail
			r.fail = nil
			return err
		}
		return io.EOF
	}
	copy(dest, r.rows[0])
	r.rows = r.rows[1:]
	r.f.rowsRead++
	if text, ok := dest[0].(string); ok && len(text) > admittedSnapshotMetadataBytes+1 {
		dest[0] = text[:admittedSnapshotMetadataBytes+1]
	}
	return nil
}
func canonicalFixture(t *testing.T) (*Store, *canonicalBudgetFixture, ports.WorkerJobState) {
	t.Helper()
	s := currentWorkerAuthorityFixture(t)
	f := &canonicalBudgetFixture{ratedSQLFixture: &ratedSQLFixture{rows: make(map[string][]driver.Value), slots: make(map[uint32][]driver.Value)}, at: s.Job.CreatedAt}
	for table, value := range map[string]any{"runs": s.Run, "attempts": s.Attempt, "quota_reservations": s.Reservation, "artifact_manifests": s.InputManifest, "worker_jobs": s.Job} {
		ratedFixturePut(t, f.ratedSQLFixture, table, value)
	}
	f.rows["run_idempotency"] = []driver.Value{string(s.Run.ID)}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return &Store{db: db, idempotencyRetention: time.Hour}, f, s
}

func TestOwnedCanonicalExecutorActualWritersShareBudgetAndClock(t *testing.T) {
	store, f, s := canonicalFixture(t)
	store.attachedWorkerNow = func(context.Context, rowQuery) (time.Time, error) {
		t.Error("legacy hook escaped owned clock")
		return time.Time{}, nil
	}
	var budget *ownedAuthorityBudget
	err := store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		budget = q.budget
		tx := q.canonicalState(store, s.Run.TenantID)
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil || !at.Equal(q.at) {
			return errors.New("shared clock lost")
		}
		if err := tx.PutRun(ctx, s.Run); err != nil {
			return err
		}
		if err := tx.PutQuotaReservation(ctx, s.Reservation); err != nil {
			return err
		}
		return tx.PutWorkerJob(ctx, s.Job)
	})
	wantBytes := uint64(8 + 3*len(f.rows["runs"][0].(string)) + 2*len(f.rows["quota_reservations"][0].(string)) + len(f.rows["attempts"][0].(string)) + len(f.rows["artifact_manifests"][0].(string)) + len(s.Run.ID))
	if err != nil || budget == nil || budget.statements != 22 || budget.bytes != wantBytes || f.commits != 1 || f.writes != 9 || f.persistedWrites != 9 || len(f.isolations) != 1 || f.isolations[0] != driver.IsolationLevel(sql.LevelSerializable) || f.deadlineMissing {
		t.Fatalf("err=%v statements=%d bytes=%d wantBytes=%d commits=%d writes=%d persisted=%d isolation=%v deadlineMissing=%t", err, budget.statements, budget.bytes, wantBytes, f.commits, f.writes, f.persistedWrites, f.isolations, f.deadlineMissing)
	}
}

func TestOwnedCanonicalExecutorIgnoredExhaustionRollsBackCanonicalWrites(t *testing.T) {
	store, f, s := canonicalFixture(t)
	err := store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		tx := q.canonicalState(store, s.Run.TenantID)
		if err := tx.PutRun(ctx, s.Run); err != nil {
			return err
		}
		for i := 0; i < 25; i++ {
			_, _, _ = tx.GetRun(ctx, s.Run.ID)
		}
		return nil
	})
	if !errors.Is(err, domain.ErrMembershipDenied) || len(f.statements) != 32 || f.writes != 3 || f.persistedWrites != 0 || f.commits != 0 || f.rollbacks != 1 {
		t.Fatalf("err=%v statements=%d writes=%d persisted=%d commits=%d rollback=%d", err, len(f.statements), f.writes, f.persistedWrites, f.commits, f.rollbacks)
	}
}

func TestOwnedCanonicalExecutorPointReadsAreBoundedAndFailClosed(t *testing.T) {
	for _, c := range []struct {
		name    string
		raw     driver.Value
		wantErr bool
	}{
		{"exact_cap", `{"value":"` + strings.Repeat("a", ownedAuthorityHeadBytes-12) + `"}`, false},
		{"cap_plus_one", `{"value":"` + strings.Repeat("a", ownedAuthorityHeadBytes-11) + `"}`, true},
		{"malformed", "{", true}, {"null_cell", nil, true}, {"absent", nil, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			store, f, s := canonicalFixture(t)
			f.rows["runs"] = []driver.Value{c.raw}
			if c.name == "absent" {
				f.rows["runs"] = nil
			}
			var b *ownedAuthorityBudget
			err := store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
				b = q.budget
				_, _, err := readJSON[map[string]string](ctx, q, "SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2", s.Run.TenantID, s.Run.ID)
				if c.wantErr {
					_ = err
					return nil
				} // Sticky failure must prevent ignored-error commit.
				return err
			})
			if (err != nil) != c.wantErr || f.commits != map[bool]int{true: 0, false: 1}[c.wantErr] {
				t.Fatalf("err=%v commits=%d bytes=%d", err, f.commits, b.bytes)
			}
			if raw, ok := c.raw.(string); ok && b.bytes != 8+uint64(min(len(raw), ownedAuthorityHeadBytes+1)) {
				t.Fatalf("charged=%d raw=%d", b.bytes, len(raw))
			}
		})
	}
	t.Run("unknown_query_no_fallback", func(t *testing.T) {
		store, f, _ := canonicalFixture(t)
		err := store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
			_, _, _ = readJSON[domain.Run](ctx, q, "SELECT payload FROM runs LIMIT 1")
			return nil
		})
		if !errors.Is(err, domain.ErrMembershipDenied) || f.queryCalls != 1 || f.commits != 0 {
			t.Fatalf("err=%v queries=%d commits=%d", err, f.queryCalls, f.commits)
		}
	})
}

func canonicalSnapshot(t *testing.T, s ports.WorkerJobState, version uint64) string {
	t.Helper()
	v := domain.SessionSnapshot{ID: "snapshot-test", TenantID: s.Run.TenantID, SessionID: s.Run.SessionID, Version: version, ThroughSequence: 1, FormatVersion: 1, Compression: "zstd", EventCount: 1, UncompressedSize: 10, CreatedAt: s.Job.CreatedAt,
		Payload: domain.BlobRef{TenantID: s.Run.TenantID, Key: domain.SessionSnapshotObjectKey(s.Run.TenantID, s.Run.SessionID, version), Size: 10, SHA256: strings.Repeat("a", 64)}}
	if err := v.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func TestOwnedCanonicalExecutorSnapshotDrainsChargesAndCloses(t *testing.T) {
	for _, name := range []string{"all_rows", "overflow_after_valid", "row_error_after_valid", "seventeenth_row", "statement_limit"} {
		t.Run(name, func(t *testing.T) {
			store, f, s := canonicalFixture(t)
			raw := canonicalSnapshot(t, s, 3)
			f.snapshots = [][]driver.Value{{raw}, {canonicalSnapshot(t, s, 2)}}
			if name == "overflow_after_valid" {
				f.snapshots[1][0] = strings.Repeat("a", admittedSnapshotMetadataBytes+1)
			}
			if name == "row_error_after_valid" {
				f.rowError = errors.New("fixture row stream failed")
			}
			if name == "seventeenth_row" {
				f.snapshots = nil
				for i := 0; i < 17; i++ {
					f.snapshots = append(f.snapshots, []driver.Value{raw})
				}
			}
			var b *ownedAuthorityBudget
			var got *domain.SessionContextWindow
			err := store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
				b = q.budget
				if name == "statement_limit" {
					for i := 0; i < 31; i++ {
						if _, _, err := q.canonicalState(store, s.Run.TenantID).GetRun(ctx, s.Run.ID); err != nil {
							return err
						}
					}
				}
				var err error
				got, err = selectAdmittedContextWindow(ctx, q.canonicalState(store, s.Run.TenantID), s.Run.SessionID, &domain.SessionContextWindow{ThroughSequence: 1})
				return err
			})
			if name == "all_rows" {
				wantBytes := uint64(8 + len(raw) + len(f.snapshots[1][0].(string)))
				if err != nil || got == nil || got.SnapshotVersion == nil || *got.SnapshotVersion != 3 || f.rowsRead != 2 || b.bytes != wantBytes || f.commits != 1 {
					t.Fatalf("err=%v got=%+v rows=%d bytes=%d want=%d commits=%d", err, got, f.rowsRead, b.bytes, wantBytes, f.commits)
				}
			} else if err == nil || f.commits != 0 || got != nil {
				t.Fatalf("err=%v got=%+v commits=%d", err, got, f.commits)
			}
			if name == "statement_limit" {
				if f.queryCalls != 32 || f.rowsClosed != 0 {
					t.Fatalf("queries=%d closed=%d", f.queryCalls, f.rowsClosed)
				}
			} else if f.rowsClosed != 1 {
				t.Fatalf("rows close=%d", f.rowsClosed)
			}
		})
	}
}

func TestOwnedCanonicalExecutorActualReadRetriesRetainAggregateBudget(t *testing.T) {
	store, f, s := canonicalFixture(t)
	f.commitModes = []string{"rollback", "rollback"}
	var b *ownedAuthorityBudget
	var states []*stateTx
	err := store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		b = q.budget
		tx := q.canonicalState(store, s.Run.TenantID)
		states = append(states, tx)
		for i := 0; i < 31; i++ {
			if _, _, err := tx.GetRun(ctx, s.Run.ID); err != nil {
				return err
			}
		}
		return nil
	})
	raw := f.rows["runs"][0].(string)
	if err != nil || b.statements != 96 || b.callbacks != 3 || b.bytes != uint64(24+93*len(raw)) || f.commits != 3 || len(states) != 3 || states[0] == states[1] || states[1] == states[2] || f.deadlineMissing {
		t.Fatalf("err=%v statements=%d callbacks=%d bytes=%d commits=%d states=%d deadline=%t", err, b.statements, b.callbacks, b.bytes, f.commits, len(states), f.deadlineMissing)
	}
	if !reflect.DeepEqual(f.isolations, []driver.IsolationLevel{driver.IsolationLevel(sql.LevelSerializable), driver.IsolationLevel(sql.LevelSerializable), driver.IsolationLevel(sql.LevelSerializable)}) {
		t.Fatal(f.isolations)
	}
}

func TestOwnedCanonicalExecutorLargeSourceBearingEnvelope(t *testing.T) {
	store, f, s := canonicalFixture(t)
	r := newOwnedReaderFixtureAt(t, f.at, strings.Repeat("x", 149))
	for i := range r.spending {
		r.spending[i].MutationID = strings.Repeat("m", 160)
	}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(domain.AttemptExecutionSourceScopeV1{TenantID: r.scope.tenant, SessionID: r.scope.session, OwnerUserID: r.scope.owner, RunID: r.scope.run, AttemptID: r.scope.attempt}, r.profile, r.spending, f.at)
	if err != nil {
		t.Fatal(err)
	}
	source, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
	if err != nil {
		t.Fatal(err)
	}
	s.Run.TenantID, s.Run.SessionID, s.Run.ID = r.scope.tenant, r.scope.session, r.scope.run
	s.Job.TenantID, s.Job.SessionID, s.Job.RunID, s.Job.AttemptID = r.scope.tenant, r.scope.session, r.scope.run, r.scope.attempt
	s.Job.CredentialOwnerUserID = r.scope.owner
	s.Job.ExecutionPlacementV2, s.Job.HarnessBinding, s.Job.SubstrateBinding, s.Job.AdmissionCostCeiling = pkg.Placement, pkg.Binding, &pkg.Substrate, &pkg.Cost
	s.Job.OwnedExecutionSourceRecord = string(source)
	s.Job.ContextSnapshot.TenantID, s.Job.ContextSnapshot.Key = r.scope.tenant, domain.SessionObjectPrefix(r.scope.tenant, r.scope.session)+"context"
	s.Job.DeliveryChat.TenantID = r.scope.tenant
	// Existing canonical metadata supports an explicit server allowlist. These
	// are test-owned names, NOT approval of MCP endpoints or paid/live services.
	for i := 0; i < 100; i++ {
		s.Job.AllowedMCPServers = append(s.Job.AllowedMCPServers, strings.Repeat("s", 150)+string(rune('A'+i)))
	}
	if err := s.Job.ValidateForRun(s.Run); err != nil {
		t.Fatal(err)
	}
	raw, err := marshal(s.Job)
	if err != nil {
		t.Fatal(err)
	}
	if len(source) > ownedAuthorityHeadBytes || len(raw) <= ownedAuthorityHeadBytes || len(raw) > ownedCanonicalEnvelopeBytes {
		t.Fatalf("source=%d envelope=%d", len(source), len(raw))
	}
	f.rows["worker_jobs"] = []driver.Value{raw}
	var got domain.WorkerJob
	err = store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		var err error
		got, _, err = readJSON[domain.WorkerJob](ctx, q, "SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2", s.Run.TenantID, s.Run.ID)
		return err
	})
	if err != nil || !reflect.DeepEqual(got, s.Job) || got.OwnedExecutionSourceRecord != string(source) {
		t.Fatalf("err=%v source=%d envelope=%d exact=%t", err, len(source), len(raw), reflect.DeepEqual(got, s.Job))
	}
	t.Logf("legitimate structural source=%d outerJob=%d; not current installation/admission permission", len(source), len(raw))
}
