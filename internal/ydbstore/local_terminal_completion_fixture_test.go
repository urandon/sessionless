package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// This is a strict scripted SQL model, NOT native YDB atomicity proof. It is
// installed before NewLocalV1 and delegates the original authority/operation
// SQL unchanged. Canonical completion executes the actual production helpers;
// no private owner, measured usage, evidence or finalization is manufactured.
type LocalTerminalCompletionSQLFixture struct {
	cleanup               *LocalTerminalCleanupSQLFixture
	state                 LocalTerminalCompletionSnapshot
	active                bool
	trace                 []string
	transactionCounts     []int
	currentTransaction    int
	lastSQL, lastError    string
	completionCommitFault string
	lostACKs              int
	terminalExecAttempts  int
}

type LocalTerminalCompletionSnapshot struct {
	Run                               domain.Run
	Attempt                           domain.Attempt
	Reservation                       domain.QuotaReservation
	Manifest                          domain.ArtifactManifest
	Session                           domain.Session
	Events                            []domain.SessionEvent
	EvidenceDigest, EvidenceRecord    string
	FinalStatus, FinalDigest          string
	Finalizations, Commits, Mutations int
	LostACKs                          int
	LeasePresent                      bool
	ActiveRuns                        uint32
	CountersPresent                   bool
	ActiveRun, ActiveReservation      string
	ReplayRowsDigest                  string
	material                          map[string][]driver.Value
	eventIndex                        map[string][]driver.Value
	eventRecords                      map[string]string
}

type LocalTerminalCompletionTrace struct {
	TransactionCounts  []int
	LastShapes         []string
	LastSQL, LastError string
	ExecAttempts       int
}

func (f *LocalTerminalCompletionSQLFixture) Trace() LocalTerminalCompletionTrace {
	counts := f.transactionCounts
	if len(counts) > 8 {
		counts = counts[len(counts)-8:]
	}
	shapes := f.trace
	if len(shapes) > 8 {
		shapes = shapes[len(shapes)-8:]
	}
	return LocalTerminalCompletionTrace{TransactionCounts: append([]int(nil), counts...), LastShapes: append([]string(nil), shapes...), LastSQL: f.lastSQL, LastError: f.lastError, ExecAttempts: f.terminalExecAttempts}
}
func (f *LocalTerminalCompletionSQLFixture) traceSQL(statement string) func(error) {
	shape := completionSQL(statement)
	if len(shape) > 200 {
		shape = shape[:200]
	}
	f.lastSQL = shape
	f.lastError = ""
	if len(f.trace) == 64 {
		copy(f.trace, f.trace[1:])
		f.trace = f.trace[:63]
	}
	f.trace = append(f.trace, shape)
	if f.currentTransaction >= 0 && f.currentTransaction < len(f.transactionCounts) {
		f.transactionCounts[f.currentTransaction]++
	}
	return func(err error) {
		if err != nil {
			f.lastError = err.Error()
			if len(f.lastError) > 160 {
				f.lastError = f.lastError[:160]
			}
		}
	}
}

// Inspection of actual original receipts only. No permission, mutation,
// reconstructed receipt or artificial clock/timestamp is supplied to runtime.
type LocalTerminalCompletionDiagnostic struct {
	Original, ResidualValid, AccountingValid, PreparedValid bool
	RetainedValid, ReplayValid, ReplayMatched, RejoinValid  bool
	Stage, PhaseError                                       string
	StoppedAt, RetiredAt, ErasedAt                          time.Time
}

func (f *LocalTerminalCompletionSQLFixture) DiagnoseCleanup(t *testing.T, r *LocalTerminalCleanupV1) LocalTerminalCompletionDiagnostic {
	t.Helper()
	var d LocalTerminalCompletionDiagnostic
	if r == nil || r.self != r || !r.owner.original() {
		return d
	}
	p := r.owner
	if !p.parent.entry.preparationMu.TryLock() {
		t.Fatal("diagnostic requires idle original terminal barrier")
	}
	defer p.parent.entry.preparationMu.Unlock()
	if p.parent.source == nil || p.parent.source.state == nil {
		return d
	}
	var c *localTerminalCompletionV2
	func() {
		p.parent.source.state.mu.Lock()
		defer p.parent.source.state.mu.Unlock()
		p.mu.Lock()
		defer p.mu.Unlock()
		d.Original = p.cleanup == r
		d.StoppedAt, d.RetiredAt = r.stoppedAt, r.allocation.Current.UpdatedAt
		d.ErasedAt, _ = r.erasure.ClosedAt()
		if p.accounting != nil {
			_, e := p.accounting.Digest()
			d.AccountingValid = e == nil
			_, e = p.accounting.Residual(r.allocation.Current, r.erasure)
			d.ResidualValid = e == nil
		}
		var e error
		c = p.completion
		if c == nil {
			c, e = prepareLocalTerminalCompletion(r)
		}
		d.PreparedValid = e == nil && c != nil && c.original()
	}()
	if !d.PreparedValid {
		d.Stage = "prepare"
		return d
	}
	phaseError := func(stage string, e error) {
		d.Stage = stage
		if e != nil {
			d.PhaseError = e.Error()
			if len(d.PhaseError) > 160 {
				d.PhaseError = d.PhaseError[:160]
			}
		}
	}
	// These are genuine read-only production phases. Release source/output
	// locks before actual blob reads, retaining the original entry barrier.
	call, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	materials, e := c.readRetained(call)
	if e != nil {
		phaseError("retained", e)
		return d
	}
	defer materials.Release()
	d.RetainedValid = true
	store := p.parent.entry.owner.store
	e = store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		var err error
		d.ReplayMatched, err = c.observeReplay(ctx, q)
		return err
	})
	if e != nil {
		phaseError("replay", e)
		return d
	}
	d.ReplayValid = true
	if d.ReplayMatched {
		d.Stage = "matched_replay"
		return d
	}
	p.parent.source.state.mu.Lock()
	e = store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error { _, err := c.rejoinFirst(ctx, q); return err })
	p.parent.source.state.mu.Unlock()
	if e != nil {
		phaseError("rejoin", e)
		return d
	}
	d.RejoinValid = true
	d.Stage = "all_read_phases_valid"
	return d
}

func (f *LocalTerminalCompletionSQLFixture) Snapshot(t *testing.T) LocalTerminalCompletionSnapshot {
	t.Helper()
	v := f.state
	v.LostACKs = f.lostACKs
	decode := func(table string, target any) {
		row := f.cleanup.seed.parent.graph.rows[table]
		if len(row) != 1 || json.Unmarshal([]byte(row[0].(string)), target) != nil {
			t.Fatalf("completion missing %s row", table)
		}
	}
	decode("runs", &v.Run)
	decode("attempts", &v.Attempt)
	decode("quota_reservations", &v.Reservation)
	v.Run = v.Run.Clone()
	v.Events = append([]domain.SessionEvent(nil), v.Events...)
	body, err := json.Marshal(struct {
		Indexes                                  map[string][]driver.Value
		Records                                  map[string]string
		Status, Digest, EvidenceDigest, Evidence string
		Manifest                                 domain.ArtifactManifest
	}{f.state.eventIndex, f.state.eventRecords, f.state.FinalStatus, f.state.FinalDigest, f.state.EvidenceDigest, f.state.EvidenceRecord, f.state.Manifest})
	if err != nil {
		t.Fatal(err)
	}
	v.ReplayRowsDigest = fmt.Sprintf("%x", sha256.Sum256(body))
	v.material = nil
	v.eventIndex = nil
	v.eventRecords = nil
	return v
}

func (f *LocalTerminalCompletionSQLFixture) RequestCancellation(t *testing.T) {
	t.Helper()
	var run domain.Run
	if json.Unmarshal([]byte(f.cleanup.seed.parent.graph.rows["runs"][0].(string)), &run) != nil {
		t.Fatal("completion original Run")
	}
	at := f.cleanup.seed.parent.graph.at
	if err := run.RequestCancellation(at); err != nil {
		t.Fatal(err)
	}
	if run.Validate() != nil {
		t.Fatal("valid cancellation row required")
	}
	b, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	f.cleanup.seed.parent.graph.rows["runs"] = []driver.Value{string(b)}
}

// Switch only the scripted SQL model's fail-closed mutation inventory. This
// confers no production permission and changes no durable authority row.
func (f *LocalTerminalCompletionSQLFixture) ObserveCompletion() { f.active = true }

// Change only actual scripted durable DATA after real cleanup, before the
// first terminal transaction. All non-deletion rows remain structurally valid;
// no original producer, source bytes, receipt or wall-clock field is changed.
func (f *LocalTerminalCompletionSQLFixture) TamperBeforeFirstCommit(t *testing.T, mode string) {
	t.Helper()
	if f.state.Finalizations != 0 || f.state.Mutations != 0 {
		t.Fatal("precommit fault requires no canonical mutation")
	}
	graph := f.cleanup.seed.parent.graph
	var run domain.Run
	if json.Unmarshal([]byte(graph.rows["runs"][0].(string)), &run) != nil {
		t.Fatal("original precommit Run")
	}
	store := func(table string, v any) {
		body, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		graph.rows[table] = []driver.Value{string(body)}
	}
	switch mode {
	case "lease_fence":
		row := append([]driver.Value(nil), f.cleanup.seed.parent.sql.leaseRow...)
		if len(row) != 17 {
			t.Fatal("original exact lease projection")
		}
		var lease domain.Lease
		if json.Unmarshal([]byte(row[16].(string)), &lease) != nil {
			t.Fatal("original lease record")
		}
		lease.FenceToken++
		var attempt domain.Attempt
		if json.Unmarshal([]byte(graph.rows["attempts"][0].(string)), &attempt) != nil || lease.ValidateForAttempt(run, attempt) != nil {
			t.Fatal("valid replacement fence fixture required")
		}
		body, err := json.Marshal(lease)
		if err != nil {
			t.Fatal(err)
		}
		row[5], row[13], row[16] = lease.FenceToken, lease.FenceToken, string(body)
		f.cleanup.seed.parent.sql.leaseRow = row
	case "missing_lease":
		f.cleanup.seed.parent.sql.leaseRow = nil
		f.state.LeasePresent = false
	case "session_deletion":
		f.cleanup.SetFault("deletion_registered")
	case "changed_plan":
		if run.TerminalCustody == nil {
			t.Fatal("genuine registered plan required")
		}
		run.TerminalCustody.Plan.FrontierDigest = strings.Repeat("0", 64)
		if run.Validate() != nil {
			t.Fatal("structurally valid changed plan required")
		}
		store("runs", run)
	case "changed_derived":
		if run.DerivedContext == nil || len(run.DerivedContext.Inventory) != 2 {
			t.Fatal("genuine two-compaction metadata required")
		}
		run.DerivedContext = nil
		if run.Validate() != nil {
			t.Fatal("structurally valid stripped derived metadata required")
		}
		store("runs", run)
	case "changed_attempt":
		var a domain.Attempt
		if json.Unmarshal([]byte(graph.rows["attempts"][0].(string)), &a) != nil {
			t.Fatal("original Attempt")
		}
		a.WorkerID = "foreign-completion-worker"
		if a.ValidateForRun(run) != nil {
			t.Fatal("structurally valid changed Attempt required")
		}
		store("attempts", a)
	case "changed_quota":
		var r domain.QuotaReservation
		if json.Unmarshal([]byte(graph.rows["quota_reservations"][0].(string)), &r) != nil {
			t.Fatal("original quota")
		}
		r.CapacityUnits++
		if r.ValidateForRun(run) != nil {
			t.Fatal("structurally valid changed quota required")
		}
		store("quota_reservations", r)
	default:
		t.Fatal("closed precommit DATA fault")
	}
}

// Genuine concurrent calls on the original source and cleanup owner. The only
// gate is that source's real mutex; neither scheduler order nor private state
// fields are manufactured. Cleanup is registered before either goroutine.
func (f *LocalTerminalCompletionSQLFixture) ClearSourceOverlappingCompletion(t *testing.T, r *LocalTerminalCleanupV1) error {
	t.Helper()
	if r == nil || r.self != r || !r.owner.original() || r.owner.parent.source == nil || r.owner.parent.source.state == nil {
		t.Fatal("race requires original captured source/cleanup")
	}
	s := r.owner.parent.source
	wait, cancelWait := context.WithTimeout(t.Context(), 15*time.Second)
	call, cancelCall := context.WithCancel(wait)
	t.Cleanup(cancelWait)
	type outcome struct {
		clear bool
		err   error
	}
	started := make(chan struct{}, 2)
	done := make(chan outcome, 2)
	s.state.mu.Lock()
	var unlock sync.Once
	release := func() { unlock.Do(s.state.mu.Unlock) }
	remaining := 2
	t.Cleanup(func() {
		cancelCall()
		release()
		for remaining > 0 {
			select {
			case <-done:
				remaining--
			case <-wait.Done():
				select {
				case <-done:
					remaining--
				default:
					t.Error("original clear/completion did not join before retained test bound")
					return
				}
			}
		}
	})
	go func() { started <- struct{}{}; s.Clear(); done <- outcome{clear: true} }()
	go func() { started <- struct{}{}; done <- outcome{err: r.Complete(call)} }()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-wait.Done():
			select {
			case <-started:
			default:
				t.Fatal("original clear/completion did not announce start")
			}
		}
	}
	release()
	var result error
	for remaining > 0 {
		var got outcome
		select {
		case got = <-done:
		case <-wait.Done():
			select {
			case got = <-done:
			default:
				t.Fatal("original clear/completion remained active")
			}
		}
		remaining--
		if !got.clear {
			result = got.err
		}
	}
	cancelCall()
	return result
}

// One fault on the ACTUAL next mutating terminal commit, not its earlier
// read-only observation transaction. The original graphTx owns persistence or
// rollback; no synthetic finalization marker is inserted by this injection.
func (f *LocalTerminalCompletionSQLFixture) LoseNextCompletionACK(t *testing.T, mode string) {
	t.Helper()
	if mode != "ambiguous" && mode != "rollback" {
		t.Fatal("closed completion ACK fault")
	}
	f.completionCommitFault = mode
}

// Corruption of test-owned durable rows AFTER an actual original canonical
// commit. It cannot supply a production owner, approval or replacement receipt.
func (f *LocalTerminalCompletionSQLFixture) TamperReplay(t *testing.T, mode string) {
	t.Helper()
	if f.state.Finalizations != 1 || len(f.state.Events) != 1 {
		t.Fatal("tamper requires actual complete canonical commit")
	}
	event := f.state.Events[0]
	switch mode {
	case "missing_marker":
		f.state.Finalizations = 0
	case "corrupt_marker":
		f.state.FinalDigest = strings.Repeat("0", 64)
	case "missing_evidence":
		f.state.EvidenceRecord = ""
		f.state.EvidenceDigest = ""
	case "corrupt_evidence":
		f.state.EvidenceRecord = `{}`
	case "missing_event":
		delete(f.state.eventRecords, string(event.ID))
	case "missing_index":
		delete(f.state.eventIndex, string(event.IdempotencyKey))
	case "corrupt_event":
		event.Payload.SHA256 = strings.Repeat("0", 64)
		body, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		f.state.eventRecords[string(event.ID)] = string(body)
	case "missing_manifest":
		f.state.Manifest = domain.ArtifactManifest{}
	case "changed_custody":
		var run domain.Run
		graph := f.cleanup.seed.parent.graph
		if json.Unmarshal([]byte(graph.rows["runs"][0].(string)), &run) != nil || run.TerminalCustody == nil || run.TerminalCustody.LocalEvidence == nil {
			t.Fatal("actual completed local custody required")
		}
		run.TerminalCustody.LocalEvidence.ResidualReceiptDigest = strings.Repeat("0", 64)
		var err error
		run.TerminalCustody.LocalEvidence.EvidenceDigest, err = run.TerminalCustody.LocalEvidence.Digest()
		if err != nil || run.Validate() != nil {
			t.Fatal("changed custody must be valid DATA, not original authority")
		}
		body, err := json.Marshal(run)
		if err != nil {
			t.Fatal(err)
		}
		graph.rows["runs"] = []driver.Value{string(body)}
	default:
		t.Fatal("closed durable replay tamper")
	}
}

type terminalCompletionConnector struct {
	f *LocalTerminalCompletionSQLFixture
}
type terminalCompletionConn struct {
	*terminalCleanupConn
	f *LocalTerminalCompletionSQLFixture
}
type terminalCompletionTx struct {
	driver.Tx
	f      *LocalTerminalCompletionSQLFixture
	before LocalTerminalCompletionSnapshot
	done   bool
	writes int
}

func completionSQL(s string) string { return strings.Join(strings.Fields(s), " ") }
func (c terminalCompletionConnector) Driver() driver.Driver {
	return (terminalCleanupConnector{fixture: c.f.cleanup}).Driver()
}
func (c terminalCompletionConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := (terminalCleanupConnector{fixture: c.f.cleanup}).Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &terminalCompletionConn{terminalCleanupConn: conn.(*terminalCleanupConn), f: c.f}, nil
}
func (c *terminalCompletionConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	tx, err := c.terminalCleanupConn.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	if len(c.f.transactionCounts) == 64 {
		copy(c.f.transactionCounts, c.f.transactionCounts[1:])
		c.f.transactionCounts = c.f.transactionCounts[:63]
	}
	c.f.transactionCounts = append(c.f.transactionCounts, 0)
	c.f.currentTransaction = len(c.f.transactionCounts) - 1
	before := c.f.state
	before.Events = append([]domain.SessionEvent(nil), before.Events...)
	before.material = make(map[string][]driver.Value, len(c.f.state.material))
	for k, v := range c.f.state.material {
		before.material[k] = append([]driver.Value(nil), v...)
	}
	before.eventIndex = make(map[string][]driver.Value, len(c.f.state.eventIndex))
	for k, v := range c.f.state.eventIndex {
		before.eventIndex[k] = append([]driver.Value(nil), v...)
	}
	before.eventRecords = make(map[string]string, len(c.f.state.eventRecords))
	for k, v := range c.f.state.eventRecords {
		before.eventRecords[k] = v
	}
	return &terminalCompletionTx{Tx: tx, f: c.f, before: before, writes: c.f.state.Mutations}, nil
}
func (tx *terminalCompletionTx) Commit() error {
	mutated := tx.f.state.Mutations > tx.writes
	mode := ""
	if mutated && tx.f.completionCommitFault != "" {
		mode = tx.f.completionCommitFault
		tx.f.completionCommitFault = ""
		tx.f.lostACKs++
		tx.f.cleanup.seed.parent.graph.commitModes = []string{mode}
	}
	err := tx.Tx.Commit()
	tx.done = true
	if mode == "rollback" {
		tx.f.state = tx.before
	} else if mutated && (err == nil || mode == "ambiguous") {
		tx.f.state.Commits++
	}
	return err
}
func (tx *terminalCompletionTx) Rollback() error {
	if !tx.done {
		tx.f.state = tx.before
		tx.done = true
	}
	return tx.Tx.Rollback()
}

func (s *LocalTerminalPublicationSessionConnectedSeed) InstallTerminalCompletionSQL(t *testing.T) *LocalTerminalCompletionSQLFixture {
	t.Helper()
	cleanup := s.InstallTerminalCleanupSQL(t)
	f := &LocalTerminalCompletionSQLFixture{cleanup: cleanup, currentTransaction: -1, state: LocalTerminalCompletionSnapshot{LeasePresent: true, material: make(map[string][]driver.Value), eventIndex: make(map[string][]driver.Value), eventRecords: make(map[string]string)}}
	if counters := s.parent.graph.counters; len(counters) != 0 {
		if len(counters) != 2 {
			t.Fatal("original scheduler counter projection")
		}
		var active uint64
		switch n := counters[1].(type) {
		case int64:
			if n < 0 {
				t.Fatal("negative original active counter")
			}
			active = uint64(n)
		case uint32:
			active = uint64(n)
		case uint64:
			active = n
		default:
			t.Fatal("invalid original active counter type")
		}
		if active > 1<<32-1 {
			t.Fatal("oversize original active counter")
		}
		f.state.CountersPresent, f.state.ActiveRuns = true, uint32(active)
	}
	if err := json.Unmarshal([]byte(s.parent.base.session[0].(string)), &f.state.Session); err != nil {
		t.Fatal(err)
	}
	// The original scripted canonical source already contains these events.
	// Align its Session frontier before ANY real factory/Source consumption.
	for _, row := range s.compaction.compactionSourceSQL.events {
		var e domain.SessionEvent
		if err := json.Unmarshal([]byte(row[0].(string)), &e); err != nil {
			t.Fatal(err)
		}
		if e.Sequence > f.state.Session.LastEventSequence {
			f.state.Session.LastEventSequence = e.Sequence
		}
	}
	raw, err := json.Marshal(f.state.Session)
	if err != nil {
		t.Fatal(err)
	}
	s.parent.base.session[0] = string(raw)
	s.parent.base.session[3] = int64(f.state.Session.LastEventSequence)
	db := sql.OpenDB(terminalCompletionConnector{f: f})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	s.DB = db
	return f
}

func (c *terminalCompletionConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (_ driver.Rows, err error) {
	finish := c.f.traceSQL(statement)
	defer func() { finish(err) }()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	flat := completionSQL(statement)
	row := func(v ...driver.Value) (driver.Rows, error) { return &explanationFixtureRows{values: v}, nil }
	empty := func() (driver.Rows, error) { return &compactionSourceRows{}, nil }
	f := c.f
	if len(args) >= 3 && len(args) <= 27 && statement == localTerminalReplayEventsSQL(len(args)-2) {
		if args[0].Value != string(f.state.Session.TenantID) || args[1].Value != string(f.state.Session.ID) {
			return nil, errors.New("replay JOIN foreign tenant/Session")
		}
		seen := make(map[string]bool, len(args)-2)
		var rows [][]driver.Value
		for _, arg := range args[2:] {
			key, ok := arg.Value.(string)
			if !ok || seen[key] {
				return nil, errors.New("replay JOIN malformed/duplicate original key")
			}
			seen[key] = true
			index, exists := f.state.eventIndex[key]
			if !exists {
				continue // Actual SQL has no matching primary index row.
			}
			record, exists := f.state.eventRecords[index[1].(string)]
			if !exists {
				continue
			} // Actual INNER JOIN omits a missing event body.
			rows = append(rows, []driver.Value{index[0], index[1], record})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i][0].(int64) < rows[j][0].(int64) })
		return &compactionSourceRows{rows: rows}, nil
	}
	manifestQuery, _, _ := ownedCanonicalJSONSelect("SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2")
	if statement == manifestQuery {
		if len(args) != 2 || args[0].Value != string(f.state.Session.TenantID) {
			return nil, errors.New("replay manifest foreign key")
		}
		var input domain.ArtifactManifest
		if json.Unmarshal([]byte(f.cleanup.seed.parent.graph.rows["artifact_manifests"][0].(string)), &input) != nil {
			return nil, errors.New("invalid original input manifest fixture")
		}
		if args[1].Value == string(input.ID) {
			return c.terminalCleanupConn.QueryContext(ctx, statement, args)
		}
		if f.state.Manifest.ID == "" || args[1].Value != string(f.state.Manifest.ID) {
			return empty()
		}
		body, err := json.Marshal(f.state.Manifest)
		if err != nil {
			return nil, err
		}
		return row(string(body))
	}
	switch flat {
	case completionSQL(localTerminalMarkerSQL):
		var run domain.Run
		if len(args) != 2 || json.Unmarshal([]byte(f.cleanup.seed.parent.graph.rows["runs"][0].(string)), &run) != nil || args[0].Value != string(run.TenantID) || args[1].Value != string(run.ID) {
			return nil, errors.New("bounded marker requires original Run key")
		}
		f.active = true
		if f.state.Finalizations == 0 {
			return empty()
		}
		return row(f.state.FinalStatus, f.state.FinalDigest)
	case "SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2":
		b, err := json.Marshal(f.state.Session)
		if err != nil {
			return nil, err
		}
		return row(string(b))
	case "SELECT entitlement_state FROM subscription_connections WHERE tenant_id = $1 AND subscription_connection_id = $2":
		return row(f.cleanup.seed.parent.graph.entitlement)
	case "SELECT queue_depth, active_runs FROM tenant_scheduler_counters WHERE tenant_id = $1":
		if len(args) != 1 || args[0].Value != string(f.state.Session.TenantID) {
			return nil, errors.New("counter query requires original tenant")
		}
		if !f.state.CountersPresent {
			return empty()
		}
		return row(f.cleanup.seed.parent.graph.counters...)
	case "SELECT lease_id, fence_token, expires_at FROM lease_heads WHERE tenant_id = $1 AND run_id = $2":
		if !f.state.LeasePresent {
			return empty()
		}
		r := f.cleanup.seed.parent.sql.leaseRow
		// The current original lease query has its own bounded projection.
		if len(r) < 7 {
			if len(r) == 0 {
				return empty()
			}
			return nil, errors.New("completion missing original lease")
		}
		return row(r[2], r[5], r[6])
	case "SELECT binding_id FROM frontend_bindings_by_session WHERE tenant_id = $1 AND session_id = $2 LIMIT $3",
		"SELECT record FROM session_displays WHERE tenant_id = $1 AND session_id = $2":
		return empty()
	case "SELECT completed_at FROM session_lifecycle_backfill_state WHERE backfill_id = $1":
		return row(f.cleanup.seed.parent.graph.at)
	case "SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND status = $3 LIMIT $4":
		return row(f.cleanup.seed.parent.base.participant[0])
	case completionSQL(sessionMaterialCurrentSQL):
		return row(f.state.material["current"]...)
	case completionSQL(sessionMaterialBoundarySQL):
		return row(f.state.material[fmt.Sprint(args[2].Value)]...)
	case completionSQL(localTerminalEvidenceSQL):
		if f.state.EvidenceRecord == "" {
			return empty()
		}
		return row(f.state.EvidenceDigest, f.state.EvidenceRecord)
	}
	return c.terminalCleanupConn.QueryContext(ctx, statement, args)
}

// Exact mutation statements accepted by this model. An unrecognized statement
// never succeeds merely because a table name or verb happens to match.
var terminalCompletionWrites = map[string]int{
	completionSQL(`INSERT INTO substrate_execution_evidence (tenant_id,attempt_id,run_id,physical_invocation_claim_id,evidence_digest,recorded_at,record) VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS JsonDocument))`):                                                             7,
	completionSQL(`UPSERT INTO runs (tenant_id, run_id, session_id, trigger_event_id, subscription_connection_id, status, created_at, updated_at, payload) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))`):                                              9,
	completionSQL(`UPSERT INTO runs_by_session (tenant_id, session_id, created_at, run_id, trigger_event_id, status) VALUES ($1, $2, $3, $4, $5, $6)`):                                                                                                                      6,
	completionSQL(`UPSERT INTO run_idempotency (tenant_id, idempotency_key, run_id, created_at, expire_at) VALUES ($1, $2, $3, $4, $5)`):                                                                                                                                    5,
	completionSQL(`UPSERT INTO attempts (tenant_id, attempt_id, run_id, attempt_number, status, worker_id, created_at, updated_at, retention_expire_at, payload) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10 AS JsonDocument))`):                                   10,
	completionSQL(`DELETE FROM quota_expiry_v2 WHERE shard_bucket = $1 AND expires_at = $2 AND tenant_id = $3 AND quota_reservation_id = $4`):                                                                                                                               4,
	completionSQL(`UPSERT INTO quota_reservations (tenant_id, quota_reservation_id, run_id, subscription_connection_id, status, capacity_units, held_at, expires_at, retention_expire_at, payload) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10 AS JsonDocument))`): 10,
	completionSQL(`UPSERT INTO artifact_manifests (tenant_id, artifact_manifest_id, run_id, created_at, payload) VALUES ($1, $2, $3, $4, CAST($5 AS JsonDocument))`):                                                                                                        5,
	completionSQL(`UPSERT INTO artifact_manifests_by_run (tenant_id, run_id, artifact_manifest_id) VALUES ($1, $2, $3)`):                                                                                                                                                    3,
	completionSQL(`INSERT INTO session_events (tenant_id, session_id, sequence, event_id, kind, author_user_id, run_id, idempotency_key, blob_key, created_at, record) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CAST($11 AS JsonDocument))`):                        11,
	completionSQL(`INSERT INTO session_event_idempotency (tenant_id, session_id, idempotency_key, sequence, event_id, created_at) VALUES ($1, $2, $3, $4, $5, $6)`):                                                                                                         6,
	completionSQL(`INSERT INTO session_material_boundaries_v1 (tenant_id,session_id,sequence,version,through_sequence,last_event_id,prefix_digest,record) VALUES ($1,$2,$3,$4,$5,$6,$7,CAST($8 AS String))`):                                                                8,
	completionSQL(`UPSERT INTO session_material_current_v1 (tenant_id,session_id,version,through_sequence,last_event_id,prefix_digest,record) VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS String))`):                                                                               7,
	completionSQL(`UPDATE sessions SET status = $1, last_event_sequence = $2, updated_at = $3, archived_at = $4, record = CAST($5 AS JsonDocument) WHERE tenant_id = $6 AND session_id = $7`):                                                                               7,
	completionSQL(`DELETE FROM session_activity WHERE tenant_id = $1 AND user_id = $2 AND status = $3 AND activity_bucket = $4 AND updated_at = $5 AND session_id = $6`):                                                                                                    6,
	completionSQL(`UPSERT INTO session_activity (tenant_id, user_id, status, activity_bucket, updated_at, session_id) VALUES ($1, $2, $3, $4, $5, $6)`):                                                                                                                     6,
	completionSQL(`UPSERT INTO session_displays (tenant_id, session_id, updated_at, record) VALUES ($1, $2, $3, CAST($4 AS JsonDocument))`):                                                                                                                                 4,
	completionSQL(`INSERT INTO run_finalizations (tenant_id, run_id, terminal_status, content_digest, created_at) VALUES ($1, $2, $3, $4, $5)`):                                                                                                                             5,
	completionSQL(`UPSERT INTO subscription_scheduler_slots (tenant_id, subscription_connection_id, state, active_run_id, active_reservation_id, blocked_until, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`):                                                           7,
	completionSQL(`UPDATE tenant_scheduler_counters SET queue_depth = $1, active_runs = $2, updated_at = $3 WHERE tenant_id = $4`):                                                                                                                                          4,
	completionSQL(`INSERT INTO tenant_scheduler_counters (tenant_id, queue_depth, active_runs, updated_at) VALUES ($1, $2, $3, $4)`):                                                                                                                                        4,
	completionSQL(`DELETE FROM lease_expiry_v2 WHERE shard_bucket = $1 AND expires_at = $2 AND tenant_id = $3 AND run_id = $4`):                                                                                                                                             4,
	completionSQL(`DELETE FROM lease_heads WHERE tenant_id = $1 AND run_id = $2`):                                                                                                                                                                                           2,
	completionSQL(`UPSERT INTO audit_events (tenant_id, occurred_at, audit_event_id, actor_id, action, subject_kind, subject_id, outcome, metadata, expire_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument), $10)`):                                     10,
	completionSQL(`UPSERT INTO run_explanation_heads_v1 (tenant_id,run_id,revision,record) VALUES ($1,$2,$3,CAST($4 AS JsonDocument))`):                                                                                                                                     4,
}

func (c *terminalCompletionConn) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (_ driver.Result, err error) {
	finish := c.f.traceSQL(statement)
	defer func() { finish(err) }()
	if !c.f.active {
		return c.terminalCleanupConn.ExecContext(ctx, statement, args)
	}
	c.f.terminalExecAttempts++
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err := applyTerminalCompletionWrite(&c.f.state, c.f.cleanup.seed.parent.graph.rows, &c.f.cleanup.seed.parent.graph.counters, terminalCompletionLocalV2, statement, args); err != nil {
		return nil, err
	}
	return c.terminalCleanupConn.ExecContext(ctx, statement, args)
}

// One strict storage model for the existing canonical writer, not another
// execution/finalization authority. The producing test factory chooses the
// on-disk format; no callback or caller approval enters production.
type terminalCompletionFormat uint8

const (
	terminalCompletionLocalV2 terminalCompletionFormat = iota + 1
	terminalCompletionManagedV1
)

func applyTerminalCompletionWrite(state *LocalTerminalCompletionSnapshot, rows map[string][]driver.Value, counters *[]driver.Value, format terminalCompletionFormat, statement string, args []driver.NamedValue) error {
	if state == nil || rows == nil || counters == nil || (format != terminalCompletionLocalV2 && format != terminalCompletionManagedV1) {
		return errors.New("invalid canonical fixture storage host")
	}
	flat := completionSQL(statement)
	n, ok := terminalCompletionWrites[flat]
	if !ok || len(args) != n {
		return fmt.Errorf("unmodeled exact canonical SQL: %s (args=%d)", flat, len(args))
	}
	val := func(i int) driver.Value { return args[i].Value }
	decode := func(i int, target any) error {
		s, ok := val(i).(string)
		if !ok {
			return errors.New("canonical payload not String")
		}
		return json.Unmarshal([]byte(s), target)
	}
	f := struct {
		state *LocalTerminalCompletionSnapshot
	}{state}
	switch {
	case strings.HasPrefix(flat, "INSERT INTO substrate_execution_evidence "):
		if f.state.EvidenceRecord != "" {
			return errors.New("duplicate terminal evidence")
		}
		if format == terminalCompletionLocalV2 {
			var v domain.ExecutionEvidenceV2
			if decode(6, &v) != nil || v.LocalV1 == nil || v.LocalV1.Validate() != nil || v.LocalV1.EvidenceDigest != val(4) {
				return errors.New("invalid local V2 evidence")
			}
		} else {
			var v domain.SubstrateExecutionEvidenceV1
			if decode(6, &v) != nil || v.Version != 1 || string(v.EvidenceDigest) != val(4) {
				return errors.New("invalid managed V1 evidence storage")
			}
		}
		f.state.EvidenceRecord = val(6).(string)
		f.state.EvidenceDigest = val(4).(string)
	case strings.HasPrefix(flat, "UPSERT INTO runs "):
		var v domain.Run
		if decode(8, &v) != nil || v.Validate() != nil {
			return errors.New("invalid terminal Run")
		}
		rows["runs"] = []driver.Value{val(8)}
	case strings.HasPrefix(flat, "UPSERT INTO attempts "):
		var v domain.Attempt
		if decode(9, &v) != nil {
			return errors.New("invalid terminal Attempt")
		}
		rows["attempts"] = []driver.Value{val(9)}
	case strings.HasPrefix(flat, "UPSERT INTO quota_reservations "):
		rows["quota_reservations"] = []driver.Value{val(9)}
	case strings.HasPrefix(flat, "UPSERT INTO run_explanation_heads_v1 "):
		rows["run_explanation_heads_v1"] = []driver.Value{val(3)}
	case strings.HasPrefix(flat, "UPSERT INTO artifact_manifests "):
		if decode(4, &f.state.Manifest) != nil {
			return errors.New("invalid terminal Manifest")
		}
	case strings.HasPrefix(flat, "INSERT INTO session_events "):
		var v domain.SessionEvent
		if decode(10, &v) != nil || v.Validate() != nil || v.Sequence != f.state.Session.LastEventSequence+uint64(len(f.state.Events))+1 {
			return errors.New("invalid/duplicate canonical event")
		}
		f.state.Events = append(f.state.Events, v)
		if _, exists := f.state.eventRecords[string(v.ID)]; exists {
			return errors.New("duplicate canonical event key")
		}
		f.state.eventRecords[string(v.ID)] = val(10).(string)
	case strings.HasPrefix(flat, "INSERT INTO session_event_idempotency "):
		key := val(2).(string)
		if _, exists := f.state.eventIndex[key]; exists {
			return errors.New("duplicate canonical idempotency key")
		}
		f.state.eventIndex[key] = []driver.Value{val(3), val(4)}
	case strings.HasPrefix(flat, "INSERT INTO session_material_boundaries_v1 "):
		key := fmt.Sprint(val(2))
		if _, exists := f.state.material[key]; exists {
			return errors.New("duplicate immutable material boundary")
		}
		f.state.material[key] = []driver.Value{val(7), val(3), val(4), val(0), val(1), val(5), val(6)}
	case strings.HasPrefix(flat, "UPSERT INTO session_material_current_v1 "):
		f.state.material["current"] = []driver.Value{val(6), val(2), val(3), val(0), val(1), val(4), val(5)}
	case strings.HasPrefix(flat, "UPDATE sessions "):
		if decode(4, &f.state.Session) != nil || f.state.Session.Validate() != nil {
			return errors.New("invalid terminal Session")
		}
	case strings.HasPrefix(flat, "INSERT INTO run_finalizations "):
		if f.state.Finalizations != 0 {
			return errors.New("duplicate finalization")
		}
		if format == terminalCompletionLocalV2 {
			if _, err := decodeLocalFinalizationMarkerV2(val(3).(string)); err != nil || (val(2) != string(domain.RunSucceeded) && val(2) != string(domain.RunCancelled)) {
				return errors.New("local completion requires exact tagged success/cancellation marker")
			}
		} else if domain.ValidateSHA256Digest("fixture.v1.marker", val(3).(string)) != nil {
			return errors.New("managed V1 marker must retain the original raw encoding")
		}
		f.state.Finalizations++
		f.state.FinalStatus = val(2).(string)
		f.state.FinalDigest = val(3).(string)
	case strings.HasPrefix(flat, "DELETE FROM lease_heads "):
		f.state.LeasePresent = false
	case strings.HasPrefix(flat, "UPDATE tenant_scheduler_counters "):
		f.state.ActiveRuns = uint32(val(1).(int64))
		f.state.CountersPresent = true
		*counters = []driver.Value{val(0), val(1)}
	case strings.HasPrefix(flat, "INSERT INTO tenant_scheduler_counters "):
		if f.state.CountersPresent {
			return errors.New("duplicate canonical scheduler counters")
		}
		f.state.CountersPresent, f.state.ActiveRuns = true, uint32(val(2).(int64))
		*counters = []driver.Value{val(1), val(2)}
	case strings.HasPrefix(flat, "UPSERT INTO subscription_scheduler_slots "):
		f.state.ActiveRun = val(3).(string)
		f.state.ActiveReservation = val(4).(string)
	}
	f.state.Mutations++
	return nil
}
