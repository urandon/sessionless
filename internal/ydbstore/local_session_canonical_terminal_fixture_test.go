//go:build sessionless_mcp_offline

package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// The original canonical/tool peer remains the authority DATA source. Only
// terminal storage statements are added; no execution/cleanup owner is supplied.
type canonicalTerminalSQL struct {
	seed                            *LocalCanonicalToolSeed
	publication                     *terminalPublicationSQL
	publicationConn                 *terminalPublicationConn
	state                           LocalTerminalCompletionSnapshot
	markerSeen, rawCanonical        bool
	rawStatements, maxRawStatements int
	projection                      map[string]domain.FrontendProjection
	physical                        map[string][][]driver.Value
	lastSQL, lastError              string
	completionFault                 string
	lostACKs                        int
}

func (f *canonicalTerminalSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *canonicalTerminalSQL) Driver() driver.Driver                        { return f.seed.peer.Driver() }
func (f *canonicalTerminalSQL) Prepare(s string) (driver.Stmt, error)        { return f.seed.peer.Prepare(s) }
func (f *canonicalTerminalSQL) Close() error                                 { return f.seed.peer.Close() }
func (f *canonicalTerminalSQL) Begin() (driver.Tx, error) {
	return nil, errors.New("terminal requires explicit bounded transaction")
}

type canonicalTerminalTx struct {
	driver.Tx
	f          *canonicalTerminalSQL
	state      LocalTerminalCompletionSnapshot
	physical   map[string][][]driver.Value
	projection map[string]domain.FrontendProjection
	writes     int
	expiry     []driver.Value
	done       bool
}

func cloneCanonicalTerminalState(v LocalTerminalCompletionSnapshot) LocalTerminalCompletionSnapshot {
	v.Events = append([]domain.SessionEvent(nil), v.Events...)
	m := make(map[string][]driver.Value, len(v.material))
	for k, r := range v.material {
		m[k] = append([]driver.Value(nil), r...)
	}
	v.material = m
	i := make(map[string][]driver.Value, len(v.eventIndex))
	for k, r := range v.eventIndex {
		i[k] = append([]driver.Value(nil), r...)
	}
	v.eventIndex = i
	e := make(map[string]string, len(v.eventRecords))
	for k, r := range v.eventRecords {
		e[k] = r
	}
	v.eventRecords = e
	return v
}
func (f *canonicalTerminalSQL) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	f.seed.LocalCanonicalLoopSeed.loop.callbackSQL = 0
	f.seed.LocalCanonicalLoopSeed.peer.localWorkerLifecycleSQL.beginPhase("canonical terminal callback")
	f.markerSeen, f.rawCanonical = false, false
	f.rawStatements = 0
	tx, err := f.publicationConn.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	p := make(map[string][][]driver.Value, len(f.physical))
	for k, r := range f.physical {
		p[k] = cloneGenerationRows(r)
	}
	projections := make(map[string]domain.FrontendProjection, len(f.projection))
	for k, r := range f.projection {
		projections[k] = r
	}
	return &canonicalTerminalTx{Tx: tx, f: f, state: cloneCanonicalTerminalState(f.state), physical: p, projection: projections, writes: f.state.Mutations, expiry: append([]driver.Value(nil), f.seed.LocalCanonicalLoopSeed.peer.base.expiry...)}, nil
}
func (tx *canonicalTerminalTx) Commit() error {
	mutated := tx.f.state.Mutations > tx.writes
	mode := ""
	if mutated && tx.f.completionFault != "" {
		mode = tx.f.completionFault
		tx.f.completionFault = ""
		tx.f.lostACKs++
		tx.f.seed.LocalCanonicalLoopSeed.peer.base.commitModes = []string{mode}
	}
	err := tx.Tx.Commit()
	tx.done = true
	if mode == "rollback" {
		tx.restore()
	} else if err == nil || mode == "ambiguous" {
		if mutated {
			tx.f.state.Commits++
		}
	}
	return err
}
func (tx *canonicalTerminalTx) Rollback() error {
	if !tx.done {
		tx.restore()
		tx.done = true
	}
	return tx.Tx.Rollback()
}
func (tx *canonicalTerminalTx) restore() {
	tx.f.state = tx.state
	tx.f.physical = tx.physical
	tx.f.projection = tx.projection
	tx.f.seed.LocalCanonicalLoopSeed.peer.base.expiry = append([]driver.Value(nil), tx.expiry...)
}
func (f *canonicalTerminalSQL) trace(s string, err error) {
	f.lastSQL = completionSQL(s)
	if len(f.lastSQL) > 180 {
		f.lastSQL = f.lastSQL[:180]
	}
	f.lastError = ""
	if err != nil {
		f.lastError = err.Error()
		if len(f.lastError) > 160 {
			f.lastError = f.lastError[:160]
		}
	}
}
func (f *canonicalTerminalSQL) exact(args []driver.NamedValue, want ...driver.Value) bool {
	return f.seed.LocalCanonicalLoopSeed.peer.exactArgs(args, want...)
}
func (f *canonicalTerminalSQL) rawCount(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("unbounded canonical helper")
	}
	f.rawStatements++
	f.maxRawStatements = max(f.maxRawStatements, f.rawStatements)
	return nil
}
func (f *canonicalTerminalSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (_ driver.Rows, err error) {
	defer func() { f.trace(s, err) }()
	seed := f.seed.LocalCanonicalLoopSeed
	r := seed.peer.base.request
	row := func(v []driver.Value) (driver.Rows, error) {
		return &explanationFixtureRows{values: append([]driver.Value(nil), v...)}, nil
	}
	empty := func() (driver.Rows, error) { return &compactionSourceRows{}, nil }
	flat := completionSQL(s)
	// The first raw helper read marks the production's documented SAME-sql.Tx
	// canonical handoff, after its owned-q rejoin. No q reads follow its writes.
	if f.markerSeen && flat == "SELECT lease_id, fence_token, expires_at FROM lease_heads WHERE tenant_id = $1 AND run_id = $2" {
		f.rawCanonical = true
	}
	if f.rawCanonical {
		if e := f.rawCount(ctx); e != nil {
			return nil, e
		}
		return f.rawQuery(ctx, s, args)
	}
	if s == localTerminalMarkerSQL {
		if !f.exact(args, string(r.TenantID), string(r.RunID)) {
			return nil, errors.New("terminal marker exact Run")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		f.markerSeen = true
		if f.state.Finalizations == 0 {
			return empty()
		}
		return row([]driver.Value{f.state.FinalStatus, f.state.FinalDigest})
	}
	if s == localTerminalEvidenceSQL {
		if !f.exact(args, string(r.TenantID), string(r.AttemptID)) {
			return nil, errors.New("terminal evidence exact Attempt")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		if f.state.EvidenceRecord == "" {
			return empty()
		}
		return row([]driver.Value{f.state.EvidenceDigest, f.state.EvidenceRecord})
	}
	if len(args) >= 3 && len(args) <= 27 && s == localTerminalReplayEventsSQL(len(args)-2) {
		if args[0].Value != string(r.TenantID) || args[1].Value != string(r.SessionID) {
			return nil, errors.New("terminal replay exact Session")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		seen := map[string]bool{}
		var rows [][]driver.Value
		for _, a := range args[2:] {
			key, ok := a.Value.(string)
			if !ok || seen[key] {
				return nil, errors.New("terminal replay unique keys")
			}
			seen[key] = true
			if i, ok := f.state.eventIndex[key]; ok {
				if record, ok := f.state.eventRecords[i[1].(string)]; ok {
					rows = append(rows, []driver.Value{i[0], i[1], record})
				}
			}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i][0].(int64) < rows[j][0].(int64) })
		return &compactionSourceRows{rows: rows}, nil
	}
	manifest, _, _ := ownedCanonicalJSONSelect("SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2")
	if s == manifest && len(args) == 2 && f.state.Manifest.ID != "" && args[1].Value == string(f.state.Manifest.ID) {
		if args[0].Value != string(r.TenantID) {
			return nil, errors.New("terminal manifest exact tenant")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		raw, e := json.Marshal(f.state.Manifest)
		if e != nil {
			return nil, e
		}
		return row([]driver.Value{string(raw)})
	}
	if flat == "SELECT state FROM session_deletions WHERE tenant_id = $1 AND session_id = $2" {
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("terminal deletion exact Session")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		return empty()
	}
	if s == boundedRunPayloadSQL && f.publication.commits > 0 {
		f.publication.readbacks++
	}
	return f.seed.peer.QueryContext(ctx, s, args)
}

func (f *canonicalTerminalSQL) rawQuery(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	seed := f.seed.LocalCanonicalLoopSeed
	r := seed.peer.base.request
	flat := completionSQL(s)
	row := func(v []driver.Value) (driver.Rows, error) {
		return &explanationFixtureRows{values: append([]driver.Value(nil), v...)}, nil
	}
	empty := func() (driver.Rows, error) { return &compactionSourceRows{}, nil }
	switch flat {
	case completionSQL(localTerminalEvidenceSQL):
		if !f.exact(args, string(r.TenantID), string(r.AttemptID)) {
			return nil, errors.New("canonical original evidence key")
		}
		if f.state.EvidenceRecord == "" {
			return empty()
		}
		return row([]driver.Value{f.state.EvidenceDigest, f.state.EvidenceRecord})
	case "SELECT lease_id, fence_token, expires_at FROM lease_heads WHERE tenant_id = $1 AND run_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.RunID)) {
			return nil, errors.New("canonical real lease key")
		}
		head := seed.peer.base.rows["lease_heads"]
		if len(head) == 0 {
			return empty()
		}
		if len(head) != 8 {
			return nil, errors.New("canonical lease head shape")
		}
		return row([]driver.Value{head[2], head[5], head[6]})
	case "SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("canonical Session key")
		}
		raw, e := json.Marshal(f.state.Session)
		if e != nil {
			return nil, e
		}
		return row([]driver.Value{string(raw)})
	case "SELECT record FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.BindingID)) {
			return nil, errors.New("canonical original binding key")
		}
		return row(seed.peer.base.rows["frontend_bindings"])
	case "SELECT record FROM session_displays WHERE tenant_id = $1 AND session_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("canonical original display key")
		}
		return row(seed.peer.base.rows["session_displays"])
	case "SELECT state FROM session_deletions WHERE tenant_id = $1 AND session_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("canonical deletion key")
		}
		return empty()
	case "SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("canonical deletion record key")
		}
		return row(seed.peer.base.rows["session_deletions"])
	case "SELECT binding_id FROM frontend_bindings_by_session WHERE tenant_id = $1 AND session_id = $2 LIMIT $3":
		if !f.exact(args, string(r.TenantID), string(r.SessionID), int64(maxSessionDeletionRows+1)) {
			return nil, errors.New("canonical original binding range")
		}
		// Original setup retained the legacy binding row, not this index.
		return empty()
	case "SELECT completed_at FROM session_lifecycle_backfill_state WHERE backfill_id = $1":
		if !f.exact(args, sessionLifecycleBackfillID) {
			return nil, errors.New("canonical exact lifecycle marker")
		}
		return empty()
	case "SELECT binding_id FROM frontend_bindings WHERE tenant_id = $1 AND session_id = $2 LIMIT $3":
		if !f.exact(args, string(r.TenantID), string(r.SessionID), int64(maxSessionDeletionRows+1)) {
			return nil, errors.New("canonical original legacy binding range")
		}
		var b domain.FrontendBinding
		if json.Unmarshal([]byte(seed.peer.base.rows["frontend_bindings"][0].(string)), &b) != nil {
			return nil, errors.New("canonical original binding")
		}
		return row([]driver.Value{string(b.ID)})
	case "SELECT queue_depth, active_runs FROM tenant_scheduler_counters WHERE tenant_id = $1":
		if !f.exact(args, string(r.TenantID)) {
			return nil, errors.New("canonical counter key")
		}
		return row(seed.peer.base.counters)
	case "SELECT state, active_run_id, active_reservation_id, blocked_until, updated_at, SUBSTRING(local_capacity_association,0,32769), SUBSTRING(CAST(local_capacity_digest AS String),0,65) FROM subscription_scheduler_slots WHERE tenant_id = $1 AND subscription_connection_id = $2":
		physical, e := encodeCapacityStorageV1(seed.Loaded.Run.TenantID, "", seed.Loaded.Run.LocalCapacityAssociation)
		if e != nil || !f.exact(args, string(r.TenantID), physical.key) {
			return nil, errors.New("canonical original local slot key")
		}
		return row(seed.peer.base.slot)
	case completionSQL(sessionMaterialCurrentSQL):
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("canonical material current key")
		}
		return row(f.state.material["current"])
	case completionSQL(sessionMaterialBoundarySQL):
		if len(args) != 3 || args[0].Value != string(r.TenantID) || args[1].Value != string(r.SessionID) {
			return nil, errors.New("canonical material boundary key")
		}
		return row(f.state.material[fmt.Sprint(args[2].Value)])
	}
	// Every remaining helper query must already be in the exact original
	// canonical/lifecycle inventory. This is not a table-name SQL fallback.
	if _, ok := seed.peer.base.reads[flat]; ok {
		return seed.peer.base.QueryContext(ctx, s, args)
	}
	if s == quotaReservationStorageSQLV1 || s == quotaExpiryStorageSQLV1 {
		return seed.peer.QueryContext(ctx, s, args)
	}
	for statement := range ownedCanonicalJSONReads {
		bounded, _, _ := ownedCanonicalJSONSelect(statement)
		if s == bounded {
			return seed.peer.QueryContext(ctx, s, args)
		}
	}
	return nil, fmt.Errorf("unregistered canonical terminal read: %s", flat)
}

const canonicalTerminalSlotSQL = `UPSERT INTO subscription_scheduler_slots (tenant_id, subscription_connection_id, state, active_run_id, active_reservation_id, blocked_until, updated_at, local_capacity_association, local_capacity_digest) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

var canonicalTerminalProjectionSQL = map[string]int{
	"INSERT INTO frontend_projection_outbox (tenant_id, frontend_projection_id, session_id, event_id, event_sequence, binding_id, binding_revision, frontend, status, created_at, updated_at, record) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CAST($12 AS JsonDocument))": 12,
	"INSERT INTO frontend_projections_by_session (tenant_id, session_id, frontend_projection_id) VALUES ($1, $2, $3)":                                                                                                                                                                   3,
	"INSERT INTO frontend_projections_by_run (tenant_id, run_id, frontend, frontend_projection_id) VALUES ($1, $2, $3, $4)":                                                                                                                                                             4,
	"INSERT INTO frontend_projection_ready_v1 (frontend, shard_bucket, created_at, tenant_id, frontend_projection_id, run_id) VALUES ($1, $2, $3, $4, $5, $6)":                                                                                                                          6,
}

func (f *canonicalTerminalSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (_ driver.Result, err error) {
	defer func() { f.trace(s, err) }()
	seed := f.seed.LocalCanonicalLoopSeed
	if s == localRetainedRunUpdateSQL {
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		return f.publicationConn.ExecContext(ctx, s, args)
	}
	if !f.rawCanonical {
		return f.seed.peer.ExecContext(ctx, s, args)
	}
	if e := f.rawCount(ctx); e != nil {
		return nil, e
	}
	flat := completionSQL(s)
	values := make([]driver.Value, len(args))
	for i, a := range args {
		values[i] = a.Value
	}
	if flat == canonicalTerminalSlotSQL {
		previous := seed.peer.base.slot
		if len(args) != 9 || len(previous) != 7 || args[0].Value != string(seed.Event.TenantID) || args[7].Value != previous[5] || args[8].Value != previous[6] {
			return nil, errors.New("terminal local slot exact original association")
		}
		physical, e := encodeCapacityStorageV1(seed.Loaded.Run.TenantID, "", seed.Loaded.Run.LocalCapacityAssociation)
		if e != nil || args[1].Value != physical.key {
			return nil, errors.New("terminal local slot physical key")
		}
		seed.peer.base.slot = []driver.Value{values[2], values[3], values[4], values[5], values[6], values[7], values[8]}
		f.state.ActiveRun = values[3].(string)
		f.state.ActiveReservation = values[4].(string)
		f.state.Mutations++
	} else if n, ok := canonicalTerminalProjectionSQL[flat]; ok {
		if len(args) != n {
			return nil, errors.New("terminal projection exact write shape")
		}
		if strings.HasPrefix(flat, "INSERT INTO frontend_projection_outbox ") {
			var p domain.FrontendProjection
			var b domain.FrontendBinding
			if json.Unmarshal([]byte(values[11].(string)), &p) != nil || json.Unmarshal([]byte(seed.peer.base.rows["frontend_bindings"][0].(string)), &b) != nil {
				return nil, errors.New("terminal projection body")
			}
			var event domain.SessionEvent
			for _, v := range f.state.Events {
				if v.ID == p.EventID {
					event = v
				}
			}
			if p.ValidateFor(event, b) != nil {
				return nil, errors.New("terminal projection original binding/event")
			}
			if _, ok := f.projection[string(p.ID)]; ok {
				return nil, errors.New("duplicate terminal projection")
			}
			f.projection[string(p.ID)] = p
		}
		f.state.Mutations++
	} else {
		if e := applyTerminalCompletionWrite(&f.state, seed.peer.base.rows, &seed.peer.base.counters, terminalCompletionLocalV2, s, args); e != nil {
			return nil, e
		}
	}
	// Store the exact physical mutation arguments as well as typed rows. All
	// rollback snapshots include these indexes/audit/activity projection effects.
	f.physical[flat] = append(f.physical[flat], values)
	switch {
	case strings.HasPrefix(flat, "DELETE FROM quota_expiry_v2 "):
		old := seed.peer.base.rows["quota_expiry_v2"]
		if len(old) != 8 || !f.exact(args, old[:4]...) {
			return nil, errors.New("terminal original quota expiry deletion")
		}
		seed.peer.base.rows["quota_expiry_v2"] = nil
		seed.peer.base.expiry = nil
	case strings.HasPrefix(flat, "DELETE FROM lease_heads "):
		seed.peer.base.rows["lease_heads"] = nil
	case strings.HasPrefix(flat, "DELETE FROM lease_expiry_v2 "):
		seed.peer.base.rows["lease_expiry_v2"] = nil
	case strings.HasPrefix(flat, "UPDATE sessions "):
		seed.peer.base.rows["sessions"] = []driver.Value{values[4]}
	case strings.HasPrefix(flat, "UPSERT INTO session_displays "):
		seed.peer.base.rows["session_displays"] = []driver.Value{values[3]}
	}
	return driver.RowsAffected(1), nil
}

type LocalCanonicalTerminalSeed struct {
	*LocalCanonicalToolSeed
	terminal *canonicalTerminalSQL
}

func NewLocalCanonicalTerminalSeed(t *testing.T, endpoint string) *LocalCanonicalTerminalSeed {
	t.Helper()
	seed := NewLocalCanonicalToolSeed(t, endpoint)
	p := &terminalPublicationSQL{base: &compactionPublicationSQL{compactionEffectSQL: seed.peer.effects}}
	f := &canonicalTerminalSQL{seed: seed, publication: p, publicationConn: &terminalPublicationConn{Conn: p.base, f: p}, projection: map[string]domain.FrontendProjection{}, physical: map[string][][]driver.Value{}, state: LocalTerminalCompletionSnapshot{material: map[string][]driver.Value{}, eventIndex: map[string][]driver.Value{}, eventRecords: map[string]string{}}}
	base := seed.LocalCanonicalLoopSeed.peer.base
	if json.Unmarshal([]byte(base.rows["sessions"][0].(string)), &f.state.Session) != nil || f.state.Session.LastEventSequence != seed.Event.Sequence {
		t.Fatal("actual canonical Session frontier")
	}
	f.state.material["current"] = append([]driver.Value(nil), base.rows["session_material_current_v1"]...)
	f.state.material[fmt.Sprint(seed.Event.Sequence)] = append([]driver.Value(nil), base.rows["session_material_boundaries_v1"]...)
	f.state.eventIndex[string(seed.Event.IdempotencyKey)] = []driver.Value{int64(seed.Event.Sequence), string(seed.Event.ID)}
	f.state.eventRecords[string(seed.Event.ID)] = base.rows["session_events"][0].(string)
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	seed.DB = db
	return &LocalCanonicalTerminalSeed{LocalCanonicalToolSeed: seed, terminal: f}
}
func (s *LocalCanonicalTerminalSeed) TerminalRun(t *testing.T) domain.Run {
	t.Helper()
	var r domain.Run
	if json.Unmarshal([]byte(s.LocalCanonicalLoopSeed.peer.base.rows["runs"][0].(string)), &r) != nil || r.Validate() != nil {
		t.Fatal("actual terminal Run DATA")
	}
	return r
}
func (s *LocalCanonicalTerminalSeed) TerminalPublicationCounts() (int, int, int) {
	f := s.terminal.publication
	return f.writes, f.commits, f.readbacks
}
func (s *LocalCanonicalTerminalSeed) TerminalAllocation(t *testing.T) (domain.RegisteredAllocationV2, domain.RegisteredAllocationV2) {
	t.Helper()
	row := s.LocalCanonicalLoopSeed.peer.alloc.row
	if len(row) != 12 {
		t.Fatal("actual allocation missing")
	}
	a, e := domain.DecodeRegisteredAllocationV2([]byte(row[6].(string)))
	if e != nil {
		t.Fatal(e)
	}
	b, e := domain.DecodeRegisteredAllocationV2([]byte(row[8].(string)))
	if e != nil {
		t.Fatal(e)
	}
	return a, b
}
func (s *LocalCanonicalTerminalSeed) TerminalSnapshot(t *testing.T) LocalTerminalCompletionSnapshot {
	t.Helper()
	v := cloneCanonicalTerminalState(s.terminal.state)
	base := s.LocalCanonicalLoopSeed.peer.base
	for table, target := range map[string]any{"runs": &v.Run, "attempts": &v.Attempt, "quota_reservations": &v.Reservation} {
		if json.Unmarshal([]byte(base.rows[table][0].(string)), target) != nil {
			t.Fatal("actual terminal DATA missing", table)
		}
	}
	v.LeasePresent = len(base.rows["lease_heads"]) != 0
	if len(base.slot) == 7 {
		v.ActiveRun = base.slot[1].(string)
		v.ActiveReservation = base.slot[2].(string)
	}
	if len(base.counters) == 2 {
		v.ActiveRuns = uint32(base.counters[1].(int64))
		v.CountersPresent = true
	}
	v.LostACKs = s.terminal.lostACKs
	return v
}

// Fault only the next ACTUAL mutating canonical commit. No finalization,
// evidence, owner or custody DATA is inserted by this test-only control.
func (s *LocalCanonicalTerminalSeed) LoseNextTerminalCommit(t *testing.T, mode string) {
	t.Helper()
	if mode != "rollback" && mode != "ambiguous" {
		t.Fatal("closed terminal commit fault")
	}
	if s.terminal.state.Finalizations != 0 {
		t.Fatal("fault must precede actual canonical commit")
	}
	s.terminal.completionFault = mode
}
func (s *LocalCanonicalTerminalSeed) TerminalProjectionCounts() (int, [4]int) {
	var counts [4]int
	for sql, rows := range s.terminal.physical {
		switch {
		case strings.HasPrefix(sql, "INSERT INTO frontend_projection_outbox "):
			counts[0] = len(rows)
		case strings.HasPrefix(sql, "INSERT INTO frontend_projections_by_session "):
			counts[1] = len(rows)
		case strings.HasPrefix(sql, "INSERT INTO frontend_projections_by_run "):
			counts[2] = len(rows)
		case strings.HasPrefix(sql, "INSERT INTO frontend_projection_ready_v1 "):
			counts[3] = len(rows)
		}
	}
	return len(s.terminal.projection), counts
}
func (s *LocalCanonicalTerminalSeed) TerminalTrace() string {
	return s.terminal.lastSQL + " / " + s.terminal.lastError
}
func (s *LocalCanonicalTerminalSeed) TerminalRawStatements() int { return s.terminal.maxRawStatements }
func (s *LocalCanonicalTerminalSeed) TerminalMaterial(t *testing.T) sessioncontext.SessionMaterialPrefixV1 {
	t.Helper()
	row := s.terminal.state.material["current"]
	if len(row) != 7 {
		t.Fatal("actual canonical current material missing")
	}
	v, e := sessioncontext.DecodeSessionMaterialPrefixV1([]byte(row[0].(string)))
	if e != nil {
		t.Fatal(e)
	}
	if len(s.terminal.state.material) != 5 {
		t.Fatal("actual canonical material boundaries missing")
	}
	return v
}
func (s *LocalCanonicalTerminalSeed) TerminalStorageReleased() bool {
	base := s.LocalCanonicalLoopSeed.peer.base
	return len(base.rows["quota_expiry_v2"]) == 0 && len(base.rows["lease_heads"]) == 0 && len(base.rows["lease_expiry_v2"]) == 0 && len(base.slot) == 7 && base.slot[1] == "" && base.slot[2] == "" && reflect.DeepEqual(base.counters, []driver.Value{int64(0), int64(0)})
}
