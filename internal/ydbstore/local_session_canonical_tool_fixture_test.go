//go:build sessionless_mcp_offline

package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
)

// Only initially-empty physical DATA machinery is reused. This overlay never
// invokes a rich fixture constructor, verifier, issuer or proxy producer.
type canonicalToolSQL struct {
	seed                                  *LocalCanonicalLoopSeed
	effects                               *compactionEffectSQL
	inserts, calls, observations, proxies int
	orderedReservations                   int
	lastSQL                               string
	diagnostic                            canonicalToolDiagnostic
}

// Observation only: no row, permission, clock or error is changed. Keep just
// the last call-index read and bounded counters, never authority/body bytes.
type canonicalToolDiagnostic struct {
	mu sync.Mutex
	canonicalToolDiagnosticSnapshot
}

type canonicalToolDiagnosticSnapshot struct {
	Queries, Rows, EOFs, NextErrors, Closes, CloseErrors int
	LastNext, LastClose                                  string
	LastSequence                                         uint64
	IndexMatches                                         bool
	CommittedOperations, PendingOperations               int
	CompletedObservations, UnknownObservations           int
	PendingObserved                                      bool
	FrontierSequence                                     uint64
	FrontierKind                                         string
	Deadline                                             time.Time
	DBDeadlineRemaining                                  time.Duration
}

type canonicalToolDiagnosticRows struct {
	driver.Rows
	diagnostic *canonicalToolDiagnostic
	sequence   uint64
	digest     string // comparison only; never included in the diagnostic snapshot
}

func (r *canonicalToolDiagnosticRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	d := r.diagnostic
	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case err == nil:
		d.Rows++
		d.LastNext = "row"
		var sequence uint64
		if len(dest) == 2 {
			switch v := dest[0].(type) {
			case uint64:
				sequence = v
			case int64:
				if v >= 0 {
					sequence = uint64(v)
				}
			}
			digest, ok := dest[1].(string)
			d.IndexMatches = ok && r.sequence != 0 && sequence == r.sequence && digest == r.digest
		}
		d.LastSequence = sequence
	case err == io.EOF:
		d.EOFs++
		d.LastNext = "eof"
	default:
		d.NextErrors++
		d.LastNext = "error"
	}
	return err
}

func (r *canonicalToolDiagnosticRows) Close() error {
	err := r.Rows.Close()
	d := r.diagnostic
	d.mu.Lock()
	d.Closes++
	d.LastClose = "ok"
	if err != nil {
		d.CloseErrors++
		d.LastClose = "error"
	}
	d.mu.Unlock()
	return err
}

func (f *canonicalToolSQL) diagnosticRows(rows driver.Rows, args []driver.NamedValue) driver.Rows {
	d := &f.diagnostic
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Queries++
	d.LastNext, d.LastClose = "not_called", "not_called"
	d.LastSequence, d.IndexMatches = 0, false
	d.CommittedOperations = len(f.effects.operations)
	d.PendingOperations = 0
	d.PendingObserved = false
	d.CompletedObservations, d.UnknownObservations = 0, 0
	d.FrontierSequence, d.FrontierKind, d.Deadline = 0, "none", time.Time{}
	var sequence uint64
	var digest string
	for _, row := range f.effects.operations[:min(len(f.effects.operations), 3)] {
		if len(row) != 11 {
			continue
		}
		var reservation domain.AttemptOperationReservationV1
		raw, _ := row[1].(string)
		if json.Unmarshal([]byte(raw), &reservation) != nil {
			continue
		}
		if reservation.Operation.CallID == args[2].Value {
			sequence, digest = reservation.Operation.Sequence, string(reservation.OperationDigest)
		}
		d.FrontierSequence = reservation.Operation.Sequence
		d.FrontierKind = "invalid"
		switch reservation.Operation.Kind {
		case domain.AttemptOperationModelV1, domain.AttemptOperationMCPToolV1:
			d.FrontierKind = string(reservation.Operation.Kind)
		}
		d.Deadline = reservation.Operation.Deadline
		var observation domain.AttemptOperationObservationV1
		observed, _ := row[7].(string)
		if json.Unmarshal([]byte(observed), &observation) == nil {
			switch observation.Outcome {
			case domain.AttemptOperationCompletedV1:
				d.CompletedObservations++
			case domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationRejectedBeforeSendV1:
				d.UnknownObservations++
			}
		}
	}
	if len(f.effects.pending) > 0 {
		d.PendingOperations = 1
		var reservation domain.AttemptOperationReservationV1
		raw, _ := f.effects.pending[0].(string)
		if json.Unmarshal([]byte(raw), &reservation) == nil && reservation.Operation.CallID == args[2].Value {
			sequence, digest = reservation.Operation.Sequence, string(reservation.OperationDigest)
		}
		if reservation.Operation.Sequence >= d.FrontierSequence {
			d.FrontierSequence = reservation.Operation.Sequence
			d.FrontierKind = "invalid"
			switch reservation.Operation.Kind {
			case domain.AttemptOperationModelV1, domain.AttemptOperationMCPToolV1:
				d.FrontierKind = string(reservation.Operation.Kind)
			}
			d.Deadline = reservation.Operation.Deadline
		}
		d.PendingObserved = len(f.effects.pending) == 8 && f.effects.pending[4] == uint64(1) && f.effects.pending[5] == uint64(1)
	}
	d.DBDeadlineRemaining = 0
	if !d.Deadline.IsZero() {
		d.DBDeadlineRemaining = d.Deadline.Sub(f.seed.peer.base.at)
	}
	return &canonicalToolDiagnosticRows{Rows: rows, diagnostic: d, sequence: sequence, digest: digest}
}

func (f *canonicalToolSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *canonicalToolSQL) Prepare(s string) (driver.Stmt, error)        { return f.seed.loop.Prepare(s) }
func (f *canonicalToolSQL) Close() error                                 { return f.seed.loop.Close() }
func (f *canonicalToolSQL) Begin() (driver.Tx, error) {
	return nil, errors.New("canonical tool requires bounded explicit transaction")
}
func (f *canonicalToolSQL) Driver() driver.Driver { return f.seed.loop.Driver() }
func (f *canonicalToolSQL) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	f.seed.loop.callbackSQL = 0
	f.seed.peer.localWorkerLifecycleSQL.beginPhase("canonical tool callback")
	return f.effects.BeginTx(ctx, o)
}
func (f *canonicalToolSQL) observe(ctx context.Context, s string) error {
	f.lastSQL = strings.Join(strings.Fields(s), " ")
	if len(f.lastSQL) > 160 {
		f.lastSQL = f.lastSQL[:160]
	}
	return f.seed.loop.countStatement(ctx)
}
func (f *canonicalToolSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	if err := f.observe(ctx, s); err != nil {
		return nil, err
	}
	r := f.seed.peer.base.request
	switch s {
	case ownedOperationReadSQL, ownedOperationCallSQL, ownedOperationProxyRegistrationSQL:
		if len(args) != 3 || args[0].Value != string(r.TenantID) || args[1].Value != string(r.AttemptID) {
			return nil, errors.New("canonical tool point scope")
		}
		if s != ownedOperationCallSQL {
			n, ok := args[2].Value.(int64)
			if !ok || n < 1 || n > 3 {
				return nil, errors.New("canonical tool bounded sequence")
			}
		}
		if s == ownedOperationProxyRegistrationSQL && args[2].Value == int64(1) {
			row := f.effects.firstProxy
			if row == nil {
				row = f.effects.row
			}
			if len(row) != 12 {
				return &explanationFixtureRows{}, nil
			}
			return &explanationFixtureRows{values: append(append([]driver.Value(nil), row...), string(domain.RegisteredProxyProviderV1))}, nil
		}
		rows, err := f.effects.QueryContext(ctx, s, args)
		if s == ownedOperationCallSQL && err == nil && rows != nil {
			rows = f.diagnosticRows(rows, args)
		}
		return rows, err
	case ownedProxyRegistrationSQL:
		if len(f.seed.peer.parent.parentRow) != 15 || !f.seed.peer.exactArgs(args, string(r.TenantID), string(r.AttemptID), f.seed.peer.parent.parentRow[2]) {
			return nil, errors.New("canonical tool original parent proxy key")
		}
		return f.effects.QueryContext(ctx, s, args)
	case localCompactionEventsSQL:
		if len(args) != 6 || !f.seed.peer.exactArgs(args[:4], string(r.TenantID), string(r.SessionID), int64(0), int64(1)) || args[4].Value != int64(2) || args[5].Value != int64(8193) {
			return nil, errors.New("canonical tool original committed event range")
		}
		return f.effects.QueryContext(ctx, s, args)
	case localCompactionOperationsSQL, localCompactionProxiesSQL:
		if !f.seed.peer.exactArgs(args, string(r.TenantID), string(r.AttemptID)) {
			return nil, errors.New("canonical tool inventory scope")
		}
		return f.effects.QueryContext(ctx, s, args)
	}
	return f.seed.peer.QueryContext(ctx, s, args)
}

const canonicalToolProxyInsertSQL = `INSERT INTO attempt_operation_proxies_v1 (tenant_id,attempt_id,sequence,service_kind,original_record,original_digest,current_record,current_digest,policy_body,attestation_body) VALUES ($1,$2,$3,$4,$5,$6,$5,$6,$7,$8)`

func (f *canonicalToolSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Result, error) {
	if err := f.observe(ctx, s); err != nil {
		return nil, err
	}
	switch s {
	case loopbackOperationInsertSQL:
		if len(args) != 7 {
			return nil, errors.New("canonical tool operation insert shape")
		}
		var next domain.AttemptOperationReservationV1
		if json.Unmarshal([]byte(args[3].Value.(string)), &next) != nil || next.Operation.Sequence < 1 || next.Operation.Sequence > 3 {
			return nil, errors.New("canonical tool finite original operation")
		}
		if next.Operation.Sequence > 1 {
			rows := f.effects.operations
			if len(rows) != int(next.Operation.Sequence)-1 {
				return nil, errors.New("canonical tool predecessor not committed")
			}
			var previous domain.AttemptOperationObservationV1
			row := rows[len(rows)-1]
			if len(row) != 11 || row[5] != uint64(1) || row[6] != uint64(1) || json.Unmarshal([]byte(row[7].(string)), &previous) != nil || previous.Outcome != domain.AttemptOperationCompletedV1 {
				return nil, errors.New("canonical tool predecessor not durable completed")
			}
			digest, err := previous.Digest()
			if err != nil || next.Operation.PredecessorObservationDigest != digest || next.ReservedAt.Before(previous.ObservedAt) {
				return nil, errors.New("canonical tool original predecessor identity")
			}
		}
		result, err := f.effects.ExecContext(ctx, s, args)
		if err == nil {
			f.inserts++
			if next.Operation.Sequence > 1 {
				f.orderedReservations++
			}
		}
		return result, err
	case loopbackCallInsertSQL, loopbackObservationUpdateSQL, ownedProxyRegistrationCreateSQL, canonicalToolProxyInsertSQL:
		result, err := f.effects.ExecContext(ctx, s, args)
		if err == nil {
			switch s {
			case loopbackCallInsertSQL:
				f.calls++
			case loopbackObservationUpdateSQL:
				f.observations++
			default:
				f.proxies++
			}
		}
		return result, err
	}
	return f.seed.peer.ExecContext(ctx, s, args)
}

type LocalCanonicalToolSeed struct {
	*LocalCanonicalLoopSeed
	Grant mcpadmission.MCPToolGrantV1
	peer  *canonicalToolSQL
	hold  []driver.Value
}

func NewLocalCanonicalToolSeed(t *testing.T, endpoint string) *LocalCanonicalToolSeed {
	t.Helper()
	seed := NewLocalCanonicalLoopSeed(t, endpoint)
	source, err := domain.DecodeAttemptExecutionSourceV1([]byte(seed.Loaded.Job.OwnedExecutionSourceRecord))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := seed.Source.ReadMCPToolGrant(t.Context(), source.Profile, seed.Now())
	if err != nil {
		t.Fatal("same original Source installed tool grant DATA", err)
	}
	rows := &compactionSourceSQL{retainedEntrySQL: &retainedEntrySQL{loopbackOperationSQL: seed.loop.operations}, events: [][]driver.Value{append([]driver.Value(nil), seed.peer.base.rows["session_events"]...)}}
	effects := &compactionEffectSQL{compactionSourceSQL: rows}
	peer := &canonicalToolSQL{seed: seed, effects: effects}
	if effects.operations != nil || effects.proxies != nil || effects.row != nil || seed.peer.parent.parentRow != nil || seed.peer.alloc.row != nil {
		t.Fatal("canonical tool setup seeded effects")
	}
	db := sql.OpenDB(peer)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	seed.DB = db
	return &LocalCanonicalToolSeed{LocalCanonicalLoopSeed: seed, Grant: grant, peer: peer, hold: append([]driver.Value(nil), seed.peer.base.rows["quota_reservations"]...)}
}

func (s *LocalCanonicalToolSeed) OperationReadback(t *testing.T) ([]domain.AttemptOperationReservationV1, []domain.AttemptOperationObservationV1) {
	t.Helper()
	var reservations []domain.AttemptOperationReservationV1
	var observations []domain.AttemptOperationObservationV1
	for _, row := range s.peer.effects.operations {
		var r domain.AttemptOperationReservationV1
		var o domain.AttemptOperationObservationV1
		if len(row) != 11 || json.Unmarshal([]byte(row[1].(string)), &r) != nil || r.Validate() != nil || row[5] != uint64(1) || row[6] != uint64(1) || json.Unmarshal([]byte(row[7].(string)), &o) != nil || o.ValidateForReservation(r) != nil {
			t.Fatal("canonical tool durable row invalid")
		}
		rd, _ := r.Digest()
		od, _ := o.Digest()
		if row[2] != string(rd) || row[8] != string(od) || row[9] != r.Operation.Sequence || row[10] != string(r.OperationDigest) {
			t.Fatal("canonical tool exact physical digest/index mismatch")
		}
		reservations = append(reservations, r)
		observations = append(observations, o)
	}
	return reservations, observations
}
func (s *LocalCanonicalToolSeed) ExactThreeEffects() bool {
	f := s.peer
	row := s.LocalCanonicalLoopSeed.peer.parent.accountingRow
	var accounting ownedAttemptAccounting
	var last domain.AttemptOperationReservationV1
	if len(row) != 4 || row[0] != uint64(1) || row[1] != uint64(1) || json.Unmarshal([]byte(row[2].(string)), &accounting) != nil || len(f.effects.operations) != 3 || json.Unmarshal([]byte(f.effects.operations[2][1].(string)), &last) != nil || accounting.Aggregate != last.AggregateDebit || accounting.Amounts != last.ComponentAmounts {
		return false
	}
	return f.inserts == 3 && f.calls == 3 && f.observations == 3 && f.proxies == 3 && f.orderedReservations == 2 && len(f.effects.proxies) == 2 && len(f.effects.firstProxy) == 12 && reflect.DeepEqual(s.hold, s.LocalCanonicalLoopSeed.peer.base.rows["quota_reservations"])
}
func (s *LocalCanonicalToolSeed) LastSQL() string { return s.peer.lastSQL }

// Called only after a failed synchronous loop. DATA was captured at the last
// actual index query; the failure sample uses the fixture's same UTC-us clock.
func (s *LocalCanonicalToolSeed) OperationDiagnostic() string {
	d := &s.peer.diagnostic
	d.mu.Lock()
	v := d.canonicalToolDiagnosticSnapshot
	d.mu.Unlock()
	remaining := time.Duration(0)
	if !v.Deadline.IsZero() {
		remaining = v.Deadline.Sub(s.Now())
	}
	return fmt.Sprintf("last_index_snapshot={queries=%d rows=%d eof=%d next_errors=%d closes=%d close_errors=%d last_next=%s last_close=%s last_sequence=%d index_matches=%t committed=%d pending=%d completed=%d unknown_or_rejected=%d pending_observed=%t frontier_sequence=%d frontier_kind=%s db_deadline_remaining=%s} failure_deadline_remaining=%s",
		v.Queries, v.Rows, v.EOFs, v.NextErrors, v.Closes, v.CloseErrors, v.LastNext, v.LastClose, v.LastSequence, v.IndexMatches,
		v.CommittedOperations, v.PendingOperations, v.CompletedObservations, v.UnknownObservations, v.PendingObserved,
		v.FrontierSequence, v.FrontierKind, v.DBDeadlineRemaining, remaining)
}

type canonicalDiagnosticRowsFixture struct {
	values            []driver.Value
	nextErr, closeErr error
}

func (*canonicalDiagnosticRowsFixture) Columns() []string { return []string{"sequence", "digest"} }
func (r *canonicalDiagnosticRowsFixture) Close() error    { return r.closeErr }
func (r *canonicalDiagnosticRowsFixture) Next(dest []driver.Value) error {
	copy(dest, r.values)
	return r.nextErr
}

func TestLocalSessionCredentialedMCPOfflineCanonicalDiagnosticRows(t *testing.T) {
	nextErr, closeErr := errors.New("next sentinel"), errors.New("close sentinel")
	for _, tc := range []struct {
		name, nextClass string
		err             error
	}{{name: "row", nextClass: "row"}, {name: "eof", nextClass: "eof", err: io.EOF}, {name: "error", nextClass: "error", err: nextErr}} {
		t.Run(tc.name, func(t *testing.T) {
			original := &canonicalDiagnosticRowsFixture{values: []driver.Value{uint64(2), "private digest"}, nextErr: tc.err, closeErr: closeErr}
			d := &canonicalToolDiagnostic{}
			rows := &canonicalToolDiagnosticRows{Rows: original, diagnostic: d, sequence: 2, digest: "private digest"}
			dest := make([]driver.Value, 2)
			if err := rows.Next(dest); err != tc.err || !reflect.DeepEqual(dest, original.values) || !reflect.DeepEqual(rows.Columns(), original.Columns()) {
				t.Fatal("diagnostic changed underlying row values, columns or Next error")
			}
			if err := rows.Close(); err != closeErr || d.LastNext != tc.nextClass || d.LastClose != "error" || d.Closes != 1 || d.CloseErrors != 1 {
				t.Fatal("diagnostic changed Close error or missed row/close observation")
			}
			if tc.err == nil && (!d.IndexMatches || d.LastSequence != 2 || d.Rows != 1) {
				t.Fatal("diagnostic missed exact delivered call-index association")
			}
		})
	}
}
