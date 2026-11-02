package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// DATA-only overlay over the SAME genuine canonical/lease seed. No rich
// running fixture, publication verifier, socket witness or grant is supplied.
type canonicalLoopSQL struct {
	*canonicalSessionSQL
	operations                  *loopbackOperationSQL
	callbackSQL, maxCallbackSQL int
}

func (f *canonicalLoopSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *canonicalLoopSQL) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	f.localWorkerLifecycleSQL.beginPhase("canonical worker loop callback")
	f.callbackSQL = 0
	return f.operations.BeginTx(ctx, opts)
}
func (f *canonicalLoopSQL) countStatement(ctx context.Context) error {
	f.callbackSQL++
	f.maxCallbackSQL = max(f.maxCallbackSQL, f.callbackSQL)
	if ctx.Err() != nil || f.callbackSQL > 32 {
		return errors.New("canonical loop original callback statement bound")
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("canonical loop unbounded callback")
	}
	return nil
}
func (f *canonicalLoopSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	if err := f.countStatement(ctx); err != nil {
		return nil, err
	}
	r := f.base.request
	switch s {
	case ownedProxyRegistrationSQL:
		if len(f.parent.parentRow) != 15 || !f.exactArgs(args, string(r.TenantID), string(r.AttemptID), f.parent.parentRow[2]) {
			return nil, errors.New("canonical loop original parent proxy key")
		}
		return f.operations.QueryContext(ctx, s, args)
	case ownedOperationReadSQL, ownedOperationCallSQL:
		if len(args) != 3 || args[0].Value != string(r.TenantID) || args[1].Value != string(r.AttemptID) || (s == ownedOperationReadSQL && args[2].Value != int64(1)) {
			return nil, errors.New("canonical loop original operation key")
		}
		return f.operations.QueryContext(ctx, s, args)
	case ownedOperationProxyRegistrationSQL:
		if !f.exactArgs(args, string(r.TenantID), string(r.AttemptID), int64(1)) || len(f.operations.row) != 12 {
			return nil, errors.New("canonical loop exact original proxy key")
		}
		f.base.recordQuery(s, args)
		return &explanationFixtureRows{values: append(append([]driver.Value(nil), f.operations.row...), string(domain.RegisteredProxyProviderV1))}, nil
	case localCompactionEventsSQL:
		if len(args) != 6 || !f.exactArgs(args[:4], string(r.TenantID), string(r.SessionID), int64(0), int64(1)) || args[4].Value != int64(2) || args[5].Value != int64(8193) {
			return nil, errors.New("canonical loop exact committed event range")
		}
		f.base.recordQuery(s, args)
		row := append([]driver.Value(nil), f.base.rows["session_events"]...)
		if len(row) != 1 {
			return nil, errors.New("canonical loop committed event missing")
		}
		if raw := row[0].(string); len(raw) > int(args[5].Value.(int64)) {
			row[0] = raw[:int(args[5].Value.(int64))]
		}
		return &compactionSourceRows{rows: [][]driver.Value{row}}, nil
	case localCompactionOperationsSQL, localCompactionProxiesSQL:
		if !f.exactArgs(args, string(r.TenantID), string(r.AttemptID)) {
			return nil, errors.New("canonical loop exact operation inventory")
		}
		f.base.recordQuery(s, args)
		var rows [][]driver.Value
		if s == localCompactionOperationsSQL && f.operations.operation != nil {
			var reservation domain.AttemptOperationReservationV1
			if json.Unmarshal([]byte(f.operations.operation[0].(string)), &reservation) != nil || len(f.operations.call) != 2 {
				return nil, errors.New("canonical loop persisted operation/index mismatch")
			}
			row := append([]driver.Value{int64(reservation.Operation.Sequence)}, f.operations.operation...)
			row = append(row, f.operations.call...)
			for i, limit := range map[int]int{1: 32769, 2: 65, 3: 65, 4: 161, 7: 32769, 8: 65, 10: 65} {
				if raw := row[i].(string); len(raw) > limit {
					row[i] = raw[:limit]
				}
			}
			rows = [][]driver.Value{row}
		}
		// This first-model peer has no sequence>1 proxy; its actual first proxy
		// lives only in the physical parent publication columns above.
		return &compactionSourceRows{rows: rows}, nil
	}
	return f.canonicalSessionSQL.QueryContext(ctx, s, args)
}
func (f *canonicalLoopSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Result, error) {
	if err := f.countStatement(ctx); err != nil {
		return nil, err
	}
	switch s {
	case ownedProxyRegistrationCreateSQL, loopbackOperationInsertSQL, loopbackCallInsertSQL, loopbackObservationUpdateSQL:
		return f.operations.ExecContext(ctx, s, args)
	}
	return f.canonicalSessionSQL.ExecContext(ctx, s, args)
}

func (s *LocalCanonicalLoopSeed) MaxCallbackSQL() int { return s.loop.maxCallbackSQL }
func (s *LocalCanonicalLoopSeed) ModelEffectsAbsent() bool {
	f := s.loop.operations
	return f.row == nil && f.operation == nil && f.call == nil && f.operationWrites == 0 && f.callWrites == 0 && f.observationWrites == 0 && f.writes == 0
}

// Only raw dependencies and persisted observation DATA cross the test package
// cycle. The external test still constructs the original session/keeper.
type LocalCanonicalLoopSeed struct {
	*LocalCanonicalSessionSeed
	Payload domain.BlobRef
	Event   domain.SessionEvent
	Body    []byte
	loop    *canonicalLoopSQL
}

func NewLocalCanonicalLoopSeed(t *testing.T, endpoint string) *LocalCanonicalLoopSeed {
	t.Helper()
	seed := NewLocalCanonicalSessionSeed(t, endpoint)
	var event domain.SessionEvent
	if len(seed.peer.base.rows["session_events"]) != 1 || json.Unmarshal([]byte(seed.peer.base.rows["session_events"][0].(string)), &event) != nil {
		t.Fatal("actual canonical event absent")
	}
	body, _, err := seed.peer.base.request.PreparedMaterial.CopyForFinalV1(event)
	if err != nil {
		t.Fatal("original committed material copy", err)
	}
	operations := &loopbackOperationSQL{localProxySQL: &localProxySQL{allocationConn: allocationConn{seed.peer.alloc}}}
	peer := &canonicalLoopSQL{canonicalSessionSQL: seed.peer, operations: operations}
	db := sql.OpenDB(peer)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		clear(body)
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	seed.DB = db
	return &LocalCanonicalLoopSeed{LocalCanonicalSessionSeed: seed, Payload: event.Payload, Event: event, Body: body, loop: peer}
}

func (s *LocalCanonicalLoopSeed) DurableModel() (domain.AttemptOperationReservationV1, domain.AttemptOperationObservationV1, bool) {
	var r domain.AttemptOperationReservationV1
	var o domain.AttemptOperationObservationV1
	row := s.loop.operations.operation
	if len(row) != 8 || json.Unmarshal([]byte(row[0].(string)), &r) != nil || row[4] != uint64(1) || row[5] != uint64(1) || json.Unmarshal([]byte(row[6].(string)), &o) != nil {
		return r, o, false
	}
	return r, o, r.Validate() == nil && o.ValidateForReservation(r) == nil && s.loop.operations.operationWrites == 1 && s.loop.operations.callWrites == 1 && s.loop.operations.observationWrites == 1 && s.loop.operations.writes == 1
}
