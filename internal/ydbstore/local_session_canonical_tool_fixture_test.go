//go:build sessionless_mcp_offline

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
		return f.effects.QueryContext(ctx, s, args)
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
