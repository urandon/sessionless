package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/runexplanationrate"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

// This standalone database/sql connector drives the real adapter and SDK
// retry path. Its staged writes model known rollback and committed/lost ACK,
// not native YDB serializability, query plans, or distributed contention.
type ratedSQLFixture struct {
	rows                       map[string][]driver.Value
	slots                      map[uint32][]driver.Value
	pending                    map[uint32][]driver.Value
	statements                 []string
	arguments                  [][]driver.NamedValue
	isolations                 []driver.IsolationLevel
	writes, commits, rollbacks int
	commitModes                []string
	afterCommit                func()
	failTable                  string
	deadlineMissing            bool
}

func (f *ratedSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (*ratedSQLFixture) Driver() driver.Driver                          { return explanationFixtureDriver{} }
func (*ratedSQLFixture) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fixture prepare forbidden")
}
func (*ratedSQLFixture) Close() error { return nil }
func (*ratedSQLFixture) Begin() (driver.Tx, error) {
	return nil, errors.New("fixture requires BeginTx")
}
func (f *ratedSQLFixture) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	f.isolations = append(f.isolations, options.Isolation)
	f.pending = make(map[uint32][]driver.Value)
	return ratedFixtureTx{f}, nil
}

type ratedFixtureTx struct{ f *ratedSQLFixture }

func (tx ratedFixtureTx) Commit() error {
	f := tx.f
	f.commits++
	mode := ""
	if len(f.commitModes) > 0 {
		mode, f.commitModes = f.commitModes[0], f.commitModes[1:]
	}
	if mode != "rollback" {
		for key, row := range f.pending {
			f.slots[key] = row
		}
	}
	f.pending = nil
	if f.afterCommit != nil {
		f.afterCommit()
	}
	switch mode {
	case "rollback", "ambiguous":
		return retry.RetryableError(errors.New("fixture commit acknowledgement unavailable"), retry.WithBackoff(retry.TypeNoBackoff))
	case "unknown":
		return errors.New("fixture unresolved commit acknowledgement")
	default:
		return nil
	}
}
func (tx ratedFixtureTx) Rollback() error { tx.f.rollbacks++; tx.f.pending = nil; return nil }

func (f *ratedSQLFixture) statement(ctx context.Context, statement string, args []driver.NamedValue) error {
	f.statements = append(f.statements, statement)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	if _, ok := ctx.Deadline(); !ok {
		f.deadlineMissing = true
	}
	if strings.Contains(statement, "ORDER BY") || strings.Contains(statement, "audit") || strings.Contains(statement, "worker_jobs") {
		return errors.New("fixture forbids non-point/private query")
	}
	return nil
}
func (f *ratedSQLFixture) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if err := f.statement(ctx, statement, args); err != nil {
		return nil, err
	}
	if strings.Contains(statement, "CurrentUtcTimestamp") {
		return &explanationFixtureRows{values: f.rows["clock"]}, nil
	}
	if strings.Contains(statement, "FROM run_explanation_rate_slots_v1 ") {
		if statement != `SELECT SUBSTRING(CAST(record AS String),0,8193), expire_at FROM run_explanation_rate_slots_v1 WHERE slot_id=$1` || len(args) != 1 {
			return nil, errors.New("fixture rate read must be bounded exact PK")
		}
		key, err := ratedFixtureKey(args[0].Value)
		if err != nil {
			return nil, err
		}
		return &explanationFixtureRows{values: f.slots[key]}, nil
	}
	for table, row := range f.rows {
		if strings.Contains(statement, "FROM "+table+" ") {
			if table == f.failTable {
				return nil, errors.New("fixture backend read failed")
			}
			if !strings.Contains(statement, "SUBSTRING(") && table != "attached_workers" && table != "attached_worker_connections" {
				return nil, errors.New("fixture JSON query must be bounded")
			}
			return &explanationFixtureRows{values: row}, nil
		}
	}
	return &explanationFixtureRows{}, nil
}
func (f *ratedSQLFixture) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if err := f.statement(ctx, statement, args); err != nil {
		return nil, err
	}
	if statement != `UPSERT INTO run_explanation_rate_slots_v1 (slot_id, record, expire_at) VALUES ($1, CAST($2 AS JsonDocument), $3)` || len(args) != 3 || f.pending == nil {
		return nil, errors.New("fixture forbids non-rate mutation")
	}
	key, err := ratedFixtureKey(args[0].Value)
	if err != nil {
		return nil, err
	}
	encoded, ok := args[1].Value.(string)
	if !ok {
		return nil, errors.New("fixture rate JSON must be string")
	}
	expires, ok := args[2].Value.(time.Time)
	if !ok {
		return nil, errors.New("fixture rate expiry must be timestamp")
	}
	slot, err := runexplanationrate.DecodeSlot([]byte(encoded))
	if err != nil || !slot.ExpiresAt.Equal(expires) {
		return nil, errors.New("fixture rate write must validate")
	}
	f.writes++
	f.pending[key] = []driver.Value{encoded, expires}
	return driver.RowsAffected(1), nil
}
func ratedFixtureKey(value any) (uint32, error) {
	key, ok := value.(int64)
	if !ok || key < 0 || key >= runexplanationrate.MaxSlots {
		return 0, errors.New("fixture key outside finite Uint32 space")
	}
	return uint32(key), nil
}

func ratedReadFixture(t *testing.T, attached bool) (*Store, *ratedSQLFixture, ports.RunExplanationAuthorizationV1, domain.Run) {
	t.Helper()
	var base *explanationSQLFixture
	var auth ports.RunExplanationAuthorizationV1
	var run domain.Run
	if attached {
		_, base, auth, run = explanationAttachedReadFixture(t)
	} else {
		_, base, auth, run = explanationReadFixture(t)
	}
	f := &ratedSQLFixture{rows: base.rows, slots: make(map[uint32][]driver.Value)}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close rated fixture DB: %v", err)
		}
	})
	store, err := New(db, Options{})
	if err != nil {
		t.Fatalf("create rated fixture store: %v", err)
	}
	return store, f, auth, run
}
func ratedFixtureRecord[T any](t *testing.T, f *ratedSQLFixture, table string) T {
	t.Helper()
	var value T
	if err := json.Unmarshal([]byte(f.rows[table][0].(string)), &value); err != nil {
		t.Fatalf("decode fixture %s: %v", table, err)
	}
	return value
}
func ratedFixturePut(t *testing.T, f *ratedSQLFixture, table string, value any) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode fixture %s: %v", table, err)
	}
	f.rows[table] = []driver.Value{string(encoded)}
}
func ratedFixtureSlot(t *testing.T, f *ratedSQLFixture) runexplanationrate.Slot {
	t.Helper()
	if len(f.slots) != 1 {
		t.Fatalf("committed slots=%d want=1", len(f.slots))
	}
	for _, row := range f.slots {
		slot, err := runexplanationrate.DecodeSlot([]byte(row[0].(string)))
		if err != nil {
			t.Fatalf("decode committed slot: %v", err)
		}
		return slot
	}
	panic("unreachable")
}
func assertRatedOutcome(t *testing.T, value ports.RunExplanationReadOutcomeV1, err error, kind ports.RunExplanationReadOutcomeKindV1) {
	t.Helper()
	if err != nil || value.Kind != kind || value.Validate() != nil {
		t.Fatalf("rated outcome=%+v error=%v want=%s valid committed outcome", value, err, kind)
	}
}

func TestRatedRunExplanationActualStatementManifest(t *testing.T) {
	for _, test := range []struct {
		name       string
		attached   bool
		statements int
	}{{name: "historical", statements: 12}, {name: "attached", attached: true, statements: 17}} {
		t.Run(test.name, func(t *testing.T) {
			store, f, auth, run := ratedReadFixture(t, test.attached)
			beforeSession := f.rows["web_sessions"][0]
			beforeMembership := f.rows["tenant_memberships"][0]
			value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "request-one", run.ID)
			assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
			if len(f.statements) != test.statements || f.writes != 1 || f.commits != 1 || len(f.isolations) != 1 || f.isolations[0] != driver.IsolationLevel(sql.LevelSerializable) || f.deadlineMissing {
				t.Fatalf("actual statements=%d writes=%d commits=%d isolation=%v deadlineMissing=%t want=%d,1,1,serializable,false", len(f.statements), f.writes, f.commits, f.isolations, f.deadlineMissing, test.statements)
			}
			if !strings.HasPrefix(f.statements[7], "UPSERT INTO run_explanation_rate_slots_v1") || !strings.Contains(f.statements[8], "FROM runs ") {
				t.Fatalf("debit must follow all four keys and precede target; statements=%v", f.statements)
			}
			if f.rows["web_sessions"][0] != beforeSession || f.rows["tenant_memberships"][0] != beforeMembership {
				t.Fatal("READ changed canonical authorization/activity")
			}
			encoded, _ := json.Marshal(value)
			if len(encoded) > runexplanation.MaxResponseBytes || strings.Contains(string(encoded), "private-") {
				t.Fatalf("unsafe rated response bytes=%d body=%s", len(encoded), encoded)
			}
		})
	}
}

func TestRatedRunExplanationUsesOnlyCurrentCanonicalIdentityAndTenant(t *testing.T) {
	store, f, auth, run := ratedReadFixture(t, false)
	session := ratedFixtureRecord[domain.WebSession](t, f, "web_sessions")
	session.UserID, session.ActiveTenantID = "current-user", "current-tenant"
	session.MembershipSecurityVersion = 7
	ratedFixturePut(t, f, "web_sessions", session)
	membership := ratedFixtureRecord[domain.TenantMembership](t, f, "tenant_memberships")
	membership.UserID, membership.TenantID, membership.SecurityVersion = session.UserID, session.ActiveTenantID, 7
	ratedFixturePut(t, f, "tenant_memberships", membership)
	run.TenantID = session.ActiveTenantID
	ratedFixturePut(t, f, "runs", run)
	canonical := ratedFixtureRecord[domain.Session](t, f, "sessions")
	canonical.TenantID = session.ActiveTenantID
	ratedFixturePut(t, f, "sessions", canonical)
	participant := ratedFixtureRecord[domain.SessionParticipant](t, f, "session_participants")
	participant.UserID, participant.TenantID = session.UserID, session.ActiveTenantID
	ratedFixturePut(t, f, "session_participants", participant)
	value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "current-context", run.ID)
	assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
	userBucket, _ := webBucket(string(session.UserID))
	for _, test := range []struct {
		index  int
		values []any
	}{
		{index: 2, values: []any{int64(userBucket), string(session.UserID), string(session.ActiveTenantID)}},
		{index: 8, values: []any{string(session.ActiveTenantID), string(run.ID)}},
		{index: 9, values: []any{string(session.ActiveTenantID), string(run.SessionID)}},
		{index: 10, values: []any{string(session.ActiveTenantID), string(run.SessionID), string(session.UserID)}},
	} {
		args := f.arguments[test.index]
		if len(args) != len(test.values) {
			t.Fatalf("statement%d argument count=%d want=%d", test.index, len(args), len(test.values))
		}
		for i, want := range test.values {
			if args[i].Value != want {
				t.Errorf("statement%d argument%d=%v want current canonical=%v", test.index, i, args[i].Value, want)
			}
		}
	}
	identity, _, _ := runexplanationrate.IdentityKeys(session.ActiveTenantID, session.UserID)
	if got := ratedFixtureSlot(t, f).IdentityDigest; got != identity {
		t.Fatalf("charged identity=%s want resolved canonical=%s", got, identity)
	}
}

func TestRatedRunExplanationCommittedNotFoundConsumesDebit(t *testing.T) {
	for _, table := range []string{"runs", "sessions", "session_participants"} {
		t.Run(table, func(t *testing.T) {
			store, f, auth, run := ratedReadFixture(t, false)
			delete(f.rows, table)
			for i := 0; i < 3; i++ {
				value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, fmt.Sprintf("request-%d", i), run.ID)
				kind := ports.RunExplanationReadNotFoundV1
				if i == 2 {
					kind = ports.RunExplanationReadRateLimitedV1
				}
				assertRatedOutcome(t, value, err, kind)
			}
			if f.commits != 3 || f.writes != 2 || len(ratedFixtureSlot(t, f).Receipts) != 2 {
				t.Fatalf("opaque %s commits=%d writes=%d want=3,2", table, f.commits, f.writes)
			}
		})
	}
}

func TestRatedRunExplanationRateDenialCommitsWithoutResourceReadOrMutation(t *testing.T) {
	for _, mode := range []string{"confirmed", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			store, f, auth, run := ratedReadFixture(t, false)
			for i := 0; i < 2; i++ {
				value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, fmt.Sprintf("fill-%d", i), run.ID)
				assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
			}
			before, _ := json.Marshal(ratedFixtureSlot(t, f))
			start := len(f.statements)
			if mode == "unknown" {
				f.commitModes = []string{"unknown"}
			}
			value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "rate-denied", "unread-guessed-run")
			if mode == "unknown" {
				if !errors.Is(err, ports.ErrRunExplanationUnavailable) || value.Kind != "" {
					t.Fatalf("unconfirmed 429 outcome=%+v err=%v want empty unavailable", value, err)
				}
			} else {
				assertRatedOutcome(t, value, err, ports.RunExplanationReadRateLimitedV1)
				if value.RetryAfter != 5*time.Second {
					t.Fatalf("RetryAfter=%v want=5s", value.RetryAfter)
				}
			}
			after, _ := json.Marshal(ratedFixtureSlot(t, f))
			if string(before) != string(after) || f.writes != 2 || f.commits != 3 || len(f.statements)-start != 7 {
				t.Fatalf("429 changed slot=%t writes=%d commits=%d statements=%d want false,2,3,7", string(before) != string(after), f.writes, f.commits, len(f.statements)-start)
			}
			for _, statement := range f.statements[start:] {
				if strings.Contains(statement, "FROM runs ") || strings.Contains(statement, "UPSERT") {
					t.Fatalf("429 accessed target/mutated: %s", statement)
				}
			}
		})
	}
}

func TestRatedRunExplanationRefillAndLogicalExpiryUseTransactionClock(t *testing.T) {
	store, f, auth, run := ratedReadFixture(t, false)
	start := f.rows["clock"][0].(time.Time)
	for i := 0; i < 2; i++ {
		value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, fmt.Sprintf("fill-%d", i), run.ID)
		assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
	}
	// Fractional remaining time is rounded up; denied reads do not renew TTL.
	f.rows["clock"] = []driver.Value{start.Add(4*time.Second + time.Nanosecond)}
	value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "not-yet", run.ID)
	assertRatedOutcome(t, value, err, ports.RunExplanationReadRateLimitedV1)
	if value.RetryAfter != time.Second {
		t.Fatalf("fractional RetryAfter=%v want=1s", value.RetryAfter)
	}
	f.rows["clock"] = []driver.Value{start.Add(5 * time.Second)}
	value, err = store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "at-refill", run.ID)
	assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
	charged := ratedFixtureSlot(t, f)
	if !charged.LastDebitAt.Equal(start.Add(5 * time.Second)) {
		t.Fatalf("debit time=%v want sampled=%v", charged.LastDebitAt, start.Add(5*time.Second))
	}
	// Keep the expired physical row present, as optional YDB TTL need not have
	// removed it. Logical expiry alone safely replaces it with a fresh burst.
	f.rows["clock"] = []driver.Value{charged.ExpiresAt}
	value, err = store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "after-logical-expiry", run.ID)
	assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
	refilled := ratedFixtureSlot(t, f)
	if len(refilled.Receipts) != 1 || !refilled.TheoreticalArrivalAt.Equal(charged.ExpiresAt.Add(5*time.Second)) || !refilled.ExpiresAt.Equal(charged.ExpiresAt.Add(10*time.Minute)) {
		t.Fatalf("logical expiry replacement=%+v want one new receipt, TAT=now+5s, expiry=now+10m", refilled)
	}
}

func TestRatedRunExplanationReplayIsNotCachedParticipantAuthorization(t *testing.T) {
	store, f, auth, run := ratedReadFixture(t, false)
	value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "stable-request", run.ID)
	assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
	delete(f.rows, "session_participants")
	value, err = store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "stable-request", run.ID)
	assertRatedOutcome(t, value, err, ports.RunExplanationReadNotFoundV1)
	if f.writes != 1 || f.commits != 2 {
		t.Fatalf("participant loss replay writes=%d commits=%d want=1,2", f.writes, f.commits)
	}
}

// Find real overlapping hash probes, bounded and deterministic. Each stored
// occupant's own identity must legitimately select the physical key; arbitrary
// foreign hashes at a key would be corruption, not capacity contention.
func ratedFixtureCollisions(t *testing.T, f *ratedSQLFixture, auth ports.RunExplanationAuthorizationV1, runID domain.RunID) [runexplanationrate.ProbeCount]uint32 {
	t.Helper()
	_, candidates, err := runexplanationrate.IdentityKeys(auth.TenantID, auth.UserID)
	if err != nil {
		t.Fatalf("derive caller probes: %v", err)
	}
	now := f.rows["clock"][0].(time.Time)
	selector, err := runexplanationrate.SelectorDigest(runID)
	if err != nil {
		t.Fatalf("derive collision selector: %v", err)
	}
	for n := 0; n < 65536 && len(f.slots) < len(candidates); n++ {
		identity, occupantKeys, err := runexplanationrate.IdentityKeys(auth.TenantID, domain.UserID(fmt.Sprintf("collision-occupant-%d", n)))
		if err != nil {
			t.Fatalf("derive collision identity n=%d: %v", n, err)
		}
		for _, key := range candidates {
			if _, filled := f.slots[key]; filled {
				continue
			}
			matches := false
			for _, occupantKey := range occupantKeys {
				matches = matches || occupantKey == key
			}
			if !matches {
				continue
			}
			slot := runexplanationrate.Slot{Version: runexplanationrate.Version, IdentityDigest: identity, TheoreticalArrivalAt: now.Add(5 * time.Second), LastDebitAt: now, ExpiresAt: now.Add(10 * time.Minute), Receipts: []runexplanationrate.Receipt{{RequestID: fmt.Sprintf("occupant-request-%d", n), SelectorDigest: selector, DebitedAt: now}}}
			encoded, err := runexplanationrate.EncodeSlot(slot)
			if err != nil {
				t.Fatalf("encode legitimate collision key=%d: %v", key, err)
			}
			f.slots[key] = []driver.Value{string(encoded), slot.ExpiresAt}
			// Use each occupant once, so the fixture does not invent duplicate
			// live records for one identity within the caller's four probes.
			break
		}
	}
	if len(f.slots) != len(candidates) {
		t.Fatalf("bounded collision search filled=%d want=%d after65536 identities", len(f.slots), len(candidates))
	}
	return candidates
}

func TestRatedRunExplanationGenuineCollisionDenialAndExpiredSlotReuse(t *testing.T) {
	store, f, auth, run := ratedReadFixture(t, false)
	keys := ratedFixtureCollisions(t, f, auth, run.ID)
	before, _ := json.Marshal(f.slots)
	value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "collision-denied", run.ID)
	assertRatedOutcome(t, value, err, ports.RunExplanationReadRateLimitedV1)
	after, _ := json.Marshal(f.slots)
	if value.RetryAfter != 5*time.Second || f.writes != 0 || f.commits != 1 || len(f.statements) != 7 || string(before) != string(after) {
		t.Fatalf("collision RetryAfter=%v writes=%d commits=%d statements=%d changed=%t want5s,0,1,7,false", value.RetryAfter, f.writes, f.commits, len(f.statements), string(before) != string(after))
	}
	expires := f.slots[keys[0]][1].(time.Time)
	f.rows["clock"] = []driver.Value{expires}
	value, err = store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "reuse-expired-collision", run.ID)
	assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
	identity, _, _ := runexplanationrate.IdentityKeys(auth.TenantID, auth.UserID)
	replacement, err := runexplanationrate.DecodeSlot([]byte(f.slots[keys[0]][0].(string)))
	if err != nil || replacement.IdentityDigest != identity || f.writes != 1 || len(f.slots) != 4 {
		t.Fatalf("expired replacement=%+v err=%v writes=%d physicalSlots=%d want own firstkey,1,4", replacement, err, f.writes, len(f.slots))
	}
}

func TestRatedRunExplanationChecksAllProbesBeforeReplay(t *testing.T) {
	store, f, auth, run := ratedReadFixture(t, false)
	value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "stable-request", run.ID)
	assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
	_, keys, _ := runexplanationrate.IdentityKeys(auth.TenantID, auth.UserID)
	f.slots[keys[3]] = []driver.Value{"{", f.rows["clock"][0].(time.Time)}
	start := len(f.statements)
	value, err = store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "stable-request", run.ID)
	if !errors.Is(err, ports.ErrRunExplanationUnavailable) || value.Kind != "" || len(f.statements)-start != 7 || f.writes != 1 || f.commits != 1 {
		t.Fatalf("corrupt fourth probe replay outcome=%+v err=%v statements=%d writes=%d commits=%d want unavailable,7,1,1", value, err, len(f.statements)-start, f.writes, f.commits)
	}
}

func TestRatedRunExplanationReplayReauthorizesAndMatchesSelector(t *testing.T) {
	store, f, auth, run := ratedReadFixture(t, false)
	value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "request-one", run.ID)
	assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
	first := ratedFixtureSlot(t, f)
	value, err = store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "request-one", run.ID)
	assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
	second := ratedFixtureSlot(t, f)
	if f.writes != 1 || !first.TheoreticalArrivalAt.Equal(second.TheoreticalArrivalAt) || len(second.Receipts) != 1 {
		t.Fatalf("replay debited: writes=%d slot=%+v", f.writes, second)
	}
	value, err = store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "request-one", "different-run")
	if !errors.Is(err, ports.ErrRunExplanationUnavailable) || value.Kind != "" || f.writes != 1 {
		t.Fatalf("selector mismatch outcome=%+v err=%v writes=%d", value, err, f.writes)
	}
	delete(f.rows, "tenant_memberships")
	value, err = store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "request-one", run.ID)
	if !errors.Is(err, domain.ErrMembershipDenied) || value.Kind != "" || f.writes != 1 {
		t.Fatalf("replay after READ loss outcome=%+v err=%v writes=%d", value, err, f.writes)
	}
}

func TestRatedRunExplanationCurrentAuthDominatesRateDenial(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*testing.T, *ratedSQLFixture)
		want   error
	}{
		{name: "missing session", change: func(_ *testing.T, f *ratedSQLFixture) { delete(f.rows, "web_sessions") }, want: domain.ErrWebSessionRevoked},
		{name: "digest mismatch", change: func(t *testing.T, f *ratedSQLFixture) {
			s := ratedFixtureRecord[domain.WebSession](t, f, "web_sessions")
			s.SessionDigest = domain.DigestSecret("other")
			ratedFixturePut(t, f, "web_sessions", s)
		}, want: domain.ErrWebSessionRevoked},
		{name: "revoked before absent membership", change: func(t *testing.T, f *ratedSQLFixture) {
			s := ratedFixtureRecord[domain.WebSession](t, f, "web_sessions")
			at := f.rows["clock"][0].(time.Time)
			s.RevokedAt = &at
			ratedFixturePut(t, f, "web_sessions", s)
			delete(f.rows, "tenant_memberships")
		}, want: domain.ErrWebSessionRevoked},
		{name: "expired before absent membership", change: func(t *testing.T, f *ratedSQLFixture) {
			s := ratedFixtureRecord[domain.WebSession](t, f, "web_sessions")
			s.IdleExpiresAt = f.rows["clock"][0].(time.Time)
			ratedFixturePut(t, f, "web_sessions", s)
			delete(f.rows, "tenant_memberships")
		}, want: domain.ErrWebSessionExpired},
		{name: "membership missing", change: func(_ *testing.T, f *ratedSQLFixture) { delete(f.rows, "tenant_memberships") }, want: domain.ErrMembershipDenied},
		{name: "membership foreign tenant", change: func(t *testing.T, f *ratedSQLFixture) {
			m := ratedFixtureRecord[domain.TenantMembership](t, f, "tenant_memberships")
			m.TenantID = "other-tenant"
			ratedFixturePut(t, f, "tenant_memberships", m)
		}, want: domain.ErrMembershipDenied},
		{name: "READ preserving version bump", change: func(t *testing.T, f *ratedSQLFixture) {
			m := ratedFixtureRecord[domain.TenantMembership](t, f, "tenant_memberships")
			m.SecurityVersion++
			ratedFixturePut(t, f, "tenant_memberships", m)
		}, want: domain.ErrMembershipVersionChanged},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, f, auth, run := ratedReadFixture(t, false)
			for i := 0; i < 2; i++ {
				value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, fmt.Sprintf("fill-%d", i), run.ID)
				assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
			}
			test.change(t, f)
			start := len(f.statements)
			value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "denied-request", run.ID)
			if !errors.Is(err, test.want) || value.Kind != "" || f.commits != 2 || f.writes != 2 {
				t.Fatalf("auth-before-429 outcome=%+v err=%v want=%v commits=%d writes=%d", value, err, test.want, f.commits, f.writes)
			}
			for _, statement := range f.statements[start:] {
				if strings.Contains(statement, "run_explanation_rate_slots_v1") || strings.Contains(statement, "FROM runs ") {
					t.Fatalf("rate/target read after auth failed: %s", statement)
				}
			}
		})
	}
}

func TestRatedRunExplanationSDKCommitRetriesAndAggregateBudget(t *testing.T) {
	for _, test := range []struct {
		name                                     string
		attached                                 bool
		modes                                    []string
		wantKind                                 ports.RunExplanationReadOutcomeKindV1
		wantStatements, wantWrites, wantReceipts int
		unavailable                              bool
	}{
		{name: "known rollback retries not-found", modes: []string{"rollback"}, wantKind: ports.RunExplanationReadNotFoundV1, wantStatements: 18, wantWrites: 2, wantReceipts: 1},
		{name: "committed lost ACK replays not-found", modes: []string{"ambiguous"}, wantKind: ports.RunExplanationReadNotFoundV1, wantStatements: 17, wantWrites: 1, wantReceipts: 1},
		{name: "unknown commit cannot publish", modes: []string{"unknown"}, wantStatements: 12, wantWrites: 1, wantReceipts: 1, unavailable: true},
		{name: "historical retry exhausts before21", modes: []string{"rollback"}, wantStatements: 20, wantWrites: 2, unavailable: true},
		{name: "attached lost ACK exhausts before21", attached: true, modes: []string{"ambiguous"}, wantStatements: 20, wantWrites: 1, wantReceipts: 1, unavailable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, f, auth, run := ratedReadFixture(t, test.attached)
			if test.wantKind == ports.RunExplanationReadNotFoundV1 {
				delete(f.rows, "runs")
			}
			f.commitModes = append([]string(nil), test.modes...)
			value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "stable-request", run.ID)
			if test.unavailable {
				if !errors.Is(err, ports.ErrRunExplanationUnavailable) || value.Kind != "" {
					t.Fatalf("unresolved outcome=%+v err=%v want empty unavailable", value, err)
				}
			} else {
				assertRatedOutcome(t, value, err, test.wantKind)
			}
			if len(f.statements) != test.wantStatements || f.writes != test.wantWrites {
				t.Fatalf("retry statements=%d writes=%d commits=%d want=%d,%d", len(f.statements), f.writes, f.commits, test.wantStatements, test.wantWrites)
			}
			if test.wantReceipts > 0 {
				if got := len(ratedFixtureSlot(t, f).Receipts); got != test.wantReceipts {
					t.Fatalf("receipts=%d want=%d", got, test.wantReceipts)
				}
			} else if len(f.slots) != 0 {
				t.Fatalf("rolled-back slots=%v want empty", f.slots)
			}
		})
	}
}

func TestRatedRunExplanationAbortedResourceAndStaleRetryOutcome(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("auth_changes_on_retry_%t", afterCommit), func(t *testing.T) {
			store, f, auth, run := ratedReadFixture(t, false)
			if afterCommit {
				f.commitModes = []string{"ambiguous"}
				f.afterCommit = func() { delete(f.rows, "tenant_memberships") }
			} else {
				f.failTable = "sessions"
			}
			value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "stable-request", run.ID)
			want := ports.ErrRunExplanationUnavailable
			if afterCommit {
				want = domain.ErrMembershipDenied
			}
			if !errors.Is(err, want) || value.Kind != "" {
				t.Fatalf("aborted/stale outcome=%+v err=%v want=%v", value, err, want)
			}
			if !afterCommit && (len(f.slots) != 0 || f.rollbacks != 1) {
				t.Fatalf("known abort slots=%v rollbacks=%d want empty,1", f.slots, f.rollbacks)
			}
		})
	}
}

func TestRatedRunExplanationCorruptExpiryAndEncodedRowsFailClosed(t *testing.T) {
	for _, test := range []string{"expiry mismatch", "oversize", "malformed", "unsupported expired"} {
		t.Run(test, func(t *testing.T) {
			store, f, auth, run := ratedReadFixture(t, false)
			value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "seed", run.ID)
			assertRatedOutcome(t, value, err, ports.RunExplanationReadSuccessV1)
			for key, row := range f.slots {
				switch test {
				case "expiry mismatch":
					row[1] = row[1].(time.Time).Add(time.Second)
				case "oversize":
					row[0] = strings.Repeat("x", runexplanationrate.MaxRowBytes+1)
				case "malformed":
					row[0] = "{"
				case "unsupported expired":
					slot := ratedFixtureSlot(t, f)
					slot.Version++
					encoded, _ := json.Marshal(slot)
					row[0] = string(encoded)
					f.rows["clock"] = []driver.Value{slot.ExpiresAt}
				}
				f.slots[key] = row
			}
			start := len(f.statements)
			value, err = store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "read-corrupt", run.ID)
			if !errors.Is(err, ports.ErrRunExplanationUnavailable) || value.Kind != "" || f.writes != 1 || f.commits != 1 {
				t.Fatalf("corrupt row=%s outcome=%+v err=%v writes=%d commits=%d", test, value, err, f.writes, f.commits)
			}
			for _, statement := range f.statements[start:] {
				if strings.Contains(statement, "FROM runs ") {
					t.Fatalf("target accessed with corrupt rate state: %s", statement)
				}
			}
		})
	}
}

func TestRatedRunExplanationInvalidInputsAndCancelledContextDoNotAccessDatabase(t *testing.T) {
	for _, test := range []struct {
		name      string
		digest    domain.SecretDigest
		request   string
		run       domain.RunID
		cancelled bool
	}{
		{name: "invalid digest", digest: "bad", request: "request", run: "run"},
		{name: "empty request", digest: domain.DigestSecret("session-test"), run: "run"},
		{name: "overlong request", digest: domain.DigestSecret("session-test"), request: strings.Repeat("x", 161), run: "run"},
		{name: "invalid run", digest: domain.DigestSecret("session-test"), request: "request"},
		{name: "cancelled outer context", digest: domain.DigestSecret("session-test"), request: "request", run: "run", cancelled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, f, _, _ := ratedReadFixture(t, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.cancelled {
				cancel()
			}
			value, err := store.ReadRatedRunExplanationV1(ctx, test.digest, test.request, test.run)
			if !errors.Is(err, ports.ErrRunExplanationUnavailable) || value.Kind != "" || len(f.statements) != 0 || len(f.isolations) != 0 {
				t.Fatalf("input outcome=%+v err=%v statements=%d transactions=%d want unavailable,no DB", value, err, len(f.statements), len(f.isolations))
			}
		})
	}
}
