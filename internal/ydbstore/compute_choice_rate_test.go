package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanationrate"
)

// This fault connector models atomic metadata and commit ACK loss, not native
// YDB serializability. Only the compute table's four exact finite keys may be
// read and only its validated debit may be written. Explanation SQL is denied.
type computeChoiceRateFixture struct {
	*computeChoiceSQLFixture
	keys [runexplanationrate.ProbeCount]uint32
}

func (fixture *computeChoiceRateFixture) Connect(context.Context) (driver.Conn, error) {
	return fixture, nil
}

func (fixture *computeChoiceRateFixture) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if statement != computeChoiceRateReadSQL {
		return fixture.computeChoiceSQLFixture.QueryContext(ctx, statement, args)
	}
	if err := fixture.statement(ctx, statement, args); err != nil {
		return nil, err
	}
	if len(args) != 1 {
		return nil, errors.New("fixture requires exactly one finite PK")
	}
	key, err := ratedFixtureKey(args[0].Value)
	if err != nil {
		return nil, err
	}
	found := false
	for _, expected := range fixture.keys {
		found = found || expected == key
	}
	if !found {
		return nil, errors.New("fixture forbids other owners' finite slots")
	}
	return &explanationFixtureRows{values: fixture.slots[key]}, nil
}

func (fixture *computeChoiceRateFixture) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if err := fixture.statement(ctx, statement, args); err != nil {
		return nil, err
	}
	if statement != computeChoiceRateWriteSQL || len(args) != 3 || fixture.pending == nil {
		return nil, errors.New("fixture forbids non-compute-rate mutation")
	}
	key, err := ratedFixtureKey(args[0].Value)
	if err != nil {
		return nil, err
	}
	encoded, ok := args[1].Value.(string)
	if !ok {
		return nil, errors.New("fixture requires bounded slot JSON")
	}
	expires, ok := args[2].Value.(time.Time)
	slot, decodeErr := runexplanationrate.DecodeSlot([]byte(encoded))
	if !ok || decodeErr != nil || !slot.ExpiresAt.Equal(expires) {
		return nil, errors.New("fixture rejects corrupt rate write")
	}
	found := false
	for _, expected := range fixture.keys {
		found = found || expected == key
	}
	if !found {
		return nil, errors.New("fixture rejects foreign rate write")
	}
	fixture.writes++
	fixture.pending[key] = []driver.Value{encoded, expires}
	return driver.RowsAffected(1), nil
}

func newComputeChoiceRateFixture(t *testing.T) (*computeChoiceSourceFixture, *computeChoiceRateFixture) {
	t.Helper()
	f := newComputeChoiceSourceFixture(t)
	_, keys, err := computechoice.ListRateIdentityKeys(f.cookie.ActiveTenantID, f.cookie.UserID)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &computeChoiceRateFixture{computeChoiceSQLFixture: f.driver, keys: keys}
	db := sql.OpenDB(fixture)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store = &Store{db: db}
	return f, fixture
}

func computeChoiceRateInput() computechoice.ListQuery {
	return computechoice.ListQuery{Limit: 4, InputKind: computechoice.InputText}
}

// Test-only whole-operation wrapper deliberately reuses production readers and
// authTx. Any candidate failure rolls the speculative debit back. The real page
// integration must preserve these same current-auth/reset/budget guarantees.
func (f *computeChoiceSourceFixture) readRated(input computechoice.ListQuery, requestID string, budget *computechoice.ResourceBudget, candidate func(*computeChoiceQuery) error) (runexplanationrate.Decision, error) {
	if input.Validate() != nil || domain.ValidateOpaqueID("request_id", requestID) != nil {
		return runexplanationrate.Decision{}, errComputeChoiceSource
	}
	ctx, cancel := context.WithTimeout(context.Background(), computechoice.ResourceDeadline)
	defer cancel()
	var pending runexplanationrate.Decision
	err := f.store.authTx(ctx, "compute_choice.rated_fixture", func(tx *sql.Tx) error {
		pending = runexplanationrate.Decision{}
		attempt, err := budget.NewAttempt()
		if err != nil {
			return err
		}
		defer attempt.Finish()
		query := &computeChoiceQuery{tx: tx, budget: attempt}
		auth, err := readComputeChoiceAuth(ctx, query, f.cookie.SessionDigest, f.session.ID)
		if err != nil {
			return err
		}
		if err := readComputeChoiceCutover(ctx, query, f.pins, auth.at); err != nil {
			return err
		}
		decision, err := readComputeChoiceListRate(ctx, query, auth, input, requestID)
		if err != nil {
			return err
		}
		if decision.Allowed && candidate != nil {
			if err := candidate(query); err != nil {
				return err
			}
		}
		pending = decision
		return nil
	})
	if err != nil {
		return runexplanationrate.Decision{}, err
	}
	return pending, nil
}

func TestComputeChoiceRateSameTransactionReplayAndDenial(t *testing.T) {
	f, driver := newComputeChoiceRateFixture(t)
	input := computeChoiceRateInput()
	budget := computechoice.NewResourceBudget()
	got, err := f.readRated(input, "list-first", budget, nil)
	if err != nil || !got.Allowed || !got.Debited {
		t.Fatalf("first read: allowed=%v debit=%v error=%v", got.Allowed, got.Debited, err)
	}
	if snapshot := budget.Snapshot(); snapshot.TotalStatements != 11 || snapshot.Active || snapshot.Failed {
		t.Fatalf("all six auth/cutover + four probes + debit must count: %+v", snapshot)
	}
	if driver.deadlineMissing || len(driver.isolations) != 1 || driver.isolations[0] != driverpkgSerializable() || driver.writes != 1 || driver.commits != 1 {
		t.Fatalf("same Serializable deadline/debit invariant: %+v", driver.ratedSQLFixture)
	}
	got, err = f.readRated(input, "list-first", computechoice.NewResourceBudget(), nil)
	if err != nil || !got.Allowed || got.Debited || driver.writes != 1 {
		t.Fatalf("exact replay spent capacity: allowed=%v debit=%v writes=%d error=%v", got.Allowed, got.Debited, driver.writes, err)
	}
	// A new cookie and a different Session cannot mint a tenant/user rate bucket.
	f.cookie.SessionDigest = domain.SecretDigest(strings.Repeat("f", 64))
	f.session.ID = "another-session"
	f.participant.SessionID = f.session.ID
	f.syncRows(t)
	got, err = f.readRated(input, "list-second", computechoice.NewResourceBudget(), nil)
	if err != nil || !got.Allowed || !got.Debited {
		t.Fatalf("second burst: allowed=%v debit=%v error=%v", got.Allowed, got.Debited, err)
	}
	budget = computechoice.NewResourceBudget()
	called := false
	got, err = f.readRated(input, "list-third", budget, func(*computeChoiceQuery) error { called = true; return nil })
	if err != nil || got.Allowed || got.Debited || got.RetryAfter != 5*time.Second || called || driver.writes != 2 || budget.Snapshot().TotalStatements != 10 {
		t.Fatalf("rate denial must read no candidates/write no debit: allowed=%v debit=%v retry=%v called=%v writes=%d budget=%+v err=%v", got.Allowed, got.Debited, got.RetryAfter, called, driver.writes, budget.Snapshot(), err)
	}
}

func driverpkgSerializable() driver.IsolationLevel {
	return driver.IsolationLevel(sql.LevelSerializable)
}

func TestComputeChoiceRateFreshAuthorityBeforeAnyMetadata(t *testing.T) {
	for _, denied := range []string{"revoked", "viewer", "cutover"} {
		t.Run(denied, func(t *testing.T) {
			f, driver := newComputeChoiceRateFixture(t)
			switch denied {
			case "revoked":
				at := f.at
				f.cookie.RevokedAt = &at
			case "viewer":
				f.member.Role = domain.TenantMembershipViewer
			case "cutover":
				f.cutover.Enabled = false
			}
			f.syncRows(t)
			got, err := f.readRated(computeChoiceRateInput(), "denied", computechoice.NewResourceBudget(), nil)
			if err == nil || !reflect.DeepEqual(got, runexplanationrate.Decision{}) || driver.writes != 0 || len(driver.slots) != 0 {
				t.Fatalf("denied auth must have no speculative data/debit: got=%+v writes=%d err=%v", got, driver.writes, err)
			}
			for _, statement := range driver.statements {
				if statement == computeChoiceRateReadSQL || statement == computeChoiceRateWriteSQL {
					t.Fatal("rate metadata touched before current authorization/cutover")
				}
			}
		})
	}
}

func TestComputeChoiceRateChangedRetryContextDenied(t *testing.T) {
	for _, changed := range []string{"limit", "input", "Session", "membership"} {
		t.Run(changed, func(t *testing.T) {
			f, driver := newComputeChoiceRateFixture(t)
			input := computeChoiceRateInput()
			if _, err := f.readRated(input, "same-server-request", computechoice.NewResourceBudget(), nil); err != nil {
				t.Fatal(err)
			}
			switch changed {
			case "limit":
				input.Limit = 1
			case "input":
				input.InputKind = computechoice.InputImage
			case "Session":
				f.session.ID = "other-session"
				f.participant.SessionID = f.session.ID
			case "membership":
				f.member.SecurityVersion++
				f.cookie.MembershipSecurityVersion++
			}
			f.syncRows(t)
			got, err := f.readRated(input, "same-server-request", computechoice.NewResourceBudget(), nil)
			if !errors.Is(err, errComputeChoiceSource) || !reflect.DeepEqual(got, runexplanationrate.Decision{}) || driver.writes != 1 {
				t.Fatalf("changed %s escaped receipt sealing: got=%+v writes=%d error=%v", changed, got, driver.writes, err)
			}
		})
	}
}

func TestComputeChoiceRateRollbackLostAckAndUnresolvedCommit(t *testing.T) {
	for _, mode := range []string{"rollback", "ambiguous", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			f, driver := newComputeChoiceRateFixture(t)
			driver.commitModes = []string{mode}
			budget := computechoice.NewResourceBudget()
			got, err := f.readRated(computeChoiceRateInput(), "commit-request", budget, nil)
			if mode == "unknown" {
				if err == nil || !reflect.DeepEqual(got, runexplanationrate.Decision{}) {
					t.Fatalf("unknown commit published speculative rate/page: got=%+v err=%v", got, err)
				}
				got, err = f.readRated(computeChoiceRateInput(), "commit-request", computechoice.NewResourceBudget(), nil)
				if err != nil || !got.Allowed || got.Debited || driver.writes != 1 {
					t.Fatalf("confirmed exact-key recovery must not refund/redebit: got=%+v writes=%d err=%v", got, driver.writes, err)
				}
				return
			}
			wantWrites, wantSQL := 2, uint32(22)
			if mode == "ambiguous" {
				wantWrites, wantSQL = 1, 21
			}
			if err != nil || !got.Allowed || driver.writes != wantWrites || driver.commits != 2 || budget.Snapshot().TotalStatements != wantSQL || len(driver.slots) != 1 {
				t.Fatalf("commit=%s replay/reset aggregate: got=%+v writes=%d commits=%d slots=%d budget=%+v error=%v", mode, got, driver.writes, driver.commits, len(driver.slots), budget.Snapshot(), err)
			}
		})
	}
}

func TestComputeChoiceRateRetryRevocationAndResourceFailure(t *testing.T) {
	f, driver := newComputeChoiceRateFixture(t)
	driver.commitModes = []string{"ambiguous"}
	driver.afterCommit = func() {
		at := f.at
		f.cookie.RevokedAt = &at
		f.syncRows(t)
	}
	got, err := f.readRated(computeChoiceRateInput(), "revoked-on-retry", computechoice.NewResourceBudget(), nil)
	if !errors.Is(err, domain.ErrWebSessionRevoked) || !reflect.DeepEqual(got, runexplanationrate.Decision{}) || driver.writes != 1 {
		t.Fatalf("live revocation must defeat committed debit replay: got=%+v writes=%d error=%v", got, driver.writes, err)
	}
	f, driver = newComputeChoiceRateFixture(t)
	got, err = f.readRated(computeChoiceRateInput(), "candidate-failed", computechoice.NewResourceBudget(), func(*computeChoiceQuery) error { return errComputeChoiceSource })
	if !errors.Is(err, errComputeChoiceSource) || !reflect.DeepEqual(got, runexplanationrate.Decision{}) || len(driver.slots) != 0 || driver.commits != 0 || driver.rollbacks != 1 {
		t.Fatalf("failed candidate must atomically roll back debit: got=%+v slots=%d commits=%d rollback=%d error=%v", got, len(driver.slots), driver.commits, driver.rollbacks, err)
	}
}

func TestComputeChoiceRateCorruptionAndAggregateBudget(t *testing.T) {
	for _, corrupt := range []string{"oversize", "scalar expiry", "expired malformed"} {
		t.Run(corrupt, func(t *testing.T) {
			f, driver := newComputeChoiceRateFixture(t)
			got, err := f.readRated(computeChoiceRateInput(), "first", computechoice.NewResourceBudget(), nil)
			if err != nil {
				t.Fatal(err)
			}
			row := driver.slots[got.SlotID]
			want := errComputeChoiceSource
			switch corrupt {
			case "oversize":
				row[0] = strings.Repeat("x", 8193)
				want = computechoice.ErrResourceBudgetExceeded
			case "scalar expiry":
				row[1] = f.at
			case "expired malformed":
				row[0], row[1] = "{}", f.at.Add(-time.Hour)
			}
			budget := computechoice.NewResourceBudget()
			got, err = f.readRated(computeChoiceRateInput(), "after-corruption", budget, nil)
			if !errors.Is(err, want) || !reflect.DeepEqual(got, runexplanationrate.Decision{}) || driver.writes != 1 || budget.Snapshot().TotalBytes == 0 {
				t.Fatalf("corrupt %s must count prefix then deny: got=%+v writes=%d budget=%+v error=%v", corrupt, got, driver.writes, budget.Snapshot(), err)
			}
		})
	}
	f, driver := newComputeChoiceRateFixture(t)
	driver.commitModes = []string{"rollback", "rollback", "rollback", "rollback", "rollback", "rollback", "rollback", "rollback", "rollback"}
	budget := computechoice.NewResourceBudget()
	got, err := f.readRated(computeChoiceRateInput(), "budget-retries", budget, nil)
	if !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || !reflect.DeepEqual(got, runexplanationrate.Decision{}) || budget.Snapshot().TotalStatements != computechoice.MaxStatementsTotal || len(driver.slots) != 0 || len(driver.statements) != int(computechoice.MaxStatementsTotal) {
		t.Fatalf("retry metadata cannot erase cost or exceed finite SQL: got=%+v driverSQL=%d slots=%d budget=%+v error=%v", got, len(driver.statements), len(driver.slots), budget.Snapshot(), err)
	}
}
