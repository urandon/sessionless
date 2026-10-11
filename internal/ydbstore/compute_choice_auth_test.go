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

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Exercises the actual authTx SDK retry callback and exact readers. This SQL
// fault connector is NOT proof of native YDB isolation/query plans/rollout.
type computeChoiceSQLFixture struct {
	*ratedSQLFixture
	expected map[string][]driver.Value
}

func (fixture *computeChoiceSQLFixture) Connect(context.Context) (driver.Conn, error) {
	return fixture, nil
}
func (fixture *computeChoiceSQLFixture) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if err := fixture.statement(ctx, statement, args); err != nil {
		return nil, err
	}
	expected, allowed := fixture.expected[statement]
	if !allowed {
		return nil, errors.New("fixture forbids non-manifest query")
	}
	values := make([]driver.Value, len(args))
	for i := range args {
		values[i] = args[i].Value
	}
	if !reflect.DeepEqual(expected, values) {
		return nil, errors.New("fixture rejects caller/foreign key")
	}
	return &explanationFixtureRows{values: fixture.rows[statement]}, nil
}
func (*computeChoiceSQLFixture) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("fixture forbids auth/cutover mutation")
}

type computeChoiceSourceFixture struct {
	store       *Store
	driver      *computeChoiceSQLFixture
	at          time.Time
	cookie      domain.WebSession
	member      domain.TenantMembership
	session     domain.Session
	participant domain.SessionParticipant
	cutover     computeChoiceCutoverRecord
	pins        computeChoiceCutoverPins
}

func newComputeChoiceSourceFixture(t *testing.T) *computeChoiceSourceFixture {
	t.Helper()
	at := time.Date(2026, 10, 10, 19, 0, 0, 123456000, time.UTC)
	f := &computeChoiceSourceFixture{at: at, driver: &computeChoiceSQLFixture{ratedSQLFixture: &ratedSQLFixture{rows: make(map[string][]driver.Value), slots: make(map[uint32][]driver.Value)}, expected: make(map[string][]driver.Value)}}
	f.cookie = domain.WebSession{SessionDigest: domain.SecretDigest(strings.Repeat("a", 64)), CSRFTokenDigest: domain.SecretDigest(strings.Repeat("b", 64)), UserID: "choice-user", ActiveTenantID: "choice-tenant", AuthenticatedSubject: domain.ExternalSubject{Provider: "yandex", Subject: "choice-subject"}, MembershipSecurityVersion: 7, IssuedAt: at.Add(-time.Hour), LastSeenAt: at.Add(-time.Minute), IdleExpiresAt: at.Add(time.Hour), AbsoluteExpiresAt: at.Add(24 * time.Hour)}
	f.member = domain.TenantMembership{TenantID: f.cookie.ActiveTenantID, UserID: f.cookie.UserID, Role: domain.TenantMembershipMember, Status: domain.TenantMembershipActive, SecurityVersion: 7, CreatedAt: at.Add(-time.Hour), UpdatedAt: at.Add(-time.Minute)}
	f.session = domain.Session{ID: "choice-session", TenantID: f.cookie.ActiveTenantID, CreatedBy: f.cookie.UserID, Status: domain.SessionActive, LastEventSequence: 41, CreatedAt: at.Add(-time.Hour), UpdatedAt: at.Add(-time.Minute)}
	f.participant = domain.SessionParticipant{TenantID: f.cookie.ActiveTenantID, SessionID: f.session.ID, UserID: f.cookie.UserID, Role: domain.SessionParticipantMember, Status: domain.SessionParticipantActive, CreatedAt: at.Add(-time.Hour), UpdatedAt: at.Add(-time.Minute)}
	f.pins = computeChoiceCutoverPins{manifestDigest: strings.Repeat("c", 64), writerCommit: strings.Repeat("d", 40)}
	f.cutover = computeChoiceCutoverRecord{Version: 1, ManifestDigest: f.pins.manifestDigest, WriterRevision: 1, SchemaRevision: 1, Enabled: true, WriterCommit: f.pins.writerCommit, DrainedInventoryDigest: strings.Repeat("e", 64), CompletedAt: at.Add(-time.Hour)}
	f.syncRows(t)
	db := sql.OpenDB(f.driver)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store = &Store{db: db}
	return f
}

func choiceFixtureJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func (f *computeChoiceSourceFixture) syncRows(t *testing.T) {
	t.Helper()
	bucket, err := webBucket(string(f.cookie.SessionDigest))
	if err != nil {
		t.Fatal(err)
	}
	userBucket, err := webBucket(string(f.cookie.UserID))
	if err != nil {
		t.Fatal(err)
	}
	var revoked driver.Value = time.Unix(0, 0)
	if f.cookie.RevokedAt != nil {
		revoked = f.cookie.RevokedAt.UTC().Truncate(time.Microsecond)
	}
	var archived driver.Value
	if f.session.ArchivedAt != nil {
		archived = f.session.ArchivedAt.UTC().Truncate(time.Microsecond)
	}
	f.driver.rows = map[string][]driver.Value{
		`SELECT CurrentUtcTimestamp()`: {f.at},
		computeChoiceCookieSQL:         {choiceFixtureJSON(t, f.cookie), string(f.cookie.UserID), string(f.cookie.ActiveTenantID), string(f.cookie.CSRFTokenDigest), int64(f.cookie.MembershipSecurityVersion), f.cookie.IdleExpiresAt.UTC().Truncate(time.Microsecond), f.cookie.AbsoluteExpiresAt.UTC().Truncate(time.Microsecond), revoked},
		computeChoiceMembershipSQL:     {choiceFixtureJSON(t, f.member), string(f.member.Role), string(f.member.Status), int64(f.member.SecurityVersion)},
		computeChoiceSessionSQL:        {choiceFixtureJSON(t, f.session), string(f.session.CreatedBy), string(f.session.Status), int64(f.session.LastEventSequence), archived},
		computeChoiceParticipantSQL:    {choiceFixtureJSON(t, f.participant), string(f.participant.Role), string(f.participant.Status)},
		computeChoiceCutoverSQL:        {choiceFixtureJSON(t, f.cutover), int64(f.cutover.Version), f.cutover.ManifestDigest, int64(f.cutover.WriterRevision), int64(f.cutover.SchemaRevision), f.cutover.Enabled, f.cutover.WriterCommit, f.cutover.DrainedInventoryDigest, int64(f.cutover.OldWriterCount), f.cutover.CompletedAt.UTC().Truncate(time.Microsecond)},
	}
	f.driver.expected = map[string][]driver.Value{
		`SELECT CurrentUtcTimestamp()`: {},
		computeChoiceCookieSQL:         {int64(bucket), string(f.cookie.SessionDigest)},
		computeChoiceMembershipSQL:     {int64(userBucket), string(f.cookie.UserID), string(f.cookie.ActiveTenantID)},
		computeChoiceSessionSQL:        {string(f.cookie.ActiveTenantID), string(f.session.ID)},
		computeChoiceParticipantSQL:    {string(f.cookie.ActiveTenantID), string(f.session.ID), string(f.cookie.UserID)},
		computeChoiceCutoverSQL:        {"serving"},
	}
}

// Test-only operation wrapper supplies the SAME private query to both readers,
// clears speculative state each retry and publishes only after confirmed tx.
// It does not introduce an exported production preflight permission API.
func (f *computeChoiceSourceFixture) read(budget *computechoice.ResourceBudget) (computeChoiceAuth, error) {
	ctx, cancel := context.WithTimeout(context.Background(), computechoice.ResourceDeadline)
	defer cancel()
	var pending computeChoiceAuth
	err := f.store.authTx(ctx, "compute_choice.reader_fixture", func(tx *sql.Tx) error {
		pending = computeChoiceAuth{}
		attempt, err := budget.NewAttempt()
		if err != nil {
			return err
		}
		defer attempt.Finish()
		query := &computeChoiceQuery{tx: tx, budget: attempt}
		current, err := readComputeChoiceAuth(ctx, query, f.cookie.SessionDigest, f.session.ID)
		if err != nil {
			return err
		}
		if err := readComputeChoiceCutover(ctx, query, f.pins, current.at); err != nil {
			return err
		}
		pending = current
		return nil
	})
	if err != nil {
		return computeChoiceAuth{}, err
	}
	return pending, nil
}

func TestComputeChoiceCurrentWriteReadersUseOneTransaction(t *testing.T) {
	f := newComputeChoiceSourceFixture(t)
	budget := computechoice.NewResourceBudget()
	got, err := f.read(budget)
	if err != nil {
		t.Fatal(err)
	}
	want := computechoice.Scope{TenantID: f.cookie.ActiveTenantID, UserID: f.cookie.UserID, SessionID: f.session.ID, MembershipSecurityVersion: 7}
	if got.scope != want || got.lastEventSequence != 41 || !got.at.Equal(f.at) || !got.authorityExpiresAt.Equal(f.cookie.IdleExpiresAt) {
		t.Fatalf("current scope/clock/prefix got=%+v want=%+v", got, want)
	}
	if f.driver.commits != 1 || f.driver.deadlineMissing || len(f.driver.isolations) != 1 || f.driver.isolations[0] != driver.IsolationLevel(sql.LevelSerializable) {
		t.Fatalf("transaction/deadline evidence: %+v", f.driver.ratedSQLFixture)
	}
	if got := budget.Snapshot(); got.TotalStatements != 6 || got.AttemptStatements != 6 || got.TotalBytes == 0 || got.Active || got.Failed {
		t.Fatalf("shared budget evidence: %+v", got)
	}
	if len(f.driver.statements) != 6 {
		t.Fatalf("point manifest got=%d want=6", len(f.driver.statements))
	}
}

func TestComputeChoiceReadersDenyCurrentAccessAndCorruptSources(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *computeChoiceSourceFixture)
		want   error
	}{
		{"absent cookie", func(_ *testing.T, f *computeChoiceSourceFixture) { delete(f.driver.rows, computeChoiceCookieSQL) }, domain.ErrWebSessionRevoked},
		{"revoked cookie", func(t *testing.T, f *computeChoiceSourceFixture) { at := f.at; f.cookie.RevokedAt = &at; f.syncRows(t) }, domain.ErrWebSessionRevoked},
		{"idle exact expiry", func(t *testing.T, f *computeChoiceSourceFixture) { f.cookie.IdleExpiresAt = f.at; f.syncRows(t) }, domain.ErrWebSessionExpired},
		{"absolute exact expiry", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.cookie.AbsoluteExpiresAt = f.at
			f.cookie.IdleExpiresAt = f.at
			f.syncRows(t)
		}, domain.ErrWebSessionExpired},
		{"membership viewer is not WRITE", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.member.Role = domain.TenantMembershipViewer
			f.syncRows(t)
		}, domain.ErrMembershipDenied},
		{"membership revoked", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.member.Status = domain.TenantMembershipRevoked
			f.syncRows(t)
		}, domain.ErrMembershipDenied},
		{"membership version changed", func(t *testing.T, f *computeChoiceSourceFixture) { f.member.SecurityVersion++; f.syncRows(t) }, domain.ErrMembershipVersionChanged},
		{"absent membership", func(_ *testing.T, f *computeChoiceSourceFixture) { delete(f.driver.rows, computeChoiceMembershipSQL) }, domain.ErrMembershipDenied},
		{"absent Session", func(_ *testing.T, f *computeChoiceSourceFixture) { delete(f.driver.rows, computeChoiceSessionSQL) }, errComputeChoiceSession},
		{"archived Session", func(t *testing.T, f *computeChoiceSourceFixture) {
			at := f.at
			f.session.Status = domain.SessionArchived
			f.session.ArchivedAt = &at
			f.syncRows(t)
		}, errComputeChoiceSession},
		{"owner without participation", func(_ *testing.T, f *computeChoiceSourceFixture) { delete(f.driver.rows, computeChoiceParticipantSQL) }, errComputeChoiceSession},
		{"participant viewer", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.participant.Role = domain.SessionParticipantViewer
			f.syncRows(t)
		}, errComputeChoiceSession},
		{"removed participant", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.participant.Status = domain.SessionParticipantRemoved
			f.syncRows(t)
		}, errComputeChoiceSession},
		{"cookie foreign encoded tenant", func(t *testing.T, f *computeChoiceSourceFixture) {
			cookie := f.cookie
			cookie.ActiveTenantID = "foreign-tenant"
			f.driver.rows[computeChoiceCookieSQL][0] = choiceFixtureJSON(t, cookie)
		}, errComputeChoiceSource},
		{"cookie wrong PK digest", func(t *testing.T, f *computeChoiceSourceFixture) {
			cookie := f.cookie
			cookie.SessionDigest = domain.SecretDigest(strings.Repeat("f", 64))
			f.driver.rows[computeChoiceCookieSQL][0] = choiceFixtureJSON(t, cookie)
		}, errComputeChoiceSource},
		{"cookie scalar user diverges", func(_ *testing.T, f *computeChoiceSourceFixture) {
			f.driver.rows[computeChoiceCookieSQL][1] = "foreign-user"
		}, errComputeChoiceSource},
		{"cookie scalar CSRF diverges", func(_ *testing.T, f *computeChoiceSourceFixture) {
			f.driver.rows[computeChoiceCookieSQL][3] = strings.Repeat("f", 64)
		}, errComputeChoiceSource},
		{"cookie scalar expiry diverges", func(_ *testing.T, f *computeChoiceSourceFixture) {
			f.driver.rows[computeChoiceCookieSQL][5] = f.at.Add(2 * time.Hour)
		}, errComputeChoiceSource},
		{"cookie scalar revoked diverges", func(_ *testing.T, f *computeChoiceSourceFixture) { f.driver.rows[computeChoiceCookieSQL][7] = f.at }, errComputeChoiceSource},
		{"cookie issued in future", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.cookie.IssuedAt = f.at.Add(time.Second)
			f.cookie.LastSeenAt = f.cookie.IssuedAt
			f.syncRows(t)
		}, errComputeChoiceSource},
		{"membership foreign tenant", func(t *testing.T, f *computeChoiceSourceFixture) { f.member.TenantID = "foreign-tenant"; f.syncRows(t) }, errComputeChoiceSource},
		{"membership scalar role divergence", func(_ *testing.T, f *computeChoiceSourceFixture) {
			f.driver.rows[computeChoiceMembershipSQL][1] = "owner"
		}, errComputeChoiceSource},
		{"membership unknown role", func(t *testing.T, f *computeChoiceSourceFixture) { f.member.Role = "future-role"; f.syncRows(t) }, errComputeChoiceSource},
		{"Session foreign tenant", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.session.TenantID = "foreign-tenant"
			f.syncRows(t)
		}, errComputeChoiceSource},
		{"Session wrong encoded ID", func(t *testing.T, f *computeChoiceSourceFixture) {
			session := f.session
			session.ID = "foreign-session"
			f.driver.rows[computeChoiceSessionSQL][0] = choiceFixtureJSON(t, session)
		}, errComputeChoiceSource},
		{"Session scalar prefix divergence", func(_ *testing.T, f *computeChoiceSourceFixture) {
			f.driver.rows[computeChoiceSessionSQL][3] = int64(999)
		}, errComputeChoiceSource},
		{"participant foreign user", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.participant.UserID = "foreign-user"
			f.syncRows(t)
		}, errComputeChoiceSource},
		{"participant scalar status divergence", func(_ *testing.T, f *computeChoiceSourceFixture) {
			f.driver.rows[computeChoiceParticipantSQL][2] = "removed"
		}, errComputeChoiceSource},
		{"oversize before decode", func(_ *testing.T, f *computeChoiceSourceFixture) {
			f.driver.rows[computeChoiceCookieSQL][0] = strings.Repeat("x", 8193)
		}, computechoice.ErrResourceBudgetExceeded},
		{"JSON duplicate", func(_ *testing.T, f *computeChoiceSourceFixture) {
			raw := f.driver.rows[computeChoiceCookieSQL][0].(string)
			f.driver.rows[computeChoiceCookieSQL][0] = strings.Replace(raw, `"user_id":`, `"user_id":"foreign-user","user_id":`, 1)
		}, errComputeChoiceSource},
		{"JSON wrong case", func(_ *testing.T, f *computeChoiceSourceFixture) {
			raw := f.driver.rows[computeChoiceCookieSQL][0].(string)
			f.driver.rows[computeChoiceCookieSQL][0] = strings.Replace(raw, `"user_id"`, `"USER_ID"`, 1)
		}, errComputeChoiceSource},
		{"JSON unknown field", func(_ *testing.T, f *computeChoiceSourceFixture) {
			raw := f.driver.rows[computeChoiceCookieSQL][0].(string)
			f.driver.rows[computeChoiceCookieSQL][0] = `{"extra":true,` + raw[1:]
		}, errComputeChoiceSource},
		{"JSON nested duplicate", func(_ *testing.T, f *computeChoiceSourceFixture) {
			raw := f.driver.rows[computeChoiceCookieSQL][0].(string)
			f.driver.rows[computeChoiceCookieSQL][0] = strings.Replace(raw, `"provider":`, `"provider":"foreign","provider":`, 1)
		}, errComputeChoiceSource},
		{"JSON null required", func(_ *testing.T, f *computeChoiceSourceFixture) {
			raw := f.driver.rows[computeChoiceCookieSQL][0].(string)
			f.driver.rows[computeChoiceCookieSQL][0] = strings.Replace(raw, `"membership_security_version":7`, `"membership_security_version":null`, 1)
		}, errComputeChoiceSource},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f := newComputeChoiceSourceFixture(t)
			test.change(t, f)
			got, err := f.read(computechoice.NewResourceBudget())
			if !errors.Is(err, test.want) || got != (computeChoiceAuth{}) {
				t.Fatalf("got=%+v err=%v want zero/%v", got, err, test.want)
			}
		})
	}
}

func TestComputeChoiceCutoverRejectsMissingMixedOrUndrainedManifest(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *computeChoiceSourceFixture)
	}{
		{"absent", func(_ *testing.T, f *computeChoiceSourceFixture) { delete(f.driver.rows, computeChoiceCutoverSQL) }},
		{"disabled", func(t *testing.T, f *computeChoiceSourceFixture) { f.cutover.Enabled = false; f.syncRows(t) }},
		{"wrong manifest", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.cutover.ManifestDigest = strings.Repeat("f", 64)
			f.syncRows(t)
		}},
		{"wrong writer commit", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.cutover.WriterCommit = strings.Repeat("f", 40)
			f.syncRows(t)
		}},
		{"wrong version", func(t *testing.T, f *computeChoiceSourceFixture) { f.cutover.Version = 2; f.syncRows(t) }},
		{"wrong writer", func(t *testing.T, f *computeChoiceSourceFixture) { f.cutover.WriterRevision = 2; f.syncRows(t) }},
		{"wrong schema", func(t *testing.T, f *computeChoiceSourceFixture) { f.cutover.SchemaRevision = 2; f.syncRows(t) }},
		{"not drained", func(t *testing.T, f *computeChoiceSourceFixture) { f.cutover.OldWriterCount = 1; f.syncRows(t) }},
		{"no drain digest", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.cutover.DrainedInventoryDigest = ""
			f.syncRows(t)
		}},
		{"future receipt", func(t *testing.T, f *computeChoiceSourceFixture) {
			f.cutover.CompletedAt = f.at.Add(time.Second)
			f.syncRows(t)
		}},
		{"scalar record divergence", func(_ *testing.T, f *computeChoiceSourceFixture) { f.driver.rows[computeChoiceCutoverSQL][5] = false }},
		{"scalar manifest divergence", func(_ *testing.T, f *computeChoiceSourceFixture) {
			f.driver.rows[computeChoiceCutoverSQL][2] = strings.Repeat("f", 64)
		}},
		{"scalar time divergence", func(_ *testing.T, f *computeChoiceSourceFixture) { f.driver.rows[computeChoiceCutoverSQL][9] = f.at }},
		{"missing trusted manifest pin", func(_ *testing.T, f *computeChoiceSourceFixture) { f.pins.manifestDigest = "" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			f := newComputeChoiceSourceFixture(t)
			test.change(t, f)
			got, err := f.read(computechoice.NewResourceBudget())
			if !errors.Is(err, errComputeChoiceSource) || got != (computeChoiceAuth{}) {
				t.Fatalf("cutover got=%+v err=%v want generic unavailable", got, err)
			}
		})
	}
}

func TestComputeChoiceRetryReauthorizesInsteadOfPublishingOldScope(t *testing.T) {
	f := newComputeChoiceSourceFixture(t)
	f.driver.commitModes = []string{"rollback"}
	f.driver.afterCommit = func() { at := f.at; f.cookie.RevokedAt = &at; f.syncRows(t) }
	budget := computechoice.NewResourceBudget()
	got, err := f.read(budget)
	if !errors.Is(err, domain.ErrWebSessionRevoked) || got != (computeChoiceAuth{}) {
		t.Fatalf("revoked retry leaked scope: got=%+v err=%v", got, err)
	}
	if got := budget.Snapshot(); got.AttemptNumber != 2 || got.TotalStatements != 8 || got.AttemptStatements != 2 || got.Active {
		t.Fatalf("retry reset totals or failed to re-read cookie: %+v", got)
	}
}

func TestComputeChoiceRetryKeepsAggregateBudgetAndUnknownCommitEmpty(t *testing.T) {
	t.Run("unknown commit", func(t *testing.T) {
		f := newComputeChoiceSourceFixture(t)
		f.driver.commitModes = []string{"unknown"}
		got, err := f.read(computechoice.NewResourceBudget())
		if err == nil || got != (computeChoiceAuth{}) {
			t.Fatalf("unconfirmed commit leaked scope: %+v err=%v", got, err)
		}
	})
	t.Run("total statements", func(t *testing.T) {
		f := newComputeChoiceSourceFixture(t)
		f.driver.commitModes = make([]string, 16)
		for i := range f.driver.commitModes {
			f.driver.commitModes[i] = "rollback"
		}
		budget := computechoice.NewResourceBudget()
		got, err := f.read(budget)
		if !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || got != (computeChoiceAuth{}) {
			t.Fatalf("retry budget got=%+v err=%v", got, err)
		}
		if snapshot := budget.Snapshot(); snapshot.TotalStatements != 96 || !snapshot.Failed || snapshot.Active || len(f.driver.statements) != 96 {
			t.Fatalf("budget bypass: %+v SQL=%d", snapshot, len(f.driver.statements))
		}
	})
	t.Run("precharged bytes", func(t *testing.T) {
		f := newComputeChoiceSourceFixture(t)
		budget := computechoice.NewResourceBudget()
		attempt, err := budget.NewAttempt()
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 6; i++ {
			if err := attempt.RecordMaterialized(computechoice.SourceConnection, 128*1024); err != nil {
				t.Fatal(err)
			}
		}
		if err := attempt.Finish(); err != nil {
			t.Fatal(err)
		}
		attempt, err = budget.NewAttempt()
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if err := attempt.RecordMaterialized(computechoice.SourceConnection, 128*1024); err != nil {
				t.Fatal(err)
			}
		}
		if err := attempt.Finish(); err != nil {
			t.Fatal(err)
		}
		got, err := f.read(budget)
		if !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || got != (computeChoiceAuth{}) {
			t.Fatalf("preparse aggregate budget bypass got=%+v err=%v", got, err)
		}
	})
}

func TestComputeChoiceTimestampUsesExactDatabasePrecision(t *testing.T) {
	f := newComputeChoiceSourceFixture(t)
	f.cookie.IdleExpiresAt = f.cookie.IdleExpiresAt.Add(500 * time.Nanosecond)
	f.cutover.CompletedAt = f.cutover.CompletedAt.Add(500 * time.Nanosecond)
	f.syncRows(t)
	if _, err := f.read(computechoice.NewResourceBudget()); err != nil {
		t.Fatalf("valid microsecond persisted scalar denied: %v", err)
	}
	if computeChoiceTimestampEqual(f.at.Add(time.Microsecond), f.at) {
		t.Fatal("one microsecond divergence accepted")
	}
}
