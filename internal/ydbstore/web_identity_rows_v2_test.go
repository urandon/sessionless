package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// This controlled driver exercises the real private row helpers, exact keys,
// caller-owned transaction and bounded reads. It is NOT evidence of genuine YDB
// isolation, JsonDocument behavior, migration application or provider proof.
type identityRowsV2Projection struct {
	user    domain.UserID
	subject domain.ExternalSubject
	created time.Time
}

type identityRowsV2State struct {
	forward  map[string]string
	sessions map[string]string
	members  map[string]string
	reverse  map[string]identityRowsV2Projection
}

func (state identityRowsV2State) clone() identityRowsV2State {
	next := identityRowsV2State{forward: make(map[string]string), sessions: make(map[string]string), members: make(map[string]string), reverse: make(map[string]identityRowsV2Projection)}
	for key, value := range state.forward {
		next.forward[key] = value
	}
	for key, value := range state.sessions {
		next.sessions[key] = value
	}
	for key, value := range state.members {
		next.members[key] = value
	}
	for key, value := range state.reverse {
		next.reverse[key] = value
	}
	return next
}

type identityRowsV2Fixture struct {
	mu                                                                  sync.Mutex
	durable                                                             identityRowsV2State
	pending                                                             *identityRowsV2State
	queries, writes, begins, commits, rollbacks, rangeOpen, rangeClosed int
	queryLimit                                                          int
	statements                                                          []string
	isolations                                                          []driver.IsolationLevel
	failStatement                                                       string
	failure                                                             error
	rangeCloseError                                                     error
	closed                                                              bool
}

type identityRowsV2Conn struct{ fixture *identityRowsV2Fixture }
type identityRowsV2Tx struct{ fixture *identityRowsV2Fixture }
type identityRowsV2Driver struct{}

func (*identityRowsV2Driver) Open(string) (driver.Conn, error) {
	return nil, errors.New("connector-only fixture")
}
func (fixture *identityRowsV2Fixture) Driver() driver.Driver { return &identityRowsV2Driver{} }
func (fixture *identityRowsV2Fixture) Connect(context.Context) (driver.Conn, error) {
	return &identityRowsV2Conn{fixture: fixture}, nil
}
func (*identityRowsV2Conn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fixture requires explicit context statements")
}
func (conn *identityRowsV2Conn) Close() error {
	conn.fixture.mu.Lock()
	defer conn.fixture.mu.Unlock()
	conn.fixture.closed = true
	return nil
}
func (*identityRowsV2Conn) Begin() (driver.Tx, error) {
	return nil, errors.New("fixture requires explicit serializable BeginTx")
}
func (conn *identityRowsV2Conn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	fixture := conn.fixture
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if ctx.Err() != nil || fixture.pending != nil || options.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("fixture rejects invalid transaction boundary")
	}
	next := fixture.durable.clone()
	fixture.pending = &next
	fixture.begins++
	fixture.isolations = append(fixture.isolations, options.Isolation)
	return &identityRowsV2Tx{fixture: fixture}, nil
}
func (tx *identityRowsV2Tx) Commit() error {
	fixture := tx.fixture
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.pending == nil || fixture.rangeOpen != 0 {
		return errors.New("fixture commit without closed original transaction")
	}
	fixture.durable = fixture.pending.clone()
	fixture.pending = nil
	fixture.commits++
	return nil
}
func (tx *identityRowsV2Tx) Rollback() error {
	fixture := tx.fixture
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.pending == nil {
		return errors.New("fixture rollback without original transaction")
	}
	fixture.pending = nil
	fixture.rollbacks++
	return nil
}

func identityRowsV2Statement(statement string) string {
	return strings.Join(strings.Fields(statement), " ")
}
func identityRowsV2SubjectKey(subject domain.ExternalSubject) string {
	return string(subject.Provider) + "\x00" + subject.Subject
}
func identityRowsV2ReverseKey(user domain.UserID, subject domain.ExternalSubject) string {
	return string(user) + "\x00" + identityRowsV2SubjectKey(subject)
}
func identityRowsV2MemberKey(user domain.UserID, tenant domain.TenantID) string {
	return string(user) + "\x00" + string(tenant)
}

func identityRowsV2Args(args []driver.NamedValue) []any {
	values := make([]any, len(args))
	for index, arg := range args {
		values[index] = arg.Value
	}
	return values
}
func identityRowsV2ExpectedBucket(value string) int64 {
	bucket, _ := webBucket(value)
	return int64(bucket)
}

func (fixture *identityRowsV2Fixture) statement(ctx context.Context, statement string, args []driver.NamedValue, write bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if _, present := ctx.Deadline(); !present {
		return errors.New("fixture requires bounded statement context")
	}
	if fixture.pending == nil || fixture.rangeOpen != 0 {
		return errors.New("fixture requires original transaction and closed range before point reads")
	}
	if fixture.queries+fixture.writes >= fixture.queryLimit {
		return errors.New("fixture statement ceiling exceeded")
	}
	for index, arg := range args {
		if arg.Ordinal != index+1 || arg.Name != "" {
			return errors.New("fixture rejects nonexact positional arguments")
		}
	}
	fixture.statements = append(fixture.statements, identityRowsV2Statement(statement))
	if write {
		fixture.writes++
	} else {
		fixture.queries++
	}
	if fixture.failStatement == identityRowsV2Statement(statement) {
		return fixture.failure
	}
	return nil
}

func (conn *identityRowsV2Conn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	fixture := conn.fixture
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if err := fixture.statement(ctx, statement, args, false); err != nil {
		return nil, err
	}
	values := identityRowsV2Args(args)
	result := &identityRowsV2Rows{fixture: fixture, columns: []string{"record"}}
	switch statement {
	case webIdentityRowSQLV2:
		if len(values) != 3 {
			return nil, errors.New("forward read requires exact three keys")
		}
		provider, providerOK := values[1].(string)
		subject, subjectOK := values[2].(string)
		key := domain.ExternalSubject{Provider: domain.IdentityProvider(provider), Subject: subject}
		if !providerOK || !subjectOK || values[0] != identityRowsV2ExpectedBucket(key.String()) {
			return nil, errors.New("forward read key/bucket mismatch")
		}
		if raw, found := fixture.pending.forward[identityRowsV2SubjectKey(key)]; found {
			result.values = [][]driver.Value{{raw}}
		}
	case webIdentitySessionSQLV2:
		if len(values) != 2 {
			return nil, errors.New("session read requires exact two keys")
		}
		digest, ok := values[1].(string)
		if !ok || values[0] != identityRowsV2ExpectedBucket(digest) {
			return nil, errors.New("session read key/bucket mismatch")
		}
		if raw, found := fixture.pending.sessions[digest]; found {
			result.values = [][]driver.Value{{raw}}
		}
	case webIdentityActiveSetSQLV2:
		if len(values) != 2 {
			return nil, errors.New("reverse read requires exact user keys")
		}
		user, ok := values[1].(string)
		if !ok || values[0] != identityRowsV2ExpectedBucket(user) {
			return nil, errors.New("reverse read key/bucket mismatch")
		}
		var projected []identityRowsV2Projection
		for _, entry := range fixture.pending.reverse {
			if entry.user == domain.UserID(user) {
				projected = append(projected, entry)
			}
		}
		sort.Slice(projected, func(left, right int) bool {
			return identityRowsV2SubjectKey(projected[left].subject) < identityRowsV2SubjectKey(projected[right].subject)
		})
		if len(projected) > 3 {
			projected = projected[:3]
		}
		result.columns = []string{"provider", "subject", "created_at"}
		for _, entry := range projected {
			result.values = append(result.values, []driver.Value{string(entry.subject.Provider), entry.subject.Subject, entry.created})
		}
		result.rangeRead = true
		fixture.rangeOpen++
	case `SELECT record FROM tenant_memberships
		 WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3`:
		if len(values) != 3 {
			return nil, errors.New("membership read requires exact three keys")
		}
		user, userOK := values[1].(string)
		tenant, tenantOK := values[2].(string)
		if !userOK || !tenantOK || values[0] != identityRowsV2ExpectedBucket(user) {
			return nil, errors.New("membership read key/bucket mismatch")
		}
		if raw, found := fixture.pending.members[identityRowsV2MemberKey(domain.UserID(user), domain.TenantID(tenant))]; found {
			result.values = [][]driver.Value{{raw}}
		}
	default:
		return nil, fmt.Errorf("fixture forbids query outside exact manifest: %s", identityRowsV2Statement(statement))
	}
	return result, nil
}

const identityRowsV2ForwardWrite = `UPSERT INTO external_identities (shard_bucket, provider, subject, user_id, created_at, updated_at, record) VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS JsonDocument))`
const identityRowsV2ReverseWrite = `UPSERT INTO external_identities_by_user (user_bucket,user_id,provider,subject,created_at) VALUES ($1,$2,$3,$4,$5)`
const identityRowsV2ReverseDelete = `DELETE FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 AND provider=$3 AND subject=$4`
const identityRowsV2SessionWrite = `UPSERT INTO web_sessions (shard_bucket,session_digest,csrf_token_digest,user_id,active_tenant_id, identity_provider,external_subject,membership_security_version,issued_at, last_seen_at,idle_expires_at,absolute_expires_at,revoked_at,record) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,CAST($14 AS JsonDocument))`

func (conn *identityRowsV2Conn) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	fixture := conn.fixture
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if err := fixture.statement(ctx, statement, args, true); err != nil {
		return nil, err
	}
	values := identityRowsV2Args(args)
	switch identityRowsV2Statement(statement) {
	case identityRowsV2ForwardWrite:
		if len(values) != 7 {
			return nil, errors.New("forward write shape mismatch")
		}
		raw, ok := values[6].(string)
		if !ok {
			return nil, errors.New("forward record is not private JSON")
		}
		row, err := domain.DecodeExternalIdentityV2([]byte(raw))
		if err != nil {
			return nil, err
		}
		want := []any{identityRowsV2ExpectedBucket(row.Subject.String()), string(row.Subject.Provider), row.Subject.Subject, string(row.UserID), row.CreatedAt, row.UpdatedAt, raw}
		if !reflect.DeepEqual(values, want) {
			return nil, errors.New("forward write scalar/record/key mismatch")
		}
		fixture.pending.forward[identityRowsV2SubjectKey(row.Subject)] = raw
	case identityRowsV2ReverseWrite, identityRowsV2ReverseDelete:
		wantCount := 5
		if identityRowsV2Statement(statement) == identityRowsV2ReverseDelete {
			wantCount = 4
		}
		if len(values) != wantCount {
			return nil, errors.New("reverse write shape mismatch")
		}
		user, userOK := values[1].(string)
		provider, providerOK := values[2].(string)
		subject, subjectOK := values[3].(string)
		if !userOK || !providerOK || !subjectOK || values[0] != identityRowsV2ExpectedBucket(user) {
			return nil, errors.New("reverse write exact key/bucket mismatch")
		}
		target := domain.ExternalSubject{Provider: domain.IdentityProvider(provider), Subject: subject}
		key := identityRowsV2ReverseKey(domain.UserID(user), target)
		if wantCount == 4 {
			delete(fixture.pending.reverse, key)
		} else {
			created, ok := values[4].(time.Time)
			if !ok {
				return nil, errors.New("reverse created_at mismatch")
			}
			fixture.pending.reverse[key] = identityRowsV2Projection{user: domain.UserID(user), subject: target, created: created}
		}
	case identityRowsV2SessionWrite:
		if len(values) != 14 {
			return nil, errors.New("session write shape mismatch")
		}
		raw, ok := values[13].(string)
		if !ok {
			return nil, errors.New("session record is not JSON")
		}
		row, err := domain.DecodeWebSessionV2([]byte(raw))
		if err != nil {
			return nil, err
		}
		s := row.Session
		revoked := time.Unix(0, 0).UTC()
		if s.RevokedAt != nil {
			revoked = *s.RevokedAt
		}
		want := []any{identityRowsV2ExpectedBucket(string(s.SessionDigest)), string(s.SessionDigest), string(s.CSRFTokenDigest), string(s.UserID), string(s.ActiveTenantID), string(s.AuthenticatedSubject.Provider), s.AuthenticatedSubject.Subject, int64(s.MembershipSecurityVersion), s.IssuedAt, s.LastSeenAt, s.IdleExpiresAt, s.AbsoluteExpiresAt, revoked, raw}
		if !reflect.DeepEqual(values, want) {
			return nil, errors.New("session write scalar/record/key mismatch")
		}
		fixture.pending.sessions[string(s.SessionDigest)] = raw
	default:
		return nil, fmt.Errorf("fixture forbids write outside exact manifest: %s", identityRowsV2Statement(statement))
	}
	return driver.RowsAffected(1), nil
}

type identityRowsV2Rows struct {
	fixture           *identityRowsV2Fixture
	columns           []string
	values            [][]driver.Value
	index             int
	rangeRead, closed bool
}

func (rows *identityRowsV2Rows) Columns() []string { return rows.columns }
func (rows *identityRowsV2Rows) Close() error {
	rows.fixture.mu.Lock()
	defer rows.fixture.mu.Unlock()
	if rows.closed {
		return nil
	}
	rows.closed = true
	if rows.rangeRead {
		rows.fixture.rangeOpen--
		rows.fixture.rangeClosed++
		return rows.fixture.rangeCloseError
	}
	return nil
}
func (rows *identityRowsV2Rows) Next(destination []driver.Value) error {
	if rows.index >= len(rows.values) {
		return io.EOF
	}
	copy(destination, rows.values[rows.index])
	rows.index++
	return nil
}

func newIdentityRowsV2Fixture(t *testing.T, state identityRowsV2State) (*identityRowsV2Fixture, context.Context, *sql.Tx) {
	t.Helper()
	fixture := &identityRowsV2Fixture{durable: state.clone(), queryLimit: 8}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	db := sql.OpenDB(fixture)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close isolated SQL fixture: %v", err)
		}
	})
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("rollback original fixture transaction: %v", err)
		}
	})
	t.Cleanup(func() {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		if fixture.rangeOpen != 0 {
			t.Errorf("fixture leaked %d open ranges", fixture.rangeOpen)
		}
	})
	return fixture, ctx, tx
}

func identityRowsV2At() time.Time { return time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC) }
func identityRowsV2Identity(provider domain.IdentityProvider, revision domain.AuthRevisionV2) domain.ExternalIdentityV2 {
	at := identityRowsV2At().Add(-time.Hour)
	return domain.ExternalIdentityV2{Version: 2, Subject: domain.ExternalSubject{Provider: provider, Subject: "123"}, UserID: "rows-user", CreatedAt: at, UpdatedAt: at, State: domain.ExternalIdentityActiveV2, Revision: revision}
}
func identityRowsV2JSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
func identityRowsV2Data(t *testing.T) (identityRowsV2State, domain.ExternalIdentityV2, domain.WebSessionV2, domain.TenantMembership) {
	t.Helper()
	identity := identityRowsV2Identity(domain.IdentityProviderYandex, 7)
	member := domain.TenantMembership{TenantID: "rows-tenant", UserID: identity.UserID, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 3, CreatedAt: identity.CreatedAt, UpdatedAt: identity.UpdatedAt}
	session := domain.WebSessionV2{Version: 2, IdentityRevision: identity.Revision, Session: domain.WebSession{SessionDigest: domain.DigestSecret("rows-session"), CSRFTokenDigest: domain.DigestSecret("rows-csrf"), UserID: identity.UserID, ActiveTenantID: member.TenantID, AuthenticatedSubject: identity.Subject, MembershipSecurityVersion: member.SecurityVersion, IssuedAt: identity.CreatedAt, LastSeenAt: identity.CreatedAt, IdleExpiresAt: identityRowsV2At().Add(time.Hour), AbsoluteExpiresAt: identityRowsV2At().Add(24 * time.Hour)}}
	state := identityRowsV2State{forward: map[string]string{identityRowsV2SubjectKey(identity.Subject): identityRowsV2JSON(t, identity)}, sessions: map[string]string{string(session.Session.SessionDigest): identityRowsV2JSON(t, session)}, members: map[string]string{identityRowsV2MemberKey(member.UserID, member.TenantID): identityRowsV2JSON(t, member)}, reverse: map[string]identityRowsV2Projection{identityRowsV2ReverseKey(identity.UserID, identity.Subject): {user: identity.UserID, subject: identity.Subject, created: identity.CreatedAt}}}
	return state, identity, session, member
}

func TestWebIdentityRowsV2ExactBoundedReaders(t *testing.T) {
	if !strings.Contains(webIdentityRowSQLV2, "SUBSTRING(CAST(record AS String),0,8193)") || !strings.Contains(webIdentitySessionSQLV2, "SUBSTRING(CAST(record AS String),0,8193)") || !strings.Contains(webIdentityActiveSetSQLV2, "SUBSTRING(CAST(provider AS String),0,161)") || !strings.Contains(webIdentityActiveSetSQLV2, "SUBSTRING(CAST(subject AS String),0,161)") || !strings.HasSuffix(webIdentityActiveSetSQLV2, "ORDER BY provider, subject LIMIT 3") {
		t.Fatal("bounded canonical/reverse SQL contract changed")
	}
	for _, revision := range []domain.AuthRevisionV2{1<<53 - 1, 1 << 53, 1<<53 + 1, math.MaxUint64} {
		t.Run(revision.String(), func(t *testing.T) {
			state, identity, session, _ := identityRowsV2Data(t)
			identity.Revision, session.IdentityRevision = revision, revision
			state.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
			state.sessions[string(session.Session.SessionDigest)] = identityRowsV2JSON(t, session)
			fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
			got, found, err := readExternalIdentityV2(ctx, tx, identity.Subject)
			if err != nil || !found || !reflect.DeepEqual(got, identity) {
				t.Fatalf("controlled forward read = (%+v,%v,%v), want %+v", got, found, err, identity)
			}
			cookie, found, err := readWebSessionV2(ctx, tx, session.Session.SessionDigest)
			if err != nil || !found || !reflect.DeepEqual(cookie, session) {
				t.Fatalf("controlled session read = (%+v,%v,%v), want %+v", cookie, found, err, session)
			}
			if fixture.queries != 2 || fixture.begins != 1 || fixture.writes != 0 {
				t.Errorf("point read evidence: queries=%d begins=%d writes=%d", fixture.queries, fixture.begins, fixture.writes)
			}
		})
	}
}

func TestWebIdentityRowsV2ReadersRejectLegacyScopeAndOversize(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*testing.T, *identityRowsV2State, domain.ExternalIdentityV2, domain.WebSessionV2)
		session bool
		missing bool
	}{
		{name: "missing identity", missing: true, mutate: func(_ *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2, _ domain.WebSessionV2) {
			delete(s.forward, identityRowsV2SubjectKey(i.Subject))
		}},
		{name: "legacy identity", mutate: func(t *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2, _ domain.WebSessionV2) {
			s.forward[identityRowsV2SubjectKey(i.Subject)] = identityRowsV2JSON(t, domain.ExternalIdentity{Subject: i.Subject, UserID: i.UserID, CreatedAt: i.CreatedAt, UpdatedAt: i.UpdatedAt})
		}},
		{name: "foreign identity subject", mutate: func(t *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2, _ domain.WebSessionV2) {
			key := identityRowsV2SubjectKey(i.Subject)
			i.Subject.Subject = "foreign"
			s.forward[key] = identityRowsV2JSON(t, i)
		}},
		{name: "oversize identity", mutate: func(_ *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2, _ domain.WebSessionV2) {
			s.forward[identityRowsV2SubjectKey(i.Subject)] = strings.Repeat("x", 8193)
		}},
		{name: "empty identity", mutate: func(_ *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2, _ domain.WebSessionV2) {
			s.forward[identityRowsV2SubjectKey(i.Subject)] = ""
		}},
		{name: "missing session", session: true, missing: true, mutate: func(_ *testing.T, s *identityRowsV2State, _ domain.ExternalIdentityV2, w domain.WebSessionV2) {
			delete(s.sessions, string(w.Session.SessionDigest))
		}},
		{name: "legacy session", session: true, mutate: func(t *testing.T, s *identityRowsV2State, _ domain.ExternalIdentityV2, w domain.WebSessionV2) {
			s.sessions[string(w.Session.SessionDigest)] = identityRowsV2JSON(t, w.Session)
		}},
		{name: "foreign session digest", session: true, mutate: func(t *testing.T, s *identityRowsV2State, _ domain.ExternalIdentityV2, w domain.WebSessionV2) {
			key := string(w.Session.SessionDigest)
			w.Session.SessionDigest = domain.DigestSecret("other")
			s.sessions[key] = identityRowsV2JSON(t, w)
		}},
		{name: "oversize session", session: true, mutate: func(_ *testing.T, s *identityRowsV2State, _ domain.ExternalIdentityV2, w domain.WebSessionV2) {
			s.sessions[string(w.Session.SessionDigest)] = strings.Repeat("x", 8193)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			state, identity, session, _ := identityRowsV2Data(t)
			test.mutate(t, &state, identity, session)
			fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
			if test.session {
				got, found, err := readWebSessionV2(ctx, tx, session.Session.SessionDigest)
				if found || !reflect.DeepEqual(got, domain.WebSessionV2{}) || (err == nil) != test.missing {
					t.Errorf("denied session = (%+v,%v,%v), missing=%v", got, found, err, test.missing)
				}
			} else {
				got, found, err := readExternalIdentityV2(ctx, tx, identity.Subject)
				if found || !reflect.DeepEqual(got, domain.ExternalIdentityV2{}) || (err == nil) != test.missing {
					t.Errorf("denied identity = (%+v,%v,%v), missing=%v", got, found, err, test.missing)
				}
			}
			if fixture.queries != 1 {
				t.Errorf("bounded denial queries=%d, want1", fixture.queries)
			}
		})
	}
}

func TestWebIdentityRowsV2ActiveRangeClosesBeforeCanonicalReads(t *testing.T) {
	state, identity, _, _ := identityRowsV2Data(t)
	tombstone := identityRowsV2Identity(domain.IdentityProviderTelegram, 7)
	tombstone.State = domain.ExternalIdentityUnlinkedV2
	state.forward[identityRowsV2SubjectKey(tombstone.Subject)] = identityRowsV2JSON(t, tombstone)
	fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
	active, err := readActiveExternalIdentitiesV2(ctx, tx, identity.UserID)
	if err != nil || !reflect.DeepEqual(active, []domain.ExternalIdentityV2{identity}) {
		t.Fatalf("active canonical set = (%+v,%v), want only active forward row", active, err)
	}
	if fixture.rangeOpen != 0 || fixture.rangeClosed != 1 || fixture.queries != 2 || len(fixture.statements) != 2 || fixture.statements[0] != identityRowsV2Statement(webIdentityActiveSetSQLV2) || fixture.statements[1] != identityRowsV2Statement(webIdentityRowSQLV2) {
		t.Errorf("range-before-point evidence: open=%d closed=%d queries=%d statements=%v", fixture.rangeOpen, fixture.rangeClosed, fixture.queries, fixture.statements)
	}
}

func TestWebIdentityRowsV2ActiveSetRejectsProjectionDrift(t *testing.T) {
	for _, test := range []struct {
		name        string
		mutate      func(*testing.T, *identityRowsV2State, domain.ExternalIdentityV2)
		wantQueries int
	}{
		{name: "empty reverse", wantQueries: 1, mutate: func(_ *testing.T, s *identityRowsV2State, _ domain.ExternalIdentityV2) { clear(s.reverse) }},
		{name: "missing forward", wantQueries: 2, mutate: func(_ *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2) {
			delete(s.forward, identityRowsV2SubjectKey(i.Subject))
		}},
		{name: "creation mismatch", wantQueries: 2, mutate: func(_ *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2) {
			key := identityRowsV2ReverseKey(i.UserID, i.Subject)
			row := s.reverse[key]
			row.created = row.created.Add(time.Second)
			s.reverse[key] = row
		}},
		{name: "foreign owner", wantQueries: 2, mutate: func(t *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2) {
			key := identityRowsV2SubjectKey(i.Subject)
			i.UserID = "foreign"
			s.forward[key] = identityRowsV2JSON(t, i)
		}},
		{name: "unlinked projection", wantQueries: 2, mutate: func(t *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2) {
			i.State = domain.ExternalIdentityUnlinkedV2
			s.forward[identityRowsV2SubjectKey(i.Subject)] = identityRowsV2JSON(t, i)
		}},
		{name: "revision mismatch", wantQueries: 3, mutate: func(t *testing.T, s *identityRowsV2State, _ domain.ExternalIdentityV2) {
			i := identityRowsV2Identity(domain.IdentityProviderTelegram, 8)
			s.forward[identityRowsV2SubjectKey(i.Subject)] = identityRowsV2JSON(t, i)
			s.reverse[identityRowsV2ReverseKey(i.UserID, i.Subject)] = identityRowsV2Projection{user: i.UserID, subject: i.Subject, created: i.CreatedAt}
		}},
		{name: "duplicate provider", wantQueries: 3, mutate: func(t *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2) {
			i.Subject.Subject = "456"
			s.forward[identityRowsV2SubjectKey(i.Subject)] = identityRowsV2JSON(t, i)
			s.reverse[identityRowsV2ReverseKey(i.UserID, i.Subject)] = identityRowsV2Projection{user: i.UserID, subject: i.Subject, created: i.CreatedAt}
		}},
		{name: "three sentinel", wantQueries: 1, mutate: func(_ *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2) {
			for _, subject := range []string{"456", "789"} {
				v := i.Subject
				v.Subject = subject
				s.reverse[identityRowsV2ReverseKey(i.UserID, v)] = identityRowsV2Projection{user: i.UserID, subject: v, created: i.CreatedAt}
			}
		}},
		{name: "oversize subject projection", wantQueries: 1, mutate: func(_ *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2) {
			clear(s.reverse)
			i.Subject.Subject = strings.Repeat("x", 161)
			s.reverse[identityRowsV2ReverseKey(i.UserID, i.Subject)] = identityRowsV2Projection{user: i.UserID, subject: i.Subject, created: i.CreatedAt}
		}},
		{name: "oversize provider projection", wantQueries: 1, mutate: func(_ *testing.T, s *identityRowsV2State, i domain.ExternalIdentityV2) {
			clear(s.reverse)
			i.Subject.Provider = domain.IdentityProvider(strings.Repeat("x", 161))
			s.reverse[identityRowsV2ReverseKey(i.UserID, i.Subject)] = identityRowsV2Projection{user: i.UserID, subject: i.Subject, created: i.CreatedAt}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			state, identity, _, _ := identityRowsV2Data(t)
			test.mutate(t, &state, identity)
			fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
			active, err := readActiveExternalIdentitiesV2(ctx, tx, identity.UserID)
			if err == nil || active != nil {
				t.Errorf("projection drift = (%+v,%v), want nil/error", active, err)
			}
			if fixture.queries != test.wantQueries || fixture.rangeOpen != 0 || fixture.rangeClosed != 1 {
				t.Errorf("bounded failure queries=%d want=%d rangeOpen=%d closed=%d", fixture.queries, test.wantQueries, fixture.rangeOpen, fixture.rangeClosed)
			}
		})
	}
}

func TestWebIdentityRowsV2WritesPreserveCanonicalAndReverseKeys(t *testing.T) {
	for _, unlinked := range []bool{false, true} {
		t.Run(fmt.Sprintf("unlinked_%v", unlinked), func(t *testing.T) {
			state, identity, _, _ := identityRowsV2Data(t)
			other := identityRowsV2Identity(domain.IdentityProviderTelegram, 7)
			state.forward[identityRowsV2SubjectKey(other.Subject)] = identityRowsV2JSON(t, other)
			state.reverse[identityRowsV2ReverseKey(other.UserID, other.Subject)] = identityRowsV2Projection{user: other.UserID, subject: other.Subject, created: other.CreatedAt}
			fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
			next := identity
			next.Revision++
			next.UpdatedAt = identityRowsV2At()
			if unlinked {
				next.State = domain.ExternalIdentityUnlinkedV2
			}
			if err := writeExternalIdentityV2(ctx, tx, next); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(fixture.durable, state) || fixture.commits != 0 {
				t.Fatal("private helper published before caller committed original transaction")
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			got, err := domain.DecodeExternalIdentityV2([]byte(fixture.durable.forward[identityRowsV2SubjectKey(next.Subject)]))
			if err != nil || !reflect.DeepEqual(got, next) {
				t.Errorf("canonical write = (%+v,%v), want %+v", got, err, next)
			}
			_, projected := fixture.durable.reverse[identityRowsV2ReverseKey(next.UserID, next.Subject)]
			if projected == unlinked || len(fixture.durable.forward) != 2 || fixture.durable.forward[identityRowsV2SubjectKey(other.Subject)] != state.forward[identityRowsV2SubjectKey(other.Subject)] {
				t.Errorf("unlink=%v projection=%v canonical/other ownership changed: %+v", unlinked, projected, fixture.durable)
			}
			if fixture.queries != 1 || fixture.writes != 2 || fixture.commits != 1 || fixture.begins != 1 {
				t.Errorf("one original write transaction queries=%d writes=%d commits=%d begins=%d", fixture.queries, fixture.writes, fixture.commits, fixture.begins)
			}
			wantLast := identityRowsV2ReverseWrite
			if unlinked {
				wantLast = identityRowsV2ReverseDelete
			}
			if fixture.statements[2] != wantLast {
				t.Errorf("reverse statement=%s want=%s", fixture.statements[2], wantLast)
			}
		})
	}
}

func TestWebIdentityRowsV2WriteRejectsImmutableOwnershipAndRollsBackReverseFailure(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*domain.ExternalIdentityV2)
	}{
		{name: "foreign owner", mutate: func(i *domain.ExternalIdentityV2) { i.UserID = "other" }},
		{name: "creation changed", mutate: func(i *domain.ExternalIdentityV2) { i.CreatedAt = i.CreatedAt.Add(-time.Second) }},
		{name: "state without revision advance", mutate: func(i *domain.ExternalIdentityV2) { i.State = domain.ExternalIdentityUnlinkedV2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			state, identity, _, _ := identityRowsV2Data(t)
			fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
			next := identity
			test.mutate(&next)
			if err := writeExternalIdentityV2(ctx, tx, next); err == nil {
				t.Error("immutable identity mutation accepted")
			}
			if fixture.writes != 0 || !reflect.DeepEqual(fixture.durable, state) {
				t.Errorf("denied ownership mutation wrote state: writes=%d", fixture.writes)
			}
		})
	}
	for _, unlinked := range []bool{false, true} {
		t.Run(fmt.Sprintf("caller_rollback_after_reverse_failure_%v", unlinked), func(t *testing.T) {
			state, identity, _, _ := identityRowsV2Data(t)
			fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
			fixture.failStatement = identityRowsV2ReverseWrite
			if unlinked {
				fixture.failStatement = identityRowsV2ReverseDelete
			}
			fixture.failure = errors.New("controlled reverse failure")
			next := identity
			next.Revision++
			next.UpdatedAt = identityRowsV2At()
			if unlinked {
				next.State = domain.ExternalIdentityUnlinkedV2
			}
			if err := writeExternalIdentityV2(ctx, tx, next); !errors.Is(err, fixture.failure) {
				t.Fatalf("reverse failure error=%v want=%v", err, fixture.failure)
			}
			if fixture.pending.forward[identityRowsV2SubjectKey(identity.Subject)] == state.forward[identityRowsV2SubjectKey(identity.Subject)] {
				t.Fatal("fixture did not observe speculative forward write before reverse failure")
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if fixture.commits != 0 || fixture.rollbacks != 1 || !reflect.DeepEqual(fixture.durable, state) || fixture.pending != nil {
				t.Errorf("caller rollback did not restore original snapshot: commits=%d rollbacks=%d durable=%+v", fixture.commits, fixture.rollbacks, fixture.durable)
			}
		})
	}
}

func TestWebIdentityRowsV2SessionWriteUsesPrivateVersionedRecord(t *testing.T) {
	state, _, session, _ := identityRowsV2Data(t)
	clear(state.sessions)
	fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
	if err := writeWebSessionV2(ctx, tx, session); err != nil {
		t.Fatal(err)
	}
	got, found, err := readWebSessionV2(ctx, tx, session.Session.SessionDigest)
	if err != nil || !found || !reflect.DeepEqual(got, session) {
		t.Fatalf("private session read-your-write=(%+v,%v,%v)", got, found, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if fixture.writes != 1 || fixture.queries != 1 || fixture.commits != 1 {
		t.Errorf("session transaction evidence writes=%d queries=%d commits=%d", fixture.writes, fixture.queries, fixture.commits)
	}
}

func TestWebIdentityRowsV2AuthorizationReturnsNoDataOnDenial(t *testing.T) {
	for _, test := range []struct {
		name       string
		mutate     func(*identityRowsV2State, *domain.ExternalIdentityV2, *domain.WebSessionV2, *domain.TenantMembership)
		permission domain.TenantPermission
	}{
		{name: "missing session", mutate: func(s *identityRowsV2State, _ *domain.ExternalIdentityV2, w *domain.WebSessionV2, _ *domain.TenantMembership) {
			delete(s.sessions, string(w.Session.SessionDigest))
		}},
		{name: "missing identity", mutate: func(s *identityRowsV2State, i *domain.ExternalIdentityV2, _ *domain.WebSessionV2, _ *domain.TenantMembership) {
			delete(s.forward, identityRowsV2SubjectKey(i.Subject))
		}},
		{name: "missing membership", mutate: func(s *identityRowsV2State, _ *domain.ExternalIdentityV2, _ *domain.WebSessionV2, m *domain.TenantMembership) {
			delete(s.members, identityRowsV2MemberKey(m.UserID, m.TenantID))
		}},
		{name: "stale identity revision", mutate: func(_ *identityRowsV2State, i *domain.ExternalIdentityV2, _ *domain.WebSessionV2, _ *domain.TenantMembership) {
			i.Revision++
		}},
		{name: "unlinked identity", mutate: func(_ *identityRowsV2State, i *domain.ExternalIdentityV2, _ *domain.WebSessionV2, _ *domain.TenantMembership) {
			i.State = domain.ExternalIdentityUnlinkedV2
		}},
		{name: "foreign identity owner", mutate: func(_ *identityRowsV2State, i *domain.ExternalIdentityV2, _ *domain.WebSessionV2, _ *domain.TenantMembership) {
			i.UserID = "foreign-user"
		}},
		{name: "future identity", mutate: func(_ *identityRowsV2State, i *domain.ExternalIdentityV2, _ *domain.WebSessionV2, _ *domain.TenantMembership) {
			i.UpdatedAt = identityRowsV2At().Add(time.Second)
		}},
		{name: "revoked session", mutate: func(_ *identityRowsV2State, _ *domain.ExternalIdentityV2, w *domain.WebSessionV2, _ *domain.TenantMembership) {
			at := identityRowsV2At()
			w.Session.RevokedAt = &at
		}},
		{name: "future session", mutate: func(_ *identityRowsV2State, _ *domain.ExternalIdentityV2, w *domain.WebSessionV2, _ *domain.TenantMembership) {
			w.Session.LastSeenAt = identityRowsV2At().Add(time.Second)
		}},
		{name: "idle expiry equality", mutate: func(_ *identityRowsV2State, _ *domain.ExternalIdentityV2, w *domain.WebSessionV2, _ *domain.TenantMembership) {
			w.Session.IdleExpiresAt = identityRowsV2At()
		}},
		{name: "absolute expiry equality", mutate: func(_ *identityRowsV2State, _ *domain.ExternalIdentityV2, w *domain.WebSessionV2, _ *domain.TenantMembership) {
			w.Session.AbsoluteExpiresAt = identityRowsV2At()
			w.Session.IdleExpiresAt = identityRowsV2At()
		}},
		{name: "membership suspended", mutate: func(_ *identityRowsV2State, _ *domain.ExternalIdentityV2, _ *domain.WebSessionV2, m *domain.TenantMembership) {
			m.Status = domain.TenantMembershipSuspended
		}},
		{name: "membership revoked", mutate: func(_ *identityRowsV2State, _ *domain.ExternalIdentityV2, _ *domain.WebSessionV2, m *domain.TenantMembership) {
			m.Status = domain.TenantMembershipRevoked
		}},
		{name: "membership revision", mutate: func(_ *identityRowsV2State, _ *domain.ExternalIdentityV2, _ *domain.WebSessionV2, m *domain.TenantMembership) {
			m.SecurityVersion++
		}},
		{name: "foreign membership", mutate: func(_ *identityRowsV2State, _ *domain.ExternalIdentityV2, _ *domain.WebSessionV2, m *domain.TenantMembership) {
			m.TenantID = "foreign"
		}},
		{name: "future membership", mutate: func(_ *identityRowsV2State, _ *domain.ExternalIdentityV2, _ *domain.WebSessionV2, m *domain.TenantMembership) {
			m.UpdatedAt = identityRowsV2At().Add(time.Second)
		}},
		{name: "viewer write", permission: domain.TenantPermissionWrite, mutate: func(_ *identityRowsV2State, _ *domain.ExternalIdentityV2, _ *domain.WebSessionV2, m *domain.TenantMembership) {
			m.Role = domain.TenantMembershipViewer
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			state, identity, session, member := identityRowsV2Data(t)
			identityKey, sessionKey, memberKey := identityRowsV2SubjectKey(identity.Subject), string(session.Session.SessionDigest), identityRowsV2MemberKey(member.UserID, member.TenantID)
			test.mutate(&state, &identity, &session, &member)
			if _, found := state.forward[identityKey]; found {
				state.forward[identityKey] = identityRowsV2JSON(t, identity)
			}
			if _, found := state.sessions[sessionKey]; found {
				state.sessions[sessionKey] = identityRowsV2JSON(t, session)
			}
			if _, found := state.members[memberKey]; found {
				state.members[memberKey] = identityRowsV2JSON(t, member)
			}
			fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
			permission := test.permission
			if permission == "" {
				permission = domain.TenantPermissionRead
			}
			got, membership, err := authorizeWebSessionV2(ctx, tx, domain.SecretDigest(sessionKey), permission, identityRowsV2At())
			if !errors.Is(err, domain.ErrWebSessionRevoked) || !reflect.DeepEqual(got, domain.WebSessionV2{}) || !reflect.DeepEqual(membership, domain.TenantMembership{}) {
				t.Errorf("denied current join returned authority: session=%+v membership=%+v err=%v", got, membership, err)
			}
			if fixture.queries > 3 || fixture.writes != 0 || fixture.begins != 1 {
				t.Errorf("current join query/transaction bounds: %+v", fixture.statements)
			}
		})
	}
}

func TestWebIdentityRowsV2AuthorizationUsesOriginalTransaction(t *testing.T) {
	state, identity, session, member := identityRowsV2Data(t)
	fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
	got, membership, err := authorizeWebSessionV2(ctx, tx, session.Session.SessionDigest, domain.TenantPermissionWrite, identityRowsV2At())
	if err != nil || !reflect.DeepEqual(got, session) || !reflect.DeepEqual(membership, member) {
		t.Fatalf("current V2 join=(%+v,%+v,%v)", got, membership, err)
	}
	if fixture.queries != 3 || fixture.begins != 1 || fixture.commits != 0 {
		t.Errorf("join did not use exact original transaction: queries=%d begins=%d commits=%d", fixture.queries, fixture.begins, fixture.commits)
	}
	identity.Revision++
	fixture.pending.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
	if got, membership, err := authorizeWebSessionV2(ctx, tx, session.Session.SessionDigest, domain.TenantPermissionRead, identityRowsV2At()); err == nil || !reflect.DeepEqual(got, domain.WebSessionV2{}) || !reflect.DeepEqual(membership, domain.TenantMembership{}) {
		t.Errorf("later same-transaction identity change bypassed current join: (%+v,%+v,%v)", got, membership, err)
	}
}

func TestWebIdentityRowsV2OwnedPointReadUsesOriginalBudget(t *testing.T) {
	state, identity, _, _ := identityRowsV2Data(t)
	fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
	budget := &ownedAuthorityBudget{}
	callback, err := budget.begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { budget.finish(callback) })
	query := &ownedAuthorityQuery{tx: tx, budget: budget, callback: callback, at: identityRowsV2At()}
	got, found, err := readExternalIdentityV2(ctx, query, identity.Subject)
	if err != nil || !found || !reflect.DeepEqual(got, identity) {
		t.Fatalf("owned current point=(%+v,%v,%v)", got, found, err)
	}
	if budget.statements != 1 || budget.callbackStatements != 1 || budget.bytes == 0 || fixture.queries != 1 {
		t.Errorf("point read reset or bypassed budget: statements=%d callbackStatements=%d bytes=%d driverQueries=%d", budget.statements, budget.callbackStatements, budget.bytes, fixture.queries)
	}
	budget.callbackStatements = ownedAuthorityMaxStatements
	if got, found, err := readExternalIdentityV2(ctx, query, identity.Subject); err == nil || found || !reflect.DeepEqual(got, domain.ExternalIdentityV2{}) {
		t.Errorf("exhausted original budget returned identity: (%+v,%v,%v)", got, found, err)
	}
	if fixture.queries != 1 || !budget.failed {
		t.Errorf("overbudget read reached driver or lost sticky denial: queries=%d failed=%v", fixture.queries, budget.failed)
	}
}

func TestWebIdentityRowsV2CompleteTwoMethodSetAndRangeCloseFailure(t *testing.T) {
	for _, closeFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("range_close_failure_%v", closeFailure), func(t *testing.T) {
			state, identity, _, _ := identityRowsV2Data(t)
			other := identityRowsV2Identity(domain.IdentityProviderTelegram, identity.Revision)
			state.forward[identityRowsV2SubjectKey(other.Subject)] = identityRowsV2JSON(t, other)
			state.reverse[identityRowsV2ReverseKey(other.UserID, other.Subject)] = identityRowsV2Projection{user: other.UserID, subject: other.Subject, created: other.CreatedAt}
			fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
			if closeFailure {
				fixture.rangeCloseError = errors.New("controlled range close failure")
			}
			active, err := readActiveExternalIdentitiesV2(ctx, tx, identity.UserID)
			if closeFailure {
				if !errors.Is(err, fixture.rangeCloseError) || active != nil || fixture.queries != 1 {
					t.Errorf("range close failure advanced canonical reads: active=%+v err=%v queries=%d", active, err, fixture.queries)
				}
			} else if err != nil || !reflect.DeepEqual(active, []domain.ExternalIdentityV2{other, identity}) || fixture.queries != 3 {
				t.Errorf("complete two-method join = (%+v,%v), queries=%d want two canonical rows/3 queries", active, err, fixture.queries)
			}
			if fixture.rangeOpen != 0 || fixture.rangeClosed != 1 {
				t.Errorf("range lifecycle open=%d closed=%d", fixture.rangeOpen, fixture.rangeClosed)
			}
		})
	}
}

func TestWebIdentityRowsV2OwnedActiveSetPreservesMaterializationBudget(t *testing.T) {
	for _, limit := range []string{"exact_charge", "only_canonical_fits", "range_exhausted"} {
		t.Run(limit, func(t *testing.T) {
			state, identity, _, _ := identityRowsV2Data(t)
			fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
			budget := &ownedAuthorityBudget{}
			callback, err := budget.begin()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { budget.finish(callback) })
			query := &ownedAuthorityQuery{tx: tx, budget: budget, callback: callback, at: identityRowsV2At()}
			rangeBytes := uint64(len(identity.Subject.Provider) + len(identity.Subject.Subject) + 8)
			canonicalBytes := uint64(len(state.forward[identityRowsV2SubjectKey(identity.Subject)]))
			switch limit {
			case "exact_charge":
				// Preserve prior material from this same logical call, not a
				// newly allocated per-helper budget.
				if err := budget.materialized(callback, 17); err != nil {
					t.Fatal(err)
				}
			case "only_canonical_fits":
				if err := budget.materialized(callback, ownedAuthorityMaxBytes-canonicalBytes); err != nil {
					t.Fatal(err)
				}
			case "range_exhausted":
				if err := budget.materialized(callback, ownedAuthorityMaxBytes-rangeBytes+1); err != nil {
					t.Fatal(err)
				}
			}
			active, err := readActiveExternalIdentitiesV2(ctx, query, identity.UserID)
			if limit == "exact_charge" {
				if err != nil || !reflect.DeepEqual(active, []domain.ExternalIdentityV2{identity}) || budget.bytes != 17+rangeBytes+canonicalBytes || budget.statements != 2 || budget.failed {
					t.Fatalf("original budget charge: err=%v active=%+v bytes=%d want=%d statements=%d failed=%v", err, active, budget.bytes, 17+rangeBytes+canonicalBytes, budget.statements, budget.failed)
				}
			} else {
				if !errors.Is(err, domain.ErrMembershipDenied) || active != nil || !budget.failed || budget.check() == nil {
					t.Fatalf("overbudget active set escaped: err=%v active=%+v bytes=%d failed=%v", err, active, budget.bytes, budget.failed)
				}
				if limit == "range_exhausted" && fixture.queries != 1 {
					t.Errorf("exhausted range issued canonical reads: queries=%d want=1", fixture.queries)
				}
			}
			if fixture.rangeOpen != 0 || fixture.rangeClosed != 1 || budget.bytes > ownedAuthorityMaxBytes {
				t.Errorf("range/budget lifecycle: open=%d closed=%d bytes=%d", fixture.rangeOpen, fixture.rangeClosed, budget.bytes)
			}
		})
	}
}

func TestWebIdentityRowsV2AbsentTargetCreatesExactForwardAndProjection(t *testing.T) {
	state, _, _, _ := identityRowsV2Data(t)
	target := identityRowsV2Identity(domain.IdentityProviderTelegram, 8)
	target.CreatedAt, target.UpdatedAt = identityRowsV2At(), identityRowsV2At()
	fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
	if err := writeExternalIdentityV2(ctx, tx, target); err != nil {
		t.Fatalf("write absent canonical target: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := domain.DecodeExternalIdentityV2([]byte(fixture.durable.forward[identityRowsV2SubjectKey(target.Subject)]))
	projection, found := fixture.durable.reverse[identityRowsV2ReverseKey(target.UserID, target.Subject)]
	if err != nil || !reflect.DeepEqual(got, target) || !found || projection.user != target.UserID || projection.subject != target.Subject || !projection.created.Equal(target.CreatedAt) {
		t.Errorf("new target canonical/projection mismatch: row=%+v projection=%+v found=%v err=%v", got, projection, found, err)
	}
	if fixture.queries != 1 || fixture.writes != 2 || fixture.commits != 1 {
		t.Errorf("new target original transaction queries=%d writes=%d commits=%d", fixture.queries, fixture.writes, fixture.commits)
	}
}

func TestWebIdentityRowsV2ReadGateDeniesBeforeSQL(t *testing.T) {
	state, identity, session, _ := identityRowsV2Data(t)
	fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
	if raw, found, err := readWebIdentityJSONV2(ctx, tx, "SELECT record FROM arbitrary_table"); err == nil || found || raw != nil {
		t.Errorf("unlisted SQL accepted: raw=%q found=%v err=%v", raw, found, err)
	}
	if row, found, err := readExternalIdentityV2(ctx, tx, domain.ExternalSubject{}); err == nil || found || !reflect.DeepEqual(row, domain.ExternalIdentityV2{}) {
		t.Errorf("invalid subject reached V2 read: (%+v,%v,%v)", row, found, err)
	}
	if row, found, err := readWebSessionV2(ctx, tx, "invalid-digest"); err == nil || found || !reflect.DeepEqual(row, domain.WebSessionV2{}) {
		t.Errorf("invalid session digest reached V2 read: (%+v,%v,%v)", row, found, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if row, found, err := readExternalIdentityV2(cancelled, tx, identity.Subject); err == nil || found || !reflect.DeepEqual(row, domain.ExternalIdentityV2{}) {
		t.Errorf("cancelled context returned identity: (%+v,%v,%v)", row, found, err)
	}
	if row, found, err := readWebSessionV2(nil, tx, session.Session.SessionDigest); err == nil || found || !reflect.DeepEqual(row, domain.WebSessionV2{}) {
		t.Errorf("nil context returned session: (%+v,%v,%v)", row, found, err)
	}
	if fixture.queries != 0 || fixture.writes != 0 {
		t.Errorf("rejected read gate issued queries=%d writes=%d", fixture.queries, fixture.writes)
	}
}
