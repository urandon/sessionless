package ydbstore

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"gitcode.com/urandon/sessionless/internal/yandexoauth"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"github.com/golang-jwt/jwt/v4"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

const IdentityAccountNativeOriginV2 = "https://account-native.example.com"

// Raw test-only assembly. The public production runtime constructor owns both
// Store and service; this fixture never sets Store's private configuration.
// HTTP peers are controlled; SQL, JsonDocument and commit are genuine YDB.
type IdentityAccountNativeFixtureV2 struct {
	Store             *Store
	Service           ports.IdentityAccountServiceV2
	Yandex            *yandexoauth.Provider
	Telegram          *telegramoidc.Provider
	Clock             ports.Clock
	Owner, OtherOwner domain.UserID
	Tenants           [2]domain.TenantID
	OtherTenant       domain.TenantID
	Subjects          map[domain.IdentityProvider]domain.ExternalSubject
	CanonicalSession  domain.Session
	t                 *testing.T
	ctx               context.Context
	db                *sql.DB
	control           *accountNativeControlV2
	peer              *accountNativePeerV2
}

type IdentityAccountNativeSnapshotV2 struct {
	Identities                                             map[domain.IdentityProvider]domain.ExternalIdentityV2
	Active                                                 map[domain.UserID][]domain.ExternalIdentityV2
	Members                                                map[domain.TenantID]domain.TenantMembership
	Sessions                                               map[domain.SecretDigest]domain.WebSessionV2
	Operations                                             map[domain.SecretDigest]domain.IdentityOperationV2
	Children                                               map[domain.SecretDigest]domain.IdentityChallengeV2
	Scrubbed                                               map[domain.SecretDigest]bool
	Audits                                                 map[string]int
	Session                                                domain.Session
	Participant                                            domain.SessionParticipant
	Material                                               string
	Tokens, Infos, JWKS, Closed, WriteCommits, LostReplies int
}

type accountNativeClockV2 struct {
	f *IdentityAccountNativeFixtureV2
}

func (c accountNativeClockV2) Now() time.Time { return c.f.now() }
func (f *IdentityAccountNativeFixtureV2) now() time.Time {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	var at time.Time
	if err := f.db.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil || at.IsZero() {
		f.t.Fatal("native account SQL clock unavailable", err)
	}
	return at.UTC()
}
func (f *IdentityAccountNativeFixtureV2) Context() context.Context { return f.ctx }

type accountNativeSecurityKeyV2 struct {
	At time.Time
	ID string
}
type accountNativeControlV2 struct {
	mu                                sync.Mutex
	users                             map[domain.UserID]bool
	sessions, challenges              map[domain.SecretDigest]bool
	security                          map[string]accountNativeSecurityKeyV2
	active, writeCommits, lostReplies int
	lose                              domain.SecretDigest
}
type accountNativeConnectorV2 struct {
	actual  driver.Connector
	control *accountNativeControlV2
}

func (c *accountNativeConnectorV2) Driver() driver.Driver { return c.actual.Driver() }
func (c *accountNativeConnectorV2) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.actual.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &accountNativeConnV2{Conn: conn, control: c.control}, nil
}

type accountNativeConnV2 struct {
	driver.Conn
	control *accountNativeControlV2
	tx      *accountNativeTxV2
}

func (c *accountNativeConnV2) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}
func (c *accountNativeConnV2) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	beginner, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, errors.New("native driver lacks explicit transaction interface")
	}
	tx, err := beginner.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	c.control.mu.Lock()
	c.control.active++
	c.control.mu.Unlock()
	c.tx = &accountNativeTxV2{Tx: tx, control: c.control}
	return c.tx, nil
}
func (c *accountNativeConnV2) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	reader, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, errors.New("native driver lacks direct query interface")
	}
	return reader.QueryContext(ctx, query, args)
}
func (c *accountNativeConnV2) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	writer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("native driver lacks direct exec interface")
	}
	if c.tx == nil || c.tx.finished {
		return nil, errors.New("native account write outside original transaction")
	}
	if err := c.observeWrite(query, args); err != nil {
		return nil, err
	}
	c.tx.writes = true
	return writer.ExecContext(ctx, query, args)
}

// Observe exact production writes BEFORE forwarding them, including keys whose
// acknowledgements/cookies might be lost. Never replace SQL, values or results.
func (c *accountNativeConnV2) observeWrite(query string, args []driver.NamedValue) error {
	c.control.mu.Lock()
	defer c.control.mu.Unlock()
	if strings.Contains(query, "UPSERT INTO web_sessions") {
		if len(args) != 14 {
			return errors.New("unexpected native session write shape")
		}
		raw, ok := args[13].Value.(string)
		if !ok {
			return errors.New("native session record must be string DATA")
		}
		record, err := domain.DecodeWebSessionV2([]byte(raw))
		if err != nil || !c.control.users[record.Session.UserID] {
			return errors.New("native session write outside owned users")
		}
		if len(c.control.sessions) >= 32 && !c.control.sessions[record.Session.SessionDigest] {
			return errors.New("native session fixture key cap")
		}
		c.control.sessions[record.Session.SessionDigest] = true
	}
	if strings.Contains(query, "UPSERT INTO oidc_login_challenges") {
		if len(args) != 11 {
			return errors.New("unexpected native challenge write shape")
		}
		raw, ok := args[9].Value.(string)
		if !ok {
			return errors.New("native challenge record must be string DATA")
		}
		var tag struct {
			Purpose domain.IdentityRecordPurposeV2 `json:"purpose"`
		}
		if json.Unmarshal([]byte(raw), &tag) != nil {
			return errors.New("native challenge tag invalid")
		}
		var digest domain.SecretDigest
		if tag.Purpose == domain.IdentityPurposeOperationV2 {
			operation, err := domain.DecodeIdentityOperationV2([]byte(raw))
			if err != nil {
				return err
			}
			user := domain.UserID("")
			if operation.Pending != nil {
				user = operation.Pending.Scope.UserID
			} else {
				user = operation.Receipt.UserID
				c.tx.terminal = operation.OperationDigest
			}
			if !c.control.users[user] {
				return errors.New("native operation write outside owned user")
			}
			digest = operation.OperationDigest
		} else {
			child, err := domain.DecodeIdentityChallengeV2([]byte(raw))
			if err != nil {
				return err
			}
			digest = child.StateDigest
			if child.Proof != nil && !c.control.challenges[child.Proof.ParentOperationDigest] {
				return errors.New("native proof child lacks owned original parent")
			}
		}
		if len(c.control.challenges) >= 24 && !c.control.challenges[digest] {
			return errors.New("native challenge fixture key cap")
		}
		c.control.challenges[digest] = true
	}
	if strings.Contains(query, "UPSERT INTO web_security_audit_events") {
		if len(args) != 12 {
			return errors.New("unexpected native security audit shape")
		}
		raw, ok := args[10].Value.(string)
		if !ok {
			return errors.New("native security audit record not string")
		}
		var event domain.WebSecurityAuditEvent
		if json.Unmarshal([]byte(raw), &event) != nil || event.Validate() != nil || event.UserID != "" && !c.control.users[event.UserID] {
			return errors.New("native security audit outside owned scope")
		}
		if len(c.control.security) >= 32 {
			return errors.New("native security audit fixture key cap")
		}
		c.control.security[event.RequestID] = accountNativeSecurityKeyV2{event.OccurredAt, event.RequestID}
	}
	return nil
}

type accountNativeTxV2 struct {
	driver.Tx
	control          *accountNativeControlV2
	writes, finished bool
	terminal         domain.SecretDigest
}

func (t *accountNativeTxV2) Commit() error {
	err := t.Tx.Commit()
	t.control.mu.Lock()
	defer t.control.mu.Unlock()
	if !t.finished {
		t.finished = true
		t.control.active--
	}
	if err == nil && t.writes {
		t.control.writeCommits++
	}
	if err == nil && t.writes && t.terminal != "" && t.control.lose == t.terminal {
		t.control.lose = ""
		t.control.lostReplies++
		return errors.New("test lost reply AFTER actual acknowledged native confirmation commit")
	}
	return err
}
func (t *accountNativeTxV2) Rollback() error {
	err := t.Tx.Rollback()
	t.control.mu.Lock()
	defer t.control.mu.Unlock()
	if !t.finished {
		t.finished = true
		t.control.active--
	}
	return err
}

func NewIdentityAccountNativeFixtureV2(t *testing.T, initial domain.IdentityProvider, targetConflict bool) *IdentityAccountNativeFixtureV2 {
	t.Helper()
	dsn := webIdentityV2NativeDSN(t) // skip only when the original gate is OFF
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	native, err := ydb.Open(ctx, dsn)
	if err != nil {
		t.Fatal("open guarded native account driver", err)
	}
	connector, err := ydb.Connector(native, ydb.WithQueryService(false), ydb.WithAutoDeclare(), ydb.WithNumericArgs())
	if err != nil {
		_ = native.Close(ctx)
		t.Fatal(err)
	}
	raw := sql.OpenDB(connector)
	control := &accountNativeControlV2{users: make(map[domain.UserID]bool), sessions: make(map[domain.SecretDigest]bool), challenges: make(map[domain.SecretDigest]bool), security: make(map[string]accountNativeSecurityKeyV2)}
	wrapped := sql.OpenDB(&accountNativeConnectorV2{actual: connector, control: control})
	t.Cleanup(func() {
		if err := wrapped.Close(); err != nil {
			t.Error(err)
		}
		if err := raw.Close(); err != nil {
			t.Error(err)
		}
		closeCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := native.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})
	nonce := rand.Text()
	digest := sha256.Sum256([]byte(nonce))
	number := strconv.FormatUint(binary.BigEndian.Uint64(digest[:8])%900000000000000000+100000000000000000, 10)
	f := &IdentityAccountNativeFixtureV2{t: t, ctx: ctx, db: raw, control: control, Owner: domain.UserID("account-native-" + nonce), OtherOwner: domain.UserID("account-other-" + nonce), Tenants: [2]domain.TenantID{domain.TenantID("account-a-tenant-" + nonce), domain.TenantID("account-b-tenant-" + nonce)}, OtherTenant: domain.TenantID("account-other-tenant-" + nonce), Subjects: map[domain.IdentityProvider]domain.ExternalSubject{domain.IdentityProviderYandex: {Provider: domain.IdentityProviderYandex, Subject: number}, domain.IdentityProviderTelegram: {Provider: domain.IdentityProviderTelegram, Subject: number}}}
	if initial != domain.IdentityProviderYandex && initial != domain.IdentityProviderTelegram {
		t.Fatal("unknown native initial provider")
	}
	control.users[f.Owner] = true
	control.users[f.OtherOwner] = true
	// All declared scopes and dynamic-key collections are owned before FIRST write.
	t.Cleanup(f.cleanup)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f.peer = &accountNativePeerV2{f: f, key: key, expected: make(map[string]url.Values), used: make(map[string]bool)}
	client := &http.Client{Transport: f.peer}
	yandex, err := yandexoauth.New(yandexoauth.Config{ClientID: "native-account-yandex", ClientSecret: "synthetic-yandex", RedirectURI: IdentityAccountNativeOriginV2 + "/auth/login/callback", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	telegram, err := telegramoidc.New(telegramoidc.Config{ClientID: "100000", ClientSecret: "synthetic-telegram", RedirectURI: IdentityAccountNativeOriginV2 + "/auth/telegram/callback", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	f.Yandex, f.Telegram = yandex, telegram
	providers := []IdentityAccountProviderV2{{Binding: domain.IdentityProviderBindingV2{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "native-account-yandex", CallbackURI: IdentityAccountNativeOriginV2 + "/auth/login/callback"}, Prover: yandex}, {Binding: domain.IdentityProviderBindingV2{Provider: domain.IdentityProviderTelegram, Issuer: telegramoidc.DefaultIssuer, ClientID: "100000", CallbackURI: IdentityAccountNativeOriginV2 + "/auth/telegram/callback"}, Prover: telegram}}
	f.Store, f.Service, err = NewWebIdentityRuntimeV2(wrapped, Options{}, WebIdentityRuntimeOptionsV2{Providers: providers, HMACKey: []byte("native-account-synthetic-key-32bytes"), SensitiveOperationsEnabled: true})
	if err != nil {
		t.Fatal("actual immutable native runtime construction", err)
	}
	f.Clock = accountNativeClockV2{f}
	at := f.now()
	tx, err := raw.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	identity := domain.ExternalIdentityV2{Version: 2, Subject: f.Subjects[initial], UserID: f.Owner, CreatedAt: at, UpdatedAt: at, State: domain.ExternalIdentityActiveV2, Revision: 1}
	if err := writeExternalIdentityV2(ctx, tx, identity); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range f.Tenants {
		m := domain.TenantMembership{TenantID: tenant, UserID: f.Owner, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: at, UpdatedAt: at}
		if err := putMembershipTx(ctx, tx, m); err != nil {
			t.Fatal(err)
		}
	}
	if targetConflict {
		target := domain.IdentityProviderTelegram
		if initial == target {
			target = domain.IdentityProviderYandex
		}
		other := identity
		other.Subject = f.Subjects[target]
		other.UserID = f.OtherOwner
		if err := writeExternalIdentityV2(ctx, tx, other); err != nil {
			t.Fatal(err)
		}
		m := domain.TenantMembership{TenantID: f.OtherTenant, UserID: f.OtherOwner, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: at, UpdatedAt: at}
		if err := putMembershipTx(ctx, tx, m); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	f.CanonicalSession = domain.Session{ID: domain.SessionID("native-canonical-" + nonce), TenantID: f.Tenants[0], CreatedBy: f.Owner, Status: domain.SessionActive, CreatedAt: at, UpdatedAt: at}
	owner := domain.SessionParticipant{TenantID: f.Tenants[0], SessionID: f.CanonicalSession.ID, UserID: f.Owner, Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: at, UpdatedAt: at}
	if err := f.Store.CreateSession(ctx, f.CanonicalSession, owner); err != nil {
		t.Fatal("real canonical session/participant baseline", err)
	}
	return f
}

func (f *IdentityAccountNativeFixtureV2) cleanup() {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f.control.mu.Lock()
	sessions := make([]domain.SecretDigest, 0, len(f.control.sessions))
	for d := range f.control.sessions {
		sessions = append(sessions, d)
	}
	children := make([]domain.SecretDigest, 0, len(f.control.challenges))
	for d := range f.control.challenges {
		children = append(children, d)
	}
	security := make([]accountNativeSecurityKeyV2, 0, len(f.control.security))
	for _, key := range f.control.security {
		security = append(security, key)
	}
	f.control.mu.Unlock()
	exec := func(q string, args ...any) {
		if _, err := f.db.ExecContext(ctx, q, args...); err != nil {
			f.t.Error("cleanup exact owned native account key", err)
		}
	}
	for _, d := range sessions {
		bucket, _ := webBucket(string(d))
		exec(`DELETE FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, bucket, d)
	}
	for _, d := range children {
		bucket, _ := webBucket(string(d))
		exec(`DELETE FROM oidc_login_challenges WHERE shard_bucket=$1 AND state_digest=$2`, bucket, d)
	}
	for _, key := range security {
		bucket, _ := webBucket(key.ID)
		exec(`DELETE FROM web_security_audit_events WHERE shard_bucket=$1 AND occurred_at=$2 AND request_id=$3`, bucket, key.At, key.ID)
	}
	for _, subject := range f.Subjects {
		identity, found, err := readExternalIdentityV2(ctx, f.db, subject)
		if err != nil || found && identity.UserID != f.Owner && identity.UserID != f.OtherOwner {
			f.t.Error("refuse cleanup of unowned/malformed forward identity", err)
			continue
		}
		if !found {
			continue
		}
		ub, _ := webBucket(string(identity.UserID))
		exec(`DELETE FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 AND provider=$3 AND subject=$4`, ub, identity.UserID, subject.Provider, subject.Subject)
		bucket, _ := webBucket(subject.String())
		exec(`DELETE FROM external_identities WHERE shard_bucket=$1 AND provider=$2 AND subject=$3`, bucket, subject.Provider, subject.Subject)
	}
	for _, tenant := range []domain.TenantID{f.Tenants[0], f.Tenants[1], f.OtherTenant} {
		user := f.Owner
		if tenant == f.OtherTenant {
			user = f.OtherOwner
		}
		bucket, _ := webBucket(string(user))
		exec(`DELETE FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`, bucket, user, tenant)
		exec(`DELETE FROM audit_events WHERE tenant_id=$1`, tenant) // entire newly owned tenant, never a shared scope
	}
	if f.CanonicalSession.ID != "" {
		activityBucket, _ := ydbpartition.BucketV1(string(f.CanonicalSession.ID))
		exec(`DELETE FROM session_activity WHERE tenant_id=$1 AND user_id=$2 AND status=$3 AND activity_bucket=$4 AND updated_at=$5 AND session_id=$6`, f.Tenants[0], f.Owner, domain.SessionActive, activityBucket, f.CanonicalSession.UpdatedAt, f.CanonicalSession.ID)
		exec(`DELETE FROM session_material_current_v1 WHERE tenant_id=$1 AND session_id=$2`, f.Tenants[0], f.CanonicalSession.ID)
		exec(`DELETE FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`, f.Tenants[0], f.CanonicalSession.ID, f.Owner)
		exec(`DELETE FROM sessions WHERE tenant_id=$1 AND session_id=$2`, f.Tenants[0], f.CanonicalSession.ID)
	}
}

func (f *IdentityAccountNativeFixtureV2) ObserveAuthorization(code, raw string) {
	f.t.Helper()
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 8192 || len(code) > 160 {
		f.t.Fatal("invalid observed native authorization")
	}
	f.peer.mu.Lock()
	defer f.peer.mu.Unlock()
	if len(f.peer.expected) >= 16 || f.peer.expected[code] != nil {
		f.t.Fatal("native authorization observation fixture cap/reuse")
	}
	f.peer.expected[code] = u.Query()
}
func (f *IdentityAccountNativeFixtureV2) LoseNextConfirmationReply(id string) {
	f.t.Helper()
	f.control.mu.Lock()
	defer f.control.mu.Unlock()
	digest := domain.DigestSecret(id)
	if !f.control.challenges[digest] || f.control.lose != "" || f.control.lostReplies != 0 {
		f.t.Fatal("lost reply requires one actual owned original operation")
	}
	f.control.lose = digest
}
func (f *IdentityAccountNativeFixtureV2) RevokeMembership(tenant domain.TenantID) {
	f.t.Helper()
	if tenant != f.Tenants[0] && tenant != f.Tenants[1] {
		f.t.Fatal("foreign membership mutation")
	}
	tx, err := f.db.BeginTx(f.ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		f.t.Fatal(err)
	}
	defer tx.Rollback()
	member, found, err := readMembershipTx(f.ctx, tx, f.Owner, tenant)
	if err != nil || !found {
		f.t.Fatal("original native membership missing", err)
	}
	member.Status = domain.TenantMembershipSuspended
	member.UpdatedAt = f.now()
	member.SecurityVersion++
	if err := putMembershipTx(f.ctx, tx, member); err != nil {
		f.t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		f.t.Fatal(err)
	}
}

func (f *IdentityAccountNativeFixtureV2) Snapshot() IdentityAccountNativeSnapshotV2 {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	result := IdentityAccountNativeSnapshotV2{Identities: make(map[domain.IdentityProvider]domain.ExternalIdentityV2), Active: make(map[domain.UserID][]domain.ExternalIdentityV2), Members: make(map[domain.TenantID]domain.TenantMembership), Sessions: make(map[domain.SecretDigest]domain.WebSessionV2), Operations: make(map[domain.SecretDigest]domain.IdentityOperationV2), Children: make(map[domain.SecretDigest]domain.IdentityChallengeV2), Scrubbed: make(map[domain.SecretDigest]bool), Audits: make(map[string]int)}
	tx, err := f.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		f.t.Fatal(err)
	}
	defer tx.Rollback()
	for provider, subject := range f.Subjects {
		identity, found, err := readExternalIdentityV2(ctx, tx, subject)
		if err != nil {
			f.t.Fatal(err)
		}
		if found {
			result.Identities[provider] = identity
		}
	}
	for _, user := range []domain.UserID{f.Owner, f.OtherOwner} {
		hasActive := false
		for _, identity := range result.Identities {
			hasActive = hasActive || identity.UserID == user && identity.State == domain.ExternalIdentityActiveV2
		}
		if hasActive {
			active, err := readActiveExternalIdentitiesV2(ctx, tx, user)
			if err != nil {
				f.t.Fatal(err)
			}
			result.Active[user] = active
		}
	}
	for _, tenant := range []domain.TenantID{f.Tenants[0], f.Tenants[1], f.OtherTenant} {
		user := f.Owner
		if tenant == f.OtherTenant {
			user = f.OtherOwner
		}
		member, found, err := readMembershipTx(ctx, tx, user, tenant)
		if err != nil {
			f.t.Fatal(err)
		}
		if found {
			result.Members[tenant] = member
		}
	}
	f.control.mu.Lock()
	sessions := make([]domain.SecretDigest, 0, len(f.control.sessions))
	for d := range f.control.sessions {
		sessions = append(sessions, d)
	}
	challenges := make([]domain.SecretDigest, 0, len(f.control.challenges))
	for d := range f.control.challenges {
		challenges = append(challenges, d)
	}
	result.WriteCommits = f.control.writeCommits
	result.LostReplies = f.control.lostReplies
	f.control.mu.Unlock()
	for _, digest := range sessions {
		session, found, err := readWebSessionV2(ctx, tx, digest)
		if err != nil {
			f.t.Fatal(err)
		}
		if found {
			result.Sessions[digest] = session
		}
	}
	for _, digest := range challenges {
		bucket, _ := webBucket(string(digest))
		var raw, pkce, nonce string
		var consumed time.Time
		err := tx.QueryRowContext(ctx, `SELECT SUBSTRING(CAST(record AS String),0,8193),pkce_verifier,nonce,consumed_at FROM oidc_login_challenges WHERE shard_bucket=$1 AND state_digest=$2`, bucket, digest).Scan(&raw, &pkce, &nonce, &consumed)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil || len(raw) > 8192 {
			f.t.Fatal("native owned challenge readback", err)
		}
		var tag struct {
			Purpose domain.IdentityRecordPurposeV2 `json:"purpose"`
		}
		if json.Unmarshal([]byte(raw), &tag) != nil {
			f.t.Fatal("native challenge tag")
		}
		if tag.Purpose == domain.IdentityPurposeOperationV2 {
			operation, err := domain.DecodeIdentityOperationV2([]byte(raw))
			if err != nil {
				f.t.Fatal(err)
			}
			result.Operations[digest] = operation
			result.Scrubbed[digest] = operation.Receipt != nil && pkce == "" && nonce == "" && consumed.Equal(operation.Receipt.CompletedAt)
		} else {
			child, err := domain.DecodeIdentityChallengeV2([]byte(raw))
			if err != nil {
				f.t.Fatal(err)
			}
			result.Children[digest] = child
			result.Scrubbed[digest] = child.ConsumedAt != nil && child.Secrets == nil && pkce == "" && nonce == "" && consumed.Equal(*child.ConsumedAt)
		}
	}
	for _, tenant := range []domain.TenantID{f.Tenants[0], f.Tenants[1], f.OtherTenant} {
		rows, err := tx.QueryContext(ctx, `SELECT action FROM audit_events WHERE tenant_id=$1 LIMIT 65`, tenant)
		if err != nil {
			f.t.Fatal(err)
		}
		count := 0
		for rows.Next() {
			count++
			var action string
			if err := rows.Scan(&action); err != nil {
				_ = rows.Close()
				f.t.Fatal(err)
			}
			result.Audits[action]++
		}
		err = errors.Join(rows.Err(), rows.Close())
		if err != nil || count > 64 {
			f.t.Fatal("owned audit readback bound", err)
		}
	}
	var sessionRaw, ownerRaw string
	if err := tx.QueryRowContext(ctx, `SELECT CAST(record AS String) FROM sessions WHERE tenant_id=$1 AND session_id=$2`, f.Tenants[0], f.CanonicalSession.ID).Scan(&sessionRaw); err != nil {
		f.t.Fatal(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT CAST(record AS String) FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`, f.Tenants[0], f.CanonicalSession.ID, f.Owner).Scan(&ownerRaw); err != nil {
		f.t.Fatal(err)
	}
	if json.Unmarshal([]byte(sessionRaw), &result.Session) != nil || json.Unmarshal([]byte(ownerRaw), &result.Participant) != nil || result.Session.Validate() != nil || result.Participant.Validate() != nil {
		f.t.Fatal("canonical native baseline readback invalid")
	}
	if err := tx.QueryRowContext(ctx, `SELECT record FROM session_material_current_v1 WHERE tenant_id=$1 AND session_id=$2`, f.Tenants[0], f.CanonicalSession.ID).Scan(&result.Material); err != nil || result.Material == "" {
		f.t.Fatal("real canonical genesis baseline", err)
	}
	if err := tx.Commit(); err != nil {
		f.t.Fatal(err)
	}
	f.peer.mu.Lock()
	result.Tokens, result.Infos, result.JWKS, result.Closed = f.peer.tokens, f.peer.infos, f.peer.jwks, f.peer.closed
	f.peer.mu.Unlock()
	return result
}

type accountNativePeerV2 struct {
	f                                  *IdentityAccountNativeFixtureV2
	key                                *rsa.PrivateKey
	mu                                 sync.Mutex
	expected                           map[string]url.Values
	used                               map[string]bool
	tokens, infos, jwks, closed, calls int
}
type accountNativeBodyV2 struct {
	io.Reader
	peer *accountNativePeerV2
}

func (b *accountNativeBodyV2) Close() error {
	b.peer.mu.Lock()
	b.peer.closed++
	b.peer.mu.Unlock()
	return nil
}
func (p *accountNativePeerV2) response(r *http.Request, body string) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, ContentLength: int64(len(body)), Body: &accountNativeBodyV2{Reader: strings.NewReader(body), peer: p}, Request: r}, nil
}
func (p *accountNativePeerV2) RoundTrip(r *http.Request) (*http.Response, error) {
	p.mu.Lock()
	p.calls++
	calls := p.calls
	p.mu.Unlock()
	if calls > 32 {
		return nil, errors.New("native fake provider finite call cap")
	}
	p.f.control.mu.Lock()
	active := p.f.control.active
	p.f.control.mu.Unlock()
	if active != 0 {
		p.f.t.Error("provider exchange inside native SQL transaction")
	}
	if r.URL.String() == telegramoidc.DefaultJWKSURL {
		p.mu.Lock()
		p.jwks++
		p.mu.Unlock()
		body, _ := json.Marshal(map[string]any{"keys": []any{map[string]string{"kid": "native-account-key", "kty": "RSA", "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(p.key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(p.key.E)).Bytes())}}})
		return p.response(r, string(body))
	}
	if r.URL.String() == yandexoauth.DefaultInfoEndpoint+"?format=json" {
		p.mu.Lock()
		p.infos++
		p.mu.Unlock()
		if r.Header.Get("Authorization") != "OAuth native-synthetic-access" {
			return nil, errors.New("native Yandex info authorization mismatch")
		}
		return p.response(r, fmt.Sprintf(`{"id":%q,"client_id":"native-account-yandex"}`, p.f.Subjects[domain.IdentityProviderYandex].Subject))
	}
	provider := domain.IdentityProviderYandex
	if r.URL.String() == telegramoidc.DefaultTokenEndpoint {
		provider = domain.IdentityProviderTelegram
	} else if r.URL.String() != yandexoauth.DefaultTokenEndpoint {
		return nil, errors.New("unexpected controlled native provider URL")
	}
	if r.Method != http.MethodPost || r.ParseForm() != nil {
		return nil, errors.New("native provider token request shape")
	}
	code := r.PostForm.Get("code")
	p.mu.Lock()
	q := p.expected[code]
	repeated := p.used[code]
	p.used[code] = true
	p.tokens++
	p.mu.Unlock()
	if q == nil || repeated {
		return nil, errors.New("native code missing actual authorization or re-exchanged")
	}
	expectedClient, secret := "native-account-yandex", "synthetic-yandex"
	if provider == domain.IdentityProviderTelegram {
		expectedClient, secret = "100000", "synthetic-telegram"
	}
	client, provided, ok := r.BasicAuth()
	challenge := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
	if !ok || client != expectedClient || provided != secret || q.Get("client_id") != expectedClient || r.PostForm.Get("redirect_uri") != q.Get("redirect_uri") || base64.RawURLEncoding.EncodeToString(challenge[:]) != q.Get("code_challenge") {
		return nil, errors.New("native provider client/callback/PKCE correspondence")
	}
	// A separate real read proves consume committed BEFORE any provider response.
	digest := domain.DigestSecret(q.Get("state"))
	bucket, _ := webBucket(string(digest))
	var raw, pkce, nonce string
	var consumed time.Time
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := p.f.db.QueryRowContext(ctx, `SELECT CAST(record AS String),pkce_verifier,nonce,consumed_at FROM oidc_login_challenges WHERE shard_bucket=$1 AND state_digest=$2`, bucket, digest).Scan(&raw, &pkce, &nonce, &consumed); err != nil {
		return nil, err
	}
	child, err := domain.DecodeIdentityChallengeV2([]byte(raw))
	if err != nil || child.ConsumedAt == nil || child.Secrets != nil || pkce != "" || nonce != "" || !consumed.Equal(*child.ConsumedAt) {
		return nil, errors.New("native provider I/O before committed scrub")
	}
	if provider == domain.IdentityProviderYandex {
		return p.response(r, `{"access_token":"native-synthetic-access","token_type":"bearer","expires_in":3600}`)
	}
	var at time.Time
	if err := p.f.db.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		return nil, err
	}
	at = at.UTC()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"iss": telegramoidc.DefaultIssuer, "aud": "100000", "sub": p.f.Subjects[provider].Subject, "iat": at.Unix(), "exp": at.Add(time.Hour).Unix(), "nonce": q.Get("nonce"), "auth_time": at.Unix()})
	token.Header["kid"] = "native-account-key"
	signed, err := token.SignedString(p.key)
	if err != nil {
		return nil, err
	}
	body, _ := json.Marshal(map[string]string{"id_token": signed})
	return p.response(r, string(body))
}
