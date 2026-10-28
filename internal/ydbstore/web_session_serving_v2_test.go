package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Per-test Connector extends the frozen canonical-row fixture; all sessions,
// memberships, identities and audits share one transaction snapshot. This is
// offline source evidence, not genuine YDB isolation or JsonDocument behavior.
type sessionServingSQLV2 struct {
	*identityOperationSQLV2
	rollbackOnce, bumpRevision bool
	retryAdvance               time.Duration
}
type sessionServingConnV2 struct {
	*identityOperationConnV2
	f *sessionServingSQLV2
}
type sessionServingTxV2 struct{ f *sessionServingSQLV2 }

func (f *sessionServingSQLV2) Connect(context.Context) (driver.Conn, error) {
	return &sessionServingConnV2{identityOperationConnV2: &identityOperationConnV2{identityRowsV2Conn: &identityRowsV2Conn{fixture: f.base}, f: f.identityOperationSQLV2}, f: f}, nil
}
func (c *sessionServingConnV2) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if _, err := c.identityOperationConnV2.BeginTx(ctx, options); err != nil {
		return nil, err
	}
	return &sessionServingTxV2{f: c.f}, nil
}
func (tx *sessionServingTxV2) Rollback() error {
	return (&identityOperationTxV2{f: tx.f.identityOperationSQLV2}).Rollback()
}
func (tx *sessionServingTxV2) Commit() error {
	f := tx.f
	if !f.rollbackOnce {
		return (&identityOperationTxV2{f: f.identityOperationSQLV2}).Commit()
	}
	f.base.mu.Lock()
	defer f.base.mu.Unlock()
	if f.base.pending == nil || f.base.rangeOpen != 0 {
		return errors.New("retry requires complete original snapshot")
	}
	f.rollbackOnce = false
	f.base.pending = nil
	f.pendingOperation, f.pendingAudit = "", nil
	f.base.rollbacks++
	f.at = f.at.Add(f.retryAdvance)
	if f.bumpRevision {
		for key, raw := range f.base.durable.forward {
			identity, err := domain.DecodeExternalIdentityV2([]byte(raw))
			if err != nil {
				return err
			}
			identity.Revision++
			encoded, err := domain.EncodeExternalIdentityV2(identity)
			if err != nil {
				return err
			}
			f.base.durable.forward[key] = string(encoded)
		}
	}
	return retry.RetryableError(errors.New("controlled serialization conflict"), retry.WithBackoff(retry.TypeNoBackoff))
}

const sessionServingAuditSQLTestV2 = `UPSERT INTO audit_events (tenant_id,occurred_at,audit_event_id,actor_id,action,subject_kind,subject_id,outcome,metadata,expire_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS JsonDocument),$10)`

func (c *sessionServingConnV2) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if identityRowsV2Statement(statement) != sessionServingAuditSQLTestV2 {
		return c.identityOperationConnV2.ExecContext(ctx, statement, args)
	}
	f := c.f
	f.base.mu.Lock()
	defer f.base.mu.Unlock()
	if err := f.base.statement(ctx, statement, args, true); err != nil {
		return nil, err
	}
	values := identityRowsV2Args(args)
	if len(values) != 10 || values[1] != f.at || values[7] != "succeeded" {
		return nil, errors.New("session audit not original database time/closed shape")
	}
	action, ok := values[4].(string)
	if !ok || (action != "web.login.succeeded" && action != "web.tenant.switched" && action != "web.logout") {
		return nil, errors.New("unexpected session audit action")
	}
	matched := false
	for _, raw := range f.base.pending.sessions {
		session, err := domain.DecodeWebSessionV2([]byte(raw))
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256([]byte(action + ":" + string(session.Session.SessionDigest)))
		if values[2] != "audit-web-v2-"+hex.EncodeToString(sum[:]) {
			continue
		}
		metadata, _ := json.Marshal(map[string]any{"provider": session.Session.AuthenticatedSubject.Provider, "membership_security_version": session.Session.MembershipSecurityVersion, "identity_revision": session.IdentityRevision})
		subjectKind, subjectID := "user", string(session.Session.UserID)
		if action == "web.tenant.switched" {
			subjectKind, subjectID = "tenant", string(session.Session.ActiveTenantID)
		}
		if values[0] != string(session.Session.ActiveTenantID) || values[3] != string(session.Session.UserID) || values[5] != subjectKind || values[6] != subjectID || values[8] != string(metadata) || values[9] != f.at.Add(defaultOperationalRetention) {
			return nil, errors.New("session audit key/privacy/revision/retention mismatch")
		}
		matched = true
	}
	if !matched {
		return nil, errors.New("session audit does not match exact canonical session")
	}
	f.pendingAudit = append(f.pendingAudit, values)
	return driver.RowsAffected(1), nil
}

func newSessionServingFixtureV2(t *testing.T) (*sessionServingSQLV2, *Store, context.Context, domain.ExternalIdentityV2, domain.WebSessionV2, domain.TenantMembership) {
	t.Helper()
	state, identity, session, membership := identityRowsV2Data(t)
	f := &sessionServingSQLV2{identityOperationSQLV2: &identityOperationSQLV2{base: &identityRowsV2Fixture{durable: state.clone(), queryLimit: 96}, at: identityRowsV2At()}}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	store, err := New(db, Options{})
	if err != nil {
		t.Fatal(err)
	}
	store.webIdentityV2 = &identityConfigurationV2{ManifestDigest: domain.DigestSecret("session-serving-manifest"), Providers: []domain.IdentityProviderBindingV2{{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "session-serving-yandex", CallbackURI: "https://sessionless.example/auth/login/callback"}}}
	return f, store, ctx, identity, session, membership
}

func sessionServingNextV2(previous domain.WebSession, membership domain.TenantMembership, at time.Time) domain.WebSession {
	next := previous
	next.SessionDigest, next.CSRFTokenDigest = domain.DigestSecret("serving-next-session"), domain.DigestSecret("serving-next-csrf")
	next.ActiveTenantID, next.MembershipSecurityVersion = membership.TenantID, membership.SecurityVersion
	next.IssuedAt, next.LastSeenAt, next.RevokedAt = at, at, nil
	next.IdleExpiresAt, next.AbsoluteExpiresAt = at.Add(time.Hour), at.Add(24*time.Hour)
	return next
}

func sessionServingSelectedV2(t *testing.T, f *sessionServingSQLV2, original domain.TenantMembership) domain.TenantMembership {
	t.Helper()
	selected := original
	selected.TenantID = "serving-other-tenant"
	selected.SecurityVersion++
	f.base.durable.members[identityRowsV2MemberKey(selected.UserID, selected.TenantID)] = identityRowsV2JSON(t, selected)
	return selected
}

func TestSessionServingV2CreationSnapshotsExactHighRevision(t *testing.T) {
	for _, revision := range []domain.AuthRevisionV2{1<<53 - 1, 1 << 53, 1<<53 + 1, math.MaxUint64} {
		t.Run(revision.String(), func(t *testing.T) {
			f, store, ctx, identity, session, _ := newSessionServingFixtureV2(t)
			identity.Revision = revision
			f.base.durable.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
			delete(f.base.durable.sessions, string(session.Session.SessionDigest))
			before := session.Session
			if err := store.CreateWebSession(ctx, before); err != nil {
				t.Fatal(err)
			}
			stored, err := domain.DecodeWebSessionV2([]byte(f.base.durable.sessions[string(before.SessionDigest)]))
			if err != nil || stored.IdentityRevision != revision || !reflect.DeepEqual(stored.Session, before) || len(f.audit) != 1 || f.base.rangeOpen != 0 || f.base.rangeClosed != 1 {
				t.Fatalf("creation lost exact immutable session/revision: revision=%s stored=%+v err=%v audits=%d", revision, stored, err, len(f.audit))
			}
			writes := f.base.writes
			if err := store.CreateWebSession(ctx, before); err != nil || f.base.writes != writes {
				t.Fatalf("exact current idempotent creation rewrote session/audit: err=%v writes=%d want=%d", err, f.base.writes, writes)
			}
		})
	}
}

func TestSessionServingV2CreationRechecksCanonicalSetAndTime(t *testing.T) {
	for _, name := range []string{"missing reverse", "tombstone", "foreign owner", "future identity", "future session", "expired session", "membership changed", "different immutable session", "V1 session", "disabled provider", "malformed config"} {
		t.Run(name, func(t *testing.T) {
			f, store, ctx, identity, session, membership := newSessionServingFixtureV2(t)
			switch name {
			case "missing reverse":
				delete(f.base.durable.reverse, identityRowsV2ReverseKey(identity.UserID, identity.Subject))
			case "tombstone":
				identity.State = domain.ExternalIdentityUnlinkedV2
			case "foreign owner":
				identity.UserID = "foreign-owner"
			case "future identity":
				identity.UpdatedAt = f.at.Add(time.Second)
			case "future session":
				session.Session.IssuedAt, session.Session.LastSeenAt = f.at.Add(time.Second), f.at.Add(time.Second)
			case "expired session":
				f.at = session.Session.IdleExpiresAt
			case "membership changed":
				membership.SecurityVersion++
			case "different immutable session":
				session.Session.CSRFTokenDigest = domain.DigestSecret("different-serving-csrf")
			case "V1 session":
				f.base.durable.sessions[string(session.Session.SessionDigest)] = identityRowsV2JSON(t, session.Session)
			case "disabled provider":
				store.webIdentityV2.Providers[0] = domain.IdentityProviderBindingV2{Provider: domain.IdentityProviderTelegram, Issuer: "https://oauth.telegram.org", ClientID: "telegram", CallbackURI: "https://sessionless.example/auth/telegram/callback"}
			case "malformed config":
				store.webIdentityV2.ManifestDigest = ""
			}
			f.base.durable.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
			f.base.durable.members[identityRowsV2MemberKey(membership.UserID, membership.TenantID)] = identityRowsV2JSON(t, membership)
			before := f.base.durable.clone()
			if err := store.createWebSessionV2(ctx, session.Session); err == nil || !reflect.DeepEqual(before, f.base.durable) || f.base.writes != 0 || len(f.audit) != 0 {
				t.Errorf("creation accepted %s or changed authority: err=%v writes=%d", name, err, f.base.writes)
			}
		})
	}
}

func TestSessionServingV2AuthorizationUsesDatabaseClockAndPreservesV2(t *testing.T) {
	f, store, ctx, _, session, membership := newSessionServingFixtureV2(t)
	f.at = f.at.Add(time.Minute)
	got, err := store.AuthorizeWebSession(ctx, session.Session.SessionDigest, domain.TenantPermissionRead, f.at.Add(100*365*24*time.Hour))
	if err != nil || got.Membership != membership || !got.Session.LastSeenAt.Equal(f.at) || !got.Session.IdleExpiresAt.Equal(f.at.Add(store.webSessionIdleTTL)) {
		t.Fatalf("authorization trusted caller time or lost membership: got=%+v err=%v", got, err)
	}
	stored, err := domain.DecodeWebSessionV2([]byte(f.base.durable.sessions[string(session.Session.SessionDigest)]))
	if err != nil || stored.IdentityRevision != session.IdentityRevision || stored.Version != 2 || len(f.audit) != 0 {
		t.Errorf("touch downgraded V2/current revision: got=%+v err=%v", stored, err)
	}
	f.at = got.Session.IdleExpiresAt
	got, err = store.AuthorizeWebSession(ctx, session.Session.SessionDigest, domain.TenantPermissionRead, session.Session.IssuedAt)
	if err == nil || !reflect.DeepEqual(got, ports.WebAuthorization{}) {
		t.Errorf("old caller time bypassed DB expiry: got=%+v err=%v", got, err)
	}
}

func TestSessionServingV2AuthorizationRejectsCurrentAuthorityFailures(t *testing.T) {
	for _, name := range []string{"missing session", "V1 session", "missing identity", "tombstone", "foreign owner", "stale revision", "future identity", "future session", "revoked session", "missing membership", "suspended membership", "changed membership", "future membership", "permission", "disabled provider"} {
		t.Run(name, func(t *testing.T) {
			f, store, ctx, identity, session, membership := newSessionServingFixtureV2(t)
			permission := domain.TenantPermissionRead
			switch name {
			case "missing session":
				delete(f.base.durable.sessions, string(session.Session.SessionDigest))
			case "V1 session":
				f.base.durable.sessions[string(session.Session.SessionDigest)] = identityRowsV2JSON(t, session.Session)
			case "missing identity":
				delete(f.base.durable.forward, identityRowsV2SubjectKey(identity.Subject))
			case "tombstone":
				identity.State = domain.ExternalIdentityUnlinkedV2
			case "foreign owner":
				identity.UserID = "foreign-owner"
			case "stale revision":
				identity.Revision++
			case "future identity":
				identity.UpdatedAt = f.at.Add(time.Second)
			case "future session":
				session.Session.LastSeenAt = f.at.Add(time.Second)
			case "revoked session":
				at := f.at
				session.Session.RevokedAt = &at
			case "missing membership":
				delete(f.base.durable.members, identityRowsV2MemberKey(membership.UserID, membership.TenantID))
			case "suspended membership":
				membership.Status = domain.TenantMembershipSuspended
			case "changed membership":
				membership.SecurityVersion++
			case "future membership":
				membership.UpdatedAt = f.at.Add(time.Second)
			case "permission":
				permission = "unknown-permission"
			case "disabled provider":
				store.webIdentityV2 = nil
			}
			if name != "missing identity" {
				f.base.durable.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
			}
			if name != "missing session" && name != "V1 session" {
				f.base.durable.sessions[string(session.Session.SessionDigest)] = identityRowsV2JSON(t, session)
			}
			if name != "missing membership" {
				f.base.durable.members[identityRowsV2MemberKey(membership.UserID, membership.TenantID)] = identityRowsV2JSON(t, membership)
			}
			before := f.base.durable.clone()
			got, err := store.authorizeWebSessionServingV2(ctx, session.Session.SessionDigest, permission, f.at.Add(-time.Hour))
			if err == nil || !reflect.DeepEqual(got, ports.WebAuthorization{}) || !reflect.DeepEqual(before, f.base.durable) || f.base.writes != 0 {
				t.Errorf("authorization accepted %s or leaked data/write: got=%+v err=%v writes=%d", name, got, err, f.base.writes)
			}
		})
	}
}

func TestSessionServingV2SwitchAtomicallyPreservesIdentityAndMembership(t *testing.T) {
	f, store, ctx, _, session, membership := newSessionServingFixtureV2(t)
	selected := sessionServingSelectedV2(t, f, membership)
	prepared := f.at.Add(-time.Second)
	next := sessionServingNextV2(session.Session, selected, prepared)
	got, err := store.SwitchTenant(ctx, session.Session.SessionDigest, next, selected.TenantID, prepared)
	if err != nil || !reflect.DeepEqual(got, ports.WebAuthorization{Session: next, Membership: selected}) || len(f.audit) != 1 {
		t.Fatalf("tenant switch=(%+v,%v) audits=%d", got, err, len(f.audit))
	}
	old, oldErr := domain.DecodeWebSessionV2([]byte(f.base.durable.sessions[string(session.Session.SessionDigest)]))
	rotated, newErr := domain.DecodeWebSessionV2([]byte(f.base.durable.sessions[string(next.SessionDigest)]))
	if oldErr != nil || newErr != nil || old.Session.RevokedAt == nil || !old.Session.RevokedAt.Equal(f.at) || rotated.IdentityRevision != session.IdentityRevision || rotated.Session.AuthenticatedSubject != session.Session.AuthenticatedSubject || !reflect.DeepEqual(rotated.Session, next) {
		t.Errorf("switch lost V2/rotation atomicity: old=%+v next=%+v errors=(%v,%v)", old, rotated, oldErr, newErr)
	}
}

func TestSessionServingV2SwitchRejectsChangedBindingsAndRollsBackAuditFailure(t *testing.T) {
	for _, name := range []string{"stale identity", "selected membership revoked", "current membership changed", "different subject", "different user", "same csrf", "future next", "next exists", "audit failure"} {
		t.Run(name, func(t *testing.T) {
			f, store, ctx, identity, session, membership := newSessionServingFixtureV2(t)
			selected := sessionServingSelectedV2(t, f, membership)
			prepared := f.at.Add(-time.Second)
			next := sessionServingNextV2(session.Session, selected, prepared)
			switch name {
			case "stale identity":
				identity.Revision++
				f.base.durable.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
			case "selected membership revoked":
				selected.Status = domain.TenantMembershipRevoked
				f.base.durable.members[identityRowsV2MemberKey(selected.UserID, selected.TenantID)] = identityRowsV2JSON(t, selected)
			case "current membership changed":
				membership.SecurityVersion++
				f.base.durable.members[identityRowsV2MemberKey(membership.UserID, membership.TenantID)] = identityRowsV2JSON(t, membership)
			case "different subject":
				next.AuthenticatedSubject.Subject = "different-subject"
			case "different user":
				next.UserID = "different-user"
			case "same csrf":
				next.CSRFTokenDigest = session.Session.CSRFTokenDigest
			case "future next":
				prepared = f.at.Add(time.Second)
				next = sessionServingNextV2(session.Session, selected, prepared)
			case "next exists":
				nextRecord := domain.WebSessionV2{Version: 2, Session: next, IdentityRevision: session.IdentityRevision}
				f.base.durable.sessions[string(next.SessionDigest)] = identityRowsV2JSON(t, nextRecord)
			case "audit failure":
				f.base.failStatement, f.base.failure = sessionServingAuditSQLTestV2, errors.New("controlled audit failure")
			}
			before := f.base.durable.clone()
			got, err := store.switchTenantV2(ctx, session.Session.SessionDigest, next, selected.TenantID, prepared)
			if err == nil || !reflect.DeepEqual(got, ports.WebAuthorization{}) || !reflect.DeepEqual(before, f.base.durable) || len(f.audit) != 0 || f.base.pending != nil {
				t.Errorf("switch accepted %s/partial effect: got=%+v err=%v audits=%d", name, got, err, len(f.audit))
			}
		})
	}
}

func TestSessionServingV2LogoutIsRevocationOnlyAndIdempotent(t *testing.T) {
	f, store, ctx, _, session, membership := newSessionServingFixtureV2(t)
	membership.Status = domain.TenantMembershipRevoked
	f.base.durable.members[identityRowsV2MemberKey(membership.UserID, membership.TenantID)] = identityRowsV2JSON(t, membership)
	f.at = session.Session.AbsoluteExpiresAt.Add(time.Second)
	if err := store.RevokeWebSession(ctx, session.Session.SessionDigest, session.Session.IssuedAt); err != nil {
		t.Fatal(err)
	}
	stored, err := domain.DecodeWebSessionV2([]byte(f.base.durable.sessions[string(session.Session.SessionDigest)]))
	if err != nil || stored.IdentityRevision != session.IdentityRevision || stored.Session.RevokedAt == nil || !stored.Session.RevokedAt.Equal(f.at) || len(f.audit) != 1 {
		t.Fatalf("logout lost V2 or DB time: stored=%+v err=%v audits=%d", stored, err, len(f.audit))
	}
	writes := f.base.writes
	if err := store.RevokeWebSession(ctx, session.Session.SessionDigest, f.at.Add(time.Hour)); err != nil || f.base.writes != writes {
		t.Errorf("repeat logout rewrote original revocation: err=%v writes=%d want=%d", err, f.base.writes, writes)
	}
	if err := store.RevokeWebSession(ctx, domain.DigestSecret("absent-session"), f.at); err != nil || f.base.writes != writes {
		t.Errorf("absent logout not idempotent: err=%v writes=%d want=%d", err, f.base.writes, writes)
	}
}

func TestSessionServingV2LogoutRejectsStaleIdentityAndNeverDowngrades(t *testing.T) {
	for _, name := range []string{"stale", "tombstone", "foreign", "V1", "audit failure"} {
		t.Run(name, func(t *testing.T) {
			f, store, ctx, identity, session, _ := newSessionServingFixtureV2(t)
			switch name {
			case "stale":
				identity.Revision++
			case "tombstone":
				identity.State = domain.ExternalIdentityUnlinkedV2
			case "foreign":
				identity.UserID = "foreign-owner"
			case "V1":
				f.base.durable.sessions[string(session.Session.SessionDigest)] = identityRowsV2JSON(t, session.Session)
			case "audit failure":
				f.base.failStatement, f.base.failure = sessionServingAuditSQLTestV2, errors.New("logout audit unavailable")
			}
			f.base.durable.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
			before := f.base.durable.clone()
			if err := store.revokeWebSessionV2(ctx, session.Session.SessionDigest, f.at); err == nil || !reflect.DeepEqual(before, f.base.durable) || len(f.audit) != 0 {
				t.Errorf("logout accepted %s or downgraded session: err=%v audits=%d", name, err, len(f.audit))
			}
		})
	}
}

func TestSessionServingV2RetryRejoinsCurrentAuthorityAndReturnsZero(t *testing.T) {
	for _, method := range []string{"authorize", "switch"} {
		t.Run(method, func(t *testing.T) {
			f, store, ctx, _, session, membership := newSessionServingFixtureV2(t)
			selected := sessionServingSelectedV2(t, f, membership)
			f.at = f.at.Add(time.Minute)
			prepared := f.at.Add(-time.Second)
			next := sessionServingNextV2(session.Session, selected, prepared)
			before := f.base.durable.clone()
			f.rollbackOnce, f.bumpRevision = true, true
			var got ports.WebAuthorization
			var err error
			if method == "authorize" {
				got, err = store.authorizeWebSessionServingV2(ctx, session.Session.SessionDigest, domain.TenantPermissionRead, prepared)
			} else {
				got, err = store.switchTenantV2(ctx, session.Session.SessionDigest, next, selected.TenantID, prepared)
			}
			if err == nil || !reflect.DeepEqual(got, ports.WebAuthorization{}) || !reflect.DeepEqual(before.sessions, f.base.durable.sessions) || !reflect.DeepEqual(before.members, f.base.durable.members) || len(f.audit) != 0 || f.base.begins != 2 || f.base.commits != 0 {
				t.Errorf("retry reused stale %s observation: got=%+v err=%v begins=%d commits=%d", method, got, err, f.base.begins, f.base.commits)
			}
		})
	}
}

func TestSessionServingV2AmbiguousCommitNeverReturnsAuthorization(t *testing.T) {
	for _, method := range []string{"authorize", "switch"} {
		t.Run(method, func(t *testing.T) {
			f, store, ctx, _, session, membership := newSessionServingFixtureV2(t)
			selected := sessionServingSelectedV2(t, f, membership)
			f.at = f.at.Add(time.Minute)
			prepared := f.at.Add(-time.Second)
			next := sessionServingNextV2(session.Session, selected, prepared)
			f.commitReplyError = errors.New("commit acknowledgement lost")
			var got ports.WebAuthorization
			var err error
			if method == "authorize" {
				got, err = store.authorizeWebSessionServingV2(ctx, session.Session.SessionDigest, domain.TenantPermissionRead, prepared)
			} else {
				got, err = store.switchTenantV2(ctx, session.Session.SessionDigest, next, selected.TenantID, prepared)
			}
			if err == nil || !reflect.DeepEqual(got, ports.WebAuthorization{}) || f.base.commits != 1 || f.base.pending != nil {
				t.Errorf("ambiguous %s returned grant/false rollback: got=%+v err=%v commits=%d", method, got, err, f.base.commits)
			}
		})
	}
}

func TestSessionServingV2AuditDistinctForSameTimeSessions(t *testing.T) {
	f, store, ctx, _, original, _ := newSessionServingFixtureV2(t)
	for _, suffix := range []string{"first", "second"} {
		session := original.Session
		session.SessionDigest, session.CSRFTokenDigest = domain.DigestSecret("session-"+suffix), domain.DigestSecret("csrf-"+suffix)
		if err := store.createWebSessionV2(ctx, session); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.audit) != 2 || f.audit[0][2] == f.audit[1][2] {
		t.Fatal("same-time sessions collided in audit key")
	}
	for _, event := range f.audit {
		metadata := event[8].(string)
		if strings.Contains(metadata, original.Session.AuthenticatedSubject.Subject) || strings.Contains(metadata, string(original.Session.SessionDigest)) {
			t.Error("audit exposed subject or cookie digest")
		}
	}
}

func TestSessionServingV2CreationCompleteTwoMethodSetAndBudget(t *testing.T) {
	for _, name := range []string{"two methods", "different revision", "current omitted", "excess sentinel", "duplicate provider"} {
		t.Run(name, func(t *testing.T) {
			f, store, ctx, identity, session, _ := newSessionServingFixtureV2(t)
			other := identityRowsV2Identity(domain.IdentityProviderTelegram, identity.Revision)
			if name == "different revision" {
				other.Revision++
			}
			if name == "duplicate provider" {
				other.Subject.Provider, other.Subject.Subject = identity.Subject.Provider, "different-provider-slot"
			}
			f.base.durable.forward[identityRowsV2SubjectKey(other.Subject)] = identityRowsV2JSON(t, other)
			f.base.durable.reverse[identityRowsV2ReverseKey(other.UserID, other.Subject)] = identityRowsV2Projection{user: other.UserID, subject: other.Subject, created: other.CreatedAt}
			if name == "current omitted" {
				delete(f.base.durable.reverse, identityRowsV2ReverseKey(identity.UserID, identity.Subject))
			}
			if name == "excess sentinel" {
				third := other
				third.Subject.Subject = "third-method"
				f.base.durable.forward[identityRowsV2SubjectKey(third.Subject)] = identityRowsV2JSON(t, third)
				f.base.durable.reverse[identityRowsV2ReverseKey(third.UserID, third.Subject)] = identityRowsV2Projection{user: third.UserID, subject: third.Subject, created: third.CreatedAt}
			}
			delete(f.base.durable.sessions, string(session.Session.SessionDigest))
			before := f.base.durable.clone()
			err := store.createWebSessionV2(ctx, session.Session)
			if name == "two methods" {
				if err != nil || f.base.queries != 6 || f.base.writes != 2 || f.base.rangeClosed != 1 {
					t.Errorf("complete bounded set rejected/unbounded: err=%v queries=%d want=6 writes=%d want=2 closed=%d", err, f.base.queries, f.base.writes, f.base.rangeClosed)
				}
			} else if err == nil || !reflect.DeepEqual(before, f.base.durable) || f.base.writes != 0 {
				t.Errorf("incomplete/inconsistent %s created session: err=%v writes=%d", name, err, f.base.writes)
			}
		})
	}
}

func TestSessionServingV2TouchCannotExtendAbsoluteExpiry(t *testing.T) {
	f, store, ctx, _, session, _ := newSessionServingFixtureV2(t)
	session.Session.IdleExpiresAt = session.Session.AbsoluteExpiresAt
	f.base.durable.sessions[string(session.Session.SessionDigest)] = identityRowsV2JSON(t, session)
	f.at = session.Session.AbsoluteExpiresAt.Add(-time.Second)
	got, err := store.authorizeWebSessionServingV2(ctx, session.Session.SessionDigest, domain.TenantPermissionRead, time.Time{})
	if err != nil || !got.Session.IdleExpiresAt.Equal(session.Session.AbsoluteExpiresAt) || !got.Session.AbsoluteExpiresAt.Equal(session.Session.AbsoluteExpiresAt) || !got.Session.IssuedAt.Equal(session.Session.IssuedAt) {
		t.Errorf("touch extended immutable absolute expiry: got=%+v err=%v", got, err)
	}
}

func TestSessionServingV2CreationRetryPreservesPreparedLifetime(t *testing.T) {
	f, store, ctx, _, session, _ := newSessionServingFixtureV2(t)
	delete(f.base.durable.sessions, string(session.Session.SessionDigest))
	f.rollbackOnce, f.retryAdvance = true, time.Minute
	if err := store.createWebSessionV2(ctx, session.Session); err != nil {
		t.Fatal(err)
	}
	stored, err := domain.DecodeWebSessionV2([]byte(f.base.durable.sessions[string(session.Session.SessionDigest)]))
	if err != nil || !reflect.DeepEqual(stored, session) || f.base.begins != 2 || f.base.commits != 1 || len(f.audit) != 1 {
		t.Errorf("retry reset prepared identity/lifetime: stored=%+v err=%v begins=%d commits=%d audits=%d", stored, err, f.base.begins, f.base.commits, len(f.audit))
	}
}

func TestSessionServingV2RetryCannotRenewExpiredSession(t *testing.T) {
	f, store, ctx, _, session, _ := newSessionServingFixtureV2(t)
	f.at = session.Session.IdleExpiresAt.Add(-time.Second)
	f.rollbackOnce, f.retryAdvance = true, time.Second
	before := f.base.durable.clone()
	got, err := store.authorizeWebSessionServingV2(ctx, session.Session.SessionDigest, domain.TenantPermissionRead, session.Session.IssuedAt)
	if err == nil || !reflect.DeepEqual(got, ports.WebAuthorization{}) || !reflect.DeepEqual(before, f.base.durable) || f.base.begins != 2 || f.base.commits != 0 || f.base.writes != 1 {
		t.Errorf("retry renewed expired canonical session: got=%+v err=%v begins=%d commits=%d writes=%d", got, err, f.base.begins, f.base.commits, f.base.writes)
	}
}

func TestSessionServingV2SwitchCannotAuthorizeAtPreparedTime(t *testing.T) {
	f, store, ctx, _, session, membership := newSessionServingFixtureV2(t)
	selected := sessionServingSelectedV2(t, f, membership)
	prepared := session.Session.IdleExpiresAt.Add(-time.Second)
	next := sessionServingNextV2(session.Session, selected, prepared)
	f.at = session.Session.IdleExpiresAt
	before := f.base.durable.clone()
	got, err := store.switchTenantV2(ctx, session.Session.SessionDigest, next, selected.TenantID, prepared)
	if err == nil || !reflect.DeepEqual(got, ports.WebAuthorization{}) || !reflect.DeepEqual(before, f.base.durable) || f.base.writes != 0 {
		t.Errorf("prepared timestamp authorized expired old session: got=%+v err=%v writes=%d", got, err, f.base.writes)
	}
}
