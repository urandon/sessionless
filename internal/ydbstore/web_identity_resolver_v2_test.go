package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Extend the existing transaction-owned snapshot, not a process-global mock or
// a second model of identity writes. This is not native YDB race evidence.
type identityResolverSQLV2 struct{ base *identityOperationSQLV2 }
type identityResolverConnV2 struct{ *identityOperationConnV2 }

func (f *identityResolverSQLV2) Driver() driver.Driver { return f.base.Driver() }
func (f *identityResolverSQLV2) Connect(context.Context) (driver.Conn, error) {
	return &identityResolverConnV2{&identityOperationConnV2{identityRowsV2Conn: &identityRowsV2Conn{fixture: f.base.base}, f: f.base}}, nil
}
func (c *identityResolverConnV2) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if statement != identityCandidateReverseSQLV2 && statement != identityCandidateMembershipSQLV2 {
		return c.identityOperationConnV2.QueryContext(ctx, statement, args)
	}
	f := c.f.base
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.statement(ctx, statement, args, false); err != nil {
		return nil, err
	}
	values := identityRowsV2Args(args)
	if len(values) != 2 {
		return nil, errors.New("candidate lookup requires exact user prefix")
	}
	user, ok := values[1].(string)
	if !ok || values[0] != identityRowsV2ExpectedBucket(user) {
		return nil, errors.New("candidate prefix bucket mismatch")
	}
	rows := &identityRowsV2Rows{fixture: f, columns: []string{"sentinel"}}
	if statement == identityCandidateReverseSQLV2 {
		for _, entry := range f.pending.reverse {
			if entry.user == domain.UserID(user) {
				rows.values = [][]driver.Value{{string(entry.subject.Provider)}}
				break
			}
		}
	} else {
		for key := range f.pending.members {
			if tenant, found := strings.CutPrefix(key, user+"\x00"); found {
				rows.values = [][]driver.Value{{tenant}}
				break
			}
		}
	}
	return rows, nil
}

func newIdentityResolverServingV2(t *testing.T) (*identityOperationSQLV2, *Store, context.Context, domain.ExternalIdentityV2) {
	t.Helper()
	f, _, ctx, _, config, _ := identityOperationConfirmationFixtureV2(t, domain.IdentityOperationLinkV2)
	db := sql.OpenDB(&identityResolverSQLV2{base: f})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := New(db, Options{})
	if err != nil {
		t.Fatal(err)
	}
	store.webIdentityV2 = &config
	return f, store, ctx, identityRowsV2Identity(domain.IdentityProviderYandex, 7)
}

func TestIdentityResolverServingV2InitializesOnlyFreshUserAtDatabaseTime(t *testing.T) {
	f, store, ctx, existing := newIdentityResolverServingV2(t)
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "new-provider-subject"}
	before := f.base.durable.clone()
	// The public historical clock argument is not database authority in V2.
	got, created, err := store.ResolveOrCreateExternalIdentity(ctx, subject, "fresh-server-user", f.at.Add(24*time.Hour))
	if err != nil || !created || got.UserID != "fresh-server-user" || got.Subject != subject || !got.CreatedAt.Equal(f.at) || !got.UpdatedAt.Equal(f.at) {
		t.Fatalf("initial mapping got=%+v created=%v err=%v DB=%s", got, created, err, f.at)
	}
	stored, err := domain.DecodeExternalIdentityV2([]byte(f.base.durable.forward[identityRowsV2SubjectKey(subject)]))
	if err != nil || stored.Revision != 1 || stored.State != domain.ExternalIdentityActiveV2 || stored.UserID != got.UserID {
		t.Errorf("not a closed initial V2 mapping: %+v err=%v", stored, err)
	}
	if !reflect.DeepEqual(f.base.durable.members, before.members) || !reflect.DeepEqual(f.base.durable.sessions, before.sessions) ||
		f.base.durable.forward[identityRowsV2SubjectKey(existing.Subject)] != before.forward[identityRowsV2SubjectKey(existing.Subject)] || len(f.audit) != 0 {
		t.Error("ordinary resolution changed existing user, sessions, grants or history")
	}
	if f.base.commits != 1 || f.base.rangeOpen != 0 || f.base.queries > 8 || f.base.writes != 2 {
		t.Errorf("unexpected transaction boundaries/bounds: commits=%d queries=%d writes=%d open=%d", f.base.commits, f.base.queries, f.base.writes, f.base.rangeOpen)
	}
}

func TestIdentityResolverServingV2PreservesExistingOwnerAndHighRevision(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(provider), func(t *testing.T) {
			f, store, ctx, identity := newIdentityResolverServingV2(t)
			identity.Subject.Provider, identity.Revision = provider, domain.AuthRevisionV2(math.MaxUint64)
			f.base.durable.forward = map[string]string{identityRowsV2SubjectKey(identity.Subject): identityRowsV2JSON(t, identity)}
			f.base.durable.reverse = map[string]identityRowsV2Projection{identityRowsV2ReverseKey(identity.UserID, identity.Subject): {user: identity.UserID, subject: identity.Subject, created: identity.CreatedAt}}
			before := f.base.durable.clone()
			got, created, err := store.ResolveOrCreateExternalIdentity(ctx, identity.Subject, "unrelated-new-candidate", time.Time{})
			if err != nil || created || got.UserID != identity.UserID || got.Subject != identity.Subject || !got.CreatedAt.Equal(identity.CreatedAt) {
				t.Fatalf("existing owner changed: got=%+v created=%v err=%v", got, created, err)
			}
			if !reflect.DeepEqual(before, f.base.durable) || f.base.writes != 0 {
				t.Error("existing high revision resolution rewrote canonical ownership")
			}
		})
	}
}

func TestIdentityResolverServingV2DeniesCandidateReuseWithoutWrites(t *testing.T) {
	for _, memberOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "existing_identity", true: "membership_without_identity"}[memberOnly], func(t *testing.T) {
			f, store, ctx, identity := newIdentityResolverServingV2(t)
			if memberOnly {
				f.base.durable.forward, f.base.durable.reverse = map[string]string{}, map[string]identityRowsV2Projection{}
			}
			before := f.base.durable.clone()
			got, created, err := store.ResolveOrCreateExternalIdentity(ctx, domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "unused"}, identity.UserID, f.at)
			if !errors.Is(err, domain.ErrExternalIdentityConflict) || created || got != (domain.ExternalIdentity{}) || f.base.writes != 0 || !reflect.DeepEqual(before, f.base.durable) {
				t.Fatalf("candidate reuse attached identity: got=%+v created=%v err=%v writes=%d", got, created, err, f.base.writes)
			}
		})
	}
}

func TestIdentityResolverServingV2DeniesTombstonesAndInconsistentActiveSet(t *testing.T) {
	for _, name := range []string{"tombstone", "missing_reverse", "different_revision", "future_identity", "legacy_record", "disabled_provider"} {
		t.Run(name, func(t *testing.T) {
			f, store, ctx, identity := newIdentityResolverServingV2(t)
			switch name {
			case "tombstone":
				identity.State = domain.ExternalIdentityUnlinkedV2
				f.base.durable.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
			case "missing_reverse":
				f.base.durable.reverse = map[string]identityRowsV2Projection{}
			case "different_revision":
				other := identityRowsV2Identity(domain.IdentityProviderTelegram, identity.Revision+1)
				f.base.durable.forward[identityRowsV2SubjectKey(other.Subject)] = identityRowsV2JSON(t, other)
				f.base.durable.reverse[identityRowsV2ReverseKey(other.UserID, other.Subject)] = identityRowsV2Projection{user: other.UserID, subject: other.Subject, created: other.CreatedAt}
			case "future_identity":
				identity.UpdatedAt = f.at.Add(time.Second)
				f.base.durable.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
			case "legacy_record":
				f.base.durable.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, domain.ExternalIdentity{Subject: identity.Subject, UserID: identity.UserID, CreatedAt: identity.CreatedAt, UpdatedAt: identity.UpdatedAt})
			case "disabled_provider":
				store.webIdentityV2.Providers = store.webIdentityV2.Providers[1:]
			}
			got, created, err := store.ResolveOrCreateExternalIdentity(ctx, identity.Subject, "fresh-candidate", f.at)
			if err == nil || created || got != (domain.ExternalIdentity{}) || f.base.writes != 0 {
				t.Fatalf("%s returned authority: got=%+v created=%v err=%v writes=%d", name, got, created, err, f.base.writes)
			}
		})
	}
}

func TestIdentityResolverServingV2RollbackAndAmbiguousCommitReturnNoSpeculativeIdentity(t *testing.T) {
	for _, lostCommit := range []bool{false, true} {
		t.Run(map[bool]string{false: "reverse_write_failure", true: "lost_commit_reply"}[lostCommit], func(t *testing.T) {
			f, store, ctx, _ := newIdentityResolverServingV2(t)
			before := f.base.durable.clone()
			if lostCommit {
				f.commitReplyError = errors.New("controlled lost commit response")
			} else {
				f.base.failStatement, f.base.failure = identityRowsV2ReverseWrite, errors.New("controlled projection failure")
			}
			got, created, err := store.ResolveOrCreateExternalIdentity(ctx, domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "fresh-subject"}, "fresh-user", f.at)
			if err == nil || created || got != (domain.ExternalIdentity{}) {
				t.Fatalf("speculative identity escaped: got=%+v created=%v err=%v", got, created, err)
			}
			if !lostCommit && (!reflect.DeepEqual(before, f.base.durable) || f.base.rollbacks != 1) {
				t.Error("failed reverse write did not roll back forward ownership")
			}
			if lostCommit && f.base.commits != 1 {
				t.Error("fixture failed to establish genuine commit-before-lost-response condition")
			}
		})
	}
}

func TestIdentityServingV2UnsupportedLegacyWritersDenyBeforeSQL(t *testing.T) {
	for _, name := range []string{"create_challenge", "consume_challenge", "enroll", "bootstrap", "bootstrap_subject", "telegram_ingress"} {
		t.Run(name, func(t *testing.T) {
			f, store, ctx, identity := newIdentityResolverServingV2(t)
			challenge := domain.OIDCLoginChallenge{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, Audience: "legacy-test-client", StateDigest: domain.DigestSecret("legacy-challenge"), BrowserBindingDigest: domain.DigestSecret("legacy-browser"), PKCEVerifier: strings.Repeat("p", 43), RedirectPath: "/", CreatedAt: f.at, ExpiresAt: f.at.Add(time.Minute)}
			grant := domain.DevelopmentBootstrapGrant{TenantID: "rows-tenant", UserID: identity.UserID, Role: domain.TenantMembershipOwner, Environment: domain.DevelopmentEnvironment, Operator: "controlled-test", Reason: "explicit test fixture only", GrantedAt: f.at}
			binding := domain.FrontendBindingID("existing-binding")
			enrollment := ports.EnrollmentRequest{Identity: domain.ExternalIdentity{Subject: identity.Subject, UserID: identity.UserID, CreatedAt: identity.CreatedAt, UpdatedAt: identity.UpdatedAt}, TenantID: grant.TenantID, Source: domain.EnrollmentExistingFrontend, FrontendBindingID: &binding, At: f.at}
			ingress := ports.TelegramIdentityRequest{TenantID: grant.TenantID, Actor: domain.ActorRef{TenantID: grant.TenantID, Frontend: domain.FrontendTelegram, ExternalID: "123", ID: "actor-123"}, Conversation: domain.ConversationRef{TenantID: grant.TenantID, Frontend: domain.FrontendTelegram, ExternalID: "456", ID: "conversation-456"}, SubscriptionConnectionID: "test-connection", Provider: "test-provider", ObservedAt: f.at}
			for _, validation := range []error{challenge.Validate(), grant.Validate(), validateEnrollmentRequest(enrollment), validateTelegramIdentity(ingress)} {
				if validation != nil {
					t.Fatalf("fixture is invalid before writer guard: %v", validation)
				}
			}
			var err error
			switch name {
			case "create_challenge":
				err = store.CreateLoginChallenge(ctx, challenge)
			case "consume_challenge":
				var got domain.OIDCLoginChallenge
				got, err = store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "legacy-browser", f.at)
				if !reflect.DeepEqual(got, domain.OIDCLoginChallenge{}) {
					t.Error("legacy challenge escaped")
				}
			case "enroll":
				_, err = store.Enroll(ctx, enrollment)
			case "bootstrap":
				_, err = store.BootstrapDevelopmentMembership(ctx, grant)
			case "bootstrap_subject":
				_, err = store.BootstrapDevelopmentMembershipForSubject(ctx, grant, identity.Subject)
			case "telegram_ingress":
				_, err = store.EnsureTelegramIdentity(ctx, ingress)
			}
			if !errors.Is(err, domain.ErrMembershipDenied) || f.base.begins != 0 || f.base.queries != 0 || f.base.writes != 0 {
				t.Errorf("%s escaped V1 writer guard: err=%v begins=%d queries=%d writes=%d", name, err, f.base.begins, f.base.queries, f.base.writes)
			}
		})
	}
}

func TestIdentityServingV2NotInstalledByPublicStoreOptions(t *testing.T) {
	_, configured, _, _ := newIdentityResolverServingV2(t)
	store, err := New(configured.db, Options{})
	if err != nil || store.webIdentityV2 != nil {
		t.Fatalf("partial serving mode exposed by public construction: err=%v", err)
	}
}
