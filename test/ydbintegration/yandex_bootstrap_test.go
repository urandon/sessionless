//go:build ydbintegration

package ydbintegration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type yandexBootstrapFixture struct {
	ctx   context.Context
	store *ydbstore.Store
	db    *sql.DB
	clock onboardingDatabaseClock
}

func newYandexBootstrapFixture(t *testing.T) yandexBootstrapFixture {
	t.Helper()
	store, client := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	return yandexBootstrapFixture{ctx: ctx, store: store, db: client.DB, clock: onboardingDatabaseClock{t: t, ctx: ctx, db: client.DB}}
}

func (f yandexBootstrapFixture) grant() domain.DevelopmentBootstrapGrant {
	return domain.DevelopmentBootstrapGrant{TenantID: domain.TenantID(uniqueID("yandex-bootstrap-tenant")), UserID: domain.UserID(uniqueID("yandex-bootstrap-user")), Role: domain.TenantMembershipOwner, Environment: domain.DevelopmentEnvironment, Operator: "yandex-bootstrap-ci", Reason: "explicit first-login provisioning proof", GrantedAt: f.clock.Now()}
}

func TestYandexBootstrapYDBAtomicFirstIdentityAndNoLink(t *testing.T) {
	t.Run("fresh-audited-idempotent", yandexBootstrapFreshIdentityAndAuditedMembershipAreAtomicAndIdempotent)
	t.Run("no-link-or-steal", yandexBootstrapDoesNotLinkIdentityOrStealAnotherSubject)
	t.Run("membership-conflict-rollback", yandexBootstrapMembershipConflictRollsBackBothIdentityIndexes)
	t.Run("concurrent-first-identities", yandexBootstrapConcurrentFirstIdentitiesHaveExactlyOneWinner)
}

func yandexBootstrapFreshIdentityAndAuditedMembershipAreAtomicAndIdempotent(t *testing.T) {
	f := newYandexBootstrapFixture(t)
	grant := f.grant()
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: uniqueID("yandex-bootstrap-subject")}
	member, err := f.store.BootstrapDevelopmentMembershipForSubject(f.ctx, grant, subject)
	if err != nil || member.UserID != grant.UserID || member.TenantID != grant.TenantID || member.Role != grant.Role || member.Status != domain.TenantMembershipActive {
		t.Fatalf("fresh first-login bootstrap membership=%+v grant=%+v err=%v", member, grant, err)
	}
	identity, created, err := f.store.ResolveOrCreateExternalIdentity(f.ctx, subject, domain.UserID(uniqueID("ignored-bootstrap-candidate")), f.clock.Now())
	if err != nil || created || identity.UserID != grant.UserID || identity.Subject != subject {
		t.Fatalf("committed exact identity=%+v created=%t err=%v", identity, created, err)
	}
	f.assertIdentityRows(t, subject, grant.UserID, 1)
	repeatedGrant := grant
	repeatedGrant.GrantedAt = f.clock.Now()
	repeated, err := f.store.BootstrapDevelopmentMembershipForSubject(f.ctx, repeatedGrant, subject)
	if err != nil || !reflect.DeepEqual(repeated, member) {
		t.Fatalf("idempotent bootstrap membership=%+v want=%+v err=%v", repeated, member, err)
	}
	memberships, err := f.store.ListTenantMemberships(f.ctx, grant.UserID, 10)
	if err != nil || len(memberships) != 1 || !reflect.DeepEqual(memberships[0], member) {
		t.Fatalf("fresh memberships=%+v want=%+v err=%v", memberships, member, err)
	}
	var auditCount uint64
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM audit_events WHERE tenant_id = $1 AND actor_id = $2 AND action = $3`, grant.TenantID, grant.UserID, "web.membership.cloud_dev_bootstrap").Scan(&auditCount); err != nil {
		t.Fatalf("read exact tenant bootstrap audit: %v", err)
	}
	if auditCount != 1 {
		t.Fatalf("bootstrap audit count=%d want=1", auditCount)
	}
	var auditMetadata string
	if err := f.db.QueryRowContext(f.ctx, `SELECT metadata FROM audit_events WHERE tenant_id = $1 AND occurred_at = $2 AND actor_id = $3 AND action = $4`, grant.TenantID, grant.GrantedAt, grant.UserID, "web.membership.cloud_dev_bootstrap").Scan(&auditMetadata); err != nil {
		t.Fatalf("read exact bootstrap audit metadata: %v", err)
	}
	var metadata map[string]any
	if err := json.Unmarshal([]byte(auditMetadata), &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["operator"] != grant.Operator || metadata["reason"] != grant.Reason || metadata["environment"] != domain.DevelopmentEnvironment {
		t.Fatalf("bootstrap audit metadata=%v", metadata)
	}
}

func yandexBootstrapDoesNotLinkIdentityOrStealAnotherSubject(t *testing.T) {
	for _, kind := range []string{"existing-telegram-user", "subject-owned-by-another-user", "changed-grant-role"} {
		t.Run(kind, func(t *testing.T) {
			f := newYandexBootstrapFixture(t)
			grant := f.grant()
			subject := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: uniqueID("rejected-yandex-bootstrap-subject")}
			subjectOwner := grant.UserID
			switch kind {
			case "existing-telegram-user":
				_, _, err := f.store.ResolveOrCreateExternalIdentity(f.ctx, domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: uniqueID("bootstrap-existing-telegram")}, grant.UserID, f.clock.Now())
				if err != nil {
					t.Fatalf("prepare existing Telegram identity: %v", err)
				}
			case "subject-owned-by-another-user":
				subjectOwner = domain.UserID(uniqueID("bootstrap-original-owner"))
				_, _, err := f.store.ResolveOrCreateExternalIdentity(f.ctx, subject, subjectOwner, f.clock.Now())
				if err != nil {
					t.Fatalf("prepare original subject owner: %v", err)
				}
			case "changed-grant-role":
				original := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: uniqueID("bootstrap-original-subject")}
				if _, err := f.store.BootstrapDevelopmentMembershipForSubject(f.ctx, grant, original); err != nil {
					t.Fatalf("prepare original audited grant: %v", err)
				}
				grant.Role = domain.TenantMembershipViewer
			}
			_, err := f.store.BootstrapDevelopmentMembershipForSubject(f.ctx, grant, subject)
			if !errors.Is(err, domain.ErrMembershipDenied) {
				t.Fatalf("rejected bootstrap case=%s err=%v want membership-denied", kind, err)
			}
			if kind == "subject-owned-by-another-user" {
				f.assertIdentityRows(t, subject, subjectOwner, 1)
				f.assertReverseIdentityRows(t, subject, grant.UserID, 0)
			} else {
				f.assertIdentityRows(t, subject, grant.UserID, 0)
			}
			memberships, err := f.store.ListTenantMemberships(f.ctx, grant.UserID, 10)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if kind == "changed-grant-role" {
				want = 1
			}
			if len(memberships) != want {
				t.Fatalf("denied operation memberships=%+v want count=%d", memberships, want)
			}
			if kind == "changed-grant-role" && memberships[0].Role != domain.TenantMembershipOwner {
				t.Fatalf("denied changed grant mutated membership=%+v", memberships[0])
			}
		})
	}
}

func yandexBootstrapMembershipConflictRollsBackBothIdentityIndexes(t *testing.T) {
	f := newYandexBootstrapFixture(t)
	grant := f.grant()
	original := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: uniqueID("orphan-original-subject")}
	identity, _, err := f.store.ResolveOrCreateExternalIdentity(f.ctx, original, grant.UserID, f.clock.Now())
	if err != nil {
		t.Fatalf("prepare identity through normal port: %v", err)
	}
	viewerGrant := grant
	viewerGrant.Role = domain.TenantMembershipViewer
	membership, err := f.store.Enroll(f.ctx, ports.EnrollmentRequest{Identity: identity, Source: domain.EnrollmentDevelopmentBootstrap, TenantID: grant.TenantID, Bootstrap: &viewerGrant, At: viewerGrant.GrantedAt})
	if err != nil {
		t.Fatalf("prepare existing membership through normal enrollment: %v", err)
	}
	// A deliberately corrupted legacy orphan is the negative fixture, not a
	// setup shortcut. Delete only these unique test-owned identity keys; keep
	// the port-created membership so rejection happens after prospective mapping.
	bucket, err := ydbpartition.BucketV1(original.String())
	if err != nil {
		t.Fatal(err)
	}
	userBucket, err := ydbpartition.BucketV1(string(grant.UserID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(f.ctx, `DELETE FROM external_identities WHERE shard_bucket = $1 AND provider = $2 AND subject = $3`, bucket, original.Provider, original.Subject); err != nil {
		t.Fatalf("prepare exact orphan forward index: %v", err)
	}
	if _, err := f.db.ExecContext(f.ctx, `DELETE FROM external_identities_by_user WHERE user_bucket = $1 AND user_id = $2 AND provider = $3 AND subject = $4`, userBucket, grant.UserID, original.Provider, original.Subject); err != nil {
		t.Fatalf("prepare exact orphan reverse index: %v", err)
	}
	newSubject := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: uniqueID("rollback-yandex-subject")}
	if _, err := f.store.BootstrapDevelopmentMembershipForSubject(f.ctx, grant, newSubject); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("conflicting membership error=%v want membership-denied", err)
	}
	f.assertIdentityRows(t, newSubject, grant.UserID, 0)
	memberships, err := f.store.ListTenantMemberships(f.ctx, grant.UserID, 10)
	if err != nil || len(memberships) != 1 || !reflect.DeepEqual(memberships[0], membership) {
		t.Fatalf("rollback preserved membership=%+v want=%+v err=%v", memberships, membership, err)
	}
	var grants uint64
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM development_bootstrap_grants WHERE tenant_id = $1 AND user_id = $2`, grant.TenantID, grant.UserID).Scan(&grants); err != nil {
		t.Fatal(err)
	}
	if grants != 0 {
		t.Fatalf("denied bootstrap left grants=%d want=0", grants)
	}
}

func yandexBootstrapConcurrentFirstIdentitiesHaveExactlyOneWinner(t *testing.T) {
	f := newYandexBootstrapFixture(t)
	user := domain.UserID(uniqueID("concurrent-bootstrap-user"))
	grants := []domain.DevelopmentBootstrapGrant{f.grant(), f.grant()}
	subjects := []domain.ExternalSubject{{Provider: domain.IdentityProviderYandex, Subject: uniqueID("bootstrap-contender-a")}, {Provider: domain.IdentityProviderYandex, Subject: uniqueID("bootstrap-contender-b")}}
	for i := range grants {
		grants[i].UserID = user
	}
	start := make(chan struct{})
	type outcome struct {
		index int
		err   error
	}
	outcomes := make(chan outcome, 2)
	var wait sync.WaitGroup
	for i := range grants {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			<-start
			_, err := f.store.BootstrapDevelopmentMembershipForSubject(f.ctx, grants[i], subjects[i])
			outcomes <- outcome{index: i, err: err}
		}(i)
	}
	close(start)
	wait.Wait()
	close(outcomes)
	winners := 0
	for result := range outcomes {
		if result.err == nil {
			winners++
			f.assertIdentityRows(t, subjects[result.index], user, 1)
		} else {
			if !errors.Is(result.err, domain.ErrMembershipDenied) {
				t.Fatalf("contender=%d err=%v want membership-denied", result.index, result.err)
			}
			f.assertIdentityRows(t, subjects[result.index], user, 0)
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent first-identity winners=%d want=1", winners)
	}
	memberships, err := f.store.ListTenantMemberships(f.ctx, user, 10)
	if err != nil || len(memberships) != 1 {
		t.Fatalf("only winning transaction grants membership: memberships=%+v err=%v", memberships, err)
	}
}

func (f yandexBootstrapFixture) assertIdentityRows(t *testing.T, subject domain.ExternalSubject, user domain.UserID, want uint64) {
	t.Helper()
	bucket, err := ydbpartition.BucketV1(subject.String())
	if err != nil {
		t.Fatal(err)
	}
	var forward uint64
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM external_identities WHERE shard_bucket = $1 AND provider = $2 AND subject = $3 AND user_id = $4`, bucket, subject.Provider, subject.Subject, user).Scan(&forward); err != nil {
		t.Fatalf("read exact forward identity index: %v", err)
	}
	if forward != want {
		t.Fatalf("forward identity provider=%s subject=%s user=%s rows=%d want=%d", subject.Provider, subject.Subject, user, forward, want)
	}
	f.assertReverseIdentityRows(t, subject, user, want)
}
func (f yandexBootstrapFixture) assertReverseIdentityRows(t *testing.T, subject domain.ExternalSubject, user domain.UserID, want uint64) {
	t.Helper()
	bucket, err := ydbpartition.BucketV1(string(user))
	if err != nil {
		t.Fatal(err)
	}
	var reverse uint64
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM external_identities_by_user WHERE user_bucket = $1 AND user_id = $2 AND provider = $3 AND subject = $4`, bucket, user, subject.Provider, subject.Subject).Scan(&reverse); err != nil {
		t.Fatalf("read exact reverse identity index: %v", err)
	}
	if reverse != want {
		t.Fatalf("reverse identity provider=%s subject=%s user=%s rows=%d want=%d", subject.Provider, subject.Subject, user, reverse, want)
	}
}
