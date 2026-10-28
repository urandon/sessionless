package ydbstore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"math"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
)

// This gate exercises the real row primitives in the existing migrated,
// disposable CI database. It does not prove the still separate public
// operation/proof/audit/cookie protocol, nor enable V2 on a serving instance.
func newWebIdentityV2NativeFixture(t *testing.T) (*sql.DB, context.Context, domain.ExternalIdentityV2, domain.WebSessionV2, domain.TenantMembership) {
	t.Helper()
	if os.Getenv("SESSIONLESS_WEB_IDENTITY_V2_YDB_GATE") != "1" {
		t.Skip("explicit disposable CI V2 identity gate not requested")
	}
	u, err := url.Parse(os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil || os.Getenv("GITHUB_ACTIONS") != "true" || os.Getenv("SESSIONLESS_WEB_IDENTITY_V2_YDB_DISPOSABLE") != "1" ||
		(u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") ||
		u.Query().Get("go_query_mode") != "data" || u.Query().Has("go_fake_tx") {
		t.Fatal("requires disposable migrated CI loopback database with real data transactions")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	client, err := ydbclient.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := client.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	var at time.Time
	if err := client.DB.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil || at.IsZero() {
		t.Fatalf("sample owning database time: %v", err)
	}
	at = at.UTC()
	id := rand.Text()
	identity := domain.ExternalIdentityV2{Version: 2, Subject: domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "auth-v2-" + id}, UserID: domain.UserID("auth-v2-user-" + id), CreatedAt: at, UpdatedAt: at, State: domain.ExternalIdentityActiveV2, Revision: 1}
	membership := domain.TenantMembership{TenantID: domain.TenantID("auth-v2-tenant-" + id), UserID: identity.UserID, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: at, UpdatedAt: at}
	session := domain.WebSessionV2{Version: 2, IdentityRevision: 1, Session: domain.WebSession{SessionDigest: domain.DigestSecret("auth-v2-session-" + id), CSRFTokenDigest: domain.DigestSecret("auth-v2-csrf-" + id), UserID: identity.UserID, ActiveTenantID: membership.TenantID, AuthenticatedSubject: identity.Subject, MembershipSecurityVersion: membership.SecurityVersion, IssuedAt: at, LastSeenAt: at, IdleExpiresAt: at.Add(30 * time.Minute), AbsoluteExpiresAt: at.Add(time.Hour)}}
	userBucket, err := webBucket(string(identity.UserID))
	if err != nil {
		t.Fatal(err)
	}
	identityBucket, err := webBucket(identity.Subject.String())
	if err != nil {
		t.Fatal(err)
	}
	sessionBucket, err := webBucket(string(session.Session.SessionDigest))
	if err != nil {
		t.Fatal(err)
	}
	// Exact newly generated keys only. Cleanup is armed before any write and
	// remains bounded even after a failed read or partially prepared test.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		for _, deletion := range []struct {
			statement string
			args      []any
		}{
			{`DELETE FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, []any{sessionBucket, session.Session.SessionDigest}},
			{`DELETE FROM external_identities WHERE shard_bucket=$1 AND provider=$2 AND subject=$3`, []any{identityBucket, identity.Subject.Provider, identity.Subject.Subject}},
			{`DELETE FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 AND provider=$3 AND subject=$4`, []any{userBucket, identity.UserID, identity.Subject.Provider, identity.Subject.Subject}},
			{`DELETE FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`, []any{userBucket, identity.UserID, membership.TenantID}},
		} {
			if _, err := client.DB.ExecContext(cleanup, deletion.statement, deletion.args...); err != nil {
				t.Errorf("cleanup exact V2 fixture key: %v", err)
			}
		}
	})
	return client.DB, ctx, identity, session, membership
}

func webIdentityV2NativeTx(t *testing.T, ctx context.Context, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("rollback V2 fixture transaction: %v", err)
		}
	})
	return tx
}

func TestWebIdentityV2YDBLosslessRevisionAndCurrentJoin(t *testing.T) {
	for _, revision := range []uint64{1<<53 - 1, 1 << 53, 1<<53 + 1, math.MaxUint64} {
		t.Run(domain.AuthRevisionV2(revision).String(), func(t *testing.T) {
			db, ctx, identity, session, membership := newWebIdentityV2NativeFixture(t)
			identity.Revision = domain.AuthRevisionV2(revision)
			session.IdentityRevision = identity.Revision
			write := webIdentityV2NativeTx(t, ctx, db)
			if err := writeExternalIdentityV2(ctx, write, identity); err != nil {
				t.Fatal("write exact V2 identity", err)
			}
			if err := putMembershipTx(ctx, write, membership); err != nil {
				t.Fatal("write exact current membership", err)
			}
			if err := writeWebSessionV2(ctx, write, session); err != nil {
				t.Fatal("write V2 session", err)
			}
			if err := write.Commit(); err != nil {
				t.Fatal("commit real V2 rows", err)
			}
			read := webIdentityV2NativeTx(t, ctx, db)
			active, err := readActiveExternalIdentitiesV2(ctx, read, identity.UserID)
			if err != nil || !reflect.DeepEqual(active, []domain.ExternalIdentityV2{identity}) {
				t.Fatalf("actual stored JsonDocument identity revision=%s: active=%+v err=%v", identity.Revision, active, err)
			}
			var at time.Time
			if err := read.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
				t.Fatal(err)
			}
			got, member, err := authorizeWebSessionV2(ctx, read, session.Session.SessionDigest, domain.TenantPermissionWrite, at.UTC())
			if err != nil || !reflect.DeepEqual(got, session) || !reflect.DeepEqual(member, membership) {
				t.Fatalf("actual stored V2 session/current identity join revision=%s: got=%+v member=%+v err=%v", identity.Revision, got, member, err)
			}
			if revision == math.MaxUint64 {
				if next, err := domain.NextAuthRevisionV2(identity.Revision); !errors.Is(err, domain.ErrAuthRevisionOverflowV2) || next != 0 {
					t.Fatalf("stored MaxUint64 increment: next=%s err=%v", next, err)
				}
			}
			if err := read.Commit(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWebIdentityV2YDBRevisionAndMembershipRevokeDenyOldCookie(t *testing.T) {
	for _, mutation := range []string{"identity_revision", "membership_revoked"} {
		t.Run(mutation, func(t *testing.T) {
			db, ctx, identity, session, membership := newWebIdentityV2NativeFixture(t)
			setup := webIdentityV2NativeTx(t, ctx, db)
			if err := writeExternalIdentityV2(ctx, setup, identity); err != nil {
				t.Fatal(err)
			}
			if err := putMembershipTx(ctx, setup, membership); err != nil {
				t.Fatal(err)
			}
			if err := writeWebSessionV2(ctx, setup, session); err != nil {
				t.Fatal(err)
			}
			if err := setup.Commit(); err != nil {
				t.Fatal(err)
			}
			change := webIdentityV2NativeTx(t, ctx, db)
			if mutation == "identity_revision" {
				identity.Revision++
				if err := writeExternalIdentityV2(ctx, change, identity); err != nil {
					t.Fatal(err)
				}
			} else {
				membership.Status = domain.TenantMembershipRevoked
				membership.SecurityVersion++
				if err := putMembershipTx(ctx, change, membership); err != nil {
					t.Fatal(err)
				}
			}
			if err := change.Commit(); err != nil {
				t.Fatal(err)
			}
			read := webIdentityV2NativeTx(t, ctx, db)
			var at time.Time
			if err := read.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
				t.Fatal(err)
			}
			got, member, err := authorizeWebSessionV2(ctx, read, session.Session.SessionDigest, domain.TenantPermissionWrite, at.UTC())
			if !errors.Is(err, domain.ErrWebSessionRevoked) || !reflect.DeepEqual(got, domain.WebSessionV2{}) || !reflect.DeepEqual(member, domain.TenantMembership{}) {
				t.Fatalf("old cookie survived committed %s: session=%+v member=%+v err=%v", mutation, got, member, err)
			}
		})
	}
}
