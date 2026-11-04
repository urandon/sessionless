package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func explanationIdentityV2Fixture(t *testing.T, provider domain.IdentityProvider, revision domain.AuthRevisionV2, attached bool) (*Store, *ratedSQLFixture, ports.RunExplanationAuthorizationV1, domain.Run, domain.WebSessionV2, domain.ExternalIdentityV2) {
	t.Helper()
	store, fixture, auth, run := ratedReadFixture(t, attached)
	session := domain.WebSessionV2{Version: 2, IdentityRevision: revision, Session: ratedFixtureRecord[domain.WebSession](t, fixture, "web_sessions")}
	session.Session.AuthenticatedSubject.Provider = provider
	identity := domain.ExternalIdentityV2{Version: 2, Subject: session.Session.AuthenticatedSubject, UserID: auth.UserID, State: domain.ExternalIdentityActiveV2, Revision: revision, CreatedAt: session.Session.IssuedAt, UpdatedAt: session.Session.IssuedAt}
	putExplanationIdentityV2(t, fixture, session, identity)
	binding := domain.IdentityProviderBindingV2{Provider: provider, Issuer: domain.YandexOAuthIssuer, ClientID: "explanation-yandex", CallbackURI: "https://sessionless.example/auth/login/callback"}
	if provider == domain.IdentityProviderTelegram {
		binding.Issuer, binding.ClientID, binding.CallbackURI = "https://oauth.telegram.org", "explanation-telegram", "https://sessionless.example/auth/telegram/callback"
	}
	store.webIdentityV2 = &identityConfigurationV2{ManifestDigest: domain.DigestSecret("explanation-manifest"), Providers: []domain.IdentityProviderBindingV2{binding}}
	return store, fixture, auth, run, session, identity
}

func putExplanationIdentityV2(t *testing.T, fixture *ratedSQLFixture, session domain.WebSessionV2, identity domain.ExternalIdentityV2) {
	t.Helper()
	s, err := domain.EncodeWebSessionV2(session)
	if err != nil {
		t.Fatal(err)
	}
	i, err := domain.EncodeExternalIdentityV2(identity)
	if err != nil {
		t.Fatal(err)
	}
	fixture.rows["web_sessions"] = []driver.Value{string(s)}
	fixture.rows["external_identities"] = []driver.Value{string(i)}
}

func readExplanationIdentityV2(t *testing.T, store *Store, auth ports.RunExplanationAuthorizationV1, run domain.Run, rated bool) error {
	t.Helper()
	if rated {
		value, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "identity-request", run.ID)
		if err != nil && !reflect.DeepEqual(value, ports.RunExplanationReadOutcomeV1{}) {
			t.Fatal("failed rated authority leaked speculative output")
		}
		if err == nil && (value.Kind != ports.RunExplanationReadSuccessV1 || value.Explanation == nil || value.Explanation.RunID != run.ID || value.Validate() != nil) {
			t.Fatal("successful rated call lacks the genuine requested projection")
		}
		return err
	}
	value, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID)
	if err != nil && value.RunID != "" {
		t.Fatal("failed direct authority leaked a resource")
	}
	if err == nil && value.RunID != run.ID {
		t.Fatal("successful direct call lacks the genuine requested projection")
	}
	return err
}

func TestRunExplanationIdentityV2AuthorityDenialsBeforeDebit(t *testing.T) {
	for _, rated := range []bool{false, true} {
		for _, fault := range []string{"missing_identity", "unlinked", "stale_revision", "foreign_user", "foreign_subject", "future_identity", "future_session", "revoked_session", "expired_session", "disabled_provider", "membership_version", "suspended_membership", "legacy_cookie", "legacy_identity", "numeric_revision", "duplicate_revision", "unknown_field", "oversize_identity", "oversize_cookie", "expected_user"} {
			if rated && fault == "expected_user" {
				continue
			} // Rated API has no caller identity.
			name := "direct/" + fault
			if rated {
				name = "rated/" + fault
			}
			t.Run(name, func(t *testing.T) {
				store, f, auth, run, session, identity := explanationIdentityV2Fixture(t, domain.IdentityProviderTelegram, 4, false)
				at := f.rows["clock"][0].(time.Time)
				switch fault {
				case "unlinked":
					identity.State = domain.ExternalIdentityUnlinkedV2
				case "stale_revision":
					identity.Revision++
				case "foreign_user":
					identity.UserID = "another-user"
				case "foreign_subject":
					identity.Subject.Subject = "another-subject"
				case "future_identity":
					identity.UpdatedAt = at.Add(time.Microsecond)
				case "future_session":
					session.Session.LastSeenAt = at.Add(time.Microsecond)
				case "revoked_session":
					session.Session.RevokedAt = &at
				case "expired_session":
					session.Session.IdleExpiresAt = at
				}
				putExplanationIdentityV2(t, f, session, identity)
				switch fault {
				case "missing_identity":
					delete(f.rows, "external_identities")
				case "disabled_provider":
					store.webIdentityV2.Providers = []domain.IdentityProviderBindingV2{{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "different-client", CallbackURI: "https://sessionless.example/auth/login/callback"}}
				case "membership_version", "suspended_membership":
					member := ratedFixtureRecord[domain.TenantMembership](t, f, "tenant_memberships")
					if fault == "membership_version" {
						member.SecurityVersion++
					} else {
						member.Status = domain.TenantMembershipSuspended
					}
					ratedFixturePut(t, f, "tenant_memberships", member)
				case "legacy_cookie":
					ratedFixturePut(t, f, "web_sessions", session.Session)
				case "legacy_identity":
					ratedFixturePut(t, f, "external_identities", domain.ExternalIdentity{Subject: identity.Subject, UserID: identity.UserID, CreatedAt: identity.CreatedAt, UpdatedAt: identity.UpdatedAt})
				case "numeric_revision", "duplicate_revision", "unknown_field":
					raw := f.rows["external_identities"][0].(string)
					changed := ""
					if fault == "numeric_revision" {
						changed = strings.Replace(raw, `"security_revision":"4"`, `"security_revision":4`, 1)
					}
					if fault == "duplicate_revision" {
						changed = strings.Replace(raw, `"security_revision":"4"`, `"security_revision":"4","security_revision":"4"`, 1)
					}
					if fault == "unknown_field" {
						changed = strings.Replace(raw, `"version":2`, `"version":2,"extra":true`, 1)
					}
					if changed == raw {
						t.Fatal("malformed record mutation did not apply")
					}
					f.rows["external_identities"] = []driver.Value{changed}
				case "oversize_identity":
					f.rows["external_identities"] = []driver.Value{strings.Repeat("x", domain.IdentityAuthMaxRecordBytesV2+1)}
				case "oversize_cookie":
					f.rows["web_sessions"] = []driver.Value{strings.Repeat("x", domain.IdentityAuthMaxRecordBytesV2+1)}
				case "expected_user":
					auth.UserID = "untrusted-user"
				}
				if err := readExplanationIdentityV2(t, store, auth, run, rated); err == nil {
					t.Fatalf("%s did not deny", fault)
				}
				if f.writes != 0 || len(f.slots) != 0 {
					t.Error("invalid identity/member acquired a rate debit")
				}
				for _, statement := range f.statements {
					if strings.Contains(statement, "FROM runs ") || strings.Contains(statement, "FROM sessions ") || strings.Contains(statement, "run_explanation_rate_slots_v1") {
						t.Errorf("read resource/rate before current identity authority: %s", statement)
					}
				}
			})
		}
	}
}

func TestRunExplanationIdentityV2RetryRechecksAndSharesBudget(t *testing.T) {
	for _, rated := range []bool{false, true} {
		for _, revoke := range []bool{false, true} {
			name := "direct"
			if rated {
				name = "rated"
			}
			if revoke {
				name += "/revoked"
			} else {
				name += "/exhaustion"
			}
			t.Run(name, func(t *testing.T) {
				store, f, auth, run, session, identity := explanationIdentityV2Fixture(t, domain.IdentityProviderTelegram, 4, false)
				if revoke {
					f.commitModes = []string{"rollback"}
					f.afterCommit = func() { identity.Revision++; putExplanationIdentityV2(t, f, session, identity) }
				} else {
					f.commitModes = []string{"rollback", "rollback", "rollback", "rollback"}
				}
				if err := readExplanationIdentityV2(t, store, auth, run, rated); err == nil {
					t.Fatal("retry returned stale authorization or reset aggregate statement budget")
				}
				if len(f.statements) > 20 || f.commits < 1 || len(f.slots) != 0 {
					t.Errorf("retry escaped original20statement budget/rollback custody: statements=%d commits=%d slots=%d", len(f.statements), f.commits, len(f.slots))
				}
				if revoke {
					identityReads := 0
					for _, statement := range f.statements {
						if statement == webIdentityRowSQLV2 {
							identityReads++
						}
					}
					if identityReads != 2 {
						t.Errorf("identity revision not re-read in both original attempts: %d", identityReads)
					}
				} else if len(f.statements) != 20 {
					t.Errorf("expected exhaustion before statement21, got=%d", len(f.statements))
				}
			})
		}
	}
}

func TestRunExplanationIdentityV2StaleCookieAcrossTenants(t *testing.T) {
	for _, tenant := range []domain.TenantID{"tenant-test", "another-tenant"} {
		for _, rated := range []bool{false, true} {
			name := string(tenant) + "/direct"
			if rated {
				name = string(tenant) + "/rated"
			}
			t.Run(name, func(t *testing.T) {
				store, f, auth, run, session, identity := explanationIdentityV2Fixture(t, domain.IdentityProviderYandex, 9, false)
				session.Session.ActiveTenantID, auth.TenantID = tenant, tenant
				member := ratedFixtureRecord[domain.TenantMembership](t, f, "tenant_memberships")
				member.TenantID = tenant
				ratedFixturePut(t, f, "tenant_memberships", member)
				identity.Revision++
				putExplanationIdentityV2(t, f, session, identity)
				if err := readExplanationIdentityV2(t, store, auth, run, rated); err == nil || f.writes != 0 {
					t.Error("identity revision change did not revoke old cookie across tenants")
				}
			})
		}
	}
}

func TestRunExplanationIdentityV2CurrentTransaction(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		for _, revision := range []domain.AuthRevisionV2{1, 1<<53 - 1, 1 << 53, 1<<53 + 1, math.MaxUint64} {
			for _, rated := range []bool{false, true} {
				for _, attached := range []bool{false, true} {
					name := string(provider) + "/" + revision.String() + "/direct/historical"
					if rated {
						name = string(provider) + "/" + revision.String() + "/rated/historical"
					}
					if attached {
						name += "/attached"
					}
					t.Run(name, func(t *testing.T) {
						store, f, auth, run, session, _ := explanationIdentityV2Fixture(t, provider, revision, attached)
						beforeSession, beforeMember := f.rows["web_sessions"][0], f.rows["tenant_memberships"][0]
						if err := readExplanationIdentityV2(t, store, auth, run, rated); err != nil {
							t.Fatalf("current V2 identity denied by original transaction: %v", err)
						}
						want, writes := 8, 0
						if rated {
							want, writes = 13, 1
						}
						if attached {
							want += 5
						}
						if len(f.statements) != want || f.writes != writes || f.commits != 1 || len(f.isolations) != 1 || f.isolations[0] != driver.IsolationLevel(sql.LevelSerializable) || f.deadlineMissing {
							t.Fatalf("V2 must stay in one bounded owning transaction: statements=%d want=%d writes=%d commits=%d", len(f.statements), want, f.writes, f.commits)
						}
						identityReads := 0
						for index, statement := range f.statements {
							if strings.Contains(statement, "FROM external_identities ") {
								identityReads++
								args := f.arguments[index]
								bucket, _ := webBucket(session.Session.AuthenticatedSubject.String())
								if statement != webIdentityRowSQLV2 || len(args) != 3 || args[0].Value != int64(bucket) || args[1].Value != string(provider) || args[2].Value != session.Session.AuthenticatedSubject.Subject {
									t.Error("identity lookup is not the exact bounded authenticating subject")
								}
							}
						}
						if identityReads != 1 || beforeSession != f.rows["web_sessions"][0] || beforeMember != f.rows["tenant_memberships"][0] {
							t.Error("missing canonical fence or unexpected auth activity write")
						}
					})
				}
			}
		}
	}
}
