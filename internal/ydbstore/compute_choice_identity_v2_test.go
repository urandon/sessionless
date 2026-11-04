package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Drives the ACTUAL private LIST Store path and authTx. Identity/config/SQL
// DATA are controlled, not a public installation, SEND, provider or native YDB
// qualification. No presented CSRF value is accepted by this private reader.
type computeChoiceIdentityFixtureV2 struct {
	f        *computeChoiceSourceFixture
	rate     *computeChoiceRateFixture
	catalog  *computeChoiceAttachedCatalog
	session  domain.WebSessionV2
	identity domain.ExternalIdentityV2
}

func newComputeChoiceIdentityFixtureV2(t *testing.T, provider domain.IdentityProvider, revision uint64, tenant domain.TenantID) *computeChoiceIdentityFixtureV2 {
	t.Helper()
	f, rate := newComputeChoiceRateFixture(t)
	f.cookie.ActiveTenantID = tenant
	f.cookie.AuthenticatedSubject = domain.ExternalSubject{Provider: provider, Subject: "compute-identity-subject"}
	f.member.TenantID, f.session.TenantID, f.participant.TenantID = tenant, tenant, tenant
	f.cookie.SessionDigest = domain.DigestSecret("compute-cookie-" + string(tenant))
	_, keys, err := computechoice.ListRateIdentityKeys(tenant, f.cookie.UserID)
	if err != nil {
		t.Fatal(err)
	}
	rate.keys = keys
	f.store.webIdentityV2 = &identityConfigurationV2{ManifestDigest: domain.DigestSecret("compute-private-v2-manifest"), Providers: []domain.IdentityProviderBindingV2{
		{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "compute-yandex", CallbackURI: "https://sessionless.example/auth/login/callback"},
		{Provider: domain.IdentityProviderTelegram, Issuer: "https://oauth.telegram.org", ClientID: "compute-telegram", CallbackURI: "https://sessionless.example/auth/telegram/callback"},
	}}
	v := &computeChoiceIdentityFixtureV2{f: f, rate: rate, session: domain.WebSessionV2{Version: 2, Session: f.cookie, IdentityRevision: domain.AuthRevisionV2(revision)}, identity: domain.ExternalIdentityV2{Version: 2, Subject: f.cookie.AuthenticatedSubject, UserID: f.cookie.UserID, CreatedAt: f.at.Add(-2 * time.Hour), State: domain.ExternalIdentityActiveV2, Revision: domain.AuthRevisionV2(revision), UpdatedAt: f.at.Add(-time.Minute)}}
	codec, err := computechoice.NewCursorCodec(bytes.Repeat([]byte{57}, 32))
	if err != nil {
		t.Fatal(err)
	}
	pin := v.pin()
	entries := make([]computeChoiceAttachedRegistration, 4)
	for i := range entries {
		entries[i] = computeChoiceAttachedRegistration{enumeration: computechoice.Registration{TenantID: tenant, OwnerUserID: f.cookie.UserID, ChoiceID: fmt.Sprintf("identity-choice-%d", i), RegistrationRevision: 1, DisclosureRevision: strings.Repeat("d", 64)}, pin: pin}
	}
	v.catalog, err = newComputeChoiceAttachedCatalog(f.pins, codec, entries)
	if err != nil {
		t.Fatal(err)
	}
	v.sync(t)
	return v
}

func (v *computeChoiceIdentityFixtureV2) pin() computeChoiceAttachedPin {
	return computeChoiceAttachedPin{tenantID: v.session.Session.ActiveTenantID, ownerUserID: v.session.Session.UserID, subscriptionID: "owned-subscription", actorID: "owned-actor", provider: "codex", workerID: "owned-worker", connectionID: "owned-connection", enrollmentGeneration: 1, connectionGeneration: 1, workerRevision: 1, connectionRevision: 1, capabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("c", 64))}
}

func (v *computeChoiceIdentityFixtureV2) sync(t *testing.T) {
	t.Helper()
	v.f.cookie = v.session.Session
	v.f.syncRows(t)
	session, err := domain.EncodeWebSessionV2(v.session)
	if err != nil {
		t.Fatal("valid V2 session fixture", err)
	}
	identity, err := domain.EncodeExternalIdentityV2(v.identity)
	if err != nil {
		t.Fatal("valid V2 identity fixture", err)
	}
	v.f.driver.rows[computeChoiceCookieSQL][0] = string(session)
	v.f.driver.rows[webIdentityRowSQLV2] = []driver.Value{string(identity)}
	bucket, err := webBucket(v.session.Session.AuthenticatedSubject.String())
	if err != nil {
		t.Fatal(err)
	}
	v.f.driver.expected[webIdentityRowSQLV2] = []driver.Value{int64(bucket), string(v.session.Session.AuthenticatedSubject.Provider), v.session.Session.AuthenticatedSubject.Subject}
	p := v.pin()
	v.f.driver.expected[computeChoiceAttachedSubscriptionSQL] = []driver.Value{string(p.tenantID), string(p.subscriptionID)}
	v.f.driver.expected[computeChoiceAttachedActorSQL] = []driver.Value{string(p.tenantID), string(p.actorID)}
	v.f.driver.expected[computeChoiceAttachedWorkerSQL] = []driver.Value{string(p.tenantID), string(p.ownerUserID), string(p.workerID)}
	v.f.driver.expected[computeChoiceAttachedConnectionSQL] = []driver.Value{string(p.tenantID), string(p.ownerUserID), string(p.workerID)}
	v.f.driver.expected[computeChoiceAttachedSlotSQL] = []driver.Value{string(p.tenantID), string(p.subscriptionID)}
	v.f.driver.expected[computeChoiceAttachedCountersSQL] = []driver.Value{string(p.tenantID)}
}

func (v *computeChoiceIdentityFixtureV2) list(id string) (computeChoiceAttachedPage, error) {
	return v.f.store.readRatedComputeChoiceAttachedPage(context.Background(), v.f.cookie.SessionDigest, v.f.session.ID, computeChoiceRateInput(), id, v.catalog)
}

func (v *computeChoiceIdentityFixtureV2) positive(t *testing.T, id string) {
	t.Helper()
	start, writes, commits := len(v.rate.statements), v.rate.writes, v.rate.commits
	page, err := v.list(id)
	if err != nil || page.rateLimited || len(page.candidates) != 4 || page.more || page.throughSequence != v.f.session.LastEventSequence || !page.readAt.Equal(v.f.at) || v.rate.writes != writes+1 || v.rate.commits != commits+1 || len(v.rate.statements)-start != 36 {
		t.Fatalf("actual V2 LIST baseline not established: candidates=%d SQL=%d writes=%d commits=%d err=%v", len(page.candidates), len(v.rate.statements)-start, v.rate.writes, v.rate.commits, err)
	}
	want := []string{"SELECT CurrentUtcTimestamp()", computeChoiceCookieSQL, webIdentityRowSQLV2, computeChoiceMembershipSQL, computeChoiceSessionSQL, computeChoiceParticipantSQL, computeChoiceCutoverSQL}
	if !reflect.DeepEqual(v.rate.statements[start:start+7], want) || v.rate.deadlineMissing {
		t.Fatal("V2 auth/cutover did not preserve exact owning transaction/query order")
	}
	for _, c := range page.candidates {
		if c.registration.OwnerUserID != v.f.cookie.UserID || c.registration.TenantID != v.f.cookie.ActiveTenantID || c.sources.state != computeChoiceAttachedMissing {
			t.Fatal("missing sources became authority or crossed owner")
		}
	}
}

func (v *computeChoiceIdentityFixtureV2) denied(t *testing.T, id string) error {
	t.Helper()
	start, writes, commits := len(v.rate.statements), v.rate.writes, v.rate.commits
	page, err := v.list(id)
	if err == nil || !reflect.DeepEqual(page, computeChoiceAttachedPage{}) || v.rate.writes != writes || v.rate.commits != commits {
		t.Fatalf("invalid current V2 authority disclosed/debited/committed: SQL=%d writes=%d commits=%d err=%v", len(v.rate.statements)-start, v.rate.writes, v.rate.commits, err)
	}
	for _, statement := range v.rate.statements[start:] {
		if statement == computeChoiceRateReadSQL || statement == computeChoiceRateWriteSQL || statement == computeChoiceAttachedSubscriptionSQL || statement == computeChoiceCutoverSQL {
			t.Fatal("failed current identity/session authority reached limiter/cutover/candidates")
		}
	}
	return err
}

func TestComputeChoiceIdentityV2ActualListProvidersRevisionsAndReplay(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		for _, revision := range []uint64{1, (1 << 53) - 1, 1 << 53, (1 << 53) + 1, math.MaxUint64} {
			t.Run(fmt.Sprintf("%s_%d", provider, revision), func(t *testing.T) {
				v := newComputeChoiceIdentityFixtureV2(t, provider, revision, "choice-tenant")
				v.positive(t, "v2-current-page")
				before := len(v.rate.statements)
				page, err := v.list("v2-current-page")
				if err != nil || len(page.candidates) != 4 || page.rateLimited || v.rate.writes != 1 || len(v.rate.statements)-before != 35 {
					t.Fatalf("valid replay skipped fresh identity/resources or double-debited: SQL=%d writes=%d err=%v", len(v.rate.statements)-before, v.rate.writes, err)
				}
				v.positive(t, "v2-second-page")
				before = len(v.rate.statements)
				page, err = v.list("v2-rate-denied")
				if err != nil || !page.rateLimited || len(page.candidates) != 0 || len(v.rate.statements)-before != 11 || v.rate.writes != 2 {
					t.Fatal("rate denial did not retain current V2 auth and zero candidates", err)
				}
			})
		}
	}
}

func TestComputeChoiceIdentityV2CrossTenantRevisionFence(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(provider), func(t *testing.T) {
			a := newComputeChoiceIdentityFixtureV2(t, provider, 7, "choice-tenant-a")
			b := newComputeChoiceIdentityFixtureV2(t, provider, 7, "choice-tenant-b")
			// BOTH tenant/session/catalog/limiter baselines must work BEFORE the
			// shared canonical identity revision changes. A foreign-tenant catalog
			// or stale membership cannot supply a vacuous identity denial.
			a.positive(t, "tenant-a-current")
			b.positive(t, "tenant-b-current")
			for _, v := range []*computeChoiceIdentityFixtureV2{a, b} {
				v.identity.Revision++
				v.sync(t)
				if !errors.Is(v.denied(t, "old-cookie"), domain.ErrWebSessionRevoked) {
					t.Fatal("old cookie not fenced by current shared identity")
				}
			}
		})
	}
}

func replaceComputeIdentityWireV2(t *testing.T, v *computeChoiceIdentityFixtureV2, statement, from, to string) {
	t.Helper()
	raw := v.f.driver.rows[statement][0].(string)
	if strings.Count(raw, from) != 1 {
		t.Fatal("wire mutation must be effective and unique")
	}
	v.f.driver.rows[statement][0] = strings.Replace(raw, from, to, 1)
}

func TestComputeChoiceIdentityV2ActualListCurrentDenials(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, *computeChoiceIdentityFixtureV2)
	}{
		{"stale", func(t *testing.T, v *computeChoiceIdentityFixtureV2) { v.identity.Revision++; v.sync(t) }},
		{"missing", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) { delete(v.f.driver.rows, webIdentityRowSQLV2) }},
		{"unlinked", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.identity.State = domain.ExternalIdentityUnlinkedV2
			v.sync(t)
		}},
		{"foreign_user", func(t *testing.T, v *computeChoiceIdentityFixtureV2) { v.identity.UserID = "foreign-user"; v.sync(t) }},
		{"foreign_subject", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.identity.Subject.Subject = "foreign-subject"
			v.sync(t)
		}},
		{"future_updated", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.identity.UpdatedAt = v.f.at.Add(time.Microsecond)
			v.sync(t)
		}},
		{"future_created", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.identity.CreatedAt = v.f.at.Add(time.Microsecond)
			v.identity.UpdatedAt = v.identity.CreatedAt
			v.sync(t)
		}},
		{"malformed", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) { v.f.driver.rows[webIdentityRowSQLV2][0] = "{" }},
		{"identity_oversize", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.driver.rows[webIdentityRowSQLV2][0] = strings.Repeat("x", 8193)
		}},
		{"cookie_oversize", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.driver.rows[computeChoiceCookieSQL][0] = strings.Repeat("x", 8193)
		}},
		{"identity_alias", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			replaceComputeIdentityWireV2(t, v, webIdentityRowSQLV2, `"security_revision"`, `"Security_Revision"`)
		}},
		{"identity_duplicate", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			replaceComputeIdentityWireV2(t, v, webIdentityRowSQLV2, `"security_revision":"1"`, `"security_revision":"1","security_revision":"1"`)
		}},
		{"identity_numeric_revision", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			replaceComputeIdentityWireV2(t, v, webIdentityRowSQLV2, `"security_revision":"1"`, `"security_revision":1`)
		}},
		{"identity_unknown", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			replaceComputeIdentityWireV2(t, v, webIdentityRowSQLV2, `"version":2`, `"version":2,"foreign":true`)
		}},
		{"cookie_v1_no_fallback", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.driver.rows[computeChoiceCookieSQL][0] = choiceFixtureJSON(t, v.f.cookie)
		}},
		{"cookie_alias", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			replaceComputeIdentityWireV2(t, v, computeChoiceCookieSQL, `"identity_revision"`, `"Identity_Revision"`)
		}},
		{"cookie_numeric_revision", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			replaceComputeIdentityWireV2(t, v, computeChoiceCookieSQL, `"identity_revision":"1"`, `"identity_revision":1`)
		}},
		{"cookie_duplicate", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			replaceComputeIdentityWireV2(t, v, computeChoiceCookieSQL, `"identity_revision":"1"`, `"identity_revision":"1","identity_revision":"1"`)
		}},
		{"cookie_key_correspondence", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			replaceComputeIdentityWireV2(t, v, computeChoiceCookieSQL, string(v.f.cookie.SessionDigest), strings.Repeat("f", 64))
		}},
		{"csrf_scalar", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.driver.rows[computeChoiceCookieSQL][3] = strings.Repeat("c", 64)
		}},
		{"user_scalar", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.driver.rows[computeChoiceCookieSQL][1] = "foreign-user"
		}},
		{"tenant_scalar", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.driver.rows[computeChoiceCookieSQL][2] = "foreign-tenant"
		}},
		{"version_scalar", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.driver.rows[computeChoiceCookieSQL][4] = int64(8)
		}},
		{"idle_scalar", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.driver.rows[computeChoiceCookieSQL][5] = v.f.cookie.IdleExpiresAt.Add(time.Microsecond)
		}},
		{"absolute_scalar", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.driver.rows[computeChoiceCookieSQL][6] = v.f.cookie.AbsoluteExpiresAt.Add(time.Microsecond)
		}},
		{"revoked_scalar", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.driver.rows[computeChoiceCookieSQL][7] = v.f.at
		}},
		{"cookie_revoked", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			at := v.f.at
			v.session.Session.RevokedAt = &at
			v.sync(t)
		}},
		{"cookie_expired", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.session.Session.IdleExpiresAt = v.f.at
			v.sync(t)
		}},
		{"membership_write", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.member.Role = domain.TenantMembershipViewer
			v.sync(t)
		}},
		{"membership_revoked", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.member.Status = domain.TenantMembershipRevoked
			v.sync(t)
		}},
		{"membership_revision", func(t *testing.T, v *computeChoiceIdentityFixtureV2) { v.f.member.SecurityVersion++; v.sync(t) }},
		{"participant_missing", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			delete(v.f.driver.rows, computeChoiceParticipantSQL)
		}},
		{"participant_scalar", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.driver.rows[computeChoiceParticipantSQL][2] = "revoked"
		}},
		{"current_session_missing", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			delete(v.f.driver.rows, computeChoiceSessionSQL)
		}},
		{"future_session", func(t *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.session.UpdatedAt = v.f.at.Add(time.Microsecond)
			v.sync(t)
		}},
		{"disabled_provider", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.store.webIdentityV2.Providers = v.f.store.webIdentityV2.Providers[1:]
		}},
		{"invalid_configuration", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) { v.f.store.webIdentityV2.ManifestDigest = "" }},
		{"invalid_binding", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) {
			v.f.store.webIdentityV2.Providers[0].ClientID = ""
		}},
		{"v1_mode_rejects_v2_wire", func(_ *testing.T, v *computeChoiceIdentityFixtureV2) { v.f.store.webIdentityV2 = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newComputeChoiceIdentityFixtureV2(t, domain.IdentityProviderYandex, 1, "choice-tenant")
			v.positive(t, "valid-before-mutation")
			tc.change(t, v)
			v.denied(t, "invalid-after-mutation")
		})
	}
}

func TestComputeChoiceIdentityV2ClosedRevisionWireAtListBoundary(t *testing.T) {
	for _, record := range []struct{ statement, field string }{{webIdentityRowSQLV2, "security_revision"}, {computeChoiceCookieSQL, "identity_revision"}} {
		for i, wire := range []string{`"0"`, `"01"`, `"+1"`, `" 1"`, `"1.0"`, `"1e0"`, `"18446744073709551616"`, `null`, `1`} {
			t.Run(fmt.Sprintf("%s_%d", record.field, i), func(t *testing.T) {
				v := newComputeChoiceIdentityFixtureV2(t, domain.IdentityProviderYandex, 1, "choice-tenant")
				v.positive(t, "valid-wire-baseline")
				replaceComputeIdentityWireV2(t, v, record.statement, `"`+record.field+`":"1"`, `"`+record.field+`":`+wire)
				v.denied(t, "invalid-revision-wire")
			})
		}
	}
	t.Run("disabled_telegram", func(t *testing.T) {
		v := newComputeChoiceIdentityFixtureV2(t, domain.IdentityProviderTelegram, 1, "choice-tenant")
		v.positive(t, "valid-telegram")
		v.f.store.webIdentityV2.Providers = v.f.store.webIdentityV2.Providers[:1]
		v.denied(t, "disabled-telegram")
	})
}

func TestComputeChoiceIdentityV2RetryRereadsBeforeRateAndCandidates(t *testing.T) {
	for _, mode := range []string{"rollback", "ambiguous", "unknown", "valid_rollback", "valid_lost_ack"} {
		t.Run(mode, func(t *testing.T) {
			v := newComputeChoiceIdentityFixtureV2(t, domain.IdentityProviderYandex, 1, "choice-tenant")
			commit := mode
			if mode == "valid_rollback" {
				commit = "rollback"
			}
			if mode == "valid_lost_ack" {
				commit = "ambiguous"
			}
			v.rate.commitModes = []string{commit}
			if mode == "rollback" || mode == "ambiguous" {
				v.rate.afterCommit = func() { v.identity.Revision++; v.sync(t) }
			}
			page, err := v.list("same-original-list")
			switch mode {
			case "valid_rollback", "valid_lost_ack":
				wantWrites, wantSQL := 2, 72
				if mode == "valid_lost_ack" {
					wantWrites, wantSQL = 1, 71
				}
				if err != nil || len(page.candidates) != 4 || v.rate.commits != 2 || v.rate.writes != wantWrites || len(v.rate.statements) != wantSQL || len(v.rate.slots) != 1 {
					t.Fatalf("valid retry not freshly authorized/exactly debited: SQL=%d writes=%d commits=%d slots=%d err=%v", len(v.rate.statements), v.rate.writes, v.rate.commits, len(v.rate.slots), err)
				}
			case "unknown":
				if err == nil || !reflect.DeepEqual(page, computeChoiceAttachedPage{}) || v.rate.commits != 1 || v.rate.writes != 1 {
					t.Fatal("unknown commit disclosed speculative page or replayed", err)
				}
			default:
				if !errors.Is(err, domain.ErrWebSessionRevoked) || !reflect.DeepEqual(page, computeChoiceAttachedPage{}) || v.rate.commits != 1 || v.rate.writes != 1 || len(v.rate.statements) != 40 {
					t.Fatalf("identity changed before retry leaked cached page: SQL=%d writes=%d commits=%d err=%v", len(v.rate.statements), v.rate.writes, v.rate.commits, err)
				}
				if !reflect.DeepEqual(v.rate.statements[36:], []string{"SELECT CurrentUtcTimestamp()", computeChoiceCookieSQL, webIdentityRowSQLV2, computeChoiceMembershipSQL}) {
					t.Fatal("stale retry reached rate/candidate graph")
				}
				if (len(v.rate.slots) == 1) != (mode == "ambiguous") {
					t.Fatal("scripted committed-vs-rollback outcome lost")
				}
			}
		})
	}
}

// Supplemental budget seam: the production LIST owns the same query/budget.
// Precharging never mints a page; it proves sticky admission BEFORE driver I/O
// and exact transferred bytes at the added identity materialization boundary.
func TestComputeChoiceIdentityV2MaterializationAndStatementBounds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fill     uint32
		oversize bool
	}{
		{name: "identity_8193_prefix", oversize: true},
		{name: "statement_64_before_identity", fill: 62},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newComputeChoiceIdentityFixtureV2(t, domain.IdentityProviderYandex, 1, "choice-tenant")
			if tc.oversize {
				v.f.driver.rows[webIdentityRowSQLV2][0] = strings.Repeat("x", 8193)
			}
			budget := computechoice.NewResourceBudget()
			ctx, cancel := context.WithTimeout(context.Background(), computechoice.ResourceDeadline)
			defer cancel()
			var pending computeChoiceAuth
			err := v.f.store.authTx(ctx, "compute-choice.v2-budget-fixture", func(tx *sql.Tx) error {
				a, err := budget.NewAttempt()
				if err != nil {
					return err
				}
				defer a.Finish()
				for i := uint32(0); i < tc.fill; i++ {
					if err := a.BeforeStatement(); err != nil {
						return err
					}
				}
				q := &computeChoiceQuery{tx: tx, budget: a}
				pending, err = v.f.store.readComputeChoiceAuth(ctx, q, v.f.cookie.SessionDigest, v.f.session.ID)
				if err == nil {
					return errors.New("expected budget denial")
				}
				before := len(v.rate.statements)
				_, _, again := readComputeChoiceIdentityV2(ctx, q, v.session.Session.AuthenticatedSubject)
				if !errors.Is(again, computechoice.ErrResourceBudgetExceeded) || len(v.rate.statements) != before {
					t.Fatal("sticky denial issued another identity query", again)
				}
				return err
			})
			if !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || pending != (computeChoiceAuth{}) || !budget.Snapshot().Failed || v.rate.commits != 0 || v.rate.writes != 0 {
				t.Fatal("V2 budget failure leaked/reset scope", err, budget.Snapshot())
			}
			if tc.oversize {
				cookie := v.f.driver.rows[computeChoiceCookieSQL][0].(string)
				wantBytes := uint64(8 + 4*8 + len(cookie) + len(v.f.cookie.UserID) + len(v.f.cookie.ActiveTenantID) + len(v.f.cookie.CSRFTokenDigest) + 8193)
				if budget.Snapshot().TotalBytes != wantBytes || len(v.rate.statements) != 3 {
					t.Fatalf("oversize prefix was not charged before denial: got=%d want=%d SQL=%d", budget.Snapshot().TotalBytes, wantBytes, len(v.rate.statements))
				}
			} else if budget.Snapshot().TotalStatements != 64 || len(v.rate.statements) != 2 {
				t.Fatal("denied identity statement reached driver", budget.Snapshot(), len(v.rate.statements))
			}
		})
	}
}

func TestComputeChoiceIdentityV2ByteExhaustionAtIdentityIsSticky(t *testing.T) {
	for _, total := range []bool{false, true} {
		t.Run(fmt.Sprintf("total_%t", total), func(t *testing.T) {
			v := newComputeChoiceIdentityFixtureV2(t, domain.IdentityProviderYandex, 1, "choice-tenant")
			cookie := v.f.driver.rows[computeChoiceCookieSQL][0].(string)
			identity := v.f.driver.rows[webIdentityRowSQLV2][0].(string)
			transferred := uint64(8 + 4*8 + len(cookie) + len(v.f.cookie.UserID) + len(v.f.cookie.ActiveTenantID) + len(v.f.cookie.CSRFTokenDigest) + len(identity))
			ceiling := uint64(computechoice.MaxMaterializedBytesPerAttempt)
			if total {
				ceiling = computechoice.MaxMaterializedBytesTotal
			}
			budget := computechoice.NewResourceBudget()
			charge := func(a *computechoice.AttemptBudget, n uint64) {
				for n > 0 {
					part := min(n, uint64(computechoice.MaxConnectionRecordBytes))
					if err := a.RecordMaterialized(computechoice.SourceConnection, int64(part)); err != nil {
						t.Fatal("valid precharge", err)
					}
					n -= part
				}
			}
			precharged := uint64(0)
			if total {
				a, err := budget.NewAttempt()
				if err != nil {
					t.Fatal(err)
				}
				charge(a, computechoice.MaxMaterializedBytesPerAttempt)
				if err := a.Finish(); err != nil {
					t.Fatal(err)
				}
				precharged = computechoice.MaxMaterializedBytesPerAttempt
			}
			ctx, cancel := context.WithTimeout(context.Background(), computechoice.ResourceDeadline)
			defer cancel()
			err := v.f.store.authTx(ctx, "compute-choice.v2-byte-fixture", func(tx *sql.Tx) error {
				a, err := budget.NewAttempt()
				if err != nil {
					return err
				}
				defer a.Finish()
				charge(a, ceiling-transferred+1-precharged)
				q := &computeChoiceQuery{tx: tx, budget: a}
				got, err := v.f.store.readComputeChoiceAuth(ctx, q, v.f.cookie.SessionDigest, v.f.session.ID)
				if !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || got != (computeChoiceAuth{}) {
					t.Fatal("identity materialization did not exhaust at original boundary", err)
				}
				before := len(v.rate.statements)
				_, _, again := readComputeChoiceIdentityV2(ctx, q, v.session.Session.AuthenticatedSubject)
				if !errors.Is(again, computechoice.ErrResourceBudgetExceeded) || len(v.rate.statements) != before {
					t.Fatal("sticky byte denial reached driver", again)
				}
				return err
			})
			if !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || budget.Snapshot().TotalBytes != ceiling+1 || !budget.Snapshot().Failed || len(v.rate.statements) != 3 || v.rate.commits != 0 || v.rate.writes != 0 {
				t.Fatal("original transferred-byte cap changed/reset", err, budget.Snapshot(), len(v.rate.statements))
			}
		})
	}
}

type computeChoiceIdentityErrorSQLV2 struct {
	*computeChoiceRateFixture
	cause error
}

func (f *computeChoiceIdentityErrorSQLV2) Connect(context.Context) (driver.Conn, error) {
	return f, nil
}
func (f *computeChoiceIdentityErrorSQLV2) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if statement == webIdentityRowSQLV2 {
		if err := f.statement(ctx, statement, args); err != nil {
			return nil, err
		}
		return nil, f.cause
	}
	return f.computeChoiceRateFixture.QueryContext(ctx, statement, args)
}

func TestComputeChoiceIdentityV2SQLCauseAndRetryAggregateBound(t *testing.T) {
	t.Run("identity_sql_cause", func(t *testing.T) {
		v := newComputeChoiceIdentityFixtureV2(t, domain.IdentityProviderYandex, 1, "choice-tenant")
		v.positive(t, "valid-before-source-error")
		cause := errors.New("controlled exact identity point-read failure")
		db := sql.OpenDB(&computeChoiceIdentityErrorSQLV2{computeChoiceRateFixture: v.rate, cause: cause})
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		v.f.store.db = db
		if err := v.denied(t, "source-error"); !errors.Is(err, cause) {
			t.Fatal("SQL cause lost behind parser/authority denial", err)
		}
	})
	t.Run("list_retry_total96", func(t *testing.T) {
		v := newComputeChoiceIdentityFixtureV2(t, domain.IdentityProviderYandex, 1, "choice-tenant")
		v.rate.commitModes = []string{"rollback", "rollback", "rollback"}
		page, err := v.list("whole-v2-budget-retry")
		if !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || !reflect.DeepEqual(page, computeChoiceAttachedPage{}) || len(v.rate.statements) != computechoice.MaxStatementsTotal || len(v.rate.slots) != 0 || v.rate.commits != 2 {
			t.Fatalf("actual LIST renewed retry allowance/leaked speculation: SQL=%d slots=%d commits=%d err=%v", len(v.rate.statements), len(v.rate.slots), v.rate.commits, err)
		}
	})
}
