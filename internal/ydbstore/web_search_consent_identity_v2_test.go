package ydbstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

// Stored pending/MODEL/identity rows here are structural SQL fixture DATA,
// never proof of a genuine SEARCH producer. The actual public Store methods,
// canonical actor joins, body custody, final transaction and CAS execute. Native
// reserve/BeforeSend coverage is separate and requires the existing remote gate.
type ownedConsentIdentityFixtureV2 struct {
	f        *privateConsentViewFixture
	custody  *resourceConsentCustodyFixture
	session  domain.WebSessionV2
	identity domain.ExternalIdentityV2
}

func newOwnedConsentIdentityFixtureV2(t *testing.T, provider domain.IdentityProvider, revision uint64) *ownedConsentIdentityFixtureV2 {
	t.Helper()
	return newOwnedConsentIdentityFixtureV2FromState(t, nil, provider, revision)
}

func newOwnedConsentIdentityFixtureV2FromState(t *testing.T, state *ports.WorkerJobState, provider domain.IdentityProvider, revision uint64) *ownedConsentIdentityFixtureV2 {
	t.Helper()
	f, custody := newResourceConsentViewFixtureFromState(t, state)
	f.web.AuthenticatedSubject = domain.ExternalSubject{Provider: provider, Subject: "consent-identity-subject"}
	at := f.allocation.parent.graph.at
	v := &ownedConsentIdentityFixtureV2{f: f, custody: custody, session: domain.WebSessionV2{Version: 2, Session: f.web, IdentityRevision: domain.AuthRevisionV2(revision)}, identity: domain.ExternalIdentityV2{Version: 2, Subject: f.web.AuthenticatedSubject, UserID: f.web.UserID, CreatedAt: at.Add(-time.Hour), State: domain.ExternalIdentityActiveV2, Revision: domain.AuthRevisionV2(revision), UpdatedAt: at}}
	f.allocation.parent.store.webIdentityV2 = &identityConfigurationV2{ManifestDigest: domain.DigestSecret("consent-private-v2-manifest"), Providers: []domain.IdentityProviderBindingV2{
		{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "consent-yandex", CallbackURI: "https://sessionless.example/auth/login/callback"},
		{Provider: domain.IdentityProviderTelegram, Issuer: "https://oauth.telegram.org", ClientID: "consent-telegram", CallbackURI: "https://sessionless.example/auth/telegram/callback"},
	}}
	v.sync(t)
	return v
}

func consentIdentityTenantStateV2(t *testing.T, tenant domain.TenantID) ports.WorkerJobState {
	t.Helper()
	s := currentWorkerAuthorityFixture(t)
	s.Run.TenantID, s.Job.TenantID, s.Attempt.TenantID, s.Reservation.TenantID, s.InputManifest.TenantID = tenant, tenant, tenant, tenant, tenant
	s.Job.ContextSnapshot.TenantID = tenant
	s.Job.ContextSnapshot.Key = domain.SessionObjectPrefix(tenant, s.Run.SessionID) + "context"
	s.Job.DeliveryChat.TenantID = tenant
	s.Job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 1}
	a, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(tenant, s.Job.CredentialOwnerUserID, s.Run.ID, s.Attempt.ID, s.Run.SubscriptionConnectionID, s.Run.CreatedAt)
	if err != nil {
		t.Fatal("pre-seal tenant fixture authority", err)
	}
	s.Job.ExecutionPlacementV2, s.Job.HarnessBinding = a.ExecutionPlacementV2, a.HarnessBinding
	s.Job.SubstrateBinding, s.Job.AdmissionCostCeiling = &a.SubstrateBinding, &a.AdmissionCostCeiling
	if err := validateLoadedWorkerJob(s); err != nil {
		t.Fatal("coherent tenant fixture BEFORE source seal", err)
	}
	return s
}

func TestPrivateWebSearchConsentIdentityV2BothTenantBaselinesBeforeStale(t *testing.T) {
	for _, method := range []string{"discovery", "private-get", "read", "decide"} {
		t.Run(method, func(t *testing.T) {
			aState, bState := consentIdentityTenantStateV2(t, "consent-tenant-a"), consentIdentityTenantStateV2(t, "consent-tenant-b")
			a := newOwnedConsentIdentityFixtureV2FromState(t, &aState, domain.IdentityProviderYandex, 7)
			b := newOwnedConsentIdentityFixtureV2FromState(t, &bState, domain.IdentityProviderYandex, 7)
			if a.f.web.UserID != b.f.web.UserID || a.identity.Subject != b.identity.Subject || a.f.web.ActiveTenantID == b.f.web.ActiveTenantID {
				t.Fatal("cross-tenant fixture lacks same canonical identity/distinct tenant authority")
			}
			a.assertCurrent(t, method)
			b.assertCurrent(t, method)
			if method == "private-get" {
				// Unchanged repeat reads in BOTH tenants must reach their actual
				// final callback before a stale-cookie denial can be meaningful.
				a.assertCurrent(t, method)
				b.assertCurrent(t, method)
			}
			fixtures := []struct {
				name string
				v    *ownedConsentIdentityFixtureV2
			}{{"tenant-a", a}, {"tenant-b", b}}
			for _, fixture := range fixtures {
				v := fixture.v
				v.identity.Revision++
				v.sync(t)
			}
			for _, fixture := range fixtures {
				t.Run(fixture.name, func(t *testing.T) {
					v := fixture.v
					reads, caps, writes := len(v.f.reads), len(v.f.readCaps), v.f.decisionWrites
					grantReads, grantCaps := v.custody.reads, len(v.custody.caps)
					if state, err := v.call(t, method); !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(state, domain.WebSearchConsentStateV1{}) {
						t.Fatal("old shared identity cookie survived in another authorized tenant", err)
					}
					if len(v.f.reads) != reads || len(v.f.readCaps) != caps || v.custody.reads != grantReads || len(v.custody.caps) != grantCaps || v.f.decisionWrites != writes {
						t.Fatal("stale cookie reached body I/O or changed the original human decision")
					}
				})
			}
		})
	}
}

func (v *ownedConsentIdentityFixtureV2) sync(t *testing.T) {
	t.Helper()
	v.f.web = v.session.Session
	web, err := domain.EncodeWebSessionV2(v.session)
	if err != nil {
		t.Fatal("valid V2 cookie fixture", err)
	}
	identity, err := domain.EncodeExternalIdentityV2(v.identity)
	if err != nil {
		t.Fatal("valid V2 identity fixture", err)
	}
	w, i := string(web), string(identity)
	v.f.webV2Record, v.f.identityV2Record = &w, &i
}

func (v *ownedConsentIdentityFixtureV2) call(t *testing.T, method string) (domain.WebSearchConsentStateV1, error) {
	t.Helper()
	store := v.f.allocation.parent.store
	switch method {
	case "discovery":
		pending, err := store.DiscoverPrivateWebSearchConsent(t.Context(), privateConsentDiscoveryInput(v.f))
		if err != nil {
			return domain.WebSearchConsentStateV1{}, err
		}
		if pending == nil || pending.Locator != consentLocator(v.f.state.Request) {
			t.Fatal("actual discovery omitted original pending selector")
		}
		return v.f.state.Clone(), nil
	case "private-get":
		view, err := store.ReadPrivateWebSearchConsent(t.Context(), v.f.input)
		if err != nil {
			if !reflect.DeepEqual(view, ports.WebSearchConsentPrivateViewV1{}) {
				t.Fatal("denied private read leaked query/state")
			}
			return domain.WebSearchConsentStateV1{}, err
		}
		if view.Query != v.f.query {
			t.Fatal("authorized view changed exact immutable query")
		}
		return view.State, nil
	case "read":
		return store.ReadWebSearchConsent(t.Context(), ports.ReadWebSearchConsentRequestV1{Locator: consentLocator(v.f.state.Request), WebSessionDigest: v.f.web.SessionDigest})
	case "decide":
		return store.DecideWebSearchConsent(t.Context(), privateConsentDecisionInput(v.f))
	default:
		t.Fatal("closed test operation", method)
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
}

func (v *ownedConsentIdentityFixtureV2) assertCurrent(t *testing.T, method string) {
	t.Helper()
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	startIdentity, writes, web := v.f.identityReads, v.f.decisionWrites, v.f.web
	startReads, startCaps := len(v.f.reads), len(v.f.readCaps)
	startGrantReads, startGrantCaps := v.custody.reads, len(v.custody.caps)
	var err error
	store := v.f.allocation.parent.store
	switch method {
	case "discovery":
		var p *ports.WebSearchConsentPendingV1
		p, err = store.DiscoverPrivateWebSearchConsent(ctx, privateConsentDiscoveryInput(v.f))
		if err == nil && (p == nil || p.Locator != consentLocator(v.f.state.Request)) {
			t.Fatal("valid discovery changed selector")
		}
	case "private-get":
		var view ports.WebSearchConsentPrivateViewV1
		view, err = store.ReadPrivateWebSearchConsent(ctx, v.f.input)
		if err == nil && (view.Query != v.f.query || !reflect.DeepEqual(view.State, v.f.state)) {
			t.Fatal("valid private query/state changed")
		}
	case "read":
		_, err = store.ReadWebSearchConsent(ctx, ports.ReadWebSearchConsentRequestV1{Locator: consentLocator(v.f.state.Request), WebSessionDigest: v.f.web.SessionDigest})
	case "decide":
		var state domain.WebSearchConsentStateV1
		state, err = store.DecideWebSearchConsent(ctx, privateConsentDecisionInput(v.f))
		if err == nil && (state.Status != domain.WebSearchConsentApprovedV1 || state.Decision == nil || state.Decision.WebSessionDigest != v.f.web.SessionDigest) {
			t.Fatal("actual CAS did not preserve approved receipt")
		}
	}
	if err != nil {
		t.Fatalf("valid V2 consent baseline method=%s failed: %v", method, err)
	}
	wantCallbacks := 2
	if method == "discovery" {
		wantCallbacks = 1
	}
	b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	if b.failed || b.callbacks != wantCallbacks || b.statements > ownedAuthorityMaxTotalStatements || b.bytes > ownedAuthorityMaxBytes || v.f.identityReads-startIdentity != wantCallbacks || v.f.web != web {
		t.Fatalf("V2 mode changed budget/activity or missed callback fence: method=%s callbacks=%d statements=%d bytes=%d identity=%d failed=%t", method, b.callbacks, b.statements, b.bytes, v.f.identityReads-startIdentity, b.failed)
	}
	for _, n := range v.f.callbackStatements[len(v.f.callbackStatements)-wantCallbacks:] {
		if n > ownedAuthorityMaxStatements {
			t.Fatal("V2 exceeded callback SQL bound", n)
		}
	}
	if method != "decide" && v.f.decisionWrites != writes {
		t.Fatal("read minted decision")
	}
	if method == "private-get" && (!reflect.DeepEqual(v.f.reads[startReads:], []string{v.f.state.Request.Arguments.Key}) || !reflect.DeepEqual(v.f.readCaps[startCaps:], []uint64{uint64(v.f.state.Request.Arguments.Size)}) || v.custody.reads-startGrantReads != 1 || !reflect.DeepEqual(v.custody.caps[startGrantCaps:], []uint64{uint64(len(v.custody.body))})) {
		t.Fatal("valid private read missed exact prospectively funded grant/argument materialization")
	}
	t.Logf("%s callbacks=%d statements=%d bytes=%d identityReads=%d perCallback=%v", method, b.callbacks, b.statements, b.bytes, wantCallbacks, v.f.callbackStatements[len(v.f.callbackStatements)-wantCallbacks:])
}

func TestPrivateWebSearchConsentIdentityV2CurrentReadersAndDecision(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		for _, revision := range []uint64{1, (1 << 53) - 1, 1 << 53, (1 << 53) + 1, math.MaxUint64} {
			t.Run(fmt.Sprintf("%s_%d", provider, revision), func(t *testing.T) {
				v := newOwnedConsentIdentityFixtureV2(t, provider, revision)
				for _, method := range []string{"discovery", "private-get", "read", "decide", "decide"} {
					v.assertCurrent(t, method)
				}
				if v.f.decisionWrites != 1 {
					t.Fatal("exact decision replay wrote/redebited", v.f.decisionWrites)
				}
			})
		}
	}
}

func TestPrivateWebSearchConsentIdentityV2RejectsCurrentAndWireFaults(t *testing.T) {
	for _, method := range []string{"discovery", "private-get", "read", "decide"} {
		for _, fault := range []string{"stale", "missing", "unlinked", "foreign-owner", "foreign-subject", "future", "malformed", "alias", "duplicate", "numeric", "cookie-v1", "cookie-alias", "cookie-duplicate", "cookie-numeric", "cookie-foreign-key", "identity-oversize", "cookie-oversize", "storage", "disabled", "malformed-config"} {
			t.Run(method+"/"+fault, func(t *testing.T) {
				v := newOwnedConsentIdentityFixtureV2(t, domain.IdentityProviderYandex, 1)
				// A genuine current method baseline precedes every contradiction.
				// Do not spend the pending CAS solely to prepare a Decide denial.
				baseline := method
				if method == "decide" {
					baseline = "read"
				}
				v.assertCurrent(t, baseline)
				switch fault {
				case "stale":
					v.identity.Revision++
					v.sync(t)
				case "missing":
					v.f.identityV2Record = nil
				case "unlinked":
					v.identity.State = domain.ExternalIdentityUnlinkedV2
					v.sync(t)
				case "foreign-owner":
					v.identity.UserID = "foreign-owner"
					v.sync(t)
				case "foreign-subject":
					v.identity.Subject.Subject = "foreign-subject"
					v.sync(t)
				case "future":
					v.identity.UpdatedAt = v.f.allocation.parent.graph.at.Add(time.Microsecond)
					v.sync(t)
				case "malformed":
					s := "{"
					v.f.identityV2Record = &s
				case "alias":
					s := strings.Replace(*v.f.identityV2Record, `"security_revision"`, `"Security_Revision"`, 1)
					v.f.identityV2Record = &s
				case "duplicate":
					s := strings.Replace(*v.f.identityV2Record, `"security_revision":"1"`, `"security_revision":"1","security_revision":"1"`, 1)
					v.f.identityV2Record = &s
				case "numeric":
					s := strings.Replace(*v.f.identityV2Record, `"security_revision":"1"`, `"security_revision":1`, 1)
					v.f.identityV2Record = &s
				case "cookie-v1":
					v.f.webV2Record = nil
				case "cookie-alias":
					s := strings.Replace(*v.f.webV2Record, `"identity_revision"`, `"Identity_Revision"`, 1)
					v.f.webV2Record = &s
				case "cookie-duplicate":
					s := strings.Replace(*v.f.webV2Record, `"identity_revision":"1"`, `"identity_revision":"1","identity_revision":"1"`, 1)
					v.f.webV2Record = &s
				case "cookie-numeric":
					s := strings.Replace(*v.f.webV2Record, `"identity_revision":"1"`, `"identity_revision":1`, 1)
					v.f.webV2Record = &s
				case "cookie-foreign-key":
					s := strings.Replace(*v.f.webV2Record, string(v.f.web.SessionDigest), strings.Repeat("f", 64), 1)
					v.f.webV2Record = &s
				case "identity-oversize":
					s := strings.Repeat("x", 8193)
					v.f.identityV2Record = &s
				case "cookie-oversize":
					s := strings.Repeat("x", 8193)
					v.f.webV2Record = &s
				case "storage":
					v.f.identityReadError = errors.New("controlled identity SQL failure")
				case "disabled":
					v.f.allocation.parent.store.webIdentityV2.Providers = v.f.allocation.parent.store.webIdentityV2.Providers[1:]
				case "malformed-config":
					v.f.allocation.parent.store.webIdentityV2.ManifestDigest = ""
				}
				before, writes, reads := v.f.state.Clone(), v.f.decisionWrites, len(v.f.reads)
				state, err := v.call(t, method)
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(state, domain.WebSearchConsentStateV1{}) || !reflect.DeepEqual(v.f.state, before) || v.f.decisionWrites != writes || len(v.f.reads) != reads {
					t.Fatalf("invalid current V2 authority leaked/rewrote/read private body: method=%s fault=%s err=%v", method, fault, err)
				}
			})
		}
	}
}

func TestPrivateWebSearchConsentIdentityV2DiscoveryBeforeAuthorizedAbsence(t *testing.T) {
	for _, absence := range []string{"job", "parent", "consent", "terminal"} {
		t.Run(absence, func(t *testing.T) {
			v := newOwnedConsentIdentityFixtureV2(t, domain.IdentityProviderYandex, 1)
			v.assertCurrent(t, "discovery")
			switch absence {
			case "job":
				v.f.jobAbsent = true
			case "parent":
				v.f.parentAbsent = true
			case "consent":
				v.f.consentAbsent = true
			case "terminal":
				privateConsentDiscoveryRun(t, v.f, func(run *domain.Run) {
					run.Status = domain.RunCancelled
					at := v.f.allocation.parent.graph.at
					run.FinishedAt = &at
				})
			}
			store := v.f.allocation.parent.store
			if pending, err := store.DiscoverPrivateWebSearchConsent(t.Context(), privateConsentDiscoveryInput(v.f)); err != nil || pending != nil {
				t.Fatal("valid current identity did not receive authenticated absence", err)
			}
			v.identity.Revision++
			v.sync(t)
			if pending, err := store.DiscoverPrivateWebSearchConsent(t.Context(), privateConsentDiscoveryInput(v.f)); !errors.Is(err, domain.ErrMembershipDenied) || pending != nil {
				t.Fatal("stale cookie acquired authorized absence", err)
			}
		})
	}
}

func TestPrivateWebSearchConsentIdentityV2RejoinsAfterBodyAndCommit(t *testing.T) {
	for _, method := range []string{"private-get", "decide"} {
		for _, boundary := range []string{"after-intent", "body-io", "unknown-commit"} {
			t.Run(method+"/"+boundary, func(t *testing.T) {
				v := newOwnedConsentIdentityFixtureV2(t, domain.IdentityProviderTelegram, 1)
				v.assertCurrent(t, "read")
				commits, writes := v.f.commits, v.f.decisionWrites
				change := func() { v.identity.Revision++; v.sync(t) }
				switch boundary {
				case "after-intent":
					v.f.afterCommit = func() {
						if v.f.commits == commits+1 {
							change()
						}
					}
				case "body-io":
					v.custody.fault = func(context.Context) { change() }
				case "unknown-commit":
					v.f.commitErrorAt = commits + 2
				}
				state, err := v.call(t, method)
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(state, domain.WebSearchConsentStateV1{}) {
					t.Fatal("speculative/stale final transaction disclosed success", err)
				}
				if boundary != "unknown-commit" && v.f.decisionWrites != writes {
					t.Fatal("late identity change appended a decision")
				}
			})
		}
	}
}

func TestPrivateWebSearchConsentIdentityV2PresentedCSRFAndReceiptFence(t *testing.T) {
	v := newOwnedConsentIdentityFixtureV2(t, domain.IdentityProviderYandex, 1)
	v.assertCurrent(t, "read")
	input := privateConsentDecisionInput(v.f)
	input.CSRFTokenDigest = domain.DigestSecret("wrong-presented-consent-csrf")
	if state, err := v.f.allocation.parent.store.DecideWebSearchConsent(t.Context(), input); !errors.Is(err, domain.ErrMembershipDenied) || state.Decision != nil || v.f.decisionWrites != 0 {
		t.Fatal("V2 skipped actual presented CSRF", err)
	}
	v.assertCurrent(t, "decide")
	original := v.f.state.Clone()
	v.identity.Revision++
	v.sync(t)
	if state, err := v.call(t, "decide"); !errors.Is(err, domain.ErrMembershipDenied) || state.Decision != nil || v.f.decisionWrites != 1 || !reflect.DeepEqual(original, v.f.state) {
		t.Fatal("immutable exact receipt retry revived stale identity", err)
	}
}

func TestWebSearchConsentIdentityV2NegativeOnlyReconciliation(t *testing.T) {
	for _, fault := range []string{"stale", "unlinked", "missing", "disabled", "foreign", "future", "malformed", "oversize", "storage", "bad-config", "pending-no-cookie"} {
		t.Run(fault, func(t *testing.T) {
			v := newOwnedConsentIdentityFixtureV2(t, domain.IdentityProviderYandex, 1)
			if fault != "pending-no-cookie" {
				v.assertCurrent(t, "decide")
			}
			original, web := v.f.state.Clone(), v.f.web
			wantStop := false
			switch fault {
			case "stale":
				v.identity.Revision++
				v.sync(t)
				wantStop = true
			case "unlinked":
				v.identity.State = domain.ExternalIdentityUnlinkedV2
				v.sync(t)
				wantStop = true
			case "missing":
				v.f.identityV2Record = nil
				wantStop = true
			case "disabled":
				v.f.allocation.parent.store.webIdentityV2.Providers = v.f.allocation.parent.store.webIdentityV2.Providers[1:]
				wantStop = true
			case "foreign":
				v.identity.UserID = "other-owner"
				v.sync(t)
			case "future":
				v.identity.UpdatedAt = v.f.allocation.parent.graph.at.Add(time.Microsecond)
				v.sync(t)
			case "malformed":
				s := "{"
				v.f.identityV2Record = &s
			case "oversize":
				s := strings.Repeat("x", 8193)
				v.f.identityV2Record = &s
			case "storage":
				v.f.identityReadError = errors.New("controlled stop identity failure")
			case "bad-config":
				v.f.allocation.parent.store.webIdentityV2.ManifestDigest = ""
			case "pending-no-cookie":
				v.f.webV2Record = nil
				v.f.identityV2Record = nil
				v.f.identityReadError = errors.New("must not read pending identity")
				v.f.allocation.parent.graph.at = original.Request.ExpiresAt
			}
			reads, writes := v.f.identityReads, v.f.decisionWrites
			input := ports.ReconcileWebSearchConsentRequestV1{Locator: consentLocator(original.Request), ExpectedRevision: original.Revision}
			state, err := v.f.allocation.parent.store.ReconcileWebSearchConsent(t.Context(), input)
			if wantStop || fault == "pending-no-cookie" {
				want := domain.WebSearchConsentRevokedV1
				if fault == "pending-no-cookie" {
					want = domain.WebSearchConsentExpiredV1
				}
				if err != nil || state.Status != want || state.Revision != original.Revision+1 || v.f.decisionWrites != writes+1 || !reflect.DeepEqual(state.Request, original.Request) || !reflect.DeepEqual(state.Decision, original.Decision) || v.f.web != web {
					t.Fatal("negative transaction invented/lost original stop facts", fault, err)
				}
				if fault == "pending-no-cookie" && v.f.identityReads != reads {
					t.Fatal("pending stop invented browser identity")
				}
				again, err := v.f.allocation.parent.store.ReconcileWebSearchConsent(t.Context(), input)
				if err != nil || !reflect.DeepEqual(state, again) || v.f.decisionWrites != writes+1 {
					t.Fatal("terminal stop replay minted new state", err)
				}
			} else if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(state, domain.WebSearchConsentStateV1{}) || !reflect.DeepEqual(v.f.state, original) || v.f.decisionWrites != writes {
				t.Fatal("unknown/tainted identity became invented stop", fault, err)
			}
		})
	}
}

func TestPrivateWebSearchConsentIdentityV2PrefixChargesAndStickyBudget(t *testing.T) {
	for _, kind := range []string{"cookie", "identity", "exhausted-callbacks", "exhausted-bytes"} {
		t.Run(kind, func(t *testing.T) {
			v := newOwnedConsentIdentityFixtureV2(t, domain.IdentityProviderYandex, 1)
			v.assertCurrent(t, "discovery")
			startIdentity, writes := v.f.identityReads, v.f.decisionWrites
			cookieBytes := uint64(len(*v.f.webV2Record))
			ctx, cancel := ownedAuthorityCallContext(t.Context())
			defer cancel()
			b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
			switch kind {
			case "cookie":
				s := strings.Repeat("x", 8193)
				v.f.webV2Record = &s
			case "identity":
				s := strings.Repeat("x", 8193)
				v.f.identityV2Record = &s
			case "exhausted-callbacks":
				b.callbacks = ownedAuthorityMaxCallbacks
			case "exhausted-bytes":
				if err := chargeOwnedAuthorityBody(ctx, ownedAuthorityMaxBytes); err != nil {
					t.Fatal(err)
				}
			}
			pending, err := v.f.allocation.parent.store.DiscoverPrivateWebSearchConsent(ctx, privateConsentDiscoveryInput(v.f))
			if !errors.Is(err, domain.ErrMembershipDenied) || pending != nil || !b.failed || v.f.decisionWrites != writes {
				t.Fatal("prefix/exhaustion leaked locator or renewed authority", kind, err)
			}
			if kind == "cookie" && (b.bytes != 8+8193 || v.f.identityReads != startIdentity) {
				t.Fatal("cookie sentinel not charged before decode", b.bytes)
			}
			if kind == "identity" && (b.bytes != 8+cookieBytes+8193 || v.f.identityReads != startIdentity+1) {
				t.Fatal("identity sentinel not charged before decode", b.bytes)
			}
			reads := v.f.identityReads
			if again, err := v.f.allocation.parent.store.DiscoverPrivateWebSearchConsent(ctx, privateConsentDiscoveryInput(v.f)); !errors.Is(err, domain.ErrMembershipDenied) || again != nil || v.f.identityReads != reads {
				t.Fatal("sticky call denial issued another identity read", err)
			}
		})
	}
}
