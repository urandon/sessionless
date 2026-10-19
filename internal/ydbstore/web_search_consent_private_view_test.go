package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Extend the original parent/allocation SQL fixture, not the consent reducer.
// The persisted pending row below is structural fixture DATA, not proof that
// a genuine model/search producer exists. Actual Store reads, current joins,
// callback budgets and commit-failure handling are the subjects of these tests.
type privateConsentViewFixture struct {
	allocation ownedAllocationFixture
	state      domain.WebSearchConsentStateV1
	web        domain.WebSession
	input      ports.ReadPrivateWebSearchConsentRequestV1
	bodies     map[string][]byte
	grantRef   string
	query      string

	commits, commitErrorAt int
	intentBytes            uint64
	budget                 *ownedAuthorityBudget
	reads                  []string
	readCaps               []uint64
	lastReadBytes          uint64
	bodyFault              func(context.Context, string)
	afterCommit            func()
	pendingState           *domain.WebSearchConsentStateV1
	decisionWrites         int
	legacyGrantOnly        bool // observe old Read/Decide without adding argument I/O
}

type privateConsentViewConnector struct{ f *privateConsentViewFixture }
type privateConsentViewConn struct {
	allocationConn
	f *privateConsentViewFixture
}
type privateConsentViewTx struct {
	driver.Tx
	f   *privateConsentViewFixture
	ctx context.Context
}

func (c privateConsentViewConnector) Connect(context.Context) (driver.Conn, error) {
	return privateConsentViewConn{allocationConn{c.f.allocation.sql}, c.f}, nil
}
func (c privateConsentViewConnector) Driver() driver.Driver {
	return allocationDriver{c.f.allocation.sql}
}
func (c privateConsentViewConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	tx, err := c.allocationConn.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	c.f.pendingState = nil
	return privateConsentViewTx{tx, c.f, ctx}, nil
}
func (tx privateConsentViewTx) Commit() error {
	err := tx.Tx.Commit()
	if err == nil && tx.f.pendingState != nil {
		tx.f.state = tx.f.pendingState.Clone()
	}
	tx.f.pendingState = nil
	tx.f.commits++
	if b, ok := tx.ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget); ok {
		tx.f.budget = b
		b.mu.Lock()
		if tx.f.commits == 1 {
			tx.f.intentBytes = b.bytes
		}
		b.mu.Unlock()
	}
	if tx.f.afterCommit != nil {
		tx.f.afterCommit()
	}
	if tx.f.commits == tx.f.commitErrorAt {
		return errors.New("private-view fixture lost commit acknowledgement")
	}
	return err
}

func (tx privateConsentViewTx) Rollback() error {
	tx.f.pendingState = nil
	return tx.Tx.Rollback()
}

const privateConsentWebSessionSQL = `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`

func (c privateConsentViewConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f := c.f
	parent := f.allocation.parent
	var row []driver.Value
	switch statement {
	case privateConsentWebSessionSQL:
		bucket, _ := webBucket(string(f.web.SessionDigest))
		if len(args) != 2 || args[0].Value != int64(bucket) || args[1].Value != string(f.web.SessionDigest) {
			return &explanationFixtureRows{}, nil
		}
		raw, err := json.Marshal(f.web)
		if err != nil {
			return nil, err
		}
		row = []driver.Value{string(raw)}
	case ownedConsentReadSQL:
		r := parent.parent.Reservation
		if len(args) != 3 || args[0].Value != string(r.TenantID) || args[1].Value != string(r.AttemptID) || args[2].Value != string(r.Kind) {
			return nil, errors.New("private-view fixture rejects foreign consent selector")
		}
		pd, _ := parent.parent.Digest()
		state := f.state
		if f.pendingState != nil {
			state = f.pendingState.Clone()
		}
		raw, digest, err := ownedConsentBytes(ownedConsentStates{Version: 1, ParentDigest: pd, States: []domain.WebSearchConsentStateV1{state}}, parent.parent, parent.graph.at)
		if err != nil {
			return nil, err
		}
		row = []driver.Value{uint64(1), uint64(1), string(raw), digest}
	default:
		for _, x := range []struct {
			original string
			row      []driver.Value
		}{
			{"SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3", parent.base.membership},
			{"SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3", parent.base.participant},
		} {
			bounded, _, _ := ownedCanonicalJSONSelect(x.original)
			if statement == bounded {
				row = append([]driver.Value(nil), x.row[:1]...)
				break
			}
		}
		if row == nil {
			return c.allocationConn.QueryContext(ctx, statement, args)
		}
	}
	parent.base.recordQuery(statement, args)
	return &explanationFixtureRows{values: row}, nil
}

const privateConsentUpdateSQL = `UPDATE attempt_effect_reservations
 SET web_search_consent_record=$1,web_search_consent_digest=$2
 WHERE tenant_id=$3 AND attempt_id=$4 AND kind=$5 AND owned_digest=$6 AND record IS NULL`

func (c privateConsentViewConn) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if statement != privateConsentUpdateSQL {
		return c.allocationConn.ExecContext(ctx, statement, args)
	}
	f := c.f
	p := f.allocation.parent.parent
	pd, _ := p.Digest()
	if len(args) != 6 || args[2].Value != string(p.Scope.TenantID) || args[3].Value != string(p.Scope.AttemptID) || args[4].Value != string(p.Reservation.Kind) || args[5].Value != pd {
		return nil, errors.New("consent fixture UPDATE requires the exact original physical parent")
	}
	raw, rawOK := args[0].Value.(string)
	digest, digestOK := args[1].Value.(string)
	projection, err := decodeOwnedConsentStates(raw, digest, p, f.allocation.parent.graph.at)
	if !rawOK || !digestOK || err != nil || len(projection.States) != 1 {
		return nil, errors.New("consent fixture UPDATE requires bounded canonical state bytes and digest")
	}
	if result, err := f.allocation.parent.base.ExecContext(ctx, statement, args); err != nil {
		return nil, err
	} else {
		state := projection.States[0].Clone()
		f.pendingState = &state
		f.decisionWrites++
		return result, nil
	}
}

func (f *privateConsentViewFixture) ReadAttemptOperationBody(ctx context.Context, scope domain.AttemptOperationScopeV1, ref string, cap uint64) ([]byte, error) {
	if scope != ownedConsentOperationScope(f.state.Request.Scope) || cap == 0 || cap > 4<<10 || f.allocation.parent.sql.activeTransactions != 0 {
		return nil, errors.New("private-view keeper rejects foreign, unbounded or in-transaction I/O")
	}
	b, ok := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	if !ok || ctx.Err() != nil {
		return nil, errors.New("private-view keeper requires the live original logical budget")
	}
	b.mu.Lock()
	charged, active, callbacks := b.bytes, b.active, b.callbacks
	b.mu.Unlock()
	base := f.lastReadBytes
	if len(f.reads) == 0 {
		base = f.intentBytes
	}
	if active || callbacks != 1 || !f.legacyGrantOnly && (charged < base || charged-base < cap) {
		return nil, errors.New("private-view keeper read was not funded prospectively in the original budget")
	}
	if f.legacyGrantOnly && ref != f.grantRef {
		return nil, errors.New("legacy consent state method unexpectedly read query arguments")
	}
	f.budget, f.lastReadBytes = b, charged
	f.reads, f.readCaps = append(f.reads, ref), append(f.readCaps, cap)
	body, found := f.bodies[ref]
	if !found {
		return nil, errors.New("private-view keeper has no exact immutable body")
	}
	if f.bodyFault != nil {
		f.bodyFault(ctx, ref)
	}
	return body, nil // borrowed immutable bytes; the Store must not mutate them
}

func newPrivateConsentViewFixture(t *testing.T) *privateConsentViewFixture {
	t.Helper()
	// Freeze the synthetic grant before any profile/source/parent seal. Reuse
	// the existing fixture's scope and source setup, not post-seal rebinding.
	_, seed, _, _ := newOwnedFinalScript(t)
	p := seed.reader.profile.Clone()
	cost := p.CostCeiling.WebSearchCost
	grant := ports.OwnedFixtureToolGrantV1{Version: 1, TenantID: cost.TenantID, OwnerUserID: cost.OwnerUserID, BillingOwnerUserID: cost.BillingOwnerUserID, Kind: cost.Kind, ResourceID: cost.ResourceID, ResourceRevision: cost.ResourceRevision, RevocationGeneration: cost.RevocationGeneration, ProfileID: cost.ProfileID, ProfileRevision: cost.ProfileRevision, ConsentMode: "initiating_user_required", ConsentRevision: 1, Endpoint: "fixture://owned/web-search", MaxArgumentBytes: 4096, MaxResultBytes: 24576, ExpiresAt: cost.PriceExpiresAt}
	grantBody, err := json.Marshal(grant)
	if err != nil || grant.Validate() != nil {
		t.Fatal("private-view synthetic grant prerequisite", err)
	}
	gd := ownedConsentTestHash(grantBody)
	p.WebSearchGrantDigest, cost.ScopedGrantDigest = gd, gd
	p.Substrate.AdmissionCostCeilingDigest, err = p.CostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	sources := ownedFinalSources{profile: p}
	pd, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range seed.reader.spending {
		v = v.Clone()
		v.SourceProfile.Digest = pd
		if v.Locator.ComponentKind == domain.AttemptSpendingWebSearchV1 {
			v.ScopedGrantDigest = gd
		}
		bindReaderSpendingSource(t, &v)
		sources.spending[i] = v
	}
	a := newOwnedAllocationFixture(t, ownedFinalPrebuild{at: seed.at, sources: &sources})
	if _, err := a.parent.store.publishBoundedOwnedAllocation(t.Context(), a.publication); err != nil {
		t.Fatal("private-view actual original allocation publication", err)
	}
	query := "public compiler documentation — \"exact\"\nsecond line"
	args, err := json.Marshal(struct {
		Query      string `json:"query"`
		MaxResults int    `json:"max_results"`
	}{query, 5})
	if err != nil {
		t.Fatal(err)
	}
	at := a.parent.graph.at
	scope := ownedConsentScope(a.parent.parent)
	r := domain.WebSearchConsentRequestV1{Version: 1, ID: "private-view-consent", Scope: scope, InitiatingUserID: scope.OwnerUserID, ModelResponseDigest: strings.Repeat("6", 64), CallID: "private-view-call", ArgumentsDigest: ownedConsentTestHash(args), QueryDigest: ownedConsentTestHash([]byte(query)), GrantDigest: gd, ConsentRevision: 1, RevocationGeneration: 1, Arguments: domain.BlobRef{TenantID: scope.TenantID, Key: domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempts/" + string(scope.AttemptID) + "/consent/private.json", Size: int64(len(args)), SHA256: ownedConsentTestHash(args)}, CreatedAt: at, ExpiresAt: at.Add(time.Second)}
	if err := r.ValidateArguments(args); err != nil {
		t.Fatal("private-view exact argument prerequisite", err)
	}
	web := domain.WebSession{SessionDigest: domain.DigestSecret("private-view-cookie"), CSRFTokenDigest: domain.DigestSecret("private-view-csrf"), UserID: r.InitiatingUserID, ActiveTenantID: scope.TenantID, AuthenticatedSubject: domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "123"}, MembershipSecurityVersion: 1, IssuedAt: at, LastSeenAt: at, IdleExpiresAt: at.Add(time.Hour), AbsoluteExpiresAt: at.Add(2 * time.Hour)}
	ref, _ := ports.OwnedFixtureGrantReferenceV1(gd)
	f := &privateConsentViewFixture{allocation: a, state: domain.WebSearchConsentStateV1{Request: r, Revision: 1, Status: domain.WebSearchConsentPendingV1, UpdatedAt: at}, web: web, query: query, grantRef: ref, bodies: map[string][]byte{ref: grantBody, r.Arguments.Key: args}}
	f.input = ports.ReadPrivateWebSearchConsentRequestV1{SessionID: scope.SessionID, RunID: scope.RunID, AttemptID: scope.AttemptID, RequestID: r.ID, WebSessionDigest: web.SessionDigest}
	db := sql.OpenDB(privateConsentViewConnector{f})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	a.parent.store.db, a.parent.store.attemptOperationBodies = db, f
	return f
}

func (f *privateConsentViewFixture) read(t *testing.T, ctx context.Context) (ports.WebSearchConsentPrivateViewV1, error) {
	t.Helper()
	return f.allocation.parent.store.ReadPrivateWebSearchConsent(ctx, f.input)
}

func TestPrivateWebSearchConsentViewExactQueryAndOriginalBudget(t *testing.T) {
	f := newPrivateConsentViewFixture(t)
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	before := bytes.Clone(f.bodies[f.state.Request.Arguments.Key])
	writes, web := f.allocation.parent.graph.writes, f.web
	got, err := f.read(t, ctx)
	if err != nil || got.Query != f.query || !reflect.DeepEqual(got.State, f.state) {
		t.Fatalf("actual Store private exact-query view: err=%v query=%q state=%+v", err, got.Query, got.State)
	}
	b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	if f.budget != b || b.callbacks != 2 || b.statements > 64 || b.bytes > ownedAuthorityMaxBytes || f.commits != 2 || !reflect.DeepEqual(f.reads, []string{f.grantRef, f.state.Request.Arguments.Key}) || !bytes.Equal(before, f.bodies[f.state.Request.Arguments.Key]) || f.web != web || f.allocation.parent.graph.writes != writes {
		t.Fatalf("view changed custody/activity/budget: callbacks=%d statements=%d bytes=%d commits=%d reads=%v writes=%d/%d", b.callbacks, b.statements, b.bytes, f.commits, f.reads, f.allocation.parent.graph.writes, writes)
	}
}

func TestPrivateWebSearchConsentViewRejectsBeforeBodyDisclosure(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*privateConsentViewFixture)
	}{
		{"invalid-request", func(f *privateConsentViewFixture) { f.input.RequestID = "" }},
		{"foreign-cookie", func(f *privateConsentViewFixture) { f.input.WebSessionDigest = domain.DigestSecret("foreign-cookie") }},
		{"foreign-active-tenant", func(f *privateConsentViewFixture) { f.web.ActiveTenantID = "foreign-tenant" }},
		{"foreign-user", func(f *privateConsentViewFixture) { f.web.UserID = "foreign-user" }},
		{"foreign-session", func(f *privateConsentViewFixture) { f.input.SessionID = "foreign-session" }},
		{"foreign-request", func(f *privateConsentViewFixture) { f.input.RequestID = "foreign-request" }},
		{"stale-membership-version", func(f *privateConsentViewFixture) { f.web.MembershipSecurityVersion++ }},
		{"expired-web-session", func(f *privateConsentViewFixture) { f.web.IdleExpiresAt = f.state.Request.CreatedAt }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPrivateConsentViewFixture(t)
			tc.edit(f)
			got, err := f.read(t, t.Context())
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.WebSearchConsentPrivateViewV1{}) || len(f.reads) != 0 {
				t.Fatalf("unauthorized view leaked data/body read: err=%v view=%+v reads=%v", err, got, f.reads)
			}
		})
	}
}

func TestPrivateWebSearchConsentViewRechecksAfterImmutableBodyRead(t *testing.T) {
	for _, name := range []string{"argument-digest", "argument-size", "argument-query", "changed-intent", "revoked-web", "changed-active-tenant", "revoked-profile", "cancelled", "expired", "final-commit-unknown", "cancel-after-final-commit"} {
		t.Run(name, func(t *testing.T) {
			f := newPrivateConsentViewFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch name {
			case "argument-digest":
				f.bodies[f.state.Request.Arguments.Key] = []byte(`{"query":"changed"}`)
			case "argument-size":
				f.bodies[f.state.Request.Arguments.Key] = bytes.Repeat([]byte("x"), 4097)
			case "argument-query":
				f.state.Request.QueryDigest = strings.Repeat("7", 64)
			case "final-commit-unknown":
				f.commitErrorAt = 2
			case "cancel-after-final-commit":
				f.afterCommit = func() {
					if f.commits == 2 {
						cancel()
					}
				}
			}
			f.bodyFault = func(_ context.Context, ref string) {
				if ref != f.state.Request.Arguments.Key {
					return
				}
				switch name {
				case "changed-intent":
					f.state.Request.CallID = "replacement-call"
				case "revoked-web":
					at := f.state.Request.CreatedAt
					f.web.RevokedAt = &at
				case "changed-active-tenant":
					f.web.ActiveTenantID = "another-active-tenant"
				case "revoked-profile":
					f.allocation.parent.base.reader.driver.profileRow[4] = "revoked"
				case "cancelled":
					cancel()
				case "expired":
					f.allocation.parent.graph.at = f.state.Request.ExpiresAt
				}
			}
			got, err := f.read(t, ctx)
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.WebSearchConsentPrivateViewV1{}) {
				t.Fatalf("unconfirmed/changed authority released query: err=%v view=%+v reads=%v commits=%d", err, got, f.reads, f.commits)
			}
			if len(f.reads) != 2 {
				t.Fatalf("negative must exercise the original argument materialization, reads=%v", f.reads)
			}
		})
	}
}

func TestPrivateWebSearchConsentViewCannotResetBodyOrCallbackBudget(t *testing.T) {
	t.Run("argument-budget-exhausted-before-read", func(t *testing.T) {
		f := newPrivateConsentViewFixture(t)
		f.bodyFault = func(ctx context.Context, ref string) {
			if ref == f.grantRef {
				b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
				b.mu.Lock()
				b.bytes = ownedAuthorityMaxBytes
				b.mu.Unlock()
			}
		}
		got, err := f.read(t, t.Context())
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.WebSearchConsentPrivateViewV1{}) || !reflect.DeepEqual(f.reads, []string{f.grantRef}) {
			t.Fatalf("exhausted shared metadata budget performed another read: err=%v view=%+v reads=%v", err, got, f.reads)
		}
	})
	t.Run("original-callback-budget-not-replaced", func(t *testing.T) {
		f := newPrivateConsentViewFixture(t)
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		b.callbacks = ownedAuthorityMaxCallbacks
		got, err := f.read(t, ctx)
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.WebSearchConsentPrivateViewV1{}) || len(f.reads) != 0 || b.callbacks != ownedAuthorityMaxCallbacks {
			t.Fatalf("private view renewed exhausted callback budget: err=%v view=%+v reads=%v callbacks=%d", err, got, f.reads, b.callbacks)
		}
	})
}

func privateConsentCanonicalInput(f *privateConsentViewFixture) ports.ReadWebSearchConsentRequestV1 {
	return ports.ReadWebSearchConsentRequestV1{Locator: consentLocator(f.state.Request), WebSessionDigest: f.input.WebSessionDigest}
}

func privateConsentDecisionInput(f *privateConsentViewFixture) ports.DecideWebSearchConsentRequestV1 {
	return ports.DecideWebSearchConsentRequestV1{Locator: consentLocator(f.state.Request), WebSessionDigest: f.web.SessionDigest, CSRFTokenDigest: f.web.CSRFTokenDigest, ExpectedRevision: 1, Choice: domain.WebSearchConsentApproveV1}
}

func requirePrivateConsentGrantReadsOnly(t *testing.T, f *privateConsentViewFixture) {
	t.Helper()
	for i, ref := range f.reads {
		if ref != f.grantRef || f.readCaps[i] != 4<<10 {
			t.Fatalf("state-only legacy method read query or widened body bound: reads=%v caps=%v", f.reads, f.readCaps)
		}
	}
}

func TestPrivateWebSearchConsentViewPreservesCanonicalReadAndDecisionCAS(t *testing.T) {
	f := newPrivateConsentViewFixture(t)
	f.legacyGrantOnly = true
	store, initial, web := f.allocation.parent.store, f.state.Clone(), f.web
	state, err := store.ReadWebSearchConsent(t.Context(), privateConsentCanonicalInput(f))
	if err != nil || !reflect.DeepEqual(state, initial) || f.decisionWrites != 0 {
		t.Fatalf("original Read wrapper changed state or wrote: err=%v state=%+v writes=%d", err, state, f.decisionWrites)
	}
	input := privateConsentDecisionInput(f)
	approved, err := store.DecideWebSearchConsent(t.Context(), input)
	if err != nil || approved.Decision == nil || approved.Status != domain.WebSearchConsentApprovedV1 || approved.Revision != 2 || f.decisionWrites != 1 {
		t.Fatalf("actual canonical CAS did not append one approval: err=%v state=%+v writes=%d", err, approved, f.decisionWrites)
	}
	receipt, err := approved.Decision.Digest()
	if err != nil || approved.Decision.RequestDigest == "" || approved.Decision.ActorUserID != f.web.UserID {
		t.Fatalf("canonical Store did not return a valid actor-bound receipt: err=%v decision=%+v", err, approved.Decision)
	}
	readback, err := store.ReadWebSearchConsent(t.Context(), privateConsentCanonicalInput(f))
	if err != nil || !reflect.DeepEqual(readback, approved) {
		t.Fatalf("original Read did not recover committed decision state: err=%v state=%+v", err, readback)
	}
	again, err := store.DecideWebSearchConsent(t.Context(), input)
	if err != nil || again.Decision == nil || !reflect.DeepEqual(again, approved) || f.decisionWrites != 1 {
		t.Fatalf("identical CAS retry replaced/re-wrote receipt: err=%v state=%+v writes=%d", err, again, f.decisionWrites)
	}
	actual, err := again.Decision.Digest()
	if err != nil || actual != receipt || !again.UpdatedAt.Equal(approved.UpdatedAt) {
		t.Fatalf("identical decision retry changed original digest/time: err=%v digest=%s want=%s", err, actual, receipt)
	}
	for _, mutation := range []string{"conflicting-choice", "wrong-csrf", "stale-revision", "foreign-cookie"} {
		bad := input
		switch mutation {
		case "conflicting-choice":
			bad.Choice = domain.WebSearchConsentDenyV1
		case "wrong-csrf":
			bad.CSRFTokenDigest = domain.DigestSecret("foreign-csrf")
		case "stale-revision":
			bad.ExpectedRevision = 2
		case "foreign-cookie":
			bad.WebSessionDigest = domain.DigestSecret("foreign-cookie")
		}
		got, err := store.DecideWebSearchConsent(t.Context(), bad)
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.WebSearchConsentStateV1{}) || !reflect.DeepEqual(f.state, approved) || f.decisionWrites != 1 {
			t.Fatalf("%s replaced an immutable decision: err=%v result=%+v persisted=%+v writes=%d", mutation, err, got, f.state, f.decisionWrites)
		}
	}
	if f.web != web {
		t.Fatal("Read/Decide refreshed browser activity instead of preserving original Web session")
	}
	requirePrivateConsentGrantReadsOnly(t, f)
}

func TestPrivateWebSearchConsentViewCanonicalDecisionLostACKAndLateDenial(t *testing.T) {
	t.Run("committed-lost-ack-recovers-exact-receipt", func(t *testing.T) {
		f := newPrivateConsentViewFixture(t)
		f.legacyGrantOnly, f.commitErrorAt = true, 2
		input := privateConsentDecisionInput(f)
		store := f.allocation.parent.store
		got, err := store.DecideWebSearchConsent(t.Context(), input)
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.WebSearchConsentStateV1{}) || f.state.Decision == nil || f.decisionWrites != 1 {
			t.Fatalf("unknown final ACK must hide speculative output, preserve committed evidence: err=%v result=%+v persisted=%+v writes=%d", err, got, f.state, f.decisionWrites)
		}
		persisted := f.state.Clone()
		f.commitErrorAt = 0
		recovered, err := store.DecideWebSearchConsent(t.Context(), input)
		if err != nil || !reflect.DeepEqual(recovered, persisted) || f.decisionWrites != 1 {
			t.Fatalf("same CAS receipt recovery re-wrote/replaced commit: err=%v result=%+v want=%+v writes=%d", err, recovered, persisted, f.decisionWrites)
		}
		requirePrivateConsentGrantReadsOnly(t, f)
	})
	t.Run("late-approval-denied-before-cas-write", func(t *testing.T) {
		f := newPrivateConsentViewFixture(t)
		f.legacyGrantOnly = true
		f.allocation.parent.graph.at = f.state.Request.ExpiresAt
		initial := f.state.Clone()
		got, err := f.allocation.parent.store.DecideWebSearchConsent(t.Context(), privateConsentDecisionInput(f))
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.WebSearchConsentStateV1{}) || !reflect.DeepEqual(f.state, initial) || f.decisionWrites != 0 {
			t.Fatalf("late approval wrote/reopened pending request: err=%v result=%+v writes=%d", err, got, f.decisionWrites)
		}
		requirePrivateConsentGrantReadsOnly(t, f)
	})
}
