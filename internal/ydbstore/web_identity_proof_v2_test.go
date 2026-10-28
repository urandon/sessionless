package ydbstore

import (
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
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

// Extend the original isolated fixtures, never register a global SQL driver.
// Every transaction snapshots canonical, parent, child and audit rows together.
// This is scripted storage behavior, not genuine YDB or provider verification.
type identityProofSQLV2 struct {
	*identityOperationSQLV2
	children, pendingChildren map[domain.SecretDigest][]any
	rollbackOnce              bool
	retryAdvance              time.Duration
	childFault                error
}
type identityProofConnV2 struct {
	*identityOperationConnV2
	f *identityProofSQLV2
}
type identityProofTxV2 struct{ f *identityProofSQLV2 }

func (f *identityProofSQLV2) Connect(context.Context) (driver.Conn, error) {
	return &identityProofConnV2{identityOperationConnV2: &identityOperationConnV2{identityRowsV2Conn: &identityRowsV2Conn{fixture: f.base}, f: f.identityOperationSQLV2}, f: f}, nil
}
func (c *identityProofConnV2) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if _, err := c.identityOperationConnV2.BeginTx(ctx, options); err != nil {
		return nil, err
	}
	c.f.base.mu.Lock()
	defer c.f.base.mu.Unlock()
	c.f.pendingChildren = cloneIdentityProofRowsV2(c.f.children)
	return &identityProofTxV2{f: c.f}, nil
}
func cloneIdentityProofRowsV2(rows map[domain.SecretDigest][]any) map[domain.SecretDigest][]any {
	out := make(map[domain.SecretDigest][]any, len(rows))
	for key, value := range rows {
		out[key] = append([]any(nil), value...)
	}
	return out
}
func (tx *identityProofTxV2) Commit() error {
	f := tx.f
	f.base.mu.Lock()
	defer f.base.mu.Unlock()
	if f.base.pending == nil || f.base.rangeOpen != 0 {
		return errors.New("proof fixture commit lacks original closed snapshot")
	}
	if f.rollbackOnce {
		f.rollbackOnce = false
		f.base.pending = nil
		f.pendingOperation = ""
		f.pendingAudit = nil
		f.pendingChildren = nil
		f.base.rollbacks++
		f.at = f.at.Add(f.retryAdvance)
		return retry.RetryableError(errors.New("controlled proof serialization conflict"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	f.base.durable = f.base.pending.clone()
	f.operation, f.audit = f.pendingOperation, f.pendingAudit
	f.children = cloneIdentityProofRowsV2(f.pendingChildren)
	f.base.pending = nil
	f.pendingOperation = ""
	f.pendingAudit = nil
	f.pendingChildren = nil
	f.base.commits++
	err := f.commitReplyError
	f.commitReplyError = nil
	return err
}
func (tx *identityProofTxV2) Rollback() error {
	f := tx.f
	f.base.mu.Lock()
	defer f.base.mu.Unlock()
	if f.base.pending == nil {
		return errors.New("proof fixture rollback lacks original snapshot")
	}
	f.base.pending = nil
	f.pendingOperation = ""
	f.pendingAudit = nil
	f.pendingChildren = nil
	f.base.rollbacks++
	return nil
}
func (c *identityProofConnV2) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if statement != identityProofChildSQLV2 {
		return c.identityOperationConnV2.QueryContext(ctx, statement, args)
	}
	f := c.f
	f.base.mu.Lock()
	defer f.base.mu.Unlock()
	if err := f.base.statement(ctx, statement, args, false); err != nil {
		return nil, err
	}
	values := identityRowsV2Args(args)
	if len(values) != 2 {
		return nil, errors.New("proof read requires exact key")
	}
	digest, ok := values[1].(string)
	if !ok || values[0] != identityRowsV2ExpectedBucket(digest) {
		return nil, errors.New("proof read key/bucket mismatch")
	}
	rows := &identityRowsV2Rows{fixture: f.base, columns: []string{"record", "browser", "pkce", "nonce", "redirect", "created", "expires", "consumed", "delete_after"}}
	if stored, found := f.pendingChildren[domain.SecretDigest(digest)]; found {
		raw := stored[9].(string)
		if len(raw) > domain.IdentityAuthMaxRecordBytesV2+1 {
			raw = raw[:domain.IdentityAuthMaxRecordBytesV2+1]
		}
		rows.values = [][]driver.Value{{raw, stored[2], stored[3], stored[4], stored[5], stored[6], stored[7], stored[8], stored[10]}}
	}
	return rows, nil
}
func (c *identityProofConnV2) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	values := identityRowsV2Args(args)
	if identityRowsV2Statement(statement) != identityOperationWriteSQLTestV2 || len(values) != 11 {
		return c.identityOperationConnV2.ExecContext(ctx, statement, args)
	}
	raw, ok := values[9].(string)
	child, err := domain.DecodeIdentityChallengeV2([]byte(raw))
	if !ok || err != nil {
		return c.identityOperationConnV2.ExecContext(ctx, statement, args)
	}
	f := c.f
	f.base.mu.Lock()
	defer f.base.mu.Unlock()
	if err := f.base.statement(ctx, statement, args, true); err != nil {
		return nil, err
	}
	if f.childFault != nil {
		return nil, f.childFault
	}
	pkce, nonce := "", ""
	consumed := time.Unix(0, 0).UTC()
	if child.Secrets != nil {
		pkce, nonce = child.Secrets.PKCEVerifier, child.Secrets.Nonce
	}
	if child.ConsumedAt != nil {
		consumed = *child.ConsumedAt
	}
	want := []any{identityRowsV2ExpectedBucket(string(child.StateDigest)), string(child.StateDigest), string(child.BrowserBindingDigest), pkce, nonce, "", child.CreatedAt, child.ExpiresAt, consumed, raw, child.DeleteAfter}
	if !reflect.DeepEqual(values, want) {
		return nil, errors.New("proof write scalar/JSON/privacy/deadline mismatch")
	}
	f.pendingChildren[child.StateDigest] = values
	return driver.RowsAffected(1), nil
}

func newIdentityProofStorageV2(t *testing.T, kind domain.IdentityOperationKindV2) (*identityProofSQLV2, *Store, context.Context, identityOperationStartV2, identityConfigurationV2) {
	t.Helper()
	state, identity, session, _ := identityRowsV2Data(t)
	if kind == domain.IdentityOperationUnlinkV2 {
		other := identityRowsV2Identity(domain.IdentityProviderTelegram, identity.Revision)
		state.forward[identityRowsV2SubjectKey(other.Subject)] = identityRowsV2JSON(t, other)
		state.reverse[identityRowsV2ReverseKey(other.UserID, other.Subject)] = identityRowsV2Projection{user: other.UserID, subject: other.Subject, created: other.CreatedAt}
	}
	config := identityConfigurationV2{ManifestDigest: domain.DigestSecret("proof-manifest"), SensitiveOperationsEnabled: true, Providers: []domain.IdentityProviderBindingV2{{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "proof-yandex", CallbackURI: "https://sessionless.example/auth/login/callback"}, {Provider: domain.IdentityProviderTelegram, Issuer: "https://oauth.telegram.org", ClientID: "proof-telegram", CallbackURI: "https://sessionless.example/auth/telegram/callback"}}}
	phase := identityPhaseRequestV2{OperationDigest: domain.DigestSecret("proof-operation"), SessionDigest: session.Session.SessionDigest, CSRFDigest: session.Session.CSRFTokenDigest, BrowserBindingDigest: domain.DigestSecret("proof-browser"), ExpectedRevision: identity.Revision}
	request := identityOperationStartV2{Phase: phase, Kind: kind, TargetProvider: domain.IdentityProviderTelegram, SourceChallengeDigest: domain.DigestSecret("proof-source-child"), TargetChallengeDigest: domain.DigestSecret("proof-target-child")}
	if kind == domain.IdentityOperationUnlinkV2 {
		request.TargetProvider = domain.IdentityProviderYandex
		request.TargetChallengeDigest = ""
	}
	fixture := &identityProofSQLV2{identityOperationSQLV2: &identityOperationSQLV2{base: &identityRowsV2Fixture{durable: state.clone(), queryLimit: 192}, digest: phase.OperationDigest, at: identityRowsV2At()}, children: make(map[domain.SecretDigest][]any)}
	db := sql.OpenDB(fixture)
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := New(db, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return fixture, store, ctx, request, config
}

func identityProofChildRequestV2(operation domain.IdentityOperationV2, phase identityPhaseRequestV2) identityProofChildV2 {
	provider, digest := operation.Pending.SourceSubject.Provider, operation.Pending.SourceChallengeDigest
	if operation.Phase == domain.IdentityPhaseTargetProofV2 {
		provider, digest = operation.Pending.TargetProvider, operation.Pending.TargetChallengeDigest
	}
	nonce := ""
	if provider == domain.IdentityProviderTelegram {
		nonce = "synthetic-proof-nonce"
	}
	return identityProofChildV2{Phase: phase, ChallengeDigest: digest, PKCEVerifier: strings.Repeat("s", 64), Nonce: nonce}
}

func completeIdentityProofStepV2(t *testing.T, f *identityProofSQLV2, store *Store, ctx context.Context, phase identityPhaseRequestV2, config identityConfigurationV2, operation domain.IdentityOperationV2) domain.IdentityOperationV2 {
	t.Helper()
	f.at = f.at.Add(time.Second)
	child, err := store.createIdentityProofV2(ctx, identityProofChildRequestV2(operation, phase), config)
	if err != nil {
		t.Fatalf("create exact child: %v", err)
	}
	f.at = f.at.Add(time.Second)
	exchange, err := store.consumeIdentityProofV2(ctx, identityProofConsumeV2{Phase: phase, ChallengeDigest: child.StateDigest, BrowserSecret: "proof-browser"}, config)
	if err != nil {
		t.Fatalf("consume before synthetic exchange: %v", err)
	}
	if exchange.PKCEVerifier == "" || f.children[child.StateDigest][3] != "" || f.children[child.StateDigest][4] != "" {
		t.Fatal("confirmed consume did not expose ephemeral preparation only after scalar scrub")
	}
	stored, err := domain.DecodeIdentityChallengeV2([]byte(f.children[child.StateDigest][9].(string)))
	if err != nil || stored.Secrets != nil || stored.ConsumedAt == nil {
		t.Fatalf("consumed private JSON not scrubbed: (%+v,%v)", stored, err)
	}
	verified := acceptedIdentityProofV2{Subject: operation.Pending.SourceSubject, Kind: domain.IdentityProofYandexAuthorizationV2, Binding: child.Binding}
	if operation.Phase == domain.IdentityPhaseTargetProofV2 {
		verified.Subject = domain.ExternalSubject{Provider: operation.Pending.TargetProvider, Subject: "new-linked-subject"}
	}
	if child.Binding.Provider == domain.IdentityProviderTelegram {
		at := child.CreatedAt
		verified.Kind, verified.SignedAuthTime = domain.IdentityProofTelegramAuthTimeV2, &at
	}
	f.at = f.at.Add(time.Second)
	next, err := store.recordIdentityProofV2(ctx, identityProofCompletionV2{Phase: phase, ChallengeDigest: child.StateDigest, Verified: verified}, config)
	if err != nil {
		t.Fatalf("record accepted synthetic adapter observation: %v", err)
	}
	return next
}

func TestIdentityProofStorageV2CompleteSourceAndIndependentTarget(t *testing.T) {
	for _, kind := range []domain.IdentityOperationKindV2{domain.IdentityOperationLinkV2, domain.IdentityOperationUnlinkV2} {
		t.Run(string(kind), func(t *testing.T) {
			f, store, ctx, request, config := newIdentityProofStorageV2(t, kind)
			before := f.base.durable.clone()
			operation, err := store.startIdentityOperationV2(ctx, request, config)
			if err != nil {
				t.Fatal(err)
			}
			if operation.CreatedAt != f.at || operation.ExpiresAt != f.at.Add(10*time.Minute) || operation.Pending.Scope.ExpectedRevision != request.Phase.ExpectedRevision {
				t.Fatalf("start did not fix DB original scope/deadline: %+v", operation)
			}
			operation = completeIdentityProofStepV2(t, f, store, ctx, request.Phase, config, operation)
			if kind == domain.IdentityOperationLinkV2 {
				if operation.Phase != domain.IdentityPhaseTargetProofV2 {
					t.Fatal("source proof skipped independent target")
				}
				operation = completeIdentityProofStepV2(t, f, store, ctx, request.Phase, config, operation)
			}
			if operation.Phase != domain.IdentityPhaseConfirmV2 || operation.ValidateConfirm(f.at) != nil {
				t.Fatalf("proof storage did not reach exact confirm: %+v", operation)
			}
			if !reflect.DeepEqual(before, f.base.durable) || len(f.audit) != 0 {
				t.Error("proof acquisition attached identity, granted membership, changed cookies or wrote a success audit")
			}
			if f.base.commits != 1+3*len(f.children) || f.base.rangeOpen != 0 {
				t.Errorf("phase boundaries/closed ranges commits=%d children=%d open=%d", f.base.commits, len(f.children), f.base.rangeOpen)
			}
		})
	}
}

func TestIdentityProofStorageV2StartDerivesSlotsAndNeverResets(t *testing.T) {
	for _, test := range []struct {
		name   string
		kind   domain.IdentityOperationKindV2
		mutate func(*identityProofSQLV2, *identityOperationStartV2, *identityConfigurationV2)
		want   bool
	}{
		{name: "link missing target provider", kind: domain.IdentityOperationLinkV2, mutate: func(_ *identityProofSQLV2, _ *identityOperationStartV2, c *identityConfigurationV2) {
			c.Providers = c.Providers[:1]
		}},
		{name: "unlink disabled target with enabled survivor", kind: domain.IdentityOperationUnlinkV2, want: true, mutate: func(_ *identityProofSQLV2, _ *identityOperationStartV2, c *identityConfigurationV2) {
			c.Providers = c.Providers[1:]
		}},
		{name: "unlink disabled survivor", kind: domain.IdentityOperationUnlinkV2, mutate: func(_ *identityProofSQLV2, _ *identityOperationStartV2, c *identityConfigurationV2) {
			c.Providers = c.Providers[:1]
		}},
		{name: "wrong expected revision", kind: domain.IdentityOperationLinkV2, mutate: func(_ *identityProofSQLV2, r *identityOperationStartV2, _ *identityConfigurationV2) {
			r.Phase.ExpectedRevision++
		}},
		{name: "same child digests", kind: domain.IdentityOperationLinkV2, mutate: func(_ *identityProofSQLV2, r *identityOperationStartV2, _ *identityConfigurationV2) {
			r.TargetChallengeDigest = r.SourceChallengeDigest
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, s, ctx, r, c := newIdentityProofStorageV2(t, test.kind)
			test.mutate(f, &r, &c)
			got, err := s.startIdentityOperationV2(ctx, r, c)
			if (err == nil) != test.want || (!test.want && !reflect.DeepEqual(got, domain.IdentityOperationV2{})) {
				t.Errorf("start=(%+v,%v), wantAllowed=%v", got, err, test.want)
			}
		})
	}
	f, s, ctx, r, c := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
	original, err := s.startIdentityOperationV2(ctx, r, c)
	if err != nil {
		t.Fatal(err)
	}
	raw, writes := f.operation, f.base.writes
	f.at = f.at.Add(time.Minute)
	if got, err := s.startIdentityOperationV2(ctx, r, c); err == nil || !reflect.DeepEqual(got, domain.IdentityOperationV2{}) || f.operation != raw || f.base.writes != writes {
		t.Errorf("existing parent reset original deadline/phase: (%+v,%v), original=%+v", got, err, original)
	}
}

func TestIdentityProofStorageV2ChildPhaseReplayAndNoSecretReset(t *testing.T) {
	f, s, ctx, r, c := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
	operation, err := s.startIdentityOperationV2(ctx, r, c)
	if err != nil {
		t.Fatal(err)
	}
	f.at = f.at.Add(time.Second)
	wrong := identityProofChildRequestV2(operation, r.Phase)
	wrong.ChallengeDigest = r.TargetChallengeDigest
	if got, err := s.createIdentityProofV2(ctx, wrong, c); err == nil || !reflect.DeepEqual(got, domain.IdentityChallengeV2{}) || len(f.children) != 0 {
		t.Errorf("target child created in source phase: (%+v,%v)", got, err)
	}
	childReq := identityProofChildRequestV2(operation, r.Phase)
	child, err := s.createIdentityProofV2(ctx, childReq, c)
	if err != nil {
		t.Fatal(err)
	}
	original := append([]any(nil), f.children[child.StateDigest]...)
	childReq.PKCEVerifier = strings.Repeat("x", 64)
	if got, err := s.createIdentityProofV2(ctx, childReq, c); err == nil || !reflect.DeepEqual(got, domain.IdentityChallengeV2{}) || !reflect.DeepEqual(original, f.children[child.StateDigest]) {
		t.Errorf("existing child reset secrets/deadline: (%+v,%v)", got, err)
	}
	f.at = f.at.Add(time.Second)
	consume := identityProofConsumeV2{Phase: r.Phase, ChallengeDigest: child.StateDigest, BrowserSecret: "proof-browser"}
	if _, err := s.consumeIdentityProofV2(ctx, consume, c); err != nil {
		t.Fatal(err)
	}
	if exchange, err := s.consumeIdentityProofV2(ctx, consume, c); err == nil || !reflect.DeepEqual(exchange, domain.IdentityChallengeExchangeV2{}) {
		t.Errorf("consumed child was exchanged again: (%+v,%v)", exchange, err)
	}
}

func TestIdentityProofStorageV2AmbiguousConsumeReturnsNoExchange(t *testing.T) {
	f, s, ctx, r, c := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
	operation, err := s.startIdentityOperationV2(ctx, r, c)
	if err != nil {
		t.Fatal(err)
	}
	f.at = f.at.Add(time.Second)
	child, err := s.createIdentityProofV2(ctx, identityProofChildRequestV2(operation, r.Phase), c)
	if err != nil {
		t.Fatal(err)
	}
	f.at = f.at.Add(time.Second)
	f.commitReplyError = errors.New("controlled lost consume commit reply")
	exchange, err := s.consumeIdentityProofV2(ctx, identityProofConsumeV2{Phase: r.Phase, ChallengeDigest: child.StateDigest, BrowserSecret: "proof-browser"}, c)
	if err == nil || !reflect.DeepEqual(exchange, domain.IdentityChallengeExchangeV2{}) {
		t.Errorf("ambiguous commit released external-exchange material: (%+v,%v)", exchange, err)
	}
	stored, decodeErr := domain.DecodeIdentityChallengeV2([]byte(f.children[child.StateDigest][9].(string)))
	if decodeErr != nil || stored.ConsumedAt == nil || stored.Secrets != nil || f.children[child.StateDigest][3] != "" || f.children[child.StateDigest][4] != "" {
		t.Errorf("lost-ACK committed child not scrubbed: (%+v,%v)", stored, decodeErr)
	}
}

func TestIdentityProofStorageV2ConsumeRollbackPreservesOriginalChild(t *testing.T) {
	f, s, ctx, r, c := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
	operation, err := s.startIdentityOperationV2(ctx, r, c)
	if err != nil {
		t.Fatal(err)
	}
	f.at = f.at.Add(time.Second)
	child, err := s.createIdentityProofV2(ctx, identityProofChildRequestV2(operation, r.Phase), c)
	if err != nil {
		t.Fatal(err)
	}
	original := append([]any(nil), f.children[child.StateDigest]...)
	f.childFault = errors.New("controlled consume write failure")
	f.at = f.at.Add(time.Second)
	exchange, err := s.consumeIdentityProofV2(ctx, identityProofConsumeV2{Phase: r.Phase, ChallengeDigest: child.StateDigest, BrowserSecret: "proof-browser"}, c)
	if err == nil || !reflect.DeepEqual(exchange, domain.IdentityChallengeExchangeV2{}) || !reflect.DeepEqual(original, f.children[child.StateDigest]) || f.base.pending != nil || f.base.rollbacks == 0 {
		t.Errorf("failed original transaction leaked exchange/child: exchange=%+v err=%v rollbacks=%d", exchange, err, f.base.rollbacks)
	}
}

func TestIdentityProofStorageV2EveryPhaseRejoinsAuthority(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *identityProofSQLV2, *identityProofConsumeV2, *identityConfigurationV2)
	}{
		{name: "browser secret", mutate: func(_ *testing.T, _ *identityProofSQLV2, r *identityProofConsumeV2, _ *identityConfigurationV2) {
			r.BrowserSecret = "foreign"
		}},
		{name: "CSRF", mutate: func(_ *testing.T, _ *identityProofSQLV2, r *identityProofConsumeV2, _ *identityConfigurationV2) {
			r.Phase.CSRFDigest = domain.DigestSecret("foreign")
		}},
		{name: "configuration", mutate: func(_ *testing.T, _ *identityProofSQLV2, _ *identityProofConsumeV2, c *identityConfigurationV2) {
			c.ManifestDigest = domain.DigestSecret("foreign")
		}},
		{name: "client callback", mutate: func(_ *testing.T, _ *identityProofSQLV2, _ *identityProofConsumeV2, c *identityConfigurationV2) {
			c.Providers[0].CallbackURI = "https://other.invalid/auth/login/callback"
		}},
		{name: "identity revocation", mutate: func(t *testing.T, f *identityProofSQLV2, _ *identityProofConsumeV2, _ *identityConfigurationV2) {
			for key, raw := range f.base.durable.forward {
				i, err := domain.DecodeExternalIdentityV2([]byte(raw))
				if err != nil {
					t.Fatal(err)
				}
				i.State = domain.ExternalIdentityUnlinkedV2
				f.base.durable.forward[key] = identityRowsV2JSON(t, i)
			}
		}},
		{name: "membership suspension", mutate: func(t *testing.T, f *identityProofSQLV2, _ *identityProofConsumeV2, _ *identityConfigurationV2) {
			for key, raw := range f.base.durable.members {
				var m domain.TenantMembership
				if err := json.Unmarshal([]byte(raw), &m); err != nil {
					t.Fatal(err)
				}
				m.Status = domain.TenantMembershipSuspended
				f.base.durable.members[key] = identityRowsV2JSON(t, m)
			}
		}},
		{name: "expired logical parent", mutate: func(_ *testing.T, f *identityProofSQLV2, _ *identityProofConsumeV2, _ *identityConfigurationV2) {
			f.at = f.at.Add(10 * time.Minute)
		}},
		{name: "retained physical PKCE mismatch", mutate: func(_ *testing.T, f *identityProofSQLV2, r *identityProofConsumeV2, _ *identityConfigurationV2) {
			f.children[r.ChallengeDigest][3] = "corrupt"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, s, ctx, r, c := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
			op, err := s.startIdentityOperationV2(ctx, r, c)
			if err != nil {
				t.Fatal(err)
			}
			f.at = f.at.Add(time.Second)
			child, err := s.createIdentityProofV2(ctx, identityProofChildRequestV2(op, r.Phase), c)
			if err != nil {
				t.Fatal(err)
			}
			request := identityProofConsumeV2{Phase: r.Phase, ChallengeDigest: child.StateDigest, BrowserSecret: "proof-browser"}
			test.mutate(t, f, &request, &c)
			writes := f.base.writes
			f.at = f.at.Add(time.Second)
			got, err := s.consumeIdentityProofV2(ctx, request, c)
			if err == nil || !reflect.DeepEqual(got, domain.IdentityChallengeExchangeV2{}) || f.base.writes != writes {
				t.Errorf("changed current authority consumed proof: (%+v,%v), writes=%d before=%d", got, err, f.base.writes, writes)
			}
		})
	}
}

func TestIdentityProofStorageV2RetryKeepsFirstDatabaseObservation(t *testing.T) {
	f, s, ctx, r, c := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
	first := f.at
	f.rollbackOnce = true
	f.retryAdvance = time.Second
	op, err := s.startIdentityOperationV2(ctx, r, c)
	if err != nil {
		t.Fatal(err)
	}
	if !op.CreatedAt.Equal(first) || !op.ExpiresAt.Equal(first.Add(10*time.Minute)) || f.base.begins != 2 {
		t.Errorf("retry extended original parent: created=%v expires=%v begins=%d", op.CreatedAt, op.ExpiresAt, f.base.begins)
	}
	f.at = f.at.Add(time.Second)
	childFirst := f.at
	f.rollbackOnce = true
	child, err := s.createIdentityProofV2(ctx, identityProofChildRequestV2(op, r.Phase), c)
	if err != nil {
		t.Fatal(err)
	}
	if !child.CreatedAt.Equal(childFirst) || !child.ExpiresAt.Equal(op.ExpiresAt) {
		t.Error("retry renewed child creation/deadline")
	}
	f.at = f.at.Add(time.Second)
	consumeFirst := f.at
	f.rollbackOnce = true
	if _, err := s.consumeIdentityProofV2(ctx, identityProofConsumeV2{Phase: r.Phase, ChallengeDigest: child.StateDigest, BrowserSecret: "proof-browser"}, c); err != nil {
		t.Fatal(err)
	}
	stored, err := domain.DecodeIdentityChallengeV2([]byte(f.children[child.StateDigest][9].(string)))
	if err != nil || stored.ConsumedAt == nil || !stored.ConsumedAt.Equal(consumeFirst) {
		t.Fatalf("retry refreshed consumed observation: (%+v,%v)", stored, err)
	}
	f.at = f.at.Add(time.Second)
	completeFirst := f.at
	f.rollbackOnce = true
	next, err := s.recordIdentityProofV2(ctx, identityProofCompletionV2{Phase: r.Phase, ChallengeDigest: child.StateDigest, Verified: acceptedIdentityProofV2{Subject: op.Pending.SourceSubject, Kind: domain.IdentityProofYandexAuthorizationV2, Binding: child.Binding}}, c)
	if err != nil {
		t.Fatal(err)
	}
	if next.Pending.SourceProof == nil || !next.Pending.SourceProof.CompletedAt.Equal(completeFirst) {
		t.Errorf("retry refreshed accepted proof completion: %+v", next.Pending.SourceProof)
	}
}

func TestIdentityProofStorageV2CompletionRequiresCanonicalConsumedChild(t *testing.T) {
	for _, test := range []struct {
		name       string
		target     bool
		unconsumed bool
		mutate     func(*testing.T, *identityProofSQLV2, *identityProofCompletionV2)
	}{
		{name: "unconsumed child", unconsumed: true},
		{name: "wrong source subject", mutate: func(_ *testing.T, _ *identityProofSQLV2, r *identityProofCompletionV2) {
			r.Verified.Subject.Subject = "other-source"
		}},
		{name: "wrong accepted client", mutate: func(_ *testing.T, _ *identityProofSQLV2, r *identityProofCompletionV2) {
			r.Verified.Binding.ClientID = "other-client"
		}},
		{name: "arbitrary proof kind", mutate: func(_ *testing.T, _ *identityProofSQLV2, r *identityProofCompletionV2) { r.Verified.Kind = "verified" }},
		{name: "missing signed auth time", target: true, mutate: func(_ *testing.T, _ *identityProofSQLV2, r *identityProofCompletionV2) {
			r.Verified.SignedAuthTime = nil
		}},
		{name: "old signed auth time", target: true, mutate: func(_ *testing.T, _ *identityProofSQLV2, r *identityProofCompletionV2) {
			at := identityRowsV2At().Add(-time.Hour)
			r.Verified.SignedAuthTime = &at
		}},
		{name: "target provider mixup", target: true, mutate: func(_ *testing.T, _ *identityProofSQLV2, r *identityProofCompletionV2) {
			r.Verified.Subject.Provider = domain.IdentityProviderYandex
		}},
		{name: "foreign target ownership", target: true, mutate: func(t *testing.T, f *identityProofSQLV2, r *identityProofCompletionV2) {
			row := identityRowsV2Identity(domain.IdentityProviderTelegram, 7)
			row.Subject.Subject = "foreign-target"
			row.UserID = "other-owner"
			f.base.durable.forward[identityRowsV2SubjectKey(row.Subject)] = identityRowsV2JSON(t, row)
			r.Verified.Subject = row.Subject
		}},
		{name: "current identity revoked after consume", mutate: func(t *testing.T, f *identityProofSQLV2, _ *identityProofCompletionV2) {
			for key, raw := range f.base.durable.forward {
				row, err := domain.DecodeExternalIdentityV2([]byte(raw))
				if err != nil {
					t.Fatal(err)
				}
				row.Revision++
				f.base.durable.forward[key] = identityRowsV2JSON(t, row)
			}
		}},
		{name: "physical retained nonce after consume", target: true, mutate: func(_ *testing.T, f *identityProofSQLV2, r *identityProofCompletionV2) {
			f.children[r.ChallengeDigest][4] = "retained-nonce"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, s, ctx, start, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
			operation, err := s.startIdentityOperationV2(ctx, start, config)
			if err != nil {
				t.Fatal(err)
			}
			if test.target {
				operation = completeIdentityProofStepV2(t, f, s, ctx, start.Phase, config, operation)
			}
			f.at = f.at.Add(time.Second)
			child, err := s.createIdentityProofV2(ctx, identityProofChildRequestV2(operation, start.Phase), config)
			if err != nil {
				t.Fatal(err)
			}
			if !test.unconsumed {
				f.at = f.at.Add(time.Second)
				if _, err := s.consumeIdentityProofV2(ctx, identityProofConsumeV2{Phase: start.Phase, ChallengeDigest: child.StateDigest, BrowserSecret: "proof-browser"}, config); err != nil {
					t.Fatal(err)
				}
			}
			verified := acceptedIdentityProofV2{Subject: operation.Pending.SourceSubject, Kind: domain.IdentityProofYandexAuthorizationV2, Binding: child.Binding}
			if test.target {
				authTime := child.CreatedAt
				verified.Subject = domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "new-linked-subject"}
				verified.Kind = domain.IdentityProofTelegramAuthTimeV2
				verified.SignedAuthTime = &authTime
			}
			request := identityProofCompletionV2{Phase: start.Phase, ChallengeDigest: child.StateDigest, Verified: verified}
			if test.mutate != nil {
				test.mutate(t, f, &request)
			}
			before, writes := f.operation, f.base.writes
			canonicalBefore := f.base.durable.clone()
			f.at = f.at.Add(time.Second)
			next, err := s.recordIdentityProofV2(ctx, request, config)
			if err == nil || !reflect.DeepEqual(next, domain.IdentityOperationV2{}) || f.operation != before || f.base.writes != writes || !reflect.DeepEqual(canonicalBefore, f.base.durable) {
				t.Errorf("invalid accepted-proof input advanced/changed ownership: next=%+v err=%v writes=%d want=%d", next, err, f.base.writes, writes)
			}
		})
	}
}

func TestIdentityProofStorageV2CompletionReplayAndCreateCurrentCSRF(t *testing.T) {
	f, s, ctx, start, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
	operation, err := s.startIdentityOperationV2(ctx, start, config)
	if err != nil {
		t.Fatal(err)
	}
	f.at = f.at.Add(time.Second)
	wrong := identityProofChildRequestV2(operation, start.Phase)
	wrong.Phase.CSRFDigest = domain.DigestSecret("wrong-current-csrf")
	if child, err := s.createIdentityProofV2(ctx, wrong, config); err == nil || !reflect.DeepEqual(child, domain.IdentityChallengeV2{}) || len(f.children) != 0 {
		t.Errorf("child creation bypassed current CSRF: (%+v,%v)", child, err)
	}
	next := completeIdentityProofStepV2(t, f, s, ctx, start.Phase, config, operation)
	child, err := domain.DecodeIdentityChallengeV2([]byte(f.children[operation.Pending.SourceChallengeDigest][9].(string)))
	if err != nil {
		t.Fatal(err)
	}
	request := identityProofCompletionV2{Phase: start.Phase, ChallengeDigest: child.StateDigest, Verified: acceptedIdentityProofV2{Subject: operation.Pending.SourceSubject, Kind: domain.IdentityProofYandexAuthorizationV2, Binding: child.Binding}}
	before, writes := f.operation, f.base.writes
	f.at = f.at.Add(time.Second)
	if replay, err := s.recordIdentityProofV2(ctx, request, config); err == nil || !reflect.DeepEqual(replay, domain.IdentityOperationV2{}) || f.operation != before || f.base.writes != writes {
		t.Errorf("completion replay advanced source twice: next=%+v err=%v current=%+v", replay, err, next)
	}
}

func consumedUnlinkSourceFixtureV2(t *testing.T) (*identityProofSQLV2, *Store, context.Context, identityProofCompletionV2, identityConfigurationV2, domain.IdentityOperationV2, domain.IdentityChallengeV2) {
	t.Helper()
	f, store, ctx, start, config := newIdentityProofStorageV2(t, domain.IdentityOperationUnlinkV2)
	operation, err := store.startIdentityOperationV2(ctx, start, config)
	if err != nil {
		t.Fatal(err)
	}
	if operation.Pending.SourceSubject.Provider != domain.IdentityProviderTelegram || operation.Pending.SourceProof != nil {
		t.Fatal("causal fixture requires unproved Telegram unlink SOURCE, not an already-proved source or link target")
	}
	f.at = f.at.Add(time.Second)
	child, err := store.createIdentityProofV2(ctx, identityProofChildRequestV2(operation, start.Phase), config)
	if err != nil {
		t.Fatal(err)
	}
	f.at = f.at.Add(time.Second)
	if _, err := store.consumeIdentityProofV2(ctx, identityProofConsumeV2{Phase: start.Phase, ChallengeDigest: child.StateDigest, BrowserSecret: "proof-browser"}, config); err != nil {
		t.Fatal(err)
	}
	consumed, err := domain.DecodeIdentityChallengeV2([]byte(f.children[child.StateDigest][9].(string)))
	if err != nil || consumed.ConsumedAt == nil || consumed.Secrets != nil {
		t.Fatalf("canonical source consumption not retained: (%+v,%v)", consumed, err)
	}
	authTime := child.CreatedAt
	request := identityProofCompletionV2{Phase: start.Phase, ChallengeDigest: child.StateDigest, Verified: acceptedIdentityProofV2{Subject: operation.Pending.SourceSubject, Kind: domain.IdentityProofTelegramAuthTimeV2, SignedAuthTime: &authTime, Binding: child.Binding}}
	return f, store, ctx, request, config, operation, consumed
}

func TestIdentityProofStorageV2CompletionWindowExclusiveBoundary(t *testing.T) {
	for _, test := range []struct {
		name    string
		delay   time.Duration
		allowed bool
	}{
		{name: "just inside 15 seconds", delay: 15*time.Second - time.Microsecond, allowed: true},
		{name: "exactly 15 seconds", delay: 15 * time.Second},
		{name: "six minutes late Telegram unlink source", delay: 6 * time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, store, ctx, request, config, operation, consumed := consumedUnlinkSourceFixtureV2(t)
			before, writes, commits := f.operation, f.base.writes, f.base.commits
			childrenBefore := cloneIdentityProofRowsV2(f.children)
			f.at = consumed.ConsumedAt.Add(test.delay)
			if !f.at.Before(operation.ExpiresAt) {
				t.Fatal("causal regression must remain inside original ten-minute parent lifetime")
			}
			next, err := store.recordIdentityProofV2(ctx, request, config)
			if test.allowed {
				if err != nil || next.Phase != domain.IdentityPhaseConfirmV2 || next.Pending == nil || next.Pending.SourceProof == nil || !next.Pending.SourceProof.CompletedAt.Equal(f.at) {
					t.Fatalf("inside-window source completion=(%+v,%v)", next, err)
				}
			} else {
				if err == nil || !reflect.DeepEqual(next, domain.IdentityOperationV2{}) || f.operation != before || f.base.writes != writes || f.base.commits != commits {
					t.Errorf("late source minted fresh proof/changed original phase: next=%+v err=%v writes=%d want=%d commits=%d want=%d", next, err, f.base.writes, writes, f.base.commits, commits)
				}
				if exchange, err := store.consumeIdentityProofV2(ctx, identityProofConsumeV2{Phase: request.Phase, ChallengeDigest: request.ChallengeDigest, BrowserSecret: "proof-browser"}, config); err == nil || !reflect.DeepEqual(exchange, domain.IdentityChallengeExchangeV2{}) {
					t.Errorf("completion rejection allowed re-exchange of consumed child: (%+v,%v)", exchange, err)
				}
			}
			if !reflect.DeepEqual(childrenBefore, f.children) || f.children[request.ChallengeDigest][3] != "" || f.children[request.ChallengeDigest][4] != "" {
				t.Error("completion changed canonical consumed/scrubbed child")
			}
		})
	}
}

func TestIdentityProofStorageV2CompletionRetryCannotCrossConsumedDeadline(t *testing.T) {
	f, store, ctx, request, config, operation, consumed := consumedUnlinkSourceFixtureV2(t)
	before, writes, commits, begins := f.operation, f.base.writes, f.base.commits, f.base.begins
	childrenBefore := cloneIdentityProofRowsV2(f.children)
	f.at = consumed.ConsumedAt.Add(14 * time.Second)
	f.rollbackOnce = true
	f.retryAdvance = 2 * time.Second
	next, err := store.recordIdentityProofV2(ctx, request, config)
	if err == nil || !reflect.DeepEqual(next, domain.IdentityOperationV2{}) || f.operation != before || f.base.commits != commits || f.base.begins != begins+2 {
		t.Errorf("retry crossed immutable consume deadline: next=%+v err=%v commits=%d want=%d begins=%d want=%d", next, err, f.base.commits, commits, f.base.begins, begins+2)
	}
	// The first in-window callback wrote only its discarded snapshot. The second
	// callback must issue NO parent write and no callback may publish a result.
	if f.base.writes != writes+1 || f.base.pending != nil {
		t.Errorf("late retry issued another speculative write/leaked snapshot: writes=%d want=%d pending=%v", f.base.writes, writes+1, f.base.pending != nil)
	}
	stored, decodeErr := domain.DecodeIdentityOperationV2([]byte(f.operation))
	if decodeErr != nil || stored.Phase != operation.Phase || stored.Pending.SourceProof != nil || !stored.CreatedAt.Equal(operation.CreatedAt) || !stored.ExpiresAt.Equal(operation.ExpiresAt) || !stored.DeleteAfter.Equal(operation.DeleteAfter) {
		t.Errorf("retry changed original canonical phase/deadlines: (%+v,%v)", stored, decodeErr)
	}
	if !reflect.DeepEqual(childrenBefore, f.children) || f.children[request.ChallengeDigest][3] != "" || f.children[request.ChallengeDigest][4] != "" {
		t.Error("retry reset/unscrubbed canonical consumed child")
	}
	if exchange, err := store.consumeIdentityProofV2(ctx, identityProofConsumeV2{Phase: request.Phase, ChallengeDigest: request.ChallengeDigest, BrowserSecret: "proof-browser"}, config); err == nil || !reflect.DeepEqual(exchange, domain.IdentityChallengeExchangeV2{}) {
		t.Errorf("late retry permitted child re-exchange: (%+v,%v)", exchange, err)
	}
}
