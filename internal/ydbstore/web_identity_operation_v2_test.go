package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Extends the original isolated Connector fixture with operation/audit rows.
// One snapshot owns ALL tables; this is controlled-driver evidence, not YDB
// serializability, provider verification or operation-acquisition proof.
type identityOperationSQLV2 struct {
	base                        *identityRowsV2Fixture
	operation, pendingOperation string
	audit, pendingAudit         [][]any
	digest                      domain.SecretDigest
	at                          time.Time
	commitReplyError            error
}

type identityOperationConnV2 struct {
	*identityRowsV2Conn
	f *identityOperationSQLV2
}
type identityOperationTxV2 struct{ f *identityOperationSQLV2 }

func (f *identityOperationSQLV2) Driver() driver.Driver { return &identityRowsV2Driver{} }
func (f *identityOperationSQLV2) Connect(context.Context) (driver.Conn, error) {
	return &identityOperationConnV2{identityRowsV2Conn: &identityRowsV2Conn{fixture: f.base}, f: f}, nil
}
func (c *identityOperationConnV2) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if _, err := c.identityRowsV2Conn.BeginTx(ctx, options); err != nil {
		return nil, err
	}
	c.f.base.mu.Lock()
	defer c.f.base.mu.Unlock()
	c.f.pendingOperation = c.f.operation
	c.f.pendingAudit = append([][]any(nil), c.f.audit...)
	return &identityOperationTxV2{f: c.f}, nil
}
func (tx *identityOperationTxV2) Commit() error {
	f := tx.f
	f.base.mu.Lock()
	defer f.base.mu.Unlock()
	if f.base.pending == nil || f.base.rangeOpen != 0 {
		return errors.New("identity commit without complete original snapshot")
	}
	f.base.durable = f.base.pending.clone()
	f.operation, f.audit = f.pendingOperation, f.pendingAudit
	f.base.pending = nil
	f.pendingOperation, f.pendingAudit = "", nil
	f.base.commits++
	err := f.commitReplyError
	f.commitReplyError = nil
	return err
}
func (tx *identityOperationTxV2) Rollback() error {
	f := tx.f
	f.base.mu.Lock()
	defer f.base.mu.Unlock()
	if f.base.pending == nil {
		return errors.New("identity rollback without original snapshot")
	}
	f.base.pending = nil
	f.pendingOperation, f.pendingAudit = "", nil
	f.base.rollbacks++
	return nil
}

func (c *identityOperationConnV2) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	boundedMembership, _, _ := ownedCanonicalJSONSelect(`SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3`)
	if statement != identityOperationRowSQLV2 && statement != `SELECT CurrentUtcTimestamp()` && statement != boundedMembership {
		return c.identityRowsV2Conn.QueryContext(ctx, statement, args)
	}
	f := c.f
	f.base.mu.Lock()
	defer f.base.mu.Unlock()
	if err := f.base.statement(ctx, statement, args, false); err != nil {
		return nil, err
	}
	rows := &identityRowsV2Rows{fixture: f.base, columns: []string{"value"}}
	values := identityRowsV2Args(args)
	switch statement {
	case `SELECT CurrentUtcTimestamp()`:
		if len(values) != 0 {
			return nil, errors.New("database clock has no caller inputs")
		}
		rows.values = [][]driver.Value{{f.at}}
	case identityOperationRowSQLV2:
		if !reflect.DeepEqual(values, []any{identityRowsV2ExpectedBucket(string(f.digest)), string(f.digest)}) {
			return nil, errors.New("operation lookup not original exact key")
		}
		raw := f.pendingOperation
		if len(raw) > domain.IdentityAuthMaxRecordBytesV2+1 {
			raw = raw[:domain.IdentityAuthMaxRecordBytesV2+1]
		}
		if raw != "" {
			rows.values = [][]driver.Value{{raw}}
		}
	case boundedMembership:
		if len(values) != 3 {
			return nil, errors.New("membership requires exact keys")
		}
		user, userOK := values[1].(string)
		tenant, tenantOK := values[2].(string)
		if !userOK || !tenantOK || values[0] != identityRowsV2ExpectedBucket(user) {
			return nil, errors.New("membership key/bucket mismatch")
		}
		if raw, exists := f.base.pending.members[identityRowsV2MemberKey(domain.UserID(user), domain.TenantID(tenant))]; exists {
			if len(raw) > ownedAuthorityHeadBytes+1 {
				raw = raw[:ownedAuthorityHeadBytes+1]
			}
			rows.values = [][]driver.Value{{raw}}
		}
	}
	return rows, nil
}

const identityOperationWriteSQLTestV2 = `UPSERT INTO oidc_login_challenges (shard_bucket,state_digest,browser_binding_digest,pkce_verifier,nonce,redirect_path, created_at,expires_at,consumed_at,record,delete_after) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,CAST($10 AS JsonDocument),$11)`
const identityOperationAuditSQLTestV2 = `INSERT INTO audit_events (tenant_id,occurred_at,audit_event_id,actor_id,action,subject_kind,subject_id,outcome,metadata,expire_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS JsonDocument),$10)`

func (c *identityOperationConnV2) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	normalized := identityRowsV2Statement(statement)
	if normalized != identityOperationWriteSQLTestV2 && normalized != identityOperationAuditSQLTestV2 {
		return c.identityRowsV2Conn.ExecContext(ctx, statement, args)
	}
	f := c.f
	f.base.mu.Lock()
	defer f.base.mu.Unlock()
	if err := f.base.statement(ctx, statement, args, true); err != nil {
		return nil, err
	}
	values := identityRowsV2Args(args)
	if normalized == identityOperationWriteSQLTestV2 {
		if len(values) != 11 {
			return nil, errors.New("operation write shape mismatch")
		}
		raw, ok := values[9].(string)
		operation, err := domain.DecodeIdentityOperationV2([]byte(raw))
		if !ok || err != nil {
			return nil, errors.New("operation write not closed V2 JSON")
		}
		browser, consumed := "", time.Unix(0, 0).UTC()
		if operation.Pending != nil {
			browser = string(operation.Pending.Scope.BrowserBindingDigest)
		} else {
			consumed = operation.Receipt.CompletedAt
		}
		want := []any{identityRowsV2ExpectedBucket(string(f.digest)), string(f.digest), browser, "", "", "", operation.CreatedAt, operation.ExpiresAt, consumed, raw, operation.DeleteAfter}
		if operation.OperationDigest != f.digest || !reflect.DeepEqual(values, want) {
			return nil, errors.New("operation scalar privacy/key/retention mismatch")
		}
		f.pendingOperation = raw
	} else {
		if len(values) != 10 || values[2] != "audit-identity-"+string(f.digest) || values[5] != "identity_operation" || values[6] != string(f.digest) || values[7] != "succeeded" {
			return nil, errors.New("audit exact operation correlation mismatch")
		}
		f.pendingAudit = append(f.pendingAudit, values)
	}
	return driver.RowsAffected(1), nil
}

func identityOperationConfirmationFixtureV2(t *testing.T, kind domain.IdentityOperationKindV2) (*identityOperationSQLV2, *Store, context.Context, identityConfirmationV2, identityConfigurationV2, domain.IdentityOperationV2) {
	t.Helper()
	state, identity, session, membership := identityRowsV2Data(t)
	at := identityRowsV2At()
	config := identityConfigurationV2{ManifestDigest: domain.DigestSecret("transaction-manifest"), SensitiveOperationsEnabled: true, Providers: []domain.IdentityProviderBindingV2{
		{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "transaction-yandex", CallbackURI: "https://sessionless.example/auth/login/callback"},
		{Provider: domain.IdentityProviderTelegram, Issuer: "https://oauth.telegram.org", ClientID: "transaction-telegram", CallbackURI: "https://sessionless.example/auth/telegram/callback"},
	}}
	scope := domain.IdentityOperationScopeV2{UserID: identity.UserID, SessionDigest: session.Session.SessionDigest, AuthenticatedSubject: identity.Subject, TenantID: membership.TenantID, MembershipSecurityVersion: membership.SecurityVersion, BrowserBindingDigest: domain.DigestSecret("transaction-browser"), ConfigurationDigest: config.ManifestDigest, ExpectedRevision: identity.Revision}
	operation := domain.IdentityOperationV2{Version: 2, Purpose: domain.IdentityPurposeOperationV2, OperationDigest: domain.DigestSecret("transaction-operation"), Kind: kind, Phase: domain.IdentityPhaseSourceProofV2, CreatedAt: at.Add(-2 * time.Minute), ExpiresAt: at.Add(8 * time.Minute), DeleteAfter: at.Add(8 * time.Minute), Pending: &domain.IdentityOperationPendingV2{Scope: scope, SourceSubject: identity.Subject, TargetProvider: domain.IdentityProviderTelegram, SourceChallengeDigest: domain.DigestSecret("transaction-source-child"), TargetChallengeDigest: domain.DigestSecret("transaction-target-child")}}
	if kind == domain.IdentityOperationUnlinkV2 {
		remaining := identityRowsV2Identity(domain.IdentityProviderTelegram, identity.Revision)
		state.forward[identityRowsV2SubjectKey(remaining.Subject)] = identityRowsV2JSON(t, remaining)
		state.reverse[identityRowsV2ReverseKey(remaining.UserID, remaining.Subject)] = identityRowsV2Projection{user: remaining.UserID, subject: remaining.Subject, created: remaining.CreatedAt}
		operation.Pending.SourceSubject = remaining.Subject
		operation.Pending.TargetProvider = identity.Subject.Provider
		operation.Pending.TargetSubject = &identity.Subject
		operation.Pending.TargetChallengeDigest = ""
	}
	for operation.Phase != domain.IdentityPhaseConfirmV2 {
		pending := operation.Pending
		provider, stateDigest, expected := pending.SourceSubject.Provider, pending.SourceChallengeDigest, &pending.SourceSubject
		created := operation.CreatedAt.Add(10 * time.Second)
		if operation.Phase == domain.IdentityPhaseTargetProofV2 {
			provider, stateDigest, expected = pending.TargetProvider, pending.TargetChallengeDigest, nil
			created = pending.SourceProof.CompletedAt.Add(time.Second)
		}
		bindings, err := config.bindings()
		if err != nil {
			t.Fatal(err)
		}
		challenge := domain.IdentityChallengeV2{Version: 2, Purpose: domain.IdentityPurposeProviderProofV2, StateDigest: stateDigest, BrowserBindingDigest: scope.BrowserBindingDigest, Binding: bindings[provider], CreatedAt: created, ExpiresAt: operation.ExpiresAt, DeleteAfter: operation.ExpiresAt, Proof: &domain.IdentityProofChallengeV2{ParentOperationDigest: operation.OperationDigest, ParentPhase: operation.Phase, Scope: scope, ExpectedSubject: expected}, Secrets: &domain.IdentityChallengeSecretsV2{PKCEVerifier: strings.Repeat("p", 43)}}
		if provider == domain.IdentityProviderTelegram {
			challenge.Secrets.Nonce = "transaction-only-nonce"
		}
		consumed, _, err := challenge.Consume(bindings[provider], "transaction-browser", &operation, created.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		subject := pending.SourceSubject
		if operation.Phase == domain.IdentityPhaseTargetProofV2 {
			subject = domain.ExternalSubject{Provider: provider, Subject: "target-new"}
		}
		proof := domain.IdentityProviderProofV2{ChallengeDigest: stateDigest, ParentOperationDigest: operation.OperationDigest, ParentPhase: operation.Phase, Binding: bindings[provider], Subject: subject, Kind: domain.IdentityProofYandexAuthorizationV2, ChallengeCreatedAt: created, ChallengeExpiresAt: operation.ExpiresAt, CompletedAt: created.Add(2 * time.Second)}
		if provider == domain.IdentityProviderTelegram {
			proof.Kind, proof.SignedAuthTime = domain.IdentityProofTelegramAuthTimeV2, &created
		}
		operation, err = operation.RecordProof(consumed, proof, proof.CompletedAt)
		if err != nil {
			t.Fatal(err)
		}
	}
	fixture := &identityOperationSQLV2{base: &identityRowsV2Fixture{durable: state.clone(), queryLimit: 96}, operation: identityRowsV2JSON(t, operation), digest: operation.OperationDigest, at: at}
	db := sql.OpenDB(fixture)
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
	request := identityConfirmationV2{OperationDigest: operation.OperationDigest, SessionDigest: session.Session.SessionDigest, CSRFDigest: session.Session.CSRFTokenDigest, BrowserBindingDigest: scope.BrowserBindingDigest, ExpectedRevision: identity.Revision, NextSessionDigest: domain.DigestSecret("transaction-next-session"), NextCSRFDigest: domain.DigestSecret("transaction-next-csrf")}
	return fixture, store, ctx, request, config, operation
}

func TestIdentityOperationStorageV2AtomicLinkUnlinkAndReceipt(t *testing.T) {
	for _, kind := range []domain.IdentityOperationKindV2{domain.IdentityOperationLinkV2, domain.IdentityOperationUnlinkV2} {
		t.Run(string(kind), func(t *testing.T) {
			fixture, store, ctx, request, config, original := identityOperationConfirmationFixtureV2(t, kind)
			result, err := store.confirmIdentityOperationV2(ctx, request, config)
			if err != nil {
				t.Fatalf("atomic %s confirmation: %v; statements=%v", kind, err, fixture.base.statements)
			}
			if result.Receipt.AfterRevision == nil || *result.Receipt.AfterRevision != request.ExpectedRevision+1 || result.Session.IdentityRevision != request.ExpectedRevision+1 || result.Session.Session.AuthenticatedSubject != original.Pending.SourceSubject || fixture.base.commits != 1 || len(fixture.audit) != 1 {
				t.Fatalf("atomic outcome/cookie/audit mismatch: result=%+v commits=%d audit=%d", result, fixture.base.commits, len(fixture.audit))
			}
			terminal, err := domain.DecodeIdentityOperationV2([]byte(fixture.operation))
			if err != nil || terminal.Pending != nil || terminal.Receipt == nil || !terminal.DeleteAfter.Equal(original.CreatedAt.Add(24*time.Hour)) {
				t.Fatalf("stored receipt privacy/retention: operation=%+v err=%v", terminal, err)
			}
			old, err := domain.DecodeWebSessionV2([]byte(fixture.base.durable.sessions[string(request.SessionDigest)]))
			if err != nil || old.Session.RevokedAt == nil {
				t.Fatal("original cookie not revoked atomically", err)
			}
			for _, raw := range fixture.base.durable.forward {
				identity, err := domain.DecodeExternalIdentityV2([]byte(raw))
				if err != nil || identity.Revision != result.Session.IdentityRevision {
					t.Fatal("not every canonical method advanced", err)
				}
				_, projected := fixture.base.durable.reverse[identityRowsV2ReverseKey(identity.UserID, identity.Subject)]
				if projected != (identity.State == domain.ExternalIdentityActiveV2) {
					t.Error("reverse projection disagrees with canonical active/tombstone state")
				}
			}
			var audit map[string]any
			if err := json.Unmarshal([]byte(fixture.audit[0][8].(string)), &audit); err != nil || len(audit) != 4 || audit["before_revision"] != request.ExpectedRevision.String() || audit["after_revision"] != result.Session.IdentityRevision.String() {
				t.Errorf("content-free lossless audit: metadata=%v err=%v", audit, err)
			}
			writes := fixture.base.writes
			if denied, err := store.confirmIdentityOperationV2(ctx, request, config); err == nil || !reflect.DeepEqual(denied, identityConfirmationResultV2{}) || fixture.base.writes != writes {
				t.Error("revoked cookie recovered/replayed a commit")
			}
			request.SessionDigest, request.CSRFDigest = result.Session.Session.SessionDigest, result.Session.Session.CSRFTokenDigest
			config.SensitiveOperationsEnabled = false
			receipt, err := store.confirmIdentityOperationV2(ctx, request, config)
			if err != nil || !reflect.DeepEqual(receipt.Receipt, result.Receipt) || !reflect.DeepEqual(receipt.Session, domain.WebSessionV2{}) || fixture.base.writes != writes {
				t.Errorf("current same-user receipt read changed effects/cookie: result=%+v err=%v writes=%d", receipt, err, fixture.base.writes)
			}
		})
	}
}

func TestIdentityOperationStorageV2AuditFailureRollsBackEveryTable(t *testing.T) {
	fixture, store, ctx, request, config, _ := identityOperationConfirmationFixtureV2(t, domain.IdentityOperationLinkV2)
	before, beforeOperation := fixture.base.durable.clone(), fixture.operation
	fixture.base.failStatement = identityOperationAuditSQLTestV2
	fixture.base.failure = errors.New("controlled audit persistence failure")
	result, err := store.confirmIdentityOperationV2(ctx, request, config)
	if !errors.Is(err, fixture.base.failure) || !reflect.DeepEqual(result, identityConfirmationResultV2{}) || !reflect.DeepEqual(fixture.base.durable, before) || fixture.operation != beforeOperation || len(fixture.audit) != 0 || fixture.base.commits != 0 || fixture.base.rollbacks != 1 {
		t.Fatalf("audit failure left partial identity/cookie/result: err=%v result=%+v commits=%d rollbacks=%d", err, result, fixture.base.commits, fixture.base.rollbacks)
	}
}

func TestIdentityOperationStorageV2OriginalScopeAndCurrentnessDenials(t *testing.T) {
	for _, name := range []string{"csrf", "browser", "manifest", "stale_revision", "disabled", "expired", "wrong_client", "foreign_target", "membership_revoked"} {
		t.Run(name, func(t *testing.T) {
			fixture, store, ctx, request, config, operation := identityOperationConfirmationFixtureV2(t, domain.IdentityOperationLinkV2)
			switch name {
			case "csrf":
				request.CSRFDigest = domain.DigestSecret("different-csrf")
			case "browser":
				request.BrowserBindingDigest = domain.DigestSecret("different-browser")
			case "manifest":
				config.ManifestDigest = domain.DigestSecret("different-manifest")
			case "stale_revision":
				request.ExpectedRevision++
			case "disabled":
				config.SensitiveOperationsEnabled = false
			case "expired":
				fixture.at = operation.ExpiresAt
			case "wrong_client":
				config.Providers[0].ClientID = "different-client"
			case "foreign_target":
				target := identityRowsV2Identity(domain.IdentityProviderTelegram, 7)
				target.Subject, target.UserID = *operation.Pending.TargetSubject, "foreign-owner"
				fixture.base.durable.forward[identityRowsV2SubjectKey(target.Subject)] = identityRowsV2JSON(t, target)
			case "membership_revoked":
				for key, raw := range fixture.base.durable.members {
					var membership domain.TenantMembership
					if err := json.Unmarshal([]byte(raw), &membership); err != nil {
						t.Fatal(err)
					}
					membership.Status = domain.TenantMembershipRevoked
					fixture.base.durable.members[key] = identityRowsV2JSON(t, membership)
				}
			}
			before, beforeOperation := fixture.base.durable.clone(), fixture.operation
			result, err := store.confirmIdentityOperationV2(ctx, request, config)
			if err == nil || !reflect.DeepEqual(result, identityConfirmationResultV2{}) || !reflect.DeepEqual(fixture.base.durable, before) || fixture.operation != beforeOperation || fixture.base.commits != 0 || fixture.base.writes != 0 {
				t.Errorf("%s did not deny without effects: err=%v result=%+v commits=%d writes=%d", name, err, result, fixture.base.commits, fixture.base.writes)
			}
		})
	}
}

func TestIdentityOperationStorageV2TombstoneRevisionAndOverflow(t *testing.T) {
	for _, revision := range []domain.AuthRevisionV2{41, math.MaxUint64} {
		t.Run(revision.String(), func(t *testing.T) {
			fixture, store, ctx, request, config, operation := identityOperationConfirmationFixtureV2(t, domain.IdentityOperationLinkV2)
			target := identityRowsV2Identity(domain.IdentityProviderTelegram, revision)
			target.Subject = *operation.Pending.TargetSubject
			target.State = domain.ExternalIdentityUnlinkedV2
			fixture.base.durable.forward[identityRowsV2SubjectKey(target.Subject)] = identityRowsV2JSON(t, target)
			before, beforeOperation := fixture.base.durable.clone(), fixture.operation
			result, err := store.confirmIdentityOperationV2(ctx, request, config)
			if revision == math.MaxUint64 {
				if !errors.Is(err, domain.ErrAuthRevisionOverflowV2) || !reflect.DeepEqual(result, identityConfirmationResultV2{}) || !reflect.DeepEqual(fixture.base.durable, before) || fixture.operation != beforeOperation || fixture.base.writes != 0 {
					t.Errorf("tombstone overflow wrote/reset authority: result=%+v err=%v writes=%d", result, err, fixture.base.writes)
				}
				return
			}
			if err != nil || result.Receipt.AfterRevision == nil || *result.Receipt.AfterRevision != revision+1 {
				t.Fatalf("relink did not exceed both active and tombstone revisions: result=%+v err=%v", result, err)
			}
			got, err := domain.DecodeExternalIdentityV2([]byte(fixture.base.durable.forward[identityRowsV2SubjectKey(target.Subject)]))
			if err != nil || got.UserID != target.UserID || !got.CreatedAt.Equal(target.CreatedAt) || got.State != domain.ExternalIdentityActiveV2 || got.Revision != revision+1 {
				t.Errorf("explicit relink remapped tombstone: got=%+v err=%v", got, err)
			}
		})
	}
}

func TestIdentityOperationStorageV2CommitReplyLossNeedsCurrentReceiptReader(t *testing.T) {
	fixture, store, ctx, request, config, operation := identityOperationConfirmationFixtureV2(t, domain.IdentityOperationLinkV2)
	fixture.commitReplyError = errors.New("controlled lost commit reply after durable effect")
	result, err := store.confirmIdentityOperationV2(ctx, request, config)
	if err == nil || !reflect.DeepEqual(result, identityConfirmationResultV2{}) || fixture.base.commits != 1 || len(fixture.audit) != 1 {
		t.Fatalf("ambiguous commit invented success or lost committed evidence: result=%+v err=%v commits=%d audit=%d", result, err, fixture.base.commits, len(fixture.audit))
	}
	terminal, err := domain.DecodeIdentityOperationV2([]byte(fixture.operation))
	if err != nil || terminal.Receipt == nil {
		t.Fatal("actual committed receipt unavailable in controlled snapshot", err)
	}
	writes := fixture.base.writes
	if result, err := store.confirmIdentityOperationV2(ctx, request, config); err == nil || !reflect.DeepEqual(result, identityConfirmationResultV2{}) || fixture.base.writes != writes {
		t.Error("old cookie replayed an ambiguously committed operation")
	}
	// Test-only setup models a later successful login through the surviving
	// canonical method. It is not proof of the pending BFF/provider login path.
	login, err := domain.DecodeWebSessionV2([]byte(fixture.base.durable.sessions[string(request.NextSessionDigest)]))
	if err != nil {
		t.Fatal(err)
	}
	login.Session.SessionDigest, login.Session.CSRFTokenDigest = domain.DigestSecret("relogin-session"), domain.DigestSecret("relogin-csrf")
	fixture.base.durable.sessions[string(login.Session.SessionDigest)] = identityRowsV2JSON(t, login)
	request.SessionDigest, request.CSRFDigest = login.Session.SessionDigest, login.Session.CSRFTokenDigest
	config.SensitiveOperationsEnabled = false
	read, err := store.confirmIdentityOperationV2(ctx, request, config)
	if err != nil || !reflect.DeepEqual(read.Receipt, *terminal.Receipt) || !reflect.DeepEqual(read.Session, domain.WebSessionV2{}) || fixture.base.writes != writes || read.Receipt.BeforeRevision != operation.Pending.Scope.ExpectedRevision {
		t.Errorf("current relogin did not read exact receipt without replay/cookie: result=%+v err=%v writes=%d", read, err, fixture.base.writes)
	}
}

func TestIdentityOperationStorageV2ExpiredReceiptIsNotNonCommit(t *testing.T) {
	fixture, store, ctx, request, config, _ := identityOperationConfirmationFixtureV2(t, domain.IdentityOperationLinkV2)
	result, err := store.confirmIdentityOperationV2(ctx, request, config)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := domain.DecodeIdentityOperationV2([]byte(fixture.operation))
	if err != nil {
		t.Fatal(err)
	}
	// Use a still-current fixture cookie beyond receipt retention. Membership
	// and identity times stay database-derived; logical expiry, not TTL, denies.
	fixture.at = terminal.DeleteAfter
	login := result.Session
	login.Session.SessionDigest, login.Session.CSRFTokenDigest = domain.DigestSecret("expired-receipt-relogin"), domain.DigestSecret("expired-receipt-csrf")
	login.Session.IssuedAt = fixture.at
	login.Session.LastSeenAt = fixture.at
	login.Session.IdleExpiresAt = fixture.at.Add(time.Hour)
	login.Session.AbsoluteExpiresAt = fixture.at.Add(2 * time.Hour)
	fixture.base.durable.sessions[string(login.Session.SessionDigest)] = identityRowsV2JSON(t, login)
	request.SessionDigest, request.CSRFDigest = login.Session.SessionDigest, login.Session.CSRFTokenDigest
	writes, beforeOperation := fixture.base.writes, fixture.operation
	if denied, err := store.confirmIdentityOperationV2(ctx, request, config); err == nil || !reflect.DeepEqual(denied, identityConfirmationResultV2{}) || fixture.operation != beforeOperation || fixture.base.writes != writes || len(fixture.audit) != 1 {
		t.Errorf("expired retained receipt authorized replay or implied noncommit: result=%+v err=%v", denied, err)
	}
}
