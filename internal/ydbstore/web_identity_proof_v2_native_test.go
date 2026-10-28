package ydbstore

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Genuine existing-table SQL/JsonDocument/transaction integration, NOT genuine
// provider verification: accepted claims below are explicitly synthetic input
// at the future trusted-adapter boundary. No OAuth endpoint is contacted and
// no serving V2 mode is installed. The original fixture enforces disposable CI.
func TestWebIdentityV2YDBProofStorageAndAtomicLink(t *testing.T) {
	db, ctx, identity, session, member := newWebIdentityV2NativeFixture(t)
	store, err := New(db, Options{})
	if err != nil {
		t.Fatal(err)
	}
	key := string(identity.UserID)
	browser := "native-proof-browser-" + key
	phase := identityPhaseRequestV2{OperationDigest: domain.DigestSecret(key + "-operation"), SessionDigest: session.Session.SessionDigest, CSRFDigest: session.Session.CSRFTokenDigest, BrowserBindingDigest: domain.DigestSecret(browser), ExpectedRevision: identity.Revision}
	start := identityOperationStartV2{Phase: phase, Kind: domain.IdentityOperationLinkV2, TargetProvider: domain.IdentityProviderTelegram, SourceChallengeDigest: domain.DigestSecret(key + "-source-child"), TargetChallengeDigest: domain.DigestSecret(key + "-target-child")}
	target := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "native-target-" + key}
	confirmation := identityConfirmationV2{OperationDigest: phase.OperationDigest, SessionDigest: phase.SessionDigest, CSRFDigest: phase.CSRFDigest, BrowserBindingDigest: phase.BrowserBindingDigest, ExpectedRevision: phase.ExpectedRevision, NextSessionDigest: domain.DigestSecret(key + "-next-cookie"), NextCSRFDigest: domain.DigestSecret(key + "-next-csrf")}
	config := identityConfigurationV2{ManifestDigest: domain.DigestSecret(key + "-manifest"), SensitiveOperationsEnabled: true, Providers: []domain.IdentityProviderBindingV2{
		{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "native-synthetic-yandex", CallbackURI: "https://sessionless.example/auth/login/callback"},
		{Provider: domain.IdentityProviderTelegram, Issuer: "https://oauth.telegram.org", ClientID: "native-synthetic-telegram", CallbackURI: "https://sessionless.example/auth/telegram/callback"},
	}}
	// Arm exact freshly generated-key cleanup before any partial write. The
	// original fixture separately owns its initial identity/session/membership.
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for _, digest := range []domain.SecretDigest{phase.OperationDigest, start.SourceChallengeDigest, start.TargetChallengeDigest} {
			bucket, err := webBucket(string(digest))
			if err != nil {
				t.Error(err)
				continue
			}
			if _, err := db.ExecContext(cleanup, `DELETE FROM oidc_login_challenges WHERE shard_bucket=$1 AND state_digest=$2`, bucket, digest); err != nil {
				t.Errorf("cleanup exact native proof key: %v", err)
			}
		}
		targetBucket, targetErr := webBucket(target.String())
		userBucket, userErr := webBucket(string(identity.UserID))
		sessionBucket, sessionErr := webBucket(string(confirmation.NextSessionDigest))
		if targetErr != nil || userErr != nil || sessionErr != nil {
			t.Error("cleanup native exact-key bucket calculation failed")
			return
		}
		for _, deletion := range []struct {
			statement string
			args      []any
		}{
			{`DELETE FROM external_identities WHERE shard_bucket=$1 AND provider=$2 AND subject=$3`, []any{targetBucket, target.Provider, target.Subject}},
			{`DELETE FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 AND provider=$3 AND subject=$4`, []any{userBucket, identity.UserID, target.Provider, target.Subject}},
			{`DELETE FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, []any{sessionBucket, confirmation.NextSessionDigest}},
			{`DELETE FROM audit_events WHERE tenant_id=$1`, []any{member.TenantID}},
		} {
			if _, err := db.ExecContext(cleanup, deletion.statement, deletion.args...); err != nil {
				t.Errorf("cleanup exact disposable proof-owned scope: %v", err)
			}
		}
	})
	setup := webIdentityV2NativeTx(t, ctx, db)
	if err := writeExternalIdentityV2(ctx, setup, identity); err != nil {
		t.Fatal("setup canonical identity", err)
	}
	if err := putMembershipTx(ctx, setup, member); err != nil {
		t.Fatal("setup current membership", err)
	}
	if err := writeWebSessionV2(ctx, setup, session); err != nil {
		t.Fatal("setup current browser session", err)
	}
	if err := setup.Commit(); err != nil {
		t.Fatal(err)
	}
	operation, err := store.startIdentityOperationV2(ctx, start, config)
	if err != nil {
		t.Fatal("actual native start operation", err)
	}
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		digest, subject := start.SourceChallengeDigest, identity.Subject
		nonce := ""
		if provider == domain.IdentityProviderTelegram {
			digest, subject, nonce = start.TargetChallengeDigest, target, "native-synthetic-nonce"
		}
		child, err := store.createIdentityProofV2(ctx, identityProofChildV2{Phase: phase, ChallengeDigest: digest, PKCEVerifier: strings.Repeat("p", 43), Nonce: nonce}, config)
		if err != nil {
			t.Fatal("actual native create child", provider, err)
		}
		exchange, err := store.consumeIdentityProofV2(ctx, identityProofConsumeV2{Phase: phase, ChallengeDigest: digest, BrowserSecret: browser}, config)
		if err != nil || exchange.StateDigest != digest || exchange.PKCEVerifier == "" {
			t.Fatalf("actual native consume did not return confirmed one-use exchange: provider=%s err=%v", provider, err)
		}
		verified := acceptedIdentityProofV2{Subject: subject, Kind: domain.IdentityProofYandexAuthorizationV2, Binding: child.Binding}
		if provider == domain.IdentityProviderTelegram {
			verified.Kind = domain.IdentityProofTelegramAuthTimeV2
			verified.SignedAuthTime = &child.CreatedAt // synthetic verified-adapter input, not an ID token
		}
		operation, err = store.recordIdentityProofV2(ctx, identityProofCompletionV2{Phase: phase, ChallengeDigest: digest, Verified: verified}, config)
		if err != nil {
			t.Fatal("actual native record accepted synthetic claim", provider, err)
		}
	}
	if operation.Phase != domain.IdentityPhaseConfirmV2 {
		t.Fatal("native dual proof storage did not reach exact confirm phase")
	}
	result, err := store.confirmIdentityOperationV2(ctx, confirmation, config)
	if err != nil || result.Receipt.AfterRevision == nil || *result.Receipt.AfterRevision != identity.Revision+1 || result.Session.Session.UserID != identity.UserID {
		t.Fatalf("actual native atomic identity/cookie/audit/receipt result: err=%v", err)
	}
	if err := store.identityAuthorityTxV2(ctx, func(ctx context.Context, query *ownedAuthorityQuery) error {
		old, _, err := authorizeWebSessionV2(ctx, query, phase.SessionDigest, domain.TenantPermissionRead, query.at)
		if !errors.Is(err, domain.ErrWebSessionRevoked) || !reflect.DeepEqual(old, domain.WebSessionV2{}) {
			return errors.New("old cookie survived native atomic link")
		}
		current, currentMember, err := authorizeWebSessionV2(ctx, query, confirmation.NextSessionDigest, domain.TenantPermissionRead, query.at)
		if err != nil || !reflect.DeepEqual(current, result.Session) || !reflect.DeepEqual(currentMember, member) {
			return errors.New("rotated native cookie failed current canonical join")
		}
		active, err := readActiveExternalIdentitiesV2(ctx, query, identity.UserID)
		if err != nil || len(active) != 2 {
			return errors.New("native final canonical/reverse set is not exactly two methods")
		}
		sourceFound, targetFound := false, false
		for _, method := range active {
			if method.UserID != identity.UserID || method.State != domain.ExternalIdentityActiveV2 || method.Revision != result.Session.IdentityRevision {
				return errors.New("native final methods disagree on immutable owner/common revision")
			}
			sourceFound = sourceFound || method.Subject == identity.Subject
			targetFound = targetFound || method.Subject == target
		}
		if !sourceFound || !targetFound {
			return errors.New("native final set does not contain exact source and target")
		}
		terminal, found, err := readIdentityOperationV2(ctx, query, phase.OperationDigest)
		if err != nil || !found || terminal.Pending != nil || !reflect.DeepEqual(terminal.Receipt, &result.Receipt) {
			return errors.New("native exact minimal receipt mismatch")
		}
		for _, digest := range []domain.SecretDigest{start.SourceChallengeDigest, start.TargetChallengeDigest} {
			consumed, found, err := readIdentityProofChildV2(ctx, query, digest)
			if err != nil || !found || consumed.ConsumedAt == nil || consumed.Secrets != nil {
				return errors.New("native committed child JSON/scalar secret pruning failed")
			}
		}
		var raw, action, actor, subjectKind, subjectID string
		if err := query.scanMetadata(ctx, `SELECT SUBSTRING(CAST(metadata AS String),0,8193),
 SUBSTRING(CAST(action AS String),0,65), SUBSTRING(CAST(actor_id AS String),0,161),
 SUBSTRING(CAST(subject_kind AS String),0,65), SUBSTRING(CAST(subject_id AS String),0,65)
 FROM audit_events WHERE tenant_id=$1 AND occurred_at=$2 AND audit_event_id=$3`,
			[]any{member.TenantID, result.Receipt.CompletedAt, "audit-identity-" + string(phase.OperationDigest)}, &raw, &action, &actor, &subjectKind, &subjectID); err != nil {
			return err
		}
		var audit map[string]json.RawMessage
		if len(raw) > domain.IdentityAuthMaxRecordBytesV2 || json.Unmarshal([]byte(raw), &audit) != nil || len(audit) != 4 || action != "web.identity.linked" || actor != string(identity.UserID) || subjectKind != "identity_operation" || subjectID != string(phase.OperationDigest) {
			return errors.New("native exact content-free audit identity/shape mismatch")
		}
		for key, expected := range map[string]string{"provider": string(target.Provider), "before_revision": identity.Revision.String(), "after_revision": result.Session.IdentityRevision.String(), "operation_digest": string(phase.OperationDigest)} {
			var value string
			if json.Unmarshal(audit[key], &value) != nil || value != expected {
				return errors.New("native audit lost exact string revisions or operation correlation")
			}
		}
		return nil
	}); err != nil {
		t.Fatal("native committed readback", err)
	}
}
