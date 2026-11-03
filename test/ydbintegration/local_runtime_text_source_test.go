//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// This observer preserves the original producer request, including independently
// owned private material, and forwards it unchanged. It supplies no permission.
type localTextNativeCommitObserver struct {
	*ydbstore.Store
	t       *testing.T
	request ports.CanonicalUserEventCommit
}

func (s *localTextNativeCommitObserver) CommitCanonicalUserEvent(ctx context.Context, r ports.CanonicalUserEventCommit) (ports.CanonicalUserEventResult, error) {
	s.t.Helper()
	if s.request.PreparedMaterial != nil || r.PreparedMaterial == nil {
		s.t.Fatal("expected exactly one actual ingress material producer")
	}
	clone, err := r.PreparedMaterial.Clone()
	if err != nil {
		s.t.Fatal(err)
	}
	s.t.Cleanup(clone.Release)
	s.request = r
	s.request.PreparedMaterial = clone
	return s.Store.CommitCanonicalUserEvent(ctx, r)
}

// Actual SQL absence, not a scripted NULL projection. This runs only inside the
// existing disposable remote gate. No model peer, worker, listener or grant is
// created: the endpoint is original intake DATA and is never contacted.
func TestLocalRuntimeYDBLocalTextExactServiceAbsenceBeforeIdempotency(t *testing.T) {
	dsn, enabled, err := localRuntimeYDBConnection(os.Getenv)
	if !enabled {
		t.Skip("explicit disposable remote local-runtime gate not requested")
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	client, err := ydbclient.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closing, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if err := client.Close(closing); err != nil {
			t.Error(err)
		}
	})
	base, err := ydbstore.New(client.DB, ydbstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	establishIntegrationCutovers(t, dsn, client)
	for _, require := range []func(context.Context) error{base.RequireExecutionPlacementCutover, base.RequireHarnessBindingCutover, base.RequireManagedExecutionAuthorityV2Cutover, base.RequireAdmissionCostV2Cutover} {
		if err := require(ctx); err != nil {
			t.Fatal("original serving predecessor", err)
		}
	}
	clock := onboardingDatabaseClock{t: t, ctx: ctx, db: client.DB}
	suffix := uniqueID("text-source")
	tenant, owner := domain.TenantID("tenant-"+suffix), domain.UserID("owner-"+suffix)
	auth := &localRuntimeAuthStore{Store: base}
	// Independent bounded cleanup is armed before the first owned write.
	t.Cleanup(func() { localRuntimeCleanup(t, client.DB, auth, []domain.TenantID{tenant}) })
	hash := sha256.Sum256([]byte(owner))
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: fmt.Sprintf("%d", binary.BigEndian.Uint64(hash[:8])|1)}
	auth.identities = append(auth.identities, localRuntimeIdentity{subject: subject, user: owner})
	if resolved, fresh, err := base.ResolveOrCreateExternalIdentity(ctx, subject, owner, clock.Now()); err != nil || !fresh || resolved.UserID != owner {
		t.Fatal("fresh actual identity", fresh, err)
	}
	if _, err := base.BootstrapDevelopmentMembership(ctx, domain.DevelopmentBootstrapGrant{TenantID: tenant, UserID: owner, Role: domain.TenantMembershipOwner, Environment: domain.DevelopmentEnvironment, Operator: "local-runtime-ci", Reason: "bounded local text current-source SQL proof", GrantedAt: clock.Now()}); err != nil {
		t.Fatal(err)
	}
	root, exe, receiptPath := newLocalRuntimeOperatorFiles(t, ctx, tenant, owner, "http://127.0.0.1:35421/v1", clock.Now(), domain.AgentLoopOwnedLocalTextV1)
	if err := ydbstore.InstallLocalProfile(ctx, client.DB, root, exe, receiptPath); err != nil {
		t.Fatal("actual text installation", err)
	}
	receipt, err := localoperator.ReadProfileReceipt(ctx, receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	source, err := localoperator.OpenExisting(ctx, root, exe, receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := ydbstore.New(client.DB, ydbstore.Options{OwnedProfilePublicationVerifier: source, AttemptSpendingPublicationVerifier: source, OwnedAttemptEffectSourceVerifier: source})
	if err != nil {
		t.Fatal(err)
	}
	binder, err := ydbstore.NewLocalHarnessBinder(store, source, receipt)
	if err != nil {
		t.Fatal(err)
	}
	blobs := &localRuntimeBlobs{values: make(map[string][]byte), deleted: make(map[string]bool), absent: make(map[string]bool)}
	sessions, err := sessionapi.New(sessionapi.Config{IDKey: bytes.Repeat([]byte{71}, 32), CursorKey: bytes.Repeat([]byte{72}, 32)}, store, blobs, clock)
	if err != nil {
		t.Fatal(err)
	}
	session, fresh, err := sessions.Create(ctx, tenant, owner, "text-source-session")
	if err != nil || !fresh {
		t.Fatal("actual canonical session", fresh, err)
	}
	binding, err := sessions.BindFrontend(ctx, tenant, owner, domain.FrontendWeb, string(session.ID), session.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	observer := &localTextNativeCommitObserver{Store: store, t: t}
	ingress, err := sessioningress.New(sessioningress.Config{IDKey: bytes.Repeat([]byte{73}, 32), HarnessBinder: binder}, observer, blobs)
	if err != nil {
		t.Fatal(err)
	}
	result, err := ingress.IngestBound(ctx, sessioningress.BoundUserInput{Actor: sessioningress.Actor{TenantID: tenant, UserID: owner, Frontend: domain.FrontendWeb, ExternalConversationID: string(session.ID)}, Binding: binding, ExternalEventID: "text-source-message", ReceivedAt: clock.Now(), Text: "one bounded text message"})
	if err != nil || !result.Created {
		t.Fatal("actual new two-head canonical commit / native 24 NULL service facts", result.Created, err)
	}
	r := observer.request
	original, err := domain.DecodeAttemptExecutionSourceV1([]byte(r.OwnedExecutionSourceRecord))
	if err != nil || original.Profile.Locator.Profile != domain.AgentLoopOwnedLocalTextV1 || len(original.Spending) != 2 || original.Spending[0].Locator.ComponentKind != domain.AttemptSpendingProviderV1 || original.Spending[1].Locator.ComponentKind != domain.AttemptSpendingSubstrateV1 {
		t.Fatal("actual original two-head package", err)
	}
	localRuntimeCount(t, ctx, client.DB, "attempt_spending_authorization_current_v1", tenant, 2)
	before := localTextNativeSnapshot(t, ctx, client.DB, tenant)
	assertUnchanged := func(t *testing.T) {
		t.Helper()
		if after := localTextNativeSnapshot(t, ctx, client.DB, tenant); !reflect.DeepEqual(before, after) {
			t.Fatal("denied or duplicate commit changed original funding/canonical postimage")
		}
		for _, table := range []string{"attempt_effect_reservations", "attempt_operation_calls_v1", "attempt_operations_v1", "attempt_operation_proxies_v1"} {
			localRuntimeCount(t, ctx, client.DB, table, tenant, 0)
		}
	}
	duplicate := func(t *testing.T) {
		t.Helper()
		got, err := store.CommitCanonicalUserEvent(ctx, r)
		expectedDuplicate := result
		expectedDuplicate.Created = false
		if err != nil || got.Created || !reflect.DeepEqual(got, expectedDuplicate) {
			t.Fatal("restored original public commit", err)
		}
		assertUnchanged(t)
	}
	duplicate(t)
	for _, kind := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
		for _, mode := range []string{"active", "revoked", "malformed", "pk_only"} {
			t.Run(string(kind)+"/"+mode, func(t *testing.T) {
				data := original.Spending[0].Clone()
				data.Locator.ComponentKind = kind
				data.ResourceGeneration = 1
				data.MutationID = "adversarial-presence-" + string(kind)
				if mode == "revoked" {
					data.State = domain.OwnedAgentCurrentRevokedV1
					data.RevocationGeneration = 2
				}
				body, e := data.SourceCanonicalBytes()
				if e != nil {
					t.Fatal(e)
				}
				data.SourceAuthorization = ports.OwnedProxyBodyReferenceV1("adversarial-row-data", body)
				raw, e := domain.EncodeAttemptSpendingAuthorizationCurrentV1(data)
				decoded, de := domain.DecodeAttemptSpendingAuthorizationCurrentV1(raw)
				if e != nil || de != nil || decoded.Validate() != nil || decoded.Locator.TenantID != tenant || decoded.Locator.AttemptID != r.AttemptID || decoded.Locator.BillingOwnerUserID != owner || decoded.Locator.ComponentKind != kind {
					t.Fatal("coherent same-original-PK adversarial DATA baseline", e, de)
				}
				digest, e := data.Digest()
				if e != nil {
					t.Fatal(e)
				}
				if mode == "malformed" {
					raw = []byte("not-json")
					sum := sha256.Sum256(raw)
					digest = hex.EncodeToString(sum[:])
				}
				key := []any{tenant, r.AttemptID, kind, owner}
				t.Cleanup(func() {
					cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
					defer done()
					if _, err := client.DB.ExecContext(cleanup, "DELETE FROM attempt_spending_authorization_current_v1 WHERE tenant_id=$1 AND attempt_id=$2 AND component_kind=$3 AND billing_owner_user_id=$4", key...); err != nil {
						t.Error("owned adversarial key cleanup", err)
					}
				})
				if mode == "pk_only" {
					_, e = client.DB.ExecContext(ctx, "UPSERT INTO attempt_spending_authorization_current_v1 (tenant_id,attempt_id,component_kind,billing_owner_user_id) VALUES ($1,$2,$3,$4)", key...)
				} else {
					args := append(key, string(raw), data.Version, data.RevocationGeneration, data.State, digest, data.RunID, data.SessionID, data.OwnerUserID, data.Window.PublishedAt, data.Window.ExpiresAt, data.UpdatedAt)
					_, e = client.DB.ExecContext(ctx, "UPSERT INTO attempt_spending_authorization_current_v1 (tenant_id,attempt_id,component_kind,billing_owner_user_id,record,version,revocation_generation,state,digest,run_id,session_id,owner_user_id,published_at,expires_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)", args...)
				}
				if e != nil {
					t.Fatal("write adversarial presence DATA", e)
				}
				// Call Store directly: the higher ingress duplicate shortcut must
				// not turn this into an untested current-authority assertion.
				if denied, e := store.CommitCanonicalUserEvent(ctx, r); !errors.Is(e, domain.ErrMembershipDenied) || !reflect.DeepEqual(denied, ports.CanonicalUserEventResult{}) {
					t.Fatalf("forbidden exact-key service presence must deny authority with zero result: err=%v", e)
				}
				assertUnchanged(t)
				if _, e := client.DB.ExecContext(ctx, "DELETE FROM attempt_spending_authorization_current_v1 WHERE tenant_id=$1 AND attempt_id=$2 AND component_kind=$3 AND billing_owner_user_id=$4", key...); e != nil {
					t.Fatal("restore only owned adversarial key", e)
				}
				localRuntimeCount(t, ctx, client.DB, "attempt_spending_authorization_current_v1", tenant, 2)
				duplicate(t)
			})
		}
	}
}

// Bounded exact-tenant postimages: raw canonical records plus publication/update
// scalars cover unchanged source windows, funding and committed event/run/outbox.
// Adversarial service keys are deliberately excluded, not normalized into grants.
func localTextNativeSnapshot(t *testing.T, ctx context.Context, db *sql.DB, tenant domain.TenantID) map[string][]string {
	t.Helper()
	out := make(map[string][]string)
	for _, q := range []struct{ name, sql string }{
		{"profile", "SELECT record,digest,published_at,expires_at,updated_at FROM owned_agent_profile_current_v1 WHERE tenant_id=$1 LIMIT 3"},
		{"funding", "SELECT record,digest,published_at,expires_at,updated_at FROM attempt_spending_authorization_current_v1 WHERE tenant_id=$1 AND (component_kind='provider' OR component_kind='substrate') LIMIT 3"},
		{"run", "SELECT CAST(payload AS String) FROM runs WHERE tenant_id=$1 LIMIT 3"},
		{"events", "SELECT CAST(record AS String) FROM session_events WHERE tenant_id=$1 LIMIT 4"},
		{"outbox", "SELECT CAST(payload AS String) FROM dispatch_outbox WHERE tenant_id=$1 LIMIT 3"},
	} {
		rows, err := db.QueryContext(ctx, q.sql, tenant)
		if err != nil {
			t.Fatal(q.name, err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				rows.Close()
				t.Fatal(q.name, err)
			}
			raw, err := json.Marshal(values)
			if err != nil || len(raw) > 128<<10 {
				rows.Close()
				t.Fatal("bounded native postimage", q.name, err)
			}
			out[q.name] = append(out[q.name], string(raw))
		}
		err = rows.Err()
		closeErr := rows.Close()
		if err != nil || closeErr != nil {
			t.Fatal(q.name, err, closeErr)
		}
		sort.Strings(out[q.name])
	}
	if len(out["profile"]) != 1 || len(out["funding"]) != 2 || len(out["run"]) != 1 || len(out["events"]) < 1 || len(out["outbox"]) != 1 {
		t.Fatal("unexpected bounded original native postimage cardinality")
	}
	return out
}
