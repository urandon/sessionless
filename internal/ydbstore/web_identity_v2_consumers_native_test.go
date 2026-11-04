package ydbstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/runexplanationrate"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

// Actual disposable-YDB reader transactions, not provider proof or a serving V2
// installer. Each provider scenario owns one canonical identity in two tenants.
// Missing admission/resource observations remain explicitly unknown, never
// manufactured successful admission, eligible compute, or SEND authority.
func TestWebIdentityV2YDBDirectConsumersCurrentThenStale(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(provider), func(t *testing.T) {
			db, ctx, identity, cookie, member := newWebIdentityV2NativeFixture(t)
			// The existing fixture gates before DSN/client access, seeds nothing,
			// and owns its ORIGINAL Yandex/session/member cleanup keys only.
			identity.Subject.Provider = provider
			identity.Revision = 1<<53 + 1
			cookie.Session.AuthenticatedSubject = identity.Subject
			cookie.IdentityRevision = identity.Revision
			store, err := New(db, Options{})
			if err != nil {
				t.Fatal(err)
			}
			store.webIdentityV2 = &identityConfigurationV2{ManifestDigest: domain.DigestSecret("native-consumer-manifest"), Providers: []domain.IdentityProviderBindingV2{
				{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "native-consumer-yandex", CallbackURI: "https://sessionless.example/auth/login/callback"},
				{Provider: domain.IdentityProviderTelegram, Issuer: "https://oauth.telegram.org", ClientID: "native-consumer-telegram", CallbackURI: "https://sessionless.example/auth/telegram/callback"},
			}}
			nonce := rand.Text()
			cutover := computeChoiceCutoverRecord{Version: 1, ManifestDigest: string(domain.DigestSecret(nonce + "-manifest")), WriterRevision: 1, SchemaRevision: 1, Enabled: true, WriterCommit: string(domain.DigestSecret(nonce + "-writer"))[:40], DrainedInventoryDigest: string(domain.DigestSecret(nonce + "-drained")), CompletedAt: identity.CreatedAt}
			pins := computeChoiceCutoverPins{manifestDigest: cutover.ManifestDigest, writerCommit: cutover.WriterCommit}
			codec, err := computechoice.NewCursorCodec([]byte("0123456789abcdef0123456789abcdef")) // synthetic test DATA, not an operational key
			if err != nil {
				t.Fatal(err)
			}
			scopes := make([]nativeConsumerScopeV2, 2)
			for i := range scopes {
				name := fmt.Sprintf("tenant-%d", i+1)
				m, c := member, cookie
				if i == 1 {
					m.TenantID = domain.TenantID("native-reader-tenant-" + nonce)
					c.Session.ActiveTenantID = m.TenantID
					c.Session.SessionDigest = domain.DigestSecret(nonce + "-second-cookie")
					c.Session.CSRFTokenDigest = domain.DigestSecret(nonce + "-second-csrf")
				}
				canonical := domain.Session{ID: domain.SessionID(name + "-session-" + nonce), TenantID: m.TenantID, CreatedBy: identity.UserID, Status: domain.SessionActive, CreatedAt: identity.CreatedAt, UpdatedAt: identity.CreatedAt}
				owner := domain.SessionParticipant{TenantID: m.TenantID, SessionID: canonical.ID, UserID: identity.UserID, Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: identity.CreatedAt, UpdatedAt: identity.CreatedAt}
				run := domain.Run{ID: domain.RunID(name + "-run-" + nonce), TenantID: m.TenantID, SessionID: canonical.ID, TriggerEventID: domain.SessionEventID(name + "-trigger-" + nonce), SubscriptionConnectionID: domain.SubscriptionConnectionID(name + "-subscription-" + nonce), Status: domain.RunCreated, IdempotencyKey: domain.IdempotencyKey(name + "-idempotency-" + nonce), CreatedAt: identity.CreatedAt, UpdatedAt: identity.CreatedAt}
				pin := computeChoiceAttachedPin{tenantID: m.TenantID, ownerUserID: identity.UserID, subscriptionID: run.SubscriptionConnectionID, actorID: domain.ActorID(name + "-actor-" + nonce), provider: "codex", workerID: domain.AttachedWorkerID(name + "-worker-" + nonce), connectionID: domain.AttachedWorkerConnectionID(name + "-connection-" + nonce), enrollmentGeneration: 1, connectionGeneration: 1, workerRevision: 1, connectionRevision: 1, capabilityDigest: domain.AttachedWorkerCapabilityDigest(domain.DigestSecret(name + nonce + "-capability"))}
				registration := computechoice.Registration{TenantID: m.TenantID, OwnerUserID: identity.UserID, ChoiceID: "native-choice", RegistrationRevision: 1, DisclosureRevision: string(domain.DigestSecret(name + nonce + "-disclosure"))}
				catalog, err := newComputeChoiceAttachedCatalog(pins, codec, []computeChoiceAttachedRegistration{{enumeration: registration, pin: pin}})
				if err != nil || canonical.Validate() != nil || owner.Validate() != nil || run.Validate() != nil {
					t.Fatalf("valid pre-write canonical scope %s: %v", name, err)
				}
				scopes[i] = nativeConsumerScopeV2{name: name, cookie: c, member: m, session: canonical, owner: owner, run: run, catalog: catalog, registration: registration, pin: pin}
			}
			// Every extra key, writer-derived row and rate cell is covered BEFORE
			// the first write. Original fixture cleanup is not retargeted to TG.
			armNativeConsumerCleanupV2(t, db, identity, scopes, cutover)
			installNativeConsumerCutoverV2(t, ctx, db, cutover)
			setup := webIdentityV2NativeTx(t, ctx, db)
			if err := writeExternalIdentityV2(ctx, setup, identity); err != nil {
				t.Fatal("write real canonical forward/reverse identity", err)
			}
			for _, s := range scopes {
				if err := putMembershipTx(ctx, setup, s.member); err != nil {
					t.Fatal("write current membership", s.name, err)
				}
				if err := writeWebSessionV2(ctx, setup, s.cookie); err != nil {
					t.Fatal("write exact V2 cookie", s.name, err)
				}
			}
			if err := setup.Commit(); err != nil {
				t.Fatal("commit real canonical identity/member/cookie rows", err)
			}
			for _, s := range scopes {
				if err := store.CreateSession(ctx, s.session, s.owner); err != nil {
					t.Fatal("actual Session/owner/genesis/activity writer", s.name, err)
				}
				if err := store.Transact(ctx, s.member.TenantID, func(state ports.StateTx) error { return state.PutRun(ctx, s.run) }); err != nil {
					t.Fatal("actual canonical Run/index writer", s.name, err)
				}
			}
			// Current positives must leave cookies/members/resources untouched;
			// only the two separate finite rate owners may debit.
			resources := snapshotNativeConsumerResourcesV2(t, ctx, db, scopes)
			current := true
			for _, s := range scopes {
				for _, method := range []string{"direct", "rated", "list"} {
					current = t.Run(s.name+"/current/"+method, func(t *testing.T) {
						readNativeConsumerV2(t, ctx, store, s, method, nonce+"-current", false)
					}) && current
				}
			}
			if !current {
				t.Fatal("all six current positives are required BEFORE revision advance")
			}
			if got := snapshotNativeConsumerResourcesV2(t, ctx, db, scopes); !reflect.DeepEqual(resources, got) {
				t.Fatal("current resource reads mutated cookie/activity/member/canonical resources")
			}
			rates := snapshotNativeConsumerRatesV2(t, ctx, db, scopes)
			change := webIdentityV2NativeTx(t, ctx, db)
			var at time.Time
			if err := change.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
				t.Fatal("sample same identity-change transaction clock", err)
			}
			next := identity
			next.Revision++
			next.UpdatedAt = at.UTC()
			previous, found, err := readExternalIdentityV2(ctx, change, identity.Subject)
			if err != nil || !found || !reflect.DeepEqual(previous, identity) {
				t.Fatalf("exact original forward row before mutation: found=%t err=%v", found, err)
			}
			encoded, err := domain.EncodeExternalIdentityV2(next)
			if err != nil {
				t.Fatal(err)
			}
			bucket, err := webBucket(identity.Subject.String())
			if err != nil {
				t.Fatal(err)
			}
			// Test-owned canonical identity DATA change only. The reverse row,
			// owner, subject, original creation time and every cookie stay fixed.
			if _, err := change.ExecContext(ctx, `UPDATE external_identities SET updated_at=$1,record=CAST($2 AS JsonDocument) WHERE shard_bucket=$3 AND provider=$4 AND subject=$5 AND user_id=$6`, next.UpdatedAt, string(encoded), bucket, identity.Subject.Provider, identity.Subject.Subject, identity.UserID); err != nil {
				t.Fatal("advance ONLY original forward identity revision", err)
			}
			actual, found, err := readExternalIdentityV2(ctx, change, identity.Subject)
			if err != nil || !found || !reflect.DeepEqual(actual, next) {
				t.Fatalf("exact changed forward row readback: found=%t err=%v", found, err)
			}
			if err := change.Commit(); err != nil {
				t.Fatal("commit forward revision fence", err)
			}
			for _, s := range scopes {
				for _, method := range []string{"direct", "rated", "list"} {
					t.Run(s.name+"/stale/"+method, func(t *testing.T) {
						readNativeConsumerV2(t, ctx, store, s, method, nonce+"-stale", true)
					})
				}
			}
			postDenial := webIdentityV2NativeTx(t, ctx, db)
			unchanged, found, err := readExternalIdentityV2(ctx, postDenial, identity.Subject)
			if err != nil || !found || !reflect.DeepEqual(unchanged, next) {
				t.Fatalf("stale reads changed canonical forward identity: found=%t err=%v", found, err)
			}
			if err := postDenial.Commit(); err != nil {
				t.Fatal("commit post-denial identity read", err)
			}
			if got := snapshotNativeConsumerRatesV2(t, ctx, db, scopes); !reflect.DeepEqual(rates, got) {
				t.Error("stale direct consumer debited/changed exact owned rate rows")
			}
			if got := snapshotNativeConsumerResourcesV2(t, ctx, db, scopes); !reflect.DeepEqual(resources, got) {
				t.Error("revision fence or stale read mutated cookies/members/activity/canonical resources")
			}
		})
	}
}

type nativeConsumerScopeV2 struct {
	name         string
	cookie       domain.WebSessionV2
	member       domain.TenantMembership
	session      domain.Session
	owner        domain.SessionParticipant
	run          domain.Run
	catalog      *computeChoiceAttachedCatalog
	registration computechoice.Registration
	pin          computeChoiceAttachedPin
}

func readNativeConsumerV2(t *testing.T, ctx context.Context, store *Store, s nativeConsumerScopeV2, method, requestID string, stale bool) {
	t.Helper()
	switch method {
	case "direct":
		auth := ports.RunExplanationAuthorizationV1{TenantID: s.member.TenantID, UserID: s.member.UserID, MembershipSecurityVersion: s.member.SecurityVersion, WebSessionDigest: s.cookie.Session.SessionDigest}
		value, err := store.ReadRunExplanationV1(ctx, auth, s.run.ID)
		if stale {
			if !errors.Is(err, domain.ErrWebSessionRevoked) || !reflect.DeepEqual(value, webcontract.RunExplanationV1{}) {
				t.Fatalf("stale direct result=%+v err=%v", value, err)
			}
			return
		}
		if err != nil || value.Validate() != nil || value.RunID != s.run.ID || value.SessionID != s.session.ID || value.Status != s.run.Status || !value.CreatedAt.Equal(s.run.CreatedAt) || !value.UpdatedAt.Equal(s.run.UpdatedAt) || value.Admission.Availability != runexplanation.Unknown {
			t.Fatalf("valid direct requested Run must succeed with honest unknown admission: value=%+v err=%v", value, err)
		}
	case "rated":
		value, err := store.ReadRatedRunExplanationV1(ctx, s.cookie.Session.SessionDigest, requestID, s.run.ID)
		if stale {
			if !errors.Is(err, domain.ErrWebSessionRevoked) || !reflect.DeepEqual(value, ports.RunExplanationReadOutcomeV1{}) {
				t.Fatalf("stale rated result=%+v err=%v", value, err)
			}
			return
		}
		if err != nil || value.Validate() != nil || value.Kind != ports.RunExplanationReadSuccessV1 || value.Explanation == nil || value.Explanation.RunID != s.run.ID || value.Explanation.SessionID != s.session.ID || value.Explanation.Status != s.run.Status || !value.Explanation.CreatedAt.Equal(s.run.CreatedAt) || !value.Explanation.UpdatedAt.Equal(s.run.UpdatedAt) || value.Explanation.Admission.Availability != runexplanation.Unknown {
			t.Fatalf("valid rated read must return requested success, not NotFound/rateLimited: value=%+v err=%v", value, err)
		}
	case "list":
		value, err := store.readRatedComputeChoiceAttachedPage(ctx, s.cookie.Session.SessionDigest, s.session.ID, computechoice.ListQuery{Limit: 1, InputKind: computechoice.InputText}, requestID, s.catalog)
		if stale {
			if !errors.Is(err, domain.ErrWebSessionRevoked) || !reflect.DeepEqual(value, computeChoiceAttachedPage{}) {
				t.Fatalf("stale private LIST leaked result or wrong denial: err=%v candidates=%d", err, len(value.candidates))
			}
			return
		}
		if err != nil || value.rateLimited || value.readAt.IsZero() || value.registryRevision == "" || value.throughSequence != s.session.LastEventSequence || value.more || value.lastPosition != 1 || len(value.candidates) != 1 {
			t.Fatalf("valid private LIST lacks one scoped registration: err=%v candidates=%d limited=%t", err, len(value.candidates), value.rateLimited)
		}
		candidate := value.candidates[0]
		if candidate.registration != s.registration || candidate.sources.state != computeChoiceAttachedMissing || candidate.sources.subscription != nil || candidate.sources.worker != nil || candidate.sources.connection != nil || candidate.sources.slot != nil || candidate.sources.counters != nil || !reflect.DeepEqual(candidate.provider, computeChoiceProviderSnapshot{}) {
			t.Fatal("missing resource DATA became eligible/observed or foreign registration")
		}
	default:
		t.Fatal("closed direct native reader", method)
	}
}

type nativeConsumerRateSetV2 struct {
	table, identity string
	keys            [runexplanationrate.ProbeCount]uint32
}
type nativeConsumerRateRowV2 struct {
	record  string
	expires time.Time
}

func nativeConsumerRateSetsV2(t *testing.T, scopes []nativeConsumerScopeV2) []nativeConsumerRateSetV2 {
	t.Helper()
	var result []nativeConsumerRateSetV2
	for _, s := range scopes {
		identity, keys, err := runexplanationrate.IdentityKeys(s.member.TenantID, s.member.UserID)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, nativeConsumerRateSetV2{"run_explanation_rate_slots_v1", identity, keys})
		identity, keys, err = computechoice.ListRateIdentityKeys(s.member.TenantID, s.member.UserID)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, nativeConsumerRateSetV2{"compute_choice_rate_slots_v1", identity, keys})
	}
	return result
}

func snapshotNativeConsumerRatesV2(t *testing.T, ctx context.Context, db *sql.DB, scopes []nativeConsumerScopeV2) map[string]map[uint32]nativeConsumerRateRowV2 {
	t.Helper()
	result := make(map[string]map[uint32]nativeConsumerRateRowV2)
	for _, set := range nativeConsumerRateSetsV2(t, scopes) {
		rows := make(map[uint32]nativeConsumerRateRowV2)
		for _, key := range set.keys {
			var raw string
			var expires time.Time
			err := db.QueryRowContext(ctx, "SELECT SUBSTRING(CAST(record AS String),0,8193), expire_at FROM "+set.table+" WHERE slot_id=$1", key).Scan(&raw, &expires)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				t.Fatalf("snapshot exact rate cell table=%s slot=%d: %v", set.table, key, err)
			}
			row, err := runexplanationrate.DecodeSlot([]byte(raw))
			if err != nil {
				t.Fatalf("snapshot corrupt rate cell table=%s slot=%d: %v", set.table, key, err)
			}
			if row.IdentityDigest != set.identity {
				continue
			}
			if !expires.Equal(row.ExpiresAt) {
				t.Fatal("owned rate record/scalar expiry mismatch", set.table, key)
			}
			rows[key] = nativeConsumerRateRowV2{raw, expires}
		}
		if len(rows) != 1 {
			t.Fatalf("positive native call must leave exactly one owned debit row table=%s found=%d", set.table, len(rows))
		}
		result[set.table+":"+set.identity] = rows
	}
	return result
}

type nativeConsumerRowV2 struct {
	found  bool
	values []any
}

func snapshotNativeConsumerResourcesV2(t *testing.T, ctx context.Context, db *sql.DB, scopes []nativeConsumerScopeV2) map[string]nativeConsumerRowV2 {
	t.Helper()
	result := make(map[string]nativeConsumerRowV2)
	for _, s := range scopes {
		cookieBucket, err := webBucket(string(s.cookie.Session.SessionDigest))
		if err != nil {
			t.Fatal(err)
		}
		userBucket, err := webBucket(string(s.member.UserID))
		if err != nil {
			t.Fatal(err)
		}
		activityBucket, err := webBucket(string(s.session.ID))
		if err != nil {
			t.Fatal(err)
		}
		points := []struct {
			name, sql string
			columns   int
			args      []any
			absent    bool
		}{
			{"cutover", computeChoiceCutoverSQL, 10, []any{"serving"}, false},
			{"cookie", `SELECT CAST(record AS String),csrf_token_digest,user_id,active_tenant_id,identity_provider,external_subject,membership_security_version,issued_at,last_seen_at,idle_expires_at,absolute_expires_at,revoked_at FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, 12, []any{cookieBucket, s.cookie.Session.SessionDigest}, false},
			{"member", computeChoiceMembershipSQL, 4, []any{userBucket, s.member.UserID, s.member.TenantID}, false},
			{"reverse-identity", `SELECT created_at FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 AND provider=$3 AND subject=$4`, 1, []any{userBucket, s.member.UserID, s.cookie.Session.AuthenticatedSubject.Provider, s.cookie.Session.AuthenticatedSubject.Subject}, false},
			{"session", computeChoiceSessionSQL, 5, []any{s.member.TenantID, s.session.ID}, false},
			{"participant", computeChoiceParticipantSQL, 3, []any{s.member.TenantID, s.session.ID, s.member.UserID}, false},
			{"run", `SELECT CAST(payload AS String),status,session_id,trigger_event_id,subscription_connection_id,created_at,updated_at FROM runs WHERE tenant_id=$1 AND run_id=$2`, 7, []any{s.member.TenantID, s.run.ID}, false},
			{"run-index", `SELECT trigger_event_id,status FROM runs_by_session WHERE tenant_id=$1 AND session_id=$2 AND created_at=$3 AND run_id=$4`, 2, []any{s.member.TenantID, s.session.ID, s.run.CreatedAt, s.run.ID}, false},
			{"idempotency", `SELECT run_id,created_at,expire_at FROM run_idempotency WHERE tenant_id=$1 AND idempotency_key=$2`, 3, []any{s.member.TenantID, s.run.IdempotencyKey}, false},
			{"material", `SELECT version,through_sequence,last_event_id,prefix_digest,record FROM session_material_current_v1 WHERE tenant_id=$1 AND session_id=$2`, 5, []any{s.member.TenantID, s.session.ID}, false},
			{"activity", `SELECT session_id FROM session_activity WHERE tenant_id=$1 AND user_id=$2 AND status=$3 AND activity_bucket=$4 AND updated_at=$5 AND session_id=$6`, 1, []any{s.member.TenantID, s.member.UserID, s.session.Status, activityBucket, s.session.UpdatedAt, s.session.ID}, false},
			{"head", `SELECT CAST(record AS String) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, 1, []any{s.member.TenantID, s.run.ID}, true},
			{"subscription", computeChoiceAttachedSubscriptionSQL, 6, []any{s.member.TenantID, s.pin.subscriptionID}, true},
			{"actor", computeChoiceAttachedActorSQL, 1, []any{s.member.TenantID, s.pin.actorID}, true},
			{"worker", `SELECT CAST(record AS String) FROM attached_workers WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`, 1, []any{s.member.TenantID, s.member.UserID, s.pin.workerID}, true},
			{"connection", `SELECT CAST(record AS String) FROM attached_worker_connections WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`, 1, []any{s.member.TenantID, s.member.UserID, s.pin.workerID}, true},
			{"slot", `SELECT subscription_connection_id FROM subscription_scheduler_slots WHERE tenant_id=$1 AND subscription_connection_id=$2`, 1, []any{s.member.TenantID, s.pin.subscriptionID}, true},
			{"counters", `SELECT tenant_id FROM tenant_scheduler_counters WHERE tenant_id=$1`, 1, []any{s.member.TenantID}, true},
		}
		for _, point := range points {
			values, destinations := make([]any, point.columns), make([]any, point.columns)
			for i := range values {
				destinations[i] = &values[i]
			}
			err := db.QueryRowContext(ctx, point.sql, point.args...).Scan(destinations...)
			if errors.Is(err, sql.ErrNoRows) && point.absent {
				result[s.name+"/"+point.name] = nativeConsumerRowV2{}
				continue
			}
			if err != nil || point.absent {
				t.Fatalf("snapshot canonical resource %s/%s absent=%t err=%v", s.name, point.name, point.absent, err)
			}
			for i, value := range values {
				if raw, ok := value.([]byte); ok {
					values[i] = bytes.Clone(raw)
				}
			}
			result[s.name+"/"+point.name] = nativeConsumerRowV2{true, values}
		}
	}
	return result
}

func installNativeConsumerCutoverV2(t *testing.T, ctx context.Context, db *sql.DB, record computeChoiceCutoverRecord) {
	t.Helper()
	tx := webIdentityV2NativeTx(t, ctx, db)
	var occupied string
	if err := tx.QueryRowContext(ctx, `SELECT cutover_id FROM compute_choice_cutover_v1 WHERE cutover_id=$1`, "serving").Scan(&occupied); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("native fixture requires absent serving singleton; never overwrite an occupant: err=%v", err)
	}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO compute_choice_cutover_v1 (cutover_id,version,manifest_digest,writer_revision,schema_revision,enabled,writer_commit,drained_inventory_digest,old_writer_count,completed_at,record) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CAST($11 AS JsonDocument))`, "serving", record.Version, record.ManifestDigest, record.WriterRevision, record.SchemaRevision, record.Enabled, record.WriterCommit, record.DrainedInventoryDigest, record.OldWriterCount, record.CompletedAt, string(raw)); err != nil {
		t.Fatal("insert only exact synthetic native cutover DATA", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal("commit native cutover DATA", err)
	}
}

func armNativeConsumerCleanupV2(t *testing.T, db *sql.DB, identity domain.ExternalIdentityV2, scopes []nativeConsumerScopeV2, cutover computeChoiceCutoverRecord) {
	t.Helper()
	rateSets := nativeConsumerRateSetsV2(t, scopes)
	t.Cleanup(func() {
		// Shared-cell cleanup has its own bounded transaction. A failure there
		// must not prevent independent cleanup of unique canonical row keys.
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			// Check and delete finite shared cells atomically, like the existing
			// rated native fixture. Never erase a different live/expired occupant.
			tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
			if err != nil {
				t.Error("begin exact native reader cleanup", err)
				return
			}
			defer tx.Rollback()
			for _, set := range rateSets {
				for _, key := range set.keys {
					var raw string
					err := tx.QueryRowContext(ctx, "SELECT CAST(record AS String) FROM "+set.table+" WHERE slot_id=$1", key).Scan(&raw)
					if errors.Is(err, sql.ErrNoRows) {
						continue
					}
					if err != nil {
						t.Error("read owned native rate cleanup", set.table, key, err)
						return
					}
					var owner struct {
						Identity string `json:"identity_digest"`
					}
					if json.Unmarshal([]byte(raw), &owner) != nil || owner.Identity != set.identity {
						continue
					}
					if _, err := tx.ExecContext(ctx, "DELETE FROM "+set.table+" WHERE slot_id=$1", key); err != nil {
						t.Error("delete exact owned native rate row", err)
						return
					}
				}
			}
			var raw string
			var scalar computeChoiceCutoverRecord
			err = tx.QueryRowContext(ctx, computeChoiceCutoverSQL, "serving").Scan(&raw, &scalar.Version, &scalar.ManifestDigest, &scalar.WriterRevision, &scalar.SchemaRevision, &scalar.Enabled, &scalar.WriterCommit, &scalar.DrainedInventoryDigest, &scalar.OldWriterCount, &scalar.CompletedAt)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				t.Error("read exact cutover cleanup ownership", err)
				return
			}
			if err == nil {
				var decoded computeChoiceCutoverRecord
				d := json.NewDecoder(bytes.NewBufferString(raw))
				d.DisallowUnknownFields()
				decodeErr := d.Decode(&decoded)
				decoded.CompletedAt = decoded.CompletedAt.UTC()
				scalar.CompletedAt = scalar.CompletedAt.UTC()
				var extra any
				if decodeErr == nil && d.Decode(&extra) == io.EOF && reflect.DeepEqual(decoded, cutover) && reflect.DeepEqual(scalar, cutover) {
					if _, err := tx.ExecContext(ctx, `DELETE FROM compute_choice_cutover_v1 WHERE cutover_id=$1`, "serving"); err != nil {
						t.Error("delete exactly owned cutover record", err)
						return
					}
				}
			}
			if err := tx.Commit(); err != nil {
				t.Error("commit owned rate/singleton cleanup", err)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		// Unique test-owned exact keys cover TG (base fixture owns only its
		// original Yandex key), both cookies/members and all real writer rows.
		identityBucket, err := webBucket(identity.Subject.String())
		if err != nil {
			t.Error(err)
			return
		}
		userBucket, err := webBucket(string(identity.UserID))
		if err != nil {
			t.Error(err)
			return
		}
		for _, deletion := range []struct {
			sql  string
			args []any
		}{
			{`DELETE FROM external_identities WHERE shard_bucket=$1 AND provider=$2 AND subject=$3`, []any{identityBucket, identity.Subject.Provider, identity.Subject.Subject}},
			{`DELETE FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 AND provider=$3 AND subject=$4`, []any{userBucket, identity.UserID, identity.Subject.Provider, identity.Subject.Subject}},
		} {
			if _, err := db.ExecContext(ctx, deletion.sql, deletion.args...); err != nil {
				t.Error("cleanup exact canonical native subject", err)
			}
		}
		for _, s := range scopes {
			cookieBucket, err := webBucket(string(s.cookie.Session.SessionDigest))
			if err != nil {
				t.Error(err)
				continue
			}
			activityBucket, err := webBucket(string(s.session.ID))
			if err != nil {
				t.Error(err)
				continue
			}
			for _, deletion := range []struct {
				sql  string
				args []any
			}{
				{`DELETE FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, []any{cookieBucket, s.cookie.Session.SessionDigest}},
				{`DELETE FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`, []any{userBucket, s.member.UserID, s.member.TenantID}},
				{`DELETE FROM session_activity WHERE tenant_id=$1 AND user_id=$2 AND status=$3 AND activity_bucket=$4 AND updated_at=$5 AND session_id=$6`, []any{s.member.TenantID, s.member.UserID, s.session.Status, activityBucket, s.session.UpdatedAt, s.session.ID}},
				{`DELETE FROM session_material_current_v1 WHERE tenant_id=$1 AND session_id=$2`, []any{s.member.TenantID, s.session.ID}},
				{`DELETE FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`, []any{s.member.TenantID, s.session.ID, s.member.UserID}},
				{`DELETE FROM sessions WHERE tenant_id=$1 AND session_id=$2`, []any{s.member.TenantID, s.session.ID}},
				{`DELETE FROM runs_by_session WHERE tenant_id=$1 AND session_id=$2 AND created_at=$3 AND run_id=$4`, []any{s.member.TenantID, s.session.ID, s.run.CreatedAt, s.run.ID}},
				{`DELETE FROM run_idempotency WHERE tenant_id=$1 AND idempotency_key=$2`, []any{s.member.TenantID, s.run.IdempotencyKey}},
				{`DELETE FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, []any{s.member.TenantID, s.run.ID}},
				{`DELETE FROM runs WHERE tenant_id=$1 AND run_id=$2`, []any{s.member.TenantID, s.run.ID}},
			} {
				if _, err := db.ExecContext(ctx, deletion.sql, deletion.args...); err != nil {
					t.Error("cleanup exact native consumer resource", s.name, err)
				}
			}
		}
	})
}
