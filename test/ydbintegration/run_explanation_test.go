//go:build ydbintegration

package ydbintegration

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type explanationYDBFixture struct {
	store     *ydbstore.Store
	client    *ydbclient.Client
	ctx       context.Context
	now       time.Time
	auth      ports.RunExplanationAuthorizationV1
	web       domain.WebSession
	ingress   ydbstore.TelegramIngress
	admission ports.DispatchAdmissionRequest
}

func explanationTransactionDSNValid(dsn string) bool {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return false
	}
	query := parsed.Query()
	_, fake := query["go_fake_tx"]
	return query.Get("go_query_mode") == "data" && !fake
}

func requireExplanationTrueTransactions(t *testing.T) {
	t.Helper()
	if !explanationTransactionDSNValid(requireConnectionString(t)) {
		if os.Getenv("SESSIONLESS_RUN_EXPLANATION_YDB_GATE") == "1" {
			t.Fatal("Run explanation serialized proof requires explicit data mode and no fake transaction option")
		}
		t.Skip("Run explanation serialized proof runs in the dedicated true-data transaction gate, not the legacy scripting suite")
	}
}

func TestRunExplanationYDBTrueTransactionDSNGuard(t *testing.T) {
	for _, test := range []struct {
		dsn  string
		want bool
	}{
		{dsn: "grpc://localhost:2136/local?go_query_mode=data&go_query_bind=declare,numeric", want: true},
		{dsn: "grpc://localhost:2136/local?go_query_mode=scripting&go_fake_tx=scripting", want: false},
		{dsn: "grpc://localhost:2136/local?go_query_mode=data&go_fake_tx=", want: false},
		{dsn: "grpc://localhost:2136/local?go_fake_tx=data", want: false},
	} {
		if got := explanationTransactionDSNValid(test.dsn); got != test.want {
			t.Errorf("DSN true transaction guard=%t want=%t for %q", got, test.want, test.dsn)
		}
	}
}

func newExplanationYDBFixture(t *testing.T) explanationYDBFixture {
	t.Helper()
	requireExplanationTrueTransactions(t)
	store, client := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	var now time.Time
	if err := client.DB.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	now = now.UTC().Truncate(time.Microsecond)
	// uniqueID alone is process-local: a random test nonce prevents repeated
	// independent test processes from colliding in the same disposable DB.
	nonce := rand.Text()
	tenant := domain.TenantID(uniqueID("explanation-tenant-" + nonce))
	user := domain.UserID(uniqueID("explanation-user-" + nonce))
	ingress := ingressFixture(tenant, uniqueID("explanation-run-"+nonce), 1, now.Add(-time.Minute))
	web := webSessionFixture(uniqueID("explanation-web-"+nonce), user, tenant, uniqueID("explanation-subject-"+nonce), 1, now.Add(-time.Minute))
	// Register immediately, before fixture writes. Every cleanup selector is
	// this fixture's unique tenant or exact session lookup; no global reset.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, table := range []string{"run_explanation_heads_v1", "attempts", "runs", "runs_by_session", "run_idempotency", "sessions", "session_participants", "session_displays", "dispatch_outbox", "dispatch_ready", "dispatch_ready_v2", "artifact_manifests", "artifact_manifests_by_run", "subscription_connections", "subscription_scheduler_slots", "tenant_scheduler_counters", "quota_reservations", "quota_expiry", "quota_expiry_v2", "worker_jobs", "leases", "lease_heads", "lease_expiry", "lease_expiry_v2", "run_finalizations", "session_events", "session_event_idempotency", "session_deletions", "tenant_memberships", "tenants", "audit_events"} {
			if _, err := client.DB.ExecContext(cleanupCtx, "DELETE FROM "+table+" WHERE tenant_id=$1", tenant); err != nil {
				t.Errorf("cleanup %s unique tenant: %v", table, err)
			}
		}
		bucket, err := ydbpartition.BucketV1(string(web.SessionDigest))
		if err != nil {
			t.Error(err)
			return
		}
		if _, err := client.DB.ExecContext(cleanupCtx, `DELETE FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, bucket, web.SessionDigest); err != nil {
			t.Errorf("cleanup exact Web session: %v", err)
		}
	})
	seedCanonicalMembership(t, client.DB, tenant, user, now.Add(-time.Minute))
	session, owner := canonicalSessionFixture(tenant, user, ingress.Run.SessionID, now.Add(-time.Minute))
	if err := store.CreateSession(ctx, session, owner); err != nil {
		t.Fatal(err)
	}
	if err := store.Transact(ctx, tenant, func(state ports.StateTx) error {
		if err := state.PutRun(ctx, ingress.Run); err != nil {
			return err
		}
		if err := state.PutAttempt(ctx, ingress.Attempt); err != nil {
			return err
		}
		if err := state.PutArtifactManifest(ctx, ingress.InputManifest); err != nil {
			return err
		}
		return state.PutDispatchOutbox(ctx, ingress.Dispatch)
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateWebSession(ctx, web); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(ctx, `UPSERT INTO subscription_connections (tenant_id,subscription_connection_id,actor_id,provider,credential_ref,entitlement_state,quota_state,observed_at,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8,$8)`, tenant, ingress.Run.SubscriptionConnectionID, user, "deterministic", "", domain.EntitlementActive, domain.ProviderQuotaUnknown, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	limits := domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 4, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 64, MaxArtifacts: 4, MaxToolEvents: 16, MaxToolEventBytes: 1 << 20}
	admission := admissionFixture(ingress, domain.QuotaReservationID(uniqueID("explanation-reservation")), now.Add(-30*time.Second), limits)
	admission.HoldUntil = now.Add(24 * time.Hour)
	admission.Workload = domain.WorkloadShape{Runtime: time.Minute, Turns: 1}
	return explanationYDBFixture{store: store, client: client, ctx: ctx, now: now, auth: ports.RunExplanationAuthorizationV1{TenantID: tenant, UserID: user, MembershipSecurityVersion: 1, WebSessionDigest: web.SessionDigest}, web: web, ingress: ingress, admission: admission}
}

func readExplanationYDB(t *testing.T, f explanationYDBFixture) webcontract.RunExplanationV1 {
	t.Helper()
	value, err := f.store.ReadRunExplanationV1(f.ctx, f.auth, f.ingress.Run.ID)
	if err != nil {
		t.Fatalf("participant explanation read: %v", err)
	}
	return value
}

func TestRunExplanationYDBSelectedReadPlansArePointBounded(t *testing.T) {
	requireExplanationTrueTransactions(t)
	_, client := openStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, test := range []struct {
		table string
		query string
		args  []any
	}{
		{table: "web_sessions", query: `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, args: []any{uint32(0), domain.DigestSecret("explanation-plan")}},
		{table: "tenant_memberships", query: `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`, args: []any{uint32(0), "plan-user", "plan-tenant"}},
		{table: "runs", query: `SELECT SUBSTRING(CAST(payload AS String),0,8193) FROM runs WHERE tenant_id=$1 AND run_id=$2`, args: []any{"plan-tenant", "plan-run"}},
		{table: "sessions", query: `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM sessions WHERE tenant_id=$1 AND session_id=$2`, args: []any{"plan-tenant", "plan-session"}},
		{table: "session_participants", query: `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`, args: []any{"plan-tenant", "plan-session", "plan-user"}},
		{table: "run_explanation_heads_v1", query: `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, args: []any{"plan-tenant", "plan-run"}},
		{table: "attempts", query: `SELECT SUBSTRING(CAST(payload AS String),0,8193) FROM attempts WHERE tenant_id=$1 AND attempt_id=$2`, args: []any{"plan-tenant", "plan-attempt"}},
		{table: "attached_worker_attempt_heads", query: `SELECT SUBSTRING(CAST(payload AS String),0,8193) FROM attached_worker_attempt_heads WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`, args: []any{"plan-tenant", "plan-owner", "plan-worker"}},
		{table: "leases", query: `SELECT SUBSTRING(CAST(payload AS String),0,8193) FROM leases WHERE tenant_id=$1 AND lease_id=$2`, args: []any{"plan-tenant", "plan-lease"}},
		{table: "attached_workers", query: `SELECT enrollment_generation,connection_generation,desired_state,observed_state,revision,created_at,updated_at FROM attached_workers WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`, args: []any{"plan-tenant", "plan-owner", "plan-worker"}},
		{table: "attached_worker_connections", query: `SELECT connection_id,enrollment_generation,connection_generation,revision,state,connected_at,last_checkpoint_at,manifest_observed_at FROM attached_worker_connections WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`, args: []any{"plan-tenant", "plan-owner", "plan-worker"}},
	} {
		t.Run(test.table, func(t *testing.T) {
			var ast, plan string
			if err := client.DB.QueryRowContext(ydb.WithQueryMode(ctx, ydb.ExplainQueryMode), test.query, test.args...).Scan(&ast, &plan); err != nil {
				t.Fatalf("explain exact bounded SQL for %s: %v", test.table, err)
			}
			if err := validateBoundedQueryPlan(plan, queryPlanContract{operator: "TablePointLookup", table: test.table}); err != nil {
				t.Fatalf("%s point lookup contract: %v", test.table, err)
			}
		})
	}
}

func TestRunExplanationYDBReasonSupersessionReplayReadmissionAndInvalidation(t *testing.T) {
	f := newExplanationYDBFixture(t)
	request := f.admission
	request.Workload.Runtime = 2 * time.Minute
	if result, err := f.store.AdmitDispatch(f.ctx, request); err != nil || result.Code != "runtime_limit_exceeded" {
		t.Fatalf("runtime denial=%+v error=%v", result, err)
	}
	first := readExplanationYDB(t, f)
	if first.Admission.ReasonCode != runexplanation.RuntimeLimitExceeded {
		t.Fatalf("runtime reason=%+v", first.Admission)
	}
	// Same Run phase, scheduler state and nil retry time, different reason.
	request.Workload.Runtime = time.Minute
	request.Workload.Turns = 5
	if result, err := f.store.AdmitDispatch(f.ctx, request); err != nil || result.Code != "turn_limit_exceeded" {
		t.Fatalf("same-state changed denial=%+v error=%v", result, err)
	}
	second := readExplanationYDB(t, f)
	if second.Admission.ReasonCode != runexplanation.TurnLimitExceeded || second.Admission.DecisionRevision <= first.Admission.DecisionRevision {
		t.Fatalf("reason supersession first=%+v second=%+v", first.Admission, second.Admission)
	}
	if _, err := f.store.AdmitDispatch(f.ctx, request); err != nil {
		t.Fatal(err)
	}
	if replay := readExplanationYDB(t, f); replay.Admission.DecisionRevision != second.Admission.DecisionRevision {
		t.Fatalf("identical replay invented observation: %d -> %d", second.Admission.DecisionRevision, replay.Admission.DecisionRevision)
	}
	request.Workload.Turns = 1
	request.Now = f.now.Add(-20 * time.Second)
	if result, err := f.store.AdmitDispatch(f.ctx, request); err != nil || !result.Admitted {
		t.Fatalf("readmission=%+v error=%v", result, err)
	}
	if admitted := readExplanationYDB(t, f); admitted.Admission.ReasonCode != runexplanation.ReasonAdmitted {
		t.Fatalf("stale denial after readmission=%+v", admitted.Admission)
	}
	// A lifecycle phase change is not a new denial/admission evaluation.
	if err := f.store.Transact(f.ctx, f.auth.TenantID, func(state ports.StateTx) error {
		run, _, err := state.GetRun(f.ctx, f.ingress.Run.ID)
		if err != nil {
			return err
		}
		if err := run.Transition(domain.RunQuotaBlocked, f.now.Add(-10*time.Second)); err != nil {
			return err
		}
		return state.PutRun(f.ctx, run)
	}); err != nil {
		t.Fatal(err)
	}
	if value := readExplanationYDB(t, f); value.Admission.Availability != runexplanation.Unknown {
		t.Fatalf("lifecycle write invented admission=%+v", value.Admission)
	}
}

func TestRunExplanationYDBCanonicalAndProjectionRollbackTogether(t *testing.T) {
	f := newExplanationYDBFixture(t)
	request := f.admission
	request.Workload.Runtime = 2 * time.Minute
	if result, err := f.store.AdmitDispatch(f.ctx, request); err != nil || result.Code != "runtime_limit_exceeded" {
		t.Fatalf("runtime denial=%+v error=%v", result, err)
	}
	before := readExplanationYDB(t, f)
	if before.Admission.ReasonCode != runexplanation.RuntimeLimitExceeded {
		t.Fatalf("rollback fixture has no committed denial: %+v", before.Admission)
	}
	type persistedSnapshot struct {
		run      string
		head     string
		revision uint64
	}
	readPersisted := func() persistedSnapshot {
		t.Helper()
		var snapshot persistedSnapshot
		if err := f.client.DB.QueryRowContext(f.ctx, `SELECT CAST(payload AS String) FROM runs WHERE tenant_id=$1 AND run_id=$2`, f.auth.TenantID, f.ingress.Run.ID).Scan(&snapshot.run); err != nil {
			t.Fatal(err)
		}
		if err := f.client.DB.QueryRowContext(f.ctx, `SELECT revision,CAST(record AS String) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, f.auth.TenantID, f.ingress.Run.ID).Scan(&snapshot.revision, &snapshot.head); err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	persistedBefore := readPersisted()
	abort := errors.New("deterministic explanation rollback")
	err := f.store.Transact(f.ctx, f.auth.TenantID, func(state ports.StateTx) error {
		run, found, err := state.GetRun(f.ctx, f.ingress.Run.ID)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("rollback fixture Run missing")
		}
		if err := run.Transition(domain.RunAdmitted, f.now.Add(-10*time.Second)); err != nil {
			return err
		}
		// PutRun writes the canonical phase and invalidates its now-inapplicable
		// denial in this same transaction. Neither may survive the abort.
		if err := state.PutRun(f.ctx, run); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatalf("transaction abort=%v want deterministic sentinel", err)
	}
	if persistedAfter := readPersisted(); persistedAfter != persistedBefore {
		t.Fatal("aborted transaction changed canonical Run or projection record/revision")
	}
	if after := readExplanationYDB(t, f); !reflect.DeepEqual(after.Admission, before.Admission) || after.Status != before.Status {
		t.Fatalf("aborted transaction changed public phase/reason/revision: before=%+v after=%+v", before, after)
	}
}

func TestRunExplanationYDBReadOnlyParticipantDoesNotRefreshWebActivity(t *testing.T) {
	f := newExplanationYDBFixture(t)
	membership := domain.TenantMembership{TenantID: f.auth.TenantID, UserID: f.auth.UserID, Role: domain.TenantMembershipViewer, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: f.web.IssuedAt, UpdatedAt: f.web.IssuedAt}
	encoded, err := json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	userBucket, err := ydbpartition.BucketV1(string(f.auth.UserID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.DB.ExecContext(f.ctx, `UPDATE tenant_memberships SET role=$1,record=CAST($2 AS JsonDocument) WHERE user_bucket=$3 AND user_id=$4 AND tenant_id=$5`, membership.Role, string(encoded), userBucket, f.auth.UserID, f.auth.TenantID); err != nil {
		t.Fatal(err)
	}
	participant := domain.SessionParticipant{TenantID: f.auth.TenantID, SessionID: f.ingress.Run.SessionID, UserID: f.auth.UserID, Role: domain.SessionParticipantViewer, Status: domain.SessionParticipantActive, CreatedAt: f.web.IssuedAt, UpdatedAt: f.web.IssuedAt}
	encoded, err = json.Marshal(participant)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.DB.ExecContext(f.ctx, `UPDATE session_participants SET role=$1,record=CAST($2 AS JsonDocument) WHERE tenant_id=$3 AND session_id=$4 AND user_id=$5`, participant.Role, string(encoded), participant.TenantID, participant.SessionID, participant.UserID); err != nil {
		t.Fatal(err)
	}
	value := readExplanationYDB(t, f)
	if value.Attempt.Availability != runexplanation.Recorded {
		t.Fatalf("read-only participant selected Attempt=%+v", value.Attempt)
	}
	bucket, err := ydbpartition.BucketV1(string(f.web.SessionDigest))
	if err != nil {
		t.Fatal(err)
	}
	var record string
	if err := f.client.DB.QueryRowContext(f.ctx, `SELECT record FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`, bucket, f.web.SessionDigest).Scan(&record); err != nil {
		t.Fatal(err)
	}
	var after domain.WebSession
	if err := json.Unmarshal([]byte(record), &after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, f.web) {
		t.Fatalf("readonly resource mutated Web activity: last_seen got=%s want=%s idle got=%s want=%s", after.LastSeenAt, f.web.LastSeenAt, after.IdleExpiresAt, f.web.IdleExpiresAt)
	}
}

func TestRunExplanationYDBSecurityVersionRecheckAndOpaqueTargets(t *testing.T) {
	for _, name := range []string{"READ preserving security version", "membership removed", "Web session revoked", "Web session expired", "tenant owner nonparticipant", "foreign Run", "absent Run", "historical locator absent", "selected Attempt mismatch"} {
		t.Run(name, func(t *testing.T) {
			f := newExplanationYDBFixture(t)
			if _, err := f.store.AuthorizeWebSession(f.ctx, f.web.SessionDigest, domain.TenantPermissionRead, f.now); err != nil {
				t.Fatalf("earlier BFF authorization: %v", err)
			}
			want := error(nil)
			switch name {
			case "READ preserving security version":
				membership := domain.TenantMembership{TenantID: f.auth.TenantID, UserID: f.auth.UserID, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 2, CreatedAt: f.now.Add(-time.Minute), UpdatedAt: f.now}
				encoded, err := json.Marshal(membership)
				if err != nil {
					t.Fatal(err)
				}
				bucket, err := ydbpartition.BucketV1(string(f.auth.UserID))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.client.DB.ExecContext(f.ctx, `UPDATE tenant_memberships SET security_version=$1,record=CAST($2 AS JsonDocument) WHERE user_bucket=$3 AND user_id=$4 AND tenant_id=$5`, uint64(2), string(encoded), bucket, f.auth.UserID, f.auth.TenantID); err != nil {
					t.Fatal(err)
				}
				want = domain.ErrMembershipVersionChanged
			case "membership removed":
				bucket, err := ydbpartition.BucketV1(string(f.auth.UserID))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.client.DB.ExecContext(f.ctx, `DELETE FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`, bucket, f.auth.UserID, f.auth.TenantID); err != nil {
					t.Fatal(err)
				}
				want = domain.ErrMembershipDenied
			case "Web session revoked":
				if err := f.store.RevokeWebSession(f.ctx, f.web.SessionDigest, f.now); err != nil {
					t.Fatal(err)
				}
				want = domain.ErrWebSessionRevoked
			case "Web session expired":
				web := f.web
				web.IdleExpiresAt = f.now
				encoded, err := json.Marshal(web)
				if err != nil {
					t.Fatal(err)
				}
				bucket, err := ydbpartition.BucketV1(string(web.SessionDigest))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.client.DB.ExecContext(f.ctx, `UPDATE web_sessions SET record=CAST($1 AS JsonDocument) WHERE shard_bucket=$2 AND session_digest=$3`, string(encoded), bucket, web.SessionDigest); err != nil {
					t.Fatal(err)
				}
				want = domain.ErrWebSessionExpired
			case "tenant owner nonparticipant":
				if _, err := f.client.DB.ExecContext(f.ctx, `DELETE FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`, f.auth.TenantID, f.ingress.Run.SessionID, f.auth.UserID); err != nil {
					t.Fatal(err)
				}
				want = ports.ErrRunExplanationNotFound
			case "foreign Run":
				foreign := newExplanationYDBFixture(t)
				f.ingress.Run.ID = foreign.ingress.Run.ID
				want = ports.ErrRunExplanationNotFound
			case "absent Run":
				f.ingress.Run.ID = domain.RunID(uniqueID("absent-run"))
				want = ports.ErrRunExplanationNotFound
			case "historical locator absent":
				if _, err := f.client.DB.ExecContext(f.ctx, `DELETE FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, f.auth.TenantID, f.ingress.Run.ID); err != nil {
					t.Fatal(err)
				}
			case "selected Attempt mismatch":
				attempt := f.ingress.Attempt
				attempt.RunID = domain.RunID(uniqueID("replacement-run"))
				encoded, err := json.Marshal(attempt)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.client.DB.ExecContext(f.ctx, `UPDATE attempts SET payload=CAST($1 AS JsonDocument) WHERE tenant_id=$2 AND attempt_id=$3`, string(encoded), f.auth.TenantID, attempt.ID); err != nil {
					t.Fatal(err)
				}
			}
			value, err := f.store.ReadRunExplanationV1(f.ctx, f.auth, f.ingress.Run.ID)
			if want != nil {
				if !errors.Is(err, want) {
					t.Fatalf("resource error=%v want=%v", err, want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if value.Attempt.Availability != runexplanation.Unknown || value.Coverage.Admission != runexplanation.Unknown {
				t.Fatalf("missing/mismatch evidence=%+v", value.Coverage)
			}
		})
	}
}

func TestRunExplanationYDBFinalizeReadDeleteSnapshotsAndExactEvent(t *testing.T) {
	f := newExplanationYDBFixture(t)
	if result, err := f.store.AdmitDispatch(f.ctx, f.admission); err != nil || !result.Admitted {
		t.Fatalf("admission=%+v err=%v", result, err)
	}
	loaded, found, err := f.store.LoadWorkerJob(f.ctx, f.auth.TenantID, f.ingress.Run.ID)
	if err != nil || !found {
		t.Fatalf("worker state found=%t err=%v", found, err)
	}
	lease, err := f.store.ClaimWorkerLease(f.ctx, ports.WorkerLeaseRequest{TenantID: f.auth.TenantID, RunID: f.ingress.Run.ID, AttemptID: f.ingress.Attempt.ID, LeaseID: domain.LeaseID(uniqueID("explanation-lease")), WorkerID: "explanation-worker", Now: f.now.Add(-15 * time.Second), ExpiresAt: f.now.Add(24 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.StartWorkerJob(f.ctx, loaded, lease, f.now.Add(-10*time.Second)); err != nil {
		t.Fatal(err)
	}
	at := f.now.Add(-5 * time.Second)
	notice := canonicalTerminalDraft(f.auth.TenantID, f.ingress.Run.SessionID, "opaque-notice", domain.SessionEventSystemNotice, at)
	encoded := []byte(`{"schema":"sessionless.run-terminal-notice.v1","code":"harness_failed","cancelled":false}`)
	sum := sha256.Sum256(encoded)
	notice.Payload.SHA256 = hex.EncodeToString(sum[:])
	notice.Payload.Size = int64(len(encoded))
	failure := ports.WorkerFailure{TenantID: f.auth.TenantID, RunID: f.ingress.Run.ID, AttemptID: f.ingress.Attempt.ID, ReservationID: f.admission.ReservationID, LeaseID: lease.ID, Fence: lease.FenceToken, At: at, Code: "harness_failed", Events: []domain.SessionEventDraft{notice}}
	start := make(chan struct{})
	var wait sync.WaitGroup
	errs := make(chan error, 2)
	wait.Add(2)
	go func() { defer wait.Done(); <-start; errs <- f.store.FailWorkerJob(f.ctx, failure) }()
	go func() {
		defer wait.Done()
		<-start
		value, err := f.store.ReadRunExplanationV1(f.ctx, f.auth, f.ingress.Run.ID)
		if err == nil {
			err = value.Validate()
		}
		errs <- err
	}()
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("finalize/read snapshot race: %v", err)
		}
	}
	value := readExplanationYDB(t, f)
	if value.Status != domain.RunFailed || value.Terminal.EventID != notice.ID || value.Terminal.EventSequence != 1 || value.Terminal.ReasonCode != runexplanation.ExecutionFailed {
		t.Fatalf("exact allocated terminal metadata=%+v", value.Terminal)
	}
	if err := f.store.FailWorkerJob(f.ctx, failure); err != nil {
		t.Fatalf("durable replay: %v", err)
	}
	replay := readExplanationYDB(t, f)
	if !reflect.DeepEqual(replay.Terminal, value.Terminal) {
		t.Fatalf("replay changed terminal metadata=%+v", replay.Terminal)
	}
	deletion := domain.SessionDeletion{TenantID: f.auth.TenantID, SessionID: f.ingress.Run.SessionID, RequestedBy: f.auth.UserID, Reason: "test-owned deletion", State: domain.SessionDeletionRequested, RequestedAt: f.now}
	if _, err := f.store.RequestSessionDeletion(f.ctx, deletion); err != nil {
		t.Fatal(err)
	}
	inventory, err := f.store.BuildSessionDeletionInventory(f.ctx, f.auth.TenantID, deletion.SessionID, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.StartSessionDeletion(f.ctx, f.auth.TenantID, deletion.SessionID, f.now); err != nil {
		t.Fatal(err)
	}
	start = make(chan struct{})
	errs = make(chan error, 2)
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		_, err := f.store.CompleteSessionDeletion(f.ctx, f.auth.TenantID, deletion.SessionID, f.now, uint64(len(inventory.Objects)), inventory.TotalBytes)
		errs <- err
	}()
	go func() {
		defer wait.Done()
		<-start
		value, err := f.store.ReadRunExplanationV1(f.ctx, f.auth, f.ingress.Run.ID)
		if errors.Is(err, ports.ErrRunExplanationNotFound) {
			err = nil
		} else if err == nil {
			err = value.Validate()
		}
		errs <- err
	}()
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("delete/read snapshot race: %v", err)
		}
	}
	if _, err := f.store.ReadRunExplanationV1(f.ctx, f.auth, f.ingress.Run.ID); !errors.Is(err, ports.ErrRunExplanationNotFound) {
		t.Fatalf("deleted Run read error=%v", err)
	}
	var count uint64
	if err := f.client.DB.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, f.auth.TenantID, f.ingress.Run.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("orphan projection count=%d err=%v", count, err)
	}
}
