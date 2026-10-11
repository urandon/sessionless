package ydbstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// These opt-in tests exercise the existing SINGLE-TURN managed scheduler graph.
// Structural deterministic-fixture bindings and metadata are not installation,
// payload compatibility, funding, or owned-agent execution authority. No blob,
// provider, queue, or substrate effect is performed by these tests.
type nativeManagedAdmissionGraph struct {
	f       *ownedReaderFixture
	ctx     context.Context
	run     domain.Run
	attempt domain.Attempt
	outbox  domain.DispatchOutbox
	request ports.DispatchAdmissionRequest
}

func newNativeManagedAdmissionGraph(t *testing.T) nativeManagedAdmissionGraph {
	t.Helper()
	f, ctx := nativeOwnedCurrentFixture(t)
	store, scope, at := f.store, f.scope, f.driver.at
	store.idempotencyRetention, store.operationalRetention = time.Hour, time.Hour
	// Every table below belongs exclusively to this cryptographically unique
	// tenant. Register exact test-owned cleanup BEFORE the first seed write.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		for _, table := range []string{
			"audit_events", "run_explanation_heads_v1", "worker_jobs",
			"quota_expiry", "quota_expiry_v2", "quota_reservations",
			"subscription_scheduler_slots", "tenant_scheduler_counters",
			"dispatch_ready", "dispatch_ready_v2", "dispatch_outbox",
			"artifact_manifests_by_run", "artifact_manifests", "attempts",
			"runs_by_session", "run_idempotency", "runs", "subscription_connections",
			"session_snapshots", "session_event_idempotency", "session_events",
			"session_material_boundaries_v1", "session_material_current_v1",
			"session_activity", "session_participants", "sessions",
		} {
			if _, err := store.db.ExecContext(cleanup, "DELETE FROM "+table+" WHERE tenant_id=$1", scope.tenant); err != nil {
				t.Errorf("cleanup test-owned %s: %v", table, err)
			}
		}
	})
	connection := domain.SubscriptionConnectionID("subscription-" + string(scope.run))
	managed, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(scope.tenant, scope.owner, scope.run, scope.attempt, connection, at)
	if err != nil {
		t.Fatal(err)
	}
	if managed.HarnessBinding.Profile != domain.HarnessExecutionSingleTurnV2 || managed.HarnessBinding.Resource.CredentialMode != domain.ProviderCredentialNoneV1 {
		t.Fatal("native managed control must remain credential-free single-turn")
	}
	session := domain.Session{ID: scope.session, TenantID: scope.tenant, CreatedBy: scope.owner, Status: domain.SessionActive, CreatedAt: at, UpdatedAt: at}
	owner := domain.SessionParticipant{TenantID: scope.tenant, SessionID: scope.session, UserID: scope.owner, Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: at, UpdatedAt: at}
	if err := store.CreateSession(ctx, session, owner); err != nil {
		t.Fatalf("seed canonical Session: %v", err)
	}
	eventID := domain.SessionEventID("event-" + string(scope.run))
	contextRef := domain.BlobRef{TenantID: scope.tenant, Key: domain.SessionObjectPrefix(scope.tenant, scope.session) + "events/" + string(eventID) + "/payload.json", SHA256: strings.Repeat("a", 64), Size: 10}
	// The general canonical writer explicitly preserves unknown material when
	// no prepared real payload is supplied. This test does not certify that body.
	event := domain.SessionEvent{ID: eventID, TenantID: scope.tenant, SessionID: scope.session, Kind: domain.SessionEventUserMessage, AuthorUserID: &scope.owner, IdempotencyKey: domain.IdempotencyKey("event-key-" + string(scope.run)), Payload: contextRef, CreatedAt: at}
	if created, err := store.AppendSessionEvent(ctx, event); err != nil || !created {
		t.Fatalf("seed canonical trigger event: created=%v err=%v", created, err)
	}
	run := domain.Run{ID: scope.run, TenantID: scope.tenant, SessionID: scope.session, TriggerEventID: eventID, SubscriptionConnectionID: connection, Status: domain.RunCreated, IdempotencyKey: domain.IdempotencyKey("run-key-" + string(scope.run)), CreatedAt: at, UpdatedAt: at}
	attempt := domain.Attempt{ID: scope.attempt, TenantID: scope.tenant, RunID: scope.run, Number: 1, Status: domain.AttemptCreated, CreatedAt: at, UpdatedAt: at}
	manifest := domain.ArtifactManifest{ID: domain.ArtifactManifestID("manifest-" + string(scope.run)), TenantID: scope.tenant, RunID: scope.run, CreatedAt: at}
	if err := store.Transact(ctx, scope.tenant, func(state ports.StateTx) error {
		if err := state.PutRun(ctx, run); err != nil {
			return err
		}
		if err := state.PutAttempt(ctx, attempt); err != nil {
			return err
		}
		return state.PutArtifactManifest(ctx, manifest)
	}); err != nil {
		t.Fatalf("seed canonical Run/Attempt/input manifest: %v", err)
	}
	outbox := domain.DispatchOutbox{ID: domain.DispatchOutboxID("dispatch-" + string(scope.run)), TenantID: scope.tenant, RunID: scope.run, AttemptID: scope.attempt, InputManifestID: manifest.ID, ContextSnapshot: contextRef, ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1}, CredentialOwnerUserID: scope.owner, ExecutionPlacementV2: managed.ExecutionPlacementV2, HarnessBinding: managed.HarnessBinding, SubstrateBinding: &managed.SubstrateBinding, AdmissionCostCeiling: &managed.AdmissionCostCeiling, DeliveryChat: domain.TelegramChatRef{TenantID: scope.tenant, ChatID: 1}, ReplyToMessageID: 1, Status: domain.DispatchPending, IdempotencyKey: domain.IdempotencyKey("dispatch-key-" + string(scope.run)), CreatedAt: at, UpdatedAt: at}
	if err := outbox.ValidateForAttempt(run, attempt); err != nil {
		t.Fatalf("seed outbox structural validation: %v", err)
	}
	raw, err := marshal(outbox)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberate raw fixture installation keeps the explanation head cold;
	// PutDispatchOutbox would select it before the callback under test. No
	// trusted publisher or runtime registration is implied by this fixture.
	if _, err := store.db.ExecContext(ctx, `INSERT INTO dispatch_outbox (tenant_id,dispatch_outbox_id,run_id,attempt_id,status,created_at,updated_at,retention_expire_at,payload) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS JsonDocument))`, scope.tenant, outbox.ID, scope.run, scope.attempt, outbox.Status, at, at, at.Add(time.Hour), raw); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `INSERT INTO subscription_connections (tenant_id,subscription_connection_id,actor_id,provider,credential_ref,entitlement_state,quota_state,observed_at,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, scope.tenant, connection, "actor-"+string(scope.run), "deterministic", "", domain.EntitlementActive, domain.ProviderQuotaAvailable, at, at, at); err != nil {
		t.Fatal(err)
	}
	for version := uint64(1); version <= 16; version++ {
		snapshot := domain.SessionSnapshot{ID: domain.SessionSnapshotID(fmt.Sprintf("snapshot-%s-%d", scope.run, version)), TenantID: scope.tenant, SessionID: scope.session, Version: version, ThroughSequence: 1, FormatVersion: 1, Compression: "zstd", EventCount: 1, UncompressedSize: 10, CreatedAt: at, Payload: domain.BlobRef{TenantID: scope.tenant, Key: domain.SessionSnapshotObjectKey(scope.tenant, scope.session, version), SHA256: strings.Repeat("b", 64), Size: 10}}
		if err := store.PutSessionSnapshot(ctx, snapshot); err != nil {
			t.Fatalf("seed bounded canonical snapshot version=%d: %v", version, err)
		}
	}
	limits := domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 8, MaxContextEvents: 100, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18}
	request := ports.DispatchAdmissionRequest{TenantID: scope.tenant, OutboxID: outbox.ID, RunID: scope.run, AttemptID: scope.attempt, ReservationID: domain.QuotaReservationID("reservation-" + string(scope.run)), Now: at, HoldUntil: at.Add(time.Hour), Limits: limits, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
	if err := validateAdmissionRequest(request); err != nil {
		t.Fatal(err)
	}
	g := nativeManagedAdmissionGraph{f: f, ctx: ctx, run: run, attempt: attempt, outbox: outbox, request: request}
	for _, table := range nativeManagedAdmissionEffectTables {
		g.requireCount(t, table, 0)
	}
	return g
}

var nativeManagedAdmissionEffectTables = []string{"worker_jobs", "quota_reservations", "quota_expiry", "quota_expiry_v2", "subscription_scheduler_slots", "tenant_scheduler_counters", "run_explanation_heads_v1", "audit_events"}

func (g nativeManagedAdmissionGraph) requireCount(t *testing.T, table string, want uint64) {
	t.Helper()
	var got uint64
	if err := g.f.store.db.QueryRowContext(g.ctx, "SELECT COUNT(*) FROM "+table+" WHERE tenant_id=$1", g.run.TenantID).Scan(&got); err != nil || got != want {
		t.Errorf("test-owned table=%s count=%d want=%d err=%v", table, got, want, err)
	}
}

// Calls the SAME production admission reducer under the SAME transaction,
// clock and opt-in cache. Only captures the existing budget, not a scheduler.
func (g nativeManagedAdmissionGraph) measuredAdmission() (ports.DispatchAdmissionResult, *ownedAuthorityBudget, error) {
	result := ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
	var budget *ownedAuthorityBudget
	err := g.f.store.ownedAuthorityTx(g.ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		budget = q.budget
		result = ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		request := g.request
		request.Now = q.at
		return g.f.store.admitDispatchTx(ctx, q.canonicalState(g.f.store, g.run.TenantID), request, &result)
	})
	if err != nil {
		result = ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
	}
	return result, budget, err
}

func (g nativeManagedAdmissionGraph) requireCommittedGraph(t *testing.T, result ports.DispatchAdmissionResult) {
	t.Helper()
	if !result.Admitted || result.Code != "admitted" || result.Delivery != ports.DispatchDeliveryManagedQueue || result.State != domain.SchedulerReady || result.SessionID != g.run.SessionID || result.ThroughSequence != 1 {
		t.Fatalf("actual managed admission result=%+v", result)
	}
	for _, table := range nativeManagedAdmissionEffectTables {
		g.requireCount(t, table, 1)
	}
	run, found, err := readJSON[domain.Run](g.ctx, g.f.store.db, `SELECT payload FROM runs WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || run.Status != domain.RunQueued || run.Validate() != nil || run.CreatedAt != g.run.CreatedAt || run.UpdatedAt.Before(g.run.UpdatedAt) {
		t.Fatalf("queued canonical Run found=%v err=%v run=%+v", found, err, run)
	}
	job, found, err := readJSON[domain.WorkerJob](g.ctx, g.f.store.db, `SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || job.ValidateForRun(run) != nil || job.ContextWindow == nil || job.ContextWindow.SnapshotVersion == nil || *job.ContextWindow.SnapshotVersion != 16 || job.ContextWindow.AfterSequence != 1 || job.ContextWindow.ThroughSequence != 1 || job.AttemptID != g.attempt.ID || job.InputManifestID != g.outbox.InputManifestID || job.ReservationID != g.request.ReservationID || !job.CreatedAt.Equal(run.UpdatedAt) {
		t.Fatalf("admitted Job/window found=%v err=%v job=%+v", found, err, job)
	}
	reservation, found, err := readJSON[domain.QuotaReservation](g.ctx, g.f.store.db, `SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2`, g.run.TenantID, g.request.ReservationID)
	if err != nil || !found || reservation.ValidateForRun(run) != nil || reservation.Status != domain.ReservationHeld || reservation.CapacityUnits != 1 || !reservation.HeldAt.Equal(job.CreatedAt) || !reservation.ExpiresAt.Equal(g.request.HoldUntil) {
		t.Fatalf("held reservation found=%v err=%v reservation=%+v", found, err, reservation)
	}
	bucket, err := ydbpartition.BucketV1(string(g.request.ReservationID))
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []struct {
		query string
		args  []any
	}{
		{`SELECT run_id,subscription_connection_id FROM quota_expiry WHERE tenant_id=$1 AND expires_at=$2 AND quota_reservation_id=$3`, []any{g.run.TenantID, reservation.ExpiresAt, reservation.ID}},
		{`SELECT run_id,subscription_connection_id FROM quota_expiry_v2 WHERE shard_bucket=$1 AND expires_at=$2 AND tenant_id=$3 AND quota_reservation_id=$4`, []any{bucket, reservation.ExpiresAt, g.run.TenantID, reservation.ID}},
	} {
		var runID domain.RunID
		var connection domain.SubscriptionConnectionID
		if err := g.f.store.db.QueryRowContext(g.ctx, index.query, index.args...).Scan(&runID, &connection); err != nil || runID != run.ID || connection != run.SubscriptionConnectionID {
			t.Errorf("exact quota expiry index run=%s connection=%s err=%v", runID, connection, err)
		}
	}
	var slotState domain.SchedulerState
	var activeRun domain.RunID
	var activeReservation domain.QuotaReservationID
	var slotAt, counterAt time.Time
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT state,active_run_id,active_reservation_id,updated_at FROM subscription_scheduler_slots WHERE tenant_id=$1 AND subscription_connection_id=$2`, run.TenantID, run.SubscriptionConnectionID).Scan(&slotState, &activeRun, &activeReservation, &slotAt); err != nil || slotState != domain.SchedulerPressured || activeRun != run.ID || activeReservation != reservation.ID || !slotAt.Equal(job.CreatedAt) {
		t.Errorf("slot state=%s run=%s reservation=%s at=%v err=%v", slotState, activeRun, activeReservation, slotAt, err)
	}
	var queued, active uint32
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT queue_depth,active_runs,updated_at FROM tenant_scheduler_counters WHERE tenant_id=$1`, run.TenantID).Scan(&queued, &active, &counterAt); err != nil || queued != 1 || active != 0 || !counterAt.Equal(job.CreatedAt) {
		t.Errorf("counters queue=%d active=%d at=%v err=%v", queued, active, counterAt, err)
	}
	head, found, err := readJSON[runexplanation.HeadV1](g.ctx, g.f.store.db, `SELECT record FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, run.TenantID, run.ID)
	if err != nil || !found || head.Validate() != nil || head.SelectedAttemptID != g.attempt.ID || head.Admission == nil || head.Admission.Outcome != runexplanation.Admitted || !head.Admission.Basis.ObservedAt.Equal(job.CreatedAt) {
		t.Fatalf("admission explanation found=%v err=%v head=%+v", found, err, head)
	}
	var action, subject, outcome string
	var auditAt time.Time
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT action,subject_id,outcome,occurred_at FROM audit_events WHERE tenant_id=$1`, run.TenantID).Scan(&action, &subject, &outcome, &auditAt); err != nil || action != "scheduler.admitted" || subject != string(run.ID) || outcome != "held" || !auditAt.Equal(job.CreatedAt) {
		t.Errorf("audit action=%s subject=%s outcome=%s at=%v err=%v", action, subject, outcome, auditAt, err)
	}
}

// Native JsonDocument output can reorder keys/whitespace. Compare the actual
// stored representations on BOTH sides of replay, never Go marshal lengths.
func (g nativeManagedAdmissionGraph) persistedGraph(t *testing.T) []string {
	t.Helper()
	queries := []string{
		`SELECT CAST(payload AS String) FROM runs WHERE tenant_id=$1`,
		`SELECT CAST(payload AS String) FROM worker_jobs WHERE tenant_id=$1`,
		`SELECT CAST(payload AS String) FROM quota_reservations WHERE tenant_id=$1`,
		`SELECT CAST(record AS String) FROM run_explanation_heads_v1 WHERE tenant_id=$1`,
	}
	var values []string
	for _, query := range queries {
		var raw string
		if err := g.f.store.db.QueryRowContext(g.ctx, query, g.run.TenantID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		values = append(values, raw)
	}
	var state, runID, reservationID, action, subject, outcome, metadata string
	var slotAt, counterAt, auditAt, auditExpiry time.Time
	var queued, active uint32
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT state,active_run_id,active_reservation_id,updated_at FROM subscription_scheduler_slots WHERE tenant_id=$1`, g.run.TenantID).Scan(&state, &runID, &reservationID, &slotAt); err != nil {
		t.Fatal(err)
	}
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT queue_depth,active_runs,updated_at FROM tenant_scheduler_counters WHERE tenant_id=$1`, g.run.TenantID).Scan(&queued, &active, &counterAt); err != nil {
		t.Fatal(err)
	}
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT action,subject_id,outcome,CAST(metadata AS String),occurred_at,expire_at FROM audit_events WHERE tenant_id=$1`, g.run.TenantID).Scan(&action, &subject, &outcome, &metadata, &auditAt, &auditExpiry); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal([]any{state, runID, reservationID, slotAt, queued, active, counterAt, action, subject, outcome, metadata, auditAt, auditExpiry})
	if err != nil {
		t.Fatal(err)
	}
	return append(values, string(encoded))
}

func TestOwnedManagedAdmissionGraphYDBCompleteAndReplay(t *testing.T) {
	g := newNativeManagedAdmissionGraph(t)
	result, err := g.f.store.admitBoundedManagedDispatch(g.ctx, g.request)
	if err != nil {
		t.Fatalf("actual bounded managed admission: %v", err)
	}
	g.requireCommittedGraph(t, result)
	before := g.persistedGraph(t)
	replay, budget, err := g.measuredAdmission()
	if err != nil || !replay.Admitted || replay.Code != "already_admitted" || replay.Delivery != ports.DispatchDeliveryManagedQueue {
		t.Fatalf("actual callback replay result=%+v err=%v", replay, err)
	}
	if budget == nil || budget.statements > ownedAuthorityMaxTotalStatements || budget.bytes == 0 || budget.bytes > ownedAuthorityMaxBytes {
		t.Fatalf("replay budget=%+v", budget)
	}
	if after := g.persistedGraph(t); !reflect.DeepEqual(before, after) {
		t.Error("replay rewrote canonical payloads/head, slot/counters, or audit timestamps/metadata")
	}
	for _, table := range nativeManagedAdmissionEffectTables {
		g.requireCount(t, table, 1)
	}
	// Readback alone cannot observe a same-value UPSERT. The strict driver test
	// separately asserts zero Exec calls; native replay proves immutable state.
	t.Logf("actual bounded replay callbacks=%d statements=%d materialized_bytes=%d", budget.callbacks, budget.statements, budget.bytes)
}

func TestOwnedManagedAdmissionGraphYDBMeasuredColdCallback(t *testing.T) {
	g := newNativeManagedAdmissionGraph(t)
	result, budget, err := g.measuredAdmission()
	if err != nil {
		t.Fatalf("actual cold admission callback: %v budget=%+v", err, budget)
	}
	g.requireCommittedGraph(t, result)
	if budget == nil || budget.callbacks < 1 || budget.callbacks > ownedAuthorityMaxCallbacks || budget.statements > ownedAuthorityMaxTotalStatements || budget.callbackStatements > ownedAuthorityMaxStatements || budget.bytes == 0 || budget.bytes > ownedAuthorityMaxBytes {
		t.Fatalf("actual cold graph exceeded logical-call budget: %+v", budget)
	}
	t.Logf("actual cold graph with 16 snapshots callbacks=%d statements=%d materialized_bytes=%d", budget.callbacks, budget.statements, budget.bytes)
}

func TestOwnedManagedAdmissionGraphYDBLateSnapshotRollback(t *testing.T) {
	g := newNativeManagedAdmissionGraph(t)
	var raw string
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CAST(record AS String) FROM session_snapshots WHERE tenant_id=$1 AND session_id=$2 AND version=$3`, g.run.TenantID, g.run.SessionID, uint64(15)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	oversized := strings.TrimSuffix(raw, "}") + `,"test_owned_padding":"` + strings.Repeat("x", admittedSnapshotMetadataBytes) + `"}`
	if !json.Valid([]byte(oversized)) {
		t.Fatal("oversize fixture must remain valid JsonDocument")
	}
	if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE session_snapshots SET record=CAST($1 AS JsonDocument) WHERE tenant_id=$2 AND session_id=$3 AND version=$4`, oversized, g.run.TenantID, g.run.SessionID, uint64(15)); err != nil {
		t.Fatal(err)
	}
	result, err := g.f.store.admitBoundedManagedDispatch(g.ctx, g.request)
	want := ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, want) {
		t.Fatalf("late metadata overflow err=%v result=%+v want=%+v", err, result, want)
	}
	// Version 16 is valid and sorted first. Version 15 exceeds the source cap
	// after Run/head/reservation/index writes in the real admission reducer.
	// No speculative graph row may survive the serializable transaction.
	for _, table := range nativeManagedAdmissionEffectTables {
		g.requireCount(t, table, 0)
	}
	run, found, err := readJSON[domain.Run](g.ctx, g.f.store.db, `SELECT payload FROM runs WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || !reflect.DeepEqual(run, g.run) {
		t.Errorf("late failure changed seed Run: found=%v err=%v run=%+v want=%+v", found, err, run, g.run)
	}
	attempt, found, err := readJSON[domain.Attempt](g.ctx, g.f.store.db, `SELECT payload FROM attempts WHERE tenant_id=$1 AND attempt_id=$2`, g.run.TenantID, g.attempt.ID)
	if err != nil || !found || !reflect.DeepEqual(attempt, g.attempt) {
		t.Errorf("late failure changed seed Attempt: found=%v err=%v attempt=%+v", found, err, attempt)
	}
	var status domain.RunStatus
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT status FROM runs_by_session WHERE tenant_id=$1 AND session_id=$2 AND created_at=$3 AND run_id=$4`, g.run.TenantID, g.run.SessionID, g.run.CreatedAt, g.run.ID).Scan(&status); err != nil || status != domain.RunCreated {
		t.Errorf("late failure changed Run index status=%s err=%v", status, err)
	}
}

func newNativeManagedPreviousDeniedGraph(t *testing.T) (nativeManagedAdmissionGraph, domain.Attempt, runexplanation.HeadV1) {
	t.Helper()
	g := newNativeManagedAdmissionGraph(t)
	previous := g.attempt
	previous.ID = domain.AttemptID("previous-" + string(g.attempt.ID))
	g.attempt.Number = 2
	// Preserve the actual fixture's tenant/Run/Session/Attempt identities and
	// DB-sampled timestamps. The earlier Attempt has its own test-owned ID;
	// the current binding continues to name the original current Attempt.
	if err := g.run.Transition(domain.RunQuotaBlocked, g.f.driver.at); err != nil {
		t.Fatal(err)
	}
	if err := g.outbox.ValidateForAttempt(g.run, g.attempt); err != nil {
		t.Fatalf("current outbox with Attempt 2: %v", err)
	}
	err := g.f.store.Transact(g.ctx, g.run.TenantID, func(state ports.StateTx) error {
		if err := state.PutRun(g.ctx, g.run); err != nil {
			return err
		}
		if err := state.PutAttempt(g.ctx, previous); err != nil {
			return err
		}
		if err := state.PutAttempt(g.ctx, g.attempt); err != nil {
			return err
		}
		// Publish the previous denial last: arbitrary Attempt writes never select
		// a new Attempt, and the CURRENT admission must perform that selection.
		return recordAdmissionExplanationTx(g.ctx, state.(*stateTx), g.run, previous, g.outbox.ExecutionPlacementV2, "subscription_slot_busy", g.f.driver.at)
	})
	if err != nil {
		t.Fatalf("seed canonical previous denial/Attempt 2: %v", err)
	}
	seedHead, changed, err := runexplanation.Select(nil, g.run, previous, g.outbox.ExecutionPlacementV2, g.f.driver.at)
	if err != nil || !changed {
		t.Fatalf("independent seed selection changed=%v err=%v", changed, err)
	}
	seedHead, changed, err = runexplanation.RecordAdmission(seedHead, g.run, previous, "subscription_slot_busy", g.f.driver.at)
	if err != nil || !changed || seedHead.Admission == nil || seedHead.Admission.Outcome != runexplanation.Denied || seedHead.Revision != 2 {
		t.Fatalf("independent seed denial changed=%v err=%v head=%+v", changed, err, seedHead)
	}
	storedSeed, found, err := readJSON[runexplanation.HeadV1](g.ctx, g.f.store.db, `SELECT record FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || !reflect.DeepEqual(storedSeed, seedHead) {
		t.Fatalf("canonical previous denial found=%v err=%v got=%+v want=%+v", found, err, storedSeed, seedHead)
	}
	// This branch explicitly starts with ONE explanation, unlike the original
	// three cold-head cases. All scheduler capacity/effect rows remain absent.
	for _, table := range nativeManagedAdmissionEffectTables {
		want := uint64(0)
		if table == "run_explanation_heads_v1" {
			want = 1
		}
		g.requireCount(t, table, want)
	}
	return g, previous, seedHead
}

func TestOwnedManagedAdmissionGraphYDBPreviousDeniedExplanation(t *testing.T) {
	g, previous, seedHead := newNativeManagedPreviousDeniedGraph(t)
	result, budget, err := g.measuredAdmission()
	if err != nil {
		t.Fatalf("actual previous-denied admission callback: %v budget=%+v", err, budget)
	}
	g.requireCommittedGraph(t, result)
	if budget == nil || budget.callbacks < 1 || budget.callbacks > ownedAuthorityMaxCallbacks || budget.statements > ownedAuthorityMaxTotalStatements || budget.callbackStatements > ownedAuthorityMaxStatements || budget.bytes == 0 || budget.bytes > ownedAuthorityMaxBytes {
		t.Fatalf("previous-denied graph exceeded existing logical-call budget: %+v", budget)
	}
	queuedRun, found, err := readJSON[domain.Run](g.ctx, g.f.store.db, `SELECT payload FROM runs WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found {
		t.Fatalf("read admitted Run for authoritative reducer time: found=%v err=%v", found, err)
	}
	// Production PutRun invalidates the old denial, then the shared admission
	// writer runs Select and RecordAdmission. Calculate the ENTIRE head with
	// those pure reducers at the actual committed DB admission timestamp,
	// including exact revisions, basis, and fingerprint rather than a guess.
	expected, changed, err := runexplanation.Invalidate(seedHead, true, false)
	if err != nil || !changed {
		t.Fatalf("expected prior-denial invalidation changed=%v err=%v", changed, err)
	}
	expected, changed, err = runexplanation.Select(&expected, queuedRun, g.attempt, g.outbox.ExecutionPlacementV2, queuedRun.UpdatedAt)
	if err != nil || !changed {
		t.Fatalf("expected current Attempt selection changed=%v err=%v", changed, err)
	}
	expected, changed, err = runexplanation.RecordAdmission(expected, queuedRun, g.attempt, "admitted", queuedRun.UpdatedAt)
	if err != nil || !changed || expected.Revision != 5 || expected.Admission == nil || expected.Admission.Revision != 5 || expected.Admission.Fingerprint == seedHead.Admission.Fingerprint {
		t.Fatalf("expected current admission changed=%v err=%v head=%+v", changed, err, expected)
	}
	got, found, err := readJSON[runexplanation.HeadV1](g.ctx, g.f.store.db, `SELECT record FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || !reflect.DeepEqual(got, expected) {
		t.Errorf("ordered Invalidate/Select/RecordAdmission found=%v err=%v got=%+v want=%+v", found, err, got, expected)
	}
	for _, want := range []domain.Attempt{previous, g.attempt} {
		got, found, err := readJSON[domain.Attempt](g.ctx, g.f.store.db, `SELECT payload FROM attempts WHERE tenant_id=$1 AND attempt_id=$2`, want.TenantID, want.ID)
		if err != nil || !found || !reflect.DeepEqual(got, want) {
			t.Errorf("admission changed seed Attempt=%s found=%v err=%v got=%+v want=%+v", want.ID, found, err, got, want)
		}
	}
	t.Logf("actual previous-denied graph with 16 snapshots callbacks=%d statements=%d materialized_bytes=%d", budget.callbacks, budget.statements, budget.bytes)
}

func requireNativeManagedAdmissionRollback(t *testing.T, g nativeManagedAdmissionGraph, priorHead *runexplanation.HeadV1) {
	t.Helper()
	for _, table := range nativeManagedAdmissionEffectTables {
		want := uint64(0)
		if table == "run_explanation_heads_v1" && priorHead != nil {
			want = 1
		}
		g.requireCount(t, table, want)
	}
	run, found, err := readJSON[domain.Run](g.ctx, g.f.store.db, `SELECT payload FROM runs WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || !reflect.DeepEqual(run, g.run) {
		t.Errorf("failed admission changed original Run: found=%v err=%v got=%+v want=%+v", found, err, run, g.run)
	}
	attempt, found, err := readJSON[domain.Attempt](g.ctx, g.f.store.db, `SELECT payload FROM attempts WHERE tenant_id=$1 AND attempt_id=$2`, g.run.TenantID, g.attempt.ID)
	if err != nil || !found || !reflect.DeepEqual(attempt, g.attempt) {
		t.Errorf("failed admission changed original current Attempt: found=%v err=%v got=%+v want=%+v", found, err, attempt, g.attempt)
	}
	var status domain.RunStatus
	var eventID domain.SessionEventID
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT status,trigger_event_id FROM runs_by_session WHERE tenant_id=$1 AND session_id=$2 AND created_at=$3 AND run_id=$4`, g.run.TenantID, g.run.SessionID, g.run.CreatedAt, g.run.ID).Scan(&status, &eventID); err != nil || status != g.run.Status || eventID != g.run.TriggerEventID {
		t.Errorf("failed admission changed original Run index: status=%s event=%s err=%v", status, eventID, err)
	}
	var runID domain.RunID
	var createdAt time.Time
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT run_id,created_at FROM run_idempotency WHERE tenant_id=$1 AND idempotency_key=$2`, g.run.TenantID, g.run.IdempotencyKey).Scan(&runID, &createdAt); err != nil || runID != g.run.ID || !createdAt.Equal(g.run.CreatedAt) {
		t.Errorf("failed admission changed original Run idempotency: run=%s created_at=%v err=%v", runID, createdAt, err)
	}
	if priorHead != nil {
		got, found, err := readJSON[runexplanation.HeadV1](g.ctx, g.f.store.db, `SELECT record FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
		if err != nil || !found || !reflect.DeepEqual(got, *priorHead) {
			t.Errorf("failed admission did not restore exact previous head: found=%v err=%v got=%+v want=%+v", found, err, got, *priorHead)
		}
	}
}

func TestOwnedManagedAdmissionGraphYDBPreviousAttemptPhysicalIDRollback(t *testing.T) {
	g, previous, priorHead := newNativeManagedPreviousDeniedGraph(t)
	corrupt := previous
	corrupt.ID = domain.AttemptID("different-" + string(previous.ID))
	if err := corrupt.ValidateForRun(g.run); err != nil {
		t.Fatalf("wrong-key payload must remain separately domain-valid: %v", err)
	}
	raw, err := marshal(corrupt)
	if err != nil {
		t.Fatal(err)
	}
	// Change ONLY the JSON identity at the physical key selected by the head.
	// PK, scalar Run/number/status/worker/time, and every other payload field
	// remain unchanged, so malformed data cannot mask the missing key join.
	if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE attempts SET payload=CAST($1 AS JsonDocument) WHERE tenant_id=$2 AND attempt_id=$3`, raw, previous.TenantID, previous.ID); err != nil {
		t.Fatal(err)
	}
	checkPhysicalRow := func() {
		t.Helper()
		var payload string
		var runID domain.RunID
		var number uint32
		var status domain.AttemptStatus
		var workerID string
		var createdAt, updatedAt time.Time
		if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CAST(payload AS String),run_id,attempt_number,status,worker_id,created_at,updated_at FROM attempts WHERE tenant_id=$1 AND attempt_id=$2`, previous.TenantID, previous.ID).Scan(&payload, &runID, &number, &status, &workerID, &createdAt, &updatedAt); err != nil {
			t.Fatal(err)
		}
		var got domain.Attempt
		if err := json.Unmarshal([]byte(payload), &got); err != nil || !reflect.DeepEqual(got, corrupt) || runID != previous.RunID || number != previous.Number || status != previous.Status || workerID != previous.WorkerID || !createdAt.Equal(previous.CreatedAt) || !updatedAt.Equal(previous.UpdatedAt) {
			t.Fatalf("physical previous row was not exact intended ID-only corruption: err=%v got=%+v want=%+v scalar_run=%s number=%d status=%s worker=%s created=%v updated=%v", err, got, corrupt, runID, number, status, workerID, createdAt, updatedAt)
		}
	}
	checkPhysicalRow()
	result, err := g.f.store.admitBoundedManagedDispatch(g.ctx, g.request)
	want := ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, want) {
		t.Errorf("wrong previous physical ID accepted: err=%v result=%+v want=%+v", err, result, want)
	}
	// Selection rereads the previous row AFTER PutRun invalidates/publishes
	// the head and writes queued Run/indices, plus cold slot/counter writes.
	// The whole transaction must restore the original quota-blocked sources.
	requireNativeManagedAdmissionRollback(t, g, &priorHead)
	checkPhysicalRow()
}

func TestOwnedManagedAdmissionGraphYDBManifestPhysicalIDRollback(t *testing.T) {
	g := newNativeManagedAdmissionGraph(t)
	manifest, found, err := readJSON[domain.ArtifactManifest](g.ctx, g.f.store.db, `SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2`, g.run.TenantID, g.outbox.InputManifestID)
	if err != nil || !found || manifest.ValidateForRun(g.run) != nil || manifest.ID != g.outbox.InputManifestID {
		t.Fatalf("canonical input manifest fixture found=%v err=%v manifest=%+v", found, err, manifest)
	}
	corrupt := manifest
	corrupt.ID = domain.ArtifactManifestID("different-" + string(manifest.ID))
	if err := corrupt.ValidateForRun(g.run); err != nil {
		t.Fatalf("wrong-key manifest must remain separately domain-valid: %v", err)
	}
	raw, err := marshal(corrupt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE artifact_manifests SET payload=CAST($1 AS JsonDocument) WHERE tenant_id=$2 AND artifact_manifest_id=$3`, raw, manifest.TenantID, manifest.ID); err != nil {
		t.Fatal(err)
	}
	checkPhysicalRow := func() {
		t.Helper()
		var payload string
		var runID domain.RunID
		var createdAt time.Time
		if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CAST(payload AS String),run_id,created_at FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2`, manifest.TenantID, manifest.ID).Scan(&payload, &runID, &createdAt); err != nil {
			t.Fatal(err)
		}
		var got domain.ArtifactManifest
		if err := json.Unmarshal([]byte(payload), &got); err != nil || !reflect.DeepEqual(got, corrupt) || runID != manifest.RunID || !createdAt.Equal(manifest.CreatedAt) {
			t.Fatalf("physical manifest was not exact intended ID-only corruption: err=%v got=%+v want=%+v scalar_run=%s created=%v", err, got, corrupt, runID, createdAt)
		}
		var indexedID domain.ArtifactManifestID
		if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT artifact_manifest_id FROM artifact_manifests_by_run WHERE tenant_id=$1 AND run_id=$2 AND artifact_manifest_id=$3`, manifest.TenantID, manifest.RunID, manifest.ID).Scan(&indexedID); err != nil || indexedID != manifest.ID {
			t.Errorf("original physical manifest index changed: id=%s err=%v", indexedID, err)
		}
	}
	checkPhysicalRow()
	result, err := g.f.store.admitBoundedManagedDispatch(g.ctx, g.request)
	want := ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, want) {
		t.Errorf("wrong input manifest physical ID accepted: err=%v result=%+v want=%+v", err, result, want)
	}
	// PutWorkerJob loads the manifest AFTER queued Run/head and held quota/
	// BOTH expiry-index writes and the bounded 16-snapshot selection. No
	// speculative job/capacity/head/audit may survive that late key rejection.
	requireNativeManagedAdmissionRollback(t, g, nil)
	checkPhysicalRow()
}
