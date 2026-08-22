//go:build e2elocal

package e2e

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionlifecycle"
)

func TestOperationalTTLPreservationAndResumableSessionDeletion(t *testing.T) {
	if os.Getenv("SESSIONLESS_E2E") != "1" {
		t.Skip("set SESSIONLESS_E2E=1 and start the local stand")
	}
	slice := newLocalSlice(t)
	defer slice.close()
	slice.reset()

	base := time.Now().UTC().UnixMilli()
	sameTenantChat := base*2 + 401
	crossTenantChat := base*2 + 402

	target := slice.completeRetentionRun(base+401, sameTenantChat, "target session for exact deletion")
	newSession := slice.postMessage(base+402, sameTenantChat, "/new")
	slice.waitRunStatus(newSession, domain.RunSucceeded)
	sentinel := slice.postMessage(base+403, sameTenantChat, "same-tenant retention sentinel")
	slice.waitRunStatus(sentinel, domain.RunQueued)
	slice.runWorker(nil)
	slice.waitRunStatus(sentinel, domain.RunSucceeded)
	crossTenant := slice.completeRetentionRun(base+404, crossTenantChat, "cross-tenant retention sentinel")

	documents := len(slice.outputManifest(target).Artifacts) + len(slice.outputManifest(sentinel).Artifacts)
	crossDocuments := len(slice.outputManifest(crossTenant).Artifacts)
	slice.waitForChatMethods(map[int64]map[string]int{
		sameTenantChat:  {"sendMessage": 3, "sendDocument": documents},
		crossTenantChat: {"sendMessage": 1, "sendDocument": crossDocuments},
	})
	slice.waitTelegramDeliveryDrain(target, newSession, sentinel, crossTenant)
	slice.ensureCanonicalSnapshot(target)
	if target.TenantID != sentinel.TenantID || target.TenantID == crossTenant.TenantID {
		t.Fatalf("sentinel tenant layout is invalid: target=%s same=%s cross=%s", target.TenantID, sentinel.TenantID, crossTenant.TenantID)
	}
	if current := slice.currentSession(sameTenantChat); current != sentinel.SessionID {
		t.Fatalf("current same-tenant session = %s, want sentinel %s", current, sentinel.SessionID)
	}

	targetBefore := slice.captureRetainedSession(target, target)
	sameTenantBefore := slice.captureRetainedSession(sentinel, sentinel)
	crossTenantBefore := slice.captureRetainedSession(crossTenant, crossTenant)
	initialDeliveryLedger := slice.countRunRows(target,
		`SELECT COUNT(*) FROM telegram_deliveries_by_run WHERE tenant_id = $1 AND run_id = $2`)
	initialCheckpointLedger := slice.countRunRows(target,
		`SELECT COUNT(*) FROM checkpoint_objects_by_run WHERE tenant_id = $1 AND run_id = $2`)
	if initialDeliveryLedger == 0 || initialCheckpointLedger == 0 {
		t.Fatalf("target lacks durable object ownership ledgers: deliveries=%d checkpoints=%d",
			initialDeliveryLedger, initialCheckpointLedger)
	}

	// This is a deterministic simulation of YDB TTL effects, not a claim about
	// wall-clock TTL scheduling. Only tables whose schema is TTL-governed are
	// touched, and every deletion is scoped to the exact target run.
	slice.deleteRunOperationalTTLRows(target)
	slice.assertRunOperationalTTLRowsGone(target)
	if got := slice.countRunRows(target,
		`SELECT COUNT(*) FROM telegram_deliveries_by_run WHERE tenant_id = $1 AND run_id = $2`); got != initialDeliveryLedger {
		t.Fatalf("delivery ownership ledger changed across operational TTL simulation: got=%d want=%d", got, initialDeliveryLedger)
	}
	if got := slice.countRunRows(target,
		`SELECT COUNT(*) FROM checkpoint_objects_by_run WHERE tenant_id = $1 AND run_id = $2`); got != initialCheckpointLedger {
		t.Fatalf("checkpoint ownership ledger changed across operational TTL simulation: got=%d want=%d", got, initialCheckpointLedger)
	}
	slice.assertRetainedSession(targetBefore, slice.captureRetainedSession(target, target))
	slice.assertRetainedSession(sameTenantBefore, slice.captureRetainedSession(sentinel, sentinel))
	slice.assertRetainedSession(crossTenantBefore, slice.captureRetainedSession(crossTenant, crossTenant))

	requestedAt := time.Now().UTC()
	request, err := slice.state.RequestSessionDeletion(slice.ctx, domain.SessionDeletion{
		TenantID: target.TenantID, SessionID: target.SessionID,
		RequestedBy: slice.sessionOwner(target), Reason: "composed retention deletion e2e",
		State: domain.SessionDeletionRequested, RequestedAt: requestedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.State != domain.SessionDeletionRequested {
		t.Fatalf("deletion request = %+v", request)
	}

	failingBlobs := &failAfterDeleteBlobStore{delegate: slice.blobs}
	service, err := sessionlifecycle.New(slice.state, failingBlobs, 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := service.Plan(slice.ctx, target.TenantID, target.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Confirmation == "" || len(plan.Inventory.Objects) < 2 {
		t.Fatalf("deletion plan cannot prove an interrupted exact-object retry: %+v", plan)
	}
	if plan.Inventory.DeliveryRows != initialDeliveryLedger || plan.Inventory.CheckpointRows != initialCheckpointLedger {
		t.Fatalf("plan did not recover TTL-expired delivery/checkpoint ownership: %+v", plan.Inventory)
	}

	if _, err := service.Execute(
		slice.ctx, target.TenantID, target.SessionID, plan.Confirmation, requestedAt.Add(time.Second),
	); err == nil || !errors.Is(err, errInjectedDeleteInterruption) {
		t.Fatalf("first deletion execute error = %v, want injected interruption", err)
	}
	if failingBlobs.deleted != 1 {
		t.Fatalf("interrupted executor deleted %d exact objects, want one", failingBlobs.deleted)
	}
	deleting, found, err := slice.state.GetSessionDeletion(slice.ctx, target.TenantID, target.SessionID)
	if err != nil || !found || deleting.State != domain.SessionDeletionDeleting {
		t.Fatalf("interrupted deletion found=%t state=%+v err=%v", found, deleting, err)
	}
	slice.assertTargetCanonicalRowsPresent(target)
	slice.assertRetainedSession(sameTenantBefore, slice.captureRetainedSession(sentinel, sentinel))
	slice.assertRetainedSession(crossTenantBefore, slice.captureRetainedSession(crossTenant, crossTenant))

	replan, err := service.Plan(slice.ctx, target.TenantID, target.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if replan.Confirmation != plan.Confirmation || !reflect.DeepEqual(replan.Inventory, plan.Inventory) {
		t.Fatalf("inventory changed after an object-only interruption: before=%+v after=%+v", plan, replan)
	}

	completed, err := service.Execute(
		slice.ctx, target.TenantID, target.SessionID, replan.Confirmation, requestedAt.Add(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != domain.SessionDeletionCompleted ||
		completed.DeletedObjects != uint64(len(plan.Inventory.Objects)) ||
		completed.DeletedBytes != plan.Inventory.TotalBytes {
		t.Fatalf("completed deletion = %+v, inventory=%+v", completed, plan.Inventory)
	}
	slice.assertTargetCanonicalRowsGone(target)
	for _, ref := range plan.Inventory.Objects {
		reader, err := slice.blobs.Open(slice.ctx, target.TenantID, ref)
		if err == nil {
			_ = reader.Close()
			t.Fatalf("deleted exact object still opens: %s", ref.Key)
		}
	}
	if current := slice.currentSession(sameTenantChat); current != sentinel.SessionID {
		t.Fatalf("target deletion changed current binding to %s, want sentinel %s", current, sentinel.SessionID)
	}
	slice.assertRetainedSession(sameTenantBefore, slice.captureRetainedSession(sentinel, sentinel))
	slice.assertRetainedSession(crossTenantBefore, slice.captureRetainedSession(crossTenant, crossTenant))
	slice.assertLifecycleAudit(target, "session.deletion.request", 1)
	slice.assertLifecycleAudit(target, "session.deletion.start", 1)
	slice.assertLifecycleAudit(target, "session.deletion.complete", 1)
	slice.assertCount(3,
		`SELECT COUNT(*) FROM audit_events
		 WHERE tenant_id = $1 AND subject_kind = $2 AND subject_id = $3`,
		target.TenantID, "session", target.SessionID)

	idempotent, err := service.Execute(
		slice.ctx, target.TenantID, target.SessionID, plan.Confirmation, requestedAt.Add(3*time.Second),
	)
	if err != nil || !reflect.DeepEqual(idempotent, completed) {
		t.Fatalf("idempotent completed execute = %+v err=%v, want %+v", idempotent, err, completed)
	}
	slice.assertLifecycleAudit(target, "session.deletion.request", 1)
	slice.assertLifecycleAudit(target, "session.deletion.start", 1)
	slice.assertLifecycleAudit(target, "session.deletion.complete", 1)
}

func (slice *localSlice) completeRetentionRun(updateID, chatID int64, text string) runRef {
	slice.t.Helper()
	run := slice.postMessage(updateID, chatID, text)
	slice.setConnectionReady(run)
	slice.waitRunStatus(run, domain.RunQueued)
	slice.runWorker(nil)
	slice.waitRunStatus(run, domain.RunSucceeded)
	return run
}

type retainedSession struct {
	tenantID  domain.TenantID
	sessionID domain.SessionID
	history   []byte
	artifacts map[string][]byte
}

func (slice *localSlice) captureRetainedSession(ref runRef, artifactRuns ...runRef) retainedSession {
	slice.t.Helper()
	events, err := slice.state.ListSessionHistory(slice.ctx, ref.TenantID, ref.SessionID, 0, 200)
	if err != nil {
		slice.t.Fatal(err)
	}
	result := retainedSession{
		tenantID: ref.TenantID, sessionID: ref.SessionID,
		artifacts: make(map[string][]byte),
	}
	for _, event := range events {
		line, err := sessioncontext.EncodeRecord(event, slice.readBlob(event.Payload))
		if err != nil {
			slice.t.Fatal(err)
		}
		result.history = append(result.history, line...)
	}
	for _, run := range artifactRuns {
		manifest := slice.outputManifest(run)
		for _, artifact := range manifest.Artifacts {
			result.artifacts[artifact.Blob.Key] = append([]byte(nil), slice.readBlob(artifact.Blob)...)
		}
	}
	if len(result.history) == 0 || len(result.artifacts) == 0 {
		slice.t.Fatalf("retention fixture has no canonical content: %+v", result)
	}
	return result
}

func (slice *localSlice) assertRetainedSession(want, got retainedSession) {
	slice.t.Helper()
	if want.tenantID != got.tenantID || want.sessionID != got.sessionID ||
		!bytes.Equal(want.history, got.history) || !reflect.DeepEqual(want.artifacts, got.artifacts) {
		slice.t.Fatalf("retained session changed: before=%+v after=%+v", want, got)
	}
}

func (slice *localSlice) deleteRunOperationalTTLRows(run runRef) {
	slice.t.Helper()
	statements := []string{
		`DELETE FROM telegram_updates WHERE tenant_id = $1 AND run_id = $2`,
		`DELETE FROM run_idempotency WHERE tenant_id = $1 AND run_id = $2`,
		`DELETE FROM frontend_ingress_idempotency WHERE tenant_id = $1 AND run_id = $2`,
		`DELETE FROM attempts WHERE tenant_id = $1 AND run_id = $2`,
		`DELETE FROM leases WHERE tenant_id = $1 AND run_id = $2`,
		`DELETE FROM checkpoints WHERE tenant_id = $1 AND run_id = $2`,
		`DELETE FROM quota_reservations WHERE tenant_id = $1 AND run_id = $2`,
		`DELETE FROM usage_observations WHERE tenant_id = $1 AND run_id = $2`,
		`DELETE FROM dispatch_outbox WHERE tenant_id = $1 AND run_id = $2`,
		`DELETE FROM telegram_delivery_outbox WHERE tenant_id = $1 AND run_id = $2`,
		`DELETE FROM worker_jobs WHERE tenant_id = $1 AND run_id = $2`,
	}
	for _, statement := range statements {
		if _, err := slice.db.ExecContext(slice.ctx, statement, run.TenantID, run.RunID); err != nil {
			slice.t.Fatalf("simulate operational TTL with %q: %v", statement, err)
		}
	}
}

func (slice *localSlice) assertRunOperationalTTLRowsGone(run runRef) {
	slice.t.Helper()
	for _, table := range []string{
		"telegram_updates",
		"run_idempotency",
		"frontend_ingress_idempotency",
		"attempts",
		"leases",
		"checkpoints",
		"quota_reservations",
		"usage_observations",
		"dispatch_outbox",
		"telegram_delivery_outbox",
		"worker_jobs",
	} {
		if got := slice.countRunRows(run,
			"SELECT COUNT(*) FROM "+table+" WHERE tenant_id = $1 AND run_id = $2"); got != 0 {
			slice.t.Fatalf("TTL simulation retained %d target rows in %s", got, table)
		}
	}
}

func (slice *localSlice) assertTargetCanonicalRowsPresent(run runRef) {
	slice.t.Helper()
	for _, query := range []string{
		`SELECT COUNT(*) FROM sessions WHERE tenant_id = $1 AND session_id = $2`,
		`SELECT COUNT(*) FROM session_displays WHERE tenant_id = $1 AND session_id = $2`,
		`SELECT COUNT(*) FROM session_events WHERE tenant_id = $1 AND session_id = $2`,
		`SELECT COUNT(*) FROM session_snapshots WHERE tenant_id = $1 AND session_id = $2`,
		`SELECT COUNT(*) FROM runs_by_session WHERE tenant_id = $1 AND session_id = $2`,
		`SELECT COUNT(*) FROM runs WHERE tenant_id = $1 AND session_id = $2`,
		`SELECT COUNT(*) FROM artifact_manifests WHERE tenant_id = $1 AND run_id = $2`,
		`SELECT COUNT(*) FROM artifact_manifests_by_run WHERE tenant_id = $1 AND run_id = $2`,
	} {
		var count uint64
		second := any(run.SessionID)
		if bytes.Contains([]byte(query), []byte("run_id = $2")) {
			second = run.RunID
		}
		if err := slice.db.QueryRowContext(slice.ctx, query, run.TenantID, second).Scan(&count); err != nil {
			slice.t.Fatal(err)
		}
		if count == 0 {
			slice.t.Fatalf("canonical rows disappeared during interrupted deletion: %s", query)
		}
	}
}

func (slice *localSlice) assertTargetCanonicalRowsGone(run runRef) {
	slice.t.Helper()
	checks := []struct {
		query  string
		second any
	}{
		{`SELECT COUNT(*) FROM sessions WHERE tenant_id = $1 AND session_id = $2`, run.SessionID},
		{`SELECT COUNT(*) FROM session_displays WHERE tenant_id = $1 AND session_id = $2`, run.SessionID},
		{`SELECT COUNT(*) FROM session_events WHERE tenant_id = $1 AND session_id = $2`, run.SessionID},
		{`SELECT COUNT(*) FROM session_event_idempotency WHERE tenant_id = $1 AND session_id = $2`, run.SessionID},
		{`SELECT COUNT(*) FROM session_snapshots WHERE tenant_id = $1 AND session_id = $2`, run.SessionID},
		{`SELECT COUNT(*) FROM session_participants WHERE tenant_id = $1 AND session_id = $2`, run.SessionID},
		{`SELECT COUNT(*) FROM frontend_bindings_by_session WHERE tenant_id = $1 AND session_id = $2`, run.SessionID},
		{`SELECT COUNT(*) FROM frontend_projections_by_session WHERE tenant_id = $1 AND session_id = $2`, run.SessionID},
		{`SELECT COUNT(*) FROM runs_by_session WHERE tenant_id = $1 AND session_id = $2`, run.SessionID},
		{`SELECT COUNT(*) FROM runs WHERE tenant_id = $1 AND run_id = $2`, run.RunID},
		{`SELECT COUNT(*) FROM artifact_manifests WHERE tenant_id = $1 AND run_id = $2`, run.RunID},
		{`SELECT COUNT(*) FROM artifact_manifests_by_run WHERE tenant_id = $1 AND run_id = $2`, run.RunID},
		{`SELECT COUNT(*) FROM run_finalizations WHERE tenant_id = $1 AND run_id = $2`, run.RunID},
		{`SELECT COUNT(*) FROM frontend_projections_by_run WHERE tenant_id = $1 AND run_id = $2`, run.RunID},
		{`SELECT COUNT(*) FROM telegram_deliveries_by_run WHERE tenant_id = $1 AND run_id = $2`, run.RunID},
		{`SELECT COUNT(*) FROM checkpoint_objects_by_run WHERE tenant_id = $1 AND run_id = $2`, run.RunID},
	}
	for _, check := range checks {
		var count uint64
		if err := slice.db.QueryRowContext(slice.ctx, check.query, run.TenantID, check.second).Scan(&count); err != nil {
			slice.t.Fatal(err)
		}
		if count != 0 {
			slice.t.Fatalf("deleted target retains %d rows for %s", count, check.query)
		}
	}
	deletion, found, err := slice.state.GetSessionDeletion(slice.ctx, run.TenantID, run.SessionID)
	if err != nil || !found || deletion.State != domain.SessionDeletionCompleted {
		slice.t.Fatalf("completed tombstone found=%t deletion=%+v err=%v", found, deletion, err)
	}
}

func (slice *localSlice) assertLifecycleAudit(run runRef, action string, wanted uint64) {
	slice.t.Helper()
	slice.assertCount(wanted,
		`SELECT COUNT(*) FROM audit_events
		 WHERE tenant_id = $1 AND subject_kind = $2 AND subject_id = $3 AND action = $4`,
		run.TenantID, "session", run.SessionID, action)
}

var errInjectedDeleteInterruption = errors.New("injected exact-object deletion interruption")

type failAfterDeleteBlobStore struct {
	delegate ports.BlobStore
	deleted  int
	failed   bool
}

func (store *failAfterDeleteBlobStore) Put(
	ctx context.Context,
	tenantID domain.TenantID,
	key string,
	body io.Reader,
) (domain.BlobRef, error) {
	return store.delegate.Put(ctx, tenantID, key, body)
}

func (store *failAfterDeleteBlobStore) Open(
	ctx context.Context,
	tenantID domain.TenantID,
	ref domain.BlobRef,
) (io.ReadCloser, error) {
	return store.delegate.Open(ctx, tenantID, ref)
}

func (store *failAfterDeleteBlobStore) Delete(
	ctx context.Context,
	tenantID domain.TenantID,
	ref domain.BlobRef,
) error {
	if err := store.delegate.Delete(ctx, tenantID, ref); err != nil {
		return err
	}
	store.deleted++
	if !store.failed {
		store.failed = true
		return errInjectedDeleteInterruption
	}
	return nil
}
