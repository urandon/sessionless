//go:build ydbintegration

package ydbintegration

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

func TestSessionLifecycleHoldWriteFenceInventoryAndCompletion(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	runSuffix := fmt.Sprintf("%d", now.UnixNano())
	tenantID := domain.TenantID(uniqueID("tenant-lifecycle-" + runSuffix))
	userID := domain.UserID(uniqueID("user-lifecycle-" + runSuffix))
	seedCanonicalMembership(t, client.DB, tenantID, userID, now)
	session, owner := canonicalSessionFixture(
		tenantID, userID, domain.SessionID(uniqueID("session-lifecycle-"+runSuffix)), now,
	)
	if err := store.CreateSession(ctx, session, owner); err != nil {
		t.Fatal(err)
	}
	binding := domain.FrontendBinding{
		ID: domain.FrontendBindingID(uniqueID("binding-lifecycle")), TenantID: tenantID,
		Frontend: domain.FrontendTelegram, ExternalConversationID: uniqueID("chat-lifecycle"),
		SessionID: session.ID, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.BindFrontend(ctx, binding); err != nil {
		t.Fatal(err)
	}
	event := canonicalEventFixture(
		tenantID, session.ID, userID, domain.SessionEventID(uniqueID("event-lifecycle")), now.Add(time.Second),
	)
	if _, err := store.AppendSessionEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	snapshot := domain.SessionSnapshot{
		ID: domain.SessionSnapshotID(uniqueID("snapshot-lifecycle")), TenantID: tenantID, SessionID: session.ID,
		Version: 1, ThroughSequence: 1, FormatVersion: domain.SessionSnapshotFormatV1,
		Compression: domain.SessionSnapshotCompressionZstandard, EventCount: 1, UncompressedSize: 64,
		Payload: domain.BlobRef{
			TenantID: tenantID, Key: domain.SessionSnapshotObjectKey(tenantID, session.ID, 1),
			Size: 32, SHA256: canonicalDigest,
		},
		CreatedAt: now.Add(2 * time.Second),
	}
	if err := store.PutSessionSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	finishedAt := now.Add(2500 * time.Millisecond)
	run := domain.Run{
		ID: domain.RunID(uniqueID("run-lifecycle")), TenantID: tenantID, SessionID: session.ID,
		TriggerEventID:           event.ID,
		SubscriptionConnectionID: domain.SubscriptionConnectionID(uniqueID("subscription-lifecycle")),
		Status:                   domain.RunSucceeded, IdempotencyKey: domain.IdempotencyKey(uniqueID("run-key-lifecycle")),
		FinishedAt: &finishedAt, CreatedAt: now.Add(2 * time.Second), UpdatedAt: finishedAt,
	}
	deliveryBlob := domain.BlobRef{
		TenantID: tenantID,
		Key:      domain.SessionObjectPrefix(tenantID, session.ID) + "runs/" + string(run.ID) + "/telegram/reply.json",
		Size:     17, SHA256: canonicalDigest,
	}
	artifactBlob := domain.BlobRef{
		TenantID: tenantID,
		Key:      domain.SessionRunObjectPrefix(tenantID, session.ID, run.ID) + "artifacts/sha256/" + canonicalDigest,
		Size:     23, SHA256: canonicalDigest,
	}
	manifest := domain.ArtifactManifest{
		ID: domain.ArtifactManifestID(uniqueID("manifest-lifecycle")), TenantID: tenantID, RunID: run.ID,
		Artifacts: []domain.Artifact{{Name: "result.json", MediaType: "application/json", Blob: artifactBlob}},
		CreatedAt: finishedAt,
	}
	attempt := domain.Attempt{
		ID: domain.AttemptID(uniqueID("attempt-lifecycle")), TenantID: tenantID, RunID: run.ID,
		Number: 1, Status: domain.AttemptSucceeded, CreatedAt: now.Add(2 * time.Second),
		UpdatedAt: finishedAt, FinishedAt: &finishedAt,
	}
	checkpointBlob := domain.BlobRef{
		TenantID: tenantID,
		Key: domain.SessionRunObjectPrefix(tenantID, session.ID, run.ID) +
			"checkpoints/00000000000000000001-" + canonicalDigest + ".json",
		Size: 19, SHA256: canonicalDigest,
	}
	checkpoint := domain.Checkpoint{
		ID: domain.CheckpointID(uniqueID("checkpoint-lifecycle")), TenantID: tenantID,
		RunID: run.ID, AttemptID: attempt.ID, Sequence: 1, State: checkpointBlob, CreatedAt: finishedAt,
	}
	deliveries := []domain.TelegramDeliveryOutbox{
		{
			ID: domain.TelegramDeliveryID(uniqueID("delivery-inline-lifecycle")), TenantID: tenantID, RunID: run.ID,
			Chat: domain.TelegramChatRef{TenantID: tenantID, ChatID: 4411}, ReplyToMessageID: 101,
			Text: "sensitive inline result", Status: domain.DeliveryPending,
			IdempotencyKey: domain.IdempotencyKey(uniqueID("delivery-inline-key-lifecycle")),
			CreatedAt:      finishedAt, UpdatedAt: finishedAt,
		},
		{
			ID: domain.TelegramDeliveryID(uniqueID("delivery-blob-lifecycle")), TenantID: tenantID, RunID: run.ID,
			Chat: domain.TelegramChatRef{TenantID: tenantID, ChatID: 4411}, ReplyToMessageID: 102,
			Payload: deliveryBlob, Status: domain.DeliveryPending,
			IdempotencyKey: domain.IdempotencyKey(uniqueID("delivery-blob-key-lifecycle")),
			CreatedAt:      finishedAt, UpdatedAt: finishedAt,
		},
	}
	if err := store.Transact(ctx, tenantID, func(tx ports.StateTx) error {
		if err := tx.PutRun(ctx, run); err != nil {
			return err
		}
		if err := tx.PutAttempt(ctx, attempt); err != nil {
			return err
		}
		if err := tx.PutArtifactManifest(ctx, manifest); err != nil {
			return err
		}
		if err := tx.PutCheckpoint(ctx, checkpoint); err != nil {
			return err
		}
		for _, delivery := range deliveries {
			if err := tx.PutTelegramDeliveryOutbox(ctx, delivery); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertCount(t, client, "telegram_delivery_outbox", tenantID, 2)
	assertCount(t, client, "telegram_deliveries_by_run", tenantID, 2)
	assertCount(t, client, "checkpoint_objects_by_run", tenantID, 1)
	projectionID := domain.FrontendProjectionID(uniqueID("projection-lifecycle"))
	if _, err := client.DB.ExecContext(ctx,
		`UPSERT INTO frontend_projection_outbox
		 (tenant_id, frontend_projection_id, session_id, event_id, event_sequence,
		  binding_id, binding_revision, frontend, status, created_at, updated_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CAST($12 AS JsonDocument))`,
		tenantID, projectionID, session.ID, event.ID, event.Sequence, binding.ID, binding.Revision,
		binding.Frontend, domain.FrontendProjectionPending, finishedAt, finishedAt, `{}`,
	); err != nil {
		t.Fatal(err)
	}
	// Simulate an upgrade from the pre-index schema. Until the deployment
	// backfill marker exists, bounded fallback reads must preserve behavior.
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`DELETE FROM session_lifecycle_backfill_state WHERE backfill_id = $1`, []any{"session-lifecycle-indexes-v1"}},
		{`DELETE FROM artifact_manifests_by_run WHERE tenant_id = $1 AND run_id = $2`, []any{tenantID, run.ID}},
		{`DELETE FROM frontend_bindings_by_session WHERE tenant_id = $1 AND session_id = $2`, []any{tenantID, session.ID}},
		{`DELETE FROM frontend_projections_by_session WHERE tenant_id = $1 AND session_id = $2`, []any{tenantID, session.ID}},
		{`DELETE FROM telegram_deliveries_by_run WHERE tenant_id = $1 AND run_id = $2`, []any{tenantID, run.ID}},
		{`DELETE FROM checkpoint_objects_by_run WHERE tenant_id = $1 AND run_id = $2`, []any{tenantID, run.ID}},
	} {
		if _, err := client.DB.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	archiveAt := now.Add(3 * time.Second)
	if err := store.ArchiveSession(ctx, tenantID, session.ID, archiveAt); err != nil {
		t.Fatal(err)
	}
	if err := store.ArchiveSession(ctx, tenantID, session.ID, archiveAt.Add(time.Hour)); err != nil {
		t.Fatalf("idempotent archive retry: %v", err)
	}
	archived, found, err := store.GetSession(ctx, tenantID, session.ID)
	if err != nil || !found || archived.ArchivedAt == nil || !archived.ArchivedAt.Equal(archiveAt) {
		t.Fatalf("archived session found=%t session=%+v err=%v", found, archived, err)
	}
	if history, err := store.ListSessionHistory(ctx, tenantID, session.ID, 0, 10); err != nil || len(history) != 1 {
		t.Fatalf("archive changed canonical history: len=%d err=%v", len(history), err)
	}
	unarchiveAt := now.Add(4 * time.Second)
	if err := store.UnarchiveSession(ctx, tenantID, session.ID, unarchiveAt); err != nil {
		t.Fatal(err)
	}
	if err := store.UnarchiveSession(ctx, tenantID, session.ID, unarchiveAt.Add(time.Hour)); err != nil {
		t.Fatalf("idempotent unarchive retry: %v", err)
	}

	_, err = store.PutSessionLegalHold(ctx, domain.SessionLegalHold{
		TenantID: tenantID, SessionID: session.ID, State: domain.SessionLegalHoldActive,
		Reason: "retention investigation", SetBy: userID, SetAt: now.Add(5 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	request := domain.SessionDeletion{
		TenantID: tenantID, SessionID: session.ID, RequestedBy: userID, Reason: "owner request",
		State: domain.SessionDeletionRequested, RequestedAt: now.Add(8 * time.Second),
	}
	if _, err := store.RequestSessionDeletion(ctx, request); err == nil {
		t.Fatal("active legal hold allowed destructive deletion")
	}
	if _, err := store.ReleaseSessionLegalHold(ctx, tenantID, session.ID, userID, now.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RequestSessionDeletion(ctx, request); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendSessionEvent(ctx, event); err == nil {
		t.Fatal("deletion request did not fence canonical writes")
	}

	inventory, err := store.BuildSessionDeletionInventory(ctx, tenantID, session.ID, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if inventory.EventRows != 1 || inventory.SnapshotRows != 1 || inventory.RunRows != 1 ||
		inventory.ManifestRows != 1 || inventory.DeliveryRows != 2 || inventory.CheckpointRows != 1 ||
		inventory.ParticipantRows != 1 || inventory.BindingRows != 1 || inventory.ProjectionRows != 1 ||
		len(inventory.Objects) != 5 || inventory.TotalBytes != uint64(
		event.Payload.Size+snapshot.Payload.Size+deliveryBlob.Size+artifactBlob.Size+checkpointBlob.Size) {
		t.Fatalf("unexpected deletion inventory: %+v", inventory)
	}
	if _, err := ydbpartition.BackfillSchemaIndexes(ctx, client.DB, false); err != nil {
		t.Fatal(err)
	}
	backfilledInventory, err := store.BuildSessionDeletionInventory(ctx, tenantID, session.ID, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(backfilledInventory, inventory) {
		t.Fatalf("backfilled inventory changed: before=%+v after=%+v", inventory, backfilledInventory)
	}
	if inventory.Objects[0].Key > inventory.Objects[1].Key {
		t.Fatalf("inventory is not deterministic: %+v", inventory.Objects)
	}
	if _, err := store.PutSessionLegalHold(ctx, domain.SessionLegalHold{
		TenantID: tenantID, SessionID: session.ID, State: domain.SessionLegalHoldActive,
		Reason: "preservation before execution", SetBy: userID, SetAt: now.Add(9 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartSessionDeletion(ctx, tenantID, session.ID, now.Add(10*time.Second)); err == nil {
		t.Fatal("active hold did not block destructive cleanup")
	}
	if _, err := store.ReleaseSessionLegalHold(ctx, tenantID, session.ID, userID, now.Add(11*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartSessionDeletion(ctx, tenantID, session.ID, now.Add(12*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutSessionLegalHold(ctx, domain.SessionLegalHold{
		TenantID: tenantID, SessionID: session.ID, State: domain.SessionLegalHoldActive,
		Reason: "too late", SetBy: userID, SetAt: now.Add(13 * time.Second),
	}); err == nil {
		t.Fatal("legal hold was accepted after destructive cleanup started")
	}
	if _, err := store.CompleteSessionDeletion(
		ctx, tenantID, session.ID, now.Add(13*time.Second), uint64(len(inventory.Objects))+1, inventory.TotalBytes,
	); err == nil {
		t.Fatal("completion accepted counts that do not match the durable inventory")
	}
	completed, err := store.CompleteSessionDeletion(
		ctx, tenantID, session.ID, now.Add(14*time.Second), uint64(len(inventory.Objects)), inventory.TotalBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != domain.SessionDeletionCompleted {
		t.Fatalf("deletion = %+v", completed)
	}
	if _, found, err := store.GetSession(ctx, tenantID, session.ID); err != nil || found {
		t.Fatalf("deleted session found=%t err=%v", found, err)
	}
	if _, found, err := store.ResolveFrontendBinding(ctx, tenantID, binding.Frontend, binding.ExternalConversationID); err != nil || found {
		t.Fatalf("deleted binding found=%t err=%v", found, err)
	}
	if err := store.CreateSession(ctx, session, owner); err == nil {
		t.Fatal("durable deletion tombstone allowed session ID reuse")
	}
	storedHold, found, err := store.GetSessionLegalHold(ctx, tenantID, session.ID)
	if err != nil || !found || storedHold.State != domain.SessionLegalHoldReleased {
		t.Fatalf("legal-hold tombstone found=%t hold=%+v err=%v", found, storedHold, err)
	}
	assertCount(t, client, "session_deletions", tenantID, 1)
	assertCount(t, client, "session_events", tenantID, 0)
	assertCount(t, client, "session_snapshots", tenantID, 0)
	assertCount(t, client, "runs", tenantID, 0)
	assertCount(t, client, "telegram_delivery_outbox", tenantID, 0)
	assertCount(t, client, "telegram_deliveries_by_run", tenantID, 0)
	assertCount(t, client, "telegram_delivery_ready", tenantID, 0)
	assertCount(t, client, "telegram_delivery_ready_v2", tenantID, 0)
	assertCount(t, client, "checkpoint_objects_by_run", tenantID, 0)
	assertCount(t, client, "frontend_projection_outbox", tenantID, 0)
}
