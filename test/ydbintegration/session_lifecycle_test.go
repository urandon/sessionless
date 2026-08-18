//go:build ydbintegration

package ydbintegration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
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
	if inventory.EventRows != 1 || inventory.SnapshotRows != 1 || len(inventory.Objects) != 2 ||
		inventory.TotalBytes != uint64(event.Payload.Size+snapshot.Payload.Size) {
		t.Fatalf("unexpected deletion inventory: %+v", inventory)
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
}
