//go:build ydbintegration

package ydbintegration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const canonicalDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestCanonicalSessionCreateBindSwitchAndTenantIsolation(t *testing.T) {
	store, _ := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-session"))
	userID := domain.UserID(uniqueID("user-session"))
	first, firstOwner := canonicalSessionFixture(tenantID, userID, domain.SessionID(uniqueID("session-a")), now)
	if err := store.CreateSession(ctx, first, firstOwner); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateSession(ctx, first, firstOwner); err != nil {
		t.Fatalf("exact create retry failed: %v", err)
	}
	binding := domain.FrontendBinding{
		ID: domain.FrontendBindingID(uniqueID("binding")), TenantID: tenantID,
		Frontend: domain.FrontendTelegram, ExternalConversationID: "chat-1001",
		SessionID: first.ID, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.BindFrontend(ctx, binding); err != nil {
		t.Fatal(err)
	}
	resolved, found, err := store.ResolveFrontendBinding(ctx, tenantID, binding.Frontend, binding.ExternalConversationID)
	if err != nil || !found || resolved != binding {
		t.Fatalf("resolved binding = %#v, found=%t, err=%v", resolved, found, err)
	}

	second, secondOwner := canonicalSessionFixture(tenantID, userID, domain.SessionID(uniqueID("session-b")), now.Add(time.Second))
	switched, err := store.CreateAndSwitchSession(
		ctx, second, secondOwner, binding.ID, binding.Revision, second.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if switched.SessionID != second.ID || switched.Revision != 2 {
		t.Fatalf("switched binding = %#v", switched)
	}
	retried, err := store.CreateAndSwitchSession(
		ctx, second, secondOwner, binding.ID, binding.Revision, second.CreatedAt,
	)
	if err != nil || retried != switched {
		t.Fatalf("exact create-and-switch retry = %#v, err=%v", retried, err)
	}
	resolved, found, err = store.ResolveFrontendBinding(ctx, tenantID, binding.Frontend, binding.ExternalConversationID)
	if err != nil || !found || resolved != switched {
		t.Fatalf("binding after switch = %#v, found=%t, err=%v", resolved, found, err)
	}

	sessions, err := store.ListSessions(ctx, tenantID, userID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 2 || sessions[0].ID != second.ID || sessions[1].ID != first.ID {
		t.Fatalf("recent sessions = %#v", sessions)
	}
	if err := store.ArchiveSession(ctx, tenantID, second.ID, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	sessions, err = store.ListSessions(ctx, tenantID, userID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 2 || sessions[0].Status != domain.SessionArchived {
		t.Fatalf("sessions after archive = %#v", sessions)
	}

	otherTenant := domain.TenantID(uniqueID("tenant-other"))
	if _, found, err := store.GetSession(ctx, otherTenant, first.ID); err != nil || found {
		t.Fatalf("cross-tenant get found=%t err=%v", found, err)
	}
	if _, found, err := store.ResolveFrontendBinding(ctx, otherTenant, binding.Frontend, binding.ExternalConversationID); err != nil || found {
		t.Fatalf("cross-tenant binding found=%t err=%v", found, err)
	}
}

func TestConcurrentCanonicalEventAppendIsGapFreeAndIdempotent(t *testing.T) {
	store, _ := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-events"))
	userID := domain.UserID(uniqueID("user-events"))
	session, owner := canonicalSessionFixture(tenantID, userID, domain.SessionID(uniqueID("session-events")), now)
	if err := store.CreateSession(ctx, session, owner); err != nil {
		t.Fatal(err)
	}

	const contenders = 12
	events := make([]domain.SessionEvent, contenders)
	var wait sync.WaitGroup
	errs := make(chan error, contenders)
	for index := range events {
		eventID := domain.SessionEventID(uniqueID(fmt.Sprintf("event-%02d", index)))
		events[index] = canonicalEventFixture(tenantID, session.ID, userID, eventID, now.Add(time.Second))
		wait.Add(1)
		go func(event domain.SessionEvent) {
			defer wait.Done()
			created, err := store.AppendSessionEvent(ctx, event)
			if err == nil && !created {
				err = fmt.Errorf("new event was not created")
			}
			errs <- err
		}(events[index])
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	history, err := store.ListSessionHistory(ctx, tenantID, session.ID, 0, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != contenders {
		t.Fatalf("history length = %d, want %d", len(history), contenders)
	}
	seen := make(map[domain.SessionEventID]struct{}, contenders)
	for index, event := range history {
		if event.Sequence != uint64(index+1) {
			t.Fatalf("history[%d].sequence = %d", index, event.Sequence)
		}
		seen[event.ID] = struct{}{}
	}
	if len(seen) != contenders {
		t.Fatalf("unique event IDs = %d, want %d", len(seen), contenders)
	}

	created, err := store.AppendSessionEvent(ctx, events[0])
	if err != nil || created {
		t.Fatalf("exact event retry = created=%t err=%v", created, err)
	}
	conflict := events[0]
	conflict.ID = domain.SessionEventID(uniqueID("event-conflict"))
	conflict.Payload.Key = domain.SessionEventObjectPrefix(tenantID, session.ID, conflict.ID) + "payload.json"
	if _, err := store.AppendSessionEvent(ctx, conflict); !errors.Is(err, domain.ErrEventIdempotencyConflict) {
		t.Fatalf("idempotency conflict error = %v", err)
	}
	persisted, found, err := store.GetSession(ctx, tenantID, session.ID)
	if err != nil || !found || persisted.LastEventSequence != contenders {
		t.Fatalf("persisted session = %#v, found=%t err=%v", persisted, found, err)
	}
}

func TestCanonicalSnapshotsAndRunsUseBoundedSessionPrefixes(t *testing.T) {
	store, _ := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-snapshot"))
	userID := domain.UserID(uniqueID("user-snapshot"))
	session, owner := canonicalSessionFixture(tenantID, userID, domain.SessionID(uniqueID("session-snapshot")), now)
	if err := store.CreateSession(ctx, session, owner); err != nil {
		t.Fatal(err)
	}
	event := canonicalEventFixture(tenantID, session.ID, userID, domain.SessionEventID(uniqueID("event-snapshot")), now.Add(time.Second))
	if _, err := store.AppendSessionEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	snapshotID := domain.SessionSnapshotID(uniqueID("snapshot"))
	snapshot := domain.SessionSnapshot{
		ID: snapshotID, TenantID: tenantID, SessionID: session.ID,
		Version: 1, ThroughSequence: 1,
		Payload: domain.BlobRef{
			TenantID: tenantID,
			Key:      domain.SessionSnapshotObjectPrefix(tenantID, session.ID, snapshotID) + "context.json.zst",
			Size:     128, SHA256: canonicalDigest,
		},
		CreatedAt: now.Add(2 * time.Second),
	}
	if err := store.PutSessionSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := store.PutSessionSnapshot(ctx, snapshot); err != nil {
		t.Fatalf("exact snapshot retry failed: %v", err)
	}
	snapshots, err := store.ListSessionSnapshots(ctx, tenantID, session.ID, 0, 10)
	if err != nil || len(snapshots) != 1 || snapshots[0] != snapshot {
		t.Fatalf("snapshots = %#v, err=%v", snapshots, err)
	}

	run := domain.Run{
		ID: domain.RunID(uniqueID("run-session")), TenantID: tenantID,
		SessionID: session.ID, TriggerEventID: event.ID,
		SubscriptionConnectionID: domain.SubscriptionConnectionID(uniqueID("subscription")),
		Status:                   domain.RunCreated, IdempotencyKey: domain.IdempotencyKey(uniqueID("run-key")),
		CreatedAt: now.Add(3 * time.Second), UpdatedAt: now.Add(3 * time.Second),
	}
	if err := store.Transact(ctx, tenantID, func(tx ports.StateTx) error {
		return tx.PutRun(ctx, run)
	}); err != nil {
		t.Fatal(err)
	}
	runs, err := store.ListRunsBySession(ctx, tenantID, session.ID, 10)
	if err != nil || len(runs) != 1 || runs[0].ID != run.ID {
		t.Fatalf("runs by session = %#v, err=%v", runs, err)
	}
}

func canonicalSessionFixture(
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	at time.Time,
) (domain.Session, domain.SessionParticipant) {
	return domain.Session{
			ID: sessionID, TenantID: tenantID, CreatedBy: userID,
			Status: domain.SessionActive, CreatedAt: at, UpdatedAt: at,
		}, domain.SessionParticipant{
			TenantID: tenantID, SessionID: sessionID, UserID: userID,
			Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive,
			CreatedAt: at, UpdatedAt: at,
		}
}

func canonicalEventFixture(
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	userID domain.UserID,
	eventID domain.SessionEventID,
	at time.Time,
) domain.SessionEvent {
	return domain.SessionEvent{
		ID: eventID, TenantID: tenantID, SessionID: sessionID,
		Kind: domain.SessionEventUserMessage, AuthorUserID: &userID,
		IdempotencyKey: domain.IdempotencyKey("append-" + string(eventID)),
		Payload: domain.BlobRef{
			TenantID: tenantID,
			Key:      domain.SessionEventObjectPrefix(tenantID, sessionID, eventID) + "payload.json",
			Size:     64, SHA256: canonicalDigest,
		},
		CreatedAt: at,
	}
}
