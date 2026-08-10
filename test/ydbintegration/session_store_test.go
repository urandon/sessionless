//go:build ydbintegration

package ydbintegration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
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
	conflictingOwner := firstOwner
	conflictingOwner.UpdatedAt = conflictingOwner.UpdatedAt.Add(time.Second)
	if err := store.CreateSession(ctx, first, conflictingOwner); !errors.Is(err, ydbstore.ErrSessionConflict) {
		t.Fatalf("create retry with conflicting owner error = %v, want %v", err, ydbstore.ErrSessionConflict)
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

	archived, archivedOwner := canonicalSessionFixture(
		tenantID, userID, domain.SessionID(uniqueID("session-archived")), now.Add(3*time.Second),
	)
	if err := store.CreateSession(ctx, archived, archivedOwner); err != nil {
		t.Fatal(err)
	}
	if err := store.ArchiveSession(ctx, tenantID, archived.ID, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	archived, found, err = store.GetSession(ctx, tenantID, archived.ID)
	if err != nil || !found {
		t.Fatalf("archived session found=%t err=%v", found, err)
	}
	if _, err := store.CreateAndSwitchSession(
		ctx, archived, archivedOwner, binding.ID, switched.Revision, now.Add(5*time.Second),
	); err == nil {
		t.Fatal("create-and-switch accepted an archived session")
	}
	resolved, found, err = store.ResolveFrontendBinding(ctx, tenantID, binding.Frontend, binding.ExternalConversationID)
	if err != nil || !found || resolved != switched {
		t.Fatalf("binding changed after rejected archived switch = %#v, found=%t, err=%v", resolved, found, err)
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

func TestFrontendNeutralCanonicalIngressIsAtomicAndTenantScoped(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-ingress"))
	userID := domain.UserID(uniqueID("user-ingress"))
	seedCanonicalMembership(t, client.DB, tenantID, userID, now)

	frontend := domain.Frontend("synthetic")
	bindingID := domain.FrontendBindingID(uniqueID("binding-ingress"))
	initialSessionID := domain.SessionID(uniqueID("session-ingress-initial"))
	state, err := store.EnsureFrontendSession(ctx, ports.FrontendSessionRequest{
		TenantID: tenantID, UserID: userID, Frontend: frontend,
		ExternalConversationID: "synthetic-conversation", BindingID: bindingID,
		SessionID: initialSessionID, At: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstSession := state.Session
	secondID := domain.SessionID(uniqueID("session-ingress-second"))
	state, err = store.CreateAndSwitchFrontendSession(ctx, ports.CanonicalSessionSwitchRequest{
		TenantID: tenantID, UserID: userID, BindingID: bindingID,
		ExpectedRevision: state.Binding.Revision, SessionID: secondID, At: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	thirdID := domain.SessionID(uniqueID("session-ingress-third"))
	state, err = store.CreateAndSwitchFrontendSession(ctx, ports.CanonicalSessionSwitchRequest{
		TenantID: tenantID, UserID: userID, BindingID: bindingID,
		ExpectedRevision: state.Binding.Revision, SessionID: thirdID, At: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.GetSession(ctx, tenantID, firstSession.ID); err != nil || !found {
		t.Fatalf("previous session found=%t err=%v", found, err)
	}
	if _, err := store.CreateAndSwitchFrontendSession(ctx, ports.CanonicalSessionSwitchRequest{
		TenantID: tenantID, UserID: userID, BindingID: bindingID,
		ExpectedRevision: 1, SessionID: domain.SessionID(uniqueID("stale-session")), At: now.Add(3 * time.Second),
	}); err == nil {
		t.Fatal("stale binding switch succeeded")
	}

	committedAt := now.Add(3 * time.Second)
	eventID := domain.SessionEventID(uniqueID("event-ingress"))
	runID := domain.RunID(uniqueID("run-ingress"))
	payload := domain.BlobRef{
		TenantID: tenantID,
		Key:      domain.SessionEventObjectPrefix(tenantID, state.Session.ID, eventID) + "message.json",
		Size:     32, SHA256: canonicalDigest,
	}
	request := ports.CanonicalUserEventCommit{
		TenantID: tenantID, UserID: userID, BindingID: bindingID,
		ExpectedBindingRevision: state.Binding.Revision,
		Origin: domain.FrontendEventOrigin{
			BindingID: bindingID, BindingRevision: state.Binding.Revision,
			Frontend: frontend, ExternalConversationID: "synthetic-conversation",
			ExternalEventID: "delivery-1",
		},
		IdempotencyKey: domain.IdempotencyKey(uniqueID("ingress-key")),
		ExpireAt:       committedAt.Add(24 * time.Hour), EventID: eventID, Payload: payload,
		RunID: runID, AttemptID: domain.AttemptID(uniqueID("attempt-ingress")),
		SubscriptionConnectionID: domain.SubscriptionConnectionID(uniqueID("subscription-ingress")),
		ManifestID:               domain.ArtifactManifestID(uniqueID("manifest-ingress")),
		Artifacts:                []domain.Artifact{{Name: "message.json", MediaType: "application/json", Blob: payload}},
		DispatchID:               domain.DispatchOutboxID(uniqueID("dispatch-ingress")),
		CommittedAt:              committedAt,
	}
	start := make(chan struct{})
	results := make(chan ports.CanonicalUserEventResult, 2)
	errorsByCommit := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			result, err := store.CommitCanonicalUserEvent(ctx, request)
			results <- result
			errorsByCommit <- err
		}()
	}
	close(start)
	var created ports.CanonicalUserEventResult
	createdCount := 0
	for range 2 {
		result, err := <-results, <-errorsByCommit
		if err != nil {
			t.Fatal(err)
		}
		if result.Created {
			created, createdCount = result, createdCount+1
		}
	}
	if createdCount != 1 || created.SessionID != thirdID || created.Sequence != 1 {
		t.Fatalf("created count=%d result=%#v", createdCount, created)
	}
	fourthID := domain.SessionID(uniqueID("session-ingress-fourth"))
	state, err = store.CreateAndSwitchFrontendSession(ctx, ports.CanonicalSessionSwitchRequest{
		TenantID: tenantID, UserID: userID, BindingID: bindingID,
		ExpectedRevision: state.Binding.Revision, SessionID: fourthID, At: committedAt.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.CommitCanonicalUserEvent(ctx, request)
	if err != nil || duplicate.Created || duplicate.RunID != created.RunID {
		t.Fatalf("duplicate=%#v err=%v", duplicate, err)
	}
	if duplicate.SessionID != thirdID || state.Session.ID != fourthID {
		t.Fatalf("duplicate session=%s current session=%s", duplicate.SessionID, state.Session.ID)
	}
	for _, table := range []string{
		"frontend_ingress_idempotency", "session_events", "runs", "attempts", "artifact_manifests", "dispatch_outbox",
	} {
		assertCount(t, client, table, tenantID, 1)
	}

	bad := request
	bad.IdempotencyKey = domain.IdempotencyKey(uniqueID("ingress-bad"))
	bad.EventID = domain.SessionEventID(uniqueID("event-bad"))
	bad.RunID = domain.RunID(uniqueID("run-bad"))
	bad.AttemptID = domain.AttemptID(uniqueID("attempt-bad"))
	bad.ManifestID = domain.ArtifactManifestID(uniqueID("manifest-bad"))
	bad.DispatchID = domain.DispatchOutboxID(uniqueID("dispatch-bad"))
	bad.ExpectedBindingRevision = state.Binding.Revision
	bad.Origin.BindingRevision = state.Binding.Revision
	bad.Payload.Key = domain.SessionEventObjectPrefix(tenantID, state.Session.ID, bad.EventID) + "message.json"
	bad.Artifacts = []domain.Artifact{{
		Name: "escape.bin", MediaType: "application/octet-stream",
		Blob: domain.BlobRef{TenantID: tenantID, Key: domain.TenantObjectPrefix(tenantID) + "wrong-prefix/escape.bin", Size: 1, SHA256: canonicalDigest},
	}}
	if _, err := store.CommitCanonicalUserEvent(ctx, bad); err == nil {
		t.Fatal("commit accepted an artifact outside the session/event prefix")
	}
	for _, table := range []string{"session_events", "runs", "attempts", "dispatch_outbox"} {
		assertCount(t, client, table, tenantID, 1)
	}

	otherTenant := domain.TenantID(uniqueID("tenant-ingress-other"))
	otherUser := domain.UserID(uniqueID("user-ingress-other"))
	seedCanonicalMembership(t, client.DB, otherTenant, otherUser, now)
	cross := request
	cross.TenantID, cross.UserID = otherTenant, otherUser
	cross.Payload.TenantID = otherTenant
	cross.Payload.Key = domain.TenantObjectPrefix(otherTenant) + "cross-tenant/message.json"
	cross.Origin.ExternalEventID = "cross-tenant"
	if _, err := store.CommitCanonicalUserEvent(ctx, cross); err == nil {
		t.Fatal("cross-tenant binding access succeeded")
	}
	assertCount(t, client, "frontend_ingress_idempotency", otherTenant, 0)
}

func seedCanonicalMembership(
	t *testing.T,
	db *sql.DB,
	tenantID domain.TenantID,
	userID domain.UserID,
	at time.Time,
) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`UPSERT INTO tenants (tenant_id, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4)`,
		tenantID, "active", at, at,
	); err != nil {
		t.Fatal(err)
	}
	membership := domain.TenantMembership{
		TenantID: tenantID, UserID: userID, Role: domain.TenantMembershipOwner,
		Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: at, UpdatedAt: at,
	}
	payload, err := json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := ydbpartition.BucketV1(string(userID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPSERT INTO tenant_memberships
		 (user_bucket, user_id, tenant_id, role, status, security_version,
		  created_at, updated_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))`,
		bucket, userID, tenantID, membership.Role, membership.Status,
		membership.SecurityVersion, at, at, string(payload),
	); err != nil {
		t.Fatal(err)
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
