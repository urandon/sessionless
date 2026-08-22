//go:build ydbintegration

package ydbintegration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const canonicalDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestCanonicalIngressReusesAnExistingTelegramBindingIdentity(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenantID := domain.TenantID(uniqueID("tenant-telegram-migration"))
	userID := domain.UserID(uniqueID("user-telegram-migration"))
	seedCanonicalMembership(t, client.DB, tenantID, userID, now)
	externalConversationID := "424242"
	legacyBindingID := domain.FrontendBindingID(uniqueID("legacy-telegram-binding"))
	legacySessionID := domain.SessionID(uniqueID("legacy-telegram-session"))
	legacy, err := store.EnsureFrontendSession(ctx, ports.FrontendSessionRequest{
		TenantID: tenantID, UserID: userID, Frontend: domain.FrontendTelegram,
		ExternalConversationID: externalConversationID, BindingID: legacyBindingID,
		SessionID: legacySessionID, At: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := sessioningress.New(sessioningress.Config{
		IDKey: []byte(strings.Repeat("t", 32)),
	}, store, newSessionAPITestBlobs())
	if err != nil {
		t.Fatal(err)
	}
	actor := sessioningress.Actor{
		TenantID: tenantID, UserID: userID, Frontend: domain.FrontendTelegram,
		ExternalConversationID: externalConversationID,
	}
	result, err := service.Ingest(ctx, sessioningress.UserInput{
		Actor: actor, ExternalEventID: "bot-primary:1001", ReceivedAt: now.Add(time.Second),
		Text: "canonical Telegram message", SubscriptionConnectionID: domain.SubscriptionConnectionID(uniqueID("telegram-subscription")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.SessionID != legacySessionID {
		t.Fatalf("canonical ingress session = %q, want existing %q", result.SessionID, legacySessionID)
	}
	switched, err := service.NewSession(ctx, actor, legacy.Binding.Revision, "bot-primary:1002", now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if switched.Binding.ID != legacyBindingID || switched.Binding.Revision != legacy.Binding.Revision+1 || switched.Session.ID == legacySessionID {
		t.Fatalf("canonical switch = %#v, legacy binding = %#v", switched, legacy.Binding)
	}
	var bindingCount uint64
	if err := client.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM frontend_bindings
		 WHERE tenant_id = $1 AND frontend = $2 AND external_conversation_id = $3`,
		tenantID, domain.FrontendTelegram, externalConversationID,
	).Scan(&bindingCount); err != nil {
		t.Fatal(err)
	}
	if bindingCount != 1 {
		t.Fatalf("Telegram bindings = %d, want one authoritative binding", bindingCount)
	}
}

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
		Version: 1, ThroughSequence: 1, FormatVersion: domain.SessionSnapshotFormatV1,
		Compression: domain.SessionSnapshotCompressionZstandard,
		EventCount:  1, UncompressedSize: 256,
		Payload: domain.BlobRef{
			TenantID: tenantID,
			Key:      domain.SessionSnapshotObjectKey(tenantID, session.ID, 1),
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
	version := uint64(1)
	contextFromSnapshot, err := store.LoadWorkerContext(ctx, ports.WorkerContextRequest{
		TenantID: tenantID, SessionID: session.ID, TriggerEventID: event.ID,
		AtOrBeforeSnapshotVersion: &version, ThroughSequence: 1, MaxEvents: 1,
	})
	if err != nil || contextFromSnapshot.Snapshot == nil || len(contextFromSnapshot.Events) != 0 {
		t.Fatalf("snapshot context = %#v, err=%v", contextFromSnapshot, err)
	}
	contextFromReplay, err := store.LoadWorkerContext(ctx, ports.WorkerContextRequest{
		TenantID: tenantID, SessionID: session.ID, TriggerEventID: event.ID,
		ThroughSequence: 1, MaxEvents: 1,
	})
	if err != nil || contextFromReplay.Snapshot != nil || len(contextFromReplay.Events) != 1 || contextFromReplay.Events[0].ID != event.ID {
		t.Fatalf("replayed context = %#v, err=%v", contextFromReplay, err)
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
		MutationDigest: canonicalDigest,
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
	changed := request
	changed.MutationDigest = strings.Repeat("b", 64)
	if _, err := store.CommitCanonicalUserEvent(ctx, changed); !errors.Is(err, domain.ErrEventIdempotencyConflict) {
		t.Fatalf("changed mutation digest error=%v, want idempotency conflict", err)
	}
	for _, table := range []string{
		"frontend_ingress_idempotency", "session_events", "runs", "attempts", "artifact_manifests", "dispatch_outbox",
	} {
		assertCount(t, client, table, tenantID, 1)
	}
	var dispatchPayload string
	if err := client.DB.QueryRowContext(ctx,
		`SELECT payload FROM dispatch_outbox
		 WHERE tenant_id = $1 AND dispatch_outbox_id = $2`,
		tenantID, request.DispatchID,
	).Scan(&dispatchPayload); err != nil {
		t.Fatal(err)
	}
	var persistedDispatch domain.DispatchOutbox
	if err := json.Unmarshal([]byte(dispatchPayload), &persistedDispatch); err != nil {
		t.Fatal(err)
	}
	if persistedDispatch.CredentialOwnerUserID != userID {
		t.Fatalf("dispatch credential owner round-trip = %q, want %q", persistedDispatch.CredentialOwnerUserID, userID)
	}
	canonicalSnapshotVersion := uint64(1)
	if err := store.PutSessionSnapshot(ctx, domain.SessionSnapshot{
		ID:       domain.SessionSnapshotID(uniqueID("canonical-snapshot")),
		TenantID: tenantID, SessionID: thirdID,
		Version: canonicalSnapshotVersion, ThroughSequence: 1,
		FormatVersion: domain.SessionSnapshotFormatV1,
		Compression:   domain.SessionSnapshotCompressionZstandard,
		EventCount:    1, UncompressedSize: 256,
		Payload: domain.BlobRef{
			TenantID: tenantID,
			Key:      domain.SessionSnapshotObjectKey(tenantID, thirdID, canonicalSnapshotVersion),
			Size:     128, SHA256: canonicalDigest,
		},
		CreatedAt: committedAt,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(ctx,
		`INSERT INTO subscription_connections
		 (tenant_id, subscription_connection_id, actor_id, provider, credential_ref,
		  entitlement_state, quota_state, observed_at, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		tenantID, request.SubscriptionConnectionID, userID, "deterministic", "",
		domain.EntitlementActive, domain.ProviderQuotaUnknown,
		committedAt, committedAt, committedAt,
	); err != nil {
		t.Fatal(err)
	}
	canonicalReservationID := domain.QuotaReservationID(uniqueID("canonical-reservation"))
	admission, err := store.AdmitDispatch(ctx, ports.DispatchAdmissionRequest{
		TenantID: tenantID, OutboxID: request.DispatchID, RunID: request.RunID,
		AttemptID:     request.AttemptID,
		ReservationID: canonicalReservationID,
		Now:           committedAt, HoldUntil: committedAt.Add(time.Minute),
		Limits: domain.ProductLimits{
			MaxTenantQueueDepth: 8, MaxActiveRuns: 1,
			MaxRuntime: 15 * time.Minute, MaxTurns: 30,
			MaxInputBytes: 16 << 20, MaxContextBytes: 64 << 20, MaxContextEvents: 512, MaxArtifacts: 32,
			MaxToolEvents: 128, MaxToolEventBytes: 16 << 20,
		},
		Workload: domain.WorkloadShape{Runtime: time.Minute, Turns: 1},
	})
	if err != nil || !admission.Admitted || admission.Code != "admitted" {
		t.Fatalf("canonical admission=%#v err=%v", admission, err)
	}
	if admission.SessionID != thirdID || admission.ThroughSequence != 1 {
		t.Fatalf("canonical admission context boundary=%#v", admission)
	}
	assertCount(t, client, "worker_jobs", tenantID, 1)
	assertCount(t, client, "quota_reservations", tenantID, 1)
	secondaryBinding := domain.FrontendBinding{
		ID:       domain.FrontendBindingID(uniqueID("binding-secondary")),
		TenantID: tenantID, Frontend: domain.Frontend("synthetic"),
		ExternalConversationID: uniqueID("conversation-secondary"),
		SessionID:              thirdID, Revision: 1,
		CreatedAt: committedAt.Add(2 * time.Second), UpdatedAt: committedAt.Add(2 * time.Second),
	}
	if err := store.BindFrontend(ctx, secondaryBinding); err != nil {
		t.Fatal(err)
	}
	loaded, found, err := store.LoadWorkerJob(ctx, tenantID, request.RunID)
	if err != nil || !found {
		t.Fatalf("canonical worker job found=%t err=%v", found, err)
	}
	if loaded.Job.ContextWindow == nil ||
		loaded.Job.ContextWindow.SnapshotVersion == nil ||
		*loaded.Job.ContextWindow.SnapshotVersion != canonicalSnapshotVersion ||
		loaded.Job.ContextWindow.AfterSequence != 1 ||
		loaded.Job.ContextWindow.ThroughSequence != 1 {
		t.Fatalf("admitted context window = %#v", loaded.Job.ContextWindow)
	}
	if loaded.Job.CredentialOwnerUserID != userID {
		t.Fatalf("credential owner round-trip = %q, want %q", loaded.Job.CredentialOwnerUserID, userID)
	}
	lease, err := store.ClaimWorkerLease(ctx, ports.WorkerLeaseRequest{
		TenantID: tenantID, RunID: request.RunID, AttemptID: request.AttemptID,
		LeaseID: domain.LeaseID(uniqueID("canonical-lease")), WorkerID: "canonical-worker",
		Now: committedAt.Add(3 * time.Second), ExpiresAt: committedAt.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartWorkerJob(ctx, loaded, lease, committedAt.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	invocation, found, err := store.LoadWorkerCredentialInvocation(
		ctx, tenantID, request.RunID, request.AttemptID, lease.ID,
	)
	if err != nil || !found {
		t.Fatalf("credential invocation found=%t err=%v", found, err)
	}
	if invocation.Run.Status != domain.RunRunning ||
		invocation.Attempt.Status != domain.AttemptRunning ||
		invocation.Attempt.WorkerID != lease.WorkerID || invocation.Lease != lease {
		t.Fatalf("authoritative credential invocation = %#v", invocation)
	}
	finishedAt := committedAt.Add(4 * time.Second)
	terminalEvents := []domain.SessionEventDraft{
		canonicalTerminalDraft(tenantID, thirdID, "tool-call", domain.SessionEventToolCall, finishedAt),
		canonicalTerminalDraft(tenantID, thirdID, "tool-result", domain.SessionEventToolResult, finishedAt),
		canonicalTerminalDraft(tenantID, thirdID, "assistant", domain.SessionEventAssistantMessage, finishedAt),
	}
	completion := ports.WorkerCompletion{
		TenantID: tenantID, RunID: request.RunID, AttemptID: request.AttemptID,
		ReservationID: canonicalReservationID, LeaseID: lease.ID, Fence: lease.FenceToken,
		At: finishedAt,
		Manifest: domain.ArtifactManifest{
			ID:       domain.ArtifactManifestID(uniqueID("canonical-output-manifest")),
			TenantID: tenantID, RunID: request.RunID, CreatedAt: finishedAt,
		},
		Events: terminalEvents,
	}
	missingAssistant := completion
	missingAssistant.Events = append([]domain.SessionEventDraft(nil), terminalEvents[:2]...)
	if err := store.CompleteWorkerJob(ctx, missingAssistant); err == nil {
		t.Fatal("canonical success without an assistant event succeeded")
	}
	multipleAssistants := completion
	multipleAssistants.Events = append(
		append([]domain.SessionEventDraft(nil), terminalEvents...),
		canonicalTerminalDraft(
			tenantID, thirdID, "second-assistant", domain.SessionEventAssistantMessage, finishedAt,
		),
	)
	if err := store.CompleteWorkerJob(ctx, multipleAssistants); err == nil {
		t.Fatal("canonical success with multiple assistant events succeeded")
	}
	stale := completion
	stale.Fence++
	if err := store.CompleteWorkerJob(ctx, stale); !errors.Is(err, ydbstore.ErrLeaseLost) {
		t.Fatalf("stale canonical completion error=%v", err)
	}
	assertCount(t, client, "session_events", tenantID, 1)
	assertCount(t, client, "frontend_projection_outbox", tenantID, 0)
	assertCount(t, client, "run_finalizations", tenantID, 0)
	if err := store.CompleteWorkerJob(ctx, completion); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteWorkerJob(ctx, completion); err != nil {
		t.Fatalf("exact canonical completion retry: %v", err)
	}
	assertCount(t, client, "session_events", tenantID, 4)
	assertCount(t, client, "frontend_projection_outbox", tenantID, 1)
	assertCount(t, client, "run_finalizations", tenantID, 1)
	assertCount(t, client, "telegram_delivery_outbox", tenantID, 0)
	manifestConflict := completion
	manifestConflict.Manifest.Artifacts = []domain.Artifact{{
		Name: "changed.json", MediaType: "application/json", Blob: terminalEvents[0].Payload,
	}}
	if err := store.CompleteWorkerJob(ctx, manifestConflict); !errors.Is(err, ydbstore.ErrRunFinalizationConflict) {
		t.Fatalf("conflicting canonical manifest error=%v", err)
	}
	conflict := completion
	conflict.Events = append([]domain.SessionEventDraft(nil), completion.Events...)
	conflict.Events[2].Payload.SHA256 = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if err := store.CompleteWorkerJob(ctx, conflict); !errors.Is(err, ydbstore.ErrRunFinalizationConflict) {
		t.Fatalf("conflicting canonical completion error=%v", err)
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
	assertCount(t, client, "session_events", tenantID, 4)
	for _, table := range []string{"runs", "attempts", "dispatch_outbox"} {
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

	lookup := ports.CanonicalUserEventLookup{
		TenantID: tenantID, UserID: userID, BindingID: bindingID,
		Frontend: frontend, ExternalConversationID: "synthetic-conversation",
		IdempotencyKey: request.IdempotencyKey, MutationDigest: request.MutationDigest, EventID: request.EventID,
		RunID: request.RunID,
	}
	resolved, err := store.LookupCanonicalUserEvent(ctx, lookup)
	if err != nil || !resolved.Found || resolved.Result.SessionID != thirdID {
		t.Fatalf("canonical lookup=%#v err=%v", resolved, err)
	}
	changedLookup := lookup
	changedLookup.MutationDigest = strings.Repeat("c", 64)
	if _, err := store.LookupCanonicalUserEvent(ctx, changedLookup); !errors.Is(err, domain.ErrEventIdempotencyConflict) {
		t.Fatalf("changed lookup digest error=%v, want idempotency conflict", err)
	}
	membershipBucket, err := ydbpartition.BucketV1(string(userID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(ctx,
		`DELETE FROM tenant_memberships
		 WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3`,
		membershipBucket, userID, tenantID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LookupCanonicalUserEvent(ctx, lookup); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("revoked lookup error=%v, want membership denied", err)
	}
	if _, err := store.CommitCanonicalUserEvent(ctx, request); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("revoked duplicate commit error=%v, want membership denied", err)
	}
	seedCanonicalMembership(t, client.DB, tenantID, userID, now)
	removed := domain.SessionParticipant{
		TenantID: tenantID, SessionID: thirdID, UserID: userID,
		Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantRemoved,
		CreatedAt: now.Add(2 * time.Second), UpdatedAt: committedAt.Add(2 * time.Second),
	}
	removedRecord, err := json.Marshal(removed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(ctx,
		`UPDATE session_participants
		 SET status = $1, updated_at = $2, record = CAST($3 AS JsonDocument)
		 WHERE tenant_id = $4 AND session_id = $5 AND user_id = $6`,
		removed.Status, removed.UpdatedAt, string(removedRecord), tenantID, thirdID, userID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LookupCanonicalUserEvent(ctx, lookup); err == nil {
		t.Fatal("removed session participant resolved a duplicate")
	}
	if _, err := store.CommitCanonicalUserEvent(ctx, request); err == nil {
		t.Fatal("removed session participant committed a duplicate")
	}
}

func TestCanonicalFailureAndCancellationFinalizationAreAtomicAndIdempotent(t *testing.T) {
	store, client := openStore(t)
	ctx := context.Background()
	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	for index, testCase := range []struct {
		name          string
		cancelled     bool
		runStatus     domain.RunStatus
		attemptStatus domain.AttemptStatus
	}{
		{name: "failure", runStatus: domain.RunFailed, attemptStatus: domain.AttemptFailed},
		{name: "cancellation", cancelled: true, runStatus: domain.RunCancelled, attemptStatus: domain.AttemptCancelled},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			now := baseTime.Add(time.Duration(index) * time.Hour)
			tenantID := domain.TenantID(uniqueID("tenant-terminal-" + testCase.name))
			userID := domain.UserID(uniqueID("user-terminal-" + testCase.name))
			seedCanonicalMembership(t, client.DB, tenantID, userID, now)
			frontend := domain.Frontend("synthetic")
			bindingID := domain.FrontendBindingID(uniqueID("binding-terminal-" + testCase.name))
			sessionID := domain.SessionID(uniqueID("session-terminal-" + testCase.name))
			frontendState, err := store.EnsureFrontendSession(ctx, ports.FrontendSessionRequest{
				TenantID: tenantID, UserID: userID, Frontend: frontend,
				ExternalConversationID: uniqueID("conversation-terminal-" + testCase.name),
				BindingID:              bindingID, SessionID: sessionID, At: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			committedAt := now.Add(time.Second)
			eventID := domain.SessionEventID(uniqueID("event-user-terminal-" + testCase.name))
			runID := domain.RunID(uniqueID("run-terminal-" + testCase.name))
			payload := domain.BlobRef{
				TenantID: tenantID,
				Key: domain.SessionEventObjectPrefix(
					tenantID, frontendState.Session.ID, eventID,
				) + "message.json",
				Size: 2, SHA256: canonicalDigest,
			}
			request := ports.CanonicalUserEventCommit{
				TenantID: tenantID, UserID: userID, BindingID: bindingID,
				ExpectedBindingRevision: frontendState.Binding.Revision,
				Origin: domain.FrontendEventOrigin{
					BindingID: bindingID, BindingRevision: frontendState.Binding.Revision,
					Frontend: frontend, ExternalConversationID: frontendState.Binding.ExternalConversationID,
					ExternalEventID: uniqueID("external-terminal-" + testCase.name),
				},
				IdempotencyKey: domain.IdempotencyKey(uniqueID("ingress-terminal-key-" + testCase.name)),
				ExpireAt:       committedAt.Add(24 * time.Hour), EventID: eventID, Payload: payload,
				RunID: runID, AttemptID: domain.AttemptID(uniqueID("attempt-terminal-" + testCase.name)),
				SubscriptionConnectionID: domain.SubscriptionConnectionID(uniqueID("subscription-terminal-" + testCase.name)),
				ManifestID:               domain.ArtifactManifestID(uniqueID("manifest-terminal-" + testCase.name)),
				Artifacts:                []domain.Artifact{{Name: "message.json", MediaType: "application/json", Blob: payload}},
				DispatchID:               domain.DispatchOutboxID(uniqueID("dispatch-terminal-" + testCase.name)),
				CommittedAt:              committedAt,
			}
			if _, err := store.CommitCanonicalUserEvent(ctx, request); err != nil {
				t.Fatal(err)
			}
			if _, err := client.DB.ExecContext(ctx,
				`INSERT INTO subscription_connections
				 (tenant_id, subscription_connection_id, actor_id, provider, credential_ref,
				  entitlement_state, quota_state, observed_at, created_at, updated_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
				tenantID, request.SubscriptionConnectionID, userID, "deterministic", "",
				domain.EntitlementActive, domain.ProviderQuotaUnknown,
				committedAt, committedAt, committedAt,
			); err != nil {
				t.Fatal(err)
			}
			reservationID := domain.QuotaReservationID(uniqueID("reservation-terminal-" + testCase.name))
			admission, err := store.AdmitDispatch(ctx, ports.DispatchAdmissionRequest{
				TenantID: tenantID, OutboxID: request.DispatchID, RunID: request.RunID,
				AttemptID: request.AttemptID, ReservationID: reservationID,
				Now: committedAt, HoldUntil: committedAt.Add(10 * time.Minute),
				Limits: domain.ProductLimits{
					MaxTenantQueueDepth: 8, MaxActiveRuns: 1,
					MaxRuntime: time.Minute, MaxTurns: 4,
					MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 64, MaxArtifacts: 4,
					MaxToolEvents: 16, MaxToolEventBytes: 1 << 20,
				},
				Workload: domain.WorkloadShape{Runtime: time.Minute, Turns: 1},
			})
			if err != nil || !admission.Admitted {
				t.Fatalf("admission=%#v err=%v", admission, err)
			}
			loaded, found, err := store.LoadWorkerJob(ctx, tenantID, runID)
			if err != nil || !found {
				t.Fatalf("worker job found=%t err=%v", found, err)
			}
			lease, err := store.ClaimWorkerLease(ctx, ports.WorkerLeaseRequest{
				TenantID: tenantID, RunID: runID, AttemptID: request.AttemptID,
				LeaseID:  domain.LeaseID(uniqueID("lease-terminal-" + testCase.name)),
				WorkerID: "canonical-terminal-worker", Now: committedAt.Add(time.Second),
				ExpiresAt: committedAt.Add(5 * time.Minute),
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := store.StartWorkerJob(ctx, loaded, lease, committedAt.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			failedAt := committedAt.Add(2 * time.Second)
			failure := ports.WorkerFailure{
				TenantID: tenantID, RunID: runID, AttemptID: request.AttemptID,
				ReservationID: reservationID, LeaseID: lease.ID, Fence: lease.FenceToken,
				At: failedAt, Cancelled: testCase.cancelled, Code: testCase.name + "_fixture",
				Events: []domain.SessionEventDraft{
					canonicalTerminalDraft(
						tenantID, sessionID, testCase.name+"-notice",
						domain.SessionEventSystemNotice, failedAt,
					),
				},
			}
			invalidFailure := failure
			invalidFailure.Events = []domain.SessionEventDraft{
				canonicalTerminalDraft(
					tenantID, sessionID, testCase.name+"-tool-only",
					domain.SessionEventToolResult, failedAt,
				),
			}
			if err := store.FailWorkerJob(ctx, invalidFailure); err == nil {
				t.Fatal("canonical failure without exactly one system notice succeeded")
			}
			stale := failure
			stale.Fence++
			if err := store.FailWorkerJob(ctx, stale); !errors.Is(err, ydbstore.ErrLeaseLost) {
				t.Fatalf("stale terminal failure error=%v", err)
			}
			assertCount(t, client, "session_events", tenantID, 1)
			assertCount(t, client, "frontend_projection_outbox", tenantID, 0)
			assertCount(t, client, "run_finalizations", tenantID, 0)
			if err := store.FailWorkerJob(ctx, failure); err != nil {
				t.Fatal(err)
			}
			if err := store.FailWorkerJob(ctx, failure); err != nil {
				t.Fatalf("exact terminal failure retry: %v", err)
			}
			terminal, found, err := store.LoadWorkerJob(ctx, tenantID, runID)
			if err != nil || !found {
				t.Fatalf("terminal worker job found=%t err=%v", found, err)
			}
			if terminal.Run.Status != testCase.runStatus ||
				terminal.Attempt.Status != testCase.attemptStatus ||
				terminal.Reservation.Status != domain.ReservationReleased {
				t.Fatalf("terminal state=%#v", terminal)
			}
			assertCount(t, client, "session_events", tenantID, 2)
			assertCount(t, client, "frontend_projection_outbox", tenantID, 1)
			assertCount(t, client, "run_finalizations", tenantID, 1)
			assertCount(t, client, "telegram_delivery_outbox", tenantID, 0)
			conflict := failure
			conflict.Events = append([]domain.SessionEventDraft(nil), failure.Events...)
			conflict.Events[0].Payload.SHA256 = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
			if err := store.FailWorkerJob(ctx, conflict); !errors.Is(err, ydbstore.ErrRunFinalizationConflict) {
				t.Fatalf("conflicting terminal failure error=%v", err)
			}
			foreignTenant := domain.TenantID(uniqueID("tenant-foreign-terminal-" + testCase.name))
			foreign := failure
			foreign.TenantID = foreignTenant
			if err := store.FailWorkerJob(ctx, foreign); err == nil {
				t.Fatal("cross-tenant terminal failure succeeded")
			}
			for _, table := range []string{
				"session_events", "frontend_projection_outbox", "run_finalizations", "telegram_delivery_outbox",
			} {
				assertCount(t, client, table, foreignTenant, 0)
			}
		})
	}
}

func canonicalTerminalDraft(
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	suffix string,
	kind domain.SessionEventKind,
	at time.Time,
) domain.SessionEventDraft {
	eventID := domain.SessionEventID(uniqueID("event-" + suffix))
	return domain.SessionEventDraft{
		ID: eventID, Kind: kind,
		IdempotencyKey: domain.IdempotencyKey(uniqueID("event-key-" + suffix)),
		Payload: domain.BlobRef{
			TenantID: tenantID,
			Key:      domain.SessionEventObjectPrefix(tenantID, sessionID, eventID) + "payload.json",
			Size:     2, SHA256: canonicalDigest,
		},
		CreatedAt: at,
	}
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
