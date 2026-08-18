//go:build ydbintegration

package ydbintegration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type telegramProjectionFixture struct {
	tenant     domain.TenantID
	user       domain.UserID
	session    domain.Session
	binding    domain.FrontendBinding
	trigger    domain.SessionEvent
	event      domain.SessionEvent
	run        domain.Run
	manifest   domain.ArtifactManifest
	projection domain.FrontendProjection
}

func TestTelegramProjectionMaterializationPreservesCanonicalReferencesAndRechecksAuthorization(t *testing.T) {
	store, client := openStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	fixture := seedTelegramProjection(t, store, client, now, -1007001)

	byRun, err := store.ListRunTelegramProjections(
		context.Background(), fixture.tenant, fixture.run.ID, 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertProjectionCandidate(t, byRun, fixture)
	bucket, err := ydbpartition.BucketV1(string(fixture.projection.ID))
	if err != nil {
		t.Fatal(err)
	}
	ready, err := store.ListReadyTelegramProjections(
		context.Background(), bucket, now.Add(time.Minute), 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertProjectionCandidate(t, ready, fixture)

	prepared, err := store.MaterializeTelegramProjection(
		context.Background(), fixture.tenant, fixture.projection.ID, nil, now.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Outcome != ports.TelegramProjectionNeedsContent ||
		prepared.EventPayload != fixture.event.Payload || prepared.TriggerPayload != fixture.trigger.Payload ||
		prepared.EventKind != domain.SessionEventAssistantMessage {
		t.Fatalf("prepared projection = %#v", prepared)
	}
	manifestID := fixture.manifest.ID
	wrongPayload := fixture.event.Payload
	wrongPayload.SHA256 = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if _, err := store.MaterializeTelegramProjection(
		context.Background(), fixture.tenant, fixture.projection.ID,
		&ports.TelegramProjectionContent{
			EventPayload: wrongPayload, TriggerPayload: fixture.trigger.Payload,
			ArtifactManifestID: &manifestID, TriggerChatID: -1007001, ReplyToMessageID: 91,
		}, now.Add(2*time.Second),
	); err == nil {
		t.Fatal("projection accepted a payload digest that differs from the canonical event")
	}
	assertCount(t, client, "frontend_projection_outbox", fixture.tenant, 1)
	materialized, err := store.MaterializeTelegramProjection(
		context.Background(), fixture.tenant, fixture.projection.ID,
		&ports.TelegramProjectionContent{
			EventPayload: fixture.event.Payload, TriggerPayload: fixture.trigger.Payload,
			ArtifactManifestID: &manifestID, TriggerChatID: -1007001, ReplyToMessageID: 91,
		}, now.Add(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if materialized.Outcome != ports.TelegramProjectionMaterialized || !materialized.Created {
		t.Fatalf("materialized projection = %#v", materialized)
	}
	delivery, found, err := store.GetTelegramDelivery(
		context.Background(), fixture.tenant, materialized.DeliveryID,
	)
	if err != nil || !found {
		t.Fatalf("delivery found=%t err=%v", found, err)
	}
	if delivery.Payload != fixture.event.Payload || delivery.Text != "" ||
		delivery.Projection == nil || delivery.Projection.EventID != fixture.event.ID ||
		delivery.Chat.ChatID != -1007001 || delivery.ReplyToMessageID != 91 {
		t.Fatalf("delivery = %#v", delivery)
	}
	assertCount(t, client, "frontend_projection_outbox", fixture.tenant, 0)
	assertCount(t, client, "frontend_projections_by_run", fixture.tenant, 0)
	assertCount(t, client, "frontend_projection_ready_v1", fixture.tenant, 0)
	assertCount(t, client, "session_events", fixture.tenant, 2)

	suspendMembership(t, client.DB, fixture, now.Add(4*time.Second))
	claimed, ok, err := store.ClaimTelegramDelivery(
		context.Background(), fixture.tenant, delivery.ID, now.Add(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if ok || claimed.Status != domain.DeliveryCancelled {
		t.Fatalf("revoked delivery claim = %#v claimed=%t", claimed, ok)
	}
	assertCount(t, client, "session_events", fixture.tenant, 2)
}

func TestTelegramProjectionStaleBindingIsTerminalWithoutCreatingDelivery(t *testing.T) {
	store, client := openStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	fixture := seedTelegramProjection(t, store, client, now, 7002)

	otherSessionID := domain.SessionID(uniqueID("projection-other-session"))
	otherSession, owner := canonicalSessionFixture(fixture.tenant, fixture.user, otherSessionID, now.Add(time.Second))
	if err := store.CreateSession(context.Background(), otherSession, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SwitchFrontendBinding(
		context.Background(), fixture.tenant, fixture.binding.ID, fixture.binding.Revision,
		otherSessionID, now.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	result, err := store.MaterializeTelegramProjection(
		context.Background(), fixture.tenant, fixture.projection.ID, nil, now.Add(3*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != ports.TelegramProjectionTerminal || result.Code != "binding_stale" {
		t.Fatalf("stale projection = %#v", result)
	}
	assertCount(t, client, "frontend_projection_outbox", fixture.tenant, 0)
	assertCount(t, client, "telegram_delivery_outbox", fixture.tenant, 0)
	assertCount(t, client, "session_events", fixture.tenant, 2)
}

func seedTelegramProjection(
	t *testing.T,
	store *ydbstore.Store,
	client *ydbclient.Client,
	now time.Time,
	chatID int64,
) telegramProjectionFixture {
	t.Helper()
	ctx := context.Background()
	suffix := fmt.Sprintf("%d", now.UnixNano())
	fixture := telegramProjectionFixture{
		tenant: domain.TenantID(uniqueID("tenant-telegram-projection-" + suffix)),
		user:   domain.UserID(uniqueID("user-telegram-projection-" + suffix)),
	}
	seedCanonicalMembership(t, client.DB, fixture.tenant, fixture.user, now)
	fixture.session, _ = canonicalSessionFixture(
		fixture.tenant, fixture.user, domain.SessionID(uniqueID("session-telegram-projection")), now,
	)
	owner := domain.SessionParticipant{
		TenantID: fixture.tenant, SessionID: fixture.session.ID, UserID: fixture.user,
		Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := store.CreateSession(ctx, fixture.session, owner); err != nil {
		t.Fatal(err)
	}
	fixture.binding = domain.FrontendBinding{
		ID:       domain.FrontendBindingID(uniqueID("binding-telegram-projection")),
		TenantID: fixture.tenant, Frontend: domain.FrontendTelegram,
		ExternalConversationID: domain.TelegramChatRef{TenantID: fixture.tenant, ChatID: chatID}.Conversation("conversation-projection").ExternalID,
		SessionID:              fixture.session.ID, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.BindFrontend(ctx, fixture.binding); err != nil {
		t.Fatal(err)
	}
	triggerID := domain.SessionEventID(uniqueID("trigger-telegram-projection"))
	fixture.trigger = canonicalEventFixture(fixture.tenant, fixture.session.ID, fixture.user, triggerID, now)
	fixture.trigger.Payload.Size = 91
	if _, err := store.AppendSessionEvent(ctx, fixture.trigger); err != nil {
		t.Fatal(err)
	}
	finishedAt := now.Add(time.Second)
	fixture.run = domain.Run{
		ID: domain.RunID(uniqueID("run-telegram-projection")), TenantID: fixture.tenant,
		SessionID: fixture.session.ID, TriggerEventID: triggerID,
		SubscriptionConnectionID: domain.SubscriptionConnectionID(uniqueID("subscription-telegram-projection")),
		Status:                   domain.RunSucceeded, IdempotencyKey: domain.IdempotencyKey(uniqueID("run-key-telegram-projection")),
		CreatedAt: now, UpdatedAt: finishedAt, FinishedAt: &finishedAt,
	}
	fixture.manifest = domain.ArtifactManifest{
		ID:       domain.ArtifactManifestID(uniqueID("manifest-telegram-projection")),
		TenantID: fixture.tenant, RunID: fixture.run.ID, CreatedAt: finishedAt,
	}
	if err := store.Transact(ctx, fixture.tenant, func(tx ports.StateTx) error {
		if err := tx.PutRun(ctx, fixture.run); err != nil {
			return err
		}
		return tx.PutArtifactManifest(ctx, fixture.manifest)
	}); err != nil {
		t.Fatal(err)
	}
	eventID := domain.SessionEventID(uniqueID("assistant-telegram-projection"))
	runID := fixture.run.ID
	fixture.event = domain.SessionEvent{
		ID: eventID, TenantID: fixture.tenant, SessionID: fixture.session.ID,
		Kind: domain.SessionEventAssistantMessage, RunID: &runID,
		IdempotencyKey: domain.IdempotencyKey(uniqueID("assistant-key-telegram-projection")),
		Payload: domain.BlobRef{
			TenantID: fixture.tenant,
			Key:      domain.SessionEventObjectPrefix(fixture.tenant, fixture.session.ID, eventID) + "payload.json",
			Size:     128, SHA256: canonicalDigest,
		},
		CreatedAt: finishedAt,
	}
	if _, err := store.AppendSessionEvent(ctx, fixture.event); err != nil {
		t.Fatal(err)
	}
	fixture.event.Sequence = 2
	fixture.projection = domain.FrontendProjection{
		ID:       domain.FrontendProjectionID(uniqueID("projection-telegram")),
		TenantID: fixture.tenant, SessionID: fixture.session.ID,
		EventID: fixture.event.ID, EventSequence: fixture.event.Sequence, EventKind: fixture.event.Kind,
		BindingID: fixture.binding.ID, BindingRevision: fixture.binding.Revision,
		Frontend: domain.FrontendTelegram, Status: domain.FrontendProjectionPending,
		IdempotencyKey: domain.IdempotencyKey(uniqueID("projection-key-telegram")),
		CreatedAt:      finishedAt, UpdatedAt: finishedAt,
	}
	insertTelegramProjectionFixture(t, client.DB, fixture)
	return fixture
}

func insertTelegramProjectionFixture(t *testing.T, db *sql.DB, fixture telegramProjectionFixture) {
	t.Helper()
	record, err := json.Marshal(fixture.projection)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := ydbpartition.BucketV1(string(fixture.projection.ID))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO frontend_projection_outbox
		  (tenant_id, frontend_projection_id, session_id, event_id, event_sequence,
		   binding_id, binding_revision, frontend, status, created_at, updated_at, record)
		  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CAST($12 AS JsonDocument))`, []any{
			fixture.tenant, fixture.projection.ID, fixture.session.ID, fixture.event.ID,
			fixture.event.Sequence, fixture.binding.ID, fixture.binding.Revision,
			fixture.projection.Frontend, fixture.projection.Status,
			fixture.projection.CreatedAt, fixture.projection.UpdatedAt, string(record),
		}},
		{`INSERT INTO frontend_projections_by_session
		  (tenant_id, session_id, frontend_projection_id) VALUES ($1, $2, $3)`, []any{
			fixture.tenant, fixture.session.ID, fixture.projection.ID,
		}},
		{`INSERT INTO frontend_projections_by_run
		  (tenant_id, run_id, frontend, frontend_projection_id) VALUES ($1, $2, $3, $4)`, []any{
			fixture.tenant, fixture.run.ID, fixture.projection.Frontend, fixture.projection.ID,
		}},
		{`INSERT INTO frontend_projection_ready_v1
		  (frontend, shard_bucket, created_at, tenant_id, frontend_projection_id, run_id)
		  VALUES ($1, $2, $3, $4, $5, $6)`, []any{
			fixture.projection.Frontend, bucket, fixture.projection.CreatedAt,
			fixture.tenant, fixture.projection.ID, fixture.run.ID,
		}},
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

func suspendMembership(t *testing.T, db *sql.DB, fixture telegramProjectionFixture, at time.Time) {
	t.Helper()
	membership := domain.TenantMembership{
		TenantID: fixture.tenant, UserID: fixture.user, Role: domain.TenantMembershipOwner,
		Status: domain.TenantMembershipSuspended, SecurityVersion: 2,
		CreatedAt: fixture.session.CreatedAt, UpdatedAt: at,
	}
	record, err := json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE tenant_memberships
		 SET status = $1, security_version = $2, updated_at = $3, record = CAST($4 AS JsonDocument)
		 WHERE user_id = $5 AND tenant_id = $6`,
		membership.Status, membership.SecurityVersion, membership.UpdatedAt, string(record),
		fixture.user, fixture.tenant,
	); err != nil {
		t.Fatal(err)
	}
}

func assertProjectionCandidate(
	t *testing.T,
	candidates []ports.TelegramProjectionReady,
	fixture telegramProjectionFixture,
) {
	t.Helper()
	if len(candidates) != 1 || candidates[0].TenantID != fixture.tenant ||
		candidates[0].ProjectionID != fixture.projection.ID || candidates[0].RunID != fixture.run.ID {
		t.Fatalf("projection candidates = %#v", candidates)
	}
}
