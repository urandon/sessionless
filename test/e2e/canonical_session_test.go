//go:build e2elocal

package e2e

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"gitcode.com/urandon/sessionless/internal/syntheticfrontend"
	"gitcode.com/urandon/sessionless/internal/telegramingress"
)

type e2eClock struct{ at time.Time }

func (clock *e2eClock) Now() time.Time { return clock.at }

type canonicalRunIdentity struct {
	TriggerEventID   domain.SessionEventID
	AssistantEventID domain.SessionEventID
	ProjectionID     domain.FrontendProjectionID
	DeliveryID       domain.TelegramDeliveryID
}

func TestCanonicalSessionCrossFrontendLifecycle(t *testing.T) {
	if os.Getenv("SESSIONLESS_E2E") != "1" {
		t.Skip("set SESSIONLESS_E2E=1 and start the local stand")
	}
	slice := newLocalSlice(t)
	defer slice.close()
	slice.reset()

	base := time.Now().UTC().UnixMilli()
	telegramChat := base*2 + 101
	otherChat := base*2 + 102

	initial := slice.postMessage(base+101, telegramChat, "canonical history fixture")
	initialRun := slice.persistedRun(initial)
	duplicate := slice.postMessage(base+101, telegramChat, "canonical history duplicate")
	duplicateRun := slice.persistedRun(duplicate)
	if duplicate.RunID != initial.RunID || duplicate.SessionID != initial.SessionID ||
		duplicateRun.TriggerEventID != initialRun.TriggerEventID {
		t.Fatalf("duplicate Telegram update changed canonical identity: first=%+v/%+v duplicate=%+v/%+v", initial, initialRun, duplicate, duplicateRun)
	}
	slice.assertCanonicalIngressCardinality(initial, 1, 1, 1, 1)

	slice.setConnectionReady(initial)
	slice.waitRunStatus(initial, domain.RunQueued)
	slice.injectTelegramFailure("sendMessage", 1, 429)
	slice.runWorker(nil)
	slice.waitRunStatus(initial, domain.RunSucceeded)
	manifest := slice.outputManifest(initial)
	if len(manifest.Artifacts) == 0 {
		t.Fatal("canonical terminal manifest contains no artifacts")
	}
	slice.waitForChatMethods(map[int64]map[string]int{
		telegramChat: {"sendMessage": 1, "sendDocument": len(manifest.Artifacts)},
	})
	slice.assertDeliveryWasRetried(initial)
	slice.assertCanonicalProjectionDelivered(initial)
	terminalIdentity := slice.canonicalTerminalIdentity(initial)
	slice.assertCanonicalTerminalCardinality(initial)

	slice.publishDuplicate(initial)
	slice.runWorker(nil)
	if replayed := slice.canonicalTerminalIdentity(initial); replayed != terminalIdentity {
		t.Fatalf("terminal replay changed canonical identity: before=%+v after=%+v", terminalIdentity, replayed)
	}
	slice.assertCanonicalTerminalCardinality(initial)

	firstNew := slice.postMessage(base+102, telegramChat, "/new")
	slice.waitRunStatus(firstNew, domain.RunSucceeded)
	secondNew := slice.postMessage(base+103, telegramChat, "/new")
	slice.waitRunStatus(secondNew, domain.RunSucceeded)
	if initial.SessionID == firstNew.SessionID || initial.SessionID == secondNew.SessionID ||
		firstNew.SessionID == secondNew.SessionID {
		t.Fatalf("two /new commands did not create distinct sessions: initial=%s first=%s second=%s", initial.SessionID, firstNew.SessionID, secondNew.SessionID)
	}
	if current := slice.currentSession(telegramChat); current != secondNew.SessionID {
		t.Fatalf("current Telegram session = %s, want second /new session %s", current, secondNew.SessionID)
	}

	telegramUser := slice.sessionOwner(initial)
	clock := &e2eClock{at: time.Now().UTC()}
	api, err := sessionapi.New(sessionapi.Config{
		CursorKey: []byte(strings.Repeat("c", 32)),
		IDKey:     []byte(strings.Repeat("a", 32)),
	}, slice.state, slice.blobs, clock)
	if err != nil {
		t.Fatal(err)
	}
	active, err := api.List(slice.ctx, initial.TenantID, telegramUser, domain.SessionActive, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	assertSessionPageContains(t, active, initial.SessionID, firstNew.SessionID, secondNew.SessionID)
	for _, sessionID := range []domain.SessionID{initial.SessionID, firstNew.SessionID} {
		record, err := api.Get(slice.ctx, initial.TenantID, telegramUser, sessionID)
		if err != nil || record.Session.ID != sessionID {
			t.Fatalf("open previous session %s = %+v err=%v", sessionID, record, err)
		}
		if _, err := api.History(slice.ctx, initial.TenantID, telegramUser, sessionID, "", 100); err != nil {
			t.Fatalf("history for previous session %s: %v", sessionID, err)
		}
	}

	beforeArchive, err := api.History(slice.ctx, initial.TenantID, telegramUser, initial.SessionID, "", 100)
	if err != nil || len(beforeArchive.Items) == 0 {
		t.Fatalf("initial history before archive: items=%d err=%v", len(beforeArchive.Items), err)
	}
	artifactBefore := slice.readBlob(manifest.Artifacts[0].Blob)
	archived, err := api.SetArchived(slice.ctx, initial.TenantID, telegramUser, initial.SessionID, true, "archive-e2e-1")
	if err != nil || archived.Status != domain.SessionArchived {
		t.Fatalf("archive = %+v err=%v", archived, err)
	}
	archivedPage, err := api.List(slice.ctx, initial.TenantID, telegramUser, domain.SessionArchived, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	assertSessionPageContains(t, archivedPage, initial.SessionID)
	archivedRecord, err := api.Get(slice.ctx, initial.TenantID, telegramUser, initial.SessionID)
	if err != nil || archivedRecord.Session.Status != domain.SessionArchived {
		t.Fatalf("open archived session = %+v err=%v", archivedRecord, err)
	}
	afterArchive, err := api.History(slice.ctx, initial.TenantID, telegramUser, initial.SessionID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	assertSameHistory(t, beforeArchive, afterArchive)
	if artifactAfter := slice.readBlob(manifest.Artifacts[0].Blob); !bytes.Equal(artifactAfter, artifactBefore) {
		t.Fatal("archive changed an immutable canonical artifact")
	}
	clock.at = time.Now().UTC()
	unarchived, err := api.SetArchived(slice.ctx, initial.TenantID, telegramUser, initial.SessionID, false, "unarchive-e2e-1")
	if err != nil || unarchived.Status != domain.SessionActive || unarchived.ArchivedAt != nil {
		t.Fatalf("unarchive = %+v err=%v", unarchived, err)
	}
	activeAfterUnarchive, err := api.List(slice.ctx, initial.TenantID, telegramUser, domain.SessionActive, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	assertSessionPageContains(t, activeAfterUnarchive, initial.SessionID)

	ingress, err := sessioningress.New(sessioningress.Config{
		IDKey:                 []byte(strings.Repeat("i", 32)),
		HarnessBinder:         sessionlessharness.NewDeterministicFixtureBinderV1(),
		DispatchWakePublisher: slice.schedulerWakePublisher(),
	}, slice.state, slice.blobs)
	if err != nil {
		t.Fatal(err)
	}
	syntheticConversationID := "synthetic-e2e-" + string(secondNew.SessionID)
	synthetic := syntheticfrontend.New(ingress, initial.TenantID, telegramUser, syntheticConversationID)
	syntheticState, err := synthetic.EnsureSession(slice.ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	clock.at = time.Now().UTC()
	syntheticBinding, err := api.BindFrontend(
		slice.ctx, initial.TenantID, telegramUser, syntheticfrontend.Frontend,
		syntheticConversationID, secondNew.SessionID,
		syntheticState.Binding.Revision,
	)
	if err != nil || syntheticBinding.SessionID != secondNew.SessionID {
		t.Fatalf("attach synthetic frontend = %+v err=%v", syntheticBinding, err)
	}
	syntheticResult, err := synthetic.Send(
		slice.ctx, "synthetic-delivery-e2e-1", "shared canonical session",
		initial.ConnectionID, time.Now().UTC(),
	)
	if err != nil || syntheticResult.SessionID != secondNew.SessionID {
		t.Fatalf("synthetic canonical event = %+v err=%v", syntheticResult, err)
	}
	shared, err := api.Get(slice.ctx, initial.TenantID, telegramUser, secondNew.SessionID)
	if err != nil || shared.Display.Origin == nil || *shared.Display.Origin != syntheticfrontend.Frontend {
		t.Fatalf("shared cross-frontend session = %+v err=%v", shared, err)
	}
	telegramBinding, found, err := slice.state.ResolveFrontendBinding(
		slice.ctx, initial.TenantID, domain.FrontendTelegram, initial.Conversation.ExternalID,
	)
	if err != nil || !found || telegramBinding.SessionID != secondNew.SessionID ||
		syntheticBinding.SessionID != telegramBinding.SessionID {
		t.Fatalf("cross-frontend bindings: telegram=%+v found=%t synthetic=%+v err=%v", telegramBinding, found, syntheticBinding, err)
	}
	sharedHistory, err := api.History(slice.ctx, initial.TenantID, telegramUser, secondNew.SessionID, "", 100)
	if err != nil || !historyContainsEvent(sharedHistory, syntheticResult.EventID) {
		t.Fatalf("shared history does not contain synthetic event %s: items=%d err=%v", syntheticResult.EventID, len(sharedHistory.Items), err)
	}
	syntheticRun := runRef{
		TenantID: initial.TenantID, ConnectionID: initial.ConnectionID,
		RunID: syntheticResult.RunID, SessionID: syntheticResult.SessionID,
	}
	slice.waitRunStatus(syntheticRun, domain.RunQueued)
	slice.runWorker(nil)
	slice.waitRunStatus(syntheticRun, domain.RunSucceeded)

	otherTenant, otherUser := slice.telegramPrincipal(otherChat)
	if _, err := api.Get(slice.ctx, otherTenant, otherUser, initial.SessionID); !errors.Is(err, sessionapi.ErrSessionUnavailable) {
		t.Fatalf("cross-tenant open error = %v, want unavailable", err)
	}
	if _, err := api.History(slice.ctx, otherTenant, otherUser, initial.SessionID, "", 10); !errors.Is(err, sessionapi.ErrSessionUnavailable) {
		t.Fatalf("cross-tenant history error = %v, want unavailable", err)
	}
	if _, err := api.BindFrontend(
		slice.ctx, initial.TenantID, otherUser, syntheticfrontend.Frontend,
		"cross-tenant-binding", initial.SessionID, 0,
	); !errors.Is(err, sessionapi.ErrSessionUnavailable) {
		t.Fatalf("cross-tenant bind error = %v, want unavailable", err)
	}
	if _, err := api.List(slice.ctx, initial.TenantID, otherUser, domain.SessionActive, "", 20); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("wrong-tenant user list error = %v, want membership denied", err)
	}
	otherSessions, err := api.List(slice.ctx, otherTenant, otherUser, domain.SessionActive, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	assertSessionPageExcludes(t, otherSessions, initial.SessionID, firstNew.SessionID, secondNew.SessionID)
	if _, err := slice.blobs.Open(slice.ctx, otherTenant, manifest.Artifacts[0].Blob); err == nil {
		t.Fatal("cross-tenant canonical artifact materialization succeeded")
	} else {
		var mismatch domain.TenantMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("cross-tenant materialization error = %v, want TenantMismatchError", err)
		}
	}
	if _, err := slice.state.RequestSessionDeletion(slice.ctx, domain.SessionDeletion{
		TenantID: initial.TenantID, SessionID: initial.SessionID, RequestedBy: otherUser,
		Reason: "cross-tenant negative fixture", State: domain.SessionDeletionRequested,
		RequestedAt: time.Now().UTC(),
	}); err == nil {
		t.Fatal("cross-tenant destructive deletion request succeeded")
	}
	if _, err := api.Get(slice.ctx, initial.TenantID, telegramUser, initial.SessionID); err != nil {
		t.Fatalf("negative deletion attempt changed the selected session: %v", err)
	}
	slice.assertCount(0,
		`SELECT COUNT(*) FROM dispatch_outbox WHERE tenant_id = $1 AND status = $2`,
		initial.TenantID, domain.DispatchPending)
	slice.assertCount(0,
		`SELECT COUNT(*) FROM dispatch_outbox WHERE tenant_id = $1 AND status = $2`,
		otherTenant, domain.DispatchPending)
}

func (slice *localSlice) sessionOwner(ref runRef) domain.UserID {
	slice.t.Helper()
	session, found, err := slice.state.GetSession(slice.ctx, ref.TenantID, ref.SessionID)
	if err != nil {
		slice.t.Fatal(err)
	}
	if !found {
		slice.t.Fatalf("session %s not found", ref.SessionID)
	}
	return session.CreatedBy
}

func (slice *localSlice) telegramPrincipal(chatID int64) (domain.TenantID, domain.UserID) {
	slice.t.Helper()
	resolver, err := telegramingress.NewIdentityResolver([]byte(identityKey))
	if err != nil {
		slice.t.Fatal(err)
	}
	identity, err := resolver.ResolvePrivate(chatID, chatID, "codex")
	if err != nil {
		slice.t.Fatal(err)
	}
	state, err := slice.state.EnsureTelegramIdentity(slice.ctx, ports.TelegramIdentityRequest{
		TenantID: identity.Tenant, Actor: identity.Actor, Conversation: identity.Conversation,
		SubscriptionConnectionID: identity.SubscriptionConnection, Provider: "codex",
		ObservedAt: time.Now().UTC(),
	})
	if err != nil {
		slice.t.Fatal(err)
	}
	return identity.Tenant, state.UserID
}

func (slice *localSlice) persistedRun(ref runRef) domain.Run {
	slice.t.Helper()
	var run domain.Run
	if err := slice.state.Transact(slice.ctx, ref.TenantID, func(tx ports.StateTx) error {
		var found bool
		var err error
		run, found, err = tx.GetRun(slice.ctx, ref.RunID)
		if err != nil {
			return err
		}
		if !found {
			return sessionapi.ErrSessionUnavailable
		}
		return nil
	}); err != nil {
		slice.t.Fatal(err)
	}
	return run
}

func (slice *localSlice) assertCanonicalIngressCardinality(ref runRef, runs, attempts, dispatches, userEvents uint64) {
	slice.t.Helper()
	slice.assertCount(runs,
		`SELECT COUNT(*) FROM runs WHERE tenant_id = $1 AND run_id = $2`, ref.TenantID, ref.RunID)
	slice.assertCount(attempts,
		`SELECT COUNT(*) FROM attempts WHERE tenant_id = $1 AND run_id = $2`, ref.TenantID, ref.RunID)
	slice.assertCount(dispatches,
		`SELECT COUNT(*) FROM dispatch_outbox WHERE tenant_id = $1 AND run_id = $2`, ref.TenantID, ref.RunID)
	slice.assertCount(userEvents,
		`SELECT COUNT(*) FROM session_events
		 WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3 AND kind = $4`,
		ref.TenantID, ref.SessionID, ref.RunID, domain.SessionEventUserMessage)
}

func (slice *localSlice) canonicalTerminalIdentity(ref runRef) canonicalRunIdentity {
	slice.t.Helper()
	run := slice.persistedRun(ref)
	delivery := slice.deliveryForRun(ref)
	if delivery.Projection == nil {
		slice.t.Fatalf("run %s delivery has no canonical projection", ref.RunID)
	}
	return canonicalRunIdentity{
		TriggerEventID: run.TriggerEventID, AssistantEventID: delivery.Projection.EventID,
		ProjectionID: delivery.Projection.ProjectionID, DeliveryID: delivery.ID,
	}
}

func (slice *localSlice) assertCanonicalTerminalCardinality(ref runRef) {
	slice.t.Helper()
	slice.assertCanonicalIngressCardinality(ref, 1, 1, 1, 1)
	slice.assertCount(1,
		`SELECT COUNT(*) FROM run_finalizations WHERE tenant_id = $1 AND run_id = $2`, ref.TenantID, ref.RunID)
	slice.assertCount(1,
		`SELECT COUNT(*) FROM session_events
		 WHERE tenant_id = $1 AND session_id = $2 AND run_id = $3 AND kind = $4`,
		ref.TenantID, ref.SessionID, ref.RunID, domain.SessionEventAssistantMessage)
	slice.assertCount(1,
		`SELECT COUNT(*) FROM telegram_delivery_outbox WHERE tenant_id = $1 AND run_id = $2`, ref.TenantID, ref.RunID)
}

func (slice *localSlice) readBlob(ref domain.BlobRef) []byte {
	slice.t.Helper()
	reader, err := slice.blobs.Open(slice.ctx, ref.TenantID, ref)
	if err != nil {
		slice.t.Fatal(err)
	}
	defer reader.Close()
	payload, err := io.ReadAll(reader)
	if err != nil {
		slice.t.Fatal(err)
	}
	return payload
}

func (slice *localSlice) assertCount(wanted uint64, query string, args ...any) {
	slice.t.Helper()
	var count uint64
	if err := slice.db.QueryRowContext(slice.ctx, query, args...).Scan(&count); err != nil {
		slice.t.Fatal(err)
	}
	if count != wanted {
		slice.t.Fatalf("query count = %d, want %d for %s", count, wanted, strings.Join(strings.Fields(query), " "))
	}
}

func assertSessionPageContains(t *testing.T, page sessionapi.Page[ports.SessionRecord], wanted ...domain.SessionID) {
	t.Helper()
	seen := make(map[domain.SessionID]struct{}, len(page.Items))
	for _, item := range page.Items {
		seen[item.Session.ID] = struct{}{}
	}
	for _, id := range wanted {
		if _, found := seen[id]; !found {
			t.Fatalf("session page does not contain %s: %+v", id, page.Items)
		}
	}
}

func assertSessionPageExcludes(t *testing.T, page sessionapi.Page[ports.SessionRecord], forbidden ...domain.SessionID) {
	t.Helper()
	for _, item := range page.Items {
		for _, id := range forbidden {
			if item.Session.ID == id {
				t.Fatalf("session page exposed forbidden session %s", id)
			}
		}
	}
}

func assertSameHistory(t *testing.T, before, after sessionapi.Page[sessionapi.Event]) {
	t.Helper()
	if len(before.Items) != len(after.Items) {
		t.Fatalf("history length changed from %d to %d", len(before.Items), len(after.Items))
	}
	for index := range before.Items {
		if before.Items[index].Event.ID != after.Items[index].Event.ID ||
			!bytes.Equal(before.Items[index].Payload, after.Items[index].Payload) {
			t.Fatalf("history item %d changed across archive: before=%+v after=%+v", index, before.Items[index].Event, after.Items[index].Event)
		}
	}
}

func historyContainsEvent(page sessionapi.Page[sessionapi.Event], eventID domain.SessionEventID) bool {
	for _, item := range page.Items {
		if item.Event.ID == eventID {
			return true
		}
	}
	return false
}
