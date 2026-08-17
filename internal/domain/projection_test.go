package domain_test

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestFrontendProjectionSnapshotsCanonicalEventAndBinding(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 18, 0, 0, 0, time.UTC)
	run := domain.Run{
		ID: "run-1", TenantID: "tenant-a", SessionID: "session-1",
		TriggerEventID: "event-user", SubscriptionConnectionID: "subscription-1",
		Status: domain.RunRunning, IdempotencyKey: "run-key", CreatedAt: now, UpdatedAt: now,
	}
	draft := domain.SessionEventDraft{
		ID: "event-assistant", Kind: domain.SessionEventAssistantMessage,
		IdempotencyKey: "assistant-key", CreatedAt: now,
		Payload: domain.BlobRef{
			TenantID: "tenant-a", Key: "tenants/tenant-a/sessions/session-1/events/event-assistant/payload.json",
			SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
	if err := draft.ValidateForRun(run); err != nil {
		t.Fatal(err)
	}
	event := domain.SessionEvent{
		ID: draft.ID, TenantID: run.TenantID, SessionID: run.SessionID, Sequence: 2,
		Kind: draft.Kind, RunID: &run.ID, IdempotencyKey: draft.IdempotencyKey,
		Payload: draft.Payload, CreatedAt: now,
	}
	binding := domain.FrontendBinding{
		ID: "binding-1", TenantID: run.TenantID, Frontend: domain.FrontendTelegram,
		ExternalConversationID: "conversation-1", SessionID: run.SessionID, Revision: 3,
		CreatedAt: now, UpdatedAt: now,
	}
	projection := domain.FrontendProjection{
		ID: "projection-1", TenantID: run.TenantID, SessionID: run.SessionID,
		EventID: event.ID, EventSequence: event.Sequence, EventKind: event.Kind,
		BindingID: binding.ID, BindingRevision: binding.Revision, Frontend: binding.Frontend,
		Status: domain.FrontendProjectionPending, IdempotencyKey: "projection-key",
		CreatedAt: now, UpdatedAt: now,
	}
	if err := projection.ValidateFor(event, binding); err != nil {
		t.Fatal(err)
	}
	toolEvent := event
	toolEvent.Kind = domain.SessionEventToolResult
	toolProjection := projection
	toolProjection.EventKind = toolEvent.Kind
	if err := toolProjection.ValidateFor(toolEvent, binding); err == nil {
		t.Fatal("tool event accepted as frontend projection work")
	}
	projection.BindingRevision++
	if err := projection.ValidateFor(event, binding); err == nil {
		t.Fatal("stale projection binding snapshot accepted")
	}
}

func TestSessionEventDraftRejectsUserAndCrossSessionPayloads(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 11, 18, 0, 0, 0, time.UTC)
	run := domain.Run{
		ID: "run-1", TenantID: "tenant-a", SessionID: "session-1",
		TriggerEventID: "event-user", SubscriptionConnectionID: "subscription-1",
		Status: domain.RunRunning, IdempotencyKey: "run-key", CreatedAt: now, UpdatedAt: now,
	}
	draft := domain.SessionEventDraft{
		ID: "event-result", Kind: domain.SessionEventUserMessage,
		IdempotencyKey: "result-key", CreatedAt: now,
		Payload: domain.BlobRef{
			TenantID: "tenant-a", Key: "tenants/tenant-a/sessions/session-2/events/event-result/payload.json",
			SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
	if err := draft.ValidateForRun(run); err == nil {
		t.Fatal("user terminal event accepted")
	}
	draft.Kind = domain.SessionEventToolCall
	if err := draft.ValidateForRun(run); err == nil {
		t.Fatal("cross-session terminal payload accepted")
	}
}
