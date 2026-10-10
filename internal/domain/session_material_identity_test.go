package domain_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func materialIdentityEvent() domain.SessionEvent {
	user := domain.UserID("user-source")
	return domain.SessionEvent{
		ID: "evt-source", TenantID: "tenant-source", SessionID: "session-source", Sequence: 9,
		Kind: domain.SessionEventUserMessage, AuthorUserID: &user, IdempotencyKey: "key-source",
		Payload:   domain.BlobRef{TenantID: "tenant-source", Key: "tenants/tenant-source/sessions/session-source/events/evt-source/message.json", Size: 2, SHA256: strings.Repeat("a", 64)},
		CreatedAt: time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC),
	}
}

func TestSessionEventMaterialIdentityLeavesAllocationToFinalWriter(t *testing.T) {
	for _, kind := range []domain.SessionEventKind{domain.SessionEventUserMessage, domain.SessionEventAssistantMessage, domain.SessionEventToolCall, domain.SessionEventToolResult, domain.SessionEventSystemNotice} {
		t.Run(string(kind), func(t *testing.T) {
			event := materialIdentityEvent()
			event.Kind = kind
			run := domain.RunID("run-source")
			event.RunID = &run // Optional user/notice Run and author remain permitted.
			event.Sequence = 0
			event.CreatedAt = time.Time{}
			if err := event.ValidateMaterialIdentity(); err != nil {
				t.Fatalf("valid stable %s identity: %v", kind, err)
			}
			if err := event.Validate(); err == nil {
				t.Error("unallocated event passed final validation")
			}
			event.Sequence = 123
			event.CreatedAt = time.Date(2026, 10, 11, 0, 0, 0, 0, time.FixedZone("offset", 3600))
			if err := event.Validate(); err != nil {
				t.Errorf("actual allocated event: %v", err)
			}
			if kind != domain.SessionEventUserMessage {
				event.AuthorUserID = nil
				if err := event.ValidateMaterialIdentity(); err != nil {
					t.Errorf("canonical optional author became required: %v", err)
				}
			}
		})
	}
}

func TestSessionEventValidationRetainsMultiInvalidPrecedence(t *testing.T) {
	rows := []struct {
		name   string
		change func(*domain.SessionEvent)
		field  string
	}{
		{name: "id_before_sequence", change: func(e *domain.SessionEvent) { e.ID = ""; e.Sequence = 0 }, field: "session_event_id"},
		{name: "tenant_before_sequence", change: func(e *domain.SessionEvent) { e.TenantID = ""; e.Sequence = 0 }, field: "tenant_id"},
		{name: "session_before_sequence", change: func(e *domain.SessionEvent) { e.SessionID = ""; e.Sequence = 0 }, field: "session_id"},
		{name: "sequence_before_kind", change: func(e *domain.SessionEvent) { e.Sequence = 0; e.Kind = "unsupported" }, field: "session_event.sequence"},
		{name: "kind_before_author", change: func(e *domain.SessionEvent) { e.Kind = "unsupported"; u := domain.UserID(""); e.AuthorUserID = &u }, field: "session_event.kind"},
		{name: "author_before_run", change: func(e *domain.SessionEvent) {
			u := domain.UserID("")
			r := domain.RunID("")
			e.AuthorUserID = &u
			e.RunID = &r
		}, field: "user_id"},
		{name: "run_before_required_author", change: func(e *domain.SessionEvent) { r := domain.RunID(""); e.RunID = &r; e.AuthorUserID = nil }, field: "run_id"},
		{name: "required_author_before_idempotency", change: func(e *domain.SessionEvent) { e.AuthorUserID = nil; e.IdempotencyKey = "" }, field: "session_event.author_user_id"},
		{name: "required_run_before_idempotency", change: func(e *domain.SessionEvent) {
			e.Kind = domain.SessionEventAssistantMessage
			e.RunID = nil
			e.IdempotencyKey = ""
		}, field: "session_event.run_id"},
		{name: "idempotency_before_blob", change: func(e *domain.SessionEvent) { e.IdempotencyKey = ""; e.Payload.Key = "" }, field: "idempotency_key"},
		{name: "blob_before_time", change: func(e *domain.SessionEvent) { e.Payload.Key = ""; e.CreatedAt = time.Time{} }, field: "blob.key"},
		{name: "time_last", change: func(e *domain.SessionEvent) { e.CreatedAt = time.Time{} }, field: "session_event.created_at"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			event := materialIdentityEvent()
			row.change(&event)
			err := event.Validate()
			var validation domain.ValidationError
			if !errors.As(err, &validation) || validation.Field != row.field {
				t.Errorf("final validation got %v, want first field %s", err, row.field)
			}
		})
	}
}

func TestSessionEventMaterialIdentityUsesCanonicalPredicates(t *testing.T) {
	changes := []struct {
		name   string
		change func(*domain.SessionEvent)
	}{
		{name: "id", change: func(e *domain.SessionEvent) { e.ID = "" }},
		{name: "tenant", change: func(e *domain.SessionEvent) { e.TenantID = "" }},
		{name: "session", change: func(e *domain.SessionEvent) { e.SessionID = "" }},
		{name: "kind", change: func(e *domain.SessionEvent) { e.Kind = "unknown" }},
		{name: "author", change: func(e *domain.SessionEvent) { e.AuthorUserID = nil }},
		{name: "run", change: func(e *domain.SessionEvent) { e.Kind = domain.SessionEventAssistantMessage }},
		{name: "idempotency", change: func(e *domain.SessionEvent) { e.IdempotencyKey = "" }},
		{name: "payload_scope", change: func(e *domain.SessionEvent) { e.Payload.TenantID = "foreign" }},
		{name: "payload_namespace", change: func(e *domain.SessionEvent) { e.Payload.Key = "tenants/tenant-source/other.json" }},
		{name: "payload_hash", change: func(e *domain.SessionEvent) { e.Payload.SHA256 = "bad" }},
		{name: "payload_size", change: func(e *domain.SessionEvent) { e.Payload.Size = -1 }},
	}
	for _, row := range changes {
		t.Run(row.name, func(t *testing.T) {
			event := materialIdentityEvent()
			row.change(&event)
			materialErr, finalErr := event.ValidateMaterialIdentity(), event.Validate()
			if materialErr == nil || finalErr == nil {
				t.Fatalf("invalid %s material/final errors: %v / %v", row.name, materialErr, finalErr)
			}
			if materialErr.Error() != finalErr.Error() {
				t.Errorf("shared predicate drift for %s: %v / %v", row.name, materialErr, finalErr)
			}
		})
	}
}
