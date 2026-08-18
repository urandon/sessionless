package domain_test

import (
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func canonicalSession() domain.Session {
	return domain.Session{ID: "session-1", TenantID: "tenant-a", CreatedBy: "user-1", Status: domain.SessionActive, CreatedAt: testTime, UpdatedAt: testTime}
}

func canonicalEvent(sequence uint64) domain.SessionEvent {
	author := domain.UserID("user-1")
	return domain.SessionEvent{
		ID:       domain.SessionEventID("event-" + time.Unix(int64(sequence), 0).UTC().Format("150405")),
		TenantID: "tenant-a", SessionID: "session-1", Sequence: sequence,
		Kind: domain.SessionEventUserMessage, AuthorUserID: &author,
		IdempotencyKey: domain.IdempotencyKey("append-" + time.Unix(int64(sequence), 0).UTC().Format("150405")),
		Payload: domain.BlobRef{
			TenantID: "tenant-a",
			Key:      "tenants/tenant-a/sessions/session-1/events/event-" + time.Unix(int64(sequence), 0).UTC().Format("150405") + "/payload.json",
			Size:     42,
			SHA256:   "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		}, CreatedAt: testTime.Add(time.Duration(sequence) * time.Second),
	}
}

func TestAppendSessionEventIsStrictlyOrderedAndIdempotent(t *testing.T) {
	t.Parallel()
	session := canonicalSession()
	event := canonicalEvent(1)
	created, err := domain.AppendSessionEvent(&session, event, nil)
	if err != nil || !created {
		t.Fatalf("first append = (%v, %v), want created", created, err)
	}
	created, err = domain.AppendSessionEvent(&session, event, &event)
	if err != nil || created {
		t.Fatalf("duplicate append = (%v, %v), want no-op", created, err)
	}
	conflict := event
	conflict.ID = "event-conflict"
	conflict.Payload.Key = "tenants/tenant-a/sessions/session-1/events/event-conflict/payload.json"
	if _, err := domain.AppendSessionEvent(&session, conflict, &event); !errors.Is(err, domain.ErrEventIdempotencyConflict) {
		t.Fatalf("conflict error = %v", err)
	}
	payloadConflict := event
	payloadConflict.Payload.Key = "tenants/tenant-a/sessions/session-1/events/" + string(event.ID) + "/other-payload.json"
	if _, err := domain.AppendSessionEvent(&session, payloadConflict, &event); !errors.Is(err, domain.ErrEventIdempotencyConflict) {
		t.Fatalf("payload conflict error = %v", err)
	}
	if _, err := domain.AppendSessionEvent(&session, canonicalEvent(3), nil); err == nil {
		t.Fatal("append accepted a sequence gap")
	}
}

func TestProducedEventsRequireOwningRun(t *testing.T) {
	t.Parallel()
	event := canonicalEvent(1)
	event.Kind = domain.SessionEventAssistantMessage
	event.AuthorUserID = nil
	if err := event.Validate(); err == nil {
		t.Fatal("assistant event without run accepted")
	}
	runID := domain.RunID("run-1")
	event.RunID = &runID
	if err := event.Validate(); err != nil {
		t.Fatalf("assistant event with run rejected: %v", err)
	}
}

func TestCanonicalPayloadsCannotCrossSessionBoundaries(t *testing.T) {
	t.Parallel()
	event := canonicalEvent(1)
	event.Payload.Key = "tenants/tenant-a/sessions/session-other/events/" + string(event.ID) + "/payload.json"
	if err := event.Validate(); err == nil {
		t.Fatal("event accepted another session's object prefix")
	}

	snapshot := domain.SessionSnapshot{
		ID: "snapshot-1", TenantID: "tenant-a", SessionID: "session-1",
		Version: 1, ThroughSequence: 1, FormatVersion: domain.SessionSnapshotFormatV1,
		Compression: domain.SessionSnapshotCompressionZstandard,
		EventCount:  1, UncompressedSize: 128, CreatedAt: testTime,
		Payload: domain.BlobRef{
			TenantID: "tenant-a",
			Key:      domain.SessionSnapshotObjectKey("tenant-a", "session-other", 1),
			Size:     42,
			SHA256:   "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
	}
	if err := snapshot.Validate(); err == nil {
		t.Fatal("snapshot accepted another session's object prefix")
	}
}

func TestSessionArchiveAndUnarchiveTransitions(t *testing.T) {
	t.Parallel()
	session := canonicalSession()
	archivedAt := testTime.Add(time.Minute)
	if err := session.Archive(archivedAt); err != nil {
		t.Fatal(err)
	}
	if err := session.Archive(archivedAt.Add(time.Second)); err != nil {
		t.Fatalf("idempotent archive: %v", err)
	}
	if _, err := domain.AppendSessionEvent(&session, canonicalEvent(1), nil); err == nil {
		t.Fatal("event appended to archived session")
	}
	if err := session.Unarchive(archivedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := session.Unarchive(archivedAt.Add(2 * time.Minute)); err != nil {
		t.Fatalf("idempotent unarchive: %v", err)
	}
	if session.Status != domain.SessionActive || session.ArchivedAt != nil {
		t.Fatalf("unexpected unarchived state: %+v", session)
	}
}

func TestSessionDeletionAndLegalHoldStateMachines(t *testing.T) {
	t.Parallel()
	deletion := domain.SessionDeletion{
		TenantID: "tenant-a", SessionID: "session-1", RequestedBy: "user-1",
		Reason: "user requested erasure", State: domain.SessionDeletionRequested,
		RequestedAt: testTime,
	}
	if err := deletion.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := deletion.Start(testTime.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := deletion.Complete(testTime.Add(2*time.Minute), 3, 42); err != nil {
		t.Fatal(err)
	}
	if err := deletion.Complete(testTime.Add(3*time.Minute), 3, 42); err != nil {
		t.Fatalf("idempotent completion: %v", err)
	}
	if err := deletion.Complete(testTime.Add(3*time.Minute), 4, 42); err == nil {
		t.Fatal("conflicting completion accepted")
	}

	hold := domain.SessionLegalHold{
		TenantID: "tenant-a", SessionID: "session-1", State: domain.SessionLegalHoldActive,
		Reason: "litigation preservation", SetBy: "user-1", SetAt: testTime,
	}
	if err := hold.Release("user-2", testTime.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := hold.Release("user-2", testTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
}

func TestSessionParticipantRejectsInvalidAccess(t *testing.T) {
	t.Parallel()
	participant := domain.SessionParticipant{TenantID: "tenant-a", SessionID: "session-1", UserID: "user-1", Role: domain.SessionParticipantMember, Status: domain.SessionParticipantActive, CreatedAt: testTime, UpdatedAt: testTime}
	if err := participant.Authorize("tenant-b", "session-1", "user-1", true); err == nil {
		t.Fatal("cross-tenant access accepted")
	}
	if err := participant.Authorize("tenant-a", "session-1", "user-2", true); err == nil {
		t.Fatal("wrong user access accepted")
	}
}

func TestFrontendBindingRejectsStaleSwitch(t *testing.T) {
	t.Parallel()
	binding := domain.FrontendBinding{ID: "binding-1", TenantID: "tenant-a", Frontend: domain.FrontendTelegram, ExternalConversationID: "123", SessionID: "session-1", Revision: 2, CreatedAt: testTime, UpdatedAt: testTime}
	var stale domain.StaleBindingError
	if err := binding.Switch(1, "session-2", testTime.Add(time.Minute)); !errors.As(err, &stale) {
		t.Fatalf("stale switch error = %v", err)
	}
	if err := binding.Switch(2, "session-2", testTime.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if binding.SessionID != "session-2" || binding.Revision != 3 {
		t.Fatalf("unexpected binding state: %+v", binding)
	}
}

func TestSessionContextRequiresContiguousEventsAfterSnapshot(t *testing.T) {
	t.Parallel()
	snapshot := domain.SessionSnapshot{ID: "snapshot-1", TenantID: "tenant-a", SessionID: "session-1", Version: 1, ThroughSequence: 2, FormatVersion: domain.SessionSnapshotFormatV1, Compression: domain.SessionSnapshotCompressionZstandard, EventCount: 2, UncompressedSize: 128, Payload: domain.BlobRef{TenantID: "tenant-a", Key: domain.SessionSnapshotObjectKey("tenant-a", "session-1", 1), Size: 42, SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}, CreatedAt: testTime}
	input := domain.SessionContextInput{TenantID: "tenant-a", SessionID: "session-1", Snapshot: &snapshot, Events: []domain.SessionEvent{canonicalEvent(3)}}
	if err := input.Validate(); err != nil {
		t.Fatalf("valid context rejected: %v", err)
	}
	input.Events[0].Sequence = 4
	if err := input.Validate(); err == nil {
		t.Fatal("context accepted a sequence gap")
	}
}
