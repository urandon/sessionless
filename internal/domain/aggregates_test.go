package domain_test

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestArtifactManifestValidation(t *testing.T) {
	t.Parallel()

	run := validRun()
	manifest := domain.ArtifactManifest{
		ID:       "manifest-1",
		TenantID: run.TenantID,
		RunID:    run.ID,
		Artifacts: []domain.Artifact{{
			Name:      "result.md",
			MediaType: "text/markdown",
			Blob:      validBlob(),
		}},
		CreatedAt: testTime,
	}
	if err := manifest.ValidateForRun(run); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
	manifest.Artifacts = append(manifest.Artifacts, manifest.Artifacts[0])
	if err := manifest.ValidateForRun(run); err == nil {
		t.Fatal("duplicate artifact name accepted")
	}
}

func TestQuotaReservationAndUsageValidation(t *testing.T) {
	t.Parallel()

	run := validRun()
	attempt := validAttempt()
	reservation := domain.QuotaReservation{
		ID:                       "reservation-1",
		TenantID:                 run.TenantID,
		RunID:                    run.ID,
		SubscriptionConnectionID: run.SubscriptionConnectionID,
		Status:                   domain.ReservationHeld,
		CapacityUnits:            1,
		HeldAt:                   testTime,
		ExpiresAt:                testTime.Add(time.Minute),
		UpdatedAt:                testTime,
	}
	if err := reservation.ValidateForRun(run); err != nil {
		t.Fatalf("valid reservation rejected: %v", err)
	}
	if err := reservation.Transition(domain.ReservationCommitted, testTime.Add(time.Second)); err != nil {
		t.Fatalf("reservation transition rejected: %v", err)
	}
	if err := reservation.Transition(domain.ReservationReleased, testTime.Add(2*time.Second)); err == nil {
		t.Fatal("terminal reservation transition accepted")
	}

	input := uint64(12)
	output := uint64(8)
	observation := domain.UsageObservation{
		ID:                       "usage-1",
		TenantID:                 run.TenantID,
		RunID:                    run.ID,
		AttemptID:                attempt.ID,
		SubscriptionConnectionID: run.SubscriptionConnectionID,
		Source:                   domain.UsageSourceHarness,
		InputTokens:              &input,
		OutputTokens:             &output,
		ObservedAt:               testTime,
	}
	if err := observation.ValidateForAttempt(run, attempt); err != nil {
		t.Fatalf("valid usage observation rejected: %v", err)
	}
	observation.InputTokens = nil
	observation.OutputTokens = nil
	if err := observation.ValidateForAttempt(run, attempt); err == nil {
		t.Fatal("empty usage observation accepted")
	}
}

func TestDispatchAndTelegramOutboxes(t *testing.T) {
	t.Parallel()

	run := validRun()
	attempt := validAttempt()
	dispatch := domain.DispatchOutbox{
		ID:             "dispatch-1",
		TenantID:       run.TenantID,
		RunID:          run.ID,
		AttemptID:      attempt.ID,
		Status:         domain.DispatchPending,
		IdempotencyKey: "dispatch-run-1-attempt-1",
		CreatedAt:      testTime,
		UpdatedAt:      testTime,
	}
	if err := dispatch.ValidateForAttempt(run, attempt); err != nil {
		t.Fatalf("valid dispatch outbox rejected: %v", err)
	}
	if err := dispatch.Transition(domain.DispatchPublished, testTime.Add(time.Second)); err != nil {
		t.Fatalf("dispatch transition rejected: %v", err)
	}

	delivery := domain.TelegramDeliveryOutbox{
		ID:               "delivery-1",
		TenantID:         run.TenantID,
		RunID:            run.ID,
		Chat:             domain.TelegramChatRef{TenantID: run.TenantID, ChatID: -1000123},
		ReplyToMessageID: 77,
		Payload:          validBlob(),
		Status:           domain.DeliveryPending,
		IdempotencyKey:   "delivery-run-1",
		CreatedAt:        testTime,
		UpdatedAt:        testTime,
	}
	if err := delivery.ValidateForRun(run); err != nil {
		t.Fatalf("valid Telegram delivery rejected: %v", err)
	}
	if err := delivery.Transition(domain.DeliverySending, testTime.Add(time.Second), nil); err != nil {
		t.Fatalf("delivery sending transition rejected: %v", err)
	}
	if delivery.AttemptCount != 1 {
		t.Fatalf("AttemptCount = %d, want 1", delivery.AttemptCount)
	}
	if err := delivery.Transition(domain.DeliverySent, testTime.Add(2*time.Second), nil); err != nil {
		t.Fatalf("delivery sent transition rejected: %v", err)
	}

	inline := domain.TelegramDeliveryOutbox{
		ID: "delivery-inline", TenantID: run.TenantID, RunID: run.ID,
		Chat:             domain.TelegramChatRef{TenantID: run.TenantID, ChatID: 123},
		ReplyToMessageID: 78, Text: "command reply",
		Status: domain.DeliveryPending, IdempotencyKey: "delivery-inline",
		CreatedAt: testTime, UpdatedAt: testTime,
	}
	if err := inline.ValidateForRun(run); err != nil {
		t.Fatalf("inline Telegram delivery rejected: %v", err)
	}
	inline.Payload = validBlob()
	if err := inline.ValidateForRun(run); err == nil {
		t.Fatal("ambiguous Telegram delivery content accepted")
	}
}

func TestFrontendAdaptersAndLeaseActivity(t *testing.T) {
	t.Parallel()

	chat := domain.TelegramChatRef{TenantID: "tenant-a", ChatID: -1000123}
	conversation := chat.Conversation("conversation-1")
	if err := conversation.Validate(); err != nil {
		t.Fatalf("Telegram conversation rejected: %v", err)
	}
	user := domain.TelegramUserRef{TenantID: "tenant-a", UserID: 123}
	actor := user.Actor("actor-1")
	if err := actor.Validate(); err != nil {
		t.Fatalf("Telegram actor rejected: %v", err)
	}

	run := validRun()
	attempt := validAttempt()
	if err := attempt.Transition(domain.AttemptRunning, testTime.Add(time.Second)); err != nil {
		t.Fatalf("attempt transition rejected: %v", err)
	}
	lease := domain.Lease{
		ID:         "lease-1",
		TenantID:   run.TenantID,
		RunID:      run.ID,
		AttemptID:  attempt.ID,
		WorkerID:   "worker-1",
		FenceToken: 1,
		AcquiredAt: testTime,
		ExpiresAt:  testTime.Add(time.Minute),
	}
	if !lease.ActiveAt(testTime.Add(30 * time.Second)) {
		t.Fatal("lease should be active inside its interval")
	}
	if lease.ActiveAt(lease.ExpiresAt) {
		t.Fatal("lease should be inactive at its exclusive expiry")
	}
}

func TestEntitlementValidation(t *testing.T) {
	t.Parallel()

	snapshot := domain.EntitlementSnapshot{
		TenantID:                 "tenant-a",
		SubscriptionConnectionID: "subscription-1",
		State:                    domain.EntitlementActive,
		ObservedAt:               testTime,
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("valid entitlement rejected: %v", err)
	}
}
