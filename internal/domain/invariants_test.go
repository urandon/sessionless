package domain_test

import (
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

var testTime = time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

func validRun() domain.Run {
	return domain.Run{
		ID:                       "run-1",
		TenantID:                 "tenant-a",
		SessionID:                "session-1",
		TriggerEventID:           "event-1",
		SubscriptionConnectionID: "subscription-1",
		Status:                   domain.RunCreated,
		IdempotencyKey:           "telegram-update-1",
		CreatedAt:                testTime,
		UpdatedAt:                testTime,
	}
}

func validAttempt() domain.Attempt {
	return domain.Attempt{
		ID:        "attempt-1",
		TenantID:  "tenant-a",
		RunID:     "run-1",
		Number:    1,
		Status:    domain.AttemptCreated,
		CreatedAt: testTime,
		UpdatedAt: testTime,
	}
}

func validBlob() domain.BlobRef {
	return domain.BlobRef{
		TenantID: "tenant-a",
		Key:      "tenants/tenant-a/sessions/session-1/runs/run-1/checkpoints/context.json",
		Size:     42,
		SHA256:   "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
}

func TestCrossTenantAttemptIsRejected(t *testing.T) {
	t.Parallel()

	run := validRun()
	attempt := validAttempt()
	attempt.TenantID = "tenant-b"

	err := attempt.ValidateForRun(run)
	var mismatch domain.TenantMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("ValidateForRun() error = %v, want TenantMismatchError", err)
	}
}

func TestBlobMustRemainInsideTenantPrefix(t *testing.T) {
	t.Parallel()

	blob := validBlob()
	blob.Key = "tenants/tenant-b/runs/run-1/context.json"
	if err := blob.Validate(); err == nil {
		t.Fatal("BlobRef.Validate() succeeded for a cross-tenant key")
	}
}

func TestUnknownQuotaCannotFabricateRemainingValue(t *testing.T) {
	t.Parallel()

	remaining := int64(100)
	snapshot := domain.ProviderQuotaSnapshot{
		TenantID:                 "tenant-a",
		SubscriptionConnectionID: "subscription-1",
		State:                    domain.ProviderQuotaUnknown,
		Remaining:                &remaining,
		ObservedAt:               testTime,
	}
	if err := snapshot.Validate(); err == nil {
		t.Fatal("ProviderQuotaSnapshot.Validate() accepted fabricated unknown quota")
	}

	snapshot.Remaining = nil
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("unknown ProviderQuotaSnapshot rejected: %v", err)
	}
}

func TestRunCancellationIsIdempotent(t *testing.T) {
	t.Parallel()

	run := validRun()
	first := testTime.Add(time.Minute)
	if err := run.RequestCancellation(first); err != nil {
		t.Fatalf("first RequestCancellation() error = %v", err)
	}
	if err := run.RequestCancellation(first.Add(time.Minute)); err != nil {
		t.Fatalf("second RequestCancellation() error = %v", err)
	}
	if run.CancellationRequestedAt == nil || !run.CancellationRequestedAt.Equal(first) {
		t.Fatalf("CancellationRequestedAt = %v, want first request %v", run.CancellationRequestedAt, first)
	}
}

func TestDeliveryRetryRequiresFutureSchedule(t *testing.T) {
	t.Parallel()

	delivery := domain.TelegramDeliveryOutbox{
		Status:    domain.DeliverySending,
		UpdatedAt: testTime,
	}
	past := testTime.Add(-time.Second)
	if err := delivery.Transition(domain.DeliveryRetryWait, testTime, &past); err == nil {
		t.Fatal("Transition() accepted a retry time in the past")
	}
	future := testTime.Add(time.Minute)
	if err := delivery.Transition(domain.DeliveryRetryWait, testTime, &future); err != nil {
		t.Fatalf("Transition() rejected a future retry: %v", err)
	}
	if delivery.NextAttemptAt == nil || !delivery.NextAttemptAt.Equal(future) {
		t.Fatalf("NextAttemptAt = %v, want %v", delivery.NextAttemptAt, future)
	}
}

func TestClassifiedErrorTaxonomy(t *testing.T) {
	t.Parallel()

	base := errors.New("provider unavailable")
	err := &domain.ClassifiedError{
		Kind:       domain.ErrorRetryable,
		Code:       "provider_unavailable",
		Operation:  "execute",
		RetryAfter: time.Second,
		Cause:      base,
	}
	if !err.Retryable() {
		t.Fatal("Retryable() = false, want true")
	}
	if !errors.Is(err, base) {
		t.Fatal("ClassifiedError does not unwrap its cause")
	}
	kind, ok := domain.ClassifyError(err)
	if !ok || kind != domain.ErrorRetryable {
		t.Fatalf("ClassifyError() = %q, %v", kind, ok)
	}
}

func TestAttemptLeaseAndCheckpointStayOnOwningRun(t *testing.T) {
	t.Parallel()

	run := validRun()
	attempt := validAttempt()
	if err := attempt.ValidateForRun(run); err != nil {
		t.Fatalf("valid attempt rejected: %v", err)
	}
	lease := domain.Lease{
		ID:         "lease-1",
		TenantID:   "tenant-a",
		RunID:      run.ID,
		AttemptID:  attempt.ID,
		WorkerID:   "worker-1",
		FenceToken: 1,
		AcquiredAt: testTime,
		ExpiresAt:  testTime.Add(time.Minute),
	}
	if err := lease.ValidateForAttempt(run, attempt); err != nil {
		t.Fatalf("valid lease rejected: %v", err)
	}
	checkpoint := domain.Checkpoint{
		ID:        "checkpoint-1",
		TenantID:  "tenant-a",
		RunID:     run.ID,
		AttemptID: attempt.ID,
		Sequence:  1,
		State:     validBlob(),
		CreatedAt: testTime,
	}
	if err := checkpoint.ValidateForAttempt(run, attempt); err != nil {
		t.Fatalf("valid checkpoint rejected: %v", err)
	}
}

func TestSessionDeletionInventoryAcceptsOnlyProvenLegacyRunObjects(t *testing.T) {
	t.Parallel()

	legacy := validBlob()
	legacy.Key = domain.RunObjectPrefix("tenant-a", "run-1") + "output/result.json"
	inventory := domain.SessionDeletionInventory{
		TenantID:   "tenant-a",
		SessionID:  "session-1",
		Objects:    []domain.BlobRef{legacy},
		RunIDs:     []domain.RunID{"run-1"},
		TotalBytes: uint64(legacy.Size),
	}
	if err := inventory.Validate(1); err != nil {
		t.Fatalf("Validate() rejected an object under a proven legacy run prefix: %v", err)
	}

	inventory.RunIDs = nil
	if err := inventory.Validate(1); err == nil {
		t.Fatal("Validate() accepted a legacy run object without a proven owning run")
	}
}
