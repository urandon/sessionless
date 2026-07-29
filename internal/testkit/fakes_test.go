package testkit_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

func TestClockAndIDsAreDeterministic(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	clock := testkit.NewFakeClock(start)
	clock.Advance(5 * time.Minute)
	if got := clock.Now(); !got.Equal(start.Add(5 * time.Minute)) {
		t.Fatalf("Now() = %v", got)
	}

	ids := testkit.NewSequenceIDGenerator("test-")
	first, err := ids.NewID(context.Background(), ports.IDRun)
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	second, err := ids.NewID(context.Background(), ports.IDRun)
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	if first != "test-run-0001" || second != "test-run-0002" {
		t.Fatalf("IDs = %q, %q", first, second)
	}
}

func TestMemoryQueueModelsAtLeastOnceRetry(t *testing.T) {
	t.Parallel()

	queue := testkit.NewMemoryQueue()
	envelope := queuecontract.Envelope{
		Schema:     queuecontract.SchemaV1,
		MessageID:  domain.MessageID("msg-1"),
		Kind:       queuecontract.KindDispatchRun,
		TenantID:   domain.TenantID("tenant-a"),
		SubjectID:  "dispatch-1",
		EnqueuedAt: time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC),
	}
	ctx := context.Background()
	if err := queue.Publish(ctx, envelope); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	first, err := queue.Receive(ctx)
	if err != nil {
		t.Fatalf("first Receive() error = %v", err)
	}
	if first.DeliveryCount != 1 {
		t.Fatalf("first DeliveryCount = %d", first.DeliveryCount)
	}
	if err := queue.Retry(ctx, first.ReceiptHandle, time.Second); err != nil {
		t.Fatalf("Retry() error = %v", err)
	}
	second, err := queue.Receive(ctx)
	if err != nil {
		t.Fatalf("second Receive() error = %v", err)
	}
	if second.DeliveryCount != 2 || second.Envelope.MessageID != envelope.MessageID {
		t.Fatalf("retried message = %+v", second)
	}
	if err := queue.DeadLetter(ctx, second.ReceiptHandle, "attempts_exhausted"); err != nil {
		t.Fatalf("DeadLetter() error = %v", err)
	}
	if queue.DeadLetterCount() != 1 {
		t.Fatalf("DeadLetterCount() = %d", queue.DeadLetterCount())
	}
	if _, err := queue.Receive(ctx); err == nil {
		t.Fatal("Receive() succeeded on an empty queue")
	} else {
		var noMessage *testkit.NoMessageError
		if !errors.As(err, &noMessage) {
			t.Fatalf("Receive() error = %T, want NoMessageError", err)
		}
	}
}
