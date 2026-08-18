package outboxwake

import (
	"context"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

func TestPublisherEmitsDeterministicPayloadFreeHints(t *testing.T) {
	now := time.Date(2026, 8, 10, 15, 0, 0, 0, time.UTC)
	queue := testkit.NewMemoryQueue()
	publisher, err := NewPublisher(queue)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishDispatchWake(context.Background(), "tenant-a", "dispatch-a", now); err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishDispatchWake(context.Background(), "tenant-a", "dispatch-a", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	first, err := queue.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := queue.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Envelope.Kind != queuecontract.KindWakeDispatch ||
		first.Envelope.SubjectID != "dispatch-a" ||
		first.Envelope.MessageID != second.Envelope.MessageID {
		t.Fatalf("first=%#v second=%#v", first.Envelope, second.Envelope)
	}
}

func TestPublisherEmitsRunScopedProjectionWake(t *testing.T) {
	now := time.Date(2026, 8, 18, 15, 0, 0, 0, time.UTC)
	queue := testkit.NewMemoryQueue()
	publisher, err := NewPublisher(queue)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishFrontendProjectionWake(context.Background(), "tenant-a", "run-a", now); err != nil {
		t.Fatal(err)
	}
	message, err := queue.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if message.Envelope.Kind != queuecontract.KindWakeProjection ||
		message.Envelope.SubjectID != "run-a" || message.Envelope.TenantID != "tenant-a" {
		t.Fatalf("projection wake = %#v", message.Envelope)
	}
}

func TestOperationalOutboxIDsAreStablePerRun(t *testing.T) {
	if DispatchOutboxID("run-a") != DispatchOutboxID("run-a") ||
		TelegramDeliveryID("run-a") != TelegramDeliveryID("run-a") ||
		TelegramProjectionDeliveryID("projection-a") != TelegramProjectionDeliveryID("projection-a") ||
		DispatchOutboxID("run-a") == DispatchOutboxID("run-b") {
		t.Fatal("outbox IDs are not stable and run-scoped")
	}
}
