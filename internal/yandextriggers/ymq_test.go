package yandextriggers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestYMQTriggerBatch(t *testing.T) {
	body := fmt.Sprintf(`{"messages":[{"event_metadata":{"event_id":"evt-1"},"details":{"message":{"message_id":"msg-1","body":%q,"attributes":{"ApproximateReceiveCount":"3"}}}}]}`,
		`{"schema":"sessionless.queue.v1","message_id":"msg-12345678","kind":"dispatch.run","tenant_id":"ten-12345678","subject_id":"run-12345678","enqueued_at":"2026-07-31T00:00:00Z"}`,
	)
	queue, err := NewYMQQueue(strings.NewReader(body), 1)
	if err != nil {
		t.Fatal(err)
	}
	message, err := queue.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if message.DeliveryCount != 3 || message.ReceiptHandle != "evt-1" {
		t.Fatalf("unexpected message: %+v", message)
	}
	if err := queue.Ack(context.Background(), message.ReceiptHandle); err != nil {
		t.Fatal(err)
	}
}

func TestYMQTriggerRetryFailsWholeInvocation(t *testing.T) {
	body := fmt.Sprintf(`{"messages":[{"event_metadata":{"event_id":"evt-1"},"details":{"message":{"body":%q}}}]}`,
		`{"schema":"sessionless.queue.v1","message_id":"msg-12345678","kind":"dispatch.run","tenant_id":"ten-12345678","subject_id":"run-12345678","enqueued_at":"2026-07-31T00:00:00Z"}`,
	)
	queue, err := NewYMQQueue(strings.NewReader(body), 1)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := queue.Receive(context.Background())
	if err := queue.Retry(context.Background(), message.ReceiptHandle, time.Second); !errors.Is(err, ErrRetryInvocation) {
		t.Fatalf("retry error = %v", err)
	}
}

func TestYMQTriggerRejectsTrailingJSON(t *testing.T) {
	body := fmt.Sprintf(`{"messages":[{"event_metadata":{"event_id":"evt-1"},"details":{"message":{"body":%q}}}]} {}`,
		`{"schema":"sessionless.queue.v1","message_id":"msg-12345678","kind":"dispatch.run","tenant_id":"ten-12345678","subject_id":"run-12345678","enqueued_at":"2026-07-31T00:00:00Z"}`,
	)
	if _, err := NewYMQQueue(strings.NewReader(body), 1); err == nil {
		t.Fatal("expected trailing JSON to be rejected")
	}
}
