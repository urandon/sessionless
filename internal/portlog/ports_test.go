package portlog

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

func TestQueueLogsStableCorrelationFields(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	queue := NewQueue(logger, "worker-runtime", testkit.NewMemoryQueue())
	envelope := queuecontract.Envelope{
		Schema: queuecontract.SchemaV1, MessageID: "message-1",
		Kind: queuecontract.KindDispatchRun, TenantID: "tenant-a",
		SubjectID: "run-a", EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Receive(context.Background()); err != nil {
		t.Fatal(err)
	}
	logged := output.String()
	for _, wanted := range []string{
		`"component":"worker-runtime"`,
		`"message_id":"message-1"`,
		`"tenant_id":"tenant-a"`,
		`"run_id":"run-a"`,
	} {
		if !strings.Contains(logged, wanted) {
			t.Fatalf("log output %q does not contain %q", logged, wanted)
		}
	}
}

func TestTelegramClientLogsRunAndDelivery(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	client := NewTelegramClient(logger, telegramStub{})
	_, err := client.Send(context.Background(), ports.TelegramSendRequest{
		TenantID: "tenant-a", RunID: "run-a", DeliveryID: "delivery-a",
		Chat: domain.TelegramChatRef{TenantID: "tenant-a", ChatID: 123},
	})
	if err != nil {
		t.Fatal(err)
	}
	logged := output.String()
	for _, wanted := range []string{
		`"tenant_id":"tenant-a"`,
		`"run_id":"run-a"`,
		`"delivery_id":"delivery-a"`,
	} {
		if !strings.Contains(logged, wanted) {
			t.Fatalf("log output %q does not contain %q", logged, wanted)
		}
	}
}

type telegramStub struct{}

func (telegramStub) Send(
	context.Context,
	ports.TelegramSendRequest,
) (ports.TelegramSendResult, error) {
	return ports.TelegramSendResult{MessageID: 456, SentAt: time.Now().UTC()}, nil
}
