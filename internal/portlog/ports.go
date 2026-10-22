// Package portlog decorates process-boundary ports with structured correlation
// logs. Domain and adapter packages remain independent of a logging framework.
package portlog

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
)

type Queue struct {
	logger    *slog.Logger
	component string
	next      ports.Queue
	ops       opssignal.Sink
}

func NewQueue(logger *slog.Logger, component string, next ports.Queue, signals ...opssignal.Sink) *Queue {
	var sink opssignal.Sink
	if len(signals) != 0 {
		sink = signals[0]
	}
	return &Queue{logger: logger, component: component, next: next, ops: sink}
}

func (queue *Queue) Publish(ctx context.Context, envelope queuecontract.Envelope) error {
	err := queue.next.Publish(ctx, envelope)
	queue.observe(ctx, opssignal.QueuePublish, err)
	queue.log("queue message published", envelope, err)
	return err
}

func (queue *Queue) Receive(ctx context.Context) (ports.ReceivedMessage, error) {
	message, err := queue.next.Receive(ctx)
	queue.observe(ctx, opssignal.QueueReceive, err)
	if err == nil {
		queue.log("queue message received", message.Envelope, nil)
	}
	return message, err
}

func (queue *Queue) Ack(ctx context.Context, receiptHandle string) error {
	err := queue.next.Ack(ctx, receiptHandle)
	queue.observe(ctx, opssignal.QueueAck, err)
	return err
}

func (queue *Queue) Retry(
	ctx context.Context,
	receiptHandle string,
	delay time.Duration,
) error {
	err := queue.next.Retry(ctx, receiptHandle, delay)
	queue.observe(ctx, opssignal.QueueRetry, err)
	return err
}

func (queue *Queue) DeadLetter(
	ctx context.Context,
	receiptHandle string,
	reasonCode string,
) error {
	err := queue.next.DeadLetter(ctx, receiptHandle, reasonCode)
	queue.observe(ctx, opssignal.QueueDeadLetter, err)
	return err
}

func (queue *Queue) observe(ctx context.Context, stage opssignal.Stage, err error) {
	// Empty receives and trigger-owned retry/redrive requests are transport
	// control flow, not failed queue infrastructure. Never inspect error text.
	if err == nil || ports.IsQueueControlFlow(err) {
		return
	}
	class := opssignal.Queue
	if errors.Is(err, context.DeadlineExceeded) {
		class = opssignal.DeadlineExceeded
	}
	opssignal.Emit(ctx, queue.ops, opssignal.ControlQueueFailure, stage, class)
}

func (queue *Queue) log(message string, envelope queuecontract.Envelope, err error) {
	attributes := []any{
		"component", queue.component,
		"message_id", envelope.MessageID,
		"message_kind", envelope.Kind,
		"tenant_id", envelope.TenantID,
		"run_id", envelope.SubjectID,
	}
	if err != nil {
		attributes = append(attributes, "error", err)
		queue.logger.Error(message, attributes...)
		return
	}
	queue.logger.Info(message, attributes...)
}

type TelegramClient struct {
	logger *slog.Logger
	next   ports.TelegramClient
}

func NewTelegramClient(logger *slog.Logger, next ports.TelegramClient) *TelegramClient {
	return &TelegramClient{logger: logger, next: next}
}

func (client *TelegramClient) Send(
	ctx context.Context,
	request ports.TelegramSendRequest,
) (ports.TelegramSendResult, error) {
	result, err := client.next.Send(ctx, request)
	attributes := []any{
		"tenant_id", request.TenantID,
		"run_id", request.RunID,
		"delivery_id", request.DeliveryID,
		"chat_id", request.Chat.ChatID,
		"artifact_count", len(request.Artifacts),
	}
	if err != nil {
		attributes = append(attributes, "error", err)
		client.logger.Error("Telegram delivery failed", attributes...)
		return result, err
	}
	attributes = append(attributes, "telegram_message_id", result.MessageID)
	client.logger.Info("Telegram delivery sent", attributes...)
	return result, nil
}

var (
	_ ports.Queue          = (*Queue)(nil)
	_ ports.TelegramClient = (*TelegramClient)(nil)
)
