// Package yandextriggers adapts Yandex Cloud trigger event payloads to the
// repository's transport-neutral ports.
package yandextriggers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
)

var (
	ErrNoMessage         = errors.New("trigger batch contains no remaining messages")
	ErrRetryInvocation   = errors.New("trigger invocation must be retried")
	ErrDeadLetterPending = errors.New("message must be retried until YMQ redrive moves it to the DLQ")
)

type ymqEvent struct {
	Messages []struct {
		EventMetadata struct {
			EventID string `json:"event_id"`
		} `json:"event_metadata"`
		Details struct {
			Message struct {
				MessageID  string            `json:"message_id"`
				Body       string            `json:"body"`
				Attributes map[string]string `json:"attributes"`
			} `json:"message"`
		} `json:"details"`
	} `json:"messages"`
}

// Queue represents one YMQ trigger batch. Acknowledgement is implicit in the
// HTTP 2xx returned for the whole invocation; retry and dead-letter requests
// deliberately fail the invocation so YMQ retains ownership of redrive.
type Queue struct {
	messages []ports.ReceivedMessage
	next     int
	inflight map[string]struct{}
}

func NewYMQQueue(body io.Reader, maxMessages int) (*Queue, error) {
	if maxMessages <= 0 {
		return nil, fmt.Errorf("maximum trigger batch size must be positive")
	}
	decoder := json.NewDecoder(io.LimitReader(body, 1<<20))
	var event ymqEvent
	if err := decoder.Decode(&event); err != nil {
		return nil, fmt.Errorf("decode YMQ trigger event: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("decode YMQ trigger event: multiple JSON values")
		}
		return nil, fmt.Errorf("decode YMQ trigger event trailer: %w", err)
	}
	if len(event.Messages) == 0 || len(event.Messages) > maxMessages {
		return nil, fmt.Errorf("YMQ trigger batch must contain between 1 and %d messages", maxMessages)
	}
	queue := &Queue{inflight: make(map[string]struct{}, len(event.Messages))}
	for index, value := range event.Messages {
		envelope, err := queuecontract.Decode([]byte(value.Details.Message.Body))
		if err != nil {
			return nil, fmt.Errorf("decode YMQ message %d: %w", index, err)
		}
		receipt := strings.TrimSpace(value.EventMetadata.EventID)
		if receipt == "" {
			receipt = strings.TrimSpace(value.Details.Message.MessageID)
		}
		if receipt == "" {
			return nil, fmt.Errorf("YMQ message %d has no event or message ID", index)
		}
		deliveryCount := uint32(1)
		if raw := value.Details.Message.Attributes["ApproximateReceiveCount"]; raw != "" {
			parsed, parseErr := strconv.ParseUint(raw, 10, 32)
			if parseErr != nil || parsed == 0 {
				return nil, fmt.Errorf("parse YMQ message %d delivery count", index)
			}
			deliveryCount = uint32(parsed)
		}
		queue.messages = append(queue.messages, ports.ReceivedMessage{
			Envelope: envelope, ReceiptHandle: receipt, DeliveryCount: deliveryCount,
		})
	}
	return queue, nil
}

func (queue *Queue) Remaining() int { return len(queue.messages) - queue.next }

func (queue *Queue) Publish(context.Context, queuecontract.Envelope) error {
	return fmt.Errorf("YMQ trigger adapter cannot publish")
}

func (queue *Queue) Receive(context.Context) (ports.ReceivedMessage, error) {
	if queue.next >= len(queue.messages) {
		return ports.ReceivedMessage{}, ErrNoMessage
	}
	message := queue.messages[queue.next]
	queue.next++
	queue.inflight[message.ReceiptHandle] = struct{}{}
	return message, nil
}

func (queue *Queue) Ack(_ context.Context, receiptHandle string) error {
	if _, ok := queue.inflight[receiptHandle]; !ok {
		return fmt.Errorf("trigger receipt is not inflight")
	}
	delete(queue.inflight, receiptHandle)
	return nil
}

func (queue *Queue) Retry(_ context.Context, receiptHandle string, _ time.Duration) error {
	if _, ok := queue.inflight[receiptHandle]; !ok {
		return fmt.Errorf("trigger receipt is not inflight")
	}
	return ErrRetryInvocation
}

func (queue *Queue) DeadLetter(_ context.Context, receiptHandle, _ string) error {
	if _, ok := queue.inflight[receiptHandle]; !ok {
		return fmt.Errorf("trigger receipt is not inflight")
	}
	return ErrDeadLetterPending
}

var _ ports.Queue = (*Queue)(nil)
