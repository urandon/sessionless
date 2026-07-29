// Package testkit contains deterministic in-memory implementations of core
// ports. Production packages must not import it.
package testkit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
)

type FakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

func NewFakeClock(now time.Time) *FakeClock {
	return &FakeClock{now: now}
}

func (clock *FakeClock) Now() time.Time {
	clock.mu.RLock()
	defer clock.mu.RUnlock()
	return clock.now
}

func (clock *FakeClock) Advance(delta time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(delta)
}

type SequenceIDGenerator struct {
	mu     sync.Mutex
	prefix string
	next   map[ports.IDKind]uint64
}

func NewSequenceIDGenerator(prefix string) *SequenceIDGenerator {
	return &SequenceIDGenerator{
		prefix: prefix,
		next:   make(map[ports.IDKind]uint64),
	}
}

func (generator *SequenceIDGenerator) NewID(ctx context.Context, kind ports.IDKind) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	generator.mu.Lock()
	defer generator.mu.Unlock()
	generator.next[kind]++
	return fmt.Sprintf("%s%s-%04d", generator.prefix, kind, generator.next[kind]), nil
}

type retryRecord struct {
	Envelope queuecontract.Envelope
	Delay    time.Duration
	Count    uint32
}

type MemoryQueue struct {
	mu          sync.Mutex
	ready       []retryRecord
	inflight    map[string]retryRecord
	deadLetters []retryRecord
	nextReceipt uint64
}

func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{inflight: make(map[string]retryRecord)}
}

func (queue *MemoryQueue) Publish(ctx context.Context, envelope queuecontract.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := envelope.Validate(); err != nil {
		return err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.ready = append(queue.ready, retryRecord{Envelope: envelope})
	return nil
}

func (queue *MemoryQueue) Receive(ctx context.Context) (ports.ReceivedMessage, error) {
	if err := ctx.Err(); err != nil {
		return ports.ReceivedMessage{}, err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.ready) == 0 {
		return ports.ReceivedMessage{}, &NoMessageError{}
	}
	record := queue.ready[0]
	queue.ready = queue.ready[1:]
	record.Count++
	queue.nextReceipt++
	receipt := fmt.Sprintf("receipt-%04d", queue.nextReceipt)
	queue.inflight[receipt] = record
	return ports.ReceivedMessage{
		Envelope:      record.Envelope,
		ReceiptHandle: receipt,
		DeliveryCount: record.Count,
	}, nil
}

func (queue *MemoryQueue) Ack(ctx context.Context, receiptHandle string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if _, ok := queue.inflight[receiptHandle]; !ok {
		return fmt.Errorf("ack %q: receipt not found", receiptHandle)
	}
	delete(queue.inflight, receiptHandle)
	return nil
}

func (queue *MemoryQueue) Retry(ctx context.Context, receiptHandle string, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay < 0 {
		return fmt.Errorf("retry delay must not be negative")
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	record, ok := queue.inflight[receiptHandle]
	if !ok {
		return fmt.Errorf("retry %q: receipt not found", receiptHandle)
	}
	delete(queue.inflight, receiptHandle)
	record.Delay = delay
	queue.ready = append(queue.ready, record)
	return nil
}

func (queue *MemoryQueue) DeadLetter(ctx context.Context, receiptHandle, reasonCode string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if reasonCode == "" {
		return fmt.Errorf("dead-letter reason code must not be empty")
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	record, ok := queue.inflight[receiptHandle]
	if !ok {
		return fmt.Errorf("dead-letter %q: receipt not found", receiptHandle)
	}
	delete(queue.inflight, receiptHandle)
	queue.deadLetters = append(queue.deadLetters, record)
	return nil
}

func (queue *MemoryQueue) DeadLetterCount() int {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return len(queue.deadLetters)
}

type NoMessageError struct{}

func (*NoMessageError) Error() string { return "no queue message available" }

var (
	_ ports.Clock       = (*FakeClock)(nil)
	_ ports.IDGenerator = (*SequenceIDGenerator)(nil)
	_ ports.Queue       = (*MemoryQueue)(nil)
)
