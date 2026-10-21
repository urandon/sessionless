package portlog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
)

type opsSinkFunc func(context.Context, opssignal.Record) error

func (f opsSinkFunc) Observe(ctx context.Context, r opssignal.Record) error { return f(ctx, r) }

type opsQueue struct {
	err    error
	seen   context.Context
	calls  int
	delay  time.Duration
	reason string
}

func (q *opsQueue) called(ctx context.Context) error                            { q.seen = ctx; q.calls++; return q.err }
func (q *opsQueue) Publish(ctx context.Context, _ queuecontract.Envelope) error { return q.called(ctx) }
func (q *opsQueue) Receive(ctx context.Context) (ports.ReceivedMessage, error) {
	return ports.ReceivedMessage{ReceiptHandle: "private-receipt"}, q.called(ctx)
}
func (q *opsQueue) Ack(ctx context.Context, _ string) error { return q.called(ctx) }
func (q *opsQueue) Retry(ctx context.Context, _ string, delay time.Duration) error {
	q.delay = delay
	return q.called(ctx)
}
func (q *opsQueue) DeadLetter(ctx context.Context, _, reason string) error {
	q.reason = reason
	return q.called(ctx)
}

func TestOpsQueueFailuresPreserveOperationAndContext(t *testing.T) {
	for _, stage := range []opssignal.Stage{opssignal.QueuePublish, opssignal.QueueReceive, opssignal.QueueAck, opssignal.QueueRetry, opssignal.QueueDeadLetter} {
		t.Run(string(stage), func(t *testing.T) {
			ctx, cancel := context.WithDeadline(context.Background(), time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))
			defer cancel()
			original := errors.New("private-url/token/tenant/run")
			next := &opsQueue{err: original}
			observed := 0
			queue := NewQueue(slog.New(slog.NewTextHandler(io.Discard, nil)), "private-component", next, opsSinkFunc(func(got context.Context, record opssignal.Record) error {
				observed++
				if got != ctx || record.SignalID != opssignal.ControlQueueFailure || record.Stage != stage || record.ErrorClass != opssignal.Queue {
					t.Errorf("observation ctx preserved=%v, record=%+v", got == ctx, record)
				}
				return errors.New("sink full")
			}))
			var err error
			switch stage {
			case opssignal.QueuePublish:
				err = queue.Publish(ctx, queuecontract.Envelope{})
			case opssignal.QueueReceive:
				var got ports.ReceivedMessage
				got, err = queue.Receive(ctx)
				if got.ReceiptHandle != "private-receipt" {
					t.Fatalf("receive result lost: %+v", got)
				}
			case opssignal.QueueAck:
				err = queue.Ack(ctx, "private-receipt")
			case opssignal.QueueRetry:
				err = queue.Retry(ctx, "private-receipt", 17*time.Second)
			case opssignal.QueueDeadLetter:
				err = queue.DeadLetter(ctx, "private-receipt", "private-reason")
			}
			if err != original || next.calls != 1 || next.seen != ctx || observed != 1 {
				t.Fatalf("err=%v original=%v calls=%d context=%v observations=%d", err, original, next.calls, next.seen == ctx, observed)
			}
			if stage == opssignal.QueueRetry && next.delay != 17*time.Second {
				t.Fatal("retry delay changed")
			}
			if stage == opssignal.QueueDeadLetter && next.reason != "private-reason" {
				t.Fatal("redrive reason changed")
			}
		})
	}
}

func TestOpsQueueExpectedControlFlowIsNotOutage(t *testing.T) {
	for _, expected := range []error{nil, context.Canceled, ports.ErrQueueEmpty, ports.ErrTriggerQueueEmpty, ports.ErrQueueRetryRequested, ports.ErrQueueRedrivePending} {
		next := &opsQueue{err: expected}
		queue := NewQueue(slog.New(slog.NewTextHandler(io.Discard, nil)), "ignored", next, opsSinkFunc(func(context.Context, opssignal.Record) error {
			t.Errorf("expected outcome emitted an outage: %v", expected)
			return nil
		}))
		_, err := queue.Receive(context.Background())
		if err != expected || next.calls != 1 {
			t.Fatalf("expected=%v got=%v calls=%d", expected, err, next.calls)
		}
	}
}

func TestOpsQueueDelayedObservationDoesNotReplaceContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	next := &opsQueue{err: errors.New("queue failed")}
	queue := NewQueue(slog.New(slog.NewTextHandler(io.Discard, nil)), "ignored", next, opsSinkFunc(func(got context.Context, _ opssignal.Record) error {
		if got != ctx {
			t.Fatal("observer got detached context")
		}
		cancel() // deterministic stand-in for expiry while the OS write blocks
		return nil
	}))
	err := queue.Publish(ctx, queuecontract.Envelope{})
	if err != next.err || next.seen != ctx || ctx.Err() != context.Canceled || next.calls != 1 {
		t.Fatalf("observation changed authority/result: err=%v ctx=%v calls=%d", err, ctx.Err(), next.calls)
	}
}
