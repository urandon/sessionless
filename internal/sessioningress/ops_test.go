package sessioningress_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/syntheticfrontend"
)

type opsSinkFunc func(context.Context, opssignal.Record) error

func (f opsSinkFunc) Observe(ctx context.Context, r opssignal.Record) error { return f(ctx, r) }

type failedOpsWakeQueue struct {
	ports.Queue
	ctx   context.Context
	calls int
	err   error
}

func (q *failedOpsWakeQueue) Publish(ctx context.Context, e queuecontract.Envelope) error {
	q.calls++
	q.ctx = ctx
	return q.err
}

func TestOpsPostCommitWakeFailurePreservesCanonicalSuccessAndReplay(t *testing.T) {
	for _, cancelInSink := range []bool{false, true} {
		name := "sink_error"
		if cancelInSink {
			name = "context_canceled_after_commit"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store, blobs := newMemoryCanonicalStore(), newMemoryBlobs()
			queue := &failedOpsWakeQueue{err: errors.New("private-queue-url-secret")}
			observations := 0
			publisher, err := outboxwake.NewPublisher(queue, opsSinkFunc(func(got context.Context, r opssignal.Record) error {
				observations++
				if len(store.commits) != 1 || got != ctx || r.SignalID != opssignal.ControlWakeDeferred || r.Stage != opssignal.WakePublish {
					t.Errorf("wake observation before commit or wrong scope: commits=%d context=%v record=%+v", len(store.commits), got == ctx, r)
				}
				if cancelInSink {
					cancel()
				}
				return io.ErrShortWrite
			}))
			if err != nil {
				t.Fatal(err)
			}
			config := testIngressConfig("ops")
			config.DispatchWakePublisher = publisher
			service, err := sessioningress.New(config, store, blobs)
			if err != nil {
				t.Fatal(err)
			}
			input := sessioningress.UserInput{
				Actor:           sessioningress.Actor{TenantID: "tenant-a", UserID: "user-a", Frontend: syntheticfrontend.Frontend, ExternalConversationID: "private-conversation"},
				ExternalEventID: "private-delivery", ReceivedAt: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC), Text: "private-prompt", SubscriptionConnectionID: domain.SubscriptionConnectionID("subscription-a"),
			}
			first, err := service.Ingest(ctx, input)
			if err != nil || !first.Created || len(store.commits) != 1 || queue.calls != 1 || observations != 1 || queue.ctx != ctx {
				t.Fatalf("postcommit result=%+v err=%v commits=%d publishes=%d observations=%d", first, err, len(store.commits), queue.calls, observations)
			}
			if cancelInSink {
				if ctx.Err() != context.Canceled {
					t.Fatal("original cancellation not preserved")
				}
				return
			}
			objects := len(blobs.objects)
			replay, err := service.Ingest(ctx, input)
			if err != nil || replay.Created || replay.RunID != first.RunID || replay.EventID != first.EventID || len(store.commits) != 1 || len(blobs.objects) != objects || queue.calls != 2 || observations != 2 {
				t.Fatalf("replay changed durable outcome: result=%+v err=%v commits=%d objects=%d publishes=%d observations=%d", replay, err, len(store.commits), len(blobs.objects), queue.calls, observations)
			}
		})
	}
}
