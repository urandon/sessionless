package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/deterministicharness"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/portlog"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/serverlesshttp"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/yandextriggers"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ydb, err := ydbclient.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		logger.Error("open YDB", "error", err)
		os.Exit(1)
	}
	deliveryWakeQueue, err := sqsqueue.New(ctx, sqsqueue.Config{
		Endpoint: os.Getenv("QUEUE_ENDPOINT"), Region: envOrDefault("QUEUE_REGION", "ru-central1"),
		QueueURL:        os.Getenv("DELIVERY_QUEUE_URL"),
		AccessKeyID:     firstNonEmpty(os.Getenv("OUTBOX_QUEUE_ACCESS_KEY_ID"), os.Getenv("QUEUE_ACCESS_KEY_ID")),
		SecretAccessKey: firstNonEmpty(os.Getenv("OUTBOX_QUEUE_SECRET_ACCESS_KEY"), os.Getenv("QUEUE_SECRET_ACCESS_KEY")),
	})
	if err != nil {
		logger.Error("create delivery wake queue", "error", err)
		os.Exit(1)
	}
	deliveryWakePublisher, err := outboxwake.NewPublisher(deliveryWakeQueue)
	if err != nil {
		logger.Error("create delivery wake publisher", "error", err)
		os.Exit(1)
	}
	defer ydb.Close(context.Background())
	state, err := ydbstore.New(ydb.DB, ydbstore.Options{})
	if err != nil {
		logger.Error("create worker state store", "error", err)
		os.Exit(1)
	}
	blobs, err := s3store.New(ctx, s3store.Config{
		Endpoint: os.Getenv("S3_ENDPOINT"), Region: envOrDefault("S3_REGION", "ru-central1"),
		Bucket: os.Getenv("S3_BUCKET"), AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey:        os.Getenv("S3_SECRET_ACCESS_KEY"),
		ForcePathStyle:         envBool("S3_FORCE_PATH_STYLE"),
		IAMMetadataCredentials: envBool("S3_IAM_METADATA_CREDENTIALS"),
		MaxObjectBytes:         int64(envUint64("WORKER_MAX_BLOB_BYTES", 64<<20)),
	})
	if err != nil {
		logger.Error("create worker blob store", "error", err)
		os.Exit(1)
	}
	harness, err := deterministicharness.New(deterministicharness.Config{
		Turns:               envUint64("DETERMINISTIC_HARNESS_TURNS", 2),
		Artifacts:           envUint64("DETERMINISTIC_HARNESS_ARTIFACTS", 1),
		FailBeforeFirstTurn: envBool("DETERMINISTIC_HARNESS_FAIL_BEFORE_FIRST_TURN"),
		FailAtTurn:          envUint64("DETERMINISTIC_HARNESS_FAIL_AT_TURN", 0),
		RetryableFail:       envBool("DETERMINISTIC_HARNESS_RETRYABLE_FAIL"),
	})
	if err != nil {
		logger.Error("create deterministic harness", "error", err)
		os.Exit(1)
	}
	managerConfig := worker.Config{
		ScratchRoot: envOrDefault("WORKER_SCRATCH_ROOT", "/tmp/sessionless-worker"),
		WorkerID:    envOrDefault("WORKER_ID", defaultWorkerID()),
		LeaseTTL:    envDuration("WORKER_LEASE_TTL", 2*time.Minute),
		RetryDelay:  envDuration("WORKER_RETRY_DELAY", 5*time.Second),
		RetryObserver: func(cause error) {
			logger.Warn("worker invocation scheduled for retry", "error", cause)
		},
		MaxDeliveryCount:      uint32(envUint64("WORKER_MAX_DELIVERY_COUNT", 5)),
		MaxMaterializedBytes:  int64(envUint64("WORKER_MAX_BLOB_BYTES", 64<<20)),
		MaxSnapshotFallbacks:  uint32(envUint64("WORKER_MAX_SNAPSHOT_FALLBACKS", 4)),
		DeliveryWakePublisher: deliveryWakePublisher,
	}
	newManager := func(queue ports.Queue) (*worker.Manager, error) {
		return worker.New(
			managerConfig, systemClock{}, portlog.NewQueue(logger, "worker-runtime", queue),
			state, blobs, harness,
		)
	}
	if envBool("SERVERLESS_TRIGGER_HTTP") {
		err = serverlesshttp.Serve(
			ctx, ":"+envOrDefault("PORT", "8080"), logger,
			func(invocationCtx context.Context, request *http.Request) (any, error) {
				queue, parseErr := yandextriggers.NewYMQQueue(request.Body, 10)
				if parseErr != nil {
					return nil, parseErr
				}
				manager, managerErr := newManager(queue)
				if managerErr != nil {
					return nil, managerErr
				}
				outcomes := make([]worker.Outcome, 0, queue.Remaining())
				for queue.Remaining() > 0 {
					outcome, runErr := manager.RunOnce(invocationCtx)
					if runErr != nil {
						return nil, runErr
					}
					outcomes = append(outcomes, outcome)
				}
				return map[string]any{"processed": len(outcomes), "outcomes": outcomes}, nil
			},
		)
		if err != nil {
			logger.Error("worker trigger server stopped", "error", err)
			os.Exit(1)
		}
		return
	}

	queue, err := sqsqueue.New(ctx, sqsqueue.Config{
		Endpoint: os.Getenv("QUEUE_ENDPOINT"), Region: envOrDefault("QUEUE_REGION", "ru-central1"),
		QueueURL: os.Getenv("DISPATCH_QUEUE_URL"), DeadLetterURL: os.Getenv("DEAD_LETTER_QUEUE_URL"),
		AccessKeyID: os.Getenv("QUEUE_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("QUEUE_SECRET_ACCESS_KEY"),
		WaitTime: envDuration("WORKER_QUEUE_WAIT", 2*time.Second),
	})
	if err != nil {
		logger.Error("create worker queue", "error", err)
		os.Exit(1)
	}
	manager, err := newManager(queue)
	if err != nil {
		logger.Error("create worker manager", "error", err)
		os.Exit(1)
	}
	outcome, err := manager.RunOnce(ctx)
	if errors.Is(err, sqsqueue.ErrNoMessage) {
		logger.Info("worker queue empty")
		return
	}
	if err != nil {
		logger.Error("worker invocation failed", "error", err)
		os.Exit(1)
	}
	logger.Info("worker invocation finished", "outcome", outcome)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envUint64(name string, fallback uint64) uint64 {
	value, err := strconv.ParseUint(os.Getenv(name), 10, 64)
	if err != nil {
		return fallback
	}
	return value
}

func envBool(name string) bool {
	value, _ := strconv.ParseBool(os.Getenv(name))
	return value
}

func defaultWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return "worker-runtime"
	}
	return "worker-runtime-" + hostname
}
