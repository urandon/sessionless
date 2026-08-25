package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/portlog"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/scheduler"
	"gitcode.com/urandon/sessionless/internal/serverlesshttp"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
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
	defer ydb.Close(context.Background())
	state, err := ydbstore.New(ydb.DB, ydbstore.Options{})
	if err != nil {
		logger.Error("create state store", "error", err)
		os.Exit(1)
	}
	if err := state.RequireExecutionPlacementCutover(ctx); err != nil {
		logger.Error("require execution placement cutover", "error", err)
		os.Exit(1)
	}
	queue, err := sqsqueue.New(ctx, sqsqueue.Config{
		Endpoint:        os.Getenv("QUEUE_ENDPOINT"),
		Region:          envOrDefault("QUEUE_REGION", "ru-central1"),
		QueueURL:        os.Getenv("DISPATCH_QUEUE_URL"),
		DeadLetterURL:   os.Getenv("DEAD_LETTER_QUEUE_URL"),
		AccessKeyID:     os.Getenv("QUEUE_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("QUEUE_SECRET_ACCESS_KEY"),
	})
	if err != nil {
		logger.Error("create dispatch queue", "error", err)
		os.Exit(1)
	}
	limits := domain.ProductLimits{
		MaxTenantQueueDepth: envUint("LIMIT_TENANT_QUEUE_DEPTH", 8),
		MaxActiveRuns:       envUint("LIMIT_ACTIVE_RUNS", 1),
		MaxRuntime:          envDuration("LIMIT_RUNTIME", 15*time.Minute),
		MaxTurns:            envUint("LIMIT_TURNS", 30),
		MaxInputBytes:       uint64(envUint("LIMIT_INPUT_BYTES", 16<<20)),
		MaxContextBytes:     uint64(envUint("LIMIT_CONTEXT_BYTES", 64<<20)),
		MaxContextEvents:    uint64(envUint("LIMIT_CONTEXT_EVENTS", 512)),
		MaxArtifacts:        envUint("LIMIT_ARTIFACTS", 32),
		MaxToolEvents:       envUint("LIMIT_TOOL_EVENTS", 128),
		MaxToolEventBytes:   uint64(envUint("LIMIT_TOOL_EVENT_BYTES", 16<<20)),
	}
	snapshotBlobs, err := s3store.New(ctx, s3store.Config{
		Endpoint: os.Getenv("S3_ENDPOINT"), Region: envOrDefault("S3_REGION", "ru-central1"),
		Bucket: os.Getenv("S3_BUCKET"), AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey:        os.Getenv("S3_SECRET_ACCESS_KEY"),
		ForcePathStyle:         envBool("S3_FORCE_PATH_STYLE"),
		IAMMetadataCredentials: envBool("S3_IAM_METADATA_CREDENTIALS"),
		MaxObjectBytes:         int64(limits.MaxContextBytes),
	})
	if err != nil {
		logger.Error("create snapshot blob store", "error", err)
		os.Exit(1)
	}
	snapshotBuilder, err := sessioncontext.NewSnapshotBuilder(state, snapshotBlobs)
	if err != nil {
		logger.Error("create snapshot builder", "error", err)
		os.Exit(1)
	}
	snapshotMaintainer, err := sessioncontext.NewMaintainer(
		state, snapshotBuilder, sessioncontext.MaintenancePolicy{
			IntervalEvents: uint64(envUint("SNAPSHOT_INTERVAL_EVENTS", 128)),
			MaxEvents:      limits.MaxContextEvents,
			MaxBytes:       limits.MaxContextBytes,
			MaxVersions:    uint64(envUint("SNAPSHOT_MAX_VERSIONS", 32)),
		},
	)
	if err != nil {
		logger.Error("create snapshot maintainer", "error", err)
		os.Exit(1)
	}
	dispatcher, err := scheduler.NewDispatcher(
		scheduler.Config{
			BatchSize:            uint64(envUint("SCHEDULER_BATCH_SIZE", 25)),
			ReservationTTL:       envDuration("SCHEDULER_RESERVATION_TTL", 5*time.Minute),
			WakeRetryDelay:       envDuration("SCHEDULER_WAKE_RETRY_DELAY", time.Second),
			MaxWakeDeliveryCount: envUint("SCHEDULER_WAKE_MAX_DELIVERY_COUNT", 5),
			Limits:               limits,
			SnapshotMaintainer:   snapshotMaintainer,
			SnapshotObserver: func(cause error) {
				logger.Warn("snapshot maintenance deferred to canonical replay", "error", cause)
			},
			DefaultWorkload: domain.WorkloadShape{
				Runtime:      envDuration("DEFAULT_WORKLOAD_RUNTIME", 5*time.Minute),
				Turns:        envUint("DEFAULT_WORKLOAD_TURNS", 1),
				InputBytes:   uint64(envUint("DEFAULT_WORKLOAD_INPUT_BYTES", 1<<20)),
				ContextBytes: uint64(envUint("DEFAULT_WORKLOAD_CONTEXT_BYTES", 4<<20)),
				Artifacts:    envUint("DEFAULT_WORKLOAD_ARTIFACTS", 4),
			},
		},
		systemClock{},
		state,
		portlog.NewQueue(logger, "reconciler", queue),
	)
	if err != nil {
		logger.Error("create scheduler", "error", err)
		os.Exit(1)
	}
	if envBool("SERVERLESS_TRIGGER_HTTP") {
		err = serverlesshttp.Serve(
			ctx, ":"+envOrDefault("PORT", "8080"), logger,
			func(invocationCtx context.Context, request *http.Request) (any, error) {
				switch request.URL.Path {
				case "/wake":
					wakeQueue, parseErr := yandextriggers.NewYMQQueue(request.Body, 10)
					if parseErr != nil {
						return nil, parseErr
					}
					results := make([]scheduler.WakeResult, 0, wakeQueue.Remaining())
					for wakeQueue.Remaining() > 0 {
						result, runErr := dispatcher.RunWake(invocationCtx, wakeQueue)
						if runErr != nil {
							return nil, runErr
						}
						results = append(results, result)
					}
					return map[string]any{"processed": len(results), "results": results}, nil
				case "/recovery":
					result, runErr := dispatcher.RunOnce(invocationCtx)
					if runErr != nil {
						return nil, runErr
					}
					return result, nil
				default:
					return nil, fmt.Errorf("unsupported trigger path %q", request.URL.Path)
				}
			},
		)
		if err != nil {
			logger.Error("reconciler trigger server stopped", "error", err)
			os.Exit(1)
		}
		return
	}

	wakeQueue, err := sqsqueue.New(ctx, sqsqueue.Config{
		Endpoint: os.Getenv("QUEUE_ENDPOINT"), Region: envOrDefault("QUEUE_REGION", "ru-central1"),
		QueueURL:      os.Getenv("SCHEDULER_WAKE_QUEUE_URL"),
		DeadLetterURL: os.Getenv("SCHEDULER_WAKE_DEAD_LETTER_QUEUE_URL"),
		AccessKeyID:   os.Getenv("QUEUE_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("QUEUE_SECRET_ACCESS_KEY"),
		WaitTime: envDuration("SCHEDULER_QUEUE_WAIT", 2*time.Second),
	})
	if err != nil {
		logger.Error("create scheduler wake queue", "error", err)
		os.Exit(1)
	}
	if result, runErr := dispatcher.RunOnce(ctx); runErr != nil && ctx.Err() == nil {
		logger.Error("scheduler startup recovery failed", "error", runErr)
	} else if result.Considered > 0 || result.Expired > 0 {
		logger.Info("scheduler startup recovery completed", "considered", result.Considered, "expired", result.Expired)
	}
	for {
		result, runErr := dispatcher.RunWake(ctx, wakeQueue)
		if errors.Is(runErr, sqsqueue.ErrNoMessage) {
			continue
		}
		if runErr != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("scheduler wake failed", "error", runErr)
			if !waitForRetry(ctx, time.Second) {
				return
			}
			continue
		}
		logger.Info("scheduler wake completed", "outcome", result.Outcome, "code", result.Code)
	}
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
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

func envBool(name string) bool {
	value, _ := strconv.ParseBool(os.Getenv(name))
	return value
}

func envUint(name string, fallback uint32) uint32 {
	value, err := strconv.ParseUint(os.Getenv(name), 10, 32)
	if err != nil || value == 0 {
		return fallback
	}
	return uint32(value)
}
