package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/portlog"
	"gitcode.com/urandon/sessionless/internal/scheduler"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
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
	dispatcher, err := scheduler.NewDispatcher(
		scheduler.Config{
			BatchSize:      uint64(envUint("SCHEDULER_BATCH_SIZE", 25)),
			ReservationTTL: envDuration("SCHEDULER_RESERVATION_TTL", 5*time.Minute),
			Limits: domain.ProductLimits{
				MaxTenantQueueDepth: envUint("LIMIT_TENANT_QUEUE_DEPTH", 8),
				MaxActiveRuns:       envUint("LIMIT_ACTIVE_RUNS", 1),
				MaxRuntime:          envDuration("LIMIT_RUNTIME", 15*time.Minute),
				MaxTurns:            envUint("LIMIT_TURNS", 30),
				MaxInputBytes:       uint64(envUint("LIMIT_INPUT_BYTES", 16<<20)),
				MaxContextBytes:     uint64(envUint("LIMIT_CONTEXT_BYTES", 64<<20)),
				MaxArtifacts:        envUint("LIMIT_ARTIFACTS", 32),
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

	interval := envDuration("SCHEDULER_POLL_INTERVAL", time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		result, runErr := dispatcher.RunOnce(ctx)
		if runErr != nil && ctx.Err() == nil {
			logger.Error("scheduler pass failed", "error", runErr)
		} else if result.Considered > 0 || result.Expired > 0 {
			logger.Info(
				"scheduler pass completed",
				"considered", result.Considered,
				"admitted", result.Admitted,
				"published", result.Published,
				"blocked", result.Blocked,
				"expired", result.Expired,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
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

func envUint(name string, fallback uint32) uint32 {
	value, err := strconv.ParseUint(os.Getenv(name), 10, 32)
	if err != nil || value == 0 {
		return fallback
	}
	return uint32(value)
}
