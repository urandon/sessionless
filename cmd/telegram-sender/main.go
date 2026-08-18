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

	"gitcode.com/urandon/sessionless/internal/portlog"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/serverlesshttp"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
	"gitcode.com/urandon/sessionless/internal/telegramdelivery"
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
	blobs, err := s3store.New(ctx, s3store.Config{
		Endpoint: os.Getenv("S3_ENDPOINT"), Region: os.Getenv("S3_REGION"),
		Bucket: os.Getenv("S3_BUCKET"), AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey:        os.Getenv("S3_SECRET_ACCESS_KEY"),
		ForcePathStyle:         envBool("S3_FORCE_PATH_STYLE"),
		IAMMetadataCredentials: envBool("S3_IAM_METADATA_CREDENTIALS"),
	})
	if err != nil {
		logger.Error("open object storage", "error", err)
		os.Exit(1)
	}
	client, err := telegramdelivery.NewClient(
		envOrDefault("TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
		os.Getenv("TELEGRAM_BOT_TOKEN"),
		&http.Client{Timeout: 30 * time.Second},
		blobs,
	)
	if err != nil {
		logger.Error("create Telegram client", "error", err)
		os.Exit(1)
	}
	sender, err := telegramdelivery.NewSender(
		telegramdelivery.Config{
			BatchSize:   uint64(envUint("TELEGRAM_SENDER_BATCH_SIZE", 25)),
			MaxAttempts: envUint("TELEGRAM_SENDER_MAX_ATTEMPTS", 5),
			BaseBackoff: envDuration("TELEGRAM_SENDER_BASE_BACKOFF", 5*time.Second),
			MaxBackoff:  envDuration("TELEGRAM_SENDER_MAX_BACKOFF", 5*time.Minute),
		},
		systemClock{}, state, blobs, portlog.NewTelegramClient(logger, client),
	)
	if err != nil {
		logger.Error("create Telegram sender", "error", err)
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
					results := make([]telegramdelivery.WakeResult, 0, wakeQueue.Remaining())
					for wakeQueue.Remaining() > 0 {
						result, runErr := sender.RunWake(invocationCtx, wakeQueue)
						if runErr != nil {
							return nil, runErr
						}
						results = append(results, result)
					}
					return map[string]any{"processed": len(results), "results": results}, nil
				case "/recovery":
					processed, runErr := sender.RunOnce(invocationCtx)
					if runErr != nil {
						return nil, runErr
					}
					return map[string]int{"processed": processed}, nil
				default:
					return nil, fmt.Errorf("unsupported trigger path %q", request.URL.Path)
				}
			},
		)
		if err != nil {
			logger.Error("Telegram sender trigger server stopped", "error", err)
			os.Exit(1)
		}
		return
	}

	wakeQueue, err := sqsqueue.New(ctx, sqsqueue.Config{
		Endpoint: os.Getenv("QUEUE_ENDPOINT"), Region: envOrDefault("QUEUE_REGION", "ru-central1"),
		QueueURL:      os.Getenv("DELIVERY_QUEUE_URL"),
		DeadLetterURL: os.Getenv("DELIVERY_DEAD_LETTER_QUEUE_URL"),
		AccessKeyID:   os.Getenv("QUEUE_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("QUEUE_SECRET_ACCESS_KEY"),
		WaitTime: envDuration("TELEGRAM_SENDER_QUEUE_WAIT", 2*time.Second),
	})
	if err != nil {
		logger.Error("create Telegram delivery wake queue", "error", err)
		os.Exit(1)
	}
	if processed, runErr := sender.RunOnce(ctx); runErr != nil && ctx.Err() == nil {
		logger.Error("Telegram delivery startup recovery failed", "error", runErr)
	} else if processed > 0 {
		logger.Info("Telegram delivery startup recovery completed", "processed", processed)
	}
	for {
		result, runErr := sender.RunWake(ctx, wakeQueue)
		if errors.Is(runErr, sqsqueue.ErrNoMessage) {
			continue
		}
		if runErr != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("Telegram delivery wake failed", "error", runErr)
			if !waitForRetry(ctx, time.Second) {
				return
			}
			continue
		}
		logger.Info("Telegram delivery wake completed", "outcome", result.Outcome, "code", result.Code)
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

func envDuration(name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
