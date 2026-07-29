package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/telegramdelivery"
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
		SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"),
		ForcePathStyle:  envBool("S3_FORCE_PATH_STYLE"),
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
		telegramdelivery.Config{}, systemClock{}, state, client,
	)
	if err != nil {
		logger.Error("create Telegram sender", "error", err)
		os.Exit(1)
	}

	interval := envDuration("TELEGRAM_SENDER_POLL_INTERVAL", time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		processed, runErr := sender.RunOnce(ctx)
		if runErr != nil && ctx.Err() == nil {
			logger.Error("Telegram delivery pass failed", "error", runErr)
		} else if processed > 0 {
			logger.Info("Telegram delivery pass completed", "processed", processed)
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

func envBool(name string) bool {
	value, _ := strconv.ParseBool(os.Getenv(name))
	return value
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
