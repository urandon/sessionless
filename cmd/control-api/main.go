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

	"gitcode.com/urandon/sessionless/internal/buildinfo"
	"gitcode.com/urandon/sessionless/internal/controlapi"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
	"gitcode.com/urandon/sessionless/internal/telegramingress"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const component = "control-api"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	address := ":" + envOrDefault("PORT", "8080")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	handler, closeDependencies, err := buildHandler(ctx, logger)
	if err != nil {
		logger.Error("control API configuration failed", "error", err)
		os.Exit(1)
	}
	defer closeDependencies()

	server := &http.Server{
		Addr:              address,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
		}
	}()

	logger.Info("starting HTTP server", "component", component, "address", address)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("HTTP server stopped", "error", err)
		os.Exit(1)
	}
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func buildHandler(
	ctx context.Context,
	logger *slog.Logger,
) (http.Handler, func(), error) {
	info := buildinfo.Current(component)
	webhookSecret := os.Getenv("TELEGRAM_WEBHOOK_SECRET")
	if webhookSecret == "" {
		logger.Warn("Telegram webhook is disabled", "reason", "TELEGRAM_WEBHOOK_SECRET is unset")
		return controlapi.NewHandler(logger, info), func() {}, nil
	}
	identity, err := telegramingress.NewIdentityResolver(
		[]byte(os.Getenv("TELEGRAM_IDENTITY_HMAC_KEY")),
	)
	if err != nil {
		return nil, func() {}, err
	}
	ydb, err := ydbclient.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		return nil, func() {}, fmt.Errorf("open YDB: %w", err)
	}
	closeYDB := func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := ydb.Close(closeCtx); err != nil {
			logger.Error("close YDB", "error", err)
		}
	}
	state, err := ydbstore.New(ydb.DB, ydbstore.Options{})
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	blobs, err := s3store.New(ctx, s3store.Config{
		Endpoint: os.Getenv("S3_ENDPOINT"), Region: os.Getenv("S3_REGION"),
		Bucket: os.Getenv("S3_BUCKET"), AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey:        os.Getenv("S3_SECRET_ACCESS_KEY"),
		ForcePathStyle:         envBool("S3_FORCE_PATH_STYLE"),
		IAMMetadataCredentials: envBool("S3_IAM_METADATA_CREDENTIALS"),
	})
	if err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("open object storage: %w", err)
	}
	fileClient, err := telegramingress.NewBotFileClient(
		envOrDefault("TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
		os.Getenv("TELEGRAM_BOT_TOKEN"), nil, 0,
	)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	queueConfig := func(queueURL string) sqsqueue.Config {
		return sqsqueue.Config{
			Endpoint: os.Getenv("QUEUE_ENDPOINT"), Region: envOrDefault("QUEUE_REGION", "ru-central1"),
			QueueURL:        queueURL,
			AccessKeyID:     firstNonEmpty(os.Getenv("OUTBOX_QUEUE_ACCESS_KEY_ID"), os.Getenv("QUEUE_ACCESS_KEY_ID")),
			SecretAccessKey: firstNonEmpty(os.Getenv("OUTBOX_QUEUE_SECRET_ACCESS_KEY"), os.Getenv("QUEUE_SECRET_ACCESS_KEY")),
		}
	}
	dispatchWakeQueue, err := sqsqueue.New(ctx, queueConfig(os.Getenv("SCHEDULER_WAKE_QUEUE_URL")))
	if err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("open scheduler wake queue: %w", err)
	}
	deliveryWakeQueue, err := sqsqueue.New(ctx, queueConfig(os.Getenv("DELIVERY_QUEUE_URL")))
	if err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("open delivery wake queue: %w", err)
	}
	dispatchWakePublisher, err := outboxwake.NewPublisher(dispatchWakeQueue)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	deliveryWakePublisher, err := outboxwake.NewPublisher(deliveryWakeQueue)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	processor, err := telegramingress.NewProcessor(
		telegramingress.ProcessorConfig{
			SourceID:              envOrDefault("TELEGRAM_SOURCE_ID", "bot-primary"),
			Provider:              envOrDefault("DEFAULT_COMPUTE_PROVIDER", "codex"),
			DispatchWakePublisher: dispatchWakePublisher,
			DeliveryWakePublisher: deliveryWakePublisher,
			WakePublishError: func(publishErr error) {
				logger.Warn("durable outbox wake publication deferred to recovery", "error", publishErr)
			},
		},
		identity, idgen.New(), systemClock{}, blobs, fileClient, state,
	)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	webhook, err := telegramingress.NewWebhook(webhookSecret, processor, logger)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	return controlapi.NewHandlerWithOptions(logger, info, controlapi.Options{
		TelegramWebhook: webhook,
	}), closeYDB, nil
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

func envBool(name string) bool {
	value, _ := strconv.ParseBool(os.Getenv(name))
	return value
}
