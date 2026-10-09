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
	"strings"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/buildinfo"
	"gitcode.com/urandon/sessionless/internal/controlapi"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
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
	attached, err := attachedControlConfigFromEnv(os.Getenv)
	if err != nil {
		return nil, func() {}, err
	}
	webhookSecret := os.Getenv("TELEGRAM_WEBHOOK_SECRET")
	if webhookSecret == "" {
		logger.Warn("Telegram webhook is disabled", "reason", "TELEGRAM_WEBHOOK_SECRET is unset")
	}
	if webhookSecret == "" && !attached.enabled {
		return controlapi.NewHandler(logger, info), func() {}, nil
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
	// Every unsuccessful startup after opening YDB owns its cleanup. On
	// success that ownership transfers to main's shutdown callback.
	ready := false
	defer func() {
		if !ready {
			closeYDB()
		}
	}()
	state, err := ydbstore.New(ydb.DB, ydbstore.Options{})
	if err != nil {
		return nil, func() {}, err
	}
	if err := state.RequireExecutionPlacementCutover(ctx); err != nil {
		return nil, func() {}, fmt.Errorf("require execution placement cutover: %w", err)
	}
	if err := state.RequireHarnessBindingCutover(ctx); err != nil {
		return nil, func() {}, fmt.Errorf("require harness binding cutover: %w", err)
	}
	if err := state.RequireManagedExecutionAuthorityV2Cutover(ctx); err != nil {
		return nil, func() {}, fmt.Errorf("require managed execution authority v2 cutover: %w", err)
	}
	blobs, err := s3store.New(ctx, s3store.Config{
		Endpoint: os.Getenv("S3_ENDPOINT"), Region: os.Getenv("S3_REGION"),
		Bucket: os.Getenv("S3_BUCKET"), AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey:        os.Getenv("S3_SECRET_ACCESS_KEY"),
		ForcePathStyle:         envBool("S3_FORCE_PATH_STYLE"),
		IAMMetadataCredentials: envBool("S3_IAM_METADATA_CREDENTIALS"),
	})
	if err != nil {
		return nil, func() {}, fmt.Errorf("open object storage: %w", err)
	}
	options := controlapi.Options{}
	if attached.enabled {
		options, err = controlapi.AttachedWorkerOptions(attached.audience, state, blobs)
		if err != nil {
			return nil, func() {}, err
		}
	}
	if webhookSecret != "" {
		options.TelegramWebhook, err = buildTelegramWebhook(ctx, logger, webhookSecret, state, blobs)
		if err != nil {
			return nil, func() {}, err
		}
	}
	ready = true
	return controlapi.NewHandlerWithOptions(logger, info, options), closeYDB, nil
}

// Telegram alone owns its bot identity and queues. Attached-only startup
// neither reads nor validates that optional frontend's configuration.
func buildTelegramWebhook(ctx context.Context, logger *slog.Logger, webhookSecret string,
	state *ydbstore.Store, blobs *s3store.Store,
) (http.Handler, error) {
	identityKey := []byte(os.Getenv("TELEGRAM_IDENTITY_HMAC_KEY"))
	identity, err := telegramingress.NewIdentityResolver(identityKey)
	if err != nil {
		return nil, err
	}
	fileClient, err := telegramingress.NewBotFileClient(
		envOrDefault("TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
		os.Getenv("TELEGRAM_BOT_TOKEN"), nil, 0,
	)
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("open scheduler wake queue: %w", err)
	}
	deliveryWakeQueue, err := sqsqueue.New(ctx, queueConfig(os.Getenv("DELIVERY_QUEUE_URL")))
	if err != nil {
		return nil, fmt.Errorf("open delivery wake queue: %w", err)
	}
	dispatchWakePublisher, err := outboxwake.NewPublisher(dispatchWakeQueue)
	if err != nil {
		return nil, err
	}
	deliveryWakePublisher, err := outboxwake.NewPublisher(deliveryWakeQueue)
	if err != nil {
		return nil, err
	}
	canonicalIngress, err := sessioningress.New(sessioningress.Config{
		IDKey:                 identityKey,
		HarnessBinder:         sessionlessharness.NewDeterministicFixtureBinderV1(),
		DispatchWakePublisher: dispatchWakePublisher,
		WakePublishError: func(publishErr error) {
			logger.Warn("durable outbox wake publication deferred to recovery", "error", publishErr)
		},
	}, state, blobs)
	if err != nil {
		return nil, err
	}
	processor, err := telegramingress.NewProcessor(
		telegramingress.ProcessorConfig{
			SourceID:              envOrDefault("TELEGRAM_SOURCE_ID", "bot-primary"),
			Provider:              envOrDefault("DEFAULT_COMPUTE_PROVIDER", "codex"),
			DeliveryWakePublisher: deliveryWakePublisher,
			WakePublishError: func(publishErr error) {
				logger.Warn("durable outbox wake publication deferred to recovery", "error", publishErr)
			},
		},
		identity, idgen.New(), systemClock{}, fileClient, canonicalIngress, state,
	)
	if err != nil {
		return nil, err
	}
	webhook, err := telegramingress.NewWebhook(webhookSecret, processor, logger)
	if err != nil {
		return nil, err
	}
	return webhook, nil
}

type attachedControlConfig struct {
	enabled  bool
	audience string
}

// Only exact explicit values activate the route; typos must never silently
// downgrade an intended deployment to health-only or enable a worker path.
func attachedControlConfigFromEnv(getenv func(string) string) (attachedControlConfig, error) {
	var config attachedControlConfig
	switch getenv("ATTACHED_WORKER_CONTROL_ENABLED") {
	case "", "false":
		return config, nil
	case "true":
		config.enabled = true
	default:
		return config, fmt.Errorf("ATTACHED_WORKER_CONTROL_ENABLED must be true or false")
	}
	config.audience = getenv("ATTACHED_WORKER_CONTROL_AUDIENCE")
	if config.audience == "" || config.audience != strings.TrimSpace(config.audience) || len(config.audience) > 256 {
		return attachedControlConfig{}, fmt.Errorf("ATTACHED_WORKER_CONTROL_AUDIENCE is required and must be an exact audience of at most 256 bytes")
	}
	for _, name := range []string{"YDB_CONNECTION_STRING", "S3_REGION", "S3_BUCKET"} {
		if strings.TrimSpace(getenv(name)) == "" {
			return attachedControlConfig{}, fmt.Errorf("%s is required for attached worker control", name)
		}
	}
	return config, nil
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
