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
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"gitcode.com/urandon/sessionless/internal/webapi"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/webstatic"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const component = "web-bff"

const defaultWebBlobLimit = int64(64 << 20)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	handler, closeDependencies, err := buildHandler(ctx, logger)
	if err != nil {
		logger.Error("web BFF configuration failed", "component", component, "error", err)
		os.Exit(1)
	}
	defer closeDependencies()

	server := &http.Server{
		Addr:              webListenAddress(),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("web BFF graceful shutdown failed", "component", component, "error", err)
		}
	}()
	logger.Info("starting web BFF", "component", component, "address", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("web BFF stopped", "component", component, "error", err)
		os.Exit(1)
	}
}

func webListenAddress() string {
	return ":" + envOrDefault("WEB_PORT", "8083")
}

func buildHandler(ctx context.Context, logger *slog.Logger) (http.Handler, func(), error) {
	baseURL := os.Getenv("WEB_BASE_URL")
	redirectURI := baseURL + webcontract.RouteOIDCCallback
	environment := envOrDefault("SESSIONLESS_ENVIRONMENT", "cloud-dev")
	allowLocal := environment == "local"
	provider, err := telegramoidc.New(telegramoidc.Config{
		Issuer:                envOrDefault("TELEGRAM_OIDC_ISSUER", telegramoidc.DefaultIssuer),
		AuthorizationEndpoint: envOrDefault("TELEGRAM_OIDC_AUTHORIZATION_ENDPOINT", telegramoidc.DefaultAuthorizationEndpoint),
		TokenEndpoint:         envOrDefault("TELEGRAM_OIDC_TOKEN_ENDPOINT", telegramoidc.DefaultTokenEndpoint),
		JWKSURL:               envOrDefault("TELEGRAM_OIDC_JWKS_URL", telegramoidc.DefaultJWKSURL),
		ClientID:              os.Getenv("TELEGRAM_OIDC_CLIENT_ID"), ClientSecret: os.Getenv("TELEGRAM_OIDC_CLIENT_SECRET"),
		RedirectURI: redirectURI, AllowedAlgorithms: []string{"RS256"}, AllowLoopbackProvider: allowLocal,
	})
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
			logger.Error("close web BFF YDB", "component", component, "error", err)
		}
	}
	store, err := ydbstore.New(ydb.DB, ydbstore.Options{})
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	maxUploadBytes, err := envPositiveInt64("WEB_MAX_UPLOAD_BYTES", 32<<20)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	maxBlobBytes := defaultWebBlobLimit
	if maxUploadBytes > maxBlobBytes {
		maxBlobBytes = maxUploadBytes
	}
	blobs, err := s3store.New(ctx, s3store.Config{
		Endpoint: os.Getenv("S3_ENDPOINT"), Region: os.Getenv("S3_REGION"),
		Bucket: os.Getenv("S3_BUCKET"), AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"), ForcePathStyle: envBool("S3_FORCE_PATH_STYLE"),
		IAMMetadataCredentials: envBool("S3_IAM_METADATA_CREDENTIALS"),
		MaxObjectBytes:         maxBlobBytes,
	})
	if err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("open web BFF object storage: %w", err)
	}
	sessions, err := sessionapi.New(sessionapi.Config{
		CursorKey: []byte(os.Getenv("SESSION_API_CURSOR_HMAC_KEY")),
		IDKey:     []byte(os.Getenv("SESSION_API_ID_HMAC_KEY")),
	}, store, blobs, systemClock{})
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	dispatchWakeQueue, err := sqsqueue.New(ctx, sqsqueue.Config{
		Endpoint: os.Getenv("QUEUE_ENDPOINT"), Region: envOrDefault("QUEUE_REGION", "ru-central1"),
		QueueURL: os.Getenv("SCHEDULER_WAKE_QUEUE_URL"),
		AccessKeyID: firstNonEmpty(
			os.Getenv("OUTBOX_QUEUE_ACCESS_KEY_ID"), os.Getenv("QUEUE_ACCESS_KEY_ID"),
		),
		SecretAccessKey: firstNonEmpty(
			os.Getenv("OUTBOX_QUEUE_SECRET_ACCESS_KEY"), os.Getenv("QUEUE_SECRET_ACCESS_KEY"),
		),
	})
	if err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("open web scheduler wake queue: %w", err)
	}
	dispatchWakePublisher, err := outboxwake.NewPublisher(dispatchWakeQueue)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	identityKey := []byte(os.Getenv("SESSION_API_ID_HMAC_KEY"))
	canonicalIngress, err := sessioningress.New(sessioningress.Config{
		IDKey: identityKey, DispatchWakePublisher: dispatchWakePublisher,
		WakePublishError: func(publishErr error) {
			logger.Warn("durable web dispatch wake publication deferred to recovery", "error", publishErr)
		},
	}, store, blobs)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	api, err := webapi.New(webapi.Config{
		IDKey:             identityKey,
		MaxUploadBytes:    maxUploadBytes,
		AllowedMCPServers: envCSV("WEB_ALLOWED_MCP_SERVERS"),
	}, sessions, canonicalIngress, store, store, blobs, systemClock{})
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	backend, err := webbff.New(webbff.Config{
		BaseURL: baseURL, RedirectURI: redirectURI,
		ObjectStorageOrigin:        os.Getenv("WEB_OBJECT_STORAGE_ORIGIN"),
		AllowLoopbackObjectStorage: allowLocal,
		OIDCPolicy: domain.OIDCVerificationPolicy{
			Issuer:   envOrDefault("TELEGRAM_OIDC_ISSUER", telegramoidc.DefaultIssuer),
			Audience: os.Getenv("TELEGRAM_OIDC_CLIENT_ID"), AllowedAlgorithms: []string{"RS256"},
			MaxClockSkew: 30 * time.Second,
		},
		Provider: provider, Store: store, Sessions: sessions, API: api, IDs: idgen.New(), Clock: systemClock{},
		Logger: logger, Build: buildinfo.Current(component),
	})
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	handler, err := webstatic.New(webstatic.Config{
		Backend:                    backend,
		ObjectStorageOrigin:        os.Getenv("WEB_OBJECT_STORAGE_ORIGIN"),
		AllowLoopbackObjectStorage: allowLocal,
	})
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	return handler, closeYDB, nil
}

func envBool(name string) bool {
	value, _ := strconv.ParseBool(os.Getenv(name))
	return value
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func envPositiveInt64(name string, fallback int64) (int64, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func envCSV(name string) []string {
	var result []string
	for _, value := range strings.Split(os.Getenv(name), ",") {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}
