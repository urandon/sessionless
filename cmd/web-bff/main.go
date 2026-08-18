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
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const component = "web-bff"

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
	blobs, err := s3store.New(ctx, s3store.Config{
		Endpoint: os.Getenv("S3_ENDPOINT"), Region: os.Getenv("S3_REGION"),
		Bucket: os.Getenv("S3_BUCKET"), AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"), ForcePathStyle: envBool("S3_FORCE_PATH_STYLE"),
		IAMMetadataCredentials: envBool("S3_IAM_METADATA_CREDENTIALS"),
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
	handler, err := webbff.New(webbff.Config{
		BaseURL: baseURL, RedirectURI: redirectURI,
		OIDCPolicy: domain.OIDCVerificationPolicy{
			Issuer:   envOrDefault("TELEGRAM_OIDC_ISSUER", telegramoidc.DefaultIssuer),
			Audience: os.Getenv("TELEGRAM_OIDC_CLIENT_ID"), AllowedAlgorithms: []string{"RS256"},
			MaxClockSkew: 30 * time.Second,
		},
		Provider: provider, Store: store, Sessions: sessions, IDs: idgen.New(), Clock: systemClock{},
		Logger: logger, Build: buildinfo.Current(component),
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
