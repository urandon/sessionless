package main

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"gitcode.com/urandon/sessionless/internal/oidcfixture"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	port := envOrDefault("PORT", "8082")
	issuer := envOrDefault("OIDC_FIXTURE_ISSUER", "http://127.0.0.1:"+port)
	server, err := oidcfixture.New(oidcfixture.Config{
		Environment: os.Getenv("SESSIONLESS_ENVIRONMENT"), Issuer: issuer,
		ClientID:     envOrDefault("OIDC_FIXTURE_CLIENT_ID", "100000"),
		ClientSecret: envOrDefault("OIDC_FIXTURE_CLIENT_SECRET", "local-fixture-secret"),
		RedirectURI:  envOrDefault("OIDC_FIXTURE_REDIRECT_URI", "https://web.localhost/auth/telegram/callback"),
		Subject:      envOrDefault("OIDC_FIXTURE_SUBJECT", strconv.FormatInt(424242, 10)),
	})
	if err != nil {
		logger.Error("OIDC fixture configuration failed", "error", err)
		os.Exit(1)
	}
	httpServer := &http.Server{
		Addr: ":" + port, Handler: server, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
	}
	logger.Info("starting local OIDC fixture", "address", httpServer.Addr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("OIDC fixture stopped", "error", err)
		os.Exit(1)
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
