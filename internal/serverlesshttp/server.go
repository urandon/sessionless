// Package serverlesshttp exposes bounded trigger handlers as HTTP servers for
// Yandex Serverless Containers while keeping the domain components unaware of
// the cloud transport.
package serverlesshttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type RunFunc func(context.Context, *http.Request) (any, error)

func NewHandler(logger *slog.Logger, run RunFunc) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /", func(w http.ResponseWriter, request *http.Request) {
		result, err := run(request.Context(), request)
		if err != nil {
			logger.Error(
				"serverless trigger invocation failed",
				"request_id", request.Header.Get("X-Request-Id"),
				"error", err,
			)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "retryable_invocation_failure"})
			return
		}
		if result == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	return mux
}

func Serve(ctx context.Context, address string, logger *slog.Logger, run RunFunc) error {
	if address == "" {
		return fmt.Errorf("serverless HTTP address is required")
	}
	server := &http.Server{
		Addr:              address,
		Handler:           NewHandler(logger, run),
		ReadHeaderTimeout: 5 * time.Second,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("serverless HTTP shutdown failed", "error", err)
		}
	}()
	logger.Info("starting serverless trigger HTTP server", "address", address)
	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-done
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
