// Package controlapi contains the dependency-light HTTP surface for the control plane.
package controlapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"gitcode.com/urandon/sessionless/internal/buildinfo"
)

type Options struct {
	TelegramWebhook http.Handler
}

// NewHandler builds the control API routing tree without optional frontend
// adapters. It remains useful for health-only local and unit-test processes.
func NewHandler(logger *slog.Logger, info buildinfo.Info) http.Handler {
	return NewHandlerWithOptions(logger, info, Options{})
}

func NewHandlerWithOptions(logger *slog.Logger, info buildinfo.Info, options Options) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", writeStatus("ok"))
	mux.HandleFunc("GET /readyz", writeStatus("ready"))
	mux.HandleFunc("GET /version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, info)
	})
	if options.TelegramWebhook != nil {
		mux.Handle("POST /telegram/webhook", options.TelegramWebhook)
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		logger.Info("route not found", "method", r.Method, "path", r.URL.Path)
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
	})
	return mux
}

func writeStatus(status string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": status})
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
