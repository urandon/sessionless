// Package controlapi contains the dependency-light HTTP surface for the control plane.
package controlapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"gitcode.com/urandon/sessionless/internal/buildinfo"
)

// NewHandler builds the control API routing tree.
func NewHandler(logger *slog.Logger, info buildinfo.Info) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", writeStatus("ok"))
	mux.HandleFunc("GET /readyz", writeStatus("ready"))
	mux.HandleFunc("GET /version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, info)
	})
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
