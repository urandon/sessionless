package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAttachedControlConfigStrictFlag(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"TRUE", "1", "yes", " true", "false ", "typo"} {
		t.Run(value, func(t *testing.T) {
			_, err := attachedControlConfigFromEnv(func(name string) string {
				if name == "ATTACHED_WORKER_CONTROL_ENABLED" {
					return value
				}
				return ""
			})
			if err == nil || !strings.Contains(err.Error(), "ATTACHED_WORKER_CONTROL_ENABLED") {
				t.Fatalf("flag %q: error=%v, want strict flag rejection", value, err)
			}
		})
	}
}

func TestAttachedControlConfigDisabledIgnoresOptionalDependencies(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "false"} {
		t.Run("flag_"+value, func(t *testing.T) {
			config, err := attachedControlConfigFromEnv(func(name string) string {
				if name == "ATTACHED_WORKER_CONTROL_ENABLED" {
					return value
				}
				if name == "ATTACHED_WORKER_CONTROL_AUDIENCE" {
					return " invalid disabled audience "
				}
				return ""
			})
			if err != nil || config.enabled || config.audience != "" {
				t.Fatalf("disabled flag %q: config=%+v error=%v", value, config, err)
			}
		})
	}
}

func TestAttachedControlConfigRequiresExactAudienceAndSharedDependencies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, key, value string
	}{
		{name: "missing_audience", key: "ATTACHED_WORKER_CONTROL_AUDIENCE"},
		{name: "padded_audience", key: "ATTACHED_WORKER_CONTROL_AUDIENCE", value: " audience "},
		{name: "oversized_audience", key: "ATTACHED_WORKER_CONTROL_AUDIENCE", value: strings.Repeat("a", 257)},
		{name: "missing_ydb", key: "YDB_CONNECTION_STRING"},
		{name: "missing_region", key: "S3_REGION"},
		{name: "missing_bucket", key: "S3_BUCKET"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := attachedConfigFixture()
			values[tc.key] = tc.value
			_, err := attachedControlConfigFromEnv(func(name string) string { return values[name] })
			if err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("config %s: error=%v, want rejection of %s", tc.name, err, tc.key)
			}
		})
	}
}

func TestAttachedControlConfigHasNoTelegramOrQueueRequirement(t *testing.T) {
	t.Parallel()
	values := attachedConfigFixture()
	config, err := attachedControlConfigFromEnv(func(name string) string { return values[name] })
	if err != nil || !config.enabled || config.audience != values["ATTACHED_WORKER_CONTROL_AUDIENCE"] {
		t.Fatalf("attached-only config=%+v error=%v, want enabled without Telegram or queues", config, err)
	}
}

func TestBuildHandlerDefaultOffDoesNotInitializeDependencies(t *testing.T) {
	// Invalid dependency coordinates must be ignored in health-only startup.
	t.Setenv("ATTACHED_WORKER_CONTROL_ENABLED", "")
	t.Setenv("ATTACHED_WORKER_CONTROL_AUDIENCE", "invalid disabled audience ")
	t.Setenv("TELEGRAM_WEBHOOK_SECRET", "")
	t.Setenv("TELEGRAM_BOT_TOKEN", "")
	t.Setenv("TELEGRAM_IDENTITY_HMAC_KEY", "")
	t.Setenv("YDB_CONNECTION_STRING", "invalid")
	t.Setenv("S3_BUCKET", "")
	handler, cleanup, err := buildHandler(context.Background(), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("default-off startup: %v", err)
	}
	t.Cleanup(cleanup)
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{method: http.MethodGet, path: "/healthz", want: http.StatusOK},
		{method: http.MethodPost, path: "/telegram/webhook", want: http.StatusNotFound},
		{method: http.MethodPost, path: "/attached-worker/v1/challenges", want: http.StatusNotFound},
		{method: http.MethodPost, path: "/attached-worker/v1/attach", want: http.StatusNotFound},
		{method: http.MethodPost, path: "/attached-worker/v1/exchange", want: http.StatusNotFound},
		{method: http.MethodPost, path: "/attached-worker/v1/sealed-input", want: http.StatusNotFound},
		{method: http.MethodPost, path: "/attached-worker/v1/output-receipt", want: http.StatusNotFound},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(tc.method, tc.path, nil))
		if recorder.Code != tc.want {
			t.Errorf("%s %s: status=%d, want %d", tc.method, tc.path, recorder.Code, tc.want)
		}
	}
}

func TestBuildHandlerRejectsEnabledPartialConfigBeforeOpeningDependencies(t *testing.T) {
	t.Setenv("ATTACHED_WORKER_CONTROL_ENABLED", "true")
	t.Setenv("ATTACHED_WORKER_CONTROL_AUDIENCE", "sessionless:attached-worker:v1")
	t.Setenv("YDB_CONNECTION_STRING", "invalid")
	t.Setenv("S3_REGION", "ru-central1")
	t.Setenv("S3_BUCKET", "")
	t.Setenv("TELEGRAM_WEBHOOK_SECRET", "")
	handler, cleanup, err := buildHandler(context.Background(), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	t.Cleanup(cleanup)
	if handler != nil || err == nil || !strings.Contains(err.Error(), "S3_BUCKET") {
		t.Fatalf("partial startup: handler=%v error=%v, want S3_BUCKET config failure before YDB open", handler, err)
	}
}

func attachedConfigFixture() map[string]string {
	return map[string]string{
		"ATTACHED_WORKER_CONTROL_ENABLED": "true", "ATTACHED_WORKER_CONTROL_AUDIENCE": "sessionless:attached-worker:v1",
		"YDB_CONNECTION_STRING": "grpc://localhost:2136/local", "S3_REGION": "ru-central1", "S3_BUCKET": "test-bucket",
	}
}
