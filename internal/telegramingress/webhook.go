package telegramingress

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"gitcode.com/urandon/sessionless/internal/ports"
)

const maxWebhookBytes = 8 << 20

var ErrUnsupportedUpdate = errors.New("unsupported Telegram update")

type UpdateProcessor interface {
	Process(ctx context.Context, update Update) (ports.TelegramIngressResult, error)
}

type Webhook struct {
	secret    string
	processor UpdateProcessor
	logger    *slog.Logger
}

func NewWebhook(secret string, processor UpdateProcessor, logger *slog.Logger) (*Webhook, error) {
	if secret == "" {
		return nil, errors.New("Telegram webhook secret must not be empty")
	}
	if processor == nil {
		return nil, errors.New("Telegram update processor must not be nil")
	}
	if logger == nil {
		return nil, errors.New("logger must not be nil")
	}
	return &Webhook{secret: secret, processor: processor, logger: logger}, nil
}

func (webhook *Webhook) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(w, `{"error":"method_not_allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	provided := request.Header.Get("X-Telegram-Bot-Api-Secret-Token")
	if len(provided) != len(webhook.secret) ||
		subtle.ConstantTimeCompare([]byte(provided), []byte(webhook.secret)) != 1 {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxWebhookBytes+1))
	if err != nil || len(body) > maxWebhookBytes {
		http.Error(w, `{"error":"invalid_request"}`, http.StatusBadRequest)
		return
	}
	var update Update
	if err := json.Unmarshal(body, &update); err != nil {
		http.Error(w, `{"error":"invalid_json"}`, http.StatusBadRequest)
		return
	}
	result, err := webhook.processor.Process(request.Context(), update)
	if err != nil {
		if errors.Is(err, ErrUnsupportedUpdate) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true,"ignored":true}`))
			return
		}
		webhook.logger.Error("Telegram webhook processing failed",
			"update_id", update.UpdateID, "error", err)
		http.Error(w, `{"error":"temporarily_unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	webhook.logger.Info("Telegram update committed",
		"update_id", update.UpdateID, "run_id", result.RunID, "created", result.Created)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}
