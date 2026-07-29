package telegramingress

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type processorStub struct {
	calls int
}

func (stub *processorStub) Process(
	_ context.Context,
	update Update,
) (ports.TelegramIngressResult, error) {
	stub.calls++
	return ports.TelegramIngressResult{
		RunID: domain.RunID("run-webhook"), Created: update.UpdateID == 42,
	}, nil
}

func TestWebhookAuthenticatesBeforeCommitting(t *testing.T) {
	t.Parallel()
	processor := &processorStub{}
	webhook, err := NewWebhook(
		"webhook-secret", processor,
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"update_id":42,"message":{"message_id":7,"from":{"id":9},"chat":{"id":11,"type":"private"},"text":"hello"}}`

	unauthorized := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	unauthorizedResult := httptest.NewRecorder()
	webhook.ServeHTTP(unauthorizedResult, unauthorized)
	if unauthorizedResult.Code != http.StatusUnauthorized || processor.calls != 0 {
		t.Fatalf("unauthorized result = %d, calls = %d", unauthorizedResult.Code, processor.calls)
	}

	authorized := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	authorized.Header.Set("X-Telegram-Bot-Api-Secret-Token", "webhook-secret")
	authorizedResult := httptest.NewRecorder()
	webhook.ServeHTTP(authorizedResult, authorized)
	if authorizedResult.Code != http.StatusOK || processor.calls != 1 {
		t.Fatalf("authorized result = %d, calls = %d", authorizedResult.Code, processor.calls)
	}
}
