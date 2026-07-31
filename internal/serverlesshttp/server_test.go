package serverlesshttp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerReportsTriggerSuccessAndFailure(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name string
		run  RunFunc
		want int
	}{
		{name: "success", run: func(context.Context, *http.Request) (any, error) {
			return map[string]int{"processed": 1}, nil
		}, want: http.StatusOK},
		{name: "failure", run: func(context.Context, *http.Request) (any, error) {
			return nil, errors.New("retry")
		}, want: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/invoke", strings.NewReader(`{}`))
			response := httptest.NewRecorder()
			NewHandler(logger, test.run).ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d", response.Code, test.want)
			}
		})
	}
}

func TestHandlerRejectsNonTriggerMethod(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/invoke", nil)
	response := httptest.NewRecorder()
	NewHandler(slog.Default(), func(context.Context, *http.Request) (any, error) {
		return nil, nil
	}).ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
}
