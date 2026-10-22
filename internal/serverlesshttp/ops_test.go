package serverlesshttp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type opsSinkFunc func(context.Context, opssignal.Record) error

func (f opsSinkFunc) Observe(ctx context.Context, r opssignal.Record) error { return f(ctx, r) }

func TestOpsIngressPreservesHTTPAndRetryOwnership(t *testing.T) {
	for _, tc := range []struct {
		name            string
		err             error
		status, signals int
	}{
		{"success", nil, http.StatusNoContent, 0},
		{"infrastructure", errors.New("secret provider URL"), http.StatusServiceUnavailable, 1},
		{"retry", ports.ErrQueueRetryRequested, http.StatusServiceUnavailable, 0},
		{"redrive", ports.ErrQueueRedrivePending, http.StatusServiceUnavailable, 0},
		{"empty", ports.ErrTriggerQueueEmpty, http.StatusServiceUnavailable, 0},
		{"policy", &domain.ClassifiedError{Kind: domain.ErrorPolicyDenied, Code: "private-code"}, http.StatusServiceUnavailable, 0},
		{"quota", &domain.ClassifiedError{Kind: domain.ErrorQuotaBlocked, Code: "private-code"}, http.StatusServiceUnavailable, 0},
		{"membership", domain.ErrMembershipDenied, http.StatusServiceUnavailable, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), struct{}{}, "private-value")
			calls, signals := 0, 0
			h := NewHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), func(got context.Context, _ *http.Request) (any, error) {
				calls++
				if got != ctx {
					t.Error("runner context replaced")
				}
				return nil, tc.err
			}, opsSinkFunc(func(got context.Context, r opssignal.Record) error {
				signals++
				if got != ctx || r.Stage != opssignal.Ingress || r.SignalID != opssignal.ControlQueueFailure {
					t.Errorf("invalid ingress record/context: %+v", r)
				}
				return errors.New("short write")
			}))
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx))
			if w.Code != tc.status || signals != tc.signals || calls != 1 {
				t.Fatalf("status=%d signals=%d calls=%d", w.Code, signals, calls)
			}
		})
	}
}
