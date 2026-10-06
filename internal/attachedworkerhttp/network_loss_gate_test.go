package attachedworkerhttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
)

type networkLossCycle func(context.Context) error

func (cycle networkLossCycle) Exchange(ctx context.Context) error { return cycle(ctx) }

// The server consumes A's request before losing the response. A retry could
// repeat an already-applied exchange; B's independent connection must remain
// usable while A waits for authoritative reconciliation.
func TestAW07PostExchangeNetworkLossFencesReplayWithoutStoppingPeer(t *testing.T) {
	var aEffects, bEffects atomic.Int32
	aApplied := make(chan struct{}, 1)
	releaseA := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseA) }) }
	defer release()
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != ExchangePathV1 {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, err := attachedworkerprotocol.DecodeBatchV1(body); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		switch request.Header.Get("Authorization") {
		case "Bearer owner-a-token":
			aEffects.Add(1)
			select {
			case aApplied <- struct{}{}:
			default:
			}
			<-releaseA
			hijacker, ok := writer.(http.Hijacker)
			if !ok {
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			connection, _, err := hijacker.Hijack()
			if err != nil {
				return
			}
			_ = connection.Close()
		case "Bearer owner-b-token":
			bEffects.Add(1)
			writer.WriteHeader(http.StatusNoContent)
		default:
			writer.WriteHeader(http.StatusUnauthorized)
		}
	}))
	t.Cleanup(server.Close)

	newPoller := func(bearer string) *attachedworkertransport.Poller {
		t.Helper()
		client, err := NewClientFromBearerBytes(ClientBytesConfig{
			BaseURL: server.URL, Bearer: []byte(bearer), HTTPClient: server.Client(),
			RequestTimeout: 30 * time.Second,
		})
		if err != nil {
			t.Fatalf("construct %s client: %v", bearer, err)
		}
		t.Cleanup(func() { _ = client.Close() })
		poller, err := attachedworkertransport.NewPoller(attachedworkertransport.Config{
			Enabled: true, PollInterval: attachedworkertransport.MinimumHeartbeatInterval,
			InitialBackoff: time.Second, MaxBackoff: time.Minute,
			Random: bytes.NewReader(make([]byte, 8)),
		}, networkLossCycle(func(ctx context.Context) error {
			_, err := client.Exchange(ctx, testBatch(1))
			return err
		}))
		if err != nil {
			t.Fatalf("construct %s poller: %v", bearer, err)
		}
		return poller
	}
	a, b := newPoller("owner-a-token"), newPoller("owner-b-token")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	aResult := make(chan error, 1)
	go func() { aResult <- a.Step(ctx) }()
	select {
	case <-aApplied:
	case <-ctx.Done():
		t.Fatalf("A request was not applied before the network drop: %v", ctx.Err())
	}
	bCtx, bCancel := context.WithTimeout(ctx, 5*time.Second)
	defer bCancel()
	if err := b.Step(bCtx); err != nil {
		t.Fatalf("B exchange failed while A's applied request was awaiting its lost response: %v", err)
	}
	select {
	case err := <-aResult:
		t.Fatalf("A exchange finished before the response was released: %v", err)
	default:
	}
	release()
	var first error
	select {
	case first = <-aResult:
	case <-ctx.Done():
		t.Fatalf("A did not observe the bounded network loss: %v", ctx.Err())
	}
	var exchangeErr *ExchangeError
	if !errors.As(first, &exchangeErr) || exchangeErr.Kind != ErrorUnavailable || !exchangeErr.Retryable() {
		t.Fatalf("A applied request then lost response: error=%v, want retryable unavailable classification", first)
	}
	if got := a.Step(ctx); !errors.Is(got, attachedworkertransport.ErrReconciliationRequired) {
		t.Fatalf("A repeated ambiguous exchange: error=%v, want reconciliation required", got)
	}
	if gotA, gotB := aEffects.Load(), bEffects.Load(); gotA != 1 || gotB != 1 {
		t.Fatalf("server effects A=%d B=%d, want one each with no A replay", gotA, gotB)
	}
}
