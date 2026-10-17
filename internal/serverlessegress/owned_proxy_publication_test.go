package serverlessegress

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestOwnedProxyPublicationCannotBeSynthesizedOrSerialized(t *testing.T) {
	var absent ports.OwnedProxyPublicationV2
	if err := absent.Validate(); err == nil {
		t.Fatal("zero claimed publication accepted")
	}
	if _, err := json.Marshal(absent); err == nil {
		t.Fatal("publication must not serialize")
	}
	if err := json.Unmarshal([]byte(`{}`), &absent); err == nil {
		t.Fatal("publication must not deserialize")
	}
	f := newPinnedTLSFixture(t, "openrouter.ai", true, func(http.ResponseWriter, *http.Request) { t.Error("denied publication sent HTTP") })
	f.preflight(t)
	if _, err := f.p.ObserveOwnedProxy(domain.RegisteredAllocationV1{}, domain.AttemptExecutionSourceV1{}, domain.AttemptOperationV1{}); err == nil {
		t.Fatal("actual TLS handshake is not a fabricated allocation/source proof")
	}
	if f.requests.Load() != 0 || f.dials.Load() != 1 || f.lookups.Load() != 1 {
		t.Fatal("publication validation emitted payload or refreshed original transport")
	}
}

// These real-TLS controls prove only the private physical seam, not an
// authenticated current YDB join or a credential-free installed producer.
func TestPinnedOwnedBeforeSendDenialBurnsConnectionWithoutPayload(t *testing.T) {
	f := newPinnedTLSFixture(t, "openrouter.ai", true, func(http.ResponseWriter, *http.Request) { t.Error("denied gate emitted HTTP") })
	a := f.preflight(t)
	var gates atomic.Int32
	v := f.invocation(a)
	gate := func(context.Context) error { gates.Add(1); return errors.New("synthetic current authority denial") }
	if _, err := f.p.invoke(f.ctx, v, gate); err == nil {
		t.Fatal("gate denial accepted")
	}
	if _, err := f.p.invoke(f.ctx, v, gate); err == nil {
		t.Fatal("denied connection became sendable again")
	}
	if gates.Load() != 1 || f.requests.Load() != 0 || f.dials.Load() != 1 {
		t.Fatalf("gate/send/redial counts=%d/%d/%d", gates.Load(), f.requests.Load(), f.dials.Load())
	}
	select {
	case <-f.closed:
	case <-f.ctx.Done():
		t.Fatal("denied gate leaked original connection")
	}
}

func TestPinnedOwnedBeforeSendPrecedesOneActualPhysicalRequest(t *testing.T) {
	var gates atomic.Int32
	f := newPinnedTLSFixture(t, "openrouter.ai", true, func(w http.ResponseWriter, _ *http.Request) {
		if gates.Load() != 1 {
			t.Error("physical HTTP arrived before the gate")
		}
		_, _ = w.Write([]byte("opaque"))
	})
	a := f.preflight(t)
	v := f.invocation(a)
	gate := func(context.Context) error { gates.Add(1); return nil }
	result, err := f.p.invoke(f.ctx, v, gate)
	if err != nil || result.Acceptance != domain.ProviderAcceptanceUnknownV1 || string(result.Response) != "opaque" {
		t.Fatalf("actual physical request: err=%v acceptance=%s", err, result.Acceptance)
	}
	if _, err := f.p.invoke(f.ctx, v, gate); err == nil {
		t.Fatal("second physical send accepted")
	}
	if gates.Load() != 1 || f.requests.Load() != 1 || f.dials.Load() != 1 {
		t.Fatal("gate or physical request retried")
	}
}
