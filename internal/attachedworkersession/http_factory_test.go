package attachedworkersession

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestHTTPExchangeFactoryOwnsPerConnectionBearerWithoutAmbientProxy(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	seen := make(chan string, 2)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		seen <- request.Header.Get("Authorization")
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	factory, err := NewHTTPExchangeFactory(HTTPExchangeFactoryConfig{BaseURL: server.URL, RootCAs: roots})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"first-bearer", "second-bearer"} {
		bearer := []byte(secret)
		exchange, openErr := factory.Open(testFactoryBinding(), bearer)
		if openErr != nil {
			t.Fatal(openErr)
		}
		clear(bearer)
		if _, callErr := exchange.Exchange(context.Background(), factoryTestBatch()); callErr != nil {
			t.Fatal(callErr)
		}
		select {
		case got := <-seen:
			if got != "Bearer "+secret {
				t.Fatalf("authorization mismatch for %q", secret)
			}
		case <-time.After(time.Second):
			t.Fatal("exchange did not reach pinned TLS endpoint")
		}
		closer, ok := exchange.(interface{ Close() error })
		if !ok || closer.Close() != nil {
			t.Fatal("connection exchange is not closable")
		}
		if _, callErr := exchange.Exchange(context.Background(), factoryTestBatch()); callErr == nil {
			t.Fatal("closed connection retained exchange authority")
		}
	}
}

func TestHTTPExchangeFactoryRejectsInvalidAuthorityAndEndpoint(t *testing.T) {
	if _, err := NewHTTPExchangeFactory(HTTPExchangeFactoryConfig{BaseURL: "http://localhost:3000"}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("insecure origin error = %v", err)
	}
	factory, err := NewHTTPExchangeFactory(HTTPExchangeFactoryConfig{BaseURL: "https://control.example"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.Open(ConnectionBindingV1{}, []byte("bearer")); !errors.Is(err, ErrInvalidAuthority) {
		t.Fatalf("invalid binding error = %v", err)
	}
	if _, err := factory.Open(testFactoryBinding(), []byte("invalid bearer")); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("invalid bearer error = %v", err)
	}
}

func testFactoryBinding() ConnectionBindingV1 {
	digest := sha256.Sum256([]byte("capability"))
	return ConnectionBindingV1{
		TenantID: "tenant-1", OwnerUserID: "user-1", WorkerID: "worker-1",
		EnrollmentGeneration: 1, ConnectionGeneration: 1,
		ProtocolVersion: attachedworkerprotocol.ProtocolVersionV1,
		ConnectionID:    "connection-1", CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digest[:])),
		AuthenticationExpires: time.Unix(2000000000, 0).UTC(),
	}
}

func factoryTestBatch() attachedworkerprotocol.BatchV1 {
	return attachedworkerprotocol.BatchV1{Version: attachedworkerprotocol.ProtocolVersionV1, Frames: []attachedworkerprotocol.FrameV1{{
		Version:   attachedworkerprotocol.ProtocolVersionV1,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 1),
		WorkerID:  "worker-1", EnrollmentGeneration: 1, ConnectionGeneration: 1,
		Sequence: 1, Kind: attachedworkerprotocol.MessageHeartbeat,
		Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: 1, Available: true},
	}}}
}
