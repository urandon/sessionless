package controlapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/buildinfo"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// This runs the actual production composition and TLS HTTP/1 response writer.
// Invalid bearer rejection must precede any access to the unopened DB or the
// panic-on-use BlobStore. It is not a valid-attempt/YDB custody/native proof.
func TestInlineOutputV2NormalMuxRejectsAuthBeforeStoreOverTLS(t *testing.T) {
	options, err := AttachedWorkerOptions("sessionless:attached-worker:v1", &ydbstore.Store{}, unusedControlBlobs{})
	if err != nil {
		t.Fatal(err)
	}
	if options.AttachedWorkerInlineOutputV2 == nil {
		t.Fatal("normal attached composition omitted inline handler")
	}
	server := httptest.NewUnstartedServer(NewHandlerWithOptions(discardLogger(), buildinfo.Current("test"), options))
	server.Config.MaxHeaderBytes = MaxRequestHeaderBytes
	server.Config.ReadHeaderTimeout = 5 * time.Second
	server.StartTLS()
	t.Cleanup(server.Close)
	client := server.Client()
	t.Cleanup(client.CloseIdleConnections)
	for _, path := range []string{attachedworkerreceipt.PathInlinePrepareV2, attachedworkerreceipt.PathInlineBodyV2, attachedworkerreceipt.PathInlineReconcileV2} {
		for _, auth := range []string{"Basic denied", "Bearer invalid-connection-encoding"} {
			t.Run(path+"/"+strings.Fields(auth)[0], func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				expires := time.Now().UTC().Add(5 * time.Second)
				var wire any = attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2}
				if path == attachedworkerreceipt.PathInlinePrepareV2 {
					wire = attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, ExpiresAt: expires}
				}
				if path == attachedworkerreceipt.PathInlineBodyV2 {
					wire = attachedworkeroutput.AttachedWorkerInlineBodyV2{Version: 2, ExpiresAt: expires}
				}
				encoded, err := json.Marshal(wire)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(encoded)
				bodyBytes := encoded
				if path == attachedworkerreceipt.PathInlineBodyV2 {
					bodyBytes = []byte("original synthetic artifact")
				}
				request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+path, bytes.NewReader(bodyBytes))
				if err != nil {
					t.Fatal(err)
				}
				request.Close = true
				request.Header.Set("Authorization", auth)
				request.Header.Set("Content-Type", "application/json")
				// Malformed bearer syntax can reject at the header; invalid bearer
				// encoding rejects in the real binder after bounded control parsing.
				// Neither path reaches artifact application Read or the receipt store.
				if path == attachedworkerreceipt.PathInlineBodyV2 {
					request.Header.Set("Content-Type", attachedworkerreceipt.InlineBodyMediaTypeV2)
					request.Header.Set(attachedworkerreceipt.InlineRequestHeaderV2, base64.RawStdEncoding.EncodeToString(encoded))
				}
				response, err := client.Do(request)
				if err != nil {
					t.Fatalf("actual TLS mux rejected transport rather than auth: %v", err)
				}
				defer response.Body.Close()
				body, err := io.ReadAll(io.LimitReader(response.Body, 1025))
				if err != nil || len(body) > 1024 || response.ProtoMajor != 1 || response.StatusCode != http.StatusUnauthorized || response.Header.Get("Cache-Control") != "no-store" {
					t.Fatalf("TLS auth response proto=%s status=%d bytes=%d readerr=%v", response.Proto, response.StatusCode, len(body), err)
				}
			})
		}
	}
}

func TestControlHeaderParserRejectsOversizeBeforeHandler(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(http.StatusNoContent) }))
	server.Config.MaxHeaderBytes = MaxRequestHeaderBytes
	server.Config.ReadHeaderTimeout = 5 * time.Second
	server.StartTLS()
	t.Cleanup(server.Close)
	client := server.Client()
	t.Cleanup(client.CloseIdleConnections)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+attachedworkerreceipt.PathInlinePrepareV2, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Close = true
	request.Header.Set("X-Oversized-Owned-Fixture", strings.Repeat("x", 32<<10))
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestHeaderFieldsTooLarge || calls.Load() != 0 {
		t.Fatalf("parser status=%d calls=%d", response.StatusCode, calls.Load())
	}
}
