package s3store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type tokenProviderFunc func(context.Context) (string, error)

func (provider tokenProviderFunc) Token(ctx context.Context) (string, error) {
	return provider(ctx)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func TestIAMObjectClientUsesBearerTokenForObjectLifecycle(t *testing.T) {
	const objectPath = "/artifact-bucket/tenants/tenant-a/inputs/message%20one.txt"
	requestCount := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestCount++
		if request.Header.Get("Authorization") != "Bearer test-iam-token" {
			t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
		}
		if request.URL.EscapedPath() != objectPath {
			t.Fatalf("path = %q", request.URL.EscapedPath())
		}
		response := &http.Response{Header: make(http.Header), Body: http.NoBody}
		switch request.Method {
		case http.MethodPut:
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			if string(body) != "payload" {
				t.Fatalf("PUT body = %q", body)
			}
			response.StatusCode = http.StatusOK
		case http.MethodGet:
			response.StatusCode = http.StatusOK
			response.Body = io.NopCloser(bytes.NewBufferString("payload"))
		case http.MethodDelete:
			response.StatusCode = http.StatusNoContent
		default:
			t.Fatalf("method = %s", request.Method)
		}
		return response, nil
	})}

	endpoint, err := url.Parse("https://storage.example")
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{
		bucket:         "artifact-bucket",
		maxObjectBytes: 1024,
		iamClient: &iamObjectClient{
			endpoint: endpoint,
			tokens: tokenProviderFunc(func(context.Context) (string, error) {
				return "test-iam-token", nil
			}),
			http: httpClient,
		},
	}
	ref, err := store.Put(context.Background(), "tenant-a", "inputs/message one.txt", bytes.NewBufferString("payload"))
	if err != nil {
		t.Fatal(err)
	}
	body, err := store.Open(context.Background(), "tenant-a", ref)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(data) != "payload" {
		t.Fatalf("GET body = %q", data)
	}
	if err := store.Delete(context.Background(), "tenant-a", ref); err != nil {
		t.Fatal(err)
	}
	if requestCount != 3 {
		t.Fatalf("request count = %d, want 3", requestCount)
	}
}

func TestTenantObjectKeyAddsTenantPrefix(t *testing.T) {
	key, err := tenantObjectKey("tenant-a", "inputs/message.txt")
	if err != nil {
		t.Fatalf("tenantObjectKey: %v", err)
	}
	if key != "tenants/tenant-a/inputs/message.txt" {
		t.Fatalf("key = %q", key)
	}
}

func TestTenantObjectKeyRejectsOtherTenantPrefix(t *testing.T) {
	_, err := tenantObjectKey("tenant-a", "tenants/tenant-b/private.txt")
	var mismatch domain.TenantMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %v, want TenantMismatchError", err)
	}
	if mismatch.Expected != "tenant-a" || mismatch.Actual != "tenant-b" {
		t.Fatalf("mismatch = %#v", mismatch)
	}
}

func TestTenantObjectKeyRejectsTraversal(t *testing.T) {
	for _, key := range []string{"../private.txt", "inputs/../private.txt", "/absolute"} {
		if _, err := tenantObjectKey("tenant-a", key); err == nil {
			t.Fatalf("key %q was accepted", key)
		}
	}
}

func TestAuthorizeRefRequiresCallerTenant(t *testing.T) {
	ref := domain.BlobRef{
		TenantID: "tenant-a",
		Key:      "tenants/tenant-a/result.txt",
		Size:     1,
		SHA256:   "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
	}
	err := authorizeRef("tenant-b", ref)
	var mismatch domain.TenantMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("error = %v, want TenantMismatchError", err)
	}
}
