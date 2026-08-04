package s3store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
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

func TestDeletePrefixListsAndDeletesOnlySessionlessOwnedObjects(t *testing.T) {
	const listing = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>tenants/tenant-a/sessions/session-a/event.json</Key></Contents>
  <Contents><Key>tenants/tenant-b/sessions/session-b/snapshot.json</Key></Contents>
</ListBucketResult>`
	deleted := map[string]bool{}
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Authorization") != "Bearer reset-token" {
			t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
		}
		response := &http.Response{Header: make(http.Header), Body: http.NoBody}
		switch request.Method {
		case http.MethodGet:
			if request.URL.EscapedPath() != "/artifact-bucket" ||
				request.URL.Query().Get("list-type") != "2" ||
				request.URL.Query().Get("prefix") != "tenants/" {
				t.Fatalf("unexpected listing URL: %s", request.URL)
			}
			response.StatusCode = http.StatusOK
			response.Body = io.NopCloser(bytes.NewBufferString(listing))
		case http.MethodDelete:
			key := strings.TrimPrefix(request.URL.EscapedPath(), "/artifact-bucket/")
			if !strings.HasPrefix(key, "tenants/") {
				t.Fatalf("delete escaped tenants prefix: %q", key)
			}
			deleted[key] = true
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
		bucket: "artifact-bucket",
		iamClient: &iamObjectClient{
			endpoint: endpoint,
			tokens: tokenProviderFunc(func(context.Context) (string, error) {
				return "reset-token", nil
			}),
			http: httpClient,
		},
	}
	count, err := store.DeletePrefix(context.Background(), "tenants/")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 || len(deleted) != 2 {
		t.Fatalf("deleted count = %d, keys = %#v", count, deleted)
	}
}

func TestDeletePrefixRejectsBroadOrNonSessionlessPrefixes(t *testing.T) {
	store := &Store{}
	for _, prefix := range []string{"", "/", "shared/", "tenants", "tenants/../shared/"} {
		if _, err := store.DeletePrefix(context.Background(), prefix); err == nil {
			t.Fatalf("prefix %q was accepted", prefix)
		}
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
