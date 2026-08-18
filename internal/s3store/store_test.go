package s3store

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ycstorage "github.com/yandex-cloud/go-genproto/yandex/cloud/storage/v1"
	"google.golang.org/grpc"
	grpcmetadata "google.golang.org/grpc/metadata"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type tokenProviderFunc func(context.Context) (string, error)

func (provider tokenProviderFunc) Token(ctx context.Context) (string, error) {
	return provider(ctx)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

type presignServiceFunc func(
	context.Context,
	*ycstorage.PresignURLsRequest,
	...grpc.CallOption,
) (*ycstorage.PresignURLsResponse, error)

func (service presignServiceFunc) Create(
	ctx context.Context,
	request *ycstorage.PresignURLsRequest,
	options ...grpc.CallOption,
) (*ycstorage.PresignURLsResponse, error) {
	return service(ctx, request, options...)
}

type fakeS3ObjectAPI struct {
	head func(*s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	copy func(*s3.CopyObjectInput) (*s3.CopyObjectOutput, error)
}

func (fake *fakeS3ObjectAPI) PutObject(
	context.Context, *s3.PutObjectInput, ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	return nil, errors.New("unexpected PutObject")
}

func (fake *fakeS3ObjectAPI) GetObject(
	context.Context, *s3.GetObjectInput, ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	return nil, errors.New("unexpected GetObject")
}

func (fake *fakeS3ObjectAPI) DeleteObject(
	context.Context, *s3.DeleteObjectInput, ...func(*s3.Options),
) (*s3.DeleteObjectOutput, error) {
	return nil, errors.New("unexpected DeleteObject")
}

func (fake *fakeS3ObjectAPI) ListObjectsV2(
	context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	return nil, errors.New("unexpected ListObjectsV2")
}

func (fake *fakeS3ObjectAPI) HeadObject(
	_ context.Context,
	input *s3.HeadObjectInput,
	_ ...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	return fake.head(input)
}

func (fake *fakeS3ObjectAPI) CopyObject(
	_ context.Context,
	input *s3.CopyObjectInput,
	_ ...func(*s3.Options),
) (*s3.CopyObjectOutput, error) {
	return fake.copy(input)
}

func TestStaticPresignerBindsExactUploadMetadata(t *testing.T) {
	store, err := New(context.Background(), Config{
		Endpoint:        "http://minio.example:9000",
		Region:          "us-east-1",
		Bucket:          "artifact-bucket",
		AccessKeyID:     "local-access",
		SecretAccessKey: "local-secret",
		ForcePathStyle:  true,
		MaxObjectBytes:  1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	capability, err := store.PresignUpload(context.Background(), ports.UploadCapabilityRequest{
		TenantID: "tenant-a", ObjectKey: "uploads/upload-a/picture.png",
		MediaType: "image/png", Size: 123, SHA256: digest, ExpiresIn: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(capability.URL)
	if err != nil {
		t.Fatal(err)
	}
	if capability.Method != http.MethodPut ||
		parsed.EscapedPath() != "/artifact-bucket/tenants/tenant-a/uploads/upload-a/picture.png" {
		t.Fatalf("unexpected capability: %#v", capability)
	}
	if capability.Headers["content-length"] != "123" ||
		capability.Headers["content-type"] != "image/png" ||
		capability.Headers["x-amz-checksum-sha256"] != "ASNFZ4mrze8BI0VniavN7wEjRWeJq83vASNFZ4mrze8=" {
		t.Fatalf("signed headers = %#v", capability.Headers)
	}
	signedHeaders := parsed.Query().Get("X-Amz-SignedHeaders")
	for _, header := range []string{"content-length", "content-type"} {
		if !strings.Contains(signedHeaders, header) {
			t.Fatalf("%s is not signed: %q URL=%s", header, signedHeaders, capability.URL)
		}
	}
	if parsed.Query().Get("X-Amz-Checksum-Sha256") != capability.Headers["x-amz-checksum-sha256"] {
		t.Fatalf("checksum is not query-bound: %s", capability.URL)
	}
	if parsed.Query().Get("X-Amz-Expires") != "300" {
		t.Fatalf("expiry = %q", parsed.Query().Get("X-Amz-Expires"))
	}
	download, err := store.PresignDownload(context.Background(), "tenant-a", domain.BlobRef{
		TenantID: "tenant-a", Key: "tenants/tenant-a/uploads/upload-a/picture.png",
		Size: 123, SHA256: digest,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	downloadURL, _ := url.Parse(download.URL)
	if download.Method != http.MethodGet || downloadURL.EscapedPath() != parsed.EscapedPath() ||
		downloadURL.Query().Get("X-Amz-Expires") != "60" {
		t.Fatalf("download capability = %#v", download)
	}
}

func TestIAMPresignerUsesBearerTokenAndExactObjectRequest(t *testing.T) {
	fixedNow := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	called := false
	store := &Store{
		bucket: "artifact-bucket", maxObjectBytes: 1024, now: func() time.Time { return fixedNow },
		iamClient: &iamObjectClient{
			tokens: tokenProviderFunc(func(context.Context) (string, error) { return "iam-token", nil }),
			presign: presignServiceFunc(func(
				ctx context.Context,
				request *ycstorage.PresignURLsRequest,
				_ ...grpc.CallOption,
			) (*ycstorage.PresignURLsResponse, error) {
				called = true
				metadata, _ := grpcmetadata.FromOutgoingContext(ctx)
				if got := metadata.Get("authorization"); len(got) != 1 || got[0] != "Bearer iam-token" {
					t.Fatalf("authorization metadata = %#v", got)
				}
				if request.BucketName != "artifact-bucket" || len(request.Objects) != 1 {
					t.Fatalf("request = %#v", request)
				}
				object := request.Objects[0]
				if object.Name != "tenants/tenant-a/uploads/upload-a/input.txt" ||
					object.Method != http.MethodPut || object.Expires != 90 ||
					object.Headers["content-length"] != "7" ||
					object.Headers["content-type"] != "text/plain" ||
					object.Headers["x-amz-checksum-sha256"] == "" {
					t.Fatalf("object request = %#v", object)
				}
				return &ycstorage.PresignURLsResponse{Urls: []string{
					"https://artifact-bucket.storage.yandexcloud.net/tenants/tenant-a/uploads/upload-a/input.txt?signature=secret",
				}}, nil
			}),
		},
	}
	capability, err := store.PresignUpload(context.Background(), ports.UploadCapabilityRequest{
		TenantID: "tenant-a", ObjectKey: "uploads/upload-a/input.txt", MediaType: "text/plain",
		Size: 7, SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		ExpiresIn: 90 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called || capability.Method != http.MethodPut ||
		!capability.ExpiresAt.Equal(fixedNow.Add(90*time.Second)) {
		t.Fatalf("capability = %#v, called = %t", capability, called)
	}
}

func TestIAMStatAndPromoteUseExactConditionalRequests(t *testing.T) {
	digestHex := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	digestBytes, _ := base64.StdEncoding.DecodeString("ASNFZ4mrze8BI0VniavN7wEjRWeJq83vASNFZ4mrze8=")
	if len(digestBytes) != 32 {
		t.Fatal("bad checksum fixture")
	}
	requestCount := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestCount++
		if request.Header.Get("Authorization") != "Bearer iam-token" {
			t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
		}
		response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}
		if request.Method == http.MethodHead {
			if request.Header.Get("x-amz-checksum-mode") != "ENABLED" {
				t.Fatalf("HEAD checksum mode = %q", request.Header.Get("x-amz-checksum-mode"))
			}
			response.Header.Set("Content-Length", "7")
			response.Header.Set("Content-Type", "text/plain")
			response.Header.Set("ETag", `"etag-a"`)
			response.Header.Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(digestBytes))
			return response, nil
		}
		if request.Method != http.MethodPut {
			t.Fatalf("unexpected method: %s", request.Method)
		}
		if request.URL.EscapedPath() != "/artifact-bucket/tenants/tenant-a/sessions/session-a/input.txt" ||
			request.Header.Get("If-None-Match") != "*" ||
			request.Header.Get("x-amz-copy-source-if-match") != `"etag-a"` ||
			request.Header.Get("x-amz-meta-sha256") != digestHex ||
			request.Header.Get("Content-Type") != "text/plain" {
			t.Fatalf("copy request URL=%s headers=%#v", request.URL, request.Header)
		}
		if !strings.Contains(request.Header.Get("x-amz-copy-source"), "uploads") {
			t.Fatalf("copy source = %q", request.Header.Get("x-amz-copy-source"))
		}
		return response, nil
	})}
	endpoint, _ := url.Parse("https://storage.yandexcloud.net")
	store := &Store{bucket: "artifact-bucket", maxObjectBytes: 1024, iamClient: &iamObjectClient{
		endpoint: endpoint, http: httpClient,
		tokens: tokenProviderFunc(func(context.Context) (string, error) { return "iam-token", nil }),
	}}
	sourceMetadata, err := store.StatObject(
		context.Background(), "tenant-a", "uploads/upload-a/input.txt",
	)
	if err != nil {
		t.Fatal(err)
	}
	if sourceMetadata.Blob.SHA256 != digestHex || sourceMetadata.ETag != "etag-a" {
		t.Fatalf("metadata = %#v", sourceMetadata)
	}
	ref, err := store.PromoteObject(context.Background(), ports.PromoteObjectRequest{
		TenantID: "tenant-a", Source: sourceMetadata.Blob, SourceETag: sourceMetadata.ETag,
		FinalKey: "sessions/session-a/input.txt", MediaType: "text/plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Key != "tenants/tenant-a/sessions/session-a/input.txt" || ref.SHA256 != digestHex {
		t.Fatalf("promoted ref = %#v", ref)
	}
	if requestCount != 4 {
		t.Fatalf("request count = %d, want caller HEAD + guarded source HEAD + COPY + final HEAD", requestCount)
	}
}

func TestStaticPromotionReusesIdenticalExistingDestinationAfterConditionalFailure(t *testing.T) {
	const (
		digestHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		sourceKey = "tenants/tenant-a/uploads/upload-a/input.txt"
		finalKey  = "tenants/tenant-a/sessions/session-a/input.txt"
	)
	digest, _ := base64.StdEncoding.DecodeString("ASNFZ4mrze8BI0VniavN7wEjRWeJq83vASNFZ4mrze8=")
	headCount := 0
	copyErr := errors.New("PreconditionFailed: destination exists")
	fake := &fakeS3ObjectAPI{
		head: func(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			headCount++
			if aws.ToString(input.Key) != sourceKey && aws.ToString(input.Key) != finalKey {
				t.Fatalf("HEAD key = %q", aws.ToString(input.Key))
			}
			return &s3.HeadObjectOutput{
				ContentLength: aws.Int64(7), ContentType: aws.String("text/plain"),
				ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(digest)),
				ETag:           aws.String(`"etag-a"`),
			}, nil
		},
		copy: func(input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
			if aws.ToString(input.Key) != finalKey || aws.ToString(input.IfNoneMatch) != "*" {
				t.Fatalf("conditional copy = %#v", input)
			}
			return nil, copyErr
		},
	}
	store := &Store{bucket: "artifact-bucket", maxObjectBytes: 1024, s3Client: fake}
	ref, err := store.PromoteObject(context.Background(), ports.PromoteObjectRequest{
		TenantID: "tenant-a",
		Source: domain.BlobRef{
			TenantID: "tenant-a", Key: sourceKey, Size: 7, SHA256: digestHex,
		},
		SourceETag: "etag-a", FinalKey: finalKey, MediaType: "text/plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Key != finalKey || ref.SHA256 != digestHex || headCount != 2 {
		t.Fatalf("ref = %#v, HEAD count = %d", ref, headCount)
	}
}

func TestIAMPromotionRejectsMismatchedExistingDestinationAfterConditionalFailure(t *testing.T) {
	const digestHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	digest, _ := base64.StdEncoding.DecodeString("ASNFZ4mrze8BI0VniavN7wEjRWeJq83vASNFZ4mrze8=")
	requestCount := 0
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestCount++
		response := &http.Response{Header: make(http.Header), Body: http.NoBody}
		if request.Method == http.MethodPut {
			response.StatusCode = http.StatusPreconditionFailed
			response.Body = io.NopCloser(strings.NewReader("destination exists"))
			return response, nil
		}
		if request.Method != http.MethodHead {
			t.Fatalf("method = %s", request.Method)
		}
		response.StatusCode = http.StatusOK
		response.Header.Set("Content-Type", "text/plain")
		response.Header.Set("ETag", `"etag-a"`)
		response.Header.Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(digest))
		if strings.Contains(request.URL.Path, "/uploads/") {
			response.Header.Set("Content-Length", "7")
		} else {
			response.Header.Set("Content-Length", "8")
		}
		return response, nil
	})}
	endpoint, _ := url.Parse("https://storage.yandexcloud.net")
	store := &Store{bucket: "artifact-bucket", maxObjectBytes: 1024, iamClient: &iamObjectClient{
		endpoint: endpoint, http: httpClient,
		tokens: tokenProviderFunc(func(context.Context) (string, error) { return "iam-token", nil }),
	}}
	_, err := store.PromoteObject(context.Background(), ports.PromoteObjectRequest{
		TenantID: "tenant-a",
		Source: domain.BlobRef{
			TenantID: "tenant-a", Key: "tenants/tenant-a/uploads/upload-a/input.txt",
			Size: 7, SHA256: digestHex,
		},
		SourceETag: "etag-a", FinalKey: "sessions/session-a/input.txt", MediaType: "text/plain",
	})
	if err == nil || !strings.Contains(err.Error(), "destination metadata differs") ||
		!strings.Contains(err.Error(), "status 412") {
		t.Fatalf("error = %v", err)
	}
	if requestCount != 3 {
		t.Fatalf("request count = %d, want source HEAD + failed COPY + destination HEAD", requestCount)
	}
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
