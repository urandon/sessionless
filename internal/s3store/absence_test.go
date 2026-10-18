package s3store

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func absenceTestRef() domain.BlobRef {
	return domain.BlobRef{TenantID: "tenant-absence", Key: domain.SessionRunObjectPrefix("tenant-absence", "session-absence", "run-absence") + "attempt-operations-v1/body", Size: 1, SHA256: strings.Repeat("a", 64)}
}
func absenceTestStore(t *testing.T, endpoint string, iam bool, client *http.Client) *Store {
	t.Helper()
	if iam {
		u, err := url.Parse(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		return &Store{bucket: "artifact-bucket", iamClient: &iamObjectClient{endpoint: u, tokens: fixedToken("fixture-token-not-a-credential"), http: client}}
	}
	c := s3.NewFromConfig(aws.Config{Region: "test-region", Credentials: credentials.NewStaticCredentialsProvider("fixture-access", "fixture-secret", ""), HTTPClient: client}, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
	return &Store{bucket: "artifact-bucket", s3Client: c, maxObjectBytes: 1024}
}

// Owned HTTP peers and fixture credentials only, no real Object Storage or
// late-PUT reconciliation proof. Both production transport branches execute.
func TestRequireBlobAbsentExactSingleHEAD(t *testing.T) {
	for _, iam := range []bool{false, true} {
		name := "sdk"
		if iam {
			name = "iam"
		}
		t.Run(name, func(t *testing.T) {
			for _, status := range []int{404, 200, 403, 500, 307, 308} {
				t.Run(http.StatusText(status), func(t *testing.T) {
					ref := absenceTestRef()
					var calls atomic.Int64
					var bad atomic.Bool
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						if r.Method != http.MethodHead || r.URL.Path != "/artifact-bucket/"+ref.Key {
							bad.Store(true)
						}
						if r.URL.Path == "/redirected" {
							w.WriteHeader(404)
							return
						}
						if status == 307 || status == 308 {
							w.Header().Set("Location", "/redirected")
						}
						w.WriteHeader(status)
					}))
					t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
					client := server.Client()
					store := absenceTestStore(t, server.URL, iam, client)
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					err := store.RequireBlobAbsent(ctx, ref.TenantID, ref)
					if (err == nil) != (status == 404) || calls.Load() != 1 || bad.Load() {
						t.Fatalf("mode=%s status=%d err=%v calls=%d exact=%t", name, status, err, calls.Load(), !bad.Load())
					}
					if client.CheckRedirect != nil {
						t.Fatal("absence check mutated shared client")
					}
				})
			}
		})
	}
}

func TestRequireBlobAbsentDeniesBeforeTransport(t *testing.T) {
	for _, iam := range []bool{false, true} {
		for _, mode := range []string{"foreign", "cancelled", "unbounded", "invalid_ref"} {
			t.Run(map[bool]string{false: "sdk", true: "iam"}[iam]+"/"+mode, func(t *testing.T) {
				var calls atomic.Int64
				client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					calls.Add(1)
					return nil, errors.New("unexpected transport")
				})}
				store := absenceTestStore(t, "http://fixture.invalid", iam, client)
				ref := absenceTestRef()
				tenant := ref.TenantID
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				switch mode {
				case "foreign":
					tenant = "foreign"
				case "cancelled":
					cancel()
				case "unbounded":
					ctx = context.Background()
				case "invalid_ref":
					ref.SHA256 = "bad"
				}
				if err := store.RequireBlobAbsent(ctx, tenant, ref); err == nil || calls.Load() != 0 {
					t.Fatal("pre-transport denial", mode, err, calls.Load())
				}
			})
		}
	}
}

type absenceCloseBody struct {
	fail   bool
	closes *atomic.Int64
}

func (*absenceCloseBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *absenceCloseBody) Close() error {
	b.closes.Add(1)
	if b.fail {
		return errors.New("owned close failure")
	}
	return nil
}

func TestRequireBlobAbsentRejectsUncertainTransport(t *testing.T) {
	for _, iam := range []bool{false, true} {
		for _, mode := range []string{"network", "cancel_after_response", "close_failure"} {
			if !iam && mode == "close_failure" {
				continue
			} // SDK owns and closes its HEAD response internally.
			t.Run(map[bool]string{false: "sdk", true: "iam"}[iam]+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var calls, closes atomic.Int64
				client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					if mode == "network" {
						return nil, errors.New("owned transport failure")
					}
					if mode == "cancel_after_response" {
						cancel()
					}
					return &http.Response{StatusCode: 404, Header: make(http.Header), Request: r, Body: &absenceCloseBody{fail: mode == "close_failure", closes: &closes}}, nil
				})}
				store := absenceTestStore(t, "http://fixture.invalid", iam, client)
				ref := absenceTestRef()
				if err := store.RequireBlobAbsent(ctx, ref.TenantID, ref); err == nil || calls.Load() != 1 {
					t.Fatal("uncertain transport became absence/retried", mode, err, calls.Load())
				}
				if mode != "network" && closes.Load() != 1 {
					t.Fatal("HEAD response not synchronously closed", mode, closes.Load())
				}
			})
		}
	}
}
