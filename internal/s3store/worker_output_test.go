package s3store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	awsv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ycstorage "github.com/yandex-cloud/go-genproto/yandex/cloud/storage/v1"
	"google.golang.org/grpc"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type workerOutputPresigner struct {
	calls int
	put   func(context.Context, *s3.PutObjectInput, time.Duration) (*awsv4.PresignedHTTPRequest, error)
}

func (f *workerOutputPresigner) PresignPutObject(ctx context.Context, in *s3.PutObjectInput, options ...func(*s3.PresignOptions)) (*awsv4.PresignedHTTPRequest, error) {
	f.calls++
	var o s3.PresignOptions
	for _, option := range options {
		option(&o)
	}
	return f.put(ctx, in, o.Expires)
}
func (*workerOutputPresigner) PresignGetObject(context.Context, *s3.GetObjectInput, ...func(*s3.PresignOptions)) (*awsv4.PresignedHTTPRequest, error) {
	return nil, errors.New("unexpected get signing")
}

func workerOutputFixture(t *testing.T) (time.Time, ports.AttachedWorkerOutputObjectRequestV1) {
	t.Helper()
	now := time.Date(2026, 10, 15, 10, 0, 0, 250000000, time.UTC)
	h := domain.AttachedWorkerAttemptV1{Version: 1, TenantID: "tenant-output", OwnerUserID: "owner-output", WorkerID: "worker-output", ConnectionID: "connection-output", ExecutionConnectionID: "connection-output", RunID: "run-output", AttemptID: "attempt-output", ReservationID: "reservation-output", LeaseID: "lease-output", LeaseGeneration: 1, EnrollmentGeneration: 2, ConnectionGeneration: 3, ExecutionConnectionGeneration: 3,
		ContextDigest: domain.AttachedWorkerContextDigest(strings.Repeat("a", 64)), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("b", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("c", 64)), State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 2, WorkerAttemptSequence: 1, LeaseExpiresAt: now.Add(time.Hour), CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Second), Revision: 4}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(h.TenantID, h.OwnerUserID, h.WorkerID, h.RunID, h.AttemptID, h.LeaseID, h.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	h.FenceToken = fence
	a := ports.AttachedWorkerSealedInputAuthorization{TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, WorkerID: h.WorkerID, ConnectionID: h.ConnectionID, EnrollmentGeneration: h.EnrollmentGeneration, ConnectionGeneration: h.ConnectionGeneration, RunID: h.RunID, AttemptID: h.AttemptID, AttemptSequence: 1, LeaseID: h.LeaseID, LeaseGeneration: h.LeaseGeneration, FenceToken: fence, LeaseExpiresAtUnixMicro: h.LeaseExpiresAt.UnixMicro(), ContextDigest: h.ContextDigest, CapabilityDigest: h.CapabilityDigest, PolicyDigest: h.PolicyDigest, ExpectedAttemptRevision: h.Revision}
	r := ports.AttachedWorkerOutputObjectRequestV1{Version: 1, Authorization: a, AcceptedAttempt: h, SessionID: "session-output", HarnessBindingDigest: domain.HarnessBindingDigestV2(strings.Repeat("d", 64)), ProfileDigest: strings.Repeat("e", 64), SchemaDigest: strings.Repeat("f", 64), FinalDigest: strings.Repeat("1", 64), PlanDigest: strings.Repeat("2", 64), Ordinal: 0, MediaType: "text/plain", ExpiresAt: now.Add(10 * time.Second)}
	workerOutputSetBody(&r, []byte("payload"))
	if err := r.ValidateAt(now); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return now, r
}

func workerOutputSetBody(r *ports.AttachedWorkerOutputObjectRequestV1, body []byte) {
	s, m := sha256.Sum256(body), md5.Sum(body)
	r.Size, r.SHA256, r.ContentMD5 = int64(len(body)), hex.EncodeToString(s[:]), base64.StdEncoding.EncodeToString(m[:])
}

// This synthetic SigV4-shaped result is a DATA fixture, not proof of a valid
// signature, actual provider expiry/conditional enforcement, or current source.
func workerOutputSignedURL(origin, path string, now time.Time, lifetime time.Duration, headers map[string]string) string {
	u, _ := url.Parse(origin)
	u.Path = path
	names := []string{"host"}
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	q := url.Values{"X-Amz-Algorithm": {"AWS4-HMAC-SHA256"}, "X-Amz-Credential": {"synthetic-key/" + now.Format("20060102") + "/region/s3/aws4_request"}, "X-Amz-Date": {now.Format("20060102T150405Z")}, "X-Amz-Expires": {strconv.FormatInt(int64(lifetime/time.Second), 10)}, "X-Amz-SignedHeaders": {strings.Join(names, ";")}, "X-Amz-Signature": {strings.Repeat("3", 64)}}
	u.RawQuery = q.Encode()
	return u.String()
}

func workerOutputAWS(t *testing.T, now time.Time, r ports.AttachedWorkerOutputObjectRequestV1) (*Store, *workerOutputPresigner) {
	t.Helper()
	s := &Store{bucket: "artifact-bucket", workerOutputOrigin: "https://storage.example", workerOutputPathStyle: true, now: func() time.Time { return now }}
	f := &workerOutputPresigner{}
	f.put = func(ctx context.Context, in *s3.PutObjectInput, lifetime time.Duration) (*awsv4.PresignedHTTPRequest, error) {
		if ctx == nil || *in.Bucket != s.bucket || *in.Key != r.CandidateKey() || *in.ContentLength != r.Size || *in.ContentType != r.MediaType || *in.ContentMD5 != r.ContentMD5 || *in.IfNoneMatch != "*" {
			t.Fatal("signer did not receive exact conditional request")
		}
		d, _ := hex.DecodeString(r.SHA256)
		if *in.ChecksumSHA256 != base64.StdEncoding.EncodeToString(d) {
			t.Fatal("SHA256 not bound")
		}
		if lifetime != 10*time.Second {
			t.Fatalf("lifetime=%v", lifetime)
		}
		h := map[string]string{"content-length": strconv.FormatInt(r.Size, 10), "content-type": r.MediaType, "content-md5": r.ContentMD5, "if-none-match": "*", "x-amz-checksum-sha256": *in.ChecksumSHA256}
		signed := make(http.Header)
		for k, v := range h {
			signed.Set(k, v)
		}
		return &awsv4.PresignedHTTPRequest{Method: http.MethodPut, URL: workerOutputSignedURL(s.workerOutputOrigin, "/"+s.bucket+"/"+r.CandidateKey(), now, lifetime, h), SignedHeader: signed}, nil
	}
	s.s3Presigner = f
	return s, f
}

func TestWorkerOutputSignerExactConditionalCapability(t *testing.T) {
	for _, body := range []string{"", "payload", strings.Repeat("x", 32<<10)} {
		t.Run(strconv.Itoa(len(body)), func(t *testing.T) {
			now, r := workerOutputFixture(t)
			workerOutputSetBody(&r, []byte(body))
			r.Ordinal = 1
			s, f := workerOutputAWS(t, now, r)
			c, err := s.PresignAttachedWorkerOutputObject(context.Background(), r)
			if err != nil || f.calls != 1 || c.Object.Key != r.CandidateKey() || c.Object.Size != r.Size || c.Object.SHA256 != r.SHA256 || c.ExpiresAt != r.ExpiresAt || c.Headers["if-none-match"] != "*" {
				t.Fatalf("exact capability err=%v calls=%d", err, f.calls)
			}
			if !strings.HasSuffix(c.Object.Key, "/"+r.PlanDigest+"/1") {
				t.Fatal("key did not bind generated plan and ordinal")
			}
			encoded, err := json.Marshal(c)
			if err != nil || string(encoded) != `"[REDACTED]"` || strings.Contains(fmt.Sprintf("%#v", c), "synthetic-key") {
				t.Fatal("capability leaks through generic diagnostics")
			}
		})
	}
}

func TestWorkerOutputSignerRejectsBeforeSigning(t *testing.T) {
	cases := map[string]func(*ports.AttachedWorkerOutputObjectRequestV1){
		"wildcard-revision": func(r *ports.AttachedWorkerOutputObjectRequestV1) { r.Authorization.ExpectedAttemptRevision = 0 },
		"foreign-head":      func(r *ports.AttachedWorkerOutputObjectRequestV1) { r.Authorization.RunID = "foreign" },
		"foreign-execution-connection": func(r *ports.AttachedWorkerOutputObjectRequestV1) {
			r.AcceptedAttempt.ExecutionConnectionID = "foreign-execution"
		},
		"stale-execution-generation": func(r *ports.AttachedWorkerOutputObjectRequestV1) { r.AcceptedAttempt.ExecutionConnectionGeneration-- },
		"secret-digest": func(r *ports.AttachedWorkerOutputObjectRequestV1) {
			r.Authorization.PresentedSecretDigest = domain.AttachedWorkerConnectionSecretDigest(strings.Repeat("a", 64))
		},
		"bad-profile":     func(r *ports.AttachedWorkerOutputObjectRequestV1) { r.ProfileDigest = "unknown" },
		"upper-sha":       func(r *ports.AttachedWorkerOutputObjectRequestV1) { r.SHA256 = strings.Repeat("A", 64) },
		"bad-md5":         func(r *ports.AttachedWorkerOutputObjectRequestV1) { r.ContentMD5 = strings.Repeat("x", 24) },
		"first-body-over": func(r *ports.AttachedWorkerOutputObjectRequestV1) { r.Size = (32 << 10) + 1 },
		"third-body":      func(r *ports.AttachedWorkerOutputObjectRequestV1) { r.Ordinal = 2 },
		"first-id-over": func(r *ports.AttachedWorkerOutputObjectRequestV1) {
			r.SessionID = domain.SessionID(strings.Repeat("s", 161))
		},
		"unsupported-type": func(r *ports.AttachedWorkerOutputObjectRequestV1) { r.MediaType = "image/png" },
		"empty-json": func(r *ports.AttachedWorkerOutputObjectRequestV1) {
			workerOutputSetBody(r, nil)
			r.MediaType = "application/json"
		},
		"expired":        func(r *ports.AttachedWorkerOutputObjectRequestV1) { r.ExpiresAt = r.ExpiresAt.Add(-10 * time.Second) },
		"renewed-window": func(r *ports.AttachedWorkerOutputObjectRequestV1) { r.ExpiresAt = r.ExpiresAt.Add(6 * time.Second) },
		"subsecond": func(r *ports.AttachedWorkerOutputObjectRequestV1) {
			r.ExpiresAt = r.ExpiresAt.Add(-9500 * time.Millisecond)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			now, r := workerOutputFixture(t)
			s, f := workerOutputAWS(t, now, r)
			mutate(&r)
			if _, err := s.PresignAttachedWorkerOutputObject(context.Background(), r); !errors.Is(err, ports.ErrAttachedWorkerOutputObjectInvalid) || f.calls != 0 {
				t.Fatalf("err=%v calls=%d", err, f.calls)
			}
		})
	}
}

func TestWorkerOutputSignerRejectsReturnedCapabilityDrift(t *testing.T) {
	cases := map[string]func(*awsv4.PresignedHTTPRequest){
		"method": func(v *awsv4.PresignedHTTPRequest) { v.Method = http.MethodPost },
		"foreign-origin": func(v *awsv4.PresignedHTTPRequest) {
			v.URL = strings.Replace(v.URL, "storage.example", "foreign.example", 1)
		},
		"foreign-key": func(v *awsv4.PresignedHTTPRequest) { v.URL = strings.Replace(v.URL, "/0?", "/1?", 1) },
		"userinfo": func(v *awsv4.PresignedHTTPRequest) {
			v.URL = strings.Replace(v.URL, "https://", "https://user:secret@", 1)
		},
		"header":               func(v *awsv4.PresignedHTTPRequest) { v.SignedHeader.Set("If-None-Match", "other") },
		"authorization-header": func(v *awsv4.PresignedHTTPRequest) { v.SignedHeader.Set("Authorization", "Bearer raw-secret") },
		"duplicate-header":     func(v *awsv4.PresignedHTTPRequest) { v.SignedHeader["content-type"] = []string{"text/plain"} },
		"unknown-query":        func(v *awsv4.PresignedHTTPRequest) { v.URL += "&credential=raw-secret" },
		"over-url":             func(v *awsv4.PresignedHTTPRequest) { v.URL += strings.Repeat("x", workerOutputURLBytes) },
		"duplicate-query":      func(v *awsv4.PresignedHTTPRequest) { v.URL += "&X-Amz-Expires=10" },
		"renewed-expiry": func(v *awsv4.PresignedHTTPRequest) {
			u, _ := url.Parse(v.URL)
			q := u.Query()
			q.Set("X-Amz-Expires", "15")
			u.RawQuery = q.Encode()
			v.URL = u.String()
		},
		"precondition-unsigned": func(v *awsv4.PresignedHTTPRequest) {
			u, _ := url.Parse(v.URL)
			q := u.Query()
			q.Set("X-Amz-SignedHeaders", strings.Replace(q.Get("X-Amz-SignedHeaders"), ";if-none-match", "", 1))
			u.RawQuery = q.Encode()
			v.URL = u.String()
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			now, r := workerOutputFixture(t)
			s, f := workerOutputAWS(t, now, r)
			original := f.put
			f.put = func(ctx context.Context, in *s3.PutObjectInput, lifetime time.Duration) (*awsv4.PresignedHTTPRequest, error) {
				v, e := original(ctx, in, lifetime)
				mutate(v)
				return v, e
			}
			if _, err := s.PresignAttachedWorkerOutputObject(context.Background(), r); !errors.Is(err, ports.ErrAttachedWorkerOutputObjectInvalid) || f.calls != 1 {
				t.Fatalf("err=%v calls=%d", err, f.calls)
			}
		})
	}
}

func TestWorkerOutputSignerIAMPreservesConditionalMetadata(t *testing.T) {
	now, r := workerOutputFixture(t)
	s := &Store{bucket: "artifact-bucket", workerOutputOrigin: "https://storage.example", now: func() time.Time { return now }}
	calls := 0
	s.iamClient = &iamObjectClient{tokens: tokenProviderFunc(func(context.Context) (string, error) { return "synthetic-iam-token", nil }), presign: presignServiceFunc(func(_ context.Context, in *ycstorage.PresignURLsRequest, _ ...grpc.CallOption) (*ycstorage.PresignURLsResponse, error) {
		calls++
		if in.BucketName != s.bucket || len(in.Objects) != 1 || in.Objects[0].Name != r.CandidateKey() || in.Objects[0].Method != http.MethodPut || in.Objects[0].Expires != 10 || in.Objects[0].Headers["if-none-match"] != "*" || in.Objects[0].Headers["content-length"] != "7" || in.Objects[0].Headers["content-md5"] != r.ContentMD5 || in.Objects[0].Headers["x-amz-checksum-sha256"] == "" {
			t.Fatal("IAM exact conditional request missing")
		}
		return &ycstorage.PresignURLsResponse{Urls: []string{workerOutputSignedURL("https://artifact-bucket.storage.example", "/"+r.CandidateKey(), now, 10*time.Second, in.Objects[0].Headers)}}, nil
	})}
	c, err := s.PresignAttachedWorkerOutputObject(context.Background(), r)
	if err != nil || calls != 1 || c.ExpiresAt != r.ExpiresAt {
		t.Fatalf("IAM err=%v calls=%d", err, calls)
	}
}

func TestWorkerOutputSignerRetainsOriginalCutoffOnLateReturn(t *testing.T) {
	now, r := workerOutputFixture(t)
	s, f := workerOutputAWS(t, now, r)
	original := f.put
	f.put = func(ctx context.Context, in *s3.PutObjectInput, d time.Duration) (*awsv4.PresignedHTTPRequest, error) {
		v, e := original(ctx, in, d)
		now = r.ExpiresAt
		return v, e
	}
	s.now = func() time.Time { return now }
	if _, err := s.PresignAttachedWorkerOutputObject(context.Background(), r); !errors.Is(err, ports.ErrAttachedWorkerOutputObjectInvalid) || f.calls != 1 {
		t.Fatalf("late result err=%v calls=%d", err, f.calls)
	}
}

func TestWorkerOutputSignerHoistedChecksumIsStillBound(t *testing.T) {
	now, r := workerOutputFixture(t)
	s, f := workerOutputAWS(t, now, r)
	original := f.put
	f.put = func(ctx context.Context, in *s3.PutObjectInput, d time.Duration) (*awsv4.PresignedHTTPRequest, error) {
		v, e := original(ctx, in, d)
		u, _ := url.Parse(v.URL)
		q := u.Query()
		q.Set("X-Amz-Checksum-Sha256", v.SignedHeader.Get("X-Amz-Checksum-Sha256"))
		q.Set("X-Amz-SignedHeaders", strings.Replace(q.Get("X-Amz-SignedHeaders"), ";x-amz-checksum-sha256", "", 1))
		v.SignedHeader.Del("X-Amz-Checksum-Sha256")
		u.RawQuery = q.Encode()
		v.URL = u.String()
		return v, e
	}
	if _, err := s.PresignAttachedWorkerOutputObject(context.Background(), r); err != nil || f.calls != 1 {
		t.Fatalf("hoisted checksum err=%v calls=%d", err, f.calls)
	}
}

func TestWorkerOutputSignerRejectsInvalidServerOriginAndCancellation(t *testing.T) {
	for _, endpoint := range []string{"", "http://storage.example", "https://user:secret@storage.example", "https://storage.example/base", "https://storage.example?auth=secret", "https://storage.example#fragment"} {
		t.Run(strconv.Itoa(len(endpoint))+"-"+strings.ReplaceAll(endpoint, "/", "_"), func(t *testing.T) {
			now, r := workerOutputFixture(t)
			s, f := workerOutputAWS(t, now, r)
			s.workerOutputOrigin = workerOutputOrigin(endpoint)
			if _, err := s.PresignAttachedWorkerOutputObject(context.Background(), r); !errors.Is(err, ports.ErrAttachedWorkerOutputObjectInvalid) || f.calls != 0 {
				t.Fatalf("configuration err=%v calls=%d", err, f.calls)
			}
		})
	}
	now, r := workerOutputFixture(t)
	s, f := workerOutputAWS(t, now, r)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.PresignAttachedWorkerOutputObject(ctx, r); !errors.Is(err, ports.ErrAttachedWorkerOutputObjectInvalid) || f.calls != 0 {
		t.Fatalf("cancel err=%v calls=%d", err, f.calls)
	}
}

func TestWorkerOutputSignerPreservesMaximumDomainID(t *testing.T) {
	now, r := workerOutputFixture(t)
	h := r.AcceptedAttempt
	h.TenantID = domain.TenantID(strings.Repeat("t", 160))
	h.OwnerUserID = domain.UserID(strings.Repeat("o", 160))
	h.WorkerID = domain.AttachedWorkerID(strings.Repeat("w", 160))
	h.ConnectionID = domain.AttachedWorkerConnectionID(strings.Repeat("c", 160))
	h.ExecutionConnectionID = h.ConnectionID
	h.RunID = domain.RunID(strings.Repeat("r", 160))
	h.AttemptID = domain.AttemptID(strings.Repeat("a", 160))
	h.LeaseID = domain.LeaseID(strings.Repeat("l", 160))
	h.ReservationID = domain.QuotaReservationID(strings.Repeat("q", 160))
	fence, err := domain.NewAttachedWorkerFenceTokenV1(h.TenantID, h.OwnerUserID, h.WorkerID, h.RunID, h.AttemptID, h.LeaseID, h.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	h.FenceToken = fence
	r.AcceptedAttempt = h
	r.SessionID = domain.SessionID(strings.Repeat("s", 160))
	a := r.Authorization
	a.TenantID = h.TenantID
	a.OwnerUserID = h.OwnerUserID
	a.WorkerID = h.WorkerID
	a.ConnectionID = h.ConnectionID
	a.RunID = h.RunID
	a.AttemptID = h.AttemptID
	a.LeaseID = h.LeaseID
	a.FenceToken = fence
	r.Authorization = a
	s, f := workerOutputAWS(t, now, r)
	if _, err := s.PresignAttachedWorkerOutputObject(context.Background(), r); err != nil || f.calls != 1 {
		t.Fatalf("max-domain-ID err=%v calls=%d", err, f.calls)
	}
}
