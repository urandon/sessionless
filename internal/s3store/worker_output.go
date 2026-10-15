package s3store

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ycstorage "github.com/yandex-cloud/go-genproto/yandex/cloud/storage/v1"
	"google.golang.org/grpc"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// These are explicit capability transport admission ceilings, not native fit,
// profile permissions, body allowances, or separate cumulative upload budgets.
const workerOutputURLBytes = 8 << 10
const workerOutputHeaderBytes = 4 << 10

func workerOutputOrigin(endpoint string) string {
	if len(endpoint) == 0 || len(endpoint) > 2048 {
		return ""
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" ||
		u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" ||
		u.RawPath != "" || u.Path != "" && u.Path != "/" || strings.HasSuffix(u.Host, ":") {
		return ""
	}
	return "https://" + strings.ToLower(u.Host)
}

func workerOutputBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 || bucket[0] == '.' || bucket[0] == '-' || bucket[len(bucket)-1] == '.' || bucket[len(bucket)-1] == '-' || strings.Contains(bucket, "..") {
		return false
	}
	for _, b := range bucket {
		if b != '.' && b != '-' && (b < 'a' || b > 'z') && (b < '0' || b > '9') {
			return false
		}
	}
	return true
}

func (s *Store) workerOutputLocation(key string) (string, string, error) {
	if s == nil || s.workerOutputOrigin == "" || !workerOutputBucket(s.bucket) {
		return "", "", ports.ErrAttachedWorkerOutputObjectInvalid
	}
	u, err := url.Parse(s.workerOutputOrigin)
	if err != nil || workerOutputOrigin(s.workerOutputOrigin) != s.workerOutputOrigin {
		return "", "", ports.ErrAttachedWorkerOutputObjectInvalid
	}
	if s.workerOutputPathStyle {
		return s.workerOutputOrigin, "/" + s.bucket + "/" + key, nil
	}
	// Bucket is a bounded DNS label sequence, not an arbitrary URL prefix.
	u.Host = s.bucket + "." + u.Host
	return u.Scheme + "://" + u.Host, "/" + key, nil
}

// PresignAttachedWorkerOutputObject is the distinct server-side exact-object
// signer. Its caller must ALREADY own the admitted durable pre-Put record. This
// method cannot establish currentness, private producer provenance, two-body
// aggregate custody, or remote quiescence. It performs no artifact Put.
func (s *Store) PresignAttachedWorkerOutputObject(ctx context.Context, request ports.AttachedWorkerOutputObjectRequestV1) (ports.AttachedWorkerOutputObjectCapabilityV1, error) {
	deny := func() (ports.AttachedWorkerOutputObjectCapabilityV1, error) {
		return ports.AttachedWorkerOutputObjectCapabilityV1{}, ports.ErrAttachedWorkerOutputObjectInvalid
	}
	if s == nil || ctx == nil || ctx.Err() != nil || s.now == nil {
		return deny()
	}
	now := s.now()
	if request.ValidateAt(now) != nil {
		return deny()
	}
	if d, ok := ctx.Deadline(); ok && request.ExpiresAt.After(d) {
		return deny()
	}
	key := request.CandidateKey()
	origin, path, err := s.workerOutputLocation(key)
	if err != nil {
		return deny()
	}
	// Floor, never round up or restart a lifetime. Sub-second remaining authority
	// cannot be represented by either existing signer and therefore denies.
	lifetime := request.ExpiresAt.Sub(now) / time.Second * time.Second
	if lifetime < time.Second {
		return deny()
	}
	digest, err := hex.DecodeString(request.SHA256)
	if err != nil {
		return deny()
	}
	checksum := base64.StdEncoding.EncodeToString(digest)
	headers := map[string]string{"content-length": strconv.FormatInt(request.Size, 10), "content-type": request.MediaType,
		"content-md5": request.ContentMD5, "if-none-match": "*", "x-amz-checksum-sha256": checksum}
	var capabilityURL string
	if s.iamClient != nil {
		// The lower-level IAM signer receives the SAME conditional/checksum
		// headers. The old browser PresignUpload branch is deliberately not used.
		inputHeaders := make(map[string]string, len(headers))
		for k, v := range headers {
			inputHeaders[k] = v
		}
		if s.iamClient.presign == nil || s.iamClient.tokens == nil {
			return deny()
		}
		private := *s.iamClient
		private.presign = boundedWorkerOutputPresign{delegate: private.presign}
		capability, err := private.presignObject(ctx, s.bucket, key, http.MethodPut, inputHeaders, lifetime, request.ExpiresAt)
		if err != nil || capability.Method != http.MethodPut || capability.ExpiresAt != request.ExpiresAt || !sameWorkerOutputHeaders(capability.Headers, headers) {
			return deny()
		}
		capabilityURL = capability.URL
	} else {
		if s.s3Presigner == nil {
			return deny()
		}
		result, err := s.s3Presigner.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key),
			ContentLength: aws.Int64(request.Size), ContentType: aws.String(request.MediaType), ContentMD5: aws.String(request.ContentMD5),
			ChecksumSHA256: aws.String(checksum), IfNoneMatch: aws.String("*")}, func(options *s3.PresignOptions) { options.Expires = lifetime })
		if err != nil || result == nil || result.Method != http.MethodPut {
			return deny()
		}
		// A signed header cannot be silently overwritten by required DATA. The
		// checksum may instead be hoisted into the signed query by this exact SDK.
		if len(result.SignedHeader) > 6 {
			return deny()
		}
		actual := make(map[string]string, len(result.SignedHeader))
		var bytes int
		for name, values := range result.SignedHeader {
			if len(name) > workerOutputHeaderBytes-bytes || len(values) != 1 {
				return deny()
			}
			bytes += len(name)
			if len(values[0]) > workerOutputHeaderBytes-bytes {
				return deny()
			}
			bytes += len(values[0])
			lower := strings.ToLower(name)
			if _, exists := actual[lower]; exists {
				return deny()
			}
			if lower == "host" {
				if values[0] != strings.TrimPrefix(origin, "https://") {
					return deny()
				}
				continue
			}
			if headers[lower] != values[0] {
				return deny()
			}
			actual[lower] = values[0]
		}
		for _, name := range []string{"content-length", "content-type", "content-md5", "if-none-match"} {
			if actual[name] != headers[name] {
				return deny()
			}
		}
		capabilityURL = result.URL
	}
	returned := s.now()
	if ctx.Err() != nil || returned.Before(now) || request.ValidateAt(returned) != nil || !validWorkerOutputURL(capabilityURL, origin, path, headers, request.ExpiresAt, returned) {
		return deny()
	}
	// ExpiresAt preserves the original authority ceiling. The signed URL can
	// expire earlier due to whole-second encoding; this is not a promise that
	// the provider accepts it until the ceiling or that a late write has ceased.
	return ports.AttachedWorkerOutputObjectCapabilityV1{Version: ports.AttachedWorkerOutputObjectVersionV1, Method: http.MethodPut,
		URL: capabilityURL, Headers: headers, Object: domain.BlobRef{TenantID: request.Authorization.TenantID, Key: key, Size: request.Size, SHA256: request.SHA256}, ExpiresAt: request.ExpiresAt}, nil
}

// Reuse the original lower-level IAM signer, but bound its one URL before that
// helper parses/copies it. One protobuf string has a one-byte tag and at most a
// two-byte length for this admitted URL bound. gRPC's receive cap is transport
// admission, not a claim about every native allocation or remote quiescence.
type boundedWorkerOutputPresign struct {
	delegate ycstorage.PresignServiceClient
}

func (p boundedWorkerOutputPresign) Create(ctx context.Context, request *ycstorage.PresignURLsRequest, options ...grpc.CallOption) (*ycstorage.PresignURLsResponse, error) {
	if p.delegate == nil || len(options) != 0 {
		return nil, ports.ErrAttachedWorkerOutputObjectInvalid
	}
	response, err := p.delegate.Create(ctx, request, grpc.MaxCallRecvMsgSize(workerOutputURLBytes+3))
	if err != nil || response == nil || len(response.GetUrls()) != 1 || len(response.GetUrls()[0]) > workerOutputURLBytes || len(response.ProtoReflect().GetUnknown()) != 0 {
		return nil, ports.ErrAttachedWorkerOutputObjectInvalid
	}
	return response, nil
}

func sameWorkerOutputHeaders(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range b {
		if a[key] != value {
			return false
		}
	}
	return true
}

func validWorkerOutputURL(raw, origin, path string, headers map[string]string, cutoff, now time.Time) bool {
	if len(raw) == 0 || len(raw) > workerOutputURLBytes {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme+"://"+u.Host != origin || u.User != nil || u.Opaque != "" || u.Path != path || u.RawPath != "" || u.Fragment != "" || u.RawFragment != "" || u.ForceQuery || u.RawQuery == "" {
		return false
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(q) < 6 || len(q) > 11 {
		return false
	}
	for name, values := range q {
		if len(values) != 1 || values[0] == "" {
			return false
		}
		switch name {
		case "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-Expires", "X-Amz-SignedHeaders", "X-Amz-Signature", "X-Amz-Security-Token", "X-Amz-Checksum-Sha256", "X-Amz-Content-Sha256", "x-id", "x-amz-sdk-checksum-algorithm":
		default:
			return false
		}
	}
	if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" || q.Get("x-id") != "" && q.Get("x-id") != "PutObject" ||
		q.Get("X-Amz-Content-Sha256") != "" && q.Get("X-Amz-Content-Sha256") != "UNSIGNED-PAYLOAD" ||
		q.Get("x-amz-sdk-checksum-algorithm") != "" && q.Get("x-amz-sdk-checksum-algorithm") != "SHA256" {
		return false
	}
	credential := strings.Split(q.Get("X-Amz-Credential"), "/")
	if len(credential) != 5 || !workerOutputToken(credential[0], 256) || !workerOutputToken(credential[2], 64) || credential[3] != "s3" || credential[4] != "aws4_request" {
		return false
	}
	date, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
	if err != nil || date.After(now) || credential[1] != date.Format("20060102") {
		return false
	}
	seconds, err := strconv.ParseUint(q.Get("X-Amz-Expires"), 10, 8)
	if err != nil || seconds == 0 || seconds > 15 || strconv.FormatUint(seconds, 10) != q.Get("X-Amz-Expires") {
		return false
	}
	expires := date.Add(time.Duration(seconds) * time.Second)
	if !now.Before(expires) || expires.After(cutoff) {
		return false
	}
	if !lowerWorkerOutputHex(q.Get("X-Amz-Signature"), 64) {
		return false
	}
	signed := strings.Split(q.Get("X-Amz-SignedHeaders"), ";")
	seen := make(map[string]bool, 6)
	previous := ""
	for _, name := range signed {
		if name <= previous || seen[name] {
			return false
		}
		previous, seen[name] = name, true
		if name != "host" && headers[name] == "" {
			return false
		}
	}
	for _, name := range []string{"host", "content-length", "content-md5", "content-type", "if-none-match"} {
		if !seen[name] {
			return false
		}
	}
	if !seen["x-amz-checksum-sha256"] && q.Get("X-Amz-Checksum-Sha256") != headers["x-amz-checksum-sha256"] {
		return false
	}
	if v := q.Get("X-Amz-Checksum-Sha256"); v != "" && v != headers["x-amz-checksum-sha256"] {
		return false
	}
	return true
}

func lowerWorkerOutputHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, b := range s {
		if b < '0' || b > '9' && b < 'a' || b > 'f' {
			return false
		}
	}
	return true
}

func workerOutputToken(s string, limit int) bool {
	if len(s) == 0 || len(s) > limit {
		return false
	}
	for _, b := range s {
		if b != '-' && b != '_' && (b < 'a' || b > 'z') && (b < 'A' || b > 'Z') && (b < '0' || b > '9') {
			return false
		}
	}
	return true
}

var _ ports.AttachedWorkerOutputObjectSignerV1 = (*Store)(nil)
