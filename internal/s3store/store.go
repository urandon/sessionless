// Package s3store implements the tenant-scoped blob port over S3-compatible
// object storage. Local development uses MinIO; cloud deployments use the same
// adapter with Yandex Object Storage endpoints and workload credentials.
package s3store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ycstorage "github.com/yandex-cloud/go-genproto/yandex/cloud/storage/v1"
	ycmetadata "github.com/ydb-platform/ydb-go-yc-metadata"
	"google.golang.org/grpc"
	grpccredentials "google.golang.org/grpc/credentials"
	grpcmetadata "google.golang.org/grpc/metadata"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const defaultMaxObjectBytes int64 = 64 << 20

const (
	defaultIAMPresignEndpoint = "storage.api.cloud.yandex.net:443"
	maxCapabilityLifetime     = 15 * time.Minute
)

type Config struct {
	Endpoint               string
	Region                 string
	Bucket                 string
	AccessKeyID            string
	SecretAccessKey        string
	ForcePathStyle         bool
	IAMMetadataCredentials bool
	IAMToken               string
	MaxObjectBytes         int64
}

type Store struct {
	s3Client       s3ObjectAPI
	s3Presigner    s3PresignAPI
	iamClient      *iamObjectClient
	bucket         string
	maxObjectBytes int64
	now            func() time.Time
}

type s3ObjectAPI interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
}

type s3PresignAPI interface {
	PresignPutObject(context.Context, *s3.PutObjectInput, ...func(*s3.PresignOptions)) (*awsv4.PresignedHTTPRequest, error)
	PresignGetObject(context.Context, *s3.GetObjectInput, ...func(*s3.PresignOptions)) (*awsv4.PresignedHTTPRequest, error)
}

type tokenProvider interface {
	Token(context.Context) (string, error)
}

type iamObjectClient struct {
	endpoint *url.URL
	tokens   tokenProvider
	http     *http.Client
	presign  ycstorage.PresignServiceClient
}

func New(ctx context.Context, config Config) (*Store, error) {
	if strings.TrimSpace(config.Region) == "" {
		return nil, fmt.Errorf("S3 region is required")
	}
	if strings.TrimSpace(config.Bucket) == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}
	if (config.AccessKeyID == "") != (config.SecretAccessKey == "") {
		return nil, fmt.Errorf("S3 access key and secret must be supplied together")
	}
	if (config.IAMMetadataCredentials || config.IAMToken != "") && config.AccessKeyID != "" {
		return nil, fmt.Errorf("S3 IAM credentials and static credentials are mutually exclusive")
	}
	if config.IAMMetadataCredentials && config.IAMToken != "" {
		return nil, fmt.Errorf("S3 IAM metadata and injected token are mutually exclusive")
	}
	if config.MaxObjectBytes <= 0 {
		config.MaxObjectBytes = defaultMaxObjectBytes
	}
	store := &Store{bucket: config.Bucket, maxObjectBytes: config.MaxObjectBytes, now: time.Now}
	if config.IAMMetadataCredentials || config.IAMToken != "" {
		if strings.TrimSpace(config.Endpoint) == "" {
			return nil, fmt.Errorf("S3 endpoint is required for IAM metadata credentials")
		}
		endpoint, err := url.Parse(config.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("parse S3 endpoint: %w", err)
		}
		if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
			return nil, fmt.Errorf("S3 endpoint must use http or https")
		}
		var tokens tokenProvider = ycmetadata.NewInstanceServiceAccount()
		if config.IAMToken != "" {
			tokens = fixedToken(config.IAMToken)
		}
		connection, err := grpc.NewClient(defaultIAMPresignEndpoint, grpc.WithTransportCredentials(
			grpccredentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
		))
		if err != nil {
			return nil, fmt.Errorf("configure Object Storage presign API: %w", err)
		}
		store.iamClient = &iamObjectClient{
			endpoint: endpoint,
			tokens:   tokens,
			http:     &http.Client{Timeout: 30 * time.Second},
			presign:  ycstorage.NewPresignServiceClient(connection),
		}
		return store, nil
	}

	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(config.Region),
	}
	if config.AccessKeyID != "" {
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(
			awscredentials.NewStaticCredentialsProvider(config.AccessKeyID, config.SecretAccessKey, ""),
		))
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load S3 configuration: %w", err)
	}

	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.UsePathStyle = config.ForcePathStyle
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		options.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if config.Endpoint != "" {
			options.BaseEndpoint = aws.String(config.Endpoint)
		}
	})
	store.s3Client = client
	store.s3Presigner = s3.NewPresignClient(client)
	return store, nil
}

type fixedToken string

func (token fixedToken) Token(context.Context) (string, error) {
	if strings.TrimSpace(string(token)) == "" {
		return "", fmt.Errorf("injected IAM token must not be empty")
	}
	return string(token), nil
}

func (store *Store) PresignUpload(
	ctx context.Context,
	request ports.UploadCapabilityRequest,
) (ports.ObjectCapability, error) {
	objectKey, digest, contentMD5, err := store.validateUploadCapability(request)
	if err != nil {
		return ports.ObjectCapability{}, err
	}
	expiresAt := store.currentTime().Add(request.ExpiresIn)
	headers := map[string]string{
		"content-length": strconv.FormatInt(request.Size, 10),
		"content-md5":    contentMD5,
		"content-type":   request.MediaType,
	}
	if store.iamClient != nil {
		return store.iamClient.presignObject(
			ctx, store.bucket, objectKey, http.MethodPut, headers, request.ExpiresIn, expiresAt,
		)
	}
	if store.s3Presigner == nil {
		return ports.ObjectCapability{}, fmt.Errorf("S3 presigner is not configured")
	}
	headers["x-amz-checksum-sha256"] = base64.StdEncoding.EncodeToString(digest)
	result, err := store.s3Presigner.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:         aws.String(store.bucket),
		Key:            aws.String(objectKey),
		ContentLength:  aws.Int64(request.Size),
		ContentMD5:     aws.String(contentMD5),
		ContentType:    aws.String(request.MediaType),
		ChecksumSHA256: aws.String(headers["x-amz-checksum-sha256"]),
	}, func(options *s3.PresignOptions) {
		options.Expires = request.ExpiresIn
	})
	if err != nil {
		return ports.ObjectCapability{}, fmt.Errorf("presign S3 upload: %w", err)
	}
	return presignedCapability(result, headers, expiresAt), nil
}

func (store *Store) PresignDownload(
	ctx context.Context,
	tenantID domain.TenantID,
	ref domain.BlobRef,
	expiresIn time.Duration,
) (ports.ObjectCapability, error) {
	if err := authorizeRef(tenantID, ref); err != nil {
		return ports.ObjectCapability{}, err
	}
	if err := validateCapabilityLifetime(expiresIn); err != nil {
		return ports.ObjectCapability{}, err
	}
	expiresAt := store.currentTime().Add(expiresIn)
	if store.iamClient != nil {
		return store.iamClient.presignObject(
			ctx, store.bucket, ref.Key, http.MethodGet, nil, expiresIn, expiresAt,
		)
	}
	if store.s3Presigner == nil {
		return ports.ObjectCapability{}, fmt.Errorf("S3 presigner is not configured")
	}
	result, err := store.s3Presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(store.bucket), Key: aws.String(ref.Key),
	}, func(options *s3.PresignOptions) {
		options.Expires = expiresIn
	})
	if err != nil {
		return ports.ObjectCapability{}, fmt.Errorf("presign S3 download: %w", err)
	}
	return presignedCapability(result, nil, expiresAt), nil
}

func (store *Store) StatObject(
	ctx context.Context,
	tenantID domain.TenantID,
	key string,
) (ports.ObjectMetadata, error) {
	objectKey, err := tenantObjectKey(tenantID, key)
	if err != nil {
		return ports.ObjectMetadata{}, err
	}
	if store.iamClient != nil {
		return store.iamClient.stat(ctx, store.bucket, tenantID, objectKey, store.maxObjectBytes)
	}
	result, err := store.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(store.bucket), Key: aws.String(objectKey), ChecksumMode: "ENABLED",
	})
	if err != nil {
		return ports.ObjectMetadata{}, fmt.Errorf("head S3 object: %w", err)
	}
	metadata, err := metadataFromHead(
		tenantID, objectKey, aws.ToInt64(result.ContentLength), aws.ToString(result.ChecksumSHA256),
		aws.ToString(result.ContentType), aws.ToString(result.ETag),
	)
	if err == nil {
		err = validateStoredObjectSize(metadata.Blob.Size, store.maxObjectBytes)
	}
	if err != nil || metadata.Blob.SHA256 != "" {
		return metadata, err
	}
	opened, err := store.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(store.bucket), Key: aws.String(objectKey),
		IfMatch: aws.String(quoteETag(metadata.ETag)),
	})
	if err != nil {
		return ports.ObjectMetadata{}, fmt.Errorf("conditionally get S3 object for SHA-256 verification: %w", err)
	}
	defer opened.Body.Close()
	digest, err := hashObjectSHA256(opened.Body, metadata.Blob.Size, store.maxObjectBytes)
	if err != nil {
		return ports.ObjectMetadata{}, err
	}
	metadata.Blob.SHA256 = digest
	return metadata, nil
}

func (store *Store) PromoteObject(
	ctx context.Context,
	request ports.PromoteObjectRequest,
) (domain.BlobRef, error) {
	if err := authorizeRef(request.TenantID, request.Source); err != nil {
		return domain.BlobRef{}, err
	}
	if !strings.HasPrefix(request.Source.Key, domain.TenantObjectPrefix(request.TenantID)+"uploads/") {
		return domain.BlobRef{}, fmt.Errorf("promotion source must be an upload staging object")
	}
	if strings.TrimSpace(request.SourceETag) == "" {
		return domain.BlobRef{}, fmt.Errorf("promotion source ETag is required")
	}
	if err := validateMediaType(request.MediaType); err != nil {
		return domain.BlobRef{}, err
	}
	finalKey, err := tenantObjectKey(request.TenantID, request.FinalKey)
	if err != nil {
		return domain.BlobRef{}, err
	}
	if finalKey == request.Source.Key || strings.HasPrefix(finalKey, domain.TenantObjectPrefix(request.TenantID)+"uploads/") {
		return domain.BlobRef{}, fmt.Errorf("promotion final key must be outside upload staging")
	}
	staged, err := store.StatObject(ctx, request.TenantID, request.Source.Key)
	if err != nil {
		return domain.BlobRef{}, fmt.Errorf("verify promotion source: %w", err)
	}
	if staged.Blob != request.Source || staged.ETag != normalizeETag(request.SourceETag) ||
		staged.MediaType != request.MediaType {
		return domain.BlobRef{}, fmt.Errorf("promotion source changed after verification")
	}
	if store.iamClient != nil {
		err = store.iamClient.copyIfAbsent(
			ctx, store.bucket, request.Source.Key, request.SourceETag, finalKey,
			request.MediaType, request.Source.SHA256,
		)
	} else {
		_, err = store.s3Client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            aws.String(store.bucket),
			Key:               aws.String(finalKey),
			CopySource:        aws.String(url.PathEscape(store.bucket + "/" + request.Source.Key)),
			CopySourceIfMatch: aws.String(quoteETag(request.SourceETag)),
			IfNoneMatch:       aws.String("*"),
			ContentType:       aws.String(request.MediaType),
			MetadataDirective: "REPLACE",
			ChecksumAlgorithm: "SHA256",
			Metadata: map[string]string{
				"sha256": request.Source.SHA256,
			},
		})
	}
	if err != nil {
		copyErr := fmt.Errorf("conditionally promote S3 object: %w", err)
		existing, statErr := store.StatObject(ctx, request.TenantID, finalKey)
		if statErr == nil && promotedObjectMatches(existing, request.Source, finalKey, request.MediaType) {
			return existing.Blob, nil
		}
		if statErr != nil {
			return domain.BlobRef{}, fmt.Errorf("%w (exact destination verification failed: %v)", copyErr, statErr)
		}
		return domain.BlobRef{}, fmt.Errorf("%w (existing destination metadata differs from verified source)", copyErr)
	}
	observed, err := store.StatObject(ctx, request.TenantID, finalKey)
	if err != nil {
		return domain.BlobRef{}, fmt.Errorf("verify promoted S3 object: %w", err)
	}
	if !promotedObjectMatches(observed, request.Source, finalKey, request.MediaType) {
		return domain.BlobRef{}, fmt.Errorf("promoted S3 object metadata differs from verified source")
	}
	return observed.Blob, nil
}

func promotedObjectMatches(
	observed ports.ObjectMetadata,
	source domain.BlobRef,
	finalKey string,
	mediaType string,
) bool {
	return observed.Blob.TenantID == source.TenantID &&
		observed.Blob.Key == finalKey &&
		observed.Blob.Size == source.Size &&
		observed.Blob.SHA256 == source.SHA256 &&
		observed.MediaType == mediaType
}

func (store *Store) validateUploadCapability(
	request ports.UploadCapabilityRequest,
) (string, []byte, string, error) {
	objectKey, err := tenantObjectKey(request.TenantID, request.ObjectKey)
	if err != nil {
		return "", nil, "", err
	}
	if !strings.HasPrefix(objectKey, domain.TenantObjectPrefix(request.TenantID)+"uploads/") {
		return "", nil, "", fmt.Errorf("upload capability key must be under upload staging")
	}
	if request.Size <= 0 || request.Size > store.maxObjectBytes {
		return "", nil, "", fmt.Errorf("upload size must be between 1 and %d bytes", store.maxObjectBytes)
	}
	if err := validateMediaType(request.MediaType); err != nil {
		return "", nil, "", err
	}
	digest, err := hex.DecodeString(request.SHA256)
	if err != nil || len(digest) != sha256.Size || request.SHA256 != strings.ToLower(request.SHA256) {
		return "", nil, "", fmt.Errorf("upload SHA-256 must be a lowercase 64-character digest")
	}
	md5Digest, err := base64.StdEncoding.Strict().DecodeString(request.ContentMD5)
	if err != nil || len(md5Digest) != 16 || base64.StdEncoding.EncodeToString(md5Digest) != request.ContentMD5 {
		return "", nil, "", fmt.Errorf("upload Content-MD5 must be the canonical base64 encoding of a 16-byte digest")
	}
	if err := validateCapabilityLifetime(request.ExpiresIn); err != nil {
		return "", nil, "", err
	}
	return objectKey, digest, request.ContentMD5, nil
}

func (store *Store) currentTime() time.Time {
	if store.now != nil {
		return store.now()
	}
	return time.Now()
}

func validateCapabilityLifetime(lifetime time.Duration) error {
	if lifetime < time.Second || lifetime > maxCapabilityLifetime || lifetime%time.Second != 0 {
		return fmt.Errorf("capability lifetime must be whole seconds between 1s and %s", maxCapabilityLifetime)
	}
	return nil
}

func validateMediaType(mediaType string) error {
	if strings.TrimSpace(mediaType) == "" || strings.TrimSpace(mediaType) != mediaType ||
		strings.ContainsAny(mediaType, "\r\n") {
		return fmt.Errorf("object media type must be a non-empty HTTP header value")
	}
	return nil
}

func presignedCapability(
	request *awsv4.PresignedHTTPRequest,
	requiredHeaders map[string]string,
	expiresAt time.Time,
) ports.ObjectCapability {
	headers := make(map[string]string, len(request.SignedHeader)+len(requiredHeaders))
	for key, values := range request.SignedHeader {
		headers[strings.ToLower(key)] = strings.Join(values, ",")
	}
	for key, value := range requiredHeaders {
		headers[strings.ToLower(key)] = value
	}
	return ports.ObjectCapability{
		Method: request.Method, URL: request.URL, Headers: headers, ExpiresAt: expiresAt,
	}
}

func (store *Store) Put(
	ctx context.Context,
	tenantID domain.TenantID,
	key string,
	body io.Reader,
) (domain.BlobRef, error) {
	objectKey, err := tenantObjectKey(tenantID, key)
	if err != nil {
		return domain.BlobRef{}, err
	}
	if body == nil {
		return domain.BlobRef{}, fmt.Errorf("blob body is required")
	}

	data, err := io.ReadAll(io.LimitReader(body, store.maxObjectBytes+1))
	if err != nil {
		return domain.BlobRef{}, fmt.Errorf("read blob: %w", err)
	}
	if int64(len(data)) > store.maxObjectBytes {
		return domain.BlobRef{}, fmt.Errorf("blob exceeds %d bytes", store.maxObjectBytes)
	}
	digest := sha256.Sum256(data)

	if store.iamClient != nil {
		err = store.iamClient.put(ctx, store.bucket, objectKey, data)
	} else {
		_, err = store.s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(store.bucket),
			Key:           aws.String(objectKey),
			Body:          bytes.NewReader(data),
			ContentLength: aws.Int64(int64(len(data))),
		})
	}
	if err != nil {
		return domain.BlobRef{}, fmt.Errorf("put S3 object: %w", err)
	}

	return domain.BlobRef{
		TenantID: tenantID,
		Key:      objectKey,
		Size:     int64(len(data)),
		SHA256:   hex.EncodeToString(digest[:]),
	}, nil
}

func (store *Store) Open(
	ctx context.Context,
	tenantID domain.TenantID,
	ref domain.BlobRef,
) (io.ReadCloser, error) {
	if err := authorizeRef(tenantID, ref); err != nil {
		return nil, err
	}
	if store.iamClient != nil {
		return store.iamClient.open(ctx, store.bucket, ref.Key)
	}
	result, err := store.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(store.bucket),
		Key:    aws.String(ref.Key),
	})
	if err != nil {
		return nil, fmt.Errorf("get S3 object: %w", err)
	}
	return result.Body, nil
}

func (store *Store) Delete(ctx context.Context, tenantID domain.TenantID, ref domain.BlobRef) error {
	if err := authorizeRef(tenantID, ref); err != nil {
		return err
	}
	if store.iamClient != nil {
		return store.iamClient.delete(ctx, store.bucket, ref.Key)
	}
	_, err := store.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(store.bucket),
		Key:    aws.String(ref.Key),
	})
	if err != nil {
		return fmt.Errorf("delete S3 object: %w", err)
	}
	return nil
}

// DeletePrefix is intentionally outside the BlobStore port. It exists only
// for the guarded pre-production reset command and refuses bucket-root or
// non-Sessionless prefixes.
func (store *Store) DeletePrefix(ctx context.Context, prefix string) (uint64, error) {
	if prefix != "tenants/" && !strings.HasPrefix(prefix, "tenants/") {
		return 0, fmt.Errorf("reset prefix must remain under tenants/")
	}
	if path.Clean(prefix) == "." || strings.HasPrefix(prefix, "/") ||
		!strings.HasSuffix(prefix, "/") || hasTraversalSegment(prefix) {
		return 0, fmt.Errorf("reset prefix must be a normalized directory prefix")
	}
	var deleted uint64
	continuation := ""
	for {
		keys, next, err := store.listPrefix(ctx, prefix, continuation)
		if err != nil {
			return deleted, err
		}
		for _, key := range keys {
			if !strings.HasPrefix(key, prefix) {
				return deleted, fmt.Errorf("object listing escaped reset prefix: %q", key)
			}
			if store.iamClient != nil {
				err = store.iamClient.delete(ctx, store.bucket, key)
			} else {
				_, err = store.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(store.bucket), Key: aws.String(key),
				})
			}
			if err != nil {
				return deleted, fmt.Errorf("delete reset object %q: %w", key, err)
			}
			deleted++
		}
		if next == "" {
			return deleted, nil
		}
		continuation = next
	}
}

func (store *Store) listPrefix(ctx context.Context, prefix, continuation string) ([]string, string, error) {
	if store.iamClient != nil {
		return store.iamClient.list(ctx, store.bucket, prefix, continuation)
	}
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(store.bucket), Prefix: aws.String(prefix),
	}
	if continuation != "" {
		input.ContinuationToken = aws.String(continuation)
	}
	result, err := store.s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, "", fmt.Errorf("list S3 prefix: %w", err)
	}
	keys := make([]string, 0, len(result.Contents))
	for _, object := range result.Contents {
		if object.Key != nil {
			keys = append(keys, *object.Key)
		}
	}
	next := ""
	if result.IsTruncated != nil && *result.IsTruncated {
		if result.NextContinuationToken == nil || *result.NextContinuationToken == "" {
			return nil, "", fmt.Errorf("list S3 prefix: truncated response omitted continuation token")
		}
		next = *result.NextContinuationToken
	}
	return keys, next, nil
}

func (client *iamObjectClient) put(ctx context.Context, bucket, key string, data []byte) error {
	response, err := client.do(ctx, http.MethodPut, bucket, key, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return objectResponseError(response)
	}
	return nil
}

func (client *iamObjectClient) open(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	response, err := client.do(ctx, http.MethodGet, bucket, key, nil, 0)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		return nil, objectResponseError(response)
	}
	return response.Body, nil
}

func (client *iamObjectClient) delete(ctx context.Context, bucket, key string) error {
	response, err := client.do(ctx, http.MethodDelete, bucket, key, nil, 0)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusOK {
		return objectResponseError(response)
	}
	return nil
}

func (client *iamObjectClient) presignObject(
	ctx context.Context,
	bucket, key, method string,
	headers map[string]string,
	lifetime time.Duration,
	expiresAt time.Time,
) (ports.ObjectCapability, error) {
	if client.presign == nil {
		return ports.ObjectCapability{}, fmt.Errorf("Object Storage presign API is not configured")
	}
	token, err := client.tokens.Token(ctx)
	if err != nil {
		return ports.ObjectCapability{}, fmt.Errorf("get Object Storage IAM token: %w", err)
	}
	ctx = grpcmetadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	response, err := client.presign.Create(ctx, &ycstorage.PresignURLsRequest{
		BucketName: bucket,
		Objects: []*ycstorage.PresignObjectRequest{{
			Expires: int64(lifetime / time.Second), Name: key, Method: method, Headers: headers,
		}},
	})
	if err != nil {
		return ports.ObjectCapability{}, fmt.Errorf("presign Object Storage object: %w", err)
	}
	if len(response.GetUrls()) != 1 || strings.TrimSpace(response.GetUrls()[0]) == "" {
		return ports.ObjectCapability{}, fmt.Errorf("Object Storage presign API returned an invalid URL count")
	}
	capabilityURL, err := url.Parse(response.GetUrls()[0])
	if err != nil || capabilityURL.Scheme != "https" || capabilityURL.Host == "" {
		return ports.ObjectCapability{}, fmt.Errorf("Object Storage presign API returned a non-HTTPS URL")
	}
	resultHeaders := make(map[string]string, len(headers))
	for key, value := range headers {
		resultHeaders[strings.ToLower(key)] = value
	}
	return ports.ObjectCapability{
		Method: method, URL: capabilityURL.String(), Headers: resultHeaders, ExpiresAt: expiresAt,
	}, nil
}

func (client *iamObjectClient) stat(
	ctx context.Context,
	bucket string,
	tenantID domain.TenantID,
	key string,
	maxObjectBytes int64,
) (ports.ObjectMetadata, error) {
	response, err := client.do(ctx, http.MethodHead, bucket, key, nil, 0)
	if err != nil {
		return ports.ObjectMetadata{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ports.ObjectMetadata{}, objectResponseError(response)
	}
	size, err := strconv.ParseInt(response.Header.Get("Content-Length"), 10, 64)
	if err != nil || size < 0 {
		return ports.ObjectMetadata{}, fmt.Errorf("Object Storage HEAD returned invalid Content-Length")
	}
	metadata, err := metadataFromHead(
		tenantID, key, size, response.Header.Get("x-amz-checksum-sha256"),
		response.Header.Get("Content-Type"), response.Header.Get("ETag"),
	)
	if err == nil {
		err = validateStoredObjectSize(metadata.Blob.Size, maxObjectBytes)
	}
	if err != nil || metadata.Blob.SHA256 != "" {
		return metadata, err
	}
	body, err := client.openIfMatch(ctx, bucket, key, metadata.ETag)
	if err != nil {
		return ports.ObjectMetadata{}, fmt.Errorf("conditionally get Object Storage object for SHA-256 verification: %w", err)
	}
	defer body.Close()
	digest, err := hashObjectSHA256(body, metadata.Blob.Size, maxObjectBytes)
	if err != nil {
		return ports.ObjectMetadata{}, err
	}
	metadata.Blob.SHA256 = digest
	return metadata, nil
}

func (client *iamObjectClient) openIfMatch(
	ctx context.Context,
	bucket, key, etag string,
) (io.ReadCloser, error) {
	response, err := client.doWithHeaders(
		ctx, http.MethodGet, bucket, key, nil, 0,
		map[string]string{"If-Match": quoteETag(etag)},
	)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		return nil, objectResponseError(response)
	}
	return response.Body, nil
}

func (client *iamObjectClient) copyIfAbsent(
	ctx context.Context,
	bucket, sourceKey, sourceETag, finalKey, mediaType, sha256Hex string,
) error {
	token, err := client.tokens.Token(ctx)
	if err != nil {
		return fmt.Errorf("get Object Storage IAM token: %w", err)
	}
	endpoint := *client.endpoint
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/" + bucket + "/" + finalKey
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("create Object Storage copy request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("x-amz-copy-source", url.PathEscape(bucket+"/"+sourceKey))
	request.Header.Set("x-amz-copy-source-if-match", quoteETag(sourceETag))
	request.Header.Set("If-None-Match", "*")
	request.Header.Set("Content-Type", mediaType)
	request.Header.Set("x-amz-metadata-directive", "REPLACE")
	request.Header.Set("x-amz-checksum-algorithm", "SHA256")
	request.Header.Set("x-amz-meta-sha256", sha256Hex)
	response, err := client.http.Do(request)
	if err != nil {
		return fmt.Errorf("Object Storage copy: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return objectResponseError(response)
	}
	return nil
}

func metadataFromHead(
	tenantID domain.TenantID,
	key string,
	size int64,
	checksumBase64, mediaType, etag string,
) (ports.ObjectMetadata, error) {
	digest := ""
	checksumBase64 = strings.TrimSpace(checksumBase64)
	if checksumBase64 != "" {
		decoded, err := base64.StdEncoding.Strict().DecodeString(checksumBase64)
		if err != nil || len(decoded) != sha256.Size {
			return ports.ObjectMetadata{}, fmt.Errorf("Object Storage HEAD returned an invalid SHA-256 checksum")
		}
		digest = hex.EncodeToString(decoded)
	}
	if size < 0 {
		return ports.ObjectMetadata{}, fmt.Errorf("Object Storage HEAD returned invalid object size")
	}
	etag = normalizeETag(etag)
	if etag == "" {
		return ports.ObjectMetadata{}, fmt.Errorf("Object Storage HEAD omitted ETag")
	}
	return ports.ObjectMetadata{
		Blob:      domain.BlobRef{TenantID: tenantID, Key: key, Size: size, SHA256: digest},
		MediaType: mediaType,
		ETag:      etag,
	}, nil
}

func hashObjectSHA256(body io.Reader, expectedSize, maxObjectBytes int64) (string, error) {
	if err := validateStoredObjectSize(expectedSize, maxObjectBytes); err != nil {
		return "", err
	}
	if maxObjectBytes <= 0 {
		maxObjectBytes = defaultMaxObjectBytes
	}
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(body, maxObjectBytes+1))
	if err != nil {
		return "", fmt.Errorf("read stored object for SHA-256 verification: %w", err)
	}
	if written > maxObjectBytes {
		return "", fmt.Errorf("stored object exceeds %d bytes during SHA-256 verification", maxObjectBytes)
	}
	if written != expectedSize {
		return "", fmt.Errorf(
			"stored object changed during SHA-256 verification: read %d bytes, expected %d",
			written, expectedSize,
		)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func validateStoredObjectSize(size, maxObjectBytes int64) error {
	if maxObjectBytes <= 0 {
		maxObjectBytes = defaultMaxObjectBytes
	}
	if size < 0 || size > maxObjectBytes {
		return fmt.Errorf("stored object size must be between 0 and %d bytes", maxObjectBytes)
	}
	return nil
}

func normalizeETag(etag string) string {
	return strings.Trim(strings.TrimSpace(etag), "\"")
}

func quoteETag(etag string) string {
	return "\"" + normalizeETag(etag) + "\""
}

func (client *iamObjectClient) list(
	ctx context.Context,
	bucket, prefix, continuation string,
) ([]string, string, error) {
	token, err := client.tokens.Token(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("get Object Storage IAM token: %w", err)
	}
	endpoint := *client.endpoint
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/" + bucket
	query := endpoint.Query()
	query.Set("list-type", "2")
	query.Set("prefix", prefix)
	if continuation != "" {
		query.Set("continuation-token", continuation)
	}
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, "", fmt.Errorf("create Object Storage list request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.http.Do(request)
	if err != nil {
		return nil, "", fmt.Errorf("Object Storage list: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, "", objectResponseError(response)
	}
	var result struct {
		Contents []struct {
			Key string `xml:"Key"`
		} `xml:"Contents"`
		IsTruncated           bool   `xml:"IsTruncated"`
		NextContinuationToken string `xml:"NextContinuationToken"`
	}
	if err := xml.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&result); err != nil {
		return nil, "", fmt.Errorf("decode Object Storage listing: %w", err)
	}
	keys := make([]string, 0, len(result.Contents))
	for _, object := range result.Contents {
		keys = append(keys, object.Key)
	}
	next := ""
	if result.IsTruncated {
		next = result.NextContinuationToken
		if next == "" {
			return nil, "", fmt.Errorf("truncated Object Storage listing omitted continuation token")
		}
	}
	return keys, next, nil
}

func (client *iamObjectClient) do(
	ctx context.Context,
	method, bucket, key string,
	body io.Reader,
	contentLength int64,
) (*http.Response, error) {
	return client.doWithHeaders(ctx, method, bucket, key, body, contentLength, nil)
}

func (client *iamObjectClient) doWithHeaders(
	ctx context.Context,
	method, bucket, key string,
	body io.Reader,
	contentLength int64,
	headers map[string]string,
) (*http.Response, error) {
	token, err := client.tokens.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("get Object Storage IAM token: %w", err)
	}
	endpoint := *client.endpoint
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/" + bucket + "/" + key
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return nil, fmt.Errorf("create Object Storage request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	if method == http.MethodHead {
		request.Header.Set("x-amz-checksum-mode", "ENABLED")
	}
	request.ContentLength = contentLength
	response, err := client.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Object Storage %s: %w", method, err)
	}
	return response, nil
}

func objectResponseError(response *http.Response) error {
	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return fmt.Errorf("Object Storage returned status %d (read error body: %v)", response.StatusCode, err)
	}
	return fmt.Errorf(
		"Object Storage returned status %d: %s",
		response.StatusCode, strings.TrimSpace(string(body)),
	)
}

func tenantObjectKey(tenantID domain.TenantID, key string) (string, error) {
	if err := tenantID.Validate(); err != nil {
		return "", err
	}
	if key == "" || key == "." || strings.HasPrefix(key, "/") || path.Clean(key) != key ||
		hasTraversalSegment(key) {
		return "", domain.ValidationError{
			Field:  "blob.key",
			Reason: "must be a normalized relative object key",
		}
	}

	prefix := domain.TenantObjectPrefix(tenantID)
	if strings.HasPrefix(key, "tenants/") && !strings.HasPrefix(key, prefix) {
		return "", domain.TenantMismatchError{Expected: tenantID, Actual: tenantFromKey(key)}
	}
	if !strings.HasPrefix(key, prefix) {
		key = prefix + key
	}
	return key, nil
}

func hasTraversalSegment(key string) bool {
	for _, segment := range strings.Split(key, "/") {
		if segment == "." || segment == ".." {
			return true
		}
	}
	return false
}

func authorizeRef(tenantID domain.TenantID, ref domain.BlobRef) error {
	if err := domain.EnsureSameTenant(tenantID, ref.TenantID); err != nil {
		return err
	}
	return ref.Validate()
}

func tenantFromKey(key string) domain.TenantID {
	remainder := strings.TrimPrefix(key, "tenants/")
	tenant, _, _ := strings.Cut(remainder, "/")
	return domain.TenantID(tenant)
}

var _ ports.BlobStore = (*Store)(nil)
