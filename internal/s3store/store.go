// Package s3store implements the tenant-scoped blob port over S3-compatible
// object storage. Local development uses MinIO; cloud deployments use the same
// adapter with Yandex Object Storage endpoints and workload credentials.
package s3store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	metadata "github.com/ydb-platform/ydb-go-yc-metadata"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const defaultMaxObjectBytes int64 = 64 << 20

type Config struct {
	Endpoint               string
	Region                 string
	Bucket                 string
	AccessKeyID            string
	SecretAccessKey        string
	ForcePathStyle         bool
	IAMMetadataCredentials bool
	MaxObjectBytes         int64
}

type Store struct {
	s3Client       *s3.Client
	iamClient      *iamObjectClient
	bucket         string
	maxObjectBytes int64
}

type tokenProvider interface {
	Token(context.Context) (string, error)
}

type iamObjectClient struct {
	endpoint *url.URL
	tokens   tokenProvider
	http     *http.Client
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
	if config.IAMMetadataCredentials && config.AccessKeyID != "" {
		return nil, fmt.Errorf("S3 IAM metadata and static credentials are mutually exclusive")
	}
	if config.MaxObjectBytes <= 0 {
		config.MaxObjectBytes = defaultMaxObjectBytes
	}
	store := &Store{bucket: config.Bucket, maxObjectBytes: config.MaxObjectBytes}
	if config.IAMMetadataCredentials {
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
		store.iamClient = &iamObjectClient{
			endpoint: endpoint,
			tokens:   metadata.NewInstanceServiceAccount(),
			http:     &http.Client{Timeout: 30 * time.Second},
		}
		return store, nil
	}

	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(config.Region),
	}
	if config.AccessKeyID != "" {
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(config.AccessKeyID, config.SecretAccessKey, ""),
		))
	}
	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load S3 configuration: %w", err)
	}

	store.s3Client = s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.UsePathStyle = config.ForcePathStyle
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		options.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if config.Endpoint != "" {
			options.BaseEndpoint = aws.String(config.Endpoint)
		}
	})
	return store, nil
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

func (client *iamObjectClient) do(
	ctx context.Context,
	method, bucket, key string,
	body io.Reader,
	contentLength int64,
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
