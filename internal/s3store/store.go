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
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const defaultMaxObjectBytes int64 = 64 << 20

type Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	ForcePathStyle  bool
	MaxObjectBytes  int64
}

type Store struct {
	client         *s3.Client
	bucket         string
	maxObjectBytes int64
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
	if config.MaxObjectBytes <= 0 {
		config.MaxObjectBytes = defaultMaxObjectBytes
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

	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.UsePathStyle = config.ForcePathStyle
		options.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		options.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		if config.Endpoint != "" {
			options.BaseEndpoint = aws.String(config.Endpoint)
		}
	})
	return &Store{
		client:         client,
		bucket:         config.Bucket,
		maxObjectBytes: config.MaxObjectBytes,
	}, nil
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

	_, err = store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(store.bucket),
		Key:           aws.String(objectKey),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
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
	result, err := store.client.GetObject(ctx, &s3.GetObjectInput{
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
	_, err := store.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(store.bucket),
		Key:    aws.String(ref.Key),
	})
	if err != nil {
		return fmt.Errorf("delete S3 object: %w", err)
	}
	return nil
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
