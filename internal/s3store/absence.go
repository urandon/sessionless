package s3store

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var _ ports.BlobAbsenceVerifierV1 = (*Store)(nil)

// RequireBlobAbsent observes exact-object absence only. It grants no deletion
// authority and cannot resolve a late, ambiguously failed PUT. No listing,
// prefix deletion, GET-error inference or automatic polling is performed.
func (store *Store) RequireBlobAbsent(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if store == nil || ctx == nil || ctx.Err() != nil {
		return fmt.Errorf("blob absence unavailable")
	}
	if _, finite := ctx.Deadline(); !finite {
		return fmt.Errorf("blob absence requires a finite deadline")
	}
	if err := authorizeRef(tenant, ref); err != nil {
		return err
	}
	if store.iamClient != nil {
		if store.iamClient.http == nil {
			return fmt.Errorf("blob absence transport unavailable")
		}
		// Do not mutate the shared client or follow a redirect to a different
		// resource whose 404 says nothing about this exact configured object.
		client := *store.iamClient
		transport := *client.http
		transport.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		client.http = &transport
		response, err := client.do(ctx, http.MethodHead, store.bucket, ref.Key, nil, 0)
		if err != nil {
			return fmt.Errorf("check Object Storage absence: %w", err)
		}
		if response == nil || response.Body == nil {
			return fmt.Errorf("blob absence response unavailable")
		}
		closeErr := response.Body.Close()
		if closeErr != nil || ctx.Err() != nil || response.StatusCode != http.StatusNotFound {
			return fmt.Errorf("blob absence not confirmed")
		}
		return nil
	}
	if store.s3Client == nil {
		return fmt.Errorf("blob absence transport unavailable")
	}
	client, ok := store.s3Client.(*s3.Client)
	if !ok {
		return fmt.Errorf("blob absence requires its original SDK transport")
	}
	transport, finish, err := exactAbsenceHTTPClient(client.Options().HTTPClient)
	if err != nil {
		return err
	}
	defer finish()
	_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(store.bucket), Key: aws.String(ref.Key)}, func(o *s3.Options) { o.RetryMaxAttempts = 1; o.HTTPClient = transport })
	var response *smithyhttp.ResponseError
	var api smithy.APIError
	if ctx.Err() != nil || !errors.As(err, &response) || response == nil || response.Response == nil || response.Response.Response == nil || response.HTTPStatusCode() != http.StatusNotFound ||
		!errors.As(err, &api) || (api.ErrorCode() != "NotFound" && api.ErrorCode() != "NoSuchKey") {
		return fmt.Errorf("blob absence not confirmed")
	}
	return nil
}

// The SDK's standard BuildableClient follows 307/308. This one read must never
// follow any redirect: another key/host's 404 is not exact-object absence. Clone
// only the original supported transport; an opaque custom client is explicitly
// unsupported instead of silently weakening this storage fact.
func exactAbsenceHTTPClient(original s3.HTTPClient) (*http.Client, func(), error) {
	if wrapped, ok := original.(noReplayPutHTTPClient); ok {
		original = wrapped.delegate
	}
	deny := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	switch c := original.(type) {
	case *awshttp.BuildableClient:
		tr := c.GetTransport()
		tr.DisableKeepAlives = true
		return &http.Client{Transport: tr, Timeout: c.GetTimeout(), CheckRedirect: deny}, tr.CloseIdleConnections, nil
	case *http.Client:
		clone := *c
		clone.CheckRedirect = deny
		return &clone, func() {}, nil
	default:
		return nil, nil, fmt.Errorf("blob absence requires a redirect-controlled original transport")
	}
}
