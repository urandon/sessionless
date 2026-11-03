package s3store

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// The bound covers SDK deserialization too, not just the application's reader.
type providerBoundedBody struct {
	io.ReadCloser
	remaining int64
}

func (b *providerBoundedBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		var one [1]byte
		n, err := b.ReadCloser.Read(one[:])
		if n > 0 {
			return 0, errProviderCredentialObject
		}
		return 0, err
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, err
}

type providerHTTPClient struct {
	client *http.Client
	bound  int64
}

func (c providerHTTPClient) Do(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.GetBody = nil
	response, err := c.client.Do(r)
	if err != nil {
		return nil, errProviderCredentialObject
	}
	if response == nil || response.Body == nil {
		return nil, errProviderCredentialObject
	}
	response.Body = &providerBoundedBody{ReadCloser: response.Body, remaining: c.bound}
	// The pinned SDK scalar decoder accepts misleading roots/duplicate Codes.
	// Authenticate exact absence with the same bounded parser used by IAM,
	// including EOF and clean Close, before the SDK may interpret a 404.
	if response.StatusCode == http.StatusNotFound {
		if !providerIAMMissing(r.Context(), response) {
			return nil, errProviderCredentialObject
		}
		const missing = `<Error><Code>NoSuchKey</Code></Error>`
		response.Body = io.NopCloser(strings.NewReader(missing))
		response.ContentLength = int64(len(missing))
		response.Header = response.Header.Clone()
		if response.Header == nil {
			response.Header = make(http.Header)
		}
		response.Header.Set("Content-Length", strconv.Itoa(len(missing)))
		response.Header.Del("Content-Encoding")
	}
	// The SDK's typed bool alone cannot retain duplicate/missing XML flags.
	// Validate only this closed list wire before its ordinary deserialization.
	if response.StatusCode == http.StatusOK && r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		raw, err := providerRead(response.Body, 64<<10)
		if err != nil {
			return nil, errProviderCredentialObject
		}
		if _, _, err := decodeProviderList(raw); err != nil {
			return nil, errProviderCredentialObject
		}
		response.Body = io.NopCloser(bytes.NewReader(raw))
	}
	return response, nil
}

func decodeProviderList(raw []byte) ([]string, bool, error) {
	var page struct {
		XMLName  xml.Name `xml:"ListBucketResult"`
		Contents []struct {
			Keys []string `xml:"Key"`
		} `xml:"Contents"`
		Flags []string `xml:"IsTruncated"`
	}
	decoder := xml.NewDecoder(bytes.NewReader(raw))
	if len(raw) > 64<<10 || decoder.Decode(&page) != nil || page.XMLName.Local != "ListBucketResult" || len(page.Flags) != 1 || page.Flags[0] != "true" && page.Flags[0] != "false" || len(page.Contents) > 65 {
		return nil, false, errProviderCredentialObject
	}
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, false, errProviderCredentialObject
		}
		text, ok := token.(xml.CharData)
		if !ok || strings.TrimSpace(string(text)) != "" {
			return nil, false, errProviderCredentialObject
		}
	}
	var keys []string
	for _, v := range page.Contents {
		if len(v.Keys) != 1 || len(v.Keys[0]) > 512 {
			return nil, false, errProviderCredentialObject
		}
		keys = append(keys, v.Keys[0])
	}
	return keys, page.Flags[0] == "true", nil
}

func (s *ProviderCredentialObjectStore) sdkOptions(bound int64) (func(*s3.Options), func(), error) {
	client, ok := s.objects.s3Client.(*s3.Client)
	if !ok {
		return nil, nil, errProviderCredentialObject
	}
	httpClient, finish, err := exactAbsenceHTTPClient(client.Options().HTTPClient)
	if err != nil {
		return nil, nil, errProviderCredentialObject
	}
	return func(o *s3.Options) {
		o.RetryMaxAttempts = 1
		o.HTTPClient = providerHTTPClient{client: httpClient, bound: bound}
	}, finish, nil
}

func (s *ProviderCredentialObjectStore) iamRequest(ctx context.Context, method, key string, query url.Values, body []byte) (*http.Response, error) {
	c := s.objects.iamClient
	if c == nil || c.http == nil || c.endpoint == nil || c.tokens == nil {
		return nil, errProviderCredentialObject
	}
	token, err := c.tokens.Token(ctx)
	if err != nil {
		return nil, errProviderCredentialObject
	}
	u := *c.endpoint
	u.Path = strings.TrimRight(u.Path, "/") + "/" + s.objects.bucket
	if key != "" {
		u.Path += "/" + key
	}
	u.RawPath = ""
	u.RawQuery = query.Encode()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	r, err := http.NewRequestWithContext(ctx, method, u.String(), reader)
	if err != nil {
		return nil, errProviderCredentialObject
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.GetBody = nil
	if method == http.MethodPut {
		r.Header.Set("If-None-Match", "*")
		r.Header.Set("Content-Type", "application/octet-stream")
		r.ContentLength = int64(len(body))
	}
	h := *c.http
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return (providerHTTPClient{client: &h, bound: providerEnvelopeMax}).Do(r)
}

func providerMissing(err error) bool {
	var response *smithyhttp.ResponseError
	var api smithy.APIError
	return errors.As(err, &response) && response.HTTPStatusCode() == http.StatusNotFound && errors.As(err, &api) && api.ErrorCode() == "NoSuchKey"
}

// Only the configured service's bounded, structurally valid NoSuchKey response
// proves exact-object absence. A proxy/bucket/authentication 404 does not.
func providerIAMMissing(ctx context.Context, response *http.Response) bool {
	if response == nil || response.StatusCode != http.StatusNotFound {
		return false
	}
	raw, err := providerRead(response.Body, 4096)
	if err != nil || ctx.Err() != nil {
		return false
	}
	var body struct {
		XMLName xml.Name `xml:"Error"`
		Codes   []string `xml:"Code"`
	}
	decoder := xml.NewDecoder(bytes.NewReader(raw))
	if decoder.Decode(&body) != nil || body.XMLName.Local != "Error" || len(body.Codes) != 1 || body.Codes[0] != "NoSuchKey" {
		return false
	}
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return true
		}
		if err != nil {
			return false
		}
		text, ok := token.(xml.CharData)
		if !ok || strings.TrimSpace(string(text)) != "" {
			return false
		}
	}
}

func providerRead(body io.ReadCloser, bound int) ([]byte, error) {
	if body == nil {
		return nil, errProviderCredentialObject
	}
	data, err := io.ReadAll(io.LimitReader(body, int64(bound)+1))
	closeErr := body.Close()
	if err != nil || closeErr != nil || len(data) > bound {
		return nil, errProviderCredentialObject
	}
	return data, nil
}

func (s *ProviderCredentialObjectStore) get(ctx context.Context, key string) ([]byte, bool, error) {
	if s.objects.iamClient != nil {
		r, err := s.iamRequest(ctx, http.MethodGet, key, nil, nil)
		if err != nil {
			return nil, false, errProviderCredentialObject
		}
		if r.StatusCode == http.StatusNotFound {
			if providerIAMMissing(ctx, r) {
				return nil, false, nil
			}
			return nil, false, errProviderCredentialObject
		}
		if r.StatusCode != http.StatusOK {
			_ = r.Body.Close()
			return nil, false, errProviderCredentialObject
		}
		data, err := providerRead(r.Body, providerEnvelopeMax)
		if ctx.Err() != nil {
			return nil, false, errProviderCredentialObject
		}
		return data, err == nil, err
	}
	options, finish, err := s.sdkOptions(providerEnvelopeMax)
	if err != nil {
		return nil, false, err
	}
	defer finish()
	r, err := s.objects.s3Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.objects.bucket), Key: aws.String(key)}, options)
	if err != nil {
		if ctx.Err() == nil && providerMissing(err) {
			return nil, false, nil
		}
		return nil, false, errProviderCredentialObject
	}
	if r == nil {
		return nil, false, errProviderCredentialObject
	}
	data, err := providerRead(r.Body, providerEnvelopeMax)
	if ctx.Err() != nil {
		return nil, false, errProviderCredentialObject
	}
	return data, err == nil, err
}

func (s *ProviderCredentialObjectStore) put(ctx context.Context, key string, wire []byte) error {
	if s.objects.iamClient != nil {
		r, err := s.iamRequest(ctx, http.MethodPut, key, nil, wire)
		if err != nil {
			return errProviderCredentialObject
		}
		closeErr := r.Body.Close()
		if closeErr != nil || r.StatusCode != http.StatusOK || ctx.Err() != nil {
			return errProviderCredentialObject
		}
		return nil
	}
	options, finish, err := s.sdkOptions(providerEnvelopeMax)
	if err != nil {
		return err
	}
	defer finish()
	_, err = s.objects.s3Client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.objects.bucket), Key: aws.String(key), IfNoneMatch: aws.String("*"), Body: bytes.NewReader(wire), ContentLength: aws.Int64(int64(len(wire))), ContentType: aws.String("application/octet-stream")}, options)
	if err != nil || ctx.Err() != nil {
		return errProviderCredentialObject
	}
	return nil
}

func (s *ProviderCredentialObjectStore) delete(ctx context.Context, key string) error {
	if s.objects.iamClient != nil {
		r, err := s.iamRequest(ctx, http.MethodDelete, key, nil, nil)
		if err != nil {
			return errProviderCredentialObject
		}
		if r.StatusCode == http.StatusNotFound {
			if providerIAMMissing(ctx, r) {
				return nil
			}
			return errProviderCredentialObject
		}
		closeErr := r.Body.Close()
		if closeErr != nil || (r.StatusCode != http.StatusNoContent && r.StatusCode != http.StatusOK) || ctx.Err() != nil {
			return errProviderCredentialObject
		}
		return nil
	}
	options, finish, err := s.sdkOptions(providerEnvelopeMax)
	if err != nil {
		return err
	}
	defer finish()
	_, err = s.objects.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.objects.bucket), Key: aws.String(key)}, options)
	if err != nil || ctx.Err() != nil {
		return errProviderCredentialObject
	}
	return nil
}

func (s *ProviderCredentialObjectStore) list(ctx context.Context, prefix, after string, limit uint64) ([]string, bool, error) {
	if limit == 0 || limit > 65 || !strings.HasPrefix(prefix, providerObjectRoot) || after != "" && !strings.HasPrefix(after, prefix) {
		return nil, false, errProviderCredentialObject
	}
	var keys []string
	var more bool
	if s.objects.iamClient != nil {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}, "max-keys": {strconv.FormatUint(limit, 10)}}
		if after != "" {
			q.Set("start-after", after)
		}
		r, err := s.iamRequest(ctx, http.MethodGet, "", q, nil)
		if err != nil {
			return nil, false, errProviderCredentialObject
		}
		if r.StatusCode != http.StatusOK {
			_ = r.Body.Close()
			return nil, false, errProviderCredentialObject
		}
		raw, err := providerRead(r.Body, 64<<10)
		if err != nil {
			return nil, false, err
		}
		keys, more, err = decodeProviderList(raw)
		if err != nil {
			return nil, false, err
		}
	} else {
		options, finish, err := s.sdkOptions(64 << 10)
		if err != nil {
			return nil, false, err
		}
		defer finish()
		r, err := s.objects.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(s.objects.bucket), Prefix: aws.String(prefix), StartAfter: aws.String(after), MaxKeys: aws.Int32(int32(limit))}, options)
		if err != nil || r == nil || r.IsTruncated == nil {
			return nil, false, errProviderCredentialObject
		}
		for _, v := range r.Contents {
			if v.Key == nil {
				return nil, false, errProviderCredentialObject
			}
			keys = append(keys, *v.Key)
		}
		more = aws.ToBool(r.IsTruncated)
	}
	if ctx.Err() != nil || uint64(len(keys)) > limit || more && len(keys) == 0 {
		return nil, false, errProviderCredentialObject
	}
	last := after
	for _, key := range keys {
		if len(key) > 512 || !strings.HasPrefix(key, prefix) || key <= last {
			return nil, false, errProviderCredentialObject
		}
		last = key
	}
	return keys, more, nil
}
