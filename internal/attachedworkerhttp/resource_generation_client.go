package attachedworkerhttp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// ResourceGenerationClientV1 is one connection's bounded metadata transport,
// not an ACK owner or a source of publication authority. The original Session
// must derive requests from its installed baseline and original local store.
// Every method performs exactly one HTTP call; it never retries or reads back.
type ResourceGenerationClientV1 struct {
	origin string
	http   http.Client
	mu     sync.Mutex
	bearer []byte
	life   context.Context
	cancel context.CancelFunc
	closed bool
}

func NewResourceGenerationClientV1(origin string, client *http.Client, bearer []byte) (*ResourceGenerationClientV1, error) {
	parsed, err := url.Parse(origin)
	if err != nil || len(origin) > 4096 || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.Opaque != "" ||
		parsed.Path != "" && parsed.Path != "/" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.ForceQuery || strings.Contains(origin, "#") ||
		client == nil || client.Transport == nil || len(bearer) == 0 || len(bearer) > attachedworkertransport.ConnectionBearerMaxBytes {
		return nil, ErrInvalidRequest
	}
	owned := bytes.Clone(bearer)
	if _, err := attachedworkertransport.ParseConnectionBearer(owned); err != nil {
		clear(owned)
		return nil, ErrInvalidRequest
	}
	// Keep the explicitly supplied original transport/trust configuration. Do
	// not discover a global DefaultTransport, ambient proxy or cookie jar.
	configured := *client
	configured.Jar = nil
	configured.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if configured.Timeout <= 0 || configured.Timeout > ports.AttachedResourceGenerationOperationTimeoutV1 {
		configured.Timeout = ports.AttachedResourceGenerationOperationTimeoutV1
	}
	parsed.Path = ""
	life, cancel := context.WithCancel(context.Background())
	return &ResourceGenerationClientV1{origin: parsed.String(), http: configured, bearer: owned, life: life, cancel: cancel}, nil
}

func (*ResourceGenerationClientV1) String() string {
	return "ResourceGenerationClientV1{private:[redacted]}"
}
func (client *ResourceGenerationClientV1) GoString() string { return client.String() }

func (client *ResourceGenerationClientV1) Publish(ctx context.Context, publication ports.AttachedResourceGenerationPublicationV1) (ports.AttachedResourceGenerationResultV1, error) {
	zero := ports.AttachedResourceGenerationResultV1{}
	auth, target := publication.Authorization, publication.Target
	if target.Validate() != nil || target.OwnerUserID != auth.OwnerUserID || target.WorkerID != auth.WorkerID ||
		target.EnrollmentGeneration != auth.EnrollmentGeneration || target.BaselineRevision != auth.BaselineRevision {
		return zero, &ExchangeError{Kind: ErrorProtocol}
	}
	return client.post(ctx, auth, ResourceGenerationPublishPathV1,
		ResourceGenerationPublishRequestV1{Version: 1, Authorization: auth, ExpectedCredentialGeneration: publication.ExpectedCredentialGeneration, Target: target}, &target)
}

func (client *ResourceGenerationClientV1) Read(ctx context.Context, authorization ports.AttachedResourceGenerationAuthorizationV1) (ports.AttachedResourceGenerationResultV1, error) {
	return client.post(ctx, authorization, ResourceGenerationReadPathV1, ResourceGenerationReadRequestV1{Version: 1, Authorization: authorization}, nil)
}

func (client *ResourceGenerationClientV1) post(ctx context.Context, auth ports.AttachedResourceGenerationAuthorizationV1, path string, payload any,
	target *domain.AttachedResourceGenerationHeadV1,
) (ports.AttachedResourceGenerationResultV1, error) {
	zero := ports.AttachedResourceGenerationResultV1{}
	if client == nil || ctx == nil || ctx.Err() != nil {
		return zero, &ExchangeError{Kind: ErrorUnavailable}
	}
	// This cap never renews an earlier caller deadline. A publish/readback pair
	// must be passed the SAME original context by its Session owner.
	call, cancel := context.WithTimeout(ctx, ports.AttachedResourceGenerationOperationTimeoutV1)
	defer cancel()
	client.mu.Lock()
	if client.closed || client.cancel == nil || client.life == nil {
		client.mu.Unlock()
		return zero, &ExchangeError{Kind: ErrorUnavailable}
	}
	bearer, life := bytes.Clone(client.bearer), client.life
	client.mu.Unlock()
	defer clear(bearer)
	stop := context.AfterFunc(life, cancel)
	defer stop()
	if life.Err() != nil || call.Err() != nil {
		return zero, &ExchangeError{Kind: ErrorUnavailable}
	}
	if attachedworkertransport.ValidateResourceGenerationBearerV1(bearer, auth) != nil {
		return zero, &ExchangeError{Kind: ErrorUnauthorized}
	}
	encoded, err := json.Marshal(payload)
	if err != nil || len(encoded) > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
		clear(encoded)
		return zero, &ExchangeError{Kind: ErrorProtocol}
	}
	defer clear(encoded)
	request, err := http.NewRequestWithContext(call, http.MethodPost, client.origin+path, bytes.NewReader(encoded))
	if err != nil {
		return zero, &ExchangeError{Kind: ErrorProtocol}
	}
	// NewRequest recognizes bytes.Reader as replayable. Deliberately remove
	// that facility; neither redirects nor transport-level replay own this CAS.
	request.GetBody = nil
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+string(bearer))
	defer request.Header.Del("Authorization")
	response, err := client.http.Do(request)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return zero, &ExchangeError{Kind: ErrorUnavailable}
	}
	defer response.Body.Close()
	if call.Err() != nil || life.Err() != nil {
		return zero, &ExchangeError{Kind: ErrorUnavailable}
	}
	if response.StatusCode != http.StatusOK {
		switch response.StatusCode {
		case http.StatusNotFound:
			if target == nil {
				return ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionNotFound}, nil
			}
		case http.StatusUnauthorized, http.StatusForbidden:
			return zero, &ExchangeError{Kind: ErrorUnauthorized}
		case http.StatusConflict:
			return zero, &ExchangeError{Kind: ErrorConflict}
		case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return zero, &ExchangeError{Kind: ErrorUnavailable}
		}
		return zero, &ExchangeError{Kind: ErrorProtocol}
	}
	if !resourceGenerationHeadersBounded(response.Header) || !exactJSONMediaType(response.Header.Values("Content-Type")) ||
		response.ContentLength > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
		return zero, &ExchangeError{Kind: ErrorProtocol}
	}
	// The original HTTP transport must obey request cancellation while reading;
	// context alone cannot interrupt a malicious, non-cooperative Body reader.
	body, err := io.ReadAll(io.LimitReader(response.Body, domain.AttachedResourceGenerationMetadataMaxBytesV1+1))
	defer clear(body)
	if call.Err() != nil || life.Err() != nil {
		return zero, &ExchangeError{Kind: ErrorUnavailable}
	}
	if err != nil {
		return zero, &ExchangeError{Kind: ErrorUnavailable}
	}
	var result ResourceGenerationResponseV1
	if len(body) > domain.AttachedResourceGenerationMetadataMaxBytesV1 || decodeResourceGenerationV1(body, &result) != nil || result.Version != 1 ||
		result.Head.Validate() != nil || result.Head.OwnerUserID != auth.OwnerUserID || result.Head.WorkerID != auth.WorkerID ||
		result.Head.EnrollmentGeneration != auth.EnrollmentGeneration || result.Head.BaselineRevision != auth.BaselineRevision ||
		result.Status != ports.AttachedWorkerExecutionApplied && (target == nil || result.Status != ports.AttachedWorkerExecutionReplayed) || target != nil && result.Head != *target {
		return zero, &ExchangeError{Kind: ErrorProtocol}
	}
	return ports.AttachedResourceGenerationResultV1{Status: result.Status, Head: result.Head}, nil
}

// Close fences future calls, clears this client's bearer and cancels only its
// own in-flight contexts. It does not close a caller-shared transport or wait
// for a non-cooperative HTTP peer. The original Session retains cleanup owner.
func (client *ResourceGenerationClientV1) Close() error {
	if client == nil {
		return nil
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.closed = true
	clear(client.bearer)
	client.bearer = nil
	if client.cancel != nil {
		client.cancel()
	}
	return nil
}
