// Package attachedworkerhttp provides the immediate, outbound-worker HTTP
// adapter for the attached-worker protocol. It opens no listener on a worker.
package attachedworkerhttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
)

const (
	ExchangePathV1        = "/attached-worker/v1/exchange"
	defaultRequestTimeout = 15 * time.Second
)

var (
	ErrUnauthorized   = errors.New("attached worker exchange is unauthorized")
	ErrConflict       = errors.New("attached worker exchange conflicts with authoritative state")
	ErrUnavailable    = errors.New("attached worker exchange is temporarily unavailable")
	ErrInvalidRequest = errors.New("attached worker exchange request is invalid")
)

type ErrorKind string

const (
	ErrorUnauthorized ErrorKind = "unauthorized"
	ErrorConflict     ErrorKind = "conflict"
	ErrorUnavailable  ErrorKind = "unavailable"
	ErrorProtocol     ErrorKind = "protocol"
)

// ExchangeError is intentionally sanitized: it never wraps an HTTP error,
// endpoint, response body, or bearer credential. Retryable asks a caller to
// apply its own bounded retry policy; untrusted Retry-After response headers
// are deliberately not represented here.
type ExchangeError struct {
	Kind      ErrorKind
	retryable bool
}

func (err *ExchangeError) Error() string {
	if err == nil || err.Kind == "" {
		return "attached worker exchange failed"
	}
	return "attached worker exchange " + string(err.Kind)
}

func (err *ExchangeError) Retryable() bool { return err != nil && err.retryable }

type ExchangeService interface {
	Exchange(context.Context, BearerToken, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error)
}

type Handler struct {
	service ExchangeService
}

func NewHandler(service ExchangeService) (*Handler, error) {
	if service == nil {
		return nil, errors.New("attached worker exchange service must not be nil")
	}
	return &Handler{service: service}, nil
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setSafeResponseHeaders(writer.Header())
	if request.Method != http.MethodPost || request.URL.Path != ExchangePathV1 {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if !immediateQuery(request.URL.RawQuery) {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if !exactJSONMediaType(request.Header.Values("Content-Type")) {
		writer.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	if !exactJSONMediaType(request.Header.Values("Accept")) {
		writer.WriteHeader(http.StatusNotAcceptable)
		return
	}
	token, err := authorizationToken(request.Header.Values("Authorization"))
	if err != nil {
		writer.Header().Set("WWW-Authenticate", `Bearer realm="attached-worker"`)
		writer.WriteHeader(http.StatusUnauthorized)
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, attachedworkerprotocol.MaxBatchBytes)
	encoded, err := io.ReadAll(request.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writer.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	batch, err := attachedworkerprotocol.DecodeBatchV1(encoded)
	if err != nil {
		writeProtocolStatus(writer, err)
		return
	}
	response, err := handler.service.Exchange(request.Context(), token, batch)
	if err != nil {
		writeServiceStatus(writer, err)
		return
	}
	if response == nil {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	encodedResponse, err := attachedworkerprotocol.EncodeBatchV1(*response)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encodedResponse)
}

func immediateQuery(rawQuery string) bool {
	if rawQuery == "" {
		return true
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return false
	}
	if len(values) != 1 {
		return false
	}
	waits, ok := values["wait_ms"]
	if !ok || len(waits) != 1 {
		return false
	}
	wait, err := strconv.ParseUint(waits[0], 10, 63)
	return err == nil && wait == 0
}

func authorizationToken(values []string) (BearerToken, error) {
	if len(values) != 1 || len(values[0]) <= len("Bearer ") || values[0][:len("Bearer ")] != "Bearer " {
		return BearerToken{}, ErrInvalidBearerToken
	}
	return ParseBearerToken(values[0][len("Bearer "):])
}

func exactJSONMediaType(values []string) bool {
	if len(values) != 1 {
		return false
	}
	mediaType, parameters, err := mime.ParseMediaType(values[0])
	return err == nil && mediaType == "application/json" && len(parameters) == 0
}

func setSafeResponseHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set("X-Content-Type-Options", "nosniff")
}

func writeProtocolStatus(writer http.ResponseWriter, err error) {
	var protocolErr *attachedworkerprotocol.ProtocolError
	if errors.As(err, &protocolErr) && protocolErr.Code == attachedworkerprotocol.ErrorFrameTooLarge {
		writer.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	writer.WriteHeader(http.StatusBadRequest)
}

func writeServiceStatus(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrUnauthorized):
		writer.Header().Set("WWW-Authenticate", `Bearer realm="attached-worker"`)
		writer.WriteHeader(http.StatusUnauthorized)
	case errors.Is(err, ErrConflict):
		writer.WriteHeader(http.StatusConflict)
	case errors.Is(err, ErrUnavailable):
		writer.Header().Set("Retry-After", "1")
		writer.WriteHeader(http.StatusServiceUnavailable)
	default:
		writer.WriteHeader(http.StatusInternalServerError)
	}
}

type ClientConfig struct {
	BaseURL        string
	Token          BearerToken
	HTTPClient     *http.Client
	RequestTimeout time.Duration
}

type Client struct {
	endpoint *url.URL
	token    BearerToken
	http     http.Client
	timeout  time.Duration
}

func NewClient(config ClientConfig) (*Client, error) {
	base, err := url.Parse(config.BaseURL)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" ||
		(base.Path != "" && base.Path != "/") || !config.Token.valid() {
		return nil, ErrInvalidRequest
	}
	base.Path = ExchangePathV1
	timeout := config.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	if timeout <= 0 || timeout > time.Minute {
		return nil, ErrInvalidRequest
	}
	httpClient := http.Client{}
	if config.HTTPClient != nil {
		httpClient = *config.HTTPClient
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{endpoint: base, token: config.Token, http: httpClient, timeout: timeout}, nil
}

func (client *Client) Exchange(ctx context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	if client == nil || client.endpoint == nil || !client.token.valid() {
		return nil, &ExchangeError{Kind: ErrorProtocol}
	}
	encoded, err := attachedworkerprotocol.EncodeBatchV1(batch)
	if err != nil {
		return nil, &ExchangeError{Kind: ErrorProtocol}
	}
	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, client.endpoint.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, &ExchangeError{Kind: ErrorProtocol}
	}
	request.Header.Set("Authorization", client.token.headerValue())
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := client.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if requestContext.Err() != nil {
			return nil, &ExchangeError{Kind: ErrorUnavailable, retryable: true}
		}
		return nil, &ExchangeError{Kind: ErrorUnavailable, retryable: true}
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		if body, readErr := io.ReadAll(io.LimitReader(response.Body, 1)); readErr != nil || len(body) != 0 {
			return nil, &ExchangeError{Kind: ErrorProtocol}
		}
		return nil, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, statusError(response.StatusCode)
	}
	if !exactJSONMediaType(response.Header.Values("Content-Type")) {
		return nil, &ExchangeError{Kind: ErrorProtocol}
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, attachedworkerprotocol.MaxBatchBytes+1))
	if readErr != nil || len(body) == 0 || len(body) > attachedworkerprotocol.MaxBatchBytes {
		return nil, &ExchangeError{Kind: ErrorProtocol}
	}
	decoded, decodeErr := attachedworkerprotocol.DecodeBatchV1(body)
	if decodeErr != nil {
		return nil, &ExchangeError{Kind: ErrorProtocol}
	}
	return &decoded, nil
}

func statusError(status int) error {
	// Retry-After is intentionally ignored. The header is controlled by the
	// remote endpoint and could bypass the poller's locally bounded full-jitter
	// policy; retryable responses therefore carry classification only.
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return &ExchangeError{Kind: ErrorUnauthorized}
	case http.StatusConflict:
		return &ExchangeError{Kind: ErrorConflict}
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return &ExchangeError{Kind: ErrorUnavailable, retryable: true}
	default:
		return &ExchangeError{Kind: ErrorProtocol}
	}
}
