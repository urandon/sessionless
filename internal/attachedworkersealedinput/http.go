package attachedworkersealedinput

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
)

const PathV1 = "/attached-worker/v1/sealed-input"

const maxRequestBytes = 16 << 10
const maxResponseBytes = 4 << 20

// Handler is intentionally a single-purpose content endpoint. All scope and
// durable-head authority comes from Service, never from HTTP routing alone.
func Handler(service *Service) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if service == nil || request.Method != http.MethodPost || request.URL.Path != PathV1 ||
			request.URL.RawQuery != "" || request.Header.Get("Content-Type") != "application/json" ||
			len(request.Header.Values("Authorization")) != 1 {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		authorization := request.Header.Get("Authorization")
		if !strings.HasPrefix(authorization, "Bearer ") || len(authorization) <= len("Bearer ") {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		var materialization attachedworkerdaemontransport.MaterializationRequestV1
		if decoder.Decode(&materialization) != nil || decoder.Decode(new(any)) != io.EOF {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		input, err := service.Load(request.Context(), []byte(strings.TrimPrefix(authorization, "Bearer ")), materialization)
		if err != nil {
			status := http.StatusServiceUnavailable
			if errors.Is(err, ErrUnauthorized) {
				status = http.StatusUnauthorized
			} else if errors.Is(err, ErrInvalid) || errors.Is(err, ErrUnsupported) {
				status = http.StatusUnprocessableEntity
			}
			http.Error(writer, http.StatusText(status), status)
			return
		}
		defer clearResult(&input)
		payload, err := json.Marshal(input)
		if err != nil || len(payload) > maxResponseBytes {
			clearBytes(payload)
			http.Error(writer, "unavailable", http.StatusServiceUnavailable)
			return
		}
		defer clearBytes(payload)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(payload)
	})
}

// ClientSource is the local daemon's bounded, credential-free input port.
// A connection bearer authenticates the request; this source never receives
// provider credentials or host paths from the response.
type ClientSource struct {
	endpoint string
	client   http.Client
	mu       sync.Mutex
	bearer   []byte
	closed   bool
}

func NewClientSource(endpoint string, client *http.Client, bearer []byte) (*ClientSource, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != PathV1 || parsed.RawQuery != "" || parsed.Fragment != "" || len(bearer) == 0 {
		return nil, ErrInvalid
	}
	result := &ClientSource{endpoint: endpoint, bearer: append([]byte(nil), bearer...)}
	if client != nil {
		result.client = *client
	}
	if result.client.Timeout <= 0 || result.client.Timeout > time.Minute {
		result.client.Timeout = time.Minute
	}
	result.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return result, nil
}

func (source *ClientSource) Load(ctx context.Context, request attachedworkerdaemontransport.MaterializationRequestV1) (attachedworkerdaemontransport.SealedInputV1, error) {
	if source == nil || ctx == nil || ctx.Err() != nil {
		return attachedworkerdaemontransport.SealedInputV1{}, ErrInvalid
	}
	source.mu.Lock()
	if source.closed {
		source.mu.Unlock()
		return attachedworkerdaemontransport.SealedInputV1{}, ErrUnavailable
	}
	bearer := append([]byte(nil), source.bearer...)
	source.mu.Unlock()
	defer clearBytes(bearer)
	data, err := json.Marshal(request)
	if err != nil || len(data) > maxRequestBytes {
		return attachedworkerdaemontransport.SealedInputV1{}, ErrInvalid
	}
	defer clearBytes(data)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, source.endpoint, bytes.NewReader(data))
	if err != nil {
		return attachedworkerdaemontransport.SealedInputV1{}, ErrInvalid
	}
	httpRequest.Header.Set("Authorization", "Bearer "+string(bearer))
	defer httpRequest.Header.Del("Authorization")
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	response, err := source.client.Do(httpRequest)
	if err != nil {
		return attachedworkerdaemontransport.SealedInputV1{}, ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/json" {
		return attachedworkerdaemontransport.SealedInputV1{}, ErrUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes {
		clearBytes(body)
		return attachedworkerdaemontransport.SealedInputV1{}, ErrUnavailable
	}
	defer clearBytes(body)
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var input attachedworkerdaemontransport.SealedInputV1
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || ctx.Err() != nil {
		clearResult(&input)
		return attachedworkerdaemontransport.SealedInputV1{}, ErrUnavailable
	}
	return input, nil
}

func (source *ClientSource) Close() error {
	if source == nil {
		return nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	clearBytes(source.bearer)
	source.bearer = nil
	source.closed = true
	return nil
}

var _ attachedworkerdaemontransport.SealedInputSource = (*ClientSource)(nil)
