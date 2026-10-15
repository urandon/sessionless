package attachedworkersealedinput

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const PiAPIUsePathV1 = "/attached-worker/v1/pi-api-use"

// PiAPIClient is a default-off, SAME-session metadata client, not a server
// authorizer or permission issuer. It does not select W/L/S/F, sample a clock,
// mint a permit, construct current source facts or register a serving driver.
type PiAPIClient struct {
	endpoint  string
	client    http.Client
	transport *http.Transport
	mu        sync.Mutex
	binding   attachedworkersession.ConnectionBindingV1
	bearer    []byte
	closed    bool
	life      context.Context
	cancel    context.CancelFunc
}

func piEndpoint(endpoint string) (*url.URL, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || strings.HasSuffix(u.Host, ":") || u.User != nil ||
		u.Opaque != "" || u.Path != PiAPIUsePathV1 || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.String() != endpoint {
		return nil, ErrInvalid
	}
	return u, nil
}

// Keep the existing phase/source transport entrypoint and its package error
// contract. All HTTPS policy lives in the shared factory, not a second copy.
func piHTTPClient(client *http.Client, hostname string) (*http.Client, *http.Transport, error) {
	private, transport, err := attachedworkertransport.NewPrivateControlHTTPClient(client, hostname)
	if err != nil {
		return nil, nil, ErrInvalid
	}
	return private, transport, nil
}

func validPiBinding(b attachedworkersession.ConnectionBindingV1) bool {
	return b.TenantID.Validate() == nil && b.OwnerUserID.Validate() == nil && b.WorkerID.Validate() == nil && b.ConnectionID.Validate() == nil &&
		b.EnrollmentGeneration > 0 && b.ConnectionGeneration > 0 && b.ProtocolVersion == attachedworkerprotocol.ProtocolVersionV1 &&
		b.CapabilityDigest.Validate() == nil && !b.AuthenticationExpires.IsZero()
}
func piScopeMatches(b attachedworkersession.ConnectionBindingV1, a ports.AttachedWorkerSealedInputAuthorization) bool {
	return a.TenantID == b.TenantID && a.OwnerUserID == b.OwnerUserID && a.WorkerID == b.WorkerID && a.ConnectionID == b.ConnectionID &&
		a.EnrollmentGeneration == b.EnrollmentGeneration && a.ConnectionGeneration == b.ConnectionGeneration && a.CapabilityDigest == b.CapabilityDigest
}

func NewPiAPIClient(endpoint string, client *http.Client, binding attachedworkersession.ConnectionBindingV1, bearer []byte) (*PiAPIClient, error) {
	parsed, err := piEndpoint(endpoint)
	if err != nil || !validPiBinding(binding) {
		return nil, ErrInvalid
	}
	// Original connection parser checks its 1024-byte/prefix bound BEFORE base64
	// or string allocation; no invented credential DTO/limit/schema. Immutable strings and
	// native HTTP buffers are NOT claimed zeroized by clearing owned bytes.
	if _, err := attachedworkertransport.ParseConnectionBearer(bearer); err != nil {
		return nil, ErrInvalid
	}
	private, transport, err := piHTTPClient(client, parsed.Hostname())
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	return &PiAPIClient{endpoint: endpoint, client: *private, transport: transport, binding: binding, bearer: bytes.Clone(bearer), life: life, cancel: cancel}, nil
}

// RequestPiAPIUse performs ONE application Do for any of the three fixed
// phases. It returns only structurally matching metadata. The caller retains
// its original context/deadline; only cancellation is added for session Close.
// Static equality/SAME HTTP completion does NOT prove a current serializable
// source snapshot or actual original-use cutoff and is never a send grant.
func (client *PiAPIClient) RequestPiAPIUse(ctx context.Context, request pisdkphasewire.Request) (pisdkphasewire.Reply, error) {
	if client == nil || ctx == nil || ctx.Err() != nil {
		return pisdkphasewire.Reply{}, ErrInvalid
	}
	data, err := pisdkphasewire.EncodeRequest(request)
	if err != nil {
		return pisdkphasewire.Reply{}, ErrInvalid
	}
	defer clear(data)
	client.mu.Lock()
	if client.closed || !piScopeMatches(client.binding, request.Authorization) {
		closed := client.closed
		client.mu.Unlock()
		if closed {
			return pisdkphasewire.Reply{}, ErrUnavailable
		}
		return pisdkphasewire.Reply{}, ErrInvalid
	}
	bearer := bytes.Clone(client.bearer)
	life := client.life
	client.mu.Unlock()
	defer clear(bearer)
	requestCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(life, cancel)
	defer func() { stop(); cancel() }()
	if life.Err() != nil {
		return pisdkphasewire.Reply{}, ErrUnavailable
	}
	httpRequest, err := http.NewRequestWithContext(requestCtx, http.MethodPost, client.endpoint, bytes.NewReader(data))
	if err != nil {
		return pisdkphasewire.Reply{}, ErrInvalid
	}
	httpRequest.GetBody = nil
	httpRequest.Close = true
	httpRequest.Header.Set("Authorization", "Bearer "+string(bearer))
	defer httpRequest.Header.Del("Authorization")
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Accept-Encoding", "identity")
	if requestCtx.Err() != nil {
		return pisdkphasewire.Reply{}, ErrUnavailable
	}
	response, err := client.client.Do(httpRequest)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return pisdkphasewire.Reply{}, ErrUnavailable
	}
	if response.Body == nil {
		return pisdkphasewire.Reply{}, ErrUnavailable
	}
	defer response.Body.Close()
	encodings := response.Header.Values("Content-Encoding")
	if response.StatusCode != http.StatusOK || response.Uncompressed || response.ProtoMajor != 1 ||
		len(response.Header.Values("Content-Type")) != 1 || response.Header.Get("Content-Type") != "application/json" ||
		len(response.Header.Values("Cache-Control")) != 1 || response.Header.Get("Cache-Control") != "no-store" ||
		len(encodings) > 1 || len(encodings) == 1 && encodings[0] != "identity" ||
		response.ContentLength == 0 || response.ContentLength > pisdkphasewire.ReplyMaxBytes || response.ContentLength < -1 {
		return pisdkphasewire.Reply{}, ErrUnavailable
	}
	// Fixed finite arena includes exactly one overflow byte. ReadFull waits for
	// bounded EOF on a fitting body; no ReadAll/growth/drain or error-body logs.
	var arena [pisdkphasewire.ReplyMaxBytes + 1]byte
	defer clear(arena[:])
	n, readErr := io.ReadFull(response.Body, arena[:])
	if n == 0 || n > pisdkphasewire.ReplyMaxBytes || readErr != io.ErrUnexpectedEOF || requestCtx.Err() != nil ||
		response.ContentLength >= 0 && response.ContentLength != int64(n) {
		return pisdkphasewire.Reply{}, ErrUnavailable
	}
	reply, err := pisdkphasewire.DecodeReply(arena[:n])
	if err != nil {
		return pisdkphasewire.Reply{}, ErrUnavailable
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed || requestCtx.Err() != nil {
		return pisdkphasewire.Reply{}, ErrUnavailable
	}
	if reply.Status == ports.AttachedWorkerExecutionDenied {
		return pisdkphasewire.Reply{}, ErrUnauthorized
	}
	if pisdkphasewire.CompareReply(request, reply) != nil {
		return pisdkphasewire.Reply{}, ErrUnavailable
	}
	return reply, nil
}

// Close clears this owner's bearer/binding and cancels pending headers/body
// before any delegate/session close. No native buffer/GC-zeroization claim.
func (client *PiAPIClient) Close() error {
	if client == nil {
		return nil
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return nil
	}
	client.closed = true
	clear(client.bearer)
	client.bearer = nil
	client.binding = attachedworkersession.ConnectionBindingV1{}
	client.cancel()
	client.transport.CloseIdleConnections()
	return nil
}
