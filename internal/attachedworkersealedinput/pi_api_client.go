package attachedworkersealedinput

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

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

// piHTTPClient keeps explicit caller failure/header/TLS bounds, never installs
// a phase timeout default. Its fresh private H1-only nonpooled transport cannot
// take Go1.26's HTTP2 no-cached-connection or reused-connection replay paths.
// Request.GetBody is separately removed at the actual request boundary.
func piHTTPClient(client *http.Client, hostname string) (*http.Client, *http.Transport, error) {
	if client == nil || client.Jar != nil || client.Timeout <= 0 || client.Timeout > time.Minute {
		return nil, nil, ErrInvalid
	}
	t, ok := client.Transport.(*http.Transport)
	if !ok || t == nil || t.Proxy != nil || t.DialContext == nil || t.DialTLS != nil || t.DialTLSContext != nil ||
		t.TLSClientConfig == nil || t.TLSClientConfig.InsecureSkipVerify || t.TLSClientConfig.MinVersion < tls.VersionTLS12 ||
		t.TLSClientConfig.ServerName != "" && t.TLSClientConfig.ServerName != hostname ||
		t.TLSClientConfig.GetClientCertificate != nil || t.TLSClientConfig.VerifyPeerCertificate != nil || t.TLSClientConfig.VerifyConnection != nil ||
		t.TLSClientConfig.GetCertificate != nil || t.TLSClientConfig.GetConfigForClient != nil || len(t.TLSClientConfig.Certificates) != 0 ||
		t.TLSClientConfig.KeyLogWriter != nil || t.TLSClientConfig.Time != nil || t.TLSClientConfig.Rand != nil ||
		t.TLSClientConfig.EncryptedClientHelloRejectionVerify != nil || t.TLSClientConfig.GetEncryptedClientHelloKeys != nil ||
		len(t.TLSClientConfig.EncryptedClientHelloConfigList) != 0 || len(t.TLSClientConfig.EncryptedClientHelloKeys) != 0 ||
		t.TLSClientConfig.MaxVersion != 0 && t.TLSClientConfig.MaxVersion < t.TLSClientConfig.MinVersion ||
		t.TLSHandshakeTimeout <= 0 || t.TLSHandshakeTimeout > client.Timeout ||
		t.ResponseHeaderTimeout <= 0 || t.ResponseHeaderTimeout > client.Timeout || t.MaxResponseHeaderBytes <= 0 ||
		!t.DisableCompression || len(t.TLSNextProto) != 0 || t.GetProxyConnectHeader != nil || len(t.ProxyConnectHeader) != 0 {
		return nil, nil, ErrInvalid
	}
	private := t.Clone()
	if private.TLSClientConfig.RootCAs != nil {
		private.TLSClientConfig.RootCAs = private.TLSClientConfig.RootCAs.Clone()
	}
	private.TLSClientConfig.CipherSuites = append([]uint16(nil), private.TLSClientConfig.CipherSuites...)
	private.TLSClientConfig.CurvePreferences = append([]tls.CurveID(nil), private.TLSClientConfig.CurvePreferences...)
	// No inherited session cache/ticket sharing or alternative TLS credential
	// discovery; trust roots are copied like the original exchange factory.
	private.TLSClientConfig.ClientSessionCache = nil
	private.TLSClientConfig.SessionTicketsDisabled = true
	private.DisableKeepAlives = true
	private.DisableCompression = true
	private.ForceAttemptHTTP2 = false
	private.Protocols = &http.Protocols{}
	private.Protocols.SetHTTP1(true)
	private.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	private.TLSClientConfig.NextProtos = []string{"http/1.1"}
	copyClient := *client
	copyClient.Transport = private
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copyClient, private, nil
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
