package attachedworkerreceipt

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// InlineClientV2 is only a fixed-origin, one-exchange wire adapter. The caller
// owns provenance, durable burns, aggregate call budgets and recovery policy.
// The supplied transport must honor request cancellation, perform no internal
// retries, and bound response Read/Close by that context. Submit borrows an
// original bounded, nonblocking/context-cooperative reader; it neither owns nor
// closes that reader. No arbitrary RoundTripper/Reader can be made bounded by
// wrapping it in a leaking timeout goroutine.
type InlineClientV2 struct {
	origin         string
	http           http.Client
	mu             sync.Mutex
	bearer         []byte
	closed         bool
	active         map[*inlineClientCallV2]struct{}
	ownedTransport *http.Transport
}

type inlineClientCallV2 struct{ cancel context.CancelFunc }

func NewInlineClientV2(origin string, client *http.Client, bearer []byte) (*InlineClientV2, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.Path != "" || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || u.String() != origin || len(origin) > 1024 || !inlineClientBearerV2(bearer) {
		return nil, ErrInvalid
	}
	if port := u.Port(); port != "" {
		n, e := strconv.ParseUint(port, 10, 16)
		if e != nil || n == 0 {
			return nil, ErrInvalid
		}
	}
	result := &InlineClientV2{origin: origin, bearer: bytes.Clone(bearer), active: make(map[*inlineClientCallV2]struct{})}
	if client != nil {
		result.http = *client
	}
	if result.http.Transport == nil {
		// An ambient DefaultTransport enables HTTP2's pre-body retry loop and
		// a much larger response-header allocation. Use fresh HTTP1 connections:
		// pinned Go's reused-connection retry path is then unavailable. No proxy
		// environment or caller-owned transport/client is changed or inherited.
		protocols := new(http.Protocols)
		protocols.SetHTTP1(true)
		result.ownedTransport = &http.Transport{
			Protocols: protocols, DisableKeepAlives: true, DisableCompression: true,
			DialContext:         (&net.Dialer{Timeout: 15 * time.Second, KeepAlive: -1}).DialContext,
			TLSHandshakeTimeout: 15 * time.Second, ResponseHeaderTimeout: 15 * time.Second,
			MaxResponseHeaderBytes: maxInlineHeaderBytesV2, MaxConnsPerHost: 1,
		}
		result.http.Transport = result.ownedTransport
	}
	result.http.Jar = nil
	result.http.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return result, nil
}

func inlineClientBearerV2(b []byte) bool {
	if len(b) == 0 || len(b) > maxInlineHeaderBytesV2 {
		return false
	}
	for _, c := range b {
		if c < 0x21 || c > 0x7e || c == ',' {
			return false
		}
	}
	return true
}

// Close revokes future calls, cancels active exchanges, and clears the owned
// bearer. It closes idle connections only in its own default transport, never
// a caller-owned http.Client or transport.
func (c *InlineClientV2) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	clear(c.bearer)
	c.bearer = nil
	for call := range c.active {
		call.cancel()
	}
	if c.ownedTransport != nil {
		c.ownedTransport.CloseIdleConnections()
	}
	return nil
}

func inlineClientDeadlineV2(ctx context.Context, expiry time.Time, requireExpiry bool) (time.Time, error) {
	if ctx == nil || ctx.Err() != nil {
		return time.Time{}, ErrInvalid
	}
	now := time.Now()
	deadline, ok := ctx.Deadline()
	if !ok || !deadline.After(now) || deadline.Sub(now) > 15*time.Second {
		return time.Time{}, ErrInvalid
	}
	if requireExpiry {
		if expiry.IsZero() || !expiry.After(now) {
			return time.Time{}, ErrInvalid
		}
		if expiry.Before(deadline) {
			deadline = expiry
		}
	}
	return deadline, nil
}

func (c *InlineClientV2) begin(ctx context.Context, deadline time.Time) (context.Context, []byte, func(), error) {
	if c == nil {
		return nil, nil, nil, ErrUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.active == nil || len(c.bearer) == 0 {
		return nil, nil, nil, ErrUnavailable
	}
	bound, cancel := context.WithDeadline(ctx, deadline)
	call := &inlineClientCallV2{cancel: cancel}
	c.active[call] = struct{}{}
	bearer := bytes.Clone(c.bearer)
	return bound, bearer, func() {
		cancel()
		clear(bearer)
		c.mu.Lock()
		delete(c.active, call)
		c.mu.Unlock()
	}, nil
}

type inlineClientExpectedV2 struct {
	nonce        domain.IdempotencyKey
	fingerprint  string
	expiry       time.Time
	noCapability bool
	noPrepared   bool
}

func (c *InlineClientV2) Prepare(ctx context.Context, input attachedworkeroutput.AttachedWorkerInlinePrepareV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	deadline, err := inlineClientDeadlineV2(ctx, input.ExpiresAt, true)
	if err != nil || !inlineClientScopeV2(input.Version, input.Authorization, input.Nonce) || input.Candidate.Validate() != nil ||
		input.Observation.ValidateFor(domain.AttachedWorkerTerminalSucceeded, input.Observation.CredentialReleaseRequired) != nil ||
		!inlineClientStringsV2(string(input.Producer.SessionID), string(input.Producer.ReservationID), string(input.Producer.HarnessBindingDigest), input.Producer.SchemaDigest, input.Producer.FinalDigest, input.Producer.AnswerDigest) {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrInvalid
	}
	fingerprint, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(input)
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrInvalid
	}
	encoded, err := json.Marshal(input)
	defer clear(encoded)
	if err != nil || len(encoded) > maxInlinePrepareBytesV2 {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrInvalid
	}
	return c.exchange(ctx, deadline, PathInlinePrepareV2, encoded, nil, 0, nil, inlineClientExpectedV2{nonce: input.Nonce, fingerprint: fingerprint, expiry: input.ExpiresAt})
}

func (c *InlineClientV2) Submit(ctx context.Context, input attachedworkeroutput.AttachedWorkerInlineBodyV2, length int64, body io.Reader) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	deadline, err := inlineClientDeadlineV2(ctx, input.ExpiresAt, true)
	if err != nil || !inlineClientScopeV2(input.Version, input.Authorization, input.Nonce) || !inlineHexV2(input.Fingerprint) || !inlineHexV2(input.Capability) || length <= 0 || length > maxInlineBodyBytesV2 || body == nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrInvalid
	}
	metadata, err := json.Marshal(input)
	defer clear(metadata)
	if err != nil || len(metadata) > maxInlineControlBytesV2 {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrInvalid
	}
	return c.exchange(ctx, deadline, PathInlineBodyV2, nil, body, length, metadata, inlineClientExpectedV2{nonce: input.Nonce, fingerprint: input.Fingerprint, expiry: input.ExpiresAt, noCapability: true, noPrepared: true})
}

func (c *InlineClientV2) Reconcile(ctx context.Context, input attachedworkeroutput.AttachedWorkerInlineReconcileV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	deadline, err := inlineClientDeadlineV2(ctx, time.Time{}, false)
	if err != nil || !inlineClientScopeV2(input.Version, input.Authorization, input.Nonce) || !inlineHexV2(input.Fingerprint) {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrInvalid
	}
	encoded, err := json.Marshal(input)
	defer clear(encoded)
	if err != nil || len(encoded) > maxInlineControlBytesV2 {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrInvalid
	}
	// The caller's immutable journal, not this stateless wire adapter, compares
	// Reconcile's returned expiry with its commitment. READY may be expired.
	return c.exchange(ctx, deadline, PathInlineReconcileV2, encoded, nil, 0, nil, inlineClientExpectedV2{nonce: input.Nonce, fingerprint: input.Fingerprint, noCapability: input.BodySendBurned})
}

func inlineClientStringsV2(values ...string) bool {
	for _, value := range values {
		if len(value) > 160 || !utf8.ValidString(value) {
			return false
		}
	}
	return true
}

func inlineClientScopeV2(version uint32, a ports.AttachedWorkerSealedInputAuthorization, nonce domain.IdempotencyKey) bool {
	return version == 2 && nonce.Validate() == nil && a.PresentedSecretDigest == "" && inlineClientStringsV2(string(a.TenantID), string(a.OwnerUserID), string(a.WorkerID), string(a.ConnectionID), string(a.RunID), string(a.AttemptID), string(a.LeaseID), string(a.FenceToken), string(a.ContextDigest), string(a.CapabilityDigest), string(a.PolicyDigest))
}

func (c *InlineClientV2) exchange(parent context.Context, deadline time.Time, path string, encoded []byte, body io.Reader, length int64, metadata []byte, expected inlineClientExpectedV2) (result attachedworkeroutput.AttachedWorkerInlineResultV2, err error) {
	ctx, bearer, finish, err := c.begin(parent, deadline)
	if err != nil {
		return result, err
	}
	defer finish()
	if body == nil {
		body = bytes.NewReader(encoded)
		length = int64(len(encoded))
	}
	framed := &inlineClientBodyV2{reader: body, remaining: length}
	defer framed.Close()
	request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+path, framed)
	if requestErr != nil {
		return result, ErrInvalid
	}
	request.GetBody = nil
	request.ContentLength = length
	request.Header.Set("Content-Length", strconv.FormatInt(length, 10))
	request.Header.Set("Authorization", "Bearer "+string(bearer))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("User-Agent", "sessionless-inline-v2")
	if metadata != nil {
		request.Header.Set("Content-Type", InlineBodyMediaTypeV2)
		request.Header.Set(InlineRequestHeaderV2, base64.RawStdEncoding.EncodeToString(metadata))
	}
	if !inlineHeadersV2(request) {
		return result, ErrInvalid
	}
	// One Do only: no retry loop, GetBody, idempotency hint or redirect replay.
	response, doErr := c.http.Do(request)
	if response != nil && response.Body != nil {
		defer func() {
			if response.Body.Close() != nil || ctx.Err() != nil {
				result = attachedworkeroutput.AttachedWorkerInlineResultV2{}
				err = ErrUnavailable
			}
		}()
	}
	if doErr != nil || response == nil || response.Body == nil {
		return result, ErrUnavailable
	}
	if response.StatusCode != http.StatusOK {
		return result, inlineClientStatusV2(response.StatusCode)
	}
	media, single := inlineOneHeaderV2(response.Header, "Content-Type")
	cl, singleLength := inlineOneHeaderV2(response.Header, "Content-Length")
	if !single || media != "application/json" || !singleLength || cl != strconv.FormatInt(response.ContentLength, 10) || response.ContentLength <= 0 || response.ContentLength > maxInlineControlBytesV2 ||
		len(response.TransferEncoding) != 0 || len(response.Trailer) != 0 || response.Uncompressed || inlineHeaderCountV2(response.Header, "Content-Encoding") != 0 || !inlineClientResponseHeadersV2(response.Header) {
		return result, ErrUnavailable
	}
	raw := make([]byte, int(response.ContentLength))
	defer clear(raw)
	if _, readErr := io.ReadFull(response.Body, raw); readErr != nil {
		return result, ErrUnavailable
	}
	var trailing [1]byte
	defer clear(trailing[:])
	if n, readErr := io.ReadFull(response.Body, trailing[:]); n != 0 || readErr != io.EOF || ctx.Err() != nil || !framed.valid() {
		return result, ErrUnavailable
	}
	if !inlineDecodeV2(raw, maxInlineControlBytesV2, &result) || !inlineResponseV2(result) || result.Nonce != expected.nonce || result.Fingerprint != expected.fingerprint ||
		!expected.expiry.IsZero() && !result.ExpiresAt.Equal(expected.expiry) || expected.noCapability && result.Capability != "" || expected.noPrepared && result.Stage == attachedworkeroutput.InlinePreparedV2 ||
		result.Capability != "" && !result.ExpiresAt.After(time.Now()) {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrUnavailable
	}
	return result, nil
}

func inlineClientStatusV2(status int) error {
	switch status {
	case http.StatusBadRequest:
		return ErrInvalid
	case http.StatusUnauthorized, http.StatusForbidden:
		return ErrUnauthorized
	case http.StatusConflict:
		return ErrConflict
	default:
		return ErrUnavailable
	}
}

func inlineClientResponseHeadersV2(header http.Header) bool {
	n := 2
	for key, values := range header {
		for _, value := range values {
			if len(key)+len(value)+4 > maxInlineHeaderBytesV2-n {
				return false
			}
			n += len(key) + len(value) + 4
		}
	}
	return n <= maxInlineHeaderBytesV2
}

// Request framing is checked only when the transport reads it, never before
// Do. The one-byte EOF probe is not sent as content. Close drops the borrowed
// reader without closing or erasing caller-owned original semantic bytes.
type inlineClientBodyV2 struct {
	mu                       sync.Mutex
	reader                   io.Reader
	remaining                int64
	checked, invalid, closed bool
}

func (b *inlineClientBodyV2) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if b.remaining > 0 {
		if int64(len(p)) > b.remaining {
			p = p[:int(b.remaining)]
		}
		n, err := b.reader.Read(p)
		b.remaining -= int64(n)
		if err == io.EOF {
			if b.remaining == 0 {
				b.checked = true
			} else {
				b.invalid = true
				err = io.ErrUnexpectedEOF
			}
		} else if err != nil {
			b.invalid = true
		}
		return n, err
	}
	var extra [1]byte
	defer clear(extra[:])
	n, err := b.reader.Read(extra[:])
	if n != 0 || err != nil && err != io.EOF {
		b.invalid = true
		return 0, ErrInvalid
	}
	if err == io.EOF {
		b.checked = true
	}
	return 0, err
}

func (b *inlineClientBodyV2) valid() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remaining == 0 && b.checked && !b.invalid
}
func (b *inlineClientBodyV2) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.reader = nil
	return nil
}
