package attachedworkerupload

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeruploadwire"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Client owns one finite metadata schedule for one original attempt. A future
// private runner must create exactly ONE instance, retain genuine original
// cancellation/cutoffs, and prospectively fund ALL its work on the SAME ledger
// before Issue. This public constructor cannot prove those premises. Neither
// this client nor its replies establish Source authority, provenance, fit,
// quiescence, signing permission, or permission to use a returned capability.
type Client struct {
	endpoint          string
	client            http.Client
	transport         *http.Transport
	mu                sync.Mutex
	binding           attachedworkersession.ConnectionBindingV1
	bearer            []byte
	original          attachedworkeruploadwire.Request
	expected          domain.AttachedWorkerAttemptV1
	plan              *attachedworkeroutput.AttemptOutputUploadPlanV1
	observed          [2]attachedworkeroutput.UploadOrdinalStateV1
	claimed           [2]bool
	admitted          bool
	reconciles, calls uint32
	remaining         uint64
	deadline          time.Time
	checkedAt         time.Time
	closed            bool
	inflight          chan struct{}
	life              context.Context
	cancel            context.CancelFunc
}

// ExchangeResult times the entire call, including encoding, HTTP, bounded EOF,
// decoding and association. These local monotonic samples are DATA for the
// original owner's deadline arithmetic, never a default allowance or grant.
type ExchangeResult struct {
	Reply              attachedworkeruploadwire.Reply
	SentAt, ReceivedAt time.Time
}

func (*Client) String() string                      { return "UploadClient{private:[redacted]}" }
func (c *Client) GoString() string                  { return c.String() }
func (*Client) MarshalJSON() ([]byte, error)        { return []byte(`"[REDACTED]"`), nil }
func (ExchangeResult) String() string               { return "UploadExchange{private:[redacted]}" }
func (r ExchangeResult) GoString() string           { return r.String() }
func (ExchangeResult) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }

func uploadEndpoint(endpoint string) (*url.URL, error) {
	if len(endpoint) == 0 || len(endpoint) > attachedworkeruploadwire.URLMaxBytes {
		return nil, ErrInvalid
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || strings.HasSuffix(u.Host, ":") || u.User != nil ||
		u.Opaque != "" || u.Path != PathV1 || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.String() != endpoint {
		return nil, ErrInvalid
	}
	return u, nil
}

func uploadBindingMatches(b attachedworkersession.ConnectionBindingV1, a ports.AttachedWorkerSealedInputAuthorization) bool {
	return b.ProtocolVersion == attachedworkerprotocol.ProtocolVersionV1 && !b.AuthenticationExpires.IsZero() &&
		b.TenantID == a.TenantID && b.OwnerUserID == a.OwnerUserID && b.WorkerID == a.WorkerID && b.ConnectionID == a.ConnectionID &&
		b.EnrollmentGeneration == a.EnrollmentGeneration && b.ConnectionGeneration == a.ConnectionGeneration && b.CapabilityDigest == a.CapabilityDigest
}

func normalizedUploadHead(h domain.AttachedWorkerAttemptV1) domain.AttachedWorkerAttemptV1 {
	h.CreatedAt = h.CreatedAt.UTC().Round(0)
	h.UpdatedAt = h.UpdatedAt.UTC().Round(0)
	h.LeaseExpiresAt = h.LeaseExpiresAt.UTC().Round(0)
	h.CancelDeadline = h.CancelDeadline.UTC().Round(0)
	return h
}

func NewClient(endpoint string, httpClient *http.Client, binding attachedworkersession.ConnectionBindingV1, bearer []byte,
	original attachedworkeruploadwire.Request, expected domain.AttachedWorkerAttemptV1) (*Client, error) {
	parsed, err := uploadEndpoint(endpoint)
	if err != nil || len(bearer) == 0 || len(bearer) > attachedworkertransport.ConnectionBearerMaxBytes {
		return nil, ErrInvalid
	}
	// The original parser enforces its bound before base64/string copies. Do not
	// clone a caller token first or treat token syntax as source authorization.
	if _, err := attachedworkertransport.ParseConnectionBearer(bearer); err != nil {
		return nil, ErrInvalid
	}
	if original.Operation != attachedworkeruploadwire.Admit || len(original.Artifacts) > 2 ||
		attachedworkeruploadwire.ValidateRequest(original) != nil || original.CallerDeadline.RemainingNanoseconds > uint64(attachedworkeroutput.AttemptOutputUploadMaxDuration) ||
		!uploadBindingMatches(binding, original.Authorization) || expected.Validate() != nil || expected.State != domain.AttachedWorkerAttemptClaimed ||
		expected.CancelRevision != 0 || !expected.CancelDeadline.IsZero() || expected.TerminalSequence != 0 || expected.TerminalStatus != "" || expected.TerminalEvidenceDigest != "" ||
		expected.ExecutionConnectionID != original.Authorization.ConnectionID || expected.ExecutionConnectionGeneration != original.Authorization.ConnectionGeneration ||
		!(ports.AttachedWorkerSealedInputAuthorizationResult{Status: ports.AttachedWorkerExecutionApplied, AttemptRevision: expected.Revision, Attempt: expected}).Matches(original.Authorization) {
		return nil, ErrInvalid
	}
	private, transport, err := attachedworkertransport.NewPrivateControlHTTPClient(httpClient, parsed.Hostname())
	if err != nil {
		return nil, ErrInvalid
	}
	original.Artifacts = append([]attachedworkeroutput.UploadArtifactMetadataV1(nil), original.Artifacts...)
	life, cancel := context.WithCancel(context.Background())
	return &Client{endpoint: endpoint, client: *private, transport: transport, binding: binding, bearer: bytes.Clone(bearer),
		original: original, expected: normalizedUploadHead(expected), remaining: original.CallerDeadline.RemainingNanoseconds, life: life, cancel: cancel}, nil
}

// Exchange spends one schedule slot BEFORE encoding or Do. Failed, denied,
// malformed, lost and late calls consume it permanently. No automatic retry,
// claim reconstruction, reconciliation settlement, or body request exists here.
// Only W may vary, and it can only tighten the original positive <=15s value.
// The caller must keep its genuine original context; deadline equality here is
// just a structural no-expansion guard, not proof of original clock ownership.
func (c *Client) Exchange(ctx context.Context, operation attachedworkeruploadwire.Operation, ordinal uint32, callerDeadline attachedworkeruploadwire.CallerDeadline) (result ExchangeResult, resultErr error) {
	sent := time.Now()
	if c == nil || ctx == nil || ctx.Err() != nil {
		return ExchangeResult{}, ErrInvalid
	}
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline || callerDeadline.Version != 1 || callerDeadline.RemainingNanoseconds == 0 ||
		callerDeadline.RemainingNanoseconds > uint64(attachedworkeroutput.AttemptOutputUploadMaxDuration) {
		return ExchangeResult{}, ErrInvalid
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ExchangeResult{}, ErrUnavailable
	}
	if c.inflight != nil || c.calls >= attachedworkeruploadwire.MaxControlRPCs || callerDeadline.RemainingNanoseconds > c.remaining ||
		!c.deadline.IsZero() && deadline.After(c.deadline) {
		c.mu.Unlock()
		return ExchangeResult{}, ErrInvalid
	}
	r := attachedworkeruploadwire.Request{Version: 1, Operation: operation, Authorization: c.original.Authorization, Nonce: c.original.Nonce, CallerDeadline: callerDeadline}
	switch operation {
	case attachedworkeruploadwire.Admit:
		if c.admitted || ordinal != 0 {
			c.mu.Unlock()
			return ExchangeResult{}, ErrInvalid
		}
		r = c.original
		r.CallerDeadline = callerDeadline
		c.admitted = true // A lost admit may only use read-only reconciliation.
	case attachedworkeruploadwire.Claim:
		if !c.admitted || c.plan == nil || ordinal >= uint32(len(c.plan.Ordinals)) || c.claimed[ordinal] || c.observed[ordinal] != attachedworkeroutput.UploadOrdinalUnissuedV1 {
			c.mu.Unlock()
			return ExchangeResult{}, ErrInvalid
		}
		r.PlanDigest, r.Ordinal = c.plan.PlanDigest, ordinal
		c.claimed[ordinal] = true // Never refunded, including after no reply.
	case attachedworkeruploadwire.Reconcile:
		if !c.admitted || c.reconciles >= 2 || ordinal != 0 {
			c.mu.Unlock()
			return ExchangeResult{}, ErrInvalid
		}
		r.ProducerDigest, r.FinalDigest = c.original.ProducerDigest, c.original.FinalDigest
		c.reconciles++
	default:
		c.mu.Unlock()
		return ExchangeResult{}, ErrInvalid
	}
	c.calls++
	c.remaining, c.deadline = callerDeadline.RemainingNanoseconds, deadline
	c.inflight = make(chan struct{})
	life := c.life
	c.mu.Unlock()
	// Installed before owned request/reply cleanup; Close joins only after all
	// those aliases and the HTTP body have been released by this call.
	defer func() {
		c.mu.Lock()
		if c.closed || ctx.Err() != nil {
			result, resultErr = ExchangeResult{}, ErrUnavailable
		} else if resultErr == nil && result.Reply.Status != attachedworkeruploadwire.Pending && !c.acceptReply(r, result.Reply) {
			result, resultErr = ExchangeResult{}, ErrUnavailable
		}
		if resultErr == nil {
			result.SentAt, result.ReceivedAt = sent, time.Now()
		}
		close(c.inflight)
		c.inflight = nil
		c.mu.Unlock()
	}()
	data, err := attachedworkeruploadwire.EncodeRequest(r)
	if err != nil {
		return ExchangeResult{}, ErrInvalid
	}
	defer clear(data)
	requestCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(life, cancel)
	defer func() { stop(); cancel() }()
	if life.Err() != nil {
		return ExchangeResult{}, ErrUnavailable
	}
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, c.endpoint, bytes.NewReader(data))
	if err != nil {
		return ExchangeResult{}, ErrInvalid
	}
	request.GetBody, request.Close = nil, true
	request.Header.Set("Authorization", "Bearer "+string(c.bearer))
	defer request.Header.Del("Authorization")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	response, err := c.client.Do(request)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return ExchangeResult{}, ErrUnavailable
	}
	if response.Body == nil {
		return ExchangeResult{}, ErrUnavailable
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			result, resultErr = ExchangeResult{}, ErrUnavailable
		}
	}()
	limit := attachedworkeruploadwire.ReplyWithoutCapabilityMaxBytes
	if operation == attachedworkeruploadwire.Claim {
		limit = attachedworkeruploadwire.ReplyMaxBytes
	}
	encodings := response.Header.Values("Content-Encoding")
	if response.StatusCode != http.StatusOK || response.Uncompressed || response.ProtoMajor != 1 || response.ProtoMinor != 1 ||
		len(response.Header.Values("Content-Type")) != 1 || response.Header.Get("Content-Type") != "application/json" ||
		len(response.Header.Values("Cache-Control")) != 1 || response.Header.Get("Cache-Control") != "no-store" ||
		len(encodings) > 1 || len(encodings) == 1 && encodings[0] != "identity" || len(response.Header.Values("Content-Length")) > 1 ||
		len(response.Trailer) != 0 || len(response.Header.Values("Trailer")) != 0 ||
		len(response.TransferEncoding) > 1 || len(response.TransferEncoding) == 1 && (response.TransferEncoding[0] != "chunked" || response.ContentLength != -1) ||
		response.ContentLength == 0 || response.ContentLength < -1 || response.ContentLength > int64(limit) {
		return ExchangeResult{}, ErrUnavailable
	}
	var arena [attachedworkeruploadwire.ReplyMaxBytes + 1]byte
	defer clear(arena[:])
	n, readErr := io.ReadFull(response.Body, arena[:limit+1])
	if n == 0 || n > limit || readErr != io.ErrUnexpectedEOF || requestCtx.Err() != nil ||
		response.ContentLength >= 0 && response.ContentLength != int64(n) || len(response.Trailer) != 0 {
		return ExchangeResult{}, ErrUnavailable
	}
	reply, err := attachedworkeruploadwire.DecodeReply(arena[:n])
	if err != nil {
		return ExchangeResult{}, ErrUnavailable
	}
	if reply.Status == ports.AttachedWorkerExecutionDenied {
		return ExchangeResult{}, ErrUnauthorized
	}
	if reply.Status == attachedworkeruploadwire.Pending {
		return ExchangeResult{Reply: reply}, nil
	}
	return ExchangeResult{Reply: reply}, nil
}

// Caller holds mu and has exclusive inflight ownership. Keep a pristine DATA
// plan for immutable identity/key comparison, and observations separately: the
// wire intentionally contains no persisted grant digest or verified BlobRef.
func (c *Client) acceptReply(request attachedworkeruploadwire.Request, reply attachedworkeruploadwire.Reply) bool {
	if normalizedUploadHead(reply.AcceptedAttempt) != c.expected || !c.checkedAt.IsZero() && reply.CheckedAt.Before(c.checkedAt) {
		return false
	}
	retained := c.plan
	if retained == nil {
		id := attachedworkeroutput.UploadPlanIdentityV1{Binding: c.original.Authorization, SessionID: c.original.SessionID,
			HarnessBindingDigest: c.original.HarnessBindingDigest, ProfileDigest: c.original.ProfileDigest, SchemaDigest: c.original.SchemaDigest,
			FinalDigest: c.original.FinalDigest, AnswerDigest: c.original.AnswerDigest, ProducerDigest: c.original.ProducerDigest,
			Nonce: c.original.Nonce, CreatedAt: reply.CreatedAt, ExpiresAt: reply.ExpiresAt}
		plan, err := attachedworkeroutput.NewAttemptOutputUploadPlanV1(id, c.original.Artifacts)
		if err != nil {
			return false
		}
		retained = &plan
	}
	if attachedworkeruploadwire.CompareReply(c.original, request, reply, retained) != nil {
		return false
	}
	for i, ordinal := range reply.Ordinals {
		old := c.observed[i]
		if old != "" && !uploadObservationAdvances(old, ordinal.State) {
			return false
		}
	}
	if c.plan == nil {
		c.plan = retained
	}
	for i, ordinal := range reply.Ordinals {
		c.observed[i] = ordinal.State
	}
	c.checkedAt = reply.CheckedAt
	return true
}

func uploadObservationAdvances(old, next attachedworkeroutput.UploadOrdinalStateV1) bool {
	if old == attachedworkeroutput.UploadOrdinalUnissuedV1 {
		return true
	}
	if old == attachedworkeroutput.UploadOrdinalWritePendingV1 {
		return next == old || next == attachedworkeroutput.UploadOrdinalNotDispatchedV1 || next == attachedworkeroutput.UploadOrdinalSourceVerifiedV1
	}
	return next == old
}

// Close cancels and joins the client-owned call without holding the mutex its
// return path needs. It clears owned metadata/bearer after joining; immutable
// strings and native HTTP/TLS buffers are NOT claimed universally zeroized.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}
	done := c.inflight
	c.mu.Unlock()
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	if done != nil {
		<-done
	}
	c.mu.Lock()
	clear(c.bearer)
	c.bearer = nil
	c.binding = attachedworkersession.ConnectionBindingV1{}
	c.original = attachedworkeruploadwire.Request{}
	c.expected = domain.AttachedWorkerAttemptV1{}
	c.plan = nil
	clear(c.observed[:])
	c.mu.Unlock()
	return nil
}
