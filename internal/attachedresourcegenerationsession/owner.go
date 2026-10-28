// Package attachedresourcegenerationsession joins one original connected
// Session to metadata from its original owner-local credential lifecycle.
// It neither issues credentials nor grants execution or terminal authority.
package attachedresourcegenerationsession

import (
	"context"
	"errors"
	"math"
	"net/http"
	"reflect"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrDenied      = errors.New("attached resource generation session denied")
	ErrUnavailable = errors.New("attached resource generation acknowledgement unavailable")
)

// SourceFactoryV1 preserves the existing two-phase source lifetime. Retirement
// must synchronously fence input/receipt reads and cancel their bearer clients
// without waiting for a foreign exchange Close. Close owns that later cleanup;
// implementations must also fence an Open racing retirement/Close.
type SourceFactoryV1 interface {
	attachedworkersession.ExchangeFactory
	RetireConnectionSourcesV1() error
	Close() error
}

// OwnerV1 borrows the SAME concrete lifecycle used by the outer runner. Its
// acknowledged head is ephemeral: it is not a second credential/ACK ledger.
type OwnerV1 struct {
	self          *OwnerV1
	mu            sync.Mutex
	gate          chan struct{}
	baseline      domain.AttachedResourceGenerationBaselineV1
	lifecycle     *ownerlocalcredential.Lifecycle
	origin        string
	http          *http.Client
	delegate      SourceFactoryV1
	client        *attachedworkerhttp.ResourceGenerationClientV1
	binding       attachedworkersession.ConnectionBindingV1
	session       *attachedworkersession.Session
	mode          attachedworkersession.ResourceGenerationConnectionModeV1
	ack           *domain.AttachedResourceGenerationHeadV1
	attempted     *domain.AttachedResourceGenerationHeadV1
	opened        bool
	retired       bool
	closed        bool
	reconnectRead bool
	closeFinished bool
	closeErr      error
}

// NewV1 accepts installed data and original concrete owners, never a caller
// publication DTO, metadata-reader callback or replacement credential issuer.
func NewV1(origin string, client *http.Client, delegate SourceFactoryV1, lifecycle *ownerlocalcredential.Lifecycle,
	baseline domain.AttachedResourceGenerationBaselineV1) (*OwnerV1, error) {
	if delegate == nil || lifecycle == nil || baseline.Validate() != nil ||
		baseline.Binding.Backend.BackendKind != domain.HarnessBackendCodexExecV1 || baseline.Binding.ModelVendorID != "openai" || client == nil {
		return nil, ErrDenied
	}
	// Endpoint and explicit transport validation are performed by the existing
	// client in Open, where the original connection bearer first exists.
	configured := *client
	v := &OwnerV1{origin: origin, http: &configured, delegate: delegate, lifecycle: lifecycle, baseline: baseline.Clone(), gate: make(chan struct{}, 1)}
	v.self = v
	v.gate <- struct{}{}
	return v, nil
}

func (*OwnerV1) String() string     { return "ResourceGenerationSessionV1{private:[redacted]}" }
func (v *OwnerV1) GoString() string { return v.String() }

// Open is the existing authenticated source-factory seam. It binds a client,
// but performs NO metadata I/O: the Connector has not accepted Ready yet.
func (v *OwnerV1) Open(binding attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
	if v == nil || v.self != v {
		return nil, ErrDenied
	}
	v.mu.Lock()
	if v.closed || v.retired || v.opened || !v.matchesBinding(binding) {
		v.mu.Unlock()
		return nil, ErrDenied
	}
	v.opened = true // reserve before constructing/delegating; failure is terminal
	v.mu.Unlock()
	client, err := attachedworkerhttp.NewResourceGenerationClientV1(v.origin, v.http, bearer)
	if err != nil {
		_ = v.retire()
		return nil, ErrDenied
	}
	v.mu.Lock()
	if v.closed || v.retired {
		v.mu.Unlock()
		_ = client.Close()
		return nil, ErrUnavailable
	}
	// Install before entering delegate.Open so concurrent retirement can clear
	// this owned bearer even if a foreign Open has not returned yet.
	v.binding, v.client = binding, client
	v.mu.Unlock()
	port, err := v.delegate.Open(binding, bearer)
	if err != nil || port == nil {
		_ = client.Close()
		_ = v.retire()
		return nil, ErrUnavailable
	}
	v.mu.Lock()
	if v.closed || v.retired {
		v.mu.Unlock()
		_ = client.Close()
		_ = v.delegate.RetireConnectionSourcesV1()
		return nil, ErrUnavailable
	}
	v.mu.Unlock()
	return &closingExchange{owner: v, delegate: port}, nil
}

func (v *OwnerV1) matchesBinding(b attachedworkersession.ConnectionBindingV1) bool {
	return b.TenantID == v.baseline.TenantID && b.OwnerUserID == v.baseline.OwnerUserID && b.WorkerID == v.baseline.WorkerID &&
		b.EnrollmentGeneration == v.baseline.EnrollmentGeneration && b.CapabilityDigest == v.baseline.CapabilityDigest &&
		b.ConnectionID.Validate() == nil && b.ConnectionGeneration > 0 && b.ProtocolVersion == attachedworkerprotocol.ProtocolVersionV1 && !b.AuthenticationExpires.IsZero()
}

func (v *OwnerV1) BindInitialSessionV1(ctx context.Context, session *attachedworkersession.Session) error {
	return v.bind(ctx, session, attachedworkersession.ResourceGenerationInitialV1)
}
func (v *OwnerV1) BindReconnectedSessionV1(ctx context.Context, session *attachedworkersession.Session) error {
	return v.bind(ctx, session, attachedworkersession.ResourceGenerationReconnectV1)
}
func (v *OwnerV1) bind(ctx context.Context, session *attachedworkersession.Session, mode attachedworkersession.ResourceGenerationConnectionModeV1) error {
	if v == nil || v.self != v || session == nil || ctx == nil || ctx.Err() != nil {
		return ErrDenied
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.retired || v.client == nil || v.session != nil || session.CheckResourceGenerationConnectionV1(ctx, v.binding, mode, nil) != nil {
		return ErrDenied
	}
	v.session, v.mode = session, mode
	return nil
}

type connection struct {
	session *attachedworkersession.Session
	client  *attachedworkerhttp.ResourceGenerationClientV1
	binding attachedworkersession.ConnectionBindingV1
	mode    attachedworkersession.ResourceGenerationConnectionModeV1
}

func (v *OwnerV1) connection() (connection, error) {
	if v == nil || v.self != v {
		return connection{}, ErrDenied
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.retired || v.session == nil || v.client == nil {
		return connection{}, ErrDenied
	}
	return connection{v.session, v.client, v.binding, v.mode}, nil
}

func (v *OwnerV1) check(ctx context.Context, c connection, positive bool) error {
	var expiry *time.Time
	if positive {
		expiry = v.baseline.Binding.EvidenceExpiresAt
	}
	if ctx == nil || ctx.Err() != nil || c.session.CheckResourceGenerationConnectionV1(ctx, c.binding, c.mode, expiry) != nil {
		return ErrDenied
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.retired || v.session != c.session || v.client != c.client || v.binding != c.binding || v.mode != c.mode {
		return ErrDenied
	}
	return nil
}

// localHead reads metadata only from the original concrete lifecycle. The
// secret reference/fingerprint never leave this frame or enter publication.
func (v *OwnerV1) localHead(ctx context.Context) (domain.AttachedResourceGenerationHeadV1, error) {
	b, found, err := v.lifecycle.LoadCredentialBinding(ctx, v.baseline.TenantID, v.baseline.SubscriptionConnectionID)
	if err != nil || !found || b.Validate() != nil || b.TenantID != v.baseline.TenantID || b.OwnerUserID != v.baseline.OwnerUserID ||
		b.SubscriptionConnectionID != v.baseline.SubscriptionConnectionID || b.Provider != "openai" || b.AuthMode != "chatgpt" ||
		!b.Revoked && b.Generation == math.MaxUint64 {
		return domain.AttachedResourceGenerationHeadV1{}, ErrDenied
	}
	head := domain.AttachedResourceGenerationHeadV1{Version: 1, OwnerUserID: v.baseline.OwnerUserID, WorkerID: v.baseline.WorkerID,
		EnrollmentGeneration: v.baseline.EnrollmentGeneration, BaselineRevision: v.baseline.BaselineRevision, CredentialGeneration: b.Generation, Revoked: b.Revoked}
	if head.Validate() != nil {
		return domain.AttachedResourceGenerationHeadV1{}, ErrDenied
	}
	return head, nil
}

func (v *OwnerV1) authorization(c connection) ports.AttachedResourceGenerationAuthorizationV1 {
	return ports.AttachedResourceGenerationAuthorizationV1{TenantID: v.baseline.TenantID, OwnerUserID: v.baseline.OwnerUserID,
		SubscriptionConnectionID: v.baseline.SubscriptionConnectionID, WorkerID: v.baseline.WorkerID,
		EnrollmentGeneration: v.baseline.EnrollmentGeneration, BaselineRevision: v.baseline.BaselineRevision,
		ConnectionID: c.binding.ConnectionID, ConnectionGeneration: c.binding.ConnectionGeneration}
}

// SynchronizeV1 owns ONE publication and at most ONE exact readback within the
// same original bounded context. Initial reconnect recovery is read-only;
// after its exact ACK, subsequent local changes use the same one-shot CAS as
// an initially connected owner. Connection mode never changes. A failed/unknown
// publication target cannot be automatically published again by this owner.
func (v *OwnerV1) SynchronizeV1(ctx context.Context) error {
	if v == nil || v.self != v || ctx == nil || ctx.Err() != nil {
		return ErrDenied
	}
	op, cancel := context.WithTimeout(ctx, ports.AttachedResourceGenerationOperationTimeoutV1)
	defer cancel()
	select {
	case <-op.Done():
		return ErrUnavailable
	case <-v.gate:
	}
	defer func() { v.gate <- struct{}{} }()
	c, err := v.connection()
	if err != nil || v.check(op, c, false) != nil {
		return ErrDenied
	}
	target, err := v.localHead(op)
	v.mu.Lock()
	recovering := c.mode == attachedworkersession.ResourceGenerationReconnectV1 && v.ack == nil
	v.mu.Unlock()
	positive := !target.Revoked && !recovering
	if err != nil || v.check(op, c, positive) != nil {
		return ErrDenied
	}
	v.mu.Lock()
	if v.ack != nil && *v.ack == target {
		v.mu.Unlock()
		return v.confirm(op, c, target, positive)
	}
	if recovering {
		if v.reconnectRead {
			v.mu.Unlock()
			return ErrUnavailable
		}
		v.reconnectRead = true
		v.mu.Unlock()
		result, err := c.client.Read(op, v.authorization(c))
		if err != nil || result.Status != ports.AttachedWorkerExecutionApplied || result.Head != target {
			return ErrUnavailable
		}
		return v.confirm(op, c, target, false)
	}
	if v.attempted != nil && *v.attempted == target {
		v.mu.Unlock()
		return ErrUnavailable
	}
	expected := uint64(0)
	if v.ack != nil {
		expected = v.ack.CredentialGeneration
	}
	if !target.Revoked && (v.ack == nil && target.CredentialGeneration != 1 || v.ack != nil &&
		(v.ack.Revoked || expected == math.MaxUint64 || target.CredentialGeneration != expected+1)) {
		v.mu.Unlock()
		return ErrDenied
	}
	copy := target
	v.attempted = &copy
	v.mu.Unlock()
	result, err := c.client.Publish(op, ports.AttachedResourceGenerationPublicationV1{Authorization: v.authorization(c), ExpectedCredentialGeneration: expected, Target: target})
	if err != nil {
		var exchange *attachedworkerhttp.ExchangeError
		if !errors.As(err, &exchange) || exchange.Kind != attachedworkerhttp.ErrorUnavailable || op.Err() != nil || v.check(op, c, false) != nil {
			return ErrUnavailable
		}
		// No new operation deadline: the point read consumes this SAME window.
		result, err = c.client.Read(op, v.authorization(c))
	}
	if err != nil || result.Head != target || result.Status != ports.AttachedWorkerExecutionApplied && result.Status != ports.AttachedWorkerExecutionReplayed {
		return ErrUnavailable
	}
	return v.confirm(op, c, target, positive)
}

func (v *OwnerV1) confirm(ctx context.Context, c connection, target domain.AttachedResourceGenerationHeadV1, positive bool) error {
	current, err := v.localHead(ctx)
	if err != nil || current != target || v.check(ctx, c, positive) != nil {
		return ErrDenied
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.retired || v.session != c.session || v.client != c.client {
		return ErrDenied
	}
	copy := target
	v.ack = &copy
	return nil
}

// CheckNewResourceV1 is ONLY a new-input fence, not an execution grant. Native
// consumers must still prove the actual accepted sealed job and original claim.
// Do not call this from old-N receipt/finalizer/terminal cleanup paths.
func (v *OwnerV1) CheckNewResourceV1(ctx context.Context, resource domain.ProviderResourceBindingV1) error {
	c, err := v.connection()
	if err != nil || v.check(ctx, c, true) != nil {
		return ErrDenied
	}
	target, err := v.localHead(ctx)
	if err != nil || target.Revoked || resource.Validate() != nil {
		return ErrDenied
	}
	expected := v.baseline.Binding.Resource
	expected.CredentialGeneration = target.CredentialGeneration
	if resource != expected {
		return ErrDenied
	}
	v.mu.Lock()
	acknowledged := !v.closed && !v.retired && v.ack != nil && *v.ack == target
	v.mu.Unlock()
	if !acknowledged || v.check(ctx, c, true) != nil {
		return ErrDenied
	}
	return nil
}

// CheckNewResourceForSessionV1 is only a concrete new-input consistency fence,
// never an accepted-job, execution or terminal grant. Matching snapshots,
// credential metadata or baseline digests cannot replace the original owners.
// It neither synchronizes/publishes metadata nor issues credentials/reads secret bytes.
func (v *OwnerV1) CheckNewResourceForSessionV1(ctx context.Context, actual *attachedworkersession.Session,
	same *ownerlocalcredential.Lifecycle, installed domain.AttachedResourceGenerationBaselineV1,
	resource domain.ProviderResourceBindingV1) error {
	if v == nil || v.self != v || ctx == nil || ctx.Err() != nil || actual == nil || same == nil || installed.Validate() != nil {
		return ErrDenied
	}
	v.mu.Lock()
	matched := !v.closed && !v.retired && v.session == actual && v.lifecycle == same && reflect.DeepEqual(v.baseline, installed)
	v.mu.Unlock()
	if !matched {
		return ErrDenied
	}
	// The existing check rereads the original lifecycle's metadata and exact
	// current active ACK, with the original connection checked before AND after.
	if err := v.CheckNewResourceV1(ctx, resource); err != nil {
		return err
	}
	v.mu.Lock()
	matched = !v.closed && !v.retired && v.session == actual && v.lifecycle == same
	v.mu.Unlock()
	if !matched || ctx.Err() != nil {
		return ErrDenied
	}
	return nil
}

func (v *OwnerV1) retire() error {
	if v == nil || v.self != v {
		return nil
	}
	v.mu.Lock()
	v.retired = true
	v.ack = nil
	client := v.client
	v.client = nil
	v.mu.Unlock()
	clientErr := client.Close()
	sourceErr := v.delegate.RetireConnectionSourcesV1()
	if clientErr != nil || sourceErr != nil {
		return ErrUnavailable
	}
	return nil
}

// Close first revokes metadata immediately, then performs the original outer
// delegate cleanup. It never closes/releases the borrowed credential lifecycle.
func (v *OwnerV1) Close() error {
	if v == nil || v.self != v {
		return nil
	}
	v.mu.Lock()
	if v.closed {
		err := v.closeErr
		if !v.closeFinished {
			err = ErrUnavailable
		}
		v.mu.Unlock()
		return err
	}
	v.closed = true
	v.mu.Unlock()
	// Claim exactly once before retirement/foreign cleanup. A concurrent Close
	// must not turn "started" into successful cleanup, and a later Close must
	// retain the original sanitized failure rather than retrying the delegate.
	err := v.retire()
	if v.delegate.Close() != nil {
		err = ErrUnavailable
	}
	v.mu.Lock()
	v.closeErr, v.closeFinished = err, true
	v.mu.Unlock()
	return err
}

type closingExchange struct {
	owner    *OwnerV1
	delegate attachedworkersession.ExchangePort
}

func (p *closingExchange) Exchange(ctx context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	return p.delegate.Exchange(ctx, batch)
}

// The Session-facing phase never waits for a foreign delegate's Close. The
// original outer owner must still call OwnerV1.Close for delegate cleanup.
func (p *closingExchange) Close() error { return p.owner.retire() }

var _ attachedworkersession.ExchangeFactory = (*OwnerV1)(nil)
