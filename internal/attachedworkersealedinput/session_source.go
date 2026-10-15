package attachedworkersealedinput

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
)

// SessionSourceFactory binds sealed-input reads to the bearer produced by one
// authenticated connection. Open is called by the session only after the
// connection generation has been accepted; the source is unavailable before
// that point and is retired with the exchange. This remains a default-off
// library seam, not a shipped command or provider-credential path.
type SessionSourceFactory struct {
	delegate        attachedworkersession.ExchangeFactory
	endpoint        string
	client          *http.Client
	receiptEndpoint string
	phaseEndpoint   string

	mu        sync.Mutex
	state     sourceState
	source    *ClientSource
	publisher *attachedworkerreceipt.ClientPublisher
	phase     *PiAPIClient
	port      attachedworkersession.ExchangePort
	cancel    context.CancelFunc
	life      context.Context
}

type sourceState uint8

const (
	sourceUnopened sourceState = iota
	sourceOpening
	sourceOpen
	sourceClosed
)

func NewSessionSourceFactory(endpoint string, client *http.Client, delegate attachedworkersession.ExchangeFactory) (*SessionSourceFactory, error) {
	if delegate == nil {
		return nil, ErrInvalid
	}
	// NewClientSource is also the single endpoint validator. The temporary
	// sentinel is never sent or retained after Close.
	probe, err := NewClientSource(endpoint, client, []byte{1})
	if err != nil {
		return nil, err
	}
	_ = probe.Close()
	life, cancel := context.WithCancel(context.Background())
	return &SessionSourceFactory{delegate: delegate, endpoint: endpoint, client: client, life: life, cancel: cancel}, nil
}

// NewReceiptSessionSourceFactory explicitly composes sealed input and output
// receipts for one accepted connection. Both HTTPS endpoints must have the
// same origin. This library constructor enables neither provider input nor
// credentials, invocation, or an activation profile; ordinary activation still
// uses NewSessionSourceFactory. Server authorization remains authoritative.
func NewReceiptSessionSourceFactory(endpoint, receiptEndpoint string, client *http.Client, delegate attachedworkersession.ExchangeFactory) (*SessionSourceFactory, error) {
	inputURL, inputErr := url.Parse(endpoint)
	receiptURL, receiptErr := url.Parse(receiptEndpoint)
	if inputErr != nil || receiptErr != nil || inputURL.Scheme != receiptURL.Scheme || inputURL.Host != receiptURL.Host {
		return nil, ErrInvalid
	}
	probe, err := attachedworkerreceipt.NewClientPublisher(receiptEndpoint, client, []byte{1})
	if err != nil {
		return nil, ErrInvalid
	}
	_ = probe.Close()
	factory, err := NewSessionSourceFactory(endpoint, client, delegate)
	if err != nil {
		return nil, err
	}
	factory.receiptEndpoint = receiptEndpoint
	return factory, nil
}

// NewPiPhaseSessionSourceFactory explicitly composes input, receipt and phase
// metadata for the SAME accepted Session/bearer/origin. Existing constructors
// and normal composition are unchanged. There is no server operation/issuer or
// driver/credential/profile activation here; canonical API input additionally
// requires the explicit NewPiBoundMaterializer arm. Full private-parent/native
// qualification and the original currentness/deadline gates remain required.
func NewPiPhaseSessionSourceFactory(endpoint, receiptEndpoint, phaseEndpoint string, client *http.Client, delegate attachedworkersession.ExchangeFactory) (*SessionSourceFactory, error) {
	phaseURL, err := piEndpoint(phaseEndpoint)
	if err != nil {
		return nil, ErrInvalid
	}
	inputURL, inputErr := url.Parse(endpoint)
	receiptURL, receiptErr := url.Parse(receiptEndpoint)
	if inputErr != nil || receiptErr != nil || !exactPiCompanionEndpoint(inputURL, endpoint, PathV1) || !exactPiCompanionEndpoint(receiptURL, receiptEndpoint, attachedworkerreceipt.PathV1) || inputURL.Scheme != phaseURL.Scheme || inputURL.Host != phaseURL.Host || receiptURL.Scheme != phaseURL.Scheme || receiptURL.Host != phaseURL.Host {
		return nil, ErrInvalid
	}
	private, transport, err := piHTTPClient(client, phaseURL.Hostname())
	if err != nil {
		return nil, ErrInvalid
	}
	factory, err := NewReceiptSessionSourceFactory(endpoint, receiptEndpoint, private, delegate)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	factory.phaseEndpoint = phaseEndpoint
	return factory, nil
}

func exactPiCompanionEndpoint(u *url.URL, endpoint, path string) bool {
	return u != nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.Opaque == "" && u.Path == path &&
		u.RawPath == "" && u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && u.String() == endpoint
}

func (factory *SessionSourceFactory) Open(binding attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
	if factory == nil || factory.delegate == nil || factory.cancel == nil {
		return nil, ErrInvalid
	}
	factory.mu.Lock()
	if factory.state != sourceUnopened {
		factory.mu.Unlock()
		return nil, ErrUnavailable
	}
	factory.state = sourceOpening
	factory.mu.Unlock()
	if factory.phaseEndpoint != "" {
		_, bearerErr := attachedworkertransport.ParseConnectionBearer(bearer)
		if !validPiBinding(binding) || bearerErr != nil {
			_ = factory.Close()
			return nil, ErrInvalid
		}
	}

	source, err := NewClientSource(factory.endpoint, factory.client, bearer)
	if err != nil {
		_ = factory.Close()
		return nil, err
	}
	var publisher *attachedworkerreceipt.ClientPublisher
	if factory.receiptEndpoint != "" {
		publisher, err = attachedworkerreceipt.NewClientPublisher(factory.receiptEndpoint, factory.client, bearer)
		if err != nil {
			_ = source.Close()
			_ = factory.Close()
			return nil, ErrInvalid
		}
	}
	var phase *PiAPIClient
	if factory.phaseEndpoint != "" {
		phase, err = NewPiAPIClient(factory.phaseEndpoint, factory.client, binding, bearer)
		if err != nil {
			_ = source.Close()
			_ = publisher.Close()
			_ = factory.Close()
			return nil, ErrInvalid
		}
	}
	// In the new phase branch, publish only ownership slots (NOT open state)
	// before a possibly held delegate.Open. Retirement can clear/cancel every
	// newly retained bearer without waiting for that delegate to return.
	if phase != nil {
		factory.mu.Lock()
		if factory.state != sourceOpening {
			factory.mu.Unlock()
			_ = source.Close()
			_ = publisher.Close()
			_ = phase.Close()
			return nil, ErrUnavailable
		}
		factory.source = source
		factory.publisher = publisher
		factory.phase = phase
		factory.mu.Unlock()
	}
	exchange, err := factory.delegate.Open(binding, bearer)
	if err != nil || exchange == nil {
		_ = source.Close()
		_ = publisher.Close()
		_ = phase.Close()
		_ = factory.Close()
		if closer, ok := exchange.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return nil, ErrUnavailable
	}
	factory.mu.Lock()
	if factory.state != sourceOpening {
		factory.mu.Unlock()
		_ = source.Close()
		_ = publisher.Close()
		_ = phase.Close()
		if closer, ok := exchange.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return nil, ErrUnavailable
	}
	factory.source = source
	factory.publisher = publisher
	factory.phase = phase
	factory.port = exchange
	factory.state = sourceOpen
	factory.mu.Unlock()
	return &sourceClosingExchange{delegate: exchange, source: factory}, nil
}

// RequestPiAPIUse exposes only the SAME accepted session's phase metadata
// channel. It never computes a new deadline/permit or proves source currentness.
func (factory *SessionSourceFactory) RequestPiAPIUse(ctx context.Context, request pisdkphasewire.Request) (pisdkphasewire.Reply, error) {
	if factory == nil || ctx == nil || ctx.Err() != nil {
		return pisdkphasewire.Reply{}, ErrInvalid
	}
	factory.mu.Lock()
	phase := factory.phase
	open := factory.state == sourceOpen
	factory.mu.Unlock()
	if !open || phase == nil {
		return pisdkphasewire.Reply{}, ErrUnavailable
	}
	reply, err := phase.RequestPiAPIUse(ctx, request)
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if factory.state != sourceOpen || ctx.Err() != nil {
		return pisdkphasewire.Reply{}, ErrUnavailable
	}
	return reply, err
}

func (factory *SessionSourceFactory) Load(ctx context.Context, request attachedworkerdaemontransport.MaterializationRequestV1) (attachedworkerdaemontransport.SealedInputV1, error) {
	if factory == nil || ctx == nil || ctx.Err() != nil {
		return attachedworkerdaemontransport.SealedInputV1{}, ErrInvalid
	}
	factory.mu.Lock()
	source, life := factory.source, factory.life
	open := factory.state == sourceOpen
	factory.mu.Unlock()
	if !open || source == nil {
		return attachedworkerdaemontransport.SealedInputV1{}, ErrUnavailable
	}
	requestCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(life, cancel)
	defer func() { stop(); cancel() }()
	if life.Err() != nil {
		return attachedworkerdaemontransport.SealedInputV1{}, ErrUnavailable
	}
	return source.Load(requestCtx, request)
}

// Publish uses only the bearer accepted by Open. Exact response-loss replay is
// owned by ClientPublisher, not by an invocation retry or another session.
func (factory *SessionSourceFactory) Publish(ctx context.Context, submission attachedworkerdaemontransport.ReceiptSubmissionV1) (attachedworkerdaemontransport.ReceiptCommitmentV1, error) {
	if factory == nil || ctx == nil || ctx.Err() != nil {
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrInvalid
	}
	factory.mu.Lock()
	publisher, life := factory.publisher, factory.life
	open := factory.state == sourceOpen
	factory.mu.Unlock()
	if !open || publisher == nil {
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrUnavailable
	}
	requestCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(life, cancel)
	defer func() { stop(); cancel() }()
	if life.Err() != nil {
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrUnavailable
	}
	return publisher.Publish(requestCtx, submission)
}

// revokeSource clears both bearers and cancels in-flight input reads/receipts
// without waiting for the transport delegate's Close. This lets a bounded
// caller revoke fetch authority before a late session/lease finalizer waits
// for an injected exchange implementation that ignores its close contract.
func (factory *SessionSourceFactory) revokeSource() error {
	if factory == nil {
		return nil
	}
	factory.mu.Lock()
	source := factory.source
	publisher := factory.publisher
	phase := factory.phase
	factory.source = nil
	factory.publisher = nil
	factory.phase = nil
	factory.state = sourceClosed
	if factory.cancel != nil {
		factory.cancel()
	}
	factory.mu.Unlock()
	return errors.Join(source.Close(), publisher.Close(), phase.Close())
}

// Close revokes the bearer and closes the acquired exchange. The outer
// SyntheticRuntime owner calls it after the foreground runtime has closed its
// session and released the local lease; the session-facing wrapper only
// revokes authority so a stalled delegate Close cannot retain that lease.
func (factory *SessionSourceFactory) Close() error {
	err := factory.revokeSource()
	if factory == nil {
		return err
	}
	factory.mu.Lock()
	port := factory.port
	factory.port = nil
	factory.mu.Unlock()
	if closer, ok := port.(interface{ Close() error }); ok {
		if closer.Close() != nil {
			err = errors.Join(err, ErrUnavailable)
		}
	}
	return err
}

type sourceClosingExchange struct {
	delegate attachedworkersession.ExchangePort
	source   *SessionSourceFactory
}

func (exchange *sourceClosingExchange) Exchange(ctx context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	if exchange == nil || exchange.delegate == nil {
		return nil, ErrUnavailable
	}
	exchange.source.mu.Lock()
	open := exchange.source.state == sourceOpen
	exchange.source.mu.Unlock()
	if !open {
		return nil, ErrUnavailable
	}
	return exchange.delegate.Exchange(ctx, batch)
}

func (exchange *sourceClosingExchange) Close() error {
	if exchange == nil {
		return nil
	}
	return exchange.source.revokeSource()
}

var _ attachedworkersession.ExchangeFactory = (*SessionSourceFactory)(nil)
var _ attachedworkerdaemontransport.SealedInputSource = (*SessionSourceFactory)(nil)
var _ attachedworkerdaemontransport.ReceiptPublisher = (*SessionSourceFactory)(nil)
