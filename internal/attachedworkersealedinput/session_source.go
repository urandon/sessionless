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

	mu        sync.Mutex
	state     sourceState
	source    *ClientSource
	publisher *attachedworkerreceipt.ClientPublisher
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
	exchange, err := factory.delegate.Open(binding, bearer)
	if err != nil || exchange == nil {
		_ = source.Close()
		_ = publisher.Close()
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
		if closer, ok := exchange.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return nil, ErrUnavailable
	}
	factory.source = source
	factory.publisher = publisher
	factory.port = exchange
	factory.state = sourceOpen
	factory.mu.Unlock()
	return &sourceClosingExchange{delegate: exchange, source: factory}, nil
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
	factory.source = nil
	factory.publisher = nil
	factory.state = sourceClosed
	if factory.cancel != nil {
		factory.cancel()
	}
	factory.mu.Unlock()
	return errors.Join(source.Close(), publisher.Close())
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
