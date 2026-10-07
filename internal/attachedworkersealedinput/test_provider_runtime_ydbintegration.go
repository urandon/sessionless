//go:build ydbintegration

package attachedworkersealedinput

import (
	"context"
	"errors"
	"sync"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// ConnectTestProviderPinnedRuntime exists only in the YDB integration binary.
// The shipped ConnectSyntheticPinnedRuntime always uses DeniedCredentialLifecycle
// and cannot be converted to this path by a profile or network frame.
func ConnectTestProviderPinnedRuntime(ctx context.Context, config SyntheticRuntimeConfig,
	credentials ports.CredentialLifecycle, candidate attachedworkerdaemontransport.ReceiptCandidateBuilder,
	receiptEndpoint string,
) (*SyntheticRuntime, error) {
	if ctx == nil || ctx.Err() != nil || config.Store == nil || config.Bootstrap == nil ||
		credentials == nil || candidate == nil || receiptEndpoint == "" {
		return nil, ErrInvalid
	}
	base, err := NewSessionSourceFactory(config.SealedEndpoint, config.SealedClient, config.Exchange)
	if err != nil {
		return nil, err
	}
	source := &testProviderSource{SessionSourceFactory: base, receiptEndpoint: receiptEndpoint}
	materializer, err := attachedworkerdaemontransport.NewTestProviderMaterializer(source, config.MaxInputBytes, config.Now)
	if err != nil {
		_ = source.Close()
		return nil, err
	}
	config.Adapter.ReceiptPublisher = source
	config.Adapter.ReceiptCandidate = candidate
	runtime, err := attachedworkerdaemontransport.ConnectPinnedForegroundRuntime(
		ctx, config.Store, config.Bootstrap, source, config.Session, config.Connect,
		materializer, config.Adapter, config.Poll, config.Stack, credentials, config.Runtime,
	)
	if err != nil || runtime == nil {
		_ = source.Close()
		return nil, errors.Join(ErrUnavailable, err)
	}
	return &SyntheticRuntime{runtime: runtime, source: source, done: make(chan struct{})}, nil
}

type testProviderSource struct {
	*SessionSourceFactory
	receiptEndpoint string
	mu              sync.Mutex
	publisher       *attachedworkerreceipt.ClientPublisher
}

func (source *testProviderSource) Open(binding attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
	exchange, err := source.SessionSourceFactory.Open(binding, bearer)
	if err != nil {
		return nil, err
	}
	publisher, err := attachedworkerreceipt.NewClientPublisher(source.receiptEndpoint, source.client, bearer)
	if err != nil {
		_ = source.Close()
		return nil, err
	}
	source.mu.Lock()
	source.publisher = publisher
	source.mu.Unlock()
	return &testProviderClosingExchange{ExchangePort: exchange, source: source}, nil
}

// Closing a session must revoke the bearer held by both HTTPS clients, not
// merely the sealed-input client wrapped by SessionSourceFactory.Open.
type testProviderClosingExchange struct {
	attachedworkersession.ExchangePort
	source *testProviderSource
}

func (exchange *testProviderClosingExchange) Close() error {
	if exchange == nil {
		return nil
	}
	return exchange.source.revokeSource()
}

func (source *testProviderSource) Publish(ctx context.Context, submission attachedworkerdaemontransport.ReceiptSubmissionV1) (attachedworkerdaemontransport.ReceiptCommitmentV1, error) {
	source.mu.Lock()
	publisher := source.publisher
	source.mu.Unlock()
	if publisher == nil {
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrUnavailable
	}
	return publisher.Publish(ctx, submission)
}

func (source *testProviderSource) revokeSource() error {
	source.mu.Lock()
	publisher := source.publisher
	source.publisher = nil
	source.mu.Unlock()
	var err error
	if publisher != nil {
		err = publisher.Close()
	}
	return errors.Join(err, source.SessionSourceFactory.revokeSource())
}

func (source *testProviderSource) Close() error {
	return errors.Join(source.revokeSource(), source.SessionSourceFactory.Close())
}

var _ attachedworkersession.ExchangeFactory = (*testProviderSource)(nil)
var _ attachedworkerdaemontransport.SealedInputSource = (*testProviderSource)(nil)
var _ attachedworkerdaemontransport.ReceiptPublisher = (*testProviderSource)(nil)
var _ runtimeSource = (*testProviderSource)(nil)
