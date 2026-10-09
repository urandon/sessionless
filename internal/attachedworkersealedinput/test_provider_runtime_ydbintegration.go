//go:build ydbintegration

package attachedworkersealedinput

import (
	"context"
	"errors"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
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
	source, err := NewReceiptSessionSourceFactory(config.SealedEndpoint, receiptEndpoint, config.SealedClient, config.Exchange)
	if err != nil {
		return nil, err
	}
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
