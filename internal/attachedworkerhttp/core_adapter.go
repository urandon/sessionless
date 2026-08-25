package attachedworkerhttp

import (
	"context"
	"errors"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
)

var errCoreExchange = errors.New("attached worker core exchange failed")

// CoreExchange is the narrow compile-visible seam exposed by the transport
// core. It deliberately accepts credential bytes rather than HTTP types.
type CoreExchange interface {
	ExchangeBearer(context.Context, []byte, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error)
}

// CoreExchangeAdapter binds the HTTP exchange service to the transport core
// without making the core depend on HTTP concerns.
type CoreExchangeAdapter struct {
	core CoreExchange
}

var _ ExchangeService = (*CoreExchangeAdapter)(nil)

func NewCoreExchangeAdapter(core CoreExchange) (*CoreExchangeAdapter, error) {
	if core == nil {
		return nil, errCoreExchange
	}
	return &CoreExchangeAdapter{core: core}, nil
}

func (adapter *CoreExchangeAdapter) Exchange(
	ctx context.Context,
	token BearerToken,
	batch attachedworkerprotocol.BatchV1,
) (*attachedworkerprotocol.BatchV1, error) {
	if adapter == nil || adapter.core == nil || !token.valid() {
		return nil, ErrUnauthorized
	}
	response, err := adapter.core.ExchangeBearer(ctx, token.Bytes(), batch)
	if err == nil {
		return response, nil
	}
	switch {
	case errors.Is(err, attachedworkertransport.ErrTransportUnauthorized):
		return nil, ErrUnauthorized
	case errors.Is(err, attachedworkertransport.ErrTransportConflict):
		return nil, ErrConflict
	case errors.Is(err, attachedworkertransport.ErrTransportBackend):
		return nil, ErrUnavailable
	default:
		return nil, errCoreExchange
	}
}
