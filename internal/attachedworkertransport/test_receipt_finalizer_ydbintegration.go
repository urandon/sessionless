//go:build ydbintegration

package attachedworkertransport

import "gitcode.com/urandon/sessionless/internal/ports"

// NewTestReceiptFinalizingService enables automatic terminal commitment only
// in the YDB integration binary, whose joined gate offers receipt-capable
// attempts exclusively. The production transport cannot enable this path.
func NewTestReceiptFinalizingService(config ServiceConfig, store ports.AttachedWorkerTransportStore,
	broker AttemptBroker,
) (*Service, error) {
	finalizer, ok := broker.(receiptTerminalCommitBroker)
	if !ok {
		return nil, ErrTransportConfig
	}
	service, err := NewService(config, store, broker)
	if err != nil {
		return nil, err
	}
	service.receiptFinalizer = finalizer
	return service, nil
}
