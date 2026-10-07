// Package attachedworkerreceipt exposes the server-owned output receipt at a
// narrow authenticated boundary. Neither the caller nor HTTP chooses a
// canonical digest, event identity, or terminal materialization.
package attachedworkerreceipt

import (
	"context"
	"errors"

	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

var (
	ErrInvalid      = errors.New("attached-worker receipt request is invalid")
	ErrUnauthorized = errors.New("attached-worker receipt request is unauthorized")
	ErrConflict     = errors.New("attached-worker receipt conflicts with authoritative state")
	ErrUnavailable  = errors.New("attached-worker receipt service is unavailable")
)

// BearerBinder authenticates the connection and derives its secret digest.
// This is only binding, not write authority: ReceiptStore rechecks all mutable
// owner, attempt, cancellation, lease and fence state in one YDB transaction.
type BearerBinder interface {
	BindOutputReceiptBearer([]byte, ports.AttachedWorkerSealedInputAuthorization) (ports.AttachedWorkerSealedInputAuthorization, error)
}

type ReceiptStore interface {
	CreateAttachedWorkerOutputReceipt(context.Context, ports.BlobStore, ydbstore.AttachedWorkerOutputReceiptRequest) (ydbstore.AttachedWorkerOutputReceiptResult, error)
}

type Service struct {
	binder BearerBinder
	store  ReceiptStore
	blobs  ports.BlobStore
}

func NewService(binder BearerBinder, store ReceiptStore, blobs ports.BlobStore) (*Service, error) {
	if binder == nil || store == nil || blobs == nil {
		return nil, ErrInvalid
	}
	return &Service{binder: binder, store: store, blobs: blobs}, nil
}

func (service *Service) Create(ctx context.Context, bearer []byte, request ydbstore.AttachedWorkerOutputReceiptRequest) (ydbstore.AttachedWorkerOutputReceiptResult, error) {
	if service == nil || ctx == nil || ctx.Err() != nil || len(bearer) == 0 {
		return ydbstore.AttachedWorkerOutputReceiptResult{}, ErrUnauthorized
	}
	bound, err := service.binder.BindOutputReceiptBearer(bearer, request.Authorization)
	if err != nil {
		return ydbstore.AttachedWorkerOutputReceiptResult{}, ErrUnauthorized
	}
	request.Authorization = bound
	result, err := service.store.CreateAttachedWorkerOutputReceipt(ctx, service.blobs, request)
	if err != nil {
		return ydbstore.AttachedWorkerOutputReceiptResult{}, ErrUnavailable
	}
	switch result.Status {
	case ports.AttachedWorkerExecutionApplied, ports.AttachedWorkerExecutionReplayed:
		return result, nil
	case ports.AttachedWorkerExecutionConflict:
		return ydbstore.AttachedWorkerOutputReceiptResult{}, ErrConflict
	default:
		return ydbstore.AttachedWorkerOutputReceiptResult{}, ErrUnauthorized
	}
}
