// Package attachedworkerreceipt exposes the server-owned output receipt at a
// narrow authenticated boundary. Neither the caller nor HTTP chooses a
// canonical digest, event identity, or terminal materialization.
package attachedworkerreceipt

import (
	"context"
	"errors"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
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
	opsSignals opssignal.Sink
	binder     BearerBinder
	store      ReceiptStore
	blobs      ports.BlobStore
}

func NewService(binder BearerBinder, store ReceiptStore, blobs ports.BlobStore) (*Service, error) {
	return NewServiceWithOpsSignals(binder, store, blobs, nil)
}

// NewServiceWithOpsSignals preserves the original publication boundary; a nil
// sink is a no-op and an observation failure never changes the receipt result.
func NewServiceWithOpsSignals(binder BearerBinder, store ReceiptStore, blobs ports.BlobStore, signals opssignal.Sink) (*Service, error) {
	if binder == nil || store == nil || blobs == nil {
		return nil, ErrInvalid
	}
	return &Service{binder: binder, store: store, blobs: blobs, opsSignals: signals}, nil
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
		var validation domain.ValidationError
		var validationPointer *domain.ValidationError
		kind, classified := domain.ClassifyError(err)
		if !errors.Is(err, context.Canceled) && !errors.Is(err, domain.ErrMembershipDenied) && !errors.As(err, &validation) && !errors.As(err, &validationPointer) && !errors.Is(err, attachedworkeroutput.ErrCandidateInvalid) &&
			!errors.Is(err, ydbstore.ErrAttachedWorkerReceiptCopyInProgress) &&
			!errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) &&
			!(classified && (kind == domain.ErrorPolicyDenied || kind == domain.ErrorQuotaBlocked || kind == domain.ErrorReauthRequired)) {
			opssignal.Emit(ctx, service.opsSignals, opssignal.AttachedPublicationFailure, opssignal.Publication, opssignal.Unexpected)
		}
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
