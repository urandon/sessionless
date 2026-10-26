package attachedworkerreceipt

import (
	"context"
	"errors"
	"io"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// InlineStoreV2 owns transactional eligibility, the durable one-shot body
// claim, and canonical write custody. In particular, Submit must claim before
// its first body Read. Binding a bearer here does not grant write authority.
type InlineStoreV2 interface {
	PrepareAttachedWorkerInlineOutput(context.Context, ports.BlobStore, attachedworkeroutput.AttachedWorkerInlinePrepareV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error)
	SubmitAttachedWorkerInlineOutput(context.Context, ports.BlobStore, attachedworkeroutput.AttachedWorkerInlineBodyV2, int64, io.Reader) (attachedworkeroutput.AttachedWorkerInlineResultV2, error)
	ReconcileAttachedWorkerInlineOutput(context.Context, attachedworkeroutput.AttachedWorkerInlineReconcileV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error)
}

type InlineServiceV2 struct {
	binder BearerBinder
	store  InlineStoreV2
	blobs  ports.BlobStore
}

func NewInlineServiceV2(binder BearerBinder, store InlineStoreV2, blobs ports.BlobStore) (*InlineServiceV2, error) {
	if binder == nil || store == nil || blobs == nil {
		return nil, ErrInvalid
	}
	return &InlineServiceV2{binder: binder, store: store, blobs: blobs}, nil
}

func (s *InlineServiceV2) bind(ctx context.Context, bearer []byte, version uint32, auth ports.AttachedWorkerSealedInputAuthorization) (ports.AttachedWorkerSealedInputAuthorization, error) {
	if version != 2 || auth.PresentedSecretDigest != "" {
		return ports.AttachedWorkerSealedInputAuthorization{}, ErrInvalid
	}
	if s == nil || ctx == nil || ctx.Err() != nil || len(bearer) == 0 || len(bearer) > maxInlineHeaderBytesV2 {
		return ports.AttachedWorkerSealedInputAuthorization{}, ErrUnauthorized
	}
	bound, err := s.binder.BindOutputReceiptBearer(bearer, auth)
	if err != nil {
		return ports.AttachedWorkerSealedInputAuthorization{}, ErrUnauthorized
	}
	return bound, nil
}

func (s *InlineServiceV2) Prepare(ctx context.Context, bearer []byte, request attachedworkeroutput.AttachedWorkerInlinePrepareV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	bound, err := s.bind(ctx, bearer, request.Version, request.Authorization)
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	request.Authorization = bound
	result, err := s.store.PrepareAttachedWorkerInlineOutput(ctx, s.blobs, request)
	return inlineResultV2(result, err)
}

func (s *InlineServiceV2) Submit(ctx context.Context, bearer []byte, request attachedworkeroutput.AttachedWorkerInlineBodyV2, length int64, body io.Reader) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	if length <= 0 || length > maxInlineBodyBytesV2 || body == nil || request.ExpiresAt.IsZero() {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrInvalid
	}
	bound, err := s.bind(ctx, bearer, request.Version, request.Authorization)
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	request.Authorization = bound
	// No body peek, buffer or read occurs before the store's durable claim.
	result, err := s.store.SubmitAttachedWorkerInlineOutput(ctx, s.blobs, request, length, body)
	if err == nil && result.Capability != "" {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrUnavailable
	}
	return inlineResultV2(result, err)
}

func (s *InlineServiceV2) Reconcile(ctx context.Context, bearer []byte, request attachedworkeroutput.AttachedWorkerInlineReconcileV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	bound, err := s.bind(ctx, bearer, request.Version, request.Authorization)
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	request.Authorization = bound
	result, err := s.store.ReconcileAttachedWorkerInlineOutput(ctx, request)
	if err == nil && request.BodySendBurned && result.Capability != "" {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrUnavailable
	}
	return inlineResultV2(result, err)
}

func inlineResultV2(result attachedworkeroutput.AttachedWorkerInlineResultV2, err error) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	if err == nil {
		return result, nil
	}
	// Never return provider text, request content, or credentials across this
	// boundary. Membership is rechecked by the store, not inferred from binding.
	if errors.Is(err, domain.ErrMembershipDenied) {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrUnauthorized
	}
	if errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) || errors.Is(err, ydbstore.ErrAttachedWorkerReceiptCopyInProgress) {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrConflict
	}
	kind, classified := domain.ClassifyError(err)
	if classified && (kind == domain.ErrorPolicyDenied || kind == domain.ErrorQuotaBlocked || kind == domain.ErrorReauthRequired) {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrUnauthorized
	}
	return attachedworkeroutput.AttachedWorkerInlineResultV2{}, ErrUnavailable
}
