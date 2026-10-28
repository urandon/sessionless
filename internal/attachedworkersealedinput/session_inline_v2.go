package attachedworkersealedinput

import (
	"context"
	"io"
	"net/http"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
)

// InlineSessionSourceFactoryV2 explicitly composes input, V1 failure receipts
// and inline output for ONE original connection. Its base is private, not an
// exported embedding or bearer accessor. Ordinary SessionSourceFactory does
// not implement InlineReceiptPublisherV2. This constructor activates no worker.
type InlineSessionSourceFactoryV2 struct {
	base *SessionSourceFactory
}

func NewInlineSessionSourceFactoryV2(origin string, client *http.Client, delegate attachedworkersession.ExchangeFactory) (*InlineSessionSourceFactoryV2, error) {
	// InlineClientV2 is the canonical strict HTTPS-origin validator. Its probe
	// requires a printable synthetic bearer; it never performs HTTP.
	probe, err := attachedworkerreceipt.NewInlineClientV2(origin, client, []byte("inline-validation-probe"))
	if err != nil {
		return nil, ErrInvalid
	}
	_ = probe.Close()
	base, err := NewReceiptSessionSourceFactory(origin+PathV1, origin+attachedworkerreceipt.PathV1, client, delegate)
	if err != nil {
		return nil, err
	}
	base.inlineOrigin = origin
	return &InlineSessionSourceFactoryV2{base: base}, nil
}

func (v *InlineSessionSourceFactoryV2) sourceFactory() *SessionSourceFactory {
	if v == nil {
		return nil
	}
	return v.base
}

func (v *InlineSessionSourceFactoryV2) Open(binding attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
	return v.sourceFactory().Open(binding, bearer)
}

func (v *InlineSessionSourceFactoryV2) Load(ctx context.Context, input attachedworkerdaemontransport.MaterializationRequestV1) (attachedworkerdaemontransport.SealedInputV1, error) {
	return v.sourceFactory().Load(ctx, input)
}

func (v *InlineSessionSourceFactoryV2) Publish(ctx context.Context, input attachedworkerdaemontransport.ReceiptSubmissionV1) (attachedworkerdaemontransport.ReceiptCommitmentV1, error) {
	return v.sourceFactory().Publish(ctx, input)
}

func (v *InlineSessionSourceFactoryV2) RetireConnectionSourcesV1() error {
	return v.sourceFactory().RetireConnectionSourcesV1()
}

func (v *InlineSessionSourceFactoryV2) Close() error { return v.sourceFactory().Close() }

// The inline adapter/journal, not this factory, owns provenance, burn records
// and the original shared output budget. Forward unchanged once; never retry,
// renew an expiry, reopen/close a borrowed body or infer READY.
func (v *InlineSessionSourceFactoryV2) Prepare(ctx context.Context, input attachedworkeroutput.AttachedWorkerInlinePrepareV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	client, call, release, err := v.inlineCall(ctx)
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	defer release()
	return client.Prepare(call, input)
}

func (v *InlineSessionSourceFactoryV2) Submit(ctx context.Context, input attachedworkeroutput.AttachedWorkerInlineBodyV2, length int64, body io.Reader) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	client, call, release, err := v.inlineCall(ctx)
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	defer release()
	return client.Submit(call, input, length, body)
}

func (v *InlineSessionSourceFactoryV2) Reconcile(ctx context.Context, input attachedworkeroutput.AttachedWorkerInlineReconcileV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	client, call, release, err := v.inlineCall(ctx)
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	defer release()
	return client.Reconcile(call, input)
}

func (v *InlineSessionSourceFactoryV2) inlineCall(ctx context.Context) (*attachedworkerreceipt.InlineClientV2, context.Context, func(), error) {
	base := v.sourceFactory()
	if base == nil || ctx == nil || ctx.Err() != nil {
		return nil, nil, nil, ErrInvalid
	}
	base.mu.Lock()
	client, life, open := base.inline, base.life, base.state == sourceOpen
	base.mu.Unlock()
	if !open || client == nil || life == nil {
		return nil, nil, nil, ErrUnavailable
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(life, cancel)
	release := func() { stop(); cancel() }
	if life.Err() != nil {
		release()
		return nil, nil, nil, ErrUnavailable
	}
	return client, call, release, nil
}

var _ attachedworkersession.ExchangeFactory = (*InlineSessionSourceFactoryV2)(nil)
var _ attachedworkerdaemontransport.SealedInputSource = (*InlineSessionSourceFactoryV2)(nil)
var _ attachedworkerdaemontransport.ReceiptPublisher = (*InlineSessionSourceFactoryV2)(nil)
var _ attachedworkerdaemontransport.InlineReceiptPublisherV2 = (*InlineSessionSourceFactoryV2)(nil)
