package providercredential

import (
	"context"
	"sync/atomic"

	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
)

// PRIVATE preparation only. There is deliberately no public root-artifact or
// transport override: a source-selected installed policy is still missing in
// the original owner, which must remain default-off/ErrMissingProducer.
func (p *PreparedPiFileUseV1) prepareHTTPS(bundle piHTTPSRootArtifact) error {
	if p == nil || p.state == nil {
		return ErrInvalid
	}
	s := p.state
	if !s.mu.TryLock() {
		s.abort()
		return ErrConsumed
	}
	defer s.mu.Unlock()
	if s.stopped.Load() || s.httpsPrepared || s.claimed.Load() || s.callback != 0 || s.auth == nil || s.http == nil || s.responseScan == nil || s.file == nil || s.file.ctx.Err() != nil {
		s.abort()
		return ErrConsumed
	}
	s.httpsPrepared = true
	f := s.file
	f.service.mu.Lock()
	defer f.service.mu.Unlock()
	if f.service.preparedPiFiles[f.scope] != s {
		s.abort()
		return ErrConsumed
	}
	network, err := newPiHTTPS(s.ledger, s.auth, s.body, f.maxBytes, bundle)
	if network != nil {
		s.live += network.live
		s.network.Store(network)
	}
	if err != nil {
		s.abort()
		return err
	}
	if s.stopped.Load() || f.ctx.Err() != nil {
		s.abort()
		return ErrConsumed
	}
	return nil
}

// A private borrow retains s.mu AND the original use gate after full receive.
// Its DATA has not passed semantic/secret quarantine and cannot be replayed or
// converted to producer custody. The future concrete guard must run here while
// that SAME borrow pins the original secret. No public response is returned.
type piHTTPSResponseBorrow struct {
	s       *preparedPiFileUse
	use     *immutableAPIFileUse
	view    piUnapprovedHTTPView
	ctx     context.Context
	stopUse func() bool
	useDone chan struct{}
	closed  atomic.Bool
}

func (*piHTTPSResponseBorrow) String() string     { return "piHTTPSResponseBorrow{unapproved:[redacted]}" }
func (p *piHTTPSResponseBorrow) GoString() string { return p.String() }

func (s *preparedPiFileUse) beginHTTPS(frame pisdkhttpwire.Frame) (_ *piHTTPSResponseBorrow, err error) {
	if s == nil {
		return nil, ErrInvalid
	}
	if !s.macAttempted.CompareAndSwap(false, true) || !s.mu.TryLock() {
		s.abort()
		return nil, ErrConsumed
	}
	if s.stopped.Load() || s.callback != 1 || s.auth == nil || s.file == nil || s.file.ctx.Err() != nil || s.http == nil || s.responseScan == nil || s.network.Load() == nil {
		s.abort()
		s.mu.Unlock()
		return nil, ErrConsumed
	}
	use := s.use.Load()
	if use == nil {
		s.abort()
		s.mu.Unlock()
		return nil, ErrConsumed
	}
	select {
	case <-use.gate:
	default:
		s.abort()
		s.mu.Unlock()
		return nil, ErrConsumed
	}
	b := &piHTTPSResponseBorrow{s: s, use: use}
	defer func() {
		if err != nil {
			b.close()
		}
	}()
	if err = s.compareRequestMACLocked(use, frame); err != nil {
		return nil, err
	}
	if !s.network.Load().admitsKey(use.secret) {
		return nil, ErrDeliveryMismatch // before original Source or any socket
	}
	send := use.send.Load()
	if send == nil {
		return nil, ErrConsumed
	}
	// use.cancel and Source withdrawal are different contexts. Link cancellation
	// before the potentially held Source RPC and join the callback before unlock.
	b.useDone = make(chan struct{})
	b.stopUse = context.AfterFunc(use.ctx, func() { defer close(b.useDone); send.Revoke() })
	ctx, cutoff, e := send.AuthorizeNetworkUse()
	if e != nil || ctx == nil || ctx.Err() != nil || s.stopped.Load() || use.ctx.Err() != nil || !use.active.Load() {
		return nil, ErrConsumed
	}
	b.ctx = ctx
	view, e := s.network.Load().receive(ctx, cutoff, use.secret, s.http)
	if e != nil || ctx.Err() != nil || s.stopped.Load() || use.ctx.Err() != nil || !use.active.Load() || s.file.ctx.Err() != nil {
		return nil, ErrBackend
	}
	if e = s.responseScan.scan(ctx, view, use.secret); e != nil || ctx.Err() != nil || s.stopped.Load() || use.ctx.Err() != nil || !use.active.Load() || s.file.ctx.Err() != nil {
		return nil, ErrDeliveryMismatch
	}
	if e = s.responseScan.scanPiSemantics(); e != nil || ctx.Err() != nil || s.stopped.Load() || use.ctx.Err() != nil || !use.active.Load() || s.file.ctx.Err() != nil {
		return nil, ErrDeliveryMismatch
	}
	// Grammar/raw/scalar/assembly checks are necessary but NOT closed effect,
	// request-echo, identity or terminal approval. Borrow remains UNAPPROVED DATA.
	b.view = view
	s.httpBorrow.Store(true)
	return b, nil
}

// close is withdrawal/zero/join, never approval. It retains immutable owner
// pointers so a losing concurrent/late close can reject without a data race.
func (b *piHTTPSResponseBorrow) close() {
	if b == nil || !b.closed.CompareAndSwap(false, true) {
		return
	}
	b.s.abort() // revoke Source and close socket before any gate/alias release
	if b.stopUse != nil && !b.stopUse() {
		<-b.useDone
	}
	b.s.http.clear()
	b.s.responseScan.clear()
	b.view = piUnapprovedHTTPView{}
	b.s.httpBorrow.Store(false)
	b.use.gate <- struct{}{}
	b.s.mu.Unlock()
}
