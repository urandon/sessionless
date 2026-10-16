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
	if s.stopped.Load() || s.httpsPrepared || s.claimed.Load() || s.callback != 0 || s.auth == nil || s.http == nil || s.responseScan == nil || s.expected == nil || s.file == nil || s.file.ctx.Err() != nil {
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
// Raw/scalar, closed-policy and Pi-assembly checks run while this SAME borrow
// pins the original secret. It cannot be replayed or converted to producer
// custody; the qualified source/SDK/replay consumer is still absent.
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
	if s.stopped.Load() || s.callback != 1 || s.auth == nil || s.file == nil || s.file.ctx.Err() != nil || s.http == nil || s.responseScan == nil || s.expected == nil || s.network.Load() == nil {
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
	if e = s.responseScan.checkResponsePolicy(s.expected); e != nil || ctx.Err() != nil || s.stopped.Load() || use.ctx.Err() != nil || !use.active.Load() || s.file.ctx.Err() != nil {
		return nil, ErrDeliveryMismatch
	}
	if e = s.responseScan.scanPiSemantics(); e != nil || ctx.Err() != nil || s.stopped.Load() || use.ctx.Err() != nil || !use.active.Load() || s.file.ctx.Err() != nil {
		return nil, ErrDeliveryMismatch
	}
	// This remains private DATA, not a serving/profile/custody or replay witness.
	// Real native guard/parser and complete source/SDK composition are required.
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
	b.joinUseStop()
	b.releaseResponse()
}

func (b *piHTTPSResponseBorrow) joinUseStop() {
	if b.stopUse != nil && !b.stopUse() {
		<-b.useDone
	}
}

// Only the private fixed replay driver calls this AFTER a nil, committed End
// write. Normal child exit may already have withdrawn b.ctx (Source/network),
// while the original Supervisor IO/F context still admits Final drain. Do not
// abort that drain or manufacture another permission from this local cleanup.
func (b *piHTTPSResponseBorrow) finishReplay() error {
	if b == nil || !b.closed.CompareAndSwap(false, true) {
		return ErrConsumed
	}
	b.joinUseStop()
	var err error
	if b.s.stopped.Load() || b.use.ctx.Err() != nil || !b.use.active.Load() || b.s.file.ctx.Err() != nil {
		b.s.abort()
		err = ErrConsumed
	}
	b.releaseResponse()
	return err
}

// The caller has consumed the shared closed latch and joined any cancellation
// callback. Response borrows end here; only the original file owner zeroes its
// key, after the entire synchronous Materialize callback/process has joined.
func (b *piHTTPSResponseBorrow) releaseResponse() {
	b.s.http.clear()
	b.s.responseScan.clear()
	if b.s.expected != nil {
		b.s.expected.graph.clear()
	}
	b.view = piUnapprovedHTTPView{}
	b.s.httpBorrow.Store(false)
	b.use.gate <- struct{}{}
	b.s.mu.Unlock()
}
