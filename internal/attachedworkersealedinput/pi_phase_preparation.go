package attachedworkersealedinput

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"sync"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
)

const piPhaseBindingBytes = 1 << 20 // original inherited complete sealed-input cap

// piPreparedPhases is private, prepaid metadata transport custody. It neither
// proves native/model fit nor makes a phase response into permission. In
// particular the incomplete PiPreparedOwner still never calls request.
type piPreparedPhases struct {
	mu      sync.Mutex // one nonqueueing request borrower; clear cancels then joins
	factory *SessionSourceFactory
	client  *PiAPIClient
	input   *attachedworkerdaemontransport.PiInputAcquisitionV1
	ledger  *codexexec.CanonicalMediaLedgerV1
	scope   pisdkphasewire.Request // W and phase supplied only by original runner
	ctx     context.Context
	cancel  context.CancelFunc
	live    uint64
	next    uint8
	failed  bool
	closed  bool
}

func (*piPreparedPhases) String() string     { return "piPreparedPhases{private:[redacted]}" }
func (p *piPreparedPhases) GoString() string { return p.String() }

// This schedule funds ALL THREE application-level appearances before any
// encode/bearer copy/read/decode, not a new per-RPC pool. Q/R are codec maxima;
// R+1 includes the overflow sentinel and another R retains decoded scalar
// strings, including all three returned replies until original retirement.
// B includes []byte clone, conversion string and Authorization concatenation;
// H covers the fixed application headers, E the endpoint/URL projection.
// Metadata is retained Go DATA layout only, not allocator size or TLS/http heap
// fit. Internal net/http/TLS buffers and native/platform qualification remain
// explicitly unproven; none of these debits supplies that missing producer.
func piPhaseReservation(bearerBytes, endpointBytes uint64) (work, live, nodes uint64) {
	q, r := uint64(pisdkphasewire.RequestMaxBytes), uint64(pisdkphasewire.ReplyMaxBytes)
	h := uint64(len("AuthorizationBearer Content-Typeapplication/jsonAcceptapplication/jsonAccept-Encodingidentity"))
	metadata := uint64(reflect.TypeOf(http.Request{}).Size() + reflect.TypeOf(url.URL{}).Size() + reflect.TypeOf(pisdkphasewire.Reply{}).Size())
	perLive := q + (r + 1) + r + 3*bearerBytes + h + endpointBytes + metadata + piPhaseBindingBytes
	// Two request encoder passes; bounded authorization/fence validation and
	// comparison passes; read + two reply parser passes, reply validation and
	// full original tuple/window comparisons. Reserve eight whole Q and twelve
	// whole R walks conservatively, not bytes-per-object or token coefficients.
	// The original runner also hashes its already admitted single-turn binding
	// before each phase. Its JSON/hash scratch is a subset of the full original
	// 1MiB seal, NOT merely Q bytes of phase metadata.
	perWork := 8*q + 12*r + 4*(3*bearerBytes+h+endpointBytes) + metadata + 8*piPhaseBindingBytes
	return 3*perWork + uint64(reflect.TypeOf(piPreparedPhases{}).Size()),
		3*perLive + uint64(reflect.TypeOf(piPreparedPhases{}).Size()),
		3 * (pisdkphasewire.RequestNodes + pisdkphasewire.AppliedReplyNodes + piPhaseBindingBytes)
}

// preparePiPhases requires the concrete original acquisition/factory/client;
// there is no optional nil-plan fallback in the prepared path. The immutable
// scope has already passed the original canonical/compiler validation. Only
// the small fixed codec metadata is validated here, after prepayment.
func (f *SessionSourceFactory) preparePiPhases(a *attachedworkerdaemontransport.PiInputAcquisitionV1, scope pisdkphasewire.Request) (*piPreparedPhases, error) {
	if f == nil || a == nil || !f.OwnsOriginalPiInput(a) {
		return nil, ErrInvalid
	}
	ledger, ok := a.OriginalLedger().(*codexexec.CanonicalMediaLedgerV1)
	original := a.OriginalContext()
	if !ok || ledger == nil || original == nil || original.Err() != nil {
		return nil, ErrInvalid
	}
	if _, ok := original.Deadline(); !ok {
		return nil, ErrInvalid
	}
	f.mu.Lock()
	client := f.phase
	open := f.state == sourceOpen
	f.mu.Unlock()
	if !open || client == nil {
		return nil, ErrUnavailable
	}
	client.mu.Lock()
	b, e := len(client.bearer), len(client.endpoint)
	valid := !client.closed && piScopeMatches(client.binding, scope.Authorization)
	client.mu.Unlock()
	if !valid || b == 0 || b > attachedworkertransport.ConnectionBearerMaxBytes || e == 0 || e > 2048 {
		return nil, ErrInvalid
	}
	work, live, nodes := piPhaseReservation(uint64(b), uint64(e))
	if ledger.Reserve(work, live, nodes) != nil {
		return nil, ErrUnavailable
	}
	keep := false
	defer func() {
		if !keep {
			_ = ledger.EndLive(live)
		}
	}()
	// One prepaid validation encoding; its Q arena is included as an additional
	// temporary cohort below, rather than borrowing any phase's later request.
	if ledger.Reserve(8*pisdkphasewire.RequestMaxBytes, pisdkphasewire.RequestMaxBytes, pisdkphasewire.RequestNodes) != nil {
		return nil, ErrUnavailable
	}
	data, err := pisdkphasewire.EncodeRequest(scope)
	clear(data)
	_ = ledger.EndLive(pisdkphasewire.RequestMaxBytes)
	if err != nil || !f.OwnsOriginalPiInput(a) {
		return nil, ErrInvalid
	}
	ctx, cancel := context.WithCancel(original)
	p := &piPreparedPhases{factory: f, client: client, input: a, ledger: ledger, scope: scope, ctx: ctx, cancel: cancel, live: live}
	keep = true
	return p, nil
}

func (p *piPreparedPhases) matchesSource() bool {
	if !p.factory.OwnsOriginalPiInput(p.input) {
		return false
	}
	p.factory.mu.Lock()
	defer p.factory.mu.Unlock()
	return p.factory.state == sourceOpen && p.factory.phase == p.client
}

func (p *piPreparedPhases) request(ctx context.Context, request pisdkphasewire.Request) (reply pisdkphasewire.Reply, err error) {
	if p == nil || ctx == nil || !p.mu.TryLock() {
		return reply, ErrUnavailable
	}
	defer p.mu.Unlock()
	phases := [3]pisdkphasewire.Phase{pisdkphasewire.PreIssue, pisdkphasewire.PhysicalSend, pisdkphasewire.ImmutableFinalize}
	if p.closed || p.failed || p.ctx.Err() != nil || ctx.Err() != nil || int(p.next) >= len(phases) {
		return reply, ErrUnavailable
	}
	// The parent still owns W and its original S/F calculation. Bound this
	// HTTP operation by BOTH contexts without minting/refreshing a duration.
	deadline, ok := ctx.Deadline()
	originalDeadline, originalOK := p.ctx.Deadline()
	if !ok || !originalOK || deadline.After(originalDeadline) || request.Version != p.scope.Version || request.Authorization != p.scope.Authorization ||
		request.ExpectedHarnessBindingDigest != p.scope.ExpectedHarnessBindingDigest || request.ExpectedAttachedPiAPIInvocationPolicyDigest != p.scope.ExpectedAttachedPiAPIInvocationPolicyDigest ||
		request.Phase != phases[p.next] || !p.matchesSource() {
		p.failed = true
		return reply, ErrInvalid
	}
	p.next++ // spend before EncodeRequest/Do, never replenish after any error
	requestCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.ctx, cancel)
	defer func() { stop(); cancel() }()
	reply, err = p.client.RequestPiAPIUse(requestCtx, request)
	if err != nil || requestCtx.Err() != nil || !p.matchesSource() {
		p.failed = true
		if err == nil {
			err = ErrUnavailable
		}
		return pisdkphasewire.Reply{}, err
	}
	return reply, nil
}

func (p *piPreparedPhases) clear() {
	if p == nil {
		return
	}
	p.cancel() // revoke immediately, THEN join an actual in-flight HTTP borrower
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.scope = pisdkphasewire.Request{}
	p.input, p.factory, p.client = nil, nil, nil
	// The ORIGINAL composite record retires this prepaid partition together
	// with its frozen stage delta. Never independently refund it while that
	// record or a returned phase reply can still be retained by the runner.
	p.live = 0
}
