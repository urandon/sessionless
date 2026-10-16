package pisdkmedia

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"

	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
)

// HTTPInitPlanV1 is separate SPU3 preparation DATA, not a SPU2 alias/profile
// grant. Copies share the same emission/retirement state and original debit.
// The original owner must retain the borrowed body until its actual consumer
// and entire Materialize callback/process join before Close, just as for Init.
type HTTPInitPlanV1 struct{ state *httpInitState }
type httpInitState struct {
	mu       sync.Mutex
	base     *InitPlanV1
	ledger   *codexexec.CanonicalMediaLedgerV1
	metadata uint64
	closed   bool
	closeErr error
	emitted  atomic.Bool
}

func (HTTPInitPlanV1) String() string     { return "HTTPInitPlanV1{data:[redacted]}" }
func (p HTTPInitPlanV1) GoString() string { return p.String() }

// PrepareHTTPInitV1 uses the SAME complete canonical projection/file/ledger
// before Issue. It does not install roots, qualify native allocation, select a
// process, construct an SDK, authorize Source or own a credential lifecycle.
func PrepareHTTPInitV1(ctx context.Context, p *RequestProjectionV1, model []byte, timeout uint64, file *providercredential.PreparedImmutableAPIFileV1) (*HTTPInitPlanV1, error) {
	if ctx == nil || ctx.Err() != nil || p == nil || p.ctx == nil || p.ctx.Err() != nil || p.ledger == nil || file == nil {
		return nil, ErrProjection
	}
	// Fund the new state and bounded original-binding/profile comparisons
	// BEFORE the shared emitter's validators. Not a runtime/native heap proof.
	before := p.ledger.Accounting()
	metadata := uint64(reflect.TypeOf(HTTPInitPlanV1{}).Size() + reflect.TypeOf(httpInitState{}).Size() + reflect.TypeOf(InitPlanV1{}).Size())
	if p.ledger.Reserve(8192+metadata, metadata, 3) != nil {
		return nil, ErrProjection
	}
	comparisonBytes, err := httpInitComparisonBytes(p)
	if err != nil || p.ledger.Reserve(4*comparisonBytes+16*domain.AttachedPiSDKCanonicalMaxBytesV1, 0, 0) != nil {
		_ = p.ledger.EndLive(metadata)
		return nil, ErrProjection
	}
	base, err := prepareInit(ctx, p, model, timeout, file, true)
	if err != nil {
		_ = p.ledger.EndLive(metadata)
		return nil, err
	}
	base.accounting.Before = before
	return &HTTPInitPlanV1{state: &httpInitState{base: base, ledger: p.ledger, metadata: metadata}}, nil
}

// Fixed-schema metadata walk (funded above) reads ONLY string lengths, never
// payload bytes. Go equality compares pointer identity, not pointee graphs.
// Thus later original-binding/scope equality has an explicit byte budget.
func httpInitComparisonBytes(p *RequestProjectionV1) (uint64, error) {
	var total uint64
	var visit func(reflect.Value) bool
	visit = func(v reflect.Value) bool {
		if v.Kind() == reflect.String {
			n := uint64(v.Len())
			if n > (1<<20)-total {
				return false
			}
			total += n
			return true
		}
		if v.Kind() == reflect.Struct {
			for i := 0; i < v.NumField(); i++ {
				if !visit(v.Field(i)) {
					return false
				}
			}
		}
		return true
	}
	for _, v := range []reflect.Value{reflect.ValueOf(p.scope), reflect.ValueOf(p.binding), reflect.ValueOf(p.config.profile)} {
		if !visit(v) {
			return 0, ErrProjection
		}
	}
	return total, nil
}

func httpInitWireLimits(l domain.AttachedPiSDKNativeLimitsV1) (pisdkhttpwire.Limits, error) {
	if l.ProcessFrameBytes < 114 || l.ProcessFrameBytes > 1<<20 || l.RequestHeaderBytes > l.ProcessFrameBytes-85 {
		return pisdkhttpwire.Limits{}, ErrProjection
	}
	w := pisdkhttpwire.Limits{FrameBytes: l.ProcessFrameBytes, InitializeBytes: l.ProcessFrameBytes - 113,
		RequestBytes: min(l.NativeRequestBytes, l.ProcessFrameBytes-85-l.RequestHeaderBytes), FinalBytes: min(l.RawFinalBytes, l.StructuredFinalBytes, l.ProcessFrameBytes-17),
		RequestHeaderBytes: l.RequestHeaderBytes, ResponseHeaderBytes: l.ResponseHeaderBytes, ResponseBytes: l.ResponseBodyBytes, ResponseChunkBytes: l.ResponseChunkBytes,
		ParentBytes: 4 << 20, ChildBytes: l.ProcessStdoutBytes, Frames: l.ProcessEvents}
	if _, err := pisdkhttpwire.Measure(w); err != nil {
		return pisdkhttpwire.Limits{}, ErrProjection
	}
	return w, nil
}

// Conservative bounded logical payload schedule for http_stdio's fixed arenas,
// byte appearances and N+2 notices. Promise/closure/V8/SDK object byte costs remain
// unqualified; this is not a complete platform/native-fit certificate.
func httpInitTransportPartition(l domain.AttachedPiSDKNativeLimitsV1, a pisdkhttpwire.Accounting) (work, live, nodes uint64) {
	q := min(l.NativeRequestBytes, l.ProcessFrameBytes-85-l.RequestHeaderBytes)
	final := min(l.RawFinalBytes, l.StructuredFinalBytes, l.ProcessFrameBytes-17)
	// Canonical auth.headers coexists with requestBytes and outMetadata's Hq.
	// Keep Final's L bytes separately funded: valid profiles may have Q < L.
	// The extra 4096 covers the bounded 2048-byte endpoint, POST/length words,
	// authentication MAC and SHA digests. Key/Bearer stays in bootstrapLive.
	// Do not reuse unrelated Init decoder allowance to pay transport buffers.
	work = 8*(q+2*l.RequestHeaderBytes) + 8*(l.ResponseBodyBytes+l.ResponseHeaderBytes+final) + 4*(l.ProcessFrameBytes-113) + 1024*a.Frames + 81920
	live = q + (l.ProcessFrameBytes - 113) + l.ResponseBodyBytes + 2*l.RequestHeaderBytes + l.ResponseHeaderBytes + final + 17*2 + 96*2 + 68 + 64 + 4096
	nodes = 4*(a.Chunks+2) + 32
	return
}

func (p *HTTPInitPlanV1) EntryArguments() [17]string {
	if p == nil || p.state == nil {
		return [17]string{}
	}
	s := p.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.base == nil || s.base.ctx.Err() != nil {
		return [17]string{}
	}
	return s.base.httpArguments
}
func (p *HTTPInitPlanV1) DecodeLimits() InitDecodeLimitsV1 {
	if p == nil || p.state == nil {
		return InitDecodeLimitsV1{}
	}
	s := p.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.base == nil {
		return InitDecodeLimitsV1{}
	}
	return s.base.decodeLimits
}
func (p *HTTPInitPlanV1) Accounting() InitAccountingV1 {
	if p == nil || p.state == nil {
		return InitAccountingV1{}
	}
	s := p.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.base == nil {
		return InitAccountingV1{}
	}
	return s.base.accounting
}
func (p *HTTPInitPlanV1) MaterializedBody(ctx context.Context, h ports.ProviderInvocationCredentialV1, m ports.ProviderCredentialMaterializationV1) ([]byte, error) {
	if p == nil || p.state == nil {
		return nil, ErrProjection
	}
	s := p.state
	if !s.emitted.CompareAndSwap(false, true) {
		return nil, ErrProjection
	}
	if !s.mu.TryLock() {
		return nil, ErrProjection
	}
	defer s.mu.Unlock()
	if s.closed || s.base == nil {
		return nil, ErrProjection
	}
	return s.base.materializedBody(ctx, h, m)
}
func (p *HTTPInitPlanV1) Close() error {
	if p == nil || p.state == nil {
		return ErrProjection
	}
	s := p.state
	if !s.mu.TryLock() {
		return ErrProjection
	}
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	base, metadata := s.base, s.metadata
	s.base = nil
	s.metadata = 0
	s.closed = true
	// Terminal BEFORE touching accounting: no copied wrapper may retry a
	// failed debit against a later owner's live reservation. On base failure
	// retain metadata conservatively instead of attempting another refund.
	s.closeErr = base.clear()
	if s.closeErr == nil {
		s.closeErr = s.ledger.EndLive(metadata)
	}
	return s.closeErr
}
