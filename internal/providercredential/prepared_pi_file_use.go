package providercredential

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// PreparedPiFileUseV1 is a prospectively funded recipe belonging to one exact
// prepared immutable-file owner. No method exposes a key, Source authorization,
// writer, endpoint, socket, response or successful-use witness. This connects
// custody only; fixed HTTPS/HMAC/quarantine and native fit are still required.
// The nonce is comparison DATA. Copies share the private lifetime state.
type PreparedPiFileUseV1 struct{ state *preparedPiFileUse }

type preparedPiFileUse struct {
	mu           sync.Mutex
	file         *PreparedImmutableAPIFileV1
	ledger       attachedworkerdaemon.PreparedAPILedgerV1
	body         []byte
	bodyHash     [sha256.Size]byte
	profile      string
	nonce        [32]byte
	live         uint64
	claimed      atomic.Bool
	stopped      atomic.Bool
	use          atomic.Pointer[immutableAPIFileUse]
	clearErr     error
	callback     uint8 // under mu:0 not started,1 active,2 joined/zeroed/unlocked
	auth         *piRequestAuthentication
	authPrepared bool
	macAttempted atomic.Bool
	http         *piHTTPReceive
	httpPrepared bool
}

func (PreparedPiFileUseV1) String() string     { return "PreparedPiFileUseV1{private:[redacted]}" }
func (p PreparedPiFileUseV1) GoString() string { return p.String() }

// PreparePiFileUseV1 runs before original Issue. It borrows the original body;
// its owner must retain it unchanged until Close joins and drops that alias.
// The Go metadata/file-verification schedule below does not certify complete
// runtime, native heap, HMAC/HTTP/quarantine allocation or model input fit.
func (p *PreparedImmutableAPIFileV1) PreparePiFileUseV1(ledger attachedworkerdaemon.PreparedAPILedgerV1, body []byte, profile string) (*PreparedPiFileUseV1, error) {
	if p == nil || p.ctx == nil || p.ctx.Err() != nil || p.service == nil || p.files == nil ||
		nilPiLedger(ledger) || len(body) == 0 || uint64(len(body)) > pisdkphasewire.TransportMaxFrameBytes ||
		p.maxBytes < 1 || p.maxBytes > 64<<10 || p.pathBytes == 0 || p.pathBytes > pisdkphasewire.TransportMaxFrameBytes ||
		len(profile) != 64 || profile != p.binding.value.Backend.BackendProfileDigest {
		return nil, ErrInvalid
	}
	if !p.piPrepared.CompareAndSwap(false, true) {
		return nil, ErrConsumed
	}
	metadata := uint64(reflect.TypeOf(PreparedPiFileUseV1{}).Size()) + uint64(reflect.TypeOf(preparedPiFileUse{}).Size()) + uint64(reflect.TypeOf(immutableAPIFileUse{}).Size())
	metadata += uint64(reflect.TypeOf(ports.ProviderInvocationCredentialV1{}).Size()) + uint64(reflect.TypeOf((*preparedPiFileUse)(nil)).Size())
	// Original file verifier has one fixed4096-byte scratch buffer. Reserve its
	// payload plus hash scratch conservatively; runtime channel/context/allocator
	// bookkeeping remains part of the still-missing complete qualification.
	live := metadata + 4096 + 256
	work := metadata + 2*uint64(len(body)) + 8*uint64(p.maxBytes) + 4*p.pathBytes + 16384
	if ledger.Reserve(work, live, 3) != nil {
		return nil, ErrInvalid
	}
	s := &preparedPiFileUse{file: p, ledger: ledger, body: body, profile: profile, live: live}
	if _, err := rand.Read(s.nonce[:]); err != nil {
		_ = ledger.EndLive(live)
		return nil, ErrBackend
	}
	s.bodyHash = sha256.Sum256(body)
	p.service.mu.Lock()
	if p.service.preparedPiFiles == nil {
		p.service.preparedPiFiles = make(map[ports.ProviderInvocationCredentialV1]*preparedPiFileUse)
	}
	if p.service.preparedPiFiles[p.scope] != nil {
		p.service.mu.Unlock()
		_ = ledger.EndLive(live)
		return nil, ErrConsumed
	}
	p.service.preparedPiFiles[p.scope] = s
	p.service.mu.Unlock()
	return &PreparedPiFileUseV1{state: s}, nil
}

func nilPiLedger(v attachedworkerdaemon.PreparedAPILedgerV1) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	return r.Kind() != reflect.Pointer || r.IsNil()
}

// Nonce returns nonsecret pre-Issue comparison DATA; it is not a send grant.
func (p *PreparedPiFileUseV1) Nonce() [32]byte {
	if p == nil || p.state == nil {
		return [32]byte{}
	}
	s := p.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped.Load() {
		return [32]byte{}
	}
	return s.nonce
}

// BindMaterialized resolves the real private active use record. Caller handle
// and plan are locators/comparisons only. The daemon binder receives the actual
// original owner's context/handle/plan, never those caller values as authority.
// A bind spends this recipe even on denial, but never calls Source or performs
// network IO. A future fixed transport must verify HMAC/file/request/TLS before
// consuming the private retained send; no public Authorize/Exchange exists here.
func (p *PreparedPiFileUseV1) BindMaterialized(ctx context.Context, handle ports.ProviderInvocationCredentialV1, plan ports.ProviderCredentialMaterializationV1,
	sender attachedworkerdaemon.PreparedAPIPhysicalSendV1) error {
	succeeded := false
	defer func() {
		if !succeeded {
			attachedworkerdaemon.RevokePreparedAPISendV1(sender)
		}
	}()
	if p == nil || p.state == nil {
		return ErrInvalid
	}
	s := p.state
	if !s.claimed.CompareAndSwap(false, true) {
		s.abort()
		return ErrConsumed
	}
	if !s.mu.TryLock() {
		s.abort()
		return ErrConsumed
	}
	defer s.mu.Unlock()
	if s.stopped.Load() || s.file == nil || s.file.ctx.Err() != nil || ctx == nil || ctx.Err() != nil {
		return ErrConsumed
	}
	f := s.file
	if uint64(len(plan.FilePath)) != f.pathBytes || uint64(len(plan.RootDir)) > f.pathBytes || plan.EnvironmentName != "" {
		return ErrDeliveryMismatch
	}
	want := handle
	want.HandleID = ""
	if want != f.scope {
		return ErrDeliveryMismatch
	}
	if domain.ValidateOpaqueID("provider_credential.handle_id", handle.HandleID) != nil {
		return ErrInvalid
	}
	f.service.mu.Lock()
	state := f.service.handles[handle.HandleID]
	var use *immutableAPIFileUse
	if state != nil && state.handle == handle {
		use = state.fileUse.Load()
	}
	f.service.mu.Unlock()
	if use == nil {
		return ErrConsumed
	}
	s.use.Store(use)
	if s.stopped.Load() {
		s.abort()
		return ErrConsumed
	}
	select {
	case <-use.gate:
	default:
		s.abort()
		return ErrConsumed
	}
	defer func() { use.gate <- struct{}{} }()
	if !use.active.Load() || use.ctx.Err() != nil || !samePiUseContext(ctx, use.ownerCtx) ||
		use.prepared != f || use.binding != f.binding || use.files != f.files || use.handle != handle || use.plan != plan || use.file == nil ||
		filepath.Base(use.plan.FilePath) != f.fileName ||
		!f.service.clock.Now().Before(handle.ExpiresAt) || sha256.Sum256(s.body) != s.bodyHash {
		s.abort()
		return ErrConsumed
	}
	id := attachedworkerdaemon.InvocationIdentity{TenantID: use.handle.TenantID, OwnerUserID: use.handle.OwnerUserID,
		WorkerID: domain.AttachedWorkerID(use.handle.WorkerID), RunID: use.handle.RunID, AttemptID: use.handle.AttemptID, LeaseID: use.handle.LeaseID, FenceToken: use.handle.LeaseFence}
	borrow, err := attachedworkerdaemon.BindPreparedAPISendV1(sender, use.ownerCtx, id, s.ledger, use.handle, use.plan, s.body, s.profile)
	if err != nil {
		s.abort()
		return ErrConsumed
	}
	if !use.send.CompareAndSwap(nil, borrow) {
		borrow.Revoke()
		s.abort()
		return ErrConsumed
	}
	if s.stopped.Load() || !use.active.Load() || use.ctx.Err() != nil || f.ctx.Err() != nil || ctx.Err() != nil ||
		len(use.secret) == 0 || int64(len(use.secret)) > f.maxBytes || domain.FingerprintCredential(use.secret) != use.fingerprint ||
		f.files.VerifyImmutable(use.ctx, use.file, use.fingerprint, f.maxBytes) != nil {
		s.abort()
		return ErrDeliveryMismatch
	}
	if !use.active.Load() || use.ctx.Err() != nil || s.stopped.Load() {
		s.abort()
		return ErrConsumed
	}
	succeeded = true
	return nil
}

func samePiUseContext(a, b context.Context) bool {
	return a != nil && b != nil && reflect.TypeOf(a).Comparable() && reflect.TypeOf(b).Comparable() && a == b
}

func (s *preparedPiFileUse) abort() {
	s.stopped.Store(true)
	if use := s.use.Load(); use != nil {
		use.cancel()
		if send := use.send.Load(); send != nil {
			send.Revoke()
		}
	}
}

func (s *preparedPiFileUse) beginCallback() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped.Load() || s.callback != 0 {
		return false
	}
	s.callback = 1
	return true
}

func (s *preparedPiFileUse) finishCallback() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.callback = 2
}

// Close revokes before joining local verification, then drops borrowed data and
// returns ONLY this recipe's live reservation, never cumulative work/nodes.
// During an active Materialize callback it revokes and returns ErrConsumed
// WITHOUT refunding. The owner retries retirement after Materialize has returned;
// this is cleanup only, never a retry of binding, Source or provider effects.
func (p *PreparedPiFileUseV1) Close() error {
	if p == nil || p.state == nil {
		return nil
	}
	s := p.state
	s.abort()
	s.mu.Lock()
	defer s.mu.Unlock()
	// Never wait for the callback while called inside that callback. Cancellation
	// has happened; a later cleanup after its final join/zero/unlock may retry.
	if s.callback == 1 {
		return ErrConsumed
	}
	s.use.Store(nil)
	if s.http != nil {
		s.http.clear()
		s.http = nil
	}
	if s.auth != nil {
		clear(s.auth.headers)
		s.auth = nil
	}
	if s.file != nil {
		f := s.file
		f.service.mu.Lock()
		if f.service.preparedPiFiles[f.scope] == s {
			delete(f.service.preparedPiFiles, f.scope)
		}
		f.service.mu.Unlock()
	}
	s.file, s.body, s.profile = nil, nil, ""
	clear(s.nonce[:])
	if s.clearErr != nil || s.live == 0 {
		return s.clearErr
	}
	s.clearErr = s.ledger.EndLive(s.live)
	if s.clearErr == nil {
		s.live = 0
		s.ledger = nil
	}
	return s.clearErr
}
