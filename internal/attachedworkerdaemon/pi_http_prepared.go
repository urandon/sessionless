package attachedworkerdaemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
)

// ErrPiHTTPRetirementPending means actual borrowers have not joined. No debit
// was retired by this pending step; retry cleanup only after their real join.
// Accounting failures are distinct terminal errors, never this sentinel.
var ErrPiHTTPRetirementPending = errors.New("Pi HTTP retirement has active borrowers")

// PiHTTPIOLeaseV1 is nonsecret process IO, NOT provider or source authority.
// Only the actual Supervisor pump constructs an active lease. Copies share its
// one-shot state. A trusted driver must join every borrower before returning.
type PiHTTPIOLeaseV1 struct {
	io   *piHTTPIO
	plan *PreparedPiHTTPStdioV1
}

func (PiHTTPIOLeaseV1) String() string     { return "PiHTTPIOLeaseV1{private:[redacted]}" }
func (v PiHTTPIOLeaseV1) GoString() string { return v.String() }

// Request borrows the actual child's frozen Fetch until the driver returns.
// Neither the borrowed frame nor its digest is an authorization or approval.
func (v *PiHTTPIOLeaseV1) Request() (pisdkhttpwire.Frame, [32]byte, error) {
	if v == nil || v.io == nil {
		return pisdkhttpwire.Frame{}, [32]byte{}, ErrSupervisorConfig
	}
	if v.plan == nil || v.plan.preparedPiHTTPState == nil {
		return pisdkhttpwire.Frame{}, [32]byte{}, ErrSupervisorConfig
	}
	return v.io.requestChecked(v.plan.preparedPiHTTPState)
}

// Write snapshots one parent response frame into Supervisor-owned storage.
// The caller retains its input unchanged until return. It cannot write Init,
// Final or raw bytes, replace pipes, or renew the original Source cutoff.
func (v *PiHTTPIOLeaseV1) Write(frame pisdkhttpwire.Frame) error {
	if v == nil || v.io == nil {
		return ErrSupervisorConfig
	}
	return v.io.write(frame)
}

func (v *PiHTTPIOLeaseV1) Abort() {
	if v != nil && v.io != nil && v.io.owner != nil {
		b := v.io.owner
		b.leaseMu.Lock()
		defer b.leaseMu.Unlock()
		if b.leaseActive.Load() && b.state.Load() == 1 {
			b.fail()
		}
	}
}

// PreparedPiHTTPStdioV1 prospectively owns SPU3 mechanics only. Its trusted Go
// callback is NOT serving registration, provider quarantine approval, a native
// qualification or #200 producer evidence. The genuine source must privately
// fix its concrete file driver; native input cannot select this callback.
type PreparedPiHTTPStdioV1 struct {
	*preparedPiHTTPState
}

// Every exported wrapper copy retains this SAME once-only state and debit.
// Copying an atomic-bearing public plan by value must not create a new owner.
type preparedPiHTTPState struct {
	bridge     *piHTTPStdio
	lease      PiHTTPIOLeaseV1
	identity   InvocationIdentity
	profile    domain.AttachedPiSDKNativeProfileV1
	profileID  string
	nonce      [32]byte
	binding    [32]byte
	profileSHA [32]byte
	requestSHA [4][32]byte
	requestLen [4]int
	preparedAt time.Time
	state      atomic.Uint32 // prepared0, binding/running1, returned2, closed3
	live       uint64
	ledger     PreparedAPILedgerV1
	clearErr   error
}

func (*PreparedPiHTTPStdioV1) String() string     { return "PreparedPiHTTPStdioV1{private:[redacted]}" }
func (p *PreparedPiHTTPStdioV1) GoString() string { return p.String() }

// PreparePiHTTPStdioV1 allocates the fixed plan before original Issue, when no
// send or materialized credential locator exists. All limits derive from P;
// this method does not lower P's caps or claim that native/full MIN fit passed.
func (s *Supervisor) PreparePiHTTPStdioV1(identity InvocationIdentity, request PreparedAPIRequestV1,
	ledger PreparedAPILedgerV1, nonce, binding [32]byte, driver func(context.Context, *PiHTTPIOLeaseV1) error) (_ *PreparedPiHTTPStdioV1, err error) {
	if s == nil || nilPreparedAPIDependency(ledger) || driver == nil || !piHTTPBoundedIdentity(identity) ||
		!piHTTPBoundedProfile(reflect.ValueOf(request.Profile)) || nonce == [32]byte{} || binding == [32]byte{} {
		return nil, ErrSupervisorConfig
	}
	// Only fixed field/length reads precede this reservation. Domain validation
	// scans strings and parses URLs; even its rejecting walks must be prepaid.
	m := uint64(reflect.TypeOf(PreparedPiHTTPStdioV1{}).Size()+reflect.TypeOf(preparedPiHTTPState{}).Size()) + 1024
	scratch := uint64(4*domain.AttachedPiSDKCanonicalMaxBytesV1 + 32768)
	work := 16*uint64(domain.AttachedPiSDKCanonicalMaxBytesV1) + 32768 + m
	for _, n := range [4]int{len(request.Body), len(request.Model), len(request.Schema), len(request.Headers)} {
		if uint64(n) > (^uint64(0)-work)/8 {
			return nil, ErrSupervisorConfig
		}
		work += 8 * uint64(n)
	}
	if ledger.Reserve(work, m+scratch, 8) != nil {
		return nil, ErrSupervisorConfig
	}
	live, published := m+scratch, false
	defer func() {
		if !published {
			err = errors.Join(err, ledger.EndLive(live))
		}
	}()
	if identity.Validate() != nil || request.Profile.Validate() != nil {
		return nil, ErrSupervisorConfig
	}
	v := request.Profile.Limits
	if v.ProcessFrameBytes < 114 || v.RequestHeaderBytes > v.ProcessFrameBytes-85 {
		return nil, ErrSupervisorConfig
	}
	l := pisdkhttpwire.Limits{FrameBytes: v.ProcessFrameBytes, InitializeBytes: v.ProcessFrameBytes - 113,
		RequestBytes:       min(v.NativeRequestBytes, v.ProcessFrameBytes-85-v.RequestHeaderBytes),
		FinalBytes:         min(v.RawFinalBytes, v.StructuredFinalBytes, v.ProcessFrameBytes-17),
		RequestHeaderBytes: v.RequestHeaderBytes, ResponseHeaderBytes: v.ResponseHeaderBytes,
		ResponseBytes: v.ResponseBodyBytes, ResponseChunkBytes: v.ResponseChunkBytes,
		ParentBytes: maxSupervisorStdinBytes, ChildBytes: min(uint64(s.maxStdout), v.ProcessStdoutBytes), Frames: v.ProcessEvents}
	if _, err := pisdkhttpwire.Measure(l); err != nil || !piHTTPBoundedRequest(request, l) {
		return nil, ErrSupervisorConfig
	}
	profile, digestErr := request.Profile.Digest()
	var profileSHA [32]byte
	_, decodeErr := hex.Decode(profileSHA[:], []byte(profile))
	if digestErr != nil || decodeErr != nil {
		return nil, ErrSupervisorConfig
	}
	if retireErr := ledger.EndLive(scratch); retireErr != nil {
		return nil, errors.Join(ErrSupervisorConfig, retireErr)
	}
	live = m
	p := &PreparedPiHTTPStdioV1{&preparedPiHTTPState{identity: identity, profile: request.Profile, profileID: profile, nonce: nonce, binding: binding,
		profileSHA: profileSHA, live: m, ledger: ledger}}
	p.requestSHA = piHTTPRequestHashes(request)
	p.requestLen = piHTTPRequestLengths(request)
	b, err := preparePiHTTPStdioPlan(s, l, ledger, func(ctx context.Context, _ *piHTTPIO) error { return driver(ctx, &p.lease) })
	if err != nil {
		return nil, err
	}
	p.bridge, p.lease.io, p.lease.plan = b, &b.lease, p
	p.preparedAt = time.Now() // all fixed payload arenas/signals already exist
	published = true
	return p, nil
}

func piHTTPBoundedIdentity(v InvocationIdentity) bool {
	return len(v.TenantID) <= 512 && len(v.OwnerUserID) <= 512 && len(v.WorkerID) <= 512 &&
		len(v.RunID) <= 512 && len(v.AttemptID) <= 512 && len(v.LeaseID) <= 512
}

// The canonical shape is fixed. Bound string lengths before any validator can
// scan attacker-sized config DATA; this is not a native allocation fit claim.
func piHTTPBoundedProfile(v reflect.Value) bool {
	if v.Kind() == reflect.String {
		return v.Len() <= 2048
	}
	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			if !piHTTPBoundedProfile(v.Field(i)) {
				return false
			}
		}
	}
	return true
}

func piHTTPBoundedRequest(r PreparedAPIRequestV1, l pisdkhttpwire.Limits) bool {
	return len(r.Body) > 0 && uint64(len(r.Body)) <= l.RequestBytes &&
		len(r.Model) > 0 && uint64(len(r.Model)) <= r.Profile.Limits.NativeRequestBytes &&
		len(r.Schema) > 0 && uint64(len(r.Schema)) <= r.Profile.Limits.NativeRequestBytes &&
		len(r.Headers) > 0 && uint64(len(r.Headers)) <= l.RequestHeaderBytes
}

func piHTTPRequestHashes(r PreparedAPIRequestV1) [4][32]byte {
	return [4][32]byte{sha256.Sum256(r.Body), sha256.Sum256(r.Model), sha256.Sum256(r.Schema), sha256.Sum256(r.Headers)}
}

func piHTTPRequestLengths(r PreparedAPIRequestV1) [4]int {
	return [4]int{len(r.Body), len(r.Model), len(r.Schema), len(r.Headers)}
}

// RunPreparedPiHTTP uses the original RunPrepared callback and SAME Supervisor.
// It requires the genuine, already file-bound original send, not a structural
// implementation. Success remains mechanics DATA: no piStdio stamp is minted.
func (s *Supervisor) RunPreparedPiHTTP(parent context.Context, in PreparedInvocationV1, p *PreparedPiHTTPStdioV1) (AttemptResult, error) {
	if p == nil || p.preparedPiHTTPState == nil || !p.state.CompareAndSwap(0, 1) {
		RevokePreparedAPISendV1(in.PhysicalSend)
		return AttemptResult{}, ErrSupervisorConfig
	}
	defer p.state.Store(2)
	completed := false
	defer func() {
		if completed {
			if send, ok := in.PhysicalSend.(*preparedAPIPhysicalSend); ok && send != nil {
				send.revoke() // normal exit never invalidates original F
			}
		} else {
			RevokePreparedAPISendV1(in.PhysicalSend)
		}
	}()
	send, ok := in.PhysicalSend.(*preparedAPIPhysicalSend)
	if s == nil || p.bridge == nil || p.bridge.owner != s || parent == nil || parent.Err() != nil ||
		!ok || send == nil || !send.mu.TryLock() {
		return AttemptResult{}, ErrSupervisorConfig
	}
	valid := send.bound && send.handleSealed && send.planSealed && !send.retired.Load() && !send.invalid.Load() && !send.consumed &&
		sameOriginalAPIContext(parent, send.ownerCtx) && send.cancel.Load() != nil &&
		in.Identity == p.identity && in.Identity == send.identity && in.Credential == send.handle && in.Materialization == send.plan &&
		samePiLedger(in.Ledger, p.ledger) && samePiLedger(in.Ledger, send.ledger) &&
		p.preparedAt != p.preparedAt.Round(0) && in.apiIssueStarted != in.apiIssueStarted.Round(0) && !p.preparedAt.After(in.apiIssueStarted) &&
		in.APIRequest != nil && in.APIRequest.Profile == p.profile && piHTTPBoundedRequest(*in.APIRequest, p.bridge.core.limits) &&
		piHTTPRequestLengths(*in.APIRequest) == p.requestLen &&
		piHTTPRequestHashes(*in.APIRequest) == p.requestSHA && bytes.Equal(in.APIRequest.Body, send.body) &&
		send.profile == p.profileID && piExecutableMatches(in.Process.ExecutableDigest, p.profile.Sources.NativeArtifactDigest) &&
		len(in.Process.Stdin) > 0 && uint64(len(in.Process.Stdin)) <= p.bridge.core.limits.InitializeBytes && utf8.Valid(in.Process.Stdin)
	send.mu.Unlock()
	if !valid {
		return AttemptResult{}, ErrSupervisorConfig
	}
	init := pisdkhttpwire.Frame{Kind: pisdkhttpwire.Initialize, Nonce: p.nonce, Binding: p.binding, Profile: p.profileSHA, Body: in.Process.Stdin}
	if err := p.bridge.bind(send, init); err != nil {
		return AttemptResult{}, err
	}
	spec := in.Process
	spec.Stdin, spec.piHTTP = nil, p.bridge
	result, err := s.Run(p.bridge.ctx, spec)
	if !p.bridge.stopAndJoin(s.grace) {
		return result, errors.Join(err, ErrSupervisorConfig)
	}
	// All driver/request borrowers have joined. Re-read the retained actual
	// request arena, not driver-returned data or a mutable caller digest.
	f := p.bridge.core.requestFrame
	n := 85 + len(f.Headers) + len(f.Body)
	if err != nil || !p.bridge.complete || p.bridge.failure.Load() || f.Profile != p.profileSHA ||
		n > len(p.bridge.core.request) || sha256.Sum256(p.bridge.core.request[:n]) != p.bridge.requestDigest ||
		sha256.Sum256(f.Body) != p.requestSHA[0] || in.APIRequest.Profile != p.profile || !piHTTPBoundedRequest(*in.APIRequest, p.bridge.core.limits) ||
		piHTTPRequestLengths(*in.APIRequest) != p.requestLen ||
		piHTTPRequestHashes(*in.APIRequest) != p.requestSHA {
		return result, errors.Join(err, ErrSupervisorConfig)
	}
	completed = true
	return result, nil
}

func (p *PreparedPiHTTPStdioV1) Close() error {
	if p == nil || p.preparedPiHTTPState == nil {
		return nil
	}
	for {
		state := p.state.Load()
		if state == 3 {
			return p.clearErr
		}
		if state != 0 && state != 2 {
			return ErrPiHTTPRetirementPending
		}
		if p.state.CompareAndSwap(state, 1) {
			break
		}
	}
	if err := p.bridge.clear(); err != nil {
		if errors.Is(err, ErrPiHTTPRetirementPending) {
			p.state.Store(2) // preserve debit/aliases until actual borrowers join
		} else {
			p.clearErr = err
			p.state.Store(3)
		}
		return err
	}
	if err := p.ledger.EndLive(p.live); err != nil {
		p.clearErr = err
		p.state.Store(3) // never retry a failed debit against later owners
		return err
	}
	p.profile, p.identity = domain.AttachedPiSDKNativeProfileV1{}, InvocationIdentity{}
	p.profileID = ""
	p.ledger, p.live = nil, 0
	p.state.Store(3)
	return nil
}
