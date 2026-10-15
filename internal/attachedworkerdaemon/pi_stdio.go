package attachedworkerdaemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// PreparedPiStdioV1 owns prospectively funded GO payload buffers only. It is
// neither a full Init/schema producer nor SDK/native/platform fit or authority.
// Prepare it on the SAME original ledger before Issue; keep it alive through
// RunPrepared, semantic collection and all owned joins. Close releases only its
// own live reservation, never cumulative work or another owner's live bytes.
type PreparedPiStdioV1 struct {
	owner          *Supervisor
	profile        domain.AttachedPiSDKNativeProfileV1
	limits         pisdkphasewire.FrameLimits
	ledger         PreparedAPILedgerV1
	live           uint64
	init, assembly []byte
	final          []byte
	state          atomic.Uint32 // 0 prepared, 1 running, 2 returned, 3 cleared
	preparedAt     time.Time
	observation    *PiStdioObservationV1 // allocated/funded BEFORE Issue
}

func (*PreparedPiStdioV1) String() string     { return "PreparedPiStdioV1{data:[redacted]}" }
func (p *PreparedPiStdioV1) GoString() string { return p.String() }

// PreparedProcessTimeoutV1 exposes the actual configured process bound as
// DATA. The original owner anchors it to original entry, never preparation end.
// This neither starts a timer nor attests the launcher/native implementation.
func (s *Supervisor) PreparedProcessTimeoutV1() time.Duration {
	if s == nil {
		return 0
	}
	return s.timeout
}

// PreparePiStdioV1 does not manufacture the still-missing genuine full producer.
// These are conservative Go payload/copy reservations, not heap/SDK/native/pipe
// bounds. The original full prospective producer/ledger/profile is required
// independently; this disconnected library method enables no serving route.
func (s *Supervisor) PreparePiStdioV1(p domain.AttachedPiSDKNativeProfileV1, ledger PreparedAPILedgerV1) (*PreparedPiStdioV1, error) {
	if s == nil || nilPreparedAPIDependency(ledger) || p.Validate() != nil {
		return nil, ErrSupervisorConfig
	}
	frame := p.Limits.ProcessFrameBytes
	if frame > pisdkphasewire.TransportMaxFrameBytes || frame < 147 || p.Limits.ProcessEvents < 4 {
		return nil, ErrSupervisorConfig
	}
	l := pisdkphasewire.FrameLimits{
		InitializeBodyBytes: frame - 17,
		RequestBodyBytes:    min(p.Limits.NativeRequestBytes, frame-146),
		FinalBodyBytes:      min(p.Limits.StructuredFinalBytes, p.Limits.RawFinalBytes, frame-17),
		FrameBytes:          frame,
	}
	// Header preflight validates all positive consistent caps without payloads.
	var h [17]byte
	copy(h[:4], "SPU2")
	h[4] = pisdkphasewire.Retire
	if _, err := pisdkphasewire.InspectHeader(h[:], l); err != nil {
		return nil, ErrSupervisorConfig
	}
	wire, wireErr := pisdkphasewire.MeasureWireAccounting(l.InitializeBodyBytes, l.RequestBodyBytes, l.FinalBodyBytes)
	if wireErr != nil || wire.ParentStdinBytes > maxSupervisorStdinBytes || wire.ChildStdoutBytes > min(uint64(s.maxStdout), p.Limits.ProcessStdoutBytes) {
		return nil, ErrSupervisorConfig
	}
	// Two complete preallocated arenas; at most one decoded request/P and one
	// decoded Final overlap. Fixed header/use bytes are included prospectively.
	// 8F+6B+6L+2048 conservatively covers app copies/scans/hash/compare, not
	// native allocations. All terms <=1MiB so these uint64 sums cannot overflow.
	metadata := uint64(reflect.TypeOf(PiStdioObservationV1{}).Size()) + uint64(reflect.TypeOf(piObservationStamp{}).Size())
	live := 2*frame + l.RequestBodyBytes + l.FinalBodyBytes + 64 + 43 + metadata
	work := 8*frame + 6*l.RequestBodyBytes + 6*l.FinalBodyBytes + 2048 + metadata
	if ledger.Reserve(work, live, pisdkphasewire.ChildMetadataNodes) != nil {
		return nil, ErrSupervisorConfig
	}
	plan := &PreparedPiStdioV1{owner: s, profile: p, limits: l, ledger: ledger, live: live, init: make([]byte, int(frame)), assembly: make([]byte, int(frame)), observation: &PiStdioObservationV1{stamp: &piObservationStamp{}}}
	plan.preparedAt = time.Now() // AFTER reservation and both arena allocations
	return plan, nil
}

func (p *PreparedPiStdioV1) Close() error {
	if p == nil {
		return ErrSupervisorConfig
	}
	for {
		state := p.state.Load()
		if state == 3 {
			return nil
		}
		if state == 1 {
			return ErrSupervisorConfig
		} // never clear beneath owned IO
		if p.state.CompareAndSwap(state, 3) {
			break
		}
	}
	clear(p.init)
	clear(p.assembly)
	clear(p.final)
	p.init = nil
	p.assembly = nil
	p.final = nil
	if p.observation != nil {
		p.observation.send, p.observation.ledger = nil, nil
		p.observation.credential, p.observation.materialization = ports.ProviderInvocationCredentialV1{}, ports.ProviderCredentialMaterializationV1{}
	}
	return p.ledger.EndLive(p.live)
}

// PiStdioObservationV1 is actual local process DATA only, not #200 custody or
// immutable credential finalization evidence. Final borrows the original plan
// until Close. No raw request/Init bytes are exposed in public AttemptResult.
type PiStdioObservationV1 struct {
	StdoutDigest [sha256.Size]byte
	FinalDigest  [sha256.Size]byte
	Final        []byte
	stamp        *piObservationStamp
	// Only SAME RunPreparedPi fills this private original association after
	// actual process/pump joins. The public DATA accessor strips it. No plan,
	// input, credential path or key is retained through output custody.
	send            *preparedAPIPhysicalSend
	identity        InvocationIdentity
	head            domain.AttachedWorkerAttemptV1
	profile         domain.AttachedPiSDKNativeProfileV1
	ledger          PreparedAPILedgerV1
	requestHash     [4][sha256.Size]byte
	credential      ports.ProviderInvocationCredentialV1
	materialization ports.ProviderCredentialMaterializationV1
}

// Nonzero-sized private identity allocated by the real Supervisor plan BEFORE
// Issue. It carries no context, buffer, credential or capability and has no
// public constructor/accessor. It only identifies that actual observation.
type piObservationStamp struct{ identity byte }

func (PiStdioObservationV1) String() string     { return "PiStdioObservationV1{data:[redacted]}" }
func (p PiStdioObservationV1) GoString() string { return p.String() }
func (r AttemptResult) PiStdioObservationV1() (PiStdioObservationV1, bool) {
	if r.piStdio == nil {
		return PiStdioObservationV1{}, false
	}
	return PiStdioObservationV1{StdoutDigest: r.piStdio.StdoutDigest, FinalDigest: r.piStdio.FinalDigest, Final: r.piStdio.Final}, true
}

// RunPreparedPi executes through SAME Run, not another process/credential owner.
// The real complete Init must already have been funded by the genuine producer;
// Process.Stdin is borrowed DATA, not a provenance or fit certificate. Only the
// original InvocationRunner's private concrete one-shot capability is accepted.
func (s *Supervisor) RunPreparedPi(parent context.Context, invocation PreparedInvocationV1, p *PreparedPiStdioV1) (AttemptResult, error) {
	send, ok := invocation.PhysicalSend.(*preparedAPIPhysicalSend)
	if ok && send != nil {
		defer send.revoke()
	}
	if s == nil || parent == nil || parent.Err() != nil || p == nil || p.owner != s || invocation.APIRequest == nil ||
		p.profile != invocation.APIRequest.Profile || !samePiLedger(p.ledger, invocation.Ledger) ||
		p.preparedAt == p.preparedAt.Round(0) || invocation.apiIssueStarted == invocation.apiIssueStarted.Round(0) || p.preparedAt.After(invocation.apiIssueStarted) ||
		!ok || send == nil || send.cancel.Load() == nil || !samePiPointer(parent, send.ownerCtx) ||
		len(invocation.Process.Stdin) == 0 || uint64(len(invocation.Process.Stdin)) > p.limits.InitializeBodyBytes ||
		len(invocation.APIRequest.Body) == 0 || uint64(len(invocation.APIRequest.Body)) > p.limits.RequestBodyBytes ||
		!bytes.Equal(invocation.APIRequest.Body, send.body) || !piExecutableMatches(invocation.Process.ExecutableDigest, p.profile.Sources.NativeArtifactDigest) || !p.state.CompareAndSwap(0, 1) {
		return AttemptResult{}, ErrSupervisorConfig
	}
	defer p.state.Store(2)
	// Funded init arena, no new payload allocation or independent copy pool.
	body := invocation.Process.Stdin
	if !utf8.Valid(body) {
		return AttemptResult{}, ErrSupervisorConfig
	}
	frame := p.init[:17+len(body)]
	copy(frame[:4], "SPU2")
	frame[4] = pisdkphasewire.Initialize
	binary.BigEndian.PutUint32(frame[5:9], 0)
	binary.BigEndian.PutUint64(frame[9:17], uint64(len(body)))
	copy(frame[17:], body)
	ctx, cancel := context.WithCancel(parent)
	b := &piStdioBridge{ctx: ctx, cancel: cancel, plan: p, send: send, init: frame, done: make(chan struct{}), digest: sha256.New()}
	spec := invocation.Process
	spec.Stdin = nil
	spec.piStdio = b
	result, err := s.Run(ctx, spec)
	b.stopAndJoin() // joins Source and all pipe operations BEFORE borrowed data returns
	if b.expired.Load() {
		result.Deadline = true
	}
	if !b.started || !b.complete || b.failure.Load() {
		if result.FailureCode == "" {
			result.FailureCode = "pi_stdio_incomplete"
		}
		return result, errors.Join(err, ErrSupervisorConfig)
	}
	if err != nil || result.Cancelled || result.Deadline || result.ExitCode != 0 || result.FailureCode != "" ||
		!result.DescendantsReaped || !result.BoundaryReleased || !result.CleanupSucceeded {
		return result, err
	}
	observation := p.observation
	observation.Final, observation.FinalDigest = p.final, sha256.Sum256(p.final)
	if invocation.VerifiedInput != nil && len(invocation.APIRequest.Model) > 0 && len(invocation.APIRequest.Schema) > 0 && len(invocation.APIRequest.Headers) > 0 &&
		uint64(len(invocation.APIRequest.Model)) <= p.profile.Limits.NativeRequestBytes && uint64(len(invocation.APIRequest.Schema)) <= p.profile.Limits.NativeRequestBytes &&
		uint64(len(invocation.APIRequest.Headers)) <= p.profile.Limits.RequestHeaderBytes {
		observation.send, observation.identity, observation.head = send, invocation.Identity, invocation.VerifiedInput.AcceptedAttempt
		observation.profile, observation.ledger = p.profile, invocation.Ledger
		observation.credential, observation.materialization = invocation.Credential, invocation.Materialization
		for i, v := range [][]byte{invocation.APIRequest.Body, invocation.APIRequest.Model, invocation.APIRequest.Schema, invocation.APIRequest.Headers} {
			observation.requestHash[i] = sha256.Sum256(v)
		}
	}
	b.digest.Sum(observation.StdoutDigest[:0])
	result.piStdio = observation
	return result, nil
}

func samePiLedger(a, b PreparedAPILedgerV1) bool {
	if nilPreparedAPIDependency(a) || nilPreparedAPIDependency(b) {
		return false
	}
	return samePiPointer(a, b)
}

func samePiPointer(a, b any) bool {
	if nilPreparedAPIDependency(a) || nilPreparedAPIDependency(b) {
		return false
	}
	x, y := reflect.ValueOf(a), reflect.ValueOf(b)
	return x.Type() == y.Type() && x.Kind() == reflect.Pointer && x.Pointer() == y.Pointer()
}

func piExecutableMatches(d ExecutableDigest, expected string) bool {
	if len(expected) != 64 {
		return false
	}
	const hex = "0123456789abcdef"
	for i, b := range d {
		if expected[2*i] != hex[b>>4] || expected[2*i+1] != hex[b&15] {
			return false
		}
	}
	return true
}

type piStdioBridge struct {
	ctx               context.Context
	cancel            context.CancelFunc
	plan              *PreparedPiStdioV1
	send              *preparedAPIPhysicalSend
	init              []byte
	done              chan struct{}
	digest            hash.Hash
	started, complete bool // owner reads only after done
	failure           atomic.Bool
	expired           atomic.Bool
	cutoff            time.Time        // pump-owned once-selected bound
	clock             func() time.Time // test-only synthetic elapsed projection; nil uses real Go monotonic
	stdout            io.Reader
	stdin             *os.File
	stdoutFile        *os.File
	observed          *boundedBuffer
	violation         chan<- string
	processDeadline   time.Time
	closeOnce         sync.Once
	timerMu           sync.Mutex
	stopping          bool
	timer             *time.Timer
	timerDone         chan struct{}
}

func (b *piStdioBridge) closeIO() {
	b.closeOnce.Do(func() {
		if b.stdin != nil {
			_ = b.stdin.Close()
		}
		if b.stdoutFile != nil {
			_ = b.stdoutFile.Close()
		}
	})
}
func (b *piStdioBridge) revoke() {
	b.send.revoke()
	b.cancel()
	b.closeIO()
}
func (b *piStdioBridge) fail() {
	b.failure.Store(true)
	select {
	case b.violation <- "pi_stdio_invalid":
	default:
	}
	b.revoke()
}
func (b *piStdioBridge) start(reader io.Reader, readerFile, writer *os.File, observed *boundedBuffer, violation chan<- string, deadline time.Time) <-chan struct{} {
	b.stdout = reader
	b.stdoutFile = readerFile
	b.stdin = writer
	b.observed = observed
	b.violation = violation
	b.processDeadline = deadline
	b.started = true
	go func() {
		defer close(b.done)
		if b.pump() != nil {
			b.fail()
		}
	}()
	return b.done
}
func (b *piStdioBridge) stopAndJoin() {
	b.timerMu.Lock()
	b.stopping = true
	timer, done := b.timer, b.timerDone
	b.timerMu.Unlock()
	b.revoke()
	if b.started {
		<-b.done
	}
	if timer != nil {
		if timer.Stop() {
			close(done)
		}
		<-done
	}
}
func (b *piStdioBridge) arm(cutoff time.Time) (time.Duration, error) {
	if cutoff == cutoff.Round(0) {
		return 0, ErrSupervisorConfig
	}
	if d, ok := b.ctx.Deadline(); ok && d.Before(cutoff) {
		cutoff = d
	}
	if b.processDeadline.Before(cutoff) {
		cutoff = b.processDeadline
	}
	if cutoff == cutoff.Round(0) {
		return 0, ErrSupervisorConfig
	}
	b.timerMu.Lock()
	defer b.timerMu.Unlock()
	if b.stopping || b.timer != nil || b.ctx.Err() != nil {
		return 0, ErrSupervisorConfig
	}
	q := b.currentTime()
	if q == q.Round(0) {
		return 0, ErrSupervisorConfig
	}
	n := cutoff.Sub(q)
	if n <= 0 || !q.Add(n).Equal(cutoff) {
		return 0, ErrSupervisorConfig
	}
	b.timerDone = make(chan struct{})
	b.cutoff = cutoff
	b.timer = time.AfterFunc(n, func() { defer close(b.timerDone); b.expired.Store(true); b.revoke() })
	return n, nil
}

func (b *piStdioBridge) currentTime() time.Time {
	if b.clock != nil {
		return b.clock()
	}
	return time.Now()
}

func (b *piStdioBridge) writeFull(data []byte) error {
	for len(data) > 0 {
		if b.ctx.Err() != nil || !b.cutoff.IsZero() && !b.currentTime().Before(b.cutoff) {
			return ErrSupervisorConfig
		}
		n, err := b.stdin.Write(data[:min(len(data), 32<<10)])
		if err != nil || n <= 0 {
			return ErrSupervisorConfig
		}
		data = data[n:]
	}
	return nil
}
func (b *piStdioBridge) readFull(data []byte) (int, error) {
	read := 0
	for read < len(data) {
		if b.ctx.Err() != nil || !b.cutoff.IsZero() && !b.currentTime().Before(b.cutoff) {
			return read, ErrSupervisorConfig
		}
		n, err := b.stdout.Read(data[read:])
		if n > 0 {
			if _, e := b.observed.Write(data[read : read+n]); e != nil {
				return read, ErrSupervisorConfig
			}
			_, _ = b.digest.Write(data[read : read+n])
			read += n
		}
		if err != nil {
			if read == len(data) {
				return read, nil
			}
			return read, err
		}
		if n == 0 {
			return read, ErrSupervisorConfig
		}
	}
	return read, nil
}
func (b *piStdioBridge) pump() error {
	if b.writeFull(b.init) != nil {
		return ErrSupervisorConfig
	}
	state := pisdkphasewire.PhysicalRequest
	for {
		var header [17]byte
		n, err := b.readFull(header[:])
		if err != nil {
			if n == 0 && errors.Is(err, io.EOF) && state == 0 {
				b.complete = true
				return nil
			}
			return ErrSupervisorConfig
		}
		h, err := pisdkphasewire.InspectHeader(header[:], b.plan.limits)
		if err != nil || h.Direction != pisdkphasewire.ChildToParent || h.Kind != state {
			return ErrSupervisorConfig
		}
		// Validate cumulative stdout before reading either admitted region.
		remaining := uint64(b.observed.limit - b.observed.countValue())
		if h.TotalBytes-17 > remaining {
			return ErrSupervisorConfig
		}
		raw := b.plan.assembly[:int(h.TotalBytes)]
		copy(raw[:17], header[:])
		if _, err := b.readFull(raw[17:]); err != nil {
			return ErrSupervisorConfig
		}
		f, err := pisdkphasewire.DecodeFrame(raw, b.plan.limits)
		if err != nil {
			return ErrSupervisorConfig
		}
		clear(raw)
		if state == pisdkphasewire.PhysicalRequest {
			cutoff, authErr := b.send.Authorize(f.Body, f.BackendProfileDigest)
			clear(f.Body)
			if authErr != nil || b.ctx.Err() != nil {
				return ErrSupervisorConfig
			}
			allowance, err := b.arm(cutoff)
			if err != nil {
				return ErrSupervisorConfig
			}
			// Fixed pre-funded binary response. q was sampled once by arm, BEFORE
			// encoding/writing: blocked writes consume, never renew this allowance.
			var use [26]byte
			copy(use[:4], "SPU2")
			use[4] = pisdkphasewire.UseResult
			binary.BigEndian.PutUint32(use[5:9], 9)
			use[17] = 1
			binary.BigEndian.PutUint64(use[18:26], uint64(allowance))
			if b.writeFull(use[:]) != nil {
				return ErrSupervisorConfig
			}
			state = pisdkphasewire.StructuredFinal
		} else {
			b.plan.final = f.Body // funded independent retained copy; NOT final custody
			state = 0             // only exact EOF now; any further header fails before region read
		}
	}
}
