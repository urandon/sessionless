package pisdkhttpwire

import (
	"crypto/sha256"
	"hash"
	"sync"
	"sync/atomic"
)

// Sequence is a structural byte-stream checker, NEVER a permission or an HTTP
// EOF/process/native-fit witness. Copies share the same private state. Prepare
// the fixed metadata/hash state before Issue on the original ledger. Abort is
// concurrent-safe; all other methods use nonqueueing single-consumer entry.
type Sequence struct{ state *sequenceState }

func (Sequence) String() string     { return "PiHTTPSequence{data:[redacted]}" }
func (s Sequence) GoString() string { return s.String() }

type sequenceState struct {
	gate              sync.Mutex
	failed            atomic.Bool
	limits            Limits
	chunk             uint64
	phase             Kind
	profile, digest   [32]byte
	count, next       uint32
	bytes, seen       uint64
	digestState       hash.Hash
	digestScratch     [sha256.Size]byte // prepaid Sum destination; zero outside the sequence gate
	retired, finished bool
	final             atomic.Bool
	pendingStage      byte // 0 none, 1 metadata required, 2 body required
	pendingHeader     Header
	pendingPeer       Direction
	pendingFrame      Frame
}

func NewSequence(l Limits) (*Sequence, error) {
	a, err := Measure(l)
	if err != nil {
		return nil, err
	}
	return &Sequence{state: &sequenceState{limits: l, chunk: a.ChunkBytes, digestState: sha256.New()}}, nil
}

func (s *Sequence) Abort() {
	if s != nil && s.state != nil {
		s.state.failed.Store(true)
	}
}

// Accept checks an ALREADY-owned typed frame. A stream reader must instead use
// BeginHeader -> PreflightMetadata -> AcceptBody to gate reads before assembly.
func (s *Sequence) Accept(peer Direction, f Frame) error {
	if s == nil || s.state == nil {
		return ErrInvalid
	}
	v := s.state
	if !v.gate.TryLock() {
		v.failed.Store(true)
		return ErrInvalid
	}
	defer v.gate.Unlock()
	if v.pendingStage != 0 && f.Kind != Retire {
		v.failed.Store(true)
		return ErrInvalid
	}
	return v.acceptLocked(peer, f)
}

// PreflightWrite checks an already-owned PARENT frame without changing any
// protocol/hash/pending/failure state. It is not a write grant or a reservation
// for that frame. The supervisor must serialize preflight -> actual full write
// -> Accept on its own immutable snapshot. It MUST Abort on any error or short
// write; retries/probing another frame cannot repair a failed physical run.
// Fund both validation walks before Issue on the original ledger.
func (s *Sequence) PreflightWrite(f Frame) error {
	if s == nil || s.state == nil {
		return ErrInvalid
	}
	v := s.state
	if !v.gate.TryLock() {
		return ErrInvalid
	}
	defer v.gate.Unlock()
	if v.pendingStage != 0 {
		return ErrInvalid
	}
	return v.checkTransition(ParentToChild, f)
}

// The one canonical read-only validator serves both outbound preflight and
// actual commit. It neither advances state nor marks a failed validation; the
// consuming operation owns that irreversible transition (Accept or seam Abort).
func (v *sequenceState) checkTransition(peer Direction, f Frame) error {
	h, err := inspectFrame(f, v.limits)
	if err != nil || v.retired || v.finished {
		return ErrInvalid
	}
	want := ParentToChild
	if h.Kind == FetchRequest || h.Kind == Pull || h.Kind == StructuredFinal {
		want = ChildToParent
	}
	if peer != want {
		return ErrInvalid
	}
	// Retirement is legal after failure as cleanup DATA, but cannot restore a
	// valid stream or make an early retirement finish successfully.
	if f.Kind == Retire {
		return nil
	}
	if v.failed.Load() || !v.expected(f.Kind) {
		return ErrInvalid
	}
	switch f.Kind {
	case FetchRequest:
		if f.Profile != v.profile {
			return ErrInvalid
		}
	case Pull:
		if f.Index != v.next {
			return ErrInvalid
		}
	case ResponsePart:
		if f.Index != v.next || uint64(len(f.Body)) != min(v.chunk, v.bytes-v.seen) {
			return ErrInvalid
		}
	case ResponseEnd:
		if v.next != v.count || v.seen != v.bytes || f.Count != v.count || f.Bytes != v.bytes || f.Digest != v.digest {
			return ErrInvalid
		}
		// A stack array passed through hash.Hash.Sum escapes on the pinned
		// toolchain. Reuse the pre-Issue state-owned scratch instead; never
		// alter the accumulated hash or retain temporary digest bytes.
		v.digestState.Sum(v.digestScratch[:0])
		match := v.digestScratch == v.digest
		clear(v.digestScratch[:])
		if !match {
			return ErrInvalid
		}
	}
	if v.failed.Load() {
		return ErrInvalid
	}
	return nil
}

func (v *sequenceState) acceptLocked(peer Direction, f Frame) error {
	if v.checkTransition(peer, f) != nil {
		v.failed.Store(true)
		return ErrInvalid
	}
	if f.Kind == Retire {
		v.retired = true
		v.pendingStage, v.pendingFrame = 0, Frame{}
		return nil
	}
	switch f.Kind {
	case Initialize:
		v.profile = f.Profile
	case ResponseStart:
		v.count, v.bytes, v.digest = f.Count, f.Bytes, f.Digest
	case ResponsePart:
		_, _ = v.digestState.Write(f.Body) // fixed stdlib SHA256, never caller code
		v.seen += uint64(len(f.Body))
		v.next++
	case StructuredFinal:
		v.final.Store(true)
	}
	v.phase = f.Kind
	if v.failed.Load() {
		return ErrInvalid
	}
	return nil
}

// Finish records the owned CHILD wire EOF only, not real HTTP EOF or process
// quiescence. The actual original supervisor must prove those independently.
func (s *Sequence) Finish() error {
	if s == nil || s.state == nil {
		return ErrInvalid
	}
	v := s.state
	if !v.gate.TryLock() {
		v.failed.Store(true)
		return ErrInvalid
	}
	defer v.gate.Unlock()
	if v.failed.Load() || v.finished || v.pendingStage != 0 || !v.final.Load() || v.phase != StructuredFinal {
		v.failed.Store(true)
		return ErrInvalid
	}
	v.finished = true
	return nil
}
