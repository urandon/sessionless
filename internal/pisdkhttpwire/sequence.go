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
// concurrent-safe; Accept and Finish use nonqueueing single-consumer entry.
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

func (v *sequenceState) acceptLocked(peer Direction, f Frame) error {
	fail := func() error { v.failed.Store(true); return ErrInvalid }
	h, err := inspectFrame(f, v.limits)
	if err != nil || v.retired || v.finished {
		return fail()
	}
	want := ParentToChild
	if h.Kind == FetchRequest || h.Kind == Pull || h.Kind == StructuredFinal {
		want = ChildToParent
	}
	if peer != want {
		return fail()
	}
	// Retirement is legal after failure as cleanup DATA, but cannot restore a
	// valid stream or make an early retirement finish successfully.
	if f.Kind == Retire {
		v.retired = true
		v.pendingStage, v.pendingFrame = 0, Frame{}
		return nil
	}
	if v.failed.Load() {
		return ErrInvalid
	}
	switch f.Kind {
	case Initialize:
		if v.phase != 0 {
			return fail()
		}
		v.profile = f.Profile
	case FetchRequest:
		if v.phase != Initialize || f.Profile != v.profile {
			return fail()
		}
	case ResponseStart:
		if v.phase != FetchRequest {
			return fail()
		}
		v.count, v.bytes, v.digest = f.Count, f.Bytes, f.Digest
	case Pull:
		if (v.phase != ResponseStart && v.phase != ResponsePart) || v.next >= v.count || f.Index != v.next {
			return fail()
		}
	case ResponsePart:
		if v.phase != Pull || f.Index != v.next || v.seen >= v.bytes || uint64(len(f.Body)) != min(v.chunk, v.bytes-v.seen) {
			return fail()
		}
		_, _ = v.digestState.Write(f.Body) // fixed stdlib SHA256, never caller code
		v.seen += uint64(len(f.Body))
		v.next++
	case ResponseEnd:
		if v.phase != ResponsePart || v.next != v.count || v.seen != v.bytes || f.Count != v.count || f.Bytes != v.bytes || f.Digest != v.digest {
			return fail()
		}
		var actual [32]byte
		v.digestState.Sum(actual[:0])
		if actual != v.digest {
			return fail()
		}
	case StructuredFinal:
		if v.phase != ResponseEnd {
			return fail()
		}
		v.final.Store(true)
	default:
		return fail()
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
