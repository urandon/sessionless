package pisdkhttpwire

import (
	"bytes"
	"encoding"
	"reflect"
	"testing"
)

// Snapshot every protocol field and the full SHA state, not only its digest.
// Do not copy the mutex/atomics or assume a read-only check may set failed.
type preflightSnapshot struct {
	failed, final, retired, finished bool
	limits                           Limits
	chunk                            uint64
	phase                            Kind
	profile, digest                  [32]byte
	count, next                      uint32
	bytes, seen                      uint64
	hashState                        []byte
	digestScratch                    [32]byte
	pendingStage                     byte
	pendingHeader                    Header
	pendingPeer                      Direction
	pendingFrame                     Frame
}

func snapshotPreflight(t *testing.T, s *Sequence) preflightSnapshot {
	t.Helper()
	if reflect.TypeOf(sequenceState{}).NumField() != 20 {
		t.Fatal("sequence fields changed; update complete preflight state snapshot")
	}
	v := s.state
	h, ok := v.digestState.(encoding.BinaryMarshaler)
	if !ok {
		t.Fatal("fixed SHA state cannot be snapshotted")
	}
	b, err := h.MarshalBinary()
	if err != nil {
		t.Fatal("snapshot SHA state:", err)
	}
	pending := v.pendingFrame
	pending.Headers, pending.Body = bytes.Clone(pending.Headers), bytes.Clone(pending.Body)
	return preflightSnapshot{failed: v.failed.Load(), final: v.final.Load(), retired: v.retired, finished: v.finished,
		limits: v.limits, chunk: v.chunk, phase: v.phase, profile: v.profile, digest: v.digest, count: v.count, next: v.next,
		bytes: v.bytes, seen: v.seen, hashState: b, digestScratch: v.digestScratch, pendingStage: v.pendingStage, pendingHeader: v.pendingHeader,
		pendingPeer: v.pendingPeer, pendingFrame: pending}
}

func preflightUnchanged(t *testing.T, s *Sequence, f Frame, valid bool) {
	t.Helper()
	before := snapshotPreflight(t, s)
	err := s.PreflightWrite(f)
	if (err == nil) != valid {
		t.Fatalf("preflight kind=%d phase=%d valid=%v err=%v", f.Kind, before.phase, valid, err)
	}
	if !reflect.DeepEqual(before, snapshotPreflight(t, s)) {
		t.Fatalf("preflight kind=%d phase=%d changed protocol/hash/pending/failure state", f.Kind, before.phase)
	}
}

func sequencePrefix(t *testing.T, l Limits, frames []Frame) *Sequence {
	t.Helper()
	s := newSequence(t, l)
	for _, f := range frames {
		stagedFrame(t, s, f, l)
	}
	return s
}

func TestPreflightWriteReadOnlyThroughCompleteStream(t *testing.T) {
	l := testLimits()
	l.ResponseChunkBytes = 2
	frames := streamFrames(l, []byte("data: €\n\n"))
	s := newSequence(t, l)
	for _, f := range frames {
		if peer(f.Kind) == ParentToChild {
			for range 3 {
				preflightUnchanged(t, s, f, true)
			}
			if err := s.Accept(ParentToChild, f); err != nil {
				t.Fatalf("commit kind=%d after read-only checks: %v", f.Kind, err)
			}
		} else {
			// Preflight is parent-only even for an otherwise valid child frame.
			preflightUnchanged(t, s, f, false)
			stagedFrame(t, s, f, l)
		}
	}
	if err := s.Finish(); err != nil {
		t.Fatal("complete original stream denied:", err)
	}
	preflightUnchanged(t, s, Frame{Kind: Retire}, false)
}

func TestPreflightWriteDoesNotPublishNextChildPhase(t *testing.T) {
	l := testLimits()
	frames := streamFrames(l, []byte("0123456789"))
	for _, tc := range []struct {
		name             string
		prefix, outbound int
		nextChild        Frame
	}{
		{"initialize", 0, 0, frames[1]},
		{"start", 2, 2, frames[3]},
		{"part", 4, 4, frames[5]},
		{"end", 7, 7, frames[8]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := sequencePrefix(t, l, frames[:tc.prefix])
			preflightUnchanged(t, s, frames[tc.outbound], true)
			header := encode(t, tc.nextChild, l)[:HeaderBytes]
			if _, err := s.BeginHeader(header, ChildToParent); err == nil {
				t.Fatal("next child frame admitted before actual parent commit")
			}
			if err := s.Finish(); err == nil {
				t.Fatal("preflight manufactured complete child EOF")
			}
		})
	}
}

func TestPreflightWriteInvalidFramePreservesStateAndOwnerAborts(t *testing.T) {
	l := testLimits()
	frames := streamFrames(l, []byte("0123456789"))
	badEnd := frames[7]
	badEnd.Digest[0] ^= 1
	for _, tc := range []struct {
		name   string
		prefix int
		bad    Frame
	}{
		{"start_before_fetch", 0, frames[2]},
		{"repeated_initialize", 1, frames[0]},
		{"part_without_credit", 3, frames[4]},
		{"wrong_part_index", 4, Frame{Kind: ResponsePart, Index: 1, Body: []byte("0123456")}},
		{"short_nonfinal_part", 4, Frame{Kind: ResponsePart, Body: []byte("012345")}},
		{"end_too_early", 5, frames[7]},
		{"end_wrong_digest", 7, badEnd},
		{"unused_field", 0, Frame{Kind: Initialize, Body: []byte("{}"), MAC: [32]byte{1}}},
		{"invalid_utf8", 0, Frame{Kind: Initialize, Body: []byte{0xff}}},
		{"child_frame", 1, frames[1]},
		{"unknown_kind", 0, Frame{Kind: 255}},
		{"first_over_body", 0, Frame{Kind: Initialize, Body: bytes.Repeat([]byte("x"), int(l.InitializeBytes)+1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := sequencePrefix(t, l, frames[:tc.prefix])
			preflightUnchanged(t, s, tc.bad, false)
			// Mandatory owner behavior: this is not a retryable frame probe.
			s.Abort()
			if s.Accept(ParentToChild, frames[0]) == nil || s.Finish() == nil {
				t.Fatal("owning seam failed to make preflight denial irreversible")
			}
			preflightUnchanged(t, s, Frame{Kind: Retire}, true)
			if err := s.Accept(ParentToChild, Frame{Kind: Retire}); err != nil {
				t.Fatal("failure prevented bounded cleanup DATA:", err)
			}
			if s.Finish() == nil {
				t.Fatal("cleanup repaired failed stream")
			}
		})
	}
}

func TestPreflightWritePendingConflictAndNonqueueingEntry(t *testing.T) {
	l := testLimits()
	init := testFrames()[0]
	for _, name := range []string{"header", "metadata", "gate", "retired", "aborted"} {
		t.Run(name, func(t *testing.T) {
			s := newSequence(t, l)
			f := init
			if name == "header" || name == "metadata" {
				wire := encode(t, init, l)
				h, err := s.BeginHeader(wire[:HeaderBytes], ParentToChild)
				if err != nil {
					t.Fatal(err)
				}
				if name == "metadata" {
					if err := s.PreflightMetadata(wire[HeaderBytes : HeaderBytes+int(h.MetadataBytes)]); err != nil {
						t.Fatal(err)
					}
				}
				f = Frame{Kind: Retire} // valid cleanup, but not during a preflight transaction
			} else if name == "gate" {
				s.state.gate.Lock() // deterministic contention, no scheduler/sleep
				defer s.state.gate.Unlock()
			} else if name == "retired" {
				if err := s.Accept(ParentToChild, Frame{Kind: Retire}); err != nil {
					t.Fatal(err)
				}
			} else {
				s.Abort()
			}
			preflightUnchanged(t, s, f, false)
		})
	}
	for _, s := range []*Sequence{nil, {}} {
		if err := s.PreflightWrite(init); err == nil {
			t.Fatal("nil/zero sequence returned a usable preflight")
		}
	}
}

func TestPreflightWriteActualCommitRevalidatesAndHashesActualParts(t *testing.T) {
	l := testLimits()
	frames := streamFrames(l, []byte("0123456789"))
	s := newSequence(t, l)
	preflightUnchanged(t, s, frames[0], true)
	changed := frames[0]
	changed.MAC[0] = 1
	if err := s.Accept(ParentToChild, changed); err == nil || !s.state.failed.Load() {
		t.Fatal("actual commit trusted a prior successful preflight instead of revalidation")
	}
	s = newSequence(t, l)
	for _, f := range frames[:7] {
		if peer(f.Kind) == ParentToChild {
			preflightUnchanged(t, s, f, true)
		}
		if f.Kind == ResponsePart && f.Index == 0 {
			// Model different actual committed bytes, not an actual pipe test.
			f.Body = bytes.Clone(f.Body)
			f.Body[0] ^= 1
		}
		if err := s.Accept(peer(f.Kind), f); err != nil {
			t.Fatal("failure before intended actual digest check:", err)
		}
	}
	preflightUnchanged(t, s, frames[7], false)
	if err := s.Accept(ParentToChild, frames[7]); err == nil {
		t.Fatal("matching End declarations hid corruption of actual committed part bytes")
	}
}

func TestPreflightWriteNoPostPreparationPayloadAllocation(t *testing.T) {
	l := testLimits()
	frames := streamFrames(l, []byte("0123456789"))
	for _, prefix := range []int{0, 2, 4, 7} {
		s := sequencePrefix(t, l, frames[:prefix])
		f := frames[prefix]
		if n := testing.AllocsPerRun(100, func() {
			if err := s.PreflightWrite(f); err != nil {
				panic("fixed valid preflight failed")
			}
		}); n != 0 {
			t.Fatalf("kind=%d preflight allocations=%v want0 after preparation", f.Kind, n)
		}
	}
}
