package pisdkhttpwire

import (
	"bytes"
	"crypto/sha256"
	"math"
	"testing"
)

func streamFrames(l Limits, payload []byte) []Frame {
	a, err := Measure(l)
	if err != nil || len(payload) == 0 {
		panic("invalid fixed test stream")
	}
	d := sha256.Sum256(payload)
	n := uint32(1 + (uint64(len(payload))-1)/a.ChunkBytes)
	frames := []Frame{
		{Kind: Initialize, Profile: [32]byte{3}, Body: []byte("{}")},
		{Kind: FetchRequest, Profile: [32]byte{3}, Headers: requestHeaders(), Body: []byte("{}")},
		{Kind: ResponseStart, Count: n, Bytes: uint64(len(payload)), Digest: d, Headers: responseHeaders()},
	}
	for i, offset := uint32(0), 0; offset < len(payload); i++ {
		end := min(len(payload), offset+int(a.ChunkBytes))
		frames = append(frames, Frame{Kind: Pull, Index: i}, Frame{Kind: ResponsePart, Index: i, Body: payload[offset:end]})
		offset = end
	}
	return append(frames, Frame{Kind: ResponseEnd, Count: n, Bytes: uint64(len(payload)), Digest: d}, Frame{Kind: StructuredFinal, Body: []byte("{}")}, Frame{Kind: Retire})
}

func newSequence(t *testing.T, l Limits) *Sequence {
	t.Helper()
	s, err := NewSequence(l)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSequenceOpaqueUTF8ChunksAndEndBeforeFinal(t *testing.T) {
	l := testLimits()
	l.ResponseChunkBytes = 2
	frames := streamFrames(l, []byte("data: €\n\n"))
	s := newSequence(t, l)
	for i, frame := range frames {
		wire := encode(t, frame, l)
		f, err := DecodeView(wire, peer(frame.Kind), l)
		if err != nil {
			t.Fatalf("decode frame%d: %v", i, err)
		}
		if err := s.Accept(peer(f.Kind), f); err != nil {
			t.Fatalf("accept frame%d kind%d: %v", i, f.Kind, err)
		}
	}
	if err := s.Finish(); err != nil {
		t.Fatal("valid child wire EOF denied", err)
	}
	if err := s.Finish(); err == nil {
		t.Fatal("wire EOF replay accepted")
	}
}

func TestSequenceRejectsActualByteCorruption(t *testing.T) {
	l := testLimits()
	frames := streamFrames(l, []byte("data: original\n\n"))
	s := newSequence(t, l)
	for i, f := range frames {
		if f.Kind == ResponsePart && f.Index == 0 {
			f.Body = bytes.Clone(f.Body)
			f.Body[0] ^= 1
		}
		err := s.Accept(peer(f.Kind), f)
		if f.Kind == ResponseEnd {
			if err == nil {
				t.Fatal("changed actual byte accepted despite matching start/end metadata")
			}
			if err := s.Accept(ChildToParent, Frame{Kind: StructuredFinal, Body: []byte("{}")}); err == nil {
				t.Fatal("hash failure became valid final")
			}
			return
		}
		if err != nil {
			t.Fatalf("frame%d failed before intended actual digest comparison: %v", i, err)
		}
	}
	t.Fatal("no response end reached")
}

func TestSequenceInvalidTransitionsBurnAndRetire(t *testing.T) {
	l := testLimits()
	frames := streamFrames(l, []byte("0123456789"))
	// Prefix lengths deliberately include the exact phase before each defect.
	for _, tc := range []struct {
		name   string
		prefix int
		bad    Frame
	}{
		{"fetch_before_init", 0, frames[1]},
		{"repeated_init", 1, frames[0]},
		{"foreign_profile", 1, Frame{Kind: FetchRequest, Profile: [32]byte{4}, Headers: requestHeaders(), Body: []byte("{}")}},
		{"repeated_fetch", 2, frames[1]},
		{"pull_during_quarantine", 2, Frame{Kind: Pull}},
		{"part_without_credit", 3, frames[4]},
		{"skipped_pull", 3, Frame{Kind: Pull, Index: 1}},
		{"duplicate_pull", 4, frames[3]},
		{"wrong_part_index", 4, Frame{Kind: ResponsePart, Index: 1, Body: []byte("0123456")}},
		{"short_nonfinal_part", 4, Frame{Kind: ResponsePart, Body: []byte("012345")}},
		{"end_too_early", 5, frames[7]},
		{"speculative_extra_pull", 7, Frame{Kind: Pull, Index: 2}},
		{"early_final", 7, frames[8]},
		{"duplicate_final", 9, frames[8]},
		{"typed_unused_field", 0, Frame{Kind: Initialize, Body: []byte("{}"), MAC: [32]byte{1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSequence(t, l)
			for i, f := range frames[:tc.prefix] {
				if err := s.Accept(peer(f.Kind), f); err != nil {
					t.Fatalf("prefix%d: %v", i, err)
				}
			}
			if err := s.Accept(peer(tc.bad.Kind), tc.bad); err == nil {
				t.Fatal("invalid transition accepted")
			}
			if err := s.Accept(ParentToChild, frames[0]); err == nil {
				t.Fatal("failed stream reset to initialize")
			}
			if err := s.Accept(ParentToChild, Frame{Kind: Retire}); err != nil {
				t.Fatal("failure prevented bounded retire", err)
			}
			if err := s.Finish(); err == nil {
				t.Fatal("failed retirement became success")
			}
		})
	}
}

func TestSequenceEndMetadataAndPrematureEOF(t *testing.T) {
	l := testLimits()
	frames := streamFrames(l, []byte("0123456789"))
	for name, change := range map[string]func(*Frame){
		"digest": func(f *Frame) { f.Digest[0] ^= 1 },
		"bytes":  func(f *Frame) { f.Bytes++ },
		"count":  func(f *Frame) { f.Count++ },
	} {
		t.Run(name, func(t *testing.T) {
			s := newSequence(t, l)
			for _, f := range frames[:7] {
				if err := s.Accept(peer(f.Kind), f); err != nil {
					t.Fatal(err)
				}
			}
			f := frames[7]
			change(&f)
			if s.Accept(ParentToChild, f) == nil {
				t.Fatal("mismatched response end accepted")
			}
		})
	}
	for prefix := 0; prefix < 9; prefix++ {
		s := newSequence(t, l)
		for _, f := range frames[:prefix] {
			if err := s.Accept(peer(f.Kind), f); err != nil {
				t.Fatal(err)
			}
		}
		if s.Finish() == nil {
			t.Fatalf("EOF accepted before final atprefix%d", prefix)
		}
	}
}

func TestSequenceCopiesShareFailureAndAbortDoesNotQueue(t *testing.T) {
	l := testLimits()
	s := newSequence(t, l)
	copyOf := *s
	s.Abort()
	if copyOf.Accept(ParentToChild, testFrames()[0]) == nil || copyOf.Finish() == nil {
		t.Fatal("copied handle escaped original abort")
	}
	s = newSequence(t, l)
	s.state.gate.Lock() // deterministic concurrent-entry barrier; no clock/sleep
	err := s.Accept(ParentToChild, testFrames()[0])
	s.Abort()
	s.state.gate.Unlock()
	if err == nil || s.Accept(ParentToChild, testFrames()[0]) == nil {
		t.Fatal("concurrent entry queued or did not burn stream")
	}
	if new(Sequence).Finish() == nil || new(Sequence).Accept(ParentToChild, testFrames()[0]) == nil {
		t.Fatal("zero sequence became valid")
	}
}

func TestMeasureExactBoundsAndOneOver(t *testing.T) {
	l := testLimits()
	a, err := Measure(l)
	if err != nil || a.ChunkBytes != 7 || a.Chunks != 10 || a.Frames != 26 ||
		a.ParentBytes != 128+64+128+254+210 || a.ChildBytes != 256+128+512+102+210 ||
		a.TotalBytes != a.ParentBytes+a.ChildBytes {
		t.Fatalf("wire totals %+v err=%v", a, err)
	}
	l.ParentBytes, l.ChildBytes, l.Frames = a.ParentBytes, a.ChildBytes, a.Frames
	if _, err := Measure(l); err != nil {
		t.Fatal("exact directional/event fit denied", err)
	}
	for _, field := range []string{"parent", "child", "frames"} {
		bad := l
		switch field {
		case "parent":
			bad.ParentBytes--
		case "child":
			bad.ChildBytes--
		case "frames":
			bad.Frames--
		}
		if _, err := Measure(bad); err == nil {
			t.Fatalf("first-over %s accepted", field)
		}
	}
	for _, change := range []func(*Limits){
		func(l *Limits) { l.FrameBytes = math.MaxUint64 },
		func(l *Limits) { l.InitializeBytes = math.MaxUint64 },
		func(l *Limits) { l.RequestHeaderBytes = math.MaxUint64 },
		func(l *Limits) { l.RequestBytes = math.MaxUint64 },
		func(l *Limits) { l.ResponseHeaderBytes = math.MaxUint64 },
		func(l *Limits) { l.ResponseBytes = math.MaxUint64 },
		func(l *Limits) { l.ResponseChunkBytes = 0 },
		func(l *Limits) { l.ResponseBytes = 0 },
		func(l *Limits) { l.FinalBytes = 128<<10 + 1 },
		func(l *Limits) { l.Frames = 4097 },
		func(l *Limits) { l.ParentBytes = 4<<20 + 1 },
		func(l *Limits) { l.ChildBytes = 16<<20 + 1 },
	} {
		bad := testLimits()
		change(&bad)
		if _, err := Measure(bad); err == nil {
			t.Fatalf("invalid limits accepted: %+v", bad)
		}
		if _, err := NewSequence(bad); err == nil {
			t.Fatal("invalid limits constructed sequence")
		}
	}
	l = testLimits()
	l.ResponseChunkBytes, l.ResponseBytes = 1, 2045
	if got, err := Measure(l); err != nil || got.Frames != 4096 {
		t.Fatalf("exact4096frames: %+v %v", got, err)
	}
	l.ResponseBytes++
	if _, err := Measure(l); err == nil {
		t.Fatal("4098frames accepted")
	}
	l = testLimits()
	l.FrameBytes = 1024
	l.InitializeBytes, l.RequestBytes = 1024-113, 1024-85-l.RequestHeaderBytes
	for _, tc := range []struct {
		kind Kind
		n    int
	}{{Initialize, int(l.InitializeBytes)}, {FetchRequest, int(l.RequestBytes)}, {StructuredFinal, int(l.FinalBytes)}} {
		f := testFrames()[int(tc.kind)-1]
		f.Body = bytes.Repeat([]byte{'x'}, tc.n)
		_ = encode(t, f, l)
		f.Body = append(f.Body, 'x')
		if _, err := EncodeInto(make([]byte, l.FrameBytes+1), f, l); err == nil {
			t.Fatalf("kind%d first-over body accepted", tc.kind)
		}
	}
}
