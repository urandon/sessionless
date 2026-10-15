package pisdkhttpwire

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"testing"
)

func stagedFrame(t *testing.T, s *Sequence, f Frame, l Limits) {
	t.Helper()
	wire := encode(t, f, l)
	h, err := s.BeginHeader(wire[:HeaderBytes], peer(f.Kind))
	if err != nil {
		t.Fatalf("begin kind%d: %v", f.Kind, err)
	}
	offset := HeaderBytes + int(h.MetadataBytes)
	if err := s.PreflightMetadata(wire[HeaderBytes:offset:offset]); err != nil {
		t.Fatalf("metadata kind%d: %v", f.Kind, err)
	}
	if err := s.AcceptBody(wire[offset:len(wire):len(wire)]); err != nil {
		t.Fatalf("body kind%d: %v", f.Kind, err)
	}
}

func TestStagedCompleteStreamWithSplitUTF8(t *testing.T) {
	l := testLimits()
	l.ResponseChunkBytes = 2
	s := newSequence(t, l)
	for _, f := range streamFrames(l, []byte("data: €\n\n")) {
		stagedFrame(t, s, f, l)
	}
	if err := s.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestStagedOrderAndNestedLengthDenyWithoutBody(t *testing.T) {
	l := testLimits()
	frames := streamFrames(l, []byte("0123456789"))
	for _, tc := range []struct {
		name   string
		prefix int
		frame  Frame
	}{
		{"early_final", 0, frames[8]},
		{"fetch_before_init", 0, frames[1]},
		{"pull_during_quarantine", 2, frames[3]},
		{"part_before_credit", 3, frames[4]},
		{"duplicate_fetch", 2, frames[1]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSequence(t, l)
			for _, f := range frames[:tc.prefix] {
				stagedFrame(t, s, f, l)
			}
			// Only header bytes are supplied; no body is allocated or passed to
			// the stream gate on an illegal phase, even if its lengths fit.
			header := encode(t, tc.frame, l)[:HeaderBytes]
			if _, err := s.BeginHeader(header, peer(tc.frame.Kind)); err == nil {
				t.Fatal("illegal phase asked for metadata/body")
			}
			if s.PreflightMetadata(nil) == nil || s.AcceptBody(nil) == nil {
				t.Fatal("failed header was resumed")
			}
		})
	}
	for _, change := range []func([]byte){
		func(m []byte) { binary.BigEndian.PutUint32(m[64:68], math.MaxUint32) },
		func(m []byte) { m[0] ^= 1 },
		func(m []byte) { m[72] = 'A' }, // noncanonical request name
	} {
		s := newSequence(t, l)
		stagedFrame(t, s, frames[0], l)
		wire := encode(t, frames[1], l)
		h, err := s.BeginHeader(wire[:HeaderBytes], ChildToParent)
		if err != nil {
			t.Fatal(err)
		}
		// Independent metadata copy only: actual request body is never passed.
		m := bytes.Clone(wire[HeaderBytes : HeaderBytes+int(h.MetadataBytes)])
		change(m)
		if s.PreflightMetadata(m) == nil {
			t.Fatal("bad Hq/profile/header asked for body")
		}
		if s.AcceptBody(nil) == nil {
			t.Fatal("failed metadata was resumed")
		}
	}
}

func TestStagedPendingOwnershipAndShortBody(t *testing.T) {
	l := testLimits()
	init := testFrames()[0]
	for _, tc := range []string{"second_header", "body_before_metadata", "typed_bypass", "short_body", "repeated_metadata", "abort"} {
		t.Run(tc, func(t *testing.T) {
			s := newSequence(t, l)
			b := encode(t, init, l)
			h, err := s.BeginHeader(b[:HeaderBytes], ParentToChild)
			if err != nil {
				t.Fatal(err)
			}
			m := b[HeaderBytes : HeaderBytes+int(h.MetadataBytes)]
			var got error
			switch tc {
			case "second_header":
				_, got = s.BeginHeader(b[:HeaderBytes], ParentToChild)
			case "body_before_metadata":
				got = s.AcceptBody(init.Body)
			case "typed_bypass":
				got = s.Accept(ParentToChild, init)
			default:
				if err := s.PreflightMetadata(m); err != nil {
					t.Fatal(err)
				}
				switch tc {
				case "short_body":
					got = s.AcceptBody(init.Body[:1])
				case "repeated_metadata":
					got = s.PreflightMetadata(m)
				case "abort":
					s.Abort()
					got = s.AcceptBody(init.Body)
				}
			}
			if got == nil || s.Finish() == nil {
				t.Fatal("pending/failed stream escaped its one-frame borrow")
			}
		})
	}
}

func TestTypedPreflightUsesIntegerLengthsOnly(t *testing.T) {
	l := testLimits()
	for _, kind := range []Kind{Initialize, FetchRequest, StructuredFinal} {
		h := uint64(0)
		if kind == FetchRequest {
			h = uint64(len(requestHeaders()))
		}
		// No slice exists: the preflight cannot traverse UTF8 or header data.
		if _, err := inspectLengths(kind, math.MaxUint64, h, l); err == nil {
			t.Fatalf("kind%d accepted unchecked huge body length", kind)
		}
	}
	for _, kind := range []Kind{FetchRequest, ResponseStart} {
		if _, err := inspectLengths(kind, 0, math.MaxUint64, l); err == nil {
			t.Fatalf("kind%d accepted unchecked huge header length", kind)
		}
	}
}

func TestLiteralMetadataOffsets(t *testing.T) {
	frames := testFrames()
	for _, f := range frames {
		if f.Kind == Pull || f.Kind == ResponsePart {
			f.Index = 3
		}
		b := encode(t, f, testLimits())
		m := b[HeaderBytes : HeaderBytes+int(binary.BigEndian.Uint32(b[5:9]))]
		var want []byte
		switch f.Kind {
		case Initialize:
			want = make([]byte, 96)
			want[0], want[32], want[64] = 1, 2, 3
		case FetchRequest:
			want = make([]byte, 68)
			want[0], want[32] = 3, 4
			binary.BigEndian.PutUint32(want[64:68], uint32(len(f.Headers)))
			want = append(want, f.Headers...)
		case ResponseStart:
			want = make([]byte, 46)
			want[1], want[5], want[13] = 200, 1, 3
			d := sha256.Sum256([]byte("abc"))
			copy(want[14:46], d[:])
		case ResponsePart, Pull:
			want = []byte{0, 0, 0, 3}
		case ResponseEnd:
			want = make([]byte, 44)
			want[3], want[11] = 1, 3
			d := sha256.Sum256([]byte("abc"))
			copy(want[12:44], d[:])
		}
		if !bytes.Equal(m, want) {
			t.Fatalf("kind%d literal metadata mismatch got=%x want=%x", f.Kind, m, want)
		}
	}
}
