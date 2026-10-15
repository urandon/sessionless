package pisdkhttpwire

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func testLimits() Limits {
	return Limits{FrameBytes: 4096, InitializeBytes: 128, RequestBytes: 256, FinalBytes: 128,
		RequestHeaderBytes: 512, ResponseHeaderBytes: 128, ResponseBytes: 64, ResponseChunkBytes: 7,
		ParentBytes: 4 << 20, ChildBytes: 16 << 20, Frames: 4096}
}

func headerBlock(pairs [][2]string) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(len(pairs)))
	for _, pair := range pairs {
		b = binary.BigEndian.AppendUint16(b, uint16(len(pair[0])))
		b = append(b, pair[0]...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(pair[1])))
		b = append(b, pair[1]...)
	}
	return b
}

func requestHeaders() []byte {
	// Independent contract literals, not the implementation's allowlist.
	return headerBlock([][2]string{
		{"accept", "fixture/1"}, {"content-type", "fixture/1"}, {"user-agent", "fixture/1"},
		{"x-stainless-arch", "fixture/1"}, {"x-stainless-lang", "fixture/1"}, {"x-stainless-os", "fixture/1"},
		{"x-stainless-package-version", "fixture/1"}, {"x-stainless-retry-count", "fixture/1"},
		{"x-stainless-runtime", "fixture/1"}, {"x-stainless-runtime-version", "fixture/1"}, {"x-stainless-timeout", "fixture/1"},
	})
}

func responseHeaders() []byte {
	return headerBlock([][2]string{{"content-type", "text/event-stream"}})
}

func testFrames() []Frame {
	digest := sha256.Sum256([]byte("abc"))
	return []Frame{
		{Kind: Initialize, Body: []byte("{}"), Nonce: [32]byte{1}, Binding: [32]byte{2}, Profile: [32]byte{3}},
		{Kind: FetchRequest, Profile: [32]byte{3}, MAC: [32]byte{4}, Headers: requestHeaders(), Body: []byte("{}")},
		{Kind: ResponseStart, Count: 1, Bytes: 3, Digest: digest, Headers: responseHeaders()},
		{Kind: ResponsePart, Body: []byte{0xe2, 0x82}},
		{Kind: Pull},
		{Kind: ResponseEnd, Count: 1, Bytes: 3, Digest: digest},
		{Kind: StructuredFinal, Body: []byte("{}")},
		{Kind: Retire},
	}
}

func peer(k Kind) Direction {
	if k == FetchRequest || k == Pull || k == StructuredFinal {
		return ChildToParent
	}
	return ParentToChild
}

func encode(t *testing.T, f Frame, l Limits) []byte {
	t.Helper()
	b := make([]byte, l.FrameBytes)
	n, err := EncodeInto(b, f, l)
	if err != nil {
		t.Fatalf("encode kind=%d: %v", f.Kind, err)
	}
	return b[:n]
}

func TestAllKindsGoldenHeaderAndBorrowedRoundTrip(t *testing.T) {
	l := testLimits()
	metadata := []uint32{96, uint32(68 + len(requestHeaders())), 46, 4, 4, 44, 0, 0}
	for i, f := range testFrames() {
		t.Run(string(rune('1'+i)), func(t *testing.T) {
			b := encode(t, f, l)
			body := f.Body
			if f.Kind == ResponseStart {
				body = f.Headers
			}
			var want [HeaderBytes]byte
			copy(want[:4], "SPU3")
			want[4] = byte(i + 1)
			binary.BigEndian.PutUint32(want[5:9], metadata[i])
			binary.BigEndian.PutUint64(want[9:], uint64(len(body)))
			if !bytes.Equal(b[:HeaderBytes], want[:]) {
				t.Fatalf("header=%x want=%x", b[:HeaderBytes], want)
			}
			got, err := DecodeView(b, peer(f.Kind), l)
			if err != nil || !reflect.DeepEqual(got, f) {
				t.Fatalf("roundtrip kind=%d mismatch err=%v", f.Kind, err)
			}
			if cap(got.Body) != len(got.Body) || cap(got.Headers) != len(got.Headers) {
				t.Fatal("borrowed views extend beyond declared region")
			}
			if len(got.Body) != 0 && &got.Body[0] != &b[len(b)-len(got.Body)] {
				t.Fatal("decoder cloned body instead of borrowing owned input")
			}
			if _, err := DecodeView(b, Direction(3-peer(f.Kind)), l); err == nil {
				t.Fatal("wrong pipe direction accepted")
			}
			for _, bad := range [][]byte{b[:len(b)-1], append(bytes.Clone(b), 0)} {
				got, err := DecodeView(bad, peer(f.Kind), l)
				if err == nil || !reflect.DeepEqual(got, Frame{}) {
					t.Fatal("truncated/trailing frame returned data or success")
				}
			}
		})
	}
}

func TestHeaderRejectsLengthsAndAliasesBeforeBody(t *testing.T) {
	l := testLimits()
	b := encode(t, testFrames()[1], l)
	for name, mutate := range map[string]func([]byte){
		"SPU2":              func(b []byte) { copy(b[:4], "SPU2") },
		"high_magic_bits":   func(b []byte) { b[0] |= 128 },
		"unknown_kind":      func(b []byte) { b[4] = 9 },
		"metadata_overflow": func(b []byte) { binary.BigEndian.PutUint32(b[5:9], math.MaxUint32) },
		"body_overflow":     func(b []byte) { binary.BigEndian.PutUint64(b[9:], math.MaxUint64) },
		"body_zero":         func(b []byte) { binary.BigEndian.PutUint64(b[9:], 0) },
	} {
		t.Run(name, func(t *testing.T) {
			h := bytes.Clone(b[:HeaderBytes])
			mutate(h)
			if got, err := InspectHeader(h, ChildToParent, l); err == nil || got != (Header{}) {
				t.Fatalf("malformed header accepted: %x err=%v", h, err)
			}
		})
	}
	for _, n := range []int{0, 16, 18} {
		if _, err := InspectHeader(make([]byte, n), ParentToChild, l); err == nil {
			t.Fatalf("header length %d accepted", n)
		}
	}
	bad := bytes.Clone(b)
	binary.BigEndian.PutUint32(bad[HeaderBytes+64:HeaderBytes+68], math.MaxUint32)
	if _, err := DecodeView(bad, ChildToParent, l); err == nil {
		t.Fatal("nested header length mismatch accepted")
	}
	bad = encode(t, testFrames()[2], l)
	binary.BigEndian.PutUint16(bad[HeaderBytes:], 201)
	if _, err := DecodeView(bad, ParentToChild, l); err == nil {
		t.Fatal("non200 response start accepted")
	}
}

func TestTypedFieldsAndDestinationUnchangedOnFailure(t *testing.T) {
	l := testLimits()
	for i, f := range testFrames() {
		bad := f
		bad.Index = math.MaxUint32
		dst := bytes.Repeat([]byte{0x55}, int(l.FrameBytes))
		before := bytes.Clone(dst)
		if n, err := EncodeInto(dst, bad, l); err == nil || n != 0 || !bytes.Equal(dst, before) {
			t.Fatalf("kind%d ignored invalid/unused index or changed destination", i+1)
		}
		wire := encode(t, f, l)
		short := bytes.Repeat([]byte{0x55}, len(wire)-1)
		before = bytes.Clone(short)
		if n, err := EncodeInto(short, f, l); err == nil || n != 0 || !bytes.Equal(short, before) {
			t.Fatalf("kind%d short destination changed", i+1)
		}
	}
	for _, kind := range []Kind{Initialize, FetchRequest, StructuredFinal} {
		f := testFrames()[int(kind)-1]
		f.Body = []byte{0xe2}
		if _, err := EncodeInto(make([]byte, l.FrameBytes), f, l); err == nil {
			t.Fatalf("full UTF8 body kind%d accepted a partial scalar", kind)
		}
	}
}

func TestEncodeDeniesOverlappingArenaWithoutMutation(t *testing.T) {
	l := testLimits()
	for _, offset := range []int{0, 1, HeaderBytes, 100} {
		t.Run(string(rune('a'+offset)), func(t *testing.T) {
			arena := bytes.Repeat([]byte{'x'}, int(l.FrameBytes)+200)
			f := Frame{Kind: Initialize, Body: arena[offset : offset+10]}
			before := bytes.Clone(arena)
			if n, err := EncodeInto(arena[:l.FrameBytes], f, l); err == nil || n != 0 || !bytes.Equal(arena, before) {
				t.Fatal("overlapping body accepted or arena mutated")
			}
		})
	}
	arena := bytes.Repeat([]byte{'x'}, int(l.FrameBytes)+200)
	f := Frame{Kind: Initialize, Body: arena[:128]}
	before := bytes.Clone(arena)
	if _, err := EncodeInto(arena[100:], f, l); err == nil || !bytes.Equal(arena, before) {
		t.Fatal("source containing destination start accepted")
	}
	h := requestHeaders()
	copy(arena[20:], h)
	f = Frame{Kind: FetchRequest, Body: []byte("{}"), Headers: arena[20 : 20+len(h)]}
	before = bytes.Clone(arena)
	if _, err := EncodeInto(arena, f, l); err == nil || !bytes.Equal(arena, before) {
		t.Fatal("overlapping headers accepted or mutated")
	}
}

func TestCanonicalHeaders(t *testing.T) {
	valid := requestHeaders()
	if err := ValidateHeaders(valid, uint64(len(valid)), true); err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string][]byte{
		"duplicate":      headerBlock([][2]string{{"accept", "x"}, {"accept", "y"}}),
		"descending":     headerBlock([][2]string{{"z", "x"}, {"a", "y"}}),
		"uppercase":      headerBlock([][2]string{{"Content-type", "x"}}),
		"space_name":     headerBlock([][2]string{{"bad name", "x"}}),
		"leading_space":  headerBlock([][2]string{{"a", " x"}}),
		"trailing_space": headerBlock([][2]string{{"a", "x "}}),
		"crlf":           headerBlock([][2]string{{"a", "x\r\ny"}}),
		"tab":            headerBlock([][2]string{{"a", "x\ty"}}),
		"nonascii":       headerBlock([][2]string{{"a", "é"}}),
		"empty_name":     headerBlock([][2]string{{"", "x"}}),
		"empty_value":    headerBlock([][2]string{{"a", ""}}),
	} {
		t.Run(name, func(t *testing.T) {
			if ValidateHeaders(b, 4096, false) == nil {
				t.Fatalf("bad structural block accepted: %q", b)
			}
		})
	}
	if err := ValidateHeaders(headerBlock([][2]string{{"user-agent", "OpenAI/JS 6.40.0"}}), 4096, false); err != nil {
		t.Fatal("internal ASCII space rejected", err)
	}
	for cut := 0; cut < len(valid); cut++ {
		if ValidateHeaders(valid[:cut], 4096, true) == nil {
			t.Fatalf("truncated header block at%d accepted", cut)
		}
	}
	if ValidateHeaders(append(bytes.Clone(valid), 0), 4096, true) == nil || ValidateHeaders(valid, uint64(len(valid)-1), true) == nil {
		t.Fatal("trailing or first-over header bytes accepted")
	}
	wrong := bytes.Clone(valid)
	copy(wrong[4:10], "secret") // same length, noncanonical first request name
	if ValidateHeaders(wrong, 4096, true) == nil {
		t.Fatal("request accepted an arbitrary replacement name")
	}
}

func TestFormattingRedactsPrivateWireData(t *testing.T) {
	f := Frame{Kind: Initialize, Body: []byte("private-fixture-body"), Headers: []byte("private-fixture-header"), MAC: [32]byte{7}}
	for _, output := range []string{fmt.Sprintf("%v", f), fmt.Sprintf("%+v", f), fmt.Sprintf("%#v", f), fmt.Sprintf("%#v", newSequence(t, testLimits()))} {
		if !strings.Contains(output, "[redacted]") || strings.Contains(output, "fixture") || strings.Contains(output, "state:") {
			t.Fatalf("wire formatting was not redacted: %q", output)
		}
	}
}

func FuzzDecodeView(f *testing.F) {
	for _, frame := range testFrames() {
		f.Add(encodeSeed(frame), byte(peer(frame.Kind)))
	}
	f.Add([]byte("SPU2"), byte(ParentToChild))
	f.Fuzz(func(t *testing.T, b []byte, direction byte) {
		if len(b) > 8192 {
			return
		}
		got, err := DecodeView(b, Direction(direction), testLimits())
		if err != nil {
			if !reflect.DeepEqual(got, Frame{}) {
				t.Fatal("decode failure retained partial data")
			}
			return
		}
		out := make([]byte, testLimits().FrameBytes)
		n, err := EncodeInto(out, got, testLimits())
		if err != nil || !bytes.Equal(out[:n], b) {
			t.Fatalf("accepted noncanonical bytes err=%v", err)
		}
	})
}

func encodeSeed(frame Frame) []byte {
	b := make([]byte, testLimits().FrameBytes)
	n, err := EncodeInto(b, frame, testLimits())
	if err != nil {
		panic(err) // only fixed committed test literals
	}
	return b[:n]
}
