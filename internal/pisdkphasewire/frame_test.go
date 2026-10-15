package pisdkphasewire

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strings"
	"testing"
)

func nativeFixture() []byte {
	// Synthetic COMPLETE history/schema/media bytes, not a serving Pi producer.
	return []byte(`{"model":"synthetic","input":[{"role":"user","content":[{"type":"input_text","text":"old history 9007199254740993"},{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]},{"role":"user","content":[{"type":"input_text","text":"новое 123"}]}],"text":{"format":{"type":"json_schema","name":"result","schema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}}},"tools":[],"store":false}`)
}
func fixtureLimits(n uint64) FrameLimits {
	return FrameLimits{InitializeBodyBytes: n, RequestBodyBytes: n, FinalBodyBytes: n, FrameBytes: n + HeaderBytes + ChildMetadataMaxBytes}
}
func childFixture() Frame {
	return Frame{Kind: PhysicalRequest, BackendProfileDigest: strings.Repeat("a", 64), Body: nativeFixture()}
}
func mustFrame(t *testing.T, f Frame, l FrameLimits) []byte {
	t.Helper()
	b, err := EncodeFrame(f, l)
	if err != nil {
		t.Fatalf("encode kind=%d limits=%+v: %v", f.Kind, l, err)
	}
	return b
}
func header(kind byte, meta uint32, body uint64) []byte {
	b := make([]byte, HeaderBytes)
	copy(b, "SPU2")
	b[4] = kind
	binary.BigEndian.PutUint32(b[5:9], meta)
	binary.BigEndian.PutUint64(b[9:17], body)
	return b
}

func TestSPU2HeaderFirstPreflight(t *testing.T) {
	l := fixtureLimits(1)
	b := header(PhysicalRequest, 129, 1)
	h, err := InspectHeader(b, l)
	if err != nil || h.Kind != PhysicalRequest || h.MetadataBytes != 129 || h.RawBodyBytes != 1 || h.TotalBytes != 147 {
		t.Fatalf("preflight=%+v err=%v", h, err)
	}
	for _, tc := range []struct {
		name string
		b    []byte
		l    FrameLimits
	}{
		{"short", b[:16], l}, {"long", append(bytes.Clone(b), 0), l},
		{"old_magic", append([]byte("SPU1"), b[4:]...), l}, {"unknown_magic", append([]byte("XXXX"), b[4:]...), l},
		{"zero_caps", b, FrameLimits{}}, {"unknown_kind", header(6, 0, 0), l},
		{"init_meta", header(Initialize, 1, 1), l}, {"empty_init", header(Initialize, 0, 0), l}, {"init_over", header(Initialize, 0, 2), l},
		{"short_request_meta", header(PhysicalRequest, 128, 1), l}, {"long_request_meta", header(PhysicalRequest, 130, 1), l},
		{"empty_request", header(PhysicalRequest, 129, 0), l}, {"request_over", header(PhysicalRequest, 129, 2), l},
		{"short_use", header(UseResult, 8, 0), l}, {"long_use", header(UseResult, 10, 0), l}, {"use_body", header(UseResult, 9, 1), l},
		{"final_meta", header(StructuredFinal, 1, 1), l}, {"empty_final", header(StructuredFinal, 0, 0), l}, {"final_over", header(StructuredFinal, 0, 2), l},
		{"retire_meta", header(Retire, 1, 0), l}, {"retire_body", header(Retire, 0, 1), l},
		{"u64_body", header(PhysicalRequest, 129, math.MaxUint64), l}, {"u32_meta", header(PhysicalRequest, math.MaxUint32, 1), l},
		{"little_endian_meta", []byte{'S', 'P', 'U', '2', 2, 129, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, l},
		{"little_endian_body", []byte{'S', 'P', 'U', '2', 2, 0, 0, 0, 129, 1, 0, 0, 0, 0, 0, 0, 0}, l},
	} {
		t.Run(tc.name, func(t *testing.T) { _, err := InspectHeader(tc.b, tc.l); invalid(t, err) })
	}
	for _, change := range []struct {
		name   string
		mutate func(*FrameLimits)
	}{
		{"frame_zero", func(v *FrameLimits) { v.FrameBytes = 0 }},
		{"frame_short", func(v *FrameLimits) { v.FrameBytes = 146 }},
		{"frame_over", func(v *FrameLimits) { v.FrameBytes = TransportMaxFrameBytes + 1 }},
		{"frame_overflow", func(v *FrameLimits) { v.FrameBytes = math.MaxUint64 }},
		{"init_zero", func(v *FrameLimits) { v.InitializeBodyBytes = 0 }},
		{"request_zero", func(v *FrameLimits) { v.RequestBodyBytes = 0 }},
		{"final_zero", func(v *FrameLimits) { v.FinalBodyBytes = 0 }},
		{"init_inconsistent", func(v *FrameLimits) { v.InitializeBodyBytes = v.FrameBytes - 16 }},
		{"request_inconsistent", func(v *FrameLimits) { v.RequestBodyBytes = v.FrameBytes - 145 }},
		{"final_inconsistent", func(v *FrameLimits) { v.FinalBodyBytes = v.FrameBytes - 16 }},
	} {
		t.Run(change.name, func(t *testing.T) { bad := l; change.mutate(&bad); _, err := InspectHeader(b, bad); invalid(t, err) })
	}
	if a := testing.AllocsPerRun(20, func() { _, _ = InspectHeader(b, l) }); a != 0 {
		t.Fatalf("header-only allocations=%v", a)
	}
}

func TestSPU2ExactCeilingAndFirstOverflow(t *testing.T) {
	l := FrameLimits{InitializeBodyBytes: TransportMaxFrameBytes - 17, RequestBodyBytes: TransportMaxFrameBytes - 146, FinalBodyBytes: TransportMaxFrameBytes - 17, FrameBytes: TransportMaxFrameBytes}
	for _, kind := range []byte{Initialize, PhysicalRequest, StructuredFinal} {
		t.Run(string(rune('0'+kind)), func(t *testing.T) {
			n := l.InitializeBodyBytes
			if kind == PhysicalRequest {
				n = l.RequestBodyBytes
			}
			f := Frame{Kind: kind, Body: bytes.Repeat([]byte("x"), int(n))}
			if kind == PhysicalRequest {
				f.BackendProfileDigest = strings.Repeat("a", 64)
			}
			b := mustFrame(t, f, l)
			if len(b) != TransportMaxFrameBytes {
				t.Fatalf("exact inherited ceiling kind=%d got=%d", kind, len(b))
			}
			got, err := DecodeFrame(b, l)
			if err != nil || !bytes.Equal(got.Body, f.Body) {
				t.Fatalf("ceiling roundtrip: %v", err)
			}
			f.Body = append(f.Body, 'x')
			_, err = EncodeFrame(f, l)
			invalid(t, err)
		})
	}
}

func TestSPU2HeaderDirectionsBeforePayload(t *testing.T) {
	l := fixtureLimits(1)
	for _, tc := range []struct {
		kind      byte
		meta      uint32
		body      uint64
		direction Direction
	}{
		{Initialize, 0, 1, ParentToChild},
		{PhysicalRequest, 129, 1, ChildToParent},
		{UseResult, 9, 0, ParentToChild},
		{StructuredFinal, 0, 1, ChildToParent},
		{Retire, 0, 0, ParentToChild},
	} {
		h, err := InspectHeader(header(tc.kind, tc.meta, tc.body), l)
		if err != nil || h.Direction != tc.direction {
			t.Fatalf("kind=%d direction=%d want=%d err=%v", tc.kind, h.Direction, tc.direction, err)
		}
		// No payload exists yet. A future owned pump must reject the other
		// endpoint here, before a region read/allocation, not after DecodeFrame.
		other := ParentToChild
		if tc.direction == ParentToChild {
			other = ChildToParent
		}
		if h.Direction == other {
			t.Fatalf("kind=%d classified as wrong peer", tc.kind)
		}
	}
}

func TestSPU2WholeFrameIdentityOwnershipAndUse(t *testing.T) {
	f := childFixture()
	l := fixtureLimits(uint64(len(f.Body)))
	cases := []Frame{
		{Kind: Initialize, Body: []byte(`{"projection":"synthetic"}`)}, f,
		{Kind: UseResult}, {Kind: UseResult, Use: UseAllowance{Allowed: true, RemainingNanoseconds: 1}},
		{Kind: UseResult, Use: UseAllowance{Allowed: true, RemainingNanoseconds: math.MaxInt64}},
		{Kind: StructuredFinal, Body: []byte(`{"text":"ok","artifacts":[]}`)}, {Kind: Retire},
	}
	for i, f := range cases {
		b := mustFrame(t, f, l)
		if string(b[:4]) != "SPU2" || b[4] != f.Kind {
			t.Fatalf("case%d header", i)
		}
		got, err := DecodeFrame(b, l)
		if err != nil || got.Kind != f.Kind || got.BackendProfileDigest != f.BackendProfileDigest || got.Use != f.Use || !bytes.Equal(got.Body, f.Body) {
			t.Fatalf("case%d roundtrip got=%+v err=%v", i, got, err)
		}
		clear(b)
		if !bytes.Equal(got.Body, f.Body) || got.BackendProfileDigest != f.BackendProfileDigest {
			t.Fatalf("case%d decode aliases frame", i)
		}
		b = mustFrame(t, f, l)
		if len(f.Body) > 0 {
			f.Body[0] ^= 1
			if bytes.Equal(b[len(b)-len(f.Body):], f.Body) {
				t.Fatalf("case%d encode aliases input", i)
			}
			f.Body[0] ^= 1
		}
		if f.Kind == UseResult {
			if len(b) != 26 || b[17] != boolByte(f.Use.Allowed) || binary.BigEndian.Uint64(b[18:26]) != f.Use.RemainingNanoseconds {
				t.Fatalf("case%d use binary partition", i)
			}
		}
	}
	b := mustFrame(t, f, l)
	if len(b) != 146+len(f.Body) || binary.BigEndian.Uint32(b[5:9]) != 129 || binary.BigEndian.Uint64(b[9:17]) != uint64(len(f.Body)) {
		t.Fatal("physical complete header/endian")
	}
	got, err := DecodeFrame(b, l)
	if err != nil {
		t.Fatal(err)
	}
	if err := CompareBodyProfile(got, f.Body, f.BackendProfileDigest); err != nil {
		t.Fatal(err)
	}
}
func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

func TestSPU2LiteralWireVectorsAndFragmentAssembly(t *testing.T) {
	l := fixtureLimits(1)
	for _, tc := range []struct {
		name, fixedHex, metadata string
		frame                    Frame
	}{
		{"init", "5350553201000000000000000000000001", "", Frame{Kind: Initialize, Body: []byte("x")}},
		{"request", "5350553202000000810000000000000001", `{"version":1,"phase":"physical_send","backend_profile_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`, Frame{Kind: PhysicalRequest, BackendProfileDigest: strings.Repeat("a", 64), Body: []byte("x")}},
		{"denied", "5350553203000000090000000000000000000000000000000000", "", Frame{Kind: UseResult}},
		{"max_use", "5350553203000000090000000000000000017fffffffffffffff", "", Frame{Kind: UseResult, Use: UseAllowance{Allowed: true, RemainingNanoseconds: math.MaxInt64}}},
		{"final", "5350553204000000000000000000000001", "", Frame{Kind: StructuredFinal, Body: []byte("x")}},
		{"retire", "5350553205000000000000000000000000", "", Frame{Kind: Retire}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := hex.DecodeString(tc.fixedHex)
			if err != nil {
				t.Fatal(err)
			}
			want = append(want, []byte(tc.metadata)...)
			want = append(want, tc.frame.Body...)
			got := mustFrame(t, tc.frame, l)
			if !bytes.Equal(got, want) {
				t.Fatalf("wire=%x want=%x", got, want)
			}
			// Legal byte boundaries do not coincide with OS reads. This checks
			// complete-frame reassembly, NOT an unimplemented stream pump.
			for split := 0; split <= len(want); split++ {
				assembled := append(bytes.Clone(want[:split]), want[split:]...)
				f, err := DecodeFrame(assembled, l)
				if err != nil || f.Kind != tc.frame.Kind || f.Use != tc.frame.Use || f.BackendProfileDigest != tc.frame.BackendProfileDigest || !bytes.Equal(f.Body, tc.frame.Body) {
					t.Fatalf("split=%d err=%v", split, err)
				}
			}
		})
	}
}

func TestSPU2FullBodyMutationIsNotIdentity(t *testing.T) {
	f := childFixture()
	expected := bytes.Clone(f.Body)
	for _, tc := range []struct{ name, old, new string }{
		{"history", "old history", "bad history"}, {"numeric", "9007199254740993", "9007199254740992"}, {"media", "base64,AAAA", "base64,BBBB"}, {"schema", `"answer"`, `"wrong!"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := f
			c.Body = bytes.Replace(f.Body, []byte(tc.old), []byte(tc.new), 1)
			if bytes.Equal(c.Body, expected) {
				t.Fatal("mutation did not change fixture")
			}
			l := fixtureLimits(uint64(len(c.Body)))
			got, err := DecodeFrame(mustFrame(t, c, l), l)
			if err != nil {
				t.Fatal(err)
			}
			invalid(t, CompareBodyProfile(got, expected, f.BackendProfileDigest))
		})
	}
	invalid(t, CompareBodyProfile(f, expected, strings.Repeat("b", 64)))
	invalid(t, CompareBodyProfile(f, nil, f.BackendProfileDigest))
	badKind := f
	badKind.Kind = Initialize
	invalid(t, CompareBodyProfile(badKind, expected, f.BackendProfileDigest))
}

func TestSPU2ClosedForms(t *testing.T) {
	f := childFixture()
	l := fixtureLimits(uint64(len(f.Body)))
	good := mustFrame(t, f, l)
	for _, b := range [][]byte{nil, good[:16], good[:len(good)-1], append(bytes.Clone(good), 0), append(bytes.Clone(good), header(Retire, 0, 0)...)} {
		_, err := DecodeFrame(b, l)
		invalid(t, err)
	}
	for _, tc := range []struct {
		name string
		f    Frame
	}{
		{"init_with_profile", Frame{Kind: Initialize, BackendProfileDigest: f.BackendProfileDigest, Body: []byte("x")}},
		{"init_with_use", Frame{Kind: Initialize, Use: UseAllowance{Allowed: true, RemainingNanoseconds: 1}, Body: []byte("x")}},
		{"request_zero_digest", Frame{Kind: PhysicalRequest, BackendProfileDigest: strings.Repeat("0", 64), Body: []byte("x")}},
		{"request_with_use", Frame{Kind: PhysicalRequest, BackendProfileDigest: f.BackendProfileDigest, Body: []byte("x"), Use: UseAllowance{Allowed: true, RemainingNanoseconds: 1}}},
		{"use_with_profile", Frame{Kind: UseResult, BackendProfileDigest: f.BackendProfileDigest}},
		{"use_with_body", Frame{Kind: UseResult, Body: []byte("x")}},
		{"allowed_zero", Frame{Kind: UseResult, Use: UseAllowance{Allowed: true}}},
		{"denied_positive", Frame{Kind: UseResult, Use: UseAllowance{RemainingNanoseconds: 1}}},
		{"allowed_overflow", Frame{Kind: UseResult, Use: UseAllowance{Allowed: true, RemainingNanoseconds: math.MaxInt64 + 1}}},
		{"final_with_profile", Frame{Kind: StructuredFinal, BackendProfileDigest: f.BackendProfileDigest, Body: []byte("x")}},
		{"final_with_use", Frame{Kind: StructuredFinal, Body: []byte("x"), Use: UseAllowance{Allowed: true, RemainingNanoseconds: 1}}},
		{"retire_with_profile", Frame{Kind: Retire, BackendProfileDigest: f.BackendProfileDigest}},
		{"retire_with_use", Frame{Kind: Retire, Use: UseAllowance{Allowed: true, RemainingNanoseconds: 1}}},
		{"retire_with_body", Frame{Kind: Retire, Body: []byte("x")}}, {"unknown", Frame{Kind: 6}},
	} {
		t.Run(tc.name, func(t *testing.T) { _, err := EncodeFrame(tc.f, l); invalid(t, err) })
	}
	for _, kind := range []byte{Initialize, PhysicalRequest, StructuredFinal} {
		c := Frame{Kind: kind, Body: []byte{0xff}}
		if kind == PhysicalRequest {
			c.BackendProfileDigest = f.BackendProfileDigest
		}
		_, err := EncodeFrame(c, l)
		invalid(t, err)
		c.Body = []byte("x")
		b := mustFrame(t, c, l)
		b[len(b)-1] = 0xff
		_, err = DecodeFrame(b, l)
		invalid(t, err)
	}
	meta := good[17:146]
	for _, change := range []struct{ name, old, new string }{
		{"version", `"version":1`, `"version":2`}, {"wrong_phase", "physical_send", "pre_issue"},
		{"duplicate", `"version":1,`, `"version":1,"version":1,`}, {"escaped", `"phase":`, `"\u0070hase":`},
		{"null_digest", `"backend_profile_digest":"` + strings.Repeat("a", 64) + `"`, `"backend_profile_digest":null`},
		{"zero_digest", strings.Repeat("a", 64), strings.Repeat("0", 64)},
		{"uppercase_digest", strings.Repeat("a", 64), strings.Repeat("A", 64)},
		{"nonhex_digest", strings.Repeat("a", 64), strings.Repeat("g", 64)},
	} {
		t.Run(change.name, func(t *testing.T) {
			badMeta := replace(t, meta, change.old, change.new)
			b := header(PhysicalRequest, uint32(len(badMeta)), uint64(len(f.Body)))
			b = append(b, badMeta...)
			b = append(b, f.Body...)
			_, err := DecodeFrame(b, l)
			invalid(t, err)
		})
	}
	for _, tc := range []struct {
		status byte
		n      uint64
	}{{0, 1}, {1, 0}, {1, math.MaxInt64 + 1}, {2, 0}, {255, 1}} {
		b := header(UseResult, 9, 0)
		m := make([]byte, 9)
		m[0] = tc.status
		binary.BigEndian.PutUint64(m[1:], tc.n)
		b = append(b, m...)
		_, err := DecodeFrame(b, l)
		invalid(t, err)
	}
	// Text syntax/Init schema/strict final are original producer obligations, not
	// permissions invented by structural UTF8 decoding.
	mustFrame(t, Frame{Kind: Initialize, Body: []byte("é")}, l)
}

func TestSPU2WireAccountingOnly(t *testing.T) {
	i, b, l := uint64(23), uint64(len(nativeFixture())), uint64(40)
	got, err := MeasureWireAccounting(i, b, l)
	want := WireAccounting{ParentStdinBytes: i + 17 + 26 + 17, ChildStdoutBytes: b + l + 146 + 17, TotalIPCBytes: i + b + l + 223}
	if err != nil || got != want || got.TotalIPCBytes != got.ParentStdinBytes+got.ChildStdoutBytes {
		t.Fatalf("wire=%+v want=%+v err=%v", got, want, err)
	}
	got, err = MeasureWireAccounting(1, 1, math.MaxUint64-225)
	if err != nil || got.TotalIPCBytes != math.MaxUint64 {
		t.Fatalf("last checked fit=%+v err=%v", got, err)
	}
	for _, tc := range [][3]uint64{{0, 1, 1}, {1, 0, 1}, {1, 1, 0}, {1, 1, math.MaxUint64 - 224}, {math.MaxUint64, 1, 1}, {1, math.MaxUint64, 1}, {1, 1, math.MaxUint64}} {
		_, err := MeasureWireAccounting(tc[0], tc[1], tc[2])
		invalid(t, err)
	}
}

func FuzzSPU2(f *testing.F) {
	l := fixtureLimits(256)
	for _, v := range []Frame{{Kind: Initialize, Body: []byte("x")}, {Kind: PhysicalRequest, BackendProfileDigest: strings.Repeat("a", 64), Body: []byte("x")}, {Kind: UseResult}, {Kind: UseResult, Use: UseAllowance{Allowed: true, RemainingNanoseconds: 1}}, {Kind: StructuredFinal, Body: []byte("x")}, {Kind: Retire}} {
		b, err := EncodeFrame(v, l)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Add(header(PhysicalRequest, 129, math.MaxUint64))
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		if got, err := DecodeFrame(b, l); err == nil && !bytes.Equal(mustFrame(t, got, l), b) {
			t.Fatal("canonical parity")
		}
	})
}
