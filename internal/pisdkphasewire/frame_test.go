package pisdkphasewire

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"
)

func nativeFixture() []byte {
	// Synthetic complete input/history/schema/media numeric lexemes; never an
	// actual Pi serializer/profile, serving model or smaller-cohort substitute.
	return []byte(`{"model":"synthetic","input":[{"role":"user","content":[{"type":"input_text","text":"old history 9007199254740993"},{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]},{"role":"user","content":[{"type":"input_text","text":"новое 123"}]}],"text":{"format":{"type":"json_schema","name":"result","schema":{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"],"additionalProperties":false}}},"tools":[],"store":false}`)
}
func childFixture() Frame {
	return Frame{Kind: ChildRequest, BackendProfileDigest: strings.Repeat("a", 64), Body: nativeFixture()}
}
func mustFrame(t *testing.T, f Frame, cap uint64) []byte {
	t.Helper()
	b, err := EncodeFrame(f, cap)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func header(kind byte, meta uint32, body uint64) []byte {
	b := make([]byte, HeaderBytes)
	copy(b, "SPU1")
	b[4] = kind
	binary.BigEndian.PutUint32(b[5:9], meta)
	binary.BigEndian.PutUint64(b[9:17], body)
	return b
}

func TestHeaderFirstLengthPreflight(t *testing.T) {
	b := header(ChildRequest, ChildMetadataMaxBytes, 1)
	h, err := InspectHeader(b, 1)
	if err != nil || h.Kind != ChildRequest || h.MetadataBytes != 129 || h.RawBodyBytes != 1 || h.TotalBytes != 147 {
		t.Fatalf("header preflight got=%+v err=%v", h, err)
	}
	for _, tc := range []struct {
		name string
		b    []byte
		cap  uint64
	}{
		{"short", b[:16], 1}, {"long", append(append([]byte(nil), b...), 0), 1},
		{"bad_magic", append([]byte("XXXX"), b[4:]...), 1}, {"zero_cap", b, 0},
		{"unknown_kind", header(4, 0, 0), 1}, {"zero_body", header(1, 129, 0), 1}, {"first_body_over", header(1, 129, 2), 1},
		{"short_child_meta", header(1, 128, 1), 1}, {"long_child_meta", header(1, 130, 1), 1},
		{"reply_body", header(2, 19, 1), 1}, {"reply_meta_over", header(2, ReplyMaxBytes+1, 0), 1}, {"reply_meta_short", header(2, 18, 0), 1},
		{"retire_meta", header(3, 1, 0), 1}, {"retire_body", header(3, 0, 1), 1},
		{"u64_overflow", header(1, 129, math.MaxUint64), math.MaxUint64},
		{"platform_overflow", header(1, 129, uint64(^uint(0)>>1)-145), math.MaxUint64},
		{"little_endian_meta", []byte{'S', 'P', 'U', '1', 1, 129, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, 1},
		{"little_endian_body", []byte{'S', 'P', 'U', '1', 1, 0, 0, 0, 129, 1, 0, 0, 0, 0, 0, 0, 0}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) { _, err := InspectHeader(tc.b, tc.cap); invalid(t, err) })
	}
	maxInt := uint64(^uint(0) >> 1)
	got, err := InspectHeader(header(1, 129, maxInt-146), math.MaxUint64)
	if err != nil || got.TotalBytes != maxInt {
		t.Fatalf("platform exact boundary %v %+v", err, got)
	}
	// Inspect only 17 supplied bytes; no speculative body exists/gets allocated.
	if alloc := testing.AllocsPerRun(20, func() { _, _ = InspectHeader(b, 1) }); alloc != 0 {
		t.Fatalf("header-only allocations=%v", alloc)
	}
}

func TestWholeFrameIdentityAndOwnership(t *testing.T) {
	f := childFixture()
	n := uint64(len(f.Body))
	encoded := mustFrame(t, f, n)
	if len(encoded) != HeaderBytes+129+int(n) {
		t.Fatalf("frame total %d", len(encoded))
	}
	if string(encoded[:5]) != "SPU1\x01" || binary.BigEndian.Uint32(encoded[5:9]) != 129 || binary.BigEndian.Uint64(encoded[9:17]) != n {
		t.Fatal("actual endian header")
	}
	got, err := DecodeFrame(encoded, n)
	if err != nil || got.Kind != f.Kind || got.BackendProfileDigest != f.BackendProfileDigest || !bytes.Equal(got.Body, f.Body) {
		t.Fatalf("full raw identity %v", err)
	}
	if err = CompareBodyProfile(got, f.Body, f.BackendProfileDigest); err != nil {
		t.Fatal(err)
	}
	clear(encoded)
	if !bytes.Equal(got.Body, f.Body) || got.BackendProfileDigest != f.BackendProfileDigest {
		t.Fatal("decoded frame aliases input")
	}
	encoded = mustFrame(t, f, n)
	f.Body[0] = '!'
	if encoded[HeaderBytes+129] != '{' {
		t.Fatal("encoded frame aliases input")
	}
	_, reply := fixture(t)
	for _, r := range []Reply{reply, {Status: "denied"}} {
		b := mustFrame(t, Frame{Kind: ParentReply, Reply: r}, 1)
		decoded, err := DecodeFrame(b, 1)
		if err != nil || decoded.Reply != r || len(decoded.Body) != 0 {
			t.Fatalf("reply frame %v", err)
		}
	}
	retire := mustFrame(t, Frame{Kind: ParentRetire}, 1)
	if len(retire) != HeaderBytes {
		t.Fatal("retire header-only")
	}
	decoded, err := DecodeFrame(retire, 1)
	if err != nil || decoded.Kind != ParentRetire || decoded.Reply != (Reply{}) || decoded.Body != nil {
		t.Fatalf("retire %v", err)
	}
}

func TestFullBodyMutationIsNotOriginalIdentity(t *testing.T) {
	f := childFixture()
	expected := append([]byte(nil), f.Body...)
	for _, tc := range []struct{ name, old, new string }{
		{"history", "old history", "bad history"}, {"numeric", "9007199254740993", "9007199254740992"},
		{"media", "base64,AAAA", "base64,BBBB"}, {"schema", `"answer"`, `"wrong!"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := f
			c.Body = bytes.Replace(f.Body, []byte(tc.old), []byte(tc.new), 1)
			got, err := DecodeFrame(mustFrame(t, c, uint64(len(c.Body))), uint64(len(c.Body)))
			if err != nil {
				t.Fatal(err)
			}
			// Lawful UTF8 is structural; only full byte comparison rejects mutation.
			invalid(t, CompareBodyProfile(got, expected, f.BackendProfileDigest))
		})
	}
	invalid(t, CompareBodyProfile(f, expected, strings.Repeat("b", 64)))
	invalid(t, CompareBodyProfile(f, nil, f.BackendProfileDigest))
}

func TestFrameClosedForms(t *testing.T) {
	f := childFixture()
	n := uint64(len(f.Body))
	good := mustFrame(t, f, n)
	for _, b := range [][]byte{nil, good[:16], good[:len(good)-1], append(append([]byte(nil), good...), 0), append(append([]byte(nil), good...), header(3, 0, 0)...)} {
		_, err := DecodeFrame(b, n)
		invalid(t, err)
	}
	_, err := DecodeFrame(good, n-1)
	invalid(t, err)
	for _, tc := range []struct {
		name string
		f    Frame
		cap  uint64
	}{
		{"zero_cap", f, 0}, {"body_over", f, n - 1}, {"empty_body", Frame{Kind: 1, BackendProfileDigest: f.BackendProfileDigest}, 1},
		{"invalid_utf8", Frame{Kind: 1, BackendProfileDigest: f.BackendProfileDigest, Body: []byte{0xff}}, 1},
		{"bad_digest", Frame{Kind: 1, BackendProfileDigest: strings.Repeat("0", 64), Body: []byte("x")}, 1},
		{"reply_with_profile", Frame{Kind: 2, BackendProfileDigest: f.BackendProfileDigest, Reply: Reply{Status: "denied"}}, 1},
		{"reply_with_body", Frame{Kind: 2, Reply: Reply{Status: "denied"}, Body: []byte("x")}, 1},
		{"retire_with_reply", Frame{Kind: 3, Reply: Reply{Status: "denied"}}, 1},
		{"retire_with_body", Frame{Kind: 3, Body: []byte("x")}, 1}, {"unknown", Frame{Kind: 4}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) { _, err := EncodeFrame(tc.f, tc.cap); invalid(t, err) })
	}
	meta := good[HeaderBytes : HeaderBytes+129]
	for _, change := range []struct{ name, old, new string }{
		{"version", `"version":1`, `"version":2`}, {"wrong_phase", "physical_send", "pre_issue"},
		{"duplicate", `"version":1,`, `"version":1,"version":1,`}, {"escaped", `"phase":`, `"\u0070hase":`},
		{"null_digest", `"backend_profile_digest":"` + strings.Repeat("a", 64) + `"`, `"backend_profile_digest":null`},
	} {
		t.Run(change.name, func(t *testing.T) {
			badMeta := replace(t, meta, change.old, change.new)
			b := header(1, uint32(len(badMeta)), n)
			b = append(b, badMeta...)
			b = append(b, f.Body...)
			_, err := DecodeFrame(b, n)
			invalid(t, err)
		})
	}
	badUTF8 := append([]byte(nil), good...)
	badUTF8[len(badUTF8)-1] = 0xff
	_, err = DecodeFrame(badUTF8, n)
	invalid(t, err)
	// Bounded one-token frame does not require that native UTF8 is itself JSON;
	// exact canonical producer/body/schema enforcement belongs to original owner.
	mustFrame(t, Frame{Kind: 1, BackendProfileDigest: f.BackendProfileDigest, Body: []byte("é")}, 2)
}

func TestSharedAccountingCombinedFitAndOverflow(t *testing.T) {
	n := uint64(len(nativeFixture()))
	a, err := MeasureAccounting(n)
	if err != nil {
		t.Fatal(err)
	}
	if a.RemoteBodyBytes != 3*(RequestMaxBytes+ReplyMaxBytes) || a.IPCBytes != 129+ReplyMaxBytes+3*17+n ||
		a.ApplicationBytes != a.RemoteBodyBytes+a.IPCBytes || a.LogicalCopyBytes != 9*(RequestMaxBytes+ReplyMaxBytes)+6*129+4*ReplyMaxBytes+6*17+2*n || a.JSONNodes != 555 {
		t.Fatalf("whole accounting %+v", a)
	}
	got, err := CheckSharedRemainder(n, a.LogicalCopyBytes, a.JSONNodes)
	if err != nil || got != a {
		t.Fatalf("joint exact fit %v", err)
	}
	_, err = CheckSharedRemainder(n, a.LogicalCopyBytes-1, a.JSONNodes)
	invalid(t, err)
	_, err = CheckSharedRemainder(n, a.LogicalCopyBytes, a.JSONNodes-1)
	invalid(t, err)
	// Each appearance alone fits, while their simultaneous conservative sum does
	// not. A caller may not give every helper an independent full pool/refund.
	individual := uint64(9 * (RequestMaxBytes + ReplyMaxBytes))
	if individual >= a.LogicalCopyBytes {
		t.Fatal("fixture combined relation")
	}
	_, err = CheckSharedRemainder(n, individual, 555)
	invalid(t, err)
	max := (uint64(math.MaxUint64) - 58421) / 2
	got, err = MeasureAccounting(max)
	if err != nil || got.LogicalCopyBytes != 58421+2*max {
		t.Fatalf("checked arithmetic last fit %v", err)
	}
	for _, n := range []uint64{0, max + 1, math.MaxUint64} {
		_, err := MeasureAccounting(n)
		invalid(t, err)
	}
	for _, c := range []Counts{
		{PreIssueRequests: 2}, {PreIssueReplies: 1}, {PhysicalSendRequests: 2}, {PhysicalSendReplies: 1},
		{FinalizeRequests: 2}, {FinalizeReplies: 1}, {ChildRequests: 2}, {ChildRequests: 1}, {ParentReplies: 1}, {ParentRetires: 2},
	} {
		invalid(t, ValidateCounts(c))
	}
	if err := ValidateCounts(Counts{PreIssueRequests: 1, PreIssueReplies: 1, PhysicalSendRequests: 1, PhysicalSendReplies: 1, FinalizeRequests: 1, FinalizeReplies: 1, ChildRequests: 1, ParentReplies: 1, ParentRetires: 1}); err != nil {
		t.Fatal(err)
	}
}

func FuzzSPU1(f *testing.F) {
	f.Add(header(3, 0, 0))
	f.Add(header(1, 129, math.MaxUint64))
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		cap := uint64(256) // Synthetic fuzz fixture, NOT serving P.
		if got, err := DecodeFrame(b, cap); err == nil {
			if !bytes.Equal(mustFrame(t, got, cap), b) {
				t.Fatal("frame canonical parity")
			}
		}
	})
}
