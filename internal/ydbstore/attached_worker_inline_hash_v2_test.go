package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

type inlineReceiptHashFixtureV2 struct {
	Version   uint64 `json:"version"`
	Candidate struct {
		Summary string   `json:"summary"`
		Names   []string `json:"names"`
	} `json:"candidate"`
	Optional *uint64 `json:"optional"`
	Empty    []int   `json:"empty"`
	Omitted  string  `json:"omitted,omitempty"`
}

func inlineReceiptHashFixtureForStringV2(value string) inlineReceiptHashFixtureV2 {
	v := inlineReceiptHashFixtureV2{Version: ^uint64(0), Empty: []int{}}
	v.Candidate.Summary = value
	v.Candidate.Names = []string{"original <name>", `"summary":"` + inlineReceiptHashMarkerV2 + `"`}
	return v
}

func inlineReceiptHashStringCasesV2() map[string]string {
	controls := make([]byte, 33)
	for i := 0; i < 32; i++ {
		controls[i] = byte(i)
	}
	controls[32] = 0x7f
	cases := map[string]string{
		"empty": "", "ASCII": "same original answer", "quote_slash": `"/\`,
		"HTML": "<>&<script>\"&", "controls": string(controls),
		"Unicode":               "é ё 😀 \u2028\u2029 �", // includes valid UTF-8 U+FFFD
		"invalid_UTF8":          string([]byte{0xff, 0xfe, 0xc0, 0xaf, 0xed, 0xa0, 0x80, 0xe2, 0x82}),
		"maximum_escaped":       strings.Repeat("<", 15_808) + strings.Repeat("\u2028", 384) + strings.Repeat("<", 15_808),
		"maximum_control_bytes": strings.Repeat("\x00", 32<<10),
	}
	for _, n := range []int{255, 256, 257, 1023, 1024, 1025, 2047, 2048} {
		cases[fmt.Sprintf("raw_boundary_%d", n)] = strings.Repeat("a", n)
	}
	for n := 1020; n <= 1026; n++ {
		cases[fmt.Sprintf("escaped_boundary_%d", n)] = strings.Repeat("a", n) + "<\u2028😀\\\x00\n" + string([]byte{0xff})
	}
	return cases
}

func TestInlineReceiptProjectedHashV2MatchesOriginalMarshal(t *testing.T) {
	cases := inlineReceiptHashStringCasesV2()
	if len(cases["maximum_escaped"]) != 32<<10 || utf8.RuneCountInString(cases["maximum_escaped"]) != 32_000 {
		t.Fatal("full original byte/rune maximum fixture changed")
	}
	for name, original := range cases {
		t.Run(name, func(t *testing.T) {
			full := inlineReceiptHashFixtureForStringV2(original)
			projected := full
			projected.Candidate.Summary = inlineReceiptHashMarkerV2
			before, err := json.Marshal(projected)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(before)
			// Independent reference: Marshal the ORIGINAL typed value, then
			// hash prefix and payload separately, never reuse the new streamer.
			encoded, err := json.Marshal(full)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(encoded)
			for _, prefix := range []string{"", "sessionless:attached-worker-inline-plan:v2\x00"} {
				h := sha256.New()
				_, _ = h.Write([]byte(prefix))
				_, _ = h.Write(encoded)
				want := hex.EncodeToString(h.Sum(nil))
				got, err := inlineReceiptProjectedHashV2(prefix, projected, "summary", original)
				if err != nil || got != want {
					t.Fatalf("original Marshal hash changed: input_bytes=%d got=%s want=%s err=%v", len(original), got, want, err)
				}
			}
			after, err := json.Marshal(projected)
			defer clear(after)
			if err != nil || !bytes.Equal(before, after) || full.Candidate.Summary != original {
				t.Fatal("hashing modified caller-owned metadata/content", err)
			}
		})
	}
}

func TestInlineReceiptProjectedHashV2SelectsExactlyOneTypedField(t *testing.T) {
	valid := inlineReceiptHashFixtureForStringV2(inlineReceiptHashMarkerV2)
	for name, input := range map[string]struct {
		projection any
		field      string
	}{
		"unknown field":      {valid, "unknown"},
		"wrong field marker": {inlineReceiptHashFixtureForStringV2("not the marker"), "summary"},
		"null optional":      {valid, "optional"},
		"multiple selected markers": {struct {
			A struct {
				Summary string `json:"summary"`
			} `json:"a"`
			B struct {
				Summary string `json:"summary"`
			} `json:"b"`
		}{A: struct {
			Summary string `json:"summary"`
		}{inlineReceiptHashMarkerV2}, B: struct {
			Summary string `json:"summary"`
		}{inlineReceiptHashMarkerV2}}, "summary"},
	} {
		t.Run(name, func(t *testing.T) {
			if digest, err := inlineReceiptProjectedHashV2("prefix", input.projection, input.field, "original"); digest != "" || !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
				t.Fatalf("missing/ambiguous typed marker accepted: digest=%s err=%v", digest, err)
			}
		})
	}
	_, err := inlineReceiptProjectedHashV2("prefix", struct{ Unsupported chan int }{}, "Unsupported", "original")
	var unsupported *json.UnsupportedTypeError
	if !errors.As(err, &unsupported) {
		t.Fatal("metadata Marshal error lost its original type", err)
	}
	// The key itself must use Marshal's default HTML spelling too.
	full := struct {
		Value string `json:"answer<>&"`
	}{Value: "original <answer>"}
	projected := full
	projected.Value = inlineReceiptHashMarkerV2
	raw, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	want := sha256.Sum256(raw)
	got, err := inlineReceiptProjectedHashV2("", projected, "answer<>&", full.Value)
	if err != nil || got != hex.EncodeToString(want[:]) {
		t.Fatal("selected JSON key escaping changed original hash", err)
	}
}

type inlineReceiptJSONCollectorV2 struct {
	body     bytes.Buffer
	chunks   []int
	borrowed []byte
}

func (w *inlineReceiptJSONCollectorV2) Write(p []byte) (int, error) {
	w.chunks = append(w.chunks, len(p))
	w.borrowed = p
	return w.body.Write(p)
}

func TestInlineReceiptJSONStringV2MatchesMarshalInBoundedChunks(t *testing.T) {
	for name, original := range inlineReceiptHashStringCasesV2() {
		t.Run(name, func(t *testing.T) {
			want, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			defer clear(want)
			var writer inlineReceiptJSONCollectorV2
			if err := streamInlineReceiptJSONStringV2(&writer, original); err != nil || !bytes.Equal(want, writer.body.Bytes()) {
				t.Fatalf("quoted string differs from default Marshal: bytes=%d err=%v", len(original), err)
			}
			for _, chunk := range writer.chunks {
				if chunk == 0 || chunk > 1024 {
					t.Fatalf("stream emitted chunk=%d outside fixed scratch bound", chunk)
				}
			}
			if len(writer.chunks) == 0 || !bytes.Equal(writer.borrowed, make([]byte, len(writer.borrowed))) {
				t.Fatal("stream retained uncleared owned scratch after return")
			}
		})
	}
}

var errInlineReceiptJSONWriteV2 = errors.New("controlled inline JSON writer failure")

type inlineReceiptJSONFaultWriterV2 struct {
	failAt int
	short  bool
	calls  int
}

func (w *inlineReceiptJSONFaultWriterV2) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		if w.short {
			return len(p) - 1, nil
		}
		return len(p) / 2, errInlineReceiptJSONWriteV2
	}
	return len(p), nil
}

func TestInlineReceiptJSONStringV2PreservesWriteFailuresWithoutRetry(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("write_%d_short_%t", failAt, short), func(t *testing.T) {
				writer := inlineReceiptJSONFaultWriterV2{failAt: failAt, short: short}
				err := streamInlineReceiptJSONStringV2(&writer, strings.Repeat("<", 1024))
				want := errInlineReceiptJSONWriteV2
				if short {
					want = io.ErrShortWrite
				}
				if !errors.Is(err, want) || writer.calls != failAt {
					t.Fatalf("stream hid/retried writer failure: calls=%d want_calls=%d err=%v", writer.calls, failAt, err)
				}
			})
		}
	}
}

func TestInlineReceiptJSONStringV2MaximumStreamAllocation(t *testing.T) {
	original := strings.Repeat("\x00<>&\"\\\n", (32<<10)/7) + strings.Repeat("x", (32<<10)%7)
	if len(original) != 32<<10 {
		t.Fatal("maximum stream allocation fixture changed")
	}
	allocations := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := streamInlineReceiptJSONStringV2(io.Discard, original); err != nil {
				b.Fatal(err)
			}
		}
	})
	// STREAM ONLY: no metadata Marshal, fingerprint, SQL, pool-history or
	// whole-phase 1 MiB proof is inferred from this bounded scratch check.
	if got := allocations.AllocedBytesPerOp(); got >= 32<<10 {
		t.Fatalf("stream allocated content-sized buffers: got=%d limit_less_than=%d", got, 32<<10)
	}
}

func FuzzInlineReceiptJSONStringV2MarshalParity(f *testing.F) {
	for _, value := range inlineReceiptHashStringCasesV2() {
		f.Add(value)
	}
	allBytes := make([]byte, 256)
	for i := range allBytes {
		allBytes[i] = byte(i)
	}
	f.Add(string(allBytes))
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 32<<10 {
			t.Skip("bounded string-encoder fuzz input")
		}
		want, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		defer clear(want)
		var got bytes.Buffer
		if err := streamInlineReceiptJSONStringV2(&got, value); err != nil || !bytes.Equal(want, got.Bytes()) {
			t.Fatalf("Marshal parity changed: input_bytes=%d err=%v", len(value), err)
		}
	})
}
