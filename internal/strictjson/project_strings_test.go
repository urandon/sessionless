package strictjson

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestProjectStringsPreservesShapeAndOriginalLexemes(t *testing.T) {
	raw := []byte(" \n" + `{"cand\u0069date":{"summary":"<\u2028>"},"events":[{"display_text":" \ud83d\ude00 "}],"n":9007199254740993123456789,"nil":null,"empty":[]}` + " \t")
	before := bytes.Clone(raw)
	fields := []StringProjection{{Path: []string{"candidate", "summary"}, MaxBytes: 5}, {Path: []string{"events", "0", "display_text"}, MaxBytes: 6}}
	metadata, original, err := ProjectStrings(raw, 1024, fields)
	want := bytes.Replace(raw, []byte(`"<\u2028>"`), []byte(`"x"`), 1)
	want = bytes.Replace(want, []byte(`" \ud83d\ude00 "`), []byte(`"x"`), 1)
	if err != nil || !bytes.Equal(metadata, want) || len(original) != 2 || len(metadata) != cap(metadata) {
		t.Fatalf("projection metadata=%s fields=%d err=%v", metadata, len(original), err)
	}
	for i, token := range [][]byte{[]byte(`"<\u2028>"`), []byte(`" \ud83d\ude00 "`)} {
		offset := bytes.Index(raw, token)
		if !bytes.Equal(original[i], token) || &original[i][0] != &raw[offset] {
			t.Fatalf("field %d lost original borrowed lexeme", i)
		}
	}
	if !bytes.Equal(raw, before) {
		t.Fatal("projection changed original document")
	}
}

func FuzzProjectStringsPreservesDocument(f *testing.F) {
	for _, seed := range []string{
		`{"a":"<\u2028>"}`, `{"a":""}`, `{"a":null}`, `{"a":["x"]}`,
		`{"a":"\ud83d\ude00","n":9007199254740993123456789}`, `{"a":"x","\u0061":"y"}`,
		`{"a":"\ud800"}`, `{"nested":{"a":"x"},"a":"x"}`, `{"a":"x","n":1e999}`,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 4096 {
			return
		}
		before := bytes.Clone(raw)
		metadata, original, err := ProjectStrings(raw, 4096, []StringProjection{{Path: []string{"a"}, MaxBytes: 4096}})
		if err != nil {
			return
		}
		if ValidateDocument(metadata, 4096, 64, 4096) != nil {
			t.Fatal("projection generated invalid metadata")
		}
		decode := func(input []byte) any {
			d := json.NewDecoder(bytes.NewReader(input))
			d.UseNumber()
			var value any
			if err := d.Decode(&value); err != nil {
				t.Fatal("valid document failed oracle decode", err)
			}
			return value
		}
		want, got := decode(raw), decode(metadata)
		if original[0] != nil {
			var value string
			object, ok := got.(map[string]any)
			if !ok || json.Unmarshal(original[0], &value) != nil || object["a"] != "x" {
				t.Fatal("projection lost exact selected original string")
			}
			object["a"] = value
		}
		if !reflect.DeepEqual(want, got) || !bytes.Equal(before, raw) {
			t.Fatal("projection changed document semantics or original bytes")
		}
	})
}

func TestProjectStringsByteAndDocumentBounds(t *testing.T) {
	for _, token := range []string{`"ёж"`, `"\u0451\u0436"`, `"\ud83d\ude00"`, `"a\n\t\""`} {
		t.Run(token, func(t *testing.T) {
			var value string
			if json.Unmarshal([]byte(token), &value) != nil {
				t.Fatal("invalid test lexeme")
			}
			raw := []byte(`{"a":` + token + `}`)
			for _, extra := range []int{-1, 0} {
				metadata, original, err := ProjectStrings(raw, 1024, []StringProjection{{Path: []string{"a"}, MaxBytes: uint64(len(value) + extra)}})
				if extra == 0 && (err != nil || string(metadata) != `{"a":"x"}` || len(original) != 1) || extra < 0 && (err == nil || metadata != nil || original != nil) {
					t.Fatalf("decoded bytes=%d extra=%d err=%v", len(value), extra, err)
				}
			}
		})
	}
	for _, token := range []string{`""`, `null`, `[]`, `{}`, `2`} {
		raw := []byte(`{"a":` + token + `}`)
		metadata, original, err := ProjectStrings(raw, 1024, []StringProjection{{Path: []string{"a"}, MaxBytes: 32}})
		if err != nil || !bytes.Equal(metadata, raw) || original[0] != nil {
			t.Fatalf("empty/non-string shape changed: input=%s metadata=%s err=%v", raw, metadata, err)
		}
	}
	for _, bad := range []string{`{"a":"x","\u0061":"y"}`, `{"a":"\ud800"}`, `{"a":"x"}true`} {
		if _, _, err := ProjectStrings([]byte(bad), 1024, []StringProjection{{Path: []string{"a"}, MaxBytes: 32}}); err == nil {
			t.Fatalf("malformed or duplicate document accepted: %s", bad)
		}
	}
	raw := []byte(` { "a" : "\u0061", "n" : 1e999 } `)
	want := bytes.Replace(raw, []byte(`"\u0061"`), []byte(`"x"`), 1)
	for _, limit := range []int{len(raw) - 1, len(raw)} {
		metadata, _, err := ProjectStrings(raw, uint64(limit), []StringProjection{{Path: []string{"a"}, MaxBytes: 1}})
		if limit == len(raw) && (err != nil || !bytes.Equal(metadata, want)) || limit < len(raw) && err == nil {
			t.Fatalf("document bound=%d metadata bytes=%d err=%v", limit, len(metadata), err)
		}
	}
}

func TestProjectStringsPreservesLargeUnselectedStrings(t *testing.T) {
	raw := []byte(` { "a":"original", "opaque":"` + strings.Repeat(`\u0061`, 24_000) + `", "` + strings.Repeat(`\u0062`, 5000) + `":1e999 } `)
	metadata, original, err := ProjectStrings(raw, uint64(len(raw)), []StringProjection{{Path: []string{"a"}, MaxBytes: 8}})
	want := bytes.Replace(raw, []byte(`"original"`), []byte(`"x"`), 1)
	if err != nil || !bytes.Equal(metadata, want) || len(original) != 1 || string(original[0]) != `"original"` || cap(metadata) != len(want) {
		t.Fatal("large unselected value/key spelling changed or introduced a hidden limit", err)
	}
}

func TestProjectStringsRejectsAmbiguousConfiguration(t *testing.T) {
	for _, fields := range [][]StringProjection{
		nil,
		{{Path: nil, MaxBytes: 1}},
		{{Path: []string{"a"}, MaxBytes: 1025}},
		{{Path: []string{"a"}, MaxBytes: 1}, {Path: []string{"a"}, MaxBytes: 1}},
		{{Path: []string{"a"}, MaxBytes: 1}, {Path: []string{"a", "b"}, MaxBytes: 1}},
		{{Path: []string{"a", "b"}, MaxBytes: 1}, {Path: []string{"a"}, MaxBytes: 1}},
	} {
		if _, _, err := ProjectStrings([]byte(`{"a":"x"}`), 1024, fields); err == nil {
			t.Fatal("ambiguous/invalid projection configuration was accepted")
		}
	}
}

func TestProjectStringEncodingLengthMatchesJSON(t *testing.T) {
	values := []string{"", "ёж", "😀", "\u2028\u2029", "<>&/\\\"", strings.Repeat("a", 160)}
	for r := rune(0); r < 128; r++ {
		values = append(values, string(r))
	}
	for _, value := range values {
		encoded, err := json.Marshal(value)
		if err != nil || decodedStringBytes(encoded) != uint64(len(value)) {
			t.Fatalf("JSON string length mismatch for %q: decoded=%d err=%v", value, decodedStringBytes(encoded), err)
		}
	}
}
