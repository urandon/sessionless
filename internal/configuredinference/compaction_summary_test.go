package configuredinference

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func summaryReferences() []CompactionReferenceV1 {
	return []CompactionReferenceV1{
		{SourceID: strings.Repeat("1", 64), Kind: CompactionCanonicalReferenceV1},
		{SourceID: strings.Repeat("2", 64), Kind: CompactionOperationReferenceV1},
		{SourceID: strings.Repeat("3", 64), Kind: CompactionArtifactReferenceV1, Digest: strings.Repeat("a", 64), MediaType: "text/plain", Size: 7, OmissionAllowed: true},
		{SourceID: strings.Repeat("4", 64), Kind: CompactionArtifactReferenceV1, Digest: strings.Repeat("b", 64), MediaType: "image/png", Size: 9},
	}
}

func summaryBody() []byte {
	return []byte(`{"schema":"sessionless.compaction-summary.v1","task":[{"text":"Keep the original task","source_ids":["` + strings.Repeat("1", 64) + `"]}],"constraints":[],"facts":[{"text":"The observed value is seven","source_ids":["` + strings.Repeat("2", 64) + `"]}],"unresolved":[],"artifacts":[{"source_id":"` + strings.Repeat("4", 64) + `","digest":"` + strings.Repeat("b", 64) + `","media_type":"image/png","size":9}],"omissions":[{"source_id":"` + strings.Repeat("3", 64) + `","reason":"profile_reference_only"}]}`)
}

func TestCompactionSummaryStrictPositiveAndOwnedBytes(t *testing.T) {
	body, refs := summaryBody(), summaryReferences()
	original := bytes.Clone(body)
	summary, err := DecodeCompactionSummaryV1(body, refs)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	if !bytes.Equal(summary.Body(), body) || summary.Digest() != hex.EncodeToString(sum[:]) {
		t.Fatal("summary changed exact original bytes/digest")
	}
	clear(body)
	refs[0].SourceID = "changed"
	clear(summary.Body())
	if !bytes.Equal(summary.Body(), original) {
		t.Fatal("caller mutation changed private summary")
	}
	for _, value := range []any{summary, *summary} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
			if text := fmt.Sprintf(format, value); !strings.Contains(text, "[redacted]") || strings.Contains(text, "observed value") {
				t.Fatalf("format %s disclosed summary", format)
			}
		}
		if raw, err := json.Marshal(value); !errors.Is(err, ErrContract) || raw != nil {
			t.Fatal("summary acquired generic JSON transport")
		}
	}
}

func TestCompactionSummaryRejectsMalformedOrUnsubstantiatedData(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(string) string
	}{
		{"unknown", func(s string) string { return strings.TrimSuffix(s, "}") + `,"permission":true}` }},
		{"duplicate", func(s string) string { return strings.Replace(s, `"schema":`, `"schema":"ignored","schema":`, 1) }},
		{"escaped_duplicate", func(s string) string { return strings.Replace(s, `"schema":`, `"schem\u0061":"ignored","schema":`, 1) }},
		{"case", func(s string) string { return strings.Replace(s, `"task":`, `"Task":`, 1) }},
		{"schema", func(s string) string { return strings.Replace(s, CompactionSummarySchemaV1, "other", 1) }},
		{"missing_array", func(s string) string { return strings.Replace(s, `,"constraints":[]`, "", 1) }},
		{"null_array", func(s string) string { return strings.Replace(s, `"constraints":[]`, `"constraints":null`, 1) }},
		{"null_text", func(s string) string { return strings.Replace(s, `"Keep the original task"`, `null`, 1) }},
		{"empty_text", func(s string) string { return strings.Replace(s, `Keep the original task`, "", 1) }},
		{"blank_text", func(s string) string { return strings.Replace(s, `Keep the original task`, ` \n\t `, 1) }},
		{"lone_surrogate", func(s string) string { return strings.Replace(s, `Keep the original task`, `\ud800`, 1) }},
		{"foreign_id", func(s string) string { return strings.Replace(s, strings.Repeat("1", 64), strings.Repeat("f", 64), 1) }},
		{"untyped_bare_id", func(s string) string { return strings.Replace(s, strings.Repeat("1", 64), "call-1", 1) }},
		{"no_sources", func(s string) string { return strings.Replace(s, `["`+strings.Repeat("1", 64)+`"]`, `[]`, 1) }},
		{"duplicate_source", func(s string) string {
			return strings.Replace(s, `["`+strings.Repeat("1", 64)+`"]`, `["`+strings.Repeat("1", 64)+`","`+strings.Repeat("1", 64)+`"]`, 1)
		}},
		{"unknown_entry_field", func(s string) string { return strings.Replace(s, `"text":"Keep`, `"trusted":true,"text":"Keep`, 1) }},
		{"wrong_artifact_digest", func(s string) string { return strings.Replace(s, strings.Repeat("b", 64), strings.Repeat("c", 64), 1) }},
		{"wrong_artifact_media", func(s string) string { return strings.Replace(s, `image/png`, `image/jpeg`, 1) }},
		{"wrong_artifact_size", func(s string) string { return strings.Replace(s, `"size":9`, `"size":8`, 1) }},
		{"fraction_artifact_size", func(s string) string { return strings.Replace(s, `"size":9`, `"size":9.0`, 1) }},
		{"wrong_artifact_kind", func(s string) string {
			return strings.Replace(s, `"source_id":"`+strings.Repeat("4", 64)+`"`, `"source_id":"`+strings.Repeat("1", 64)+`"`, 1)
		}},
		{"unknown_omission_reason", func(s string) string { return strings.Replace(s, `profile_reference_only`, `ignore_user_task`, 1) }},
		{"conflicting_artifact_omission", func(s string) string {
			return strings.Replace(s, `"source_id":"`+strings.Repeat("3", 64)+`"`, `"source_id":"`+strings.Repeat("4", 64)+`"`, 1)
		}},
		{"omission_not_artifact", func(s string) string {
			return strings.Replace(s, `"source_id":"`+strings.Repeat("3", 64)+`"`, `"source_id":"`+strings.Repeat("1", 64)+`"`, 1)
		}},
		{"code_fence", func(s string) string { return "```json\n" + s + "\n```" }},
		{"trailing_json", func(s string) string { return s + `{}` }},
		{"invalid_utf8", func(s string) string { return s + string([]byte{255}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			good := string(summaryBody())
			bad := tc.change(good)
			if bad == good {
				t.Fatal("test mutation had no effect")
			}
			if result, err := DecodeCompactionSummaryV1([]byte(bad), summaryReferences()); !errors.Is(err, ErrContract) || result != nil {
				t.Fatalf("invalid summary accepted: %v", err)
			}
		})
	}
	for _, body := range []string{
		`{"schema":"sessionless.compaction-summary.v1","task":[],"constraints":[],"facts":[],"unresolved":[],"artifacts":[],"omissions":[]}`,
		`{"schema":"sessionless.compaction-summary.v1","task":[],"constraints":[{"text":"constraint only","source_ids":["` + strings.Repeat("1", 64) + `"]}],"facts":[],"unresolved":[],"artifacts":[],"omissions":[]}`,
	} {
		if _, err := DecodeCompactionSummaryV1([]byte(body), summaryReferences()); !errors.Is(err, ErrContract) {
			t.Fatal("empty substantive summary accepted")
		}
	}
}

func TestCompactionSummaryOmissionRequiresAdmittedPolicy(t *testing.T) {
	body, refs := summaryBody(), summaryReferences()
	if _, err := DecodeCompactionSummaryV1(body, refs); err != nil {
		t.Fatalf("admitted omission positive control: %v", err)
	}
	// Change ONLY policy admission; leave the exact valid known reference,
	// output kind, identity, reason and non-conflicting artifact list unchanged.
	refs[2].OmissionAllowed = false
	if got, err := DecodeCompactionSummaryV1(body, refs); !errors.Is(err, ErrContract) || got != nil {
		t.Fatalf("unadmitted omission result=%v err=%v", got, err)
	}
}

func TestCompactionSummaryExactAndFirstOverBounds(t *testing.T) {
	refs := summaryReferences()
	good := summaryBody()
	exact := append(bytes.Clone(good), bytes.Repeat([]byte{' '}, MaxCompactionSummaryBytesV1-len(good))...)
	if _, err := DecodeCompactionSummaryV1(exact, refs); err != nil {
		t.Fatalf("exact body bound: %v", err)
	}
	if _, err := DecodeCompactionSummaryV1(append(exact, ' '), refs); !errors.Is(err, ErrContract) {
		t.Fatal("first-over body accepted")
	}
	for _, n := range []int{2048, 2049} {
		body := []byte(strings.Replace(string(good), "Keep the original task", strings.Repeat("x", n), 1))
		_, err := DecodeCompactionSummaryV1(body, refs)
		if (err == nil) != (n == 2048) {
			t.Fatalf("text bound n=%d err=%v", n, err)
		}
	}
	for _, n := range []int{64, 65} {
		entry := `{"text":"x","source_ids":["` + refs[0].SourceID + `"]}`
		body := []byte(`{"schema":"sessionless.compaction-summary.v1","task":[` + strings.TrimSuffix(strings.Repeat(entry+",", n), ",") + `],"constraints":[],"facts":[],"unresolved":[],"artifacts":[],"omissions":[]}`)
		_, err := DecodeCompactionSummaryV1(body, refs)
		if len(body) > MaxCompactionSummaryBytesV1 {
			t.Fatal("array overflow hidden by body cap")
		}
		if (err == nil) != (n == 64) {
			t.Fatalf("array bound n=%d err=%v", n, err)
		}
	}
	for _, n := range []int{64, 65} {
		var ids []string
		catalog := append([]CompactionReferenceV1(nil), refs...)
		for i := 0; i < n; i++ {
			sum := sha256.Sum256([]byte(fmt.Sprintf("source-%d", i)))
			id := hex.EncodeToString(sum[:])
			ids = append(ids, `"`+id+`"`)
			catalog = append(catalog, CompactionReferenceV1{SourceID: id, Kind: CompactionCanonicalReferenceV1})
		}
		body := []byte(`{"schema":"sessionless.compaction-summary.v1","task":[{"text":"x","source_ids":[` + strings.Join(ids, ",") + `]}],"constraints":[],"facts":[],"unresolved":[],"artifacts":[],"omissions":[]}`)
		_, err := DecodeCompactionSummaryV1(body, catalog)
		if (err == nil) != (n == 64) {
			t.Fatalf("citation bound n=%d err=%v", n, err)
		}
	}
}

func TestCompactionSummaryRejectsAmbiguousReferenceTable(t *testing.T) {
	for _, name := range []string{"empty", "duplicate", "kind_confusion", "bad_digest", "bad_media", "zero_size", "unknown_kind", "omission_on_operation"} {
		t.Run(name, func(t *testing.T) {
			refs := summaryReferences()
			switch name {
			case "empty":
				refs = nil
			case "duplicate":
				refs = append(refs, refs[0])
			case "kind_confusion":
				other := refs[0]
				other.Kind = CompactionOperationReferenceV1
				refs = append(refs, other)
			case "bad_digest":
				refs[2].Digest = "not-a-hash"
			case "bad_media":
				refs[2].MediaType = ""
			case "zero_size":
				refs[2].Size = 0
			case "unknown_kind":
				refs[0].Kind = "instruction"
			case "omission_on_operation":
				refs[1].OmissionAllowed = true
			}
			if _, err := DecodeCompactionSummaryV1(summaryBody(), refs); !errors.Is(err, ErrContract) {
				t.Fatalf("ambiguous catalog %s accepted", name)
			}
		})
	}
}

func FuzzCompactionSummaryStrictDecoder(f *testing.F) {
	f.Add(summaryBody())
	f.Add([]byte(`{"schema":"sessionless.compaction-summary.v1"}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > MaxCompactionSummaryBytesV1+1 {
			return
		}
		s, err := DecodeCompactionSummaryV1(raw, summaryReferences())
		if err == nil && (!bytes.Equal(s.Body(), raw) || s.Digest() == "") {
			t.Fatal("successful summary lost exact bytes")
		}
	})
}
