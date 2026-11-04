package configuredinference

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/canonicalprompt"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func citedAnswerTestBody(t *testing.T, answer string, ids []string) []byte {
	t.Helper()
	if ids == nil {
		ids = []string{}
	}
	body, err := json.Marshal(struct {
		Schema    string   `json:"schema"`
		Answer    string   `json:"answer"`
		SourceIDs []string `json:"source_ids"`
	}{CitedAnswerSchemaV1, answer, ids})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestCitedAnswerExactTextAndOwnedReferences(t *testing.T) {
	t.Parallel()
	ids := []string{"source-" + strings.Repeat("b", 64), "source-" + strings.Repeat("a", 64)}
	answer := " \nΩ and \"quotes\"; <untrusted title>\t "
	body := citedAnswerTestBody(t, answer, ids)
	before := bytes.Clone(body)
	got, err := DecodeCitedAnswerV1(body, MaxCitedAnswerBytesV1)
	if err != nil || got.Answer() != answer || !reflect.DeepEqual(got.SourceIDs(), ids) || !bytes.Equal(body, before) {
		t.Fatalf("candidate text/order/input ownership: error=%v", err)
	}
	copy := got.SourceIDs()
	copy[0] = "changed-caller-view"
	clear(body)
	if got.Answer() != answer || !reflect.DeepEqual(got.SourceIDs(), ids) {
		t.Fatal("candidate retained caller bytes or exposed its source slice")
	}
	// This checks the existing scalar frame contract, not a new assistant-v2
	// reader. That versioned reader/finalizer is deliberately not installed by
	// the pure decoder. It must preserve this exact answer when implemented.
	payload, err := json.Marshal(struct {
		Schema             string `json:"schema"`
		Summary            string `json:"summary"`
		ArtifactManifestID string `json:"artifact_manifest_id"`
	}{"sessionless.assistant-message.v1", got.Answer(), "test-manifest"})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := canonicalprompt.RenderFrameV1(domain.SessionEventAssistantMessage, payload)
	if err != nil || string(frame) != "[assistant]\n"+answer+"\n" {
		t.Fatalf("decoded scalar no longer fits exact existing framing: error=%v", err)
	}
}

func TestCitedAnswerCandidateDoesNotEstablishSourceProvenance(t *testing.T) {
	t.Parallel()
	var ids []string
	for i := 0; i < MaxCitedAnswerSourcesV1; i++ {
		ids = append(ids, fmt.Sprintf("source-%064x", i))
	}
	got, err := DecodeCitedAnswerV1(citedAnswerTestBody(t, "Untrusted model statement", ids), MaxCitedAnswerBytesV1)
	if err != nil || !reflect.DeepEqual(got.SourceIDs(), ids) {
		t.Fatal("syntactically valid unknown references must remain candidate DATA, not a source lookup", err)
	}
	uncited, err := DecodeCitedAnswerV1(citedAnswerTestBody(t, "No sources claimed", nil), MaxCitedAnswerBytesV1)
	if err != nil || uncited.SourceIDs() == nil || len(uncited.SourceIDs()) != 0 {
		t.Fatal("empty candidate reference list was converted to absence or invented evidence", err)
	}
}

func TestCitedAnswerRejectsClosedGrammarAndDecodedNUL(t *testing.T) {
	t.Parallel()
	valid := string(citedAnswerTestBody(t, "ordinary answer", nil))
	id := "source-" + strings.Repeat("a", 64)
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"decoded-NUL", []byte(`{"schema":"sessionless.cited-answer.v1","answer":"a\u0000b","source_ids":[]}`)},
		{"literal-NUL", append([]byte(`{"answer":"a`), 0)},
		{"empty-answer", citedAnswerTestBody(t, "", nil)},
		{"null-answer", []byte(strings.Replace(valid, `"ordinary answer"`, `null`, 1))},
		{"number-answer", []byte(strings.Replace(valid, `"ordinary answer"`, `42`, 1))},
		{"null-ids", []byte(strings.Replace(valid, `"source_ids":[]`, `"source_ids":null`, 1))},
		{"missing-ids", []byte(strings.Replace(valid, `,"source_ids":[]`, ``, 1))},
		{"unknown-field", []byte(strings.TrimSuffix(valid, "}") + `,"url":"https://example.invalid"}`)},
		{"duplicate-key", []byte(strings.TrimSuffix(valid, "}") + `,"answer":"second"}`)},
		{"escaped-duplicate-key", []byte(strings.TrimSuffix(valid, "}") + `,"answ\u0065r":"second"}`)},
		{"aliased-key", []byte(strings.Replace(valid, `"answer"`, `"Answer"`, 1))},
		{"wrong-schema", []byte(strings.Replace(valid, CitedAnswerSchemaV1, "sessionless.cited-answer.v2", 1))},
		{"trailing-data", []byte(valid + ` {}`)},
		{"Markdown-fences", []byte("```json\n" + valid + "\n```")},
		{"duplicate-ids", citedAnswerTestBody(t, "answer", []string{id, id})},
		{"uppercase-id", citedAnswerTestBody(t, "answer", []string{"source-" + strings.Repeat("A", 64)})},
		{"wrong-prefix", citedAnswerTestBody(t, "answer", []string{strings.Repeat("a", 64)})},
		{"short-id", citedAnswerTestBody(t, "answer", []string{"source-a"})},
		{"non-string-id", []byte(strings.Replace(valid, `"source_ids":[]`, `"source_ids":[1]`, 1))},
		{"nested-id", []byte(strings.Replace(valid, `"source_ids":[]`, `"source_ids":[[[]]]`, 1))},
		{"lone-surrogate", []byte(strings.Replace(valid, `"ordinary answer"`, `"\ud800"`, 1))},
		{"invalid-UTF8", append([]byte(`{"schema":"sessionless.cited-answer.v1","answer":"`), 0xff)},
		{"array-root", []byte(`[]`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeCitedAnswerV1(tc.body, MaxCitedAnswerBytesV1)
			if !errors.Is(err, ErrContract) || got != nil {
				t.Fatalf("closed candidate accepted: error=%v", err)
			}
		})
	}
	// Both outer escaped-Unicode text and the decoded scalar remain ordinary
	// positives. NUL rejection must not become a ban on all escape sequences.
	for _, body := range []string{valid, `{"schema":"sessionless.cited-answer.v1","answer":"\u03a9 \ud83d\ude00","source_ids":[]}`} {
		got, err := DecodeCitedAnswerV1([]byte(body), MaxCitedAnswerBytesV1)
		if err != nil || got.Answer() == "" {
			t.Fatal("ordinary/escaped Unicode control rejected", err)
		}
	}
}

func TestCitedAnswerWireBudgetAndReferenceCount(t *testing.T) {
	t.Parallel()
	fixed := len(citedAnswerTestBody(t, "", nil))
	for _, limit := range []int{1024, MaxCitedAnswerBytesV1} {
		body := citedAnswerTestBody(t, strings.Repeat("x", limit-fixed), nil)
		if len(body) != limit {
			t.Fatal("exact wire boundary prerequisite")
		}
		if _, err := DecodeCitedAnswerV1(body, uint64(limit)); err != nil {
			t.Fatal("exact admitted wire limit rejected", err)
		}
		if got, err := DecodeCitedAnswerV1(body, uint64(limit-1)); err == nil || got != nil {
			t.Fatal("JSON overhead escaped original final-text limit")
		}
	}
	oversized := citedAnswerTestBody(t, strings.Repeat("x", MaxCitedAnswerBytesV1-fixed+1), nil)
	if got, err := DecodeCitedAnswerV1(oversized, ^uint64(0)); err == nil || got != nil {
		t.Fatal("caller widened fixed candidate cap")
	}
	if got, err := DecodeCitedAnswerV1(citedAnswerTestBody(t, "answer", nil), 0); err == nil || got != nil {
		t.Fatal("zero admission limit accepted")
	}
	ids := make([]string, MaxCitedAnswerSourcesV1+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("source-%064x", i)
	}
	if got, err := DecodeCitedAnswerV1(citedAnswerTestBody(t, "answer", ids), MaxCitedAnswerBytesV1); err == nil || got != nil {
		t.Fatal("eleven source references accepted")
	}
	escaped := citedAnswerTestBody(t, strings.Repeat("<", 100), nil)
	if got, err := DecodeCitedAnswerV1(escaped, uint64(fixed+100)); err == nil || got != nil {
		t.Fatal("encoded escaping was not charged to the original wire cap")
	}
}

func TestCitedAnswerDiagnosticsAndNilViews(t *testing.T) {
	t.Parallel()
	got, err := DecodeCitedAnswerV1(citedAnswerTestBody(t, "private-answer-marker", []string{"source-" + strings.Repeat("a", 64)}), MaxCitedAnswerBytesV1)
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		if text := fmt.Sprintf(format, got); strings.Contains(text, "private-answer-marker") || strings.Contains(text, "source-") {
			t.Fatal("candidate content leaked through formatting")
		}
	}
	if body, err := json.Marshal(got); err == nil || body != nil {
		t.Fatal("generic JSON serialization exported private candidate")
	}
	var absent *CitedAnswerV1
	if absent.Answer() != "" || absent.SourceIDs() != nil {
		t.Fatal("nil candidate exposes a value")
	}
}

func FuzzCitedAnswerBoundedCandidate(f *testing.F) {
	f.Add([]byte(`{"schema":"sessionless.cited-answer.v1","answer":"answer","source_ids":[]}`), uint16(1024))
	f.Add([]byte(`{"schema":"sessionless.cited-answer.v1","answer":"a\u0000b","source_ids":[]}`), uint16(1024))
	f.Fuzz(func(t *testing.T, body []byte, limit uint16) {
		if len(body) > MaxCitedAnswerBytesV1+1 {
			return
		}
		before := bytes.Clone(body)
		got, err := DecodeCitedAnswerV1(body, uint64(limit))
		if !bytes.Equal(body, before) {
			t.Fatal("decoder mutated caller bytes")
		}
		if err != nil {
			if got != nil {
				t.Fatal("failed decoder released a candidate")
			}
			return
		}
		if len(body) > int(limit) || len(body) > MaxCitedAnswerBytesV1 || got.Answer() == "" || !utf8.ValidString(got.Answer()) || strings.ContainsRune(got.Answer(), 0) || len(got.SourceIDs()) > MaxCitedAnswerSourcesV1 {
			t.Fatal("accepted candidate violates scalar/count/wire invariants")
		}
	})
}
