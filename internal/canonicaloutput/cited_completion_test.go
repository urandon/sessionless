package canonicaloutput_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func citedMessage(t *testing.T) canonicaloutput.CitedAssistantMessageV2 {
	t.Helper()
	op := domain.AttemptOperationDigestV1(strings.Repeat("a", 64))
	uri := "https://example.com/source?a=1&b=2"
	source := domain.AnswerCitationSourceV1{SourceID: domain.WebSearchSourceIDV1(op, 1, uri), Rank: 1, URL: uri, Title: `<script>untrusted & "title"</script>`, OperationDigest: op, ObservationDigest: domain.AttemptOperationObservationDigestV1(strings.Repeat("b", 64)), ResponseDigest: strings.Repeat("c", 64), RetrievedAt: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC), ContentKind: domain.AnswerCitationSearchExcerptV1}
	return canonicaloutput.CitedAssistantMessageV2{Schema: canonicaloutput.CitedAssistantMessageSchemaV2, Summary: " \n Ответ <answer> & \"quoted\" 😺\t ", ArtifactManifestID: "artifact-citations", Citations: domain.AnswerCitationManifestV1{Schema: domain.AnswerCitationManifestSchemaV1, RunID: "run-citations", AttemptID: "attempt-citations", Sources: []domain.AnswerCitationSourceV1{source}}}
}

func encodeCitedMessage(t *testing.T, v canonicaloutput.CitedAssistantMessageV2) []byte {
	t.Helper()
	body, err := canonicaloutput.EncodeCitedAssistantMessageV2(v, configuredinference.MaxCitedAnswerBytesV1, 256<<10)
	if err != nil {
		t.Fatalf("encode valid cited assistant: %v", err)
	}
	t.Cleanup(func() { clear(body) })
	return body
}

func TestCitedAssistantMessageV2ExactAnswerAndManifest(t *testing.T) {
	v := citedMessage(t)
	body := encodeCitedMessage(t, v)
	got, err := canonicaloutput.DecodeCitedAssistantMessageV2(body, configuredinference.MaxCitedAnswerBytesV1, uint64(len(body)))
	if err != nil || got.Summary != v.Summary || got.Citations.Sources[0].Title != v.Citations.Sources[0].Title || got.Citations.Sources[0].URL != v.Citations.Sources[0].URL {
		t.Fatalf("exact text/manifest roundtrip: %v", err)
	}
	var wire struct {
		Citations json.RawMessage `json:"citations"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		t.Fatal(err)
	}
	manifest, err := domain.EncodeAnswerCitationManifestV1(v.Citations)
	if err != nil || !bytes.Equal(manifest, wire.Citations) {
		t.Fatal("nested manifest differs from sole canonical encoding")
	}
	if !bytes.Equal(body, encodeCitedMessage(t, got)) {
		t.Fatal("v2 canonical bytes changed")
	}
	copy := got.Clone()
	copy.Citations.Sources[0].Title = "changed"
	if got.Citations.Sources[0].Title != v.Citations.Sources[0].Title {
		t.Fatal("clone shares manifest source slice")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%q"} {
		if s := fmt.Sprintf(format, got); strings.Contains(s, "Ответ") || strings.Contains(s, "untrusted") {
			t.Fatal("formatting disclosed answer/source")
		}
	}
}

func TestCitedAssistantMessageV2UncitedAndWhitespace(t *testing.T) {
	v := citedMessage(t)
	v.Summary = " \n\t "
	v.Citations.Sources = []domain.AnswerCitationSourceV1{}
	body := encodeCitedMessage(t, v)
	got, err := canonicaloutput.DecodeCitedAssistantMessageV2(body, 16<<10, uint64(len(body)))
	if err != nil || got.Summary != v.Summary || got.Citations.Sources == nil || len(got.Citations.Sources) != 0 {
		t.Fatal("uncited/whitespace answer trimmed or repaired")
	}
}

func TestCitedAssistantMessageV2AdmittedBounds(t *testing.T) {
	v := citedMessage(t)
	v.Summary = strings.Repeat("x", configuredinference.MaxCitedAnswerBytesV1)
	body := encodeCitedMessage(t, v)
	for _, tc := range []struct {
		name          string
		text, payload uint64
		valid         bool
	}{
		{"exact", uint64(len(v.Summary)), uint64(len(body)), true},
		{"text-first-over", uint64(len(v.Summary) - 1), uint64(len(body)), false},
		{"payload-first-over", uint64(len(v.Summary)), uint64(len(body) - 1), false},
		{"text-zero", 0, uint64(len(body)), false},
		{"payload-zero", uint64(len(v.Summary)), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := canonicaloutput.EncodeCitedAssistantMessageV2(v, tc.text, tc.payload)
			defer clear(b)
			if (err == nil) != tc.valid {
				t.Fatalf("encode valid=%v err=%v", tc.valid, err)
			}
			_, err = canonicaloutput.DecodeCitedAssistantMessageV2(body, tc.text, tc.payload)
			if (err == nil) != tc.valid {
				t.Fatalf("decode valid=%v err=%v", tc.valid, err)
			}
		})
	}
	v.Summary += "x"
	if b, err := canonicaloutput.EncodeCitedAssistantMessageV2(v, ^uint64(0), ^uint64(0)); err == nil || b != nil {
		t.Fatal("hard final-text maximum bypassed by caller")
	}
}

func TestCitedAssistantMessageV2Denials(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*canonicaloutput.CitedAssistantMessageV2)
	}{
		{"schema-v1", func(v *canonicaloutput.CitedAssistantMessageV2) { v.Schema = "sessionless.assistant-message.v1" }},
		{"empty-summary", func(v *canonicaloutput.CitedAssistantMessageV2) { v.Summary = "" }},
		{"nul-summary", func(v *canonicaloutput.CitedAssistantMessageV2) { v.Summary = "a\x00b" }},
		{"utf8-summary", func(v *canonicaloutput.CitedAssistantMessageV2) { v.Summary = string([]byte{0xff}) }},
		{"manifest-id", func(v *canonicaloutput.CitedAssistantMessageV2) { v.ArtifactManifestID = "" }},
		{"nil-sources", func(v *canonicaloutput.CitedAssistantMessageV2) { v.Citations.Sources = nil }},
		{"source-id", func(v *canonicaloutput.CitedAssistantMessageV2) { v.Citations.Sources[0].SourceID = "fabricated" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := citedMessage(t)
			tc.change(&v)
			if b, err := canonicaloutput.EncodeCitedAssistantMessageV2(v, 16<<10, 256<<10); err == nil || b != nil {
				t.Fatal("invalid cited assistant accepted")
			}
		})
	}
	v := citedMessage(t)
	v.Summary = "answer"
	body := string(encodeCitedMessage(t, v))
	for _, tc := range []struct{ name, old, next string }{
		{"v1-tag", `sessionless.assistant-message.v2`, `sessionless.assistant-message.v1`},
		{"summary-nul", `"summary":"answer"`, `"summary":"a\u0000b"`},
		{"summary-surrogate", `"summary":"answer"`, `"summary":"\ud800"`},
		{"summary-null", `"summary":"answer"`, `"summary":null`},
		{"summary-alias", `"summary":`, `"Summary":`},
		{"summary-duplicate", `"summary":`, `"summary":"other","summary":`},
		{"manifest-null", `"citations":{`, `"citations":null,"unused":{`},
		{"manifest-no-false", `"title_truncated":false,`, ``},
		{"extra-field", `"summary":`, `"query":"private","summary":`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(body, tc.old) {
				t.Fatal("wire mutation baseline absent")
			}
			mutated := []byte(strings.Replace(body, tc.old, tc.next, 1))
			if _, err := canonicaloutput.DecodeCitedAssistantMessageV2(mutated, 16<<10, 256<<10); err == nil {
				t.Fatal("invalid v2 wire accepted")
			}
		})
	}
	if _, err := canonicaloutput.DecodeCitedAssistantMessageV2([]byte(body+` {}`), 16<<10, 256<<10); err == nil {
		t.Fatal("trailing JSON accepted")
	}
}
