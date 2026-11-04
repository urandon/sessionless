package canonicalprompt

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func citedFramePayload(t *testing.T) (domain.CitedAssistantMessageV2, []byte) {
	t.Helper()
	op := domain.AttemptOperationDigestV1(strings.Repeat("a", 64))
	uri := "https://example.com/source"
	v := domain.CitedAssistantMessageV2{Schema: domain.CitedAssistantMessageSchemaV2, Summary: " \nТочный <answer> & 😺\t ", ArtifactManifestID: "manifest", Citations: domain.AnswerCitationManifestV1{Schema: domain.AnswerCitationManifestSchemaV1, RunID: "run", AttemptID: "attempt", Sources: []domain.AnswerCitationSourceV1{{SourceID: domain.WebSearchSourceIDV1(op, 1, uri), Rank: 1, URL: uri, Title: "untrusted title", OperationDigest: op, ObservationDigest: domain.AttemptOperationObservationDigestV1(strings.Repeat("b", 64)), ResponseDigest: strings.Repeat("c", 64), RetrievedAt: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC), ContentKind: domain.AnswerCitationSearchExcerptV1}}}}
	body, err := domain.EncodeCitedAssistantMessageV2(v, domain.CitedAnswerBytesV1, MaxPayloadBytesV1)
	if err != nil {
		t.Fatalf("valid shared v2 codec setup: %v", err)
	}
	t.Cleanup(func() { clear(body) })
	return v, body
}

func TestRenderFrameV1CitedAssistantExactText(t *testing.T) {
	v, body := citedFramePayload(t)
	before := bytes.Clone(body)
	defer clear(before)
	frame, err := RenderFrameV1(domain.SessionEventAssistantMessage, body)
	if err != nil || string(frame) != "[assistant]\n"+v.Summary+"\n" {
		t.Fatalf("valid canonical v2 frame rejected or changed: err=%v", err)
	}
	if !bytes.Equal(body, before) || bytes.Contains(frame, []byte("untrusted title")) || bytes.Contains(frame, []byte("https://")) {
		t.Fatal("renderer mutated source or promoted metadata to answer text")
	}
	frame[0] = '!'
	again, err := RenderFrameV1(domain.SessionEventAssistantMessage, body)
	if err != nil || string(again) != "[assistant]\n"+v.Summary+"\n" {
		t.Fatal("returned frame shares source or global storage")
	}
}

func TestRenderFrameV1CitedAssistantBoundsAndMixedHistory(t *testing.T) {
	v, _ := citedFramePayload(t)
	v.Summary = strings.Repeat("x", domain.CitedAnswerBytesV1)
	first := v.Citations.Sources[0]
	v.Citations.Sources = []domain.AnswerCitationSourceV1{}
	for i := range domain.AnswerCitationSourcesV1 {
		s := first
		s.OperationDigest = domain.AttemptOperationDigestV1(fmt.Sprintf("%064x", i/5+1))
		s.Rank = uint32(i%5 + 1)
		s.URL = "https://example.com/" + strings.Repeat("a", 2048-len("https://example.com/"))
		s.Title = strings.Repeat("t", 512)
		s.SourceID = domain.WebSearchSourceIDV1(s.OperationDigest, s.Rank, s.URL)
		v.Citations.Sources = append(v.Citations.Sources, s)
	}
	body, err := domain.EncodeCitedAssistantMessageV2(v, domain.CitedAnswerBytesV1, MaxPayloadBytesV1)
	if err != nil {
		t.Fatalf("maximum valid components setup: %v", err)
	}
	defer clear(body)
	frame, err := RenderFrameV1(domain.SessionEventAssistantMessage, body)
	if err != nil || string(frame) != "[assistant]\n"+v.Summary+"\n" {
		t.Fatal("maximum valid v2 text/citation components rejected")
	}
	v.Summary = " \n\t "
	v.Citations.Sources = []domain.AnswerCitationSourceV1{}
	body, err = domain.EncodeCitedAssistantMessageV2(v, domain.CitedAnswerBytesV1, MaxPayloadBytesV1)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(body)
	for _, tc := range []struct {
		body []byte
		text string
	}{
		{[]byte(`{"schema":"sessionless.assistant-message.v1","summary":"legacy"}`), "legacy"},
		{body, v.Summary},
		// The effective legacy schema still has its original last-wins rules.
		{[]byte(`{"schema":"sessionless.assistant-message.v2","schema":"sessionless.assistant-message.v1","summary":"first","summary":"legacy last"}`), "legacy last"},
		{[]byte(`{"SCHEMA":"sessionless.assistant-message.v1","SUMMARY":"legacy alias"}`), "legacy alias"},
	} {
		got, err := RenderFrameV1(domain.SessionEventAssistantMessage, tc.body)
		if err != nil || string(got) != "[assistant]\n"+tc.text+"\n" {
			t.Fatal("mixed legacy/v2 framing changed")
		}
	}
}

func TestRenderFrameV1CitedAssistantStrictDenials(t *testing.T) {
	_, body := citedFramePayload(t)
	for _, tc := range []struct{ name, old, next string }{
		{"alias", `"schema":`, `"SCHEMA":`},
		{"duplicate", `"schema":`, `"schema":"legacy","schema":`},
		{"missing-false", `"title_truncated":false,`, ``},
		{"null-citations", `"citations":{`, `"citations":null,"extra":{`},
		{"summary-nul", `"summary":`, `"summary":"\u0000","old_summary":`},
		{"wrong-version", `sessionless.assistant-message.v2`, `sessionless.assistant-message.v3`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !bytes.Contains(body, []byte(tc.old)) {
				t.Fatal("mutation setup absent")
			}
			bad := []byte(strings.Replace(string(body), tc.old, tc.next, 1))
			if got, err := RenderFrameV1(domain.SessionEventAssistantMessage, bad); got != nil || !errors.Is(err, ErrInvalidFrame) {
				t.Fatal("invalid selected v2 fell back to legacy or rendered")
			}
		})
	}
	for _, kind := range []domain.SessionEventKind{domain.SessionEventUserMessage, domain.SessionEventToolCall, domain.SessionEventToolResult, domain.SessionEventSystemNotice} {
		if got, err := RenderFrameV1(kind, body); got != nil || !errors.Is(err, ErrInvalidFrame) {
			t.Fatal("v2 assistant accepted as another event kind")
		}
	}
}
