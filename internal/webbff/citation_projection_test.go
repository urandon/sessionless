package webbff

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
)

func citedProjectionEvent(t *testing.T) (sessionapi.Event, domain.CitedAssistantMessageV2) {
	t.Helper()
	op := domain.AttemptOperationDigestV1(strings.Repeat("a", 64))
	uri := "https://example.com/source"
	v := domain.CitedAssistantMessageV2{Schema: domain.CitedAssistantMessageSchemaV2, Summary: " \nAnswer <exact> & 😺\t ", ArtifactManifestID: "manifest", Citations: domain.AnswerCitationManifestV1{Schema: domain.AnswerCitationManifestSchemaV1, RunID: "run-cited", AttemptID: "attempt-cited", Sources: []domain.AnswerCitationSourceV1{{SourceID: domain.WebSearchSourceIDV1(op, 1, uri), Rank: 1, URL: uri, Title: "untrusted <title>", OperationDigest: op, ObservationDigest: domain.AttemptOperationObservationDigestV1(strings.Repeat("b", 64)), ResponseDigest: strings.Repeat("c", 64), RetrievedAt: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC), ContentKind: domain.AnswerCitationSearchExcerptV1}}}}
	body, err := domain.EncodeCitedAssistantMessageV2(v, domain.CitedAnswerBytesV1, 256<<10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clear(body) })
	run := v.Citations.RunID
	return sessionapi.Event{Event: domain.SessionEvent{ID: "event-cited", Kind: domain.SessionEventAssistantMessage, RunID: &run}, Payload: body}, v
}

func TestProjectCitedAssistantPreservesOneManifest(t *testing.T) {
	item, value := citedProjectionEvent(t)
	got, err := projectEvent(item)
	if err != nil || got.Content.Text != value.Summary || got.Content.Data != nil || got.Content.Citations == nil || got.Content.ArtifactManifest == nil || got.Content.ArtifactManifest.RunID != value.Citations.RunID || got.RunID == item.Event.RunID {
		t.Fatalf("exact cited projection: %v", err)
	}
	want, err := domain.EncodeAnswerCitationManifestV1(value.Citations)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(want)
	actual, err := json.Marshal(got.Content.Citations)
	if err != nil || !bytes.Equal(want, actual) {
		t.Fatal("public manifest differs from sole canonical source manifest")
	}
	clear(item.Payload)
	*item.Event.RunID = "changed"
	if got.RunID == nil || *got.RunID != value.Citations.RunID || got.Content.Citations.Sources[0].Title != value.Citations.Sources[0].Title {
		t.Fatal("projected content retained mutable input")
	}
	for _, key := range []string{"query", "excerpt", "credential", "receipt", "verified", "supports_claim"} {
		if bytes.Contains(actual, []byte(`"`+key+`"`)) {
			t.Fatal("private/unsupported public citation field")
		}
	}
}

func TestProjectCitedAssistantRejectsInsteadOfUncitedFallback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*sessionapi.Event)
	}{
		{"partial-schema-error-before-v2", func(v *sessionapi.Event) {
			v.Payload = bytes.Replace(v.Payload, []byte(`"schema":`), []byte(`"schema":123,"schema":`), 1)
		}},
		{"partial-schema-error-after-v2", func(v *sessionapi.Event) {
			v.Payload = bytes.Replace(v.Payload, []byte(`"summary":`), []byte(`"schema":123,"summary":`), 1)
		}},
		{"missing-run", func(v *sessionapi.Event) { v.Event.RunID = nil }},
		{"crossed-run", func(v *sessionapi.Event) { r := domain.RunID("other-run"); v.Event.RunID = &r }},
		{"invalid-run", func(v *sessionapi.Event) { r := domain.RunID("bad id"); v.Event.RunID = &r }},
		{"alias", func(v *sessionapi.Event) {
			v.Payload = bytes.Replace(v.Payload, []byte(`"schema":`), []byte(`"SCHEMA":`), 1)
		}},
		{"missing-false", func(v *sessionapi.Event) {
			v.Payload = bytes.Replace(v.Payload, []byte(`"title_truncated":false,`), nil, 1)
		}},
		{"extra-private", func(v *sessionapi.Event) {
			v.Payload = bytes.Replace(v.Payload, []byte(`"summary":`), []byte(`"receipt":"private","summary":`), 1)
		}},
		{"unsupported-version", func(v *sessionapi.Event) {
			v.Payload = bytes.Replace(v.Payload, []byte(domain.CitedAssistantMessageSchemaV2), []byte("sessionless.assistant-message.v3"), 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item, _ := citedProjectionEvent(t)
			tc.change(&item)
			got, err := projectEvent(item)
			if err == nil || got.Content.Text != "" || got.Content.Data != nil || got.Content.Citations != nil || got.RunID != nil {
				t.Fatal("invalid selected v2 became an uncited/generic response")
			}
			if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "Answer") {
				t.Fatal("projection error disclosed content")
			}
		})
	}
}

func TestProjectCitedAssistantEmptySourcesRemainExplicit(t *testing.T) {
	item, value := citedProjectionEvent(t)
	value.Citations.Sources = []domain.AnswerCitationSourceV1{}
	body, err := domain.EncodeCitedAssistantMessageV2(value, domain.CitedAnswerBytesV1, 256<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(body)
	item.Payload = body
	got, err := projectEvent(item)
	if err != nil || got.Content.Citations == nil || got.Content.Citations.Sources == nil || len(got.Content.Citations.Sources) != 0 {
		t.Fatal("empty citations omitted or invented")
	}
}
