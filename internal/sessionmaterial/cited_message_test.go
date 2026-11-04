package sessionmaterial

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicalprompt"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func citedMaterialPayload(t *testing.T) (domain.CitedAssistantMessageV2, []byte) {
	t.Helper()
	op := domain.AttemptOperationDigestV1(strings.Repeat("a", 64))
	uri := "https://example.com/source"
	v := domain.CitedAssistantMessageV2{Schema: domain.CitedAssistantMessageSchemaV2, Summary: " \nТочный <answer> & 😺\t ", ArtifactManifestID: "manifest", Citations: domain.AnswerCitationManifestV1{Schema: domain.AnswerCitationManifestSchemaV1, RunID: "private-run", AttemptID: "attempt", Sources: []domain.AnswerCitationSourceV1{{SourceID: domain.WebSearchSourceIDV1(op, 1, uri), Rank: 1, URL: uri, Title: "untrusted title", OperationDigest: op, ObservationDigest: domain.AttemptOperationObservationDigestV1(strings.Repeat("b", 64)), ResponseDigest: strings.Repeat("c", 64), RetrievedAt: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC), ContentKind: domain.AnswerCitationSearchExcerptV1}}}}
	body, err := domain.EncodeCitedAssistantMessageV2(v, domain.CitedAnswerBytesV1, MaxPayloadBytesV1)
	if err != nil {
		t.Fatalf("valid shared v2 codec setup: %v", err)
	}
	t.Cleanup(func() { clear(body) })
	return v, body
}

func TestPreparedCitedAssistantPreservesFullMaterial(t *testing.T) {
	v, body := citedMaterialPayload(t)
	want := bytes.Clone(body)
	defer clear(want)
	event := preparedEvent(t, domain.SessionEventAssistantMessage, body)
	p, err := PrepareV1(event, body, nil)
	if err != nil || p == nil {
		t.Fatalf("valid canonical v2 material rejected: %v", err)
	}
	t.Cleanup(p.Release)
	clone, err := p.Clone()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clone.Release)
	clear(body)
	final := preparedFinal(event)
	copy, measures, err := p.CopyForFinalV1(final)
	defer clear(copy)
	if err != nil || !bytes.Equal(copy, want) || measures.PayloadBytes != uint64(len(want)) || !measures.CodexCompatible || measures.RenderedFrameBytes != uint64(len("[assistant]\n"+v.Summary+"\n")) {
		t.Fatalf("full canonical payload or driver measure changed: %v", err)
	}
	frame, err := canonicalprompt.RenderFrameV1(final.Kind, copy)
	if err != nil || uint64(len(frame)) != measures.RenderedFrameBytes {
		t.Fatal("material measure differs from actual shared driver renderer")
	}
	alias := *p
	p.Release()
	if b, _, err := alias.CopyForFinalV1(final); err == nil || b != nil {
		t.Fatal("released alias regained material")
	}
	owned, _, err := clone.CopyForFinalV1(final)
	defer clear(owned)
	if err != nil || !bytes.Equal(owned, want) {
		t.Fatal("independent clone lost full citation material")
	}
}

func TestPreparedCitedAssistantRejectsCrossedMaterial(t *testing.T) {
	_, body := citedMaterialPayload(t)
	event := preparedEvent(t, domain.SessionEventAssistantMessage, body)
	for _, tc := range []struct {
		name   string
		change func(*domain.SessionEvent)
	}{
		{"foreign-run", func(e *domain.SessionEvent) { r := domain.RunID("another-run"); e.RunID = &r }},
		{"missing-run", func(e *domain.SessionEvent) { e.RunID = nil }},
		{"corrupt-sha", func(e *domain.SessionEvent) { e.Payload.SHA256 = strings.Repeat("f", 64) }},
		{"wrong-size", func(e *domain.SessionEvent) { e.Payload.Size++ }},
		{"foreign-payload-tenant", func(e *domain.SessionEvent) { e.Payload.TenantID = "other-tenant" }},
		{"wrong-kind", func(e *domain.SessionEvent) { e.Kind = domain.SessionEventToolResult }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := event
			tc.change(&e)
			if p, err := PrepareV1(e, body, nil); p != nil || !errors.Is(err, ErrNotPrepared) {
				t.Fatal("crossed v2 material accepted")
			}
		})
	}
	for _, tc := range []struct{ name, old, next string }{
		{"schema-alias", `"schema":`, `"SCHEMA":`},
		{"duplicate", `"schema":`, `"schema":"legacy","schema":`},
		{"missing-false", `"title_truncated":false,`, ``},
		{"null-published", `"content_kind":`, `"published_at":null,"content_kind":`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !bytes.Contains(body, []byte(tc.old)) {
				t.Fatal("mutation setup absent")
			}
			bad := []byte(strings.Replace(string(body), tc.old, tc.next, 1))
			// Rebind the legitimate event SHA/size: rejection must be the strict
			// selected schema, not merely the old payload's integrity mismatch.
			e := preparedEvent(t, domain.SessionEventAssistantMessage, bad)
			if p, err := PrepareV1(e, bad, nil); p != nil || !errors.Is(err, ErrNotPrepared) {
				t.Fatal("malformed v2 with coherent raw identity accepted")
			}
		})
	}
}

func TestPreparedCitedAssistantMixedLegacyAndEmpty(t *testing.T) {
	v, _ := citedMaterialPayload(t)
	v.Summary = " \n\t "
	v.Citations.Sources = []domain.AnswerCitationSourceV1{}
	body, err := domain.EncodeCitedAssistantMessageV2(v, domain.CitedAnswerBytesV1, MaxPayloadBytesV1)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(body)
	for _, payload := range [][]byte{
		[]byte(`{"schema":"sessionless.assistant-message.v1","summary":"legacy"}`),
		body,
		[]byte(`{"schema":"sessionless.assistant-message.v2","schema":"sessionless.assistant-message.v1","summary":"first","summary":"legacy last"}`),
	} {
		event := preparedEvent(t, domain.SessionEventAssistantMessage, payload)
		p := requirePrepared(t, event, payload, nil)
		copy, m, err := p.CopyForFinalV1(preparedFinal(event))
		defer clear(copy)
		if err != nil || !bytes.Equal(copy, payload) || !m.CodexCompatible {
			t.Fatal("mixed original material changed or became incompatible")
		}
	}
}
