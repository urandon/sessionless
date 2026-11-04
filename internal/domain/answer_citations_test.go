package domain_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func citationManifest(t *testing.T, count int) domain.AnswerCitationManifestV1 {
	t.Helper()
	v := domain.AnswerCitationManifestV1{Schema: domain.AnswerCitationManifestSchemaV1, RunID: "run-citations", AttemptID: "attempt-citations", Sources: []domain.AnswerCitationSourceV1{}}
	at := time.Date(2026, 11, 4, 0, 0, 0, 123000, time.UTC)
	for i := range count {
		op := domain.AttemptOperationDigestV1(fmt.Sprintf("%064x", i/5+1))
		rank := uint32(i%5 + 1)
		uri := fmt.Sprintf("https://example.com/source/%d", i)
		v.Sources = append(v.Sources, domain.AnswerCitationSourceV1{SourceID: domain.WebSearchSourceIDV1(op, rank, uri), Rank: rank, URL: uri, Title: "Untrusted <title> & source", OperationDigest: op, ObservationDigest: domain.AttemptOperationObservationDigestV1(strings.Repeat("a", 64)), ResponseDigest: strings.Repeat("b", 64), RetrievedAt: at, ContentKind: domain.AnswerCitationSearchExcerptV1})
	}
	return v
}

func encodeCitation(t *testing.T, v domain.AnswerCitationManifestV1) []byte {
	t.Helper()
	b, err := domain.EncodeAnswerCitationManifestV1(v)
	if err != nil {
		t.Fatalf("encode valid citation manifest: %v", err)
	}
	t.Cleanup(func() { clear(b) })
	return b
}

func TestAnswerCitationManifestV1RoundTripAndOrder(t *testing.T) {
	for _, n := range []int{0, 1, 10} {
		t.Run(fmt.Sprintf("sources-%d", n), func(t *testing.T) {
			v := citationManifest(t, n)
			if n > 1 {
				// Same URL and rank from different operations are distinct sources.
				v.Sources[5].URL, v.Sources[5].Rank = v.Sources[0].URL, v.Sources[0].Rank
				v.Sources[5].SourceID = domain.WebSearchSourceIDV1(v.Sources[5].OperationDigest, v.Sources[5].Rank, v.Sources[5].URL)
				v.Sources[0], v.Sources[5] = v.Sources[5], v.Sources[0]
				published := v.Sources[0].RetrievedAt.Add(-time.Hour)
				v.Sources[0].PublishedAt, v.Sources[0].TitleTruncated = &published, true
			}
			body := encodeCitation(t, v)
			got, err := domain.DecodeAnswerCitationManifestV1(body)
			if err != nil || got.Validate() != nil || got.Sources == nil || len(got.Sources) != n {
				t.Fatalf("decode valid %d-source manifest: err=%v count=%d", n, err, len(got.Sources))
			}
			again := encodeCitation(t, got)
			if !bytes.Equal(body, again) {
				t.Fatal("canonical bytes/order changed after decode")
			}
			for i := range n {
				if got.Sources[i].SourceID != v.Sources[i].SourceID || got.Sources[i].Title != v.Sources[i].Title {
					t.Fatalf("source %d identity/text/order changed", i)
				}
			}
		})
	}
}

func TestAnswerCitationManifestV1FieldDenials(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*domain.AnswerCitationManifestV1)
	}{
		{"schema", func(v *domain.AnswerCitationManifestV1) { v.Schema = "other" }},
		{"run", func(v *domain.AnswerCitationManifestV1) { v.RunID = "" }},
		{"attempt", func(v *domain.AnswerCitationManifestV1) { v.AttemptID = "" }},
		{"nil-sources", func(v *domain.AnswerCitationManifestV1) { v.Sources = nil }},
		{"eleven-sources", func(v *domain.AnswerCitationManifestV1) { v.Sources = citationManifest(t, 11).Sources }},
		{"duplicate-id", func(v *domain.AnswerCitationManifestV1) { v.Sources = append(v.Sources, v.Sources[0]) }},
		{"invented-id", func(v *domain.AnswerCitationManifestV1) { v.Sources[0].SourceID = "source-" + strings.Repeat("c", 64) }},
		{"rank-zero", func(v *domain.AnswerCitationManifestV1) { v.Sources[0].Rank = 0 }},
		{"rank-six", func(v *domain.AnswerCitationManifestV1) { v.Sources[0].Rank = 6 }},
		{"operation", func(v *domain.AnswerCitationManifestV1) { v.Sources[0].OperationDigest = "bad" }},
		{"observation", func(v *domain.AnswerCitationManifestV1) { v.Sources[0].ObservationDigest = "bad" }},
		{"response", func(v *domain.AnswerCitationManifestV1) { v.Sources[0].ResponseDigest = strings.Repeat("A", 64) }},
		{"empty-title", func(v *domain.AnswerCitationManifestV1) { v.Sources[0].Title = "" }},
		{"large-title", func(v *domain.AnswerCitationManifestV1) { v.Sources[0].Title = strings.Repeat("x", 513) }},
		{"invalid-utf8", func(v *domain.AnswerCitationManifestV1) { v.Sources[0].Title = string([]byte{0xff}) }},
		{"retrieval-zero", func(v *domain.AnswerCitationManifestV1) { v.Sources[0].RetrievedAt = time.Time{} }},
		{"retrieval-submicrosecond", func(v *domain.AnswerCitationManifestV1) {
			v.Sources[0].RetrievedAt = v.Sources[0].RetrievedAt.Add(time.Nanosecond)
		}},
		{"publication-zero", func(v *domain.AnswerCitationManifestV1) { z := time.Time{}; v.Sources[0].PublishedAt = &z }},
		{"content-kind", func(v *domain.AnswerCitationManifestV1) { v.Sources[0].ContentKind = "page_read" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := citationManifest(t, 1)
			tc.change(&v)
			body, err := domain.EncodeAnswerCitationManifestV1(v)
			if err == nil || body != nil || v.Validate() == nil {
				t.Fatal("invalid typed manifest accepted")
			}
			if strings.Contains(err.Error(), "Untrusted") {
				t.Fatal("error disclosed source text")
			}
		})
	}
	for _, uri := range []string{"http://example.com/", "https://127.0.0.1/", "https://host.local/", "https://user:pass@example.com/", "https://example.com/" + strings.Repeat("x", 2048)} {
		v := citationManifest(t, 1)
		v.Sources[0].URL = uri
		v.Sources[0].SourceID = domain.WebSearchSourceIDV1(v.Sources[0].OperationDigest, 1, uri)
		if v.Validate() == nil {
			t.Fatal("unsafe/oversized URL accepted with otherwise coherent ID")
		}
	}
}

func TestAnswerCitationManifestV1StrictWire(t *testing.T) {
	body := string(encodeCitation(t, citationManifest(t, 1)))
	for _, tc := range []struct{ name, old, next string }{
		{"schema-alias", `"schema":`, `"Schema":`},
		{"schema-duplicate", `"schema":`, `"schema":"other","schema":`},
		{"escaped-duplicate", `"schema":`, `"\u0073chema":"other","schema":`},
		{"extra", `"run_id":`, `"private_receipt":"secret","run_id":`},
		{"missing-false", `"title_truncated":false,`, ``},
		{"null-false", `"title_truncated":false`, `"title_truncated":null`},
		{"null-published", `"content_kind":`, `"published_at":null,"content_kind":`},
		{"missing-rank", `"rank":1,`, ``},
		{"numeric-alias", `"rank":1,`, `"rank":1.0,`},
		{"null-title", `"title":"Untrusted \u003ctitle\u003e \u0026 source"`, `"title":null`},
		{"unpaired-surrogate", `"title":"Untrusted \u003ctitle\u003e \u0026 source"`, `"title":"\ud800"`},
		{"field-alias", `"source_id":`, `"Source_ID":`},
		{"noncanonical-time", `2026-11-04T00:00:00.000123Z`, `2026-11-04T00:00:00.000123+00:00`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(body, tc.old) {
				t.Fatal("mutation baseline absent")
			}
			mutated := []byte(strings.Replace(body, tc.old, tc.next, 1))
			if _, err := domain.DecodeAnswerCitationManifestV1(mutated); err == nil {
				t.Fatal("invalid wire accepted")
			}
		})
	}
	for _, bad := range [][]byte{append([]byte(body), []byte(` {}`)...), append([]byte{' '}, []byte(body)...), []byte(`null`), []byte(`[]`), []byte(`{"schema":` + strings.Repeat(`[`, 6) + `0` + strings.Repeat(`]`, 6) + `}`), []byte{0xff}} {
		if _, err := domain.DecodeAnswerCitationManifestV1(bad); err == nil {
			t.Fatal("noncanonical/malformed document accepted")
		}
	}
}

func TestAnswerCitationManifestV1ExactByteBound(t *testing.T) {
	v := citationManifest(t, 10)
	for i := range v.Sources {
		v.Sources[i].Title = strings.Repeat("t", 500)
		v.Sources[i].URL = "https://example.com/" + strings.Repeat("a", 1000)
		v.Sources[i].SourceID = domain.WebSearchSourceIDV1(v.Sources[i].OperationDigest, v.Sources[i].Rank, v.Sources[i].URL)
	}
	// Each literal '<' is valid URL syntax but costs six canonical JSON bytes.
	// At most ten bounded steps build a legitimate exact-cap representation.
	for i := range v.Sources {
		body, err := json.Marshal(v)
		if err != nil || len(body) > domain.AnswerCitationManifestBytesV1 {
			t.Fatal("invalid bound setup")
		}
		n := min(1000, (domain.AnswerCitationManifestBytesV1-len(body))/5)
		v.Sources[i].URL = "https://example.com/" + strings.Repeat("<", n) + strings.Repeat("a", 1000-n)
		v.Sources[i].SourceID = domain.WebSearchSourceIDV1(v.Sources[i].OperationDigest, v.Sources[i].Rank, v.Sources[i].URL)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	remaining := domain.AnswerCitationManifestBytesV1 - len(raw)
	if remaining < 0 || remaining > 4 {
		t.Fatalf("exact-cap setup remainder=%d", remaining)
	}
	v.Sources[0].Title += strings.Repeat("t", remaining)
	body := encodeCitation(t, v)
	if len(body) != domain.AnswerCitationManifestBytesV1 {
		t.Fatalf("cap got=%d want=%d", len(body), domain.AnswerCitationManifestBytesV1)
	}
	if _, err := domain.DecodeAnswerCitationManifestV1(body); err != nil {
		t.Fatalf("exact cap decode: %v", err)
	}
	v.Sources[0].Title += "t"
	if b, err := domain.EncodeAnswerCitationManifestV1(v); err == nil || b != nil {
		t.Fatal("first byte over cap accepted")
	}
	if _, err := domain.DecodeAnswerCitationManifestV1(append(body, ' ')); err == nil {
		t.Fatal("oversized wire accepted")
	}
}

func TestAnswerCitationManifestV1CloneAndRedaction(t *testing.T) {
	v := citationManifest(t, 1)
	published := v.Sources[0].RetrievedAt.Add(-time.Hour)
	v.Sources[0].PublishedAt = &published
	before := encodeCitation(t, v)
	copy := v.Clone()
	copy.Sources[0].Title = "changed"
	*copy.Sources[0].PublishedAt = published.Add(-time.Hour)
	if !bytes.Equal(before, encodeCitation(t, v)) {
		t.Fatal("clone mutated source owner")
	}
	for _, value := range []any{v, &v, v.Sources[0], &v.Sources[0]} {
		for _, format := range []string{"%v", "%+v", "%#v", "%q"} {
			if strings.Contains(fmt.Sprintf(format, value), "Untrusted") {
				t.Fatal("formatting disclosed title")
			}
		}
	}
	invalid := v.Clone()
	invalid.Sources = nil
	if invalid.Clone().Sources != nil {
		t.Fatal("clone repaired invalid absent sources")
	}
}
