package ydbstore

import (
	"encoding/json"
	"testing"
)

func TestPrivateWebSearchConsentViewPreservesGeneratedResultLimit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		maximum *int
	}{
		{name: "omitted"},
		{name: "one", maximum: consentResultLimit(1)},
		{name: "three", maximum: consentResultLimit(3)},
		{name: "five", maximum: consentResultLimit(5)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPrivateConsentViewFixture(t)
			// These are explicit stored-pending DATA fixtures, not a genuine
			// MODEL producer. Keep the exact body and its retained commitments
			// coherent before the original private Store read.
			body, err := json.Marshal(struct {
				Query      string `json:"query"`
				MaxResults *int   `json:"max_results,omitempty"`
			}{f.query, tc.maximum})
			if err != nil {
				t.Fatal(err)
			}
			r := &f.state.Request
			r.Arguments.Size, r.Arguments.SHA256 = int64(len(body)), ownedConsentTestHash(body)
			r.ArgumentsDigest = r.Arguments.SHA256
			f.bodies[r.Arguments.Key] = body
			if err := r.ValidateArguments(body); err != nil {
				t.Fatal("coherent original argument fixture", err)
			}
			got, err := f.read(t, t.Context())
			if err != nil || got.Query != f.query {
				t.Fatalf("original private read: query=%q err=%v", got.Query, err)
			}
			// Inspect the returned DATA without assuming a new Go member:
			// this same test compiles on the unchanged baseline and exposes
			// the actual lost argument rather than a missing-API compile error.
			encoded, err := json.Marshal(got)
			var observed struct{ MaxResults *int }
			if err != nil || json.Unmarshal(encoded, &observed) != nil {
				t.Fatal("decode returned private DATA", err)
			}
			if (observed.MaxResults == nil) != (tc.maximum == nil) {
				t.Fatalf("max_results presence: got=%t want=%t", observed.MaxResults != nil, tc.maximum != nil)
			}
			if tc.maximum != nil && *observed.MaxResults != *tc.maximum {
				t.Fatalf("private view max_results: got=%d want=%d", *observed.MaxResults, *tc.maximum)
			}
		})
	}
}

func consentResultLimit(value int) *int { return &value }
