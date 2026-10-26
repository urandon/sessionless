package strictjson

import (
	"strings"
	"testing"
)

func TestDocumentProspectiveStringBounds(t *testing.T) {
	for _, tc := range []struct {
		name, raw    string
		token, total uint64
		valid        bool
	}{
		{"exact", `{"a":"abc"}`, 3, 4, true},
		{"token-overflow", `{"a":"abc"}`, 2, 4, false},
		{"total-overflow", `{"a":"abc"}`, 3, 3, false},
		{"key-is-charged", `{"abcd":""}`, 3, 10, false},
		{"escaped-key-and-value", `{"\u0061":"\u0001"}`, 1, 2, true},
		{"escaped-total-overflow", `{"\u0061":"\u0001"}`, 1, 1, false},
		{"unicode-byte-count", `{"é":"\ud83d\ude00"}`, 4, 6, true},
		{"unicode-byte-overflow", `{"é":"\ud83d\ude00"}`, 4, 5, false},
		{"multiple-values", `["ab","cd"]`, 2, 4, true},
		{"multiple-values-overflow", `["ab","cd"]`, 2, 3, false},
		{"nested-keys-count", `{"a":{"b":"c"}}`, 1, 3, true},
		{"nested-budget-overflow", `{"a":{"b":"c"}}`, 1, 2, false},
		{"empty-tokens-zero-budget", `{"":""}`, 0, 0, true},
		{"zero-is-not-unlimited", `["a"]`, 0, 0, false},
		{"no-strings-zero-budget", `[null,true,1]`, 0, 0, true},
		{"duplicate-decoded-key", `{"a":"","\u0061":""}`, 8, 16, false},
		{"invalid-surrogate", `{"a":"\ud800"}`, 8, 16, false},
		{"trailing-document", `{"a":"b"}{}`, 8, 16, false},
		{"large-key-before-unescape", `{"` + strings.Repeat("k", 100_000) + `":""}`, 32 << 10, 96 << 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDocumentStringBounds([]byte(tc.raw), 256<<10, 16, 64, StringBounds{TokenBytes: tc.token, TotalBytes: tc.total})
			if (err == nil) != tc.valid {
				t.Fatalf("bounded string validation valid=%t want=%t token=%d total=%d: %v", err == nil, tc.valid, tc.token, tc.total, err)
			}
		})
	}
}

func TestDocumentStringBoundsRetainLexicalLimits(t *testing.T) {
	raw := []byte(`{"a":["b","c"]}`)
	for _, tc := range []struct {
		name                  string
		bytes, depth, members uint64
	}{
		{"raw", uint64(len(raw) - 1), 16, 64},
		{"depth", 256, 1, 64},
		{"members", 256, 16, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if ValidateDocumentStringBounds(raw, tc.bytes, tc.depth, tc.members, StringBounds{TokenBytes: 16, TotalBytes: 64}) == nil {
				t.Fatal("string budget bypassed existing lexical limit")
			}
		})
	}
}
