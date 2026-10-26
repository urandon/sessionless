package ydbstore

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestInlineReceiptVersionDispatchDoesNotCopyPayload(t *testing.T) {
	r := maximumInlineIdentityFixtureV2(t)
	if r.Materialization.Completion.Events[0].DisplayText != r.Candidate.Summary {
		t.Fatal("maximum answer fixture lost interior whitespace or display bytes")
	}
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(raw)
	allocations := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			version, err := attachedWorkerReceiptVersion(raw)
			if err != nil || version != 2 {
				b.Fatal("maximum receipt version dispatch failed", err)
			}
		}
	})
	// Only the tiny top-level names/version are needed. This measures dispatch
	// allocations, NOT total decoder allocation or a whole-invocation heap peak.
	if got := allocations.AllocedBytesPerOp(); got > 32<<10 {
		t.Fatalf("version dispatch copied receipt content: allocated=%d limit=%d record=%d", got, 32<<10, len(raw))
	}
	t.Logf("version dispatch allocated=%d B/op, original record=%d B; not full decoder/heap proof", allocations.AllocedBytesPerOp(), len(raw))
}

func TestInlineReceiptVersionDispatchPreservesScalarSemantics(t *testing.T) {
	for _, raw := range []string{
		`{"version":1}`, `{"version":2}`, `{"version":0}`, `{"version":99}`, `{"version":4294967295}`,
		`{"version":4294967296}`, `{"version":-1}`, `{"version":-0}`, `{"version":1.0}`, `{"version":2e0}`,
		`{"version":null}`, `{"version":false}`, `{"version":"2"}`, `{"version":{}}`, `{"version":[]}`,
		`{"version":2,"Version":1}`, `{"Version":2}`, `{"vErSiOn":2}`, `{"vers\u0069on":2}`, `{}`, `null`, `[]`,
		`{"version":2,"ignored":{"text":"<\u2028>","version":1,"counter":9007199254740993123456789}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			// Independent previous dispatch, including its exact uint32/null
			// and case-alias behavior. Complete envelope checks remain separate.
			var old map[string]json.RawMessage
			want, valid := uint32(0), json.Unmarshal([]byte(raw), &old) == nil
			for key := range old {
				if strings.EqualFold(key, "version") && key != "version" {
					valid = false
				}
			}
			value, exists := old["version"]
			valid = valid && exists && json.Unmarshal(value, &want) == nil
			got, err := attachedWorkerReceiptVersion([]byte(raw))
			if valid && (err != nil || got != want) || !valid && !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
				t.Fatalf("version scalar dispatch got=%d err=%v old=%d valid=%t", got, err, want, valid)
			}
		})
	}
}
