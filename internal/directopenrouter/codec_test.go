package directopenrouter

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEncodeRequestUsesExactClosedShape(t *testing.T) {
	t.Parallel()
	encoded, err := encodeRequest([]byte("Sessionless canonical transcript v1\n[user]\npublic fixture\n"), 8192)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"model":"stealth/ox-alpha","messages":[{"role":"user","content":"Sessionless canonical transcript v1\n[user]\npublic fixture\n"}],"max_completion_tokens":8192,"stream":false,"provider":{"allow_fallbacks":false,"require_parameters":true,"only":["stealth"]}}`
	if string(encoded) != want {
		t.Fatalf("request = %s", encoded)
	}
	if strings.Contains(string(encoded), "models") || strings.Contains(string(encoded), "user_id") {
		t.Fatalf("request contains alternate routing or identity fields: %s", encoded)
	}
}

func TestParseResponseRequiresExactRouteModelAndUsage(t *testing.T) {
	t.Parallel()
	observation, err := parseResponse(successResponse("bounded result"))
	if err != nil {
		t.Fatal(err)
	}
	if string(observation.Final) != "bounded result" || observation.InputTokens != 11 || observation.OutputTokens != 7 {
		t.Fatalf("observation = %+v", observation)
	}
	clear(observation.Final)

	mutations := map[string]func(map[string]any){
		"model":            func(value map[string]any) { value["model"] = "stealth/other" },
		"unknown":          func(value map[string]any) { value["private"] = "body" },
		"fallback attempt": func(value map[string]any) { value["openrouter_metadata"].(map[string]any)["attempt"] = float64(2) },
		"route strategy":   func(value map[string]any) { value["openrouter_metadata"].(map[string]any)["strategy"] = "fallback" },
		"usage":            func(value map[string]any) { value["usage"].(map[string]any)["total_tokens"] = float64(19) },
		"provider": func(value map[string]any) {
			value["openrouter_metadata"].(map[string]any)["endpoints"].(map[string]any)["available"].([]any)[0].(map[string]any)["provider"] = "Other"
		},
		"extra choice": func(value map[string]any) {
			choices := value["choices"].([]any)
			value["choices"] = append(choices, choices[0])
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			var value map[string]any
			if err := json.Unmarshal(successResponse("answer"), &value); err != nil {
				t.Fatal(err)
			}
			mutate(value)
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := parseResponse(encoded); err == nil {
				t.Fatal("mutated response was accepted")
			}
		})
	}
}

func TestParseResponseRejectsAmbiguousAndUnboundedJSON(t *testing.T) {
	t.Parallel()
	base := string(successResponse("answer"))
	tests := map[string][]byte{
		"duplicate": []byte(strings.Replace(base, `"object":"chat.completion"`, `"object":"chat.completion","Object":"chat.completion"`, 1)),
		"trailing":  append(successResponse("answer"), []byte(` {}`)...),
		"truncated": successResponse("answer")[:len(successResponse("answer"))-1],
		"nul":       append(successResponse("answer"), 0),
		"oversized": []byte(`{"id":"` + strings.Repeat("x", maxResponseBytes) + `"}`),
	}
	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := parseResponse(value); err == nil {
				t.Fatal("ambiguous response was accepted")
			}
		})
	}
}

func FuzzResponseParserNeverCommitsMalformedTerminal(f *testing.F) {
	f.Add(successResponse("answer"))
	f.Add([]byte(`{"object":"chat.completion"}`))
	f.Fuzz(func(t *testing.T, value []byte) {
		observation, err := parseResponse(value)
		if err == nil && (len(observation.Final) == 0 || observation.InputTokens+observation.OutputTokens == 0) {
			t.Fatalf("invalid successful observation: %+v", observation)
		}
	})
}

func successResponse(final string) []byte {
	value := map[string]any{
		"id": "chatcmpl-fixture", "object": "chat.completion", "created": uint64(1), "model": WireModelIDV1,
		"system_fingerprint": nil,
		"choices":            []any{map[string]any{"index": uint64(0), "message": map[string]any{"role": "assistant", "content": final}, "finish_reason": "stop"}},
		"usage":              map[string]any{"prompt_tokens": uint64(11), "completion_tokens": uint64(7), "total_tokens": uint64(18)},
		"openrouter_metadata": map[string]any{
			"attempt": uint64(1), "generation_time": uint64(20), "is_byok": false, "region": "iad",
			"requested": WireModelIDV1, "strategy": "direct", "summary": "available=1, selected=Stealth",
			"endpoints": map[string]any{"total": uint64(1), "available": []any{map[string]any{"model": WireModelIDV1, "provider": metadataProviderNameV1, "selected": true}}},
		},
	}
	encoded, _ := json.Marshal(value)
	return encoded
}
