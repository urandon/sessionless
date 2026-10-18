package configuredinference

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// These are pure DATA tests, not authenticated provider observations, current
// authority, tool dispatch, summary authority, or a real model qualification.
func normalizedFreeze(t testing.TB, p domain.ConfiguredInferenceProfileV1, limits domain.ConfiguredInferenceLimitsV1, request RequestV1) *FrozenRequestV1 {
	t.Helper()
	f, err := FreezeRequestV1(p, limits, request)
	if err != nil {
		t.Fatalf("freeze normalized-response fixture: %v", err)
	}
	return f
}

func normalizedRecord(t testing.TB, f *FrozenRequestV1, wire []byte, kind domain.AttemptOperationKindV1) []byte {
	t.Helper()
	body, err := f.NormalizeResponseBodyV1(wire, kind, 64<<10)
	if err != nil || len(body) == 0 {
		t.Fatalf("normalize complete fixture: bytes=%d err=%v", len(body), err)
	}
	return body
}

func normalizedSHA(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func normalizedReplace(t testing.TB, body []byte, old, replacement string) []byte {
	t.Helper()
	if count := bytes.Count(body, []byte(old)); count != 1 {
		t.Fatalf("mutation oracle occurrences=%d, want one", count)
	}
	return bytes.Replace(body, []byte(old), []byte(replacement), 1)
}

func normalizedDenied(t testing.TB, p domain.ConfiguredInferenceProfileV1, request, record []byte, kind domain.AttemptOperationKindV1) {
	t.Helper()
	out, err := DecodeNormalizedResponseBodyV1(p, request, record, kind)
	if err == nil || !reflect.DeepEqual(out, ResponseV1{}) {
		t.Fatalf("invalid normalized DATA returned usable response: err=%v stop=%s textBytes=%d call=%t usage=%s", err, out.Stop, len(out.Text), out.Call != nil, out.Usage.Provenance)
	}
}

func TestConfiguredNormalizedFinalToolAndExactEffectiveLimits(t *testing.T) {
	p := testProfile(t)
	limits := domain.ConfiguredInferenceLimitsV1{RequestBytes: 4096, ResponseBytes: 4096, ResponseHeaderBytes: 1024,
		JSONDepth: 12, JSONMembers: 64, Choices: 1, ToolCalls: 1, ToolNameBytes: 16, ToolCallIDBytes: 16,
		ToolArgumentsBytes: 128, FinalTextBytes: 256, Messages: 8, Tools: 1, MaxInputTokens: 128, MaxOutputTokens: 64,
		OperationTimeout: 2 * time.Second, CleanupTimeout: time.Second}
	f := normalizedFreeze(t, p, limits, testRequest())
	for _, tc := range []struct {
		name string
		wire []byte
		stop StopV1
		text string
		call bool
	}{
		{name: "final", wire: responseWire("alias-local", "stop", "complete answer", nil), stop: StopFinalV1, text: "complete answer"},
		{name: "tool", wire: toolWire("new-call", "lookup", ` { "query" : "docs" } `), stop: StopToolV1, call: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := normalizedRecord(t, f, tc.wire, domain.AttemptOperationModelV1)
			var facts struct {
				Version       uint32
				Kind          string
				OperationKind domain.AttemptOperationKindV1 `json:"operation_kind"`
				ProfileDigest string                        `json:"profile_digest"`
				RequestDigest string                        `json:"request_digest"`
				Limits        domain.ConfiguredInferenceLimitsV1
			}
			if err := json.Unmarshal(record, &facts); err != nil {
				t.Fatal(err)
			}
			pd, _ := p.Digest()
			if facts.Version != 1 || facts.Kind != "configured_model_response" || facts.OperationKind != domain.AttemptOperationModelV1 || facts.ProfileDigest != pd || facts.RequestDigest != normalizedSHA(f.Body()) || facts.RequestDigest == f.Digest() || facts.Limits != limits {
				t.Fatal("normalized envelope lost full profile, RAW request SHA, kind, or exact narrower effective limits")
			}
			out, err := DecodeNormalizedResponseBodyV1(p, f.Body(), record, domain.AttemptOperationModelV1)
			if err != nil || out.Stop != tc.stop || out.Text != tc.text || (out.Call != nil) != tc.call || out.ConfiguredModel != p.ModelID || out.ModelProvenance != ModelReportedV1 {
				t.Fatalf("normalized roundtrip: err=%v stop=%s call=%t modelProvenance=%s", err, out.Stop, out.Call != nil, out.ModelProvenance)
			}
			if tc.call && (out.Call.ID != "new-call" || out.Call.Name != "lookup" || string(out.Call.Arguments) != `{"query":"docs"}`) {
				t.Fatal("complete schema-valid function call lost correlation or canonical arguments")
			}
			if tc.name == "final" && out.ReportedModel != "alias-local" {
				t.Fatal("reported alias was replaced with configured identity")
			}
		})
	}
}

func TestConfiguredNormalizedRestoresOriginalSchemaBytesAndHistory(t *testing.T) {
	p := testProfile(t)
	r := testRequest()
	r.Tools[0].Parameters = []byte(" \n { \"required\" : [\"query\"], \"additionalProperties\" : false, \"properties\" : {\"query\":{\"maxLength\":30,\"type\":\"string\",\"minLength\":1}}, \"type\" : \"object\" } \t ")
	r.Messages = append(r.Messages,
		MessageV1{Role: RoleAssistantV1, Call: &FunctionCallV1{ID: "history-call", Name: "lookup", Arguments: []byte(`{"query":"old"}`)}},
		MessageV1{Role: RoleToolV1, ToolCallID: "history-call", Text: "original result"})
	f := normalizedFreeze(t, p, p.Limits, r)
	request := f.Body()
	if !bytes.Contains(request, r.Tools[0].Parameters) {
		t.Fatal("fixture did not retain original leading/trailing schema whitespace and key order")
	}
	record := normalizedRecord(t, f, toolWire("fresh-call", "lookup", `{"query":"next"}`), domain.AttemptOperationModelV1)
	out, err := DecodeNormalizedResponseBodyV1(p, request, record, domain.AttemptOperationModelV1)
	if err != nil || out.Call == nil || out.Call.ID != "fresh-call" {
		t.Fatalf("original schema/history restore: err=%v call=%t", err, out.Call != nil)
	}

	// Canonical mutation keeps the original request/profile digests; only the
	// returned call ID changes to an ID already consumed in that exact history.
	repeated := normalizedReplace(t, record, `"id":"fresh-call"`, `"id":"history-call"`)
	normalizedDenied(t, p, request, repeated, domain.AttemptOperationModelV1)
	if raw, err := f.NormalizeResponseBodyV1(toolWire("history-call", "lookup", `{"query":"next"}`), domain.AttemptOperationModelV1, 64<<10); err == nil || len(raw) != 0 {
		t.Fatal("normalizer accepted a history-reused call ID")
	}

	for _, tc := range []struct{ name, old, replacement string }{
		{name: "orphan-result", old: `"tool_call_id":"history-call"`, replacement: `"tool_call_id":"orphan"`},
		{name: "unsupported-schema", old: `"additionalProperties" : false`, replacement: `"additionalProperties" : true`},
		{name: "wrong-model", old: `"model":"model-local"`, replacement: `"model":"foreign-model"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			badRequest := normalizedReplace(t, request, tc.old, tc.replacement)
			// Recompute the raw-data locator so this exercises restoration, not
			// merely an early request-digest mismatch.
			bound := normalizedReplace(t, record, normalizedSHA(request), normalizedSHA(badRequest))
			normalizedDenied(t, p, badRequest, bound, domain.AttemptOperationModelV1)
		})
	}
}

func TestConfiguredNormalizedRejectsCanonicalSemanticMutations(t *testing.T) {
	p := testProfile(t)
	f := normalizedFreeze(t, p, p.Limits, testRequest())
	record := normalizedRecord(t, f, responseWire("model-local", "stop", "complete", nil), domain.AttemptOperationModelV1)
	pd, _ := p.Digest()
	for _, tc := range []struct{ name, old, replacement string }{
		{name: "version", old: `"version":1`, replacement: `"version":2`},
		{name: "body-kind", old: `"kind":"configured_model_response"`, replacement: `"kind":"other"`},
		{name: "operation-kind", old: `"operation_kind":"model"`, replacement: `"operation_kind":"compaction"`},
		{name: "profile-digest", old: pd, replacement: strings.Repeat("9", 64)},
		{name: "request-codec-digest-is-not-raw-SHA", old: normalizedSHA(f.Body()), replacement: f.Digest()},
		{name: "unknown-reported-model", old: `"reported_model":"model-local"`, replacement: `"reported_model":"foreign"`},
		{name: "empty-text", old: `"text":"complete"`, replacement: `"text":""`},
		{name: "non-success-stop", old: `"stop":"final"`, replacement: `"stop":"length"`},
		{name: "measured-provenance", old: `"state":"provider_reported"`, replacement: `"state":"observed"`},
		{name: "unknown-with-numbers", old: `"state":"provider_reported"`, replacement: `"state":"unknown"`},
		{name: "invented-money", old: `"output_tokens":2`, replacement: `"output_tokens":2,"amount_microunits":0`},
		{name: "zero-effective-limit", old: `"tool_arguments_bytes":4096`, replacement: `"tool_arguments_bytes":0`},
		{name: "wider-than-profile", old: `"max_output_tokens":1024`, replacement: `"max_output_tokens":1025`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			normalizedDenied(t, p, f.Body(), normalizedReplace(t, record, tc.old, tc.replacement), domain.AttemptOperationModelV1)
		})
	}
	changedProfile := p.Clone()
	changedProfile.ProfileRevision++
	normalizedDenied(t, changedProfile, f.Body(), record, domain.AttemptOperationModelV1)
	changedRequest := normalizedReplace(t, f.Body(), `"content":"question"`, `"content":"changed"`)
	normalizedDenied(t, p, changedRequest, record, domain.AttemptOperationModelV1)
	for _, kind := range []domain.AttemptOperationKindV1{"", domain.AttemptOperationMCPToolV1, domain.AttemptOperationWebSearchV1} {
		normalizedDenied(t, p, f.Body(), record, kind)
	}
}

func TestConfiguredNormalizedToolArgumentsAndExclusiveShape(t *testing.T) {
	p := testProfile(t)
	f := normalizedFreeze(t, p, p.Limits, testRequest())
	record := normalizedRecord(t, f, toolWire("fresh", "lookup", `{"query":"docs"}`), domain.AttemptOperationModelV1)
	for _, tc := range []struct{ name, old, replacement string }{
		{name: "foreign-name", old: `"name":"lookup"`, replacement: `"name":"foreign"`},
		{name: "schema-value", old: `"query":"docs"`, replacement: `"query":""`},
		{name: "extra-argument", old: `"arguments":{"query":"docs"}`, replacement: `"arguments":{"extra":1,"query":"docs"}`},
		{name: "noncanonical-arguments", old: `"arguments":{"query":"docs"}`, replacement: `"arguments":{ "query":"docs"}`},
		{name: "text-plus-call", old: `"text":""`, replacement: `"text":"not exclusive"`},
		{name: "missing-call", old: `"call":{"id":"fresh","name":"lookup","arguments":{"query":"docs"}}`, replacement: `"call":null`},
		{name: "final-plus-call", old: `"stop":"tool_call"`, replacement: `"stop":"final"`},
		{name: "too-tight-effective-arguments", old: `"tool_arguments_bytes":4096`, replacement: `"tool_arguments_bytes":1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			normalizedDenied(t, p, f.Body(), normalizedReplace(t, record, tc.old, tc.replacement), domain.AttemptOperationModelV1)
		})
	}
}

func TestConfiguredNormalizedRejectsMalformedAndNoncanonicalRecords(t *testing.T) {
	p := testProfile(t)
	f := normalizedFreeze(t, p, p.Limits, testRequest())
	record := normalizedRecord(t, f, responseWire("model-local", "stop", "complete", nil), domain.AttemptOperationModelV1)
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{name: "empty"}, {name: "truncated", body: record[:len(record)-1]},
		{name: "trailing-object", body: append(bytes.Clone(record), []byte(`{}`)...)},
		{name: "leading-whitespace", body: append([]byte(" "), record...)},
		{name: "trailing-whitespace", body: append(bytes.Clone(record), '\n')},
		{name: "duplicate-key", body: append([]byte(`{"version":1,`), record[1:]...)},
		{name: "escaped-duplicate-key", body: append([]byte(`{"\u0076ersion":1,`), record[1:]...)},
		{name: "unknown-key", body: append(bytes.Clone(record[:len(record)-1]), []byte(`,"unknown":true}`)...)},
		{name: "unpaired-surrogate", body: normalizedReplace(t, record, `"text":"complete"`, `"text":"\ud800"`)},
		{name: "invalid-UTF8", body: bytes.Replace(record, []byte("complete"), []byte{0xff}, 1)},
		{name: "overflow-number", body: normalizedReplace(t, record, `"input_tokens":9`, `"input_tokens":1e9999`)},
		{name: "underflow-number", body: normalizedReplace(t, record, `"input_tokens":9`, `"input_tokens":1e-9999`)},
		{name: "fractional-count", body: normalizedReplace(t, record, `"input_tokens":9`, `"input_tokens":1.5`)},
		{name: "uint64-overflow", body: normalizedReplace(t, record, `"input_tokens":9`, `"input_tokens":18446744073709551616`)},
		{name: "noncanonical-version-number", body: normalizedReplace(t, record, `"version":1`, `"version":1.0`)},
		{name: "depth", body: append([]byte(strings.Repeat("[", 65)), []byte("0"+strings.Repeat("]", 65))...)},
		{name: "members", body: []byte("[" + strings.Repeat("0,", 4096) + "0]")},
		{name: "global-record-ceiling", body: bytes.Repeat([]byte(" "), (64<<10)+1)},
	} {
		t.Run(tc.name, func(t *testing.T) { normalizedDenied(t, p, f.Body(), tc.body, domain.AttemptOperationModelV1) })
	}
}

func TestConfiguredNormalizedNeverPromotesNonSuccessWire(t *testing.T) {
	f := freeze(t)
	for _, tc := range []struct {
		name string
		wire []byte
	}{
		{name: "length", wire: responseWire("model-local", "length", "private partial", nil)},
		{name: "error-finish", wire: responseWire("model-local", "error", "private error", nil)},
		{name: "filtered", wire: responseWire("model-local", "content_filter", "private partial", nil)},
		{name: "unknown-finish", wire: responseWire("model-local", "other", "private partial", nil)},
		{name: "unverified-model", wire: responseWire("foreign", "stop", "answer", nil)},
		{name: "service-error", wire: []byte(`{"model":"model-local","error":{"message":"private transport details"}}`)},
		{name: "refusal", wire: []byte(`{"model":"model-local","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"private text","refusal":"not allowed"}}]}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if body, err := f.NormalizeResponseBodyV1(tc.wire, domain.AttemptOperationModelV1, 64<<10); err == nil || len(body) != 0 {
				t.Fatalf("non-success wire exported normalized success: bytes=%d err=%v", len(body), err)
			}
		})
	}
}

func TestConfiguredNormalizedOmitsHiddenReasoningAndRawWireMetadata(t *testing.T) {
	p := testProfile(t)
	f := normalizedFreeze(t, p, p.Limits, testRequest())
	const hidden = "private-reasoning-marker"
	const rawID = "private-wire-request-marker"
	wire := []byte(`{"id":"` + rawID + `","model":"model-local","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"public answer","reasoning_content":"` + hidden + `"}}]}`)
	record := normalizedRecord(t, f, wire, domain.AttemptOperationModelV1)
	if bytes.Contains(record, []byte(hidden)) || bytes.Contains(record, []byte(rawID)) || bytes.Contains(record, []byte("reasoning_content")) {
		t.Fatal("normalized body retained private reasoning or raw transport metadata")
	}
	out, err := DecodeNormalizedResponseBodyV1(p, f.Body(), record, domain.AttemptOperationModelV1)
	if err != nil || out.Text != "public answer" || out.Stop != StopFinalV1 {
		t.Fatalf("redaction lost complete public result: err=%v stop=%s", err, out.Stop)
	}
}

func TestConfiguredNormalizedUsageUnknownPartialAndOverrunRemainTruthful(t *testing.T) {
	p := testProfile(t)
	f := normalizedFreeze(t, p, p.Limits, testRequest())
	for _, tc := range []struct {
		name, usage string
		want        domain.AttemptOperationUsageV1
		overrun     bool
	}{
		{name: "unknown", usage: `null`, want: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}},
		{name: "no-valid-component", usage: `{"prompt_tokens":-1,"completion_tokens":"bad"}`, want: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}},
		{name: "partial-known-overrun", usage: `{"prompt_tokens":100,"completion_tokens":"bad","total_tokens":0}`, want: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageCount(100)}, overrun: true},
		{name: "mismatched-total-preserves-components", usage: `{"prompt_tokens":100,"completion_tokens":2,"total_tokens":1}`, want: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageCount(100), OutputTokens: usageCount(2)}, overrun: true},
		{name: "reported-zero-not-unknown", usage: `{"completion_tokens":0}`, want: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, OutputTokens: usageCount(0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(responseWire("model-local", "stop", "complete", nil), &wire); err != nil {
				t.Fatal(err)
			}
			wire["usage"] = json.RawMessage(tc.usage)
			raw, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			record := normalizedRecord(t, f, raw, domain.AttemptOperationModelV1)
			out, err := DecodeNormalizedResponseBodyV1(p, f.Body(), record, domain.AttemptOperationModelV1)
			if err != nil {
				t.Fatal(err)
			}
			usage, err := out.Usage.OperationUsage()
			if err != nil || !reflect.DeepEqual(usage, tc.want) || usage.AmountMicrounits != nil || usage.ExceedsReservation(domain.AttemptOperationDebitV1{InputTokens: 10, OutputTokens: 5}) != tc.overrun {
				t.Fatalf("usage provenance/components/overrun changed: err=%v state=%s", err, usage.State)
			}
		})
	}
}

func TestConfiguredNormalizedCompactionFinalOnlyAndSummaryByteBound(t *testing.T) {
	p := testProfile(t)
	f := normalizedFreeze(t, p, p.Limits, testRequest())
	for _, size := range []int{16 << 10, (16 << 10) + 1} {
		wire := responseWire("model-local", "stop", strings.Repeat("x", size), nil)
		record, err := f.NormalizeResponseBodyV1(wire, domain.AttemptOperationCompactionV1, 64<<10)
		if size == 16<<10 {
			if err != nil {
				t.Fatalf("exact summary bound rejected: %v", err)
			}
			out, err := DecodeNormalizedResponseBodyV1(p, f.Body(), record, domain.AttemptOperationCompactionV1)
			if err != nil || len(out.Text) != size || out.Stop != StopFinalV1 || out.Call != nil {
				t.Fatalf("compaction final DATA: err=%v bytes=%d", err, len(out.Text))
			}
			normalizedDenied(t, p, f.Body(), record, domain.AttemptOperationModelV1)
		} else if err == nil || len(record) != 0 {
			t.Fatal("first summary byte over bound accepted")
		}
	}
	if record, err := f.NormalizeResponseBodyV1(toolWire("fresh", "lookup", `{"query":"docs"}`), domain.AttemptOperationCompactionV1, 64<<10); err == nil || len(record) != 0 {
		t.Fatal("compaction tool call became a summary")
	}
	modelRecord := normalizedRecord(t, f, responseWire("model-local", "stop", strings.Repeat("x", (16<<10)+1), nil), domain.AttemptOperationModelV1)
	retagged := normalizedReplace(t, modelRecord, `"operation_kind":"model"`, `"operation_kind":"compaction"`)
	normalizedDenied(t, p, f.Body(), retagged, domain.AttemptOperationCompactionV1)
}

func TestConfiguredNormalizedEncodedCapCountsEscapingAndEnvelope(t *testing.T) {
	f := freeze(t)
	raw := responseWire("model-local", "stop", strings.Repeat("\"\\\n", 20), nil)
	record := normalizedRecord(t, f, raw, domain.AttemptOperationModelV1)
	cap := uint64(len(record))
	if exact, err := f.NormalizeResponseBodyV1(raw, domain.AttemptOperationModelV1, cap); err != nil || !bytes.Equal(exact, record) {
		t.Fatalf("exact encoded envelope boundary rejected: bytes=%d err=%v", len(exact), err)
	}
	for _, bound := range []uint64{0, cap - 1, (64 << 10) + 1} {
		if out, err := f.NormalizeResponseBodyV1(raw, domain.AttemptOperationModelV1, bound); err == nil || len(out) != 0 {
			t.Fatalf("encoded cap %d accepted %d bytes: %v", bound, len(out), err)
		}
	}
}

func TestConfiguredNormalizedReturnedBuffersAndUsageAreOwned(t *testing.T) {
	p := testProfile(t)
	f := normalizedFreeze(t, p, p.Limits, testRequest())
	raw := toolWire("fresh", "lookup", `{"query":"docs"}`)
	record := normalizedRecord(t, f, raw, domain.AttemptOperationModelV1)
	baseline := bytes.Clone(record)
	out, err := DecodeNormalizedResponseBodyV1(p, f.Body(), record, domain.AttemptOperationModelV1)
	if err != nil || out.Call == nil || out.Usage.InputTokens == nil {
		t.Fatalf("owned fixture decode: %v", err)
	}
	clear(out.Call.Arguments)
	*out.Usage.InputTokens = 999
	if !bytes.Equal(record, baseline) {
		t.Fatal("returned arguments/usage alias encoded record")
	}
	again, err := DecodeNormalizedResponseBodyV1(p, f.Body(), record, domain.AttemptOperationModelV1)
	if err != nil || again.Call == nil || again.Usage.InputTokens == nil || string(again.Call.Arguments) != `{"query":"docs"}` || *again.Usage.InputTokens != 9 {
		t.Fatalf("returned mutation affected a second decode: %v", err)
	}
	clear(record)
	clear(raw)
	if string(again.Call.Arguments) != `{"query":"docs"}` || *again.Usage.InputTokens != 9 {
		t.Fatal("input mutation affected retained decoded response")
	}
	newRecord := normalizedRecord(t, f, toolWire("fresh", "lookup", `{"query":"docs"}`), domain.AttemptOperationModelV1)
	if !bytes.Equal(newRecord, baseline) {
		t.Fatal("caller mutation changed frozen request or later normalization")
	}
}
