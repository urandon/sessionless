package configuredinference

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Synthetic DATA only: no installation, capability qualification, current
// authority, calibrated token fit, model/socket, credential or transport proof.
func testProfile(t testing.TB) domain.ConfiguredInferenceProfileV1 {
	t.Helper()
	hash := strings.Repeat("1", 64)
	ref := domain.OwnedAgentAuthorityBodyRefV1{Ref: "synthetic-evidence", Digest: hash, Bytes: 32}
	p := domain.ConfiguredInferenceProfileV1{
		Version: 1, TenantID: "codec-tenant", OwnerUserID: "codec-owner", ProfileRevision: 1,
		Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceLocalEndpointV1, ResourceID: "codec-resource", OwnerUserID: "codec-owner", Revision: 1, CredentialMode: domain.ProviderCredentialNoneV1},
		BaseURL:  "http://127.0.0.1:11434/v1", EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, NativeProtocolVersion: domain.ConfiguredInferenceProtocolChatCompletionsV1, Auth: domain.ConfiguredInferenceAuthNoneV1,
		ModelVendorID: "synthetic", ModelID: "model-local", ResponseModelIDs: []string{"alias-local", "model-local"}, ServerID: "synthetic-server", ServerVersion: "synthetic-version", ChatTemplateDigest: hash,
		Capabilities:       domain.ConfiguredInferenceCapabilitiesV1{Text: domain.ProviderEvidenceSupportedV1, Image: domain.ProviderEvidenceUnsupportedV1, File: domain.ProviderEvidenceUnsupportedV1, ToolCalls: domain.ProviderEvidenceSupportedV1, JSONSchema: domain.ProviderEvidenceUnsupportedV1},
		CapabilityEvidence: ref, ProviderCatalogDigest: hash, ProviderRouteDigest: hash, PrivacyPolicyDigest: hash, EffectivePolicyDigest: hash, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}, ModelWindowTokens: 4096, TokenEstimatorVersion: "synthetic-estimator", CalibrationDigest: hash,
		Limits:              domain.ConfiguredInferenceLimitsV1{RequestBytes: 1 << 20, ResponseBytes: 1 << 20, ResponseHeaderBytes: 16 << 10, JSONDepth: 32, JSONMembers: 256, Choices: 1, ToolCalls: 1, ToolNameBytes: 128, ToolCallIDBytes: 128, ToolArgumentsBytes: 4 << 10, FinalTextBytes: 256 << 10, Messages: 256, Tools: 16, MaxInputTokens: 1024, MaxOutputTokens: 1024, OperationTimeout: 30 * time.Second, CleanupTimeout: 30 * time.Second},
		LocalInstallationID: "synthetic-installation", LocalInstallationRevision: 1, LocalHostIdentityDigest: hash, LocalNetworkNamespaceIdentityDigest: hash, WorkerExecutableDigest: hash, OperatorTerms: ref, OriginalSource: ref, IssuedAt: time.Unix(100, 0).UTC(), ExpiresAt: time.Unix(3700, 0).UTC(),
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("synthetic profile: %v", err)
	}
	return p
}

const toolSchema = `{"type":"object","properties":{"query":{"type":"string","minLength":1,"maxLength":30}},"required":["query"],"additionalProperties":false}`

func testRequest() RequestV1 {
	return RequestV1{Messages: []MessageV1{{Role: RoleSystemV1, Text: "trusted instructions"}, {Role: RoleUserV1, Text: "question"}}, Tools: []ToolV1{{Name: "lookup", Description: "bounded synthetic lookup", Parameters: []byte(toolSchema)}}, MaxOutputTokens: 32}
}
func freeze(t testing.TB) *FrozenRequestV1 {
	t.Helper()
	p := testProfile(t)
	f, err := FreezeRequestV1(p, p.Limits, testRequest())
	if err != nil {
		t.Fatalf("freeze request: %v", err)
	}
	return f
}
func responseWire(model, finish string, content any, calls any) []byte {
	m := map[string]any{"role": "assistant", "content": content}
	if calls != nil {
		m["tool_calls"] = calls
	}
	raw, _ := json.Marshal(map[string]any{"model": model, "choices": []any{map[string]any{"index": 0, "message": m, "finish_reason": finish}}, "usage": map[string]any{"prompt_tokens": 9, "completion_tokens": 2, "total_tokens": 11}})
	return raw
}
func toolWire(id, name, args string) []byte {
	return responseWire("model-local", "tool_calls", nil, []any{map[string]any{"id": id, "type": "function", "function": map[string]any{"name": name, "arguments": args}}})
}

func TestConfiguredCodecIndependentWireAndCompleteContinuation(t *testing.T) {
	f := freeze(t)
	// Literal independent oracle, not a round trip through our own decoder.
	want := `{"model":"model-local","stream":false,"n":1,"max_tokens":32,"messages":[{"role":"system","content":"trusted instructions"},{"role":"user","content":"question"}],"tools":[{"type":"function","function":{"name":"lookup","description":"bounded synthetic lookup","parameters":` + toolSchema + `}}]}`
	if string(f.Body()) != want {
		t.Fatalf("wire mismatch\n got %s\nwant %s", f.Body(), want)
	}
	call, err := f.DecodeResponse(toolWire("call-1", "lookup", `{"query":"public docs"}`))
	if err != nil || call.Stop != StopToolV1 || call.Call == nil || string(call.Call.Arguments) != `{"query":"public docs"}` || call.Text != "" {
		t.Fatalf("complete call: stop=%s err=%v", call.Stop, err)
	}
	p := testProfile(t)
	r := testRequest()
	r.Messages = append(r.Messages, MessageV1{Role: RoleAssistantV1, Call: call.Call}, MessageV1{Role: RoleToolV1, ToolCallID: "call-1", Text: "bounded result"})
	next, err := FreezeRequestV1(p, p.Limits, r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(next.Body(), []byte(`{"role":"assistant","content":null,"tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"query\":\"public docs\"}"}}]},{"role":"tool","content":"bounded result","tool_call_id":"call-1"}`)) {
		t.Fatal("complete assistant/tool unit or original order was not preserved")
	}
	final, err := next.DecodeResponse(responseWire("model-local", "stop", "answer", nil))
	if err != nil || final.Stop != StopFinalV1 || final.Text != "answer" || final.Call != nil || final.ModelProvenance != ModelReportedV1 || final.Usage.Provenance != UsageProviderReportedV1 || *final.Usage.InputTokens != 9 {
		t.Fatalf("final answer: stop=%s err=%v", final.Stop, err)
	}
	if out, err := next.DecodeResponse(toolWire("call-1", "lookup", `{"query":"again"}`)); err == nil || out.Call != nil {
		t.Fatal("history call ID was reused")
	}
}

func TestConfiguredCodecFrozenCopiesAndDigest(t *testing.T) {
	p := testProfile(t)
	r := testRequest()
	f, err := FreezeRequestV1(p, p.Limits, r)
	if err != nil {
		t.Fatal(err)
	}
	body, digest := f.Body(), f.Digest()
	p.ResponseModelIDs[0] = "evil"
	r.Messages[0].Text = "changed"
	r.Tools[0].Name = "changed"
	clear(r.Tools[0].Parameters)
	copyBody := f.Body()
	clear(copyBody)
	if !bytes.Equal(f.Body(), body) || f.Digest() != digest {
		t.Fatal("caller mutation changed frozen bytes/digest")
	}
	if out, err := f.DecodeResponse(toolWire("new-call", "lookup", `{"query":"valid"}`)); err != nil || out.Call == nil {
		t.Fatal("caller mutation changed frozen schema/catalog")
	}
	base := testProfile(t)
	for _, change := range []func(*domain.ConfiguredInferenceProfileV1, *domain.ConfiguredInferenceLimitsV1, *RequestV1){
		func(p *domain.ConfiguredInferenceProfileV1, _ *domain.ConfiguredInferenceLimitsV1, _ *RequestV1) {
			p.BaseURL = "http://[::1]:11434/v1"
		},
		func(p *domain.ConfiguredInferenceProfileV1, _ *domain.ConfiguredInferenceLimitsV1, _ *RequestV1) {
			p.ProfileRevision++
		},
		func(_ *domain.ConfiguredInferenceProfileV1, l *domain.ConfiguredInferenceLimitsV1, _ *RequestV1) {
			l.CleanupTimeout -= time.Second
		},
		func(_ *domain.ConfiguredInferenceProfileV1, l *domain.ConfiguredInferenceLimitsV1, _ *RequestV1) {
			l.MaxInputTokens--
		},
		func(_ *domain.ConfiguredInferenceProfileV1, _ *domain.ConfiguredInferenceLimitsV1, r *RequestV1) {
			r.Messages[1].Text = "changed"
		},
	} {
		p := base.Clone()
		l := p.Limits
		r := testRequest()
		change(&p, &l, &r)
		other, err := FreezeRequestV1(p, l, r)
		if err != nil || other.Digest() == digest {
			t.Fatalf("profile/limits/wire digest mutation not sealed: %v", err)
		}
	}
}

func TestConfiguredCodecRejectsEveryUnboundedEffectiveLimit(t *testing.T) {
	p := testProfile(t)
	typ := reflect.TypeOf(p.Limits)
	for i := range typ.NumField() {
		t.Run(typ.Field(i).Name, func(t *testing.T) {
			zero := p.Limits
			reflect.ValueOf(&zero).Elem().Field(i).SetZero()
			if _, err := FreezeRequestV1(p, zero, testRequest()); err == nil {
				t.Fatal("zero effective ceiling accepted")
			}
			larger := p.Limits
			field := reflect.ValueOf(&larger).Elem().Field(i)
			if field.Kind() == reflect.Int64 {
				field.SetInt(field.Int() + 1)
			} else {
				field.SetUint(field.Uint() + 1)
			}
			if _, err := FreezeRequestV1(p, larger, testRequest()); err == nil {
				t.Fatal("effective ceiling exceeded profile")
			}
		})
	}
}

func TestConfiguredCodecRejectsRequestAndHistoryFaults(t *testing.T) {
	for name, change := range map[string]func(*domain.ConfiguredInferenceProfileV1, *RequestV1){
		"stream_protocol": func(p *domain.ConfiguredInferenceProfileV1, _ *RequestV1) {
			p.NativeProtocolVersion = "openai-chat-completions-stream.v1"
		},
		"responses": func(p *domain.ConfiguredInferenceProfileV1, _ *RequestV1) {
			p.NativeProtocolVersion = "openai-responses.v1"
		},
		"DNS": func(p *domain.ConfiguredInferenceProfileV1, _ *RequestV1) { p.BaseURL = "http://localhost:11434/v1" },
		"unsupported_tools": func(p *domain.ConfiguredInferenceProfileV1, _ *RequestV1) {
			p.Capabilities.ToolCalls = domain.ProviderEvidenceUnknownV1
		},
		"tokens_zero":    func(_ *domain.ConfiguredInferenceProfileV1, r *RequestV1) { r.MaxOutputTokens = 0 },
		"tokens_over":    func(_ *domain.ConfiguredInferenceProfileV1, r *RequestV1) { r.MaxOutputTokens = 1025 },
		"empty":          func(_ *domain.ConfiguredInferenceProfileV1, r *RequestV1) { r.Messages = nil },
		"unknown_role":   func(_ *domain.ConfiguredInferenceProfileV1, r *RequestV1) { r.Messages[0].Role = "developer" },
		"invalid_UTF8":   func(_ *domain.ConfiguredInferenceProfileV1, r *RequestV1) { r.Messages[0].Text = string([]byte{0xff}) },
		"NUL":            func(_ *domain.ConfiguredInferenceProfileV1, r *RequestV1) { r.Messages[0].Text = "bad\x00" },
		"empty_user":     func(_ *domain.ConfiguredInferenceProfileV1, r *RequestV1) { r.Messages[1].Text = "" },
		"duplicate_tool": func(_ *domain.ConfiguredInferenceProfileV1, r *RequestV1) { r.Tools = append(r.Tools, r.Tools[0]) },
		"orphan_result": func(_ *domain.ConfiguredInferenceProfileV1, r *RequestV1) {
			r.Messages = append(r.Messages, MessageV1{Role: RoleToolV1, ToolCallID: "orphan", Text: "result"})
		},
		"unfinished_call": func(_ *domain.ConfiguredInferenceProfileV1, r *RequestV1) {
			r.Messages = append(r.Messages, MessageV1{Role: RoleAssistantV1, Call: &FunctionCallV1{ID: "call", Name: "lookup", Arguments: []byte(`{"query":"x"}`)}})
		},
		"schema_drift": func(_ *domain.ConfiguredInferenceProfileV1, r *RequestV1) {
			r.Tools[0].Parameters = []byte(`{"type":"object","properties":{},"required":[],"additionalProperties":true}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := testProfile(t)
			r := testRequest()
			change(&p, &r)
			if f, err := FreezeRequestV1(p, p.Limits, r); err == nil || f != nil {
				t.Fatal("invalid request yielded frozen wire")
			}
		})
	}
	for _, name := range []string{"wrong_result", "interleaved", "duplicate_id", "text_and_call", "foreign_name", "invalid_arguments"} {
		t.Run(name, func(t *testing.T) {
			p := testProfile(t)
			r := testRequest()
			call := &FunctionCallV1{ID: "call", Name: "lookup", Arguments: []byte(`{"query":"x"}`)}
			r.Messages = append(r.Messages, MessageV1{Role: RoleAssistantV1, Call: call}, MessageV1{Role: RoleToolV1, ToolCallID: "call", Text: "result"})
			switch name {
			case "wrong_result":
				r.Messages[3].ToolCallID = "other"
			case "interleaved":
				r.Messages[3].Role = RoleUserV1
			case "duplicate_id":
				r.Messages = append(r.Messages, r.Messages[2], r.Messages[3])
			case "text_and_call":
				r.Messages[2].Text = "hidden extra"
			case "foreign_name":
				call.Name = "other"
			case "invalid_arguments":
				call.Arguments = []byte(`{"extra":1}`)
			}
			if _, err := FreezeRequestV1(p, p.Limits, r); err == nil {
				t.Fatal("invalid complete history accepted")
			}
		})
	}
}

func TestConfiguredCodecEncodedByteBoundaryBeforeEscaping(t *testing.T) {
	p := testProfile(t)
	r := testRequest()
	r.Tools = nil
	r.Messages = []MessageV1{{Role: RoleUserV1, Text: "<>&\n😀"}}
	f, err := FreezeRequestV1(p, p.Limits, r)
	if err != nil {
		t.Fatal(err)
	}
	size := uint64(len(f.Body()))
	l := p.Limits
	l.RequestBytes = size
	if _, err := FreezeRequestV1(p, l, r); err != nil {
		t.Fatalf("exact encoded bound: %v", err)
	}
	l.RequestBytes--
	if _, err := FreezeRequestV1(p, l, r); err == nil {
		t.Fatal("encoded escaping expansion exceeded body ceiling")
	}
	r.Messages[0].Text = strings.Repeat("x", int(p.Limits.RequestBytes)+1)
	if _, err := FreezeRequestV1(p, p.Limits, r); err == nil {
		t.Fatal("oversized caller text accepted")
	}
}

func TestConfiguredCodecStrictEnvelopeRejectsMalformedEverywhere(t *testing.T) {
	f := freeze(t)
	good := string(responseWire("model-local", "stop", "answer", nil))
	for name, raw := range map[string]string{
		"truncated": good[:len(good)-1], "trailing": good + ` {}`, "duplicate": strings.Replace(good, `"model":"model-local"`, `"model":"model-local","model":"model-local"`, 1),
		"decoded_duplicate": strings.Replace(good, `"model":"model-local"`, `"model":"model-local","mo\u0064el":"model-local"`, 1),
		"unknown_duplicate": strings.TrimSuffix(good, "}") + `,"metadata":{"x":1,"x":2}}`,
		"surrogate_high":    strings.Replace(good, `"answer"`, `"\ud800"`, 1), "surrogate_low": strings.Replace(good, `"answer"`, `"\udc00"`, 1),
		"metadata_surrogate": strings.TrimSuffix(good, "}") + `,"metadata":"\ud800"}`,
		"zero_exponent_bomb": strings.TrimSuffix(good, "}") + `,"metadata":0e999999999}`,
		"nonfinite":          strings.TrimSuffix(good, "}") + `,"metadata":1e999}`, "underflow": strings.TrimSuffix(good, "}") + `,"metadata":1e-999}`, "NaN": strings.TrimSuffix(good, "}") + `,"metadata":NaN}`,
		"leading_zero": strings.TrimSuffix(good, "}") + `,"metadata":01}`, "fraction_truncated": strings.TrimSuffix(good, "}") + `,"metadata":1.}`,
		"overflow_counter": strings.Replace(good, `"prompt_tokens":9`, `"prompt_tokens":18446744073709551616`, 1),
		"unknown_delta":    strings.TrimSuffix(good, "}") + `,"delta":{}}`, "unknown_fetch": strings.TrimSuffix(good, "}") + `,"metadata":{"url":"http://127.0.0.1"}}`,
		"unknown_function": strings.TrimSuffix(good, "}") + `,"function_call":{"name":"x"}}`,
		"unknown_calls":    strings.TrimSuffix(good, "}") + `,"calls":[{"name":"x","arguments":{}}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			out, err := f.DecodeResponse([]byte(raw))
			if err == nil || !reflect.DeepEqual(out, ResponseV1{}) {
				t.Fatalf("malformed %s yielded normalized data: %v", name, err)
			}
		})
	}
	bad := bytes.Replace([]byte(good), []byte(`"answer"`), []byte{'"', 0xff, '"'}, 1)
	if _, err := f.DecodeResponse(bad); err == nil {
		t.Fatal("invalid UTF8 accepted")
	}
	unicode := strings.Replace(good, `"answer"`, `"\ud83d\ude00"`, 1)
	out, err := f.DecodeResponse([]byte(unicode))
	if err != nil || out.Text != "😀" {
		t.Fatalf("valid surrogate pair denied: %v", err)
	}
}

func TestConfiguredCodecNoneAndBearerAreOnlyProfileData(t *testing.T) {
	none := testProfile(t)
	bearer := none.Clone()
	bearer.Auth = domain.ConfiguredInferenceAuthBearerV1
	bearer.Resource.CredentialMode = domain.ProviderCredentialInvocationV1
	bearer.Resource.CredentialGeneration = 7
	a, err := FreezeRequestV1(none, none.Limits, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	b, err := FreezeRequestV1(bearer, bearer.Limits, testRequest())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Body(), b.Body()) || a.Digest() == b.Digest() {
		t.Fatal("none/Bearer profile not separately sealed, or credential details entered model body")
	}
	for _, raw := range [][]byte{a.Body(), b.Body()} {
		for _, denied := range []string{"Authorization", "Bearer", "credential", "OpenRouter", "provider", "response_format", "stream_options", "image_url"} {
			if bytes.Contains(raw, []byte(denied)) {
				t.Errorf("closed wire acquired forbidden metadata %q", denied)
			}
		}
	}
}

func TestConfiguredCodecCompleteCallFailuresNeverReturnCall(t *testing.T) {
	f := freeze(t)
	for name, raw := range map[string][]byte{
		"partial_arguments": toolWire("call", "lookup", `{"query":`), "duplicate_arg": toolWire("call", "lookup", `{"query":"a","query":"b"}`),
		"foreign_tool": toolWire("call", "unknown", `{"query":"a"}`), "empty_ID": toolWire("", "lookup", `{"query":"a"}`),
		"extra_arg": toolWire("call", "lookup", `{"query":"a","extra":1}`), "missing_arg": toolWire("call", "lookup", `{}`), "wrong_type": toolWire("call", "lookup", `{"query":1}`),
		"too_long":       toolWire("call", "lookup", `{"query":"`+strings.Repeat("x", 31)+`"}`),
		"two_calls":      responseWire("model-local", "tool_calls", nil, []any{map[string]any{"id": "a"}, map[string]any{"id": "b"}}),
		"text_and_call":  responseWire("model-local", "tool_calls", "answer", []any{map[string]any{"id": "a"}}),
		"stop_with_call": responseWire("model-local", "stop", "answer", []any{map[string]any{"id": "a"}}),
		"no_call":        responseWire("model-local", "tool_calls", nil, nil),
	} {
		t.Run(name, func(t *testing.T) {
			out, err := f.DecodeResponse(raw)
			if err == nil || out.Call != nil || out.Text != "" {
				t.Fatalf("invalid call %s returned usable data: %v", name, err)
			}
		})
	}
}

func TestConfiguredCodecFinishModelUsageAndRedaction(t *testing.T) {
	f := freeze(t)
	for finish, want := range map[string]StopV1{"length": StopLengthV1, "content_filter": StopFilteredV1, "error": StopServiceErrorV1, "novel_finish": StopUnsupportedFinishV1} {
		out, err := f.DecodeResponse(responseWire("model-local", finish, "do not export", nil))
		if err != nil || out.Stop != want || out.Text != "" || out.Call != nil {
			t.Errorf("finish %s: stop=%s err=%v", finish, out.Stop, err)
		}
	}
	for _, model := range []string{"alias-local", "foreign-model", ""} {
		raw := responseWire(model, "stop", "answer", nil)
		if model == "" {
			var obj map[string]any
			_ = json.Unmarshal(raw, &obj)
			delete(obj, "model")
			raw, _ = json.Marshal(obj)
		}
		out, err := f.DecodeResponse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if model == "alias-local" {
			if out.ModelProvenance != ModelReportedV1 || out.ReportedModel != model || out.ConfiguredModel != "model-local" || out.Text != "answer" {
				t.Fatal("declared alias lost honest identity distinction")
			}
		} else if out.Stop != StopUnverifiedModelV1 || out.ModelProvenance != ModelUnverifiedV1 || out.Text != "" || out.Call != nil {
			t.Fatal("missing/foreign metadata became verified model")
		}
	}
	good := string(responseWire("model-local", "stop", "answer", nil))
	for _, usage := range []string{`null`, `{}`, `{"prompt_tokens":-1,"completion_tokens":"bad","total_tokens":1}`, `"malformed"`} {
		var obj map[string]json.RawMessage
		_ = json.Unmarshal([]byte(good), &obj)
		obj["usage"] = json.RawMessage(usage)
		raw, _ := json.Marshal(obj)
		out, err := f.DecodeResponse(raw)
		if err != nil || out.Usage.Provenance != UsageUnknownV1 || out.Usage.InputTokens != nil || out.Usage.OutputTokens != nil || out.Usage.TotalTokens != nil || out.Text != "answer" {
			t.Fatalf("malformed usage was measured/zero or broke complete response: %v", err)
		}
	}
	for _, tc := range []struct {
		raw           string
		input, output *uint64
	}{
		{`{"prompt_tokens":-1,"completion_tokens":2,"total_tokens":1}`, nil, usageCount(2)},
		{`{"prompt_tokens":1.5,"completion_tokens":2,"total_tokens":3.5}`, nil, usageCount(2)},
		{`{"prompt_tokens":1,"completion_tokens":2,"total_tokens":4}`, usageCount(1), usageCount(2)},
	} {
		var obj map[string]json.RawMessage
		_ = json.Unmarshal([]byte(good), &obj)
		obj["usage"] = json.RawMessage(tc.raw)
		raw, _ := json.Marshal(obj)
		out, err := f.DecodeResponse(raw)
		if err != nil || out.Text != "answer" || out.Usage.Provenance != UsageProviderReportedV1 || !reflect.DeepEqual(out.Usage.InputTokens, tc.input) || !reflect.DeepEqual(out.Usage.OutputTokens, tc.output) || out.Usage.TotalTokens != nil {
			t.Fatalf("malformed summary hid valid reported components: %v", err)
		}
	}
	marker := "PRIVATE_REASONING_TOKEN"
	raw := strings.Replace(good, `"content":"answer"`, `"reasoning":"`+marker+`","content":"answer"`, 1)
	out, err := f.DecodeResponse([]byte(raw))
	if err != nil || out.Text != "answer" {
		t.Fatal(err)
	}
	service, err := f.DecodeResponse([]byte(`{"error":{"message":"` + marker + `","details":{"secret":"` + marker + `"}}}`))
	if err != nil || service.Stop != StopServiceErrorV1 || service.Text != "" || service.Call != nil {
		t.Fatal("raw error was usable")
	}
	for _, value := range []any{out, service, f, testRequest(), ToolV1{Description: marker}, MessageV1{Text: marker}, FunctionCallV1{Arguments: []byte(marker)}, ErrContract} {
		if strings.Contains(fmt.Sprintf("%v %#v", value, value), marker) {
			t.Fatal("sensitive content leaked through formatting")
		}
	}
}

func TestConfiguredCodecClosedRecursiveSchema(t *testing.T) {
	const schema = `{"type":"object","properties":{"label":{"type":"string","enum":["😀","x"],"minLength":1,"maxLength":1},"ratio":{"type":"number","minimum":0.1,"maximum":0.3},"count":{"type":"integer","minimum":-2,"maximum":2},"items":{"type":"array","minItems":0,"maxItems":2,"items":{"type":"object","properties":{"ok":{"type":"boolean"},"empty":{"type":"null"}},"required":["ok","empty"],"additionalProperties":false}}},"required":["label","ratio","count","items"],"additionalProperties":false}`
	p := testProfile(t)
	r := testRequest()
	r.Tools[0].Parameters = []byte(schema)
	f, err := FreezeRequestV1(p, p.Limits, r)
	if err != nil {
		t.Fatalf("closed recursive schema: %v", err)
	}
	valid := `{"label":"😀","ratio":0.2,"count":2.0,"items":[{"ok":true,"empty":null}]}`
	if out, err := f.DecodeResponse(toolWire("call", "lookup", valid)); err != nil || out.Call == nil {
		t.Fatalf("exact recursive instance denied: %v", err)
	}
	for name, args := range map[string]string{
		"enum": strings.Replace(valid, "😀", "z", 1), "codepoint_length": strings.Replace(valid, "😀", "xx", 1),
		"numeric_upper_exact": strings.Replace(valid, `0.2`, `0.30000000000000000000000000001`, 1), "numeric_lower": strings.Replace(valid, `0.2`, `0.09`, 1),
		"fractional_integer": strings.Replace(valid, `2.0`, `1.5`, 1), "integer_upper": strings.Replace(valid, `2.0`, `3`, 1),
		"nested_missing": strings.Replace(valid, `,"empty":null`, "", 1), "nested_extra": strings.Replace(valid, `"empty":null`, `"empty":null,"extra":1`, 1),
		"boolean_type": strings.Replace(valid, `true`, `"true"`, 1), "null_type": strings.Replace(valid, `null`, `false`, 1),
		"array_upper": strings.Replace(valid, `[{"ok":true,"empty":null}]`, `[{"ok":true,"empty":null},{"ok":true,"empty":null},{"ok":true,"empty":null}]`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if out, err := f.DecodeResponse(toolWire("call", "lookup", args)); err == nil || out.Call != nil {
				t.Fatalf("schema constraint %s ignored", name)
			}
		})
	}
}

func TestConfiguredCodecRejectsUnsupportedSchemaKeywords(t *testing.T) {
	for name, schema := range map[string]string{
		"dialect":               strings.TrimSuffix(toolSchema, "}") + `,"$schema":"https://json-schema.org/draft/2020-12/schema"}`,
		"ref":                   `{"$ref":"https://example.invalid/schema"}`,
		"local_ref":             `{"type":"object","$ref":"#","properties":{},"required":[],"additionalProperties":false}`,
		"combinator":            `{"type":"object","properties":{},"required":[],"additionalProperties":false,"allOf":[]}`,
		"ignored_format":        strings.Replace(toolSchema, `"minLength":1`, `"minLength":1,"format":"email"`, 1),
		"ignored_pattern":       strings.Replace(toolSchema, `"minLength":1`, `"minLength":1,"pattern":"^x$"`, 1),
		"unbounded_extra":       strings.Replace(toolSchema, `"additionalProperties":false`, `"additionalProperties":true`, 1),
		"missing_required_list": strings.Replace(toolSchema, `,"required":["query"]`, "", 1),
		"unknown_required":      strings.Replace(toolSchema, `["query"]`, `["unknown"]`, 1),
		"duplicate_required":    strings.Replace(toolSchema, `["query"]`, `["query","query"]`, 1),
		"duplicate_property":    strings.Replace(toolSchema, `"query":{"type":"string","minLength":1,"maxLength":30}`, `"query":{"type":"string"},"query":{"type":"number"}`, 1),
		"type_union":            strings.Replace(toolSchema, `"type":"string"`, `"type":["string","null"]`, 1),
		"array_missing_limit":   strings.Replace(toolSchema, `{"type":"string","minLength":1,"maxLength":30}`, `{"type":"array","items":{"type":"boolean"},"minItems":0}`, 1),
		"numeric_reversed":      strings.Replace(toolSchema, `{"type":"string","minLength":1,"maxLength":30}`, `{"type":"number","minimum":3,"maximum":2}`, 1),
		"string_reversed":       strings.Replace(toolSchema, `"minLength":1,"maxLength":30`, `"minLength":31,"maxLength":30`, 1),
		"unknown_primitive":     strings.Replace(toolSchema, `"type":"string"`, `"type":"date"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			p := testProfile(t)
			r := testRequest()
			r.Tools[0].Parameters = []byte(schema)
			if _, err := FreezeRequestV1(p, p.Limits, r); err == nil {
				t.Fatalf("unsupported/ambiguous schema %s accepted", name)
			}
		})
	}
}

func TestConfiguredCodecBoundsCountsAndCompleteValues(t *testing.T) {
	p := testProfile(t)
	for name, change := range map[string]func(*domain.ConfiguredInferenceLimitsV1, *RequestV1){
		"messages": func(l *domain.ConfiguredInferenceLimitsV1, r *RequestV1) { l.Messages = 1 },
		"tools": func(l *domain.ConfiguredInferenceLimitsV1, r *RequestV1) {
			l.Tools = 1
			r.Tools = append(r.Tools, ToolV1{Name: "second", Parameters: []byte(toolSchema)})
		},
		"tool_name": func(l *domain.ConfiguredInferenceLimitsV1, r *RequestV1) {
			l.ToolNameBytes = 5
			r.Tools[0].Name = "lookup"
		},
		"JSON_depth":   func(l *domain.ConfiguredInferenceLimitsV1, _ *RequestV1) { l.JSONDepth = 2 },
		"JSON_members": func(l *domain.ConfiguredInferenceLimitsV1, _ *RequestV1) { l.JSONMembers = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			l := p.Limits
			r := testRequest()
			change(&l, &r)
			if _, err := FreezeRequestV1(p, l, r); err == nil {
				t.Fatal("count/depth ceiling ignored")
			}
		})
	}
	bounded := func(l domain.ConfiguredInferenceLimitsV1, r RequestV1) *FrozenRequestV1 {
		t.Helper()
		f, err := FreezeRequestV1(p, l, r)
		if err != nil {
			t.Fatalf("valid bounded codec fixture: %v", err)
		}
		return f
	}
	good := responseWire("model-local", "stop", "answer", nil)
	l := p.Limits
	l.FinalTextBytes = 5
	f := bounded(l, testRequest())
	if out, err := f.DecodeResponse(good); err == nil || out.Text != "" {
		t.Fatal("oversized complete final text truncated/returned")
	}
	l.FinalTextBytes = 6
	f = bounded(l, testRequest())
	if out, err := f.DecodeResponse(good); err != nil || out.Text != "answer" {
		t.Fatalf("exact text byte boundary denied: %v", err)
	}
	l.ToolCallIDBytes = 4
	f = bounded(l, testRequest())
	if _, err := f.DecodeResponse(toolWire("12345", "lookup", `{"query":"x"}`)); err == nil {
		t.Fatal("call ID bound ignored")
	}
	l.ToolCallIDBytes = 128
	args := []byte(`{"query":"xy"}`)
	l.ToolArgumentsBytes = uint64(len(args))
	r := testRequest()
	r.Tools[0].Parameters = []byte(strings.Replace(toolSchema, `"maxLength":30`, `"maxLength":2`, 1))
	f = bounded(l, r)
	if out, err := f.DecodeResponse(toolWire("call", "lookup", `{"query":"xy"}`)); err != nil || out.Call == nil {
		t.Fatalf("exact complete argument byte boundary denied: %v", err)
	}
	l.ToolArgumentsBytes--
	f = bounded(l, r)
	if out, err := f.DecodeResponse(toolWire("call", "lookup", `{"query":"xy"}`)); err == nil || out.Call != nil {
		t.Fatal("oversized argument truncated/returned")
	}
	l = p.Limits
	l.ResponseBytes, l.FinalTextBytes, l.ToolArgumentsBytes = uint64(len(good)), 6, 64
	f = bounded(l, testRequest())
	if _, err := f.DecodeResponse(good); err != nil {
		t.Fatalf("exact response byte ceiling denied: %v", err)
	}
	l.ResponseBytes--
	f = bounded(l, testRequest())
	if _, err := f.DecodeResponse(good); err == nil {
		t.Fatal("response byte ceiling ignored")
	}
	for _, test := range []struct {
		raw            string
		depth, members uint64
		ok             bool
	}{
		{`{"a":[{"x":true}]}`, 4, 2, true}, {`{"a":[{"x":true}]}`, 3, 2, false},
		{`{"a":1,"b":2}`, 2, 2, true}, {`{"a":1,"b":2,"c":3}`, 2, 2, false},
		{`[1,2]`, 2, 2, true}, {`[1,2,3]`, 2, 2, false},
	} {
		_, err := strictJSON([]byte(test.raw), 1024, test.depth, test.members)
		if (err == nil) != test.ok {
			t.Errorf("strict shape depth=%d members=%d raw=%s: %v", test.depth, test.members, test.raw, err)
		}
	}
}

func TestConfiguredCodecDigestSealsEachEffectiveCeiling(t *testing.T) {
	p := testProfile(t)
	base := freeze(t)
	typ := reflect.TypeOf(p.Limits)
	for i := range typ.NumField() {
		t.Run(typ.Field(i).Name, func(t *testing.T) {
			l := p.Limits
			field := reflect.ValueOf(&l).Elem().Field(i)
			if field.Kind() == reflect.Int64 {
				field.SetInt(field.Int() - 1)
			} else if field.Uint() > 1 {
				field.SetUint(field.Uint() - 1)
			} else {
				return
			}
			f, err := FreezeRequestV1(p, l, testRequest())
			if err != nil || f.Digest() == base.Digest() {
				t.Fatalf("effective %s not independently sealed: %v", typ.Field(i).Name, err)
			}
		})
	}
}

func FuzzConfiguredCodecResponse(f *testing.F) {
	for _, seed := range [][]byte{responseWire("model-local", "stop", "answer", nil), toolWire("call", "lookup", `{"query":"x"}`), []byte(`{"choices":[]}`), []byte(`{"x":"\ud800"}`), []byte(`{"x":1,"\u0078":2}`), []byte(`{"x":1e999}`)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			t.Skip()
		}
		codec := freeze(t)
		out, err := codec.DecodeResponse(raw)
		if err != nil {
			if !reflect.DeepEqual(out, ResponseV1{}) {
				t.Fatal("failed decode returned normalized data")
			}
			return
		}
		if out.Stop != StopFinalV1 && out.Stop != StopToolV1 && (out.Text != "" || out.Call != nil) {
			t.Fatal("non-success exported usable content")
		}
		if out.Stop == StopToolV1 && (out.Call == nil || out.Text != "") {
			t.Fatal("tool response not exclusive complete call")
		}
	})
}
