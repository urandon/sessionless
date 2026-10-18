package configuredinference

import (
	"encoding/json"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func validString(s string, limit uint64, empty bool) bool {
	return (empty || len(s) > 0) && uint64(len(s)) <= limit && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

type bodyBuilder struct {
	b      []byte
	limit  uint64
	failed bool
}

func (b *bodyBuilder) add(s string) {
	if b.failed {
		return
	}
	if uint64(len(s)) > b.limit-uint64(len(b.b)) {
		b.failed = true
		return
	}
	b.b = append(b.b, s...)
}
func (b *bodyBuilder) raw(s []byte) {
	if b.failed {
		return
	}
	if uint64(len(s)) > b.limit-uint64(len(b.b)) {
		b.failed = true
		return
	}
	b.b = append(b.b, s...)
}
func (b *bodyBuilder) text(s string) {
	if b.failed {
		return
	}
	var n uint64 = 2
	for _, r := range s {
		switch r {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			n += 2
		case '<', '>', '&', '\u2028', '\u2029':
			n += 6
		default:
			if r < 0x20 {
				n += 6
			} else {
				n += uint64(utf8.RuneLen(r))
			}
		}
		if n > b.limit-uint64(len(b.b)) {
			b.failed = true
			return
		}
	}
	if n > b.limit-uint64(len(b.b)) {
		b.failed = true
		return
	}
	raw, err := json.Marshal(s)
	if err != nil {
		b.failed = true
		return
	}
	b.raw(raw)
}

// FreezeRequestV1 validates DATA and copies it into a closed wire request. It
// does not establish token fit or current admission. No headers, credentials,
// network or extra parameters exist here. Stream/Responses/media/formats have
// no alias: a profile selecting another native protocol fails validation.
func FreezeRequestV1(profile domain.ConfiguredInferenceProfileV1, effective domain.ConfiguredInferenceLimitsV1, request RequestV1) (*FrozenRequestV1, error) {
	if profile.Validate() != nil || !limitsWithin(effective, profile.Limits) || len(request.Messages) == 0 || uint64(len(request.Messages)) > effective.Messages || uint64(len(request.Tools)) > effective.Tools || request.MaxOutputTokens == 0 || request.MaxOutputTokens > effective.MaxOutputTokens {
		return nil, ErrContract
	}
	if len(request.Tools) > 0 && profile.Capabilities.ToolCalls != domain.ProviderEvidenceSupportedV1 {
		return nil, ErrContract
	}
	// This lower bound on the eventual wire must fit BEFORE schema decoding or
	// allocating copied catalog/history data. Escaping and structural overhead
	// are checked exactly by bodyBuilder before each bounded string allocation.
	total := uint64(len(profile.ModelID))
	add := func(n int) bool {
		if uint64(n) > effective.RequestBytes-total {
			return false
		}
		total += uint64(n)
		return true
	}
	if total > effective.RequestBytes {
		return nil, ErrContract
	}
	for _, tool := range request.Tools {
		if !add(len(tool.Name)) || !add(len(tool.Description)) || !add(len(tool.Parameters)) {
			return nil, ErrContract
		}
	}
	for _, message := range request.Messages {
		if !add(len(message.Text)) || !add(len(message.ToolCallID)) {
			return nil, ErrContract
		}
		if message.Call != nil && (!add(len(message.Call.ID)) || !add(len(message.Call.Name)) || !add(len(message.Call.Arguments))) {
			return nil, ErrContract
		}
	}
	f := &FrozenRequestV1{profile: profile.Clone(), limits: effective, tools: make(map[string]*schemaV1), priorIDs: make(map[string]struct{})}
	for _, tool := range request.Tools {
		if !validString(tool.Name, effective.ToolNameBytes, false) || !validString(tool.Description, effective.RequestBytes, true) || f.tools[tool.Name] != nil {
			return nil, ErrContract
		}
		v, err := strictJSON(tool.Parameters, effective.RequestBytes, effective.JSONDepth, effective.JSONMembers)
		if err != nil {
			return nil, ErrContract
		}
		schema, err := compileSchema(v, 1, effective)
		if err != nil || schema.kind != "object" {
			return nil, ErrContract
		}
		f.tools[tool.Name] = schema
	}
	var pending *FunctionCallV1
	for _, m := range request.Messages {
		if !validString(m.Text, effective.RequestBytes, true) {
			return nil, ErrContract
		}
		if pending != nil && m.Role != RoleToolV1 {
			return nil, ErrContract
		}
		switch m.Role {
		case RoleSystemV1, RoleUserV1:
			if m.Text == "" || m.Call != nil || m.ToolCallID != "" {
				return nil, ErrContract
			}
		case RoleAssistantV1:
			if m.ToolCallID != "" || (m.Call == nil && m.Text == "") {
				return nil, ErrContract
			}
			if m.Call != nil {
				if m.Text != "" || !f.validCall(*m.Call) {
					return nil, ErrContract
				}
				if _, exists := f.priorIDs[m.Call.ID]; exists {
					return nil, ErrContract
				}
				f.priorIDs[m.Call.ID] = struct{}{}
				pending = m.Call
			}
		case RoleToolV1:
			if pending == nil || m.Call != nil || m.ToolCallID != pending.ID {
				return nil, ErrContract
			}
			pending = nil
		default:
			return nil, ErrContract
		}
	}
	if pending != nil {
		return nil, ErrContract
	}
	b := bodyBuilder{limit: effective.RequestBytes}
	b.add(`{"model":`)
	b.text(profile.ModelID)
	b.add(`,"stream":false,"n":1,"max_tokens":`)
	number, _ := json.Marshal(request.MaxOutputTokens)
	b.raw(number)
	b.add(`,"messages":[`)
	for i, m := range request.Messages {
		if i > 0 {
			b.add(",")
		}
		b.add(`{"role":`)
		b.text(string(m.Role))
		b.add(`,"content":`)
		if m.Call != nil {
			b.add(`null,"tool_calls":[{"id":`)
			b.text(m.Call.ID)
			b.add(`,"type":"function","function":{"name":`)
			b.text(m.Call.Name)
			b.add(`,"arguments":`)
			b.text(string(m.Call.Arguments))
			b.add(`}}]`)
		} else {
			b.text(m.Text)
		}
		if m.Role == RoleToolV1 {
			b.add(`,"tool_call_id":`)
			b.text(m.ToolCallID)
		}
		b.add("}")
	}
	b.add("]")
	if len(request.Tools) > 0 {
		b.add(`,"tools":[`)
		for i, t := range request.Tools {
			if i > 0 {
				b.add(",")
			}
			b.add(`{"type":"function","function":{"name":`)
			b.text(t.Name)
			b.add(`,"description":`)
			b.text(t.Description)
			b.add(`,"parameters":`)
			b.raw(t.Parameters)
			b.add("}}")
		}
		b.add("]")
	}
	b.add("}")
	if b.failed {
		clear(b.b)
		return nil, ErrContract
	}
	if _, err := strictJSON(b.b, effective.RequestBytes, effective.JSONDepth, effective.JSONMembers); err != nil {
		clear(b.b)
		return nil, ErrContract
	}
	f.body = b.b
	var err error
	f.digest, err = requestDigest(f.profile, effective, f.body)
	if err != nil {
		clear(f.body)
		return nil, ErrContract
	}
	return f, nil
}

func (f *FrozenRequestV1) validCall(call FunctionCallV1) bool {
	if !validString(call.ID, f.limits.ToolCallIDBytes, false) || !validString(call.Name, f.limits.ToolNameBytes, false) {
		return false
	}
	s := f.tools[call.Name]
	if s == nil {
		return false
	}
	v, err := strictJSON(call.Arguments, f.limits.ToolArgumentsBytes, f.limits.JSONDepth, f.limits.JSONMembers)
	return err == nil && s.accepts(v)
}

// Unknown metadata is allowed only after the complete lexical/body bounds. No
// unknown effect-shaped branch may smuggle calls, streamed deltas or fetch URLs.
func safeMetadata(v any) bool {
	switch v := v.(type) {
	case map[string]any:
		for k, child := range v {
			key := strings.ToLower(k)
			if strings.Contains(key, "tool") || strings.Contains(key, "function") || strings.Contains(key, "call") || strings.Contains(key, "url") || key == "uri" || key == "delta" || key == "audio" || key == "image" || key == "video" || key == "error" || key == "refusal" {
				return false
			}
			if !safeMetadata(child) {
				return false
			}
		}
	case []any:
		for _, child := range v {
			if !safeMetadata(child) {
				return false
			}
		}
	}
	return true
}
func unknownSafe(m map[string]any, known ...string) bool {
	for k, v := range m {
		found := false
		for _, key := range known {
			if k == key {
				found = true
				break
			}
		}
		if !found {
			if !safeMetadata(map[string]any{k: v}) {
				return false
			}
		}
	}
	return true
}

func (f *FrozenRequestV1) DecodeResponse(body []byte) (ResponseV1, error) {
	if f == nil || f.digest == "" {
		return ResponseV1{}, ErrContract
	}
	v, err := strictJSON(body, f.limits.ResponseBytes, f.limits.JSONDepth, f.limits.JSONMembers)
	if err != nil {
		return ResponseV1{}, ErrContract
	}
	root, ok := object(v)
	if !ok || !unknownSafe(root, "id", "object", "created", "model", "choices", "usage", "error") {
		return ResponseV1{}, ErrContract
	}
	out := ResponseV1{ConfiguredModel: f.profile.ModelID, ModelProvenance: ModelUnverifiedV1, Usage: UsageV1{Provenance: UsageUnknownV1}}
	if id, exists := root["id"]; exists {
		value, ok := id.(string)
		if !ok || !validString(value, 256, false) {
			return ResponseV1{}, ErrContract
		}
	}
	if created, exists := root["created"]; exists {
		if _, ok := uintValue(created); !ok {
			return ResponseV1{}, ErrContract
		}
	}
	if !safeMetadata(root["usage"]) || !safeMetadata(root["logprobs"]) {
		return ResponseV1{}, ErrContract
	}
	if raw, exists := root["model"]; exists {
		model, ok := raw.(string)
		if !ok || !validString(model, 256, false) {
			return ResponseV1{}, ErrContract
		}
		out.ReportedModel = model
		for _, allowed := range f.profile.ResponseModelIDs {
			if model == allowed {
				out.ModelProvenance = ModelReportedV1
				break
			}
		}
	}
	if raw, exists := root["error"]; exists && raw != nil {
		out.Stop = StopServiceErrorV1
		return out, nil
	}
	if objectName, exists := root["object"]; exists && objectName != "chat.completion" {
		return ResponseV1{}, ErrContract
	}
	choices, ok := root["choices"].([]any)
	if !ok || len(choices) != 1 {
		return ResponseV1{}, ErrContract
	}
	choice, ok := object(choices[0])
	if !ok || !unknownSafe(choice, "index", "message", "finish_reason", "logprobs") {
		return ResponseV1{}, ErrContract
	}
	index, ok := uintValue(choice["index"])
	if !ok || index != 0 {
		return ResponseV1{}, ErrContract
	}
	message, ok := object(choice["message"])
	if !ok || message["role"] != "assistant" || !unknownSafe(message, "role", "content", "tool_calls", "refusal", "reasoning", "reasoning_content", "reasoning_details") {
		return ResponseV1{}, ErrContract
	}
	if !safeMetadata(choice["logprobs"]) {
		return ResponseV1{}, ErrContract
	}
	finish, ok := choice["finish_reason"].(string)
	if !ok || !validString(finish, 128, false) {
		return ResponseV1{}, ErrContract
	}
	out.Usage = parseUsage(root["usage"])
	if out.ModelProvenance != ModelReportedV1 {
		out.Stop = StopUnverifiedModelV1
		return out, nil
	}
	if refusal, exists := message["refusal"]; exists && refusal != nil {
		out.Stop = StopRefusalV1
		return out, nil
	}
	switch finish {
	case "length":
		out.Stop = StopLengthV1
		return out, nil
	case "content_filter":
		out.Stop = StopFilteredV1
		return out, nil
	case "error":
		out.Stop = StopServiceErrorV1
		return out, nil
	case "stop", "tool_calls":
	default:
		out.Stop = StopUnsupportedFinishV1
		return out, nil
	}
	text := ""
	if content, exists := message["content"]; exists && content != nil {
		var ok bool
		text, ok = content.(string)
		if !ok || !validString(text, f.limits.FinalTextBytes, true) {
			return ResponseV1{}, ErrContract
		}
	}
	rawCalls, hasCalls := message["tool_calls"]
	calls, ok := rawCalls.([]any)
	if finish == "stop" {
		if hasCalls && (rawCalls != nil && (!ok || len(calls) > 0)) || text == "" {
			return ResponseV1{}, ErrContract
		}
		out.Stop, out.Text = StopFinalV1, text
		return out, nil
	}
	if text != "" || !hasCalls || !ok || len(calls) != 1 || len(f.tools) == 0 {
		return ResponseV1{}, ErrContract
	}
	call, ok := object(calls[0])
	if !ok || !exactKeys(call, "id", "type", "function") || call["type"] != "function" {
		return ResponseV1{}, ErrContract
	}
	function, ok := object(call["function"])
	if !ok || !exactKeys(function, "name", "arguments") {
		return ResponseV1{}, ErrContract
	}
	id, idOK := call["id"].(string)
	name, nameOK := function["name"].(string)
	args, argsOK := function["arguments"].(string)
	if !idOK || !nameOK || !argsOK || uint64(len(args)) > f.limits.ToolArgumentsBytes {
		return ResponseV1{}, ErrContract
	}
	result := FunctionCallV1{ID: id, Name: name, Arguments: []byte(args)}
	if _, repeated := f.priorIDs[id]; repeated || !f.validCall(result) {
		clear(result.Arguments)
		return ResponseV1{}, ErrContract
	}
	out.Stop, out.Call = StopToolV1, &result
	return out, nil
}

func parseUsage(v any) UsageV1 {
	unknown := UsageV1{Provenance: UsageUnknownV1}
	m, ok := object(v)
	if !ok || !safeMetadata(m) {
		return unknown
	}
	a, aOK := uintValue(m["prompt_tokens"])
	b, bOK := uintValue(m["completion_tokens"])
	total, totalOK := uintValue(m["total_tokens"])
	if !aOK && !bOK {
		return unknown
	}
	// Retain each independently valid provider report. Missing/invalid totals
	// or another unknown metric must not erase a known token overrun. The
	// fields remain unverified reports; an absent metric is never inferred zero.
	usage := UsageV1{Provenance: UsageProviderReportedV1}
	if aOK {
		usage.InputTokens = &a
	}
	if bOK {
		usage.OutputTokens = &b
	}
	if aOK && bOK && totalOK && a <= ^uint64(0)-b && a+b == total {
		usage.TotalTokens = &total
	}
	return usage
}
