package mcpwire

import (
	"bytes"
	"encoding/json"
)

// NewToolCallRequestV1 freezes one tools/call DATA message. Schema acceptance,
// installed grant, current authority and actual dispatch belong to its owners.
func NewToolCallRequestV1(callID string, d DescriptorV1, arguments []byte, limits LimitsV1) (RequestV1, error) {
	if limits.Validate() != nil || !validString(callID, 128, false) || len(d.body) == 0 {
		return RequestV1{}, ErrProtocol
	}
	checked, err := ParseDescriptorV1(d.body, limits)
	if err != nil || checked.digest != d.digest {
		return RequestV1{}, ErrProtocol
	}
	value, err := parse(arguments, limits.ArgumentBytes, limits)
	if _, ok := value.(map[string]any); err != nil || !ok {
		return RequestV1{}, ErrProtocol
	}
	canonicalArgs, err := canonical(value, limits.ArgumentBytes)
	if err != nil || !bytes.Equal(canonicalArgs, arguments) {
		return RequestV1{}, ErrProtocol
	}
	body, err := canonical(map[string]any{"jsonrpc": "2.0", "id": callID, "method": "tools/call", "params": map[string]any{"name": checked.name, "arguments": value}}, limits.MessageBytes)
	if err != nil {
		return RequestV1{}, ErrProtocol
	}
	return RequestV1{kind: RequestCallToolV1, id: callID, limits: limits, body: body, descriptor: &checked}, nil
}

// DecodeToolResultV1 accepts only a known, complete, text-only JSON-RPC result
// for THIS frozen call. Protocol errors, unsupported content and unknown fields
// cannot become a successful/domain-error tool result or continuation receipt.
func (r RequestV1) DecodeToolResultV1(body []byte) (ToolResultV1, error) {
	if r.kind != RequestCallToolV1 || r.descriptor == nil || r.limits.Validate() != nil || len(r.body) == 0 {
		return ToolResultV1{}, ErrProtocol
	}
	v, err := parse(body, r.limits.MessageBytes, r.limits)
	root, ok := object(v)
	if err != nil || !ok || len(root) != 3 || !fields(root, "jsonrpc", "id", "result") || root["jsonrpc"] != "2.0" || root["id"] != r.id {
		return ToolResultV1{}, ErrProtocol
	}
	result, ok := object(root["result"])
	if !ok || !fields(result, "content", "isError") {
		return ToolResultV1{}, ErrProtocol
	}
	content, ok := result["content"].([]any)
	if !ok || uint64(len(content)) > r.limits.ContentItems {
		return ToolResultV1{}, ErrProtocol
	}
	isError := false
	if raw, present := result["isError"]; present {
		var ok bool
		isError, ok = raw.(bool)
		if !ok {
			return ToolResultV1{}, ErrProtocol
		}
	}
	out := ToolResultV1{IsError: isError, Content: make([]TextContentV1, 0, len(content))}
	var total uint64
	for _, raw := range content {
		item, ok := object(raw)
		if !ok || len(item) != 2 || !fields(item, "type", "text") || item["type"] != "text" {
			return ToolResultV1{}, ErrProtocol
		}
		text, ok := item["text"].(string)
		if !ok || !validString(text, r.limits.ResultTextBytes, true) || uint64(len(text)) > r.limits.ResultTextBytes-total {
			return ToolResultV1{}, ErrProtocol
		}
		total += uint64(len(text))
		encoded, err := canonical(item, r.limits.MessageBytes)
		if err != nil {
			return ToolResultV1{}, ErrProtocol
		}
		out.Content = append(out.Content, TextContentV1{Text: text, body: encoded})
	}
	result["isError"] = isError
	out.body, err = canonical(result, r.limits.MessageBytes)
	if err != nil {
		return ToolResultV1{}, ErrProtocol
	}
	return out, nil
}

// ValidateToolResultV1 rechecks that exported content fields still match the
// immutable parsed result. A caller-built struct or edited IsError is not a
// parsed result, and even valid DATA does not prove a remote peer replied.
func (r RequestV1) ValidateToolResultV1(result ToolResultV1) (ToolResultV1, error) {
	if len(result.body) == 0 {
		return ToolResultV1{}, ErrProtocol
	}
	id, _ := json.Marshal(r.id)
	wrapped := append([]byte(`{"jsonrpc":"2.0","id":`), id...)
	wrapped = append(wrapped, `,"result":`...)
	wrapped = append(wrapped, result.body...)
	wrapped = append(wrapped, '}')
	checked, err := r.DecodeToolResultV1(wrapped)
	if err != nil || checked.IsError != result.IsError || len(checked.Content) != len(result.Content) {
		return ToolResultV1{}, ErrProtocol
	}
	for i := range checked.Content {
		if checked.Content[i].Text != result.Content[i].Text {
			return ToolResultV1{}, ErrProtocol
		}
	}
	return checked, nil
}
