package mcpwire

import "bytes"

// These closed setup messages are DATA, not a connection, session, catalog
// qualification or permission to disclose a query. The transport owns counters
// and checks the original canonical authority before each physical message.
func NewInitializeRequestV1(id string, limits LimitsV1) (RequestV1, error) {
	return setupRequestV1(RequestInitializeV1, id, map[string]any{
		"protocolVersion": ProtocolVersionV1,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "sessionless", "version": "1"},
	}, limits)
}

func NewListToolsRequestV1(id string, limits LimitsV1) (RequestV1, error) {
	return setupRequestV1(RequestListToolsV1, id, map[string]any{}, limits)
}

func setupRequestV1(kind RequestKindV1, id string, params any, limits LimitsV1) (RequestV1, error) {
	if limits.Validate() != nil || !validString(id, 256, false) {
		return RequestV1{}, ErrProtocol
	}
	body, err := canonical(map[string]any{"jsonrpc": "2.0", "id": id, "method": string(kind), "params": params}, limits.MessageBytes)
	if err != nil {
		return RequestV1{}, ErrProtocol
	}
	return RequestV1{kind: kind, id: id, limits: limits, body: body}, nil
}

func InitializedNotificationV1(limits LimitsV1) ([]byte, error) {
	if limits.Validate() != nil {
		return nil, ErrProtocol
	}
	return canonical(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"}, limits.MessageBytes)
}

func (r *RequestV1) setupResultV1(raw []byte) (map[string]any, error) {
	if r == nil || r.limits.Validate() != nil {
		return nil, ErrProtocol
	}
	v, err := parse(raw, r.limits.MessageBytes, r.limits)
	m, ok := object(v)
	if err != nil || !ok || !fields(m, "jsonrpc", "id", "result") || m["jsonrpc"] != "2.0" || m["id"] != r.id || len(m) != 3 {
		return nil, ErrProtocol
	}
	result, ok := object(m["result"])
	if !ok {
		return nil, ErrProtocol
	}
	return result, nil
}

// ValidateInitializationV1 admits exactly the frozen protocol and a tools-only
// server. Unsupported server capabilities/instructions cannot become tools or
// trusted model instructions. No nearest-version negotiation occurs here.
func (r *RequestV1) ValidateInitializationV1(raw []byte) error {
	if r == nil || r.kind != RequestInitializeV1 {
		return ErrProtocol
	}
	v, err := r.setupResultV1(raw)
	if err != nil || !fields(v, "protocolVersion", "capabilities", "serverInfo") || len(v) != 3 || v["protocolVersion"] != ProtocolVersionV1 {
		return ErrProtocol
	}
	caps, ok := object(v["capabilities"])
	if !ok || !fields(caps, "tools") || len(caps) != 1 {
		return ErrProtocol
	}
	tools, ok := object(caps["tools"])
	if !ok || !fields(tools, "listChanged") {
		return ErrProtocol
	}
	if changed, present := tools["listChanged"]; present && changed != false {
		return ErrProtocol
	}
	info, ok := object(v["serverInfo"])
	if !ok || !fields(info, "name", "version") || len(info) != 2 {
		return ErrProtocol
	}
	name, n := text(info["name"])
	version, ver := text(info["version"])
	if !n || !ver || !validString(name, 256, false) || !validString(version, 256, false) {
		return ErrProtocol
	}
	return nil
}

// ValidateCatalogV1 accepts one exact captured descriptor and no pagination.
// A changed, larger or otherwise useful-looking catalog invalidates this arm.
func (r *RequestV1) ValidateCatalogV1(raw []byte, installed DescriptorV1) error {
	if r == nil || r.kind != RequestListToolsV1 {
		return ErrProtocol
	}
	v, err := r.setupResultV1(raw)
	if err != nil || !fields(v, "tools") || len(v) != 1 {
		return ErrProtocol
	}
	tools, ok := v["tools"].([]any)
	if !ok || len(tools) != 1 {
		return ErrProtocol
	}
	body, err := canonical(tools[0], r.limits.DescriptorBytes)
	if err != nil {
		return ErrProtocol
	}
	defer clear(body)
	actual, err := ParseDescriptorV1(body, r.limits)
	if err != nil || actual.Digest() != installed.Digest() || !bytes.Equal(actual.Body(), installed.Body()) {
		return ErrProtocol
	}
	return nil
}
