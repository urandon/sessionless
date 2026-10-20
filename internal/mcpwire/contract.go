// Package mcpwire implements bounded MCP 2025-11-25 protocol DATA codecs only.
// It owns no network, session lifecycle, authorization, schema validator, grant,
// retry, billing or effect receipt. A decoded catalog is untrusted DATA, not an
// enabled tool. The actual transport must bound reads BEFORE allocating a body;
// finite-body parsing here does not establish streaming/socket resource bounds.
// Protocol references:
// https://modelcontextprotocol.io/specification/2025-11-25/basic/transports
// https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle
// https://modelcontextprotocol.io/specification/2025-11-25/server/tools
package mcpwire

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const ProtocolVersionV1 = "2025-11-25"

var ErrProtocol = errors.New("MCP protocol data is invalid or unsupported")

// Every ceiling is explicit and positive. These are per-message/parser bounds;
// the owning invocation must separately count aggregate protocol work across
// requests, including initialization, cancellation and teardown.
type LimitsV1 struct {
	MessageBytes    uint64
	AggregateBytes  uint64
	JSONDepth       uint64
	JSONMembers     uint64
	SSELineBytes    uint64
	SSEEventBytes   uint64
	SSEEvents       uint64
	Tools           uint64
	DescriptorBytes uint64
	ArgumentBytes   uint64
	ResultTextBytes uint64
	ContentItems    uint64
}

func (v LimitsV1) Validate() error {
	for _, pair := range [][2]uint64{
		{v.MessageBytes, 256 << 10}, {v.AggregateBytes, 256 << 10}, {v.JSONDepth, 64}, {v.JSONMembers, 4096},
		{v.SSELineBytes, 256 << 10}, {v.SSEEventBytes, 256 << 10}, {v.SSEEvents, 128}, {v.Tools, 64},
		{v.DescriptorBytes, 64 << 10}, {v.ArgumentBytes, 4 << 10}, {v.ResultTextBytes, 64 << 10}, {v.ContentItems, 128},
	} {
		if pair[0] == 0 || pair[0] > pair[1] {
			return ErrProtocol
		}
	}
	if v.MessageBytes > v.AggregateBytes || v.SSEEventBytes > v.MessageBytes || v.SSELineBytes > v.AggregateBytes || v.DescriptorBytes > v.MessageBytes {
		return ErrProtocol
	}
	return nil
}

type RequestKindV1 string

const (
	RequestInitializeV1 RequestKindV1 = "initialize"
	RequestListToolsV1  RequestKindV1 = "tools/list"
	RequestCallToolV1   RequestKindV1 = "tools/call"
)

// A request freezes exact correlation DATA, not permission or proof it was sent.
type RequestV1 struct {
	kind       RequestKindV1
	id         string
	limits     LimitsV1
	body       []byte
	descriptor *DescriptorV1
}

func (r *RequestV1) Body() []byte {
	if r == nil {
		return nil
	}
	return bytes.Clone(r.body)
}
func (r *RequestV1) ID() string {
	if r == nil {
		return ""
	}
	return r.id
}
func (r *RequestV1) Kind() RequestKindV1 {
	if r == nil {
		return ""
	}
	return r.kind
}

// DescriptorDigest returns correlation DATA only. It does not approve a grant,
// validate arguments against a schema or establish that a request was sent.
func (r *RequestV1) DescriptorDigest() string {
	if r == nil || r.descriptor == nil {
		return ""
	}
	return r.descriptor.digest
}

// Limits returns immutable parser DATA, not an admission decision.
func (r *RequestV1) Limits() LimitsV1 {
	if r == nil {
		return LimitsV1{}
	}
	return r.limits
}
func (*RequestV1) String() string     { return "MCPRequestV1{content:[redacted]}" }
func (r *RequestV1) GoString() string { return r.String() }

// DescriptorV1 preserves ALL accepted descriptor fields, including complete
// schema/prose/annotation metadata, in its canonical identity. No JSON Schema
// dialect or keyword is approved here. The closed text-only profile rejects
// outputSchema until an owning output-schema validator is implemented.
type DescriptorV1 struct {
	name         string
	body, input  []byte
	digest       string
	taskRequired bool
}

func (d DescriptorV1) Name() string        { return d.name }
func (d DescriptorV1) Body() []byte        { return bytes.Clone(d.body) }
func (d DescriptorV1) InputSchema() []byte { return bytes.Clone(d.input) }
func (d DescriptorV1) Digest() string      { return d.digest }
func (DescriptorV1) String() string        { return "MCPDescriptorV1{content:[redacted]}" }
func (d DescriptorV1) GoString() string    { return d.String() }

type InitializeResultV1 struct{ body []byte }

func (v InitializeResultV1) Body() []byte     { return bytes.Clone(v.body) }
func (InitializeResultV1) String() string     { return "MCPInitializeResultV1{content:[redacted]}" }
func (v InitializeResultV1) GoString() string { return v.String() }

type CatalogV1 struct {
	Tools []DescriptorV1
	body  []byte
}

func (v CatalogV1) Body() []byte     { return bytes.Clone(v.body) }
func (CatalogV1) String() string     { return "MCPCatalogV1{content:[redacted]}" }
func (v CatalogV1) GoString() string { return v.String() }

type TextContentV1 struct {
	Text string `json:"-"`
	body []byte
}

func (v TextContentV1) Body() []byte     { return bytes.Clone(v.body) }
func (TextContentV1) String() string     { return "MCPTextContentV1{content:[redacted]}" }
func (v TextContentV1) GoString() string { return v.String() }

type ToolResultV1 struct {
	IsError bool
	Content []TextContentV1
	body    []byte
}

func (v ToolResultV1) Body() []byte     { return bytes.Clone(v.body) }
func (ToolResultV1) String() string     { return "MCPToolResultV1{content:[redacted]}" }
func (v ToolResultV1) GoString() string { return v.String() }

// RPCError is distinct from result.isError. Its remote message/data are retained
// as bounded private DATA only, never included in the generic local error.
type RPCErrorV1 struct {
	Code int64
	body []byte
}

func (v RPCErrorV1) Body() []byte     { return bytes.Clone(v.body) }
func (RPCErrorV1) String() string     { return "MCPRPCErrorV1{content:[redacted]}" }
func (v RPCErrorV1) GoString() string { return v.String() }

type ResponseV1 struct {
	InboundFrames uint64
	Kind          RequestKindV1
	ID            string
	Initialize    *InitializeResultV1
	Catalog       *CatalogV1
	Tool          *ToolResultV1
	RPCError      *RPCErrorV1
}

func (ResponseV1) String() string     { return "MCPResponseV1{content:[redacted]}" }
func (v ResponseV1) GoString() string { return v.String() }

func validString(s string, cap uint64, empty bool) bool {
	return (empty || s != "") && uint64(len(s)) <= cap && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func object(v any) (map[string]any, bool) { m, ok := v.(map[string]any); return m, ok }
func text(v any) (string, bool)           { s, ok := v.(string); return s, ok }
func fields(v map[string]any, names ...string) bool {
	for key := range v {
		found := false
		for _, n := range names {
			found = found || key == n
		}
		if !found {
			return false
		}
	}
	return true
}
func parse(raw []byte, cap uint64, l LimitsV1) (any, error) {
	v, err := strictjson.Parse(raw, cap, l.JSONDepth, l.JSONMembers)
	if err != nil {
		return nil, ErrProtocol
	}
	return v, nil
}
func canonical(v any, cap uint64) ([]byte, error) {
	remaining := cap
	if !canonicalSizeV1(v, &remaining, 1) {
		return nil, ErrProtocol
	}
	raw, err := json.Marshal(v)
	if err != nil || uint64(len(raw)) > cap {
		clear(raw)
		return nil, ErrProtocol
	}
	return raw, nil
}

// Check the complete escaped encoding BEFORE allocating a canonical copy.
// Inputs are only bounded strictjson trees or our closed outgoing message maps.
func canonicalSizeV1(v any, remaining *uint64, depth uint64) bool {
	if depth > 64 {
		return false
	}
	add := func(n uint64) bool {
		if n > *remaining {
			return false
		}
		*remaining -= n
		return true
	}
	switch value := v.(type) {
	case nil:
		return add(4)
	case bool:
		if value {
			return add(4)
		}
		return add(5)
	case json.Number:
		return add(uint64(len(value)))
	case string:
		if !utf8.ValidString(value) || !add(2) {
			return false
		}
		for _, r := range value {
			var n uint64
			switch r {
			case '"', '\\', '\b', '\f', '\n', '\r', '\t':
				n = 2
			case '<', '>', '&', '\u2028', '\u2029':
				n = 6
			default:
				if r < 0x20 {
					n = 6
				} else {
					n = uint64(utf8.RuneLen(r))
				}
			}
			if !add(n) {
				return false
			}
		}
		return true
	case map[string]any:
		if !add(2) {
			return false
		}
		i := 0
		for key, item := range value {
			if i > 0 && !add(1) {
				return false
			}
			i++
			if !canonicalSizeV1(key, remaining, depth+1) || !add(1) || !canonicalSizeV1(item, remaining, depth+1) {
				return false
			}
		}
		return true
	case []any:
		if !add(2) {
			return false
		}
		for i, item := range value {
			if i > 0 && !add(1) {
				return false
			}
			if !canonicalSizeV1(item, remaining, depth+1) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
func descriptor(raw any, l LimitsV1) (DescriptorV1, error) {
	m, ok := object(raw)
	if !ok || !fields(m, "name", "title", "description", "inputSchema", "annotations", "icons", "execution", "_meta") {
		return DescriptorV1{}, ErrProtocol
	}
	name, ok := text(m["name"])
	if !ok || !validString(name, 128, false) {
		return DescriptorV1{}, ErrProtocol
	}
	for _, ch := range name {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-' || ch == '.') {
			return DescriptorV1{}, ErrProtocol
		}
	}
	for _, key := range []string{"title", "description"} {
		if v, present := m[key]; present {
			s, ok := text(v)
			if !ok || !validString(s, l.DescriptorBytes, true) {
				return DescriptorV1{}, ErrProtocol
			}
		}
	}
	schema, ok := object(m["inputSchema"])
	if !ok || schema["type"] != "object" {
		return DescriptorV1{}, ErrProtocol
	}
	for _, key := range []string{"annotations", "_meta"} {
		if v, present := m[key]; present {
			if _, ok := object(v); !ok {
				return DescriptorV1{}, ErrProtocol
			}
		}
	}
	if v, present := m["icons"]; present {
		if _, ok := v.([]any); !ok {
			return DescriptorV1{}, ErrProtocol
		}
	}
	required := false
	if v, present := m["execution"]; present {
		e, ok := object(v)
		if !ok || !fields(e, "taskSupport") {
			return DescriptorV1{}, ErrProtocol
		}
		if support, present := e["taskSupport"]; present {
			switch support {
			case "forbidden", "optional":
			case "required":
				required = true
			default:
				return DescriptorV1{}, ErrProtocol
			}
		}
	}
	body, err := canonical(m, l.DescriptorBytes)
	if err != nil {
		return DescriptorV1{}, ErrProtocol
	}
	input, err := canonical(schema, l.DescriptorBytes)
	if err != nil {
		return DescriptorV1{}, ErrProtocol
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless.mcp-descriptor.v1\x00"))
	_, _ = h.Write(body)
	return DescriptorV1{name: name, body: body, input: input, digest: hex.EncodeToString(h.Sum(nil)), taskRequired: required}, nil
}
