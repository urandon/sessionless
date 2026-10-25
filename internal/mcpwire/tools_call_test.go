package mcpwire

import (
	"bytes"
	"strings"
	"testing"
)

func callFixture(t *testing.T) RequestV1 {
	t.Helper()
	l := LimitsV1{MessageBytes: 65536, AggregateBytes: 262144, JSONDepth: 32, JSONMembers: 4096, SSELineBytes: 65536, SSEEventBytes: 65536, SSEEvents: 128, Tools: 1, DescriptorBytes: 32768, ArgumentBytes: 4096, ResultTextBytes: 65536, ContentItems: 16}
	d, e := ParseDescriptorV1([]byte(`{"inputSchema":{"additionalProperties":false,"properties":{"query":{"type":"string"}},"required":["query"],"type":"object"},"name":"search_openai_docs"}`), l)
	if e != nil {
		t.Fatal(e)
	}
	r, e := NewToolCallRequestV1("call-1", d, []byte(`{"query":"docs"}`), l)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestMCPToolCallAndKnownTextResult(t *testing.T) {
	r := callFixture(t)
	original := r.Body()
	b := r.Body()
	clear(b)
	if !bytes.Equal(original, r.Body()) {
		t.Fatal("request aliases caller")
	}
	v, e := r.DecodeToolResultV1([]byte(`{"jsonrpc":"2.0","id":"call-1","result":{"content":[{"type":"text","text":"public answer"}]}}`))
	if e != nil || v.IsError || len(v.Content) != 1 {
		t.Fatal("known result", e)
	}
	if _, e = r.ValidateToolResultV1(v); e != nil {
		t.Fatal(e)
	}
	v.Content[0].Text = "altered"
	if _, e = r.ValidateToolResultV1(v); e == nil {
		t.Fatal("mutable projection changed immutable result")
	}
	for _, raw := range []string{`{"jsonrpc":"2.0","id":"other","result":{"content":[]}}`, `{"jsonrpc":"2.0","id":"call-1","error":{"code":1}}`, `{"jsonrpc":"2.0","id":"call-1","result":{"content":[{"type":"image","text":"x"}]}}`, `{"jsonrpc":"2.0","id":"call-1","result":{"content":[],"structuredContent":{}}}`, `{"jsonrpc":"2.0","id":"call-1","result":{"content":[],"isError":1}}`, `{"jsonrpc":"2.0","id":"call-1","result":{"content":[{"type":"text","text":"x","extra":0}]}}`} {
		if _, e = r.DecodeToolResultV1([]byte(raw)); e == nil {
			t.Fatal("invalid accepted")
		}
	}
	if _, e = r.ValidateToolResultV1(ToolResultV1{Content: []TextContentV1{{Text: "forged"}}}); e == nil {
		t.Fatal("caller struct accepted")
	}
}
func TestMCPToolCallArgumentsAndBounds(t *testing.T) {
	r := callFixture(t)
	d := *r.descriptor
	for _, b := range []string{`[]`, `{"query":"a","query":"b"}`, ` {"query":"docs"}`, `{"query":"` + strings.Repeat("x", 4096) + `"}`} {
		if _, e := NewToolCallRequestV1("call-1", d, []byte(b), r.limits); e == nil {
			t.Fatal("invalid args accepted")
		}
	}
	l := r.limits
	l.ResultTextBytes = 2
	r.limits = l
	if _, e := r.DecodeToolResultV1([]byte(`{"jsonrpc":"2.0","id":"call-1","result":{"content":[{"type":"text","text":"ab"},{"type":"text","text":"c"}]}}`)); e == nil {
		t.Fatal("aggregate text bound ignored")
	}
}
