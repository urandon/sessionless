package mcpwire

import (
	"strings"
	"testing"
)

func setupLimitsFixture() LimitsV1 {
	return LimitsV1{MessageBytes: 64 << 10, AggregateBytes: 256 << 10, JSONDepth: 32, JSONMembers: 4096, SSELineBytes: 64 << 10, SSEEventBytes: 64 << 10, SSEEvents: 128, Tools: 1, DescriptorBytes: 32 << 10, ArgumentBytes: 4 << 10, ResultTextBytes: 64 << 10, ContentItems: 16}
}

func TestSetupInitializeNotificationAndExactCatalog(t *testing.T) {
	l := setupLimitsFixture()
	init, err := NewInitializeRequestV1("init-1", l)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(init.Body()); got != `{"id":"init-1","jsonrpc":"2.0","method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"sessionless","version":"1"},"protocolVersion":"2025-11-25"}}` {
		t.Fatalf("initialize bytes changed: %s", got)
	}
	ready, err := InitializedNotificationV1(l)
	if err != nil || string(ready) != `{"jsonrpc":"2.0","method":"notifications/initialized"}` {
		t.Fatalf("initialized notification: %s %v", ready, err)
	}
	if err := init.ValidateInitializationV1([]byte(`{"jsonrpc":"2.0","id":"init-1","result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"docs","version":"1"}}}`)); err != nil {
		t.Fatalf("empty tools capabilities: %v", err)
	}
	if err := init.ValidateInitializationV1([]byte(`{"jsonrpc":"2.0","id":"init-1","result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"docs","version":"1"}}}`)); err != nil {
		t.Fatalf("explicit false listChanged: %v", err)
	}

	list, err := NewListToolsRequestV1("list-1", l)
	if err != nil {
		t.Fatal(err)
	}
	descriptorRaw := []byte(`{"inputSchema":{"additionalProperties":false,"properties":{"query":{"maxLength":100,"minLength":1,"type":"string"}},"required":["query"],"type":"object"},"name":"search_openai_docs"}`)
	installed, err := ParseDescriptorV1(descriptorRaw, l)
	if err != nil {
		t.Fatal(err)
	}
	goodCatalog := `{"jsonrpc":"2.0","id":"list-1","result":{"tools":[` + string(descriptorRaw) + `]}}`
	if err := list.ValidateCatalogV1([]byte(goodCatalog), installed); err != nil {
		t.Fatalf("exact single catalog: %v", err)
	}
}

func TestSetupRejectsDriftNegotiationPaginationAndServerRequests(t *testing.T) {
	l := setupLimitsFixture()
	init, _ := NewInitializeRequestV1("init-1", l)
	badInitialization := []string{
		`{"jsonrpc":"2.0","id":"other","result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"docs","version":"1"}}}`,
		`{"jsonrpc":"2.0","id":"init-1","result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"docs","version":"1"}}}`,
		`{"jsonrpc":"2.0","id":"init-1","result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{"listChanged":true}},"serverInfo":{"name":"docs","version":"1"}}}`,
		`{"jsonrpc":"2.0","id":"init-1","result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{},"prompts":{}},"serverInfo":{"name":"docs","version":"1"}}}`,
		`{"jsonrpc":"2.0","id":"init-1","result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"docs","version":"1"},"instructions":"untrusted"}}`,
		`{"jsonrpc":"2.0","id":"init-1","result":{"protocolVersion":"2025-11-25","protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"docs","version":"1"}}}`,
		`{"jsonrpc":"2.0","method":"server/request","id":"srv-1","params":{}}`,
	}
	for i, raw := range badInitialization {
		if err := init.ValidateInitializationV1([]byte(raw)); err == nil {
			t.Errorf("bad initialization %d accepted", i)
		}
	}

	list, _ := NewListToolsRequestV1("list-1", l)
	descriptor := `{"inputSchema":{"additionalProperties":false,"properties":{"query":{"type":"string"}},"required":["query"],"type":"object"},"name":"search_openai_docs"}`
	installed, err := ParseDescriptorV1([]byte(descriptor), l)
	if err != nil {
		t.Fatal(err)
	}
	badCatalog := []string{
		`{"jsonrpc":"2.0","id":"other","result":{"tools":[` + descriptor + `]}}`,
		`{"jsonrpc":"2.0","id":"list-1","result":{"tools":[` + descriptor + `,` + descriptor + `]}}`,
		`{"jsonrpc":"2.0","id":"list-1","result":{"tools":[` + strings.Replace(descriptor, `"search_openai_docs"`, `"other_tool"`, 1) + `]}}`,
		`{"jsonrpc":"2.0","id":"list-1","result":{"tools":[` + descriptor + `],"nextCursor":"page-2"}}`,
		`{"jsonrpc":"2.0","id":"list-1","result":{"tools":[` + strings.Replace(descriptor, `"query"`, `"query","query"`, 1) + `]}}`,
	}
	for i, raw := range badCatalog {
		if err := list.ValidateCatalogV1([]byte(raw), installed); err == nil {
			t.Errorf("bad catalog %d accepted", i)
		}
	}
}

func TestSetupRejectsInvalidIdentifiersAndBoundsBeforeEncoding(t *testing.T) {
	l := setupLimitsFixture()
	for _, id := range []string{"", strings.Repeat("x", 257), "bad\x00id"} {
		if _, err := NewInitializeRequestV1(id, l); err == nil {
			t.Errorf("invalid id %q accepted", id)
		}
	}
	bad := l
	bad.MessageBytes = 1
	if _, err := NewListToolsRequestV1("list-1", bad); err == nil {
		t.Fatal("invalid request limits accepted")
	}
	if _, err := InitializedNotificationV1(bad); err == nil {
		t.Fatal("invalid notification limits accepted")
	}
}
