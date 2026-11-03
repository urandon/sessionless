package websearch

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestModelToolV1MatchesCompilerContract(t *testing.T) {
	tool := ModelToolV1()
	if tool.Name != "web_search" || tool.Description != "Search public web sources for a query." {
		t.Fatal("unexpected canonical public tool identity", tool.Name, tool.Description)
	}
	wantSchema := `{"type":"object","properties":{"query":{"type":"string","minLength":1,"maxLength":1024},"max_results":{"type":"integer","minimum":1,"maximum":5}},"required":["query"],"additionalProperties":false}`
	if string(tool.Parameters) != wantSchema {
		t.Fatalf("canonical SEARCH schema: got %s want %s", tool.Parameters, wantSchema)
	}
	_, _, _, at := resourceCompilerFixture(t)
	for _, args := range []string{`{"query":"public documentation"}`, `{"query":"public documentation","max_results":1}`, `{"query":"public documentation","max_results":5}`} {
		g, cost, b, _ := resourceCompilerFixture(t)
		f, err := CompileV2(g, cost, b, []byte(args), at)
		if err != nil {
			t.Fatal("canonical optional/range compiler input", args, err)
		}
		var wire struct {
			MaxResults int `json:"max_results"`
		}
		if json.Unmarshal(f.Body(), &wire) != nil || wire.MaxResults < minimumResultsV1 || wire.MaxResults > maximumResultsV1 || args == `{"query":"public documentation"}` && wire.MaxResults != defaultResultsV1 {
			t.Fatal("actual fixed wire lost default/range", args, wire.MaxResults)
		}
	}
	for _, args := range []string{`{"query":""}`, `{"query":"  "}`, `{"query":"ok","max_results":0}`, `{"query":"ok","max_results":6}`, `{"query":"ok","extra":true}`, `{"query":"` + strings.Repeat("é", 513) + `"}`} {
		g, cost, b, _ := resourceCompilerFixture(t)
		if _, err := CompileV2(g, cost, b, []byte(args), at); err == nil {
			t.Fatal("actual byte/nonblank/closed argument compiler accepted", args)
		}
	}
	// The schema goes through the existing configured schema language, not a
	// second validator. The compiler, not maxLength, owns UTF-8 byte admission.
	limits := domain.ConfiguredInferenceLimitsV1{RequestBytes: 1 << 20, ResponseBytes: 1 << 20, ResponseHeaderBytes: 16 << 10, JSONDepth: 32, JSONMembers: 256, Choices: 1, ToolCalls: 1, ToolNameBytes: 128, ToolCallIDBytes: 128, ToolArgumentsBytes: 4 << 10, FinalTextBytes: 256 << 10, Messages: 256, Tools: 2, MaxInputTokens: 1024, MaxOutputTokens: 1024, OperationTimeout: 30 * time.Second, CleanupTimeout: 30 * time.Second}
	if err := configuredinference.ValidateToolSchemaV1(tool.Parameters, limits); err != nil {
		t.Fatal("canonical schema unsupported by configured compiler", err)
	}
	clear(tool.Parameters)
	if bytes.Equal(tool.Parameters, ModelToolV1().Parameters) {
		t.Fatal("canonical tool parameters were shared mutable storage")
	}
}
