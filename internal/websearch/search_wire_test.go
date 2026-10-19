package websearch

import (
	"encoding/json"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestWebSearchWireCompilerRequiresExplicitOriginalArm(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*domain.AttemptOperationV1)
	}{
		{"missing-arm", func(op *domain.AttemptOperationV1) { op.SearchWire = nil }},
		{"wrong-argument-size", func(op *domain.AttemptOperationV1) { op.SearchWire.ArgumentsBytes++ }},
		{"wrong-compiler-artifact", func(op *domain.AttemptOperationV1) { op.SearchWire.CompilerArtifactDigest = strings.Repeat("b", 64) }},
		{"wrong-profile", func(op *domain.AttemptOperationV1) { op.SearchWire.APIProfile = "other.search.v1" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := compileSearch(t, []byte(`{"query":"public query"}`))
			op := searchOperation(f)
			op.SearchWire = &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: domain.WebSearchAPIProfileV1, ArgumentsBytes: uint64(len(f.arguments)), CompilerArtifactDigest: f.grant.AdapterArtifactDigest}
			test.change(&op)
			if _, err := f.BindOperationV1(op); err == nil {
				t.Fatal("Tavily compiler accepted absent/divergent original search-wire arm")
			}
		})
	}
}

func TestWebSearchWireCompilerAdmitsLegalArgumentsWithLargerPhysicalBody(t *testing.T) {
	args, err := json.Marshal(map[string]string{"query": strings.Repeat("\x00", 675)})
	if err != nil {
		t.Fatal(err)
	}
	f := compileSearch(t, args)
	op := searchOperation(f)
	op.SearchWire = &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: domain.WebSearchAPIProfileV1, ArgumentsBytes: uint64(len(args)), CompilerArtifactDigest: f.grant.AdapterArtifactDigest}
	if len(args) > 4096 || len(f.Body()) <= 4096 || len(f.Body()) > 8192 {
		t.Fatalf("fixture args=%d wire=%d", len(args), len(f.Body()))
	}
	if _, err := f.BindOperationV1(op); err != nil {
		t.Fatalf("valid separate argument/wire commitments rejected: %v", err)
	}
}
