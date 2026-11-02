package configuredinference

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Pure schema/request DATA only. The public installation check and runtime
// freezer must use one closed language, not a more permissive JSON Schema DSL.
func TestValidateToolSchemaV1MatchesRuntimeClosedCompiler(t *testing.T) {
	for _, check := range []struct {
		name     string
		schema   json.RawMessage
		accepted bool
	}{
		{name: "supported-query", schema: json.RawMessage(toolSchema), accepted: true},
		{name: "empty-object", schema: json.RawMessage(`{"type":"object","properties":{},"required":[],"additionalProperties":false}`), accepted: true},
		{name: "supported-nested", schema: json.RawMessage(`{"type":"object","properties":{"items":{"type":"array","items":{"type":"integer","minimum":0,"maximum":100},"minItems":0,"maxItems":2},"enabled":{"type":"boolean"},"empty":{"type":"null"},"choice":{"type":"string","enum":["a","b"],"maxLength":1}},"required":["items"],"additionalProperties":false}`), accepted: true},
		{name: "allOf", schema: json.RawMessage(`{"type":"object","properties":{},"required":[],"additionalProperties":false,"allOf":[]}`)},
		{name: "ref", schema: json.RawMessage(`{"type":"object","properties":{},"required":[],"additionalProperties":false,"$ref":"#"}`)},
		{name: "dialect", schema: json.RawMessage(`{"type":"object","properties":{},"required":[],"additionalProperties":false,"$schema":"https://json-schema.org/draft/2020-12/schema"}`)},
		{name: "nested-format", schema: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string","format":"date"}},"required":[],"additionalProperties":false}`)},
		{name: "union", schema: json.RawMessage(`{"type":"object","properties":{"query":{"type":["string","null"]}},"required":[],"additionalProperties":false}`)},
		{name: "missing-required", schema: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`)},
		{name: "open-properties", schema: json.RawMessage(`{"type":"object","properties":{},"required":[],"additionalProperties":true}`)},
		{name: "primitive-root", schema: json.RawMessage(`{"type":"string"}`)},
		{name: "duplicate-key", schema: json.RawMessage(`{"type":"object","type":"object","properties":{},"required":[],"additionalProperties":false}`)},
		{name: "trailing-value", schema: json.RawMessage(`{"type":"object","properties":{},"required":[],"additionalProperties":false}{}`)},
		{name: "invalid-utf8", schema: json.RawMessage{'{', '"', 0xff, '"', ':', '1', '}'}},
		{name: "empty"},
	} {
		t.Run(check.name, func(t *testing.T) {
			profile := testProfile(t)
			assertToolSchemaAndRuntime(t, profile, profile.Limits, check.schema, check.accepted)
		})
	}
}

func TestValidateToolSchemaV1UsesOriginalRuntimeBounds(t *testing.T) {
	for _, check := range []struct {
		name   string
		mutate func(*domain.ConfiguredInferenceLimitsV1)
	}{
		{name: "insufficient-wire-depth", mutate: func(l *domain.ConfiguredInferenceLimitsV1) { l.JSONDepth = 4 }},
		{name: "schema-depth", mutate: func(l *domain.ConfiguredInferenceLimitsV1) { l.JSONDepth = 6 }},
		{name: "schema-members", mutate: func(l *domain.ConfiguredInferenceLimitsV1) { l.JSONMembers = 3 }},
		{name: "schema-bytes", mutate: func(l *domain.ConfiguredInferenceLimitsV1) { l.RequestBytes = uint64(len(toolSchema) - 1) }},
		{name: "schema-argument-bound", mutate: func(l *domain.ConfiguredInferenceLimitsV1) { l.ToolArgumentsBytes = 29 }},
		{name: "invalid-effective-limits", mutate: func(l *domain.ConfiguredInferenceLimitsV1) { l.OperationTimeout = 0 }},
	} {
		t.Run(check.name, func(t *testing.T) {
			profile := testProfile(t)
			effective := profile.Limits
			check.mutate(&effective)
			assertToolSchemaAndRuntime(t, profile, effective, json.RawMessage(toolSchema), false)
		})
	}
}

func assertToolSchemaAndRuntime(t *testing.T, profile domain.ConfiguredInferenceProfileV1, effective domain.ConfiguredInferenceLimitsV1, schema json.RawMessage, accepted bool) {
	t.Helper()
	original := bytes.Clone(schema)
	validationErr := ValidateToolSchemaV1(schema, effective)
	request := testRequest()
	request.Tools[0].Parameters = schema
	frozen, runtimeErr := FreezeRequestV1(profile, effective, request)
	if (validationErr == nil) != accepted || (runtimeErr == nil) != accepted || (frozen != nil) != accepted {
		t.Fatalf("schema acceptance=%t validator=%v runtime=%v frozen=%t", accepted, validationErr, runtimeErr, frozen != nil)
	}
	if !accepted && (!errors.Is(validationErr, ErrContract) || !errors.Is(runtimeErr, ErrContract)) {
		t.Fatalf("schema denial lost existing contract classification: validator=%v runtime=%v", validationErr, runtimeErr)
	}
	if !bytes.Equal(original, schema) {
		t.Fatal("schema validation mutated original caller bytes")
	}
}
