package compactioncontext

import (
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// Test-only bridge to the closed private route union. It receives DATA, never
// an effect owner, and is not built into the production package.
func InspectContextV3SimultaneousRouteDenial(t *testing.T, profile domain.ConfiguredInferenceProfileV1, policy domain.AgentLoopPolicyV1, records []sessioncontext.EventPayload, operations []CompletedOperationV1, tools []configuredinference.ToolV1, data ContextDataV3) {
	t.Helper()
	route, err := newContextRouteV3(data)
	if err != nil {
		t.Fatal("valid route3 fixture", err)
	}
	if _, err := buildGenerationSource(profile, profile.Limits, policy, strings.Repeat("2", 64), records, operations, nil, tools, data.LegacyHistory, nil, route); err != nil {
		t.Fatal("valid route3 producer baseline", err)
	}
	route, err = newContextRouteV3(data)
	if err != nil {
		t.Fatal(err)
	}
	old, err := newContextRouteV2(ContextDataV2{Version: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buildGenerationSource(profile, profile.Limits, policy, strings.Repeat("2", 64), records, operations, nil, tools, data.LegacyHistory, old, route); err == nil {
		t.Fatal("simultaneous old/new private routes admitted")
	}
}
