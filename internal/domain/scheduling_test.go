package domain_test

import (
	"encoding/json"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestLegacyProductLimitsRemainValidWithFiniteToolEventBudget(t *testing.T) {
	legacyPayload, err := json.Marshal(map[string]any{
		"max_tenant_queue_depth": uint32(8),
		"max_active_runs":        uint32(1),
		"max_runtime":            time.Minute,
		"max_turns":              uint32(30),
		"max_input_bytes":        uint64(16 << 20),
		"max_context_bytes":      uint64(64 << 20),
		"max_artifacts":          uint32(32),
	})
	if err != nil {
		t.Fatal(err)
	}
	var limits domain.ProductLimits
	if err := json.Unmarshal(legacyPayload, &limits); err != nil {
		t.Fatal(err)
	}
	if err := limits.Validate(); err != nil {
		t.Fatalf("legacy limits validation failed: %v", err)
	}
	maxEvents, maxBytes := limits.EffectiveToolEventLimits()
	if maxEvents != 60 || maxBytes != 64<<20 {
		t.Fatalf("effective tool-event limits = %d/%d, want %d/%d", maxEvents, maxBytes, 60, 64<<20)
	}
	if maxContextEvents := limits.EffectiveMaxContextEvents(); maxContextEvents != 120 {
		t.Fatalf("effective context event limit = %d, want 120", maxContextEvents)
	}
}

func TestProductLimitsRejectPartialToolEventBudget(t *testing.T) {
	limits := domain.ProductLimits{
		MaxTenantQueueDepth: 8,
		MaxActiveRuns:       1,
		MaxRuntime:          time.Minute,
		MaxTurns:            30,
		MaxInputBytes:       16 << 20,
		MaxContextBytes:     64 << 20,
		MaxArtifacts:        32,
		MaxToolEvents:       128,
	}
	if err := limits.Validate(); err == nil {
		t.Fatal("partial tool-event budget unexpectedly validated")
	}
}

func TestProductLimitsRequireExplicitToolEventBudgetForAdmission(t *testing.T) {
	limits := domain.ProductLimits{
		MaxTenantQueueDepth: 8,
		MaxActiveRuns:       1,
		MaxRuntime:          time.Minute,
		MaxTurns:            30,
		MaxInputBytes:       16 << 20,
		MaxContextBytes:     64 << 20,
		MaxArtifacts:        32,
	}
	if err := limits.ValidateForAdmission(); err == nil {
		t.Fatal("legacy tool-event budget unexpectedly accepted for new admission")
	}
}
