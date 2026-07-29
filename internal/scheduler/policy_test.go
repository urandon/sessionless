package scheduler

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestEvaluateAdmissionStates(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	limits := testLimits()
	base := Snapshot{
		Entitlement: domain.EntitlementActive,
		Quota:       domain.ProviderQuotaUnknown,
		Slot: domain.SubscriptionSchedulerSlot{
			TenantID: "tenant-1", SubscriptionConnectionID: "subscription-1",
			State: domain.SchedulerReady, UpdatedAt: now,
		},
	}
	shape := domain.WorkloadShape{
		Runtime: time.Minute, Turns: 1, InputBytes: 10,
		ContextBytes: 10, Artifacts: 1,
	}

	tests := []struct {
		name     string
		snapshot Snapshot
		shape    domain.WorkloadShape
		admit    bool
		state    domain.SchedulerState
		code     string
	}{
		{name: "ready", snapshot: base, shape: shape, admit: true, state: domain.SchedulerReady, code: "admitted"},
		{name: "reauth", snapshot: withEntitlement(base, domain.EntitlementReauthRequired), shape: shape, state: domain.SchedulerReauthRequired, code: "subscription_reauthentication_required"},
		{name: "busy", snapshot: withActiveRun(base), shape: shape, state: domain.SchedulerPressured, code: "subscription_slot_busy"},
		{name: "queue depth", snapshot: withQueueDepth(base, limits.MaxTenantQueueDepth), shape: shape, state: domain.SchedulerPressured, code: "tenant_queue_depth_exhausted"},
		{name: "input", snapshot: base, shape: withInput(shape, limits.MaxInputBytes+1), state: domain.SchedulerPressured, code: "input_limit_exceeded"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision, err := Evaluate(now, limits, test.shape, test.snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if decision.Admit != test.admit || decision.State != test.state || decision.Code != test.code {
				t.Fatalf("decision = %#v", decision)
			}
		})
	}
}

func TestEvaluatePreservesKnownQuotaReset(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	reset := now.Add(10 * time.Minute)
	snapshot := Snapshot{
		Entitlement: domain.EntitlementActive,
		Quota:       domain.ProviderQuotaLimited,
		Slot: domain.SubscriptionSchedulerSlot{
			TenantID: "tenant-1", SubscriptionConnectionID: "subscription-1",
			State: domain.SchedulerBlockedUntilReset, BlockedUntil: &reset,
			UpdatedAt: now,
		},
	}
	decision, err := Evaluate(now, testLimits(), domain.WorkloadShape{
		Runtime: time.Minute, Turns: 1,
	}, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Admit || decision.RetryAt == nil || !decision.RetryAt.Equal(reset) {
		t.Fatalf("decision = %#v", decision)
	}
}

func testLimits() domain.ProductLimits {
	return domain.ProductLimits{
		MaxTenantQueueDepth: 4, MaxActiveRuns: 1,
		MaxRuntime: 10 * time.Minute, MaxTurns: 20,
		MaxInputBytes: 1 << 20, MaxContextBytes: 4 << 20,
		MaxArtifacts: 16,
	}
}

func withEntitlement(snapshot Snapshot, state domain.EntitlementState) Snapshot {
	snapshot.Entitlement = state
	return snapshot
}

func withActiveRun(snapshot Snapshot) Snapshot {
	snapshot.Slot.ActiveRunID = "run-1"
	snapshot.Slot.ActiveReservationID = "reservation-1"
	snapshot.Slot.State = domain.SchedulerPressured
	return snapshot
}

func withQueueDepth(snapshot Snapshot, depth uint32) Snapshot {
	snapshot.QueueDepth = depth
	return snapshot
}

func withInput(shape domain.WorkloadShape, size uint64) domain.WorkloadShape {
	shape.InputBytes = size
	return shape
}
