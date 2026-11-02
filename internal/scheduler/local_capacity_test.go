package scheduler

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestLocalCapacityPolicyRequiresExactReadyArm(t *testing.T) {
	at := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	for _, fault := range []string{"ready", "unknown source", "fabricated entitlement", "fabricated quota", "blocked", "reauth", "draining", "draining unavailable", "busy", "missing association", "foreign tenant", "mixed arm"} {
		t.Run(fault, func(t *testing.T) {
			association := domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: "tenant", OwnerUserID: "owner", ResourceID: "resource"}
			s := Snapshot{LocalSourceReady: true, Slot: domain.SubscriptionSchedulerSlot{TenantID: association.TenantID, LocalCapacityAssociation: &association, State: domain.SchedulerReady, UpdatedAt: at}}
			switch fault {
			case "unknown source":
				s.LocalSourceReady = false
			case "fabricated entitlement":
				s.Entitlement = domain.EntitlementActive
			case "fabricated quota":
				s.Quota = domain.ProviderQuotaAvailable
			case "blocked":
				s.Slot.State = domain.SchedulerBlockedUntilReset
			case "reauth":
				s.Slot.State = domain.SchedulerReauthRequired
			case "draining":
				s.Slot.State = domain.SchedulerDraining
			case "draining unavailable":
				s.Slot.State, s.LocalSourceReady = domain.SchedulerDraining, false
			case "busy":
				s.Slot.ActiveRunID, s.Slot.ActiveReservationID = "run", "reservation"
			case "missing association":
				s.Slot.LocalCapacityAssociation, s.Slot.SubscriptionConnectionID = nil, "subscription"
				s.Entitlement, s.Quota = domain.EntitlementActive, domain.ProviderQuotaAvailable
			case "foreign tenant":
				association.TenantID = "foreign"
			case "mixed arm":
				s.Slot.SubscriptionConnectionID = "subscription"
			}
			decision, err := Evaluate(at, testLimits(), domain.WorkloadShape{Runtime: time.Minute, Turns: 1}, s)
			if fault == "ready" {
				if err != nil || !decision.Admit {
					t.Fatalf("explicit ready local arm denied: decision=%+v err=%v", decision, err)
				}
			} else if err == nil && decision.Admit {
				t.Fatalf("local readiness promoted invalid arm: fault=%s decision=%+v", fault, decision)
			}
			if fault == "unknown source" && (err != nil || decision.Code != "local_source_unavailable") {
				t.Errorf("unknown local source lost denial: decision=%+v err=%v", decision, err)
			}
			if fault == "draining unavailable" && (err != nil || decision.State != domain.SchedulerDraining) {
				t.Errorf("unavailable source erased draining state: decision=%+v err=%v", decision, err)
			}
		})
	}
}

func TestLocalCapacityPolicyPreservesCommonLimits(t *testing.T) {
	at := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	limits := testLimits()
	for _, fault := range []string{"runtime", "turns", "input", "context", "artifacts", "queue", "active", "busy"} {
		t.Run(fault, func(t *testing.T) {
			association := domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: "tenant", OwnerUserID: "owner", ResourceID: "resource"}
			s := Snapshot{LocalSourceReady: true, Slot: domain.SubscriptionSchedulerSlot{TenantID: association.TenantID, LocalCapacityAssociation: &association, State: domain.SchedulerReady, UpdatedAt: at}}
			shape := domain.WorkloadShape{Runtime: time.Minute, Turns: 1}
			want := ""
			switch fault {
			case "runtime":
				shape.Runtime, want = limits.MaxRuntime+time.Nanosecond, "runtime_limit_exceeded"
			case "turns":
				shape.Turns, want = limits.MaxTurns+1, "turn_limit_exceeded"
			case "input":
				shape.InputBytes, want = limits.MaxInputBytes+1, "input_limit_exceeded"
			case "context":
				shape.ContextBytes, want = limits.MaxContextBytes+1, "context_limit_exceeded"
			case "artifacts":
				shape.Artifacts, want = limits.MaxArtifacts+1, "artifact_limit_exceeded"
			case "queue":
				s.QueueDepth, want = limits.MaxTenantQueueDepth, "tenant_queue_depth_exhausted"
			case "active":
				s.ActiveRuns, want = limits.MaxActiveRuns, "tenant_active_run_limit"
			case "busy":
				s.Slot.ActiveRunID, s.Slot.ActiveReservationID, want = "original-run", "original-reservation", "local_capacity_slot_busy"
			}
			decision, err := Evaluate(at, limits, shape, s)
			if err != nil || decision.Admit || decision.Code != want || decision.State != domain.SchedulerPressured {
				t.Fatalf("local capacity bypassed common bound: fault=%s decision=%+v want=%s err=%v", fault, decision, want, err)
			}
		})
	}
}
