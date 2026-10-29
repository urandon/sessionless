package scheduler

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestLocalCapacityPressureReevaluatesCurrentLimits(t *testing.T) {
	at := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	limits := testLimits()
	for _, cause := range []string{"queue", "active", "workload"} {
		t.Run(cause, func(t *testing.T) {
			association := domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: "tenant", OwnerUserID: "owner", ResourceID: "resource"}
			snapshot := Snapshot{LocalSourceReady: true, Slot: domain.SubscriptionSchedulerSlot{TenantID: association.TenantID, LocalCapacityAssociation: &association, State: domain.SchedulerReady, UpdatedAt: at}}
			shape := domain.WorkloadShape{Runtime: time.Minute, Turns: 1}
			switch cause {
			case "queue":
				snapshot.QueueDepth = limits.MaxTenantQueueDepth
			case "active":
				snapshot.ActiveRuns = limits.MaxActiveRuns
			case "workload":
				shape.Runtime = limits.MaxRuntime + time.Nanosecond
			}
			first, err := Evaluate(at, limits, shape, snapshot)
			if err != nil || first.Admit || first.State != domain.SchedulerPressured {
				t.Fatalf("initial %s pressure: decision=%+v err=%v", cause, first, err)
			}
			// This is the state persisted by admission after the first denial.
			snapshot.Slot.State = first.State
			stillPressured, err := Evaluate(at, limits, shape, snapshot)
			if err != nil || stillPressured.Admit || stillPressured.Code != first.Code {
				t.Errorf("unchanged %s pressure lost original limit: decision=%+v want=%s err=%v", cause, stillPressured, first.Code, err)
			}
			snapshot.QueueDepth, snapshot.ActiveRuns = 0, 0
			shape.Runtime = time.Minute
			recovered, err := Evaluate(at, limits, shape, snapshot)
			if err != nil || !recovered.Admit || recovered.State != domain.SchedulerReady {
				t.Fatalf("cleared %s pressure stranded empty resource: decision=%+v err=%v", cause, recovered, err)
			}
			snapshot.LocalSourceReady = false
			denied, err := Evaluate(at, limits, shape, snapshot)
			if err != nil || denied.Admit || denied.Code != "local_source_unavailable" {
				t.Fatalf("pressure recovery bypassed current Source: decision=%+v err=%v", denied, err)
			}
			snapshot.LocalSourceReady = true
			snapshot.Slot.ActiveRunID, snapshot.Slot.ActiveReservationID = "held-run", "held-reservation"
			busy, err := Evaluate(at, limits, shape, snapshot)
			if err != nil || busy.Admit || busy.Code != "local_capacity_slot_busy" {
				t.Fatalf("pressure recovery stole held resource: decision=%+v err=%v", busy, err)
			}
		})
	}
}
