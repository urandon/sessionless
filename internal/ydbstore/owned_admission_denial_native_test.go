package ydbstore

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestOwnedFinalAdmissionYDBColdUnchangedDenialKeepsCanonicalSlot(t *testing.T) {
	g := newNativeOwnedFinalGraph(t)
	at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	if err := g.f.store.Transact(g.ctx, g.run.TenantID, func(state ports.StateTx) error {
		run, found, err := state.GetRun(g.ctx, g.run.ID)
		if err != nil || !found {
			return domain.ErrMembershipDenied
		}
		if err := run.Transition(domain.RunQuotaBlocked, at); err != nil {
			return err
		}
		return state.PutRun(g.ctx, run)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE subscription_connections SET entitlement_state=$1 WHERE tenant_id=$2 AND subscription_connection_id=$3`, domain.EntitlementInactive, g.run.TenantID, g.run.SubscriptionConnectionID); err != nil {
		t.Fatal(err)
	}
	result, budget, err := g.measuredOwnedAdmission()
	if err != nil || result.Admitted || result.Code != "subscription_reauthentication_required" || result.State != domain.SchedulerReauthRequired {
		t.Fatalf("unchanged cold denial result=%+v err=%v", result, err)
	}
	requireNativeOwnedFinalBudget(t, budget, "cold-unchanged-owned-denial")
	var state domain.SchedulerState
	var run, reservation string
	var updated time.Time
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT state,active_run_id,active_reservation_id,updated_at FROM subscription_scheduler_slots WHERE tenant_id=$1 AND subscription_connection_id=$2`, g.run.TenantID, g.run.SubscriptionConnectionID).Scan(&state, &run, &reservation, &updated); err != nil || state != domain.SchedulerReauthRequired || run != "" || reservation != "" || updated.Before(at) {
		t.Fatalf("cold denial lost canonical final slot state=%s active=%s reservation=%s err=%v", state, run, reservation, err)
	}
	for _, table := range []string{"attempt_execution_admissions_v1", "attempt_execution_admissions_by_session_v1", "worker_jobs", "quota_reservations"} {
		g.requireCount(t, table, 0)
	}
	g.requireOriginalCurrentSources(t)
}
