package ydbstore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

// Fixture-only serializable admission/publication races. A committed admission
// is not terminal completion, an operation grant, live payer approval, or owned
// serving enablement. Terminal/reconciliation authorities remain separate; the
// public serving gate and registries stay closed. No execution effects occur.
type nativeOwnedRaceOutcome struct {
	index  int
	result ports.DispatchAdmissionResult
	err    error
}

// Both calls acknowledge the barrier before either is released. The barrier
// does not prescribe which transaction serializes first, or force overlapping
// SQL statements. Every worker returns its own error; only the test goroutine
// asserts. The inherited native context bounds setup, calls and collection.
func nativeOwnedAdmissionPair(t *testing.T, ctx context.Context, calls [2]func() (ports.DispatchAdmissionResult, error)) [2]nativeOwnedRaceOutcome {
	t.Helper()
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	finished := make(chan nativeOwnedRaceOutcome, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	for i, call := range calls {
		go func(index int, call func() (ports.DispatchAdmissionResult, error)) {
			defer workers.Done()
			ready <- struct{}{}
			select {
			case <-start:
				result, err := call()
				finished <- nativeOwnedRaceOutcome{index: index, result: result, err: err}
			case <-ctx.Done():
				finished <- nativeOwnedRaceOutcome{index: index, err: ctx.Err()}
			}
		}(i, call)
	}
	joined := make(chan struct{})
	go func() {
		workers.Wait()
		close(joined)
	}()
	// Runs before the fixture's earlier-registered database cleanup, including
	// a deadline/fatal branch. Never delete the scope while a worker uses it.
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		select {
		case <-joined:
		case <-cleanup.Done():
			t.Error("native race workers did not finish before bounded cleanup")
		}
	})
	released := false
	defer func() {
		if !released {
			close(start)
		}
	}()
	for range 2 {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("native race barrier readiness deadline", ctx.Err())
		}
	}
	close(start)
	released = true
	var result [2]nativeOwnedRaceOutcome
	for range 2 {
		select {
		case outcome := <-finished:
			result[outcome.index] = outcome
		case <-ctx.Done():
			t.Fatal("native race completion deadline", ctx.Err())
		}
	}
	<-joined // both calls already completed and published their outcomes
	return result
}

func TestOwnedFinalAdmissionYDBConcurrentCreateAndReplay(t *testing.T) {
	g := newNativeOwnedFinalGraph(t)
	call := func() (ports.DispatchAdmissionResult, error) {
		return g.f.store.admitBoundedOwnedDispatch(g.ctx, g.request)
	}
	outcomes := nativeOwnedAdmissionPair(t, g.ctx, [2]func() (ports.DispatchAdmissionResult, error){call, call})
	created := 0
	var committed ports.DispatchAdmissionResult
	for _, outcome := range outcomes {
		if outcome.err != nil {
			want := ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
			if !errors.Is(outcome.err, domain.ErrMembershipDenied) || !reflect.DeepEqual(outcome.result, want) {
				t.Errorf("create racer=%d escaped bounded denial: err=%v result=%+v", outcome.index, outcome.err, outcome.result)
			}
			continue // bounded serializable conflict exhaustion may deny a loser
		}
		if !outcome.result.Admitted || outcome.result.Delivery != ports.DispatchDeliveryManagedQueue {
			t.Errorf("create racer=%d success is not admission: %+v", outcome.index, outcome.result)
		}
		switch outcome.result.Code {
		case "admitted":
			created++
			committed = outcome.result
		case "already_admitted":
		default:
			t.Errorf("create racer=%d unexpected successful outcome=%s", outcome.index, outcome.result.Code)
		}
	}
	if created != 1 {
		t.Fatalf("confirmed create outcomes=%d want exactly one; outcomes=%+v", created, outcomes)
	}
	g.requireCommittedGraph(t, committed) // one held unit; queue=1, active=0
	g.requireOriginalCurrentSources(t)
	admission, raw := g.finalAdmission(t)
	before := g.persistedGraph(t)
	replay, budget, err := g.measuredOwnedAdmission()
	if err != nil || !replay.Admitted || replay.Code != "already_admitted" {
		t.Fatalf("post-race confirmed replay: err=%v result=%+v", err, replay)
	}
	requireNativeOwnedFinalBudget(t, budget, "post-create-race-replay")
	if budget.writes != 0 {
		t.Errorf("post-race replay actual SQL writes=%d want=0", budget.writes)
	}
	afterAdmission, afterRaw := g.finalAdmission(t)
	if raw != afterRaw || !reflect.DeepEqual(admission, afterAdmission) || !reflect.DeepEqual(before, g.persistedGraph(t)) {
		t.Error("post-race replay rewrote original final/Job/capacity/projection")
	}
}

func TestOwnedFinalAdmissionYDBPublicRevokeRacesPrivateAdmission(t *testing.T) {
	for _, component := range []string{"profile", "mcp_spending"} {
		t.Run(component, func(t *testing.T) {
			g := newNativeOwnedFinalGraph(t)
			priorHead, found, err := readJSON[runexplanation.HeadV1](g.ctx, g.f.store.db, `SELECT record FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
			if err != nil || !found {
				t.Fatal("missing original head before revoke race", err)
			}
			// Install the exact immutable signed intent BEFORE launching workers;
			// the fixture allowlist is never mutated concurrently with verification.
			at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
			var revoke func() (ports.DispatchAdmissionResult, error)
			if component == "profile" {
				profile := g.profileReceipt.Profile.Clone()
				profile.State, profile.RevocationGeneration = domain.OwnedAgentCurrentRevokedV1, 2
				profile.MutationID = "native-final-race-profile-revoke"
				grant := g.profileVerifier.install(t, ports.OwnedProfilePublicationGrantV1{Profile: profile, ExpectedGeneration: 1, ExpectedDigest: g.profileReceipt.Digest, IssuedAt: at, ExpiresAt: at.Add(10 * time.Second)})
				revoke = func() (ports.DispatchAdmissionResult, error) {
					receipt, err := g.f.store.PublishOwnedProfile(g.ctx, grant)
					if err != nil {
						return ports.DispatchAdmissionResult{}, fmt.Errorf("public profile revoke: %w", err)
					}
					if receipt.Replayed || receipt.Profile.State != domain.OwnedAgentCurrentRevokedV1 || receipt.Profile.RevocationGeneration != 2 {
						return ports.DispatchAdmissionResult{}, errors.New("public profile revoke receipt mismatch")
					}
					return ports.DispatchAdmissionResult{}, nil
				}
			} else {
				var receipt ports.AttemptSpendingPublicationReceiptV1
				for _, published := range g.spendReceipts {
					if published.Authorization.Locator.ComponentKind == domain.AttemptSpendingMCPToolV1 {
						receipt = published
					}
				}
				if receipt.Authorization.Locator.ComponentKind != domain.AttemptSpendingMCPToolV1 {
					t.Fatal("missing published MCP spending before race")
				}
				grant := spendingRevoke(t, g.spendVerifier, ports.AttemptSpendingPublicationGrantV1{Authorization: receipt.Authorization.Clone()}, receipt, at)
				revoke = func() (ports.DispatchAdmissionResult, error) {
					receipt, err := g.f.store.PublishAttemptSpending(g.ctx, grant)
					if err != nil {
						return ports.DispatchAdmissionResult{}, fmt.Errorf("public spending revoke: %w", err)
					}
					if receipt.Replayed || receipt.Authorization.State != domain.OwnedAgentCurrentRevokedV1 || receipt.Authorization.RevocationGeneration != 2 {
						return ports.DispatchAdmissionResult{}, errors.New("public spending revoke receipt mismatch")
					}
					return ports.DispatchAdmissionResult{}, nil
				}
			}
			admit := func() (ports.DispatchAdmissionResult, error) {
				return g.f.store.admitBoundedOwnedDispatch(g.ctx, g.request)
			}
			outcomes := nativeOwnedAdmissionPair(t, g.ctx, [2]func() (ports.DispatchAdmissionResult, error){admit, revoke})
			if outcomes[1].err != nil {
				t.Fatal("public revocation did not confirm", outcomes[1].err)
			}
			wantDenied := ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
			committed := outcomes[0].err == nil
			var before []string
			var originalFinal string
			if committed {
				// Admission may legitimately serialize BEFORE the revocation. Do
				// not infer the SQL serialization order from goroutine completion.
				g.requireCommittedGraph(t, outcomes[0].result)
				_, originalFinal = g.finalAdmission(t)
				before = g.persistedGraph(t)
			} else {
				if !errors.Is(outcomes[0].err, domain.ErrMembershipDenied) || !reflect.DeepEqual(outcomes[0].result, wantDenied) {
					t.Errorf("admission revoke-race denial: err=%v result=%+v", outcomes[0].err, outcomes[0].result)
				}
				requireNativeManagedAdmissionRollback(t, g.nativeManagedAdmissionGraph, &priorHead)
				g.requireCount(t, "attempt_execution_admissions_v1", 0)
				g.requireCount(t, "attempt_execution_admissions_by_session_v1", 0)
			}
			sources := g.sourceRecords(t)
			result, err := g.f.store.admitBoundedOwnedDispatch(g.ctx, g.request)
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, wantDenied) {
				t.Errorf("fresh request after confirmed revoke escaped denial: err=%v result=%+v", err, result)
			}
			if !reflect.DeepEqual(sources, g.sourceRecords(t)) {
				t.Error("denied post-revocation admission changed current authority")
			}
			if committed {
				_, afterFinal := g.finalAdmission(t)
				if originalFinal != afterFinal || !reflect.DeepEqual(before, g.persistedGraph(t)) {
					t.Error("post-revoke denial rewrote admission history or capacity")
				}
			} else {
				requireNativeManagedAdmissionRollback(t, g.nativeManagedAdmissionGraph, &priorHead)
				g.requireCount(t, "attempt_execution_admissions_v1", 0)
				g.requireCount(t, "attempt_execution_admissions_by_session_v1", 0)
			}
		})
	}
}

func TestOwnedFinalAdmissionYDBColdOriginalJobCollisionRollsBack(t *testing.T) {
	g := newNativeOwnedFinalGraph(t)
	priorHead, found, err := readJSON[runexplanation.HeadV1](g.ctx, g.f.store.db, `SELECT record FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found {
		t.Fatal("missing original cold head", err)
	}
	result, err := g.f.store.admitBoundedOwnedDispatch(g.ctx, g.request)
	if err != nil {
		t.Fatal("derive real committed Job for collision", err)
	}
	g.requireCommittedGraph(t, result)
	g.finalAdmission(t)
	var originalJob string
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CAST(payload AS String) FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID).Scan(&originalJob); err != nil {
		t.Fatal("read real committed Job bytes", err)
	}
	// Deliberate test-owned torn recovery setup, NOT a valid production cleanup:
	// retain the REAL committed Job, remove its admission/capacity graph, restore
	// original Run/head, and request a DIFFERENT reservation. Never fabricate a
	// Job or treat missing final history as permission to overwrite the old Job.
	err = g.f.store.Transact(g.ctx, g.run.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		for _, table := range []string{"attempt_execution_admissions_by_session_v1", "attempt_execution_admissions_v1", "quota_expiry", "quota_expiry_v2", "quota_reservations", "subscription_scheduler_slots", "tenant_scheduler_counters", "audit_events"} {
			if _, err := tx.sqlTx.ExecContext(g.ctx, "DELETE FROM "+table+" WHERE tenant_id=$1", g.run.TenantID); err != nil {
				return err
			}
		}
		if err := state.PutRun(g.ctx, g.run); err != nil {
			return err
		}
		raw, err := marshal(priorHead)
		if err != nil {
			return err
		}
		_, err = tx.sqlTx.ExecContext(g.ctx, `UPDATE run_explanation_heads_v1 SET revision=$1,record=CAST($2 AS JsonDocument) WHERE tenant_id=$3 AND run_id=$4`, priorHead.Revision, raw, g.run.TenantID, g.run.ID)
		return err
	})
	if err != nil {
		t.Fatal("install exact test-owned cold original-Job fault", err)
	}
	request := g.request
	request.ReservationID = domain.QuotaReservationID("collision-" + string(g.request.ReservationID))
	if err := validateAdmissionRequest(request); err != nil {
		t.Fatal("different reservation must be structurally valid", err)
	}
	result, err = g.f.store.admitBoundedOwnedDispatch(g.ctx, request)
	want := ports.DispatchAdmissionResult{RunID: g.run.ID, AttemptID: g.attempt.ID}
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, want) {
		t.Errorf("cold original Job collision escaped denial: err=%v result=%+v", err, result)
	}
	var afterJob string
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CAST(payload AS String) FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID).Scan(&afterJob); err != nil || afterJob != originalJob {
		t.Errorf("collision overwrote/deleted actual original Job: err=%v unchanged=%v", err, afterJob == originalJob)
	}
	for _, table := range nativeManagedAdmissionEffectTables {
		want := uint64(0)
		if table == "worker_jobs" || table == "run_explanation_heads_v1" {
			want = 1
		}
		g.requireCount(t, table, want)
	}
	g.requireCount(t, "attempt_execution_admissions_v1", 0)
	g.requireCount(t, "attempt_execution_admissions_by_session_v1", 0)
	actualRun, found, err := readJSON[domain.Run](g.ctx, g.f.store.db, `SELECT payload FROM runs WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || !reflect.DeepEqual(actualRun, g.run) {
		t.Errorf("collision rollback changed restored Run: found=%v err=%v", found, err)
	}
	actualAttempt, found, err := readJSON[domain.Attempt](g.ctx, g.f.store.db, `SELECT payload FROM attempts WHERE tenant_id=$1 AND attempt_id=$2`, g.run.TenantID, g.attempt.ID)
	if err != nil || !found || !reflect.DeepEqual(actualAttempt, g.attempt) {
		t.Errorf("collision rollback changed original Attempt: found=%v err=%v", found, err)
	}
	actualHead, found, err := readJSON[runexplanation.HeadV1](g.ctx, g.f.store.db, `SELECT record FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, g.run.TenantID, g.run.ID)
	if err != nil || !found || !reflect.DeepEqual(actualHead, priorHead) {
		t.Errorf("collision rollback changed restored head: found=%v err=%v", found, err)
	}
	var status domain.RunStatus
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT status FROM runs_by_session WHERE tenant_id=$1 AND session_id=$2 AND created_at=$3 AND run_id=$4`, g.run.TenantID, g.run.SessionID, g.run.CreatedAt, g.run.ID).Scan(&status); err != nil || status != g.run.Status {
		t.Errorf("collision rollback changed restored Run index: status=%s err=%v", status, err)
	}
	g.requireOriginalCurrentSources(t)
}
