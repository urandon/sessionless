package attachedworkerux

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworker"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type actionTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *actionTestClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *actionTestClock) Add(delta time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(delta)
}

type actionTestIDs struct {
	mu     sync.Mutex
	plan   int
	action int
}

func (ids *actionTestIDs) NewID(_ context.Context, kind ports.IDKind) (string, error) {
	ids.mu.Lock()
	defer ids.mu.Unlock()
	switch kind {
	case ports.IDAttachedWorkerActionPlan:
		ids.plan++
		return fmtID("wap-test", ids.plan), nil
	case ports.IDAttachedWorkerAction:
		ids.action++
		return fmtID("wao-test", ids.action), nil
	default:
		return "", errors.New("unexpected ID kind")
	}
}

func fmtID(prefix string, value int) string {
	return prefix + "-" + string(rune('a'+value-1))
}

type actionWorkerStore struct {
	mu         sync.Mutex
	worker     domain.AttachedWorker
	connection domain.AttachedWorkerConnection
	err        error
}

func (store *actionWorkerStore) LoadAttachedWorkerConnection(_ context.Context, tenant domain.TenantID, owner domain.UserID, workerID domain.AttachedWorkerID) (domain.AttachedWorkerConnection, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.err != nil {
		return domain.AttachedWorkerConnection{}, false, store.err
	}
	if store.connection.TenantID != tenant || store.connection.OwnerUserID != owner || store.connection.WorkerID != workerID {
		return domain.AttachedWorkerConnection{}, false, nil
	}
	return store.connection, true, nil
}

func (store *actionWorkerStore) LoadAttachedWorker(_ context.Context, tenant domain.TenantID, owner domain.UserID, workerID domain.AttachedWorkerID) (domain.AttachedWorker, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.err != nil {
		return domain.AttachedWorker{}, false, store.err
	}
	if store.worker.TenantID != tenant || store.worker.OwnerUserID != owner || store.worker.ID != workerID {
		return domain.AttachedWorker{}, false, nil
	}
	return store.worker, true, nil
}

type actionMutator struct {
	workers *actionWorkerStore
}

func (mutator actionMutator) RequestDrain(_ context.Context, tenant domain.TenantID, owner domain.UserID, request attachedworkertransport.DrainRequest) (ports.AttachedWorkerDrainResult, error) {
	mutator.workers.mu.Lock()
	defer mutator.workers.mu.Unlock()
	worker := mutator.workers.worker
	if worker.TenantID != tenant || worker.OwnerUserID != owner || worker.ID != request.WorkerID || worker.Revision != request.ExpectedWorkerRevision {
		return ports.AttachedWorkerDrainResult{}, attachedworkertransport.ErrTransportConflict
	}
	worker.DesiredState = domain.AttachedWorkerDesiredDrain
	worker.ObservedState = domain.AttachedWorkerObservedDraining
	worker.Revision++
	worker.UpdatedAt = worker.UpdatedAt.Add(time.Microsecond)
	mutator.workers.worker = worker
	return ports.AttachedWorkerDrainResult{Status: ports.AttachedWorkerExecutionApplied, Worker: worker}, nil
}

func (mutator actionMutator) Revoke(_ context.Context, tenant domain.TenantID, owner domain.UserID, request attachedworker.WorkerRevisionRequest) (domain.AttachedWorker, error) {
	mutator.workers.mu.Lock()
	defer mutator.workers.mu.Unlock()
	worker := mutator.workers.worker
	if worker.TenantID != tenant || worker.OwnerUserID != owner || worker.ID != request.WorkerID || worker.Revision != request.ExpectedRevision {
		return domain.AttachedWorker{}, attachedworker.ErrWorkerConflict
	}
	worker.DesiredState = domain.AttachedWorkerDesiredRevoked
	worker.EnrollmentGeneration++
	worker.ConnectionGeneration++
	worker.Revision++
	worker.UpdatedAt = worker.UpdatedAt.Add(time.Microsecond)
	worker.RevokedAt = worker.UpdatedAt
	mutator.workers.worker = worker
	return worker, nil
}

type actionPlanStore struct {
	mu           sync.Mutex
	plans        map[domain.AttachedWorkerActionPlanID]domain.AttachedWorkerActionPlan
	completeFail bool
}

func newActionPlanStore() *actionPlanStore {
	return &actionPlanStore{plans: make(map[domain.AttachedWorkerActionPlanID]domain.AttachedWorkerActionPlan)}
}

func (store *actionPlanStore) CreateAttachedWorkerActionPlan(_ context.Context, plan domain.AttachedWorkerActionPlan) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, found := store.plans[plan.ID]; found {
		return errors.New("duplicate plan")
	}
	store.plans[plan.ID] = plan
	return nil
}

func (store *actionPlanStore) ClaimAttachedWorkerActionPlan(_ context.Context, claim ports.AttachedWorkerActionClaim) (ports.AttachedWorkerActionClaimResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	plan, found := store.plans[claim.PlanID]
	if !found || plan.TenantID != claim.TenantID || plan.OwnerUserID != claim.OwnerUserID || plan.WorkerID != claim.WorkerID {
		return ports.AttachedWorkerActionClaimResult{Status: ports.AttachedWorkerActionMissing}, nil
	}
	if plan.Action != claim.Action {
		return ports.AttachedWorkerActionClaimResult{Status: ports.AttachedWorkerActionConflict, Plan: plan}, nil
	}
	if plan.State != domain.AttachedWorkerActionPlanned {
		status := ports.AttachedWorkerActionConflict
		if plan.IdempotencyDigest == claim.IdempotencyDigest && plan.ConfirmationDigest == claim.ConfirmationDigest {
			status = ports.AttachedWorkerActionReplayed
		}
		return ports.AttachedWorkerActionClaimResult{Status: status, Plan: plan}, nil
	}
	if !claim.Now.Before(plan.ExpiresAt) {
		return ports.AttachedWorkerActionClaimResult{Status: ports.AttachedWorkerActionExpired, Plan: plan}, nil
	}
	if plan.ConfirmationDigest != claim.ConfirmationDigest {
		return ports.AttachedWorkerActionClaimResult{Status: ports.AttachedWorkerActionConflict, Plan: plan}, nil
	}
	plan.State, plan.OperationID, plan.IdempotencyDigest = domain.AttachedWorkerActionApplying, claim.OperationID, claim.IdempotencyDigest
	plan.Revision++
	store.plans[plan.ID] = plan
	return ports.AttachedWorkerActionClaimResult{Status: ports.AttachedWorkerActionClaimed, Plan: plan}, nil
}

func (store *actionPlanStore) CompleteAttachedWorkerAction(_ context.Context, plan domain.AttachedWorkerActionPlan) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.completeFail {
		store.completeFail = false
		return errors.New("simulated completion failure")
	}
	current, found := store.plans[plan.ID]
	if !found || current.OperationID != plan.OperationID || current.Revision+1 != plan.Revision {
		return errors.New("completion conflict")
	}
	store.plans[plan.ID] = plan
	return nil
}

func (store *actionPlanStore) LoadAttachedWorkerActionOperation(_ context.Context, tenant domain.TenantID, owner domain.UserID, operationID domain.AttachedWorkerActionOperationID) (domain.AttachedWorkerActionPlan, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, plan := range store.plans {
		if plan.TenantID == tenant && plan.OwnerUserID == owner && plan.OperationID == operationID {
			return plan, true, nil
		}
	}
	return domain.AttachedWorkerActionPlan{}, false, nil
}

func newActionFixture(t *testing.T) (*ControlService, *actionTestClock, *actionWorkerStore, *actionPlanStore) {
	t.Helper()
	clock := &actionTestClock{now: time.Date(2026, 9, 16, 8, 0, 0, 0, time.UTC)}
	worker := testWorker("wrk-control")
	worker.CreatedAt, worker.UpdatedAt = clock.now.Add(-time.Hour), clock.now.Add(-time.Minute)
	connection, _ := testCapability(t, worker, clock.now)
	workers := &actionWorkerStore{worker: worker, connection: connection}
	plans := newActionPlanStore()
	mutator := actionMutator{workers: workers}
	service, err := NewControlService(ControlConfig{Clock: clock, IDs: &actionTestIDs{}}, workers, plans, mutator, mutator)
	if err != nil {
		t.Fatal(err)
	}
	return service, clock, workers, plans
}

func TestControlServicePlanApplyDrainAndReplay(t *testing.T) {
	service, _, _, _ := newActionFixture(t)
	ctx := context.Background()
	plan, err := service.Plan(ctx, "tenant-1", "owner-1", "wrk-control", ActionPlanRequestV1{Version: 1, Action: ActionDrain})
	if err != nil {
		t.Fatal(err)
	}
	if plan.WorkerRevision != 4 || plan.EnrollmentGeneration != 2 || plan.ConnectionGeneration != 3 ||
		len(plan.Confirmation) != 64 || plan.RemoteAcknowledgement != "unknown" || plan.RemoteErase != "unknown" {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	request := ActionApplyV1{Version: 1, PlanID: plan.PlanID, Action: ActionDrain, Confirmation: plan.Confirmation, IdempotencyKey: "idem-drain"}
	operation, err := service.Apply(ctx, "tenant-1", "owner-1", "wrk-control", request)
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != "succeeded" || operation.Outcome != "applied" || operation.DurableDelivery != "recorded" ||
		operation.DesiredState != "drain" || operation.WorkerRevision != 5 ||
		operation.EnrollmentGeneration != 2 || operation.ConnectionGeneration != 3 || operation.RemoteAcknowledgement != "unknown" {
		t.Fatalf("unexpected operation: %+v", operation)
	}
	replay, err := service.Apply(ctx, "tenant-1", "owner-1", "wrk-control", request)
	if err != nil || replay.OperationID != operation.OperationID || replay.Outcome != "replayed" {
		t.Fatalf("replay = %+v, err = %v", replay, err)
	}
	loaded, err := service.Operation(ctx, "tenant-1", "owner-1", domain.AttachedWorkerActionOperationID(operation.OperationID))
	if err != nil || loaded.OperationID != replay.OperationID || loaded.Outcome != "applied" {
		t.Fatalf("loaded = %+v, err = %v", loaded, err)
	}
}

func TestControlServiceRevokeNeverClaimsRemoteErase(t *testing.T) {
	service, _, _, _ := newActionFixture(t)
	plan, err := service.Plan(context.Background(), "tenant-1", "owner-1", "wrk-control", ActionPlanRequestV1{Version: 1, Action: ActionRevoke})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := service.Apply(context.Background(), "tenant-1", "owner-1", "wrk-control", ActionApplyV1{
		Version: 1, PlanID: plan.PlanID, Action: ActionRevoke, Confirmation: plan.Confirmation, IdempotencyKey: "idem-revoke",
	})
	if err != nil {
		t.Fatal(err)
	}
	if operation.DesiredState != "revoked" || operation.Outcome != "applied" || operation.DurableDelivery != "not_applicable" ||
		operation.WorkerRevision != 5 || operation.EnrollmentGeneration != 3 ||
		operation.ConnectionGeneration != 4 || operation.RemoteErase != "unknown" {
		t.Fatalf("unexpected revoke receipt: %+v", operation)
	}
}

func TestControlServiceRejectsForeignExpiredStaleAndCompetingApply(t *testing.T) {
	service, clock, workers, planStore := newActionFixture(t)
	ctx := context.Background()
	if _, err := service.Plan(ctx, "tenant-1", "owner-2", "wrk-control", ActionPlanRequestV1{Version: 1, Action: ActionDrain}); !errors.Is(err, ErrActionNotFound) {
		t.Fatalf("foreign plan err = %v", err)
	}
	if _, err := service.Plan(ctx, "tenant-1", "owner-1", "wrk-missing", ActionPlanRequestV1{Version: 1, Action: ActionDrain}); !errors.Is(err, ErrActionNotFound) {
		t.Fatalf("missing plan err = %v", err)
	}
	expired, err := service.Plan(ctx, "tenant-1", "owner-1", "wrk-control", ActionPlanRequestV1{Version: 1, Action: ActionDrain})
	if err != nil {
		t.Fatal(err)
	}
	clock.Add(defaultActionPlanTTL)
	if _, err := service.Apply(ctx, "tenant-1", "owner-1", "wrk-control", ActionApplyV1{Version: 1, PlanID: expired.PlanID, Action: ActionDrain, Confirmation: expired.Confirmation, IdempotencyKey: "idem-expired"}); !errors.Is(err, ErrActionExpired) {
		t.Fatalf("expired apply err = %v", err)
	}
	clock.Add(-defaultActionPlanTTL)
	mismatch, err := service.Plan(ctx, "tenant-1", "owner-1", "wrk-control", ActionPlanRequestV1{Version: 1, Action: ActionDrain})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Apply(ctx, "tenant-1", "owner-1", "wrk-control", ActionApplyV1{Version: 1, PlanID: mismatch.PlanID, Action: ActionRevoke, Confirmation: mismatch.Confirmation, IdempotencyKey: "idem-mismatch"}); !errors.Is(err, ErrActionConflict) {
		t.Fatalf("action mismatch err = %v", err)
	}
	if _, err := service.Apply(ctx, "tenant-1", "owner-1", "wrk-control", ActionApplyV1{Version: 1, PlanID: mismatch.PlanID, Action: ActionDrain, Confirmation: strings.Repeat("0", 64), IdempotencyKey: "idem-digest"}); !errors.Is(err, ErrActionConflict) {
		t.Fatalf("digest mismatch err = %v", err)
	}
	stale, err := service.Plan(ctx, "tenant-1", "owner-1", "wrk-control", ActionPlanRequestV1{Version: 1, Action: ActionDrain})
	if err != nil {
		t.Fatal(err)
	}
	workers.mu.Lock()
	workers.worker.Revision++
	workers.worker.UpdatedAt = workers.worker.UpdatedAt.Add(time.Microsecond)
	workers.mu.Unlock()
	if _, err := service.Apply(ctx, "tenant-1", "owner-1", "wrk-control", ActionApplyV1{Version: 1, PlanID: stale.PlanID, Action: ActionDrain, Confirmation: stale.Confirmation, IdempotencyKey: "idem-stale"}); !errors.Is(err, ErrActionConflict) {
		t.Fatalf("stale apply err = %v", err)
	}
	planStore.mu.Lock()
	failed := planStore.plans[domain.AttachedWorkerActionPlanID(stale.PlanID)]
	planStore.mu.Unlock()
	failedReceipt, err := service.Operation(ctx, "tenant-1", "owner-1", failed.OperationID)
	if err != nil || failedReceipt.Outcome != "failed" || failedReceipt.DurableDelivery != "unknown" {
		t.Fatalf("failed receipt = %+v, err = %v", failedReceipt, err)
	}

	service, _, _, _ = newActionFixture(t)
	plan, err := service.Plan(ctx, "tenant-1", "owner-1", "wrk-control", ActionPlanRequestV1{Version: 1, Action: ActionDrain})
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	for _, key := range []string{"idem-winner-a", "idem-winner-b"} {
		go func(key string) {
			_, applyErr := service.Apply(ctx, "tenant-1", "owner-1", "wrk-control", ActionApplyV1{Version: 1, PlanID: plan.PlanID, Action: ActionDrain, Confirmation: plan.Confirmation, IdempotencyKey: key})
			errs <- applyErr
		}(key)
	}
	var success, conflict int
	for range 2 {
		err := <-errs
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrActionConflict):
			conflict++
		default:
			t.Fatalf("unexpected concurrent error: %v", err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("success=%d conflict=%d", success, conflict)
	}
}

func TestControlServiceDrainRequiresCurrentOnlineConnection(t *testing.T) {
	service, _, workers, _ := newActionFixture(t)
	workers.mu.Lock()
	workers.connection = domain.AttachedWorkerConnection{}
	workers.mu.Unlock()
	if _, err := service.Plan(context.Background(), "tenant-1", "owner-1", "wrk-control", ActionPlanRequestV1{Version: 1, Action: ActionDrain}); !errors.Is(err, ErrActionUnavailable) {
		t.Fatalf("plan err = %v, want unavailable", err)
	}
}

func TestControlServiceRejectsStaleGenerations(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*domain.AttachedWorker)
	}{
		{
			name: "enrollment generation",
			mutate: func(worker *domain.AttachedWorker) {
				worker.EnrollmentGeneration++
			},
		},
		{
			name: "connection generation",
			mutate: func(worker *domain.AttachedWorker) {
				worker.ConnectionGeneration++
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, _, workers, _ := newActionFixture(t)
			ctx := context.Background()
			plan, err := service.Plan(ctx, "tenant-1", "owner-1", "wrk-control", ActionPlanRequestV1{Version: 1, Action: ActionDrain})
			if err != nil {
				t.Fatal(err)
			}
			workers.mu.Lock()
			test.mutate(&workers.worker)
			workers.worker.UpdatedAt = workers.worker.UpdatedAt.Add(time.Microsecond)
			workers.mu.Unlock()
			_, err = service.Apply(ctx, "tenant-1", "owner-1", "wrk-control", ActionApplyV1{
				Version: 1, PlanID: plan.PlanID, Action: ActionDrain, Confirmation: plan.Confirmation,
				IdempotencyKey: "idem-stale-generation",
			})
			if !errors.Is(err, ErrActionConflict) {
				t.Fatalf("apply err = %v, want conflict", err)
			}
		})
	}
}

func TestControlServiceOperationHidesForeignOwner(t *testing.T) {
	service, _, _, _ := newActionFixture(t)
	ctx := context.Background()
	plan, err := service.Plan(ctx, "tenant-1", "owner-1", "wrk-control", ActionPlanRequestV1{Version: 1, Action: ActionDrain})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := service.Apply(ctx, "tenant-1", "owner-1", "wrk-control", ActionApplyV1{
		Version: 1, PlanID: plan.PlanID, Action: ActionDrain, Confirmation: plan.Confirmation, IdempotencyKey: "idem-owner-scope",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Operation(ctx, "tenant-1", "owner-2", domain.AttachedWorkerActionOperationID(operation.OperationID)); !errors.Is(err, ErrActionNotFound) {
		t.Fatalf("foreign operation err = %v, want not found", err)
	}
}

func TestControlServiceReconcilesMutationAfterCompletionFailure(t *testing.T) {
	service, _, _, plans := newActionFixture(t)
	plan, err := service.Plan(context.Background(), "tenant-1", "owner-1", "wrk-control", ActionPlanRequestV1{Version: 1, Action: ActionDrain})
	if err != nil {
		t.Fatal(err)
	}
	plans.completeFail = true
	request := ActionApplyV1{Version: 1, PlanID: plan.PlanID, Action: ActionDrain, Confirmation: plan.Confirmation, IdempotencyKey: "idem-reconcile"}
	if _, err := service.Apply(context.Background(), "tenant-1", "owner-1", "wrk-control", request); !errors.Is(err, ErrActionBackend) {
		t.Fatalf("first apply err = %v", err)
	}
	operation, err := service.Apply(context.Background(), "tenant-1", "owner-1", "wrk-control", request)
	if err != nil || operation.State != "succeeded" || operation.DesiredState != "drain" {
		t.Fatalf("reconciled operation = %+v, err = %v", operation, err)
	}
}
