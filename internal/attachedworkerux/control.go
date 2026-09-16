package attachedworkerux

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworker"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrActionInvalid     = errors.New("attached worker action request is invalid")
	ErrActionNotFound    = errors.New("attached worker action was not found")
	ErrActionUnavailable = errors.New("attached worker action is unavailable")
	ErrActionConflict    = errors.New("attached worker action conflicts with current state")
	ErrActionExpired     = errors.New("attached worker action plan expired")
	ErrActionBackend     = errors.New("attached worker action backend is unavailable")
)

const defaultActionPlanTTL = 5 * time.Minute

type ControlConfig struct {
	Clock   ports.Clock
	IDs     ports.IDGenerator
	PlanTTL time.Duration
}

type attachedWorkerReader interface {
	LoadAttachedWorker(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorker, bool, error)
	LoadAttachedWorkerConnection(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorkerConnection, bool, error)
}

type attachedWorkerDrainer interface {
	RequestDrain(context.Context, domain.TenantID, domain.UserID, attachedworkertransport.DrainRequest) (ports.AttachedWorkerDrainResult, error)
}

type attachedWorkerRevoker interface {
	Revoke(context.Context, domain.TenantID, domain.UserID, attachedworker.WorkerRevisionRequest) (domain.AttachedWorker, error)
}

type ControlService struct {
	clock   ports.Clock
	ids     ports.IDGenerator
	ttl     time.Duration
	workers attachedWorkerReader
	plans   ports.AttachedWorkerActionPlanStore
	drainer attachedWorkerDrainer
	revoker attachedWorkerRevoker
}

func NewControlService(config ControlConfig, workers attachedWorkerReader, plans ports.AttachedWorkerActionPlanStore, drainer attachedWorkerDrainer, revoker attachedWorkerRevoker) (*ControlService, error) {
	if config.Clock == nil || config.IDs == nil || workers == nil || plans == nil || drainer == nil || revoker == nil {
		return nil, ErrActionInvalid
	}
	if config.PlanTTL == 0 {
		config.PlanTTL = defaultActionPlanTTL
	}
	if config.PlanTTL <= 0 || config.PlanTTL > defaultActionPlanTTL {
		return nil, ErrActionInvalid
	}
	return &ControlService{clock: config.Clock, ids: config.IDs, ttl: config.PlanTTL, workers: workers, plans: plans, drainer: drainer, revoker: revoker}, nil
}

func (service *ControlService) Plan(ctx context.Context, tenantID domain.TenantID, ownerUserID domain.UserID, workerID domain.AttachedWorkerID, request ActionPlanRequestV1) (ActionPlanV1, error) {
	if ctx == nil || tenantID.Validate() != nil || ownerUserID.Validate() != nil || workerID.Validate() != nil || request.Validate() != nil {
		return ActionPlanV1{}, ErrActionInvalid
	}
	worker, found, err := service.workers.LoadAttachedWorker(ctx, tenantID, ownerUserID, workerID)
	if err != nil {
		return ActionPlanV1{}, ErrActionBackend
	}
	if !found {
		return ActionPlanV1{}, ErrActionNotFound
	}
	if !workerMatchesScope(worker, tenantID, ownerUserID, workerID) {
		return ActionPlanV1{}, ErrActionBackend
	}
	connection, connectionFound, err := service.workers.LoadAttachedWorkerConnection(ctx, tenantID, ownerUserID, workerID)
	if err != nil {
		return ActionPlanV1{}, ErrActionBackend
	}
	action := domain.AttachedWorkerAction(request.Action)
	if !actionAvailable(action, worker, connection, connectionFound) {
		return ActionPlanV1{}, ErrActionUnavailable
	}
	value, err := service.ids.NewID(ctx, ports.IDAttachedWorkerActionPlan)
	if err != nil {
		return ActionPlanV1{}, ErrActionBackend
	}
	now := canonicalNow(service.clock.Now())
	plan := domain.AttachedWorkerActionPlan{
		Version: domain.AttachedWorkerActionPlanVersionV1, TenantID: tenantID, OwnerUserID: ownerUserID,
		ID: domain.AttachedWorkerActionPlanID(value), WorkerID: workerID, Action: action,
		State: domain.AttachedWorkerActionPlanned, WorkerRevision: worker.Revision,
		EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: worker.ConnectionGeneration,
		CreatedAt: now, ExpiresAt: now.Add(service.ttl), Revision: 1,
	}
	plan.ConfirmationDigest = actionConfirmation(plan)
	if err := plan.Validate(); err != nil {
		return ActionPlanV1{}, ErrActionBackend
	}
	if err := service.plans.CreateAttachedWorkerActionPlan(ctx, plan); err != nil {
		return ActionPlanV1{}, ErrActionBackend
	}
	return planView(plan), nil
}

func (service *ControlService) Apply(ctx context.Context, tenantID domain.TenantID, ownerUserID domain.UserID, workerID domain.AttachedWorkerID, request ActionApplyV1) (ActionOperationV1, error) {
	if ctx == nil || tenantID.Validate() != nil || ownerUserID.Validate() != nil || workerID.Validate() != nil || request.Validate() != nil {
		return ActionOperationV1{}, ErrActionInvalid
	}
	operationValue, err := service.ids.NewID(ctx, ports.IDAttachedWorkerAction)
	if err != nil {
		return ActionOperationV1{}, ErrActionBackend
	}
	claim, err := service.plans.ClaimAttachedWorkerActionPlan(ctx, ports.AttachedWorkerActionClaim{
		TenantID: tenantID, OwnerUserID: ownerUserID, WorkerID: workerID,
		PlanID: domain.AttachedWorkerActionPlanID(request.PlanID), OperationID: domain.AttachedWorkerActionOperationID(operationValue),
		Action: domain.AttachedWorkerAction(request.Action), ConfirmationDigest: request.Confirmation,
		IdempotencyDigest: digestText(request.IdempotencyKey), Now: service.clock.Now(),
	})
	if err != nil {
		return ActionOperationV1{}, ErrActionBackend
	}
	switch claim.Status {
	case ports.AttachedWorkerActionMissing:
		return ActionOperationV1{}, ErrActionNotFound
	case ports.AttachedWorkerActionExpired:
		return ActionOperationV1{}, ErrActionExpired
	case ports.AttachedWorkerActionConflict:
		return ActionOperationV1{}, ErrActionConflict
	case ports.AttachedWorkerActionReplayed:
		if claim.Plan.State == domain.AttachedWorkerActionSucceeded {
			return operationView(claim.Plan, "replayed"), nil
		}
		if claim.Plan.State == domain.AttachedWorkerActionFailed {
			return ActionOperationV1{}, ErrActionConflict
		}
	case ports.AttachedWorkerActionClaimed:
	default:
		return ActionOperationV1{}, ErrActionBackend
	}
	return service.execute(ctx, claim.Plan)
}

func (service *ControlService) Operation(ctx context.Context, tenantID domain.TenantID, ownerUserID domain.UserID, operationID domain.AttachedWorkerActionOperationID) (ActionOperationV1, error) {
	if ctx == nil || tenantID.Validate() != nil || ownerUserID.Validate() != nil || operationID.Validate() != nil {
		return ActionOperationV1{}, ErrActionInvalid
	}
	plan, found, err := service.plans.LoadAttachedWorkerActionOperation(ctx, tenantID, ownerUserID, operationID)
	if err != nil {
		return ActionOperationV1{}, ErrActionBackend
	}
	if !found {
		return ActionOperationV1{}, ErrActionNotFound
	}
	return operationView(plan, operationOutcome(plan)), nil
}

func (service *ControlService) execute(ctx context.Context, plan domain.AttachedWorkerActionPlan) (ActionOperationV1, error) {
	worker, found, err := service.workers.LoadAttachedWorker(ctx, plan.TenantID, plan.OwnerUserID, plan.WorkerID)
	if err != nil {
		return ActionOperationV1{}, ErrActionBackend
	}
	if !found {
		return service.fail(ctx, plan)
	}
	if !workerMatchesScope(worker, plan.TenantID, plan.OwnerUserID, plan.WorkerID) {
		return ActionOperationV1{}, ErrActionBackend
	}
	if actionResultMatches(plan, worker) {
		return service.succeed(ctx, plan, worker)
	}
	exactPreview := worker.Revision == plan.WorkerRevision && worker.EnrollmentGeneration == plan.EnrollmentGeneration &&
		worker.ConnectionGeneration == plan.ConnectionGeneration
	if worker.Revision == plan.WorkerRevision && !exactPreview {
		return service.fail(ctx, plan)
	}
	if exactPreview {
		connection, connectionFound, loadErr := service.workers.LoadAttachedWorkerConnection(ctx, plan.TenantID, plan.OwnerUserID, plan.WorkerID)
		if loadErr != nil {
			return ActionOperationV1{}, ErrActionBackend
		}
		if !actionAvailable(plan.Action, worker, connection, connectionFound) {
			return service.fail(ctx, plan)
		}
	} else if plan.Action != domain.AttachedWorkerActionDrain {
		return service.fail(ctx, plan)
	}
	var result domain.AttachedWorker
	switch plan.Action {
	case domain.AttachedWorkerActionDrain:
		drain, applyErr := service.drainer.RequestDrain(ctx, plan.TenantID, plan.OwnerUserID, attachedworkertransport.DrainRequest{
			WorkerID: plan.WorkerID, ExpectedWorkerRevision: plan.WorkerRevision,
		})
		if applyErr != nil {
			if errors.Is(applyErr, attachedworkertransport.ErrTransportConflict) || errors.Is(applyErr, attachedworkertransport.ErrTransportUnauthorized) {
				return service.fail(ctx, plan)
			}
			return ActionOperationV1{}, ErrActionBackend
		}
		result = drain.Worker
		if !drainResultMatches(plan, result) {
			return ActionOperationV1{}, ErrActionBackend
		}
	case domain.AttachedWorkerActionRevoke:
		result, err = service.revoker.Revoke(ctx, plan.TenantID, plan.OwnerUserID, attachedworker.WorkerRevisionRequest{
			WorkerID: plan.WorkerID, ExpectedRevision: plan.WorkerRevision,
		})
		if err != nil {
			if errors.Is(err, attachedworker.ErrWorkerConflict) || errors.Is(err, attachedworker.ErrWorkerNotFound) || errors.Is(err, attachedworker.ErrWorkerRevoked) {
				current, currentFound, loadErr := service.workers.LoadAttachedWorker(ctx, plan.TenantID, plan.OwnerUserID, plan.WorkerID)
				if loadErr != nil {
					return ActionOperationV1{}, ErrActionBackend
				}
				if currentFound && actionResultMatches(plan, current) {
					return service.succeed(ctx, plan, current)
				}
				return service.fail(ctx, plan)
			}
			return ActionOperationV1{}, ErrActionBackend
		}
	default:
		return ActionOperationV1{}, ErrActionInvalid
	}
	if plan.Action != domain.AttachedWorkerActionDrain && !actionResultMatches(plan, result) {
		return ActionOperationV1{}, ErrActionBackend
	}
	return service.succeed(ctx, plan, result)
}

func (service *ControlService) succeed(ctx context.Context, plan domain.AttachedWorkerActionPlan, worker domain.AttachedWorker) (ActionOperationV1, error) {
	plan.State, plan.Result, plan.FailureCode = domain.AttachedWorkerActionSucceeded, worker, ""
	plan.CompletedAt, plan.Revision = canonicalNow(service.clock.Now()), plan.Revision+1
	if err := service.plans.CompleteAttachedWorkerAction(ctx, plan); err != nil {
		return ActionOperationV1{}, ErrActionBackend
	}
	return operationView(plan, "applied"), nil
}

func (service *ControlService) fail(ctx context.Context, plan domain.AttachedWorkerActionPlan) (ActionOperationV1, error) {
	plan.State, plan.FailureCode = domain.AttachedWorkerActionFailed, "conflict"
	plan.CompletedAt, plan.Revision = canonicalNow(service.clock.Now()), plan.Revision+1
	if err := service.plans.CompleteAttachedWorkerAction(ctx, plan); err != nil {
		return ActionOperationV1{}, ErrActionBackend
	}
	return ActionOperationV1{}, ErrActionConflict
}

func actionAvailable(action domain.AttachedWorkerAction, worker domain.AttachedWorker, connection domain.AttachedWorkerConnection, connectionFound bool) bool {
	switch action {
	case domain.AttachedWorkerActionDrain:
		return worker.DesiredState == domain.AttachedWorkerDesiredActive && worker.ObservedState == domain.AttachedWorkerObservedOnline &&
			connectionFound && connection.Validate() == nil && connection.TenantID == worker.TenantID &&
			connection.OwnerUserID == worker.OwnerUserID && connection.WorkerID == worker.ID &&
			connection.EnrollmentGeneration == worker.EnrollmentGeneration && connection.ConnectionGeneration == worker.ConnectionGeneration &&
			connection.State == domain.AttachedWorkerConnectionOnline
	case domain.AttachedWorkerActionRevoke:
		return worker.DesiredState != domain.AttachedWorkerDesiredRevoked
	default:
		return false
	}
}

func actionResultMatches(plan domain.AttachedWorkerActionPlan, worker domain.AttachedWorker) bool {
	if !workerMatchesScope(worker, plan.TenantID, plan.OwnerUserID, plan.WorkerID) || worker.Revision != plan.WorkerRevision+1 {
		return false
	}
	switch plan.Action {
	case domain.AttachedWorkerActionDrain:
		return worker.EnrollmentGeneration == plan.EnrollmentGeneration && worker.ConnectionGeneration == plan.ConnectionGeneration &&
			worker.DesiredState == domain.AttachedWorkerDesiredDrain
	case domain.AttachedWorkerActionRevoke:
		return worker.EnrollmentGeneration == plan.EnrollmentGeneration+1 && worker.ConnectionGeneration == plan.ConnectionGeneration+1 &&
			worker.DesiredState == domain.AttachedWorkerDesiredRevoked
	default:
		return false
	}
}

func drainResultMatches(plan domain.AttachedWorkerActionPlan, worker domain.AttachedWorker) bool {
	return workerMatchesScope(worker, plan.TenantID, plan.OwnerUserID, plan.WorkerID) &&
		worker.Revision >= plan.WorkerRevision+1 && worker.EnrollmentGeneration == plan.EnrollmentGeneration &&
		worker.ConnectionGeneration == plan.ConnectionGeneration && worker.DesiredState == domain.AttachedWorkerDesiredDrain
}

func workerMatchesScope(worker domain.AttachedWorker, tenantID domain.TenantID, ownerUserID domain.UserID, workerID domain.AttachedWorkerID) bool {
	return worker.Validate() == nil && worker.TenantID == tenantID && worker.OwnerUserID == ownerUserID && worker.ID == workerID
}

func actionConfirmation(plan domain.AttachedWorkerActionPlan) string {
	return digestText(fmt.Sprintf("v1|%s|%s|%s|%s|%d|%d|%d|%d", plan.TenantID, plan.OwnerUserID, plan.ID, plan.Action,
		plan.WorkerRevision, plan.EnrollmentGeneration, plan.ConnectionGeneration, plan.ExpiresAt.UnixMicro()))
}

func digestText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func planView(plan domain.AttachedWorkerActionPlan) ActionPlanV1 {
	consequences := []string{"admission_closed", "remote_acknowledgement_unknown"}
	if plan.Action == domain.AttachedWorkerActionRevoke {
		consequences = []string{"credentials_invalidated", "remote_acknowledgement_unknown", "remote_erase_unknown"}
	}
	return ActionPlanV1{
		Version: ReadModelVersionV1, PlanID: string(plan.ID), WorkerID: string(plan.WorkerID), Action: ActionCodeV1(plan.Action),
		WorkerRevision: plan.WorkerRevision, EnrollmentGeneration: plan.EnrollmentGeneration, ConnectionGeneration: plan.ConnectionGeneration,
		Consequences: consequences, RemoteAcknowledgement: "unknown", RemoteErase: "unknown", ExpiresAt: plan.ExpiresAt,
		Confirmation: plan.ConfirmationDigest,
	}
}

func operationView(plan domain.AttachedWorkerActionPlan, outcome string) ActionOperationV1 {
	delivery := "unknown"
	if plan.Action == domain.AttachedWorkerActionRevoke {
		delivery = "not_applicable"
	} else if plan.Action == domain.AttachedWorkerActionDrain && plan.State == domain.AttachedWorkerActionSucceeded {
		delivery = "recorded"
	}
	result := ActionOperationV1{
		Version: ReadModelVersionV1, OperationID: string(plan.OperationID), WorkerID: string(plan.WorkerID),
		Action: ActionCodeV1(plan.Action), State: string(plan.State), Outcome: outcome, DurableDelivery: delivery,
		CreatedAt: plan.CreatedAt, CompletedAt: plan.CompletedAt,
		RemoteAcknowledgement: "unknown", RemoteErase: "unknown",
	}
	if plan.State == domain.AttachedWorkerActionFailed {
		result.ReasonCode = ActionUnavailableStaleRevision
	}
	if plan.State == domain.AttachedWorkerActionSucceeded {
		result.WorkerRevision = plan.Result.Revision
		result.EnrollmentGeneration = plan.Result.EnrollmentGeneration
		result.ConnectionGeneration = plan.Result.ConnectionGeneration
		result.DesiredState = string(plan.Result.DesiredState)
	}
	return result
}

func operationOutcome(plan domain.AttachedWorkerActionPlan) string {
	if plan.State == domain.AttachedWorkerActionSucceeded {
		return "applied"
	}
	if plan.State == domain.AttachedWorkerActionFailed {
		return "failed"
	}
	return "in_progress"
}
