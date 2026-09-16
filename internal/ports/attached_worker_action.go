package ports

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type AttachedWorkerActionClaimStatus string

const (
	AttachedWorkerActionClaimed  AttachedWorkerActionClaimStatus = "claimed"
	AttachedWorkerActionReplayed AttachedWorkerActionClaimStatus = "replayed"
	AttachedWorkerActionMissing  AttachedWorkerActionClaimStatus = "missing"
	AttachedWorkerActionExpired  AttachedWorkerActionClaimStatus = "expired"
	AttachedWorkerActionConflict AttachedWorkerActionClaimStatus = "conflict"
)

type AttachedWorkerActionClaim struct {
	TenantID           domain.TenantID
	OwnerUserID        domain.UserID
	WorkerID           domain.AttachedWorkerID
	PlanID             domain.AttachedWorkerActionPlanID
	OperationID        domain.AttachedWorkerActionOperationID
	Action             domain.AttachedWorkerAction
	ConfirmationDigest string
	IdempotencyDigest  string
	Now                time.Time
}

type AttachedWorkerActionClaimResult struct {
	Status AttachedWorkerActionClaimStatus
	Plan   domain.AttachedWorkerActionPlan
}

type AttachedWorkerActionPlanStore interface {
	CreateAttachedWorkerActionPlan(context.Context, domain.AttachedWorkerActionPlan) error
	ClaimAttachedWorkerActionPlan(context.Context, AttachedWorkerActionClaim) (AttachedWorkerActionClaimResult, error)
	CompleteAttachedWorkerAction(context.Context, domain.AttachedWorkerActionPlan) error
	LoadAttachedWorkerActionOperation(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerActionOperationID) (domain.AttachedWorkerActionPlan, bool, error)
}
