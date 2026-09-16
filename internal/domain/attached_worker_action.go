package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

const AttachedWorkerActionPlanVersionV1 uint32 = 1

type (
	AttachedWorkerActionPlanID      string
	AttachedWorkerActionOperationID string
	AttachedWorkerAction            string
	AttachedWorkerActionState       string
)

const (
	AttachedWorkerActionDrain  AttachedWorkerAction = "drain"
	AttachedWorkerActionRevoke AttachedWorkerAction = "revoke"

	AttachedWorkerActionPlanned   AttachedWorkerActionState = "planned"
	AttachedWorkerActionApplying  AttachedWorkerActionState = "applying"
	AttachedWorkerActionSucceeded AttachedWorkerActionState = "succeeded"
	AttachedWorkerActionFailed    AttachedWorkerActionState = "failed"
)

func (id AttachedWorkerActionPlanID) Validate() error {
	return ValidateOpaqueID("attached_worker_action_plan_id", string(id))
}

func (id AttachedWorkerActionOperationID) Validate() error {
	return ValidateOpaqueID("attached_worker_action_operation_id", string(id))
}

func (action AttachedWorkerAction) Valid() bool {
	return action == AttachedWorkerActionDrain || action == AttachedWorkerActionRevoke
}

func (state AttachedWorkerActionState) Valid() bool {
	return state == AttachedWorkerActionPlanned || state == AttachedWorkerActionApplying || state == AttachedWorkerActionSucceeded || state == AttachedWorkerActionFailed
}

type AttachedWorkerActionPlan struct {
	Version              uint32                          `json:"version"`
	TenantID             TenantID                        `json:"tenant_id"`
	OwnerUserID          UserID                          `json:"owner_user_id"`
	ID                   AttachedWorkerActionPlanID      `json:"id"`
	OperationID          AttachedWorkerActionOperationID `json:"operation_id,omitempty"`
	WorkerID             AttachedWorkerID                `json:"worker_id"`
	Action               AttachedWorkerAction            `json:"action"`
	State                AttachedWorkerActionState       `json:"state"`
	WorkerRevision       uint64                          `json:"worker_revision"`
	EnrollmentGeneration uint64                          `json:"enrollment_generation"`
	ConnectionGeneration uint64                          `json:"connection_generation"`
	ConfirmationDigest   string                          `json:"confirmation_digest"`
	IdempotencyDigest    string                          `json:"idempotency_digest,omitempty"`
	FailureCode          string                          `json:"failure_code,omitempty"`
	Result               AttachedWorker                  `json:"result,omitempty"`
	CreatedAt            time.Time                       `json:"created_at"`
	ExpiresAt            time.Time                       `json:"expires_at"`
	CompletedAt          time.Time                       `json:"completed_at,omitempty"`
	Revision             uint64                          `json:"revision"`
}

func (plan AttachedWorkerActionPlan) Validate() error {
	if plan.Version != AttachedWorkerActionPlanVersionV1 || plan.TenantID.Validate() != nil || plan.OwnerUserID.Validate() != nil ||
		plan.ID.Validate() != nil || plan.WorkerID.Validate() != nil || !plan.Action.Valid() || !plan.State.Valid() ||
		plan.WorkerRevision == 0 || plan.EnrollmentGeneration == 0 || !validSHA256(plan.ConfirmationDigest) ||
		plan.CreatedAt.IsZero() || !plan.ExpiresAt.After(plan.CreatedAt) || plan.Revision == 0 {
		return ValidationError{Field: "attached_worker_action_plan", Reason: "contains invalid authority"}
	}
	if plan.State == AttachedWorkerActionPlanned {
		if plan.OperationID != "" || plan.IdempotencyDigest != "" || plan.FailureCode != "" || !plan.CompletedAt.IsZero() {
			return ValidationError{Field: "attached_worker_action_plan", Reason: "planned state must not contain operation authority"}
		}
		return nil
	}
	if plan.OperationID.Validate() != nil || !validSHA256(plan.IdempotencyDigest) {
		return ValidationError{Field: "attached_worker_action_plan", Reason: "applied state requires operation authority"}
	}
	if plan.State == AttachedWorkerActionSucceeded {
		if plan.Result.Validate() != nil || plan.Result.TenantID != plan.TenantID || plan.Result.OwnerUserID != plan.OwnerUserID ||
			plan.Result.ID != plan.WorkerID || plan.CompletedAt.IsZero() {
			return ValidationError{Field: "attached_worker_action_plan", Reason: "succeeded state requires a scoped result"}
		}
	}
	if plan.State == AttachedWorkerActionFailed && (plan.FailureCode != "conflict" || plan.CompletedAt.IsZero()) {
		return ValidationError{Field: "attached_worker_action_plan", Reason: "failed state requires a bounded failure"}
	}
	return nil
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}
