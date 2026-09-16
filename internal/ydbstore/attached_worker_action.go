package ydbstore

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var ErrAttachedWorkerActionConflict = errors.New("attached worker action plan conflicts with existing state")

func (store *Store) CreateAttachedWorkerActionPlan(ctx context.Context, plan domain.AttachedWorkerActionPlan) error {
	plan = canonicalAttachedWorkerActionPlan(plan)
	if err := plan.Validate(); err != nil || plan.State != domain.AttachedWorkerActionPlanned {
		if err != nil {
			return err
		}
		return ErrAttachedWorkerActionConflict
	}
	return store.Transact(ctx, plan.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		existing, found, err := readAttachedWorkerActionPlanTx(ctx, tx, plan.OwnerUserID, plan.ID)
		if err != nil {
			return err
		}
		if found {
			if sameAttachedWorkerActionPlan(existing, plan) {
				return nil
			}
			return ErrAttachedWorkerActionConflict
		}
		payload, err := json.Marshal(plan)
		if err != nil {
			return err
		}
		_, err = tx.sqlTx.ExecContext(ctx,
			`INSERT INTO attached_worker_action_plans
			 (tenant_id,owner_user_id,plan_id,worker_id,operation_id,state,expires_at,retention_expire_at,record)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS JsonDocument))`,
			plan.TenantID, plan.OwnerUserID, plan.ID, plan.WorkerID, "", plan.State, plan.ExpiresAt,
			plan.ExpiresAt.Add(store.operationalRetention), string(payload),
		)
		return err
	})
}

func (store *Store) ClaimAttachedWorkerActionPlan(ctx context.Context, claim ports.AttachedWorkerActionClaim) (result ports.AttachedWorkerActionClaimResult, err error) {
	claim.Now = canonicalAttachedWorkerTime(claim.Now)
	if claim.TenantID.Validate() != nil || claim.OwnerUserID.Validate() != nil || claim.WorkerID.Validate() != nil ||
		claim.PlanID.Validate() != nil || claim.OperationID.Validate() != nil || !validActionDigest(claim.ConfirmationDigest) ||
		!claim.Action.Valid() || !validActionDigest(claim.IdempotencyDigest) || claim.Now.IsZero() {
		return result, domain.ValidationError{Field: "attached_worker_action_claim", Reason: "contains invalid authority"}
	}
	err = store.Transact(ctx, claim.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		plan, found, err := readAttachedWorkerActionPlanTx(ctx, tx, claim.OwnerUserID, claim.PlanID)
		if err != nil {
			return err
		}
		if !found || plan.WorkerID != claim.WorkerID {
			result.Status = ports.AttachedWorkerActionMissing
			return nil
		}
		result.Plan = plan
		if plan.Action != claim.Action {
			result.Status = ports.AttachedWorkerActionConflict
			return nil
		}
		if plan.State != domain.AttachedWorkerActionPlanned {
			if plan.OperationID != "" && plan.IdempotencyDigest == claim.IdempotencyDigest &&
				subtle.ConstantTimeCompare([]byte(plan.ConfirmationDigest), []byte(claim.ConfirmationDigest)) == 1 {
				result.Status = ports.AttachedWorkerActionReplayed
				return nil
			}
			result.Status = ports.AttachedWorkerActionConflict
			return nil
		}
		if !claim.Now.Before(plan.ExpiresAt) {
			result.Status = ports.AttachedWorkerActionExpired
			return nil
		}
		if subtle.ConstantTimeCompare([]byte(plan.ConfirmationDigest), []byte(claim.ConfirmationDigest)) != 1 {
			result.Status = ports.AttachedWorkerActionConflict
			return nil
		}
		plan.State = domain.AttachedWorkerActionApplying
		plan.OperationID = claim.OperationID
		plan.IdempotencyDigest = claim.IdempotencyDigest
		plan.Revision++
		if err := plan.Validate(); err != nil {
			return err
		}
		if err := writeAttachedWorkerActionPlanTx(ctx, tx, plan, store.operationalRetention, true); err != nil {
			return err
		}
		result.Status, result.Plan = ports.AttachedWorkerActionClaimed, plan
		return nil
	})
	return result, err
}

func (store *Store) CompleteAttachedWorkerAction(ctx context.Context, plan domain.AttachedWorkerActionPlan) (completed domain.AttachedWorkerActionPlan, err error) {
	plan = canonicalAttachedWorkerActionPlan(plan)
	if err := plan.Validate(); err != nil || (plan.State != domain.AttachedWorkerActionSucceeded && plan.State != domain.AttachedWorkerActionFailed) {
		if err != nil {
			return completed, err
		}
		return completed, ErrAttachedWorkerActionConflict
	}
	err = store.Transact(ctx, plan.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		current, found, err := readAttachedWorkerActionPlanTx(ctx, tx, plan.OwnerUserID, plan.ID)
		if err != nil {
			return err
		}
		if !found {
			return ErrAttachedWorkerActionConflict
		}
		if current.State == domain.AttachedWorkerActionSucceeded || current.State == domain.AttachedWorkerActionFailed {
			if sameAttachedWorkerActionTerminal(current, plan) {
				completed = current
				return nil
			}
			return ErrAttachedWorkerActionConflict
		}
		if current.State != domain.AttachedWorkerActionApplying || current.OperationID != plan.OperationID ||
			current.IdempotencyDigest != plan.IdempotencyDigest || plan.Revision != current.Revision+1 {
			return ErrAttachedWorkerActionConflict
		}
		if err := writeAttachedWorkerActionPlanTx(ctx, tx, plan, store.operationalRetention, false); err != nil {
			return err
		}
		completed = plan
		return nil
	})
	return completed, err
}

func (store *Store) LoadAttachedWorkerActionOperation(ctx context.Context, tenantID domain.TenantID, ownerUserID domain.UserID, operationID domain.AttachedWorkerActionOperationID) (plan domain.AttachedWorkerActionPlan, found bool, err error) {
	if tenantID.Validate() != nil || ownerUserID.Validate() != nil || operationID.Validate() != nil {
		return plan, false, domain.ValidationError{Field: "attached_worker_action_operation", Reason: "contains invalid scope"}
	}
	var payload string
	err = store.db.QueryRowContext(ctx,
		`SELECT record FROM attached_worker_action_operations
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND operation_id=$3`,
		tenantID, ownerUserID, operationID,
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return plan, false, nil
	}
	if err != nil {
		return plan, false, err
	}
	if err := json.Unmarshal([]byte(payload), &plan); err != nil {
		return plan, false, err
	}
	plan = canonicalAttachedWorkerActionPlan(plan)
	if plan.Validate() != nil || plan.TenantID != tenantID || plan.OwnerUserID != ownerUserID || plan.OperationID != operationID {
		return domain.AttachedWorkerActionPlan{}, false, ErrAttachedWorkerActionConflict
	}
	return plan, true, nil
}

func readAttachedWorkerActionPlanTx(ctx context.Context, tx *stateTx, ownerUserID domain.UserID, planID domain.AttachedWorkerActionPlanID) (plan domain.AttachedWorkerActionPlan, found bool, err error) {
	var payload string
	err = tx.sqlTx.QueryRowContext(ctx,
		`SELECT record FROM attached_worker_action_plans
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND plan_id=$3`,
		tx.tenantID, ownerUserID, planID,
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return plan, false, nil
	}
	if err != nil {
		return plan, false, err
	}
	if err := json.Unmarshal([]byte(payload), &plan); err != nil {
		return plan, false, err
	}
	plan = canonicalAttachedWorkerActionPlan(plan)
	if plan.Validate() != nil || plan.TenantID != tx.tenantID || plan.OwnerUserID != ownerUserID || plan.ID != planID {
		return domain.AttachedWorkerActionPlan{}, false, ErrAttachedWorkerActionConflict
	}
	return plan, true, nil
}

func writeAttachedWorkerActionPlanTx(ctx context.Context, tx *stateTx, plan domain.AttachedWorkerActionPlan, retention time.Duration, insertOperation bool) error {
	payload, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	retentionExpireAt := plan.ExpiresAt.Add(retention)
	if !plan.CompletedAt.IsZero() {
		retentionExpireAt = plan.CompletedAt.Add(retention)
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`UPDATE attached_worker_action_plans
		 SET operation_id=$1,state=$2,retention_expire_at=$3,record=CAST($4 AS JsonDocument)
		 WHERE tenant_id=$5 AND owner_user_id=$6 AND plan_id=$7`,
		plan.OperationID, plan.State, retentionExpireAt, string(payload), plan.TenantID, plan.OwnerUserID, plan.ID,
	); err != nil {
		return err
	}
	verb := "INSERT"
	if !insertOperation {
		verb = "UPSERT"
	}
	_, err = tx.sqlTx.ExecContext(ctx, verb+` INTO attached_worker_action_operations
		 (tenant_id,owner_user_id,operation_id,plan_id,retention_expire_at,record)
		 VALUES ($1,$2,$3,$4,$5,CAST($6 AS JsonDocument))`,
		plan.TenantID, plan.OwnerUserID, plan.OperationID, plan.ID, retentionExpireAt, string(payload),
	)
	return err
}

func canonicalAttachedWorkerActionPlan(plan domain.AttachedWorkerActionPlan) domain.AttachedWorkerActionPlan {
	plan.CreatedAt = canonicalAttachedWorkerTime(plan.CreatedAt)
	plan.ExpiresAt = canonicalAttachedWorkerTime(plan.ExpiresAt)
	plan.CompletedAt = canonicalAttachedWorkerTime(plan.CompletedAt)
	plan.Result = canonicalAttachedWorker(plan.Result)
	return plan
}

func sameAttachedWorkerActionPlan(left, right domain.AttachedWorkerActionPlan) bool {
	left, right = canonicalAttachedWorkerActionPlan(left), canonicalAttachedWorkerActionPlan(right)
	return left.Version == right.Version && left.TenantID == right.TenantID && left.OwnerUserID == right.OwnerUserID &&
		left.ID == right.ID && left.OperationID == right.OperationID && left.WorkerID == right.WorkerID &&
		left.Action == right.Action && left.State == right.State && left.WorkerRevision == right.WorkerRevision &&
		left.EnrollmentGeneration == right.EnrollmentGeneration && left.ConnectionGeneration == right.ConnectionGeneration &&
		left.ConfirmationDigest == right.ConfirmationDigest && left.IdempotencyDigest == right.IdempotencyDigest && left.FailureCode == right.FailureCode &&
		sameAttachedWorker(left.Result, right.Result) && left.CreatedAt.Equal(right.CreatedAt) && left.ExpiresAt.Equal(right.ExpiresAt) &&
		left.CompletedAt.Equal(right.CompletedAt) && left.Revision == right.Revision
}

func sameAttachedWorkerActionTerminal(left, right domain.AttachedWorkerActionPlan) bool {
	left, right = canonicalAttachedWorkerActionPlan(left), canonicalAttachedWorkerActionPlan(right)
	left.CompletedAt, right.CompletedAt = time.Time{}, time.Time{}
	return sameAttachedWorkerActionPlan(left, right)
}

func validActionDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}
