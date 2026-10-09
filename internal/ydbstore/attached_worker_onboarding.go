package ydbstore

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"math"

	"gitcode.com/urandon/sessionless/internal/attachedworkeronboarding"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// AttachedWorkerOnboardingAdapter adds current membership checks to AW-01's
// original transactions without changing legacy/internal callers' contracts.
type AttachedWorkerOnboardingAdapter struct{ *Store }

func (store *Store) OnboardingStore() *AttachedWorkerOnboardingAdapter {
	return &AttachedWorkerOnboardingAdapter{Store: store}
}

func (adapter *AttachedWorkerOnboardingAdapter) CreateAttachedWorkerEnrollment(ctx context.Context, enrollment domain.AttachedWorkerEnrollment, audit domain.AttachedWorkerAuditEvent) error {
	return adapter.Store.createAttachedWorkerEnrollment(ctx, enrollment, audit, true)
}

func (adapter *AttachedWorkerOnboardingAdapter) ClaimAttachedWorkerEnrollment(ctx context.Context, mutation ports.AttachedWorkerClaimMutation) (ports.AttachedWorkerClaimResult, error) {
	return adapter.Store.claimAttachedWorkerEnrollment(ctx, mutation, true)
}

func (adapter *AttachedWorkerOnboardingAdapter) CompareAndSwapAttachedWorker(ctx context.Context, mutation ports.AttachedWorkerCASMutation) (bool, error) {
	return adapter.Store.compareAndSwapAttachedWorker(ctx, mutation, true)
}

func (adapter *AttachedWorkerOnboardingAdapter) LoadAttachedWorker(ctx context.Context, tenant domain.TenantID, owner domain.UserID, workerID domain.AttachedWorkerID) (worker domain.AttachedWorker, found bool, err error) {
	if err := validateAttachedWorkerScope(tenant, owner, workerID); err != nil {
		return worker, false, err
	}
	err = adapter.Store.Transact(ctx, tenant, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, owner); err != nil {
			return err
		}
		worker, found, err = readAttachedWorkerTx(ctx, tx, owner, workerID)
		return err
	})
	return worker, found, err
}

func (adapter *AttachedWorkerOnboardingAdapter) RegisterAttachedComputeResource(ctx context.Context, request ports.AttachedComputeRegistration) (result ports.ComputeConnectionState, err error) {
	worker := request.Worker
	if err := worker.Validate(); err != nil {
		return result, err
	}
	if request.ResourceID != attachedworkeronboarding.ResourceIDForWorker(worker.TenantID, worker.OwnerUserID, worker.ID) ||
		request.ActorID != attachedworkeronboarding.ActorIDForOwner(worker.TenantID, worker.OwnerUserID) || worker.DesiredState != domain.AttachedWorkerDesiredActive {
		return result, ErrSubscriptionConnectionConflict
	}
	err = adapter.Store.Transact(ctx, worker.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, worker.OwnerUserID); err != nil {
			return err
		}
		current, found, err := readAttachedWorkerTx(ctx, tx, worker.OwnerUserID, worker.ID)
		if err != nil {
			return err
		}
		if !found || !sameOnboardingWorker(current, worker) {
			return ErrAttachedWorkerConflict
		}
		var actorOwner domain.UserID
		var frontend, external string
		actorErr := tx.sqlTx.QueryRowContext(ctx, `SELECT user_id, frontend, external_id FROM actors WHERE tenant_id=$1 AND actor_id=$2`, worker.TenantID, request.ActorID).Scan(&actorOwner, &frontend, &external)
		if actorErr != nil && !errors.Is(actorErr, sql.ErrNoRows) {
			return actorErr
		}
		if actorErr == nil && (actorOwner != worker.OwnerUserID || frontend != string(domain.FrontendWeb) || external != string(worker.OwnerUserID)) {
			return ErrSubscriptionConnectionProjectionConflict
		}
		rows, err := tx.sqlTx.QueryContext(ctx, `SELECT subscription_connection_id FROM subscription_connections_by_user WHERE tenant_id=$1 AND user_id=$2 ORDER BY subscription_connection_id ASC LIMIT 2`, worker.TenantID, worker.OwnerUserID)
		if err != nil {
			return err
		}
		projectionFound := false
		for rows.Next() {
			var resource domain.SubscriptionConnectionID
			if err := rows.Scan(&resource); err != nil {
				rows.Close()
				return err
			}
			if resource != request.ResourceID {
				rows.Close()
				return ErrSubscriptionConnectionConflict
			}
			projectionFound = true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		var existingActor domain.ActorID
		var provider, credential string
		connectionErr := tx.sqlTx.QueryRowContext(ctx, `SELECT actor_id, provider, credential_ref, entitlement_state, quota_state, observed_at FROM subscription_connections WHERE tenant_id=$1 AND subscription_connection_id=$2`, worker.TenantID, request.ResourceID).Scan(&existingActor, &provider, &credential, &result.Entitlement, &result.Quota, &result.ObservedAt)
		if connectionErr != nil && !errors.Is(connectionErr, sql.ErrNoRows) {
			return connectionErr
		}
		result.ID, result.Provider = request.ResourceID, "codex"
		if connectionErr == nil {
			if existingActor != request.ActorID || provider != "codex" || credential != "" || actorErr != nil {
				return ErrSubscriptionConnectionConflict
			}
			var projected domain.SubscriptionConnectionID
			if err := tx.sqlTx.QueryRowContext(ctx, `SELECT subscription_connection_id FROM subscription_connections_by_user WHERE tenant_id=$1 AND user_id=$2 AND subscription_connection_id=$3`, worker.TenantID, worker.OwnerUserID, request.ResourceID).Scan(&projected); err != nil {
				return ErrSubscriptionConnectionProjectionConflict
			}
			return validateComputeConnectionState(result)
		}
		if projectionFound {
			return ErrSubscriptionConnectionProjectionConflict
		}
		at, err := adapter.Store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		if errors.Is(actorErr, sql.ErrNoRows) {
			if _, err := tx.sqlTx.ExecContext(ctx, `INSERT INTO actors (tenant_id, actor_id, user_id, frontend, external_id, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$6)`, worker.TenantID, request.ActorID, worker.OwnerUserID, domain.FrontendWeb, string(worker.OwnerUserID), at); err != nil {
				return err
			}
		}
		result.Entitlement, result.Quota, result.ObservedAt = domain.EntitlementUnknown, domain.ProviderQuotaUnknown, at
		if _, err := tx.sqlTx.ExecContext(ctx, `INSERT INTO subscription_connections (tenant_id, subscription_connection_id, actor_id, provider, credential_ref, entitlement_state, quota_state, observed_at, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8,$8)`, worker.TenantID, request.ResourceID, request.ActorID, result.Provider, "", result.Entitlement, result.Quota, at); err != nil {
			return err
		}
		_, err = tx.sqlTx.ExecContext(ctx, `INSERT INTO subscription_connections_by_user (tenant_id,user_id,subscription_connection_id) VALUES ($1,$2,$3)`, worker.TenantID, worker.OwnerUserID, request.ResourceID)
		return err
	})
	return result, err
}

func sameOnboardingWorker(a, b domain.AttachedWorker) bool {
	return a.TenantID == b.TenantID && a.OwnerUserID == b.OwnerUserID && a.ID == b.ID && a.DisplayName == b.DisplayName &&
		bytes.Equal(a.IdentityPublicKey, b.IdentityPublicKey) && a.EnrollmentGeneration == b.EnrollmentGeneration &&
		a.ConnectionGeneration == b.ConnectionGeneration && a.Revision == b.Revision && a.DesiredState == b.DesiredState &&
		a.ObservedState == b.ObservedState && a.CreatedAt.Equal(b.CreatedAt) && a.UpdatedAt.Equal(b.UpdatedAt) && a.RevokedAt.Equal(b.RevokedAt)
}

// The resource precondition belongs to the rotation CAS transaction too: a
// preceding service read must not permit a concurrent provider activation to
// slip a disabled-onboarding identity mutation into an active resource.
func requireDisabledOnboardingResourceTx(ctx context.Context, tx *stateTx, worker domain.AttachedWorker) error {
	resource := attachedworkeronboarding.ResourceIDForWorker(worker.TenantID, worker.OwnerUserID, worker.ID)
	var actor domain.ActorID
	var provider, credential string
	var entitlement domain.EntitlementState
	var quota domain.ProviderQuotaState
	err := tx.sqlTx.QueryRowContext(ctx, `SELECT actor_id, provider, credential_ref, entitlement_state, quota_state FROM subscription_connections WHERE tenant_id=$1 AND subscription_connection_id=$2`, worker.TenantID, resource).Scan(&actor, &provider, &credential, &entitlement, &quota)
	if err != nil {
		return err
	}
	if actor != attachedworkeronboarding.ActorIDForOwner(worker.TenantID, worker.OwnerUserID) || provider != "codex" || credential != "" || entitlement != domain.EntitlementUnknown || quota != domain.ProviderQuotaUnknown {
		return ErrSubscriptionConnectionConflict
	}
	return nil
}

func (adapter *AttachedWorkerOnboardingAdapter) ReconcileAttachedWorkerRotation(ctx context.Context, prior domain.AttachedWorker, newKey []byte) (worker domain.AttachedWorker, applied bool, err error) {
	if prior.Validate() != nil || len(newKey) != ed25519.PublicKeySize || bytes.Equal(prior.IdentityPublicKey, newKey) || prior.Revision == math.MaxUint64 || prior.EnrollmentGeneration == math.MaxUint64 {
		return worker, false, ErrAttachedWorkerConflict
	}
	err = adapter.Store.Transact(ctx, prior.TenantID, func(state ports.StateTx) error {
		worker, applied = domain.AttachedWorker{}, false
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, prior.OwnerUserID); err != nil {
			return err
		}
		current, found, err := readAttachedWorkerTx(ctx, tx, prior.OwnerUserID, prior.ID)
		if err != nil || !found {
			return err
		}
		audit, found, err := readAttachedWorkerAuditEventTx(ctx, tx, prior.OwnerUserID, prior.ID, prior.Revision+1)
		if err != nil || !found {
			return err
		}
		expected := prior
		expected.IdentityPublicKey = append([]byte(nil), newKey...)
		expected.Revision++
		expected.EnrollmentGeneration++
		expected.UpdatedAt = current.UpdatedAt
		if !sameOnboardingWorker(current, expected) || audit.Action != domain.AttachedWorkerAuditIdentityRotated ||
			audit.EnrollmentGeneration != current.EnrollmentGeneration || audit.ConnectionGeneration != current.ConnectionGeneration || !audit.OccurredAt.Equal(current.UpdatedAt) {
			return nil
		}
		worker, applied = current, true
		return nil
	})
	return worker, applied, err
}

var _ ports.AttachedWorkerOnboardingStore = (*AttachedWorkerOnboardingAdapter)(nil)
