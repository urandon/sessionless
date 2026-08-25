package ydbstore

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	maxAttachedWorkerListLimit      uint64 = 100
	maxAttachedWorkerAuditListLimit uint64 = 100
)

var (
	ErrAttachedWorkerEnrollmentConflict = errors.New("attached worker enrollment conflicts with existing state")
	ErrAttachedWorkerConflict           = errors.New("attached worker conflicts with existing state")
	ErrAttachedWorkerAuditConflict      = errors.New("attached worker audit conflicts with existing state")
)

func (store *Store) CreateAttachedWorkerEnrollment(
	ctx context.Context,
	enrollment domain.AttachedWorkerEnrollment,
	audit domain.AttachedWorkerAuditEvent,
) error {
	if err := validateAttachedWorkerEnrollmentCreate(enrollment, audit); err != nil {
		return err
	}
	return store.Transact(ctx, enrollment.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		existing, found, err := readAttachedWorkerEnrollmentTx(ctx, tx, enrollment.OwnerUserID, enrollment.ID)
		if err != nil {
			return err
		}
		if found {
			if !sameAttachedWorkerEnrollment(existing, enrollment) {
				return ErrAttachedWorkerEnrollmentConflict
			}
			existingAudit, auditFound, err := readAttachedWorkerAuditEventTx(
				ctx, tx, audit.OwnerUserID, audit.WorkerID, audit.WorkerRevision,
			)
			if err != nil {
				return err
			}
			if !auditFound || !sameAttachedWorkerAuditEvent(existingAudit, audit) {
				return ErrAttachedWorkerAuditConflict
			}
			return nil
		}
		if _, workerFound, err := readAttachedWorkerTx(ctx, tx, enrollment.OwnerUserID, enrollment.WorkerID); err != nil {
			return err
		} else if workerFound {
			return ErrAttachedWorkerConflict
		}
		if err := insertAttachedWorkerEnrollmentTx(ctx, tx, enrollment); err != nil {
			return err
		}
		return insertAttachedWorkerAuditEventTx(ctx, tx, audit)
	})
}

func (store *Store) LoadAttachedWorkerEnrollment(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	enrollmentID domain.AttachedWorkerEnrollmentID,
) (result domain.AttachedWorkerEnrollment, found bool, err error) {
	if err := validateAttachedWorkerEnrollmentScope(tenantID, ownerUserID, enrollmentID); err != nil {
		return result, false, err
	}
	result, found, err = readJSON[domain.AttachedWorkerEnrollment](ctx, store.db,
		`SELECT record FROM attached_worker_enrollments
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND enrollment_id = $3`,
		tenantID, ownerUserID, enrollmentID,
	)
	if err != nil || !found {
		return result, found, err
	}
	if err := result.Validate(); err != nil {
		return domain.AttachedWorkerEnrollment{}, false, err
	}
	if result.TenantID != tenantID || result.OwnerUserID != ownerUserID || result.ID != enrollmentID {
		return domain.AttachedWorkerEnrollment{}, false, ErrAttachedWorkerEnrollmentConflict
	}
	return result, true, nil
}

func (store *Store) ClaimAttachedWorkerEnrollment(
	ctx context.Context,
	mutation ports.AttachedWorkerClaimMutation,
) (result ports.AttachedWorkerClaimResult, err error) {
	if err := validateAttachedWorkerClaimMutation(mutation); err != nil {
		return result, err
	}
	result.Status = ports.AttachedWorkerDenied
	err = store.Transact(ctx, mutation.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		enrollment, found, err := readAttachedWorkerEnrollmentTx(
			ctx, tx, mutation.OwnerUserID, mutation.EnrollmentID,
		)
		if err != nil {
			return err
		}
		if !found {
			result = ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerDenied}
			return nil
		}
		if enrollment.Audience != mutation.PresentedAudience ||
			subtle.ConstantTimeCompare(
				[]byte(enrollment.BootstrapDigest), []byte(mutation.PresentedDigest),
			) != 1 {
			result = ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerDenied}
			return nil
		}
		if !enrollment.ConsumedAt.IsZero() {
			result = ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerConsumed}
			return nil
		}
		if !mutation.At.Before(enrollment.ExpiresAt) {
			result = ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerExpired}
			return nil
		}
		if enrollment.Revision != mutation.ExpectedEnrollmentRevision {
			result = ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerConflict}
			return nil
		}
		if enrollment.WorkerID != mutation.Worker.ID || enrollment.DisplayName != mutation.Worker.DisplayName {
			result = ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerConflict}
			return nil
		}
		if _, workerFound, err := readAttachedWorkerTx(ctx, tx, mutation.OwnerUserID, mutation.Worker.ID); err != nil {
			return err
		} else if workerFound {
			result = ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerConflict}
			return nil
		}
		enrollment.ConsumedAt = mutation.At
		enrollment.Revision++
		if err := updateAttachedWorkerEnrollmentTx(ctx, tx, enrollment); err != nil {
			return err
		}
		if err := insertAttachedWorkerTx(ctx, tx, mutation.Worker); err != nil {
			return err
		}
		if err := insertAttachedWorkerAuditEventTx(ctx, tx, mutation.Audit); err != nil {
			return err
		}
		result = ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerClaimed, Worker: mutation.Worker}
		return nil
	})
	return result, err
}

func (store *Store) LoadAttachedWorker(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
) (result domain.AttachedWorker, found bool, err error) {
	if err := validateAttachedWorkerScope(tenantID, ownerUserID, workerID); err != nil {
		return result, false, err
	}
	result, found, err = readJSON[domain.AttachedWorker](ctx, store.db,
		`SELECT record FROM attached_workers
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3`,
		tenantID, ownerUserID, workerID,
	)
	if err != nil || !found {
		return result, found, err
	}
	if err := result.Validate(); err != nil {
		return domain.AttachedWorker{}, false, err
	}
	if result.TenantID != tenantID || result.OwnerUserID != ownerUserID || result.ID != workerID {
		return domain.AttachedWorker{}, false, ErrAttachedWorkerConflict
	}
	return result, true, nil
}

func (store *Store) ListAttachedWorkers(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	afterWorkerID domain.AttachedWorkerID,
	limit uint64,
) (result []domain.AttachedWorker, err error) {
	if err := validateAttachedWorkerList(tenantID, ownerUserID, afterWorkerID, limit); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT record FROM attached_workers
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id > $3
		 ORDER BY worker_id ASC LIMIT $4`,
		tenantID, ownerUserID, afterWorkerID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var record string
		if err := rows.Scan(&record); err != nil {
			return nil, err
		}
		var worker domain.AttachedWorker
		if err := unmarshalStoredJSON(record, &worker); err != nil {
			return nil, err
		}
		if err := worker.Validate(); err != nil {
			return nil, err
		}
		if worker.TenantID != tenantID || worker.OwnerUserID != ownerUserID {
			return nil, ErrAttachedWorkerConflict
		}
		result = append(result, worker)
	}
	return result, rows.Err()
}

func (store *Store) CompareAndSwapAttachedWorker(
	ctx context.Context,
	mutation ports.AttachedWorkerCASMutation,
) (swapped bool, err error) {
	if err := validateAttachedWorkerCASMutation(mutation); err != nil {
		return false, err
	}
	err = store.Transact(ctx, mutation.Next.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		current, found, err := readAttachedWorkerTx(ctx, tx, mutation.Next.OwnerUserID, mutation.Next.ID)
		if err != nil {
			return err
		}
		if !found || current.Revision != mutation.ExpectedRevision {
			swapped = false
			return nil
		}
		if err := validateAttachedWorkerCASTransition(current, mutation); err != nil {
			return err
		}
		if err := updateAttachedWorkerTx(ctx, tx, mutation.Next); err != nil {
			return err
		}
		if err := insertAttachedWorkerAuditEventTx(ctx, tx, mutation.Audit); err != nil {
			return err
		}
		swapped = true
		return nil
	})
	return swapped, err
}

func (store *Store) RevokeAttachedWorker(
	ctx context.Context,
	mutation ports.AttachedWorkerRevokeMutation,
) (revoked bool, err error) {
	if err := validateAttachedWorkerRevokeMutation(mutation); err != nil {
		return false, err
	}
	err = store.Transact(ctx, mutation.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		current, found, err := readAttachedWorkerTx(ctx, tx, mutation.OwnerUserID, mutation.WorkerID)
		if err != nil {
			return err
		}
		if !found || current.Revision != mutation.ExpectedRevision {
			revoked = false
			return nil
		}
		if err := validateAttachedWorkerRevokeTransition(current, mutation); err != nil {
			return err
		}
		if err := updateAttachedWorkerTx(ctx, tx, mutation.Next); err != nil {
			return err
		}
		if err := insertAttachedWorkerAuditEventTx(ctx, tx, mutation.Audit); err != nil {
			return err
		}
		revoked = true
		return nil
	})
	return revoked, err
}

func (store *Store) ListAttachedWorkerAuditEvents(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
	fromWorkerRevision uint64,
	limit uint64,
) (result []domain.AttachedWorkerAuditEvent, err error) {
	if err := validateAttachedWorkerScope(tenantID, ownerUserID, workerID); err != nil {
		return nil, err
	}
	if limit == 0 || limit > maxAttachedWorkerAuditListLimit {
		return nil, domain.ValidationError{Field: "attached_worker_audit.limit", Reason: "must be between 1 and 100"}
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT version, enrollment_id, action, worker_revision,
		        enrollment_generation, connection_generation, occurred_at
		 FROM attached_worker_audit_events
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3
		       AND worker_revision >= $4
		 ORDER BY worker_revision ASC LIMIT $5`,
		tenantID, ownerUserID, workerID, fromWorkerRevision, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		event := domain.AttachedWorkerAuditEvent{
			TenantID: tenantID, OwnerUserID: ownerUserID, WorkerID: workerID,
		}
		if err := rows.Scan(
			&event.Version, &event.EnrollmentID, &event.Action, &event.WorkerRevision,
			&event.EnrollmentGeneration, &event.ConnectionGeneration, &event.OccurredAt,
		); err != nil {
			return nil, err
		}
		if err := event.Validate(); err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func readAttachedWorkerEnrollmentTx(
	ctx context.Context,
	tx *stateTx,
	ownerUserID domain.UserID,
	enrollmentID domain.AttachedWorkerEnrollmentID,
) (domain.AttachedWorkerEnrollment, bool, error) {
	result, found, err := readJSON[domain.AttachedWorkerEnrollment](ctx, tx.sqlTx,
		`SELECT record FROM attached_worker_enrollments
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND enrollment_id = $3`,
		tx.tenantID, ownerUserID, enrollmentID,
	)
	if err != nil || !found {
		return result, found, err
	}
	if err := result.Validate(); err != nil {
		return domain.AttachedWorkerEnrollment{}, false, err
	}
	if result.TenantID != tx.tenantID || result.OwnerUserID != ownerUserID || result.ID != enrollmentID {
		return domain.AttachedWorkerEnrollment{}, false, ErrAttachedWorkerEnrollmentConflict
	}
	return result, true, nil
}

func readAttachedWorkerTx(
	ctx context.Context,
	tx *stateTx,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
) (domain.AttachedWorker, bool, error) {
	result, found, err := readJSON[domain.AttachedWorker](ctx, tx.sqlTx,
		`SELECT record FROM attached_workers
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3`,
		tx.tenantID, ownerUserID, workerID,
	)
	if err != nil || !found {
		return result, found, err
	}
	if err := result.Validate(); err != nil {
		return domain.AttachedWorker{}, false, err
	}
	if result.TenantID != tx.tenantID || result.OwnerUserID != ownerUserID || result.ID != workerID {
		return domain.AttachedWorker{}, false, ErrAttachedWorkerConflict
	}
	return result, true, nil
}

func readAttachedWorkerAuditEventTx(
	ctx context.Context,
	tx *stateTx,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
	workerRevision uint64,
) (event domain.AttachedWorkerAuditEvent, found bool, err error) {
	event.TenantID, event.OwnerUserID, event.WorkerID = tx.tenantID, ownerUserID, workerID
	err = tx.sqlTx.QueryRowContext(ctx,
		`SELECT version, enrollment_id, action, enrollment_generation,
		        connection_generation, occurred_at
		 FROM attached_worker_audit_events
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3
		       AND worker_revision = $4`,
		tx.tenantID, ownerUserID, workerID, workerRevision,
	).Scan(
		&event.Version, &event.EnrollmentID, &event.Action,
		&event.EnrollmentGeneration, &event.ConnectionGeneration, &event.OccurredAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.AttachedWorkerAuditEvent{}, false, nil
	}
	if err != nil {
		return domain.AttachedWorkerAuditEvent{}, false, err
	}
	event.WorkerRevision = workerRevision
	return event, true, nil
}

func insertAttachedWorkerEnrollmentTx(ctx context.Context, tx *stateTx, enrollment domain.AttachedWorkerEnrollment) error {
	record, err := marshal(enrollment)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO attached_worker_enrollments
		 (tenant_id, owner_user_id, enrollment_id, worker_id, display_name,
		  audience, bootstrap_digest, expires_at, retain_until, consumed_at,
		  created_at, revision, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
		         CAST($13 AS JsonDocument))`,
		enrollment.TenantID, enrollment.OwnerUserID, enrollment.ID, enrollment.WorkerID,
		enrollment.DisplayName, enrollment.Audience, enrollment.BootstrapDigest,
		enrollment.ExpiresAt, enrollment.RetainUntil, optionalAttachedWorkerTime(enrollment.ConsumedAt),
		enrollment.CreatedAt, enrollment.Revision, record,
	)
	return err
}

func updateAttachedWorkerEnrollmentTx(ctx context.Context, tx *stateTx, enrollment domain.AttachedWorkerEnrollment) error {
	if err := enrollment.Validate(); err != nil {
		return err
	}
	record, err := marshal(enrollment)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPDATE attached_worker_enrollments
		 SET consumed_at = $1, revision = $2, record = CAST($3 AS JsonDocument)
		 WHERE tenant_id = $4 AND owner_user_id = $5 AND enrollment_id = $6`,
		optionalAttachedWorkerTime(enrollment.ConsumedAt), enrollment.Revision, record,
		enrollment.TenantID, enrollment.OwnerUserID, enrollment.ID,
	)
	return err
}

func insertAttachedWorkerTx(ctx context.Context, tx *stateTx, worker domain.AttachedWorker) error {
	if err := worker.Validate(); err != nil {
		return err
	}
	record, err := marshal(worker)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO attached_workers
		 (tenant_id, owner_user_id, worker_id, display_name, identity_public_key,
		  enrollment_generation, connection_generation, desired_state,
		  observed_state, revision, created_at, updated_at, revoked_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
		         CAST($14 AS JsonDocument))`,
		worker.TenantID, worker.OwnerUserID, worker.ID, worker.DisplayName, worker.IdentityPublicKey,
		worker.EnrollmentGeneration, worker.ConnectionGeneration, worker.DesiredState,
		worker.ObservedState, worker.Revision, worker.CreatedAt, worker.UpdatedAt,
		optionalAttachedWorkerTime(worker.RevokedAt), record,
	)
	return err
}

func updateAttachedWorkerTx(ctx context.Context, tx *stateTx, worker domain.AttachedWorker) error {
	if err := worker.Validate(); err != nil {
		return err
	}
	record, err := marshal(worker)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPDATE attached_workers
		 SET display_name = $1, identity_public_key = $2,
		     enrollment_generation = $3, connection_generation = $4,
		     desired_state = $5, observed_state = $6, revision = $7,
		     updated_at = $8, revoked_at = $9, record = CAST($10 AS JsonDocument)
		 WHERE tenant_id = $11 AND owner_user_id = $12 AND worker_id = $13`,
		worker.DisplayName, worker.IdentityPublicKey,
		worker.EnrollmentGeneration, worker.ConnectionGeneration,
		worker.DesiredState, worker.ObservedState, worker.Revision,
		worker.UpdatedAt, optionalAttachedWorkerTime(worker.RevokedAt), record,
		worker.TenantID, worker.OwnerUserID, worker.ID,
	)
	return err
}

func insertAttachedWorkerAuditEventTx(ctx context.Context, tx *stateTx, event domain.AttachedWorkerAuditEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	_, err := tx.sqlTx.ExecContext(ctx,
		`INSERT INTO attached_worker_audit_events
		 (tenant_id, owner_user_id, worker_id, worker_revision, version,
		  enrollment_id, action, enrollment_generation, connection_generation, occurred_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		event.TenantID, event.OwnerUserID, event.WorkerID, event.WorkerRevision,
		event.Version, event.EnrollmentID, event.Action, event.EnrollmentGeneration,
		event.ConnectionGeneration, event.OccurredAt,
	)
	return err
}

func validateAttachedWorkerEnrollmentCreate(
	enrollment domain.AttachedWorkerEnrollment,
	audit domain.AttachedWorkerAuditEvent,
) error {
	if err := enrollment.Validate(); err != nil {
		return err
	}
	if err := audit.Validate(); err != nil {
		return err
	}
	if enrollment.Revision != 1 || !enrollment.ConsumedAt.IsZero() {
		return domain.ValidationError{Field: "attached_worker_enrollment", Reason: "creation requires revision one and an unused enrollment"}
	}
	if audit.Action != domain.AttachedWorkerAuditEnrollmentCreated ||
		audit.TenantID != enrollment.TenantID || audit.OwnerUserID != enrollment.OwnerUserID ||
		audit.WorkerID != enrollment.WorkerID || audit.EnrollmentID != enrollment.ID ||
		!audit.OccurredAt.Equal(enrollment.CreatedAt) {
		return domain.ValidationError{Field: "attached_worker_audit", Reason: "must exactly describe enrollment creation"}
	}
	return nil
}

func validateAttachedWorkerClaimMutation(mutation ports.AttachedWorkerClaimMutation) error {
	if err := validateAttachedWorkerEnrollmentScope(mutation.TenantID, mutation.OwnerUserID, mutation.EnrollmentID); err != nil {
		return err
	}
	if mutation.ExpectedEnrollmentRevision == 0 || mutation.At.IsZero() || strings.TrimSpace(mutation.PresentedAudience) == "" {
		return domain.ValidationError{Field: "attached_worker_claim", Reason: "requires revision, audience, and time"}
	}
	if err := mutation.PresentedDigest.Validate(); err != nil {
		return err
	}
	if err := mutation.Worker.Validate(); err != nil {
		return err
	}
	if err := mutation.Audit.Validate(); err != nil {
		return err
	}
	if mutation.Worker.TenantID != mutation.TenantID || mutation.Worker.OwnerUserID != mutation.OwnerUserID ||
		mutation.Worker.Revision != 1 || mutation.Worker.EnrollmentGeneration != 1 ||
		mutation.Worker.ConnectionGeneration != 0 || mutation.Worker.DesiredState != domain.AttachedWorkerDesiredActive ||
		mutation.Worker.ObservedState != domain.AttachedWorkerObservedOffline || !mutation.Worker.RevokedAt.IsZero() ||
		!mutation.Worker.CreatedAt.Equal(mutation.At) || !mutation.Worker.UpdatedAt.Equal(mutation.At) {
		return domain.ValidationError{Field: "attached_worker_claim.worker", Reason: "must be a pristine initial worker"}
	}
	if mutation.Audit.Action != domain.AttachedWorkerAuditEnrollmentClaimed ||
		mutation.Audit.TenantID != mutation.TenantID || mutation.Audit.OwnerUserID != mutation.OwnerUserID ||
		mutation.Audit.WorkerID != mutation.Worker.ID || mutation.Audit.EnrollmentID != mutation.EnrollmentID ||
		!mutation.Audit.OccurredAt.Equal(mutation.At) {
		return domain.ValidationError{Field: "attached_worker_claim.audit", Reason: "must exactly describe the claimed worker"}
	}
	return nil
}

func validateAttachedWorkerCASMutation(mutation ports.AttachedWorkerCASMutation) error {
	if mutation.ExpectedRevision == 0 || mutation.At.IsZero() {
		return domain.ValidationError{Field: "attached_worker_cas", Reason: "requires revision and time"}
	}
	if mutation.ExpectedRevision == math.MaxUint64 {
		return domain.ValidationError{Field: "attached_worker_cas.revision", Reason: "is exhausted"}
	}
	if err := mutation.Next.Validate(); err != nil {
		return err
	}
	if err := mutation.Audit.Validate(); err != nil {
		return err
	}
	if mutation.Next.Revision != mutation.ExpectedRevision+1 || !mutation.Next.UpdatedAt.Equal(mutation.At) ||
		mutation.Audit.TenantID != mutation.Next.TenantID || mutation.Audit.OwnerUserID != mutation.Next.OwnerUserID ||
		mutation.Audit.WorkerID != mutation.Next.ID || mutation.Audit.WorkerRevision != mutation.Next.Revision ||
		mutation.Audit.EnrollmentGeneration != mutation.Next.EnrollmentGeneration ||
		mutation.Audit.ConnectionGeneration != mutation.Next.ConnectionGeneration ||
		!mutation.Audit.OccurredAt.Equal(mutation.At) {
		return domain.ValidationError{Field: "attached_worker_cas", Reason: "next worker and audit must describe one resulting revision"}
	}
	return nil
}

func validateAttachedWorkerCASTransition(current domain.AttachedWorker, mutation ports.AttachedWorkerCASMutation) error {
	next := mutation.Next
	if !sameAttachedWorkerIdentity(current, next) || !next.CreatedAt.Equal(current.CreatedAt) ||
		next.DesiredState != current.DesiredState || next.ObservedState != current.ObservedState ||
		!next.RevokedAt.Equal(current.RevokedAt) {
		return domain.ValidationError{Field: "attached_worker_cas", Reason: "immutable scope and states cannot change"}
	}
	switch mutation.Audit.Action {
	case domain.AttachedWorkerAuditWorkerRenamed:
		if next.DisplayName == current.DisplayName || !bytes.Equal(next.IdentityPublicKey, current.IdentityPublicKey) ||
			next.EnrollmentGeneration != current.EnrollmentGeneration || next.ConnectionGeneration != current.ConnectionGeneration {
			return domain.ValidationError{Field: "attached_worker_cas.rename", Reason: "must only change display_name"}
		}
	case domain.AttachedWorkerAuditIdentityRotated:
		if current.EnrollmentGeneration == math.MaxUint64 {
			return domain.ValidationError{Field: "attached_worker_cas.identity", Reason: "enrollment_generation is exhausted"}
		}
		if next.DisplayName != current.DisplayName || bytes.Equal(next.IdentityPublicKey, current.IdentityPublicKey) ||
			next.EnrollmentGeneration != current.EnrollmentGeneration+1 || next.ConnectionGeneration != current.ConnectionGeneration {
			return domain.ValidationError{Field: "attached_worker_cas.identity", Reason: "must rotate identity and advance only enrollment_generation"}
		}
	case domain.AttachedWorkerAuditConnectionGenerationAdvanced:
		if current.ConnectionGeneration == math.MaxUint64 {
			return domain.ValidationError{Field: "attached_worker_cas.connection", Reason: "connection_generation is exhausted"}
		}
		if next.DisplayName != current.DisplayName || !bytes.Equal(next.IdentityPublicKey, current.IdentityPublicKey) ||
			next.EnrollmentGeneration != current.EnrollmentGeneration || next.ConnectionGeneration != current.ConnectionGeneration+1 {
			return domain.ValidationError{Field: "attached_worker_cas.connection", Reason: "must advance only connection_generation"}
		}
	default:
		return domain.ValidationError{Field: "attached_worker_cas.action", Reason: "is not a CAS action"}
	}
	return nil
}

func validateAttachedWorkerRevokeMutation(mutation ports.AttachedWorkerRevokeMutation) error {
	if err := validateAttachedWorkerScope(mutation.TenantID, mutation.OwnerUserID, mutation.WorkerID); err != nil {
		return err
	}
	if mutation.ExpectedRevision == 0 || mutation.At.IsZero() {
		return domain.ValidationError{Field: "attached_worker_revoke", Reason: "requires revision and time"}
	}
	if mutation.ExpectedRevision == math.MaxUint64 {
		return domain.ValidationError{Field: "attached_worker_revoke.revision", Reason: "is exhausted"}
	}
	if err := mutation.Next.Validate(); err != nil {
		return err
	}
	if err := mutation.Audit.Validate(); err != nil {
		return err
	}
	if mutation.Next.TenantID != mutation.TenantID || mutation.Next.OwnerUserID != mutation.OwnerUserID || mutation.Next.ID != mutation.WorkerID ||
		mutation.Next.Revision != mutation.ExpectedRevision+1 || mutation.Next.DesiredState != domain.AttachedWorkerDesiredRevoked ||
		!mutation.Next.UpdatedAt.Equal(mutation.At) || !mutation.Next.RevokedAt.Equal(mutation.At) ||
		mutation.Audit.Action != domain.AttachedWorkerAuditWorkerRevoked || mutation.Audit.TenantID != mutation.TenantID ||
		mutation.Audit.OwnerUserID != mutation.OwnerUserID || mutation.Audit.WorkerID != mutation.WorkerID ||
		mutation.Audit.WorkerRevision != mutation.Next.Revision || mutation.Audit.EnrollmentGeneration != mutation.Next.EnrollmentGeneration ||
		mutation.Audit.ConnectionGeneration != mutation.Next.ConnectionGeneration || !mutation.Audit.OccurredAt.Equal(mutation.At) {
		return domain.ValidationError{Field: "attached_worker_revoke", Reason: "next worker and audit must describe deny-first revocation"}
	}
	return nil
}

func validateAttachedWorkerRevokeTransition(current domain.AttachedWorker, mutation ports.AttachedWorkerRevokeMutation) error {
	next := mutation.Next
	if current.EnrollmentGeneration == math.MaxUint64 || current.ConnectionGeneration == math.MaxUint64 {
		return domain.ValidationError{Field: "attached_worker_revoke", Reason: "worker generation is exhausted"}
	}
	if !sameAttachedWorkerIdentity(current, next) || !next.CreatedAt.Equal(current.CreatedAt) ||
		next.DisplayName != current.DisplayName || !bytes.Equal(next.IdentityPublicKey, current.IdentityPublicKey) ||
		next.ObservedState != current.ObservedState || next.EnrollmentGeneration != current.EnrollmentGeneration+1 ||
		next.ConnectionGeneration != current.ConnectionGeneration+1 {
		return domain.ValidationError{Field: "attached_worker_revoke", Reason: "must preserve remote observation and advance both fences"}
	}
	return nil
}

func validateAttachedWorkerEnrollmentScope(
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	enrollmentID domain.AttachedWorkerEnrollmentID,
) error {
	if err := tenantID.Validate(); err != nil {
		return err
	}
	if err := ownerUserID.Validate(); err != nil {
		return err
	}
	return enrollmentID.Validate()
}

func validateAttachedWorkerScope(tenantID domain.TenantID, ownerUserID domain.UserID, workerID domain.AttachedWorkerID) error {
	if err := tenantID.Validate(); err != nil {
		return err
	}
	if err := ownerUserID.Validate(); err != nil {
		return err
	}
	return workerID.Validate()
}

func validateAttachedWorkerList(
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	afterWorkerID domain.AttachedWorkerID,
	limit uint64,
) error {
	if err := tenantID.Validate(); err != nil {
		return err
	}
	if err := ownerUserID.Validate(); err != nil {
		return err
	}
	if afterWorkerID != "" {
		if err := afterWorkerID.Validate(); err != nil {
			return err
		}
	}
	if limit == 0 || limit > maxAttachedWorkerListLimit {
		return domain.ValidationError{Field: "attached_worker.limit", Reason: "must be between 1 and 100"}
	}
	return nil
}

func sameAttachedWorkerEnrollment(left, right domain.AttachedWorkerEnrollment) bool {
	return left.TenantID == right.TenantID && left.OwnerUserID == right.OwnerUserID && left.ID == right.ID &&
		left.WorkerID == right.WorkerID && left.DisplayName == right.DisplayName && left.Audience == right.Audience &&
		left.BootstrapDigest == right.BootstrapDigest && left.ExpiresAt.Equal(right.ExpiresAt) &&
		left.RetainUntil.Equal(right.RetainUntil) && left.ConsumedAt.Equal(right.ConsumedAt) &&
		left.CreatedAt.Equal(right.CreatedAt) && left.Revision == right.Revision
}

func sameAttachedWorkerIdentity(left, right domain.AttachedWorker) bool {
	return left.TenantID == right.TenantID && left.OwnerUserID == right.OwnerUserID && left.ID == right.ID
}

func sameAttachedWorkerAuditEvent(left, right domain.AttachedWorkerAuditEvent) bool {
	left.OccurredAt, right.OccurredAt = left.OccurredAt.UTC(), right.OccurredAt.UTC()
	return reflect.DeepEqual(left, right)
}

func optionalAttachedWorkerTime(value time.Time) any {
	if value.IsZero() {
		return nullableTime(nil)
	}
	return nullableTime(&value)
}

func unmarshalStoredJSON(record string, value any) error {
	if err := json.Unmarshal([]byte(record), value); err != nil {
		return fmt.Errorf("decode stored JSON: %w", err)
	}
	return nil
}

var _ ports.AttachedWorkerStore = (*Store)(nil)
