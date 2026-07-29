package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrLeaseHeld = errors.New("run already has an active lease")
	ErrLeaseLost = errors.New("lease fence no longer owns the run")
)

type TelegramIngress struct {
	TenantID domain.TenantID
	SourceID string
	UpdateID int64
	ExpireAt time.Time
	Run      domain.Run
	Attempt  domain.Attempt
	Dispatch domain.DispatchOutbox
}

type TelegramIngressResult struct {
	RunID   domain.RunID
	Created bool
}

// IngestTelegram atomically deduplicates the frontend update and writes the
// initial run, attempt, and dispatch outbox rows. A duplicate returns the
// already-associated run without emitting a second outbox row.
func (store *Store) IngestTelegram(
	ctx context.Context,
	request TelegramIngress,
) (result TelegramIngressResult, err error) {
	if err := request.TenantID.Validate(); err != nil {
		return result, err
	}
	if err := domain.ValidateOpaqueID("telegram.source_id", request.SourceID); err != nil {
		return result, err
	}
	if request.UpdateID < 0 {
		return result, domain.ValidationError{
			Field: "telegram.update_id", Reason: "must not be negative",
		}
	}
	if request.ExpireAt.IsZero() {
		return result, domain.ValidationError{
			Field: "telegram.expire_at", Reason: "must not be zero",
		}
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		var existing string
		queryErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT run_id FROM telegram_updates
			 WHERE tenant_id = $1 AND source_id = $2 AND update_id = $3`,
			request.TenantID, request.SourceID, request.UpdateID,
		).Scan(&existing)
		switch {
		case queryErr == nil:
			result = TelegramIngressResult{RunID: domain.RunID(existing), Created: false}
			return nil
		case !errors.Is(queryErr, sql.ErrNoRows):
			return queryErr
		}
		if err := state.PutRun(ctx, request.Run); err != nil {
			return err
		}
		if err := state.PutAttempt(ctx, request.Attempt); err != nil {
			return err
		}
		if err := state.PutDispatchOutbox(ctx, request.Dispatch); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`INSERT INTO telegram_updates
			 (tenant_id, source_id, update_id, run_id, received_at, expire_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			request.TenantID, request.SourceID, request.UpdateID, request.Run.ID,
			request.Run.CreatedAt, request.ExpireAt,
		); err != nil {
			return err
		}
		result = TelegramIngressResult{RunID: request.Run.ID, Created: true}
		return nil
	})
	return result, err
}

type LeaseClaim struct {
	TenantID  domain.TenantID
	RunID     domain.RunID
	AttemptID domain.AttemptID
	LeaseID   domain.LeaseID
	WorkerID  string
	Now       time.Time
	ExpiresAt time.Time
}

// ClaimLease uses the tenant/run lease head as the contention point. YDB's
// serializable conflict retry makes concurrent claimers re-read the winning
// head; exactly one distinct lease can remain active.
func (store *Store) ClaimLease(
	ctx context.Context,
	claim LeaseClaim,
) (result domain.Lease, err error) {
	if err := validateLeaseClaim(claim); err != nil {
		return result, err
	}
	err = store.Transact(ctx, claim.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		var currentLeaseID, currentAttemptID, currentWorker string
		var fence uint64
		var expiresAt time.Time
		queryErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT lease_id, attempt_id, worker_id, fence_token, expires_at
			 FROM lease_heads WHERE tenant_id = $1 AND run_id = $2`,
			claim.TenantID, claim.RunID,
		).Scan(&currentLeaseID, &currentAttemptID, &currentWorker, &fence, &expiresAt)
		switch {
		case queryErr == nil && currentLeaseID == string(claim.LeaseID):
			lease, found, err := readJSON[domain.Lease](ctx, tx.sqlTx,
				`SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2`,
				claim.TenantID, claim.LeaseID,
			)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("lease head %q has no lease row", currentLeaseID)
			}
			result = lease
			return nil
		case queryErr == nil && expiresAt.After(claim.Now):
			return fmt.Errorf(
				"%w: lease=%s worker=%s attempt=%s expires_at=%s",
				ErrLeaseHeld, currentLeaseID, currentWorker, currentAttemptID,
				expiresAt.Format(time.RFC3339),
			)
		case queryErr != nil && !errors.Is(queryErr, sql.ErrNoRows):
			return queryErr
		}
		if queryErr == nil {
			if _, err := tx.sqlTx.ExecContext(ctx,
				`DELETE FROM lease_expiry
				 WHERE tenant_id = $1 AND expires_at = $2 AND run_id = $3`,
				claim.TenantID, expiresAt, claim.RunID,
			); err != nil {
				return err
			}
		}
		result = domain.Lease{
			ID:         claim.LeaseID,
			TenantID:   claim.TenantID,
			RunID:      claim.RunID,
			AttemptID:  claim.AttemptID,
			WorkerID:   claim.WorkerID,
			FenceToken: fence + 1,
			AcquiredAt: claim.Now,
			ExpiresAt:  claim.ExpiresAt,
		}
		if err := state.PutLease(ctx, result); err != nil {
			return err
		}
		_, err := tx.sqlTx.ExecContext(ctx,
			`UPSERT INTO lease_heads
			 (tenant_id, run_id, lease_id, attempt_id, worker_id,
			  fence_token, expires_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			result.TenantID, result.RunID, result.ID, result.AttemptID,
			result.WorkerID, result.FenceToken, result.ExpiresAt, claim.Now,
		)
		return err
	})
	return result, err
}

func (store *Store) RenewLease(
	ctx context.Context,
	tenantID domain.TenantID,
	leaseID domain.LeaseID,
	fence uint64,
	now time.Time,
	newExpiry time.Time,
) (result domain.Lease, err error) {
	if err := tenantID.Validate(); err != nil {
		return result, err
	}
	if err := leaseID.Validate(); err != nil {
		return result, err
	}
	if fence == 0 || now.IsZero() || !newExpiry.After(now) {
		return result, domain.ValidationError{
			Field: "lease renewal", Reason: "requires a positive fence and future expiry",
		}
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		lease, found, err := readJSON[domain.Lease](ctx, tx.sqlTx,
			`SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2`,
			tenantID, leaseID,
		)
		if err != nil {
			return err
		}
		if !found {
			return ErrLeaseLost
		}
		var currentLeaseID string
		var currentFence uint64
		var currentExpiry time.Time
		if err := tx.sqlTx.QueryRowContext(ctx,
			`SELECT lease_id, fence_token, expires_at
			 FROM lease_heads WHERE tenant_id = $1 AND run_id = $2`,
			tenantID, lease.RunID,
		).Scan(&currentLeaseID, &currentFence, &currentExpiry); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrLeaseLost
			}
			return err
		}
		if currentLeaseID != string(leaseID) || currentFence != fence || !currentExpiry.After(now) {
			return ErrLeaseLost
		}
		lease.ExpiresAt = newExpiry
		if err := state.PutLease(ctx, lease); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`UPDATE lease_heads SET expires_at = $1, updated_at = $2
			 WHERE tenant_id = $3 AND run_id = $4
			 AND lease_id = $5 AND fence_token = $6`,
			newExpiry, now, tenantID, lease.RunID, leaseID, fence,
		); err != nil {
			return err
		}
		result = lease
		return nil
	})
	return result, err
}

func (store *Store) TransitionQuotaReservation(
	ctx context.Context,
	tenantID domain.TenantID,
	reservationID domain.QuotaReservationID,
	to domain.ReservationStatus,
	at time.Time,
) error {
	return store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		reservation, found, err := readJSON[domain.QuotaReservation](ctx, tx.sqlTx,
			`SELECT payload FROM quota_reservations
			 WHERE tenant_id = $1 AND quota_reservation_id = $2`,
			tenantID, reservationID,
		)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("quota reservation %q not found", reservationID)
		}
		if err := reservation.Transition(to, at); err != nil {
			return err
		}
		return state.PutQuotaReservation(ctx, reservation)
	})
}

func (store *Store) AcknowledgeDispatch(
	ctx context.Context,
	tenantID domain.TenantID,
	outboxID domain.DispatchOutboxID,
	at time.Time,
) error {
	return store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		outbox, found, err := readJSON[domain.DispatchOutbox](ctx, tx.sqlTx,
			`SELECT payload FROM dispatch_outbox
			 WHERE tenant_id = $1 AND dispatch_outbox_id = $2`,
			tenantID, outboxID,
		)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("dispatch outbox %q not found", outboxID)
		}
		if outbox.Status == domain.DispatchPublished {
			return nil
		}
		if err := outbox.Transition(domain.DispatchPublished, at); err != nil {
			return err
		}
		return state.PutDispatchOutbox(ctx, outbox)
	})
}

func (store *Store) TransitionTelegramDelivery(
	ctx context.Context,
	tenantID domain.TenantID,
	deliveryID domain.TelegramDeliveryID,
	to domain.DeliveryStatus,
	at time.Time,
	retryAt *time.Time,
) error {
	return store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		delivery, found, err := readJSON[domain.TelegramDeliveryOutbox](ctx, tx.sqlTx,
			`SELECT payload FROM telegram_delivery_outbox
			 WHERE tenant_id = $1 AND telegram_delivery_id = $2`,
			tenantID, deliveryID,
		)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("Telegram delivery %q not found", deliveryID)
		}
		if delivery.Status == to {
			return nil
		}
		if err := delivery.Transition(to, at, retryAt); err != nil {
			return err
		}
		return state.PutTelegramDeliveryOutbox(ctx, delivery)
	})
}

type ExpiredLease struct {
	RunID     domain.RunID
	LeaseID   domain.LeaseID
	Fence     uint64
	ExpiresAt time.Time
}

// ListExpiredLeases uses the tenant/time primary-key range; it never scans
// payloads or another tenant's rows.
func (store *Store) ListExpiredLeases(
	ctx context.Context,
	tenantID domain.TenantID,
	before time.Time,
	limit uint64,
) (result []ExpiredLease, err error) {
	if limit == 0 {
		return nil, domain.ValidationError{Field: "lease expiry limit", Reason: "must be positive"}
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		rows, err := tx.sqlTx.QueryContext(ctx,
			`SELECT run_id, lease_id, fence_token, expires_at
			 FROM lease_expiry
			 WHERE tenant_id = $1 AND expires_at <= $2
			 ORDER BY expires_at, run_id
			 LIMIT $3`,
			tenantID, before, limit,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item ExpiredLease
			if err := rows.Scan(&item.RunID, &item.LeaseID, &item.Fence, &item.ExpiresAt); err != nil {
				return err
			}
			result = append(result, item)
		}
		return rows.Err()
	})
	return result, err
}

type CompletedRun struct {
	Run      domain.Run
	LeaseID  domain.LeaseID
	Fence    uint64
	At       time.Time
	Manifest domain.ArtifactManifest
	Delivery domain.TelegramDeliveryOutbox
	Usage    []domain.UsageObservation
}

// CompleteRun persists the terminal run, immutable result manifest, usage
// observations, and delivery outbox in one transaction.
func (store *Store) CompleteRun(ctx context.Context, result CompletedRun) error {
	return store.Transact(ctx, result.Run.TenantID, func(state ports.StateTx) error {
		if err := requireLeaseOwnership(
			ctx, state.(*stateTx), result.Run.ID, result.LeaseID, result.Fence, result.At,
		); err != nil {
			return err
		}
		if err := state.PutRun(ctx, result.Run); err != nil {
			return err
		}
		for _, observation := range result.Usage {
			if err := state.AppendUsageObservation(ctx, observation); err != nil {
				return err
			}
		}
		if err := state.PutArtifactManifest(ctx, result.Manifest); err != nil {
			return err
		}
		return state.PutTelegramDeliveryOutbox(ctx, result.Delivery)
	})
}

func (store *Store) SaveCheckpoint(
	ctx context.Context,
	checkpoint domain.Checkpoint,
	leaseID domain.LeaseID,
	fence uint64,
	at time.Time,
) error {
	return store.Transact(ctx, checkpoint.TenantID, func(state ports.StateTx) error {
		if err := requireLeaseOwnership(
			ctx, state.(*stateTx), checkpoint.RunID, leaseID, fence, at,
		); err != nil {
			return err
		}
		return state.PutCheckpoint(ctx, checkpoint)
	})
}

func validateLeaseClaim(claim LeaseClaim) error {
	if err := claim.TenantID.Validate(); err != nil {
		return err
	}
	if err := claim.RunID.Validate(); err != nil {
		return err
	}
	if err := claim.AttemptID.Validate(); err != nil {
		return err
	}
	if err := claim.LeaseID.Validate(); err != nil {
		return err
	}
	if err := domain.ValidateOpaqueID("lease.worker_id", claim.WorkerID); err != nil {
		return err
	}
	if claim.Now.IsZero() || !claim.ExpiresAt.After(claim.Now) {
		return domain.ValidationError{
			Field: "lease.expires_at", Reason: "must follow a non-zero claim time",
		}
	}
	return nil
}

func requireLeaseOwnership(
	ctx context.Context,
	tx *stateTx,
	runID domain.RunID,
	leaseID domain.LeaseID,
	fence uint64,
	at time.Time,
) error {
	if err := leaseID.Validate(); err != nil {
		return err
	}
	if fence == 0 || at.IsZero() {
		return domain.ValidationError{
			Field: "lease ownership", Reason: "requires a positive fence and non-zero time",
		}
	}
	var currentLeaseID string
	var currentFence uint64
	var expiresAt time.Time
	if err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT lease_id, fence_token, expires_at
		 FROM lease_heads WHERE tenant_id = $1 AND run_id = $2`,
		tx.tenantID, runID,
	).Scan(&currentLeaseID, &currentFence, &expiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLeaseLost
		}
		return err
	}
	if currentLeaseID != string(leaseID) || currentFence != fence || !expiresAt.After(at) {
		return ErrLeaseLost
	}
	return nil
}
