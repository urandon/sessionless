// Package ydbstore implements the harness-neutral StateStore port with YDB
// serializable transactions and idempotent row writes.
package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

const (
	defaultIdempotencyRetention  = 30 * 24 * time.Hour
	defaultOperationalRetention  = 90 * 24 * time.Hour
	defaultWebSessionIdleTTL     = 12 * time.Hour
	telegramDeliveryClaimTimeout = 2 * time.Minute
)

var ErrIdempotencyConflict = errors.New("idempotency key already belongs to another run")

const executionPlacementCutoverID = "execution-placement-v1-empty-cutover"

type Options struct {
	IdempotencyRetention time.Duration
	OperationalRetention time.Duration
	WebSessionIdleTTL    time.Duration
}

type Store struct {
	db                   *sql.DB
	idempotencyRetention time.Duration
	operationalRetention time.Duration
	webSessionIdleTTL    time.Duration
	attachedWorkerNow    func(context.Context, *sql.Tx) (time.Time, error)
}

func New(db *sql.DB, options Options) (*Store, error) {
	if db == nil {
		return nil, errors.New("YDB database must not be nil")
	}
	if options.IdempotencyRetention <= 0 {
		options.IdempotencyRetention = defaultIdempotencyRetention
	}
	if options.OperationalRetention <= 0 {
		options.OperationalRetention = defaultOperationalRetention
	}
	if options.WebSessionIdleTTL <= 0 {
		options.WebSessionIdleTTL = defaultWebSessionIdleTTL
	}
	return &Store{
		db:                   db,
		idempotencyRetention: options.IdempotencyRetention,
		operationalRetention: options.OperationalRetention,
		webSessionIdleTTL:    options.WebSessionIdleTTL,
		attachedWorkerNow:    currentAttachedWorkerTransactionTime,
	}, nil
}

// RequireExecutionPlacementCutover is the serving startup gate for the first
// explicit placement rollout. Scheduler and worker processes must refuse to
// start until the serializable empty-backlog cutover has committed; serving
// code never treats a missing placement as managed.
func (store *Store) RequireExecutionPlacementCutover(ctx context.Context) error {
	if store == nil || store.db == nil {
		return errors.New("YDB store must not be nil")
	}
	var completedAt time.Time
	if err := store.db.QueryRowContext(ctx,
		`SELECT completed_at FROM execution_placement_cutover_state WHERE cutover_id=$1`,
		executionPlacementCutoverID,
	).Scan(&completedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("execution placement cutover is not complete")
		}
		return fmt.Errorf("read execution placement cutover marker: %w", err)
	}
	if completedAt.IsZero() {
		return errors.New("execution placement cutover marker has no completion time")
	}
	return nil
}

func (store *Store) Transact(
	ctx context.Context,
	tenantID domain.TenantID,
	fn func(ports.StateTx) error,
) error {
	if err := tenantID.Validate(); err != nil {
		return err
	}
	if fn == nil {
		return domain.ValidationError{Field: "state transaction callback", Reason: "must not be nil"}
	}
	err := retry.DoTx(ctx, store.db, func(ctx context.Context, sqlTx *sql.Tx) error {
		tx := &stateTx{store: store, sqlTx: sqlTx, tenantID: tenantID}
		if err := fn(tx); err != nil {
			return callbackError{err: err}
		}
		return nil
	}, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
		Isolation: sql.LevelSerializable,
	}))
	var callback callbackError
	if errors.As(err, &callback) {
		return callback.err
	}
	if err == nil {
		return nil
	}
	kind := domain.ErrorTerminal
	if retry.Check(err).MustRetry(true) {
		kind = domain.ErrorRetryable
	}
	return &domain.ClassifiedError{
		Kind:      kind,
		Code:      "ydb_transaction_failed",
		Operation: "state_store.transact",
		Cause:     err,
	}
}

type callbackError struct {
	err error
}

func (err callbackError) Error() string { return err.err.Error() }
func (err callbackError) Unwrap() error { return err.err }

type stateTx struct {
	store    *Store
	sqlTx    *sql.Tx
	tenantID domain.TenantID
}

func (tx *stateTx) GetRun(ctx context.Context, id domain.RunID) (domain.Run, bool, error) {
	if err := id.Validate(); err != nil {
		return domain.Run{}, false, err
	}
	return readJSON[domain.Run](ctx, tx.sqlTx,
		`SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`,
		tx.tenantID, id,
	)
}

func (tx *stateTx) FindRunByIdempotencyKey(
	ctx context.Context,
	key domain.IdempotencyKey,
) (domain.Run, bool, error) {
	if err := key.Validate(); err != nil {
		return domain.Run{}, false, err
	}
	var runID string
	err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT run_id FROM run_idempotency
		 WHERE tenant_id = $1 AND idempotency_key = $2 AND expire_at > CurrentUtcTimestamp()`,
		tx.tenantID, key,
	).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Run{}, false, nil
	}
	if err != nil {
		return domain.Run{}, false, err
	}
	return tx.GetRun(ctx, domain.RunID(runID))
}

func (tx *stateTx) PutRun(ctx context.Context, run domain.Run) error {
	if err := tx.validateTenant(run.TenantID); err != nil {
		return err
	}
	if err := run.Validate(); err != nil {
		return err
	}
	if err := ensureSessionWritableTx(ctx, tx, run.SessionID); err != nil {
		return err
	}
	var existing string
	err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT run_id FROM run_idempotency
		 WHERE tenant_id = $1 AND idempotency_key = $2`,
		tx.tenantID, run.IdempotencyKey,
	).Scan(&existing)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && existing != string(run.ID) {
		return fmt.Errorf("%w: tenant=%s key=%s", ErrIdempotencyConflict, tx.tenantID, run.IdempotencyKey)
	}
	stored, found, err := tx.GetRun(ctx, run.ID)
	if err != nil {
		return err
	}
	if found && (stored.SessionID != run.SessionID ||
		stored.TriggerEventID != run.TriggerEventID ||
		stored.SubscriptionConnectionID != run.SubscriptionConnectionID ||
		stored.IdempotencyKey != run.IdempotencyKey ||
		!stored.CreatedAt.Equal(run.CreatedAt)) {
		return domain.ValidationError{Field: "run", Reason: "immutable identity fields cannot change"}
	}
	payload, err := marshal(run)
	if err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO runs
		 (tenant_id, run_id, session_id, trigger_event_id,
		  subscription_connection_id, status, created_at, updated_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))`,
		run.TenantID, run.ID, run.SessionID, run.TriggerEventID,
		run.SubscriptionConnectionID, run.Status, run.CreatedAt, run.UpdatedAt, payload,
	); err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO runs_by_session
		 (tenant_id, session_id, created_at, run_id, trigger_event_id, status)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		run.TenantID, run.SessionID, run.CreatedAt, run.ID, run.TriggerEventID, run.Status,
	); err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO run_idempotency
		 (tenant_id, idempotency_key, run_id, created_at, expire_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		run.TenantID, run.IdempotencyKey, run.ID, run.CreatedAt,
		run.CreatedAt.Add(tx.store.idempotencyRetention),
	)
	return err
}

func (tx *stateTx) GetAttempt(
	ctx context.Context,
	id domain.AttemptID,
) (domain.Attempt, bool, error) {
	if err := id.Validate(); err != nil {
		return domain.Attempt{}, false, err
	}
	return readJSON[domain.Attempt](ctx, tx.sqlTx,
		`SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2`,
		tx.tenantID, id,
	)
}

func (tx *stateTx) PutAttempt(ctx context.Context, attempt domain.Attempt) error {
	run, err := tx.owningRun(ctx, attempt.TenantID, attempt.RunID)
	if err != nil {
		return err
	}
	if err := attempt.ValidateForRun(run); err != nil {
		return err
	}
	payload, err := marshal(attempt)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO attempts
		 (tenant_id, attempt_id, run_id, attempt_number, status, worker_id,
		  created_at, updated_at, retention_expire_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10 AS JsonDocument))`,
		attempt.TenantID, attempt.ID, attempt.RunID, attempt.Number, attempt.Status,
		attempt.WorkerID, attempt.CreatedAt, attempt.UpdatedAt,
		attempt.UpdatedAt.Add(tx.store.operationalRetention), payload,
	)
	return err
}

func (tx *stateTx) PutLease(ctx context.Context, lease domain.Lease) error {
	attempt, run, err := tx.owningAttempt(ctx, lease.TenantID, lease.RunID, lease.AttemptID)
	if err != nil {
		return err
	}
	if err := lease.ValidateForAttempt(run, attempt); err != nil {
		return err
	}
	payload, err := marshal(lease)
	if err != nil {
		return err
	}
	bucket, err := ydbpartition.BucketV1(string(lease.RunID))
	if err != nil {
		return err
	}
	previous, found, err := readJSON[domain.Lease](ctx, tx.sqlTx,
		`SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2`,
		lease.TenantID, lease.ID,
	)
	if err != nil {
		return err
	}
	if found {
		previousBucket, err := ydbpartition.BucketV1(string(previous.RunID))
		if err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`DELETE FROM lease_expiry
			 WHERE tenant_id = $1 AND expires_at = $2 AND run_id = $3`,
			previous.TenantID, previous.ExpiresAt, previous.RunID,
		); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`DELETE FROM lease_expiry_v2
			 WHERE shard_bucket = $1 AND expires_at = $2
			 AND tenant_id = $3 AND run_id = $4`,
			previousBucket, previous.ExpiresAt, previous.TenantID, previous.RunID,
		); err != nil {
			return err
		}
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO leases
		 (tenant_id, lease_id, run_id, attempt_id, worker_id, fence_token,
		  acquired_at, expires_at, retention_expire_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10 AS JsonDocument))`,
		lease.TenantID, lease.ID, lease.RunID, lease.AttemptID, lease.WorkerID,
		lease.FenceToken, lease.AcquiredAt, lease.ExpiresAt,
		lease.ExpiresAt.Add(tx.store.operationalRetention), payload,
	)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO lease_expiry
		 (tenant_id, expires_at, run_id, lease_id, fence_token)
		 VALUES ($1, $2, $3, $4, $5)`,
		lease.TenantID, lease.ExpiresAt, lease.RunID, lease.ID, lease.FenceToken,
	)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO lease_expiry_v2
		 (shard_bucket, expires_at, tenant_id, run_id, lease_id, fence_token)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		bucket, lease.ExpiresAt, lease.TenantID, lease.RunID, lease.ID, lease.FenceToken,
	)
	return err
}

func (tx *stateTx) PutCheckpoint(ctx context.Context, checkpoint domain.Checkpoint) error {
	attempt, run, err := tx.owningAttempt(
		ctx, checkpoint.TenantID, checkpoint.RunID, checkpoint.AttemptID,
	)
	if err != nil {
		return err
	}
	if err := checkpoint.ValidateForAttempt(run, attempt); err != nil {
		return err
	}
	payload, err := marshal(checkpoint)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO checkpoints
		 (tenant_id, attempt_id, sequence, checkpoint_id, run_id, blob_key,
		  created_at, retention_expire_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))`,
		checkpoint.TenantID, checkpoint.AttemptID, checkpoint.Sequence,
		checkpoint.ID, checkpoint.RunID, checkpoint.State.Key, checkpoint.CreatedAt,
		checkpoint.CreatedAt.Add(tx.store.operationalRetention), payload,
	)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO checkpoint_objects_by_run
		 (tenant_id, run_id, checkpoint_id, record)
		 VALUES ($1, $2, $3, CAST($4 AS JsonDocument))`,
		checkpoint.TenantID, checkpoint.RunID, checkpoint.ID, payload,
	)
	return err
}

func (tx *stateTx) PutQuotaReservation(
	ctx context.Context,
	reservation domain.QuotaReservation,
) error {
	run, err := tx.owningRun(ctx, reservation.TenantID, reservation.RunID)
	if err != nil {
		return err
	}
	if err := reservation.ValidateForRun(run); err != nil {
		return err
	}
	payload, err := marshal(reservation)
	if err != nil {
		return err
	}
	bucket, err := ydbpartition.BucketV1(string(reservation.ID))
	if err != nil {
		return err
	}
	previous, found, err := readJSON[domain.QuotaReservation](ctx, tx.sqlTx,
		`SELECT payload FROM quota_reservations
		 WHERE tenant_id = $1 AND quota_reservation_id = $2`,
		reservation.TenantID, reservation.ID,
	)
	if err != nil {
		return err
	}
	if found {
		previousBucket, err := ydbpartition.BucketV1(string(previous.ID))
		if err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`DELETE FROM quota_expiry
			 WHERE tenant_id = $1 AND expires_at = $2 AND quota_reservation_id = $3`,
			previous.TenantID, previous.ExpiresAt, previous.ID,
		); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`DELETE FROM quota_expiry_v2
			 WHERE shard_bucket = $1 AND expires_at = $2
			 AND tenant_id = $3 AND quota_reservation_id = $4`,
			previousBucket, previous.ExpiresAt, previous.TenantID, previous.ID,
		); err != nil {
			return err
		}
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO quota_reservations
		 (tenant_id, quota_reservation_id, run_id, subscription_connection_id,
		  status, capacity_units, held_at, expires_at, retention_expire_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10 AS JsonDocument))`,
		reservation.TenantID, reservation.ID, reservation.RunID,
		reservation.SubscriptionConnectionID, reservation.Status,
		reservation.CapacityUnits, reservation.HeldAt, reservation.ExpiresAt,
		reservation.ExpiresAt.Add(tx.store.operationalRetention), payload,
	)
	if err != nil || reservation.Status != domain.ReservationHeld {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO quota_expiry
		 (tenant_id, expires_at, quota_reservation_id, run_id, subscription_connection_id)
		 VALUES ($1, $2, $3, $4, $5)`,
		reservation.TenantID, reservation.ExpiresAt, reservation.ID,
		reservation.RunID, reservation.SubscriptionConnectionID,
	)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO quota_expiry_v2
		 (shard_bucket, expires_at, tenant_id, quota_reservation_id,
		  run_id, subscription_connection_id)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		bucket, reservation.ExpiresAt, reservation.TenantID, reservation.ID,
		reservation.RunID, reservation.SubscriptionConnectionID,
	)
	return err
}

func (tx *stateTx) AppendUsageObservation(
	ctx context.Context,
	observation domain.UsageObservation,
) error {
	attempt, run, err := tx.owningAttempt(
		ctx, observation.TenantID, observation.RunID, observation.AttemptID,
	)
	if err != nil {
		return err
	}
	if err := observation.ValidateForAttempt(run, attempt); err != nil {
		return err
	}
	payload, err := marshal(observation)
	if err != nil {
		return err
	}
	var inputTokens, outputTokens uint64
	if observation.InputTokens != nil {
		inputTokens = *observation.InputTokens
	}
	if observation.OutputTokens != nil {
		outputTokens = *observation.OutputTokens
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO usage_observations
		 (tenant_id, subscription_connection_id, observed_at,
		  usage_observation_id, run_id, attempt_id, source, input_tokens,
		  output_tokens, retention_expire_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CAST($11 AS JsonDocument))`,
		observation.TenantID, observation.SubscriptionConnectionID,
		observation.ObservedAt, observation.ID, observation.RunID,
		observation.AttemptID, observation.Source, inputTokens, outputTokens,
		observation.ObservedAt.Add(tx.store.operationalRetention), payload,
	)
	return err
}

func (tx *stateTx) PutArtifactManifest(
	ctx context.Context,
	manifest domain.ArtifactManifest,
) error {
	run, err := tx.owningRun(ctx, manifest.TenantID, manifest.RunID)
	if err != nil {
		return err
	}
	if err := manifest.ValidateForRun(run); err != nil {
		return err
	}
	payload, err := marshal(manifest)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO artifact_manifests
		 (tenant_id, artifact_manifest_id, run_id, created_at, payload)
		 VALUES ($1, $2, $3, $4, CAST($5 AS JsonDocument))`,
		manifest.TenantID, manifest.ID, manifest.RunID, manifest.CreatedAt, payload,
	)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO artifact_manifests_by_run
		 (tenant_id, run_id, artifact_manifest_id)
		 VALUES ($1, $2, $3)`,
		manifest.TenantID, manifest.RunID, manifest.ID,
	)
	return err
}

func (tx *stateTx) PutWorkerJob(ctx context.Context, job domain.WorkerJob) error {
	run, err := tx.owningRun(ctx, job.TenantID, job.RunID)
	if err != nil {
		return err
	}
	if err := job.ValidateForRun(run); err != nil {
		return err
	}
	attempt, found, err := tx.GetAttempt(ctx, job.AttemptID)
	if err != nil {
		return err
	}
	if !found || attempt.RunID != job.RunID {
		return domain.ValidationError{Field: "worker_job.attempt_id", Reason: "must reference the owning run"}
	}
	reservation, found, err := readJSON[domain.QuotaReservation](
		ctx, tx.sqlTx,
		`SELECT payload FROM quota_reservations
		 WHERE tenant_id = $1 AND quota_reservation_id = $2`,
		job.TenantID, job.ReservationID,
	)
	if err != nil {
		return err
	}
	if !found || reservation.RunID != job.RunID {
		return domain.ValidationError{Field: "worker_job.reservation_id", Reason: "must reference the owning run"}
	}
	manifest, found, err := readJSON[domain.ArtifactManifest](
		ctx, tx.sqlTx,
		`SELECT payload FROM artifact_manifests
		 WHERE tenant_id = $1 AND artifact_manifest_id = $2`,
		job.TenantID, job.InputManifestID,
	)
	if err != nil {
		return err
	}
	if !found || manifest.RunID != job.RunID {
		return domain.ValidationError{Field: "worker_job.input_manifest_id", Reason: "must reference the owning run"}
	}
	payload, err := marshal(job)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO worker_jobs
		 (tenant_id, run_id, attempt_id, reservation_id, created_at,
		  retention_expire_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, CAST($7 AS JsonDocument))`,
		job.TenantID, job.RunID, job.AttemptID, job.ReservationID,
		job.CreatedAt, job.CreatedAt.Add(tx.store.operationalRetention), payload,
	)
	return err
}

func (tx *stateTx) PutDispatchOutbox(
	ctx context.Context,
	outbox domain.DispatchOutbox,
) error {
	attempt, run, err := tx.owningAttempt(ctx, outbox.TenantID, outbox.RunID, outbox.AttemptID)
	if err != nil {
		return err
	}
	if err := outbox.ValidateForAttempt(run, attempt); err != nil {
		return err
	}
	payload, err := marshal(outbox)
	if err != nil {
		return err
	}
	bucket, err := ydbpartition.BucketV1(string(outbox.ID))
	if err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`DELETE FROM dispatch_ready
		 WHERE tenant_id = $1 AND available_at = $2 AND dispatch_outbox_id = $3`,
		outbox.TenantID, outbox.CreatedAt, outbox.ID,
	); err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`DELETE FROM dispatch_ready_v2
		 WHERE shard_bucket = $1 AND available_at = $2
		 AND tenant_id = $3 AND dispatch_outbox_id = $4`,
		bucket, outbox.CreatedAt, outbox.TenantID, outbox.ID,
	); err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO dispatch_outbox
		 (tenant_id, dispatch_outbox_id, run_id, attempt_id, status,
		  created_at, updated_at, retention_expire_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))`,
		outbox.TenantID, outbox.ID, outbox.RunID, outbox.AttemptID,
		outbox.Status, outbox.CreatedAt, outbox.UpdatedAt,
		outbox.UpdatedAt.Add(tx.store.operationalRetention), payload,
	)
	if err != nil || outbox.Status != domain.DispatchPending {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO dispatch_ready
		 (tenant_id, available_at, dispatch_outbox_id, run_id, attempt_id)
		 VALUES ($1, $2, $3, $4, $5)`,
		outbox.TenantID, outbox.CreatedAt, outbox.ID, outbox.RunID, outbox.AttemptID,
	)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO dispatch_ready_v2
		 (shard_bucket, available_at, tenant_id, dispatch_outbox_id, run_id, attempt_id)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		bucket, outbox.CreatedAt, outbox.TenantID, outbox.ID, outbox.RunID, outbox.AttemptID,
	)
	return err
}

func (tx *stateTx) PutTelegramDeliveryOutbox(
	ctx context.Context,
	outbox domain.TelegramDeliveryOutbox,
) error {
	run, err := tx.owningRun(ctx, outbox.TenantID, outbox.RunID)
	if err != nil {
		return err
	}
	bucket, err := ydbpartition.BucketV1(string(outbox.ID))
	if err != nil {
		return err
	}
	if err := outbox.ValidateForRun(run); err != nil {
		return err
	}
	payload, err := marshal(outbox)
	if err != nil {
		return err
	}
	nextAttemptAt := time.Unix(0, 0).UTC()
	if outbox.NextAttemptAt != nil {
		nextAttemptAt = *outbox.NextAttemptAt
	}
	previous, found, err := readJSON[domain.TelegramDeliveryOutbox](ctx, tx.sqlTx,
		`SELECT payload FROM telegram_delivery_outbox
		 WHERE tenant_id = $1 AND telegram_delivery_id = $2`,
		outbox.TenantID, outbox.ID,
	)
	if err != nil {
		return err
	}
	if found {
		if previous.RunID != outbox.RunID {
			return domain.ValidationError{
				Field: "telegram_delivery.run_id", Reason: "cannot change after the delivery is created",
			}
		}
		previousAvailableAt := telegramDeliveryAvailableAt(previous)
		if _, err := tx.sqlTx.ExecContext(ctx,
			`DELETE FROM telegram_delivery_ready
			 WHERE tenant_id = $1 AND available_at = $2 AND telegram_delivery_id = $3`,
			previous.TenantID, previousAvailableAt, previous.ID,
		); err != nil {
			return err
		}
		previousBucket, err := ydbpartition.BucketV1(string(previous.ID))
		if err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`DELETE FROM telegram_delivery_ready_v2
			 WHERE shard_bucket = $1 AND available_at = $2
			 AND tenant_id = $3 AND telegram_delivery_id = $4`,
			previousBucket, previousAvailableAt, previous.TenantID, previous.ID,
		); err != nil {
			return err
		}
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO telegram_delivery_outbox
		 (tenant_id, telegram_delivery_id, run_id, status, chat_id,
		  next_attempt_at, created_at, updated_at, retention_expire_at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10 AS JsonDocument))`,
		outbox.TenantID, outbox.ID, outbox.RunID, outbox.Status,
		outbox.Chat.ChatID, nextAttemptAt, outbox.CreatedAt, outbox.UpdatedAt,
		outbox.UpdatedAt.Add(tx.store.operationalRetention), payload,
	)
	if err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO telegram_deliveries_by_run
		 (tenant_id, run_id, telegram_delivery_id, record)
		 VALUES ($1, $2, $3, CAST($4 AS JsonDocument))`,
		outbox.TenantID, outbox.RunID, outbox.ID, payload,
	); err != nil {
		return err
	}
	if outbox.Status != domain.DeliveryPending &&
		outbox.Status != domain.DeliveryRetryWait &&
		outbox.Status != domain.DeliverySending {
		return nil
	}
	availableAt := telegramDeliveryAvailableAt(outbox)
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO telegram_delivery_ready
		 (tenant_id, available_at, telegram_delivery_id, run_id)
		 VALUES ($1, $2, $3, $4)`,
		outbox.TenantID, availableAt, outbox.ID, outbox.RunID,
	)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO telegram_delivery_ready_v2
		 (shard_bucket, available_at, tenant_id, telegram_delivery_id, run_id)
		 VALUES ($1, $2, $3, $4, $5)`,
		bucket, availableAt, outbox.TenantID, outbox.ID, outbox.RunID,
	)
	return err
}

func telegramDeliveryAvailableAt(outbox domain.TelegramDeliveryOutbox) time.Time {
	if outbox.NextAttemptAt != nil {
		return *outbox.NextAttemptAt
	}
	if outbox.Status == domain.DeliverySending {
		return outbox.UpdatedAt.Add(telegramDeliveryClaimTimeout)
	}
	return outbox.UpdatedAt
}

func (tx *stateTx) validateTenant(actual domain.TenantID) error {
	return domain.EnsureSameTenant(tx.tenantID, actual)
}

func (tx *stateTx) owningRun(
	ctx context.Context,
	tenantID domain.TenantID,
	runID domain.RunID,
) (domain.Run, error) {
	if err := tx.validateTenant(tenantID); err != nil {
		return domain.Run{}, err
	}
	run, found, err := tx.GetRun(ctx, runID)
	if err != nil {
		return domain.Run{}, err
	}
	if !found {
		return domain.Run{}, fmt.Errorf("run %q not found in tenant %q", runID, tx.tenantID)
	}
	if err := ensureSessionWritableTx(ctx, tx, run.SessionID); err != nil {
		return domain.Run{}, err
	}
	return run, nil
}

func (tx *stateTx) owningAttempt(
	ctx context.Context,
	tenantID domain.TenantID,
	runID domain.RunID,
	attemptID domain.AttemptID,
) (domain.Attempt, domain.Run, error) {
	run, err := tx.owningRun(ctx, tenantID, runID)
	if err != nil {
		return domain.Attempt{}, domain.Run{}, err
	}
	attempt, found, err := tx.GetAttempt(ctx, attemptID)
	if err != nil {
		return domain.Attempt{}, domain.Run{}, err
	}
	if !found {
		return domain.Attempt{}, domain.Run{}, fmt.Errorf(
			"attempt %q not found in tenant %q", attemptID, tx.tenantID,
		)
	}
	return attempt, run, nil
}

func marshal(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

type rowQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readJSON[T any](
	ctx context.Context,
	query rowQuery,
	statement string,
	args ...any,
) (value T, found bool, err error) {
	var payload string
	err = query.QueryRowContext(ctx, statement, args...).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return value, false, nil
	}
	if err != nil {
		return value, false, err
	}
	if err := json.Unmarshal([]byte(payload), &value); err != nil {
		return value, false, fmt.Errorf("decode stored JSON: %w", err)
	}
	return value, true, nil
}

var _ ports.StateStore = (*Store)(nil)
var _ ports.SessionStore = (*Store)(nil)
var _ ports.StateTx = (*stateTx)(nil)
