package ydbstore

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

const maxAttachedWorkerAttemptDeadlineListLimit uint64 = 100

type attachedWorkerCancellationDeadlinePlan struct {
	Lease       domain.AttachedWorkerAttemptDeadlineV1
	Cancel      *domain.AttachedWorkerAttemptDeadlineV1
	DeleteLease bool
}

func planAttachedWorkerCancellationDeadlines(previous, next domain.AttachedWorkerAttemptV1) (attachedWorkerCancellationDeadlinePlan, error) {
	bucket, err := domain.AttachedWorkerAttemptDeadlineBucketV1(next.TenantID, next.OwnerUserID, next.WorkerID, next.AttemptID)
	if err != nil {
		return attachedWorkerCancellationDeadlinePlan{}, err
	}
	lease := domain.AttachedWorkerAttemptDeadlineV1{
		Bucket: bucket, DeadlineAt: next.LeaseExpiresAt, TenantID: next.TenantID,
		OwnerUserID: next.OwnerUserID, WorkerID: next.WorkerID, AttemptID: next.AttemptID,
		Kind: domain.AttachedWorkerDeadlineLeaseExpiry, LeaseGeneration: next.LeaseGeneration,
		AttemptRevision: previous.Revision,
	}
	if previous.State == domain.AttachedWorkerAttemptOffered {
		return attachedWorkerCancellationDeadlinePlan{Lease: lease, DeleteLease: true}, nil
	}
	lease.AttemptRevision = next.Revision
	cancel := lease
	cancel.Kind = domain.AttachedWorkerDeadlineCancelAck
	cancel.DeadlineAt = next.CancelDeadline
	return attachedWorkerCancellationDeadlinePlan{Lease: lease, Cancel: &cancel}, nil
}

func fenceAttachedWorkerAttemptWithoutProtocolMutation(state domain.AttachedWorkerAttemptState) bool {
	// A signed CancelRequested frame already commits its revision and code.
	// Deadline fencing must not replace it with a divergent CancelFenced frame.
	return state == domain.AttachedWorkerAttemptCancelRequested
}

func fenceAttachedWorkerCommittedCancel(attempt domain.AttachedWorkerAttemptV1, at time.Time) (domain.AttachedWorkerAttemptV1, error) {
	if !fenceAttachedWorkerAttemptWithoutProtocolMutation(attempt.State) || attempt.Revision == math.MaxUint64 {
		return domain.AttachedWorkerAttemptV1{}, ErrAttachedWorkerAttemptConflict
	}
	next := attempt
	next.State = domain.AttachedWorkerAttemptFencedUnknown
	next.UpdatedAt = canonicalAttachedWorkerTime(at)
	next.Revision++
	return next, nil
}

func attachedWorkerAttemptPollAuthorized(request ports.AttachedWorkerAttemptPoll, at time.Time, worker domain.AttachedWorker, connection domain.AttachedWorkerConnection) bool {
	return worker.DesiredState != domain.AttachedWorkerDesiredRevoked &&
		connection.ID == request.ConnectionID && connection.EnrollmentGeneration == worker.EnrollmentGeneration &&
		connection.ConnectionGeneration == worker.ConnectionGeneration &&
		subtle.ConstantTimeCompare([]byte(connection.SecretDigest), []byte(request.PresentedSecretDigest)) == 1 &&
		at.Before(connection.AuthExpiresAt) && at.Before(connection.PresenceExpiresAt) &&
		(connection.State == domain.AttachedWorkerConnectionOnline || connection.State == domain.AttachedWorkerConnectionDraining)
}

func pendingAttachedWorkerAttemptMessageKind(state domain.AttachedWorkerAttemptState) domain.AttachedWorkerAttemptMessageKind {
	switch state {
	case domain.AttachedWorkerAttemptOffered:
		return domain.AttachedWorkerAttemptMessageLeaseOffered
	case domain.AttachedWorkerAttemptCancelRequested, domain.AttachedWorkerAttemptCancelledBeforeClaim, domain.AttachedWorkerAttemptFencedUnknown:
		return domain.AttachedWorkerAttemptMessageCancelRequested
	case domain.AttachedWorkerAttemptTerminalCommitted:
		return domain.AttachedWorkerAttemptMessageTerminalCommitted
	default:
		return ""
	}
}

func attachedWorkerPollFramePending(workerAck, platformSequence uint64) bool {
	return workerAck < platformSequence
}

func readAttachedWorkerAttemptTx(ctx context.Context, tx *stateTx, owner domain.UserID, worker domain.AttachedWorkerID) (domain.AttachedWorkerAttemptV1, bool, error) {
	value, found, err := readJSON[domain.AttachedWorkerAttemptV1](ctx, tx.sqlTx,
		`SELECT payload FROM attached_worker_attempt_heads
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`,
		tx.tenantID, owner, worker)
	if err != nil || !found {
		return value, found, err
	}
	if err := value.Validate(); err != nil {
		return domain.AttachedWorkerAttemptV1{}, false, err
	}
	if value.TenantID != tx.tenantID || value.OwnerUserID != owner || value.WorkerID != worker {
		return domain.AttachedWorkerAttemptV1{}, false, ErrAttachedWorkerAttemptConflict
	}
	return value, true, nil
}

func upsertAttachedWorkerAttemptTx(ctx context.Context, tx *stateTx, value domain.AttachedWorkerAttemptV1, retainUntil time.Time) error {
	value.CreatedAt = canonicalAttachedWorkerTime(value.CreatedAt)
	value.UpdatedAt = canonicalAttachedWorkerTime(value.UpdatedAt)
	value.LeaseExpiresAt = canonicalAttachedWorkerTime(value.LeaseExpiresAt)
	value.CancelDeadline = canonicalAttachedWorkerTime(value.CancelDeadline)
	retainUntil = canonicalAttachedWorkerTime(retainUntil)
	if err := value.Validate(); err != nil {
		return err
	}
	if !retainUntil.After(value.UpdatedAt) {
		return domain.ValidationError{Field: "attached_worker_attempt.retention", Reason: "must be after updated_at"}
	}
	payload, err := marshal(value)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO attached_worker_attempt_heads
		 (tenant_id,owner_user_id,worker_id,connection_id,attempt_id,run_id,lease_id,
		  lease_generation,fence_token,enrollment_generation,connection_generation,state,
		  lease_expires_at,cancel_deadline,updated_at,revision,retention_expire_at,payload)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,CAST($18 AS JsonDocument))`,
		value.TenantID, value.OwnerUserID, value.WorkerID, value.ConnectionID, value.AttemptID,
		value.RunID, value.LeaseID, value.LeaseGeneration, value.FenceToken,
		value.EnrollmentGeneration, value.ConnectionGeneration, value.State,
		value.LeaseExpiresAt, optionalAttachedWorkerTime(value.CancelDeadline), value.UpdatedAt,
		value.Revision, retainUntil, payload)
	return err
}

func compareAndSwapAttachedWorkerAttemptTx(ctx context.Context, tx *stateTx, expectedRevision uint64, value domain.AttachedWorkerAttemptV1, retainUntil time.Time) error {
	if expectedRevision == 0 || value.Revision != expectedRevision+1 {
		return ErrAttachedWorkerAttemptConflict
	}
	current, found, err := readAttachedWorkerAttemptTx(ctx, tx, value.OwnerUserID, value.WorkerID)
	if err != nil || !found {
		return err
	}
	if current.Revision != expectedRevision || current.AttemptID != value.AttemptID {
		return ErrAttachedWorkerAttemptConflict
	}
	// YDB serializable transactions retain the exact row read above in the
	// conflict set; a concurrent writer forces retry before this UPSERT commits.
	return upsertAttachedWorkerAttemptTx(ctx, tx, value, retainUntil)
}

func readAttachedWorkerAttemptMessageTx(ctx context.Context, tx *stateTx, scope domain.AttachedWorkerAttemptMessageV1) (domain.AttachedWorkerAttemptMessageV1, bool, error) {
	value, found, err := readJSON[domain.AttachedWorkerAttemptMessageV1](ctx, tx.sqlTx,
		`SELECT payload FROM attached_worker_attempt_messages
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND attempt_id=$4
		       AND direction=$5 AND attempt_sequence=$6`,
		tx.tenantID, scope.OwnerUserID, scope.WorkerID, scope.AttemptID, scope.Direction, scope.AttemptSequence)
	if err != nil || !found {
		return value, found, err
	}
	if err := value.Validate(); err != nil {
		return domain.AttachedWorkerAttemptMessageV1{}, false, err
	}
	if value.TenantID != tx.tenantID || value.OwnerUserID != scope.OwnerUserID ||
		value.WorkerID != scope.WorkerID || value.AttemptID != scope.AttemptID ||
		value.Direction != scope.Direction || value.AttemptSequence != scope.AttemptSequence {
		return domain.AttachedWorkerAttemptMessageV1{}, false, ErrAttachedWorkerAttemptMessageConflict
	}
	return value, true, nil
}

func insertOrReconcileAttachedWorkerAttemptMessageTx(ctx context.Context, tx *stateTx, message domain.AttachedWorkerAttemptMessageV1, retainUntil time.Time) (bool, error) {
	message.CreatedAt = canonicalAttachedWorkerTime(message.CreatedAt)
	retainUntil = canonicalAttachedWorkerTime(retainUntil)
	if err := message.Validate(); err != nil {
		return false, err
	}
	existing, found, err := readAttachedWorkerAttemptMessageTx(ctx, tx, message)
	if err != nil {
		return false, err
	}
	if found {
		if !sameAttachedWorkerAttemptMessage(existing, message) {
			return false, ErrAttachedWorkerAttemptMessageConflict
		}
		return true, nil
	}
	if !retainUntil.After(message.CreatedAt) {
		return false, domain.ValidationError{Field: "attached_worker_attempt_message.retention", Reason: "must be after created_at"}
	}
	payload, err := marshal(message)
	if err != nil {
		return false, err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO attached_worker_attempt_messages
		 (tenant_id,owner_user_id,worker_id,attempt_id,direction,attempt_sequence,
		  connection_generation,envelope_sequence,kind,fingerprint,created_at,retention_expire_at,payload)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,CAST($13 AS JsonDocument))`,
		message.TenantID, message.OwnerUserID, message.WorkerID, message.AttemptID,
		message.Direction, message.AttemptSequence, message.ConnectionGeneration,
		message.EnvelopeSequence, message.Kind, message.Fingerprint, message.CreatedAt,
		retainUntil, payload)
	return false, err
}

func sameAttachedWorkerAttemptMessage(left, right domain.AttachedWorkerAttemptMessageV1) bool {
	return left.Version == right.Version && left.TenantID == right.TenantID &&
		left.OwnerUserID == right.OwnerUserID && left.WorkerID == right.WorkerID &&
		left.AttemptID == right.AttemptID && left.Direction == right.Direction &&
		left.AttemptSequence == right.AttemptSequence &&
		left.ConnectionGeneration == right.ConnectionGeneration &&
		left.EnvelopeSequence == right.EnvelopeSequence && left.Kind == right.Kind &&
		left.Fingerprint == right.Fingerprint && bytes.Equal(left.Payload, right.Payload)
}

func upsertAttachedWorkerAttemptDeadlineTx(ctx context.Context, tx *stateTx, deadline domain.AttachedWorkerAttemptDeadlineV1) error {
	deadline.DeadlineAt = canonicalAttachedWorkerTime(deadline.DeadlineAt)
	if err := deadline.Validate(); err != nil {
		return err
	}
	_, err := tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO attached_worker_attempt_deadlines_v1
		 (shard_bucket,deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind,
		  lease_generation,attempt_revision)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		deadline.Bucket, deadline.DeadlineAt, deadline.TenantID, deadline.OwnerUserID,
		deadline.WorkerID, deadline.AttemptID, deadline.Kind, deadline.LeaseGeneration,
		deadline.AttemptRevision)
	return err
}

func deleteAttachedWorkerAttemptDeadlineTx(ctx context.Context, tx *stateTx, deadline domain.AttachedWorkerAttemptDeadlineV1) error {
	if err := deadline.Validate(); err != nil {
		return err
	}
	_, err := tx.sqlTx.ExecContext(ctx,
		`DELETE FROM attached_worker_attempt_deadlines_v1
		 WHERE shard_bucket=$1 AND deadline_at=$2 AND tenant_id=$3 AND owner_user_id=$4
		       AND worker_id=$5 AND attempt_id=$6 AND kind=$7`,
		deadline.Bucket, canonicalAttachedWorkerTime(deadline.DeadlineAt), deadline.TenantID,
		deadline.OwnerUserID, deadline.WorkerID, deadline.AttemptID, deadline.Kind)
	return err
}

func deleteAttachedWorkerAttemptDeadlinesTx(ctx context.Context, tx *stateTx, attempt domain.AttachedWorkerAttemptV1) error {
	bucket, err := domain.AttachedWorkerAttemptDeadlineBucketV1(attempt.TenantID, attempt.OwnerUserID, attempt.WorkerID, attempt.AttemptID)
	if err != nil {
		return err
	}
	lease := domain.AttachedWorkerAttemptDeadlineV1{
		Bucket: bucket, DeadlineAt: attempt.LeaseExpiresAt, TenantID: attempt.TenantID,
		OwnerUserID: attempt.OwnerUserID, WorkerID: attempt.WorkerID, AttemptID: attempt.AttemptID,
		Kind: domain.AttachedWorkerDeadlineLeaseExpiry, LeaseGeneration: attempt.LeaseGeneration,
		AttemptRevision: attempt.Revision,
	}
	if err := deleteAttachedWorkerAttemptDeadlineTx(ctx, tx, lease); err != nil {
		return err
	}
	if attempt.CancelDeadline.IsZero() {
		return nil
	}
	cancel := lease
	cancel.Kind = domain.AttachedWorkerDeadlineCancelAck
	cancel.DeadlineAt = attempt.CancelDeadline
	return deleteAttachedWorkerAttemptDeadlineTx(ctx, tx, cancel)
}

func validateAttachedWorkerAttemptDeadlineCursor(cursor ports.AttachedWorkerAttemptDeadlineCursor) error {
	if cursor.DeadlineAt.IsZero() {
		if cursor.TenantID != "" || cursor.OwnerUserID != "" || cursor.WorkerID != "" || cursor.AttemptID != "" || cursor.Kind != "" {
			return domain.ValidationError{Field: "attached_worker_attempt_deadline.cursor", Reason: "zero cursor must have empty routing fields"}
		}
		return nil
	}
	if err := cursor.TenantID.Validate(); err != nil {
		return err
	}
	if err := cursor.OwnerUserID.Validate(); err != nil {
		return err
	}
	if err := cursor.WorkerID.Validate(); err != nil {
		return err
	}
	if err := cursor.AttemptID.Validate(); err != nil {
		return err
	}
	if !cursor.Kind.Valid() {
		return domain.ValidationError{Field: "attached_worker_attempt_deadline.cursor.kind", Reason: "is unknown"}
	}
	return nil
}

func scanAttachedWorkerAttemptDeadline(rows *sql.Rows) (domain.AttachedWorkerAttemptDeadlineV1, error) {
	var value domain.AttachedWorkerAttemptDeadlineV1
	err := rows.Scan(&value.Bucket, &value.DeadlineAt, &value.TenantID, &value.OwnerUserID,
		&value.WorkerID, &value.AttemptID, &value.Kind, &value.LeaseGeneration, &value.AttemptRevision)
	if err != nil {
		return value, err
	}
	value.DeadlineAt = canonicalAttachedWorkerTime(value.DeadlineAt)
	if err := value.Validate(); err != nil {
		return domain.AttachedWorkerAttemptDeadlineV1{}, err
	}
	return value, nil
}

func readCanonicalLeaseHeadTx(ctx context.Context, tx *stateTx, runID domain.RunID) (domain.Lease, bool, error) {
	var leaseID, attemptID, workerID string
	var fence uint64
	var expiresAt time.Time
	err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT lease_id,attempt_id,worker_id,fence_token,expires_at
		 FROM lease_heads WHERE tenant_id=$1 AND run_id=$2`, tx.tenantID, runID,
	).Scan(&leaseID, &attemptID, &workerID, &fence, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Lease{}, false, nil
	}
	if err != nil {
		return domain.Lease{}, false, err
	}
	lease, found, err := readJSON[domain.Lease](ctx, tx.sqlTx,
		`SELECT payload FROM leases WHERE tenant_id=$1 AND lease_id=$2`, tx.tenantID, leaseID)
	if err != nil || !found {
		return domain.Lease{}, false, err
	}
	if lease.RunID != runID || string(lease.AttemptID) != attemptID || lease.WorkerID != workerID || lease.FenceToken != fence || !canonicalAttachedWorkerTime(lease.ExpiresAt).Equal(canonicalAttachedWorkerTime(expiresAt)) {
		return domain.Lease{}, false, ErrAttachedWorkerAttemptConflict
	}
	return lease, true, nil
}

func loadWorkerJobStateTx(ctx context.Context, tx *stateTx, runID domain.RunID) (ports.WorkerJobState, bool, error) {
	var result ports.WorkerJobState
	var err error
	var found bool
	result.Run, found, err = tx.GetRun(ctx, runID)
	if err != nil || !found {
		if errors.Is(err, sql.ErrNoRows) {
			return result, false, nil
		}
		return result, found, err
	}
	result.Job, found, err = readJSON[domain.WorkerJob](ctx, tx.sqlTx,
		`SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, tx.tenantID, runID)
	if err != nil || !found {
		return result, found, err
	}
	result.Attempt, found, err = tx.GetAttempt(ctx, result.Job.AttemptID)
	if err != nil || !found {
		return result, found, err
	}
	result.Reservation, found, err = readJSON[domain.QuotaReservation](ctx, tx.sqlTx,
		`SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2`,
		tx.tenantID, result.Job.ReservationID)
	if err != nil || !found {
		return result, found, err
	}
	result.InputManifest, found, err = readJSON[domain.ArtifactManifest](ctx, tx.sqlTx,
		`SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2`,
		tx.tenantID, result.Job.InputManifestID)
	if err != nil || !found {
		return result, found, err
	}
	if err := validateLoadedWorkerJob(result); err != nil {
		return ports.WorkerJobState{}, false, err
	}
	return result, true, nil
}

// allocateAttachedWorkerLeaseTx is the transaction-local counterpart of
// ClaimLease. It deliberately takes the database transaction clock and is
// composed with the durable offer, so no caller-chosen fence or timestamp can
// become protocol authority.
func allocateAttachedWorkerLeaseTx(ctx context.Context, tx *stateTx, runID domain.RunID, attemptID domain.AttemptID, leaseID domain.LeaseID, workerID domain.AttachedWorkerID, at, expiresAt time.Time) (domain.Lease, bool, error) {
	at, expiresAt = canonicalAttachedWorkerTime(at), canonicalAttachedWorkerTime(expiresAt)
	var currentLeaseID, currentAttemptID, currentWorker string
	var fence uint64
	var currentExpiry time.Time
	err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT lease_id,attempt_id,worker_id,fence_token,expires_at
		 FROM lease_heads WHERE tenant_id=$1 AND run_id=$2`, tx.tenantID, runID,
	).Scan(&currentLeaseID, &currentAttemptID, &currentWorker, &fence, &currentExpiry)
	switch {
	case err == nil && currentLeaseID == string(leaseID) && canonicalAttachedWorkerTime(currentExpiry).After(at):
		if currentAttemptID != string(attemptID) || currentWorker != string(workerID) {
			return domain.Lease{}, false, ErrAttachedWorkerAttemptConflict
		}
		lease, found, readErr := readCanonicalLeaseHeadTx(ctx, tx, runID)
		if readErr != nil || !found {
			return domain.Lease{}, false, readErr
		}
		return lease, true, nil
	case err == nil && canonicalAttachedWorkerTime(currentExpiry).After(at):
		return domain.Lease{}, false, fmt.Errorf("%w: run already has an active lease", ErrLeaseHeld)
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return domain.Lease{}, false, err
	}
	if err == nil {
		bucket, bucketErr := ydbpartition.BucketV1(string(runID))
		if bucketErr != nil {
			return domain.Lease{}, false, bucketErr
		}
		if _, deleteErr := tx.sqlTx.ExecContext(ctx,
			`DELETE FROM lease_expiry WHERE tenant_id=$1 AND expires_at=$2 AND run_id=$3`,
			tx.tenantID, currentExpiry, runID); deleteErr != nil {
			return domain.Lease{}, false, deleteErr
		}
		if _, deleteErr := tx.sqlTx.ExecContext(ctx,
			`DELETE FROM lease_expiry_v2 WHERE shard_bucket=$1 AND expires_at=$2 AND tenant_id=$3 AND run_id=$4`,
			bucket, currentExpiry, tx.tenantID, runID); deleteErr != nil {
			return domain.Lease{}, false, deleteErr
		}
	}
	if fence == math.MaxUint64 {
		return domain.Lease{}, false, domain.ValidationError{Field: "lease.fence_token", Reason: "cannot advance beyond uint64"}
	}
	lease := domain.Lease{ID: leaseID, TenantID: tx.tenantID, RunID: runID, AttemptID: attemptID,
		WorkerID: string(workerID), FenceToken: fence + 1, AcquiredAt: at, ExpiresAt: expiresAt}
	if err := tx.PutLease(ctx, lease); err != nil {
		return domain.Lease{}, false, err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO lease_heads
		 (tenant_id,run_id,lease_id,attempt_id,worker_id,fence_token,expires_at,updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, lease.TenantID, lease.RunID, lease.ID,
		lease.AttemptID, lease.WorkerID, lease.FenceToken, lease.ExpiresAt, at)
	return lease, false, err
}

func loadAttachedWorkerProtocolAuthorityTx(ctx context.Context, tx *stateTx, worker domain.AttachedWorker, connection domain.AttachedWorkerConnection) (attachedworkerprotocol.MachineConfig, attachedworkerprotocol.MachineSnapshotV1, error) {
	_ = ctx
	_ = tx
	snapshot, err := attachedworkerprotocol.DecodeMachineSnapshotV1(connection.ProtocolSnapshot)
	if err != nil || snapshot.Hello == nil || snapshot.Challenge == nil {
		return attachedworkerprotocol.MachineConfig{}, attachedworkerprotocol.MachineSnapshotV1{}, ErrAttachedWorkerAttemptConflict
	}
	channelBinding, err := hex.DecodeString(string(connection.ChannelBinding))
	if err != nil {
		return attachedworkerprotocol.MachineConfig{}, attachedworkerprotocol.MachineSnapshotV1{}, ErrAttachedWorkerAttemptConflict
	}
	config := attachedworkerprotocol.MachineConfig{
		Auth: attachedworkerprotocol.AuthContextV1{
			TenantID: string(worker.TenantID), OwnerUserID: string(worker.OwnerUserID), WorkerID: string(worker.ID),
			IdentityPublicKey: append([]byte(nil), worker.IdentityPublicKey...), EnrollmentGeneration: connection.EnrollmentGeneration,
			ConnectionGeneration: connection.ConnectionGeneration, Version: attachedworkerprotocol.ProtocolVersion(connection.ProtocolVersion),
			ChannelBinding: channelBinding,
		},
		WorkerOffer: snapshot.Hello.Offer, PlatformOffer: snapshot.Challenge.PlatformOffer,
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
	}
	machine, err := attachedworkerprotocol.RestoreConformanceMachine(config, snapshot)
	if err != nil || snapshot.Platform.Sequence != connection.PlatformSequence || snapshot.Platform.Ack != connection.PlatformAck ||
		snapshot.Worker.Sequence != connection.WorkerSequence || snapshot.Worker.Ack != connection.WorkerAck ||
		!attachedWorkerProtocolStateMatches(connection.State, machine.ConnectionState()) {
		return attachedworkerprotocol.MachineConfig{}, attachedworkerprotocol.MachineSnapshotV1{}, ErrAttachedWorkerAttemptConflict
	}
	return config, snapshot, nil
}

func attachedWorkerProtocolStateMatches(connection domain.AttachedWorkerConnectionState, machine attachedworkerprotocol.ConnectionState) bool {
	switch connection {
	case domain.AttachedWorkerConnectionAttaching:
		return machine == attachedworkerprotocol.ConnectionAttached
	case domain.AttachedWorkerConnectionOnline:
		return machine == attachedworkerprotocol.ConnectionReady
	case domain.AttachedWorkerConnectionDraining:
		return machine == attachedworkerprotocol.ConnectionDraining || machine == attachedworkerprotocol.ConnectionDrained
	case domain.AttachedWorkerConnectionRevoked:
		return machine == attachedworkerprotocol.ConnectionRevoked
	case domain.AttachedWorkerConnectionOffline:
		return machine == attachedworkerprotocol.ConnectionReady || machine == attachedworkerprotocol.ConnectionDrained
	default:
		return false
	}
}

func canonicalAttachedWorkerProtocolSnapshot(snapshot attachedworkerprotocol.MachineSnapshotV1) ([]byte, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	encoded, err := attachedworkerprotocol.EncodeMachineSnapshotV1(snapshot)
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 || len(encoded) > 64<<10 {
		return nil, domain.ValidationError{Field: "attached_worker_connection.protocol_snapshot", Reason: "exceeds the 64 KiB bound"}
	}
	return encoded, nil
}

func decodeAttachedWorkerAttemptFrame(message domain.AttachedWorkerAttemptMessageV1) (attachedworkerprotocol.FrameV1, attachedworkerprotocol.Direction, error) {
	if err := message.Validate(); err != nil {
		return attachedworkerprotocol.FrameV1{}, "", err
	}
	batch, err := attachedworkerprotocol.DecodeBatchV1(message.Payload)
	if err != nil || len(batch.Frames) != 1 {
		return attachedworkerprotocol.FrameV1{}, "", ErrAttachedWorkerAttemptMessageConflict
	}
	frame := batch.Frames[0]
	direction := attachedworkerprotocol.DirectionPlatformToWorker
	if message.Direction == domain.AttachedWorkerAttemptWorkerToPlatform {
		direction = attachedworkerprotocol.DirectionWorkerToPlatform
	}
	wantKind, attemptSequence, ok := attachedWorkerFrameIdentity(frame)
	if !ok || wantKind != message.Kind || frame.Sequence != message.EnvelopeSequence ||
		frame.ConnectionGeneration != message.ConnectionGeneration || attemptSequence != message.AttemptSequence {
		return attachedworkerprotocol.FrameV1{}, "", ErrAttachedWorkerAttemptMessageConflict
	}
	fingerprint, err := attachedworkerprotocol.AttemptFrameFingerprintV1(frame)
	if err != nil || message.Fingerprint != domain.AttachedWorkerAttemptMessageFingerprint(hex.EncodeToString(fingerprint)) {
		return attachedworkerprotocol.FrameV1{}, "", ErrAttachedWorkerAttemptMessageConflict
	}
	return frame, direction, nil
}

func attachedWorkerAttemptMessageFromFrame(scope domain.AttachedWorkerAttemptV1, direction attachedworkerprotocol.Direction, frame attachedworkerprotocol.FrameV1, at time.Time) (domain.AttachedWorkerAttemptMessageV1, error) {
	kind, attemptSequence, ok := attachedWorkerFrameIdentity(frame)
	if !ok {
		return domain.AttachedWorkerAttemptMessageV1{}, ErrAttachedWorkerAttemptMessageConflict
	}
	payload, err := attachedworkerprotocol.EncodeBatchV1(attachedworkerprotocol.BatchV1{Version: frame.Version, Frames: []attachedworkerprotocol.FrameV1{frame}})
	if err != nil {
		return domain.AttachedWorkerAttemptMessageV1{}, err
	}
	fingerprint, err := attachedworkerprotocol.AttemptFrameFingerprintV1(frame)
	if err != nil {
		return domain.AttachedWorkerAttemptMessageV1{}, err
	}
	domainDirection := domain.AttachedWorkerAttemptPlatformToWorker
	if direction == attachedworkerprotocol.DirectionWorkerToPlatform {
		domainDirection = domain.AttachedWorkerAttemptWorkerToPlatform
	}
	message := domain.AttachedWorkerAttemptMessageV1{
		Version: domain.AttachedWorkerAttemptMessageVersionV1, TenantID: scope.TenantID,
		OwnerUserID: scope.OwnerUserID, WorkerID: scope.WorkerID, AttemptID: scope.AttemptID,
		Direction: domainDirection, AttemptSequence: attemptSequence,
		ConnectionGeneration: frame.ConnectionGeneration, EnvelopeSequence: frame.Sequence, Kind: kind,
		Fingerprint: domain.AttachedWorkerAttemptMessageFingerprint(hex.EncodeToString(fingerprint)),
		Payload:     payload, CreatedAt: canonicalAttachedWorkerTime(at),
	}
	return message, message.Validate()
}

func canonicalAttemptMessageTime(message domain.AttachedWorkerAttemptMessageV1, at time.Time) domain.AttachedWorkerAttemptMessageV1 {
	message.Payload = append([]byte(nil), message.Payload...)
	message.CreatedAt = canonicalAttachedWorkerTime(at)
	return message
}

func attachedWorkerFrameIdentity(frame attachedworkerprotocol.FrameV1) (domain.AttachedWorkerAttemptMessageKind, uint64, bool) {
	switch frame.Kind {
	case attachedworkerprotocol.MessageLeaseOffer:
		if frame.LeaseOffer != nil {
			return domain.AttachedWorkerAttemptMessageLeaseOffered, frame.LeaseOffer.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageLeaseClaim:
		if frame.LeaseClaim != nil {
			return domain.AttachedWorkerAttemptMessageLeaseClaim, frame.LeaseClaim.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageLeaseAccepted:
		if frame.LeaseAccepted != nil {
			return domain.AttachedWorkerAttemptMessageLeaseAccepted, frame.LeaseAccepted.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageProgress:
		if frame.Progress != nil {
			return domain.AttachedWorkerAttemptMessageProgress, frame.Progress.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageCancel:
		if frame.Cancel != nil {
			return domain.AttachedWorkerAttemptMessageCancelRequested, frame.Cancel.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageCancelAck:
		if frame.CancelAck != nil {
			return domain.AttachedWorkerAttemptMessageCancelAcknowledged, frame.CancelAck.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageTerminal:
		if frame.Terminal != nil {
			return domain.AttachedWorkerAttemptMessageTerminal, frame.Terminal.AttemptSequence, true
		}
	case attachedworkerprotocol.MessageTerminalAck:
		if frame.TerminalAck != nil {
			return domain.AttachedWorkerAttemptMessageTerminalCommitted, frame.TerminalAck.AttemptSequence, true
		}
	}
	return "", 0, false
}

func attachedWorkerFrameBinding(frame attachedworkerprotocol.FrameV1) (attachedworkerprotocol.AttemptBindingV1, bool) {
	switch frame.Kind {
	case attachedworkerprotocol.MessageLeaseOffer:
		if frame.LeaseOffer != nil {
			return frame.LeaseOffer.Binding, true
		}
	case attachedworkerprotocol.MessageLeaseClaim:
		if frame.LeaseClaim != nil {
			return frame.LeaseClaim.Binding, true
		}
	case attachedworkerprotocol.MessageLeaseAccepted:
		if frame.LeaseAccepted != nil {
			return frame.LeaseAccepted.Binding, true
		}
	case attachedworkerprotocol.MessageProgress:
		if frame.Progress != nil {
			return frame.Progress.Binding, true
		}
	case attachedworkerprotocol.MessageCancel:
		if frame.Cancel != nil {
			return frame.Cancel.Binding, true
		}
	case attachedworkerprotocol.MessageCancelAck:
		if frame.CancelAck != nil {
			return frame.CancelAck.Binding, true
		}
	case attachedworkerprotocol.MessageTerminal:
		if frame.Terminal != nil {
			return frame.Terminal.Binding, true
		}
	case attachedworkerprotocol.MessageTerminalAck:
		if frame.TerminalAck != nil {
			return frame.TerminalAck.Binding, true
		}
	}
	return attachedworkerprotocol.AttemptBindingV1{}, false
}

func advanceAttachedWorkerConnectionProtocol(connection domain.AttachedWorkerConnection, snapshot attachedworkerprotocol.MachineSnapshotV1) (domain.AttachedWorkerConnection, error) {
	encoded, err := canonicalAttachedWorkerProtocolSnapshot(snapshot)
	if err != nil {
		return domain.AttachedWorkerConnection{}, err
	}
	connection.PlatformSequence, connection.PlatformAck = snapshot.Platform.Sequence, snapshot.Platform.Ack
	connection.WorkerSequence, connection.WorkerAck = snapshot.Worker.Sequence, snapshot.Worker.Ack
	connection.ProtocolSnapshot = encoded
	switch snapshot.Connection {
	case attachedworkerprotocol.ConnectionReady:
		connection.State = domain.AttachedWorkerConnectionOnline
	case attachedworkerprotocol.ConnectionDraining, attachedworkerprotocol.ConnectionDrained:
		connection.State = domain.AttachedWorkerConnectionDraining
	case attachedworkerprotocol.ConnectionRevoked:
		connection.State = domain.AttachedWorkerConnectionRevoked
	default:
		return domain.AttachedWorkerConnection{}, ErrAttachedWorkerConnectionConflict
	}
	connection.Revision++
	return connection, connection.Validate()
}
