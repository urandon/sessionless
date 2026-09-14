package ydbstore

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrAttachedWorkerDrainConflict          = errors.New("attached worker drain conflicts with existing state")
	ErrAttachedWorkerControlMessageConflict = errors.New("attached worker control message conflicts with existing state")
)

func (store *Store) RequestAttachedWorkerDrain(ctx context.Context, request ports.AttachedWorkerDrainRequest) (result ports.AttachedWorkerDrainResult, err error) {
	if err := validateAttachedWorkerDrainRequest(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		worker, found, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found {
			result.Status = ports.AttachedWorkerExecutionNotFound
			return nil
		}
		connection, connectionFound, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !connectionFound {
			result.Status = ports.AttachedWorkerExecutionNotFound
			return nil
		}
		if worker.Revision != request.ExpectedWorkerRevision {
			return reconcileAttachedWorkerDrainRequestTx(ctx, tx, request, worker, connection, &result)
		}
		if worker.Revision == math.MaxUint64 || connection.Revision == math.MaxUint64 ||
			worker.DesiredState != domain.AttachedWorkerDesiredActive || worker.ObservedState != domain.AttachedWorkerObservedOnline ||
			connection.State != domain.AttachedWorkerConnectionOnline || !attachedWorkerDrainScopeCurrent(worker, connection) {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		config, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
		if err != nil || snapshot.Connection != attachedworkerprotocol.ConnectionReady {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		drainRevision := worker.Revision + 1
		message := domain.AttachedWorkerControlMessageV1{
			Version: domain.AttachedWorkerControlMessageVersionV1, TenantID: worker.TenantID,
			OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID, DrainRevision: drainRevision,
			Direction: domain.AttachedWorkerAttemptPlatformToWorker, Kind: domain.AttachedWorkerControlMessageDrain,
			CreatedAt: at,
		}
		nextWorker := worker
		nextWorker.DesiredState = domain.AttachedWorkerDesiredDrain
		nextWorker.Revision, nextWorker.UpdatedAt = drainRevision, at
		audit := attachedWorkerDrainAudit(nextWorker, domain.AttachedWorkerAuditDrainRequested, at)
		if snapshot.Worker.Ack >= snapshot.Platform.Sequence {
			frame, post, buildErr := attachedworkerprotocol.BuildDrainTransitionV1(config, snapshot,
				attachedworkerprotocol.DrainAuthorityV1{Revision: drainRevision, NowUnixMicro: at.UnixMicro()})
			if buildErr != nil {
				return ErrAttachedWorkerDrainConflict
			}
			message, err = attachedWorkerControlMessageFromFrame(worker, attachedworkerprotocol.DirectionPlatformToWorker, frame, at)
			if err != nil {
				return err
			}
			oldExpiry := attachedWorkerPresenceExpiry(connection)
			connection, err = advanceAttachedWorkerConnectionProtocol(connection, post)
			if err != nil {
				return err
			}
			nextWorker.ObservedState = domain.AttachedWorkerObservedDraining
			if err := replaceAttachedWorkerPresenceConnectionTx(ctx, tx, oldExpiry, connection); err != nil {
				return err
			}
			result.Outbound = &message
		}
		if err := insertOrReconcileAttachedWorkerControlMessageTx(ctx, tx, message, at.Add(store.operationalRetention)); err != nil {
			return err
		}
		if err := nextWorker.Validate(); err != nil {
			return err
		}
		if err := updateAttachedWorkerTx(ctx, tx, nextWorker); err != nil {
			return err
		}
		if err := insertAttachedWorkerAuditEventTx(ctx, tx, audit); err != nil {
			return err
		}
		result.Status, result.Worker, result.Connection = ports.AttachedWorkerExecutionApplied, nextWorker, connection
		return nil
	})
	return result, err
}

func (store *Store) PollAttachedWorkerControl(ctx context.Context, request ports.AttachedWorkerControlPoll) (result ports.AttachedWorkerDrainResult, err error) {
	if err := validateAttachedWorkerControlPoll(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		worker, found, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		connection, connectionFound, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || !connectionFound || !attachedWorkerControlBearerAuthorized(request.ConnectionID, request.PresentedSecretDigest, at, worker, connection) {
			result.Status = ports.AttachedWorkerExecutionDenied
			return nil
		}
		if worker.DesiredState != domain.AttachedWorkerDesiredDrain {
			result = ports.AttachedWorkerDrainResult{Status: ports.AttachedWorkerExecutionNotFound, Worker: worker, Connection: connection}
			return nil
		}
		message, found, err := readLatestAttachedWorkerDrainMessageTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found {
			result.Status = ports.AttachedWorkerExecutionNotFound
			return nil
		}
		config, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
		if err != nil {
			return err
		}
		if message.ConnectionGeneration == 0 {
			if snapshot.Connection != attachedworkerprotocol.ConnectionReady || snapshot.Worker.Ack < snapshot.Platform.Sequence {
				result = ports.AttachedWorkerDrainResult{Status: ports.AttachedWorkerExecutionApplied, Worker: worker, Connection: connection}
				return nil
			}
			frame, post, buildErr := attachedworkerprotocol.BuildDrainTransitionV1(config, snapshot,
				attachedworkerprotocol.DrainAuthorityV1{Revision: message.DrainRevision, NowUnixMicro: at.UnixMicro()})
			if buildErr != nil {
				return ErrAttachedWorkerDrainConflict
			}
			nextMessage, buildErr := attachedWorkerControlMessageFromFrame(worker, attachedworkerprotocol.DirectionPlatformToWorker, frame, message.CreatedAt)
			if buildErr != nil {
				return buildErr
			}
			oldExpiry := attachedWorkerPresenceExpiry(connection)
			connection, err = advanceAttachedWorkerConnectionProtocol(connection, post)
			if err != nil {
				return err
			}
			if err := replaceAttachedWorkerPresenceConnectionTx(ctx, tx, oldExpiry, connection); err != nil {
				return err
			}
			if worker.Revision == math.MaxUint64 {
				return ErrAttachedWorkerDrainConflict
			}
			nextWorker := worker
			nextWorker.ObservedState, nextWorker.Revision, nextWorker.UpdatedAt = domain.AttachedWorkerObservedDraining, worker.Revision+1, at
			if err := updateAttachedWorkerTx(ctx, tx, nextWorker); err != nil {
				return err
			}
			if err := insertAttachedWorkerAuditEventTx(ctx, tx, attachedWorkerDrainAudit(nextWorker, domain.AttachedWorkerAuditDrainStarted, at)); err != nil {
				return err
			}
			if err := replaceAttachedWorkerControlMessageEnvelopeTx(ctx, tx, message, nextMessage, at.Add(store.operationalRetention)); err != nil {
				return err
			}
			worker, message, snapshot = nextWorker, nextMessage, post
		} else if message.ConnectionGeneration < connection.ConnectionGeneration {
			if snapshot.Connection != attachedworkerprotocol.ConnectionDraining || snapshot.Worker.Ack < snapshot.Platform.Sequence {
				return ErrAttachedWorkerDrainConflict
			}
			frame, post, buildErr := attachedworkerprotocol.BuildDrainTransitionV1(config, snapshot,
				attachedworkerprotocol.DrainAuthorityV1{Revision: message.DrainRevision,
					DeliveredConnectionGeneration: message.ConnectionGeneration, NowUnixMicro: at.UnixMicro()})
			if buildErr != nil {
				return ErrAttachedWorkerDrainConflict
			}
			nextMessage, buildErr := attachedWorkerControlMessageFromFrame(worker, attachedworkerprotocol.DirectionPlatformToWorker, frame, message.CreatedAt)
			if buildErr != nil {
				return buildErr
			}
			oldExpiry := attachedWorkerPresenceExpiry(connection)
			connection, err = advanceAttachedWorkerConnectionProtocol(connection, post)
			if err != nil {
				return err
			}
			if err := replaceAttachedWorkerPresenceConnectionTx(ctx, tx, oldExpiry, connection); err != nil {
				return err
			}
			if err := replaceAttachedWorkerControlMessageEnvelopeTx(ctx, tx, message, nextMessage, at.Add(store.operationalRetention)); err != nil {
				return err
			}
			message, snapshot = nextMessage, post
		} else if message.ConnectionGeneration > connection.ConnectionGeneration {
			return ErrAttachedWorkerControlMessageConflict
		}
		frame, direction, decodeErr := decodeAttachedWorkerControlFrame(message)
		if decodeErr != nil || direction != attachedworkerprotocol.DirectionPlatformToWorker ||
			frame.Drain == nil || frame.Drain.Revision != message.DrainRevision || frame.Sequence > snapshot.Platform.Sequence {
			return ErrAttachedWorkerControlMessageConflict
		}
		result = ports.AttachedWorkerDrainResult{Status: ports.AttachedWorkerExecutionApplied, Worker: worker, Connection: connection}
		if snapshot.Worker.Ack < frame.Sequence {
			result.Outbound = &message
		}
		return nil
	})
	return result, err
}

func (store *Store) ExchangeAttachedWorkerControl(ctx context.Context, request ports.AttachedWorkerControlExchange) (result ports.AttachedWorkerDrainResult, err error) {
	if err := validateAttachedWorkerControlExchange(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		worker, found, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		connection, connectionFound, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || !connectionFound || !attachedWorkerControlAuthorized(request.ConnectionID, request.PresentedSecretDigest, at, worker, connection) {
			result.Status = ports.AttachedWorkerExecutionDenied
			return nil
		}
		inbound, buildErr := attachedWorkerControlMessageFromFrame(worker, attachedworkerprotocol.DirectionWorkerToPlatform, request.InboundFrame, at)
		if buildErr != nil {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		existing, replayed, err := readAttachedWorkerControlMessageTx(ctx, tx, inbound)
		if err != nil {
			return err
		}
		if replayed {
			if !sameAttachedWorkerControlEnvelope(existing, inbound) {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			result = ports.AttachedWorkerDrainResult{Status: ports.AttachedWorkerExecutionReplayed, Worker: worker, Connection: connection}
			return nil
		}
		outboundScope := inbound
		outboundScope.Direction = domain.AttachedWorkerAttemptPlatformToWorker
		outbound, outboundFound, err := readAttachedWorkerControlMessageTx(ctx, tx, outboundScope)
		if err != nil {
			return err
		}
		if !outboundFound || outbound.ConnectionGeneration == 0 || outbound.ConnectionGeneration != connection.ConnectionGeneration {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		config, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
		if err != nil {
			return err
		}
		attempt, attemptFound, err := readAttachedWorkerAttemptTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if (attemptFound && attempt.State != domain.AttachedWorkerAttemptRetired) || snapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptIdle ||
			worker.Revision == math.MaxUint64 || connection.Revision == math.MaxUint64 {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		post, applyErr := attachedworkerprotocol.ApplyMachineFrameV1(config, snapshot,
			attachedworkerprotocol.DirectionWorkerToPlatform, request.InboundFrame, at.UnixMicro())
		if applyErr != nil || post.Connection != attachedworkerprotocol.ConnectionDrained {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		oldExpiry := attachedWorkerPresenceExpiry(connection)
		connection, err = advanceAttachedWorkerConnectionProtocol(connection, post)
		if err != nil {
			return err
		}
		if err := replaceAttachedWorkerPresenceConnectionTx(ctx, tx, oldExpiry, connection); err != nil {
			return err
		}
		nextWorker := worker
		nextWorker.Revision, nextWorker.UpdatedAt = worker.Revision+1, at
		if err := updateAttachedWorkerTx(ctx, tx, nextWorker); err != nil {
			return err
		}
		if err := insertAttachedWorkerAuditEventTx(ctx, tx, attachedWorkerDrainAudit(nextWorker, domain.AttachedWorkerAuditDrained, at)); err != nil {
			return err
		}
		if err := insertOrReconcileAttachedWorkerControlMessageTx(ctx, tx, inbound, at.Add(store.operationalRetention)); err != nil {
			return err
		}
		result = ports.AttachedWorkerDrainResult{Status: ports.AttachedWorkerExecutionApplied, Worker: nextWorker, Connection: connection}
		return nil
	})
	return result, err
}

func reconcileAttachedWorkerDrainRequestTx(ctx context.Context, tx *stateTx, request ports.AttachedWorkerDrainRequest, worker domain.AttachedWorker, connection domain.AttachedWorkerConnection, result *ports.AttachedWorkerDrainResult) error {
	drainRevision := request.ExpectedWorkerRevision + 1
	scope := domain.AttachedWorkerControlMessageV1{OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		DrainRevision: drainRevision, Direction: domain.AttachedWorkerAttemptPlatformToWorker}
	message, found, err := readAttachedWorkerControlMessageTx(ctx, tx, scope)
	if err != nil {
		return err
	}
	audit, auditFound, err := readAttachedWorkerAuditEventTx(ctx, tx, request.OwnerUserID, request.WorkerID, drainRevision)
	if err != nil {
		return err
	}
	if !found || !auditFound || worker.DesiredState != domain.AttachedWorkerDesiredDrain || worker.Revision < drainRevision ||
		audit.Action != domain.AttachedWorkerAuditDrainRequested || audit.WorkerRevision != drainRevision ||
		!audit.OccurredAt.Equal(message.CreatedAt) {
		result.Status = ports.AttachedWorkerExecutionConflict
		return nil
	}
	result.Status, result.Worker, result.Connection = ports.AttachedWorkerExecutionReplayed, worker, connection
	if message.ConnectionGeneration > 0 {
		result.Outbound = &message
	}
	return nil
}

func attachedWorkerControlMessageFromFrame(worker domain.AttachedWorker, direction attachedworkerprotocol.Direction, frame attachedworkerprotocol.FrameV1, at time.Time) (domain.AttachedWorkerControlMessageV1, error) {
	var revision uint64
	kind := domain.AttachedWorkerControlMessageKind("")
	domainDirection := domain.AttachedWorkerAttemptPlatformToWorker
	if direction == attachedworkerprotocol.DirectionPlatformToWorker && frame.Kind == attachedworkerprotocol.MessageDrain && frame.Drain != nil {
		revision, kind = frame.Drain.Revision, domain.AttachedWorkerControlMessageDrain
	} else if direction == attachedworkerprotocol.DirectionWorkerToPlatform && frame.Kind == attachedworkerprotocol.MessageDrained && frame.Drained != nil {
		revision, kind, domainDirection = frame.Drained.Revision, domain.AttachedWorkerControlMessageDrained, domain.AttachedWorkerAttemptWorkerToPlatform
	} else {
		return domain.AttachedWorkerControlMessageV1{}, ErrAttachedWorkerControlMessageConflict
	}
	payload, err := attachedworkerprotocol.EncodeBatchV1(attachedworkerprotocol.BatchV1{Version: frame.Version, Frames: []attachedworkerprotocol.FrameV1{frame}})
	if err != nil {
		return domain.AttachedWorkerControlMessageV1{}, err
	}
	fingerprint, err := attachedworkerprotocol.FrameFingerprintV1(frame)
	if err != nil {
		return domain.AttachedWorkerControlMessageV1{}, err
	}
	message := domain.AttachedWorkerControlMessageV1{
		Version: domain.AttachedWorkerControlMessageVersionV1, TenantID: worker.TenantID,
		OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID, DrainRevision: revision,
		Direction: domainDirection, ConnectionGeneration: frame.ConnectionGeneration,
		EnvelopeSequence: frame.Sequence, Kind: kind,
		Fingerprint: domain.AttachedWorkerAttemptMessageFingerprint(hex.EncodeToString(fingerprint)),
		Payload:     payload, CreatedAt: canonicalAttachedWorkerTime(at),
	}
	return message, message.Validate()
}

func decodeAttachedWorkerControlFrame(message domain.AttachedWorkerControlMessageV1) (attachedworkerprotocol.FrameV1, attachedworkerprotocol.Direction, error) {
	if message.Validate() != nil || message.ConnectionGeneration == 0 {
		return attachedworkerprotocol.FrameV1{}, "", ErrAttachedWorkerControlMessageConflict
	}
	batch, err := attachedworkerprotocol.DecodeBatchV1(message.Payload)
	if err != nil || len(batch.Frames) != 1 {
		return attachedworkerprotocol.FrameV1{}, "", ErrAttachedWorkerControlMessageConflict
	}
	frame := batch.Frames[0]
	direction := attachedworkerprotocol.DirectionPlatformToWorker
	if message.Direction == domain.AttachedWorkerAttemptWorkerToPlatform {
		direction = attachedworkerprotocol.DirectionWorkerToPlatform
	}
	fingerprint, err := attachedworkerprotocol.FrameFingerprintV1(frame)
	if err != nil || frame.ConnectionGeneration != message.ConnectionGeneration || frame.Sequence != message.EnvelopeSequence ||
		message.Fingerprint != domain.AttachedWorkerAttemptMessageFingerprint(hex.EncodeToString(fingerprint)) {
		return attachedworkerprotocol.FrameV1{}, "", ErrAttachedWorkerControlMessageConflict
	}
	return frame, direction, nil
}

func readAttachedWorkerControlMessageTx(ctx context.Context, tx *stateTx, scope domain.AttachedWorkerControlMessageV1) (domain.AttachedWorkerControlMessageV1, bool, error) {
	value, found, err := readJSON[domain.AttachedWorkerControlMessageV1](ctx, tx.sqlTx,
		`SELECT record FROM attached_worker_control_messages
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND drain_revision=$4 AND direction=$5`,
		tx.tenantID, scope.OwnerUserID, scope.WorkerID, scope.DrainRevision, scope.Direction)
	if err != nil || !found {
		return value, found, err
	}
	if value.Validate() != nil || value.TenantID != tx.tenantID || value.OwnerUserID != scope.OwnerUserID ||
		value.WorkerID != scope.WorkerID || value.DrainRevision != scope.DrainRevision || value.Direction != scope.Direction {
		return domain.AttachedWorkerControlMessageV1{}, false, ErrAttachedWorkerControlMessageConflict
	}
	return value, true, nil
}

func readLatestAttachedWorkerDrainMessageTx(ctx context.Context, tx *stateTx, owner domain.UserID, worker domain.AttachedWorkerID) (domain.AttachedWorkerControlMessageV1, bool, error) {
	rows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT record FROM attached_worker_control_messages
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND direction=$4
		 ORDER BY drain_revision DESC LIMIT 2`, tx.tenantID, owner, worker, domain.AttachedWorkerAttemptPlatformToWorker)
	if err != nil {
		return domain.AttachedWorkerControlMessageV1{}, false, err
	}
	defer rows.Close()
	var result domain.AttachedWorkerControlMessageV1
	count := 0
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return result, false, err
		}
		count++
		if count == 1 {
			if err := json.Unmarshal([]byte(payload), &result); err != nil {
				return result, false, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return result, false, err
	}
	if count == 0 {
		return result, false, nil
	}
	if result.Validate() != nil || result.TenantID != tx.tenantID || result.OwnerUserID != owner || result.WorkerID != worker ||
		result.Direction != domain.AttachedWorkerAttemptPlatformToWorker || result.Kind != domain.AttachedWorkerControlMessageDrain {
		return domain.AttachedWorkerControlMessageV1{}, false, ErrAttachedWorkerControlMessageConflict
	}
	return result, true, nil
}

func insertOrReconcileAttachedWorkerControlMessageTx(ctx context.Context, tx *stateTx, message domain.AttachedWorkerControlMessageV1, retainUntil time.Time) error {
	message.CreatedAt, retainUntil = canonicalAttachedWorkerTime(message.CreatedAt), canonicalAttachedWorkerTime(retainUntil)
	if message.Validate() != nil || !retainUntil.After(message.CreatedAt) {
		return ErrAttachedWorkerControlMessageConflict
	}
	existing, found, err := readAttachedWorkerControlMessageTx(ctx, tx, message)
	if err != nil {
		return err
	}
	if found {
		if !sameAttachedWorkerControlMessage(existing, message) {
			return ErrAttachedWorkerControlMessageConflict
		}
		return nil
	}
	payload, err := marshal(message)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO attached_worker_control_messages
		 (tenant_id,owner_user_id,worker_id,drain_revision,direction,kind,created_at,retention_expire_at,record)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS JsonDocument))`,
		message.TenantID, message.OwnerUserID, message.WorkerID, message.DrainRevision, message.Direction,
		message.Kind, message.CreatedAt, retainUntil, payload)
	return err
}

func replaceAttachedWorkerControlMessageEnvelopeTx(ctx context.Context, tx *stateTx, previous, next domain.AttachedWorkerControlMessageV1, retainUntil time.Time) error {
	retainUntil = canonicalAttachedWorkerTime(retainUntil)
	advancesEnvelope := previous.ConnectionGeneration == 0 || next.ConnectionGeneration > previous.ConnectionGeneration
	if previous.Validate() != nil || next.Validate() != nil || !sameAttachedWorkerControlSemantic(previous, next) ||
		next.ConnectionGeneration == 0 || !advancesEnvelope || !retainUntil.After(next.CreatedAt) {
		return ErrAttachedWorkerControlMessageConflict
	}
	current, found, err := readAttachedWorkerControlMessageTx(ctx, tx, previous)
	if err != nil {
		return err
	}
	if !found || !sameAttachedWorkerControlMessage(current, previous) {
		return ErrAttachedWorkerControlMessageConflict
	}
	payload, err := marshal(next)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO attached_worker_control_messages
		 (tenant_id,owner_user_id,worker_id,drain_revision,direction,kind,created_at,retention_expire_at,record)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS JsonDocument))`,
		next.TenantID, next.OwnerUserID, next.WorkerID, next.DrainRevision, next.Direction,
		next.Kind, next.CreatedAt, retainUntil, payload)
	return err
}

func replaceAttachedWorkerPresenceConnectionTx(ctx context.Context, tx *stateTx, old domain.AttachedWorkerPresenceExpiry, connection domain.AttachedWorkerConnection) error {
	if err := deleteAttachedWorkerPresenceExpiryTx(ctx, tx, old); err != nil {
		return err
	}
	if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
		return err
	}
	return insertAttachedWorkerPresenceExpiryTx(ctx, tx, attachedWorkerPresenceExpiry(connection))
}

func sameAttachedWorkerControlSemantic(left, right domain.AttachedWorkerControlMessageV1) bool {
	return left.Version == right.Version && left.TenantID == right.TenantID && left.OwnerUserID == right.OwnerUserID &&
		left.WorkerID == right.WorkerID && left.DrainRevision == right.DrainRevision && left.Direction == right.Direction &&
		left.Kind == right.Kind && left.CreatedAt.Equal(right.CreatedAt)
}

func sameAttachedWorkerControlEnvelope(left, right domain.AttachedWorkerControlMessageV1) bool {
	return left.ConnectionGeneration == right.ConnectionGeneration && left.EnvelopeSequence == right.EnvelopeSequence &&
		left.Fingerprint == right.Fingerprint && bytes.Equal(left.Payload, right.Payload)
}

func sameAttachedWorkerControlMessage(left, right domain.AttachedWorkerControlMessageV1) bool {
	return sameAttachedWorkerControlSemantic(left, right) && sameAttachedWorkerControlEnvelope(left, right)
}

func attachedWorkerDrainAudit(worker domain.AttachedWorker, action domain.AttachedWorkerAuditAction, at time.Time) domain.AttachedWorkerAuditEvent {
	return domain.AttachedWorkerAuditEvent{
		Version: domain.AttachedWorkerAuditEventVersionV1, TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID,
		WorkerID: worker.ID, Action: action, WorkerRevision: worker.Revision,
		EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: worker.ConnectionGeneration,
		OccurredAt: canonicalAttachedWorkerTime(at),
	}
}

func attachedWorkerDrainScopeCurrent(worker domain.AttachedWorker, connection domain.AttachedWorkerConnection) bool {
	return worker.TenantID == connection.TenantID && worker.OwnerUserID == connection.OwnerUserID && worker.ID == connection.WorkerID &&
		worker.EnrollmentGeneration == connection.EnrollmentGeneration && worker.ConnectionGeneration == connection.ConnectionGeneration
}

func attachedWorkerControlAuthorized(connectionID domain.AttachedWorkerConnectionID, digest domain.AttachedWorkerConnectionSecretDigest, at time.Time, worker domain.AttachedWorker, connection domain.AttachedWorkerConnection) bool {
	return worker.DesiredState == domain.AttachedWorkerDesiredDrain &&
		attachedWorkerControlBearerAuthorized(connectionID, digest, at, worker, connection)
}

func attachedWorkerControlBearerAuthorized(connectionID domain.AttachedWorkerConnectionID, digest domain.AttachedWorkerConnectionSecretDigest, at time.Time, worker domain.AttachedWorker, connection domain.AttachedWorkerConnection) bool {
	return attachedWorkerDrainScopeCurrent(worker, connection) && worker.DesiredState != domain.AttachedWorkerDesiredRevoked &&
		connection.ID == connectionID && (connection.State == domain.AttachedWorkerConnectionOnline || connection.State == domain.AttachedWorkerConnectionDraining) &&
		subtle.ConstantTimeCompare([]byte(connection.SecretDigest), []byte(digest)) == 1 && at.Before(connection.AuthExpiresAt) && at.Before(connection.PresenceExpiresAt)
}

func validateAttachedWorkerDrainRequest(request ports.AttachedWorkerDrainRequest) error {
	if request.ExpectedWorkerRevision == 0 || request.ExpectedWorkerRevision == math.MaxUint64 {
		return domain.ValidationError{Field: "attached_worker_drain.expected_worker_revision", Reason: "must be bounded and positive"}
	}
	return validateAttachedWorkerScope(request.TenantID, request.OwnerUserID, request.WorkerID)
}

func validateAttachedWorkerControlPoll(request ports.AttachedWorkerControlPoll) error {
	if err := validateAttachedWorkerTransportScope(request.TenantID, request.OwnerUserID, request.WorkerID); err != nil {
		return err
	}
	if err := request.ConnectionID.Validate(); err != nil {
		return err
	}
	return request.PresentedSecretDigest.Validate()
}

func validateAttachedWorkerControlExchange(request ports.AttachedWorkerControlExchange) error {
	if err := validateAttachedWorkerControlPoll(ports.AttachedWorkerControlPoll{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		ConnectionID: request.ConnectionID, PresentedSecretDigest: request.PresentedSecretDigest,
	}); err != nil {
		return err
	}
	if request.InboundFrame.Validate() != nil || request.InboundFrame.Kind != attachedworkerprotocol.MessageDrained || request.InboundFrame.Drained == nil {
		return ErrAttachedWorkerControlMessageConflict
	}
	return nil
}
