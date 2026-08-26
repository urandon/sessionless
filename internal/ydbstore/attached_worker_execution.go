package ydbstore

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var _ ports.AttachedWorkerUXReadStore = (*Store)(nil)

func (store *Store) LoadAttachedWorkerAttempt(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
) (result domain.AttachedWorkerAttemptV1, found bool, err error) {
	if err := tenantID.Validate(); err != nil {
		return result, false, err
	}
	if err := ownerUserID.Validate(); err != nil {
		return result, false, err
	}
	if err := workerID.Validate(); err != nil {
		return result, false, err
	}
	result, found, err = readJSON[domain.AttachedWorkerAttemptV1](ctx, store.db,
		`SELECT payload FROM attached_worker_attempt_heads
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3`,
		tenantID, ownerUserID, workerID,
	)
	if err != nil || !found {
		return result, found, err
	}
	if err := result.Validate(); err != nil {
		return domain.AttachedWorkerAttemptV1{}, false, err
	}
	if result.TenantID != tenantID || result.OwnerUserID != ownerUserID || result.WorkerID != workerID {
		return domain.AttachedWorkerAttemptV1{}, false, ErrAttachedWorkerAttemptConflict
	}
	return result, true, nil
}

// ListAttachedWorkerAttemptMessages returns the bounded, immutable AW-04
// message ledger for one owner-scoped attempt. It is a read-only provenance
// source for AW-06 and must not advance protocol state or acknowledge frames.
func (store *Store) ListAttachedWorkerAttemptMessages(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
	attemptID domain.AttemptID,
) ([]domain.AttachedWorkerAttemptMessageV1, error) {
	if err := tenantID.Validate(); err != nil {
		return nil, err
	}
	if err := ownerUserID.Validate(); err != nil {
		return nil, err
	}
	if err := workerID.Validate(); err != nil {
		return nil, err
	}
	if err := attemptID.Validate(); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT direction,attempt_sequence,connection_generation,kind,created_at,payload
		 FROM attached_worker_attempt_messages
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND attempt_id=$4
		 ORDER BY direction,attempt_sequence LIMIT 65`,
		tenantID, ownerUserID, workerID, attemptID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]domain.AttachedWorkerAttemptMessageV1, 0, 8)
	var previous *domain.AttachedWorkerAttemptMessageV1
	for rows.Next() {
		if len(result) == attachedworkerprotocol.MaxAttemptMessages {
			return nil, ErrAttachedWorkerAttemptMessageConflict
		}
		var direction string
		var attemptSequence, connectionGeneration uint64
		var kind, payload string
		var createdAt time.Time
		if err := rows.Scan(&direction, &attemptSequence, &connectionGeneration, &kind, &createdAt, &payload); err != nil {
			return nil, err
		}
		var message domain.AttachedWorkerAttemptMessageV1
		if err := json.Unmarshal([]byte(payload), &message); err != nil {
			return nil, err
		}
		if message.Validate() != nil || message.TenantID != tenantID || message.OwnerUserID != ownerUserID ||
			message.WorkerID != workerID || message.AttemptID != attemptID ||
			string(message.Direction) != direction || message.AttemptSequence != attemptSequence ||
			message.ConnectionGeneration != connectionGeneration || string(message.Kind) != kind ||
			!message.CreatedAt.Equal(canonicalAttachedWorkerTime(createdAt)) {
			return nil, ErrAttachedWorkerAttemptMessageConflict
		}
		if previous != nil && (message.Direction < previous.Direction ||
			(message.Direction == previous.Direction && message.AttemptSequence <= previous.AttemptSequence)) {
			return nil, ErrAttachedWorkerAttemptMessageConflict
		}
		result = append(result, message)
		previous = &result[len(result)-1]
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

const maxAttachedWorkerAttemptLeaseTTL = 24 * time.Hour

var (
	ErrAttachedWorkerAttemptConflict        = errors.New("attached worker attempt conflicts with existing state")
	ErrAttachedWorkerAttemptMessageConflict = errors.New("attached worker attempt message conflicts with existing state")
)

func (store *Store) OfferAttachedWorkerAttempt(ctx context.Context, request ports.AttachedWorkerAttemptOffer) (result ports.AttachedWorkerAttemptResult, err error) {
	if err := validateAttachedWorkerAttemptOffer(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		return store.offerAttachedWorkerAttemptTx(ctx, state.(*stateTx), request, &result)
	})
	return result, err
}

// offerAttachedWorkerAttemptTx composes the protocol offer, canonical lease,
// connection snapshot, attempt ledger, and deadline with an existing scheduler
// transaction. Callers must validate request before entering the transaction.
func (store *Store) offerAttachedWorkerAttemptTx(ctx context.Context, tx *stateTx, request ports.AttachedWorkerAttemptOffer, result *ports.AttachedWorkerAttemptResult) error {
	existing, found, err := readAttachedWorkerAttemptTx(ctx, tx, request.OwnerUserID, request.WorkerID)
	if err != nil {
		return err
	}
	if found && existing.State != domain.AttachedWorkerAttemptRetired {
		return reconcileAttachedWorkerAttemptOfferTx(ctx, tx, request, existing, result)
	}

	at, err := store.attachedWorkerTransactionTime(ctx, tx)
	if err != nil {
		return err
	}
	expiresAt := canonicalAttachedWorkerTime(at.Add(request.LeaseTTL))
	worker, workerFound, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
	if err != nil {
		return err
	}
	connection, connectionFound, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
	if err != nil {
		return err
	}
	loaded, jobFound, err := loadWorkerJobStateTx(ctx, tx, request.RunID)
	if err != nil {
		return err
	}
	if !workerFound || !connectionFound || !jobFound {
		result.Status = ports.AttachedWorkerExecutionNotFound
		return nil
	}
	canonicalContextDigest, err := domain.AttachedWorkerJobContextDigestV1(loaded.Job, loaded.InputManifest)
	if err != nil {
		result.Status = ports.AttachedWorkerExecutionDenied
		return nil
	}
	placement := loaded.Job.ExecutionPlacementV2
	if !attachedWorkerOfferEligible(request, worker, connection, loaded, placement, at, expiresAt) {
		result.Status = ports.AttachedWorkerExecutionDenied
		return nil
	}
	protocolConfig, readySnapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
	if err != nil || readySnapshot.Connection != attachedworkerprotocol.ConnectionReady ||
		readySnapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptIdle {
		result.Status = ports.AttachedWorkerExecutionDenied
		return nil
	}

	lease, replayedLease, err := allocateAttachedWorkerLeaseTx(ctx, tx, request.RunID, request.AttemptID,
		request.LeaseID, request.WorkerID, at, expiresAt)
	if err != nil {
		return err
	}
	if replayedLease {
		// A canonical lease without the matching durable attempt is an
		// incomplete/divergent transaction and is never repaired silently.
		return ErrAttachedWorkerAttemptConflict
	}
	fenceToken, err := domain.NewAttachedWorkerFenceTokenV1(request.TenantID, request.OwnerUserID,
		request.WorkerID, request.RunID, request.AttemptID, request.LeaseID, lease.FenceToken)
	if err != nil {
		return err
	}
	contextDigest, _ := hex.DecodeString(string(canonicalContextDigest))
	policyDigest, _ := hex.DecodeString(string(placement.PolicyDigest))
	frame, postSnapshot, err := attachedworkerprotocol.BuildLeaseOfferTransitionV1(
		protocolConfig, readySnapshot, attachedworkerprotocol.LeaseOfferAuthorityV1{
			RunID: string(request.RunID), AttemptID: string(request.AttemptID), LeaseID: string(request.LeaseID),
			LeaseGeneration: lease.FenceToken, NowUnixMicro: at.UnixMicro(), ExpiresAtUnixMicro: expiresAt.UnixMicro(),
			ContextDigest: contextDigest, PolicyDigest: policyDigest,
		})
	if err != nil || frame.LeaseOffer == nil || frame.LeaseOffer.Binding.FenceToken != string(fenceToken) {
		return ErrAttachedWorkerAttemptConflict
	}
	framePayload, err := attachedworkerprotocol.EncodeBatchV1(attachedworkerprotocol.BatchV1{
		Version: protocolConfig.Auth.Version, Frames: []attachedworkerprotocol.FrameV1{frame},
	})
	if err != nil {
		return err
	}
	frameFingerprint, err := attachedworkerprotocol.AttemptFrameFingerprintV1(frame)
	if err != nil {
		return err
	}
	postPayload, err := canonicalAttachedWorkerProtocolSnapshot(postSnapshot)
	if err != nil {
		return err
	}

	attempt := domain.AttachedWorkerAttemptV1{
		Version: domain.AttachedWorkerAttemptVersionV1, TenantID: request.TenantID,
		OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID, ConnectionID: connection.ID,
		RunID: request.RunID, AttemptID: request.AttemptID, ReservationID: request.ReservationID,
		LeaseID: request.LeaseID, LeaseGeneration: lease.FenceToken, FenceToken: fenceToken,
		EnrollmentGeneration: connection.EnrollmentGeneration, ConnectionGeneration: connection.ConnectionGeneration,
		ContextDigest: canonicalContextDigest, CapabilityDigest: placement.CapabilityDigest,
		PolicyDigest: placement.PolicyDigest, State: domain.AttachedWorkerAttemptOffered,
		PlatformAttemptSequence: frame.LeaseOffer.AttemptSequence,
		LeaseExpiresAt:          expiresAt, CreatedAt: at, UpdatedAt: at, Revision: 1,
	}
	message := domain.AttachedWorkerAttemptMessageV1{
		Version: domain.AttachedWorkerAttemptMessageVersionV1, TenantID: request.TenantID,
		OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID, AttemptID: request.AttemptID,
		Direction: domain.AttachedWorkerAttemptPlatformToWorker, AttemptSequence: frame.LeaseOffer.AttemptSequence,
		ConnectionGeneration: connection.ConnectionGeneration, EnvelopeSequence: frame.Sequence,
		Kind:        domain.AttachedWorkerAttemptMessageLeaseOffered,
		Fingerprint: domain.AttachedWorkerAttemptMessageFingerprint(hex.EncodeToString(frameFingerprint)), Payload: framePayload, CreatedAt: at,
	}
	bucket, err := domain.AttachedWorkerAttemptDeadlineBucketV1(request.TenantID, request.OwnerUserID, request.WorkerID, request.AttemptID)
	if err != nil {
		return err
	}
	deadline := domain.AttachedWorkerAttemptDeadlineV1{Bucket: bucket, DeadlineAt: expiresAt,
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		AttemptID: request.AttemptID, Kind: domain.AttachedWorkerDeadlineLeaseExpiry,
		LeaseGeneration: lease.FenceToken, AttemptRevision: attempt.Revision}

	oldExpiry := attachedWorkerPresenceExpiry(connection)
	connection.PlatformSequence, connection.PlatformAck = postSnapshot.Platform.Sequence, postSnapshot.Platform.Ack
	connection.WorkerSequence, connection.WorkerAck = postSnapshot.Worker.Sequence, postSnapshot.Worker.Ack
	connection.ProtocolSnapshot = postPayload
	connection.Revision++
	if err := deleteAttachedWorkerPresenceExpiryTx(ctx, tx, oldExpiry); err != nil {
		return err
	}
	if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
		return err
	}
	if err := insertAttachedWorkerPresenceExpiryTx(ctx, tx, attachedWorkerPresenceExpiry(connection)); err != nil {
		return err
	}
	retainUntil := expiresAt.Add(store.operationalRetention)
	if err := upsertAttachedWorkerAttemptTx(ctx, tx, attempt, retainUntil); err != nil {
		return err
	}
	if _, err := insertOrReconcileAttachedWorkerAttemptMessageTx(ctx, tx, message, retainUntil); err != nil {
		return err
	}
	if err := upsertAttachedWorkerAttemptDeadlineTx(ctx, tx, deadline); err != nil {
		return err
	}
	*result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionApplied, Attempt: attempt, Outbound: &message}
	return nil
}

func (store *Store) PollAttachedWorkerAttempt(ctx context.Context, request ports.AttachedWorkerAttemptPoll) (result ports.AttachedWorkerAttemptResult, err error) {
	if err := validateAttachedWorkerAttemptPoll(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		worker, workerFound, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		connection, connectionFound, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !workerFound || !connectionFound || !attachedWorkerAttemptPollAuthorized(request, at, worker, connection) {
			result.Status = ports.AttachedWorkerExecutionDenied
			return nil
		}
		config, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
		if err != nil {
			return err
		}
		attempt, found, err := readAttachedWorkerAttemptTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || attempt.ConnectionID != connection.ID || attempt.ConnectionGeneration != connection.ConnectionGeneration {
			result.Status = ports.AttachedWorkerExecutionNotFound
			return nil
		}
		wantKind := pendingAttachedWorkerAttemptMessageKind(attempt.State)
		if wantKind == "" {
			result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionApplied, Attempt: attempt}
			return nil
		}
		key := domain.AttachedWorkerAttemptMessageV1{OwnerUserID: attempt.OwnerUserID, WorkerID: attempt.WorkerID,
			AttemptID: attempt.AttemptID, Direction: domain.AttachedWorkerAttemptPlatformToWorker,
			AttemptSequence: attempt.PlatformAttemptSequence}
		message, found, err := readAttachedWorkerAttemptMessageTx(ctx, tx, key)
		if err != nil {
			return err
		}
		frame, direction, decodeErr := decodeAttachedWorkerAttemptFrame(message)
		if !found || decodeErr != nil || direction != attachedworkerprotocol.DirectionPlatformToWorker || message.Kind != wantKind ||
			message.ConnectionGeneration != connection.ConnectionGeneration || frame.Sequence != snapshot.Platform.Sequence {
			return ErrAttachedWorkerAttemptConflict
		}
		if !attachedWorkerPollFramePending(snapshot.Worker.Ack, frame.Sequence) {
			if attempt.State == domain.AttachedWorkerAttemptTerminalCommitted {
				post, retireErr := attachedworkerprotocol.RetireCommittedAttemptV1(config, snapshot)
				if retireErr != nil {
					return ErrAttachedWorkerAttemptConflict
				}
				next, retireErr := retireAttachedWorkerCommittedAttempt(attempt, at)
				if retireErr != nil {
					return retireErr
				}
				oldExpiry := attachedWorkerPresenceExpiry(connection)
				connection, retireErr = advanceAttachedWorkerConnectionProtocol(connection, post)
				if retireErr != nil {
					return retireErr
				}
				if err := deleteAttachedWorkerPresenceExpiryTx(ctx, tx, oldExpiry); err != nil {
					return err
				}
				if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
					return err
				}
				if err := insertAttachedWorkerPresenceExpiryTx(ctx, tx, attachedWorkerPresenceExpiry(connection)); err != nil {
					return err
				}
				if err := compareAndSwapAttachedWorkerAttemptTx(ctx, tx, attempt.Revision, next, at.Add(store.operationalRetention)); err != nil {
					return err
				}
				if err := deleteAttachedWorkerAttemptDeadlinesTx(ctx, tx, attempt); err != nil {
					return err
				}
				attempt = next
			}
			result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionApplied, Attempt: attempt}
			return nil
		}
		result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionApplied, Attempt: attempt, Outbound: &message}
		return nil
	})
	return result, err
}

func (store *Store) ExchangeAttachedWorkerAttempt(ctx context.Context, request ports.AttachedWorkerAttemptExchange) (result ports.AttachedWorkerAttemptResult, err error) {
	if err := validateAttachedWorkerAttemptExchange(request); err != nil {
		return result, err
	}
	if _, _, ok := attachedWorkerFrameIdentity(request.InboundFrame); !ok {
		return result, ErrAttachedWorkerAttemptMessageConflict
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		attempt, found, err := readAttachedWorkerAttemptTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || attempt.AttemptID != request.AttemptID {
			result.Status = ports.AttachedWorkerExecutionNotFound
			return nil
		}
		inboundMessage, err := attachedWorkerAttemptMessageFromFrame(attempt, attachedworkerprotocol.DirectionWorkerToPlatform,
			request.InboundFrame, time.Unix(1, 0).UTC())
		if err != nil {
			return err
		}
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		worker, workerFound, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		connection, connectionFound, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		// Replay reconciliation is still an authenticated operation. A captured
		// exact frame must never reveal its stored response after secret rotation,
		// reconnect, revocation, expiry, or an owner/worker authority change.
		if !workerFound || !connectionFound || !attachedWorkerExecutionAuthorityCurrent(worker, connection) ||
			attempt.LeaseGeneration != request.LeaseGeneration || attempt.ConnectionID != request.ConnectionID ||
			connection.ID != request.ConnectionID || connection.ConnectionGeneration != attempt.ConnectionGeneration ||
			subtle.ConstantTimeCompare([]byte(connection.SecretDigest), []byte(request.PresentedSecretDigest)) != 1 ||
			!at.Before(connection.AuthExpiresAt) || !at.Before(connection.PresenceExpiresAt) {
			result.Status = ports.AttachedWorkerExecutionDenied
			return nil
		}
		storedInbound, replayFound, err := readAttachedWorkerAttemptMessageTx(ctx, tx, inboundMessage)
		if err != nil {
			return err
		}
		if replayFound {
			if !sameAttachedWorkerAttemptMessage(storedInbound, inboundMessage) {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			var replayOutbound *domain.AttachedWorkerAttemptMessageV1
			wantOutbound := domain.AttachedWorkerAttemptMessageKind("")
			switch inboundMessage.Kind {
			case domain.AttachedWorkerAttemptMessageLeaseClaim:
				wantOutbound = domain.AttachedWorkerAttemptMessageLeaseAccepted
			case domain.AttachedWorkerAttemptMessageTerminal:
				if attempt.State == domain.AttachedWorkerAttemptTerminalCommitted {
					wantOutbound = domain.AttachedWorkerAttemptMessageTerminalCommitted
				}
			}
			if wantOutbound != "" {
				outboundSequence := attachedWorkerReplayOutboundSequence(inboundMessage, attempt.PlatformAttemptSequence)
				key := domain.AttachedWorkerAttemptMessageV1{OwnerUserID: attempt.OwnerUserID, WorkerID: attempt.WorkerID, AttemptID: attempt.AttemptID, Direction: domain.AttachedWorkerAttemptPlatformToWorker, AttemptSequence: outboundSequence}
				stored, ok, err := readAttachedWorkerAttemptMessageTx(ctx, tx, key)
				if err != nil {
					return err
				}
				if !ok || stored.Kind != wantOutbound {
					result.Status = ports.AttachedWorkerExecutionConflict
					return nil
				}
				replayOutbound = &stored
			}
			result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionReplayed, Attempt: attempt, Outbound: replayOutbound}
			return nil
		}
		if attempt.Revision == math.MaxUint64 || inboundMessage.AttemptSequence != attempt.WorkerAttemptSequence+1 ||
			attempt.LeaseGeneration != request.LeaseGeneration || attempt.ConnectionID != request.ConnectionID ||
			attempt.State == domain.AttachedWorkerAttemptFencedUnknown || attempt.State == domain.AttachedWorkerAttemptTerminalCommitted {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		if !at.Before(attempt.LeaseExpiresAt) {
			result.Status = ports.AttachedWorkerExecutionExpired
			return nil
		}
		if attachedWorkerCancellationFrameExpired(attempt, at) {
			// Once the authoritative acknowledgement deadline is reached only the
			// deadline-fence transaction may resolve the attempt. A late frame must
			// not erase the due row and escape the fenced-unknown outcome.
			result.Status = ports.AttachedWorkerExecutionExpired
			return nil
		}
		if connection.Revision == math.MaxUint64 ||
			request.InboundFrame.Sequence != connection.WorkerSequence+1 ||
			connection.ConnectionGeneration != attempt.ConnectionGeneration {
			result.Status = ports.AttachedWorkerExecutionDenied
			return nil
		}
		config, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
		if err != nil {
			return err
		}
		inboundFrame := request.InboundFrame
		post, err := attachedworkerprotocol.ApplyMachineFrameV1(config, snapshot, attachedworkerprotocol.DirectionWorkerToPlatform, inboundFrame, at.UnixMicro())
		if err != nil {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		var outboundMessage *domain.AttachedWorkerAttemptMessageV1
		next := attempt
		next.WorkerAttemptSequence = inboundMessage.AttemptSequence
		switch inboundMessage.Kind {
		case domain.AttachedWorkerAttemptMessageLeaseClaim:
			acceptedFrame, acceptedPost, buildErr := attachedworkerprotocol.BuildLeaseAcceptedTransitionV1(config, post, attachedworkerprotocol.LeaseAcceptedAuthorityV1{NowUnixMicro: at.UnixMicro()})
			if buildErr != nil {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			message, buildErr := attachedWorkerAttemptMessageFromFrame(attempt, attachedworkerprotocol.DirectionPlatformToWorker, acceptedFrame, at)
			if buildErr != nil {
				return buildErr
			}
			post, outboundMessage, next.PlatformAttemptSequence = acceptedPost, &message, message.AttemptSequence
			next.State = domain.AttachedWorkerAttemptClaimed
			loaded, found, err := loadWorkerJobStateTx(ctx, tx, attempt.RunID)
			if err != nil || !found {
				return err
			}
			lease, found, err := readCanonicalLeaseHeadTx(ctx, tx, attempt.RunID)
			if err != nil || !found || lease.FenceToken != attempt.LeaseGeneration {
				return ErrAttachedWorkerAttemptConflict
			}
			if err := startWorkerJobTx(ctx, state, loaded, lease, at); err != nil {
				return err
			}
		case domain.AttachedWorkerAttemptMessageProgress:
			if inboundFrame.Progress == nil || !attachedWorkerProgressAllowed(attempt.State) {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			next.ProgressSequence = inboundFrame.Progress.ProgressSequence
		case domain.AttachedWorkerAttemptMessageCancelAcknowledged:
			if inboundFrame.CancelAck == nil || (attempt.State != domain.AttachedWorkerAttemptCancelRequested && attempt.State != domain.AttachedWorkerAttemptCancelledBeforeClaim) || inboundFrame.CancelAck.CancelRevision != attempt.CancelRevision {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			next.State = attachedWorkerStateAfterCancelAck(attempt.State)
			if next.State == domain.AttachedWorkerAttemptCancelledBeforeClaim {
				post, err = attachedworkerprotocol.RetirePreClaimCancelledAttemptV1(config, post)
				if err != nil {
					result.Status = ports.AttachedWorkerExecutionConflict
					return nil
				}
				next.State = domain.AttachedWorkerAttemptRetired
			}
		case domain.AttachedWorkerAttemptMessageTerminal:
			if inboundFrame.Terminal == nil {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			if err := applyAttachedWorkerTerminalTransition(&next, attempt.State, *inboundFrame.Terminal, post.Attempt.Summary.State); err != nil {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
		default:
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		next.UpdatedAt, next.Revision = at, attempt.Revision+1
		oldExpiry := attachedWorkerPresenceExpiry(connection)
		connection, err = advanceAttachedWorkerConnectionProtocol(connection, post)
		if err != nil {
			return err
		}
		retainUntil := attempt.LeaseExpiresAt.Add(store.operationalRetention)
		if _, err := insertOrReconcileAttachedWorkerAttemptMessageTx(ctx, tx, canonicalAttemptMessageTime(inboundMessage, at), retainUntil); err != nil {
			return err
		}
		if outboundMessage != nil {
			if _, err := insertOrReconcileAttachedWorkerAttemptMessageTx(ctx, tx, *outboundMessage, retainUntil); err != nil {
				return err
			}
		}
		if err := compareAndSwapAttachedWorkerAttemptTx(ctx, tx, attempt.Revision, next, retainUntil); err != nil {
			return err
		}
		if err := deleteAttachedWorkerPresenceExpiryTx(ctx, tx, oldExpiry); err != nil {
			return err
		}
		if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
			return err
		}
		if err := insertAttachedWorkerPresenceExpiryTx(ctx, tx, attachedWorkerPresenceExpiry(connection)); err != nil {
			return err
		}
		bucket, _ := domain.AttachedWorkerAttemptDeadlineBucketV1(next.TenantID, next.OwnerUserID, next.WorkerID, next.AttemptID)
		leaseDeadline := domain.AttachedWorkerAttemptDeadlineV1{Bucket: bucket, DeadlineAt: next.LeaseExpiresAt, TenantID: next.TenantID, OwnerUserID: next.OwnerUserID, WorkerID: next.WorkerID, AttemptID: next.AttemptID, Kind: domain.AttachedWorkerDeadlineLeaseExpiry, LeaseGeneration: next.LeaseGeneration, AttemptRevision: next.Revision}
		if next.State == domain.AttachedWorkerAttemptCancelledBeforeClaim || next.State == domain.AttachedWorkerAttemptRetired {
			// The offer was cancelled before any lease claim. A later CancelAck is
			// durable evidence that the worker observed the cancellation, but it
			// must not recreate the lease deadline removed by the cancel transaction.
			if err := deleteAttachedWorkerAttemptDeadlinesTx(ctx, tx, attempt); err != nil {
				return err
			}
		} else {
			if err := upsertAttachedWorkerAttemptDeadlineTx(ctx, tx, leaseDeadline); err != nil {
				return err
			}
			if next.State == domain.AttachedWorkerAttemptCancelRequested {
				cancel := leaseDeadline
				cancel.Kind, cancel.DeadlineAt = domain.AttachedWorkerDeadlineCancelAck, next.CancelDeadline
				if cancel.DeadlineAt.IsZero() {
					return ErrAttachedWorkerAttemptConflict
				}
				if err := upsertAttachedWorkerAttemptDeadlineTx(ctx, tx, cancel); err != nil {
					return err
				}
			} else if next.State == domain.AttachedWorkerAttemptCancelAcknowledged || next.State == domain.AttachedWorkerAttemptTerminalPending {
				cancel := leaseDeadline
				cancel.Kind, cancel.DeadlineAt = domain.AttachedWorkerDeadlineCancelAck, next.CancelDeadline
				if !cancel.DeadlineAt.IsZero() {
					if err := deleteAttachedWorkerAttemptDeadlineTx(ctx, tx, cancel); err != nil {
						return err
					}
				}
			}
		}
		result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionApplied, Attempt: next, Outbound: outboundMessage}
		return nil
	})
	return result, err
}

func (store *Store) reconcileRetiredAttachedWorkerTerminalTx(
	ctx context.Context,
	tx *stateTx,
	request ports.AttachedWorkerTerminalCommit,
	current *domain.AttachedWorkerAttemptV1,
	result *ports.AttachedWorkerAttemptResult,
) (bool, error) {
	ackMessage, ackFound, err := readAttachedWorkerAttemptMessageByKindTx(ctx, tx, request.OwnerUserID, request.WorkerID,
		request.AttemptID, domain.AttachedWorkerAttemptPlatformToWorker, domain.AttachedWorkerAttemptMessageTerminalCommitted)
	if err != nil || !ackFound {
		return false, err
	}
	ackFrame, ackDirection, err := decodeAttachedWorkerAttemptFrame(ackMessage)
	if err != nil || ackDirection != attachedworkerprotocol.DirectionPlatformToWorker || ackFrame.TerminalAck == nil {
		return false, ErrAttachedWorkerAttemptConflict
	}
	ack := ackFrame.TerminalAck
	terminalMessages, err := readAttachedWorkerAttemptMessagesByKindTx(ctx, tx, request.OwnerUserID, request.WorkerID,
		request.AttemptID, domain.AttachedWorkerAttemptWorkerToPlatform, domain.AttachedWorkerAttemptMessageTerminal)
	if err != nil {
		return false, err
	}
	var terminalMessage domain.AttachedWorkerAttemptMessageV1
	var terminalFrame attachedworkerprotocol.FrameV1
	matches := 0
	for _, candidate := range terminalMessages {
		frame, direction, decodeErr := decodeAttachedWorkerAttemptFrame(candidate)
		if decodeErr != nil || direction != attachedworkerprotocol.DirectionWorkerToPlatform || frame.Terminal == nil {
			return false, ErrAttachedWorkerAttemptConflict
		}
		terminal := frame.Terminal
		if sameAttachedWorkerAttemptBinding(terminal.Binding, ack.Binding) &&
			terminal.Binding.AttemptID == string(request.AttemptID) && terminal.Binding.LeaseGeneration == request.LeaseGeneration &&
			frame.WorkerID == string(request.WorkerID) && frame.ConnectionGeneration == candidate.ConnectionGeneration &&
			ack.TerminalSequence == terminal.TerminalSequence && ack.Status == terminal.Status && ack.Result == terminal.Result &&
			bytes.Equal(ack.EvidenceDigest, terminal.EvidenceDigest) {
			terminalMessage, terminalFrame = candidate, frame
			matches++
		}
	}
	if matches != 1 {
		return false, ErrAttachedWorkerAttemptConflict
	}
	binding := terminalFrame.Terminal.Binding
	if terminalMessage.MaterializationReservationID == "" ||
		terminalMessage.MaterializationReservationID != ackMessage.MaterializationReservationID ||
		terminalMessage.MaterializationReservationID != attachedWorkerMaterializationReservationID(request.Materialization) ||
		terminalMessage.ExecutionConnectionID == "" || terminalMessage.ExecutionConnectionID != ackMessage.ExecutionConnectionID {
		return false, ErrAttachedWorkerAttemptConflict
	}
	evidence, err := attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalStatus(terminalFrame.Terminal.Status), request.Materialization)
	if err != nil || evidence != request.Materialization.EvidenceDigest ||
		evidence != domain.AttachedWorkerTerminalEvidenceDigest(hex.EncodeToString(terminalFrame.Terminal.EvidenceDigest)) {
		return false, ErrAttachedWorkerAttemptConflict
	}
	reservationID := terminalMessage.MaterializationReservationID
	at := canonicalAttachedWorkerTime(ackMessage.CreatedAt)
	createdAt := canonicalAttachedWorkerTime(terminalMessage.CreatedAt)
	if at.Before(createdAt) {
		return false, ErrAttachedWorkerAttemptConflict
	}
	reconstructed := domain.AttachedWorkerAttemptV1{
		Version: domain.AttachedWorkerAttemptVersionV1, TenantID: request.TenantID, OwnerUserID: request.OwnerUserID,
		WorkerID: request.WorkerID, ConnectionID: terminalMessage.ExecutionConnectionID, RunID: domain.RunID(binding.RunID), AttemptID: request.AttemptID,
		ReservationID: reservationID, LeaseID: domain.LeaseID(binding.LeaseID), LeaseGeneration: binding.LeaseGeneration,
		FenceToken: domain.AttachedWorkerFenceToken(binding.FenceToken), EnrollmentGeneration: terminalFrame.EnrollmentGeneration,
		ConnectionGeneration: terminalFrame.ConnectionGeneration, ContextDigest: domain.AttachedWorkerContextDigest(hex.EncodeToString(binding.ContextDigest)),
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(binding.CapabilityDigest)),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(hex.EncodeToString(binding.PolicyDigest)), State: domain.AttachedWorkerAttemptRetired,
		PlatformAttemptSequence: ack.AttemptSequence, WorkerAttemptSequence: terminalFrame.Terminal.AttemptSequence,
		TerminalSequence: terminalFrame.Terminal.TerminalSequence, TerminalStatus: domain.AttachedWorkerTerminalStatus(terminalFrame.Terminal.Status),
		TerminalEvidenceDigest: evidence, LeaseExpiresAt: time.UnixMicro(binding.ExpiresAtUnixMicro).UTC(),
		CreatedAt: createdAt, UpdatedAt: at, Revision: 1,
	}
	if reconstructed.Validate() != nil {
		return false, ErrAttachedWorkerAttemptConflict
	}
	if err := validateAttachedWorkerTerminalMaterializationBinding(reconstructed, request.Materialization); err != nil {
		return false, err
	}
	runStatus := domain.RunSucceeded
	if terminalFrame.Terminal.Status == attachedworkerprotocol.TerminalFailed {
		runStatus = domain.RunFailed
	} else if terminalFrame.Terminal.Status == attachedworkerprotocol.TerminalCancelled {
		runStatus = domain.RunCancelled
	}
	matched, err := matchingRunFinalizationTx(ctx, tx, reconstructed.RunID, runStatus, string(evidence))
	if err != nil || !matched {
		return false, err
	}
	*result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionReplayed, Outbound: &ackMessage, Historical: current == nil}
	if current != nil {
		if !sameAttachedWorkerRetiredAttemptBinding(*current, reconstructed) {
			return false, ErrAttachedWorkerAttemptConflict
		}
		result.Attempt = *current
	}
	return true, nil
}

func (store *Store) RequestAttachedWorkerCancellation(ctx context.Context, request ports.AttachedWorkerCancellationRequest) (result ports.AttachedWorkerAttemptResult, err error) {
	if err := validateAttachedWorkerCancellationRequest(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		attempt, found, err := readAttachedWorkerAttemptTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || attempt.AttemptID != request.AttemptID {
			message, ok, err := readAttachedWorkerAttemptMessageByKindTx(ctx, tx, request.OwnerUserID, request.WorkerID,
				request.AttemptID, domain.AttachedWorkerAttemptPlatformToWorker, domain.AttachedWorkerAttemptMessageCancelRequested)
			if err != nil {
				return err
			}
			if !ok {
				result.Status = ports.AttachedWorkerExecutionNotFound
				return nil
			}
			frame, direction, decodeErr := decodeAttachedWorkerAttemptFrame(message)
			if decodeErr != nil || direction != attachedworkerprotocol.DirectionPlatformToWorker || frame.Cancel == nil ||
				frame.Cancel.Code != attachedworkerprotocol.CancelRequested || frame.Cancel.CancelRevision != 1 ||
				frame.Cancel.Binding.AttemptID != string(request.AttemptID) || frame.Cancel.Binding.LeaseGeneration != request.LeaseGeneration ||
				frame.WorkerID != string(request.WorkerID) || message.OperationDeadline.IsZero() ||
				!canonicalAttachedWorkerTime(message.CreatedAt.Add(request.AckTimeout)).Equal(message.OperationDeadline) {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionReplayed, Outbound: &message, Historical: true}
			return nil
		}
		if attachedWorkerCancellationReplayState(attempt.State) && attempt.CancelRevision == 1 && attempt.LeaseGeneration == request.LeaseGeneration {
			message, ok, err := readAttachedWorkerAttemptMessageByKindTx(ctx, tx, attempt.OwnerUserID, attempt.WorkerID,
				attempt.AttemptID, domain.AttachedWorkerAttemptPlatformToWorker, domain.AttachedWorkerAttemptMessageCancelRequested)
			if err != nil {
				return err
			}
			frame, direction, decodeErr := decodeAttachedWorkerAttemptFrame(message)
			if !ok || decodeErr != nil || direction != attachedworkerprotocol.DirectionPlatformToWorker ||
				frame.Cancel == nil || frame.Cancel.Code != attachedworkerprotocol.CancelRequested || frame.Cancel.CancelRevision != attempt.CancelRevision {
				return ErrAttachedWorkerAttemptConflict
			}
			if !message.OperationDeadline.Equal(attempt.CancelDeadline) ||
				!canonicalAttachedWorkerTime(message.CreatedAt.Add(request.AckTimeout)).Equal(message.OperationDeadline) {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionReplayed, Attempt: attempt, Outbound: &message}
			return nil
		}
		if attempt.Revision == math.MaxUint64 || attempt.LeaseGeneration != request.LeaseGeneration ||
			(attempt.State != domain.AttachedWorkerAttemptOffered && attempt.State != domain.AttachedWorkerAttemptClaimed) {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
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
		if !found || !connectionFound {
			result.Status = ports.AttachedWorkerExecutionNotFound
			return nil
		}
		if !attachedWorkerExecutionAuthorityCurrent(worker, connection) || connection.ID != attempt.ConnectionID ||
			connection.ConnectionGeneration != attempt.ConnectionGeneration || connection.EnrollmentGeneration != attempt.EnrollmentGeneration {
			result.Status = ports.AttachedWorkerExecutionDenied
			return nil
		}
		config, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
		if err != nil {
			return err
		}
		frame, post, err := attachedworkerprotocol.BuildCancelTransitionV1(config, snapshot, attachedworkerprotocol.CancelAuthorityV1{Revision: 1, Code: attachedworkerprotocol.CancelRequested, NowUnixMicro: at.UnixMicro()})
		if err != nil {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		message, err := attachedWorkerAttemptMessageFromFrame(attempt, attachedworkerprotocol.DirectionPlatformToWorker, frame, at)
		if err != nil {
			return err
		}
		next := attempt
		next.PlatformAttemptSequence = message.AttemptSequence
		next.CancelRevision = 1
		next.CancelDeadline = canonicalAttachedWorkerTime(at.Add(request.AckTimeout))
		message.OperationDeadline = next.CancelDeadline
		next.UpdatedAt, next.Revision = at, attempt.Revision+1
		next.State = domain.AttachedWorkerAttemptCancelRequested
		if attempt.State == domain.AttachedWorkerAttemptOffered {
			next.State = domain.AttachedWorkerAttemptCancelledBeforeClaim
		}
		oldExpiry := attachedWorkerPresenceExpiry(connection)
		connection, err = advanceAttachedWorkerConnectionProtocol(connection, post)
		if err != nil {
			return err
		}
		retainUntil := attempt.LeaseExpiresAt.Add(store.operationalRetention)
		if _, err := insertOrReconcileAttachedWorkerAttemptMessageTx(ctx, tx, message, retainUntil); err != nil {
			return err
		}
		if err := compareAndSwapAttachedWorkerAttemptTx(ctx, tx, attempt.Revision, next, retainUntil); err != nil {
			return err
		}
		if err := deleteAttachedWorkerPresenceExpiryTx(ctx, tx, oldExpiry); err != nil {
			return err
		}
		if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
			return err
		}
		if err := insertAttachedWorkerPresenceExpiryTx(ctx, tx, attachedWorkerPresenceExpiry(connection)); err != nil {
			return err
		}
		deadlinePlan, err := planAttachedWorkerCancellationDeadlines(attempt, next)
		if err != nil {
			return err
		}
		if deadlinePlan.DeleteLease {
			// The attempt can no longer be claimed, so the lease deadline is no
			// longer authoritative. Keep a bounded cancel-ack deadline: if the
			// worker disappears before acknowledging the pre-claim cancellation,
			// the deadline path fences the old connection generation before
			// retiring the known-unexecuted attempt.
			if err := deleteAttachedWorkerAttemptDeadlineTx(ctx, tx, deadlinePlan.Lease); err != nil {
				return err
			}
			if deadlinePlan.Cancel == nil {
				return ErrAttachedWorkerAttemptConflict
			}
			if err := upsertAttachedWorkerAttemptDeadlineTx(ctx, tx, *deadlinePlan.Cancel); err != nil {
				return err
			}
		} else {
			if err := upsertAttachedWorkerAttemptDeadlineTx(ctx, tx, deadlinePlan.Lease); err != nil {
				return err
			}
			if err := upsertAttachedWorkerAttemptDeadlineTx(ctx, tx, *deadlinePlan.Cancel); err != nil {
				return err
			}
		}
		result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionApplied, Attempt: next, Outbound: &message}
		return nil
	})
	return result, err
}

func (store *Store) CommitAttachedWorkerTerminal(ctx context.Context, request ports.AttachedWorkerTerminalCommit) (result ports.AttachedWorkerAttemptResult, err error) {
	if err := validateAttachedWorkerTerminalCommit(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		attempt, found, err := readAttachedWorkerAttemptTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || attempt.AttemptID != request.AttemptID || attempt.State == domain.AttachedWorkerAttemptRetired {
			var current *domain.AttachedWorkerAttemptV1
			if found && attempt.AttemptID == request.AttemptID && attempt.State == domain.AttachedWorkerAttemptRetired {
				current = &attempt
			}
			replayed, replayErr := store.reconcileRetiredAttachedWorkerTerminalTx(ctx, tx, request, current, &result)
			if replayErr != nil {
				return replayErr
			}
			if replayed {
				return nil
			}
		}
		if !found || attempt.AttemptID != request.AttemptID {
			result.Status = ports.AttachedWorkerExecutionNotFound
			return nil
		}
		if attempt.State == domain.AttachedWorkerAttemptTerminalCommitted && attempt.TerminalEvidenceDigest == request.Materialization.EvidenceDigest {
			at, err := store.attachedWorkerTransactionTime(ctx, tx)
			if err != nil {
				return err
			}
			if err := materializeAttachedWorkerTerminalTx(ctx, state, tx, attempt, request.Materialization, at); err != nil {
				return err
			}
			key := domain.AttachedWorkerAttemptMessageV1{OwnerUserID: attempt.OwnerUserID, WorkerID: attempt.WorkerID, AttemptID: attempt.AttemptID, Direction: domain.AttachedWorkerAttemptPlatformToWorker, AttemptSequence: attempt.PlatformAttemptSequence}
			message, ok, err := readAttachedWorkerAttemptMessageTx(ctx, tx, key)
			if err != nil {
				return err
			}
			if !ok || message.Kind != domain.AttachedWorkerAttemptMessageTerminalCommitted {
				return ErrAttachedWorkerAttemptConflict
			}
			result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionReplayed, Attempt: attempt, Outbound: &message}
			return nil
		}
		if attempt.State != domain.AttachedWorkerAttemptTerminalPending || attempt.Revision == math.MaxUint64 ||
			attempt.LeaseGeneration != request.LeaseGeneration || attempt.TerminalEvidenceDigest != request.Materialization.EvidenceDigest {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		if !at.Before(attempt.LeaseExpiresAt) {
			result.Status = ports.AttachedWorkerExecutionExpired
			return nil
		}
		worker, found, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		connection, connectionFound, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || !connectionFound {
			result.Status = ports.AttachedWorkerExecutionNotFound
			return nil
		}
		config, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
		if err != nil {
			return err
		}
		if err := materializeAttachedWorkerTerminalTx(ctx, state, tx, attempt, request.Materialization, at); err != nil {
			return err
		}
		ackFrame, post, err := attachedworkerprotocol.BuildTerminalAckTransitionV1(config, snapshot, attachedworkerprotocol.TerminalAckAuthorityV1{NowUnixMicro: at.UnixMicro()})
		if err != nil {
			return err
		}
		message, err := attachedWorkerAttemptMessageFromFrame(attempt, attachedworkerprotocol.DirectionPlatformToWorker, ackFrame, at)
		if err != nil {
			return err
		}
		next := attempt
		next.State = domain.AttachedWorkerAttemptTerminalCommitted
		next.PlatformAttemptSequence = message.AttemptSequence
		next.UpdatedAt, next.Revision = at, attempt.Revision+1
		oldExpiry := attachedWorkerPresenceExpiry(connection)
		connection, err = advanceAttachedWorkerConnectionProtocol(connection, post)
		if err != nil {
			return err
		}
		retainUntil := at.Add(store.operationalRetention)
		if _, err := insertOrReconcileAttachedWorkerAttemptMessageTx(ctx, tx, message, retainUntil); err != nil {
			return err
		}
		if err := compareAndSwapAttachedWorkerAttemptTx(ctx, tx, attempt.Revision, next, retainUntil); err != nil {
			return err
		}
		if err := deleteAttachedWorkerPresenceExpiryTx(ctx, tx, oldExpiry); err != nil {
			return err
		}
		if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
			return err
		}
		if err := insertAttachedWorkerPresenceExpiryTx(ctx, tx, attachedWorkerPresenceExpiry(connection)); err != nil {
			return err
		}
		bucket, _ := domain.AttachedWorkerAttemptDeadlineBucketV1(next.TenantID, next.OwnerUserID, next.WorkerID, next.AttemptID)
		if err := deleteAttachedWorkerAttemptDeadlineTx(ctx, tx, domain.AttachedWorkerAttemptDeadlineV1{Bucket: bucket, DeadlineAt: attempt.LeaseExpiresAt, TenantID: next.TenantID, OwnerUserID: next.OwnerUserID, WorkerID: next.WorkerID, AttemptID: next.AttemptID, Kind: domain.AttachedWorkerDeadlineLeaseExpiry, LeaseGeneration: next.LeaseGeneration, AttemptRevision: attempt.Revision}); err != nil {
			return err
		}
		result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionApplied, Attempt: next, Outbound: &message}
		return nil
	})
	return result, err
}

func (store *Store) FenceAttachedWorkerAttempt(ctx context.Context, request ports.AttachedWorkerAttemptFence) (result ports.AttachedWorkerAttemptResult, err error) {
	if err := validateAttachedWorkerAttemptFence(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		attempt, found, err := readAttachedWorkerAttemptTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || attempt.AttemptID != request.AttemptID {
			result.Status = ports.AttachedWorkerExecutionNotFound
			return nil
		}
		if (attempt.State == domain.AttachedWorkerAttemptFencedUnknown || attempt.State == domain.AttachedWorkerAttemptRetired) &&
			attempt.LeaseGeneration == request.LeaseGeneration {
			deadlineMatches := request.Reason == ports.AttachedWorkerFenceLeaseExpired && attempt.LeaseExpiresAt.Equal(canonicalAttachedWorkerTime(request.DeadlineAt))
			if request.Reason == ports.AttachedWorkerFenceCancelAckUnknown {
				deadlineMatches = attempt.CancelDeadline.Equal(canonicalAttachedWorkerTime(request.DeadlineAt))
			}
			if !deadlineMatches || request.CandidateAttemptRevision == math.MaxUint64 || request.CandidateAttemptRevision+1 != attempt.Revision {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			key := domain.AttachedWorkerAttemptMessageV1{OwnerUserID: attempt.OwnerUserID, WorkerID: attempt.WorkerID, AttemptID: attempt.AttemptID, Direction: domain.AttachedWorkerAttemptPlatformToWorker, AttemptSequence: attempt.PlatformAttemptSequence}
			message, ok, err := readAttachedWorkerAttemptMessageTx(ctx, tx, key)
			if err != nil {
				return err
			}
			if !ok || message.Kind != domain.AttachedWorkerAttemptMessageCancelRequested {
				return ErrAttachedWorkerAttemptConflict
			}
			if attempt.State == domain.AttachedWorkerAttemptRetired {
				frame, direction, decodeErr := decodeAttachedWorkerAttemptFrame(message)
				if request.Reason != ports.AttachedWorkerFenceCancelAckUnknown || decodeErr != nil ||
					direction != attachedworkerprotocol.DirectionPlatformToWorker || frame.Cancel == nil ||
					(frame.Cancel.Code != attachedworkerprotocol.CancelRequested && frame.Cancel.Code != attachedworkerprotocol.CancelFenced) {
					result.Status = ports.AttachedWorkerExecutionConflict
					return nil
				}
			}
			result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionReplayed, Attempt: attempt, Outbound: &message}
			return nil
		}
		if attempt.Revision != request.CandidateAttemptRevision || attempt.LeaseGeneration != request.LeaseGeneration {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		deadline := canonicalAttachedWorkerTime(request.DeadlineAt)
		if at.Before(deadline) {
			result.Status = ports.AttachedWorkerExecutionDenied
			return nil
		}
		if request.Reason == ports.AttachedWorkerFenceLeaseExpired && !attempt.LeaseExpiresAt.Equal(deadline) {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		if request.Reason == ports.AttachedWorkerFenceCancelAckUnknown && !attempt.CancelDeadline.Equal(deadline) {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		if attempt.State == domain.AttachedWorkerAttemptCancelledBeforeClaim && request.Reason == ports.AttachedWorkerFenceCancelAckUnknown {
			worker, workerFound, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
			if err != nil {
				return err
			}
			connection, connectionFound, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
			if err != nil {
				return err
			}
			if !workerFound || !connectionFound || worker.ConnectionGeneration < attempt.ConnectionGeneration {
				return ErrAttachedWorkerAttemptConflict
			}
			if worker.ConnectionGeneration == attempt.ConnectionGeneration {
				if connection.ID != attempt.ConnectionID || connection.ConnectionGeneration != attempt.ConnectionGeneration {
					return ErrAttachedWorkerAttemptConflict
				}
				if worker.ConnectionGeneration == math.MaxUint64 || worker.Revision == math.MaxUint64 || connection.Revision == math.MaxUint64 {
					return ErrAttachedWorkerAttemptConflict
				}
				oldExpiry := attachedWorkerPresenceExpiry(connection)
				worker.ConnectionGeneration++
				worker.ObservedState = domain.AttachedWorkerObservedOffline
				worker.Revision++
				worker.UpdatedAt = at
				connection.State = domain.AttachedWorkerConnectionSuperseded
				connection.Revision++
				if err := worker.Validate(); err != nil {
					return err
				}
				if err := connection.Validate(); err != nil {
					return err
				}
				audit := domain.AttachedWorkerAuditEvent{
					Version: domain.AttachedWorkerAuditEventVersionV1, TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID,
					WorkerID: worker.ID, Action: domain.AttachedWorkerAuditConnectionGenerationAdvanced,
					WorkerRevision: worker.Revision, EnrollmentGeneration: worker.EnrollmentGeneration,
					ConnectionGeneration: worker.ConnectionGeneration, OccurredAt: at,
				}
				if err := audit.Validate(); err != nil {
					return err
				}
				if err := updateAttachedWorkerTx(ctx, tx, worker); err != nil {
					return err
				}
				if err := insertAttachedWorkerAuditEventTx(ctx, tx, audit); err != nil {
					return err
				}
				if err := deleteAttachedWorkerPresenceExpiryTx(ctx, tx, oldExpiry); err != nil {
					return err
				}
				if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
					return err
				}
			}
			next, err := retireUnacknowledgedPreClaimCancellation(attempt, at)
			if err != nil {
				return err
			}
			if err := compareAndSwapAttachedWorkerAttemptTx(ctx, tx, attempt.Revision, next, at.Add(store.operationalRetention)); err != nil {
				return err
			}
			if err := deleteAttachedWorkerAttemptDeadlinesTx(ctx, tx, attempt); err != nil {
				return err
			}
			message, ok, err := readAttachedWorkerAttemptMessageByKindTx(ctx, tx, attempt.OwnerUserID, attempt.WorkerID,
				attempt.AttemptID, domain.AttachedWorkerAttemptPlatformToWorker, domain.AttachedWorkerAttemptMessageCancelRequested)
			if err != nil {
				return err
			}
			if !ok {
				return ErrAttachedWorkerAttemptConflict
			}
			result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionFenced, Attempt: next, Outbound: &message}
			return nil
		}
		if fenceAttachedWorkerAttemptWithoutProtocolMutation(attempt.State) {
			key := domain.AttachedWorkerAttemptMessageV1{OwnerUserID: attempt.OwnerUserID, WorkerID: attempt.WorkerID, AttemptID: attempt.AttemptID, Direction: domain.AttachedWorkerAttemptPlatformToWorker, AttemptSequence: attempt.PlatformAttemptSequence}
			message, ok, err := readAttachedWorkerAttemptMessageTx(ctx, tx, key)
			if err != nil {
				return err
			}
			if !ok || message.Kind != domain.AttachedWorkerAttemptMessageCancelRequested {
				return ErrAttachedWorkerAttemptConflict
			}
			next, err := fenceAttachedWorkerCommittedCancel(attempt, at)
			if err != nil {
				return err
			}
			if err := compareAndSwapAttachedWorkerAttemptTx(ctx, tx, attempt.Revision, next, at.Add(store.operationalRetention)); err != nil {
				return err
			}
			if err := deleteAttachedWorkerAttemptDeadlinesTx(ctx, tx, attempt); err != nil {
				return err
			}
			result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionFenced, Attempt: next, Outbound: &message}
			return nil
		}
		worker, found, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		connection, connectionFound, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || !connectionFound {
			return ErrAttachedWorkerAttemptConflict
		}
		config, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
		if err != nil {
			return err
		}
		frame, post, err := attachedworkerprotocol.BuildCancelTransitionV1(config, snapshot, attachedworkerprotocol.CancelAuthorityV1{Revision: 1, Code: attachedworkerprotocol.CancelFenced, NowUnixMicro: at.UnixMicro()})
		if err != nil {
			return err
		}
		message, err := attachedWorkerAttemptMessageFromFrame(attempt, attachedworkerprotocol.DirectionPlatformToWorker, frame, at)
		if err != nil {
			return err
		}
		next := attempt
		next.PlatformAttemptSequence = message.AttemptSequence
		next.CancelRevision = 1
		next.CancelDeadline = deadline
		next.State = domain.AttachedWorkerAttemptFencedUnknown
		if attempt.State == domain.AttachedWorkerAttemptOffered {
			next.State = domain.AttachedWorkerAttemptCancelledBeforeClaim
		}
		next.UpdatedAt, next.Revision = at, attempt.Revision+1
		previousConnectionState := connection.State
		oldExpiry := attachedWorkerPresenceExpiry(connection)
		connection, err = advanceAttachedWorkerConnectionProtocol(connection, post)
		if err != nil {
			return err
		}
		if previousConnectionState == domain.AttachedWorkerConnectionOffline {
			connection.State = previousConnectionState
		}
		retainUntil := at.Add(store.operationalRetention)
		if _, err := insertOrReconcileAttachedWorkerAttemptMessageTx(ctx, tx, message, retainUntil); err != nil {
			return err
		}
		if err := compareAndSwapAttachedWorkerAttemptTx(ctx, tx, attempt.Revision, next, retainUntil); err != nil {
			return err
		}
		if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
			return err
		}
		if previousConnectionState != domain.AttachedWorkerConnectionOffline {
			if err := deleteAttachedWorkerPresenceExpiryTx(ctx, tx, oldExpiry); err != nil {
				return err
			}
			if err := insertAttachedWorkerPresenceExpiryTx(ctx, tx, attachedWorkerPresenceExpiry(connection)); err != nil {
				return err
			}
		}
		bucket, _ := domain.AttachedWorkerAttemptDeadlineBucketV1(next.TenantID, next.OwnerUserID, next.WorkerID, next.AttemptID)
		kind := domain.AttachedWorkerDeadlineLeaseExpiry
		if request.Reason == ports.AttachedWorkerFenceCancelAckUnknown {
			kind = domain.AttachedWorkerDeadlineCancelAck
		}
		if err := deleteAttachedWorkerAttemptDeadlineTx(ctx, tx, domain.AttachedWorkerAttemptDeadlineV1{Bucket: bucket, DeadlineAt: deadline, TenantID: next.TenantID, OwnerUserID: next.OwnerUserID, WorkerID: next.WorkerID, AttemptID: next.AttemptID, Kind: kind, LeaseGeneration: next.LeaseGeneration, AttemptRevision: attempt.Revision}); err != nil {
			return err
		}
		result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionFenced, Attempt: next, Outbound: &message}
		return nil
	})
	return result, err
}

func (store *Store) ListDueAttachedWorkerAttemptDeadlines(ctx context.Context, bucket uint32, before time.Time, cursor ports.AttachedWorkerAttemptDeadlineCursor, limit uint64) (ports.AttachedWorkerAttemptDeadlinePage, error) {
	if bucket >= domain.AttachedWorkerAttemptDeadlineBuckets || before.IsZero() || limit == 0 || limit > maxAttachedWorkerAttemptDeadlineListLimit {
		return ports.AttachedWorkerAttemptDeadlinePage{}, domain.ValidationError{Field: "attached_worker_attempt_deadline.list", Reason: "requires a valid bucket, before timestamp, and bounded positive limit"}
	}
	if err := validateAttachedWorkerAttemptDeadlineCursor(cursor); err != nil {
		return ports.AttachedWorkerAttemptDeadlinePage{}, err
	}
	before = canonicalAttachedWorkerTime(before)
	var resultRows *sql.Rows
	var err error
	if !cursor.Present {
		query := `SELECT shard_bucket,deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind,lease_generation,attempt_revision
		 FROM attached_worker_attempt_deadlines_v1
		 WHERE shard_bucket=$1 AND deadline_at <= $2
		 ORDER BY deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind LIMIT $3`
		resultRows, err = store.db.QueryContext(ctx, query, bucket, before, limit)
	} else {
		query := `SELECT shard_bucket,deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind,lease_generation,attempt_revision
		 FROM attached_worker_attempt_deadlines_v1
		 WHERE shard_bucket=$1 AND deadline_at <= $2
		   AND (deadline_at > $3
		     OR (deadline_at = $3 AND tenant_id > $4)
		     OR (deadline_at = $3 AND tenant_id = $4 AND owner_user_id > $5)
		     OR (deadline_at = $3 AND tenant_id = $4 AND owner_user_id = $5 AND worker_id > $6)
		     OR (deadline_at = $3 AND tenant_id = $4 AND owner_user_id = $5 AND worker_id = $6 AND attempt_id > $7)
		     OR (deadline_at = $3 AND tenant_id = $4 AND owner_user_id = $5 AND worker_id = $6 AND attempt_id = $7 AND kind > $8))
		 ORDER BY deadline_at,tenant_id,owner_user_id,worker_id,attempt_id,kind LIMIT $9`
		resultRows, err = store.db.QueryContext(ctx, query, bucket, before, canonicalAttachedWorkerTime(cursor.DeadlineAt),
			cursor.TenantID, cursor.OwnerUserID, cursor.WorkerID, cursor.AttemptID, cursor.Kind, limit)
	}
	if err != nil {
		return ports.AttachedWorkerAttemptDeadlinePage{}, err
	}
	defer resultRows.Close()
	page := ports.AttachedWorkerAttemptDeadlinePage{Items: make([]domain.AttachedWorkerAttemptDeadlineV1, 0, limit)}
	var scanned uint64
	for resultRows.Next() {
		item, err := scanAttachedWorkerAttemptDeadlineRaw(resultRows)
		if err != nil {
			return ports.AttachedWorkerAttemptDeadlinePage{}, err
		}
		scanned++
		page.NextCursor = ports.AttachedWorkerAttemptDeadlineCursor{Present: true, DeadlineAt: item.DeadlineAt, TenantID: item.TenantID,
			OwnerUserID: item.OwnerUserID, WorkerID: item.WorkerID, AttemptID: item.AttemptID, Kind: item.Kind}
		if item.Validate() != nil {
			page.SkippedInvalid++
			continue
		}
		page.Items = append(page.Items, item)
	}
	if err := resultRows.Err(); err != nil {
		return ports.AttachedWorkerAttemptDeadlinePage{}, err
	}
	page.HasMore = scanned == limit
	return page, nil
}

func validateAttachedWorkerAttemptOffer(request ports.AttachedWorkerAttemptOffer) error {
	if err := validateAttachedWorkerTransportScope(request.TenantID, request.OwnerUserID, request.WorkerID); err != nil {
		return err
	}
	for _, validate := range []func() error{request.RunID.Validate, request.AttemptID.Validate, request.ReservationID.Validate, request.LeaseID.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if request.LeaseTTL <= 0 || request.LeaseTTL > maxAttachedWorkerAttemptLeaseTTL {
		return domain.ValidationError{Field: "attached_worker_attempt.offer", Reason: "has invalid bounded lease TTL"}
	}
	wantLeaseID, err := domain.NewAttachedWorkerLeaseIDV1(request.TenantID, request.RunID, request.AttemptID)
	if err != nil {
		return err
	}
	if request.LeaseID != wantLeaseID {
		return domain.ValidationError{Field: "attached_worker_attempt.offer.lease_id", Reason: "must be the canonical retry-stable lease identifier"}
	}
	return nil
}

func validateAttachedWorkerAttemptExchange(request ports.AttachedWorkerAttemptExchange) error {
	if err := validateAttachedWorkerTransportScope(request.TenantID, request.OwnerUserID, request.WorkerID); err != nil {
		return err
	}
	if err := request.ConnectionID.Validate(); err != nil {
		return err
	}
	if err := request.AttemptID.Validate(); err != nil {
		return err
	}
	if err := request.PresentedSecretDigest.Validate(); err != nil {
		return err
	}
	if request.LeaseGeneration == 0 || request.InboundFrame.Validate() != nil {
		return domain.ValidationError{Field: "attached_worker_attempt.exchange", Reason: "has invalid generation or frame"}
	}
	binding, ok := attachedWorkerFrameBinding(request.InboundFrame)
	if !ok || request.InboundFrame.WorkerID != string(request.WorkerID) || binding.AttemptID != string(request.AttemptID) || binding.LeaseGeneration != request.LeaseGeneration {
		return domain.ValidationError{Field: "attached_worker_attempt.exchange.frame", Reason: "does not match the authoritative route"}
	}
	return nil
}

func validateAttachedWorkerAttemptPoll(request ports.AttachedWorkerAttemptPoll) error {
	if err := validateAttachedWorkerTransportScope(request.TenantID, request.OwnerUserID, request.WorkerID); err != nil {
		return err
	}
	if err := request.ConnectionID.Validate(); err != nil {
		return err
	}
	if err := request.PresentedSecretDigest.Validate(); err != nil {
		return err
	}
	return nil
}

func validateAttachedWorkerCancellationRequest(request ports.AttachedWorkerCancellationRequest) error {
	if err := validateAttachedWorkerTransportScope(request.TenantID, request.OwnerUserID, request.WorkerID); err != nil {
		return err
	}
	if err := request.AttemptID.Validate(); err != nil {
		return err
	}
	if request.LeaseGeneration == 0 || request.AckTimeout <= 0 || request.AckTimeout > maxAttachedWorkerAttemptLeaseTTL {
		return domain.ValidationError{Field: "attached_worker_attempt.cancellation", Reason: "has invalid generation or bounded ack timeout"}
	}
	return nil
}

func validateAttachedWorkerTerminalCommit(request ports.AttachedWorkerTerminalCommit) error {
	if err := validateAttachedWorkerTransportScope(request.TenantID, request.OwnerUserID, request.WorkerID); err != nil {
		return err
	}
	if err := request.AttemptID.Validate(); err != nil {
		return err
	}
	if err := request.Materialization.EvidenceDigest.Validate(); err != nil {
		return err
	}
	if request.LeaseGeneration == 0 || (request.Materialization.Completion == nil) == (request.Materialization.Failure == nil) {
		return domain.ValidationError{Field: "attached_worker_attempt.terminal_commit", Reason: "requires an exact lease generation and one canonical materialization"}
	}
	return nil
}

func validateAttachedWorkerAttemptFence(request ports.AttachedWorkerAttemptFence) error {
	if err := validateAttachedWorkerTransportScope(request.TenantID, request.OwnerUserID, request.WorkerID); err != nil {
		return err
	}
	if err := request.AttemptID.Validate(); err != nil {
		return err
	}
	if request.LeaseGeneration == 0 || request.CandidateAttemptRevision == 0 || request.DeadlineAt.IsZero() ||
		(request.Reason != ports.AttachedWorkerFenceLeaseExpired && request.Reason != ports.AttachedWorkerFenceCancelAckUnknown) {
		return domain.ValidationError{Field: "attached_worker_attempt.fence", Reason: "requires exact deadline, revision, generation, and reason"}
	}
	return nil
}

func materializeAttachedWorkerTerminalTx(ctx context.Context, state ports.StateTx, tx *stateTx, attempt domain.AttachedWorkerAttemptV1, materialization ports.AttachedWorkerTerminalMaterialization, at time.Time) error {
	if err := validateAttachedWorkerTerminalMaterializationStatus(attempt.TerminalStatus, materialization); err != nil {
		return err
	}
	if err := validateAttachedWorkerTerminalMaterializationBinding(attempt, materialization); err != nil {
		return err
	}
	evidenceDigest, err := attachedWorkerTerminalMaterializationDigest(attempt.TerminalStatus, materialization)
	if err != nil || evidenceDigest != attempt.TerminalEvidenceDigest || evidenceDigest != materialization.EvidenceDigest {
		return ErrAttachedWorkerAttemptConflict
	}
	run, canonicalAttempt, reservation, err := loadWorkerTerminalState(ctx, state, tx, attempt.RunID, attempt.AttemptID, attempt.ReservationID)
	if err != nil {
		return err
	}
	if materialization.Completion != nil {
		completion := *materialization.Completion
		if len(completion.Events) == 0 {
			return ErrAttachedWorkerAttemptConflict
		}
		if err := validateCanonicalFinalizationEvents(domain.RunSucceeded, completion.Events); err != nil {
			return err
		}
		if err := completion.Manifest.ValidateForRun(run); err != nil {
			return err
		}
		digest, err := runFinalizationDigest(domain.RunSucceeded, &completion.Manifest, completion.Events)
		if err != nil {
			return err
		}
		matched, err := matchingRunFinalizationTx(ctx, tx, run.ID, domain.RunSucceeded, digest)
		if err != nil {
			return err
		}
		if matched {
			return nil
		}
		if run.Status.Terminal() {
			return ErrRunFinalizationConflict
		}
		return completeWorkerSuccessTx(ctx, state, tx, run, canonicalAttempt, reservation, attempt.LeaseID, attempt.LeaseGeneration, at, completion.Manifest, completion.Usage,
			func(run domain.Run) error {
				return appendCanonicalFinalizationTx(ctx, tx, run, domain.RunSucceeded, digest, completion.Events, at)
			})
	}
	failure := *materialization.Failure
	if len(failure.Events) == 0 {
		return ErrAttachedWorkerAttemptConflict
	}
	runStatus, attemptStatus := domain.RunFailed, domain.AttemptFailed
	if failure.Cancelled {
		runStatus, attemptStatus = domain.RunCancelled, domain.AttemptCancelled
	}
	if err := validateCanonicalFinalizationEvents(runStatus, failure.Events); err != nil {
		return err
	}
	digest, err := runFinalizationDigest(runStatus, nil, failure.Events)
	if err != nil {
		return err
	}
	matched, err := matchingRunFinalizationTx(ctx, tx, run.ID, runStatus, digest)
	if err != nil {
		return err
	}
	if matched {
		return nil
	}
	if run.Status.Terminal() {
		return ErrRunFinalizationConflict
	}
	return failWorkerTx(ctx, state, tx, run, canonicalAttempt, reservation, runStatus, attemptStatus, attempt.LeaseID, attempt.LeaseGeneration, at, failure.Code,
		func(run domain.Run) error {
			return appendCanonicalFinalizationTx(ctx, tx, run, runStatus, digest, failure.Events, at)
		})
}

func validateAttachedWorkerTerminalMaterializationBinding(attempt domain.AttachedWorkerAttemptV1, materialization ports.AttachedWorkerTerminalMaterialization) error {
	if materialization.Completion != nil {
		completion := materialization.Completion
		if completion.TenantID != attempt.TenantID || completion.RunID != attempt.RunID || completion.AttemptID != attempt.AttemptID ||
			completion.ReservationID != attempt.ReservationID || completion.LeaseID != attempt.LeaseID || completion.Fence != attempt.LeaseGeneration {
			return ErrAttachedWorkerAttemptConflict
		}
		return nil
	}
	if materialization.Failure != nil {
		failure := materialization.Failure
		if failure.TenantID != attempt.TenantID || failure.RunID != attempt.RunID || failure.AttemptID != attempt.AttemptID ||
			failure.ReservationID != attempt.ReservationID || failure.LeaseID != attempt.LeaseID || failure.Fence != attempt.LeaseGeneration {
			return ErrAttachedWorkerAttemptConflict
		}
		return nil
	}
	return ErrAttachedWorkerAttemptConflict
}

func sameAttachedWorkerRetiredAttemptBinding(current, reconstructed domain.AttachedWorkerAttemptV1) bool {
	return current.State == domain.AttachedWorkerAttemptRetired && current.TenantID == reconstructed.TenantID &&
		current.OwnerUserID == reconstructed.OwnerUserID && current.WorkerID == reconstructed.WorkerID &&
		current.ConnectionID == reconstructed.ConnectionID && current.RunID == reconstructed.RunID &&
		current.AttemptID == reconstructed.AttemptID && current.ReservationID == reconstructed.ReservationID &&
		current.LeaseID == reconstructed.LeaseID && current.LeaseGeneration == reconstructed.LeaseGeneration &&
		current.FenceToken == reconstructed.FenceToken && current.EnrollmentGeneration == reconstructed.EnrollmentGeneration &&
		current.ConnectionGeneration == reconstructed.ConnectionGeneration && current.ContextDigest == reconstructed.ContextDigest &&
		current.CapabilityDigest == reconstructed.CapabilityDigest && current.PolicyDigest == reconstructed.PolicyDigest &&
		current.PlatformAttemptSequence == reconstructed.PlatformAttemptSequence && current.WorkerAttemptSequence == reconstructed.WorkerAttemptSequence &&
		current.TerminalSequence == reconstructed.TerminalSequence && current.TerminalStatus == reconstructed.TerminalStatus &&
		current.TerminalEvidenceDigest == reconstructed.TerminalEvidenceDigest && current.LeaseExpiresAt.Equal(reconstructed.LeaseExpiresAt)
}

func validateAttachedWorkerTerminalMaterializationStatus(status domain.AttachedWorkerTerminalStatus, materialization ports.AttachedWorkerTerminalMaterialization) error {
	switch {
	case materialization.Completion != nil && materialization.Failure == nil && status == domain.AttachedWorkerTerminalSucceeded:
		return nil
	case materialization.Completion == nil && materialization.Failure != nil && !materialization.Failure.Cancelled && status == domain.AttachedWorkerTerminalFailed:
		return nil
	case materialization.Completion == nil && materialization.Failure != nil && materialization.Failure.Cancelled && status == domain.AttachedWorkerTerminalCancelled:
		return nil
	default:
		return ErrAttachedWorkerAttemptConflict
	}
}

// attachedWorkerTerminalMaterializationDigest is the exact content commitment
// that a worker signs in Terminal evidence. It intentionally reuses the
// canonical run-finalization identity: status, artifact manifest and ordered
// event identities. Transport metadata, timestamps and sampled usage are not
// allowed to change or substitute canonical product output.
func attachedWorkerTerminalMaterializationDigest(status domain.AttachedWorkerTerminalStatus, materialization ports.AttachedWorkerTerminalMaterialization) (domain.AttachedWorkerTerminalEvidenceDigest, error) {
	if err := validateAttachedWorkerTerminalMaterializationStatus(status, materialization); err != nil {
		return "", err
	}
	var (
		runStatus domain.RunStatus
		manifest  *domain.ArtifactManifest
		events    []domain.SessionEventDraft
	)
	if materialization.Completion != nil {
		runStatus = domain.RunSucceeded
		manifest = &materialization.Completion.Manifest
		events = materialization.Completion.Events
	} else {
		runStatus = domain.RunFailed
		if materialization.Failure.Cancelled {
			runStatus = domain.RunCancelled
		}
		events = materialization.Failure.Events
	}
	digest, err := runFinalizationDigest(runStatus, manifest, events)
	if err != nil {
		return "", err
	}
	return domain.AttachedWorkerTerminalEvidenceDigest(digest), nil
}

func attachedWorkerOfferEligible(request ports.AttachedWorkerAttemptOffer, worker domain.AttachedWorker, connection domain.AttachedWorkerConnection, loaded ports.WorkerJobState, placement domain.ExecutionPlacementV2, at, expiresAt time.Time) bool {
	wantTTL, err := domain.AttachedWorkerLeaseTTLForLimitsV1(loaded.Job.Limits)
	return err == nil && request.LeaseTTL == wantTTL && placement.Kind == domain.ExecutionPlacementAttachedWorker &&
		placement.OwnerUserID == request.OwnerUserID && placement.WorkerID == request.WorkerID &&
		worker.TenantID == request.TenantID && worker.OwnerUserID == request.OwnerUserID && worker.ID == request.WorkerID &&
		worker.DesiredState == domain.AttachedWorkerDesiredActive && worker.ObservedState == domain.AttachedWorkerObservedOnline &&
		connection.TenantID == request.TenantID && connection.OwnerUserID == request.OwnerUserID && connection.WorkerID == request.WorkerID &&
		connection.EnrollmentGeneration == worker.EnrollmentGeneration && connection.ConnectionGeneration == worker.ConnectionGeneration &&
		connection.State == domain.AttachedWorkerConnectionOnline && connection.CapabilityDigest == placement.CapabilityDigest && connection.AuthExpiresAt.After(expiresAt) &&
		connection.PresenceExpiresAt.After(at) &&
		loaded.Run.ID == request.RunID && loaded.Attempt.ID == request.AttemptID && loaded.Reservation.ID == request.ReservationID &&
		loaded.Run.Status == domain.RunQueued && loaded.Attempt.Status == domain.AttemptCreated &&
		loaded.Reservation.Status == domain.ReservationHeld && loaded.Reservation.ExpiresAt.After(at)
}

func attachedWorkerExecutionAuthorityCurrent(worker domain.AttachedWorker, connection domain.AttachedWorkerConnection) bool {
	return worker.TenantID == connection.TenantID && worker.OwnerUserID == connection.OwnerUserID && worker.ID == connection.WorkerID &&
		worker.DesiredState != domain.AttachedWorkerDesiredRevoked &&
		worker.EnrollmentGeneration == connection.EnrollmentGeneration && worker.ConnectionGeneration == connection.ConnectionGeneration &&
		connection.State == domain.AttachedWorkerConnectionOnline
}

func attachedWorkerStateAfterCancelAck(current domain.AttachedWorkerAttemptState) domain.AttachedWorkerAttemptState {
	if current == domain.AttachedWorkerAttemptCancelledBeforeClaim {
		return domain.AttachedWorkerAttemptCancelledBeforeClaim
	}
	return domain.AttachedWorkerAttemptCancelAcknowledged
}

func attachedWorkerCancellationReplayState(state domain.AttachedWorkerAttemptState) bool {
	switch state {
	case domain.AttachedWorkerAttemptCancelRequested,
		domain.AttachedWorkerAttemptCancelledBeforeClaim,
		domain.AttachedWorkerAttemptCancelAcknowledged,
		domain.AttachedWorkerAttemptTerminalPending,
		domain.AttachedWorkerAttemptTerminalCommitted,
		domain.AttachedWorkerAttemptFencedUnknown,
		domain.AttachedWorkerAttemptRetired:
		return true
	default:
		return false
	}
}

func attachedWorkerProgressAllowed(state domain.AttachedWorkerAttemptState) bool {
	switch state {
	case domain.AttachedWorkerAttemptClaimed,
		domain.AttachedWorkerAttemptCancelRequested,
		domain.AttachedWorkerAttemptCancelAcknowledged:
		return true
	default:
		return false
	}
}

func attachedWorkerCancellationFrameExpired(attempt domain.AttachedWorkerAttemptV1, at time.Time) bool {
	return attempt.State == domain.AttachedWorkerAttemptCancelRequested && !at.Before(attempt.CancelDeadline)
}

func sameAttachedWorkerAttemptBinding(left, right attachedworkerprotocol.AttemptBindingV1) bool {
	return left.RunID == right.RunID && left.AttemptID == right.AttemptID && left.LeaseID == right.LeaseID &&
		left.LeaseGeneration == right.LeaseGeneration && left.FenceToken == right.FenceToken &&
		left.ExpiresAtUnixMicro == right.ExpiresAtUnixMicro && bytes.Equal(left.ContextDigest, right.ContextDigest) &&
		bytes.Equal(left.CapabilityDigest, right.CapabilityDigest) && bytes.Equal(left.PolicyDigest, right.PolicyDigest)
}

func attachedWorkerMaterializationReservationID(materialization ports.AttachedWorkerTerminalMaterialization) domain.QuotaReservationID {
	if materialization.Completion != nil {
		return materialization.Completion.ReservationID
	}
	if materialization.Failure != nil {
		return materialization.Failure.ReservationID
	}
	return ""
}

func applyAttachedWorkerTerminalTransition(next *domain.AttachedWorkerAttemptV1, previous domain.AttachedWorkerAttemptState, terminal attachedworkerprotocol.TerminalV1, protocolState attachedworkerprotocol.AttemptState) error {
	if next == nil {
		return ErrAttachedWorkerAttemptConflict
	}
	switch protocolState {
	case attachedworkerprotocol.AttemptTerminalPending:
		next.State = domain.AttachedWorkerAttemptTerminalPending
		next.TerminalSequence = terminal.TerminalSequence
		next.TerminalStatus = domain.AttachedWorkerTerminalStatus(terminal.Status)
		next.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(hex.EncodeToString(terminal.EvidenceDigest))
		return nil
	case attachedworkerprotocol.AttemptCancelRequested:
		// The protocol deliberately consumes a crossed non-cancelled terminal
		// after CancelRequested without making it committable. Preserve the
		// cancellation head and deadline while the directional fingerprint makes
		// exact replay idempotent and divergent replay conflicting.
		if previous != domain.AttachedWorkerAttemptCancelRequested || terminal.Status == attachedworkerprotocol.TerminalCancelled {
			return ErrAttachedWorkerAttemptConflict
		}
		next.State = previous
		return nil
	case attachedworkerprotocol.AttemptCancelAcked:
		// A success/failure terminal may cross an already acknowledged cancel.
		// The protocol consumes it only for sequence/replay continuity; it is not
		// committable and must not replace the acknowledged cancellation.
		if previous != domain.AttachedWorkerAttemptCancelAcknowledged || terminal.Status == attachedworkerprotocol.TerminalCancelled {
			return ErrAttachedWorkerAttemptConflict
		}
		next.State = previous
		return nil
	default:
		return ErrAttachedWorkerAttemptConflict
	}
}

func attachedWorkerReplayOutboundSequence(inbound domain.AttachedWorkerAttemptMessageV1, currentPlatformSequence uint64) uint64 {
	if inbound.Kind == domain.AttachedWorkerAttemptMessageLeaseClaim {
		// LeaseClaim is attempt sequence 1 and its canonical acceptance is
		// platform attempt sequence 2. Later progress or cancellation may advance
		// the head, but an exact duplicate claim reconciles to the original
		// acceptance rather than whichever platform frame happens to be latest.
		return inbound.AttemptSequence + 1
	}
	return currentPlatformSequence
}

func retireAttachedWorkerCommittedAttempt(attempt domain.AttachedWorkerAttemptV1, at time.Time) (domain.AttachedWorkerAttemptV1, error) {
	if attempt.State != domain.AttachedWorkerAttemptTerminalCommitted || attempt.Revision == math.MaxUint64 || at.Before(attempt.UpdatedAt) {
		return domain.AttachedWorkerAttemptV1{}, ErrAttachedWorkerAttemptConflict
	}
	next := attempt
	next.State = domain.AttachedWorkerAttemptRetired
	next.UpdatedAt = canonicalAttachedWorkerTime(at)
	next.Revision++
	return next, nil
}

func reconcileAttachedWorkerAttemptOfferTx(ctx context.Context, tx *stateTx, request ports.AttachedWorkerAttemptOffer, existing domain.AttachedWorkerAttemptV1, result *ports.AttachedWorkerAttemptResult) error {
	if existing.State != domain.AttachedWorkerAttemptOffered || existing.RunID != request.RunID || existing.AttemptID != request.AttemptID || existing.ReservationID != request.ReservationID || existing.LeaseID != request.LeaseID {
		result.Status = ports.AttachedWorkerExecutionConflict
		return nil
	}
	loaded, found, err := loadWorkerJobStateTx(ctx, tx, request.RunID)
	if err != nil || !found {
		return err
	}
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(loaded.Job, loaded.InputManifest)
	wantTTL, ttlErr := domain.AttachedWorkerLeaseTTLForLimitsV1(loaded.Job.Limits)
	if err != nil || ttlErr != nil || request.LeaseTTL != wantTTL || existing.LeaseExpiresAt.Sub(existing.CreatedAt) != wantTTL ||
		loaded.Job.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker ||
		loaded.Job.ExecutionPlacementV2.OwnerUserID != request.OwnerUserID || loaded.Job.ExecutionPlacementV2.WorkerID != request.WorkerID ||
		existing.ContextDigest != contextDigest || existing.CapabilityDigest != loaded.Job.ExecutionPlacementV2.CapabilityDigest ||
		existing.PolicyDigest != loaded.Job.ExecutionPlacementV2.PolicyDigest {
		result.Status = ports.AttachedWorkerExecutionConflict
		return nil
	}
	worker, found, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
	if err != nil || !found {
		return err
	}
	connection, found, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
	if err != nil || !found {
		return err
	}
	if existing.ConnectionID != connection.ID || existing.EnrollmentGeneration != connection.EnrollmentGeneration ||
		existing.ConnectionGeneration != connection.ConnectionGeneration || connection.CapabilityDigest != existing.CapabilityDigest {
		result.Status = ports.AttachedWorkerExecutionConflict
		return nil
	}
	_, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
	if err != nil || snapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptOffered {
		result.Status = ports.AttachedWorkerExecutionConflict
		return nil
	}
	binding := snapshot.Attempt.Summary.Binding
	if binding.RunID != string(existing.RunID) || binding.AttemptID != string(existing.AttemptID) ||
		binding.LeaseID != string(existing.LeaseID) || binding.LeaseGeneration != existing.LeaseGeneration ||
		binding.FenceToken != string(existing.FenceToken) {
		result.Status = ports.AttachedWorkerExecutionConflict
		return nil
	}
	key := domain.AttachedWorkerAttemptMessageV1{OwnerUserID: existing.OwnerUserID, WorkerID: existing.WorkerID,
		AttemptID: existing.AttemptID, Direction: domain.AttachedWorkerAttemptPlatformToWorker,
		AttemptSequence: existing.PlatformAttemptSequence}
	message, found, err := readAttachedWorkerAttemptMessageTx(ctx, tx, key)
	if err != nil {
		return err
	}
	if !found || message.Kind != domain.AttachedWorkerAttemptMessageLeaseOffered {
		return ErrAttachedWorkerAttemptConflict
	}
	result.Status, result.Attempt, result.Outbound = ports.AttachedWorkerExecutionReplayed, existing, &message
	return nil
}
