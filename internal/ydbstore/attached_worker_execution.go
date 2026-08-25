package ydbstore

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

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
	canonicalContextDigest, err := domain.AttachedWorkerJobContextDigestV1(loaded.Job)
	if err != nil {
		result.Status = ports.AttachedWorkerExecutionDenied
		return nil
	}
	placement := loaded.Job.ExecutionPlacement
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
		_, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
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
	scope := domain.AttachedWorkerAttemptV1{TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID, AttemptID: request.AttemptID}
	inboundMessage, err := attachedWorkerAttemptMessageFromFrame(scope, attachedworkerprotocol.DirectionWorkerToPlatform, request.InboundFrame, time.Unix(1, 0).UTC())
	if err != nil {
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
				key := domain.AttachedWorkerAttemptMessageV1{OwnerUserID: attempt.OwnerUserID, WorkerID: attempt.WorkerID, AttemptID: attempt.AttemptID, Direction: domain.AttachedWorkerAttemptPlatformToWorker, AttemptSequence: attempt.PlatformAttemptSequence}
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
		if !found || !connectionFound || !attachedWorkerExecutionAuthorityCurrent(worker, connection) ||
			connection.ID != request.ConnectionID || connection.Revision == math.MaxUint64 ||
			request.InboundFrame.Sequence != connection.WorkerSequence+1 ||
			connection.ConnectionGeneration != attempt.ConnectionGeneration || subtle.ConstantTimeCompare([]byte(connection.SecretDigest), []byte(request.PresentedSecretDigest)) != 1 ||
			!at.Before(connection.AuthExpiresAt) || !at.Before(connection.PresenceExpiresAt) {
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
			if inboundFrame.Progress == nil || attempt.State != domain.AttachedWorkerAttemptClaimed {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			next.ProgressSequence = inboundFrame.Progress.ProgressSequence
		case domain.AttachedWorkerAttemptMessageCancelAcknowledged:
			if inboundFrame.CancelAck == nil || (attempt.State != domain.AttachedWorkerAttemptCancelRequested && attempt.State != domain.AttachedWorkerAttemptCancelledBeforeClaim) || inboundFrame.CancelAck.CancelRevision != attempt.CancelRevision {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			next.State = domain.AttachedWorkerAttemptCancelAcknowledged
		case domain.AttachedWorkerAttemptMessageTerminal:
			if inboundFrame.Terminal == nil {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			next.State = domain.AttachedWorkerAttemptTerminalPending
			next.TerminalSequence = inboundFrame.Terminal.TerminalSequence
			next.TerminalStatus = domain.AttachedWorkerTerminalStatus(inboundFrame.Terminal.Status)
			next.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(hex.EncodeToString(inboundFrame.Terminal.EvidenceDigest))
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
		if err := upsertAttachedWorkerAttemptDeadlineTx(ctx, tx, leaseDeadline); err != nil {
			return err
		}
		if next.State == domain.AttachedWorkerAttemptCancelAcknowledged || next.State == domain.AttachedWorkerAttemptTerminalPending {
			cancel := leaseDeadline
			cancel.Kind, cancel.DeadlineAt = domain.AttachedWorkerDeadlineCancelAck, next.CancelDeadline
			if !cancel.DeadlineAt.IsZero() {
				if err := deleteAttachedWorkerAttemptDeadlineTx(ctx, tx, cancel); err != nil {
					return err
				}
			}
		}
		result = ports.AttachedWorkerAttemptResult{Status: ports.AttachedWorkerExecutionApplied, Attempt: next, Outbound: outboundMessage}
		return nil
	})
	return result, err
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
			result.Status = ports.AttachedWorkerExecutionNotFound
			return nil
		}
		if (attempt.State == domain.AttachedWorkerAttemptCancelRequested || attempt.State == domain.AttachedWorkerAttemptCancelledBeforeClaim) && attempt.CancelRevision == 1 {
			key := domain.AttachedWorkerAttemptMessageV1{OwnerUserID: attempt.OwnerUserID, WorkerID: attempt.WorkerID, AttemptID: attempt.AttemptID, Direction: domain.AttachedWorkerAttemptPlatformToWorker, AttemptSequence: attempt.PlatformAttemptSequence}
			message, ok, err := readAttachedWorkerAttemptMessageTx(ctx, tx, key)
			if err != nil {
				return err
			}
			if !ok || message.Kind != domain.AttachedWorkerAttemptMessageCancelRequested {
				return ErrAttachedWorkerAttemptConflict
			}
			if !canonicalAttachedWorkerTime(message.CreatedAt.Add(request.AckTimeout)).Equal(attempt.CancelDeadline) {
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
			// The durable cancel frame remains replayable, but the attempt can no
			// longer be claimed. It therefore has no remaining deadline work.
			if err := deleteAttachedWorkerAttemptDeadlineTx(ctx, tx, deadlinePlan.Lease); err != nil {
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
		if attempt.State == domain.AttachedWorkerAttemptFencedUnknown && attempt.LeaseGeneration == request.LeaseGeneration {
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

func (store *Store) ListDueAttachedWorkerAttemptDeadlines(ctx context.Context, bucket uint32, before time.Time, cursor ports.AttachedWorkerAttemptDeadlineCursor, limit uint64) ([]domain.AttachedWorkerAttemptDeadlineV1, error) {
	if bucket >= domain.AttachedWorkerAttemptDeadlineBuckets || before.IsZero() || limit == 0 || limit > maxAttachedWorkerAttemptDeadlineListLimit {
		return nil, domain.ValidationError{Field: "attached_worker_attempt_deadline.list", Reason: "requires a valid bucket, before timestamp, and bounded positive limit"}
	}
	if err := validateAttachedWorkerAttemptDeadlineCursor(cursor); err != nil {
		return nil, err
	}
	before = canonicalAttachedWorkerTime(before)
	var resultRows *sql.Rows
	var err error
	if cursor.DeadlineAt.IsZero() {
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
		return nil, err
	}
	defer resultRows.Close()
	result := make([]domain.AttachedWorkerAttemptDeadlineV1, 0, limit)
	for resultRows.Next() {
		item, err := scanAttachedWorkerAttemptDeadline(resultRows)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, resultRows.Err()
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
	run, canonicalAttempt, reservation, err := loadWorkerTerminalState(ctx, state, tx, attempt.RunID, attempt.AttemptID, attempt.ReservationID)
	if err != nil {
		return err
	}
	if materialization.Completion != nil {
		completion := *materialization.Completion
		if completion.TenantID != attempt.TenantID || completion.RunID != attempt.RunID || completion.AttemptID != attempt.AttemptID || completion.ReservationID != attempt.ReservationID || completion.LeaseID != attempt.LeaseID || completion.Fence != attempt.LeaseGeneration || len(completion.Events) == 0 {
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
	if failure.TenantID != attempt.TenantID || failure.RunID != attempt.RunID || failure.AttemptID != attempt.AttemptID || failure.ReservationID != attempt.ReservationID || failure.LeaseID != attempt.LeaseID || failure.Fence != attempt.LeaseGeneration || len(failure.Events) == 0 {
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

func attachedWorkerOfferEligible(request ports.AttachedWorkerAttemptOffer, worker domain.AttachedWorker, connection domain.AttachedWorkerConnection, loaded ports.WorkerJobState, placement domain.ExecutionPlacementV1, at, expiresAt time.Time) bool {
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

func reconcileAttachedWorkerAttemptOfferTx(ctx context.Context, tx *stateTx, request ports.AttachedWorkerAttemptOffer, existing domain.AttachedWorkerAttemptV1, result *ports.AttachedWorkerAttemptResult) error {
	if existing.State != domain.AttachedWorkerAttemptOffered || existing.RunID != request.RunID || existing.AttemptID != request.AttemptID || existing.ReservationID != request.ReservationID || existing.LeaseID != request.LeaseID {
		result.Status = ports.AttachedWorkerExecutionConflict
		return nil
	}
	loaded, found, err := loadWorkerJobStateTx(ctx, tx, request.RunID)
	if err != nil || !found {
		return err
	}
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(loaded.Job)
	wantTTL, ttlErr := domain.AttachedWorkerLeaseTTLForLimitsV1(loaded.Job.Limits)
	if err != nil || ttlErr != nil || request.LeaseTTL != wantTTL || existing.LeaseExpiresAt.Sub(existing.CreatedAt) != wantTTL ||
		loaded.Job.ExecutionPlacement.Kind != domain.ExecutionPlacementAttachedWorker ||
		loaded.Job.ExecutionPlacement.OwnerUserID != request.OwnerUserID || loaded.Job.ExecutionPlacement.WorkerID != request.WorkerID ||
		existing.ContextDigest != contextDigest || existing.CapabilityDigest != loaded.Job.ExecutionPlacement.CapabilityDigest ||
		existing.PolicyDigest != loaded.Job.ExecutionPlacement.PolicyDigest {
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
