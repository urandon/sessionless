package ydbstore

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"errors"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

const (
	maxAttachedWorkerChallengeLifetime         = 15 * time.Minute
	maxAttachedWorkerChallengeRetention        = 7 * 24 * time.Hour
	maxAttachedWorkerPresenceTTL               = 30 * time.Minute
	maxAttachedWorkerAuthTTL                   = 7 * 24 * time.Hour
	maxAttachedWorkerCheckpointInterval        = 15 * time.Minute
	maxAttachedWorkerPresenceListLimit  uint64 = 100
	maxAttachedWorkerExchangeAdvance    uint64 = 32
)

var (
	ErrAttachedWorkerChallengeConflict  = errors.New("attached worker challenge conflicts with existing state")
	ErrAttachedWorkerCapabilityConflict = errors.New("attached worker capability conflicts with existing state")
	ErrAttachedWorkerConnectionConflict = errors.New("attached worker connection conflicts with existing state")
)

func (store *Store) CreateAttachedWorkerAttachChallenge(
	ctx context.Context,
	request ports.AttachedWorkerChallengeCreate,
) (result domain.AttachedWorkerAttachChallenge, err error) {
	request.WorkerProtocolVersions = append([]uint32(nil), request.WorkerProtocolVersions...)
	request.PlatformProtocolVersions = append([]uint32(nil), request.PlatformProtocolVersions...)
	if err := validateAttachedWorkerChallengeCreate(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		existing, found, err := readAttachedWorkerChallengeTx(ctx, tx, request.OwnerUserID, request.WorkerID, request.ChallengeID)
		if err != nil {
			return err
		}
		if found {
			if !sameChallengeCreate(existing, request) {
				return ErrAttachedWorkerChallengeConflict
			}
			result = existing
			return nil
		}
		worker, found, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || worker.Revision != request.ExpectedWorkerRevision ||
			worker.EnrollmentGeneration != request.ExpectedEnrollmentGeneration ||
			worker.ConnectionGeneration != request.ExpectedConnectionGeneration ||
			worker.DesiredState == domain.AttachedWorkerDesiredRevoked {
			return ErrAttachedWorkerChallengeConflict
		}
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		target := attachedWorkerChallengeTarget(request, at)
		if err := target.Validate(); err != nil {
			return err
		}
		if err := insertAttachedWorkerChallengeTx(ctx, tx, target); err != nil {
			return err
		}
		result = target
		return nil
	})
	return result, err
}

func (store *Store) LoadAttachedWorkerAttachChallenge(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
	challengeID domain.AttachedWorkerChallengeID,
) (result domain.AttachedWorkerAttachChallenge, found bool, err error) {
	if err := validateAttachedWorkerTransportScope(tenantID, ownerUserID, workerID); err != nil {
		return result, false, err
	}
	if err := challengeID.Validate(); err != nil {
		return result, false, err
	}
	result, found, err = readJSON[domain.AttachedWorkerAttachChallenge](ctx, store.db,
		`SELECT record FROM attached_worker_attach_challenges
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3 AND challenge_id = $4`,
		tenantID, ownerUserID, workerID, challengeID,
	)
	if err != nil || !found {
		return result, found, err
	}
	result = canonicalAttachedWorkerChallenge(result)
	if err := result.Validate(); err != nil || result.TenantID != tenantID || result.OwnerUserID != ownerUserID ||
		result.WorkerID != workerID || result.ID != challengeID {
		if err != nil {
			return domain.AttachedWorkerAttachChallenge{}, false, err
		}
		return domain.AttachedWorkerAttachChallenge{}, false, ErrAttachedWorkerChallengeConflict
	}
	return result, true, nil
}

func (store *Store) ActivateAttachedWorkerConnection(
	ctx context.Context,
	request ports.AttachedWorkerConnectionActivation,
) (result ports.AttachedWorkerConnectionResult, err error) {
	if err := validateAttachedWorkerConnectionActivation(request); err != nil {
		return result, err
	}
	result.Status = ports.AttachedWorkerConnectionDenied
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		challenge, found, err := readAttachedWorkerChallengeTx(ctx, tx, request.OwnerUserID, request.WorkerID, request.ChallengeID)
		if err != nil {
			return err
		}
		if !found {
			result.Status = ports.AttachedWorkerConnectionDenied
			return nil
		}
		if !challenge.ConsumedAt.IsZero() {
			applied, connection, err := reconcileAttachedWorkerActivationTx(ctx, tx, challenge, request)
			if err != nil {
				return err
			}
			if applied {
				result = ports.AttachedWorkerConnectionResult{Status: ports.AttachedWorkerConnectionActivated, Connection: connection}
			} else {
				result.Status = ports.AttachedWorkerConnectionConsumed
			}
			return nil
		}
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		if !at.Before(challenge.ExpiresAt) {
			result.Status = ports.AttachedWorkerConnectionExpired
			return nil
		}
		worker, workerFound, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !workerFound {
			result.Status = ports.AttachedWorkerConnectionDenied
			return nil
		}
		if worker.DesiredState == domain.AttachedWorkerDesiredRevoked {
			result.Status = ports.AttachedWorkerConnectionRevoked
			return nil
		}
		if !activationMatchesChallenge(request, challenge) || !activationMatchesWorker(request, challenge, worker) {
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		connection, nextWorker, audit := attachedWorkerActivationTargets(request, challenge, worker, at)
		if err := connection.Validate(); err != nil {
			return err
		}
		if _, _, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection); err != nil {
			return ErrAttachedWorkerConnectionConflict
		}
		current, currentFound, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if currentFound && current.ConnectionGeneration >= connection.ConnectionGeneration {
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		if currentFound && !current.PresenceExpiresAt.IsZero() {
			if err := deleteAttachedWorkerPresenceExpiryTx(ctx, tx, attachedWorkerPresenceExpiry(current)); err != nil {
				return err
			}
		}
		challenge.ConsumedAt, challenge.Revision = at, challenge.Revision+1
		if err := updateAttachedWorkerChallengeTx(ctx, tx, challenge); err != nil {
			return err
		}
		if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
			return err
		}
		if err := updateAttachedWorkerTx(ctx, tx, nextWorker); err != nil {
			return err
		}
		if err := insertAttachedWorkerAuditEventTx(ctx, tx, audit); err != nil {
			return err
		}
		result = ports.AttachedWorkerConnectionResult{Status: ports.AttachedWorkerConnectionActivated, Connection: connection}
		return nil
	})
	return result, err
}

func (store *Store) AcceptAttachedWorkerManifest(
	ctx context.Context,
	request ports.AttachedWorkerManifestAcceptance,
) (result ports.AttachedWorkerAuthorizationResult, err error) {
	request.Capability.ManifestPayload = append([]byte(nil), request.Capability.ManifestPayload...)
	request.Capability.CanonicalManifest = append([]byte(nil), request.Capability.CanonicalManifest...)
	request.Capability.Signature = append([]byte(nil), request.Capability.Signature...)
	if err := validateAttachedWorkerManifestAcceptance(request); err != nil {
		return result, err
	}
	result.Status = ports.AttachedWorkerConnectionDenied
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		connection, found, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || connection.ID != request.ConnectionID || connection.ConnectionGeneration != request.ConnectionGeneration ||
			subtle.ConstantTimeCompare([]byte(connection.SecretDigest), []byte(request.PresentedSecretDigest)) != 1 {
			return nil
		}
		if connection.State == domain.AttachedWorkerConnectionRevoked {
			result.Status = ports.AttachedWorkerConnectionRevoked
			return nil
		}
		if connection.Revision != request.ExpectedConnectionRevision {
			if connection.Revision == request.ExpectedConnectionRevision+1 &&
				(connection.State == domain.AttachedWorkerConnectionOnline || connection.State == domain.AttachedWorkerConnectionDraining) &&
				connection.PlatformSequence == request.PlatformSequence && connection.WorkerSequence == request.WorkerSequence &&
				connection.PlatformAck == request.PlatformAck && connection.WorkerAck == request.WorkerAck {
				manifest, manifestFound, err := readAttachedWorkerManifestTx(ctx, tx, request.OwnerUserID, request.WorkerID, request.Capability.Digest)
				if err != nil {
					return err
				}
				if manifestFound && sameAttachedWorkerCapabilityTarget(manifest, request, connection.EnrollmentGeneration) &&
					connection.ManifestRevision == request.Capability.ManifestRevision &&
					connection.ManifestIdentityKey == request.Capability.IdentityKeyDigest &&
					bytes.Equal(connection.ManifestSignature, request.Capability.Signature) &&
					bytes.Equal(connection.ProtocolSnapshot, request.ProtocolSnapshot) &&
					connection.ManifestObservedAt.Equal(connection.LastCheckpointAt) &&
					connection.PresenceExpiresAt.Equal(canonicalAttachedWorkerTime(connection.LastCheckpointAt.Add(request.PresenceTTL))) {
					worker, workerFound, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
					if err != nil {
						return err
					}
					if workerFound && worker.Revision == request.ExpectedWorkerRevision+1 &&
						worker.ConnectionGeneration == connection.ConnectionGeneration {
						audit, auditFound, err := readAttachedWorkerAuditEventTx(ctx, tx, request.OwnerUserID, request.WorkerID, worker.Revision)
						if err != nil {
							return err
						}
						if auditFound && audit.Action == domain.AttachedWorkerAuditConnectionManifestAccepted &&
							audit.EnrollmentGeneration == worker.EnrollmentGeneration &&
							audit.ConnectionGeneration == worker.ConnectionGeneration &&
							audit.OccurredAt.Equal(connection.LastCheckpointAt) {
							result = ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionAuthorized, Connection: connection, Checkpointed: true}
							return nil
						}
					}
				}
			}
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		if !at.Before(connection.AuthExpiresAt) {
			result.Status = ports.AttachedWorkerConnectionExpired
			return nil
		}
		if connection.State != domain.AttachedWorkerConnectionAttaching || connection.CapabilityDigest != request.Capability.Digest ||
			connection.ProtocolVersion != request.Capability.ProtocolVersion {
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		worker, workerFound, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !workerFound || worker.Revision != request.ExpectedWorkerRevision || worker.Revision == math.MaxUint64 ||
			worker.ConnectionGeneration != connection.ConnectionGeneration ||
			worker.EnrollmentGeneration != connection.EnrollmentGeneration ||
			domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey) != request.Capability.IdentityKeyDigest {
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		if _, _, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection); err != nil {
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		if worker.DesiredState == domain.AttachedWorkerDesiredRevoked {
			result.Status = ports.AttachedWorkerConnectionRevoked
			return nil
		}
		manifest := attachedWorkerManifestTarget(request, connection.EnrollmentGeneration)
		if domain.DigestAttachedWorkerCapability(request.Capability.CanonicalManifest) != manifest.Digest {
			result.Status = ports.AttachedWorkerConnectionDenied
			return nil
		}
		if err := insertOrReconcileAttachedWorkerManifestTx(ctx, tx, manifest); err != nil {
			return err
		}
		connection.PlatformSequence, connection.WorkerSequence = request.PlatformSequence, request.WorkerSequence
		connection.PlatformAck, connection.WorkerAck = request.PlatformAck, request.WorkerAck
		connection.ProtocolSnapshot = append([]byte(nil), request.ProtocolSnapshot...)
		connection.ManifestRevision = request.Capability.ManifestRevision
		connection.ManifestIdentityKey = request.Capability.IdentityKeyDigest
		connection.ManifestSignature = append([]byte(nil), request.Capability.Signature...)
		connection.ManifestObservedAt = at
		connection.LastCheckpointAt = at
		connection.PresenceExpiresAt = canonicalAttachedWorkerTime(at.Add(request.PresenceTTL))
		connection.State = domain.AttachedWorkerConnectionOnline
		connection.Revision++
		if err := connection.Validate(); err != nil {
			return err
		}
		if _, _, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection); err != nil {
			return ErrAttachedWorkerConnectionConflict
		}
		if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
			return err
		}
		if err := insertAttachedWorkerPresenceExpiryTx(ctx, tx, attachedWorkerPresenceExpiry(connection)); err != nil {
			return err
		}
		observed := domain.AttachedWorkerObservedOnline
		nextWorker, audit := attachedWorkerPresenceWorkerTarget(worker, observed, domain.AttachedWorkerAuditConnectionManifestAccepted, at)
		if err := nextWorker.Validate(); err != nil {
			return err
		}
		if err := audit.Validate(); err != nil {
			return err
		}
		if err := updateAttachedWorkerTx(ctx, tx, nextWorker); err != nil {
			return err
		}
		if err := insertAttachedWorkerAuditEventTx(ctx, tx, audit); err != nil {
			return err
		}
		result = ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionAuthorized, Connection: connection, Checkpointed: true}
		return nil
	})
	return result, err
}

func (store *Store) LoadAttachedWorkerConnection(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
) (result domain.AttachedWorkerConnection, found bool, err error) {
	if err := validateAttachedWorkerTransportScope(tenantID, ownerUserID, workerID); err != nil {
		return result, false, err
	}
	result, found, err = readJSON[domain.AttachedWorkerConnection](ctx, store.db,
		`SELECT record FROM attached_worker_connections
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3`,
		tenantID, ownerUserID, workerID,
	)
	if err != nil || !found {
		return result, found, err
	}
	result = canonicalAttachedWorkerConnection(result)
	if err := result.Validate(); err != nil {
		return domain.AttachedWorkerConnection{}, false, err
	}
	if result.TenantID != tenantID || result.OwnerUserID != ownerUserID || result.WorkerID != workerID {
		return domain.AttachedWorkerConnection{}, false, ErrAttachedWorkerConnectionConflict
	}
	return result, true, nil
}

func (store *Store) LoadAttachedWorkerCapabilityManifest(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
	digest domain.AttachedWorkerCapabilityDigest,
) (result domain.AttachedWorkerCapabilityManifest, found bool, err error) {
	if err := validateAttachedWorkerTransportScope(tenantID, ownerUserID, workerID); err != nil {
		return result, false, err
	}
	if err := digest.Validate(); err != nil {
		return result, false, err
	}
	result, found, err = readJSON[domain.AttachedWorkerCapabilityManifest](ctx, store.db,
		`SELECT record FROM attached_worker_capability_manifests
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3 AND manifest_digest = $4`,
		tenantID, ownerUserID, workerID, digest,
	)
	if err != nil || !found {
		return result, found, err
	}
	result = canonicalAttachedWorkerManifest(result)
	if err := result.Validate(); err != nil {
		return domain.AttachedWorkerCapabilityManifest{}, false, err
	}
	if result.TenantID != tenantID || result.OwnerUserID != ownerUserID || result.WorkerID != workerID || result.Digest != digest {
		return domain.AttachedWorkerCapabilityManifest{}, false, ErrAttachedWorkerCapabilityConflict
	}
	return result, true, nil
}

func (store *Store) AuthorizeAttachedWorkerExchange(
	ctx context.Context,
	request ports.AttachedWorkerExchangeAuthorization,
) (result ports.AttachedWorkerAuthorizationResult, err error) {
	if err := validateAttachedWorkerExchangeAuthorization(request); err != nil {
		return result, err
	}
	result.Status = ports.AttachedWorkerConnectionDenied
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		connection, found, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !found || connection.ID != request.ConnectionID || connection.ConnectionGeneration != request.ConnectionGeneration ||
			subtle.ConstantTimeCompare([]byte(connection.SecretDigest), []byte(request.PresentedSecretDigest)) != 1 {
			result.Status = ports.AttachedWorkerConnectionDenied
			return nil
		}
		if connection.State == domain.AttachedWorkerConnectionRevoked {
			result.Status = ports.AttachedWorkerConnectionRevoked
			return nil
		}
		if connection.State != domain.AttachedWorkerConnectionOnline && connection.State != domain.AttachedWorkerConnectionDraining {
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		worker, workerFound, err := readAttachedWorkerTx(ctx, tx, request.OwnerUserID, request.WorkerID)
		if err != nil {
			return err
		}
		if !workerFound {
			result.Status = ports.AttachedWorkerConnectionDenied
			return nil
		}
		if worker.DesiredState == domain.AttachedWorkerDesiredRevoked {
			result.Status = ports.AttachedWorkerConnectionRevoked
			return nil
		}
		if worker.EnrollmentGeneration != connection.EnrollmentGeneration ||
			worker.ConnectionGeneration != connection.ConnectionGeneration {
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		if _, _, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection); err != nil {
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		manifest, manifestFound, err := readAttachedWorkerManifestTx(ctx, tx, request.OwnerUserID, request.WorkerID, connection.CapabilityDigest)
		if err != nil {
			return err
		}
		if !manifestFound || manifest.EnrollmentGeneration != connection.EnrollmentGeneration ||
			manifest.ProtocolVersion != connection.ProtocolVersion || manifest.ManifestRevision != connection.ManifestRevision ||
			manifest.IdentityKeyDigest != connection.ManifestIdentityKey ||
			manifest.IdentityKeyDigest != domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey) ||
			len(connection.ManifestSignature) != ed25519.SignatureSize {
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		if request.ExpectedConnectionRevision != connection.Revision {
			if request.ExpectedConnectionRevision != math.MaxUint64 && connection.Revision == request.ExpectedConnectionRevision+1 &&
				sameAppliedAttachedWorkerCheckpoint(connection, request) {
				result = ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionAuthorized, Connection: connection, Checkpointed: true}
				return nil
			}
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		if !at.Before(connection.AuthExpiresAt) {
			result.Status = ports.AttachedWorkerConnectionExpired
			return nil
		}
		if err := validateAttachedWorkerWatermarkAdvance(connection, request); err != nil {
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		due := at.Sub(connection.LastCheckpointAt) >= request.CheckpointInterval
		advanced := !sameAttachedWorkerWatermarks(connection, request)
		if !due && !advanced {
			if !bytes.Equal(connection.ProtocolSnapshot, request.ProtocolSnapshot) {
				result.Status = ports.AttachedWorkerConnectionConflict
				return nil
			}
			result = ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionAuthorized, Connection: connection}
			return nil
		}
		if connection.Revision == math.MaxUint64 {
			result.Status = ports.AttachedWorkerConnectionConflict
			return nil
		}
		previousExpiry := attachedWorkerPresenceExpiry(connection)
		connection = attachedWorkerCheckpointTarget(connection, request, at)
		if err := connection.Validate(); err != nil {
			return err
		}
		if _, _, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection); err != nil {
			return ErrAttachedWorkerConnectionConflict
		}
		if err := deleteAttachedWorkerPresenceExpiryTx(ctx, tx, previousExpiry); err != nil {
			return err
		}
		if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
			return err
		}
		if err := insertAttachedWorkerPresenceExpiryTx(ctx, tx, attachedWorkerPresenceExpiry(connection)); err != nil {
			return err
		}
		result = ports.AttachedWorkerAuthorizationResult{Status: ports.AttachedWorkerConnectionAuthorized, Connection: connection, Checkpointed: true}
		return nil
	})
	return result, err
}

func (store *Store) ListExpiredAttachedWorkerPresence(
	ctx context.Context,
	bucket uint32,
	before time.Time,
	after ports.AttachedWorkerPresenceCursor,
	limit uint64,
) ([]domain.AttachedWorkerPresenceExpiry, error) {
	if bucket >= ydbpartition.BucketCountV1 || before.IsZero() || limit == 0 || limit > maxAttachedWorkerPresenceListLimit {
		return nil, domain.ValidationError{Field: "attached_worker_presence_expiry.list", Reason: "has invalid bucket, cutoff, or limit"}
	}
	if err := validateAttachedWorkerPresenceCursor(after); err != nil {
		return nil, err
	}
	afterAt := canonicalAttachedWorkerTime(after.PresenceExpiresAt)
	if afterAt.IsZero() {
		afterAt = time.Unix(0, 0).UTC()
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT presence_expires_at, tenant_id, owner_user_id, worker_id,
		        connection_id, connection_generation, connection_revision
		 FROM attached_worker_presence_expiry_v1
		 WHERE shard_bucket = $1 AND presence_expires_at <= $2
		   AND (presence_expires_at > $3
		        OR (presence_expires_at = $3 AND tenant_id > $4)
		        OR (presence_expires_at = $3 AND tenant_id = $4 AND owner_user_id > $5)
		        OR (presence_expires_at = $3 AND tenant_id = $4 AND owner_user_id = $5 AND worker_id > $6))
		 ORDER BY presence_expires_at, tenant_id, owner_user_id, worker_id LIMIT $7`,
		bucket, canonicalAttachedWorkerTime(before), afterAt, after.TenantID, after.OwnerUserID, after.WorkerID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]domain.AttachedWorkerPresenceExpiry, 0)
	for rows.Next() {
		item := domain.AttachedWorkerPresenceExpiry{Bucket: bucket}
		if err := rows.Scan(&item.PresenceExpiresAt, &item.TenantID, &item.OwnerUserID, &item.WorkerID,
			&item.ConnectionID, &item.ConnectionGeneration, &item.ConnectionRevision); err != nil {
			return nil, err
		}
		item.PresenceExpiresAt = canonicalAttachedWorkerTime(item.PresenceExpiresAt)
		if err := item.Validate(); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (store *Store) ExpireAttachedWorkerPresence(
	ctx context.Context,
	candidate domain.AttachedWorkerPresenceExpiry,
) (expired bool, err error) {
	candidate.PresenceExpiresAt = canonicalAttachedWorkerTime(candidate.PresenceExpiresAt)
	if err := candidate.Validate(); err != nil {
		return false, err
	}
	err = store.Transact(ctx, candidate.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		connection, found, err := readAttachedWorkerConnectionTx(ctx, tx, candidate.OwnerUserID, candidate.WorkerID)
		if err != nil {
			return err
		}
		if found && candidate.ConnectionRevision != math.MaxUint64 && connection.ID == candidate.ConnectionID &&
			connection.ConnectionGeneration == candidate.ConnectionGeneration && connection.Revision == candidate.ConnectionRevision+1 &&
			connection.PresenceExpiresAt.Equal(candidate.PresenceExpiresAt) && connection.State == domain.AttachedWorkerConnectionOffline {
			worker, workerFound, err := readAttachedWorkerTx(ctx, tx, candidate.OwnerUserID, candidate.WorkerID)
			if err != nil {
				return err
			}
			if workerFound && worker.ObservedState == domain.AttachedWorkerObservedOffline &&
				worker.ConnectionGeneration == candidate.ConnectionGeneration {
				audit, auditFound, err := readAttachedWorkerAuditEventTx(ctx, tx, candidate.OwnerUserID, candidate.WorkerID, worker.Revision)
				if err != nil {
					return err
				}
				if auditFound && audit.Action == domain.AttachedWorkerAuditConnectionPresenceExpired &&
					audit.EnrollmentGeneration == worker.EnrollmentGeneration &&
					audit.ConnectionGeneration == worker.ConnectionGeneration && audit.OccurredAt.Equal(worker.UpdatedAt) {
					expired = true
					return deleteAttachedWorkerPresenceExpiryTx(ctx, tx, candidate)
				}
			}
		}
		if !found || connection.ID != candidate.ConnectionID || connection.ConnectionGeneration != candidate.ConnectionGeneration ||
			connection.Revision != candidate.ConnectionRevision || !connection.PresenceExpiresAt.Equal(candidate.PresenceExpiresAt) {
			return deleteAttachedWorkerPresenceExpiryTx(ctx, tx, candidate)
		}
		if connection.State != domain.AttachedWorkerConnectionOnline && connection.State != domain.AttachedWorkerConnectionDraining {
			return deleteAttachedWorkerPresenceExpiryTx(ctx, tx, candidate)
		}
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		if at.Before(connection.PresenceExpiresAt) {
			return nil
		}
		worker, workerFound, err := readAttachedWorkerTx(ctx, tx, candidate.OwnerUserID, candidate.WorkerID)
		if err != nil {
			return err
		}
		// Revocation advances the authoritative worker connection generation but
		// deliberately leaves the previous transport head fenced. Its expiry row
		// is therefore stale maintenance work, not a transaction conflict. Remove
		// it so one revoked worker cannot poison every later bounded sweep.
		if workerFound && worker.ConnectionGeneration > connection.ConnectionGeneration {
			return deleteAttachedWorkerPresenceExpiryTx(ctx, tx, candidate)
		}
		if !workerFound || worker.ConnectionGeneration != connection.ConnectionGeneration {
			return ErrAttachedWorkerConnectionConflict
		}
		if connection.Revision == math.MaxUint64 || worker.Revision == math.MaxUint64 {
			return ErrAttachedWorkerConnectionConflict
		}
		connection.State, connection.Revision = domain.AttachedWorkerConnectionOffline, connection.Revision+1
		if err := upsertAttachedWorkerConnectionTx(ctx, tx, connection); err != nil {
			return err
		}
		if err := deleteAttachedWorkerPresenceExpiryTx(ctx, tx, candidate); err != nil {
			return err
		}
		nextWorker, audit := attachedWorkerPresenceWorkerTarget(worker, domain.AttachedWorkerObservedOffline, domain.AttachedWorkerAuditConnectionPresenceExpired, at)
		if err := nextWorker.Validate(); err != nil {
			return err
		}
		if err := audit.Validate(); err != nil {
			return err
		}
		if err := updateAttachedWorkerTx(ctx, tx, nextWorker); err != nil {
			return err
		}
		if err := insertAttachedWorkerAuditEventTx(ctx, tx, audit); err != nil {
			return err
		}
		expired = true
		return nil
	})
	return expired, err
}
