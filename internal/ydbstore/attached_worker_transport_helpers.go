package ydbstore

import (
	"bytes"
	"context"
	"crypto/subtle"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

func validateAttachedWorkerTransportScope(tenantID domain.TenantID, ownerUserID domain.UserID, workerID domain.AttachedWorkerID) error {
	if err := tenantID.Validate(); err != nil {
		return err
	}
	if err := ownerUserID.Validate(); err != nil {
		return err
	}
	return workerID.Validate()
}

func validateAttachedWorkerChallengeCreate(request ports.AttachedWorkerChallengeCreate) error {
	if err := validateAttachedWorkerTransportScope(request.TenantID, request.OwnerUserID, request.WorkerID); err != nil {
		return err
	}
	if err := request.ChallengeID.Validate(); err != nil {
		return err
	}
	if err := request.ConnectionID.Validate(); err != nil {
		return err
	}
	if request.ExpectedWorkerRevision == 0 || request.ExpectedWorkerRevision == math.MaxUint64 ||
		request.ExpectedEnrollmentGeneration == 0 || request.ExpectedConnectionGeneration == math.MaxUint64 ||
		request.Lifetime <= 0 || request.Lifetime > maxAttachedWorkerChallengeLifetime ||
		request.Retention <= request.Lifetime || request.Retention > maxAttachedWorkerChallengeRetention {
		return domain.ValidationError{Field: "attached_worker_challenge.create", Reason: "has invalid revisions or bounded lifetime"}
	}
	// Domain validation owns the exact offer/digest/audience contract. Fixed
	// timestamps let it run before the transaction clock is consulted.
	probe := attachedWorkerChallengeTarget(request, time.Unix(1_700_000_000, 0).UTC())
	return probe.Validate()
}

func validateAttachedWorkerConnectionActivation(request ports.AttachedWorkerConnectionActivation) error {
	if err := validateAttachedWorkerTransportScope(request.TenantID, request.OwnerUserID, request.WorkerID); err != nil {
		return err
	}
	if err := request.ChallengeID.Validate(); err != nil {
		return err
	}
	if request.ExpectedChallengeRevision == 0 || request.ExpectedChallengeRevision == math.MaxUint64 ||
		request.ExpectedWorkerRevision == 0 || request.ExpectedWorkerRevision == math.MaxUint64 ||
		request.ExpectedEnrollmentGeneration == 0 || request.ExpectedConnectionGeneration == math.MaxUint64 ||
		request.AuthTTL <= 0 || request.AuthTTL > maxAttachedWorkerAuthTTL {
		return domain.ValidationError{Field: "attached_worker_connection.activation", Reason: "has invalid revisions, generations, or TTL"}
	}
	if err := request.ConnectionSecretDigest.Validate(); err != nil {
		return err
	}
	if err := request.ChannelBinding.Validate(); err != nil {
		return err
	}
	// A digest's concrete payload is intentionally unavailable at activation;
	// validate only its canonical shape through a minimal attaching head.
	probe := domain.AttachedWorkerConnection{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		ID: "probe-connection", ActivationChallengeID: request.ChallengeID,
		EnrollmentGeneration: request.ExpectedEnrollmentGeneration,
		ConnectionGeneration: request.ExpectedConnectionGeneration + 1,
		ProtocolVersion:      1, CapabilityDigest: request.ExpectedCapabilityDigest,
		SecretDigest: request.ConnectionSecretDigest, ChannelBinding: request.ChannelBinding,
		State: domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2,
		PlatformAck: 2, WorkerAck: 1, ProtocolSnapshot: append([]byte(nil), request.ProtocolSnapshot...), ConnectedAt: time.Unix(1_700_000_000, 0).UTC(),
		AuthExpiresAt: time.Unix(1_700_000_001, 0).UTC(), Revision: 1,
	}
	if err := probe.Validate(); err != nil {
		return err
	}
	return validateAttachedWorkerProtocolSnapshotInput(request.ProtocolSnapshot)
}

func validateAttachedWorkerManifestAcceptance(request ports.AttachedWorkerManifestAcceptance) error {
	if err := validateAttachedWorkerTransportScope(request.TenantID, request.OwnerUserID, request.WorkerID); err != nil {
		return err
	}
	if err := request.ConnectionID.Validate(); err != nil {
		return err
	}
	if request.ConnectionGeneration == 0 || request.ExpectedConnectionRevision == 0 ||
		request.ExpectedConnectionRevision == math.MaxUint64 || request.ExpectedWorkerRevision == 0 ||
		request.ExpectedWorkerRevision == math.MaxUint64 || request.PresenceTTL <= 0 ||
		request.PresenceTTL > maxAttachedWorkerPresenceTTL || request.PlatformSequence != 2 ||
		request.WorkerSequence != 3 || request.PlatformAck != 2 || request.WorkerAck != 2 {
		return domain.ValidationError{Field: "attached_worker_connection.manifest_acceptance", Reason: "has invalid generation, revision, handshake watermarks, or presence TTL"}
	}
	if err := request.PresentedSecretDigest.Validate(); err != nil {
		return err
	}
	if len(request.Capability.CanonicalManifest) == 0 || len(request.Capability.CanonicalManifest) > 32<<10 ||
		domain.DigestAttachedWorkerCapability(request.Capability.CanonicalManifest) != request.Capability.Digest {
		return domain.ValidationError{Field: "attached_worker_connection.manifest_acceptance.canonical_manifest", Reason: "must hash to the negotiated capability digest within the bounded size"}
	}
	manifest := attachedWorkerManifestTarget(request, request.ConnectionGeneration)
	if err := manifest.Validate(); err != nil {
		return err
	}
	return validateAttachedWorkerProtocolSnapshotInput(request.ProtocolSnapshot)
}

func validateAttachedWorkerExchangeAuthorization(request ports.AttachedWorkerExchangeAuthorization) error {
	if err := validateAttachedWorkerTransportScope(request.TenantID, request.OwnerUserID, request.WorkerID); err != nil {
		return err
	}
	if err := request.ConnectionID.Validate(); err != nil {
		return err
	}
	if request.ConnectionGeneration == 0 || request.ExpectedConnectionRevision == 0 ||
		request.ExpectedConnectionRevision == math.MaxUint64 || request.CheckpointInterval <= 0 ||
		request.CheckpointInterval > maxAttachedWorkerCheckpointInterval || request.PresenceTTL <= 0 ||
		request.PresenceTTL > maxAttachedWorkerPresenceTTL || request.PlatformAck > request.WorkerSequence ||
		request.WorkerAck > request.PlatformSequence {
		return domain.ValidationError{Field: "attached_worker_connection.authorization", Reason: "has invalid generation, revision, watermarks, or intervals"}
	}
	return validateAttachedWorkerProtocolSnapshotInput(request.ProtocolSnapshot)
}

func validateAttachedWorkerProtocolSnapshotInput(encoded []byte) error {
	if len(encoded) == 0 || len(encoded) > 64<<10 {
		return domain.ValidationError{Field: "attached_worker_connection.protocol_snapshot", Reason: "must contain a bounded post-transition snapshot"}
	}
	return nil
}

func validateAttachedWorkerPresenceCursor(cursor ports.AttachedWorkerPresenceCursor) error {
	if cursor.PresenceExpiresAt.IsZero() {
		if cursor.TenantID != "" || cursor.OwnerUserID != "" || cursor.WorkerID != "" {
			return domain.ValidationError{Field: "attached_worker_presence_expiry.cursor", Reason: "zero cursor must have empty identifiers"}
		}
		return nil
	}
	return validateAttachedWorkerTransportScope(cursor.TenantID, cursor.OwnerUserID, cursor.WorkerID)
}

func attachedWorkerChallengeTarget(request ports.AttachedWorkerChallengeCreate, at time.Time) domain.AttachedWorkerAttachChallenge {
	at = canonicalAttachedWorkerTime(at)
	return domain.AttachedWorkerAttachChallenge{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, ID: request.ChallengeID,
		WorkerID: request.WorkerID, ConnectionID: request.ConnectionID, Purpose: request.Purpose, Audience: request.Audience,
		ExpectedWorkerRevision: request.ExpectedWorkerRevision, ExpectedEnrollmentGeneration: request.ExpectedEnrollmentGeneration,
		ExpectedConnectionGeneration: request.ExpectedConnectionGeneration, TargetConnectionGeneration: request.ExpectedConnectionGeneration + 1,
		WorkerProtocolMinimum: request.WorkerProtocolMinimum, WorkerProtocolMaximum: request.WorkerProtocolMaximum,
		WorkerProtocolVersions:  append([]uint32(nil), request.WorkerProtocolVersions...),
		PlatformProtocolMinimum: request.PlatformProtocolMinimum, PlatformProtocolMaximum: request.PlatformProtocolMaximum,
		PlatformProtocolVersions: append([]uint32(nil), request.PlatformProtocolVersions...),
		SelectedProtocolVersion:  request.SelectedProtocolVersion,
		WorkerNonceDigest:        request.WorkerNonceDigest, PlatformNonceDigest: request.PlatformNonceDigest,
		CreatedAt: at, ExpiresAt: canonicalAttachedWorkerTime(at.Add(request.Lifetime)),
		RetainUntil: canonicalAttachedWorkerTime(at.Add(request.Retention)), Revision: 1,
	}
}

func activationMatchesChallenge(request ports.AttachedWorkerConnectionActivation, challenge domain.AttachedWorkerAttachChallenge) bool {
	return challenge.Revision == request.ExpectedChallengeRevision && challenge.ExpectedWorkerRevision == request.ExpectedWorkerRevision &&
		challenge.ExpectedEnrollmentGeneration == request.ExpectedEnrollmentGeneration &&
		challenge.ExpectedConnectionGeneration == request.ExpectedConnectionGeneration &&
		challenge.TargetConnectionGeneration == request.ExpectedConnectionGeneration+1 &&
		subtle.ConstantTimeCompare([]byte(challenge.WorkerNonceDigest), []byte(request.PresentedWorkerNonceDigest)) == 1 &&
		subtle.ConstantTimeCompare([]byte(challenge.PlatformNonceDigest), []byte(request.PresentedPlatformNonceDigest)) == 1
}

func activationMatchesWorker(request ports.AttachedWorkerConnectionActivation, challenge domain.AttachedWorkerAttachChallenge, worker domain.AttachedWorker) bool {
	return worker.Revision == request.ExpectedWorkerRevision && worker.EnrollmentGeneration == request.ExpectedEnrollmentGeneration &&
		worker.ConnectionGeneration == request.ExpectedConnectionGeneration && challenge.WorkerID == worker.ID
}

func attachedWorkerActivationTargets(
	request ports.AttachedWorkerConnectionActivation,
	challenge domain.AttachedWorkerAttachChallenge,
	worker domain.AttachedWorker,
	at time.Time,
) (domain.AttachedWorkerConnection, domain.AttachedWorker, domain.AttachedWorkerAuditEvent) {
	at = canonicalAttachedWorkerTime(at)
	connection := domain.AttachedWorkerConnection{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		ID: challenge.ConnectionID, ActivationChallengeID: challenge.ID,
		EnrollmentGeneration: request.ExpectedEnrollmentGeneration, ConnectionGeneration: challenge.TargetConnectionGeneration,
		ProtocolVersion: challenge.SelectedProtocolVersion, CapabilityDigest: request.ExpectedCapabilityDigest,
		SecretDigest: request.ConnectionSecretDigest, ChannelBinding: request.ChannelBinding,
		State: domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2,
		PlatformAck: 2, WorkerAck: 1, ProtocolSnapshot: append([]byte(nil), request.ProtocolSnapshot...), ConnectedAt: at,
		AuthExpiresAt: canonicalAttachedWorkerTime(at.Add(request.AuthTTL)), Revision: 1,
	}
	nextWorker := worker
	nextWorker.ConnectionGeneration, nextWorker.Revision = challenge.TargetConnectionGeneration, worker.Revision+1
	nextWorker.UpdatedAt = at
	audit := domain.AttachedWorkerAuditEvent{
		Version: domain.AttachedWorkerAuditEventVersionV1, TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID,
		WorkerID: worker.ID, Action: domain.AttachedWorkerAuditConnectionGenerationAdvanced,
		WorkerRevision: nextWorker.Revision, EnrollmentGeneration: nextWorker.EnrollmentGeneration,
		ConnectionGeneration: nextWorker.ConnectionGeneration, OccurredAt: at,
	}
	return connection, nextWorker, audit
}

func attachedWorkerManifestTarget(request ports.AttachedWorkerManifestAcceptance, enrollmentGeneration uint64) domain.AttachedWorkerCapabilityManifest {
	return domain.AttachedWorkerCapabilityManifest{
		Version:  domain.AttachedWorkerCapabilityManifestVersionV1,
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		EnrollmentGeneration: enrollmentGeneration,
		ManifestRevision:     request.Capability.ManifestRevision, Digest: request.Capability.Digest,
		ProtocolVersion: request.Capability.ProtocolVersion, IdentityKeyDigest: request.Capability.IdentityKeyDigest,
		ManifestPayload: append([]byte(nil), request.Capability.ManifestPayload...),
	}
}

func attachedWorkerPresenceWorkerTarget(
	worker domain.AttachedWorker,
	observed domain.AttachedWorkerObservedState,
	action domain.AttachedWorkerAuditAction,
	at time.Time,
) (domain.AttachedWorker, domain.AttachedWorkerAuditEvent) {
	at = canonicalAttachedWorkerTime(at)
	next := worker
	next.ObservedState, next.Revision, next.UpdatedAt = observed, worker.Revision+1, at
	audit := domain.AttachedWorkerAuditEvent{
		Version: domain.AttachedWorkerAuditEventVersionV1, TenantID: next.TenantID, OwnerUserID: next.OwnerUserID,
		WorkerID: next.ID, Action: action, WorkerRevision: next.Revision,
		EnrollmentGeneration: next.EnrollmentGeneration, ConnectionGeneration: next.ConnectionGeneration, OccurredAt: at,
	}
	return next, audit
}

func readAttachedWorkerChallengeTx(ctx context.Context, tx *stateTx, owner domain.UserID, worker domain.AttachedWorkerID, id domain.AttachedWorkerChallengeID) (domain.AttachedWorkerAttachChallenge, bool, error) {
	result, found, err := readJSON[domain.AttachedWorkerAttachChallenge](ctx, tx.sqlTx,
		`SELECT record FROM attached_worker_attach_challenges
		 WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3 AND challenge_id = $4`,
		tx.tenantID, owner, worker, id)
	if err != nil || !found {
		return result, found, err
	}
	result = canonicalAttachedWorkerChallenge(result)
	if err := result.Validate(); err != nil || result.TenantID != tx.tenantID || result.OwnerUserID != owner ||
		result.WorkerID != worker || result.ID != id {
		if err == nil {
			return domain.AttachedWorkerAttachChallenge{}, false, ErrAttachedWorkerChallengeConflict
		}
		return domain.AttachedWorkerAttachChallenge{}, false, err
	}
	return result, true, nil
}

func insertAttachedWorkerChallengeTx(ctx context.Context, tx *stateTx, challenge domain.AttachedWorkerAttachChallenge) error {
	record, err := marshal(challenge)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO attached_worker_attach_challenges
		 (tenant_id, owner_user_id, worker_id, challenge_id, connection_id, purpose, audience,
		  expected_worker_revision, expected_enrollment_generation, expected_connection_generation,
		  target_connection_generation, selected_protocol_version, worker_nonce_digest, platform_nonce_digest,
		  created_at, expires_at, retain_until, consumed_at, revision, record)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,CAST($20 AS JsonDocument))`,
		challenge.TenantID, challenge.OwnerUserID, challenge.WorkerID, challenge.ID, challenge.ConnectionID,
		challenge.Purpose, challenge.Audience, challenge.ExpectedWorkerRevision, challenge.ExpectedEnrollmentGeneration,
		challenge.ExpectedConnectionGeneration, challenge.TargetConnectionGeneration, challenge.SelectedProtocolVersion,
		challenge.WorkerNonceDigest, challenge.PlatformNonceDigest, challenge.CreatedAt, challenge.ExpiresAt,
		challenge.RetainUntil, optionalAttachedWorkerTime(challenge.ConsumedAt), challenge.Revision, record)
	return err
}

func updateAttachedWorkerChallengeTx(ctx context.Context, tx *stateTx, challenge domain.AttachedWorkerAttachChallenge) error {
	challenge = canonicalAttachedWorkerChallenge(challenge)
	if err := challenge.Validate(); err != nil {
		return err
	}
	record, err := marshal(challenge)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPDATE attached_worker_attach_challenges SET consumed_at=$1, revision=$2, record=CAST($3 AS JsonDocument)
		 WHERE tenant_id=$4 AND owner_user_id=$5 AND worker_id=$6 AND challenge_id=$7`,
		optionalAttachedWorkerTime(challenge.ConsumedAt), challenge.Revision, record,
		challenge.TenantID, challenge.OwnerUserID, challenge.WorkerID, challenge.ID)
	return err
}

func insertOrReconcileAttachedWorkerManifestTx(ctx context.Context, tx *stateTx, manifest domain.AttachedWorkerCapabilityManifest) error {
	existing, found, err := readAttachedWorkerManifestTx(ctx, tx, manifest.OwnerUserID, manifest.WorkerID, manifest.Digest)
	if err != nil {
		return err
	}
	if found {
		if !sameAttachedWorkerManifest(existing, manifest) {
			return ErrAttachedWorkerCapabilityConflict
		}
		return nil
	}
	record, err := marshal(manifest)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO attached_worker_capability_manifests
		 (tenant_id,owner_user_id,worker_id,capability_digest,version,enrollment_generation,
		  manifest_revision,protocol_version,identity_key_digest,manifest_payload,record)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CAST($11 AS JsonDocument))`,
		manifest.TenantID, manifest.OwnerUserID, manifest.WorkerID, manifest.Digest, manifest.Version,
		manifest.EnrollmentGeneration, manifest.ManifestRevision, manifest.ProtocolVersion,
		manifest.IdentityKeyDigest, manifest.ManifestPayload, record)
	return err
}

func readAttachedWorkerManifestTx(ctx context.Context, tx *stateTx, owner domain.UserID, worker domain.AttachedWorkerID, digest domain.AttachedWorkerCapabilityDigest) (domain.AttachedWorkerCapabilityManifest, bool, error) {
	result, found, err := readJSON[domain.AttachedWorkerCapabilityManifest](ctx, tx.sqlTx,
		`SELECT record FROM attached_worker_capability_manifests
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND capability_digest=$4`,
		tx.tenantID, owner, worker, digest)
	if err != nil || !found {
		return result, found, err
	}
	result = canonicalAttachedWorkerManifest(result)
	if err := result.Validate(); err != nil || result.TenantID != tx.tenantID || result.OwnerUserID != owner ||
		result.WorkerID != worker || result.Digest != digest {
		if err == nil {
			return domain.AttachedWorkerCapabilityManifest{}, false, ErrAttachedWorkerCapabilityConflict
		}
		return domain.AttachedWorkerCapabilityManifest{}, false, err
	}
	return result, true, nil
}

func readAttachedWorkerConnectionTx(ctx context.Context, tx *stateTx, owner domain.UserID, worker domain.AttachedWorkerID) (domain.AttachedWorkerConnection, bool, error) {
	result, found, err := readJSON[domain.AttachedWorkerConnection](ctx, tx.sqlTx,
		`SELECT record FROM attached_worker_connections WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`,
		tx.tenantID, owner, worker)
	if err != nil || !found {
		return result, found, err
	}
	result = canonicalAttachedWorkerConnection(result)
	if err := result.Validate(); err != nil || result.TenantID != tx.tenantID || result.OwnerUserID != owner || result.WorkerID != worker {
		if err == nil {
			return domain.AttachedWorkerConnection{}, false, ErrAttachedWorkerConnectionConflict
		}
		return domain.AttachedWorkerConnection{}, false, err
	}
	return result, true, nil
}

func upsertAttachedWorkerConnectionTx(ctx context.Context, tx *stateTx, connection domain.AttachedWorkerConnection) error {
	connection = canonicalAttachedWorkerConnection(connection)
	if err := connection.Validate(); err != nil {
		return err
	}
	record, err := marshal(connection)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO attached_worker_connections
		 (tenant_id,owner_user_id,worker_id,connection_id,activation_challenge_id,enrollment_generation,
		  connection_generation,protocol_version,capability_digest,channel_binding_digest,secret_digest,
		  manifest_revision,manifest_identity_key_digest,manifest_signature,manifest_observed_at,state,
		  platform_sequence,worker_sequence,platform_ack,worker_ack,connected_at,last_checkpoint_at,presence_expires_at,
		  auth_expires_at,revision,record)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,CAST($26 AS JsonDocument))`,
		connection.TenantID, connection.OwnerUserID, connection.WorkerID, connection.ID, connection.ActivationChallengeID,
		connection.EnrollmentGeneration, connection.ConnectionGeneration, connection.ProtocolVersion, connection.CapabilityDigest,
		connection.ChannelBinding, connection.SecretDigest, connection.ManifestRevision, connection.ManifestIdentityKey,
		connection.ManifestSignature, optionalAttachedWorkerTime(connection.ManifestObservedAt), connection.State,
		connection.PlatformSequence, connection.WorkerSequence,
		connection.PlatformAck, connection.WorkerAck, connection.ConnectedAt, optionalAttachedWorkerTime(connection.LastCheckpointAt),
		optionalAttachedWorkerTime(connection.PresenceExpiresAt), connection.AuthExpiresAt, connection.Revision, record)
	return err
}

func attachedWorkerPresenceExpiry(connection domain.AttachedWorkerConnection) domain.AttachedWorkerPresenceExpiry {
	bucket, _ := ydbpartition.BucketV1(string(connection.WorkerID))
	return domain.AttachedWorkerPresenceExpiry{Bucket: bucket, TenantID: connection.TenantID, OwnerUserID: connection.OwnerUserID,
		WorkerID: connection.WorkerID, ConnectionID: connection.ID, ConnectionGeneration: connection.ConnectionGeneration,
		ConnectionRevision: connection.Revision, PresenceExpiresAt: connection.PresenceExpiresAt}
}

func insertAttachedWorkerPresenceExpiryTx(ctx context.Context, tx *stateTx, expiry domain.AttachedWorkerPresenceExpiry) error {
	if err := expiry.Validate(); err != nil {
		return err
	}
	_, err := tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO attached_worker_presence_expiry_v1
		 (shard_bucket,presence_expires_at,tenant_id,owner_user_id,worker_id,connection_id,connection_generation,connection_revision)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		expiry.Bucket, expiry.PresenceExpiresAt, expiry.TenantID, expiry.OwnerUserID, expiry.WorkerID,
		expiry.ConnectionID, expiry.ConnectionGeneration, expiry.ConnectionRevision)
	return err
}

func deleteAttachedWorkerPresenceExpiryTx(ctx context.Context, tx *stateTx, expiry domain.AttachedWorkerPresenceExpiry) error {
	_, err := tx.sqlTx.ExecContext(ctx,
		`DELETE FROM attached_worker_presence_expiry_v1
		 WHERE shard_bucket=$1 AND presence_expires_at=$2 AND tenant_id=$3 AND owner_user_id=$4 AND worker_id=$5`,
		expiry.Bucket, expiry.PresenceExpiresAt, expiry.TenantID, expiry.OwnerUserID, expiry.WorkerID)
	return err
}

func reconcileAttachedWorkerActivationTx(ctx context.Context, tx *stateTx, challenge domain.AttachedWorkerAttachChallenge, request ports.AttachedWorkerConnectionActivation) (bool, domain.AttachedWorkerConnection, error) {
	if challenge.Revision != request.ExpectedChallengeRevision+1 || !activationMatchesConsumedChallenge(request, challenge) {
		return false, domain.AttachedWorkerConnection{}, nil
	}
	connection, found, err := readAttachedWorkerConnectionTx(ctx, tx, request.OwnerUserID, request.WorkerID)
	if err != nil || !found {
		return false, domain.AttachedWorkerConnection{}, err
	}
	if connection.ID != challenge.ConnectionID || connection.ActivationChallengeID != challenge.ID ||
		connection.EnrollmentGeneration != request.ExpectedEnrollmentGeneration ||
		connection.ConnectionGeneration != request.ExpectedConnectionGeneration+1 ||
		connection.ProtocolVersion != challenge.SelectedProtocolVersion ||
		connection.CapabilityDigest != request.ExpectedCapabilityDigest || connection.ChannelBinding != request.ChannelBinding ||
		!bytes.Equal(connection.ProtocolSnapshot, request.ProtocolSnapshot) ||
		subtle.ConstantTimeCompare([]byte(connection.SecretDigest), []byte(request.ConnectionSecretDigest)) != 1 ||
		!connection.ConnectedAt.Equal(challenge.ConsumedAt) ||
		!connection.AuthExpiresAt.Equal(canonicalAttachedWorkerTime(challenge.ConsumedAt.Add(request.AuthTTL))) {
		return false, domain.AttachedWorkerConnection{}, nil
	}
	return true, connection, nil
}

func activationMatchesConsumedChallenge(request ports.AttachedWorkerConnectionActivation, challenge domain.AttachedWorkerAttachChallenge) bool {
	return challenge.ExpectedWorkerRevision == request.ExpectedWorkerRevision &&
		challenge.ExpectedEnrollmentGeneration == request.ExpectedEnrollmentGeneration &&
		challenge.ExpectedConnectionGeneration == request.ExpectedConnectionGeneration &&
		subtle.ConstantTimeCompare([]byte(challenge.WorkerNonceDigest), []byte(request.PresentedWorkerNonceDigest)) == 1 &&
		subtle.ConstantTimeCompare([]byte(challenge.PlatformNonceDigest), []byte(request.PresentedPlatformNonceDigest)) == 1
}

func validateAttachedWorkerWatermarkAdvance(connection domain.AttachedWorkerConnection, request ports.AttachedWorkerExchangeAuthorization) error {
	if request.PlatformSequence < connection.PlatformSequence || request.WorkerSequence < connection.WorkerSequence ||
		request.PlatformAck < connection.PlatformAck || request.WorkerAck < connection.WorkerAck ||
		request.PlatformSequence-connection.PlatformSequence > maxAttachedWorkerExchangeAdvance ||
		request.WorkerSequence-connection.WorkerSequence > maxAttachedWorkerExchangeAdvance {
		return ErrAttachedWorkerConnectionConflict
	}
	return nil
}

func sameAttachedWorkerWatermarks(connection domain.AttachedWorkerConnection, request ports.AttachedWorkerExchangeAuthorization) bool {
	return connection.PlatformSequence == request.PlatformSequence && connection.WorkerSequence == request.WorkerSequence &&
		connection.PlatformAck == request.PlatformAck && connection.WorkerAck == request.WorkerAck
}

func sameAppliedAttachedWorkerCheckpoint(connection domain.AttachedWorkerConnection, request ports.AttachedWorkerExchangeAuthorization) bool {
	return sameAttachedWorkerWatermarks(connection, request) &&
		bytes.Equal(connection.ProtocolSnapshot, request.ProtocolSnapshot) &&
		connection.PresenceExpiresAt.Equal(canonicalAttachedWorkerTime(connection.LastCheckpointAt.Add(request.PresenceTTL)))
}

func attachedWorkerCheckpointTarget(connection domain.AttachedWorkerConnection, request ports.AttachedWorkerExchangeAuthorization, at time.Time) domain.AttachedWorkerConnection {
	connection.PlatformSequence, connection.WorkerSequence = request.PlatformSequence, request.WorkerSequence
	connection.PlatformAck, connection.WorkerAck = request.PlatformAck, request.WorkerAck
	connection.ProtocolSnapshot = append([]byte(nil), request.ProtocolSnapshot...)
	if snapshot, err := attachedworkerprotocol.DecodeMachineSnapshotV1(request.ProtocolSnapshot); err == nil {
		switch snapshot.Connection {
		case attachedworkerprotocol.ConnectionReady:
			connection.State = domain.AttachedWorkerConnectionOnline
		case attachedworkerprotocol.ConnectionDraining, attachedworkerprotocol.ConnectionDrained:
			connection.State = domain.AttachedWorkerConnectionDraining
		case attachedworkerprotocol.ConnectionRevoking, attachedworkerprotocol.ConnectionRevoked:
			connection.State = domain.AttachedWorkerConnectionRevoked
		}
	}
	connection.LastCheckpointAt = canonicalAttachedWorkerTime(at)
	connection.PresenceExpiresAt = canonicalAttachedWorkerTime(at.Add(request.PresenceTTL))
	connection.Revision++
	return connection
}

func canonicalAttachedWorkerChallenge(value domain.AttachedWorkerAttachChallenge) domain.AttachedWorkerAttachChallenge {
	value.WorkerProtocolVersions = append([]uint32(nil), value.WorkerProtocolVersions...)
	value.PlatformProtocolVersions = append([]uint32(nil), value.PlatformProtocolVersions...)
	value.CreatedAt, value.ExpiresAt = canonicalAttachedWorkerTime(value.CreatedAt), canonicalAttachedWorkerTime(value.ExpiresAt)
	value.RetainUntil, value.ConsumedAt = canonicalAttachedWorkerTime(value.RetainUntil), canonicalAttachedWorkerTime(value.ConsumedAt)
	return value
}

func canonicalAttachedWorkerManifest(value domain.AttachedWorkerCapabilityManifest) domain.AttachedWorkerCapabilityManifest {
	value.ManifestPayload = append([]byte(nil), value.ManifestPayload...)
	return value
}

func canonicalAttachedWorkerConnection(value domain.AttachedWorkerConnection) domain.AttachedWorkerConnection {
	value.ManifestSignature = append([]byte(nil), value.ManifestSignature...)
	value.ProtocolSnapshot = append([]byte(nil), value.ProtocolSnapshot...)
	value.ManifestObservedAt = canonicalAttachedWorkerTime(value.ManifestObservedAt)
	value.ConnectedAt, value.LastCheckpointAt = canonicalAttachedWorkerTime(value.ConnectedAt), canonicalAttachedWorkerTime(value.LastCheckpointAt)
	value.PresenceExpiresAt, value.AuthExpiresAt = canonicalAttachedWorkerTime(value.PresenceExpiresAt), canonicalAttachedWorkerTime(value.AuthExpiresAt)
	return value
}

func sameAttachedWorkerChallenge(left, right domain.AttachedWorkerAttachChallenge) bool {
	left, right = canonicalAttachedWorkerChallenge(left), canonicalAttachedWorkerChallenge(right)
	return left.TenantID == right.TenantID && left.OwnerUserID == right.OwnerUserID && left.ID == right.ID &&
		left.WorkerID == right.WorkerID && left.ConnectionID == right.ConnectionID && left.Purpose == right.Purpose &&
		left.Audience == right.Audience && left.ExpectedWorkerRevision == right.ExpectedWorkerRevision &&
		left.ExpectedEnrollmentGeneration == right.ExpectedEnrollmentGeneration &&
		left.ExpectedConnectionGeneration == right.ExpectedConnectionGeneration &&
		left.TargetConnectionGeneration == right.TargetConnectionGeneration &&
		left.WorkerProtocolMinimum == right.WorkerProtocolMinimum && left.WorkerProtocolMaximum == right.WorkerProtocolMaximum &&
		bytes.Equal(uint32SliceBytes(left.WorkerProtocolVersions), uint32SliceBytes(right.WorkerProtocolVersions)) &&
		left.PlatformProtocolMinimum == right.PlatformProtocolMinimum && left.PlatformProtocolMaximum == right.PlatformProtocolMaximum &&
		bytes.Equal(uint32SliceBytes(left.PlatformProtocolVersions), uint32SliceBytes(right.PlatformProtocolVersions)) &&
		left.SelectedProtocolVersion == right.SelectedProtocolVersion && left.WorkerNonceDigest == right.WorkerNonceDigest &&
		left.PlatformNonceDigest == right.PlatformNonceDigest && left.CreatedAt.Equal(right.CreatedAt) &&
		left.ExpiresAt.Equal(right.ExpiresAt) && left.RetainUntil.Equal(right.RetainUntil) &&
		left.ConsumedAt.Equal(right.ConsumedAt) && left.Revision == right.Revision
}

func sameAttachedWorkerManifest(left, right domain.AttachedWorkerCapabilityManifest) bool {
	left, right = canonicalAttachedWorkerManifest(left), canonicalAttachedWorkerManifest(right)
	return left.Version == right.Version && left.TenantID == right.TenantID && left.OwnerUserID == right.OwnerUserID &&
		left.WorkerID == right.WorkerID && left.EnrollmentGeneration == right.EnrollmentGeneration &&
		left.ManifestRevision == right.ManifestRevision &&
		left.Digest == right.Digest && left.ProtocolVersion == right.ProtocolVersion && left.IdentityKeyDigest == right.IdentityKeyDigest &&
		bytes.Equal(left.ManifestPayload, right.ManifestPayload)
}

func sameAttachedWorkerCapabilityTarget(manifest domain.AttachedWorkerCapabilityManifest, request ports.AttachedWorkerManifestAcceptance, enrollmentGeneration uint64) bool {
	return sameAttachedWorkerManifest(manifest, attachedWorkerManifestTarget(request, enrollmentGeneration))
}

func sameChallengeCreate(existing domain.AttachedWorkerAttachChallenge, request ports.AttachedWorkerChallengeCreate) bool {
	target := attachedWorkerChallengeTarget(request, existing.CreatedAt)
	return sameAttachedWorkerChallenge(existing, target)
}

func uint32SliceBytes(values []uint32) []byte {
	result := make([]byte, 4*len(values))
	for index, value := range values {
		result[index*4], result[index*4+1], result[index*4+2], result[index*4+3] = byte(value>>24), byte(value>>16), byte(value>>8), byte(value)
	}
	return result
}
