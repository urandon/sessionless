package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Trusted immutable installation metadata, NEVER HTTP authority. This narrow
// source pin is not a complete backend/model/route/privacy/price/context pin.
type computeChoiceAttachedPin struct {
	tenantID             domain.TenantID
	ownerUserID          domain.UserID
	subscriptionID       domain.SubscriptionConnectionID
	actorID              domain.ActorID
	provider             string
	workerID             domain.AttachedWorkerID
	connectionID         domain.AttachedWorkerConnectionID
	enrollmentGeneration uint64
	connectionGeneration uint64
	workerRevision       uint64
	connectionRevision   uint64
	capabilityDigest     domain.AttachedWorkerCapabilityDigest
}

type computeChoiceAttachedState uint8

const (
	computeChoiceAttachedMissing computeChoiceAttachedState = iota
	computeChoiceAttachedObserved
	computeChoiceAttachedUnavailable
)

type computeChoiceAttachedSubscription struct {
	id          domain.SubscriptionConnectionID
	actorID     domain.ActorID
	provider    string
	entitlement domain.EntitlementState
	quota       domain.ProviderQuotaState
	observedAt  time.Time
}

type computeChoiceAttachedCounters struct {
	queueDepth uint32
	activeRuns uint32
	updatedAt  time.Time
}

// Private source facts, not eligibility, public DTOs or reservation authority.
// Missing optional pointers mean unknown; neither ready nor zero is invented.
type computeChoiceAttachedSnapshot struct {
	state        computeChoiceAttachedState
	subscription *computeChoiceAttachedSubscription
	worker       *domain.AttachedWorker
	connection   *domain.AttachedWorkerConnection
	slot         *domain.SubscriptionSchedulerSlot
	counters     *computeChoiceAttachedCounters
}

const computeChoiceAttachedSubscriptionSQL = `SELECT SUBSTRING(CAST(subscription_connection_id AS String),0,161),
 SUBSTRING(CAST(actor_id AS String),0,161), SUBSTRING(CAST(provider AS String),0,161),
 SUBSTRING(CAST(entitlement_state AS String),0,161), SUBSTRING(CAST(quota_state AS String),0,161), observed_at
 FROM subscription_connections WHERE tenant_id=$1 AND subscription_connection_id=$2`
const computeChoiceAttachedActorSQL = `SELECT SUBSTRING(CAST(user_id AS String),0,161)
 FROM actors WHERE tenant_id=$1 AND actor_id=$2`
const computeChoiceAttachedWorkerSQL = `SELECT SUBSTRING(CAST(record AS String),0,8193),
 SUBSTRING(CAST(tenant_id AS String),0,161), SUBSTRING(CAST(owner_user_id AS String),0,161), SUBSTRING(CAST(worker_id AS String),0,161),
 SUBSTRING(identity_public_key,0,33), enrollment_generation, connection_generation,
 SUBSTRING(CAST(desired_state AS String),0,161), SUBSTRING(CAST(observed_state AS String),0,161),
 revision, created_at, updated_at, revoked_at
 FROM attached_workers WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`
const computeChoiceAttachedConnectionSQL = `SELECT SUBSTRING(CAST(record AS String),0,131073),
 SUBSTRING(CAST(tenant_id AS String),0,161), SUBSTRING(CAST(owner_user_id AS String),0,161), SUBSTRING(CAST(worker_id AS String),0,161),
 SUBSTRING(CAST(connection_id AS String),0,161), SUBSTRING(CAST(activation_challenge_id AS String),0,161),
 enrollment_generation, connection_generation, protocol_version, SUBSTRING(CAST(capability_digest AS String),0,65),
 SUBSTRING(CAST(channel_binding_digest AS String),0,65), SUBSTRING(CAST(secret_digest AS String),0,65),
 manifest_revision, SUBSTRING(CAST(manifest_identity_key_digest AS String),0,65), SUBSTRING(manifest_signature,0,65),
 manifest_observed_at, SUBSTRING(CAST(state AS String),0,161), platform_sequence, worker_sequence, platform_ack, worker_ack,
 connected_at, last_checkpoint_at, presence_expires_at, auth_expires_at, revision
 FROM attached_worker_connections WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`
const computeChoiceAttachedSlotSQL = `SELECT SUBSTRING(CAST(state AS String),0,161),
 SUBSTRING(CAST(active_run_id AS String),0,161), SUBSTRING(CAST(active_reservation_id AS String),0,161), blocked_until, updated_at,
 SUBSTRING(CAST(subscription_connection_id AS String),0,161), SUBSTRING(local_capacity_association,0,32769), SUBSTRING(CAST(local_capacity_digest AS String),0,65)
 FROM subscription_scheduler_slots WHERE tenant_id=$1 AND subscription_connection_id=$2`
const computeChoiceAttachedCountersSQL = `SELECT queue_depth, active_runs, updated_at
 FROM tenant_scheduler_counters WHERE tenant_id=$1`

// All six point SELECTs share the caller's authorized transaction and budget.
// No credential, worker RPC, nested transaction, initialization or mutation.
func readComputeChoiceAttached(ctx context.Context, query *computeChoiceQuery, auth computeChoiceAuth, pin computeChoiceAttachedPin) (computeChoiceAttachedSnapshot, error) {
	var zero computeChoiceAttachedSnapshot
	if ctx == nil || query == nil || query.tx == nil || query.budget == nil || auth.scope.Validate() != nil || auth.at.IsZero() || !auth.authorityExpiresAt.After(auth.at) ||
		pin.tenantID != auth.scope.TenantID || pin.ownerUserID != auth.scope.UserID || pin.subscriptionID.Validate() != nil || pin.actorID.Validate() != nil ||
		pin.workerID.Validate() != nil || pin.connectionID.Validate() != nil || !computeChoiceAttachedProviderValid(pin.provider) || pin.capabilityDigest.Validate() != nil ||
		pin.enrollmentGeneration == 0 || pin.connectionGeneration == 0 || pin.workerRevision == 0 || pin.connectionRevision == 0 {
		return zero, errComputeChoiceSource
	}
	result := computeChoiceAttachedSnapshot{state: computeChoiceAttachedObserved}
	var err error
	result.subscription, err = readComputeChoiceAttachedSubscription(ctx, query, auth, pin)
	if err != nil {
		return zero, err
	}
	var owner string
	err = query.QueryRowContext(ctx, computeChoiceAttachedActorSQL, pin.tenantID, pin.actorID).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		result.state = computeChoiceAttachedMissing
	} else if err != nil {
		return zero, computeChoiceAttachedReadError(query, err)
	} else {
		if err := chargeComputeChoiceAttachedScalars(query, 0, owner); err != nil {
			return zero, err
		}
		if domain.UserID(owner).Validate() != nil || domain.UserID(owner) != auth.scope.UserID {
			return zero, errComputeChoiceSource
		}
	}
	result.worker, err = readComputeChoiceAttachedWorker(ctx, query, auth, pin)
	if err != nil {
		return zero, err
	}
	result.connection, err = readComputeChoiceAttachedConnection(ctx, query, auth, pin)
	if err != nil {
		return zero, err
	}
	result.slot, err = readComputeChoiceAttachedSlot(ctx, query, auth, pin)
	if err != nil {
		return zero, err
	}
	result.counters, err = readComputeChoiceAttachedCounters(ctx, query, auth)
	if err != nil {
		return zero, err
	}
	if result.subscription == nil || result.worker == nil || result.connection == nil || result.slot == nil || result.counters == nil {
		result.state = computeChoiceAttachedMissing
	}
	unavailable := false
	if subscription := result.subscription; subscription != nil {
		unavailable = subscription.actorID != pin.actorID || subscription.provider != pin.provider || subscription.entitlement != domain.EntitlementActive || subscription.quota == domain.ProviderQuotaUnknown || subscription.quota == domain.ProviderQuotaExhausted
	}
	if worker := result.worker; worker != nil {
		unavailable = unavailable || worker.EnrollmentGeneration != pin.enrollmentGeneration || worker.ConnectionGeneration != pin.connectionGeneration || worker.Revision != pin.workerRevision || worker.DesiredState != domain.AttachedWorkerDesiredActive || worker.ObservedState != domain.AttachedWorkerObservedOnline
	}
	if connection := result.connection; connection != nil {
		unavailable = unavailable || connection.ID != pin.connectionID || connection.EnrollmentGeneration != pin.enrollmentGeneration || connection.ConnectionGeneration != pin.connectionGeneration || connection.Revision != pin.connectionRevision || connection.CapabilityDigest != pin.capabilityDigest || connection.State != domain.AttachedWorkerConnectionOnline || !auth.at.Before(connection.PresenceExpiresAt) || !auth.at.Before(connection.AuthExpiresAt)
	}
	if result.worker != nil && result.connection != nil {
		worker, connection := *result.worker, *result.connection
		// Online current records with contrary generations are an impossible
		// coherent source tuple, not a usable reconstructed connection.
		if worker.DesiredState == domain.AttachedWorkerDesiredActive && worker.ObservedState == domain.AttachedWorkerObservedOnline && connection.State == domain.AttachedWorkerConnectionOnline &&
			(worker.EnrollmentGeneration != connection.EnrollmentGeneration || worker.ConnectionGeneration != connection.ConnectionGeneration) {
			return zero, errComputeChoiceSource
		}
		if worker.EnrollmentGeneration == connection.EnrollmentGeneration && worker.ConnectionGeneration == connection.ConnectionGeneration && connection.State != domain.AttachedWorkerConnectionSuperseded {
			if _, _, err := loadAttachedWorkerProtocolAuthorityTx(ctx, &stateTx{sqlTx: query.tx, tenantID: auth.scope.TenantID}, worker, connection); err != nil {
				return zero, errComputeChoiceSource
			}
		}
	}
	if unavailable && result.state != computeChoiceAttachedMissing {
		result.state = computeChoiceAttachedUnavailable
	}
	return result, nil
}

func computeChoiceAttachedProviderValid(provider string) bool {
	return len(provider) > 0 && len(provider) <= 160 && utf8.ValidString(provider) && strings.TrimSpace(provider) != ""
}

func computeChoiceAttachedReadError(query *computeChoiceQuery, err error) error {
	if query.statementError != nil {
		return query.readError(err)
	}
	return errComputeChoiceSource
}

func chargeComputeChoiceAttachedScalars(query *computeChoiceQuery, fixedBytes int64, values ...string) error {
	// Include selected fixed-width YDB values as well as variable prefixes:
	// Uint64/Timestamp are eight bytes and Uint32 is four. Nullable timestamps
	// conservatively count their full width even when absent.
	size := fixedBytes
	for _, value := range values {
		size += int64(len(value))
	}
	return query.budget.RecordMaterialized(computechoice.SourceOrdinary, size)
}

func readComputeChoiceAttachedSubscription(ctx context.Context, query *computeChoiceQuery, auth computeChoiceAuth, pin computeChoiceAttachedPin) (*computeChoiceAttachedSubscription, error) {
	var id, actor, provider, entitlement, quota string
	var observed time.Time
	err := query.QueryRowContext(ctx, computeChoiceAttachedSubscriptionSQL, pin.tenantID, pin.subscriptionID).Scan(&id, &actor, &provider, &entitlement, &quota, &observed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, computeChoiceAttachedReadError(query, err)
	}
	if err := chargeComputeChoiceAttachedScalars(query, 8, id, actor, provider, entitlement, quota); err != nil {
		return nil, err
	}
	value := &computeChoiceAttachedSubscription{id: domain.SubscriptionConnectionID(id), actorID: domain.ActorID(actor), provider: provider, entitlement: domain.EntitlementState(entitlement), quota: domain.ProviderQuotaState(quota), observedAt: observed}
	if value.id != pin.subscriptionID || value.actorID.Validate() != nil || !computeChoiceAttachedProviderValid(value.provider) || !value.entitlement.Valid() || !value.quota.Valid() || observed.IsZero() || observed.After(auth.at) {
		return nil, errComputeChoiceSource
	}
	return value, nil
}

var computeChoiceAttachedWorkerFields = map[string]bool{"tenant_id": true, "owner_user_id": true, "worker_id": true, "display_name": true, "identity_public_key": true, "enrollment_generation": true, "connection_generation": true, "desired_state": true, "observed_state": true, "revision": true, "created_at": true, "updated_at": true, "revoked_at": true}

// The canonical attaching record has a present-but-null manifest_signature.
// Its presence is checked separately below; all other fields remain non-null
// and required by the shared strict object validator.
var computeChoiceAttachedConnectionFields = map[string]bool{"tenant_id": true, "owner_user_id": true, "worker_id": true, "connection_id": true, "activation_challenge_id": true, "enrollment_generation": true, "connection_generation": true, "protocol_version": true, "capability_digest": true, "secret_digest": true, "channel_binding": true, "manifest_revision": true, "manifest_identity_key_digest": true, "manifest_signature": false, "manifest_observed_at": true, "state": true, "platform_sequence": true, "worker_sequence": true, "platform_ack": true, "worker_ack": true, "protocol_snapshot": true, "connected_at": true, "last_checkpoint_at": true, "presence_expires_at": true, "auth_expires_at": true, "revision": true}

func readComputeChoiceAttachedWorker(ctx context.Context, query *computeChoiceQuery, auth computeChoiceAuth, pin computeChoiceAttachedPin) (*domain.AttachedWorker, error) {
	var encoded string
	var scalar domain.AttachedWorker
	var revoked sql.NullTime
	err := query.QueryRowContext(ctx, computeChoiceAttachedWorkerSQL, pin.tenantID, pin.ownerUserID, pin.workerID).Scan(&encoded, &scalar.TenantID, &scalar.OwnerUserID, &scalar.ID, &scalar.IdentityPublicKey, &scalar.EnrollmentGeneration, &scalar.ConnectionGeneration, &scalar.DesiredState, &scalar.ObservedState, &scalar.Revision, &scalar.CreatedAt, &scalar.UpdatedAt, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, computeChoiceAttachedReadError(query, err)
	}
	var worker domain.AttachedWorker
	if err := chargeComputeChoiceAttachedScalars(query, 6*8); err != nil {
		return nil, err
	}
	if err := decodeComputeChoiceRecord(query.budget, encoded, &worker, computeChoiceAttachedWorkerFields, string(scalar.TenantID), string(scalar.OwnerUserID), string(scalar.ID), string(scalar.IdentityPublicKey), string(scalar.DesiredState), string(scalar.ObservedState)); err != nil {
		return nil, err
	}
	if worker.Validate() != nil || worker.TenantID != pin.tenantID || worker.OwnerUserID != pin.ownerUserID || worker.ID != pin.workerID || worker.TenantID != scalar.TenantID || worker.OwnerUserID != scalar.OwnerUserID || worker.ID != scalar.ID || !bytes.Equal(worker.IdentityPublicKey, scalar.IdentityPublicKey) || worker.EnrollmentGeneration != scalar.EnrollmentGeneration || worker.ConnectionGeneration != scalar.ConnectionGeneration || worker.DesiredState != scalar.DesiredState || worker.ObservedState != scalar.ObservedState || worker.Revision != scalar.Revision || !computeChoiceTimestampEqual(worker.CreatedAt, scalar.CreatedAt) || !computeChoiceTimestampEqual(worker.UpdatedAt, scalar.UpdatedAt) || !computeChoiceAttachedOptionalTimeEqual(worker.RevokedAt, revoked) || worker.UpdatedAt.After(auth.at) {
		return nil, errComputeChoiceSource
	}
	return &worker, nil
}

func readComputeChoiceAttachedConnection(ctx context.Context, query *computeChoiceQuery, auth computeChoiceAuth, pin computeChoiceAttachedPin) (*domain.AttachedWorkerConnection, error) {
	var encoded string
	var scalar domain.AttachedWorkerConnection
	var manifest, checkpoint, presence sql.NullTime
	err := query.QueryRowContext(ctx, computeChoiceAttachedConnectionSQL, pin.tenantID, pin.ownerUserID, pin.workerID).Scan(&encoded, &scalar.TenantID, &scalar.OwnerUserID, &scalar.WorkerID, &scalar.ID, &scalar.ActivationChallengeID, &scalar.EnrollmentGeneration, &scalar.ConnectionGeneration, &scalar.ProtocolVersion, &scalar.CapabilityDigest, &scalar.ChannelBinding, &scalar.SecretDigest, &scalar.ManifestRevision, &scalar.ManifestIdentityKey, &scalar.ManifestSignature, &manifest, &scalar.State, &scalar.PlatformSequence, &scalar.WorkerSequence, &scalar.PlatformAck, &scalar.WorkerAck, &scalar.ConnectedAt, &checkpoint, &presence, &scalar.AuthExpiresAt, &scalar.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, computeChoiceAttachedReadError(query, err)
	}
	if err := query.budget.RecordMaterialized(computechoice.SourceConnection, int64(len(encoded))); err != nil {
		return nil, err
	}
	if err := chargeComputeChoiceAttachedScalars(query, 13*8+4, string(scalar.TenantID), string(scalar.OwnerUserID), string(scalar.WorkerID), string(scalar.ID), string(scalar.ActivationChallengeID), string(scalar.CapabilityDigest), string(scalar.ChannelBinding), string(scalar.SecretDigest), string(scalar.ManifestIdentityKey), string(scalar.ManifestSignature), string(scalar.State)); err != nil {
		return nil, err
	}
	var connection domain.AttachedWorkerConnection
	decoder := json.NewDecoder(strings.NewReader(encoded))
	if validateComputeChoiceObject(decoder, computeChoiceAttachedConnectionFields) != nil {
		return nil, errComputeChoiceSource
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errComputeChoiceSource
	}
	// RawMessage distinguishes omission from an explicitly present JSON null
	// without rewriting the input or duplicating the shared strict parser.
	var requiredNullable struct {
		ManifestSignature json.RawMessage `json:"manifest_signature"`
	}
	if json.Unmarshal([]byte(encoded), &requiredNullable) != nil || len(requiredNullable.ManifestSignature) == 0 {
		return nil, errComputeChoiceSource
	}
	if json.Unmarshal([]byte(encoded), &connection) != nil || connection.Validate() != nil {
		return nil, errComputeChoiceSource
	}
	if connection.TenantID != pin.tenantID || connection.OwnerUserID != pin.ownerUserID || connection.WorkerID != pin.workerID || connection.TenantID != scalar.TenantID || connection.OwnerUserID != scalar.OwnerUserID || connection.WorkerID != scalar.WorkerID || connection.ID != scalar.ID || connection.ActivationChallengeID != scalar.ActivationChallengeID || connection.EnrollmentGeneration != scalar.EnrollmentGeneration || connection.ConnectionGeneration != scalar.ConnectionGeneration || connection.ProtocolVersion != scalar.ProtocolVersion || connection.CapabilityDigest != scalar.CapabilityDigest || connection.ChannelBinding != scalar.ChannelBinding || connection.SecretDigest != scalar.SecretDigest || connection.ManifestRevision != scalar.ManifestRevision || connection.ManifestIdentityKey != scalar.ManifestIdentityKey || !bytes.Equal(connection.ManifestSignature, scalar.ManifestSignature) || connection.State != scalar.State || connection.PlatformSequence != scalar.PlatformSequence || connection.WorkerSequence != scalar.WorkerSequence || connection.PlatformAck != scalar.PlatformAck || connection.WorkerAck != scalar.WorkerAck || connection.Revision != scalar.Revision || !computeChoiceTimestampEqual(connection.ConnectedAt, scalar.ConnectedAt) || !computeChoiceTimestampEqual(connection.AuthExpiresAt, scalar.AuthExpiresAt) || !computeChoiceAttachedOptionalTimeEqual(connection.ManifestObservedAt, manifest) || !computeChoiceAttachedOptionalTimeEqual(connection.LastCheckpointAt, checkpoint) || !computeChoiceAttachedOptionalTimeEqual(connection.PresenceExpiresAt, presence) || connection.ConnectedAt.After(auth.at) || connection.ManifestObservedAt.After(auth.at) || connection.LastCheckpointAt.After(auth.at) {
		return nil, errComputeChoiceSource
	}
	if _, err := attachedworkerprotocol.DecodeMachineSnapshotV1(connection.ProtocolSnapshot); err != nil {
		return nil, errComputeChoiceSource
	}
	return &connection, nil
}

func computeChoiceAttachedOptionalTimeEqual(record time.Time, scalar sql.NullTime) bool {
	if record.IsZero() {
		return !scalar.Valid
	}
	return scalar.Valid && computeChoiceTimestampEqual(record, scalar.Time)
}

func readComputeChoiceAttachedSlot(ctx context.Context, query *computeChoiceQuery, auth computeChoiceAuth, pin computeChoiceAttachedPin) (*domain.SubscriptionSchedulerSlot, error) {
	var state, run, reservation string
	var stored capacityStorageV1
	var blocked sql.NullTime
	var updated time.Time
	err := query.QueryRowContext(ctx, computeChoiceAttachedSlotSQL, pin.tenantID, pin.subscriptionID).Scan(&state, &run, &reservation, &blocked, &updated, &stored.key, &stored.association, &stored.digest)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, computeChoiceAttachedReadError(query, err)
	}
	if err := chargeComputeChoiceAttachedScalars(query, 2*8, state, run, reservation, stored.key, stored.association.String, stored.digest.String); err != nil {
		return nil, err
	}
	subscription, local, err := decodeCapacityStorageV1(pin.tenantID, stored)
	if err != nil || local != nil || subscription != pin.subscriptionID {
		return nil, errComputeChoiceSource
	}
	slot := &domain.SubscriptionSchedulerSlot{TenantID: pin.tenantID, SubscriptionConnectionID: pin.subscriptionID, State: domain.SchedulerState(state), ActiveRunID: domain.RunID(run), ActiveReservationID: domain.QuotaReservationID(reservation), UpdatedAt: updated}
	if blocked.Valid && !blocked.Time.Equal(time.Unix(0, 0)) {
		slot.BlockedUntil = &blocked.Time
	}
	if slot.Validate() != nil || updated.After(auth.at) || (slot.State == domain.SchedulerBlockedUntilReset && (slot.BlockedUntil == nil || slot.BlockedUntil.IsZero())) {
		return nil, errComputeChoiceSource
	}
	return slot, nil
}

func readComputeChoiceAttachedCounters(ctx context.Context, query *computeChoiceQuery, auth computeChoiceAuth) (*computeChoiceAttachedCounters, error) {
	var counters computeChoiceAttachedCounters
	err := query.QueryRowContext(ctx, computeChoiceAttachedCountersSQL, auth.scope.TenantID).Scan(&counters.queueDepth, &counters.activeRuns, &counters.updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, computeChoiceAttachedReadError(query, err)
	}
	if err := chargeComputeChoiceAttachedScalars(query, 2*4+8); err != nil {
		return nil, err
	}
	if counters.updatedAt.IsZero() || counters.updatedAt.After(auth.at) {
		return nil, errComputeChoiceSource
	}
	return &counters, nil
}
