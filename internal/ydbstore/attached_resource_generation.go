package ydbstore

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var ErrAttachedResourceGenerationUnavailable = errors.New("attached resource generation operation is unavailable or unresolved")

const attachedGenerationResourceSQL = `SELECT SUBSTRING(CAST(actor_id AS String),0,161),
 SUBSTRING(CAST(provider AS String),0,33),SUBSTRING(CAST(credential_ref AS String),0,2),
 SUBSTRING(CAST(attached_resource_generation_v1 AS String),0,4097)
 FROM subscription_connections WHERE tenant_id=$1 AND subscription_connection_id=$2`
const attachedGenerationActorSQL = `SELECT SUBSTRING(CAST(user_id AS String),0,161) FROM actors WHERE tenant_id=$1 AND actor_id=$2`
const attachedGenerationOwnerSQL = `SELECT SUBSTRING(CAST(subscription_connection_id AS String),0,161)
 FROM subscription_connections_by_user WHERE tenant_id=$1 AND user_id=$2 AND subscription_connection_id=$3`
const attachedGenerationUpdateSQL = `UPDATE subscription_connections
 SET attached_resource_generation_v1=CAST($3 AS JsonDocument),updated_at=$4
 WHERE tenant_id=$1 AND subscription_connection_id=$2`

func (store *Store) PublishAttachedResourceGenerationV1(ctx context.Context, request ports.AttachedResourceGenerationPublicationV1) (ports.AttachedResourceGenerationResultV1, error) {
	if request.Validate() != nil {
		return ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionDenied}, nil
	}
	var result ports.AttachedResourceGenerationResultV1
	err := store.attachedGenerationTransaction(ctx, request.Authorization.TenantID, func(call context.Context, tx *stateTx, at time.Time) error {
		baseline, head, found, err := store.authorizeAttachedResourceGenerationTx(call, tx, request.Authorization, true, at)
		if err != nil {
			return err
		}
		var current *domain.AttachedResourceGenerationHeadV1
		if found {
			current = &head
		}
		if domain.ValidateAttachedResourceGenerationTransitionV1(current, request.ExpectedCredentialGeneration, request.Target) != nil {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		if found && head == request.Target {
			result = ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionReplayed, Head: head}
			return nil
		}
		if !request.Target.Revoked {
			if !attachedGenerationBaselineCurrent(baseline, at) {
				return domain.ErrMembershipDenied
			}
			if _, _, err := domain.ProjectAttachedResourceGenerationV1(baseline, request.Target.CredentialGeneration); err != nil {
				return domain.ErrMembershipDenied
			}
		}
		raw, err := domain.EncodeAttachedResourceGenerationHeadV1(request.Target)
		if err != nil {
			return domain.ErrMembershipDenied
		}
		if _, err := tx.sqlTx.ExecContext(call, attachedGenerationUpdateSQL, tx.tenantID, request.Authorization.SubscriptionConnectionID, string(raw), at); err != nil {
			return err
		}
		result = ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionApplied, Head: request.Target}
		return nil
	})
	return attachedGenerationResult(result, err)
}

func (store *Store) ReadAttachedResourceGenerationV1(ctx context.Context, auth ports.AttachedResourceGenerationAuthorizationV1) (ports.AttachedResourceGenerationResultV1, error) {
	if auth.Validate() != nil {
		return ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionDenied}, nil
	}
	var result ports.AttachedResourceGenerationResultV1
	err := store.attachedGenerationTransaction(ctx, auth.TenantID, func(call context.Context, tx *stateTx, at time.Time) error {
		_, head, found, err := store.authorizeAttachedResourceGenerationTx(call, tx, auth, true, at)
		if err != nil {
			return err
		}
		result.Status = ports.AttachedWorkerExecutionNotFound
		if found {
			result = ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionApplied, Head: head}
		}
		return nil
	})
	return attachedGenerationResult(result, err)
}

func (store *Store) LoadAttachedResourceGenerationHeadV1(ctx context.Context, tenant domain.TenantID, owner domain.UserID, resource domain.SubscriptionConnectionID) (domain.AttachedResourceGenerationHeadV1, bool, error) {
	if store == nil || store.attachedResourceGenerations == nil {
		return domain.AttachedResourceGenerationHeadV1{}, false, domain.ErrMembershipDenied
	}
	baseline, configured := store.attachedResourceGenerations.Lookup(tenant, owner, resource)
	if !configured {
		return domain.AttachedResourceGenerationHeadV1{}, false, domain.ErrMembershipDenied
	}
	auth := ports.AttachedResourceGenerationAuthorizationV1{TenantID: tenant, OwnerUserID: owner, SubscriptionConnectionID: resource, WorkerID: baseline.WorkerID, EnrollmentGeneration: baseline.EnrollmentGeneration, BaselineRevision: baseline.BaselineRevision}
	var head domain.AttachedResourceGenerationHeadV1
	var found bool
	err := store.attachedGenerationTransaction(ctx, tenant, func(call context.Context, tx *stateTx, at time.Time) error {
		var err error
		_, head, found, err = store.authorizeAttachedResourceGenerationTx(call, tx, auth, false, at)
		return err
	})
	if err != nil {
		return domain.AttachedResourceGenerationHeadV1{}, false, err
	}
	return head, found, nil
}

func attachedGenerationResult(result ports.AttachedResourceGenerationResultV1, err error) (ports.AttachedResourceGenerationResultV1, error) {
	if errors.Is(err, domain.ErrMembershipDenied) {
		return ports.AttachedResourceGenerationResultV1{Status: ports.AttachedWorkerExecutionDenied}, nil
	}
	if err != nil {
		return ports.AttachedResourceGenerationResultV1{}, ErrAttachedResourceGenerationUnavailable
	}
	raw, encodeErr := json.Marshal(result)
	if encodeErr != nil || len(raw) > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
		return ports.AttachedResourceGenerationResultV1{}, ErrAttachedResourceGenerationUnavailable
	}
	return result, nil
}

// One direct serializable transaction, never Store.Transact/SDK mutation
// retries. An ambiguous/late Commit cannot return an acknowledged head. Only
// the original caller may perform one separate bounded point-read reconcile.
func (store *Store) attachedGenerationTransaction(ctx context.Context, tenant domain.TenantID, fn func(context.Context, *stateTx, time.Time) error) error {
	if ctx == nil || ctx.Err() != nil || tenant.Validate() != nil || store == nil || store.db == nil || store.attachedResourceGenerations == nil || fn == nil {
		return domain.ErrMembershipDenied
	}
	call, cancel := context.WithTimeout(ctx, ports.AttachedResourceGenerationOperationTimeoutV1)
	defer cancel()
	tx, err := store.db.BeginTx(call, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return ErrAttachedResourceGenerationUnavailable
	}
	defer func() { _ = tx.Rollback() }()
	state := &stateTx{store: store, sqlTx: tx, tenantID: tenant}
	at, err := currentAttachedWorkerTransactionTime(call, tx)
	if err != nil || at.IsZero() || at.Year() < 1 || at.Year() > 9999 || at.Nanosecond()%1000 != 0 {
		return ErrAttachedResourceGenerationUnavailable
	}
	if err := fn(call, state, at.UTC()); err != nil {
		return err
	}
	if call.Err() != nil {
		return ErrAttachedResourceGenerationUnavailable
	}
	if err := tx.Commit(); err != nil || call.Err() != nil {
		return ErrAttachedResourceGenerationUnavailable
	}
	return nil
}

// This joins the SAME resource row, membership, actor projection and original
// authenticated worker connection. It creates no entitlement or credential.
// The bearer-free form is private to the authenticated BFF owner preflight.
func (store *Store) authorizeAttachedResourceGenerationTx(ctx context.Context, tx *stateTx, auth ports.AttachedResourceGenerationAuthorizationV1, bearer bool, at time.Time) (domain.AttachedResourceGenerationBaselineV1, domain.AttachedResourceGenerationHeadV1, bool, error) {
	deny := func() (domain.AttachedResourceGenerationBaselineV1, domain.AttachedResourceGenerationHeadV1, bool, error) {
		return domain.AttachedResourceGenerationBaselineV1{}, domain.AttachedResourceGenerationHeadV1{}, false, domain.ErrMembershipDenied
	}
	if store.attachedResourceGenerations == nil {
		return deny()
	}
	baseline, found := store.attachedResourceGenerations.Lookup(tx.tenantID, auth.OwnerUserID, auth.SubscriptionConnectionID)
	if !found || auth.TenantID != tx.tenantID || baseline.WorkerID != auth.WorkerID || baseline.EnrollmentGeneration != auth.EnrollmentGeneration || baseline.BaselineRevision != auth.BaselineRevision {
		return deny()
	}
	member, err := attachedWorkerOwnerAuthorizedTx(ctx, tx, auth.OwnerUserID)
	if err != nil {
		return domain.AttachedResourceGenerationBaselineV1{}, domain.AttachedResourceGenerationHeadV1{}, false, err
	}
	if !member {
		return deny()
	}
	var actor domain.ActorID
	var provider, credential string
	var encoded sql.NullString
	if err := tx.sqlTx.QueryRowContext(ctx, attachedGenerationResourceSQL, tx.tenantID, auth.SubscriptionConnectionID).Scan(&actor, &provider, &credential, &encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return deny()
		}
		return domain.AttachedResourceGenerationBaselineV1{}, domain.AttachedResourceGenerationHeadV1{}, false, err
	}
	if actor.Validate() != nil || provider != "codex" || credential != "" {
		return deny()
	}
	var owner domain.UserID
	if err := tx.sqlTx.QueryRowContext(ctx, attachedGenerationActorSQL, tx.tenantID, actor).Scan(&owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return deny()
		}
		return domain.AttachedResourceGenerationBaselineV1{}, domain.AttachedResourceGenerationHeadV1{}, false, err
	}
	var projected domain.SubscriptionConnectionID
	if owner != auth.OwnerUserID {
		return deny()
	}
	if err := tx.sqlTx.QueryRowContext(ctx, attachedGenerationOwnerSQL, tx.tenantID, auth.OwnerUserID, auth.SubscriptionConnectionID).Scan(&projected); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return deny()
		}
		return domain.AttachedResourceGenerationBaselineV1{}, domain.AttachedResourceGenerationHeadV1{}, false, err
	}
	if projected != auth.SubscriptionConnectionID {
		return deny()
	}
	worker, workerFound, err := readAttachedWorkerTx(ctx, tx, auth.OwnerUserID, auth.WorkerID)
	if err != nil {
		return domain.AttachedResourceGenerationBaselineV1{}, domain.AttachedResourceGenerationHeadV1{}, false, err
	}
	connection, connectionFound, err := readAttachedWorkerConnectionTx(ctx, tx, auth.OwnerUserID, auth.WorkerID)
	if err != nil {
		return domain.AttachedResourceGenerationBaselineV1{}, domain.AttachedResourceGenerationHeadV1{}, false, err
	}
	if !workerFound || !connectionFound || worker.DesiredState == domain.AttachedWorkerDesiredRevoked ||
		worker.EnrollmentGeneration != baseline.EnrollmentGeneration || connection.EnrollmentGeneration != baseline.EnrollmentGeneration ||
		worker.ConnectionGeneration != connection.ConnectionGeneration || connection.CapabilityDigest != baseline.CapabilityDigest ||
		connection.State != domain.AttachedWorkerConnectionOnline && connection.State != domain.AttachedWorkerConnectionDraining ||
		!at.Before(connection.AuthExpiresAt) || worker.UpdatedAt.After(at) || connection.ConnectedAt.After(at) || connection.LastCheckpointAt.After(at) ||
		bearer && (connection.ID != auth.ConnectionID || connection.ConnectionGeneration != auth.ConnectionGeneration || subtle.ConstantTimeCompare([]byte(connection.SecretDigest), []byte(auth.PresentedSecretDigest)) != 1) {
		return deny()
	}
	_, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
	if err != nil || snapshot.Manifest == nil {
		return deny()
	}
	manifest, manifestFound, err := readAttachedWorkerManifestTx(ctx, tx, auth.OwnerUserID, auth.WorkerID, connection.CapabilityDigest)
	if err != nil {
		return domain.AttachedResourceGenerationBaselineV1{}, domain.AttachedResourceGenerationHeadV1{}, false, err
	}
	payload, err := json.Marshal(snapshot.Manifest)
	if err != nil || !manifestFound || manifest.EnrollmentGeneration != baseline.EnrollmentGeneration || manifest.ManifestRevision != connection.ManifestRevision ||
		manifest.ProtocolVersion != connection.ProtocolVersion || manifest.IdentityKeyDigest != connection.ManifestIdentityKey ||
		manifest.IdentityKeyDigest != domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey) || len(connection.ManifestSignature) != ed25519.SignatureSize ||
		!bytes.Equal(payload, manifest.ManifestPayload) {
		return deny()
	}
	var head domain.AttachedResourceGenerationHeadV1
	if encoded.Valid {
		if len(encoded.String) > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
			return deny()
		}
		head, err = domain.DecodeAttachedResourceGenerationHeadV1([]byte(encoded.String))
		if err != nil || head.OwnerUserID != baseline.OwnerUserID || head.WorkerID != baseline.WorkerID || head.EnrollmentGeneration != baseline.EnrollmentGeneration || head.BaselineRevision != baseline.BaselineRevision {
			return deny()
		}
	}
	return baseline, head, encoded.Valid, nil
}

func attachedGenerationBaselineCurrent(baseline domain.AttachedResourceGenerationBaselineV1, at time.Time) bool {
	if baseline.Binding.EvidenceExpiresAt == nil || !at.Before(*baseline.Binding.EvidenceExpiresAt) {
		return false
	}
	for _, observed := range []time.Time{baseline.Evidence.Catalog.ObservedAt, baseline.Evidence.Route.ObservedAt, baseline.Evidence.Capability.ObservedAt, baseline.Evidence.Privacy.ObservedAt, baseline.Evidence.Price.ObservedAt, baseline.Evidence.Policy.ObservedAt} {
		if observed.After(at) {
			return false
		}
	}
	return true
}

var _ ports.AttachedResourceGenerationStoreV1 = (*Store)(nil)
var _ ports.AttachedResourceGenerationHeadStoreV1 = (*Store)(nil)
