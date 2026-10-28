package ydbstore

import (
	"context"
	"database/sql"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const attachedGenerationPresenceSQL = `SELECT SUBSTRING(CAST(attached_resource_generation_v1 AS String),0,4097)
 FROM subscription_connections WHERE tenant_id=$1 AND subscription_connection_id=$2`

// This gate belongs to the caller's allocating transaction, not a separate
// preflight or positive cache. Historical reservation/job/attempt reconciliation
// happens before it. No existing job/outbox is rewritten to the current head.
func (store *Store) validateAttachedResourceGenerationAdmissionTx(ctx context.Context, tx *stateTx, owner domain.UserID,
	resource domain.SubscriptionConnectionID, placement domain.ExecutionPlacementV2, binding domain.HarnessBindingV2,
) error {
	if placement.Kind != domain.ExecutionPlacementAttachedWorker || binding.TenantID != tx.tenantID || binding.OwnerUserID != owner ||
		binding.Resource.Kind == domain.ProviderResourceSubscriptionV1 && binding.Resource.ResourceID != string(resource) {
		return domain.ErrMembershipDenied
	}
	// Non-subscription providers have independent resource identities (including
	// the existing credentialless route). They still pass the same subscription
	// head/worker fence below: changing kind cannot bypass published authority.
	baseline, configured := store.attachedResourceGenerations.Lookup(tx.tenantID, owner, resource)
	if !configured {
		// Removing the installed registry must not turn previously published
		// generation authority back into an unchecked fixed pin. A null head
		// alone preserves the separate, existing fixed-resource path.
		var encoded sql.NullString
		if err := tx.sqlTx.QueryRowContext(ctx, attachedGenerationPresenceSQL, tx.tenantID, resource).Scan(&encoded); err != nil {
			return domain.ErrMembershipDenied
		}
		if encoded.Valid || store.attachedResourceGenerations.ContainsWorker(tx.tenantID, owner, placement.WorkerID) {
			return domain.ErrMembershipDenied
		}
		return nil
	}
	at, err := currentAttachedWorkerTransactionTime(ctx, tx.sqlTx)
	if err != nil {
		return err
	}
	auth := ports.AttachedResourceGenerationAuthorizationV1{TenantID: tx.tenantID, OwnerUserID: owner, SubscriptionConnectionID: resource,
		WorkerID: baseline.WorkerID, EnrollmentGeneration: baseline.EnrollmentGeneration, BaselineRevision: baseline.BaselineRevision}
	_, head, found, err := store.authorizeAttachedResourceGenerationTx(ctx, tx, auth, false, at.UTC())
	if err != nil {
		return err
	}
	if !found || head.Revoked || head.CredentialGeneration != binding.Resource.CredentialGeneration || !attachedGenerationBaselineCurrent(baseline, at) {
		return domain.ErrMembershipDenied
	}
	expected, _, err := domain.ProjectAttachedResourceGenerationV1(baseline, head.CredentialGeneration)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	expectedPlacement := domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: owner, WorkerID: baseline.WorkerID,
		CapabilityDigest: baseline.CapabilityDigest, PolicyDigest: domain.AttachedWorkerPolicyDigest(expected.EffectivePolicyDigest)}
	expectedPlacementDigest, err := domain.ExecutionPlacementDigest(expectedPlacement)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	actualPlacementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil || actualPlacementDigest != expectedPlacementDigest {
		return domain.ErrMembershipDenied
	}
	expected.RunID, expected.AttemptID, expected.ExecutionPlacementDigest = binding.RunID, binding.AttemptID, string(expectedPlacementDigest)
	if expected.ValidateForScope(tx.tenantID, owner, binding.RunID, binding.AttemptID, placement) != nil || expected.EvidenceExpiresAt == nil || !at.Before(*expected.EvidenceExpiresAt) {
		return domain.ErrMembershipDenied
	}
	expectedDigest, err := expected.Digest()
	if err != nil {
		return domain.ErrMembershipDenied
	}
	actualDigest, err := binding.Digest()
	if err != nil || actualDigest != expectedDigest {
		return domain.ErrMembershipDenied
	}
	return nil
}
