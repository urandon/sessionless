package ydbstore

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// currentAttachedPiAPINewWorkTx joins the exact stored invocation mapping and
// its canonical principal with the SAME transaction that is about to admit or
// publish new work. It is not a new scheduler, reservation or permission cache.
// Callers first validate their own original outbox/job/head/receipt predicates.
// Historical reconciliation and cleanup do not acquire new spending or data
// publication authority and must not call this gate merely to replay an ACK.
func (store *Store) currentAttachedPiAPINewWorkTx(ctx context.Context, tx *stateTx, binding domain.HarnessBindingV2,
	tenant domain.TenantID, session domain.SessionID, owner domain.UserID, at time.Time,
) error {
	// Existing managed/subscription/fixture routes retain their own authority.
	// Their canonical binding validator still forbids an inapplicable carrier;
	// absence here is not an alternative way to admit a malformed Pi API job.
	if binding.AttachedPiAPIInvocationPolicyDigest == "" {
		return nil
	}
	if store == nil || tx == nil || tx.store != store || tx.sqlTx == nil || tx.tenantID != tenant ||
		ctx == nil || ctx.Err() != nil || binding.TenantID != tenant || binding.OwnerUserID != owner ||
		binding.ValidateAt(at) != nil {
		return domain.ErrMembershipDenied
	}
	if err := currentExecutionSessionPrincipalTx(ctx, tx.sqlTx, tenant, session, owner, at); err != nil {
		return err
	}
	_, _, _, _, err := store.currentAttachedPiAPIPolicyTx(ctx, tx.sqlTx, binding, at)
	return err
}

// Output uses the actual original job/manifest and Run, not just a cached
// receipt's local finalization flags. The caller retains its phase-specific
// head predicate; this function does not resurrect an expired lease or grant
// a historical observation permission to publish new canonical data.
func (store *Store) currentAttachedPiAPIOutputTx(ctx context.Context, tx *stateTx,
	auth ports.AttachedWorkerSealedInputAuthorization, job domain.WorkerJob, at time.Time,
) error {
	if job.HarnessBinding.AttachedPiAPIInvocationPolicyDigest == "" {
		return nil
	}
	loaded, found, err := loadWorkerJobStateTx(ctx, tx, auth.RunID)
	if err != nil {
		return err
	}
	if !found || validateLoadedWorkerJob(loaded) != nil || loaded.Run.Status != domain.RunRunning ||
		loaded.Run.CancellationRequestedAt != nil || loaded.Run.UpdatedAt.After(at) || loaded.Attempt.UpdatedAt.After(at) ||
		loaded.Job.CreatedAt.After(at) || loaded.Job.AttemptID != auth.AttemptID ||
		loaded.Job.CredentialOwnerUserID != auth.OwnerUserID || loaded.Job.ExecutionPlacementV2.WorkerID != auth.WorkerID ||
		loaded.Job.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker ||
		loaded.Job.ExecutionPlacementV2.CapabilityDigest != auth.CapabilityDigest || loaded.Job.ExecutionPlacementV2.PolicyDigest != auth.PolicyDigest {
		return domain.ErrMembershipDenied
	}
	actual, actualErr := loaded.Job.HarnessBinding.Digest()
	expected, expectedErr := job.HarnessBinding.Digest()
	contextDigest, contextErr := domain.AttachedWorkerJobContextDigestV1(loaded.Job, loaded.InputManifest)
	if actualErr != nil || expectedErr != nil || contextErr != nil || actual != expected || contextDigest != auth.ContextDigest {
		return domain.ErrMembershipDenied
	}
	return store.currentAttachedPiAPINewWorkTx(ctx, tx, loaded.Job.HarnessBinding,
		loaded.Run.TenantID, loaded.Run.SessionID, loaded.Job.CredentialOwnerUserID, at)
}
