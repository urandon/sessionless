package ydbstore

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// currentAttachedPiAPIInputTx authorizes data acquisition, not a provider
// phase, credential or local parent. The actual original aggregate, execution
// connection and current principal/owner decision are joined in this SAME
// serializable callback before Service may read any immutable body.
func (store *Store) currentAttachedPiAPIInputTx(ctx context.Context, tx *stateTx,
	auth ports.AttachedWorkerSealedInputAuthorization, worker domain.AttachedWorker,
	connection domain.AttachedWorkerConnection, head domain.AttachedWorkerAttemptV1,
	job domain.WorkerJob, at time.Time,
) error {
	binding := job.HarnessBinding
	apiCandidate := binding.AttachedPiAPIInvocationPolicyDigest != "" ||
		binding.Backend.BackendKind == domain.HarnessBackendPiV1 && binding.Resource.Kind == domain.ProviderResourceAPIAccountV1
	if !apiCandidate {
		return nil // existing fixture/subscription predicates remain unchanged
	}
	if binding.ValidateForScope(auth.TenantID, auth.OwnerUserID, job.RunID, job.AttemptID, job.ExecutionPlacementV2) != nil || binding.ValidateAt(at) != nil ||
		!domain.IsAttachedPiSDKNativeDescriptorV1(binding.Backend) || binding.Resource.Kind != domain.ProviderResourceAPIAccountV1 ||
		binding.AttachedPiAPIInvocationPolicyDigest == "" || job.ContextWindow == nil ||
		job.WorkspaceSnapshot != nil || job.SkillBundle != nil || len(job.AllowedMCPServers) != 0 ||
		head.ExecutionConnectionID != auth.ConnectionID || head.ExecutionConnectionGeneration != auth.ConnectionGeneration ||
		connection.CapabilityDigest != auth.CapabilityDigest || head.CancelRevision != 0 || !head.CancelDeadline.IsZero() ||
		head.TerminalSequence != 0 || head.TerminalStatus != "" || head.TerminalEvidenceDigest != "" ||
		head.CreatedAt.After(at) || head.UpdatedAt.After(at) || worker.UpdatedAt.After(at) ||
		connection.ConnectedAt.After(at) || connection.LastCheckpointAt.After(at) || connection.ManifestObservedAt.After(at) {
		return domain.ErrMembershipDenied
	}
	loaded, found, err := loadWorkerJobStateTx(ctx, tx, auth.RunID)
	if err != nil {
		return err
	}
	if !found {
		return domain.ErrMembershipDenied
	}
	lease, found, err := readCanonicalLeaseHeadTx(ctx, tx, auth.RunID)
	if err != nil {
		return err
	}
	expected, digestErr := binding.Digest()
	if !found || digestErr != nil || !piAPIOriginalJobMatches(loaded, lease, head, auth, expected, binding.AttachedPiAPIInvocationPolicyDigest, at) {
		return domain.ErrMembershipDenied
	}
	// Use fresh database time after the aggregate reads as well: elapsed source
	// work cannot renew the original runtime/lease or an expired owner decision.
	checked, err := store.attachedWorkerTransactionTime(ctx, tx)
	if err != nil {
		return err
	}
	if checked.Before(at) || !attachedWorkerSealedInputAuthorized(auth, checked, worker, connection, head) ||
		!piAPIOriginalJobMatches(loaded, lease, head, auth, expected, binding.AttachedPiAPIInvocationPolicyDigest, checked) ||
		loaded.Run.StartedAt == nil || loaded.Run.StartedAt.After(checked) ||
		!checked.Before(loaded.Run.StartedAt.Add(loaded.Job.Limits.MaxRuntime)) {
		return domain.ErrMembershipDenied
	}
	return store.currentAttachedPiAPINewWorkTx(ctx, tx, loaded.Job.HarnessBinding,
		loaded.Run.TenantID, loaded.Run.SessionID, loaded.Job.CredentialOwnerUserID, checked)
}
