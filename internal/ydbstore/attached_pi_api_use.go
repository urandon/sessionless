package ydbstore

import (
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var _ ports.AttachedWorkerPiAPIUseAuthorizerV1 = (*Store)(nil)

// AuthorizeAttachedWorkerPiAPIUse is the read-only Pi phase source behind the
// ORIGINAL connection bearer. One serializable callback reads the original
// claimed head, canonical job/Run/Attempt/Lease, principal/deletion and current
// owner decision. It never issues a credential, renews a lease or reserves an
// effect. A successful response is comparison metadata, not a local send grant:
// the original parent must still spend its permit and enforce full RPC elapsed
// time, retained monotonic deadline and safety slack at actual use.
func (store *Store) AuthorizeAttachedWorkerPiAPIUse(ctx context.Context, auth ports.AttachedWorkerSealedInputAuthorization,
	expected domain.HarnessBindingDigestV2, mapping domain.AttachedPiAPIInvocationPolicyMappingDigestV1,
	remaining uint64, phase string,
) (ports.AttachedWorkerPiAPIUseResultV1, error) {
	denied := ports.AttachedWorkerPiAPIUseResultV1{Status: ports.AttachedWorkerExecutionDenied}
	if store == nil || store.db == nil || store.attachedPiAPIPolicyCatalog == nil || ctx == nil || ctx.Err() != nil ||
		validateAttachedWorkerSealedInputAuthorization(auth) != nil {
		return denied, domain.ErrMembershipDenied
	}
	// The transport derives PresentedSecretDigest. It must not be serializable
	// as an alternative browser/worker-selected authority in the closed wire.
	wireAuth := auth
	wireAuth.PresentedSecretDigest = ""
	request := pisdkphasewire.Request{Version: 1, Authorization: wireAuth, ExpectedHarnessBindingDigest: string(expected),
		ExpectedAttachedPiAPIInvocationPolicyDigest: string(mapping), CallerDeadline: pisdkphasewire.CallerDeadline{Version: 1, RemainingNanoseconds: remaining}, Phase: pisdkphasewire.Phase(phase)}
	if _, err := pisdkphasewire.EncodeRequest(request); err != nil {
		return denied, domain.ErrMembershipDenied
	}
	result := denied
	err := store.Transact(ctx, auth.TenantID, func(state ports.StateTx) error {
		result = denied // no speculative success survives a retry or commit error
		tx := state.(*stateTx)
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		worker, found, err := readAttachedWorkerTx(ctx, tx, auth.OwnerUserID, auth.WorkerID)
		if err != nil || !found {
			return err
		}
		connection, found, err := readAttachedWorkerConnectionTx(ctx, tx, auth.OwnerUserID, auth.WorkerID)
		if err != nil || !found {
			return err
		}
		head, found, err := readAttachedWorkerAttemptTx(ctx, tx, auth.OwnerUserID, auth.WorkerID)
		if err != nil || !found {
			return err
		}
		if !attachedWorkerSealedInputAuthorized(auth, at, worker, connection, head) || head.Validate() != nil || connection.CapabilityDigest != auth.CapabilityDigest ||
			head.CancelRevision != 0 || !head.CancelDeadline.IsZero() || head.TerminalSequence != 0 || head.TerminalStatus != "" || head.TerminalEvidenceDigest != "" ||
			head.CreatedAt.After(at) || head.UpdatedAt.After(at) || worker.UpdatedAt.After(at) || connection.ConnectedAt.After(at) ||
			connection.LastCheckpointAt.After(at) || connection.ManifestObservedAt.After(at) {
			return nil
		}
		loaded, found, err := loadWorkerJobStateTx(ctx, tx, auth.RunID)
		if err != nil || !found {
			return err
		}
		lease, found, err := readCanonicalLeaseHeadTx(ctx, tx, auth.RunID)
		if err != nil || !found {
			return err
		}
		if !piAPIOriginalJobMatches(loaded, lease, head, auth, expected, mapping, at) {
			return nil
		}
		if err := currentServerlessSessionPrincipalTx(ctx, tx.sqlTx, loaded, at); err != nil {
			if errors.Is(err, domain.ErrMembershipDenied) {
				return nil
			}
			return err
		}
		selection, receipt, actualMapping, until, err := store.currentAttachedPiAPIPolicyTx(ctx, tx.sqlTx, loaded.Job.HarnessBinding, at)
		if err != nil {
			if errors.Is(err, domain.ErrMembershipDenied) {
				return nil
			}
			return err
		}
		// All source work and internal retry time stays inside the measured RPC.
		// Only this callback's final successful snapshot may furnish its times.
		checked, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		if checked.Before(at) || !attachedWorkerSealedInputAuthorized(auth, checked, worker, connection, head) {
			return nil
		}
		until, err = domain.ValidateAttachedPiAPIInvocationPolicyV1(selection.Profile, selection.Evidence, selection.Terms, receipt, actualMapping, loaded.Job.HarnessBinding, checked)
		if err != nil {
			return nil
		}
		limits := []time.Time{until, lease.ExpiresAt, head.LeaseExpiresAt, connection.AuthExpiresAt, connection.PresenceExpiresAt}
		// The model runtime is not a fresh timeout per phase. Its canonical
		// StartedAt anchor applies to pre-Issue/send; credential finalization
		// after quiescence retains the original lease/use bounds instead.
		if request.Phase != pisdkphasewire.ImmutableFinalize {
			if loaded.Run.StartedAt == nil || loaded.Run.StartedAt.After(checked) {
				return nil
			}
			limits = append(limits, loaded.Run.StartedAt.Add(loaded.Job.Limits.MaxRuntime))
		}
		expires, err := piAPIUseExpiry(checked, remaining, limits...)
		if err != nil {
			return nil
		}
		reply := pisdkphasewire.Reply{Status: ports.AttachedWorkerExecutionApplied, AcceptedAttempt: head, AcceptedAttemptRevision: head.Revision,
			HarnessBindingDigest: string(expected), AttachedPiAPIInvocationPolicyDigest: string(mapping), CheckedAt: checked, UseExpiresAt: expires}
		if pisdkphasewire.CompareReply(request, reply) != nil {
			return nil
		}
		result = ports.AttachedWorkerPiAPIUseResultV1{Status: reply.Status, AcceptedAttempt: head, AcceptedAttemptRevision: head.Revision,
			HarnessBindingDigest: expected, AttachedPiAPIInvocationPolicyDigest: mapping, CheckedAt: checked, UseExpiresAt: expires}
		return nil
	})
	if err != nil {
		return denied, err
	}
	return result, nil
}

func piAPIOriginalJobMatches(loaded ports.WorkerJobState, lease domain.Lease, head domain.AttachedWorkerAttemptV1,
	auth ports.AttachedWorkerSealedInputAuthorization, expected domain.HarnessBindingDigestV2, mapping domain.AttachedPiAPIInvocationPolicyMappingDigestV1, at time.Time,
) bool {
	j, r, a := loaded.Job, loaded.Run, loaded.Attempt
	if validateLoadedWorkerJob(loaded) != nil || r.Status != domain.RunRunning || r.CancellationRequestedAt != nil || a.Status != domain.AttemptRunning ||
		r.ID != auth.RunID || r.UpdatedAt.After(at) || a.UpdatedAt.After(at) || j.CreatedAt.After(at) ||
		j.AttemptID != auth.AttemptID || j.ReservationID != head.ReservationID || j.CredentialOwnerUserID != auth.OwnerUserID ||
		j.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker || j.ExecutionPlacementV2.OwnerUserID != auth.OwnerUserID ||
		j.ExecutionPlacementV2.WorkerID != auth.WorkerID || j.ExecutionPlacementV2.CapabilityDigest != auth.CapabilityDigest || j.ExecutionPlacementV2.PolicyDigest != auth.PolicyDigest ||
		!domain.IsAttachedPiSDKNativeDescriptorV1(j.HarnessBinding.Backend) || j.HarnessBinding.AttachedPiAPIInvocationPolicyDigest != mapping ||
		lease.ValidateForAttempt(r, a) != nil || lease.TenantID != auth.TenantID || lease.RunID != r.ID || lease.AttemptID != a.ID || lease.ID != auth.LeaseID ||
		lease.WorkerID != string(auth.WorkerID) || a.WorkerID != lease.WorkerID || lease.FenceToken != auth.LeaseGeneration || !lease.ExpiresAt.Equal(head.LeaseExpiresAt) ||
		lease.AcquiredAt.After(at) || !at.Before(lease.ExpiresAt) {
		return false
	}
	digest, err := j.HarnessBinding.Digest()
	if err != nil || digest != expected {
		return false
	}
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(j, loaded.InputManifest)
	return err == nil && contextDigest == auth.ContextDigest
}

// RemainingNanoseconds is untrusted: clamp BEFORE Add against every genuine
// server deadline. It may tighten but never authenticate the true local L.
func piAPIUseExpiry(at time.Time, remaining uint64, deadlines ...time.Time) (time.Time, error) {
	if !piPolicyTime(at) || remaining == 0 || remaining > uint64(1<<63-1) || len(deadlines) == 0 {
		return time.Time{}, domain.ErrMembershipDenied
	}
	delta := time.Duration(remaining)
	for _, deadline := range deadlines {
		if deadline.IsZero() || deadline.Location() != time.UTC || deadline.Year() < 1 || deadline.Year() > 9999 || !deadline.After(at) {
			return time.Time{}, domain.ErrMembershipDenied
		}
		if budget := deadline.Sub(at); budget < delta {
			delta = budget
		}
	}
	until := at.Add(delta)
	if !until.After(at) || until.Year() < 1 || until.Year() > 9999 {
		return time.Time{}, domain.ErrMembershipDenied
	}
	return until, nil
}
