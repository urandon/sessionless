package ydbstore

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Reconciliation observes original pending custody after a lost control reply.
// No caller supplies a plan, source ref, grant or no-dispatch/settlement flag.
type AttachedWorkerOutputUploadReconciliationV1 struct {
	Version              uint32
	Authorization        ports.AttachedWorkerSealedInputAuthorization
	Nonce                domain.IdempotencyKey
	ProducerDigest       string
	FinalDigest          string
	RemainingNanoseconds uint64
}

func (AttachedWorkerOutputUploadReconciliationV1) String() string {
	return "AttachedWorkerOutputUploadReconciliationV1{private:[redacted]}"
}
func (v AttachedWorkerOutputUploadReconciliationV1) GoString() string { return v.String() }
func (AttachedWorkerOutputUploadReconciliationV1) MarshalJSON() ([]byte, error) {
	return []byte(`"[REDACTED]"`), nil
}

// The snapshot has the same non-capability shape as admission. Reusing that
// canonical result avoids a second plan representation. Replayed means only
// an observed row, NEVER a new spend, signing right or canonical receipt.
func (store *Store) ReconcileAttachedWorkerOutputUpload(ctx context.Context, request AttachedWorkerOutputUploadReconciliationV1) (AttachedWorkerOutputUploadAdmissionResultV1, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || request.validate() != nil {
		return AttachedWorkerOutputUploadAdmissionResultV1{}, attachedworkeroutput.ErrCandidateInvalid
	}
	auth := request.Authorization
	var result AttachedWorkerOutputUploadAdmissionResultV1
	err := store.Transact(ctx, auth.TenantID, func(state ports.StateTx) error {
		result = AttachedWorkerOutputUploadAdmissionResultV1{Status: ports.AttachedWorkerExecutionDenied}
		tx := state.(*stateTx)
		prior, found, err := readAttachedWorkerOutputReceiptTx(ctx, tx, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration)
		if err != nil {
			return err
		}
		if !found || !uploadReconciliationMatchesRecord(prior, request) {
			return nil
		}
		admission := uploadAdmissionFromRecord(prior, auth)
		if admission.validate() != nil {
			return ErrAttachedWorkerAttemptConflict
		}
		scope, authorized, err := store.readAttachedWorkerOutputUploadScopeTx(ctx, tx, admission)
		if err != nil {
			return err
		}
		if !authorized {
			return nil
		}
		required := scope.job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1
		if prior.CredentialRequired != required || prior.Observation.ValidateFor(domain.AttachedWorkerTerminalSucceeded, required) != nil {
			return nil
		}
		if !scope.at.Before(prior.UploadPlan.Identity.ExpiresAt) {
			// Expiry is not no-dispatch, quiescence, settlement or permission
			// to erase a pending ordinal. No state update occurs on this path.
			result.Status = ports.AttachedWorkerExecutionExpired
			return nil
		}
		result = AttachedWorkerOutputUploadAdmissionResultV1{Status: ports.AttachedWorkerExecutionReplayed,
			AcceptedAttempt: scope.attempt, CheckedAt: scope.at, Plan: prior.UploadPlan.Clone()}
		return nil
	})
	if err != nil {
		return AttachedWorkerOutputUploadAdmissionResultV1{}, err
	}
	return result, nil
}

func (request AttachedWorkerOutputUploadReconciliationV1) validate() error {
	if request.Version != 1 || validateAttachedWorkerSealedInputAuthorization(request.Authorization) != nil ||
		request.Authorization.ExpectedAttemptRevision == 0 || request.Nonce.Validate() != nil ||
		!uploadClaimDigest(request.ProducerDigest) || !uploadClaimDigest(request.FinalDigest) ||
		request.RemainingNanoseconds == 0 || request.RemainingNanoseconds > uint64(attachedworkeroutput.AttemptOutputUploadMaxDuration) {
		return attachedworkeroutput.ErrCandidateInvalid
	}
	return nil
}

func uploadReconciliationMatchesRecord(prior AttachedWorkerOutputReceiptV1, request AttachedWorkerOutputUploadReconciliationV1) bool {
	if prior.UploadPlan == nil || prior.UploadPlan.Identity.ProducerDigest != request.ProducerDigest ||
		prior.UploadPlan.Identity.FinalDigest != request.FinalDigest {
		return false
	}
	return uploadClaimMatchesRecord(prior, AttachedWorkerOutputUploadClaimV1{
		Authorization: request.Authorization, Nonce: request.Nonce, PlanDigest: prior.UploadPlan.PlanDigest,
	})
}
