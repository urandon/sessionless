package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// AttachedWorkerOutputUploadClaimV1 requests an irreversible ordinal spend in
// the existing pending row. It carries neither content nor a destination.
// This is authenticated control DATA, not original private exporter evidence.
type AttachedWorkerOutputUploadClaimV1 struct {
	Version              uint32
	Authorization        ports.AttachedWorkerSealedInputAuthorization
	Nonce                domain.IdempotencyKey
	PlanDigest           string
	Ordinal              uint32
	RemainingNanoseconds uint64
}

func (AttachedWorkerOutputUploadClaimV1) String() string {
	return "AttachedWorkerOutputUploadClaimV1{private:[redacted]}"
}
func (v AttachedWorkerOutputUploadClaimV1) GoString() string { return v.String() }
func (AttachedWorkerOutputUploadClaimV1) MarshalJSON() ([]byte, error) {
	return []byte(`"[REDACTED]"`), nil
}

// A successful claim proves only committed pending custody. It does NOT grant
// signing: full current Source/provider authority and privately retained
// expected-profile/original-exporter evidence remain separate requirements.
// No signer/body dependency or serving constructor is connected here.
type AttachedWorkerOutputUploadClaimResultV1 struct {
	Status          ports.AttachedWorkerExecutionStatus
	AcceptedAttempt domain.AttachedWorkerAttemptV1
	CheckedAt       time.Time
	ExpiresAt       time.Time
	Plan            attachedworkeroutput.AttemptOutputUploadPlanV1
}

func (AttachedWorkerOutputUploadClaimResultV1) String() string {
	return "AttachedWorkerOutputUploadClaimResultV1{private:[redacted]}"
}
func (v AttachedWorkerOutputUploadClaimResultV1) GoString() string { return v.String() }
func (AttachedWorkerOutputUploadClaimResultV1) MarshalJSON() ([]byte, error) {
	return []byte(`"[REDACTED]"`), nil
}

// ClaimAttachedWorkerOutputUpload serializes fresh ordinary execution scope,
// Session deletion and an unissued->write_pending update in ONE transaction.
// Repeated or lost replies cannot reopen an ordinal. An unknown commit returns
// no claim result, even if a callback previously observed apparent success.
func (store *Store) ClaimAttachedWorkerOutputUpload(ctx context.Context, request AttachedWorkerOutputUploadClaimV1) (AttachedWorkerOutputUploadClaimResultV1, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || request.validate() != nil {
		return AttachedWorkerOutputUploadClaimResultV1{}, attachedworkeroutput.ErrCandidateInvalid
	}
	auth := request.Authorization
	var result AttachedWorkerOutputUploadClaimResultV1
	var claimCutoff time.Time // selected once, then only tightened across retries
	err := store.Transact(ctx, auth.TenantID, func(state ports.StateTx) error {
		result = AttachedWorkerOutputUploadClaimResultV1{Status: ports.AttachedWorkerExecutionDenied}
		tx := state.(*stateTx)
		prior, found, err := readAttachedWorkerOutputReceiptTx(ctx, tx, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration)
		if err != nil {
			return err
		}
		if !found || !uploadClaimMatchesRecord(prior, request) {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		// Reconstruct bounded metadata ONLY from the strictly read persisted
		// plan, never caller-supplied replacement metadata or a new cutoff.
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
		claimCutoff = tightenUploadClaimCutoff(claimCutoff, scope.at, request.RemainingNanoseconds,
			prior.UploadPlan.Identity.ExpiresAt, scope.attempt.LeaseExpiresAt, scope.connection.AuthExpiresAt, scope.connection.PresenceExpiresAt)
		cutoff := claimCutoff
		claimed, err := spendUploadReceiptOrdinal(prior, request, scope.at, cutoff)
		if err != nil {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		encoded, err := json.Marshal(claimed)
		if err != nil {
			return err
		}
		_, err = tx.sqlTx.ExecContext(ctx,
			`UPDATE attached_worker_output_receipts SET payload=CAST($7 AS JsonDocument)
			 WHERE tenant_id=$1 AND run_id=$2 AND owner_user_id=$3 AND worker_id=$4 AND attempt_id=$5 AND lease_generation=$6`,
			auth.TenantID, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration, string(encoded))
		if err != nil {
			return err
		}
		result = AttachedWorkerOutputUploadClaimResultV1{Status: ports.AttachedWorkerExecutionApplied,
			AcceptedAttempt: scope.attempt, CheckedAt: scope.at, ExpiresAt: cutoff, Plan: claimed.UploadPlan.Clone()}
		return nil
	})
	if err != nil {
		return AttachedWorkerOutputUploadClaimResultV1{}, err
	}
	return result, nil
}

func (request AttachedWorkerOutputUploadClaimV1) validate() error {
	if request.Version != 1 || validateAttachedWorkerSealedInputAuthorization(request.Authorization) != nil ||
		request.Authorization.ExpectedAttemptRevision == 0 || request.Nonce.Validate() != nil || !uploadClaimDigest(request.PlanDigest) ||
		request.Ordinal >= attachedworkeroutput.AttemptOutputUploadMaxArtifacts || request.RemainingNanoseconds == 0 ||
		request.RemainingNanoseconds > uint64(attachedworkeroutput.AttemptOutputUploadMaxDuration) {
		return attachedworkeroutput.ErrCandidateInvalid
	}
	return nil
}

func uploadClaimDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for i := range value {
		if b := value[i]; !(b >= '0' && b <= '9' || b >= 'a' && b <= 'f') {
			return false
		}
	}
	return true
}

// Called after request validation with canonical server timestamps. A retried
// callback must not restart W; neither a later sample nor changed connection
// bounds may extend the first selected deadline.
func tightenUploadClaimCutoff(previous, at time.Time, remaining uint64, limits ...time.Time) time.Time {
	cutoff := at.Add(time.Duration(remaining)).UTC().Truncate(time.Microsecond)
	if !previous.IsZero() && previous.Before(cutoff) {
		cutoff = previous
	}
	for _, limit := range limits {
		if limit.Before(cutoff) {
			cutoff = limit.UTC().Truncate(time.Microsecond)
		}
	}
	return cutoff
}

func uploadClaimMatchesRecord(prior AttachedWorkerOutputReceiptV1, request AttachedWorkerOutputUploadClaimV1) bool {
	if prior.UploadPlan == nil || prior.UploadPlan.Phase != attachedworkeroutput.UploadPlanActiveV1 || prior.Ready || prior.CopyInProgress ||
		prior.Nonce != request.Nonce || prior.UploadPlan.PlanDigest != request.PlanDigest {
		return false
	}
	binding := request.Authorization
	binding.PresentedSecretDigest = "" // retain exact original nonzero revision
	return binding == prior.UploadPlan.Identity.Binding
}

func uploadAdmissionFromRecord(prior AttachedWorkerOutputReceiptV1, auth ports.AttachedWorkerSealedInputAuthorization) AttachedWorkerOutputUploadAdmissionV1 {
	id := prior.UploadPlan.Identity
	metadata := make([]attachedworkeroutput.UploadArtifactMetadataV1, len(prior.UploadPlan.Ordinals))
	for i, ordinal := range prior.UploadPlan.Ordinals {
		metadata[i] = ordinal.Metadata
	}
	return AttachedWorkerOutputUploadAdmissionV1{Authorization: auth, SessionID: id.SessionID, HarnessBindingDigest: id.HarnessBindingDigest,
		ProfileDigest: id.ProfileDigest, SchemaDigest: id.SchemaDigest, FinalDigest: id.FinalDigest, AnswerDigest: id.AnswerDigest,
		ProducerDigest: id.ProducerDigest, Nonce: id.Nonce, RemainingNanoseconds: uint64(id.ExpiresAt.Sub(id.CreatedAt)),
		Observation: prior.Observation, ObservationDigest: prior.ObservationDigest, Artifacts: metadata}
}

// Pure bounded transition used by the transaction; not a commit certificate.
// No timestamps, immutable plan identity or previous spends are renewed.
func spendUploadReceiptOrdinal(prior AttachedWorkerOutputReceiptV1, request AttachedWorkerOutputUploadClaimV1, at, cutoff time.Time) (AttachedWorkerOutputReceiptV1, error) {
	if request.validate() != nil || !uploadClaimMatchesRecord(prior, request) ||
		validateAttachedWorkerReceiptRecord(prior, request.Authorization.TenantID, request.Authorization.RunID,
			request.Authorization.OwnerUserID, request.Authorization.WorkerID, request.Authorization.AttemptID, request.Authorization.LeaseGeneration) != nil ||
		!cutoff.After(at) || cutoff.After(prior.UploadPlan.Identity.ExpiresAt) || cutoff.After(at.Add(time.Duration(request.RemainingNanoseconds))) ||
		!cutoff.Equal(canonicalAttachedWorkerTime(cutoff)) {
		return AttachedWorkerOutputReceiptV1{}, attachedworkeroutput.ErrCandidateInvalid
	}
	// Domain-separated nonsecret identity, not a bearer or permission to retry.
	// PlanDigest already binds the complete immutable original identity,
	// metadata and fixed original cutoff. Retries cannot mint a new identity.
	digest := sha256.Sum256([]byte("sessionless:attempt-output-upload-claim:v1\x00" + request.PlanDigest + "\x00" +
		strconv.FormatUint(uint64(request.Ordinal), 10)))
	plan, err := prior.UploadPlan.SpendOrdinal(request.Ordinal, hex.EncodeToString(digest[:]), at)
	if err != nil {
		return AttachedWorkerOutputReceiptV1{}, err
	}
	prior.UploadPlan = &plan
	if err := validateAttachedWorkerReceiptRecord(prior, request.Authorization.TenantID, request.Authorization.RunID,
		request.Authorization.OwnerUserID, request.Authorization.WorkerID, request.Authorization.AttemptID, request.Authorization.LeaseGeneration); err != nil {
		return AttachedWorkerOutputReceiptV1{}, err
	}
	return prior, nil
}
