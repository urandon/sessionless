package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// AttachedWorkerOutputUploadAdmissionV1 is authenticated server input, NOT a
// wire codec or a private producer witness. RemainingNanoseconds may only
// tighten the server ceiling; it cannot prove an earlier original local cutoff.
// The trusted SAME-runner exporter must retain that cutoff/cancellation itself.
type AttachedWorkerOutputUploadAdmissionV1 struct {
	Authorization                                                          ports.AttachedWorkerSealedInputAuthorization
	SessionID                                                              domain.SessionID
	HarnessBindingDigest                                                   domain.HarnessBindingDigestV2
	ProfileDigest, SchemaDigest, FinalDigest, AnswerDigest, ProducerDigest string
	Nonce                                                                  domain.IdempotencyKey
	RemainingNanoseconds                                                   uint64
	Observation                                                            attachedworkeroutput.ProcessObservationV1
	ObservationDigest                                                      string
	Artifacts                                                              []attachedworkeroutput.UploadArtifactMetadataV1
}

func (AttachedWorkerOutputUploadAdmissionV1) String() string {
	return "AttachedWorkerOutputUploadAdmissionV1{private:[redacted]}"
}
func (v AttachedWorkerOutputUploadAdmissionV1) GoString() string { return v.String() }
func (AttachedWorkerOutputUploadAdmissionV1) MarshalJSON() ([]byte, error) {
	return []byte(`"[REDACTED]"`), nil
}

// This result is private control metadata, never a canonical receipt or a PUT
// capability. No signer or BlobStore is reachable from this admission method.
type AttachedWorkerOutputUploadAdmissionResultV1 struct {
	Status          ports.AttachedWorkerExecutionStatus
	AcceptedAttempt domain.AttachedWorkerAttemptV1
	CheckedAt       time.Time
	Plan            attachedworkeroutput.AttemptOutputUploadPlanV1
}

func (AttachedWorkerOutputUploadAdmissionResultV1) String() string {
	return "AttachedWorkerOutputUploadAdmissionResultV1{private:[redacted]}"
}
func (v AttachedWorkerOutputUploadAdmissionResultV1) GoString() string { return v.String() }

// AdmitAttachedWorkerOutputUpload persists deletion custody BEFORE any caller
// can request a source-object claim. It reuses the existing receipt key/row;
// an absent/error/ambiguous transaction result never grants signing or a body.
// This library seam is not registered by any serving/profile constructor.
func (store *Store) AdmitAttachedWorkerOutputUpload(ctx context.Context, request AttachedWorkerOutputUploadAdmissionV1) (AttachedWorkerOutputUploadAdmissionResultV1, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || len(request.Artifacts) == 0 || len(request.Artifacts) > attachedworkeroutput.AttemptOutputUploadMaxArtifacts {
		return AttachedWorkerOutputUploadAdmissionResultV1{}, attachedworkeroutput.ErrCandidateInvalid
	}
	// Retain one bounded snapshot across SDK transaction retries, not a
	// caller-owned mutable slice whose metadata can change between callbacks.
	request.Artifacts = append([]attachedworkeroutput.UploadArtifactMetadataV1(nil), request.Artifacts...)
	if request.validate() != nil {
		return AttachedWorkerOutputUploadAdmissionResultV1{}, attachedworkeroutput.ErrCandidateInvalid
	}
	auth := request.Authorization
	var result AttachedWorkerOutputUploadAdmissionResultV1
	err := store.Transact(ctx, auth.TenantID, func(state ports.StateTx) error {
		// Transactions may retry. Do not return a prior callback's apparent
		// success, and never call an external signer inside this callback.
		result = AttachedWorkerOutputUploadAdmissionResultV1{Status: ports.AttachedWorkerExecutionDenied}
		tx := state.(*stateTx)
		member, err := attachedWorkerOwnerAuthorizedTx(ctx, tx, auth.OwnerUserID)
		if err != nil {
			return err
		}
		if !member {
			return nil
		}
		at, err := store.attachedWorkerTransactionTime(ctx, tx)
		if err != nil {
			return err
		}
		worker, workerFound, err := readAttachedWorkerTx(ctx, tx, auth.OwnerUserID, auth.WorkerID)
		if err != nil {
			return err
		}
		connection, connectionFound, err := readAttachedWorkerConnectionTx(ctx, tx, auth.OwnerUserID, auth.WorkerID)
		if err != nil {
			return err
		}
		attempt, attemptFound, err := readAttachedWorkerAttemptTx(ctx, tx, auth.OwnerUserID, auth.WorkerID)
		if err != nil {
			return err
		}
		if !workerFound || !connectionFound || !attemptFound ||
			!attachedWorkerOutputReceiptAuthorized(auth, domain.AttachedWorkerTerminalSucceeded, at, worker, connection, attempt) {
			return nil
		}
		_, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
		if err != nil {
			return err
		}
		if snapshot.Manifest == nil || !attachedWorkerManifestHasFeature(*snapshot.Manifest, attachedworkerprotocol.FeatureOutputReceipt) || attempt.CapabilityDigest != connection.CapabilityDigest {
			return nil
		}
		job, found, err := readJSON[domain.WorkerJob](ctx, tx.sqlTx, `SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, tx.tenantID, auth.RunID)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		run, found, err := tx.GetRun(ctx, auth.RunID)
		if err != nil {
			return err
		}
		if !found || run.Status != domain.RunRunning || job.ValidateForRun(run) != nil ||
			job.SessionID != request.SessionID || job.AttemptID != auth.AttemptID || job.ReservationID != attempt.ReservationID ||
			job.CredentialOwnerUserID != auth.OwnerUserID || job.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker ||
			job.ExecutionPlacementV2.OwnerUserID != auth.OwnerUserID || job.ExecutionPlacementV2.WorkerID != auth.WorkerID ||
			job.ExecutionPlacementV2.CapabilityDigest != auth.CapabilityDigest || job.ExecutionPlacementV2.PolicyDigest != auth.PolicyDigest ||
			!domain.IsAttachedPiSDKNativeDescriptorV1(job.HarnessBinding.Backend) || job.HarnessBinding.Backend.BackendProfileDigest != request.ProfileDigest ||
			!uploadMetadataWithinJobLimits(request.Artifacts, job.Limits) {
			return nil
		}
		bindingDigest, err := job.HarnessBinding.Digest()
		if err != nil || bindingDigest != request.HarnessBindingDigest {
			return nil
		}
		manifest, found, err := readJSON[domain.ArtifactManifest](ctx, tx.sqlTx,
			`SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2`, tx.tenantID, job.InputManifestID)
		if err != nil {
			return err
		}
		if !found || manifest.ValidateForRun(run) != nil {
			return nil
		}
		contextDigest, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
		if err != nil || contextDigest != auth.ContextDigest {
			return nil
		}
		session, found, err := readSessionTx(ctx, tx, job.SessionID)
		if err != nil {
			return err
		}
		if !found || session.Validate() != nil || session.TenantID != auth.TenantID || session.Status != domain.SessionActive {
			return nil
		}
		// Read the same deletion key that Request/Start/Complete use. Its
		// absence plus our pending receipt insertion must serialize together.
		if _, deleting, err := readSessionDeletionTx(ctx, tx, job.SessionID); err != nil {
			return err
		} else if deleting {
			return nil
		}
		credentialRequired := job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1
		if request.Observation.ValidateFor(domain.AttachedWorkerTerminalSucceeded, credentialRequired) != nil {
			return nil
		}
		prior, found, err := readAttachedWorkerOutputReceiptTx(ctx, tx, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration)
		if err != nil {
			return err
		}
		if found {
			if !sameUploadAdmission(prior, request) || !at.Before(prior.UploadPlan.Identity.ExpiresAt) {
				result.Status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			// Replaying metadata retains ALL original ordinal spend states and
			// the fixed cutoff. It never reconstructs or reissues a capability.
			result = AttachedWorkerOutputUploadAdmissionResultV1{Status: ports.AttachedWorkerExecutionReplayed, AcceptedAttempt: attempt, CheckedAt: at, Plan: prior.UploadPlan.Clone()}
			return nil
		}
		cutoff := at.Add(time.Duration(request.RemainingNanoseconds)).UTC().Truncate(time.Microsecond)
		for _, limit := range []time.Time{attempt.LeaseExpiresAt, connection.AuthExpiresAt, connection.PresenceExpiresAt} {
			if limit.Before(cutoff) {
				cutoff = limit.UTC().Truncate(time.Microsecond)
			}
		}
		plan, err := request.planAt(at, cutoff)
		if err != nil {
			return err
		}
		record := AttachedWorkerOutputReceiptV1{Version: 1, Binding: receiptBindingWithoutBearer(auth), Nonce: request.Nonce,
			Observation: request.Observation, ObservationDigest: request.ObservationDigest, CredentialRequired: credentialRequired,
			HarnessBindingDigest: bindingDigest, Status: domain.AttachedWorkerTerminalSucceeded, CreatedAt: at, UploadPlan: &plan}
		if err := insertAttachedWorkerOutputReceiptTx(ctx, tx, record); err != nil {
			return err
		}
		result = AttachedWorkerOutputUploadAdmissionResultV1{Status: ports.AttachedWorkerExecutionApplied, AcceptedAttempt: attempt, CheckedAt: at, Plan: plan.Clone()}
		return nil
	})
	if err != nil {
		return AttachedWorkerOutputUploadAdmissionResultV1{}, err
	}
	return result, nil
}

func (request AttachedWorkerOutputUploadAdmissionV1) planAt(at, cutoff time.Time) (attachedworkeroutput.AttemptOutputUploadPlanV1, error) {
	binding := request.Authorization
	binding.PresentedSecretDigest = "" // retain exact nonzero accepted revision
	return attachedworkeroutput.NewAttemptOutputUploadPlanV1(attachedworkeroutput.UploadPlanIdentityV1{Binding: binding,
		SessionID: request.SessionID, HarnessBindingDigest: request.HarnessBindingDigest, ProfileDigest: request.ProfileDigest,
		SchemaDigest: request.SchemaDigest, FinalDigest: request.FinalDigest, AnswerDigest: request.AnswerDigest, ProducerDigest: request.ProducerDigest,
		Nonce: request.Nonce, CreatedAt: at, ExpiresAt: cutoff}, request.Artifacts)
}

func (request AttachedWorkerOutputUploadAdmissionV1) validate() error {
	if validateAttachedWorkerSealedInputAuthorization(request.Authorization) != nil || request.Authorization.ExpectedAttemptRevision == 0 ||
		request.RemainingNanoseconds == 0 || request.RemainingNanoseconds > uint64(attachedworkeroutput.AttemptOutputUploadMaxDuration) ||
		request.Observation.ProcessFailureCode != "" || request.Observation.InvocationFailureCode != "" ||
		request.Observation.ValidateFor(domain.AttachedWorkerTerminalSucceeded, request.Observation.CredentialReleaseRequired) != nil {
		return attachedworkeroutput.ErrCandidateInvalid
	}
	schema := sha256.Sum256([]byte(attachedworkeroutput.StructuredFinalSchemaV1))
	digest, err := request.Observation.Digest()
	if err != nil || request.ObservationDigest != digest || request.SchemaDigest != hex.EncodeToString(schema[:]) {
		return attachedworkeroutput.ErrCandidateInvalid
	}
	// Structural preflight only, not a fabricated transaction clock. The real
	// record is built again from the canonical transaction time after authority.
	ceiling := time.UnixMicro(request.Authorization.LeaseExpiresAtUnixMicro).UTC()
	_, err = request.planAt(ceiling.Add(-attachedworkeroutput.AttemptOutputUploadMaxDuration), ceiling)
	return err
}

func sameUploadAdmission(prior AttachedWorkerOutputReceiptV1, request AttachedWorkerOutputUploadAdmissionV1) bool {
	if prior.UploadPlan == nil || prior.UploadPlan.Phase != attachedworkeroutput.UploadPlanActiveV1 || prior.Ready || prior.CopyInProgress ||
		prior.Observation != request.Observation || prior.ObservationDigest != request.ObservationDigest {
		return false
	}
	plan, err := request.planAt(prior.UploadPlan.Identity.CreatedAt, prior.UploadPlan.Identity.ExpiresAt)
	return err == nil && plan.PlanDigest == prior.UploadPlan.PlanDigest
}

// Called only after bounded metadata validation; preserve the generic
// canonical materializer's existing MIN with the pinned job's product limits.
func uploadMetadataWithinJobLimits(artifacts []attachedworkeroutput.UploadArtifactMetadataV1, limits domain.ProductLimits) bool {
	if uint32(len(artifacts)) > limits.MaxArtifacts {
		return false
	}
	var total uint64
	for _, artifact := range artifacts {
		if artifact.Size < 0 || uint64(artifact.Size) > limits.MaxInputBytes-total {
			return false
		}
		total += uint64(artifact.Size)
	}
	return true
}
