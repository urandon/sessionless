package ydbstore

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// AttachedWorkerOutputReceiptRequest is an authenticated candidate, not
// canonical materialization. Its caller cannot select an event ID, manifest
// ID or finalization digest.
type AttachedWorkerOutputReceiptRequest struct {
	Authorization ports.AttachedWorkerSealedInputAuthorization
	Nonce         domain.IdempotencyKey
	Candidate     attachedworkeroutput.Candidate
	Observation   attachedworkeroutput.ProcessObservationV1
}

type AttachedWorkerOutputReceiptV1 struct {
	Version              uint32                                       `json:"version"`
	Ready                bool                                         `json:"ready"`
	CopyInProgress       bool                                         `json:"copy_in_progress"`
	Binding              ports.AttachedWorkerSealedInputAuthorization `json:"binding"`
	Nonce                domain.IdempotencyKey                        `json:"nonce"`
	CandidateFingerprint string                                       `json:"candidate_fingerprint"`
	ObservationDigest    string                                       `json:"observation_digest"`
	Observation          attachedworkeroutput.ProcessObservationV1    `json:"observation"`
	CredentialRequired   bool                                         `json:"credential_required"`
	HarnessBindingDigest domain.HarnessBindingDigestV1                `json:"harness_binding_digest"`
	Status               domain.AttachedWorkerTerminalStatus          `json:"status"`
	CanonicalDigest      domain.AttachedWorkerTerminalEvidenceDigest  `json:"canonical_digest"`
	Materialization      ports.AttachedWorkerTerminalMaterialization  `json:"materialization"`
	SourceRefs           []domain.BlobRef                             `json:"source_refs,omitempty"`
	CreatedAt            time.Time                                    `json:"created_at"`
}

type AttachedWorkerOutputReceiptResult struct {
	Status  ports.AttachedWorkerExecutionStatus
	Receipt AttachedWorkerOutputReceiptV1
}

var ErrAttachedWorkerReceiptCopyInProgress = errors.New("attached-worker receipt canonical object copy is in progress")

// ErrAttachedWorkerReceiptPutNotDispatched may be returned by a BlobStore.Put
// only when it can prove that no remote write was started. An ordinary Put
// error, including timeout or lost response, is ambiguous and must retain the
// durable copy barrier.
var ErrAttachedWorkerReceiptPutNotDispatched = errors.New("attached-worker receipt object put was not dispatched")

// CreateAttachedWorkerOutputReceipt rechecks the exact owner/bearer/lease in
// the YDB write transaction after non-transactional object verification. A
// lost response can replay the immutable record by exact nonce/fingerprint.
// No activation path calls this method until the receipt protocol is wired.
func (store *Store) CreateAttachedWorkerOutputReceipt(
	ctx context.Context,
	blobs ports.BlobStore,
	request AttachedWorkerOutputReceiptRequest,
) (result AttachedWorkerOutputReceiptResult, err error) {
	if store == nil || blobs == nil || request.Nonce.Validate() != nil ||
		validateAttachedWorkerSealedInputAuthorization(request.Authorization) != nil ||
		!request.Candidate.Status.Valid() {
		return result, attachedworkeroutput.ErrCandidateInvalid
	}
	fingerprint, err := attachedworkeroutput.CandidateFingerprint(request.Candidate)
	if err != nil {
		return result, err
	}
	observationDigest, err := request.Observation.Digest()
	if err != nil {
		return result, err
	}
	var prepareAt time.Time
	var existing AttachedWorkerOutputReceiptV1
	var found bool
	status, err := store.outputReceiptHeadTransaction(ctx, request, fingerprint, observationDigest, nil, &existing, &found, &prepareAt, nil)
	if err != nil {
		return result, err
	}
	if found && existing.Ready {
		return AttachedWorkerOutputReceiptResult{Status: status, Receipt: existing}, nil
	}
	if found && existing.CopyInProgress {
		return result, ErrAttachedWorkerReceiptCopyInProgress
	}
	if status != ports.AttachedWorkerExecutionApplied {
		return AttachedWorkerOutputReceiptResult{Status: status}, nil
	}
	if found {
		prepareAt = existing.CreatedAt
	}
	loaded, loadedFound, err := store.LoadWorkerJob(ctx, request.Authorization.TenantID, request.Authorization.RunID)
	if err != nil {
		return result, err
	}
	if !loadedFound || loaded.Job.AttemptID != request.Authorization.AttemptID ||
		loaded.Job.ReservationID == "" || loaded.Job.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker ||
		loaded.Job.ExecutionPlacementV2.OwnerUserID != request.Authorization.OwnerUserID ||
		loaded.Job.ExecutionPlacementV2.WorkerID != request.Authorization.WorkerID ||
		loaded.Job.ExecutionPlacementV2.CapabilityDigest != request.Authorization.CapabilityDigest ||
		loaded.Job.ExecutionPlacementV2.PolicyDigest != request.Authorization.PolicyDigest {
		return AttachedWorkerOutputReceiptResult{Status: ports.AttachedWorkerExecutionFenced}, nil
	}
	credentialRequired := loaded.Job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1
	bindingDigest, err := loaded.Job.HarnessBinding.Digest()
	if err != nil {
		return AttachedWorkerOutputReceiptResult{Status: ports.AttachedWorkerExecutionFenced}, nil
	}
	if err := request.Observation.ValidateFor(request.Candidate.Status, credentialRequired); err != nil {
		return result, err
	}
	planned := &plannedReceiptBlobs{upstream: blobs}
	materialization, err := attachedworkeroutput.Materialize(
		ctx, planned, loaded, request.Authorization.LeaseID,
		request.Authorization.LeaseGeneration, request.Candidate, prepareAt,
	)
	if err != nil {
		return result, err
	}
	digest, err := attachedWorkerTerminalMaterializationDigest(request.Candidate.Status, materialization)
	if err != nil {
		return result, err
	}
	materialization.EvidenceDigest = digest
	record := AttachedWorkerOutputReceiptV1{
		Version: 1, CopyInProgress: true, Binding: receiptBindingWithoutBearer(request.Authorization),
		Nonce: request.Nonce, CandidateFingerprint: fingerprint,
		ObservationDigest: observationDigest, Observation: request.Observation,
		CredentialRequired: credentialRequired, HarnessBindingDigest: bindingDigest,
		Status: request.Candidate.Status, CanonicalDigest: digest,
		Materialization: materialization, CreatedAt: prepareAt,
	}
	for _, artifact := range request.Candidate.Artifacts {
		record.SourceRefs = append(record.SourceRefs, artifact.Source)
	}
	if found && !sameOutputReceiptPlan(existing, record) {
		return AttachedWorkerOutputReceiptResult{Status: ports.AttachedWorkerExecutionConflict}, nil
	}
	found, existing = false, AttachedWorkerOutputReceiptV1{}
	var ownsCopy bool
	status, err = store.outputReceiptHeadTransaction(ctx, request, fingerprint, observationDigest, &record, &existing, &found, nil, &ownsCopy)
	if err != nil {
		return result, err
	}
	if found && existing.Ready {
		return AttachedWorkerOutputReceiptResult{Status: status, Receipt: existing}, nil
	}
	if status != ports.AttachedWorkerExecutionApplied {
		return AttachedWorkerOutputReceiptResult{Status: status}, nil
	}
	if !ownsCopy {
		return result, ErrAttachedWorkerReceiptCopyInProgress
	}
	if err := planned.Commit(ctx); err != nil {
		if errors.Is(err, ErrAttachedWorkerReceiptPutNotDispatched) {
			return result, errors.Join(err, store.releaseAttachedWorkerReceiptCopy(ctx, record))
		}
		return result, err
	}
	result, err = store.finishAttachedWorkerOutputReceipt(ctx, request, record)
	if err != nil || result.Status != ports.AttachedWorkerExecutionApplied && result.Status != ports.AttachedWorkerExecutionReplayed {
		return result, errors.Join(err, store.releaseAttachedWorkerReceiptCopy(ctx, record))
	}
	return result, nil
}

func (store *Store) outputReceiptHeadTransaction(
	ctx context.Context,
	request AttachedWorkerOutputReceiptRequest,
	fingerprint, observationDigest string,
	prepared *AttachedWorkerOutputReceiptV1,
	existing *AttachedWorkerOutputReceiptV1,
	found *bool,
	prepareAt *time.Time,
	ownsCopy *bool,
) (status ports.AttachedWorkerExecutionStatus, err error) {
	status = ports.AttachedWorkerExecutionDenied
	auth := request.Authorization
	err = store.Transact(ctx, auth.TenantID, func(state ports.StateTx) error {
		*existing, *found = AttachedWorkerOutputReceiptV1{}, false
		if ownsCopy != nil {
			*ownsCopy = false
		}
		tx := state.(*stateTx)
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
			!attachedWorkerOutputReceiptHeadCurrent(auth, at, worker, connection, attempt) {
			status = ports.AttachedWorkerExecutionFenced
			return nil
		}
		_, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, worker, connection)
		if err != nil {
			return err
		}
		if snapshot.Manifest == nil || !attachedWorkerManifestHasFeature(*snapshot.Manifest, attachedworkerprotocol.FeatureOutputReceipt) ||
			attempt.CapabilityDigest != connection.CapabilityDigest {
			status = ports.AttachedWorkerExecutionDenied
			return nil
		}
		job, jobFound, err := readJSON[domain.WorkerJob](ctx, tx.sqlTx,
			`SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, tx.tenantID, auth.RunID)
		if err != nil {
			return err
		}
		if !jobFound {
			status = ports.AttachedWorkerExecutionFenced
			return nil
		}
		bindingDigest, err := job.HarnessBinding.Digest()
		if err != nil {
			status = ports.AttachedWorkerExecutionFenced
			return nil
		}
		if _, deleting, err := readSessionDeletionTx(ctx, tx, job.SessionID); err != nil {
			return err
		} else if deleting {
			status = ports.AttachedWorkerExecutionFenced
			return nil
		}
		prior, priorFound, err := readAttachedWorkerOutputReceiptTx(ctx, tx, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration)
		if err != nil {
			return err
		}
		if priorFound {
			if prior.Nonce != request.Nonce || prior.CandidateFingerprint != fingerprint ||
				prior.ObservationDigest != observationDigest || prior.Status != request.Candidate.Status ||
				prior.HarnessBindingDigest != bindingDigest ||
				!sameReceiptStableBinding(prior.Binding, auth) {
				status = ports.AttachedWorkerExecutionConflict
				return nil
			}
			if !attachedWorkerOutputReceiptStatusAdmissible(attempt, prior.Status, at) &&
				!((attempt.State == domain.AttachedWorkerAttemptTerminalPending ||
					attempt.State == domain.AttachedWorkerAttemptTerminalCommitted) &&
					attempt.TerminalStatus == prior.Status && attempt.TerminalEvidenceDigest == prior.CanonicalDigest) {
				status = ports.AttachedWorkerExecutionFenced
				return nil
			}
			if !prior.Ready && !attachedWorkerOutputReceiptAuthorized(auth, request.Candidate.Status, at, worker, connection, attempt) {
				status = ports.AttachedWorkerExecutionFenced
				return nil
			}
			if prepared != nil && !prior.Ready && !prior.CopyInProgress {
				if !sameOutputReceiptPlan(prior, *prepared) {
					status = ports.AttachedWorkerExecutionConflict
					return nil
				}
				prior.CopyInProgress = true
				encoded, err := json.Marshal(prior)
				if err != nil {
					return err
				}
				if _, err := tx.sqlTx.ExecContext(ctx,
					`UPDATE attached_worker_output_receipts SET payload=CAST($7 AS JsonDocument)
					 WHERE tenant_id=$1 AND run_id=$2 AND owner_user_id=$3 AND worker_id=$4 AND attempt_id=$5 AND lease_generation=$6`,
					auth.TenantID, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration, string(encoded)); err != nil {
					return err
				}
				if ownsCopy != nil {
					*ownsCopy = true
				}
			}
			*existing, *found, status = prior, true, ports.AttachedWorkerExecutionApplied
			if prior.Ready {
				status = ports.AttachedWorkerExecutionReplayed
			}
			return nil
		}
		if !attachedWorkerOutputReceiptAuthorized(auth, request.Candidate.Status, at, worker, connection, attempt) {
			status = ports.AttachedWorkerExecutionFenced
			return nil
		}
		if job.AttemptID != auth.AttemptID || job.ReservationID != attempt.ReservationID ||
			job.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker ||
			job.ExecutionPlacementV2.OwnerUserID != auth.OwnerUserID ||
			job.ExecutionPlacementV2.WorkerID != auth.WorkerID ||
			job.ExecutionPlacementV2.CapabilityDigest != auth.CapabilityDigest ||
			job.ExecutionPlacementV2.PolicyDigest != auth.PolicyDigest ||
			request.Observation.ValidateFor(request.Candidate.Status,
				job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1) != nil {
			status = ports.AttachedWorkerExecutionFenced
			return nil
		}
		if prepared == nil {
			if prepareAt != nil {
				*prepareAt = at
			}
			status = ports.AttachedWorkerExecutionApplied
			return nil
		}
		if prepared.CreatedAt.After(at) || prepared.Status != request.Candidate.Status ||
			prepared.Nonce != request.Nonce || prepared.CandidateFingerprint != fingerprint ||
			prepared.ObservationDigest != observationDigest || prepared.HarnessBindingDigest != bindingDigest ||
			!sameReceiptBinding(prepared.Binding, auth) {
			return ErrAttachedWorkerAttemptConflict
		}
		if err := insertAttachedWorkerOutputReceiptTx(ctx, tx, *prepared); err != nil {
			return err
		}
		if ownsCopy != nil {
			*ownsCopy = true
		}
		status = ports.AttachedWorkerExecutionApplied
		return nil
	})
	return status, err
}

func sameOutputReceiptPlan(left, right AttachedWorkerOutputReceiptV1) bool {
	left.Ready, right.Ready = false, false
	left.CopyInProgress, right.CopyInProgress = false, false
	leftEncoded, leftErr := json.Marshal(left)
	rightEncoded, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftEncoded) == string(rightEncoded)
}

func (store *Store) finishAttachedWorkerOutputReceipt(ctx context.Context,
	request AttachedWorkerOutputReceiptRequest, planned AttachedWorkerOutputReceiptV1,
) (result AttachedWorkerOutputReceiptResult, err error) {
	auth := request.Authorization
	err = store.Transact(ctx, auth.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
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
			!attachedWorkerOutputReceiptHeadCurrent(auth, at, worker, connection, attempt) {
			result.Status = ports.AttachedWorkerExecutionFenced
			return nil
		}
		job, jobFound, err := readJSON[domain.WorkerJob](ctx, tx.sqlTx,
			`SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, tx.tenantID, auth.RunID)
		if err != nil {
			return err
		}
		if !jobFound {
			result.Status = ports.AttachedWorkerExecutionFenced
			return nil
		}
		bindingDigest, err := job.HarnessBinding.Digest()
		if err != nil || bindingDigest != planned.HarnessBindingDigest {
			result.Status = ports.AttachedWorkerExecutionFenced
			return nil
		}
		if _, deleting, err := readSessionDeletionTx(ctx, tx, job.SessionID); err != nil {
			return err
		} else if deleting {
			result.Status = ports.AttachedWorkerExecutionFenced
			return nil
		}
		prior, found, err := readAttachedWorkerOutputReceiptTx(ctx, tx, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration)
		if err != nil {
			return err
		}
		if !found || !sameOutputReceiptPlan(prior, planned) || !prior.CopyInProgress {
			result.Status = ports.AttachedWorkerExecutionConflict
			return nil
		}
		if prior.Ready {
			result = AttachedWorkerOutputReceiptResult{Status: ports.AttachedWorkerExecutionReplayed, Receipt: prior}
			return nil
		}
		if !attachedWorkerOutputReceiptAuthorized(auth, planned.Status, at, worker, connection, attempt) {
			result.Status = ports.AttachedWorkerExecutionFenced
			return nil
		}
		planned.Ready = true
		planned.CopyInProgress = false
		encoded, err := json.Marshal(planned)
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
		result = AttachedWorkerOutputReceiptResult{Status: ports.AttachedWorkerExecutionApplied, Receipt: planned}
		return nil
	})
	return result, err
}

// Only a caller that proved no remote write was dispatched, or verified every
// planned object before a YDB finish error, may release copy ownership. A
// crash or ambiguous Put error retains CopyInProgress until an explicit
// quiescence protocol proves no remote writer can complete.
func (store *Store) releaseAttachedWorkerReceiptCopy(ctx context.Context, planned AttachedWorkerOutputReceiptV1) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	auth := planned.Binding
	return store.Transact(cleanupCtx, auth.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		prior, found, err := readAttachedWorkerOutputReceiptTx(cleanupCtx, tx, auth.RunID, auth.OwnerUserID,
			auth.WorkerID, auth.AttemptID, auth.LeaseGeneration)
		if err != nil || !found || prior.Ready || !prior.CopyInProgress {
			return err
		}
		if !sameOutputReceiptPlan(prior, planned) {
			return ErrAttachedWorkerAttemptConflict
		}
		prior.CopyInProgress = false
		encoded, err := json.Marshal(prior)
		if err != nil {
			return err
		}
		_, err = tx.sqlTx.ExecContext(cleanupCtx,
			`UPDATE attached_worker_output_receipts SET payload=CAST($7 AS JsonDocument)
			 WHERE tenant_id=$1 AND run_id=$2 AND owner_user_id=$3 AND worker_id=$4 AND attempt_id=$5 AND lease_generation=$6`,
			auth.TenantID, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration, string(encoded))
		return err
	})
}

func readAttachedWorkerOutputReceiptTx(ctx context.Context, tx *stateTx, run domain.RunID, owner domain.UserID,
	worker domain.AttachedWorkerID, attempt domain.AttemptID, generation uint64,
) (record AttachedWorkerOutputReceiptV1, found bool, err error) {
	var payload string
	err = tx.sqlTx.QueryRowContext(ctx,
		`SELECT payload FROM attached_worker_output_receipts
		 WHERE tenant_id=$1 AND run_id=$2 AND owner_user_id=$3 AND worker_id=$4 AND attempt_id=$5 AND lease_generation=$6`,
		tx.tenantID, run, owner, worker, attempt, generation,
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return record, false, nil
	}
	if err != nil {
		return record, false, err
	}
	if err := json.Unmarshal([]byte(payload), &record); err != nil {
		return record, false, err
	}
	if err := validateAttachedWorkerReceiptRecord(record, tx.tenantID, run, owner, worker, attempt, generation); err != nil {
		return AttachedWorkerOutputReceiptV1{}, false, err
	}
	return record, true, nil
}

func validateAttachedWorkerReceiptRecord(record AttachedWorkerOutputReceiptV1, tenant domain.TenantID, run domain.RunID,
	owner domain.UserID, worker domain.AttachedWorkerID, attempt domain.AttemptID, generation uint64,
) error {
	if record.Version != 1 || record.Ready && record.CopyInProgress ||
		record.Binding.TenantID != tenant || record.Binding.RunID != run || record.Binding.OwnerUserID != owner ||
		record.Binding.WorkerID != worker || record.Binding.AttemptID != attempt ||
		record.Binding.LeaseGeneration != generation || record.Binding.PresentedSecretDigest != "" ||
		record.Nonce.Validate() != nil || record.CanonicalDigest.Validate() != nil ||
		record.HarnessBindingDigest.Validate() != nil ||
		record.Observation.ValidateFor(record.Status, record.CredentialRequired) != nil {
		return ErrAttachedWorkerAttemptConflict
	}
	observationDigest, err := record.Observation.Digest()
	if err != nil || observationDigest != record.ObservationDigest || len(record.CandidateFingerprint) != 64 {
		return ErrAttachedWorkerAttemptConflict
	}
	for _, ref := range record.SourceRefs {
		if ref.Validate() != nil || ref.TenantID != tenant {
			return ErrAttachedWorkerAttemptConflict
		}
	}
	digest, err := attachedWorkerTerminalMaterializationDigest(record.Status, record.Materialization)
	if err != nil || digest != record.CanonicalDigest || record.Materialization.EvidenceDigest != digest {
		return ErrAttachedWorkerAttemptConflict
	}
	return nil
}

func (store *Store) listRunOutputReceipts(ctx context.Context, tenant domain.TenantID,
	run domain.RunID, limit uint64,
) ([]AttachedWorkerOutputReceiptV1, error) {
	rows, err := store.db.QueryContext(ctx,
		`SELECT owner_user_id,worker_id,attempt_id,lease_generation,payload
		 FROM attached_worker_output_receipts WHERE tenant_id=$1 AND run_id=$2
		 ORDER BY owner_user_id,worker_id,attempt_id,lease_generation LIMIT $3`, tenant, run, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var receipts []AttachedWorkerOutputReceiptV1
	for rows.Next() {
		var owner domain.UserID
		var worker domain.AttachedWorkerID
		var attempt domain.AttemptID
		var generation uint64
		var payload string
		if err := rows.Scan(&owner, &worker, &attempt, &generation, &payload); err != nil {
			return nil, err
		}
		var record AttachedWorkerOutputReceiptV1
		if err := json.Unmarshal([]byte(payload), &record); err != nil {
			return nil, err
		}
		if err := validateAttachedWorkerReceiptRecord(record, tenant, run, owner, worker, attempt, generation); err != nil {
			return nil, err
		}
		receipts = append(receipts, record)
	}
	return receipts, rows.Err()
}

func attachedWorkerReceiptObjectRefs(record AttachedWorkerOutputReceiptV1) []domain.BlobRef {
	refs := append([]domain.BlobRef(nil), record.SourceRefs...)
	if record.Materialization.Completion != nil {
		for _, artifact := range record.Materialization.Completion.Manifest.Artifacts {
			refs = append(refs, artifact.Blob)
		}
		for _, event := range record.Materialization.Completion.Events {
			refs = append(refs, event.Payload)
		}
	} else if record.Materialization.Failure != nil {
		for _, event := range record.Materialization.Failure.Events {
			refs = append(refs, event.Payload)
		}
	}
	return refs
}

func insertAttachedWorkerOutputReceiptTx(ctx context.Context, tx *stateTx, record AttachedWorkerOutputReceiptV1) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO attached_worker_output_receipts
		 (tenant_id,run_id,owner_user_id,worker_id,attempt_id,lease_generation,canonical_digest,
		  candidate_fingerprint,created_at,payload)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,CAST($10 AS JsonDocument))`,
		record.Binding.TenantID, record.Binding.RunID, record.Binding.OwnerUserID, record.Binding.WorkerID,
		record.Binding.AttemptID, record.Binding.LeaseGeneration, record.CanonicalDigest,
		record.CandidateFingerprint, record.CreatedAt, string(encoded),
	)
	return err
}

func receiptBindingWithoutBearer(value ports.AttachedWorkerSealedInputAuthorization) ports.AttachedWorkerSealedInputAuthorization {
	value.PresentedSecretDigest = ""
	value.ExpectedAttemptRevision = 0
	return value
}

func sameReceiptBinding(left, right ports.AttachedWorkerSealedInputAuthorization) bool {
	return left == receiptBindingWithoutBearer(right)
}

func sameReceiptStableBinding(left, right ports.AttachedWorkerSealedInputAuthorization) bool {
	return left.TenantID == right.TenantID && left.OwnerUserID == right.OwnerUserID &&
		left.WorkerID == right.WorkerID && left.EnrollmentGeneration == right.EnrollmentGeneration &&
		left.RunID == right.RunID && left.AttemptID == right.AttemptID &&
		left.AttemptSequence == right.AttemptSequence && left.LeaseID == right.LeaseID &&
		left.LeaseGeneration == right.LeaseGeneration && left.FenceToken == right.FenceToken &&
		left.LeaseExpiresAtUnixMicro == right.LeaseExpiresAtUnixMicro &&
		left.ContextDigest == right.ContextDigest && left.CapabilityDigest == right.CapabilityDigest &&
		left.PolicyDigest == right.PolicyDigest
}

func attachedWorkerOutputReceiptHeadCurrent(auth ports.AttachedWorkerSealedInputAuthorization, at time.Time,
	worker domain.AttachedWorker, connection domain.AttachedWorkerConnection, attempt domain.AttachedWorkerAttemptV1,
) bool {
	poll := ports.AttachedWorkerAttemptPoll{
		TenantID: auth.TenantID, OwnerUserID: auth.OwnerUserID, WorkerID: auth.WorkerID,
		ConnectionID: auth.ConnectionID, PresentedSecretDigest: auth.PresentedSecretDigest,
	}
	return attachedWorkerAttemptPollAuthorized(poll, at, worker, connection) &&
		attachedWorkerTerminalCommitAuthorityCurrent(worker, connection, attempt) &&
		auth.AttemptSequence == 1 && worker.EnrollmentGeneration == auth.EnrollmentGeneration &&
		connection.ConnectionGeneration == auth.ConnectionGeneration &&
		attempt.TenantID == auth.TenantID && attempt.OwnerUserID == auth.OwnerUserID &&
		attempt.WorkerID == auth.WorkerID && attempt.RunID == auth.RunID &&
		attempt.AttemptID == auth.AttemptID && attempt.ConnectionID == auth.ConnectionID &&
		attempt.EnrollmentGeneration == auth.EnrollmentGeneration &&
		attempt.ConnectionGeneration == auth.ConnectionGeneration &&
		attempt.LeaseID == auth.LeaseID && attempt.LeaseGeneration == auth.LeaseGeneration &&
		attempt.FenceToken == auth.FenceToken &&
		attempt.LeaseExpiresAt.UnixMicro() == auth.LeaseExpiresAtUnixMicro &&
		attempt.ContextDigest == auth.ContextDigest && attempt.CapabilityDigest == auth.CapabilityDigest &&
		attempt.PolicyDigest == auth.PolicyDigest &&
		(auth.ExpectedAttemptRevision == 0 || attempt.Revision == auth.ExpectedAttemptRevision)
}

func attachedWorkerManifestHasFeature(manifest attachedworkerprotocol.CapabilityManifestV1, feature attachedworkerprotocol.ProtocolFeatureV1) bool {
	for _, offered := range manifest.Features {
		if offered == feature {
			return true
		}
	}
	return false
}

// resolveAttachedWorkerTerminalMaterializationTx selects exactly one authority
// for terminal output. A receipt-capable attempt cannot fall back to caller
// materialization, including when an ACK is replayed after retirement.
func resolveAttachedWorkerTerminalMaterializationTx(ctx context.Context, tx *stateTx,
	attempt domain.AttachedWorkerAttemptV1, supplied ports.AttachedWorkerTerminalMaterialization,
) (ports.AttachedWorkerTerminalMaterialization, error) {
	manifestRecord, found, err := readAttachedWorkerManifestTx(ctx, tx, attempt.OwnerUserID, attempt.WorkerID, attempt.CapabilityDigest)
	if err != nil || !found {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrAttachedWorkerAttemptConflict
	}
	var manifest attachedworkerprotocol.CapabilityManifestV1
	if err := json.Unmarshal(manifestRecord.ManifestPayload, &manifest); err != nil {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrAttachedWorkerAttemptConflict
	}
	digest, err := attachedworkerprotocol.ManifestDigestV1(manifest)
	if err != nil || hex.EncodeToString(digest) != string(attempt.CapabilityDigest) {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrAttachedWorkerAttemptConflict
	}
	if !attachedWorkerManifestHasFeature(manifest, attachedworkerprotocol.FeatureOutputReceipt) {
		if (supplied.Completion == nil) == (supplied.Failure == nil) {
			return ports.AttachedWorkerTerminalMaterialization{}, ErrAttachedWorkerAttemptConflict
		}
		return supplied, nil
	}
	if supplied.Completion != nil || supplied.Failure != nil {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrAttachedWorkerAttemptConflict
	}
	receipt, found, err := readAttachedWorkerOutputReceiptTx(ctx, tx, attempt.RunID, attempt.OwnerUserID, attempt.WorkerID, attempt.AttemptID, attempt.LeaseGeneration)
	if err != nil || !found || !receipt.Ready {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrAttachedWorkerAttemptConflict
	}
	binding := receipt.Binding
	if binding.TenantID != attempt.TenantID || binding.OwnerUserID != attempt.OwnerUserID ||
		binding.WorkerID != attempt.WorkerID || binding.RunID != attempt.RunID ||
		binding.AttemptID != attempt.AttemptID || binding.LeaseID != attempt.LeaseID ||
		binding.LeaseGeneration != attempt.LeaseGeneration || binding.FenceToken != attempt.FenceToken ||
		binding.ContextDigest != attempt.ContextDigest || binding.CapabilityDigest != attempt.CapabilityDigest ||
		binding.PolicyDigest != attempt.PolicyDigest || receipt.Status != attempt.TerminalStatus ||
		receipt.CanonicalDigest != attempt.TerminalEvidenceDigest || supplied.EvidenceDigest != receipt.CanonicalDigest {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrAttachedWorkerAttemptConflict
	}
	if attempt.State == domain.AttachedWorkerAttemptTerminalPending {
		job, found, err := readJSON[domain.WorkerJob](ctx, tx.sqlTx,
			`SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, tx.tenantID, attempt.RunID)
		if err != nil || !found {
			return ports.AttachedWorkerTerminalMaterialization{}, ErrAttachedWorkerAttemptConflict
		}
		bindingDigest, digestErr := job.HarnessBinding.Digest()
		if job.AttemptID != attempt.AttemptID || job.ReservationID != attempt.ReservationID ||
			job.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker ||
			job.ExecutionPlacementV2.OwnerUserID != attempt.OwnerUserID || job.ExecutionPlacementV2.WorkerID != attempt.WorkerID ||
			job.ExecutionPlacementV2.CapabilityDigest != attempt.CapabilityDigest || job.ExecutionPlacementV2.PolicyDigest != attempt.PolicyDigest ||
			digestErr != nil || bindingDigest != receipt.HarnessBindingDigest ||
			receipt.CredentialRequired != (job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1) {
			return ports.AttachedWorkerTerminalMaterialization{}, ErrAttachedWorkerAttemptConflict
		}
	}
	if err := receipt.Observation.ValidateFor(receipt.Status, receipt.CredentialRequired); err != nil {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrAttachedWorkerAttemptConflict
	}
	return receipt.Materialization, nil
}
