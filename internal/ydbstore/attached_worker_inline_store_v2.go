package ydbstore

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// No caller obtains write permission from these wire values alone. Each
// transition joins the existing membership/connection/attempt/deletion owner
// at its authoritative transaction time before the next irreversible effect.
func (store *Store) PrepareAttachedWorkerInlineOutput(ctx context.Context, blobs ports.BlobStore, request attachedworkeroutput.AttachedWorkerInlinePrepareV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	if store == nil || blobs == nil || ctx == nil || ctx.Err() != nil || request.Version != 2 ||
		validateAttachedWorkerSealedInputAuthorization(request.Authorization) != nil || request.Nonce.Validate() != nil || request.Candidate.Validate() != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, attachedworkeroutput.ErrCandidateInvalid
	}
	ctx, cancel := boundedInlineExportContextV2(ctx, request.ExpiresAt)
	defer cancel()
	observation, err := request.Observation.Digest()
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	fingerprint, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(request)
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	r := AttachedWorkerOutputReceiptV2{Version: 2, Stage: attachedworkeroutput.InlinePreparedV2, Binding: receiptBindingWithoutBearer(request.Authorization),
		Nonce: request.Nonce, CandidateFingerprint: fingerprint, Candidate: request.Candidate, Producer: request.Producer,
		Observation: request.Observation, ObservationDigest: observation, CredentialRequired: request.Observation.CredentialReleaseRequired, ExpiresAt: request.ExpiresAt}
	err = store.Transact(ctx, r.Binding.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		var at time.Time
		_, at, err = store.inlineOutputAuthorityTxV2(ctx, tx, request.Authorization, r, false)
		if err != nil {
			return err
		}
		_, found, err := readAttachedWorkerReceiptEnvelopeTx(ctx, tx, r.Binding.RunID, r.Binding.OwnerUserID, r.Binding.WorkerID, r.Binding.AttemptID, r.Binding.LeaseGeneration)
		if err != nil {
			return err
		}
		// Prepare is physically one-shot. A lost response uses read-only
		// reconciliation; repeating this mutation cannot reacquire body authority.
		if found {
			return ErrAttachedWorkerAttemptConflict
		}
		r.CreatedAt = at
		if (attachedWorkerReceiptEnvelope{v2: &r}).validateIdentity(r.Binding.TenantID, r.Binding.RunID, r.Binding.OwnerUserID, r.Binding.WorkerID, r.Binding.AttemptID, r.Binding.LeaseGeneration) != nil {
			return ErrAttachedWorkerAttemptConflict
		}
		return writeInlineReceiptTxV2(ctx, tx, r, true)
	})
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	if len(r.Candidate.Artifacts) == 0 {
		// No empty-body POST, no source object and no special alternate
		// publisher: answer-only enters the same claim/copy/READY path.
		body := attachedworkeroutput.AttachedWorkerInlineBodyV2{Version: 2, Authorization: request.Authorization, Nonce: r.Nonce, Fingerprint: r.CandidateFingerprint,
			Capability: inlineOutputCapabilityV2(request.Authorization, r), ExpiresAt: r.ExpiresAt}
		return store.submitInlineOutputV2(ctx, blobs, body, 0, nil, true)
	}
	return inlineOutputResultV2(r, inlineOutputCapabilityV2(request.Authorization, r)), nil
}

func (store *Store) SubmitAttachedWorkerInlineOutput(ctx context.Context, blobs ports.BlobStore, request attachedworkeroutput.AttachedWorkerInlineBodyV2, length int64, body io.Reader) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	return store.submitInlineOutputV2(ctx, blobs, request, length, body, false)
}

func (store *Store) submitInlineOutputV2(ctx context.Context, blobs ports.BlobStore, request attachedworkeroutput.AttachedWorkerInlineBodyV2, length int64, body io.Reader, answerOnly bool) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	if store == nil || blobs == nil || ctx == nil || ctx.Err() != nil || request.Version != 2 ||
		validateAttachedWorkerSealedInputAuthorization(request.Authorization) != nil || length < 0 || length > attachedworkeroutput.MaxInlineTotalBytesV2 || (!answerOnly && (length == 0 || body == nil)) {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, attachedworkeroutput.ErrCandidateInvalid
	}
	ctx, cancelExport := boundedInlineExportContextV2(ctx, request.ExpiresAt)
	defer cancelExport()
	claim, err := newInlineWriteIdentityV2()
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	var r AttachedWorkerOutputReceiptV2
	var loaded ports.WorkerJobState
	err = store.Transact(ctx, request.Authorization.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		record, found, err := readAttachedWorkerReceiptEnvelopeTx(ctx, tx, request.Authorization.RunID, request.Authorization.OwnerUserID, request.Authorization.WorkerID, request.Authorization.AttemptID, request.Authorization.LeaseGeneration)
		if err != nil {
			return err
		}
		if !found || record.v2 == nil {
			return ErrAttachedWorkerAttemptConflict
		}
		r = *record.v2
		if !inlineBodyMatchesV2(request, r) || r.Stage != attachedworkeroutput.InlinePreparedV2 || !hmac.Equal([]byte(request.Capability), []byte(inlineOutputCapabilityV2(request.Authorization, r))) {
			return ErrAttachedWorkerAttemptConflict
		}
		var expected uint64
		for _, artifact := range r.Candidate.Artifacts {
			expected += artifact.Size
		}
		if int64(expected) != length || (len(r.Candidate.Artifacts) == 0) != answerOnly {
			return ErrAttachedWorkerAttemptConflict
		}
		loaded, _, err = store.inlineOutputAuthorityTxV2(ctx, tx, request.Authorization, r, false)
		if err != nil {
			return err
		}
		r.Stage, r.BodyClaimID = attachedworkeroutput.InlineBodyClaimedV2, claim
		return writeInlineReceiptTxV2(ctx, tx, r, false)
	})
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	// Do not read a single byte before the durable one-shot identity is owned.
	// Failure/ambiguity after this point never frees or replaces that identity.
	bounded, cancel := context.WithDeadline(ctx, r.ExpiresAt)
	defer cancel()
	var content []byte
	if !answerOnly {
		content, err = readExactReceiptBytes(bounded, body, length, attachedworkeroutput.MaxInlineTotalBytesV2)
		defer func() { clear(content) }()
		if err != nil || bounded.Err() != nil {
			return attachedworkeroutput.AttachedWorkerInlineResultV2{}, attachedworkeroutput.ErrCandidateInvalid
		}
	}
	verified, err := attachedworkeroutput.VerifyInlineV2(r.Candidate, content)
	clear(content)
	content = nil
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	defer verified.Clear()
	planned := &plannedReceiptBlobs{upstream: blobs, maxBytes: 320 << 10}
	materialization, err := attachedworkeroutput.MaterializeInlineV2(bounded, planned, loaded, r.Binding.LeaseID, r.Binding.LeaseGeneration, verified, r.CreatedAt)
	// This owner was created from the received server body, not the private
	// native result retained through TerminalAck. Materialization has copied its
	// bytes into the canonical plan; no transition needs this temporary copy.
	verified.Clear()
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	// The fixed cohort is at most 64KiB artifact bytes and a bounded escaped
	// answer event. This cap is independent of V1's much larger source cohort.
	if planned.total > 320<<10 {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, attachedworkeroutput.ErrCandidateInvalid
	}
	digest, err := attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, materialization)
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	materialization.EvidenceDigest = digest
	copyOwner, err := newInlineWriteIdentityV2()
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	prepared := r
	prepared.Stage, prepared.CopyOwnerID = attachedworkeroutput.InlineCanonicalCopyingV2, copyOwner
	prepared.Materialization, prepared.CanonicalDigest = materialization, digest
	err = store.transitionInlineReceiptV2(bounded, request.Authorization, r, prepared)
	if err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	// All canonical refs and the sole physical writer are durable now. Never
	// retry Commit or clear custody from a Put error/expiry/object absence.
	if err = planned.Commit(bounded); err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	planned.clearBodies()
	ready := prepared
	ready.Stage = attachedworkeroutput.InlineReadyV2
	if err = store.transitionInlineReceiptV2(bounded, request.Authorization, prepared, ready); err != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, err
	}
	return inlineOutputResultV2(ready, ""), nil
}

func (store *Store) ReconcileAttachedWorkerInlineOutput(ctx context.Context, request attachedworkeroutput.AttachedWorkerInlineReconcileV2) (attachedworkeroutput.AttachedWorkerInlineResultV2, error) {
	if store == nil || ctx == nil || ctx.Err() != nil || request.Version != 2 || validateAttachedWorkerSealedInputAuthorization(request.Authorization) != nil {
		return attachedworkeroutput.AttachedWorkerInlineResultV2{}, attachedworkeroutput.ErrCandidateInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var result attachedworkeroutput.AttachedWorkerInlineResultV2
	err := store.Transact(ctx, request.Authorization.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		envelope, found, err := readAttachedWorkerReceiptEnvelopeTx(ctx, tx, request.Authorization.RunID, request.Authorization.OwnerUserID, request.Authorization.WorkerID, request.Authorization.AttemptID, request.Authorization.LeaseGeneration)
		if err != nil {
			return err
		}
		if !found || envelope.v2 == nil {
			return ErrAttachedWorkerAttemptConflict
		}
		r := *envelope.v2
		if r.Nonce != request.Nonce || r.CandidateFingerprint != request.Fingerprint || !sameReceiptBinding(r.Binding, request.Authorization) {
			return ErrAttachedWorkerAttemptConflict
		}
		_, _, err = store.inlineOutputAuthorityTxV2(ctx, tx, request.Authorization, r, r.Stage == attachedworkeroutput.InlineReadyV2)
		if err != nil {
			return err
		}
		capability := ""
		if r.Stage == attachedworkeroutput.InlinePreparedV2 && !request.BodySendBurned && len(r.Candidate.Artifacts) > 0 {
			capability = inlineOutputCapabilityV2(request.Authorization, r)
		}
		result = inlineOutputResultV2(r, capability)
		return nil
	})
	return result, err
}

// Local time is a failure bound only; permission uses the transaction clock.
// Neither entrypoint can extend the original expiry or an earlier parent.
func boundedInlineExportContextV2(parent context.Context, expires time.Time) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(15 * time.Second)
	if expires.Before(deadline) {
		deadline = expires
	}
	return context.WithDeadline(parent, deadline)
}

func (store *Store) transitionInlineReceiptV2(ctx context.Context, auth ports.AttachedWorkerSealedInputAuthorization, prior, next AttachedWorkerOutputReceiptV2) error {
	return store.Transact(ctx, auth.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		r, found, err := readAttachedWorkerReceiptEnvelopeTx(ctx, tx, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration)
		if err != nil {
			return err
		}
		if !found || r.v2 == nil || !sameInlineReceiptV2(*r.v2, prior) ||
			!(prior.Stage == attachedworkeroutput.InlineBodyClaimedV2 && next.Stage == attachedworkeroutput.InlineCanonicalCopyingV2 || prior.Stage == attachedworkeroutput.InlineCanonicalCopyingV2 && next.Stage == attachedworkeroutput.InlineReadyV2) {
			return ErrAttachedWorkerAttemptConflict
		}
		if _, _, err = store.inlineOutputAuthorityTxV2(ctx, tx, auth, prior, false); err != nil {
			return err
		}
		if next.CandidateFingerprint != prior.CandidateFingerprint || next.BodyClaimID != prior.BodyClaimID ||
			(attachedWorkerReceiptEnvelope{v2: &next}).validateIdentity(auth.TenantID, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration) != nil {
			return ErrAttachedWorkerAttemptConflict
		}
		return writeInlineReceiptTxV2(ctx, tx, next, false)
	})
}

func inlineBodyMatchesV2(request attachedworkeroutput.AttachedWorkerInlineBodyV2, r AttachedWorkerOutputReceiptV2) bool {
	return request.Nonce == r.Nonce && request.Fingerprint == r.CandidateFingerprint && request.ExpiresAt.Equal(r.ExpiresAt) && sameReceiptBinding(r.Binding, request.Authorization)
}

func inlineOutputResultV2(r AttachedWorkerOutputReceiptV2, capability string) attachedworkeroutput.AttachedWorkerInlineResultV2 {
	return attachedworkeroutput.AttachedWorkerInlineResultV2{Version: 2, Stage: r.Stage, Nonce: r.Nonce, Fingerprint: r.CandidateFingerprint, Capability: capability, ExpiresAt: r.ExpiresAt, CanonicalDigest: r.CanonicalDigest}
}

func inlineOutputCapabilityV2(auth ports.AttachedWorkerSealedInputAuthorization, r AttachedWorkerOutputReceiptV2) string {
	mac := hmac.New(sha256.New, []byte(auth.PresentedSecretDigest))
	_, _ = mac.Write([]byte("sessionless:attached-worker-inline-capability:v2\x00" + r.CandidateFingerprint))
	return hex.EncodeToString(mac.Sum(nil))
}

func newInlineWriteIdentityV2() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func sameInlineReceiptV2(left, right AttachedWorkerOutputReceiptV2) bool {
	// These are validated V2 records. Compare their two large UTF-8 contents
	// directly before encoding metadata: two full JSON snapshots can exceed
	// the entire invocation payload budget for an admitted escaped answer.
	if left.Candidate.Summary != right.Candidate.Summary {
		return false
	}
	l, r := left.Materialization.Completion, right.Materialization.Completion
	if (l == nil) != (r == nil) {
		return false
	}
	if l != nil {
		if len(l.Events) != len(r.Events) {
			return false
		}
		for i := range l.Events {
			if l.Events[i].DisplayText != r.Events[i].DisplayText {
				return false
			}
		}
	}
	left = inlineReceiptComparisonMetadataV2(left)
	right = inlineReceiptComparisonMetadataV2(right)
	a, ae := json.Marshal(left)
	b, be := json.Marshal(right)
	return ae == nil && be == nil && string(a) == string(b)
}

// This local comparison projection is never persisted or used as authority.
// All other JSON identity semantics stay unchanged, including timestamps,
// nil/empty slices, ignored fields and future metadata. Clone before clearing
// contents so the original receipt and any shared aliases remain untouched.
func inlineReceiptComparisonMetadataV2(record AttachedWorkerOutputReceiptV2) AttachedWorkerOutputReceiptV2 {
	record.Candidate.Summary = ""
	if original := record.Materialization.Completion; original != nil {
		completion := *original
		if original.Events != nil {
			completion.Events = make([]domain.SessionEventDraft, len(original.Events))
			copy(completion.Events, original.Events)
			for i := range completion.Events {
				completion.Events[i].DisplayText = ""
			}
		}
		record.Materialization.Completion = &completion
	}
	return record
}

func writeInlineReceiptTxV2(ctx context.Context, tx *stateTx, r AttachedWorkerOutputReceiptV2, insert bool) error {
	stored, err := encodeInlineReceiptProjectionV2(r)
	if err != nil {
		return err
	}
	w := inlineReceiptSQLWriterV2{ctx: ctx, tx: tx, record: r, insert: insert}
	err = json.NewEncoder(&w).Encode(stored)
	if w.err != nil {
		return w.err
	}
	if err != nil || !w.called {
		return ErrAttachedWorkerAttemptConflict
	}
	return nil
}

// Encoder lends its complete encoding during Write, avoiding Marshal's second
// full payload copy. Exec must finish before Write returns. Neither this writer
// nor its caller may retain or modify p: it belongs to encoding/json's pool.
// Encoder still buffers before Write and keeps its pool; this is not a bound on
// aggregate memory, pooled capacity or zeroization. The cap below gates SQL.
type inlineReceiptSQLWriterV2 struct {
	ctx    context.Context
	tx     *stateTx
	record AttachedWorkerOutputReceiptV2
	insert bool
	called bool
	err    error
}

func (w *inlineReceiptSQLWriterV2) Write(p []byte) (int, error) {
	if w.called {
		w.err = ErrAttachedWorkerAttemptConflict
		return 0, w.err
	}
	w.called = true
	if len(p) == 0 || p[len(p)-1] != '\n' || len(p)-1 > maxInlineReceiptStoredBytesV2 {
		w.err = ErrAttachedWorkerAttemptConflict
		return 0, w.err
	}
	// Default Encoder adds exactly one LF. Keep Marshal's exact SQL bytes,
	// including every escaped interior newline and HTML-sensitive character.
	encoded := p[:len(p)-1]
	r := w.record
	a := r.Binding
	if w.insert {
		_, w.err = w.tx.sqlTx.ExecContext(w.ctx, `INSERT INTO attached_worker_output_receipts
 (tenant_id,run_id,owner_user_id,worker_id,attempt_id,lease_generation,canonical_digest,candidate_fingerprint,created_at,payload)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,CAST($10 AS JsonDocument))`, a.TenantID, a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration, r.CanonicalDigest, r.CandidateFingerprint, r.CreatedAt, encoded)
	} else {
		_, w.err = w.tx.sqlTx.ExecContext(w.ctx, `UPDATE attached_worker_output_receipts SET canonical_digest=$7,payload=CAST($8 AS JsonDocument)
 WHERE tenant_id=$1 AND run_id=$2 AND owner_user_id=$3 AND worker_id=$4 AND attempt_id=$5 AND lease_generation=$6`, a.TenantID, a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration, r.CanonicalDigest, encoded)
	}
	if w.err != nil {
		return 0, w.err
	}
	return len(p), nil
}

// Called only inside the original authorized deletion transaction. The same
// serializable receipt row proves no copy authority ever existed and fences
// every late body-reader promotion. Expiry/disconnect/object absence are not
// used as evidence. Canonical-copying or malformed rows cannot take this path.
func retireInlineReceiptBeforeWritesTxV2(ctx context.Context, tx *stateTx, r AttachedWorkerOutputReceiptV2) (attachedWorkerReceiptEnvelope, error) {
	if r.Stage != attachedworkeroutput.InlinePreparedV2 && r.Stage != attachedworkeroutput.InlineBodyClaimedV2 || !inlineReceiptWithoutCanonicalPlanV2(r) || r.CopyOwnerID != "" {
		return attachedWorkerReceiptEnvelope{}, ErrAttachedWorkerAttemptConflict
	}
	a := r.Binding
	if (attachedWorkerReceiptEnvelope{v2: &r}).validateIdentity(tx.tenantID, a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration) != nil {
		return attachedWorkerReceiptEnvelope{}, ErrAttachedWorkerAttemptConflict
	}
	r.Stage = attachedworkeroutput.InlineRetiredWithoutWritesV2
	if err := writeInlineReceiptTxV2(ctx, tx, r, false); err != nil {
		return attachedWorkerReceiptEnvelope{}, err
	}
	return attachedWorkerReceiptEnvelope{v2: &r}, nil
}

// This validates canonical server subscription eligibility, not instantaneous
// owner-local OAuth state. The original private native owner checks that state;
// no binding is synthesized from a subscription projection here.
func (store *Store) inlineOutputAuthorityTxV2(ctx context.Context, tx *stateTx, auth ports.AttachedWorkerSealedInputAuthorization, r AttachedWorkerOutputReceiptV2, readyReplay bool) (loaded ports.WorkerJobState, at time.Time, err error) {
	deny := func() (ports.WorkerJobState, time.Time, error) {
		return ports.WorkerJobState{}, time.Time{}, domain.ErrMembershipDenied
	}
	member, err := attachedWorkerOwnerAuthorizedTx(ctx, tx, auth.OwnerUserID)
	if err != nil {
		return loaded, at, err
	}
	if !member {
		return deny()
	}
	at, err = store.attachedWorkerTransactionTime(ctx, tx)
	if err != nil {
		return loaded, at, err
	}
	w, wf, err := readAttachedWorkerTx(ctx, tx, auth.OwnerUserID, auth.WorkerID)
	if err != nil {
		return loaded, at, err
	}
	c, cf, err := readAttachedWorkerConnectionTx(ctx, tx, auth.OwnerUserID, auth.WorkerID)
	if err != nil {
		return loaded, at, err
	}
	a, af, err := readAttachedWorkerAttemptTx(ctx, tx, auth.OwnerUserID, auth.WorkerID)
	if err != nil {
		return loaded, at, err
	}
	if !wf || !cf || !af || !attachedWorkerOutputReceiptHeadCurrent(auth, at, w, c, a) {
		return deny()
	}
	if !readyReplay && !attachedWorkerOutputReceiptAuthorized(auth, domain.AttachedWorkerTerminalSucceeded, at, w, c, a) {
		return deny()
	}
	if readyReplay && !inlineReadyAttemptMetadataV2(a, r.CanonicalDigest) {
		return deny()
	}
	_, snapshot, err := loadAttachedWorkerProtocolAuthorityTx(ctx, tx, w, c)
	if err != nil {
		return loaded, at, err
	}
	if snapshot.Manifest == nil || !attachedWorkerManifestHasFeature(*snapshot.Manifest, attachedworkerprotocol.FeatureOutputReceipt) || !attachedWorkerManifestHasFeature(*snapshot.Manifest, attachedworkerprotocol.FeatureInlineOutputV2) {
		return deny()
	}
	var found bool
	loaded, found, err = loadWorkerJobStateTx(ctx, tx, auth.RunID)
	if err != nil {
		return loaded, at, err
	}
	if !found {
		return deny()
	}
	if validateInlineLoadedJobPhaseV2(loaded, r, at, readyReplay) != nil {
		return deny()
	}
	job := loaded.Job
	placement := job.ExecutionPlacementV2
	digest, de := job.HarnessBinding.Digest()
	contextDigest, ce := domain.AttachedWorkerJobContextDigestV1(job, loaded.InputManifest)
	if de != nil || ce != nil || digest != r.Producer.HarnessBindingDigest || contextDigest != auth.ContextDigest ||
		placement.Kind != domain.ExecutionPlacementAttachedWorker || placement.OwnerUserID != auth.OwnerUserID || placement.WorkerID != auth.WorkerID ||
		placement.CapabilityDigest != auth.CapabilityDigest || placement.PolicyDigest != auth.PolicyDigest || job.ReservationID != a.ReservationID ||
		r.Producer.SessionID != job.SessionID || r.Producer.ReservationID != a.ReservationID || !r.Producer.AcceptedAt.Equal(a.CreatedAt) ||
		r.Observation.ValidateFor(domain.AttachedWorkerTerminalSucceeded, job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1) != nil {
		return deny()
	}
	if _, deleting, e := readSessionDeletionTx(ctx, tx, job.SessionID); e != nil {
		return loaded, at, e
	} else if deleting {
		return deny()
	}
	if !readyReplay && (!at.Before(r.ExpiresAt) || r.ExpiresAt.After(at.Add(15*time.Second)) ||
		r.ExpiresAt.After(a.CreatedAt.Add(job.Limits.MaxRuntime)) || r.ExpiresAt.After(a.LeaseExpiresAt) ||
		r.ExpiresAt.After(loaded.Reservation.ExpiresAt) || r.ExpiresAt.After(c.AuthExpiresAt) ||
		job.HarnessBinding.EvidenceExpiresAt != nil && r.ExpiresAt.After(*job.HarnessBinding.EvidenceExpiresAt)) {
		return deny()
	}
	if !readyReplay {
		lease, found, err := readCanonicalLeaseHeadTx(ctx, tx, auth.RunID)
		if err != nil {
			return loaded, at, err
		}
		if !found || lease.ValidateForAttempt(loaded.Run, loaded.Attempt) != nil || !lease.ActiveAt(at) ||
			lease.ID != auth.LeaseID || lease.FenceToken != auth.LeaseGeneration || !lease.ExpiresAt.Equal(a.LeaseExpiresAt) ||
			loaded.Run.Status != domain.RunRunning || loaded.Attempt.Status != domain.AttemptRunning ||
			loaded.Reservation.Status != domain.ReservationHeld {
			return deny()
		}
	}
	if job.HarnessBinding.Resource.Kind == domain.ProviderResourceCredentiallessFixtureV1 && job.HarnessBinding.Backend.ProviderContractKind == domain.ProviderContractCredentiallessFixtureV1 {
		return loaded, at, nil
	}
	if job.HarnessBinding.Resource.Kind != domain.ProviderResourceSubscriptionV1 || job.HarnessBinding.Resource.ResourceID != string(loaded.Run.SubscriptionConnectionID) || job.HarnessBinding.Resource.OwnerUserID != auth.OwnerUserID {
		return deny()
	}
	if readyReplay {
		// A validated READY row grants metadata only, never another body or
		// canonical write. Current tenant/worker access and original scope
		// were checked above; later provider reauth cannot mint a new export
		// grant, nor should it hide this already committed receipt identity.
		return loaded, at, nil
	}
	var actor domain.ActorID
	var provider, secretRef string
	var entitlement domain.EntitlementState
	var observed, updated time.Time
	err = tx.sqlTx.QueryRowContext(ctx, `SELECT actor_id,provider,credential_ref,entitlement_state,observed_at,updated_at FROM subscription_connections WHERE tenant_id=$1 AND subscription_connection_id=$2`, tx.tenantID, loaded.Run.SubscriptionConnectionID).Scan(&actor, &provider, &secretRef, &entitlement, &observed, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return deny()
	}
	if err != nil {
		return loaded, at, err
	}
	var owner domain.UserID
	if err = tx.sqlTx.QueryRowContext(ctx, `SELECT user_id FROM actors WHERE tenant_id=$1 AND actor_id=$2`, tx.tenantID, actor).Scan(&owner); errors.Is(err, sql.ErrNoRows) {
		return deny()
	} else if err != nil {
		return loaded, at, err
	}
	var projected domain.SubscriptionConnectionID
	if err = tx.sqlTx.QueryRowContext(ctx, `SELECT subscription_connection_id FROM subscription_connections_by_user WHERE tenant_id=$1 AND user_id=$2 AND subscription_connection_id=$3`, tx.tenantID, auth.OwnerUserID, loaded.Run.SubscriptionConnectionID).Scan(&projected); errors.Is(err, sql.ErrNoRows) {
		return deny()
	} else if err != nil {
		return loaded, at, err
	}
	if provider != "codex" || secretRef != "" || entitlement != domain.EntitlementActive || owner != auth.OwnerUserID || projected != loaded.Run.SubscriptionConnectionID || observed.IsZero() || updated.IsZero() || observed.After(at) || updated.After(at) {
		return deny()
	}
	return loaded, at, nil
}

// This predicate is shared by every fresh transition and immutable READY
// reconciliation. It is not the tenant/connection/deletion authorization join.
func validateInlineLoadedJobPhaseV2(loaded ports.WorkerJobState, r AttachedWorkerOutputReceiptV2, at time.Time, readyReplay bool) error {
	evidenceAt := at
	if readyReplay {
		evidenceAt = r.CreatedAt
	}
	if validateLoadedWorkerJob(loaded) != nil || loaded.Job.HarnessBinding.ValidateAt(evidenceAt) != nil ||
		!readyReplay && loaded.Run.CancellationRequestedAt != nil {
		return ErrAttachedWorkerAttemptConflict
	}
	return nil
}

func inlineReadyAttemptMetadataV2(a domain.AttachedWorkerAttemptV1, digest domain.AttachedWorkerTerminalEvidenceDigest) bool {
	// This is receipt metadata, not a new success/lease grant. The original
	// finalizer still decides whether a terminal can commit. A changed terminal
	// identity must never be mistaken for this READY result.
	return a.State == domain.AttachedWorkerAttemptClaimed && a.CancelRevision == 0 && a.CancelDeadline.IsZero() ||
		(a.State == domain.AttachedWorkerAttemptTerminalPending || a.State == domain.AttachedWorkerAttemptTerminalCommitted) &&
			a.TerminalStatus == domain.AttachedWorkerTerminalSucceeded && a.TerminalEvidenceDigest == digest
}
