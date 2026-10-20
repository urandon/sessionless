package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Only the original persisted cancellation transition may explain these two
// changed fields. It does not hide a status, frontier, identity or plan change.
func normalizeLocalTerminalCancellation(run domain.Run, original domain.Run) (domain.Run, error) {
	if run.CancellationRequestedAt != nil {
		if original.CancellationRequestedAt != nil || run.CancellationRequestedAt.Before(original.UpdatedAt) ||
			(!run.UpdatedAt.Equal(original.UpdatedAt) && !run.UpdatedAt.Equal(*run.CancellationRequestedAt)) {
			return domain.Run{}, domain.ErrMembershipDenied
		}
		run.CancellationRequestedAt = nil
		run.UpdatedAt = original.UpdatedAt
	}
	return run, nil
}

func (c *localTerminalCompletionV2) rejoinFirst(ctx context.Context, q *ownedAuthorityQuery) (ports.WorkerJobState, error) {
	deny := func() (ports.WorkerJobState, error) { return ports.WorkerJobState{}, q.budget.deny() }
	if !c.original() {
		return deny()
	}
	p := c.cleanup.owner
	h, entry := p.parent, p.parent.entry
	if h.source == nil || h.source.state == nil || h.source.state.cleared || h.source.state.view == nil {
		return deny()
	}
	q.metadataCache = make(map[string]ownedMetadataCacheEntry)
	parent, source, burn, e := p.originalTerminalParent(ctx, q)
	if e != nil || !reflect.DeepEqual(parent, entry.parent) || !reflect.DeepEqual(source, entry.source) {
		return deny()
	}
	allocation, e := readOwnedAllocationSnapshots(ctx, q, parent, source, burn)
	if e != nil || allocation == nil || !reflect.DeepEqual(allocation.original, c.cleanup.allocation.Original) || !reflect.DeepEqual(allocation.current, c.cleanup.allocation.Current) || allocation.receipt(false).Digest != c.cleanup.allocation.Digest {
		return deny()
	}
	tuple := entry.owner.ready.Load()
	if tuple == nil || tuple.store != entry.owner.store || tuple.bridge == nil || tuple.bridge.VerifyStoppedLocalTerminal(entry.allocation) != nil || entry.terminalOutputs.VerifyNamespaceErasure(c.cleanup.erasure) != nil {
		return deny()
	}
	var deletion string
	e = q.QueryRowContext(ctx, `SELECT state FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`, parent.Scope.TenantID, parent.Scope.SessionID).Scan(&deletion)
	if !errors.Is(e, sql.ErrNoRows) {
		return deny()
	}
	snapshot, e := entry.readLocalPreparedSnapshotBody(ctx, q, localSnapshotOwner{}, localCompactionCapture)
	if e != nil {
		return deny()
	}
	job := snapshot.Job
	normalized, e := normalizeLocalTerminalCancellation(job.Run, c.run)
	if e != nil || !reflect.DeepEqual(normalized, c.run) {
		return deny()
	}
	snapshot.Job.Run = normalized
	snapshot.Job.Run.TerminalCustody = nil
	if !reflect.DeepEqual(snapshot, h.source.state.snapshot) {
		return deny()
	}
	return job, nil
}

func (c *localTerminalCompletionV2) firstCommit(ctx context.Context, q *ownedAuthorityQuery, materials *ports.CanonicalEventMaterialSetV1) error {
	p := c.cleanup.owner
	s := p.parent.source
	if s == nil || s.state == nil {
		return q.budget.deny()
	}
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	job, err := c.rejoinFirst(ctx, q)
	if err != nil || ctx.Err() != nil || !time.Now().Before(p.parent.deadline) {
		return q.budget.deny()
	}
	// Closed handoff to the EXISTING canonical helpers over THIS *sql.Tx*.
	// Authority/history retains q's32/96/4MiB budget. Canonical helpers retain
	// their distinct event/material/artifact/binding bounds; these raw statements
	// are not falsely counted as authority statements. No q reads follow writes.
	tx := &stateTx{store: p.parent.entry.owner.store, sqlTx: q.tx, tenantID: c.run.TenantID}
	entry := p.parent.entry
	lease, fence := entry.parent.Authority.Lease.ID, entry.parent.Authority.Lease.FenceToken
	if job.Run.CancellationRequestedAt != nil {
		planned := job.Run.Clone()
		if err := planned.Transition(domain.RunCancelled, q.at); err != nil {
			return err
		}
		if _, err := marshalRunPayload(planned); err != nil {
			return err
		}
		drafts := []domain.SessionEventDraft{c.cancellation}
		digest, err := runFinalizationDigestV2(domain.RunCancelled, nil, drafts, nil, planned.DerivedContext, planned.TerminalCustody)
		if err != nil {
			return err
		}
		digest, err = encodeLocalFinalizationMarkerV2(digest)
		if err != nil {
			return err
		}
		return failWorkerTx(ctx, tx, tx, job.Run, job.Attempt, job.Reservation, domain.RunCancelled, domain.AttemptCancelled,
			lease, fence, q.at, "cancelled", nil, func(run domain.Run) error {
				return appendCanonicalFinalizationTx(ctx, tx, run, domain.RunCancelled, digest, drafts, materials, q.at, canonicalFailureNotice("cancelled", true))
			})
	}
	digest, err := runFinalizationDigestV2(domain.RunSucceeded, &c.manifest, c.drafts, &c.evidence, c.run.DerivedContext, &c.custody)
	if err != nil {
		return err
	}
	digest, err = encodeLocalFinalizationMarkerV2(digest)
	if err != nil {
		return err
	}
	return completeWorkerSuccessWithAuthorityTx(ctx, tx, tx, job.Run, job.Attempt, job.Reservation, lease, fence, q.at,
		c.manifest, nil, workerSuccessAuthority{kind: workerSuccessLocalV2, local: c}, func(run domain.Run) error {
			return appendCanonicalFinalizationTx(ctx, tx, run, domain.RunSucceeded, digest, c.drafts, materials, q.at, nil)
		})
}

func (c *localTerminalCompletionV2) validateFirstCommit(store *Store, run domain.Run, attempt domain.Attempt, reservation domain.QuotaReservation) error {
	if !c.original() || store != c.cleanup.owner.parent.entry.owner.store || !reflect.DeepEqual(run, c.run) ||
		!reflect.DeepEqual(attempt, c.attempt) || !reflect.DeepEqual(reservation, c.reservation) || run.CancellationRequestedAt != nil ||
		c.evidence.ValidateForPersistedAuthority(c.cleanup.owner.parent.entry.parent.Authority, c.cleanup.owner.parent.entry.parent.Reservation) != nil {
		return domain.ErrMembershipDenied
	}
	p := c.cleanup.owner
	p.mu.Lock()
	live := p.completion == c && p.cleanup == c.cleanup && !p.cleared && p.published && !p.parent.failed.Load()
	p.mu.Unlock()
	if !live || p.accounting == nil || p.parent.entry.terminalOutputs.VerifyNamespaceErasure(c.cleanup.erasure) != nil {
		return domain.ErrMembershipDenied
	}
	digest, err := p.accounting.Digest()
	if err != nil || digest != c.evidence.LocalV1.AccountingDispositionDigest {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (c *localTerminalCompletionV2) terminalRun(run domain.Run) (domain.Run, error) {
	if !c.original() || run.Status != domain.RunSucceeded || run.CancellationRequestedAt != nil || !domain.EqualRunTerminalCustodyV1(run.TerminalCustody, c.run.TerminalCustody) || !domain.EqualRunDerivedContextV1(run.DerivedContext, c.run.DerivedContext) {
		return domain.Run{}, domain.ErrMembershipDenied
	}
	v := c.custody.Clone()
	run.TerminalCustody = &v
	if run.Validate() != nil {
		return domain.Run{}, domain.ErrMembershipDenied
	}
	if _, err := marshalRunPayload(run); err != nil {
		return domain.Run{}, err
	}
	return run, nil
}

func (c *localTerminalCompletionV2) putRun(ctx context.Context, tx *stateTx, run domain.Run) error {
	if !c.original() || tx == nil || tx.store != c.cleanup.owner.parent.entry.owner.store || run.Status != domain.RunSucceeded || !domain.EqualRunTerminalCustodyV1(run.TerminalCustody, &c.custody) {
		return domain.ErrMembershipDenied
	}
	payload, err := marshalRunPayload(run)
	if err != nil {
		return err
	}
	if err := ensureSessionWritableTx(ctx, tx, run.SessionID); err != nil {
		return err
	}
	return putCanonicalRunPayloadTx(ctx, tx, run, payload)
}

func (c *localTerminalCompletionV2) putEvidence(ctx context.Context, tx *stateTx, run domain.Run, attempt domain.Attempt, at time.Time) error {
	if !c.original() || tx == nil || tx.store != c.cleanup.owner.parent.entry.owner.store || run.TenantID != tx.tenantID || run.ID != c.run.ID || attempt.ID != c.attempt.ID || attempt.RunID != run.ID || at.IsZero() || c.evidence.ValidateForPersistedAuthority(c.cleanup.owner.parent.entry.parent.Authority, c.cleanup.owner.parent.entry.parent.Reservation) != nil {
		return domain.ErrMembershipDenied
	}
	encoded, err := json.Marshal(c.evidence)
	if err != nil || len(encoded) > domain.MaxExecutionEvidenceBytesV2 {
		return domain.ErrMembershipDenied
	}
	var digest, record string
	err = tx.sqlTx.QueryRowContext(ctx, localTerminalEvidenceSQL, tx.tenantID, attempt.ID).Scan(&digest, &record)
	if err == nil {
		if digest == c.evidence.LocalV1.EvidenceDigest && record == string(encoded) {
			return nil
		}
		return ErrSubstrateExecutionEvidenceConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx, `INSERT INTO substrate_execution_evidence
 (tenant_id,attempt_id,run_id,physical_invocation_claim_id,evidence_digest,recorded_at,record)
 VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS JsonDocument))`, tx.tenantID, attempt.ID, run.ID,
		c.evidence.LocalV1.Scope.PhysicalInvocationClaimID, c.evidence.LocalV1.EvidenceDigest, at, string(encoded))
	return err
}

const localTerminalEvidenceSQL = `SELECT SUBSTRING(CAST(evidence_digest AS String),0,65),SUBSTRING(CAST(record AS String),0,32769)
 FROM substrate_execution_evidence WHERE tenant_id=$1 AND attempt_id=$2`
