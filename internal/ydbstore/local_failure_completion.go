package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const localFailureFinalizationPrefix = "local_f1:"

func decodeLocalFailureMarker(marker string) (string, error) {
	if len(marker) != 73 || !strings.HasPrefix(marker, localFailureFinalizationPrefix) || domain.ValidateSHA256Digest("failure.marker", marker[9:]) != nil {
		return "", ErrRunFinalizationConflict
	}
	return marker[9:], nil
}

func localFailureSeal(run domain.Run, events []localCapsuleEventV1) (string, error) {
	if run.TerminalFailure == nil || run.TerminalFailure.ValidateForRun(run) != nil {
		return "", domain.ErrMembershipDenied
	}
	version := uint32(1)
	if run.TerminalFailure.CapsuleSchema == "sessionless.terminal-failure-custody.v2" {
		version = 2
	} else if run.TerminalFailure.CapsuleSchema == "sessionless.terminal-failure-custody.v3" {
		version = 3
	}
	d, err := localFailureVersionDigest(version, events)
	if err != nil || d != run.TerminalFailure.EventsDigest {
		return "", domain.ErrMembershipDenied
	}
	hash, err := localFailureVersionDigest(version, struct {
		Domain   string
		Status   domain.RunStatus
		Metadata domain.RunTerminalFailureV1
		Derived  *domain.RunDerivedContextV1
		Events   []localCapsuleEventV1
	}{"sessionless.local-failure-finalization.v1", run.Status, *run.TerminalFailure, run.DerivedContext, events})
	if err != nil {
		return "", err
	}
	return localFailureFinalizationPrefix + hash, nil
}

func (h *LocalFailureV1) readMaterials(ctx context.Context) (*ports.CanonicalEventMaterialSetV1, error) {
	body, err := h.entry.readCompactionBlob(ctx, h.plan.Capsule, h.entry.terminalOutputs.CapsuleLimit())
	if err != nil {
		return nil, err
	}
	defer clear(body)
	c, err := decodeLocalFailureCapsule(body)
	defer clearLocalFailureNormalized(c)
	if err != nil {
		return nil, err
	}
	metadata, err := failureMetadata(h.plan, c, h.metadata.RetiredAllocationDigest)
	if err != nil || metadata != h.metadata {
		return nil, domain.ErrMembershipDenied
	}
	p, err := localFailurePlan(h.run, h.snapshot.Job.Job.Limits, c)
	if err != nil {
		return nil, err
	}
	defer p.Release()
	events := p.Events()
	if len(events) != len(h.drafts) || len(c.Events) != len(h.drafts) {
		return nil, domain.ErrMembershipDenied
	}
	materials := ports.NewCanonicalEventMaterialSetV1()
	success := false
	defer func() {
		if !success {
			materials.Release()
		}
	}()
	for i, draft := range h.drafts {
		if c.Events[i] != (localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText}) || events[i].ID != draft.ID || events[i].Kind != draft.Kind {
			return nil, domain.ErrMembershipDenied
		}
		actual, err := h.entry.readCompactionBlob(ctx, draft.Payload, uint64(len(events[i].Payload)))
		equal := bytes.Equal(actual, events[i].Payload)
		if err != nil || !equal {
			clear(actual)
			return nil, domain.ErrMembershipDenied
		}
		prepared := ports.PrepareCanonicalDraftV1(h.run, draft, actual)
		clear(actual)
		if prepared == nil || !materials.Adopt(draft.ID, prepared) {
			if prepared != nil {
				prepared.Release()
			}
			return nil, domain.ErrMembershipDenied
		}
	}
	success = true
	return materials, nil
}

func (h *LocalFailureV1) observeReplay(ctx context.Context, q *ownedAuthorityQuery) (bool, error) {
	raw, err := q.readCanonicalJSON(ctx, `SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`, h.run.TenantID, h.run.ID)
	var run domain.Run
	if err != nil || decodeOwnedEffectCanonicalJSON(raw, &run) != nil || run.Validate() != nil {
		return false, q.budget.deny()
	}
	var status, marker string
	err = q.scanMetadata(ctx, localTerminalMarkerSQL, []any{h.run.TenantID, h.run.ID}, &status, &marker)
	if errors.Is(err, sql.ErrNoRows) {
		if run.Status.Terminal() {
			return false, ErrRunFinalizationConflict
		}
		return false, nil
	}
	if err != nil || status != string(domain.RunFailed) || run.Status != domain.RunFailed || !domain.EqualRunTerminalFailureV1(run.TerminalFailure, &h.metadata) {
		return false, ErrRunFinalizationConflict
	}
	base := run.Clone()
	base.Status = h.run.Status
	base.FinishedAt = h.run.FinishedAt
	base.UpdatedAt = h.run.UpdatedAt
	base.TerminalCustody = nil
	base.TerminalFailure = nil
	if !reflect.DeepEqual(base, h.run) {
		return false, ErrRunFinalizationConflict
	}
	events := make([]localCapsuleEventV1, len(h.drafts))
	for i, d := range h.drafts {
		events[i] = localCapsuleEventV1{ID: d.ID, Kind: d.Kind, IdempotencyKey: d.IdempotencyKey, Payload: d.Payload, DisplayText: d.DisplayText}
	}
	want, err := localFailureSeal(run, events)
	if err != nil || marker != want {
		return false, ErrRunFinalizationConflict
	}
	args := []any{run.TenantID, run.SessionID}
	for _, d := range h.drafts {
		args = append(args, d.IdempotencyKey)
	}
	rows, err := q.QueryContext(ctx, localTerminalReplayEventsSQL(len(h.drafts)), args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	i := 0
	var previous uint64
	for rows.Next() {
		var sequence uint64
		var id, raw string
		n, err := scanOwnedCurrentRow(rows, &sequence, &id, &raw)
		if err != nil || q.budget.materialized(q.callback, n) != nil || i >= len(h.drafts) {
			return false, ErrRunFinalizationConflict
		}
		var event domain.SessionEvent
		d := h.drafts[i]
		if decodeOwnedEffectCanonicalJSON(raw, &event) != nil || event.Validate() != nil || id != string(d.ID) || sequence != event.Sequence || event.IdempotencyKey != d.IdempotencyKey || event.ID != d.ID || event.Kind != d.Kind || event.Payload != d.Payload || event.AuthorUserID != nil || event.CreatedAt.Before(d.CreatedAt) || event.RunID == nil || *event.RunID != run.ID || event.TenantID != run.TenantID || event.SessionID != run.SessionID || i > 0 && event.Sequence != previous+1 {
			return false, ErrRunFinalizationConflict
		}
		previous = event.Sequence
		i++
	}
	if rows.Err() != nil || rows.Close() != nil || i != len(h.drafts) {
		return false, ErrRunFinalizationConflict
	}
	return true, nil
}

// Complete reuses the EXISTING fenced failure transaction. Only this original
// retained/fenced producer installs F1 metadata; the public failure port remains
// notice-only. A possibly accepted first commit can only be exactly observed.
func (h *LocalFailureV1) Complete(ctx context.Context) error {
	if !h.original() || ctx == nil || ctx.Err() != nil || !h.published || h.retained == nil {
		return domain.ErrMembershipDenied
	}
	select {
	case h.gate <- struct{}{}:
	default:
		return domain.ErrMembershipDenied
	}
	defer func() { <-h.gate }()
	call, finish := ownedAuthorityCallContext(ctx)
	defer finish()
	materials, err := h.readMaterials(call)
	if err != nil {
		return err
	}
	defer materials.Release()
	matched := false
	var replayErr error
	err = h.entry.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		matched, replayErr = h.observeReplay(ctx, q)
		return replayErr
	})
	if err != nil || matched {
		if replayErr != nil {
			return replayErr
		}
		return err
	}
	h.mu.Lock()
	live := !h.closed && !h.failed.Load()
	h.mu.Unlock()
	if !live || h.commitAttempted || !time.Now().Before(h.deadline) || !h.entry.preparationMu.TryLock() {
		return domain.ErrMembershipDenied
	}
	defer h.entry.preparationMu.Unlock()
	first, cancel := context.WithDeadline(call, h.deadline)
	defer cancel()
	err = h.entry.owner.store.ownedAuthorityTx(first, func(ctx context.Context, q *ownedAuthorityQuery) error {
		matched, replayErr = h.observeReplay(ctx, q)
		if replayErr != nil || matched {
			return replayErr
		}
		if h.commitAttempted || h.rejoin(ctx, q) != nil || h.entry.terminalOutputs.VerifyRetainedBodies(h.retained) != nil || h.entry.owner.ready.Load().bridge.VerifyStoppedLocalTerminal(h.entry.allocation) != nil {
			return q.budget.deny()
		}
		run := h.run.Clone()
		run.TerminalCustody = &domain.RunTerminalCustodyV1{Version: 1, Plan: h.plan}
		// SAME closed handoff as successful local completion: original authority
		// reads retain q's32/96/4MiB budget, existing canonical helpers retain
		// their own bounded event/material/binding checks on THIS transaction.
		// No q authority reads may follow a canonical write.
		tx := &stateTx{store: h.entry.owner.store, sqlTx: q.tx, tenantID: run.TenantID}
		h.commitAttempted = true // irreversible BEFORE any possible canonical write
		return failWorkerWithAuthorityTx(ctx, tx, tx, run, h.snapshot.Job.Attempt, h.snapshot.Job.Reservation, domain.RunFailed, domain.AttemptFailed, h.entry.parent.Authority.Lease.ID, h.entry.parent.Authority.Lease.FenceToken, q.at, h.metadata.Stop, nil, func(failed domain.Run) error {
			events := make([]localCapsuleEventV1, len(h.drafts))
			for i, d := range h.drafts {
				events[i] = localCapsuleEventV1{ID: d.ID, Kind: d.Kind, IdempotencyKey: d.IdempotencyKey, Payload: d.Payload, DisplayText: d.DisplayText}
			}
			marker, err := localFailureSeal(failed, events)
			if err != nil {
				return err
			}
			return appendCanonicalFinalizationTx(ctx, tx, failed, domain.RunFailed, marker, h.drafts, materials, q.at, canonicalFailureNotice(h.metadata.Stop, false))
		}, h)
	})
	if replayErr != nil {
		return replayErr
	}
	return err
}

func (h *LocalFailureV1) terminalRun(run domain.Run) (domain.Run, error) {
	if !h.original() || !h.commitAttempted || run.Status != domain.RunFailed || run.CancellationRequestedAt != nil || run.TerminalFailure != nil || run.TerminalCustody == nil || run.TerminalCustody.Plan != h.plan || run.TerminalCustody.LocalEvidence != nil || run.TerminalCustody.Correspondence != nil {
		return domain.Run{}, domain.ErrMembershipDenied
	}
	base := run.Clone()
	base.Status = h.run.Status
	base.UpdatedAt = h.run.UpdatedAt
	base.FinishedAt = h.run.FinishedAt
	base.TerminalCustody = nil
	if !reflect.DeepEqual(base, h.run) {
		return domain.Run{}, domain.ErrMembershipDenied
	}
	metadata := h.metadata
	run.TerminalFailure = &metadata
	if run.Validate() != nil {
		return domain.Run{}, domain.ErrMembershipDenied
	}
	return run, nil
}
func (h *LocalFailureV1) putRun(ctx context.Context, tx *stateTx, run domain.Run) error {
	if !h.original() || tx == nil || tx.store != h.entry.owner.store || !domain.EqualRunTerminalFailureV1(run.TerminalFailure, &h.metadata) || !h.commitAttempted || h.entry.terminalOutputs.VerifyRetainedBodies(h.retained) != nil {
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
