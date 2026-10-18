package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync/atomic"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// LocalRetainedCompactionV1 holds only the original entry's one-use retained
// physical slot and privately read expected Run metadata. Neither a plan nor a
// caller-created receipt can reconstruct it or authorize head publication.
type LocalRetainedCompactionV1 struct {
	self       *LocalRetainedCompactionV1
	entry      *LocalInvocationEntryV1
	operation  domain.AttemptOperationV1
	generation uint64
	expected   *domain.RunDerivedContextV1
	pair       *operationbody.RetainedPairV1
	claimed    *atomic.Bool
}

// PrepareRetainedCompaction reserves the nonrefundable encoded upper bounds
// before generation. It does not admit a compaction call or relax the current
// runner's eligibility checks; those remain separately required.
func (entry *LocalInvocationEntryV1) PrepareRetainedCompaction(ctx context.Context, operation domain.AttemptOperationV1, generation uint64) (*LocalRetainedCompactionV1, error) {
	if !entry.original() || operation.Kind != domain.AttemptOperationCompactionV1 || operation.Scope != ownedOperationScope(entry.parent) || operation.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil || generation < 1 || generation > 2 || operation.Debit.BodyBytes-operation.RequestBytes-operation.ResultBytes < operationbody.RetainedPairBytesV1 {
		return nil, domain.ErrMembershipDenied
	}
	call, finish, err := entry.retainedContext(ctx, operation)
	if err != nil {
		return nil, err
	}
	defer finish()
	call, cancel := ownedAuthorityCallContext(call)
	defer cancel()
	handle := &LocalRetainedCompactionV1{entry: entry, operation: operation.Clone(), generation: generation, claimed: new(atomic.Bool)}
	store := entry.owner.store
	err = store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		handle.expected = nil
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		run, err := entry.currentRetainedRun(ctx, q, operation)
		if err != nil {
			return err
		}
		if run.DerivedContext != nil {
			if len(run.DerivedContext.Inventory) >= 2 {
				return q.budget.deny()
			}
			value := run.DerivedContext.Clone()
			handle.expected = &value
		}
		return nil
	})
	if err != nil || call.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	handle.pair, err = entry.bodies.ReserveRetainedPair(call, operation.Sequence, generation)
	if err != nil || call.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	handle.self = handle
	return handle, nil
}

// Upload registers exact cleanup identities in the original Store transaction
// before either retained PUT. It does NOT validate structured summary fidelity,
// source closure, terminal custody or an active derived head. Unknown commit or
// PUT is terminal for this handle; an ordinary Run writer cannot replace it.
func (h *LocalRetainedCompactionV1) Upload(ctx context.Context, plan domain.DerivedContextPlanV1, observed ports.AuthenticatedAttemptOperationObservationV1, summary, manifest []byte) error {
	if h == nil || h.self != h || h.entry == nil || !h.entry.original() || h.pair == nil || h.claimed == nil || !h.claimed.CompareAndSwap(false, true) {
		return domain.ErrMembershipDenied
	}
	call, finish, err := h.entry.retainedContext(ctx, h.operation)
	if err != nil {
		return err
	}
	defer finish()
	call, cancel := ownedAuthorityCallContext(call)
	defer cancel()
	store := h.entry.owner.store
	if observed.Validate() != nil {
		return domain.ErrMembershipDenied
	}
	r, o := observed.Reservation, observed.Observation
	opDigest, oe := h.operation.Digest()
	rd, re := r.Digest()
	od, de := o.Digest()
	if plan.Validate() != nil || plan.Scope != h.operation.Scope || plan.OperationSequence != h.operation.Sequence || plan.Generation != h.generation || oe != nil || re != nil || de != nil || r.OperationDigest != opDigest || plan.ReservationDigest != rd || plan.ObservationDigest != od || plan.NormalizedResponseDigest != o.ResponseDigest || !ownedObservationAllowsSuccessor(r, o) || store.attemptOperationIssuer.VerifyAttemptOperationObservation(observed.Clone()) != nil || !localRetainedBytes(plan.Summary, summary) || !localRetainedBytes(plan.Manifest, manifest) {
		return domain.ErrMembershipDenied
	}
	// Actual request/result bytes are verified through the original fixed body
	// reader outside SQL; honest unknown token usage remains worst-case charged.
	if store.validateLocalNormalizedModel(call, h.operation, o) != nil {
		return domain.ErrMembershipDenied
	}
	registrationAttempted := false
	err = store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		// SDK read/serialization retries before a write remain bounded. Once
		// registration was attempted, even a nonpersisted lost commit cannot
		// be retried into confirmed registration and subsequent physical PUT.
		if registrationAttempted {
			return q.budget.deny()
		}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		run, err := h.entry.currentRetainedRun(ctx, q, h.operation)
		if err != nil || !domain.EqualRunDerivedContextV1(run.DerivedContext, h.expected) || r.ValidateForParent(h.entry.parent.Authority, h.entry.parent.Reservation) != nil || store.operationHistory(ctx, q, r) != nil {
			return q.budget.deny()
		}
		row, err := readOwnedOperation(ctx, q, h.operation)
		if err != nil || row == nil || row.observation == nil {
			return q.budget.deny()
		}
		actualReservation, _ := row.reservation.Digest()
		actualObservation, _ := row.observation.Digest()
		if actualReservation != rd || actualObservation != od {
			return q.budget.deny()
		}
		if run.DerivedContext == nil {
			run.DerivedContext = &domain.RunDerivedContextV1{Version: 1}
		} else {
			value := run.DerivedContext.Clone()
			run.DerivedContext = &value
		}
		if len(run.DerivedContext.Inventory) >= 2 {
			return q.budget.deny()
		}
		run.DerivedContext.Inventory = append(run.DerivedContext.Inventory, plan)
		if run.Validate() != nil {
			return q.budget.deny()
		}
		payload, err := marshalRunPayload(run)
		if err != nil {
			return q.budget.deny()
		}
		// Serializable current read + payload-only update is the private CAS.
		// Base status/timestamps and the complete old inventory/head are retained.
		registrationAttempted = true
		_, err = q.ExecContext(ctx, localRetainedRunUpdateSQL, payload, run.TenantID, run.ID)
		return err
	})
	if err != nil || call.Err() != nil {
		return domain.ErrMembershipDenied
	}
	life, err := h.entry.owner.source.ExecutionLifetime()
	if err != nil || life.Err() != nil || h.entry.invocationContext.Err() != nil || call.Err() != nil {
		return domain.ErrMembershipDenied
	}
	if h.pair.Write(call, plan, summary, manifest) != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}

const localRetainedRunUpdateSQL = `UPDATE runs SET payload=CAST($1 AS JsonDocument) WHERE tenant_id=$2 AND run_id=$3`

func localRetainedBytes(ref domain.BlobRef, body []byte) bool {
	if int64(len(body)) != ref.Size {
		return false
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]) == ref.SHA256
}

func (entry *LocalInvocationEntryV1) currentRetainedRun(ctx context.Context, q *ownedAuthorityQuery, op domain.AttemptOperationV1) (domain.Run, error) {
	store := entry.owner.store
	request, parent, source, burn, err := store.operationParent(ctx, q, op.Scope)
	if err != nil {
		return domain.Run{}, q.budget.deny()
	}
	pd, pe := parent.Digest()
	ed, ee := entry.parent.Digest()
	sd, se := source.Digest()
	es, ese := entry.source.Digest()
	if pe != nil || ee != nil || se != nil || ese != nil || pd != ed || sd != es || !q.at.Before(op.Deadline) || store.validateCurrentOwnedAttemptParent(ctx, q, request, parent) != nil {
		return domain.Run{}, q.budget.deny()
	}
	allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burn)
	if err != nil || allocation == nil || allocation.current.LocalRuntime == nil || allocation.current.ValidateAt(q.at) != nil || op.Deadline.After(allocation.current.Window.ExpiresAt) {
		return domain.Run{}, q.budget.deny()
	}
	actual, ae := allocation.original.Digest()
	original, oe := entry.allocation.Digest()
	tuple := entry.owner.ready.Load()
	if ae != nil || oe != nil || actual != original || tuple == nil || tuple.store != store || tuple.bridge.VerifyEnteredLocalAllocation(entry.allocation, q.at) != nil {
		return domain.Run{}, q.budget.deny()
	}
	run, found, err := readRunPayload(ctx, q, op.Scope.TenantID, op.Scope.RunID)
	if err != nil || !found || run.Validate() != nil || run.ID != op.Scope.RunID || run.TenantID != op.Scope.TenantID || run.SessionID != op.Scope.SessionID || run.CancellationRequestedAt != nil || run.Status != domain.RunRunning {
		return domain.Run{}, q.budget.deny()
	}
	return run.Clone(), nil
}

func (entry *LocalInvocationEntryV1) retainedContext(ctx context.Context, op domain.AttemptOperationV1) (context.Context, func(), error) {
	if !entry.original() || ctx == nil || ctx.Err() != nil || entry.invocationContext == nil || entry.invocationContext.Err() != nil {
		return nil, nil, domain.ErrMembershipDenied
	}
	life, err := entry.owner.source.ExecutionLifetime()
	if err != nil || life.Err() != nil {
		return nil, nil, domain.ErrMembershipDenied
	}
	deadline := op.Deadline
	for _, original := range []context.Context{life, entry.invocationContext} {
		if end, ok := original.Deadline(); ok && end.Before(deadline) {
			deadline = end
		}
	}
	call, cancel := context.WithDeadline(ctx, deadline)
	stopSource := context.AfterFunc(life, cancel)
	stopInvocation := context.AfterFunc(entry.invocationContext, cancel)
	finish := func() { stopSource(); stopInvocation(); cancel() }
	if life.Err() != nil || entry.invocationContext.Err() != nil || call.Err() != nil {
		finish()
		return nil, nil, domain.ErrMembershipDenied
	}
	return call, finish, nil
}
