package ydbstore

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Prepare owns one bounded capture/body-validation/plan logical call. It writes
// no terminal metadata or object and never resets the original cleanup end.
func (h *LocalFailureV1) Prepare(ctx context.Context) (err error) {
	call, finish, err := h.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if !h.entry.preparationMu.TryLock() {
		return domain.ErrMembershipDenied
	}
	defer h.entry.preparationMu.Unlock()
	if h.prepareAttempted {
		return domain.ErrMembershipDenied
	}
	h.prepareAttempted = true
	defer func() {
		if err != nil {
			h.failed.Store(true)
		}
	}()
	call, done := ownedAuthorityCallContext(call)
	defer done()
	e := h.entry
	tuple := e.owner.ready.Load()
	if tuple == nil || tuple.store != e.owner.store || tuple.bridge == nil || !sameLocalOwnerPointer(tuple.keeper, e.owner.keeper) {
		return domain.ErrMembershipDenied
	}
	keeper, ok := tuple.keeper.(interface {
		SealTerminal(time.Time) error
		StopAndJoinTerminal(context.Context) error
	})
	if !ok || keeper.SealTerminal(h.deadline) != nil || keeper.StopAndJoinTerminal(call) != nil || tuple.bridge.StopAndJoinLocalTerminal(call, e.allocation) != nil {
		return domain.ErrMembershipDenied
	}
	if err = h.capture(call); err != nil {
		return fmt.Errorf("known MCP failure frontier capture: %w", err)
	}
	bodies, err := e.terminalOutputs.SnapshotRetainedBodies(call)
	if err != nil {
		return err
	}
	identities := make([]localFailureOperationV1, len(h.ops))
	for i, op := range h.ops {
		identities[i] = localFailureOperationV1{Reservation: op.Reservation, Observation: op.Observation}
	}
	fd, err := localFailureFrontierDigest(e.parent, h.accounting, identities)
	if err != nil {
		return err
	}
	pd, err := e.owner.configuration.Digest()
	if err != nil {
		return err
	}
	c := localFailureCapsule{Schema: "sessionless.terminal-failure-custody.v1", Version: h.version, Parent: e.parent.Clone(), Source: e.source.Clone(), ProfileDigest: pd, FrontierDigest: fd, Stop: domain.KnownMCPServiceErrorV1, Cleanup: "joined_retained", Accounting: h.accounting.Clone(), Bodies: bodies, Derived: h.snapshot.Job.Run.Clone().DerivedContext}
	if h.version == 2 {
		c.Schema, c.Stop = "sessionless.terminal-failure-custody.v2", domain.ModelOutcomeUnknownV2
	} else if h.version == 3 {
		c.Schema, c.Stop = "sessionless.terminal-failure-custody.v3", domain.MCPOutcomeUnknownV3
	}
	c.CapturedAt = h.capturedAt
	for _, op := range h.ops {
		normalized := json.RawMessage(op.Response)
		if (h.version == 2 || h.version == 3) && op.Observation.Outcome == domain.AttemptOperationAcceptedOutcomeUnknownV1 {
			normalized = json.RawMessage("null")
		}
		c.Operations = append(c.Operations, localFailureOperationV1{Reservation: op.Reservation.Clone(), Observation: op.Observation.Clone(), Normalized: normalized})
	}
	h.canonical, err = localFailurePlan(h.snapshot.Job.Run, h.snapshot.Job.Job.Limits, c)
	if err != nil {
		return fmt.Errorf("known MCP failure canonical planning: %w", err)
	}
	h.run = h.snapshot.Job.Run.Clone()
	at := h.capturedAt
	for _, event := range h.canonical.Events() {
		draft, de := canonicaloutput.PlanEventDraftV1(h.run, event.ID, event.Kind, event.Payload, at)
		if de != nil {
			return de
		}
		h.drafts = append(h.drafts, draft)
		c.Events = append(c.Events, localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText})
	}
	h.capsule, err = encodeLocalFailureCapsule(c, e.terminalOutputs.CapsuleLimit())
	if err != nil {
		return err
	}
	sha := rawLocalFailureHash(h.capsule)
	key, err := domain.TerminalCustodyObjectKeyV1(ownedOperationScope(e.parent), sha)
	if err != nil {
		return err
	}
	h.plan = domain.TerminalCustodyPlanV1{Scope: ownedOperationScope(e.parent), FrontierDigest: fd, Capsule: domain.BlobRef{TenantID: h.run.TenantID, Key: key, Size: int64(len(h.capsule)), SHA256: sha}}
	if h.plan.Validate() != nil {
		return domain.ErrMembershipDenied
	}
	h.metadata, err = failureMetadata(h.plan, c, "")
	if err != nil {
		return err
	}
	h.prepared = true
	return nil
}

// Publish owns one bounded registration/confirmed readback/physical write and
// final rejoin logical call, just like the existing success publisher. It has
// the unchanged three-callback budget and the SAME original absolute deadline.
func (h *LocalFailureV1) Publish(ctx context.Context) (err error) {
	call, finish, err := h.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if !h.entry.preparationMu.TryLock() {
		return domain.ErrMembershipDenied
	}
	defer h.entry.preparationMu.Unlock()
	if !h.prepared || h.publishAttempted || h.canonical == nil {
		return domain.ErrMembershipDenied
	}
	h.publishAttempted = true
	defer func() {
		if err != nil {
			h.failed.Store(true)
		}
	}()
	call, done := ownedAuthorityCallContext(call)
	defer done()
	e := h.entry
	// Burn registration OUTSIDE SDK callbacks. A lost ACK never reopens it.
	attempted := false
	err = e.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		if attempted {
			return q.budget.deny()
		}
		if err := h.rejoin(ctx, q); err != nil {
			return err
		}
		run := h.run.Clone()
		run.TerminalCustody = &domain.RunTerminalCustodyV1{Version: 1, Plan: h.plan}
		payload, err := marshalRunPayload(run)
		if err != nil {
			return q.budget.deny()
		}
		attempted = true
		_, err = q.ExecContext(ctx, localRetainedRunUpdateSQL, payload, run.TenantID, run.ID)
		return err
	})
	if err != nil || !attempted {
		return domain.ErrMembershipDenied
	}
	h.registered = true
	// A confirmed commit alone is not publication permission: a NEW callback
	// and metadata cache must join the exact full persisted postimage first.
	if e.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error { return h.rejoin(ctx, q) }) != nil {
		return domain.ErrMembershipDenied
	}
	post := h.run.Clone()
	post.TerminalCustody = &domain.RunTerminalCustodyV1{Version: 1, Plan: h.plan}
	if chargeOwnedAuthorityBody(call, 3*uint64(len(h.capsule))) != nil || e.terminalOutputs.WriteCapsule(call, h.plan, h.capsule) != nil {
		return fmt.Errorf("known MCP failure capsule publication: %w", domain.ErrMembershipDenied)
	}
	events := h.canonical.Events()
	payloads := make([][]byte, len(events))
	for i, event := range events {
		payloads[i] = event.Payload
		if chargeOwnedAuthorityBody(call, 3*uint64(len(event.Payload))) != nil {
			return domain.ErrMembershipDenied
		}
	}
	if e.terminalOutputs.WriteFailureEventsV1(call, post, h.drafts, payloads) != nil {
		return fmt.Errorf("known MCP failure canonical publication: %w", domain.ErrMembershipDenied)
	}
	if e.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error { return h.rejoin(ctx, q) }) != nil || call.Err() != nil {
		return fmt.Errorf("known MCP failure publication rejoin: %w", domain.ErrMembershipDenied)
	}
	h.published = true
	return nil
}

// FinishCleanup performs genuine retirement and a same-adapter ordinary-body
// fence. The objects remain retained; no NamespaceErasure or success V3 is made.
func (h *LocalFailureV1) FinishCleanup(ctx context.Context) (err error) {
	call, finish, err := h.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if !h.entry.preparationMu.TryLock() {
		return domain.ErrMembershipDenied
	}
	defer h.entry.preparationMu.Unlock()
	if !h.published || h.retained != nil {
		return domain.ErrMembershipDenied
	}
	defer func() {
		if err != nil {
			h.failed.Store(true)
		}
	}()
	call, done := ownedAuthorityCallContext(call)
	defer done()
	if err = h.retire(call); err != nil {
		return err
	}
	if h.entry.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error { return h.rejoin(ctx, q) }) != nil {
		return domain.ErrMembershipDenied
	}
	c, err := decodeLocalFailureCapsule(h.capsule)
	defer clearLocalFailureNormalized(c)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(c.Accounting, h.accounting) {
		return domain.ErrMembershipDenied
	}
	h.retained, err = h.entry.terminalOutputs.FenceRetainedBodies(call, c.Bodies)
	if err != nil {
		return err
	}
	d, err := h.retained.Digest()
	if err != nil || d != h.metadata.RetainedBodiesDigest || h.entry.terminalOutputs.VerifyRetainedBodies(h.retained) != nil {
		return domain.ErrMembershipDenied
	}
	if h.entry.owner.ready.Load().bridge.VerifyStoppedLocalTerminal(h.entry.allocation) != nil || call.Err() != nil {
		return domain.ErrMembershipDenied
	}
	retiredDigest, err := h.retired.Current.Digest()
	if err != nil {
		return err
	}
	h.metadata.RetiredAllocationDigest = string(retiredDigest)
	return nil
}
