package ydbstore

import (
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Publish consumes only the original producer's owned buffers. Registration
// DATA and verified object custody are not retirement, erasure, execution
// evidence or canonical completion. The session wrapper joins this call before
// Clear; direct original callers are protected by the same entry/payload locks.
func (p *LocalPreparedOutputV1) Publish(ctx context.Context) (err error) {
	deny := func() error { return domain.ErrMembershipDenied }
	started := false
	// Registered first, so actual completion is signalled only after ALL phase
	// locks and contexts have been released. A cancelled writer retains buffers
	// until this point even when StopAndJoin has exhausted its original bound.
	defer func() {
		if !started {
			return
		}
		if err != nil {
			p.parent.failed.Store(true)
		}
		p.mu.Lock()
		p.publicationActive = false
		p.publicationCancel = nil
		if p.cleared {
			p.releaseOutputLocked()
		}
		close(p.publicationDone)
		p.mu.Unlock()
	}()
	if !p.original() || ctx == nil || ctx.Err() != nil || !p.parent.entry.preparationMu.TryLock() {
		return deny()
	}
	defer p.parent.entry.preparationMu.Unlock()
	h := p.parent
	root, finishRoot, e := h.entry.localCompactionLifetimeContext(ctx)
	if e != nil {
		return deny()
	}
	defer finishRoot()
	call, cancel := context.WithDeadline(root, h.deadline)
	defer cancel()
	call, finish := ownedAuthorityCallContext(call)
	defer finish()
	p.mu.Lock()
	if p.cleared || p.publicationAttempted || h.failed.Load() || !h.captured.Load() || h.source == nil || h.source.state == nil || h.source.entry != h.entry || call.Err() != nil || ctx.Err() != nil {
		p.mu.Unlock()
		return deny()
	}
	p.publicationAttempted = true
	p.publicationActive = true
	p.publicationCancel = cancel
	p.publicationDone = make(chan struct{})
	started = true
	p.mu.Unlock()
	s := h.source
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.self != s || s.state.cleared || s.state.view == nil || h.entry.terminalOutputs == nil || p.canonical == nil ||
		p.plan.Validate() != nil || !localRetainedBytes(p.plan.Capsule, p.capsule) || p.plan.FrontierDigest != s.state.digest ||
		p.plan.Scope != ownedOperationScope(h.entry.parent) || uint64(len(p.capsule)) > h.entry.terminalOutputs.CapsuleLimit() {
		return deny()
	}
	alive := func() bool {
		return ctx.Err() == nil && call.Err() == nil && h.entry.invocationContext.Err() == nil && !h.failed.Load()
	}
	if !alive() || chargeOwnedAuthorityBody(call, uint64(len(p.capsule))) != nil {
		return deny()
	}
	store := h.entry.owner.store
	var postimage domain.Run
	// This latch lives OUTSIDE the SDK callback. An attempted/unknown mutation
	// cannot turn into fresh registration on a retry, even if it did not persist.
	attempted := false
	if store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		if attempted || !alive() {
			return q.budget.deny()
		}
		run, e := s.rejoinRunLocked(ctx, q, nil, nil)
		if e != nil || run.TerminalCustody != nil {
			return q.budget.deny()
		}
		run.TerminalCustody = &domain.RunTerminalCustodyV1{Version: 1, Plan: p.plan}
		payload, e := marshalRunPayload(run)
		if e != nil || run.Validate() != nil || !alive() {
			return q.budget.deny()
		}
		postimage = run.Clone()
		attempted = true
		_, e = q.ExecContext(ctx, localRetainedRunUpdateSQL, payload, run.TenantID, run.ID)
		return e
	}) != nil || !attempted || !alive() {
		return deny()
	}
	// A confirmed commit alone is insufficient. Use a NEW transaction/cache to
	// read the exact complete persisted postimage and all original current joins.
	rejoin := func(ctx context.Context, q *ownedAuthorityQuery) error {
		if !alive() {
			return q.budget.deny()
		}
		current, e := h.entry.readCompactionSnapshot(ctx, q)
		if e != nil || !reflect.DeepEqual(current.Job.Run, postimage) {
			return q.budget.deny()
		}
		// Normalize ONLY the exact registered original plan after the complete
		// postimage comparison, not caller/hash-equivalent metadata or status.
		current.Job.Run.TerminalCustody = nil // captured original had no plan
		if !reflect.DeepEqual(current, s.state.snapshot) {
			return q.budget.deny()
		}
		return nil
	}
	if store.ownedAuthorityTx(call, rejoin) != nil || !alive() {
		return deny()
	}
	// SAME physical adapter gate verifies the capsule's exact immutable bytes.
	// Its allowance is irrevocably consumed even on ambiguous PUT/readback.
	if h.entry.terminalOutputs.WriteCapsule(call, p.plan, p.capsule) != nil || !alive() {
		return deny()
	}
	events := p.canonical.Events()
	if len(events) != len(p.drafts) || p.capsuleVersion != 1 && p.capsuleVersion != 2 || p.capsuleVersion == 1 && len(events) != 1 || p.capsuleVersion == 2 && (len(events) < 3 || len(events) > 9 || len(events)%2 != 1) {
		return deny()
	}
	bodies := make([][]byte, len(events))
	for i, event := range events {
		expected := p.drafts[i]
		// One sealed uploader copy, its prepared canonical material and exact
		// verification read all share the original logical materialization cap.
		if !alive() || int64(len(event.Payload)) != expected.Payload.Size || chargeOwnedAuthorityBody(call, 3*uint64(len(event.Payload))) != nil {
			return deny()
		}
		bodies[i] = event.Payload
		if p.capsuleVersion == 1 && (h.entry.terminalOutputs.WriteCanonicalAssistant(call, postimage, expected, event.Payload) != nil || !alive()) {
			return deny()
		}
	}
	if p.capsuleVersion == 2 && (h.entry.terminalOutputs.WriteCanonicalEventsV2(call, postimage, p.drafts, bodies) != nil || !alive()) {
		return deny()
	}
	// Recheck current authority after storage I/O. Registered residue remains on
	// failure; no claim that Session deletion joins other admitted writers yet.
	if store.ownedAuthorityTx(call, rejoin) != nil || !alive() {
		return deny()
	}
	p.mu.Lock()
	if p.cleared || !alive() {
		p.mu.Unlock()
		return deny()
	}
	p.published = true
	p.publicationRun = postimage.Clone()
	p.mu.Unlock()
	return nil
}
