package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Intrinsic on the constructor-fixed original keeper. It cannot be supplied
// per call, replaced by a caller callback or inferred from a free session gate.
type localTerminalKeeperV1 interface{ StopAndJoinTerminal(context.Context) error }

// LocalTerminalCleanupV1 is a private original-owner cleanup record, not local
// execution evidence, correspondence, a canonical commit or deletion readiness.
// Only the later SAME Store finalizer may consume its actual private facts.
type LocalTerminalCleanupV1 struct {
	self       *LocalTerminalCleanupV1
	owner      *LocalPreparedOutputV1
	allocation ports.OwnedAllocationRegistrationReceiptV2
	erasure    *operationbody.NamespaceErasureV1
	stoppedAt  time.Time
}

func (*LocalTerminalCleanupV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("original local cleanup cannot be serialized")
}
func (*LocalTerminalCleanupV1) String() string     { return "LocalTerminalCleanupV1{private:[redacted]}" }
func (c *LocalTerminalCleanupV1) GoString() string { return c.String() }

// FinishCleanup consumes only the exact successful private publication. The
// caller/Source active lifetime may already be cancelled, but the ORIGINAL
// absolute cleanup deadline and logical SQL/materialization budget never reset.
func (p *LocalPreparedOutputV1) FinishCleanup(ctx context.Context) (_ *LocalTerminalCleanupV1, err error) {
	deny := func() (*LocalTerminalCleanupV1, error) { return nil, domain.ErrMembershipDenied }
	started := false
	// As in Publish, signal actual completion LAST: all phase locks and call
	// contexts must already be released when StopAndJoin observes this channel.
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
	call, cancel := context.WithDeadline(ctx, h.deadline)
	defer cancel()
	call, finish := ownedAuthorityCallContext(call)
	defer finish()
	p.mu.Lock()
	if p.cleared || !p.published || p.publicationActive || p.cleanupAttempted || h.failed.Load() || p.publicationRun.Validate() != nil || call.Err() != nil {
		p.mu.Unlock()
		return deny()
	}
	p.cleanupAttempted = true
	// Reuse the original owner's actual-I/O join slot. Direct cleanup callers
	// are therefore joined by StopAndJoin/Close just like direct publishers.
	p.publicationActive = true
	p.publicationCancel = cancel
	p.publicationDone = make(chan struct{})
	started = true
	p.mu.Unlock()
	tuple := h.entry.owner.ready.Load()
	if tuple == nil || tuple.store != h.entry.owner.store || tuple.bridge == nil || !sameLocalOwnerPointer(tuple.keeper, h.entry.owner.keeper) {
		return deny()
	}
	keeper, ok := tuple.keeper.(localTerminalKeeperV1)
	if !ok || keeper.StopAndJoinTerminal(call) != nil || tuple.bridge.StopAndJoinLocalTerminal(call, h.entry.allocation) != nil || call.Err() != nil {
		return deny()
	}
	stoppedAt := time.Now().UTC().Truncate(time.Microsecond)
	s := h.source
	if s == nil || s.self != s || s.entry != h.entry || s.state == nil {
		return deny()
	}
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.cleared || s.state.view == nil || p.publicationRun.TerminalCustody == nil || p.publicationRun.TerminalCustody.Plan != p.plan {
		return deny()
	}
	original, oe := h.entry.allocation.Digest()
	if oe != nil {
		return deny()
	}
	retired, re := tuple.store.retireBoundedOwnedAllocation(call, ownedAllocationParentRequest(h.entry.parent.Reservation), string(original), domain.RegisteredPreparationRetiredV1)
	if re != nil || retired.Current.State != domain.RegisteredPreparationRetiredV1 || !reflect.DeepEqual(retired.Original, h.entry.allocation) || retired.Current.ValidateTransitionFrom(h.entry.allocation) != nil || call.Err() != nil {
		return deny()
	}
	rejoin := func(ctx context.Context, q *ownedAuthorityQuery) error { return p.rejoinCleanupLocked(ctx, q, retired) }
	if tuple.store.ownedAuthorityTx(call, rejoin) != nil || call.Err() != nil {
		return deny()
	}
	// Exact original bytes bind the semantic work performed by the authentic
	// composer before publication. No active-Source fit/decoder is replayed after
	// revocation or erasure, and no digest DATA creates producer ownership.
	capsule, ce := h.entry.readCompactionBlob(call, p.plan.Capsule, h.entry.terminalOutputs.CapsuleLimit())
	equal := bytes.Equal(capsule, p.capsule)
	clear(capsule)
	if ce != nil || !equal || call.Err() != nil {
		return deny()
	}
	for _, event := range p.canonical.Events() {
		var ref domain.BlobRef
		for _, draft := range p.drafts {
			if draft.ID == event.ID {
				ref = draft.Payload
				break
			}
		}
		body, e := h.entry.readCompactionBlob(call, ref, uint64(len(event.Payload)))
		equal := bytes.Equal(body, event.Payload)
		clear(body)
		if e != nil || !equal || call.Err() != nil {
			return deny()
		}
	}
	if p.verifyCleanupRetained(call) != nil || call.Err() != nil {
		return deny()
	}
	// All callback/retry/materialization capacity is spent BEFORE destructive
	// I/O. A lost retirement ACK can honestly exhaust the shared three callbacks;
	// it must then stop here rather than acquire a new budget and erase anyway.
	if tuple.store.ownedAuthorityTx(call, rejoin) != nil || call.Err() != nil || chargeOwnedAuthorityBody(call, h.entry.terminalOutputs.OperationBodyLimit()) != nil {
		return deny()
	}
	erased, ee := h.entry.terminalOutputs.CloseOperationNamespace(call)
	if ee != nil || h.entry.terminalOutputs.VerifyNamespaceErasure(erased) != nil || tuple.bridge.VerifyStoppedLocalTerminal(h.entry.allocation) != nil || call.Err() != nil {
		return deny()
	}
	result := &LocalTerminalCleanupV1{owner: p, allocation: retired, erasure: erased, stoppedAt: stoppedAt}
	result.self = result
	p.mu.Lock()
	if p.cleared || call.Err() != nil {
		p.mu.Unlock()
		return deny()
	}
	p.cleanup = result
	p.mu.Unlock()
	return result, nil
}

func (p *LocalPreparedOutputV1) rejoinCleanupLocked(ctx context.Context, q *ownedAuthorityQuery, retired ports.OwnedAllocationRegistrationReceiptV2) error {
	h := p.parent
	entry := h.entry
	store := entry.owner.store
	parent, source, burn, e := store.readOwnedAllocationParent(ctx, q, ownedAllocationParentRequest(entry.parent.Reservation))
	if e != nil || !reflect.DeepEqual(parent, entry.parent) || !reflect.DeepEqual(source, entry.source) {
		return q.budget.deny()
	}
	alloc, e := readOwnedAllocationSnapshots(ctx, q, parent, source, burn)
	if e != nil || alloc == nil || !reflect.DeepEqual(alloc.original, entry.allocation) || !reflect.DeepEqual(alloc.current, retired.Current) || alloc.receipt(false).Digest != retired.Digest {
		return q.budget.deny()
	}
	tuple := entry.owner.ready.Load()
	if tuple == nil || tuple.store != store || tuple.bridge.VerifyStoppedLocalTerminal(entry.allocation) != nil {
		return q.budget.deny()
	}
	// Deletion registration and custody publication serialize on this existing
	// Session fence. Any unexpected pre-existing deletion also denies erasure.
	var deletion string
	e = q.QueryRowContext(ctx, `SELECT state FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`, entry.parent.Scope.TenantID, entry.parent.Scope.SessionID).Scan(&deletion)
	if !errors.Is(e, sql.ErrNoRows) {
		return q.budget.deny()
	}
	current, e := entry.readLocalPreparedSnapshotBody(ctx, q, localSnapshotOwner{}, localCompactionCapture)
	if e != nil || !reflect.DeepEqual(current.Job.Run, p.publicationRun) {
		return q.budget.deny()
	}
	current.Job.Run.TerminalCustody = nil
	if !reflect.DeepEqual(current, h.source.state.snapshot) {
		return q.budget.deny()
	}
	return nil
}

func (p *LocalPreparedOutputV1) verifyCleanupRetained(ctx context.Context) error {
	metadata := p.publicationRun.DerivedContext
	if metadata == nil {
		return nil
	}
	if metadata.ValidateForRun(p.publicationRun) != nil || metadata.Head == nil || len(metadata.Inventory) == 0 || len(metadata.Inventory) > 2 {
		return domain.ErrMembershipDenied
	}
	policy := p.parent.entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if policy == nil {
		return domain.ErrMembershipDenied
	}
	var previous *compactioncontext.RetainedV1
	var last string
	for _, plan := range metadata.Inventory {
		summary, e := p.parent.entry.readCompactionBlob(ctx, plan.Summary, 32<<10)
		if e != nil {
			return domain.ErrMembershipDenied
		}
		manifest, e := p.parent.entry.readCompactionBlob(ctx, plan.Manifest, 128<<10)
		if e != nil {
			clear(summary)
			return domain.ErrMembershipDenied
		}
		decoded, e := compactioncontext.DecodeRetainedV1(p.parent.entry.owner.configuration, *policy, plan, summary, manifest, previous)
		clear(summary)
		clear(manifest)
		if e != nil {
			return domain.ErrMembershipDenied
		}
		previous = decoded
		last, e = plan.Digest()
		if e != nil {
			return domain.ErrMembershipDenied
		}
	}
	if metadata.Head.PlanDigest != last || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}
