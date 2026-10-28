package providerloopsession

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type localEntryRoute uint8

const (
	localEntryUnselected localEntryRoute = iota
	localEntryDirect
	localEntryWorker
)

// Keep Done/Deadline/Value on the original standard context. Explicit Err
// gates also sample the raw callers, rather than trusting callback delivery.
// Cancel closes that SAME standard Done before Err can return non-nil.
type localEntryCallerContext struct {
	context.Context
	cancel  context.CancelFunc
	callers [2]context.Context
}

func (c localEntryCallerContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	for _, caller := range c.callers {
		if caller.Err() != nil {
			c.cancel()
			break
		}
	}
	return c.Context.Err()
}

// Callers hold the session gate or mu; context identities are immutable once
// installed. Cleanup deliberately does not use this active-admission guard.
func (s *localState) callersCanceled() bool {
	return s.caller.Err() != nil ||
		(s.entryCaller != nil && s.entryCaller.Err() != nil) ||
		(s.entryHandoffCaller != nil && s.entryHandoffCaller.Err() != nil)
}

// localWorkerState is a single-invocation facade, not worker registration or an
// execution preparer. All state operations use the SAME factory Store. Only
// effect reservation is routed through its original bootstrap's fresh slot.
type localWorkerState struct {
	ports.WorkerStateStore
	self  *localWorkerState
	owner *LocalV1
}

var _ ports.AttemptEffectStoreV1 = (*localWorkerState)(nil)

// PreflightWorkerJob binds the original installed owner/profile to the actual
// canonical Job/Final/current sources BEFORE a worker claims any lease. The
// original Store repeats all authority checks at reserve/entry/send; success
// here is not a capability and cannot substitute for those joins.
func (v *LocalV1) PreflightWorkerJob(ctx context.Context, expected ports.WorkerJobState) error {
	s, call, finish, err := v.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if s.started || s.entryRoute != localEntryUnselected || s.bootstrap.PreflightLocalWorkerJob(call, expected) != nil || ctx.Err() != nil || call.Err() != nil {
		return ErrSession
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.callersCanceled() {
		return ErrSession
	}
	return nil
}

// WorkerState returns the original factory-owned state facade. It does not
// enable serving, issue grants, or supply V1 substrate execution evidence.
// A Manager V2 composition must still consume its result with EnterWorkerEffect.
func (v *LocalV1) WorkerState() (ports.WorkerStateStore, error) {
	if v == nil || v.state == nil {
		return nil, ErrSession
	}
	s := v.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.terminalRequested || s.ctx.Err() != nil || s.caller.Err() != nil {
		return nil, ErrSession
	}
	if s.workerState == nil {
		f := &localWorkerState{WorkerStateStore: s.store, owner: &LocalV1{state: s}}
		f.self = f
		s.workerState = f
	}
	return s.workerState, nil
}

func (f *localWorkerState) ReserveAttemptEffect(ctx context.Context, request ports.ReserveAttemptEffectRequestV1) (ports.ReserveAttemptEffectResultV1, error) {
	if f == nil || f.self != f || f.owner == nil {
		return ports.ReserveAttemptEffectResultV1{}, ErrSession
	}
	s, call, finish, err := f.owner.begin(ctx)
	if err != nil {
		return ports.ReserveAttemptEffectResultV1{}, err
	}
	defer finish()
	if s.entryRoute == localEntryDirect || request.Validate() != nil {
		return ports.ReserveAttemptEffectResultV1{}, ErrSession
	}
	first := s.entryRoute == localEntryUnselected
	if first {
		// Sticky route selection precedes any potentially ambiguous mutation.
		// This is not freshness: the bootstrap alone owns the private slot.
		s.entryRoute = localEntryWorker
		if _, err := s.retainEntryContext(ctx); err != nil {
			return ports.ReserveAttemptEffectResultV1{}, ErrSession
		}
	}
	// Never pass begin's short context to the first reservation: its return
	// would cancel the pending invocation. Repeats only observe the same claim.
	reserveCtx := call
	if first {
		reserveCtx = s.entryContext
	}
	result, err := s.bootstrap.ReserveLocalWorkerEffect(reserveCtx, request)
	if err != nil || ctx.Err() != nil || call.Err() != nil || s.entryCaller.Err() != nil {
		return ports.ReserveAttemptEffectResultV1{}, ErrSession
	}
	return result, nil
}

func (f *localWorkerState) RecordAttemptEffectReconciliation(ctx context.Context, record ports.AttemptEffectReconciliationRecordV1) error {
	if f == nil || f.self != f || f.owner == nil {
		return ErrSession
	}
	s, call, finish, err := f.owner.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	// Preserve the Store's actual reconciliation contract; an absent local
	// handle is not synthesized into not_found or permission to resend.
	return s.store.RecordAttemptEffectReconciliation(call, record)
}

// EnterWorkerEffect consumes the SAME bootstrap's private confirmed-fresh
// reservation. Mutable result DATA, copies, and recovered results cannot mint
// an entry. A rejected loser does not cancel or close a winning invocation.
func (v *LocalV1) EnterWorkerEffect(ctx context.Context, reserved ports.ReserveAttemptEffectResultV1) (domain.OwnedAttemptEffectParentV1, error) {
	s, call, finish, err := v.begin(ctx)
	if err != nil {
		return domain.OwnedAttemptEffectParentV1{}, err
	}
	defer finish()
	if s.entryRoute != localEntryWorker || s.started || s.entryContext == nil || s.entryCaller.Err() != nil {
		return domain.OwnedAttemptEffectParentV1{}, ErrSession
	}
	entry, err := s.bootstrap.EnterLocalWorkerEffect(ctx, reserved, s.blobs)
	if err != nil {
		// The bootstrap itself handles cleanup only AFTER consuming its slot.
		// Do not cancel the common owner for an unclaimed/foreign DATA result.
		return domain.OwnedAttemptEffectParentV1{}, ErrSession
	}
	s.started = true
	return s.installEntry(ctx, call, entry)
}

// RunWorkerLoop consumes the already-entered original Manager invocation once.
// It shares the direct routes' finite engine, but never starts or reserves a
// second parent and never owns a credential lifecycle. The original session
// gate and absolute lifetime cover the whole MODEL/COMPACTION/MCP sequence;
// terminal publication, cleanup and canonical completion remain with Manager.
func (v *LocalV1) RunWorkerLoop(ctx context.Context) (result ModelStepResultV1, err error) {
	s, call, finish, err := v.begin(ctx)
	if err != nil {
		return result, err
	}
	defer finish()
	if s.entryRoute != localEntryWorker || !s.started || s.entry == nil || s.credentialed || s.attempted || s.runtimeStarted || s.runtimeFailed {
		return result, ErrSession
	}
	// Claim before the engine validates its policy: an early failure must not
	// leave a retryable entry. Invalid/duplicate callers above do not cancel a
	// different invocation that already owns this shared gate.
	s.runtimeStarted = true
	defer func() {
		if err != nil {
			s.runtimeFailed = true
			s.cancel()
		}
	}()
	return s.runBoundedEngine(ctx, call)
}

// Both routes retain the first caller's absolute lifetime, independently of
// the short gate call. Callers hold s.gate; Close only accesses these fields
// under s.mu. No later reservation or handoff renews this context.
func (s *localState) retainEntryContext(ctx context.Context) (context.Context, error) {
	entryCtx, cancel := context.WithCancel(s.ctx)
	if deadline, ok := ctx.Deadline(); ok {
		cancel()
		entryCtx, cancel = context.WithDeadline(s.ctx, deadline)
	}
	stop := context.AfterFunc(ctx, cancel)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.entryContext != nil || ctx.Err() != nil || entryCtx.Err() != nil {
		cancel()
		stop()
		return nil, ErrSession
	}
	retained := localEntryCallerContext{Context: entryCtx, cancel: cancel, callers: [2]context.Context{s.caller, ctx}}
	s.entryContext, s.entryCaller = retained, ctx
	s.entryCancel, s.stopEntryCaller = cancel, stop
	return retained, nil
}

func (s *localState) installEntry(ctx, call context.Context, entry *ydbstore.LocalInvocationEntryV1) (domain.OwnedAttemptEffectParentV1, error) {
	if entry == nil {
		s.cancel()
		return domain.OwnedAttemptEffectParentV1{}, ErrSession
	}
	parent, pe := entry.Parent()
	source, se := entry.Source()
	allocation, ae := entry.Allocation()
	bodies, be := entry.Bodies()
	if pe != nil || se != nil || ae != nil || be != nil || bodies == nil || ctx.Err() != nil ||
		call.Err() != nil || s.callersCanceled() || s.entryContext.Err() != nil {
		s.cancel()
		return domain.OwnedAttemptEffectParentV1{}, ErrSession
	}
	// Enter already narrowed its original allocation to the handoff lifetime.
	// Retain that narrowing in the session too; never retain the gate call.
	deadline := allocation.Window.ExpiresAt
	if handoffDeadline, ok := ctx.Deadline(); ok && handoffDeadline.Before(deadline) {
		deadline = handoffDeadline
	}
	execution, cancel := context.WithDeadline(s.entryContext, deadline)
	stop := context.AfterFunc(ctx, cancel)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || ctx.Err() != nil || s.callersCanceled() || execution.Err() != nil {
		cancel()
		stop()
		s.cancel()
		return domain.OwnedAttemptEffectParentV1{}, ErrSession
	}
	// This is the winning handoff only. Admission samples it synchronously;
	// an AfterFunc callback may legitimately be delayed after cancellation.
	s.entryHandoffCaller = ctx
	s.execution, s.executionCancel = execution, func() { stop(); cancel() }
	s.entry, s.parent, s.source, s.allocation, s.bodies = entry, parent.Clone(), source.Clone(), allocation.Clone(), bodies
	return parent.Clone(), nil
}
