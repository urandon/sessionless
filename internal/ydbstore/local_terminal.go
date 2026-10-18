package ydbstore

import (
	"context"
	"sync/atomic"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// LocalTerminalV1 is the original entry's one-use capture owner. It is NOT a
// stop/join, retirement, custody, erasure or canonical-completion receipt.
// The provider session must independently seal the SAME idle Keeper before
// capture, then prepare output before stopping the original bridge.
type LocalTerminalV1 struct {
	self     *LocalTerminalV1
	entry    *LocalInvocationEntryV1
	deadline time.Time
	failed   atomic.Bool
	captured atomic.Bool
	// Guarded by the SAME entry preparation barrier. Only Capture installs this
	// original source; a caller's matching DATA cannot replace it for planning.
	source          *LocalCompactionSourceV1
	outputAttempted bool
	output          *LocalPreparedOutputV1
}

// One original barrier covers preparation, reservation/authorization and
// retained publication. Fail-fast acquisition is bounded: no abandoned waiter
// can acquire it later, and no source->entry lock inversion is permitted.
func (entry *LocalInvocationEntryV1) beginLocalMutation(ctx context.Context) (func(), error) {
	if !entry.original() || ctx == nil || ctx.Err() != nil || entry.terminal.Load() != nil || !entry.preparationMu.TryLock() {
		return nil, domain.ErrMembershipDenied
	}
	if ctx.Err() != nil || entry.terminal.Load() != nil {
		entry.preparationMu.Unlock()
		return nil, domain.ErrMembershipDenied
	}
	return entry.preparationMu.Unlock, nil
}

// The still-supported explicit first-operation path has no prepared MODEL or
// generation owner. Its local SQL/issuer arm needs the SAME entry barrier;
// nonlocal Stores keep their existing contract. Recovery is observation-only.
func (store *Store) lockLocalGenericMutation(ctx context.Context, scope domain.AttemptOperationScopeV1) (func(), error) {
	if store == nil || store.localBootstrap == nil {
		return func() {}, nil
	}
	entry := store.localBootstrap.entry.Load()
	if entry == nil && store.localBootstrap.self == store.localBootstrap && store.localBootstrap.store == store && store.localBootstrap.bodySlot == nil && store.localCompositionReady() {
		// The explicit preinstalled-body constructor has no late-body entry or
		// terminal owner. Preserve that separate existing composition unchanged.
		return func() {}, nil
	}
	if !entry.original() || entry.owner.store != store || scope != ownedOperationScope(entry.parent) {
		return nil, domain.ErrMembershipDenied
	}
	return entry.beginLocalMutation(ctx)
}

// ClaimTerminal latches BEFORE trying the existing mutation barrier. A busy
// original mutation makes this claim permanently ineligible, never queued or
// retried into success. The returned failed owner exposes only its unchanged
// cleanup deadline; Capture denies it. Invalid/copied/repeated claims get nil.
func (entry *LocalInvocationEntryV1) ClaimTerminal(ctx context.Context) (*LocalTerminalV1, error) {
	if !entry.original() || ctx == nil || ctx.Err() != nil || entry.invocationContext == nil || entry.invocationContext.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if policy == nil || policy.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	life, err := entry.owner.source.ExecutionLifetime()
	if err != nil || life.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	duration := min(policy.Limits.MaxCleanupDuration, entry.owner.configuration.Limits.CleanupTimeout, 30*time.Second)
	if duration <= 0 {
		return nil, domain.ErrMembershipDenied
	}
	h := &LocalTerminalV1{entry: entry, deadline: time.Now().Add(duration)}
	h.self = h
	if !entry.terminal.CompareAndSwap(nil, h) {
		return nil, domain.ErrMembershipDenied
	}
	if !entry.preparationMu.TryLock() {
		h.failed.Store(true)
		return h, domain.ErrMembershipDenied
	}
	defer entry.preparationMu.Unlock()
	if ctx.Err() != nil || entry.invocationContext.Err() != nil || life.Err() != nil || !time.Now().Before(h.deadline) {
		h.failed.Store(true)
		return h, domain.ErrMembershipDenied
	}
	return h, nil
}

func (h *LocalTerminalV1) original() bool {
	return h != nil && h.self == h && h.entry.original() && h.entry.terminal.Load() == h
}

// Deadline is historical cleanup DATA, not permission or an extendable window.
func (h *LocalTerminalV1) Deadline() (time.Time, error) {
	if !h.original() {
		return time.Time{}, domain.ErrMembershipDenied
	}
	return h.deadline, nil
}

// Capture reads the original complete known frontier with the existing two
// current joins and bounded SQL/blob budget. It cannot reopen effects or create
// another capture allowance. Success is structural source custody only; this
// owner does not assert the Keeper has been sealed or transport has joined.
func (h *LocalTerminalV1) Capture(ctx context.Context) (*LocalCompactionSourceV1, error) {
	if !h.original() || h.failed.Load() || ctx == nil || ctx.Err() != nil || !h.captured.CompareAndSwap(false, true) {
		return nil, domain.ErrMembershipDenied
	}
	if !h.entry.preparationMu.TryLock() {
		h.failed.Store(true)
		return nil, domain.ErrMembershipDenied
	}
	defer h.entry.preparationMu.Unlock()
	call, cancel := context.WithDeadline(ctx, h.deadline)
	defer cancel()
	if call.Err() != nil {
		h.failed.Store(true)
		return nil, domain.ErrMembershipDenied
	}
	source, err := h.entry.captureCompactionSource(call, localSourceStandalone)
	if err != nil || source == nil {
		h.failed.Store(true)
		return nil, domain.ErrMembershipDenied
	}
	source.state.mu.Lock()
	ops := source.state.operations
	valid := len(ops) > 0 && ops[len(ops)-1].Reservation.Operation.Kind == domain.AttemptOperationModelV1
	source.state.mu.Unlock()
	if !valid || call.Err() != nil {
		source.Clear()
		h.failed.Store(true)
		return nil, domain.ErrMembershipDenied
	}
	h.source = source
	return source, nil
}

func (*LocalTerminalV1) String() string     { return "LocalTerminalV1{private:[redacted]}" }
func (h *LocalTerminalV1) GoString() string { return h.String() }
