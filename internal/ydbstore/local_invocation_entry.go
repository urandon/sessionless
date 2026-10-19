package ydbstore

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// The Store owns this fixed reader from construction. Arming its private
// immutable binding does not replace Store.attemptOperationBodies or authorize
// any operation; the same concrete scoped adapter performs every body read.
type localOperationBodySlot struct {
	owner  *LocalOwnedStoreBootstrapV1
	bodies atomic.Pointer[operationbody.StoreV1]
}

func (slot *localOperationBodySlot) ReadAttemptOperationBody(ctx context.Context, scope domain.AttemptOperationScopeV1, ref string, cap uint64) ([]byte, error) {
	if slot == nil || slot.owner == nil || slot.owner.self != slot.owner || slot.owner.bodySlot != slot || ctx == nil || ctx.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	bodies := slot.bodies.Load()
	if bodies == nil {
		return nil, domain.ErrMembershipDenied
	}
	return bodies.ReadAttemptOperationBody(ctx, scope, ref, cap)
}

// LocalInvocationEntryV1 retains original canonical DATA and the actual scoped
// body adapter. It is not constructible by metadata matching and is not a send
// capability/current-state assertion. Accessors remain historical after Source
// retirement; new effects still require the entered bridge and current Store.
type LocalInvocationEntryV1 struct {
	self              *LocalInvocationEntryV1
	owner             *LocalOwnedStoreBootstrapV1
	parent            domain.OwnedAttemptEffectParentV1
	source            domain.AttemptExecutionSourceV1
	allocation        domain.RegisteredAllocationV2
	bodies            *operationbody.StoreV1
	terminalOutputs   *operationbody.TerminalOutputV1
	canonicalBlobs    ports.BlobStore
	invocationContext context.Context
	// Copies retain the same one-use slot; only original() may consume it.
	// Keep the no-copy atomic in its own allocation, not in this identity DTO.
	compactionGeneration *atomic.Pointer[localCompactionGenerationV1]
	preparedModel        *atomic.Pointer[localPreparedModelV1]
	preparationMu        *sync.Mutex
	terminal             *atomic.Pointer[LocalTerminalV1]
}

func (entry *LocalInvocationEntryV1) original() bool {
	return entry != nil && entry.self == entry && entry.owner != nil && entry.owner.self == entry.owner &&
		entry.owner.entry.Load() == entry && entry.owner.bodySlot != nil && entry.bodies != nil && entry.owner.bodySlot.bodies.Load() == entry.bodies && entry.compactionGeneration != nil && entry.preparedModel != nil && entry.preparationMu != nil && entry.terminal != nil
}
func (entry *LocalInvocationEntryV1) Parent() (domain.OwnedAttemptEffectParentV1, error) {
	if !entry.original() {
		return domain.OwnedAttemptEffectParentV1{}, domain.ErrMembershipDenied
	}
	return entry.parent.Clone(), nil
}
func (entry *LocalInvocationEntryV1) Source() (domain.AttemptExecutionSourceV1, error) {
	if !entry.original() {
		return domain.AttemptExecutionSourceV1{}, domain.ErrMembershipDenied
	}
	return entry.source.Clone(), nil
}
func (entry *LocalInvocationEntryV1) Allocation() (domain.RegisteredAllocationV2, error) {
	if !entry.original() {
		return domain.RegisteredAllocationV2{}, domain.ErrMembershipDenied
	}
	return entry.allocation.Clone(), nil
}
func (entry *LocalInvocationEntryV1) Bodies() (*operationbody.StoreV1, error) {
	if !entry.original() {
		return nil, domain.ErrMembershipDenied
	}
	return entry.bodies, nil
}

// StartLocalInvocation is the bootstrap's sole owner, not a public generic
// allocation publisher. Only a confirmed fresh canonical reservation can reach
// preparation; after the fail-fast claim, any failure/ambiguous result permanently
// exhausts this start. Invalid inputs and an incomplete constructor do not claim.
// Existing preinstalled-body compositions keep their prior path and cannot use
// this late-body entry. Source and the shared keeper/model server are not owned
// or closed here. Every SQL phase retains its existing bounded retry protocol;
// the bridge retains the ORIGINAL caller context, never a setup-only child.
func (b *LocalOwnedStoreBootstrapV1) StartLocalInvocation(ctx context.Context, request ports.ReserveAttemptEffectRequestV1, blobs ports.BlobStore) (_ *LocalInvocationEntryV1, err error) {
	if b == nil || b.self != b || ctx == nil || ctx.Err() != nil || request.Validate() != nil || blobs == nil {
		return nil, domain.ErrMembershipDenied
	}
	value := reflect.ValueOf(blobs)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return nil, domain.ErrMembershipDenied
		}
	}
	reserved, err := b.ReserveLocalWorkerEffect(ctx, request)
	if err != nil {
		return nil, err
	}
	return b.EnterLocalWorkerEffect(ctx, reserved, blobs)
}

// ReserveLocalWorkerEffect is the original owned worker effect port. It does
// not change generic Store.ReserveAttemptEffect or infer a serving registration.
// The same bootstrap's direct Start and factory-owned worker facade use it.
func (b *LocalOwnedStoreBootstrapV1) ReserveLocalWorkerEffect(ctx context.Context, request ports.ReserveAttemptEffectRequestV1) (ports.ReserveAttemptEffectResultV1, error) {
	deny := func() (ports.ReserveAttemptEffectResultV1, error) {
		return ports.ReserveAttemptEffectResultV1{}, domain.ErrMembershipDenied
	}
	if b == nil || b.self != b || ctx == nil || ctx.Err() != nil || request.Validate() != nil {
		return deny()
	}
	tuple := b.ready.Load()
	if tuple == nil || tuple.store == nil || tuple.store.localBootstrap != b || tuple.bridge == nil || b.bodySlot == nil || tuple.store.attemptOperationBodies != b.bodySlot {
		return deny()
	}
	if !b.startClaimed.CompareAndSwap(false, true) {
		b.mu.Lock()
		finished := b.reserveFinished
		b.mu.Unlock()
		if !finished {
			return deny()
		}
		return tuple.store.observeBoundedOwnedAttemptEffect(ctx, request)
	}
	defer func() { b.mu.Lock(); b.reserveFinished = true; b.mu.Unlock() }()
	reserved, err := tuple.store.reserveBoundedOwnedAttemptEffect(ctx, request)
	if err != nil || reserved.Validate() != nil || ctx.Err() != nil {
		return deny()
	}
	if reserved.Status != ports.AttemptEffectOwnedV1 {
		return tuple.store.observeBoundedOwnedAttemptEffect(ctx, request)
	}
	if reserved.Grant == nil || b.prepare.VerifyGrant(reserved.Grant.Clone()) != nil {
		return deny()
	}
	lifetime, err := b.source.ExecutionLifetime()
	if err != nil || lifetime.Err() != nil || ctx.Err() != nil {
		return deny()
	}
	if request.UpstreamIdempotencyKeyDigest != nil {
		value := *request.UpstreamIdempotencyKeyDigest
		request.UpstreamIdempotencyKeyDigest = &value
	}
	copy := cloneLocalFreshResult(reserved)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ready.Load() != tuple || b.pendingStart != nil || b.entry.Load() != nil || ctx.Err() != nil || lifetime.Err() != nil {
		return deny()
	}
	b.pendingStart = &localFreshReservationV1{invocation: ctx, request: request, result: copy, tuple: tuple}
	return cloneLocalFreshResult(copy), nil
}

func cloneLocalFreshResult(v ports.ReserveAttemptEffectResultV1) ports.ReserveAttemptEffectResultV1 {
	v.Reservation = v.Reservation.Clone()
	if v.Grant != nil {
		c := v.Grant.Clone()
		v.Grant = &c
	}
	if v.ObservationGrant != nil {
		c := v.ObservationGrant.Clone()
		v.ObservationGrant = &c
	}
	return v
}

// EnterLocalWorkerEffect consumes private confirmed freshness, never caller
// Status. A losing/copy caller has no cleanup ownership over a winning entry.
func (b *LocalOwnedStoreBootstrapV1) EnterLocalWorkerEffect(ctx context.Context, reserved ports.ReserveAttemptEffectResultV1, blobs ports.BlobStore) (_ *LocalInvocationEntryV1, err error) {
	if b == nil || b.self != b || ctx == nil || ctx.Err() != nil || blobs == nil || reserved.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	value := reflect.ValueOf(blobs)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return nil, domain.ErrMembershipDenied
		}
	}
	b.mu.Lock()
	pending := b.pendingStart
	if pending == nil || pending.tuple != b.ready.Load() || !reflect.DeepEqual(pending.result, reserved) {
		b.mu.Unlock()
		return nil, domain.ErrMembershipDenied
	}
	b.pendingStart = nil // irreversible BEFORE grant consumption, burn or preflight
	b.mu.Unlock()
	tuple, request := pending.tuple, pending.request
	if tuple == nil || tuple.store == nil || tuple.store.localBootstrap != b || tuple.bridge == nil || b.bodySlot == nil || tuple.store.attemptOperationBodies != b.bodySlot {
		return nil, domain.ErrMembershipDenied
	}
	succeeded := false
	var finishInvocation func()
	defer func() {
		if !succeeded {
			if finishInvocation != nil {
				finishInvocation()
			}
			_ = tuple.bridge.Close()
		}
	}()
	ctx, finishInvocation, err = b.localWorkerInvocationContext(pending.invocation, ctx)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	if ctx.Err() != nil || reserved.Status != ports.AttemptEffectOwnedV1 || reserved.Grant == nil || b.prepare.VerifyGrant(reserved.Grant.Clone()) != nil {
		return nil, domain.ErrMembershipDenied
	}
	grant := reserved.Grant.Clone()
	var parent domain.OwnedAttemptEffectParentV1
	var source domain.AttemptExecutionSourceV1
	var outputLimits domain.ProductLimits
	err = tuple.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		parent, source = domain.OwnedAttemptEffectParentV1{}, domain.AttemptExecutionSourceV1{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		original, e := readOwnedAttemptEffectParent(ctx, q, request)
		if e != nil || original == nil || !q.at.Before(original.PreparationExpiresAt) || !grant.GrantExpiresAt.Equal(original.PreparationExpiresAt) || tuple.store.validateCurrentOwnedAttemptParent(ctx, q, request, *original) != nil {
			return q.budget.deny()
		}
		gd, ge := grant.Reservation.DigestForAuthority(grant.Authority)
		pd, pe := original.Reservation.DigestForAuthority(original.Authority)
		rd, re := reserved.Reservation.DigestForAuthority(grant.Authority)
		if ge != nil || pe != nil || re != nil || gd != pd || gd != rd {
			return q.budget.deny()
		}
		final, e := readOwnedAdmission(ctx, q, original.Scope)
		if e != nil || final == nil || original.ValidateForAdmission(*final) != nil || tuple.store.ownedAttemptEffectSourceVerifier.VerifyOwnedAttemptEffectSource(final.Source.Clone()) != nil {
			return q.budget.deny()
		}
		burned, e := readOwnedPreparationBurn(ctx, q, *original)
		if e != nil || burned {
			return q.budget.deny()
		}
		loaded, e := loadOwnedEffectWorkerJob(ctx, q, request)
		if e != nil {
			return q.budget.deny()
		}
		outputLimits = loaded.Job.Limits
		parent, source = original.Clone(), final.Source.Clone()
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	bodies, err := operationbody.NewV1(ownedOperationScope(parent), parent.Authority.HarnessBinding.AgentLoopPolicy.Limits, blobs)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	terminalOutputs, err := bodies.AdmitTerminalOutput(ctx, outputLimits, parent.Authority.SubstrateBinding.Limits)
	if err != nil || ctx.Err() != nil || terminalOutputs.OperationBodyLimit() <= ownedSetupBodyBytes(parent) {
		return nil, domain.ErrMembershipDenied
	}
	publication, err := tuple.bridge.PrepareAllocationPublication(ctx, grant)
	if err != nil || ctx.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	registered, err := tuple.store.publishBoundedOwnedAllocation(ctx, publication)
	if err != nil || ctx.Err() != nil || registered.Replayed || registered.Current.LocalRuntime == nil || registered.Current.State != domain.RegisteredPreparationActiveV1 || registered.Current.SourceDigest == "" {
		return nil, domain.ErrMembershipDenied
	}
	sd, se := source.Digest()
	rd, re := parent.Reservation.DigestForAuthority(parent.Authority)
	if se != nil || re != nil || registered.Current.SourceDigest != sd || registered.Current.ParentReservationDigest != rd {
		return nil, domain.ErrMembershipDenied
	}
	if tuple.bridge.EnterOwnedLocalAllocation(ctx, publication) != nil || ctx.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	lifetime, e := b.source.ExecutionLifetime()
	if e != nil || lifetime.Err() != nil || ctx.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	entry := &LocalInvocationEntryV1{owner: b, parent: parent.Clone(), source: source.Clone(), allocation: registered.Current.Clone(), bodies: bodies, terminalOutputs: terminalOutputs, canonicalBlobs: blobs, invocationContext: ctx, compactionGeneration: new(atomic.Pointer[localCompactionGenerationV1]), preparedModel: new(atomic.Pointer[localPreparedModelV1]), preparationMu: new(sync.Mutex), terminal: new(atomic.Pointer[LocalTerminalV1])}
	entry.self = entry
	if !b.bodySlot.bodies.CompareAndSwap(nil, bodies) {
		return nil, domain.ErrMembershipDenied
	}
	b.entry.Store(entry)
	succeeded = true
	return entry, nil
}

// Retain the ORIGINAL reserve invocation, not a setup-only child or a later
// handoff's renewed lifetime. Direct Start already passes its retained entry
// context. Standard context propagation cancels its descendants synchronously;
// Source and the independent handoff also narrow this context and its deadline.
type localWorkerCallerContext struct {
	context.Context
	cancel  context.CancelFunc
	callers [3]context.Context
}

func (c localWorkerCallerContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	for _, caller := range c.callers {
		if caller.Err() != nil {
			// Never return a fabricated Err with a live Done channel. Cancel
			// only this original standard context; cleanup has its own owner.
			c.cancel()
			break
		}
	}
	return c.Context.Err()
}

func (b *LocalOwnedStoreBootstrapV1) localWorkerInvocationContext(original, handoff context.Context) (context.Context, func(), error) {
	if original == nil || handoff == nil || original.Err() != nil || handoff.Err() != nil {
		return nil, nil, domain.ErrMembershipDenied
	}
	life, err := b.source.ExecutionLifetime()
	if err != nil || life.Err() != nil {
		return nil, nil, domain.ErrMembershipDenied
	}
	deadline, bounded := original.Deadline()
	for _, ctx := range []context.Context{life, handoff} {
		if end, ok := ctx.Deadline(); ok && (!bounded || end.Before(deadline)) {
			deadline, bounded = end, true
		}
	}
	parent := localCompactionCallerValues{Context: original, caller: context.WithoutCancel(handoff)}
	var call context.Context
	var cancel context.CancelFunc
	if bounded {
		call, cancel = context.WithDeadline(parent, deadline)
	} else {
		call, cancel = context.WithCancel(parent)
	}
	stopSource := context.AfterFunc(life, cancel)
	stopHandoff := context.AfterFunc(handoff, cancel)
	finish := func() { stopSource(); stopHandoff(); cancel() }
	// A winning entry retains this context; release its hooks when the original
	// invocation, Source, handoff or absolute deadline ends, not on Start return.
	context.AfterFunc(call, func() { stopSource(); stopHandoff() })
	if original.Err() != nil || handoff.Err() != nil || life.Err() != nil || call.Err() != nil {
		finish()
		return nil, nil, domain.ErrMembershipDenied
	}
	// The explicit entry.invocationContext.Err gates sample the fixed raw
	// lifetime chain even when cancellation callbacks have not run. Standard
	// child contexts still use the original call's cancellation identity.
	return localWorkerCallerContext{Context: call, cancel: cancel, callers: [3]context.Context{original, handoff, life}}, finish, nil
}
