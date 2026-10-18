package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Closed PRIVATE read shapes. Capture never accepts an in-flight operation or
// future proxy. These other shapes belong only to an original prepared handle;
// none is a caller-controlled permission or an alternative Store/send API.
type localCompactionGenerationPhase uint8

const (
	localCompactionCapture localCompactionGenerationPhase = iota
	localCompactionReserve
	localCompactionBeforeSend
	localCompactionCompleted
	localCompactionPostRegistration
	localCompactionPublished
)

// The private constructor rebuilds from original source bytes and executes the
// original calibration. The original Store consumes this owner for its prepared
// reserve/before-send/completed phases. Fields other than immutable
// entry/source/self are read or filled under source.mu;
// the slot may hold an incomplete, permanently denied failure tombstone.
type localCompactionGenerationV1 struct {
	self      *localCompactionGenerationV1
	entry     *LocalInvocationEntryV1
	source    *LocalCompactionSourceV1
	retained  *LocalRetainedCompactionV1
	frozen    *configuredinference.FrozenRequestV1
	operation domain.AttemptOperationV1
	program   *compactioncontext.GenerationV1
	fit       configuredinference.ContextFitV1
	price     *ownedProviderPriceEvidence
	// Completion fields are private and accessed under source.state.mu.
	reservationAttempted bool
	reservation          domain.AttemptOperationReservationV1
	observation          ports.AuthenticatedAttemptOperationObservationV1
	publicationAttempted bool
	publication          *localCompactionPublicationV1
	parentRetained       *compactioncontext.RetainedV1
}

// localCompactionLifetimeContext is context composition, not permission. Its
// parent is the ORIGINAL Source lifetime so Source revocation synchronously
// cancels all standard context children before Close returns. Independent
// caller/invocation cancellation is also joined, with direct initial checks.
// Caller values, including the existing logical-call budget, survive without
// borrowing the caller's cancellation identity.
func (entry *LocalInvocationEntryV1) localCompactionLifetimeContext(ctx context.Context) (context.Context, func(), error) {
	if !entry.original() || ctx == nil || ctx.Err() != nil || entry.invocationContext == nil || entry.invocationContext.Err() != nil {
		return nil, nil, domain.ErrMembershipDenied
	}
	life, err := entry.owner.source.ExecutionLifetime()
	if err != nil || life.Err() != nil {
		return nil, nil, domain.ErrMembershipDenied
	}
	deadline, bounded := life.Deadline()
	for _, original := range []context.Context{ctx, entry.invocationContext} {
		if end, ok := original.Deadline(); ok && (!bounded || end.Before(deadline)) {
			deadline, bounded = end, true
		}
	}
	var call context.Context
	var cancel context.CancelFunc
	parent := localCompactionCallerValues{Context: life, caller: context.WithoutCancel(ctx)}
	if bounded {
		call, cancel = context.WithDeadline(parent, deadline)
	} else {
		call, cancel = context.WithCancel(parent)
	}
	stopCaller := context.AfterFunc(ctx, cancel)
	stopInvocation := context.AfterFunc(entry.invocationContext, cancel)
	finish := func() { stopCaller(); stopInvocation(); cancel() }
	if ctx.Err() != nil || life.Err() != nil || entry.invocationContext.Err() != nil || call.Err() != nil {
		finish()
		return nil, nil, domain.ErrMembershipDenied
	}
	return call, finish, nil
}

// WithoutCancel masks the caller's private cancelCtx key. Falling back to the
// embedded Source for that key keeps its Value/Done identity coherent, so the
// standard context child is synchronously canceled by Source revocation.
type localCompactionCallerValues struct {
	context.Context
	caller context.Context
}

func (c localCompactionCallerValues) Value(key any) any {
	if value := c.caller.Value(key); value != nil {
		return value
	}
	return c.Context.Value(key)
}

// prepareLocalCompactionGeneration is deliberately private. It cannot accept
// a caller request/catalog/policy/calibration/parent/fit, reserve an operation,
// open a transport or authorize a send. Unknown/failed preparation cannot reset
// the original entry slot. A replacement requires the exact original published
// predecessor and a newly captured complete current operation frontier.
func prepareLocalCompactionGeneration(ctx context.Context, source *LocalCompactionSourceV1) (*localCompactionGenerationV1, error) {
	if source == nil || source.entry == nil || !source.entry.original() {
		return nil, domain.ErrMembershipDenied
	}
	unlock, err := source.entry.beginLocalMutation(ctx)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	defer unlock()
	return prepareLocalCompactionGenerationLocked(ctx, ctx, source)
}

func (entry *LocalInvocationEntryV1) PrepareCompaction(ctx context.Context) (*localCompactionGenerationV1, error) {
	if !entry.original() {
		return nil, domain.ErrMembershipDenied
	}
	unlock, err := entry.beginLocalMutation(ctx)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	defer unlock()
	call, finish, err := entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	call, cancel := ownedAuthorityCallContext(call)
	defer cancel()
	source, err := entry.captureCompactionSource(call, localSourcePreparation)
	if err != nil {
		return nil, err
	}
	g, err := prepareLocalCompactionGenerationLocked(call, ctx, source)
	if err != nil {
		source.Clear()
	}
	return g, err
}

func (g *localCompactionGenerationV1) Operation() (domain.AttemptOperationV1, error) {
	if g == nil || g.source == nil || g.source.state == nil {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	g.source.state.mu.Lock()
	defer g.source.state.mu.Unlock()
	if !g.originalForPhase(g.entry, localCompactionReserve) {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	return g.operation.Clone(), nil
}
func (g *localCompactionGenerationV1) FrozenRequest() (*configuredinference.FrozenRequestV1, error) {
	if _, err := g.Operation(); err != nil {
		return nil, err
	}
	return g.frozen, nil
}
func (g *localCompactionGenerationV1) Publish(ctx context.Context) (*configuredinference.FrozenRequestV1, error) {
	return publishLocalCompaction(ctx, g)
}

func prepareLocalCompactionGenerationLocked(ctx context.Context, originalCaller context.Context, source *LocalCompactionSourceV1) (*localCompactionGenerationV1, error) {
	deny := func() (*localCompactionGenerationV1, error) { return nil, domain.ErrMembershipDenied }
	if ctx == nil || ctx.Err() != nil || source == nil || source.self != source || source.state == nil || source.entry == nil || !source.entry.original() {
		return deny()
	}
	source.state.mu.Lock()
	defer source.state.mu.Unlock()
	entry, state := source.entry, source.state
	if state.cleared || state.view == nil || state.digest == "" {
		return deny()
	}
	if _, ready := entry.completedModelPreparation(); !ready {
		return deny()
	}
	previousGeneration := entry.compactionGeneration.Load()
	parentRetained, err := entry.capturedRetainedParent(ctx, source, nil)
	if err != nil {
		return deny()
	}
	var parent *compactioncontext.ParentV1
	if parentRetained != nil {
		parent = parentRetained.Parent()
		if parent.Plan.Generation != 1 {
			return deny()
		}
	}
	if state.snapshot.Job.Run.DerivedContext != nil && len(state.snapshot.Job.Run.DerivedContext.Inventory) >= 2 {
		return deny()
	}
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if policy == nil || policy.Validate() != nil {
		return deny()
	}
	// There is no current native-catalog owner in this base. Never silently drop
	// a captured catalog or treat a prior compaction as an ordinary message.
	operations := make([]compactioncontext.CompletedOperationV1, len(state.operations))
	for i, operation := range state.operations {
		var request struct {
			Tools []json.RawMessage `json:"tools"`
		}
		if (operation.Reservation.Operation.Kind != domain.AttemptOperationModelV1 && (operation.Reservation.Operation.Kind != domain.AttemptOperationCompactionV1 || parent == nil)) || json.Unmarshal(operation.Request, &request) != nil || len(request.Tools) != 0 {
			return deny()
		}
		operations[i] = compactioncontext.CompletedOperationV1{Reservation: operation.Reservation, Observation: operation.Observation, Request: operation.Request, Response: operation.Response}
	}
	root, finishRoot, err := entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return deny()
	}
	defer finishRoot()
	call, finish, err := entry.retainedContext(root, entry.compactionSourceControl())
	if err != nil {
		return deny()
	}
	defer finish()
	call, cancel := ownedAuthorityCallContext(call)
	defer cancel()
	store := entry.owner.store
	var at time.Time
	var currentProfile domain.OwnedAgentProfileCurrentV1
	err = store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		at, currentProfile = time.Time{}, domain.OwnedAgentProfileCurrentV1{}
		if _, err := source.rejoinRunLocked(ctx, q, nil, nil); err != nil {
			return err
		}
		var err error
		currentProfile, err = readOwnedProfileCurrent(ctx, q, entry.source.Profile)
		if err != nil {
			return err
		}
		at = q.at
		return nil
	})
	if err != nil || call.Err() != nil {
		return deny()
	}
	price, err := store.readOwnedProviderPriceEvidence(call, currentProfile)
	if err != nil || price == nil || price.local == nil || price.price != nil {
		return deny()
	}
	configuration := entry.owner.configuration
	limits, used := policy.Limits, state.snapshot.Accounting.Aggregate
	if used.Operations >= limits.MaxOperations || used.ModelCalls >= limits.MaxModelCalls || used.CompactionCalls >= limits.MaxCompactionCalls || (parent == nil && used.CompactionCalls != 0) || (parent != nil && used.CompactionCalls != 1) || used.InputTokens >= limits.MaxAggregateInputTokens || used.OutputTokens >= limits.MaxAggregateOutputTokens || used.BodyBytes >= limits.MaxAggregateBodyBytes || used.ActiveDuration >= limits.MaxActiveDuration {
		return deny()
	}
	effective := configuration.Limits
	effective.MaxInputTokens = min(effective.MaxInputTokens, limits.MaxInputTokensPerCall, limits.MaxAggregateInputTokens-used.InputTokens)
	effective.MaxOutputTokens = min(effective.MaxOutputTokens, limits.MaxOutputTokensPerCall, limits.MaxAggregateOutputTokens-used.OutputTokens)
	records := state.view.Records()
	defer func() {
		for i := range records {
			clear(records[i].Payload)
		}
	}()
	var bodyBytes uint64
	for _, record := range records {
		bodyBytes += uint64(len(record.Payload))
	}
	for _, operation := range operations {
		bodyBytes += uint64(len(operation.Request) + len(operation.Response))
	}
	if chargeOwnedAuthorityBody(call, bodyBytes+configuredinference.MaxContextCalibrationBytesV1) != nil {
		return deny()
	}
	program, err := compactioncontext.BuildGenerationV1(configuration, effective, *policy, state.digest, records, operations, parent, nil)
	if err != nil {
		if errors.Is(err, configuredinference.ErrContextNoFit) {
			return nil, configuredinference.ErrContextNoFit
		}
		return deny()
	}
	if program.Number() != used.CompactionCalls+1 || program.SourceDigest() != state.digest {
		return deny()
	}
	frozen := program.FrozenRequest()
	fit, err := entry.owner.source.FitContext(frozen, configuredinference.ContextFitCeilingsV1{InputTokens: effective.MaxInputTokens, OutputTokens: effective.MaxOutputTokens, TotalTokens: configuration.ModelWindowTokens})
	if err != nil {
		if errors.Is(err, configuredinference.ErrContextNoFit) {
			return nil, configuredinference.ErrContextNoFit
		}
		return deny()
	}
	body := frozen.Body()
	defer clear(body)
	if chargeOwnedAuthorityBody(call, uint64(len(body))) != nil {
		return deny()
	}
	duration := min(limits.MaxNetworkOperationDuration, configuration.Limits.OperationTimeout, limits.MaxActiveDuration-used.ActiveDuration)
	deadline := at.Add(duration)
	for _, original := range []time.Time{entry.parent.Authority.InvocationDeadline, entry.parent.Authority.Lease.ExpiresAt, policy.ExpiresAt, entry.parent.Authority.AdmissionCostCeiling.PriceExpiresAt, entry.parent.Reservation.ReservedAt.Add(limits.MaxActiveDuration), entry.allocation.Window.ExpiresAt, configuration.ExpiresAt} {
		if original.Before(deadline) {
			deadline = original
		}
	}
	lifetime, err := entry.owner.source.ExecutionLifetime()
	if err != nil {
		return deny()
	}
	for _, original := range []context.Context{lifetime, entry.invocationContext} {
		if end, ok := original.Deadline(); ok && end.Before(deadline) {
			deadline = end
		}
	}
	if end, ok := originalCaller.Deadline(); ok && end.Before(deadline) {
		return deny()
	} // never silently shorten the immutable operation for an independent caller
	if !deadline.After(at) {
		return deny()
	}
	duration = deadline.Sub(at)
	sum := sha256.Sum256(body)
	costDigest, err := entry.parent.Authority.AdmissionCostCeiling.Digest()
	if err != nil {
		return deny()
	}
	op := domain.AttemptOperationV1{Version: 1, Scope: ownedOperationScope(entry.parent), Sequence: used.Operations + 1, Kind: domain.AttemptOperationCompactionV1, CallID: "compaction-" + state.digest[:24] + "-" + strconv.FormatUint(program.Number(), 10), RequestRef: "pending-compaction-body", RequestDigest: hex.EncodeToString(sum[:]), RequestBytes: uint64(len(body)), ResultBytes: min(uint64(64<<10), configuration.Limits.ResponseBytes), ContextBytes: uint64(len(body)), SummaryBytes: min(uint64(16<<10), limits.MaxSummaryBytes), Currency: entry.parent.Authority.AdmissionCostCeiling.Currency, PriceRevision: entry.parent.Authority.AdmissionCostCeiling.PriceRevision, AdmissionCostCeilingDigest: costDigest, Deadline: deadline, CleanupDuration: limits.MaxCleanupDuration}
	if len(operations) != 0 {
		op.PredecessorObservationDigest, err = operations[len(operations)-1].Observation.Digest()
		if err != nil {
			return deny()
		}
	}
	op.Debit = domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, CompactionCalls: 1, InputTokens: fit.InputUpperBound, OutputTokens: fit.OutputReserve, BodyBytes: op.RequestBytes + op.ResultBytes + operationbody.RetainedPairBytesV1, ResponseBytes: op.ResultBytes, ActiveDuration: duration}
	if op.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil || op.Debit.BodyBytes > limits.MaxAggregateBodyBytes-used.BodyBytes || store.validateOwnedLocalInvoiceEvidence(price.local, currentProfile, entry.source, op, at) != nil {
		return deny()
	}
	if _, err := debitOwnedOperation(state.snapshot.Accounting, op, entry.parent, entry.source, at); err != nil {
		return deny()
	}
	g := &localCompactionGenerationV1{entry: entry, source: source, frozen: frozen, program: program, fit: fit, price: price, parentRetained: parentRetained}
	g.self = g
	if !entry.compactionGeneration.CompareAndSwap(previousGeneration, g) {
		return deny()
	}
	// No reset on any failure, including ambiguous body writes or pair failure.
	// Future effect lookup MUST acquire source.state.mu before reading this
	// partial handle; retained stays nil until complete confirmed preparation.
	ref, err := entry.bodies.Write(call, body)
	if err != nil || call.Err() != nil || ref.Digest != op.RequestDigest || ref.Bytes != op.RequestBytes {
		return deny()
	}
	op.RequestRef = ref.Ref
	if op.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil {
		return deny()
	}
	g.operation = op
	var expected *domain.RunDerivedContextV1
	err = store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		run, err := source.rejoinRunLocked(ctx, q, nil, nil)
		if err != nil {
			return err
		}
		// The complete SECOND-join Run is the expected metadata source. This
		// value must equal the complete captured metadata, including any original
		// retained predecessor, rather than a caller-supplied expected value.
		if !domain.EqualRunDerivedContextV1(run.DerivedContext, state.snapshot.Job.Run.DerivedContext) {
			return q.budget.deny()
		}
		if run.DerivedContext != nil {
			value := run.DerivedContext.Clone()
			expected = &value
		}
		if !q.at.Before(op.Deadline) || store.validateOwnedOperationProviderPrice(ctx, q, op, price, entry.source) != nil {
			return q.budget.deny()
		}
		_, err = debitOwnedOperation(state.snapshot.Accounting, op, entry.parent, entry.source, q.at)
		return err
	})
	if err != nil || call.Err() != nil || !store.operationEvidenceLifetimeCurrent(&ownedOperationEvidence{price: price}) {
		return deny()
	}
	retained := &LocalRetainedCompactionV1{entry: entry, operation: op, generation: program.Number(), claimed: new(atomic.Bool), source: source, expected: expected}
	retained.pair, err = entry.bodies.ReserveRetainedPair(call, op.Sequence, program.Number())
	if err != nil || call.Err() != nil || lifetime.Err() != nil || entry.invocationContext.Err() != nil || !store.operationEvidenceLifetimeCurrent(&ownedOperationEvidence{price: price}) {
		return deny()
	}
	retained.self = retained
	g.retained = retained
	return g, nil
}

func (g *localCompactionGenerationV1) originalForPhase(entry *LocalInvocationEntryV1, phase localCompactionGenerationPhase) bool {
	if g == nil || g.self != g || g.entry != entry || !entry.original() || entry.compactionGeneration.Load() != g || g.source == nil || g.source.self != g.source || g.source.entry != entry || g.source.state == nil || g.source.state.cleared || g.retained == nil || g.retained.self != g.retained || g.retained.entry != entry || g.retained.source != g.source || g.frozen == nil || g.frozen.Digest() == "" || !reflect.DeepEqual(g.retained.operation, g.operation) || g.operation.Kind != domain.AttemptOperationCompactionV1 || g.operation.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil || g.operation.Scope != ownedOperationScope(entry.parent) || g.operation.Sequence != g.source.state.snapshot.Accounting.Aggregate.Operations+1 {
		return false
	}
	switch phase {
	case localCompactionReserve:
		return true
	case localCompactionBeforeSend:
		return g.reservation.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) == nil && reflect.DeepEqual(g.reservation.Operation, g.operation)
	case localCompactionCompleted, localCompactionPostRegistration, localCompactionPublished:
		if phase == localCompactionPublished && (g.publication == nil || g.publication.plan.Validate() != nil || g.publication.postimage.Validate() != nil) {
			return false
		}
		return g.observation.Validate() == nil && reflect.DeepEqual(g.observation.Reservation, g.reservation) && reflect.DeepEqual(g.reservation.Operation, g.operation) && ownedObservationAllowsSuccessor(g.reservation, g.observation.Observation) && entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(g.observation.Clone()) == nil
	default:
		return false
	}
}

func (g *localCompactionGenerationV1) futureProxyDigest(q *ownedAuthorityQuery, proxy *ownedProxySnapshot) (domain.RegisteredProxyAttestationDigestV2, error) {
	if g == nil || proxy == nil || !q.at.Before(g.operation.Deadline) || proxy.current.ValidateAt(q.at) != nil || g.operation.Deadline.After(proxy.current.Window.ExpiresAt) {
		return "", q.budget.deny()
	}
	od, oe := g.operation.Digest()
	ad, ae := g.entry.allocation.Digest()
	if oe != nil || ae != nil || proxy.original.RegisteredAllocationDigest != ad || proxy.original.OperationSequence != g.operation.Sequence || proxy.original.OperationDigest != od || proxy.original.ServiceKind != domain.RegisteredProxyProviderV1 {
		return "", q.budget.deny()
	}
	d, err := proxy.original.Digest()
	if err != nil {
		return "", q.budget.deny()
	}
	return d, nil
}

// Caller holds the original source's shared mutex. This reuses the same bounded
// current Store snapshot once, never N operationHistory joins or a new ledger.
func (g *localCompactionGenerationV1) rejoinLocked(ctx context.Context, q *ownedAuthorityQuery, phase localCompactionGenerationPhase) (domain.Run, error) {
	if g == nil || !g.originalForPhase(g.entry, phase) {
		return domain.Run{}, q.budget.deny()
	}
	current, err := g.entry.readLocalPreparedSnapshot(ctx, q, localSnapshotOwner{generation: g}, phase)
	if err != nil {
		return domain.Run{}, q.budget.deny()
	}
	expected := g.source.state.snapshot
	joinedRun := current.Job.Run.Clone()
	if phase == localCompactionPostRegistration || phase == localCompactionPublished {
		uploaded := g.retained.uploaded.Load()
		if uploaded == nil || uploaded.plan.SourceViewDigest != g.source.state.digest || uploaded.plan.Scope != g.operation.Scope || uploaded.plan.OperationSequence != g.operation.Sequence || uploaded.plan.Generation != g.retained.generation {
			return domain.Run{}, q.budget.deny()
		}
		postimage := &uploaded.postimage
		if phase == localCompactionPublished {
			if !reflect.DeepEqual(g.publication.plan, uploaded.plan) {
				return domain.Run{}, q.budget.deny()
			}
			postimage = &g.publication.postimage
		}
		if !domain.EqualRunDerivedContextV1(current.Job.Run.DerivedContext, postimage) {
			return domain.Run{}, q.budget.deny()
		}
		current.Job.Run.DerivedContext = expected.Job.Run.DerivedContext
	}
	if phase == localCompactionReserve {
		if len(current.Operations) != len(expected.Operations) || len(current.ProxyDigests) != len(expected.ProxyDigests)+1 {
			return domain.Run{}, q.budget.deny()
		}
	} else {
		if len(current.Operations) != len(expected.Operations)+1 || len(current.ProxyDigests) != len(expected.ProxyDigests)+1 {
			return domain.Run{}, q.budget.deny()
		}
		tail := current.Operations[len(current.Operations)-1]
		if !reflect.DeepEqual(tail.reservation, g.reservation) {
			return domain.Run{}, q.budget.deny()
		}
		if phase == localCompactionBeforeSend {
			if tail.observation != nil {
				return domain.Run{}, q.budget.deny()
			}
		} else if tail.observation == nil || !reflect.DeepEqual(*tail.observation, g.observation.Observation) {
			return domain.Run{}, q.budget.deny()
		}
		next, e := debitOwnedOperation(expected.Accounting, g.operation, g.entry.parent, g.entry.source, g.reservation.ReservedAt)
		if e != nil || !reflect.DeepEqual(next, current.Accounting) {
			return domain.Run{}, q.budget.deny()
		}
		current.Accounting = expected.Accounting
		current.Operations = current.Operations[:len(current.Operations)-1]
	}
	current.ProxyDigests = current.ProxyDigests[:len(current.ProxyDigests)-1]
	if len(expected.Operations) == 0 {
		current.Operations = nil
		current.ProxyDigests = nil
	}
	if !reflect.DeepEqual(current, expected) {
		return domain.Run{}, q.budget.deny()
	}
	return joinedRun, nil
}
