package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// A closed original-holder union, never caller facts or an approval interface.
// The zero value is accepted ONLY by standalone closed-frontier capture.
type localSnapshotOwner struct {
	generation *localCompactionGenerationV1
	model      *localPreparedModelV1
}

func (o localSnapshotOwner) present() bool { return (o.generation != nil) != (o.model != nil) }
func (o localSnapshotOwner) original(e *LocalInvocationEntryV1, p localCompactionGenerationPhase) bool {
	if !o.present() {
		return false
	}
	if o.generation != nil {
		return o.generation.originalForPhase(e, p)
	}
	return o.model.originalForPhase(e, p)
}
func (o localSnapshotOwner) operation() domain.AttemptOperationV1 {
	if o.generation != nil {
		return o.generation.operation
	}
	if o.model != nil {
		return o.model.operation
	}
	return domain.AttemptOperationV1{}
}
func (o localSnapshotOwner) futureProxyDigest(q *ownedAuthorityQuery, p *ownedProxySnapshot) (domain.RegisteredProxyAttestationDigestV2, error) {
	if o.generation != nil {
		return o.generation.futureProxyDigest(q, p)
	}
	if o.model != nil {
		return o.model.entry.localFutureProxyDigest(q, p, o.model.operation)
	}
	return "", q.budget.deny()
}

// All mutable fields are protected by source.state.mu. Entry owns one slot,
// shared by wrapper copies; ambiguous preparation/reservation is never reset.
type localPreparedModelV1 struct {
	self                 *localPreparedModelV1
	entry                *LocalInvocationEntryV1
	source               *LocalCompactionSourceV1
	frozen               *configuredinference.FrozenRequestV1
	fit                  configuredinference.ContextFitV1
	price                *ownedProviderPriceEvidence
	operation            domain.AttemptOperationV1
	reservationAttempted bool
	reservation          domain.AttemptOperationReservationV1
	observation          ports.AuthenticatedAttemptOperationObservationV1
}

func (m *localPreparedModelV1) originalForPhase(entry *LocalInvocationEntryV1, phase localCompactionGenerationPhase) bool {
	if m == nil || m.self != m || m.entry != entry || !entry.original() || entry.preparedModel.Load() != m || m.source == nil || m.source.self != m.source || m.source.entry != entry || m.source.state == nil || m.source.state.cleared ||
		m.frozen == nil || m.price == nil || m.price.local == nil || m.price.local.source != entry.owner.source || !entry.owner.store.localInvoiceLifetimeCurrent(m.price.local) ||
		entry.invocationContext == nil || entry.invocationContext.Err() != nil || m.operation.Kind != domain.AttemptOperationModelV1 || m.operation.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil || m.operation.Scope != ownedOperationScope(entry.parent) || m.operation.Sequence != m.source.state.snapshot.Accounting.Aggregate.Operations+1 ||
		m.fit.RequestDigest != m.frozen.Digest() || m.fit.CalibrationDigest != entry.owner.configuration.CalibrationDigest || m.fit.InputUpperBound != m.operation.Debit.InputTokens || m.fit.OutputReserve != m.operation.Debit.OutputTokens || m.fit.TotalUpperBound != m.fit.InputUpperBound+m.fit.OutputReserve {
		return false
	}
	switch phase {
	case localCompactionReserve:
		return true
	case localCompactionBeforeSend:
		return m.reservation.Validate() == nil && reflect.DeepEqual(m.reservation.Operation, m.operation)
	case localCompactionCompleted:
		return m.reservation.Validate() == nil && reflect.DeepEqual(m.reservation.Operation, m.operation) && m.observation.Validate() == nil && reflect.DeepEqual(m.observation.Reservation, m.reservation) && ownedObservationAllowsSuccessor(m.reservation, m.observation.Observation) && entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(m.observation.Clone()) == nil
	default:
		return false
	}
}

func (m *localPreparedModelV1) Operation() (domain.AttemptOperationV1, error) {
	if m == nil || m.source == nil || m.source.state == nil {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if !m.originalForPhase(m.entry, localCompactionReserve) {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	return m.operation.Clone(), nil
}
func (m *localPreparedModelV1) FrozenRequest() (*configuredinference.FrozenRequestV1, error) {
	if _, err := m.Operation(); err != nil {
		return nil, err
	}
	return m.frozen, nil
}
func (*localPreparedModelV1) String() string { return "LocalPreparedModelV1{private:[redacted]}" }

// PrepareModel derives the entire actual input from the original admitted
// source. No caller request/catalog/fit/parent/reader can replace that producer.
func (entry *LocalInvocationEntryV1) PrepareModel(ctx context.Context) (*localPreparedModelV1, error) {
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
	model, err := entry.prepareModelSource(call, ctx, source, nil)
	if err != nil {
		source.Clear()
	}
	return model, err
}

// A published handle is not a caller-selected context. A new complete source
// is captured; the exact current committed head/retained bytes are reread before
// constructing its ordinary request. The old generation frontier is not reused.
func (g *localCompactionGenerationV1) PrepareNextModel(ctx context.Context) (*localPreparedModelV1, error) {
	if g == nil || g.entry == nil || !g.entry.original() || g.entry.compactionGeneration.Load() != g {
		return nil, domain.ErrMembershipDenied
	}
	entry := g.entry
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
	model, err := entry.prepareModelSource(call, ctx, source, g)
	if err != nil {
		source.Clear()
	}
	return model, err
}

// preparationMu is held. This check never replaces the new source's actual
// current Store join; it prevents abandoning an unfinished/ambiguous owner.
func (entry *LocalInvocationEntryV1) completedModelPreparation() (*localPreparedModelV1, bool) {
	old := entry.preparedModel.Load()
	if old == nil {
		return nil, true
	}
	if old.source == nil || old.source.state == nil {
		return old, false
	}
	old.source.state.mu.Lock()
	defer old.source.state.mu.Unlock()
	return old, old.originalForPhase(entry, localCompactionCompleted)
}

// Reads the exact retained parent from the original owner and ORIGINAL blob
// reader. It deliberately does not rejoin the prior generation's stale frontier:
// the new source rejoins ALL subsequent completed operations before any PUT.
func (entry *LocalInvocationEntryV1) capturedRetainedParent(ctx context.Context, source *LocalCompactionSourceV1, required *localCompactionGenerationV1) (*compactioncontext.RetainedV1, error) {
	metadata := source.state.snapshot.Job.Run.DerivedContext
	g := entry.compactionGeneration.Load()
	if metadata == nil {
		if g != nil || required != nil {
			return nil, domain.ErrMembershipDenied
		}
		return nil, nil
	}
	if metadata.Head == nil || g == nil || (required != nil && required != g) || g.self != g || g.source == nil || g.source.state == nil {
		return nil, domain.ErrMembershipDenied
	}
	g.source.state.mu.Lock()
	defer g.source.state.mu.Unlock()
	if g.publication == nil || !domain.EqualRunDerivedContextV1(metadata, &g.publication.postimage) {
		return nil, domain.ErrMembershipDenied
	}
	digest, err := g.publication.plan.Digest()
	if err != nil || metadata.Head.PlanDigest != digest {
		return nil, domain.ErrMembershipDenied
	}
	retained, _, err := g.readRetainedLocked(ctx, g.publication.plan)
	return retained, err
}

func (entry *LocalInvocationEntryV1) prepareModelSource(ctx context.Context, originalCaller context.Context, source *LocalCompactionSourceV1, required *localCompactionGenerationV1) (*localPreparedModelV1, error) {
	deny := func() (*localPreparedModelV1, error) { return nil, domain.ErrMembershipDenied }
	if source == nil || source.self != source || source.entry != entry || source.state == nil {
		return deny()
	}
	old, ready := entry.completedModelPreparation()
	if !ready {
		return deny()
	}
	source.state.mu.Lock()
	defer source.state.mu.Unlock()
	if source.state.cleared || source.state.view == nil {
		return deny()
	}
	retained, err := entry.capturedRetainedParent(ctx, source, required)
	if err != nil {
		return deny()
	}
	var parent *compactioncontext.ParentV1
	if retained != nil {
		parent = retained.Parent()
	}
	store := entry.owner.store
	var at time.Time
	var profile domain.OwnedAgentProfileCurrentV1
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		if _, err := source.rejoinRunLocked(ctx, q, nil, nil); err != nil {
			return err
		}
		var err error
		profile, err = readOwnedProfileCurrent(ctx, q, entry.source.Profile)
		at = q.at
		return err
	})
	if err != nil || ctx.Err() != nil {
		return deny()
	}
	price, err := store.readOwnedProviderPriceEvidence(ctx, profile)
	if err != nil || price == nil || price.local == nil || price.price != nil {
		return deny()
	}
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if policy == nil {
		return deny()
	}
	used := source.state.snapshot.Accounting.Aggregate
	limits := policy.Limits
	configuration := entry.owner.configuration
	if used.Operations >= limits.MaxOperations || used.ModelCalls >= limits.MaxModelCalls || used.InputTokens >= limits.MaxAggregateInputTokens || used.OutputTokens >= limits.MaxAggregateOutputTokens || used.BodyBytes >= limits.MaxAggregateBodyBytes || used.ActiveDuration >= limits.MaxActiveDuration {
		return deny()
	}
	effective := configuration.Limits
	effective.MaxInputTokens = min(effective.MaxInputTokens, limits.MaxInputTokensPerCall, limits.MaxAggregateInputTokens-used.InputTokens)
	effective.MaxOutputTokens = min(effective.MaxOutputTokens, limits.MaxOutputTokensPerCall, limits.MaxAggregateOutputTokens-used.OutputTokens)
	records := source.state.view.Records()
	defer func() {
		for i := range records {
			clear(records[i].Payload)
		}
	}()
	operations := make([]compactioncontext.CompletedOperationV1, len(source.state.operations))
	var bytes uint64
	for i, o := range source.state.operations {
		var request struct {
			Tools []json.RawMessage `json:"tools"`
		}
		if json.Unmarshal(o.Request, &request) != nil || len(request.Tools) != 0 {
			return deny()
		}
		operations[i] = compactioncontext.CompletedOperationV1{Reservation: o.Reservation, Observation: o.Observation, Request: o.Request, Response: o.Response}
		bytes += uint64(len(o.Request) + len(o.Response))
	}
	for _, r := range records {
		bytes += uint64(len(r.Payload))
	}
	if chargeOwnedAuthorityBody(ctx, bytes+configuredinference.MaxContextCalibrationBytesV1) != nil {
		return deny()
	}
	frozen, err := compactioncontext.BuildOrdinaryV1(configuration, effective, *policy, source.state.digest, records, operations, parent, nil)
	if err != nil {
		if errors.Is(err, configuredinference.ErrContextNoFit) {
			return nil, configuredinference.ErrContextNoFit
		}
		return deny()
	}
	fit, err := entry.owner.source.FitContext(frozen, configuredinference.ContextFitCeilingsV1{InputTokens: effective.MaxInputTokens, OutputTokens: effective.MaxOutputTokens, TotalTokens: configuration.ModelWindowTokens})
	if err != nil {
		if errors.Is(err, configuredinference.ErrContextNoFit) {
			return nil, configuredinference.ErrContextNoFit
		}
		return deny()
	}
	op, err := entry.localPreparedModelOperation(originalCaller, source, frozen, fit, price, profile, at)
	if err != nil {
		return deny()
	}
	m := &localPreparedModelV1{entry: entry, source: source, frozen: frozen, fit: fit, price: price}
	m.self = m
	if !entry.preparedModel.CompareAndSwap(old, m) {
		return deny()
	}
	body := frozen.Body()
	defer clear(body)
	if chargeOwnedAuthorityBody(ctx, uint64(len(body))) != nil {
		return deny()
	}
	ref, err := entry.bodies.Write(ctx, body)
	if err != nil || ctx.Err() != nil || ref.Digest != op.RequestDigest || ref.Bytes != op.RequestBytes {
		return deny()
	}
	op.RequestRef = ref.Ref
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		if _, err := source.rejoinRunLocked(ctx, q, nil, nil); err != nil {
			return err
		}
		if !q.at.Before(op.Deadline) || store.validateOwnedOperationProviderPrice(ctx, q, op, price, entry.source) != nil {
			return q.budget.deny()
		}
		_, err := debitOwnedOperation(source.state.snapshot.Accounting, op, entry.parent, entry.source, q.at)
		return err
	})
	if err != nil || ctx.Err() != nil || !store.operationEvidenceLifetimeCurrent(&ownedOperationEvidence{price: price}) {
		return deny()
	}
	m.operation = op
	return m, nil
}

func (entry *LocalInvocationEntryV1) localPreparedModelOperation(ctx context.Context, source *LocalCompactionSourceV1, frozen *configuredinference.FrozenRequestV1, fit configuredinference.ContextFitV1, price *ownedProviderPriceEvidence, profile domain.OwnedAgentProfileCurrentV1, at time.Time) (domain.AttemptOperationV1, error) {
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	configuration := entry.owner.configuration
	used := source.state.snapshot.Accounting.Aggregate
	limits := policy.Limits
	duration := min(limits.MaxNetworkOperationDuration, configuration.Limits.OperationTimeout, limits.MaxActiveDuration-used.ActiveDuration)
	deadline := at.Add(duration)
	for _, end := range []time.Time{entry.parent.Authority.InvocationDeadline, entry.parent.Authority.Lease.ExpiresAt, policy.ExpiresAt, entry.parent.Authority.AdmissionCostCeiling.PriceExpiresAt, entry.parent.Reservation.ReservedAt.Add(limits.MaxActiveDuration), entry.allocation.Window.ExpiresAt, configuration.ExpiresAt} {
		if end.Before(deadline) {
			deadline = end
		}
	}
	life, err := entry.owner.source.ExecutionLifetime()
	if err != nil {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	for _, parent := range []context.Context{life, entry.invocationContext} {
		if end, ok := parent.Deadline(); ok && end.Before(deadline) {
			deadline = end
		}
	}
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	if !deadline.After(at) {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	body := frozen.Body()
	defer clear(body)
	hash := sha256.Sum256(body)
	cost, _ := entry.parent.Authority.AdmissionCostCeiling.Digest()
	op := domain.AttemptOperationV1{Version: 1, Scope: ownedOperationScope(entry.parent), Sequence: used.Operations + 1, Kind: domain.AttemptOperationModelV1, CallID: "model-" + source.state.digest[:24], RequestRef: "pending-model-body", RequestDigest: hex.EncodeToString(hash[:]), RequestBytes: uint64(len(body)), ResultBytes: min(uint64(64<<10), configuration.Limits.ResponseBytes), ContextBytes: uint64(len(body)), Currency: entry.parent.Authority.AdmissionCostCeiling.Currency, PriceRevision: entry.parent.Authority.AdmissionCostCeiling.PriceRevision, AdmissionCostCeilingDigest: cost, Deadline: deadline, CleanupDuration: limits.MaxCleanupDuration}
	if n := len(source.state.operations); n > 0 {
		op.PredecessorObservationDigest, _ = source.state.operations[n-1].Observation.Digest()
	}
	op.Debit = domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: fit.InputUpperBound, OutputTokens: fit.OutputReserve, BodyBytes: op.RequestBytes + op.ResultBytes, ResponseBytes: op.ResultBytes, ActiveDuration: deadline.Sub(at)}
	if op.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil || op.Debit.BodyBytes > limits.MaxAggregateBodyBytes-used.BodyBytes || entry.owner.store.validateOwnedLocalInvoiceEvidence(price.local, profile, entry.source, op, at) != nil {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	if _, err := debitOwnedOperation(source.state.snapshot.Accounting, op, entry.parent, entry.source, at); err != nil {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	return op, nil
}

func (m *localPreparedModelV1) rejoinLocked(ctx context.Context, q *ownedAuthorityQuery, phase localCompactionGenerationPhase) (domain.Run, error) {
	if !m.originalForPhase(m.entry, phase) {
		return domain.Run{}, q.budget.deny()
	}
	current, err := m.entry.readLocalPreparedSnapshot(ctx, q, localSnapshotOwner{model: m}, phase)
	if err != nil {
		return domain.Run{}, err
	}
	expected := m.source.state.snapshot
	run := current.Job.Run.Clone()
	if phase == localCompactionReserve {
		if len(current.Operations) != len(expected.Operations) || len(current.ProxyDigests) != len(expected.ProxyDigests)+1 {
			return domain.Run{}, q.budget.deny()
		}
	} else {
		if len(current.Operations) != len(expected.Operations)+1 || len(current.ProxyDigests) != len(expected.ProxyDigests)+1 {
			return domain.Run{}, q.budget.deny()
		}
		tail := current.Operations[len(current.Operations)-1]
		if !reflect.DeepEqual(tail.reservation, m.reservation) || (phase == localCompactionBeforeSend && tail.observation != nil) || (phase == localCompactionCompleted && (tail.observation == nil || !reflect.DeepEqual(*tail.observation, m.observation.Observation))) {
			return domain.Run{}, q.budget.deny()
		}
		next, e := debitOwnedOperation(expected.Accounting, m.operation, m.entry.parent, m.entry.source, m.reservation.ReservedAt)
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
	return run, nil
}

// This comparison is used only after the closed original holder has validated
// its identity. A proxy record or operation DTO alone is never a serving grant.
func (entry *LocalInvocationEntryV1) localFutureProxyDigest(q *ownedAuthorityQuery, p *ownedProxySnapshot, op domain.AttemptOperationV1) (domain.RegisteredProxyAttestationDigestV2, error) {
	if p == nil || !q.at.Before(op.Deadline) || p.current.ValidateAt(q.at) != nil || op.Deadline.After(p.current.Window.ExpiresAt) {
		return "", q.budget.deny()
	}
	od, oe := op.Digest()
	ad, ae := entry.allocation.Digest()
	if oe != nil || ae != nil || p.original.RegisteredAllocationDigest != ad || p.original.OperationSequence != op.Sequence || p.original.OperationDigest != od || p.original.ServiceKind != domain.RegisteredProxyProviderV1 {
		return "", q.budget.deny()
	}
	d, err := p.original.Digest()
	if err != nil {
		return "", q.budget.deny()
	}
	return d, nil
}

func localModelBodyEqual(f *configuredinference.FrozenRequestV1, body []byte) bool {
	exact := f.Body()
	defer clear(exact)
	return bytes.Equal(exact, body)
}
