package ydbstore

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func (store *Store) prepareLocalSearchEvidence(ctx context.Context, op domain.AttemptOperationV1) (*ownedOperationEvidence, error) {
	deny := func() (*ownedOperationEvidence, error) { return nil, domain.ErrMembershipDenied }
	if store == nil || op.Kind != domain.AttemptOperationWebSearchV1 || op.SearchWire == nil || !store.localCompositionReady() || store.localBootstrap == nil {
		return deny()
	}
	entry := store.localBootstrap.entry.Load()
	if !entry.original() || entry.owner.store != store || entry.preparedSearch == nil || entry.terminal.Load() != nil {
		return deny()
	}
	m := entry.preparedSearch.Load()
	if m == nil || m.source == nil || m.source.state == nil {
		return deny()
	}
	unlock, err := entry.beginLocalMutation(ctx)
	if err != nil {
		return deny()
	}
	defer unlock()
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if !m.originalForPhase(entry, localCompactionReserve) || !reflect.DeepEqual(m.operation, op) || !m.lifetimeCurrent() {
		return deny()
	}
	call, finish, err := entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return deny()
	}
	defer finish()
	if chargeOwnedAuthorityBody(call, 2*op.RequestBytes) != nil {
		return deny()
	}
	body, err := store.readOwnedOperationBody(call, op.Scope, op.RequestRef, op.RequestDigest, op.RequestBytes, domain.WebSearchWireBytesV1)
	if err != nil {
		return deny()
	}
	defer clear(body)
	if !m.exactRequest(body) || call.Err() != nil || len(m.source.state.operations) == 0 {
		return deny()
	}
	previous := m.source.state.operations[len(m.source.state.operations)-1]
	if !reflect.DeepEqual(previous.Reservation, m.pending.model) || !reflect.DeepEqual(previous.Observation, m.pending.observation) {
		return deny()
	}
	return &ownedOperationEvidence{search: m, previous: previous.Reservation.Clone(), observation: previous.Observation.Clone()}, nil
}

func (m *localPreparedSearchV1) lifetimeCurrent() bool {
	if m == nil || m.entry == nil || m.entry.invocationContext == nil || m.entry.invocationContext.Err() != nil || m.entry.owner == nil || m.entry.owner.source == nil {
		return false
	}
	life, err := m.entry.owner.source.ExecutionLifetime()
	return err == nil && life != nil && life.Err() == nil && m.entry.credentialExecutionActive()
}

func (m *localPreparedSearchV1) rejoinAndValidateCurrent(ctx context.Context, q *ownedAuthorityQuery, phase localCompactionGenerationPhase) error {
	joined, err := m.rejoinSnapshotLocked(ctx, q, phase)
	if err != nil {
		return err
	}
	state, err := m.pending.readJoinedCurrentTx(ctx, q, joined)
	if err != nil || !reflect.DeepEqual(state, m.pending.state) || state.Status != domain.WebSearchConsentApprovedV1 || m.frozen.ValidateConsentRequest(state.Request) != nil {
		return q.budget.deny()
	}
	declaration, err := m.entry.owner.source.ReadSearchCredentialDeclaration(ctx, m.entry.source.Profile)
	if err != nil || declaration != m.pending.declaration || !m.pending.grant.sourceCurrentAt(q.at) || m.pending.grant.grant.ValidateCostAt(*m.entry.parent.Authority.AdmissionCostCeiling.WebSearchCost, q.at) != nil || !m.lifetimeCurrent() {
		return q.budget.deny()
	}
	return m.entry.terminalFitSourceLocked(ctx, q, m.source, m.operation, nil)
}

func (store *Store) reservePreparedLocalSearch(ctx context.Context, op domain.AttemptOperationV1, evidence *ownedOperationEvidence) (ports.ReserveAttemptOperationResultV1, error) {
	deny := func() (ports.ReserveAttemptOperationResultV1, error) {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	if evidence == nil || evidence.search == nil {
		return deny()
	}
	m := evidence.search
	if m.source == nil || m.source.state == nil {
		return deny()
	}
	unlock, err := m.entry.beginLocalMutation(ctx)
	if err != nil {
		return deny()
	}
	defer unlock()
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if !m.originalForPhase(m.entry, localCompactionReserve) || !reflect.DeepEqual(op, m.operation) || m.reservationAttempted || m.reservation.Version != 0 || !m.lifetimeCurrent() || !reflect.DeepEqual(evidence.previous, m.pending.model) || !reflect.DeepEqual(evidence.observation, m.pending.observation) || op.ValidatePredecessor(evidence.previous, evidence.observation) != nil {
		return deny()
	}
	caller := ctx
	call, finish, err := m.entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return deny()
	}
	defer finish()
	var candidate domain.AttemptOperationReservationV1
	var at time.Time
	err = store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		candidate, at = domain.AttemptOperationReservationV1{}, time.Time{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if m.reservationAttempted || m.rejoinAndValidateCurrent(ctx, q, localCompactionReserve) != nil {
			return q.budget.deny()
		}
		parent, source := m.entry.parent, m.entry.source
		allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, source, m.source.state.snapshot.Accounting.SetupReservedAt)
		if err != nil || allocation == nil || allocation.current.ValidateAt(q.at) != nil || op.Deadline.After(allocation.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		proxy, err := readOwnedProxySnapshotAt(ctx, q, parent, source, allocation.original, op.Sequence)
		if err != nil || proxy == nil || proxy.current.ValidateAt(q.at) != nil || op.Deadline.After(proxy.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		od, oe := op.Digest()
		ad, ae := allocation.original.Digest()
		pd, pe := m.entry.localFutureSearchProxyDigest(q, proxy, op)
		if oe != nil || ae != nil || pe != nil || proxy.original.RegisteredAllocationDigest != ad || proxy.original.OperationDigest != od {
			return q.budget.deny()
		}
		old, err := readOwnedOperation(ctx, q, op)
		if err != nil || old != nil {
			return q.budget.deny()
		}
		next, err := debitOwnedOperation(m.source.state.snapshot.Accounting, op, parent, source, q.at)
		if err != nil || !store.localOperationBudgetAllows(op.Scope, next.Aggregate.BodyBytes) {
			return q.budget.deny()
		}
		value := domain.AttemptOperationReservationV1{Version: 1, Operation: op.Clone(), OperationDigest: od, ReservedAt: q.at, AggregateDebit: next.Aggregate, RegisteredAllocationDigest: ad, RegisteredProxyDigest: pd, ComponentAmounts: next.Amounts}
		if value.ValidateForParent(parent.Authority, parent.Reservation) != nil {
			return q.budget.deny()
		}
		raw, err := json.Marshal(value)
		if err != nil || len(raw) > ownedAuthorityHeadBytes {
			return q.budget.deny()
		}
		digest, _ := value.Digest()
		// One-shot before the first write. Unknown commits cannot be retried into
		// another reservation or restore any amount/count/byte allowance.
		m.reservationAttempted = true
		if _, err := q.ExecContext(ctx, `INSERT INTO attempt_operations_v1 (tenant_id,attempt_id,sequence,record,digest,operation_digest,call_id) VALUES ($1,$2,$3,$4,$5,$6,$7)`, op.Scope.TenantID, op.Scope.AttemptID, op.Sequence, string(raw), string(digest), string(od), op.CallID); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO attempt_operation_calls_v1 (tenant_id,attempt_id,call_id,sequence,operation_digest) VALUES ($1,$2,$3,$4,$5)`, op.Scope.TenantID, op.Scope.AttemptID, op.CallID, op.Sequence, string(od)); err != nil {
			return err
		}
		if err := writeOwnedAttemptAccounting(ctx, q, parent, source, next); err != nil {
			return err
		}
		candidate, at = value.Clone(), q.at
		return nil
	})
	if err != nil || call.Err() != nil || caller.Err() != nil || !m.lifetimeCurrent() {
		return deny()
	}
	result, err := store.operationResult(candidate, at, true)
	if err != nil || call.Err() != nil || caller.Err() != nil || !m.lifetimeCurrent() {
		return deny()
	}
	m.reservation = candidate.Clone()
	return result, nil
}

func (store *Store) authorizePreparedLocalSearch(ctx context.Context, capability ports.AttemptOperationCapabilityV1, evidence *ownedOperationEvidence) error {
	if evidence == nil || evidence.search == nil || evidence.search.source == nil || evidence.search.source.state == nil {
		return domain.ErrMembershipDenied
	}
	m := evidence.search
	unlock, err := m.entry.beginLocalMutation(ctx)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer unlock()
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if !m.originalForPhase(m.entry, localCompactionBeforeSend) || !reflect.DeepEqual(capability.Reservation, m.reservation) || !m.lifetimeCurrent() {
		return domain.ErrMembershipDenied
	}
	caller := ctx
	call, finish, err := m.entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer finish()
	err = store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if !q.at.Before(capability.ExpiresAt) || q.at.Before(capability.IssuedAt) || m.rejoinAndValidateCurrent(ctx, q, localCompactionBeforeSend) != nil {
			return q.budget.deny()
		}
		allocation, err := readOwnedAllocationSnapshots(ctx, q, m.entry.parent, m.entry.source, m.source.state.snapshot.Accounting.SetupReservedAt)
		if err != nil || allocation == nil || allocation.current.ValidateAt(q.at) != nil || m.operation.Deadline.After(allocation.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		proxy, err := readOwnedProxySnapshotAt(ctx, q, m.entry.parent, m.entry.source, allocation.original, m.operation.Sequence)
		if err != nil || proxy == nil || proxy.current.ValidateAt(q.at) != nil || m.operation.Deadline.After(proxy.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		ad, ae := allocation.current.Digest()
		pd, pe := proxy.current.Digest()
		if ae != nil || pe != nil || ad != m.reservation.RegisteredAllocationDigest || pd != m.reservation.RegisteredProxyDigest || validateRetainedSearchProxyOperationV4(proxy, m.reservation) != nil || !store.localOperationBudgetAllows(m.operation.Scope, m.reservation.AggregateDebit.BodyBytes) {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || caller.Err() != nil || call.Err() != nil || !m.lifetimeCurrent() || store.attemptOperationIssuer.ConsumeAttemptOperationCapability(capability) != nil || caller.Err() != nil || !m.lifetimeCurrent() {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (store *Store) localNormalizedSearch(op domain.AttemptOperationV1, o domain.AttemptOperationObservationV1) bool {
	return op.Kind == domain.AttemptOperationWebSearchV1 && op.SearchWire != nil && o.Outcome == domain.AttemptOperationCompletedV1 && (o.Reason == domain.AttemptOperationReasonNoneV1 || o.Reason == domain.AttemptOperationReasonServiceErrorV1)
}

func (store *Store) validateLocalNormalizedSearch(ctx context.Context, op domain.AttemptOperationV1, o domain.AttemptOperationObservationV1) error {
	if store == nil || store.localBootstrap == nil || !store.localNormalizedSearch(op, o) {
		return domain.ErrMembershipDenied
	}
	entry := store.localBootstrap.entry.Load()
	if !entry.original() {
		return domain.ErrMembershipDenied
	}
	m := entry.findOriginalSearch(op.Sequence)
	if m == nil || !reflect.DeepEqual(op, m.operation) || o.ValidateForReservation(m.reservation) != nil || o.Usage.State != domain.AttemptOperationUsageUnknownV1 {
		return domain.ErrMembershipDenied
	}
	if chargeOwnedAuthorityBody(ctx, 2*o.ResponseBytes) != nil {
		return domain.ErrMembershipDenied
	}
	body, err := store.readOwnedOperationBody(ctx, op.Scope, o.ResponseRef, o.ResponseDigest, o.ResponseBytes, min(op.ResultBytes, uint64(domain.WebSearchResultBytesV1)))
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer clear(body)
	result, err := domain.DecodeWebSearchResultV1(body)
	if err != nil || result.ValidateForOperation(op) != nil || result.ProfileRevision != m.pending.grant.grant.ProfileRevision || result.RetrievedAt.Before(m.reservation.ReservedAt) || result.RetrievedAt.After(o.ObservedAt) || (result.Outcome == "known_service_error") != (o.Reason == domain.AttemptOperationReasonServiceErrorV1) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// Historical issuer/readback custody only. Recording a real observation after
// Source cancellation must not erase the effect or manufacture permission for a
// successor; all fresh/current serving gates remain separate above.
func (store *Store) rememberLocalSearchObservation(ctx context.Context, observed ports.AuthenticatedAttemptOperationObservationV1) {
	if store == nil || store.localBootstrap == nil || observed.Validate() != nil || ctx.Err() != nil {
		return
	}
	entry := store.localBootstrap.entry.Load()
	if !entry.original() {
		return
	}
	m := entry.findOriginalSearch(observed.Reservation.Operation.Sequence)
	if m == nil || m.source == nil || m.source.state == nil {
		return
	}
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if !reflect.DeepEqual(m.reservation, observed.Reservation) || store.attemptOperationIssuer.VerifyAttemptOperationObservation(observed.Clone()) != nil {
		return
	}
	m.observation = observed.Clone()
}

func (store *Store) localSearchPublicationPolicy(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, allocation domain.RegisteredAllocationV2, proof ports.OwnedProxyPublicationV2, bodies ports.OwnedProxyBodiesV2) bool {
	if !store.localCompositionReady() || bodies.LocalSearchHTTPS == nil || proof.ServiceKind != domain.RegisteredProxyWebSearchV1 || len(source.Spending) != 4 {
		return false
	}
	p := bodies.LocalSearchHTTPS.Policy
	profile, err := readOwnedProfileCurrent(ctx, q, source.Profile)
	cost := parent.Authority.AdmissionCostCeiling.WebSearchCost
	gd, ge := p.Grant.Digest()
	return err == nil && ge == nil && cost != nil && p.Grant.ValidateCostAt(*cost, q.at) == nil && profile.WebSearchGrantDigest == gd && source.Spending[3].ScopedGrantDigest == gd && p.ConfiguredProfileDigest == allocation.Backend.BackendProfileDigest && p.Grant.TenantID == source.Scope.TenantID && p.Grant.OwnerUserID == source.Scope.OwnerUserID
}

// Called with the original mutation barrier already held by PublishOwnedProxy.
// An authenticated actual socket/HMAC alone is not authenticated consent.
func (store *Store) localSearchPublicationCurrent(ctx context.Context, q *ownedAuthorityQuery, proof ports.OwnedProxyPublicationV2, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, allocation *ownedAllocationSnapshots, burn time.Time) (localCompactionSnapshot, error) {
	deny := func() (localCompactionSnapshot, error) {
		return localCompactionSnapshot{}, q.budget.deny()
	}
	if store.localBootstrap == nil {
		return deny()
	}
	entry := store.localBootstrap.entry.Load()
	if !entry.original() || entry.preparedSearch == nil {
		return deny()
	}
	m := entry.preparedSearch.Load()
	if m == nil || m.source == nil || m.source.state == nil {
		return deny()
	}
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	digest, err := m.operation.Digest()
	if err != nil || !m.originalForPhase(entry, localCompactionReserve) || m.reservationAttempted || digest != proof.OperationDigest || m.operation.Sequence != proof.OperationSequence || !m.operation.Deadline.Equal(proof.OperationDeadline) {
		return deny()
	}
	// PublishOwnedProxy already read these exact parent, burn and allocation
	// rows in this transaction. Apply the SAME current validators before the
	// unchanged snapshot body/range and complete correspondence checks.
	control := entry.compactionSourceControl()
	if entry.validateRetainedCurrentParent(ctx, q, control, ownedAllocationParentRequest(parent.Reservation), parent, source) != nil {
		return deny()
	}
	if _, err := entry.currentRetainedRunFromAllocation(ctx, q, control, allocation); err != nil || entry.revalidateMCPConservativeFunding(ctx, q.at) != nil {
		return deny()
	}
	joined, err := entry.readLocalPreparedSnapshotBody(ctx, q, localSnapshotOwner{}, localCompactionCapture)
	if err != nil || !joined.Accounting.SetupReservedAt.Equal(burn) {
		return deny()
	}
	joined, err = m.source.matchCurrentSnapshotLocked(ctx, q, joined, nil, nil)
	if err != nil {
		return deny()
	}
	state, err := m.pending.readJoinedCurrentTx(ctx, q, joined)
	bodies, be := proof.Bodies()
	if err != nil || be != nil || !reflect.DeepEqual(state, m.pending.state) || state.Status != domain.WebSearchConsentApprovedV1 || bodies.LocalSearchHTTPS == nil || bodies.LocalSearchHTTPS.Policy.Grant != m.pending.grant.grant || !m.lifetimeCurrent() || entry.terminalFitSourceLocked(ctx, q, m.source, m.operation, nil) != nil {
		return deny()
	}
	return joined, nil
}

func (store *Store) localSearchProxyContinuation(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, proof ports.OwnedProxyPublicationV2, previous *ownedOperationRow) bool {
	if store.localBootstrap == nil || previous == nil || previous.observation == nil || previous.reservation.Operation.Kind != domain.AttemptOperationWebSearchV1 || proof.ServiceKind != domain.RegisteredProxyProviderV1 || proof.TransportKind != domain.RegisteredProxyTransportLocalLoopbackHTTPV2 {
		return false
	}
	entry := store.localBootstrap.entry.Load()
	if !entry.original() || entry.owner.store != store || !reflect.DeepEqual(entry.parent, parent) || !reflect.DeepEqual(entry.source, source) || !entry.originalSearchObservationAllowsSuccessor(previous.reservation, *previous.observation) {
		return false
	}
	search := entry.findOriginalSearch(previous.reservation.Operation.Sequence)
	if search == nil || !search.lifetimeCurrent() || !search.pending.grant.sourceCurrentAt(q.at) || source.Spending[3].ValidateAt(q.at) != nil {
		return false
	}
	model := entry.preparedModel.Load()
	if model == nil || model.source == nil || model.source.state == nil {
		return false
	}
	model.source.state.mu.Lock()
	defer model.source.state.mu.Unlock()
	op := model.operation
	d, err := op.Digest()
	return err == nil && model.originalForPhase(entry, localCompactionReserve) && op.Sequence == proof.OperationSequence && d == proof.OperationDigest && op.Deadline.Equal(proof.OperationDeadline) && op.ValidatePredecessor(previous.reservation, *previous.observation) == nil && q.at.Before(op.Deadline)
}
