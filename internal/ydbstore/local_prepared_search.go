package ydbstore

import (
	"bytes"
	"context"
	"reflect"
	"sync/atomic"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// This private holder is produced only from this entry's actual durable MODEL
// and authenticated decision. Public frozen request/receipt/operation DATA can
// never construct it. A previous holder retains the at-most-two original search
// observations; it is not a reconstructible registry or a renewed invocation.
type localPreparedSearchV1 struct {
	self                 *localPreparedSearchV1
	entry                *LocalInvocationEntryV1
	source               *LocalCompactionSourceV1
	pending              *localSearchConsentV1
	previous             *localPreparedSearchV1
	frozen               *websearch.FrozenRequestV2
	operation            domain.AttemptOperationV1
	prepared             bool
	reservationAttempted bool
	reservation          domain.AttemptOperationReservationV1
	observation          ports.AuthenticatedAttemptOperationObservationV1
	conservative         *atomic.Pointer[localSearchConservativeV4]
}

func (*localPreparedSearchV1) String() string { return "LocalPreparedSearchV1{private:[redacted]}" }

func (m *localPreparedSearchV1) originalForPhase(entry *LocalInvocationEntryV1, phase localCompactionGenerationPhase) bool {
	if m == nil || m.self != m || m.entry != entry || !entry.original() || entry.preparedSearch == nil || entry.preparedSearch.Load() != m || !m.prepared || m.pending == nil || !m.pending.original() || m.pending.failed || !m.pending.waited || m.pending.state.Status != domain.WebSearchConsentApprovedV1 || m.source == nil || m.source.self != m.source || m.source.entry != entry || m.source.state == nil || m.source.state.cleared || m.frozen == nil || m.conservative == nil || m.operation.Kind != domain.AttemptOperationWebSearchV1 || m.operation.SearchWire == nil || m.operation.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil || m.operation.Scope != ownedOperationScope(entry.parent) || m.operation.Sequence != m.source.state.snapshot.Accounting.Aggregate.Operations+1 || m.frozen.ValidateOperation(m.operation) != nil || m.frozen.ValidateConsentRequest(m.pending.request) != nil {
		return false
	}
	switch phase {
	case localCompactionReserve:
		return true
	case localCompactionBeforeSend:
		return m.reservation.Validate() == nil && reflect.DeepEqual(m.reservation.Operation, m.operation)
	case localCompactionCompleted:
		return m.reservation.Validate() == nil && reflect.DeepEqual(m.reservation.Operation, m.operation) && m.observation.Validate() == nil && reflect.DeepEqual(m.observation.Reservation, m.reservation) && entry.originalObservationAllowsSuccessor(m.reservation, m.observation.Observation) && entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(m.observation.Clone()) == nil
	default:
		return false
	}
}

func (m *localPreparedSearchV1) Operation() (domain.AttemptOperationV1, error) {
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

func (m *localPreparedSearchV1) FrozenRequest() (*websearch.FrozenRequestV2, error) {
	if _, err := m.Operation(); err != nil {
		return nil, err
	}
	return m.frozen, nil
}

// The public request carries exact immutable correlation DATA, not approval.
// Reserve still requires this original holder and its fresh transactional join.
func (m *localPreparedSearchV1) ReservationRequest() (ports.ReserveAttemptOperationRequestV1, error) {
	if m == nil || m.source == nil || m.source.state == nil {
		return ports.ReserveAttemptOperationRequestV1{}, domain.ErrMembershipDenied
	}
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if !m.originalForPhase(m.entry, localCompactionReserve) || m.pending.state.Decision == nil {
		return ports.ReserveAttemptOperationRequestV1{}, domain.ErrMembershipDenied
	}
	state := m.pending.state
	rd, re := state.Request.Digest()
	dd, de := state.Decision.Digest()
	r := ports.ReserveAttemptOperationRequestV1{Operation: m.operation.Clone(), ConsentReceipt: &domain.WebSearchConsentReceiptBindingV1{Request: state.Request, RequestDigest: rd, DecisionDigest: dd}}
	if re != nil || de != nil || r.Validate() != nil {
		return ports.ReserveAttemptOperationRequestV1{}, domain.ErrMembershipDenied
	}
	return r, nil
}

// CredentialRequest returns correlation and absolute-lifetime DATA only. The
// concrete service independently validates current credential custody; the
// original Store/Source still authenticates consent and the send. It never
// starts the whole invocation's final cleanup or renews a short borrow window.
func (m *localPreparedSearchV1) CredentialRequest() (ports.SearchCredentialIssueRequestV1, time.Time, error) {
	deny := func() (ports.SearchCredentialIssueRequestV1, time.Time, error) {
		return ports.SearchCredentialIssueRequestV1{}, time.Time{}, domain.ErrMembershipDenied
	}
	if m == nil || m.source == nil || m.source.state == nil {
		return deny()
	}
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if !m.originalForPhase(m.entry, localCompactionReserve) || !m.entry.credentialExecutionActive() || m.entry.credentialCleanup == nil {
		return deny()
	}
	w := m.entry.credentialCleanup
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.prepared || !w.deadline.IsZero() || m.operation.Deadline.After(w.active) {
		return deny()
	}
	digest, err := m.operation.Digest()
	if err != nil {
		return deny()
	}
	r := ports.SearchCredentialIssueRequestV1{Declaration: m.pending.declaration, Correlation: ports.SearchCredentialCorrelationV1{Scope: m.operation.Scope, OperationDigest: digest, ConsentRequestDigest: domain.WebSearchConsentRequestDigestV1(m.operation.ConsentRequestDigest), ConsentDecisionDigest: domain.WebSearchConsentDecisionDigestV1(m.operation.ConsentDecisionDigest)}, WorkerID: string(m.entry.parent.Authority.Lease.WorkerID), ExpiresAt: m.operation.Deadline}
	deadline := minSearchCleanupDeadline(m.operation.Deadline.Add(m.operation.CleanupDuration), w.ceiling).UTC().Truncate(time.Microsecond)
	if r.Validate() != nil || !deadline.After(m.operation.Deadline) {
		return deny()
	}
	return r, deadline, nil
}

func minSearchCleanupDeadline(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// PrepareSearch accepts no request, arguments, receipt, operation or grant.
// The SAME private pending holder must have completed its original wait.
func (p *localSearchConsentV1) PrepareSearch(ctx context.Context) (*localPreparedSearchV1, error) {
	deny := func() (*localPreparedSearchV1, error) { return nil, domain.ErrMembershipDenied }
	if !p.original() {
		return deny()
	}
	entry := p.entry
	unlock, err := entry.beginLocalMutation(ctx)
	if err != nil {
		return deny()
	}
	defer unlock()
	if !p.original() || !p.created || !p.waited || p.failed || p.preparationAttempted || p.state.Status != domain.WebSearchConsentApprovedV1 || p.state.Decision == nil || !entry.credentialExecutionActive() {
		return deny()
	}
	p.preparationAttempted = true // before capture/I/O; preparation denial is not retryable
	old := entry.preparedSearch.Load()
	if old != nil {
		if old.source == nil || old.source.state == nil || old.pending == p || old.previous != nil {
			return deny()
		}
		old.source.state.mu.Lock()
		completed := entry.originalSearchObservationAllowsSuccessor(old.reservation, old.observation.Observation)
		old.source.state.mu.Unlock()
		if !completed {
			return deny()
		}
	}
	call, finish, err := entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return deny()
	}
	defer finish()
	call, cancel := ownedAuthorityCallContext(call)
	defer cancel()
	source, err := entry.captureCompactionSource(call, localSourceMCPPreparation)
	if err != nil {
		return deny()
	}
	valid := false
	defer func() {
		if !valid {
			source.Clear()
		}
	}()
	if source.state == nil || len(source.state.operations) == 0 {
		return deny()
	}
	last := source.state.operations[len(source.state.operations)-1]
	if !reflect.DeepEqual(last.Reservation, p.model) || !reflect.DeepEqual(last.Observation, p.observation) {
		return deny()
	}
	var at time.Time
	err = entry.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		joined, _, err := source.rejoinSnapshotWithBurnLocked(ctx, q, nil, nil)
		if err != nil {
			return q.budget.deny()
		}
		state, err := p.readJoinedCurrentTx(ctx, q, joined)
		if err != nil || !reflect.DeepEqual(state, p.state) || state.Status != domain.WebSearchConsentApprovedV1 {
			return q.budget.deny()
		}
		at = q.at
		return nil
	})
	if err != nil || source.materializeHistoryLocked(call) != nil || source.state.history.unresolvedSearchV4 {
		return deny()
	}
	cost := entry.parent.Authority.AdmissionCostCeiling.WebSearchCost
	if cost == nil {
		return deny()
	}
	cd, err := entry.parent.Authority.AdmissionCostCeiling.Digest()
	if err != nil {
		return deny()
	}
	frozen, err := websearch.CompileV2(p.grant.grant, *cost, websearch.BindingV1{Scope: ownedOperationScope(entry.parent), Sequence: source.state.snapshot.Accounting.Aggregate.Operations + 1, CallID: p.request.CallID, ModelResponseDigest: p.request.ModelResponseDigest, InitiatingUserID: p.request.InitiatingUserID, ParentCostCeilingDigestV2: cd, ParentPriceRevision: entry.parent.Authority.AdmissionCostCeiling.PriceRevision}, p.arguments, at)
	if err != nil || frozen.ValidateConsentRequest(p.request) != nil {
		return deny()
	}
	op, err := entry.localPreparedSearchOperation(ctx, source, p, frozen, at)
	if err != nil {
		return deny()
	}
	// The actual terminal encoder/next-MODEL fit must accept the full prospective
	// turn BEFORE the first body write or transport action. A missing successor
	// format stays a denial, never a capacity flag or a legacy-format fallback.
	if err := entry.terminalFitSourceLocked(call, nil, source, op, nil); err != nil {
		return nil, err
	}
	m := &localPreparedSearchV1{entry: entry, source: source, pending: p, previous: old, frozen: frozen, operation: op, conservative: new(atomic.Pointer[localSearchConservativeV4])}
	m.self = m
	if !entry.preparedSearch.CompareAndSwap(old, m) {
		return deny()
	}
	body := frozen.Body()
	defer clear(body)
	if chargeOwnedAuthorityBody(call, uint64(len(body))) != nil {
		return deny()
	}
	ref, err := entry.bodies.Write(call, body)
	if err != nil || ref.Digest != op.RequestDigest || ref.Bytes != op.RequestBytes || call.Err() != nil {
		return deny()
	}
	op.RequestRef = ref.Ref
	frozen, err = frozen.BindOperationV2(op)
	if err != nil {
		return deny()
	}
	err = entry.owner.store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		joined, _, err := source.rejoinSnapshotWithBurnLocked(ctx, q, nil, nil)
		if err != nil {
			return q.budget.deny()
		}
		state, err := p.readJoinedCurrentTx(ctx, q, joined)
		if err != nil || !reflect.DeepEqual(state, p.state) || !q.at.Before(op.Deadline) || frozen.ValidateConsentRequest(state.Request) != nil {
			return q.budget.deny()
		}
		next, err := debitOwnedOperation(source.state.snapshot.Accounting, op, entry.parent, entry.source, q.at)
		if err != nil || !entry.owner.store.localOperationBudgetAllows(op.Scope, next.Aggregate.BodyBytes) {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || call.Err() != nil || ctx.Err() != nil {
		return deny()
	}
	m.operation, m.frozen, m.prepared = op, frozen, true
	valid = true
	return m, nil
}

func (entry *LocalInvocationEntryV1) localPreparedSearchOperation(ctx context.Context, source *LocalCompactionSourceV1, p *localSearchConsentV1, frozen *websearch.FrozenRequestV2, at time.Time) (domain.AttemptOperationV1, error) {
	deny := func() (domain.AttemptOperationV1, error) {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	policy, cost := entry.parent.Authority.HarnessBinding.AgentLoopPolicy, entry.parent.Authority.AdmissionCostCeiling.WebSearchCost
	if policy == nil || len(entry.source.Spending) != 4 || cost == nil || cost.ValidateAt(at) != nil || entry.source.Spending[3].ValidateAt(at) != nil || p.state.Decision == nil || p.grant.grant.ValidateCostAt(*cost, at) != nil {
		return deny()
	}
	used := source.state.snapshot.Accounting.Aggregate
	active := min(policy.Limits.MaxNetworkOperationDuration, entry.owner.configuration.Limits.OperationTimeout, 15*time.Second, policy.Limits.MaxActiveDuration-used.ActiveDuration)
	if active <= 0 || used.BodyBytes >= policy.Limits.MaxAggregateBodyBytes {
		return deny()
	}
	deadline := at.Add(active)
	for _, end := range []time.Time{entry.parent.Authority.InvocationDeadline, entry.parent.Authority.Lease.ExpiresAt, policy.ExpiresAt, cost.PriceExpiresAt, entry.source.Spending[3].Window.ExpiresAt, entry.parent.Reservation.ReservedAt.Add(policy.Limits.MaxActiveDuration), entry.allocation.Window.ExpiresAt, entry.owner.configuration.ExpiresAt, p.grant.sourceDeadline, p.grant.grant.ExpiresAt, p.request.ExpiresAt} {
		if end.Before(deadline) {
			deadline = end
		}
	}
	life, err := entry.owner.source.ExecutionLifetime()
	if err != nil {
		return deny()
	}
	for _, parent := range []context.Context{life, entry.invocationContext} {
		if end, ok := parent.Deadline(); ok && end.Before(deadline) {
			deadline = end
		}
	}
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		return deny()
	}
	if !deadline.After(at) {
		return deny()
	}
	body := frozen.Body()
	defer clear(body)
	args, query, wire := frozen.Digests()
	cd, err := entry.parent.Authority.AdmissionCostCeiling.Digest()
	if err != nil || len(body) == 0 || len(body) > domain.WebSearchWireBytesV1 {
		return deny()
	}
	rd, _ := p.request.Digest()
	dd, _ := p.state.Decision.Digest()
	op := domain.AttemptOperationV1{Version: 1, Scope: ownedOperationScope(entry.parent), Sequence: used.Operations + 1, Kind: domain.AttemptOperationWebSearchV1, CallID: p.request.CallID, RequestRef: "pending-search-body", RequestDigest: wire, RequestBytes: uint64(len(body)), ModelResponseDigest: p.request.ModelResponseDigest, ArgumentsDigest: args, QueryDigest: query, ToolGrantDigest: p.request.GrantDigest, ConsentRequestDigest: string(rd), ConsentDecisionDigest: string(dd), ConsentDecisionRef: string(p.request.ID), SearchWire: &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: p.grant.grant.APIProfile, ArgumentsBytes: uint64(len(p.arguments)), CompilerArtifactDigest: p.grant.grant.AdapterArtifactDigest}, Deadline: deadline.UTC().Truncate(time.Microsecond), CleanupDuration: min(policy.Limits.MaxCleanupDuration, entry.owner.configuration.Limits.CleanupTimeout), ResultBytes: min(policy.Limits.MaxToolResultBytes, uint64(64<<10)), Currency: entry.parent.Authority.AdmissionCostCeiling.Currency, PriceRevision: entry.parent.Authority.AdmissionCostCeiling.PriceRevision, AdmissionCostCeilingDigest: cd, ExternalChargeEvidenceDigest: cost.PriceEvidenceDigest}
	op.PredecessorObservationDigest, _ = p.observation.Digest()
	op.Debit = domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, WebSearches: 1, BodyBytes: op.RequestBytes + op.ResultBytes, ResponseBytes: domain.WebSearchResponseBytesV1, AmountMicrounits: p.grant.grant.PerCallReservationMicrounits, ActiveDuration: deadline.Sub(at)}
	if op.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil || frozen.ValidateOperation(op) != nil || op.Debit.BodyBytes > policy.Limits.MaxAggregateBodyBytes-used.BodyBytes {
		return deny()
	}
	if _, err := debitOwnedOperation(source.state.snapshot.Accounting, op, entry.parent, entry.source, at); err != nil {
		return deny()
	}
	return op, nil
}

func (entry *LocalInvocationEntryV1) localFutureSearchProxyDigest(q *ownedAuthorityQuery, proxy *ownedProxySnapshot, op domain.AttemptOperationV1) (domain.RegisteredProxyAttestationDigestV2, error) {
	d, err := op.Digest()
	if err != nil || proxy == nil || proxy.current.ValidateAt(q.at) != nil || !q.at.Before(op.Deadline) || op.Deadline.After(proxy.current.Window.ExpiresAt) || proxy.original.TransportKind != domain.RegisteredProxyTransportLocalSearchHTTPSV1 || proxy.original.ServiceKind != domain.RegisteredProxyWebSearchV1 || proxy.original.LocalSearchHTTPS == nil || proxy.original.Scope != entry.source.Scope || proxy.original.OperationSequence != op.Sequence || proxy.original.OperationDigest != d || proxy.original.GrantDigest != op.ToolGrantDigest {
		return "", q.budget.deny()
	}
	policy, _, err := ports.DecodeLocalSearchHTTPSProxyDataV1(proxy.policyBody, proxy.attestationBody)
	if err != nil || op.SearchWire == nil || policy.Grant.AdapterArtifactDigest != op.SearchWire.CompilerArtifactDigest || policy.Grant.APIProfile != op.SearchWire.APIProfile {
		return "", q.budget.deny()
	}
	return proxy.original.Digest()
}

func (m *localPreparedSearchV1) rejoinLocked(ctx context.Context, q *ownedAuthorityQuery, phase localCompactionGenerationPhase) (domain.Run, error) {
	current, err := m.rejoinSnapshotLocked(ctx, q, phase)
	return current.Job.Run.Clone(), err
}

func (m *localPreparedSearchV1) rejoinSnapshotLocked(ctx context.Context, q *ownedAuthorityQuery, phase localCompactionGenerationPhase) (localCompactionSnapshot, error) {
	if !m.originalForPhase(m.entry, phase) {
		return localCompactionSnapshot{}, q.budget.deny()
	}
	current, err := m.entry.readLocalPreparedSnapshot(ctx, q, localSnapshotOwner{search: m}, phase)
	if err != nil {
		return localCompactionSnapshot{}, err
	}
	expected := m.source.state.snapshot
	if phase == localCompactionReserve {
		if len(current.Operations) != len(expected.Operations) || len(current.ProxyDigests) != len(expected.ProxyDigests)+1 {
			return localCompactionSnapshot{}, q.budget.deny()
		}
	} else {
		if len(current.Operations) != len(expected.Operations)+1 || len(current.ProxyDigests) != len(expected.ProxyDigests)+1 {
			return localCompactionSnapshot{}, q.budget.deny()
		}
		tail := current.Operations[len(current.Operations)-1]
		if !reflect.DeepEqual(tail.reservation, m.reservation) || (phase == localCompactionBeforeSend && tail.observation != nil) || (phase == localCompactionCompleted && (tail.observation == nil || !reflect.DeepEqual(*tail.observation, m.observation.Observation))) {
			return localCompactionSnapshot{}, q.budget.deny()
		}
		next, err := debitOwnedOperation(expected.Accounting, m.operation, m.entry.parent, m.entry.source, m.reservation.ReservedAt)
		if err != nil || !reflect.DeepEqual(next, current.Accounting) {
			return localCompactionSnapshot{}, q.budget.deny()
		}
		current.Accounting, current.Operations = expected.Accounting, current.Operations[:len(current.Operations)-1]
	}
	current.ProxyDigests = current.ProxyDigests[:len(current.ProxyDigests)-1]
	if !reflect.DeepEqual(current, expected) || m.source.rejoinHistoryLocked(ctx, q) != nil {
		return localCompactionSnapshot{}, q.budget.deny()
	}
	return current, nil
}

func (m *localPreparedSearchV1) exactRequest(body []byte) bool {
	if m == nil || m.frozen == nil {
		return false
	}
	want := m.frozen.Body()
	defer clear(want)
	return bytes.Equal(body, want)
}
