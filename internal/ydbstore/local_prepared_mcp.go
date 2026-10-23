package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// localPreparedMCPV1 is the sole original-Store producer for one MCP dispatch.
// The opaque Call and grant are immutable DATA; this holder and the original
// Store joins—not those values—supply authority to reserve and send.
type localPreparedMCPV1 struct {
	self                 *localPreparedMCPV1
	entry                *LocalInvocationEntryV1
	source               *LocalCompactionSourceV1
	call                 mcpadmission.CallV1
	request              mcpwire.RequestV1
	grant                mcpadmission.MCPToolGrantV1
	operation            domain.AttemptOperationV1
	reservationAttempted bool
	reservation          domain.AttemptOperationReservationV1
	observation          ports.AuthenticatedAttemptOperationObservationV1
}

func (m *localPreparedMCPV1) originalForPhase(entry *LocalInvocationEntryV1, phase localCompactionGenerationPhase) bool {
	if m == nil || m.self != m || m.entry != entry || !entry.original() || entry.preparedMCP.Load() != m ||
		m.source == nil || m.source.self != m.source || m.source.entry != entry || m.source.state == nil || m.source.state.cleared ||
		m.operation.Kind != domain.AttemptOperationMCPToolV1 || m.operation.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil ||
		m.operation.Scope != ownedOperationScope(entry.parent) || m.operation.Sequence != m.source.state.snapshot.Accounting.Aggregate.Operations+1 ||
		m.call.ID() == "" || m.call.GrantDigest() != m.operation.ToolGrantDigest || m.call.ModelResponseDigest() != m.operation.ModelResponseDigest ||
		m.call.ArgumentsDigest() != m.operation.ArgumentsDigest || m.call.DescriptorDigest() == "" || m.request.Kind() != mcpwire.RequestCallToolV1 ||
		m.request.ID() != m.call.ID() || m.request.DescriptorDigest() != m.call.DescriptorDigest() {
		return false
	}
	request := m.request.Body()
	defer clear(request)
	requestDigest := sha256.Sum256(request)
	wantDigest, err := hex.DecodeString(m.operation.RequestDigest)
	if err != nil || !bytes.Equal(requestDigest[:], wantDigest) || uint64(len(request)) != m.operation.RequestBytes {
		return false
	}
	switch phase {
	case localCompactionReserve:
		return true
	case localCompactionBeforeSend:
		return m.reservation.Validate() == nil && reflect.DeepEqual(m.reservation.Operation, m.operation)
	case localCompactionCompleted:
		return m.reservation.Validate() == nil && reflect.DeepEqual(m.reservation.Operation, m.operation) &&
			m.observation.Validate() == nil && reflect.DeepEqual(m.observation.Reservation, m.reservation) &&
			ownedObservationAllowsSuccessor(m.reservation, m.observation.Observation) && entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(m.observation.Clone()) == nil
	default:
		return false
	}
}

// Operation returns an independent operation value after checking original
// holder identity. It is DATA only and cannot be submitted without Store joins.
func (m *localPreparedMCPV1) Operation() (domain.AttemptOperationV1, error) {
	if m == nil || m.source == nil || m.source.state == nil || m.entry == nil {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if !m.originalForPhase(m.entry, localCompactionReserve) {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	return m.operation.Clone(), nil
}

// Request returns the exact frozen tools/call request DATA. Sending still
// requires the original operation reservation and BeforeSend capability.
func (m *localPreparedMCPV1) Request() (mcpwire.RequestV1, error) {
	if _, err := m.Operation(); err != nil {
		return mcpwire.RequestV1{}, err
	}
	return m.request, nil
}

// Call returns immutable correlation DATA from the durable MODEL response.
func (m *localPreparedMCPV1) Call() (mcpadmission.CallV1, error) {
	if _, err := m.Operation(); err != nil {
		return mcpadmission.CallV1{}, err
	}
	return m.call, nil
}

// Grant returns cloned installed grant DATA from the original Source.
func (m *localPreparedMCPV1) Grant() (mcpadmission.MCPToolGrantV1, error) {
	if _, err := m.Operation(); err != nil {
		return mcpadmission.MCPToolGrantV1{}, err
	}
	grant := m.grant
	grant.Descriptor = bytes.Clone(grant.Descriptor)
	return grant, nil
}

func (*localPreparedMCPV1) String() string { return "LocalPreparedMCPV1{private:[redacted]}" }

// PrepareMCP consumes only a final durable MODEL call from this original entry.
// A verified final MODEL answer returns (nil,nil); incomplete or unknown data
// remains a denial. No caller-supplied call, request, grant or operation exists.
func (entry *LocalInvocationEntryV1) PrepareMCP(ctx context.Context) (*localPreparedMCPV1, error) {
	if !entry.original() {
		return nil, domain.ErrMembershipDenied
	}
	unlock, err := entry.beginLocalMutation(ctx)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	defer unlock()
	if old := entry.preparedMCP.Load(); old != nil {
		return nil, domain.ErrMembershipDenied
	}
	callCtx, finish, err := entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	callCtx, cancel := ownedAuthorityCallContext(callCtx)
	defer cancel()
	source, err := entry.captureCompactionSource(callCtx, localSourceMCPPreparation)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	valid := false
	defer func() {
		if !valid {
			source.Clear()
		}
	}()
	if source.state == nil || len(source.state.operations) == 0 {
		return nil, domain.ErrMembershipDenied
	}
	last := source.state.operations[len(source.state.operations)-1]
	if last.Reservation.Operation.Kind != domain.AttemptOperationModelV1 || last.Observation.ValidateForReservation(last.Reservation) != nil || !ownedObservationAllowsSuccessor(last.Reservation, last.Observation) {
		return nil, domain.ErrMembershipDenied
	}
	decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(entry.owner.configuration, last.Request, last.Response, domain.AttemptOperationModelV1)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	if decoded.Call == nil {
		if decoded.Stop != configuredinference.StopFinalV1 {
			return nil, domain.ErrMembershipDenied
		}
		return nil, nil
	}
	defer clear(decoded.Call.Arguments)
	if decoded.Stop != configuredinference.StopToolV1 {
		return nil, domain.ErrMembershipDenied
	}
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if policy == nil {
		return nil, domain.ErrMembershipDenied
	}
	ref := entry.source.Profile
	grant, err := entry.owner.source.ReadMCPToolGrant(callCtx, ref, source.state.snapshot.Accounting.UpdatedAt)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	bound, err := mcpadmission.BindConfiguredCallV1(entry.owner.configuration, grant, last.Reservation, last.Observation, last.Request, last.Response)
	if err != nil || bound.ID() != decoded.Call.ID || bound.Name() != decoded.Call.Name || !bytes.Equal(bound.Arguments(), decoded.Call.Arguments) {
		return nil, domain.ErrMembershipDenied
	}
	for _, prior := range source.state.operations {
		if prior.Reservation.Operation.CallID == bound.ID() {
			return nil, domain.ErrMembershipDenied
		}
	}
	request, err := bound.Request()
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	requestBody := request.Body()
	defer clear(requestBody)
	if len(requestBody) == 0 || uint64(len(requestBody)) > policy.Limits.MaxToolArgumentBytes || uint64(len(requestBody)) > 4<<10 {
		return nil, domain.ErrMembershipDenied
	}
	// The metadata call has its own shorter SQL/body deadline, not the external
	// operation lifetime. As with MODEL preparation, compare the immutable
	// network deadline against the original caller; keep all joins below under
	// the unchanged bounded metadata context.
	operation, err := entry.localPreparedMCPOperation(ctx, source, bound, request, grant, source.state.snapshot.Accounting.UpdatedAt)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	if err := entry.terminalFitSourceLocked(callCtx, nil, source, operation, nil); err != nil {
		return nil, err
	}
	m := &localPreparedMCPV1{entry: entry, source: source, call: bound, request: request, grant: grant, operation: operation}
	m.self = m
	if !entry.preparedMCP.CompareAndSwap(nil, m) {
		return nil, domain.ErrMembershipDenied
	}
	if chargeOwnedAuthorityBody(callCtx, uint64(len(requestBody))) != nil {
		return nil, domain.ErrMembershipDenied
	}
	refBody, err := entry.bodies.Write(callCtx, requestBody)
	if err != nil || callCtx.Err() != nil || refBody.Digest != operation.RequestDigest || refBody.Bytes != operation.RequestBytes {
		return nil, domain.ErrMembershipDenied
	}
	operation.RequestRef = refBody.Ref
	err = entry.owner.store.ownedAuthorityTx(callCtx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if err := source.rejoinLocked(ctx, q, nil, nil); err != nil {
			return err
		}
		if !q.at.Before(operation.Deadline) {
			return q.budget.deny()
		}
		if _, err := entry.owner.source.ReadMCPToolGrant(ctx, ref, q.at); err != nil {
			return q.budget.deny()
		}
		next, err := debitOwnedOperation(source.state.snapshot.Accounting, operation, entry.parent, entry.source, q.at)
		if err != nil || !entry.owner.store.localOperationBudgetAllows(operation.Scope, next.Aggregate.BodyBytes) {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || callCtx.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	m.operation = operation
	valid = true
	return m, nil
}

func (entry *LocalInvocationEntryV1) localPreparedMCPOperation(ctx context.Context, source *LocalCompactionSourceV1, call mcpadmission.CallV1, request mcpwire.RequestV1, grant mcpadmission.MCPToolGrantV1, at time.Time) (domain.AttemptOperationV1, error) {
	deny := func() (domain.AttemptOperationV1, error) {
		return domain.AttemptOperationV1{}, domain.ErrMembershipDenied
	}
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	cost := entry.parent.Authority.AdmissionCostCeiling.MCPToolCost
	spending := entry.source.Spending[2]
	if policy == nil || policy.Validate() != nil || cost == nil || cost.ValidateAt(at) != nil || spending.ValidateAt(at) != nil || spending.MaxAmountMicrounits == nil {
		return deny()
	}
	body := request.Body()
	defer clear(body)
	if len(body) == 0 || uint64(len(body)) > 4<<10 || uint64(len(body)) > policy.Limits.MaxToolArgumentBytes {
		return deny()
	}
	used := source.state.snapshot.Accounting.Aggregate
	if used.BodyBytes >= policy.Limits.MaxAggregateBodyBytes {
		return deny()
	}
	active := policy.Limits.MaxNetworkOperationDuration
	active = min(active, entry.owner.configuration.Limits.OperationTimeout)
	active = min(active, policy.Limits.MaxActiveDuration-used.ActiveDuration)
	if active <= 0 {
		return deny()
	}
	deadline := at.Add(active)
	for _, end := range []time.Time{entry.parent.Authority.InvocationDeadline, entry.parent.Authority.Lease.ExpiresAt, policy.ExpiresAt, cost.PriceExpiresAt, spending.Window.ExpiresAt, entry.parent.Reservation.ReservedAt.Add(policy.Limits.MaxActiveDuration), entry.allocation.Window.ExpiresAt, entry.owner.configuration.ExpiresAt, grant.ExpiresAt} {
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
	maxResult := min(policy.Limits.MaxToolResultBytes, grant.Limits.ResultTextBytes, uint64(64<<10))
	maxInbound := min(policy.Limits.MaxMCPInboundBytes, grant.Limits.AggregateBytes, uint64(256<<10))
	maxFrames := min(policy.Limits.MaxMCPInboundFrames, grant.Limits.SSEEvents)
	if maxResult == 0 || maxInbound == 0 || maxFrames == 0 {
		return deny()
	}
	costDigest, err := entry.parent.Authority.AdmissionCostCeiling.Digest()
	if err != nil {
		return deny()
	}
	sequence := used.Operations + 1
	if sequence == 0 || sequence > 12 {
		return deny()
	}
	requestDigest := sha256.Sum256(body)
	op := domain.AttemptOperationV1{Version: 1, Scope: ownedOperationScope(entry.parent), Sequence: sequence, Kind: domain.AttemptOperationMCPToolV1,
		CallID: call.ID(), RequestRef: "pending-mcp-body", RequestDigest: hex.EncodeToString(requestDigest[:]),
		ModelResponseDigest: call.ModelResponseDigest(), ArgumentsDigest: call.ArgumentsDigest(), ToolGrantDigest: call.GrantDigest(),
		Deadline: deadline.UTC().Truncate(time.Microsecond), CleanupDuration: policy.Limits.MaxCleanupDuration,
		RequestBytes: uint64(len(body)), ResultBytes: maxResult, Currency: entry.parent.Authority.AdmissionCostCeiling.Currency, PriceRevision: entry.parent.Authority.AdmissionCostCeiling.PriceRevision,
		AdmissionCostCeilingDigest: costDigest, ExternalChargeEvidenceDigest: cost.PriceEvidenceDigest}
	if len(source.state.operations) == 0 {
		return deny()
	}
	previous := source.state.operations[len(source.state.operations)-1]
	if previous.Reservation.Operation.Kind != domain.AttemptOperationModelV1 || previous.Observation.ValidateForReservation(previous.Reservation) != nil || !ownedObservationAllowsSuccessor(previous.Reservation, previous.Observation) {
		return deny()
	}
	op.PredecessorObservationDigest, _ = previous.Observation.Digest()
	op.Debit = domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, BodyBytes: op.RequestBytes + op.ResultBytes,
		MCPOutboundMessages: 4, MCPCatalogPages: 1, MCPInboundFrames: maxFrames, MCPInboundBytes: maxInbound,
		ResponseBytes: maxInbound, AmountMicrounits: *spending.MaxAmountMicrounits, ActiveDuration: deadline.Sub(at)}
	if op.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil || op.Debit.BodyBytes > policy.Limits.MaxAggregateBodyBytes-used.BodyBytes {
		return deny()
	}
	if _, err := debitOwnedOperation(source.state.snapshot.Accounting, op, entry.parent, entry.source, at); err != nil {
		return deny()
	}
	return op, nil
}

func (entry *LocalInvocationEntryV1) localFutureMCPProxyDigest(q *ownedAuthorityQuery, p *ownedProxySnapshot, op domain.AttemptOperationV1) (domain.RegisteredProxyAttestationDigestV2, error) {
	operationDigest, digestErr := op.Digest()
	if p == nil || !q.at.Before(op.Deadline) || p.current.ValidateAt(q.at) != nil || op.Deadline.After(p.current.Window.ExpiresAt) ||
		p.original.TransportKind != domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1 || p.original.ServiceKind != domain.RegisteredProxyMCPToolV1 ||
		p.original.LocalPublicMCPHTTPS == nil || p.original.Scope != entry.source.Scope || p.original.OperationSequence != op.Sequence || digestErr != nil || p.original.OperationDigest != operationDigest ||
		p.original.GrantDigest != entry.source.Spending[2].ScopedGrantDigest || p.original.LocalPublicMCPHTTPS.GrantDigest != entry.source.Spending[2].ScopedGrantDigest {
		return "", q.budget.deny()
	}
	d, err := p.original.Digest()
	if err != nil {
		return "", q.budget.deny()
	}
	return d, nil
}

func (m *localPreparedMCPV1) rejoinLocked(ctx context.Context, q *ownedAuthorityQuery, phase localCompactionGenerationPhase) (domain.Run, error) {
	if !m.originalForPhase(m.entry, phase) {
		return domain.Run{}, q.budget.deny()
	}
	current, err := m.entry.readLocalPreparedSnapshot(ctx, q, localSnapshotOwner{mcp: m}, phase)
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
		current.Operations, current.ProxyDigests = nil, nil
	}
	if !reflect.DeepEqual(current, expected) {
		return domain.Run{}, q.budget.deny()
	}
	if err := m.source.rejoinHistoryLocked(ctx, q); err != nil {
		return domain.Run{}, err
	}
	return run, nil
}
