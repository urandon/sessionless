package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func (store *Store) prepareLocalMCPEvidence(ctx context.Context, op domain.AttemptOperationV1) (*ownedOperationEvidence, bool, error) {
	if store == nil || op.Kind != domain.AttemptOperationMCPToolV1 || !store.localCompositionReady() || store.localBootstrap == nil {
		return nil, false, nil
	}
	entry := store.localBootstrap.entry.Load()
	if !entry.original() || entry.owner.store != store {
		return nil, false, nil
	}
	m := entry.preparedMCP.Load()
	if m == nil || m.source == nil || m.source.state == nil {
		return nil, true, domain.ErrMembershipDenied
	}
	unlock, err := entry.beginLocalMutation(ctx)
	if err != nil {
		return nil, true, domain.ErrMembershipDenied
	}
	defer unlock()
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if ctx.Err() != nil || !m.originalForPhase(entry, localCompactionReserve) || !reflect.DeepEqual(m.operation, op) {
		return nil, true, domain.ErrMembershipDenied
	}
	call, finish, err := entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return nil, true, err
	}
	defer finish()
	body, err := store.readOwnedOperationBody(call, op.Scope, op.RequestRef, op.RequestDigest, op.RequestBytes, 4<<10)
	if err != nil {
		return nil, true, domain.ErrMembershipDenied
	}
	defer clear(body)
	want := m.request.Body()
	if !bytes.Equal(body, want) || call.Err() != nil {
		clear(want)
		return nil, true, domain.ErrMembershipDenied
	}
	clear(want)
	// MCP uses the common reservation transaction, which must reread and compare
	// its exact durable predecessor before the specialized original-holder join.
	// Carry that predecessor from the already captured original source; do not
	// skip the common check or accept a caller-provided reservation/observation.
	if len(m.source.state.operations) == 0 {
		return nil, true, domain.ErrMembershipDenied
	}
	previous := m.source.state.operations[len(m.source.state.operations)-1]
	return &ownedOperationEvidence{mcp: m, previous: previous.Reservation.Clone(), observation: previous.Observation.Clone()}, true, nil
}

func (m *localPreparedMCPV1) rejoinAndValidateCurrent(ctx context.Context, q *ownedAuthorityQuery, phase localCompactionGenerationPhase, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1) error {
	if m == nil || m.source == nil || m.source.state == nil || m.entry == nil {
		return q.budget.deny()
	}
	if _, err := m.rejoinLocked(ctx, q, phase); err != nil {
		return err
	}
	if err := m.entry.terminalFitSourceLocked(ctx, q, m.source, m.operation, nil); err != nil {
		return err
	}
	grant, err := m.entry.owner.source.ReadMCPToolGrant(ctx, source.Profile, q.at)
	if err != nil {
		return q.budget.deny()
	}
	grantDigest, ge := grant.Digest()
	wantDigest, we := m.grant.Digest()
	profile, pe := readOwnedProfileCurrent(ctx, q, source.Profile)
	cost := parent.Authority.AdmissionCostCeiling.MCPToolCost
	if ge != nil || we != nil || grantDigest != wantDigest || grantDigest != m.operation.ToolGrantDigest ||
		pe != nil || profile.ValidateAt(q.at) != nil || profile.MCPToolGrantDigest != grantDigest ||
		cost == nil || cost.ValidateAt(q.at) != nil || cost.Kind != domain.ExternalServiceMCPToolV1 || cost.ScopedGrantDigest != grantDigest ||
		cost.PriceEvidenceDigest != m.operation.ExternalChargeEvidenceDigest || cost.ProfileID != grant.ProfileID || cost.ProfileRevision != grant.ProfileRevision {
		return q.budget.deny()
	}
	return nil
}

func (m *localPreparedMCPV1) lifetimeCurrent() bool {
	if m == nil || m.entry == nil || m.entry.invocationContext == nil || m.entry.invocationContext.Err() != nil || m.entry.owner == nil || m.entry.owner.source == nil {
		return false
	}
	life, err := m.entry.owner.source.ExecutionLifetime()
	return err == nil && life != nil && life.Err() == nil && m.entry.invocationContext.Err() == nil
}

func (store *Store) reservePreparedLocalMCP(ctx context.Context, op domain.AttemptOperationV1, evidence *ownedOperationEvidence) (ports.ReserveAttemptOperationResultV1, error) {
	deny := func() (ports.ReserveAttemptOperationResultV1, error) {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	if evidence == nil {
		return deny()
	}
	caller := ctx
	m := evidence.mcp
	if m == nil || m.source == nil || m.source.state == nil {
		return deny()
	}
	unlock, err := m.entry.beginLocalMutation(ctx)
	if err != nil {
		return deny()
	}
	defer unlock()
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if ctx.Err() != nil || !m.originalForPhase(m.entry, localCompactionReserve) || !reflect.DeepEqual(m.operation, op) || m.reservationAttempted || m.reservation.Version != 0 || evidence.mcp != m {
		return deny()
	}
	if len(m.source.state.operations) == 0 {
		return deny()
	}
	previous := m.source.state.operations[len(m.source.state.operations)-1]
	previousObservationDigest, digestErr := previous.Observation.Digest()
	if digestErr != nil || previousObservationDigest != op.PredecessorObservationDigest ||
		op.ValidatePredecessor(previous.Reservation, previous.Observation) != nil ||
		!reflect.DeepEqual(evidence.previous, previous.Reservation) || !reflect.DeepEqual(evidence.observation, previous.Observation) {
		return deny()
	}
	call, finish, err := m.entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return deny()
	}
	defer finish()
	ctx = call
	var candidate domain.AttemptOperationReservationV1
	var at time.Time
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		candidate, at = domain.AttemptOperationReservationV1{}, time.Time{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if m.reservationAttempted {
			return q.budget.deny()
		}
		if err := m.rejoinAndValidateCurrent(ctx, q, localCompactionReserve, m.entry.parent, m.entry.source); err != nil {
			return err
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
		opDigest, oe := op.Digest()
		allocationDigest, ae := allocation.original.Digest()
		proxyDigest, pe := proxy.original.Digest()
		if oe != nil || ae != nil || pe != nil || proxy.original.RegisteredAllocationDigest != allocationDigest ||
			proxy.original.OperationSequence != op.Sequence || proxy.original.OperationDigest != opDigest || proxy.original.ServiceKind != ownedOperationProxyService(op.Kind) {
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
		value := domain.AttemptOperationReservationV1{
			Version: 1, Operation: op.Clone(), OperationDigest: opDigest, ReservedAt: q.at,
			AggregateDebit: next.Aggregate, RegisteredAllocationDigest: allocationDigest,
			RegisteredProxyDigest: proxyDigest, ComponentAmounts: next.Amounts,
		}
		if value.ValidateForParent(parent.Authority, parent.Reservation) != nil {
			return q.budget.deny()
		}
		raw, _ := json.Marshal(value)
		digest, _ := value.Digest()
		if len(raw) > ownedAuthorityHeadBytes {
			return q.budget.deny()
		}
		// Burn before the first write so a lost acknowledgement cannot replay
		// this holder into a fresh reservation.
		m.reservationAttempted = true
		if _, err := q.ExecContext(ctx, `INSERT INTO attempt_operations_v1 (tenant_id,attempt_id,sequence,record,digest,operation_digest,call_id) VALUES ($1,$2,$3,$4,$5,$6,$7)`, op.Scope.TenantID, op.Scope.AttemptID, op.Sequence, string(raw), string(digest), string(opDigest), op.CallID); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO attempt_operation_calls_v1 (tenant_id,attempt_id,call_id,sequence,operation_digest) VALUES ($1,$2,$3,$4,$5)`, op.Scope.TenantID, op.Scope.AttemptID, op.CallID, op.Sequence, string(opDigest)); err != nil {
			return err
		}
		if err := writeOwnedAttemptAccounting(ctx, q, parent, source, next); err != nil {
			return err
		}
		candidate, at = value.Clone(), q.at
		return nil
	})
	if err != nil || ctx.Err() != nil || !store.operationEvidenceLifetimeCurrent(evidence) {
		return deny()
	}
	result, err := store.operationResult(candidate, at, true)
	if err != nil || caller.Err() != nil || ctx.Err() != nil || m.entry.invocationContext.Err() != nil || !store.operationEvidenceLifetimeCurrent(evidence) {
		return deny()
	}
	m.reservation = candidate.Clone()
	return result, nil
}

func (store *Store) authorizePreparedLocalMCP(ctx context.Context, capability ports.AttemptOperationCapabilityV1, evidence *ownedOperationEvidence) error {
	deny := func() error { return domain.ErrMembershipDenied }
	if evidence == nil || evidence.mcp == nil {
		return deny()
	}
	m := evidence.mcp
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
	op := capability.Reservation.Operation
	if ctx.Err() != nil || !m.originalForPhase(m.entry, localCompactionBeforeSend) ||
		!reflect.DeepEqual(capability.Reservation, m.reservation) || !reflect.DeepEqual(op, m.operation) {
		return deny()
	}
	caller := ctx
	call, finish, err := m.entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return deny()
	}
	defer finish()
	ctx = call
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if !q.at.Before(capability.ExpiresAt) || q.at.Before(capability.IssuedAt) ||
			capability.Reservation.ValidateForParent(m.entry.parent.Authority, m.entry.parent.Reservation) != nil {
			return q.budget.deny()
		}
		if err := m.rejoinAndValidateCurrent(ctx, q, localCompactionBeforeSend, m.entry.parent, m.entry.source); err != nil {
			return err
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
		reservationDigest, re := m.reservation.Digest()
		capabilityDigest, ce := capability.Reservation.Digest()
		allocationDigest, ae := allocation.current.Digest()
		proxyDigest, pe := proxy.current.Digest()
		if re != nil || ce != nil || ae != nil || pe != nil || reservationDigest != capabilityDigest ||
			allocationDigest != m.reservation.RegisteredAllocationDigest || proxyDigest != m.reservation.RegisteredProxyDigest ||
			proxy.current.OperationSequence != op.Sequence || proxy.current.OperationDigest != m.reservation.OperationDigest ||
			proxy.current.ServiceKind != ownedOperationProxyService(op.Kind) {
			return q.budget.deny()
		}
		if !store.localOperationBudgetAllows(op.Scope, m.reservation.AggregateDebit.BodyBytes) {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || caller.Err() != nil || ctx.Err() != nil || !store.operationEvidenceLifetimeCurrent(evidence) || !m.lifetimeCurrent() ||
		store.attemptOperationIssuer.ConsumeAttemptOperationCapability(capability) != nil {
		return deny()
	}
	if caller.Err() != nil || ctx.Err() != nil || m.entry.invocationContext.Err() != nil || !store.operationEvidenceLifetimeCurrent(evidence) || !m.lifetimeCurrent() {
		return deny()
	}
	return nil
}

// CheckLocalMCPProtocolAuthority is a read-only same-Store join for fixed MCP
// initialize/initialized/list protocol messages. It never mints or consumes a
// tools/call capability; that remains exclusively BeforeSend authorization.
func (store *Store) CheckLocalMCPProtocolAuthority(ctx context.Context, op domain.AttemptOperationV1) error {
	if store == nil || op.Kind != domain.AttemptOperationMCPToolV1 || !store.operationReady(ctx) || !store.localCompositionReady() || store.localBootstrap == nil {
		return domain.ErrMembershipDenied
	}
	entry := store.localBootstrap.entry.Load()
	if entry == nil {
		return domain.ErrMembershipDenied
	}
	m := entry.preparedMCP.Load()
	if m == nil || !entry.original() || m.source == nil || m.source.state == nil {
		return domain.ErrMembershipDenied
	}
	unlock, err := entry.beginLocalMutation(ctx)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer unlock()
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if ctx.Err() != nil || !m.originalForPhase(entry, localCompactionBeforeSend) || !reflect.DeepEqual(op, m.operation) {
		return domain.ErrMembershipDenied
	}
	call, finish, err := entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer finish()
	err = store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if !q.at.Before(m.reservation.Operation.Deadline) {
			return q.budget.deny()
		}
		if err := m.rejoinAndValidateCurrent(ctx, q, localCompactionBeforeSend, entry.parent, entry.source); err != nil {
			return err
		}
		allocation, err := readOwnedAllocationSnapshots(ctx, q, entry.parent, entry.source, m.source.state.snapshot.Accounting.SetupReservedAt)
		if err != nil || allocation == nil || allocation.current.ValidateAt(q.at) != nil || m.operation.Deadline.After(allocation.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		proxy, err := readOwnedProxySnapshotAt(ctx, q, entry.parent, entry.source, allocation.original, op.Sequence)
		if err != nil || proxy == nil || proxy.current.ValidateAt(q.at) != nil || m.operation.Deadline.After(proxy.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		ad, ae := allocation.current.Digest()
		pd, pe := proxy.current.Digest()
		if ae != nil || pe != nil || ad != m.reservation.RegisteredAllocationDigest || pd != m.reservation.RegisteredProxyDigest {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || call.Err() != nil || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (store *Store) rememberLocalMCPObservation(ctx context.Context, observed ports.AuthenticatedAttemptOperationObservationV1) {
	if store == nil || store.localBootstrap == nil || observed.Validate() != nil {
		return
	}
	entry := store.localBootstrap.entry.Load()
	if !entry.original() {
		return
	}
	m := entry.preparedMCP.Load()
	if m == nil || m.source == nil || m.source.state == nil {
		return
	}
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if ctx.Err() != nil || !m.originalForPhase(entry, localCompactionBeforeSend) || !reflect.DeepEqual(observed.Reservation, m.reservation) || store.attemptOperationIssuer.VerifyAttemptOperationObservation(observed.Clone()) != nil {
		return
	}
	life, err := entry.owner.source.ExecutionLifetime()
	if err != nil || life.Err() != nil || entry.invocationContext.Err() != nil || ctx.Err() != nil {
		return
	}
	m.observation = observed.Clone()
}

func (store *Store) validateLocalNormalizedMCP(ctx context.Context, op domain.AttemptOperationV1, observation domain.AttemptOperationObservationV1) error {
	if store == nil || store.localBootstrap == nil || !store.localNormalizedMCPSuccess(op, observation) {
		return domain.ErrMembershipDenied
	}
	entry := store.localBootstrap.entry.Load()
	if entry == nil {
		return domain.ErrMembershipDenied
	}
	m := entry.preparedMCP.Load()
	if m == nil || !entry.original() || !m.originalForPhase(entry, localCompactionBeforeSend) || !reflect.DeepEqual(m.operation, op) {
		return domain.ErrMembershipDenied
	}
	request := m.request.Body()
	defer clear(request)
	body, err := store.readOwnedOperationBody(ctx, op.Scope, observation.ResponseRef, observation.ResponseDigest, observation.ResponseBytes, min(op.ResultBytes, 64<<10))
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer clear(body)
	result, err := mcpadmission.DecodeToolResultV1(m.grant, op, request, body)
	if err != nil || !reflect.DeepEqual(result.Usage(), observation.Usage) || (result.IsError() != (observation.Reason == domain.AttemptOperationReasonServiceErrorV1)) {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (store *Store) localNormalizedMCPSuccess(op domain.AttemptOperationV1, o domain.AttemptOperationObservationV1) bool {
	return op.Kind == domain.AttemptOperationMCPToolV1 && o.Outcome == domain.AttemptOperationCompletedV1 &&
		(o.Reason == domain.AttemptOperationReasonNoneV1 || o.Reason == domain.AttemptOperationReasonServiceErrorV1)
}

func localMCPCanonicalToolEvents(callID, name string, arguments []byte, result mcpadmission.ResultV1) ([]ports.ExecutionToolEvent, error) {
	callPayload, err := json.Marshal(struct {
		ID        string          `json:"call_id"`
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}{callID, name, bytes.Clone(arguments)})
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	resultPayload, err := json.Marshal(struct {
		ID      string   `json:"call_id"`
		IsError bool     `json:"is_error"`
		Content []string `json:"content"`
	}{result.CallID(), result.IsError(), result.Content()})
	if err != nil {
		clear(callPayload)
		return nil, domain.ErrMembershipDenied
	}
	return []ports.ExecutionToolEvent{
		{Kind: domain.SessionEventToolCall, CallID: callID, ToolName: name, Payload: callPayload},
		{Kind: domain.SessionEventToolResult, CallID: callID, ToolName: name, Payload: resultPayload},
	}, nil
}
