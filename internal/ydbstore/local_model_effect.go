package ydbstore

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func (store *Store) localPreparedModelOwner(op domain.AttemptOperationV1) *localPreparedModelV1 {
	if store == nil || op.Kind != domain.AttemptOperationModelV1 || !store.localCompositionReady() || store.localBootstrap == nil {
		return nil
	}
	entry := store.localBootstrap.entry.Load()
	if !entry.original() || entry.owner.store != store {
		return nil
	}
	return entry.preparedModel.Load()
}
func (store *Store) prepareLocalModelEvidence(ctx context.Context, op domain.AttemptOperationV1) (*ownedOperationEvidence, bool, error) {
	m := store.localPreparedModelOwner(op)
	if m == nil {
		return nil, false, nil
	}
	if m.source == nil || m.source.state == nil {
		return nil, true, domain.ErrMembershipDenied
	}
	unlock, err := m.entry.beginLocalMutation(ctx)
	if err != nil {
		return nil, true, domain.ErrMembershipDenied
	}
	defer unlock()
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if ctx.Err() != nil || !m.originalForPhase(m.entry, localCompactionReserve) || !reflect.DeepEqual(m.operation, op) {
		return nil, true, domain.ErrMembershipDenied
	}
	call, finish, err := m.entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return nil, true, err
	}
	defer finish()
	body, err := store.readOwnedOperationBody(call, op.Scope, op.RequestRef, op.RequestDigest, op.RequestBytes, 1<<20)
	if err != nil {
		return nil, true, domain.ErrMembershipDenied
	}
	defer clear(body)
	if !localModelBodyEqual(m.frozen, body) || call.Err() != nil {
		return nil, true, domain.ErrMembershipDenied
	}
	return &ownedOperationEvidence{price: m.price, model: m}, true, nil
}

func (store *Store) reservePreparedLocalModel(ctx context.Context, op domain.AttemptOperationV1, e *ownedOperationEvidence) (ports.ReserveAttemptOperationResultV1, error) {
	deny := func() (ports.ReserveAttemptOperationResultV1, error) {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	m := e.model
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
	if ctx.Err() != nil || !m.originalForPhase(m.entry, localCompactionReserve) || !reflect.DeepEqual(m.operation, op) || m.reservationAttempted || m.reservation.Version != 0 {
		return deny()
	}
	call, finish, err := m.entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return deny()
	}
	defer finish()
	caller := ctx
	ctx = call
	var candidate domain.AttemptOperationReservationV1
	var at time.Time
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		candidate, at = domain.AttemptOperationReservationV1{}, time.Time{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if m.reservationAttempted {
			return q.budget.deny()
		}
		if _, err := m.rejoinLocked(ctx, q, localCompactionReserve); err != nil {
			return err
		}
		parent, source := m.entry.parent, m.entry.source
		if store.validateOwnedOperationProviderPrice(ctx, q, op, e.price, source) != nil {
			return q.budget.deny()
		}
		allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, source, m.source.state.snapshot.Accounting.SetupReservedAt)
		if err != nil || allocation == nil || allocation.current.ValidateAt(q.at) != nil || op.Deadline.After(allocation.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		proxy, err := readOwnedProxySnapshotAt(ctx, q, parent, source, allocation.original, op.Sequence)
		if err != nil || proxy == nil {
			return q.budget.deny()
		}
		pd, err := m.entry.localFutureProxyDigest(q, proxy, op)
		if err != nil {
			return err
		}
		old, err := readOwnedOperation(ctx, q, op)
		if err != nil || old != nil {
			return q.budget.deny()
		}
		next, err := debitOwnedOperation(m.source.state.snapshot.Accounting, op, parent, source, q.at)
		if err != nil {
			return q.budget.deny()
		}
		ad, _ := allocation.original.Digest()
		od, _ := op.Digest()
		value := domain.AttemptOperationReservationV1{Version: 1, Operation: op.Clone(), OperationDigest: od, ReservedAt: q.at, AggregateDebit: next.Aggregate, RegisteredAllocationDigest: ad, RegisteredProxyDigest: pd, ComponentAmounts: next.Amounts}
		if value.ValidateForParent(parent.Authority, parent.Reservation) != nil {
			return q.budget.deny()
		}
		raw, _ := json.Marshal(value)
		digest, _ := value.Digest()
		if len(raw) > ownedAuthorityHeadBytes {
			return q.budget.deny()
		}
		m.reservationAttempted = true // BEFORE first dispatch, including nonpersisted lost ACK.
		if _, err = q.ExecContext(ctx, `INSERT INTO attempt_operations_v1 (tenant_id,attempt_id,sequence,record,digest,operation_digest,call_id) VALUES ($1,$2,$3,$4,$5,$6,$7)`, op.Scope.TenantID, op.Scope.AttemptID, op.Sequence, string(raw), string(digest), string(od), op.CallID); err != nil {
			return err
		}
		if _, err = q.ExecContext(ctx, `INSERT INTO attempt_operation_calls_v1 (tenant_id,attempt_id,call_id,sequence,operation_digest) VALUES ($1,$2,$3,$4,$5)`, op.Scope.TenantID, op.Scope.AttemptID, op.CallID, op.Sequence, string(od)); err != nil {
			return err
		}
		if err = writeOwnedAttemptAccounting(ctx, q, parent, source, next); err != nil {
			return err
		}
		candidate, at = value.Clone(), q.at
		return nil
	})
	if err != nil || ctx.Err() != nil || !store.operationEvidenceLifetimeCurrent(e) {
		return deny()
	}
	result, err := store.operationResult(candidate, at, true)
	// The ORIGINAL issuer may block while minting. A confirmed reservation
	// stays charged, but revocation during mint must not return fresh authority
	// or install a ready owner, even if an AfterFunc has not run yet.
	if err != nil || caller.Err() != nil || ctx.Err() != nil || m.entry.invocationContext.Err() != nil || !store.operationEvidenceLifetimeCurrent(e) {
		return deny()
	}
	m.reservation = candidate.Clone()
	return result, err
}

func (store *Store) authorizePreparedLocalModel(ctx context.Context, capability ports.AttemptOperationCapabilityV1, e *ownedOperationEvidence) error {
	m := e.model
	if m == nil || m.source == nil || m.source.state == nil {
		return domain.ErrMembershipDenied
	}
	unlock, err := m.entry.beginLocalMutation(ctx)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer unlock()
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	op := capability.Reservation.Operation
	if ctx.Err() != nil || !m.originalForPhase(m.entry, localCompactionBeforeSend) || !reflect.DeepEqual(capability.Reservation, m.reservation) || !reflect.DeepEqual(op, m.operation) {
		return domain.ErrMembershipDenied
	}
	call, finish, err := m.entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer finish()
	caller := ctx
	ctx = call
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if !q.at.Before(capability.ExpiresAt) || q.at.Before(capability.IssuedAt) {
			return q.budget.deny()
		}
		if _, err := m.rejoinLocked(ctx, q, localCompactionBeforeSend); err != nil {
			return err
		}
		parent, source := m.entry.parent, m.entry.source
		if store.validateOwnedOperationProviderPrice(ctx, q, op, e.price, source) != nil {
			return q.budget.deny()
		}
		allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, source, m.source.state.snapshot.Accounting.SetupReservedAt)
		if err != nil || allocation == nil || allocation.current.ValidateAt(q.at) != nil || op.Deadline.After(allocation.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		proxy, err := readOwnedProxySnapshotAt(ctx, q, parent, source, allocation.original, op.Sequence)
		if err != nil || proxy == nil || proxy.current.ValidateAt(q.at) != nil || op.Deadline.After(proxy.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		ad, ae := allocation.current.Digest()
		pd, pe := proxy.current.Digest()
		if ae != nil || pe != nil || ad != m.reservation.RegisteredAllocationDigest || pd != m.reservation.RegisteredProxyDigest {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || caller.Err() != nil || ctx.Err() != nil || !store.operationEvidenceLifetimeCurrent(e) || store.attemptOperationIssuer.ConsumeAttemptOperationCapability(capability) != nil {
		return domain.ErrMembershipDenied
	}
	// Consumption is irreversible; cancellation while the real issuer blocks
	// denies the send without refunding or reissuing its capability.
	if caller.Err() != nil || ctx.Err() != nil || m.entry.invocationContext.Err() != nil || !store.operationEvidenceLifetimeCurrent(e) {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (store *Store) rememberLocalModelObservation(ctx context.Context, observed ports.AuthenticatedAttemptOperationObservationV1) {
	m := store.localPreparedModelOwner(observed.Reservation.Operation)
	if m == nil || m.source == nil || m.source.state == nil {
		return
	}
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if ctx.Err() != nil || observed.Validate() != nil || !m.originalForPhase(m.entry, localCompactionBeforeSend) || !reflect.DeepEqual(observed.Reservation, m.reservation) || store.attemptOperationIssuer.VerifyAttemptOperationObservation(observed.Clone()) != nil {
		return
	}
	life, err := m.entry.owner.source.ExecutionLifetime()
	if err != nil || life.Err() != nil || m.entry.invocationContext.Err() != nil || ctx.Err() != nil {
		return
	}
	m.observation = observed.Clone()
}
