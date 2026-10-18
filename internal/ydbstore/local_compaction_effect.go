package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Lookup retains identity only. Every mutable generation field is inspected
// under its original source mutex. The factory publishes entry/source/self
// before claiming the slot; partially prepared owners remain denied tombstones.
func (store *Store) localCompactionOwner(op domain.AttemptOperationV1) *localCompactionGenerationV1 {
	if store == nil || op.Kind != domain.AttemptOperationCompactionV1 || !store.localCompositionReady() || store.localBootstrap == nil {
		return nil
	}
	entry := store.localBootstrap.entry.Load()
	if !entry.original() || entry.owner.store != store {
		return nil
	}
	return entry.compactionGeneration.Load()
}

// Caller holds g.source.state.mu. No valid DATA-only program/fit/operation can
// manufacture this original entry/retained-pair/price-custody relationship.
func (store *Store) preparedLocalCompactionLocked(g *localCompactionGenerationV1, op domain.AttemptOperationV1) bool {
	if g == nil || g.self != g || g.entry == nil || !g.entry.original() || g.entry.owner.store != store || g.entry.compactionGeneration.Load() != g ||
		g.source == nil || g.source.self != g.source || g.source.entry != g.entry || g.source.state == nil || g.source.state.cleared ||
		g.program == nil || g.frozen == nil || g.program.FrozenRequest() != g.frozen || g.retained == nil || g.retained.self != g.retained || g.retained.source != g.source || g.retained.entry != g.entry ||
		g.retained.pair == nil || g.price == nil || g.price.local == nil || g.price.local.source != g.entry.owner.source || !store.localInvoiceLifetimeCurrent(g.price.local) ||
		g.entry.invocationContext == nil || g.entry.invocationContext.Err() != nil || !reflect.DeepEqual(g.operation, op) || !reflect.DeepEqual(g.retained.operation, op) ||
		g.fit.RequestDigest != g.frozen.Digest() || g.fit.CalibrationDigest != g.entry.owner.configuration.CalibrationDigest ||
		g.fit.InputUpperBound == 0 || g.fit.InputUpperBound != op.Debit.InputTokens || g.fit.OutputReserve == 0 || g.fit.OutputReserve != op.Debit.OutputTokens ||
		g.fit.TotalUpperBound != g.fit.InputUpperBound+g.fit.OutputReserve || g.program.SourceDigest() != g.source.state.digest {
		return false
	}
	return true
}

func (store *Store) prepareLocalCompactionEvidence(ctx context.Context, op domain.AttemptOperationV1) (*ownedOperationEvidence, bool, error) {
	g := store.localCompactionOwner(op)
	if g == nil {
		return nil, false, nil // preserve existing non-prepared/fixture contracts
	}
	if g.source == nil || g.source.state == nil {
		return nil, true, domain.ErrMembershipDenied
	}
	g.source.state.mu.Lock()
	defer g.source.state.mu.Unlock()
	if ctx.Err() != nil || !store.preparedLocalCompactionLocked(g, op) {
		return nil, true, domain.ErrMembershipDenied
	}
	call, finish, err := g.entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return nil, true, domain.ErrMembershipDenied
	}
	defer finish()
	body, err := store.readOwnedOperationBody(call, op.Scope, op.RequestRef, op.RequestDigest, op.RequestBytes, 1<<20)
	if err != nil {
		return nil, true, domain.ErrMembershipDenied
	}
	defer clear(body)
	exact := g.frozen.Body()
	defer clear(exact)
	if !bytes.Equal(body, exact) || call.Err() != nil {
		return nil, true, domain.ErrMembershipDenied
	}
	return &ownedOperationEvidence{price: g.price, compaction: g}, true, nil
}

// A successful lock spans the complete actual Store transaction and capability
// mint/consume. It cannot authorize an operation independently of that path.
func (store *Store) lockLocalCompactionEffect(ctx context.Context, evidence *ownedOperationEvidence, op domain.AttemptOperationV1, phase localCompactionGenerationPhase) (func(), error) {
	if evidence == nil || evidence.compaction == nil {
		return func() {}, nil
	}
	g := evidence.compaction
	if g.source == nil || g.source.state == nil {
		return nil, domain.ErrMembershipDenied
	}
	g.source.state.mu.Lock()
	if ctx.Err() != nil || !store.preparedLocalCompactionLocked(g, op) ||
		(phase != localCompactionReserve && phase != localCompactionBeforeSend) ||
		(phase == localCompactionReserve && (g.reservation.Version != 0 || g.reservationAttempted)) ||
		(phase == localCompactionBeforeSend && (g.reservation.Validate() != nil || !reflect.DeepEqual(g.reservation.Operation, op))) {
		g.source.state.mu.Unlock()
		return nil, domain.ErrMembershipDenied
	}
	return g.source.state.mu.Unlock, nil
}

// This is the prepared arm of the SAME Store port, issuer and accounting, not
// another runner or reservation authority. Join the complete source once, then
// reuse that transaction's validated preimage instead of repeating the generic
// parent/history reads and overflowing its unchanged 32-statement ceiling.
func (store *Store) reservePreparedLocalCompaction(ctx context.Context, op domain.AttemptOperationV1, evidence *ownedOperationEvidence) (ports.ReserveAttemptOperationResultV1, error) {
	caller := ctx
	unlock, err := store.lockLocalCompactionEffect(ctx, evidence, op, localCompactionReserve)
	if err != nil {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	defer unlock()
	g := evidence.compaction
	call, finish, err := g.entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	defer finish()
	ctx = call
	var candidate domain.AttemptOperationReservationV1
	var at time.Time
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		candidate, at = domain.AttemptOperationReservationV1{}, time.Time{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if g.reservationAttempted {
			return q.budget.deny()
		}
		if _, err := g.rejoinLocked(ctx, q, localCompactionReserve); err != nil {
			return err
		}
		parent, execution := g.entry.parent, g.entry.source
		if store.validateOwnedOperationProviderPrice(ctx, q, op, evidence.price, execution) != nil {
			return q.budget.deny()
		}
		allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, execution, g.source.state.snapshot.Accounting.SetupReservedAt)
		if err != nil || allocation == nil || allocation.current.ValidateAt(q.at) != nil || op.Deadline.After(allocation.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		proxy, err := readOwnedProxySnapshotAt(ctx, q, parent, execution, allocation.original, op.Sequence)
		if err != nil || proxy == nil {
			return q.budget.deny()
		}
		pd, err := g.futureProxyDigest(q, proxy)
		if err != nil {
			return q.budget.deny()
		}
		old, err := readOwnedOperation(ctx, q, op)
		if err != nil || old != nil {
			return q.budget.deny()
		}
		next, err := debitOwnedOperation(g.source.state.snapshot.Accounting, op, parent, execution, q.at)
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
		// Irreversible before first mutating dispatch, including a lost ACK with
		// no persistence. The SDK cannot retry this callback into fresh authority.
		g.reservationAttempted = true
		if _, err = q.ExecContext(ctx, `INSERT INTO attempt_operations_v1 (tenant_id,attempt_id,sequence,record,digest,operation_digest,call_id) VALUES ($1,$2,$3,$4,$5,$6,$7)`, op.Scope.TenantID, op.Scope.AttemptID, op.Sequence, string(raw), string(digest), string(od), op.CallID); err != nil {
			return err
		}
		if _, err = q.ExecContext(ctx, `INSERT INTO attempt_operation_calls_v1 (tenant_id,attempt_id,call_id,sequence,operation_digest) VALUES ($1,$2,$3,$4,$5)`, op.Scope.TenantID, op.Scope.AttemptID, op.CallID, op.Sequence, string(od)); err != nil {
			return err
		}
		if err = writeOwnedAttemptAccounting(ctx, q, parent, execution, next); err != nil {
			return err
		}
		candidate, at = value.Clone(), q.at
		return nil
	})
	if err != nil || ctx.Err() != nil || !store.operationEvidenceLifetimeCurrent(evidence) {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	result, err := store.operationResult(candidate, at, true)
	// Minting can block inside the ORIGINAL issuer. A confirmed debit remains
	// consumed, but late cancellation cannot install or return fresh authority.
	if err != nil || caller.Err() != nil || ctx.Err() != nil || g.entry.invocationContext.Err() != nil || !store.operationEvidenceLifetimeCurrent(evidence) {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	g.reservation = candidate.Clone()
	return result, err
}

func (store *Store) authorizePreparedLocalCompaction(ctx context.Context, capability ports.AttemptOperationCapabilityV1, evidence *ownedOperationEvidence) error {
	op := capability.Reservation.Operation
	unlock, err := store.lockLocalCompactionEffect(ctx, evidence, op, localCompactionBeforeSend)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer unlock()
	g := evidence.compaction
	call, finish, err := g.entry.localCompactionLifetimeContext(ctx)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer finish()
	caller := ctx
	ctx = call
	if !reflect.DeepEqual(capability.Reservation, g.reservation) {
		return domain.ErrMembershipDenied
	}
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if !q.at.Before(capability.ExpiresAt) || q.at.Before(capability.IssuedAt) {
			return q.budget.deny()
		}
		if _, err := g.rejoinLocked(ctx, q, localCompactionBeforeSend); err != nil {
			return err
		}
		parent, execution := g.entry.parent, g.entry.source
		if store.validateOwnedOperationProviderPrice(ctx, q, op, evidence.price, execution) != nil {
			return q.budget.deny()
		}
		allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, execution, g.source.state.snapshot.Accounting.SetupReservedAt)
		if err != nil || allocation == nil || allocation.current.ValidateAt(q.at) != nil || op.Deadline.After(allocation.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		proxy, err := readOwnedProxySnapshotAt(ctx, q, parent, execution, allocation.original, op.Sequence)
		if err != nil || proxy == nil || proxy.current.ValidateAt(q.at) != nil || op.Deadline.After(proxy.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		ad, ae := allocation.current.Digest()
		pd, pe := proxy.current.Digest()
		if ae != nil || pe != nil || ad != g.reservation.RegisteredAllocationDigest || pd != g.reservation.RegisteredProxyDigest {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || caller.Err() != nil || ctx.Err() != nil || !store.operationEvidenceLifetimeCurrent(evidence) || store.attemptOperationIssuer.ConsumeAttemptOperationCapability(capability) != nil {
		return domain.ErrMembershipDenied
	}
	// A consumed capability is never refunded, including cancellation while
	// Consume was blocked in the original issuer.
	if caller.Err() != nil || ctx.Err() != nil || g.entry.invocationContext.Err() != nil || !store.operationEvidenceLifetimeCurrent(evidence) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// A historical authenticated read is not a new send or publication grant. Keep
// this actual original Store result only for a later full completed-phase join.
// Unknown/failed effects and abandoned/closed preparations never become ready.
func (store *Store) rememberLocalCompactionObservation(ctx context.Context, observed ports.AuthenticatedAttemptOperationObservationV1) {
	if ctx.Err() != nil {
		return
	}
	g := store.localCompactionOwner(observed.Reservation.Operation)
	if g == nil || g.source == nil || g.source.state == nil {
		return
	}
	g.source.state.mu.Lock()
	defer g.source.state.mu.Unlock()
	if ctx.Err() != nil || !store.preparedLocalCompactionLocked(g, observed.Reservation.Operation) || store.attemptOperationIssuer.VerifyAttemptOperationObservation(observed.Clone()) != nil ||
		!reflect.DeepEqual(g.reservation, observed.Reservation) || !ownedObservationAllowsSuccessor(observed.Reservation, observed.Observation) {
		return
	}
	if g.observation.Version != 0 && !reflect.DeepEqual(g.observation, observed) {
		return
	}
	if ctx.Err() != nil {
		return
	}
	g.observation = observed.Clone()
}
