package ydbstore

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// The same original holder alone may install this private witness AFTER actual
// durable normalized bytes and the original issuer have been verified. The DATA
// accessor below is not this witness. Losing it denies continuation; it is never
// reconstructed from a retained capsule or caller projection.
type localMCPConservativeV2 struct {
	self             *localMCPConservativeV2
	owner            *localPreparedMCPV1
	observed         ports.AuthenticatedAttemptOperationObservationV1
	data             compactioncontext.MCPConservativeDataV2
	name, descriptor string
}

func (*localMCPConservativeV2) MarshalJSON() ([]byte, error) {
	return nil, errors.New("original MCP conservative witness is transient ownership")
}
func (*localMCPConservativeV2) String() string { return "localMCPConservativeV2{private:[redacted]}" }

// Caller holds entry.preparationMu. This does no SQL and reads actual bodies
// only outside retryable callbacks, under the SAME logical byte/time budget.
func (entry *LocalInvocationEntryV1) prepareMCPConservativeLocked(ctx context.Context) error {
	if !entry.original() || ctx == nil || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	m := entry.preparedMCP.Load()
	if m == nil {
		return nil
	}
	if m.source == nil || m.source.state == nil || m.conservative == nil {
		return domain.ErrMembershipDenied
	}
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if w := m.conservative.Load(); w != nil {
		if entry.originalObservationAllowsSuccessor(m.reservation, m.observation.Observation) {
			return nil
		}
		return domain.ErrMembershipDenied
	}
	if !m.originalForPhase(entry, localCompactionBeforeSend) || m.observation.Validate() != nil || !reflect.DeepEqual(m.observation.Reservation, m.reservation) || entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(m.observation.Clone()) != nil || len(m.source.state.operations) == 0 {
		return domain.ErrMembershipDenied
	}
	o, r := m.observation.Observation, m.reservation
	row, err := ports.BuildOwnedOperationAccountingRowV3(r, o, entry.parent, entry.source)
	if err != nil || row.Kind != domain.AttemptOperationMCPToolV1 || row.Disposition != ports.OwnedOperationUnknownFullReservedV3 || entry.owner.source.VerifyOwnedAttemptEffectSource(entry.source) != nil {
		return domain.ErrMembershipDenied
	}
	grant, err := entry.owner.source.ReadMCPToolGrant(ctx, entry.source.Profile, o.ObservedAt)
	gd, ge := grant.Digest()
	want, we := m.grant.Digest()
	if err != nil || ge != nil || we != nil || gd != want || gd != r.Operation.ToolGrantDigest {
		return domain.ErrMembershipDenied
	}
	if chargeOwnedAuthorityBody(ctx, 2*o.ResponseBytes+2*r.Operation.RequestBytes) != nil {
		return domain.ErrMembershipDenied
	}
	body, err := entry.owner.store.readOwnedOperationBody(ctx, r.Operation.Scope, o.ResponseRef, o.ResponseDigest, o.ResponseBytes, min(r.Operation.ResultBytes, 64<<10))
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer clear(body)
	request := m.request.Body()
	defer clear(request)
	storedRequest, err := entry.owner.store.readOwnedOperationBody(ctx, r.Operation.Scope, r.Operation.RequestRef, r.Operation.RequestDigest, r.Operation.RequestBytes, 4<<10)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer clear(storedRequest)
	if !bytes.Equal(request, storedRequest) {
		return domain.ErrMembershipDenied
	}
	result, err := mcpadmission.DecodeToolResultV1(grant, r.Operation, request, body)
	previous := m.source.state.operations[len(m.source.state.operations)-1]
	if err != nil || result.IsError() || !reflect.DeepEqual(result.Usage(), o.Usage) || previous.Reservation.Operation.Kind != domain.AttemptOperationModelV1 || r.Operation.ValidatePredecessor(previous.Reservation, previous.Observation) != nil || result.ValidateConfiguredCallV1(entry.owner.configuration, previous.Reservation, previous.Observation, previous.Request, previous.Response) != nil || result.CallID() != m.call.ID() || result.Name() != m.call.Name() || result.ArgumentsDigest() != m.call.ArgumentsDigest() || result.ModelResponseDigest() != m.call.ModelResponseDigest() || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	rd, _ := r.Digest()
	od, _ := o.Digest()
	w := &localMCPConservativeV2{owner: m, observed: m.observation.Clone(), name: result.Name(), descriptor: result.DescriptorDigest(), data: compactioncontext.MCPConservativeDataV2{Version: 2, Scope: r.Operation.Scope, Sequence: r.Operation.Sequence, ReservationDigest: rd, ObservationDigest: od, Reserved: r.Operation.Debit, MCPSpendingDigest: row.MCPSpendingDigest}}
	w.self = w
	if !m.conservative.CompareAndSwap(nil, w) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// Exact private historical classification only. It neither reads current
// funding nor grants reserve/send. Every serving caller must use its original
// current-authority snapshot wrapper; cleanup has a separately guarded owner.
func (entry *LocalInvocationEntryV1) originalObservationAllowsSuccessor(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1) bool {
	if !entry.original() || r.Operation.Scope != ownedOperationScope(entry.parent) || r.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil {
		return false
	}
	if ownedObservationAllowsSuccessor(r, o) {
		return true
	}
	if r.Operation.Kind != domain.AttemptOperationMCPToolV1 {
		return false
	}
	m := entry.preparedMCP.Load()
	if m == nil || m.self != m || m.entry != entry || m.conservative == nil {
		return false
	}
	w := m.conservative.Load()
	if w == nil || w.self != w || w.owner != m || !reflect.DeepEqual(w.observed.Reservation, r) || !reflect.DeepEqual(w.observed.Observation, o) || !reflect.DeepEqual(m.reservation, r) || !reflect.DeepEqual(m.operation, r.Operation) || !reflect.DeepEqual(m.observation, w.observed) || entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(w.observed.Clone()) != nil ||
		m.call.ID() != r.Operation.CallID || m.call.Name() != w.name || m.call.GrantDigest() != r.Operation.ToolGrantDigest || m.call.ArgumentsDigest() != r.Operation.ArgumentsDigest || m.call.ModelResponseDigest() != r.Operation.ModelResponseDigest || m.call.DescriptorDigest() != w.descriptor || m.request.ID() != r.Operation.CallID || m.request.DescriptorDigest() != w.descriptor {
		return false
	}
	row, err := ports.BuildOwnedOperationAccountingRowV3(r, o, entry.parent, entry.source)
	rd, re := r.Digest()
	od, oe := o.Digest()
	return err == nil && re == nil && oe == nil && w.data.Version == 2 && w.data.Scope == r.Operation.Scope && w.data.Sequence == r.Operation.Sequence && w.data.ReservationDigest == rd && w.data.ObservationDigest == od && w.data.Reserved == r.Operation.Debit && w.data.MCPSpendingDigest == row.MCPSpendingDigest
}

// Source already MAC-bound Spending[2].Window to the minimum grant, price,
// terms and actual Source deadline. Never recreate that end from a later read.
// This method rereads the retained independent funding at authoritative q.at.
func (entry *LocalInvocationEntryV1) revalidateMCPConservativeFunding(ctx context.Context, at time.Time) error {
	if !entry.original() {
		return domain.ErrMembershipDenied
	}
	m := entry.preparedMCP.Load()
	if m == nil || m.conservative == nil || m.conservative.Load() == nil {
		return nil
	}
	if !entry.originalObservationAllowsSuccessor(m.reservation, m.observation.Observation) || !at.Before(entry.source.Spending[2].Window.ExpiresAt) || !at.Before(entry.source.ExpiresAt) {
		return domain.ErrMembershipDenied
	}
	if entry.owner.source.VerifyOwnedAttemptEffectSource(entry.source) != nil {
		return domain.ErrMembershipDenied
	}
	grant, err := entry.owner.source.ReadMCPToolGrant(ctx, entry.source.Profile, at)
	d, de := grant.Digest()
	if err != nil || de != nil || d != m.operation.ToolGrantDigest || entry.source.Spending[2].ValidateAt(at) != nil || entry.allocation.Window.ExpiresAt.After(entry.source.Spending[2].Window.ExpiresAt) || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (entry *LocalInvocationEntryV1) mcpConservativeDataFor(operations []LocalCompactionOperationV1) ([]compactioncontext.MCPConservativeDataV2, error) {
	if !entry.original() || len(operations) > 12 {
		return nil, domain.ErrMembershipDenied
	}
	var result []compactioncontext.MCPConservativeDataV2
	for _, op := range operations {
		if op.Reservation.Operation.Kind != domain.AttemptOperationMCPToolV1 || op.Observation.Usage.State != domain.AttemptOperationUsageUnknownV1 {
			continue
		}
		if !entry.originalObservationAllowsSuccessor(op.Reservation, op.Observation) {
			return nil, domain.ErrMembershipDenied
		}
		if len(result) != 0 {
			return nil, domain.ErrMembershipDenied
		}
		m := entry.preparedMCP.Load()
		result = append(result, m.conservative.Load().data)
	}
	return result, nil
}

// Called only by the concrete original proxy publisher with the entry mutation
// barrier held and current parent/allocation/accounting rows already joined in
// the SAME transaction. It grants no generic measured-money exception: the
// exact next MODEL/COMPACTION must already exist in its original private slot.
func (store *Store) localMCPProxyContinuation(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, proof ports.OwnedProxyPublicationV2, previous *ownedOperationRow) bool {
	if store.localBootstrap == nil || previous == nil || previous.observation == nil || previous.reservation.Operation.Kind != domain.AttemptOperationMCPToolV1 || proof.ServiceKind != domain.RegisteredProxyProviderV1 || proof.TransportKind != domain.RegisteredProxyTransportLocalLoopbackHTTPV2 {
		return false
	}
	entry := store.localBootstrap.entry.Load()
	if !entry.original() || entry.owner.store != store || !reflect.DeepEqual(entry.parent, parent) || !reflect.DeepEqual(entry.source, source) || !entry.originalObservationAllowsSuccessor(previous.reservation, *previous.observation) || entry.revalidateMCPConservativeFunding(ctx, q.at) != nil {
		return false
	}
	var owner localSnapshotOwner
	if model := entry.preparedModel.Load(); model != nil && model.operation.Sequence == proof.OperationSequence {
		owner.model = model
	} else if generation := entry.compactionGeneration.Load(); generation != nil && generation.operation.Sequence == proof.OperationSequence {
		owner.generation = generation
	}
	if !owner.present() {
		return false
	}
	var preparedSource *LocalCompactionSourceV1
	if owner.model != nil {
		preparedSource = owner.model.source
	} else {
		preparedSource = owner.generation.source
	}
	if preparedSource == nil || preparedSource.state == nil {
		return false
	}
	preparedSource.state.mu.Lock()
	defer preparedSource.state.mu.Unlock()
	op := owner.operation()
	digest, err := op.Digest()
	return err == nil && owner.original(entry, localCompactionReserve) && op.Scope == ownedOperationScope(parent) && op.Sequence == proof.OperationSequence && digest == proof.OperationDigest && op.Deadline.Equal(proof.OperationDeadline) && op.ValidatePredecessor(previous.reservation, *previous.observation) == nil && q.at.Before(op.Deadline)
}
