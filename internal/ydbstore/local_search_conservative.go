package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Unknown USD retains the actual per-search reservation. This transient private
// witness is installed only after the original issuer and owned normalized body
// agree. Typed result/capsule DATA never recreates it or grants fresh execution.
type localSearchConservativeV4 struct {
	self     *localSearchConservativeV4
	owner    *localPreparedSearchV1
	observed ports.AuthenticatedAttemptOperationObservationV1
	data     compactioncontext.SearchToolDataV3
}

func (*localSearchConservativeV4) String() string {
	return "localSearchConservativeV4{private:[redacted]}"
}

func (entry *LocalInvocationEntryV1) findOriginalSearch(sequence uint64) *localPreparedSearchV1 {
	if !entry.original() || entry.preparedSearch == nil {
		return nil
	}
	m := entry.preparedSearch.Load()
	for depth := 0; m != nil && depth < 2; depth, m = depth+1, m.previous {
		if m.self != m || m.entry != entry || !m.prepared || m.pending == nil || m.pending.self != m.pending || m.pending.entry != entry || m.conservative == nil {
			return nil
		}
		if m.operation.Sequence == sequence {
			return m
		}
	}
	return nil
}

func (entry *LocalInvocationEntryV1) prepareSearchConservativeLocked(ctx context.Context) error {
	if !entry.original() || ctx == nil || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	if entry.preparedSearch == nil || entry.preparedSearch.Load() == nil {
		return nil
	}
	m := entry.preparedSearch.Load()
	if m.source == nil || m.source.state == nil || m.conservative == nil {
		return domain.ErrMembershipDenied
	}
	m.source.state.mu.Lock()
	defer m.source.state.mu.Unlock()
	if m.conservative.Load() != nil {
		if entry.originalSearchObservationAllowsSuccessor(m.reservation, m.observation.Observation) {
			return nil
		}
		return domain.ErrMembershipDenied
	}
	if !m.originalForPhase(entry, localCompactionBeforeSend) || m.observation.Validate() != nil || !reflect.DeepEqual(m.observation.Reservation, m.reservation) || entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(m.observation.Clone()) != nil || len(m.source.state.operations) == 0 {
		return domain.ErrMembershipDenied
	}
	r, o := m.reservation, m.observation.Observation
	row, err := ports.BuildOwnedOperationAccountingRowV4(r, o, entry.parent, entry.source)
	if err != nil || row.Kind != domain.AttemptOperationWebSearchV1 || row.Disposition != ports.OwnedOperationUnknownReservedV4 || entry.owner.source.VerifyOwnedAttemptEffectSource(entry.source) != nil || chargeOwnedAuthorityBody(ctx, 2*o.ResponseBytes+2*r.Operation.RequestBytes) != nil {
		return domain.ErrMembershipDenied
	}
	request, err := entry.owner.store.readOwnedOperationBody(ctx, r.Operation.Scope, r.Operation.RequestRef, r.Operation.RequestDigest, r.Operation.RequestBytes, domain.WebSearchWireBytesV1)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer clear(request)
	body, err := entry.owner.store.readOwnedOperationBody(ctx, r.Operation.Scope, o.ResponseRef, o.ResponseDigest, o.ResponseBytes, min(r.Operation.ResultBytes, uint64(domain.WebSearchResultBytesV1)))
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer clear(body)
	previous := m.source.state.operations[len(m.source.state.operations)-1]
	actual := LocalCompactionOperationV1{Reservation: r, Observation: o, Request: request, Response: body}
	if entry.validateOriginalSearchMaterial(previous, actual) != nil {
		return domain.ErrMembershipDenied
	}
	result, err := domain.DecodeWebSearchResultV1(body)
	if err != nil || (result.Outcome != "known_results" && result.Outcome != "known_empty") || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	witness := &localSearchConservativeV4{owner: m, observed: m.observation.Clone(), data: compactioncontext.SearchToolDataV3{Accounting: row, Result: result.Clone()}}
	witness.self = witness
	if !m.conservative.CompareAndSwap(nil, witness) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// Closed immutable correspondence over original holders and authenticated
// observations. Current Source/claim/grant checks belong to serving wrappers;
// this historical classifier cannot renew permission after cleanup or expiry.
func (entry *LocalInvocationEntryV1) originalSearchObservationAllowsSuccessor(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1) bool {
	if !entry.original() || r.Operation.Kind != domain.AttemptOperationWebSearchV1 || r.Operation.Scope != ownedOperationScope(entry.parent) {
		return false
	}
	m := entry.findOriginalSearch(r.Operation.Sequence)
	if m == nil || m.conservative == nil {
		return false
	}
	w := m.conservative.Load()
	if w == nil || w.self != w || w.owner != m || !reflect.DeepEqual(m.reservation, r) || !reflect.DeepEqual(m.operation, r.Operation) || !reflect.DeepEqual(m.observation, w.observed) || !reflect.DeepEqual(w.observed.Reservation, r) || !reflect.DeepEqual(w.observed.Observation, o) || entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(w.observed.Clone()) != nil {
		return false
	}
	row, err := ports.BuildOwnedOperationAccountingRowV4(r, o, entry.parent, entry.source)
	return err == nil && reflect.DeepEqual(row, w.data.Accounting) && w.data.Result.ValidateForOperation(r.Operation) == nil && (w.data.Result.Outcome == "known_results" || w.data.Result.Outcome == "known_empty")
}

func (entry *LocalInvocationEntryV1) validateOriginalSearchMaterial(model, search LocalCompactionOperationV1) error {
	op, observation := search.Reservation.Operation, search.Observation
	m := entry.findOriginalSearch(op.Sequence)
	if m == nil || m.pending == nil || m.pending.grant == nil || m.frozen == nil || !reflect.DeepEqual(m.operation, op) || m.frozen.ValidateOperation(op) != nil || !m.exactRequest(search.Request) || !reflect.DeepEqual(model.Reservation, m.pending.model) || !reflect.DeepEqual(model.Observation, m.pending.observation) || op.ValidatePredecessor(model.Reservation, model.Observation) != nil || observation.ValidateForReservation(search.Reservation) != nil || observation.Outcome != domain.AttemptOperationCompletedV1 || observation.Reason != domain.AttemptOperationReasonNoneV1 || observation.Usage.State != domain.AttemptOperationUsageUnknownV1 {
		return domain.ErrMembershipDenied
	}
	decoded, err := configuredinference.DecodeNormalizedResponseBodyV1(entry.owner.configuration, model.Request, model.Response, domain.AttemptOperationModelV1)
	if decoded.Call != nil {
		defer clear(decoded.Call.Arguments)
	}
	if err != nil || decoded.Stop != configuredinference.StopToolV1 || decoded.Call == nil || decoded.Call.Name != "web_search" || decoded.Call.ID != op.CallID || !bytes.Equal(decoded.Call.Arguments, m.pending.arguments) || op.SearchWire.ArgumentsBytes != uint64(len(decoded.Call.Arguments)) || m.pending.request.Arguments.Size != int64(len(decoded.Call.Arguments)) || m.frozen.ValidateConsentRequest(m.pending.request) != nil {
		return domain.ErrMembershipDenied
	}
	rd, _ := m.pending.request.Digest()
	if m.pending.state.Status != domain.WebSearchConsentApprovedV1 || m.pending.state.Decision == nil || m.pending.state.Decision.Validate() != nil || string(rd) != op.ConsentRequestDigest {
		return domain.ErrMembershipDenied
	}
	dd, _ := m.pending.state.Decision.Digest()
	grant := m.pending.grant.grant
	result, err := domain.DecodeWebSearchResultV1(search.Response)
	hash := sha256.Sum256(search.Response)
	if err != nil || string(dd) != op.ConsentDecisionDigest || op.ConsentDecisionRef != string(m.pending.request.ID) || result.ValidateForOperation(op) != nil || result.ProfileRevision != grant.ProfileRevision || result.RetrievedAt.Before(search.Reservation.ReservedAt) || result.RetrievedAt.After(observation.ObservedAt) || hex.EncodeToString(hash[:]) != observation.ResponseDigest || uint64(len(search.Response)) != observation.ResponseBytes {
		return domain.ErrMembershipDenied
	}
	return nil
}
