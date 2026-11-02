package ydbstore

import (
	"reflect"
	"sync"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Bounded ephemeral original issuer facts, not a durable ledger, active grant
// or caller-populatable DTO. Actual prepared replacement alone installs these
// after its original Store owner retained the confirmed authenticated row.
type localOriginalObservationLineage struct {
	mu    sync.Mutex
	facts [12]ports.AuthenticatedAttemptOperationObservationV1
}

func (e *LocalInvocationEntryV1) retainOriginalObservation(o ports.AuthenticatedAttemptOperationObservationV1) error {
	if !e.original() || e.observationLineage == nil || o.Validate() != nil || o.Reservation.Operation.Scope != ownedOperationScope(e.parent) || o.Reservation.ValidateForParent(e.parent.Authority, e.parent.Reservation) != nil || e.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(o.Clone()) != nil {
		return domain.ErrMembershipDenied
	}
	sequence := o.Reservation.Operation.Sequence
	if sequence < 1 || sequence > 12 {
		return domain.ErrMembershipDenied
	}
	l := e.observationLineage
	l.mu.Lock()
	defer l.mu.Unlock()
	old := l.facts[sequence-1]
	if old.Validate() == nil && !reflect.DeepEqual(old, o) {
		return domain.ErrMembershipDenied
	}
	l.facts[sequence-1] = o.Clone()
	return nil
}
func (e *LocalInvocationEntryV1) historicalOriginalObservation(r domain.AttemptOperationReservationV1, o domain.AttemptOperationObservationV1) (ports.AuthenticatedAttemptOperationObservationV1, bool) {
	if !e.original() || e.observationLineage == nil || r.Operation.Sequence < 1 || r.Operation.Sequence > 12 {
		return ports.AuthenticatedAttemptOperationObservationV1{}, false
	}
	l := e.observationLineage
	l.mu.Lock()
	original := l.facts[r.Operation.Sequence-1].Clone()
	l.mu.Unlock()
	return original, original.Validate() == nil && reflect.DeepEqual(original.Reservation, r) && reflect.DeepEqual(original.Observation, o) && e.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(original.Clone()) == nil
}
