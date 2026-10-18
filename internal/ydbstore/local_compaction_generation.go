package ydbstore

import (
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Closed PRIVATE read shapes. Capture never accepts an in-flight operation or
// future proxy. These other shapes belong only to an original prepared handle;
// none is a caller-controlled permission or an alternative Store/send API.
type localCompactionGenerationPhase uint8

const (
	localCompactionCapture localCompactionGenerationPhase = iota
	localCompactionReserve
	localCompactionBeforeSend
	localCompactionCompleted
	localCompactionPostRegistration
)

// No production constructor is exposed yet: the eventual constructor MUST
// rebuild the compiled generation from the original source, compare its exact
// frozen request, and execute OriginalSource.FitContext before claiming the
// entry slot. A frozen request, these fields, or a successful join is not such
// preparation and cannot enter the existing Store effect path by itself.
type localCompactionGenerationV1 struct {
	self      *localCompactionGenerationV1
	entry     *LocalInvocationEntryV1
	source    *LocalCompactionSourceV1
	retained  *LocalRetainedCompactionV1
	frozen    *configuredinference.FrozenRequestV1
	operation domain.AttemptOperationV1
	// Completion fields are private and accessed under source.state.mu.
	reservation domain.AttemptOperationReservationV1
	observation ports.AuthenticatedAttemptOperationObservationV1
}

func (g *localCompactionGenerationV1) originalForPhase(entry *LocalInvocationEntryV1, phase localCompactionGenerationPhase) bool {
	if g == nil || g.self != g || g.entry != entry || !entry.original() || entry.compactionGeneration.Load() != g || g.source == nil || g.source.self != g.source || g.source.entry != entry || g.source.state == nil || g.source.state.cleared || g.retained == nil || g.retained.self != g.retained || g.retained.entry != entry || g.retained.source != g.source || g.frozen == nil || g.frozen.Digest() == "" || !reflect.DeepEqual(g.retained.operation, g.operation) || g.operation.Kind != domain.AttemptOperationCompactionV1 || g.operation.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) != nil || g.operation.Scope != ownedOperationScope(entry.parent) || g.operation.Sequence != g.source.state.snapshot.Accounting.Aggregate.Operations+1 {
		return false
	}
	switch phase {
	case localCompactionReserve:
		return true
	case localCompactionBeforeSend:
		return g.reservation.ValidateForParent(entry.parent.Authority, entry.parent.Reservation) == nil && reflect.DeepEqual(g.reservation.Operation, g.operation)
	case localCompactionCompleted, localCompactionPostRegistration:
		return g.observation.Validate() == nil && reflect.DeepEqual(g.observation.Reservation, g.reservation) && reflect.DeepEqual(g.reservation.Operation, g.operation) && ownedObservationAllowsSuccessor(g.reservation, g.observation.Observation) && entry.owner.store.attemptOperationIssuer.VerifyAttemptOperationObservation(g.observation.Clone()) == nil
	default:
		return false
	}
}

func (g *localCompactionGenerationV1) futureProxyDigest(q *ownedAuthorityQuery, proxy *ownedProxySnapshot) (domain.RegisteredProxyAttestationDigestV2, error) {
	if g == nil || proxy == nil || !q.at.Before(g.operation.Deadline) || proxy.current.ValidateAt(q.at) != nil || g.operation.Deadline.After(proxy.current.Window.ExpiresAt) {
		return "", q.budget.deny()
	}
	od, oe := g.operation.Digest()
	ad, ae := g.entry.allocation.Digest()
	if oe != nil || ae != nil || proxy.original.RegisteredAllocationDigest != ad || proxy.original.OperationSequence != g.operation.Sequence || proxy.original.OperationDigest != od || proxy.original.ServiceKind != domain.RegisteredProxyProviderV1 {
		return "", q.budget.deny()
	}
	d, err := proxy.original.Digest()
	if err != nil {
		return "", q.budget.deny()
	}
	return d, nil
}

// Caller holds the original source's shared mutex. This reuses the same bounded
// current Store snapshot once, never N operationHistory joins or a new ledger.
func (g *localCompactionGenerationV1) rejoinLocked(ctx context.Context, q *ownedAuthorityQuery, phase localCompactionGenerationPhase) (domain.Run, error) {
	if g == nil || !g.originalForPhase(g.entry, phase) {
		return domain.Run{}, q.budget.deny()
	}
	current, err := g.entry.readCompactionGenerationSnapshot(ctx, q, g, phase)
	if err != nil {
		return domain.Run{}, q.budget.deny()
	}
	expected := g.source.state.snapshot
	joinedRun := current.Job.Run.Clone()
	if phase == localCompactionPostRegistration {
		uploaded := g.retained.uploaded.Load()
		if uploaded == nil || uploaded.plan.SourceViewDigest != g.source.state.digest || uploaded.plan.Scope != g.operation.Scope || uploaded.plan.OperationSequence != g.operation.Sequence || uploaded.plan.Generation != g.retained.generation || !domain.EqualRunDerivedContextV1(current.Job.Run.DerivedContext, &uploaded.postimage) {
			return domain.Run{}, q.budget.deny()
		}
		current.Job.Run.DerivedContext = expected.Job.Run.DerivedContext
	}
	if phase == localCompactionReserve {
		if len(current.Operations) != len(expected.Operations) || len(current.ProxyDigests) != len(expected.ProxyDigests)+1 {
			return domain.Run{}, q.budget.deny()
		}
	} else {
		if len(current.Operations) != len(expected.Operations)+1 || len(current.ProxyDigests) != len(expected.ProxyDigests)+1 {
			return domain.Run{}, q.budget.deny()
		}
		tail := current.Operations[len(current.Operations)-1]
		if !reflect.DeepEqual(tail.reservation, g.reservation) {
			return domain.Run{}, q.budget.deny()
		}
		if phase == localCompactionBeforeSend {
			if tail.observation != nil {
				return domain.Run{}, q.budget.deny()
			}
		} else if tail.observation == nil || !reflect.DeepEqual(*tail.observation, g.observation.Observation) {
			return domain.Run{}, q.budget.deny()
		}
		next, e := debitOwnedOperation(expected.Accounting, g.operation, g.entry.parent, g.entry.source, g.reservation.ReservedAt)
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
	return joinedRun, nil
}
