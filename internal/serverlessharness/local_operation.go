package serverlessharness

import (
	"context"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
)

// LocalModelOperationLeaseV1 owns one preflight resource slot on the ORIGINAL
// entered allocation. It is not a Store reservation, current-authority check or
// send permission. The concrete connector must still consume its actual socket
// and call the original Store's before-send gate before any HTTP byte.
type LocalModelOperationLeaseV1 struct {
	mu         sync.Mutex
	self       *LocalModelOperationLeaseV1
	owner      *localExecutionAllocationV1
	operation  domain.AttemptOperationV1
	allocation domain.RegisteredAllocationDigestV2
	ctx        context.Context
	cancel     context.CancelFunc
	stopSource func() bool
	closed     bool
	consumed   bool
}

// BeginLocalModelOperation fixes all operation fields before a socket can be
// opened. Retaining the slot on the original allocation prevents new wrappers
// from retrying a failed sequence. A higher sequence is NOT durable predecessor
// acceptance: the Store independently enforces that and all canonical joins.
func (bridge *BoundedOwnedAllocationPreparationV1) BeginLocalModelOperation(
	source *localoperator.Source, allocation domain.RegisteredAllocationV2,
	op domain.AttemptOperationV1, at time.Time,
) (*LocalModelOperationLeaseV1, error) {
	if bridge == nil || bridge.local == nil || source == nil || bridge.local.source != source ||
		bridge.VerifyEnteredLocalAllocation(allocation, at) != nil {
		return nil, domain.ErrMembershipDenied
	}
	l := bridge.local
	op = op.Clone()
	if (op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1) ||
		op.Scope.SessionID != allocation.Scope.SessionID || op.ValidateForParent(l.capability.Authority(), l.capability.Reservation()) != nil ||
		op.Deadline.After(allocation.Window.ExpiresAt) || !at.Before(op.Deadline) {
		return nil, domain.ErrMembershipDenied
	}
	sourceLifetime, err := source.ExecutionLifetime()
	if err != nil || sourceLifetime.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	ad, err := allocation.Digest()
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.self != l || !l.entered || l.closed || l.lifetime == nil || l.lifetime.Err() != nil ||
		!at.Before(l.activeDeadline) || l.operation != nil || op.Sequence <= l.lastSequence {
		return nil, domain.ErrMembershipDenied
	}
	ctx, cancel := context.WithDeadline(l.lifetime, op.Deadline)
	lease := &LocalModelOperationLeaseV1{owner: l, operation: op, allocation: ad, ctx: ctx, cancel: cancel}
	lease.self = lease
	lease.stopSource = context.AfterFunc(sourceLifetime, cancel)
	l.lastSequence, l.operation = op.Sequence, lease
	// Source revocation racing construction cannot leave a live returned lease.
	if sourceLifetime.Err() != nil || ctx.Err() != nil {
		cancel()
		lease.stopSource()
		l.operation = nil
		return nil, domain.ErrMembershipDenied
	}
	return lease, nil
}

// Context carries only original resource lifetime, never authority to send.
// Nil/copies fail closed. Callers cannot extend this deadline or cancel Source.
func (l *LocalModelOperationLeaseV1) Context() context.Context {
	if l == nil || l.self != l {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	return l.ctx
}

// Consume irreversibly claims this resource once BEFORE the concrete Store
// before-send transaction. A failed transaction cannot reopen the same slot.
// This proves private local correlation only, not canonical current authority.
func (l *LocalModelOperationLeaseV1) Consume(op domain.AttemptOperationV1, allocation domain.RegisteredAllocationDigestV2, at time.Time) error {
	if l == nil || l.self != l {
		return domain.ErrMembershipDenied
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.consumed {
		return domain.ErrMembershipDenied
	}
	l.consumed = true
	digest, err := op.Digest()
	expected, ee := l.operation.Digest()
	if err != nil || ee != nil || digest != expected || allocation != l.allocation || l.ctx.Err() != nil ||
		!at.Before(l.operation.Deadline) || l.owner.current(l.owner.capability.Authority(), at) != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (l *LocalModelOperationLeaseV1) Close() error {
	if l == nil {
		return nil
	}
	if l.self != l {
		return domain.ErrMembershipDenied
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.cancel()
	l.stopSource()
	l.mu.Unlock()
	l.owner.mu.Lock()
	if l.owner.operation == l {
		l.owner.operation = nil
	}
	l.owner.mu.Unlock()
	return nil
}

func (*LocalModelOperationLeaseV1) String() string {
	return "LocalModelOperationLeaseV1{private:[redacted]}"
}
func (l *LocalModelOperationLeaseV1) GoString() string { return l.String() }
