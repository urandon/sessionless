package serverlessharness

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// StopAndJoinLocalTerminal closes only the original local allocation. The
// concrete keeper must first join its real preflight/transport/lease; an
// outstanding operation slot denies rather than being treated as stopped by
// cancellation. No shared Source, host or model server is closed.
func (bridge *BoundedOwnedAllocationPreparationV1) StopAndJoinLocalTerminal(ctx context.Context, original domain.RegisteredAllocationV2) error {
	if bridge == nil || bridge.self != bridge || bridge.local == nil || ctx == nil || ctx.Err() != nil || original.ValidateInitial() != nil || original.LocalRuntime == nil {
		return domain.ErrMembershipDenied
	}
	l := bridge.local
	l.mu.Lock()
	valid := l.matchesTerminalOriginalLocked(original) && ctx.Err() == nil
	l.mu.Unlock()
	if !valid {
		return domain.ErrMembershipDenied
	}
	// Correlation is immutable once entered. A foreign or malformed input must
	// be rejected BEFORE cancelling the genuine allocation, not after close.
	bridge.local.close()
	if ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	return bridge.VerifyStoppedLocalTerminal(original)
}

// VerifyStoppedLocalTerminal authenticates this original physical owner after
// expiry/Close, not fresh send authority or a caller-authored stopped Boolean.
func (bridge *BoundedOwnedAllocationPreparationV1) VerifyStoppedLocalTerminal(original domain.RegisteredAllocationV2) error {
	if bridge == nil || bridge.self != bridge || bridge.local == nil || original.ValidateInitial() != nil || original.LocalRuntime == nil {
		return domain.ErrMembershipDenied
	}
	l := bridge.local
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.matchesTerminalOriginalLocked(original) || !l.closed || l.lifetime.Err() == nil || l.operation != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}

// Caller holds l.mu. Historical identity remains usable after Source expiry;
// it does not require or restore active execution/send permission.
func (l *localExecutionAllocationV1) matchesTerminalOriginalLocked(original domain.RegisteredAllocationV2) bool {
	p := l.capability
	return l.self == l && l.entered && l.lifetime != nil &&
		original.AllocationID == l.id && original.PreparedInvocationDigest == p.Digest() && original.PreparedAllocationDigest == p.allocationDigest &&
		original.ParentReservationDigest == p.reservationDigest && original.Backend == p.authority.HarnessBinding.Backend &&
		original.Substrate == p.authority.SubstrateBinding && !original.Window.ExpiresAt.After(l.activeDeadline)
}
