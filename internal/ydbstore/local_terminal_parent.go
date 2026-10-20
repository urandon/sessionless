package ydbstore

import (
	"context"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This is NOT an alternate generic Source verifier. Only the noncopyable,
// already captured AND successfully published output owner can use original
// canonical DATA to finish retirement/cleanup after active Source cancellation.
// Callers hold the SAME entry preparation barrier and captured-source mutex;
// all later stop/erasure/deletion/lease/fence/snapshot guards remain mandatory.
func (p *LocalPreparedOutputV1) originalTerminalParent(ctx context.Context, q *ownedAuthorityQuery) (domain.OwnedAttemptEffectParentV1, domain.AttemptExecutionSourceV1, time.Time, error) {
	deny := func() (domain.OwnedAttemptEffectParentV1, domain.AttemptExecutionSourceV1, time.Time, error) {
		return domain.OwnedAttemptEffectParentV1{}, domain.AttemptExecutionSourceV1{}, time.Time{}, q.budget.deny()
	}
	if !p.original() || ctx == nil || ctx.Err() != nil {
		return deny()
	}
	h, entry := p.parent, p.parent.entry
	if h.failed.Load() || !h.captured.Load() || !h.outputAttempted || h.burn.IsZero() || !time.Now().Before(h.deadline) ||
		h.source == nil || h.source.self != h.source || h.source.entry != entry || h.source.state == nil || h.source.state.cleared || h.source.state.view == nil {
		return deny()
	}
	deadline, bounded := ctx.Deadline()
	if !bounded || deadline.After(h.deadline) || !h.burn.Equal(h.source.state.snapshot.Accounting.SetupReservedAt) {
		return deny()
	}
	store := entry.owner.store
	tuple := entry.owner.ready.Load()
	if store == nil || store.localBootstrap != entry.owner || !store.localCompositionReady() || tuple == nil || tuple.store != store || tuple.bridge == nil ||
		!sameLocalOwnerPointer(tuple.keeper, entry.owner.keeper) || tuple.bridge.VerifyStoppedLocalTerminal(entry.allocation) != nil {
		return deny()
	}
	p.mu.Lock()
	valid := !p.cleared && p.publicationAttempted && p.published && p.cleanupAttempted && p.publicationRun.TerminalCustody != nil &&
		p.publicationRun.TerminalCustody.Plan == p.plan && p.plan.Validate() == nil && p.plan.Scope == ownedOperationScope(entry.parent) &&
		p.plan.FrontierDigest == h.source.state.digest
	p.mu.Unlock()
	if !valid {
		return deny()
	}
	parent, source, err := readOwnedAllocationParentData(ctx, q, ownedAllocationParentRequest(entry.parent.Reservation))
	if err != nil || !reflect.DeepEqual(parent, entry.parent) || !reflect.DeepEqual(source, entry.source) {
		return deny()
	}
	burned, burn, err := readOwnedPreparationBurnObservation(ctx, q, parent)
	if err != nil || !burned || !burn.Equal(h.burn) || ctx.Err() != nil || !time.Now().Before(h.deadline) {
		return deny()
	}
	return parent, source, burn, nil
}

// No caller-selected state/digest or generic retirement authority is accepted.
// The unchanged transition body operates on exactly this original allocation.
func (p *LocalPreparedOutputV1) retireOriginalTerminalAllocation(ctx context.Context) (ports.OwnedAllocationRegistrationReceiptV2, error) {
	deny := func() (ports.OwnedAllocationRegistrationReceiptV2, error) {
		return ports.OwnedAllocationRegistrationReceiptV2{}, domain.ErrMembershipDenied
	}
	if !p.original() || ctx == nil || ctx.Err() != nil {
		return deny()
	}
	entry := p.parent.entry
	store := entry.owner.store
	expected, err := entry.allocation.Digest()
	if err != nil || !store.validOwnedAllocationContext(ctx) {
		return deny()
	}
	var result ports.OwnedAllocationRegistrationReceiptV2
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = ports.OwnedAllocationRegistrationReceiptV2{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		parent, source, burn, err := p.originalTerminalParent(ctx, q)
		if err != nil {
			return err
		}
		snapshots, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burn)
		if err != nil || snapshots == nil || !reflect.DeepEqual(snapshots.original, entry.allocation) {
			return q.budget.deny()
		}
		result, err = retireOwnedAllocationSnapshotsTx(ctx, q, parent, source, burn, snapshots, string(expected), domain.RegisteredPreparationRetiredV1)
		return err
	})
	if err != nil || ctx.Err() != nil || !time.Now().Before(p.parent.deadline) {
		return deny()
	}
	return result, nil
}
