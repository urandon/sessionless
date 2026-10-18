package ydbstore

import "gitcode.com/urandon/sessionless/internal/domain"

// This is the immutable original physical reservation, not a changed policy or
// fabricated durable operation. The genuine terminal codec's prospective
// encoded-capsule fit remains a separate REQUIRED pre-effect integration.
func (entry *LocalInvocationEntryV1) localOperationBodyLimit() uint64 {
	if !entry.original() || entry.terminalOutputs == nil || entry.parent.Authority.HarnessBinding.AgentLoopPolicy == nil {
		return 0
	}
	return min(entry.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxAggregateBodyBytes, entry.terminalOutputs.OperationBodyLimit())
}

func (store *Store) localOperationBudgetAllows(scope domain.AttemptOperationScopeV1, aggregateBodyBytes uint64) bool {
	if store == nil || store.localBootstrap == nil {
		return true
	}
	entry := store.localBootstrap.entry.Load()
	if entry == nil && store.localBootstrap.self == store.localBootstrap && store.localBootstrap.store == store && store.localBootstrap.bodySlot == nil && store.localCompositionReady() {
		return true // unchanged explicit preinstalled-body composition
	}
	if !entry.original() || entry.owner.store != store || scope != ownedOperationScope(entry.parent) {
		return false
	}
	limit := entry.localOperationBodyLimit()
	return limit > 0 && aggregateBodyBytes <= limit
}
