package ydbstore

import (
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Historical V3 DATA only enters through the actual capsule/seal reader. This
// mounts ordinary MODEL history, not SEARCH compaction. Current SEARCH enters
// only through its original issuer/body-bound private conservative witness.
func (entry *LocalInvocationEntryV1) localContextDataV3(source *LocalCompactionSourceV1) (compactioncontext.ContextDataV3, bool, error) {
	deny := func() (compactioncontext.ContextDataV3, bool, error) {
		return compactioncontext.ContextDataV3{}, false, domain.ErrMembershipDenied
	}
	if !entry.original() || source == nil || source.self != source || source.entry != entry || source.state == nil || source.state.cleared || source.state.history == nil {
		return deny()
	}
	history := source.state.history
	hasSearch := false
	for _, operation := range source.state.operations {
		if operation.Reservation.Operation.Kind == domain.AttemptOperationWebSearchV1 {
			if !entry.originalSearchObservationAllowsSuccessor(operation.Reservation, operation.Observation) {
				return deny()
			}
			hasSearch = true
		}
	}
	if len(history.runsV3) == 0 && !hasSearch && entry.modelCatalog == nil {
		return compactioncontext.ContextDataV3{}, false, nil
	}
	mcp, err := entry.mcpConservativeDataFor(source.state.operations)
	if err != nil {
		return deny()
	}
	value := compactioncontext.ContextDataV3{Version: 3, Historical: history.runsV3, HistoricalV2: history.runsV2, LegacyHistory: history.runs}
	for i := range mcp {
		copy := mcp[i]
		value.Tools = append(value.Tools, compactioncontext.ToolWitnessV3{Kind: domain.AttemptOperationMCPToolV1, Scope: copy.Scope, Sequence: copy.Sequence, MCP: &copy})
	}
	for _, operation := range source.state.operations {
		if operation.Reservation.Operation.Kind != domain.AttemptOperationWebSearchV1 {
			continue
		}
		m := entry.findOriginalSearch(operation.Reservation.Operation.Sequence)
		if m == nil || m.conservative == nil || m.conservative.Load() == nil {
			return deny()
		}
		w := m.conservative.Load()
		copy := w.data
		copy.Accounting.Usage = copy.Accounting.Usage.Clone()
		copy.Result = copy.Result.Clone()
		value.Tools = append(value.Tools, compactioncontext.ToolWitnessV3{Kind: domain.AttemptOperationWebSearchV1, Scope: operation.Reservation.Operation.Scope, Sequence: operation.Reservation.Operation.Sequence, Search: &copy})
	}
	return value, true, nil
}
