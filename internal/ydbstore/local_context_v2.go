package ydbstore

import (
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Caller holds original source.state.mu and the entry preparation barrier.
// Current MCP DATA comes only from its original private witness, historical V2
// DATA only from authenticated capsule/terminal correspondence materialization.
// The returned flag selects a closed codec, never grants a financial effect.
func (entry *LocalInvocationEntryV1) localContextDataV2(source *LocalCompactionSourceV1) (compactioncontext.ContextDataV2, bool, error) {
	if !entry.original() || source == nil || source.self != source || source.entry != entry || source.state == nil || source.state.cleared || source.state.history == nil {
		return compactioncontext.ContextDataV2{}, false, domain.ErrMembershipDenied
	}
	current, err := entry.mcpConservativeDataFor(source.state.operations)
	if err != nil {
		return compactioncontext.ContextDataV2{}, false, err
	}
	h := source.state.history
	if len(current) == 0 && len(h.runsV2) == 0 {
		return compactioncontext.ContextDataV2{}, false, nil
	}
	return compactioncontext.ContextDataV2{Version: 2, ConservativeMCP: current, Historical: h.runsV2, LegacyHistory: h.runs}, true, nil
}
