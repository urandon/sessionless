package compactioncontext

import (
	"errors"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// BuildOrdinaryV1 constructs the complete actual MODEL input from the same
// validated source as compaction. No oldest-input deletion, caller prompt
// override or smaller fit probe is used. Source.FitContext must then fit THIS
// frozen request under the original remaining operation ceilings.
func BuildOrdinaryV1(profile domain.ConfiguredInferenceProfileV1, limits domain.ConfiguredInferenceLimitsV1, policy domain.AgentLoopPolicyV1,
	sourceDigest string, records []sessioncontext.EventPayload, operations []CompletedOperationV1, parent *ParentV1, tools []configuredinference.ToolV1, history ...[]HistoricalRunV1) (*configuredinference.FrozenRequestV1, error) {
	if len(history) > 1 {
		return nil, ErrCanonical
	}
	var old []HistoricalRunV1
	if len(history) == 1 {
		old = history[0]
	}
	s, err := buildGenerationSourceV1(profile, limits, policy, sourceDigest, records, operations, parent, tools, old)
	if err != nil {
		return nil, err
	}
	return buildOrdinarySourceV1(s, parent)
}

func buildOrdinarySourceV1(s *generationSourceV1, parent *ParentV1) (*configuredinference.FrozenRequestV1, error) {
	p := s.program
	frames := make([]historicalFrameV1, 0, len(s.frames))
	var protected []historicalFrameV1
	parentProtected := make(map[string]bool)
	if parent != nil {
		if uint64(len(parent.ProtectedSourceIDs)) > MaxEventsV1 {
			return nil, ErrCanonical
		}
		for _, id := range parent.ProtectedSourceIDs {
			if parentProtected[id] || domain.ValidateSHA256Digest("parent.protected", id) != nil {
				return nil, ErrCanonical
			}
			parentProtected[id] = true
		}
	}
	refs := make([]configuredinference.CompactionReferenceV1, 0, len(s.frames))
	known := make(map[string]bool, len(s.frames))
	for i, frame := range s.frames {
		if known[frame.SourceID] {
			return nil, ErrCanonical
		}
		known[frame.SourceID] = true
		kind := configuredinference.CompactionCanonicalReferenceV1
		if frame.Kind == "operation" {
			kind = configuredinference.CompactionOperationReferenceV1
		}
		refs = append(refs, configuredinference.CompactionReferenceV1{SourceID: frame.SourceID, Kind: kind})
		if s.covered[frame.SourceID] {
			if s.protectedIndex[i] {
				return nil, ErrCanonical
			}
			continue
		}
		if parentProtected[frame.SourceID] {
			if !s.protectedIndex[i] {
				return nil, ErrCanonical
			}
			protected = append(protected, frame)
		} else {
			frames = append(frames, frame)
		}
	}
	for id := range s.covered {
		if !known[id] {
			return nil, ErrCanonical
		}
	}
	for id := range parentProtected {
		if !known[id] || s.covered[id] {
			return nil, ErrCanonical
		}
	}
	summary := ""
	if parent != nil {
		if _, err := configuredinference.DecodeCompactionSummaryV1(parent.Summary, refs); err != nil {
			return nil, ErrCanonical
		}
		summary = string(parent.Summary)
	}
	text, err := marshalHistoryV1("sessionless.history.v1", protected, frames, summary, p.limits.RequestBytes)
	if err != nil {
		return nil, err
	}
	frozen, err := configuredinference.FreezeRequestV1(p.profile, p.limits, configuredinference.RequestV1{
		Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleSystemV1, Text: ordinaryInstructionsV1}, {Role: configuredinference.RoleUserV1, Text: text}},
		Tools:    p.tools, MaxOutputTokens: min(uint64(2048), p.limits.MaxOutputTokens),
	})
	if errors.Is(err, configuredinference.ErrRequestBytes) {
		return nil, configuredinference.ErrContextNoFit
	}
	return frozen, err
}
