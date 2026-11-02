package canonicaloutput

import (
	"encoding/json"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// FailurePlanV1 is a bounded pair plus the single protected failure notice.
// Unlike PlanV1 it has no assistant/manifest arm. All values remain DATA;
// only the original Store-owned capture may authenticate/physically publish it.
type FailurePlanV1 struct{ events []EventV1 }

func (FailurePlanV1) MarshalJSON() ([]byte, error) { return nil, domain.ErrMembershipDenied }
func (FailurePlanV1) String() string               { return "FailurePlanV1{transient:[redacted]}" }
func (p FailurePlanV1) GoString() string           { return p.String() }

func (p *FailurePlanV1) Events() []EventV1 {
	if p == nil {
		return nil
	}
	return append([]EventV1(nil), p.events...)
}
func (p *FailurePlanV1) Release() {
	if p != nil {
		for i := range p.events {
			clear(p.events[i].Payload)
		}
		p.events = nil
	}
}

func EncodeKnownMCPServiceFailureV1(runID domain.RunID, limits domain.ProductLimits, pair []ports.ExecutionToolEvent) (out *FailurePlanV1, err error) {
	if runID.Validate() != nil || len(pair) < 2 || len(pair) > 8 || len(pair)%2 != 0 {
		return nil, domain.ErrMembershipDenied
	}
	seen := map[string]bool{}
	for i := 0; i < len(pair); i += 2 {
		if pair[i].Kind != domain.SessionEventToolCall || pair[i+1].Kind != domain.SessionEventToolResult || pair[i].CallID != pair[i+1].CallID || pair[i].ToolName != pair[i+1].ToolName || seen[pair[i].CallID] {
			return nil, domain.ErrMembershipDenied
		}
		seen[pair[i].CallID] = true
	}
	maxEvents, maxBytes := limits.EffectiveToolEventLimits()
	if uint64(maxEvents) < uint64(len(pair)) {
		return nil, toolBytesError()
	}
	p := &FailurePlanV1{}
	defer func() {
		if err != nil {
			p.Release()
		}
	}()
	var raw, encoded uint64
	for i, tool := range pair {
		if uint64(len(tool.Payload)) > maxBytes-raw || !json.Valid(tool.Payload) || strings.TrimSpace(tool.CallID) == "" || strings.TrimSpace(tool.ToolName) == "" {
			return nil, toolBytesError()
		}
		raw += uint64(len(tool.Payload))
		remaining := maxBytes - encoded
		if uint64(len(tool.CallID)) > remaining || uint64(len(tool.ToolName)) > remaining-uint64(len(tool.CallID)) {
			return nil, toolBytesError()
		}
		e, eerr := encodeToolEventV1(runID, i, tool)
		if eerr != nil {
			return nil, eerr
		}
		if uint64(len(e.Payload)) > remaining {
			clear(e.Payload)
			return nil, toolBytesError()
		}
		encoded += uint64(len(e.Payload))
		p.events = append(p.events, e)
	}
	notice, e := EncodeFailureV1(runID, false, domain.KnownMCPServiceErrorV1)
	if e != nil {
		return nil, e
	}
	p.events = append(p.events, notice)
	return p, nil
}
