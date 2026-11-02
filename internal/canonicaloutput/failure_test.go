package canonicaloutput_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestKnownMCPFailureExactPairNoticeAndLimits(t *testing.T) {
	pair := outputResult().ToolEvents
	p, err := canonicaloutput.EncodeKnownMCPServiceFailureV1("run-output", domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 4096}, pair)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release()
	events := p.Events()
	if len(events) != 3 {
		t.Fatal("wrong failure batch length")
	}
	success, err := canonicaloutput.EncodeCompletionV1("run-output", domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 4096}, ports.ExecutionResult{ToolEvents: pair}, "manifest-output")
	if err != nil {
		t.Fatal(err)
	}
	defer success.Release()
	var n uint64
	for i := range 2 {
		if events[i].ID != success.Events()[i].ID || !bytes.Equal(events[i].Payload, success.Events()[i].Payload) {
			t.Error("canonical tool wire changed")
		}
		n += uint64(len(events[i].Payload))
	}
	notice, _ := canonicaloutput.EncodeFailureV1("run-output", false, domain.KnownMCPServiceErrorV1)
	defer clear(notice.Payload)
	if events[2].Kind != domain.SessionEventSystemNotice || events[2].ID != notice.ID || !bytes.Equal(events[2].Payload, notice.Payload) {
		t.Error("fixed single failure notice changed")
	}
	if _, err := json.Marshal(p); err == nil {
		t.Error("transient failure plan serialized")
	}
	for _, limit := range []uint64{n, n - 1} {
		candidate, err := canonicaloutput.EncodeKnownMCPServiceFailureV1("run-output", domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: limit}, pair)
		if limit == n {
			if err != nil {
				t.Error("exact boundary denied", err)
			}
		} else if err == nil {
			t.Error("one byte over admitted")
		}
		if candidate != nil {
			candidate.Release()
		}
	}
	for _, broken := range [][]ports.ExecutionToolEvent{pair[:1], {pair[1], pair[0]}, {{Kind: pair[0].Kind, CallID: "wrong", ToolName: pair[0].ToolName, Payload: pair[0].Payload}, pair[1]}, {pair[0], {Kind: pair[1].Kind, CallID: pair[1].CallID, ToolName: pair[1].ToolName, Payload: []byte(`{`)}}} {
		if candidate, err := canonicaloutput.EncodeKnownMCPServiceFailureV1("run-output", domain.ProductLimits{}, broken); err == nil || candidate != nil {
			t.Error("invalid failure whole batch escaped")
		}
	}
	aliases := p.Events()
	p.Release()
	for _, e := range aliases {
		if !bytes.Equal(e.Payload, make([]byte, len(e.Payload))) {
			t.Error("failure owned body not cleared")
		}
	}
}

// Encoding DATA only: current configured MCP still permits one aggregate
// catalog setup. This does not claim a genuine two-setup producer or authority.
func TestKnownMCPFailureCompleteMultiPairFormat(t *testing.T) {
	var tools []ports.ExecutionToolEvent
	for i := range 4 {
		call := fmt.Sprintf("call-%d", i)
		tools = append(tools, ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: call, ToolName: "lookup", Payload: []byte(`{"query":"<x>"}`)}, ports.ExecutionToolEvent{Kind: domain.SessionEventToolResult, CallID: call, ToolName: "lookup", Payload: []byte(`{"outcome":"known"}`)})
	}
	limits := domain.ProductLimits{MaxToolEvents: 8, MaxToolEventBytes: 16384}
	p, err := canonicaloutput.EncodeKnownMCPServiceFailureV1("run-prefix", limits, tools)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release()
	success, err := canonicaloutput.EncodeCompletionV1("run-prefix", limits, ports.ExecutionResult{ToolEvents: tools}, "manifest-prefix")
	if err != nil {
		t.Fatal(err)
	}
	defer success.Release()
	if len(p.Events()) != 9 || p.Events()[8].Kind != domain.SessionEventSystemNotice {
		t.Fatal("complete prefix or single notice lost")
	}
	for i := range 8 {
		if p.Events()[i].ID != success.Events()[i].ID || !bytes.Equal(p.Events()[i].Payload, success.Events()[i].Payload) {
			t.Error("prior canonical pair encoding changed", i)
		}
	}
	if _, err := canonicaloutput.EncodeKnownMCPServiceFailureV1("run-prefix", limits, append(tools, tools[:2]...)); err == nil {
		t.Error("fifth dispatch enlarged cap")
	}
	duplicate := append([]ports.ExecutionToolEvent(nil), tools...)
	duplicate[6].CallID, duplicate[7].CallID = tools[0].CallID, tools[1].CallID
	if _, err := canonicaloutput.EncodeKnownMCPServiceFailureV1("run-prefix", limits, duplicate); err == nil {
		t.Error("duplicate pair identity accepted")
	}
}
