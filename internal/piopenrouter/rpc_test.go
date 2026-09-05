package piopenrouter

import (
	"strings"
	"testing"
)

func TestRPCParserRejectsUnboundedOrAmbiguousFrames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value string
	}{
		{name: "missing newline", value: `{"id":"request-a","type":"response","command":"prompt","success":true}`},
		{name: "case folded duplicate", value: `{"id":"request-a","type":"response","Type":"response","command":"prompt","success":true}` + "\n"},
		{name: "unknown event", value: `{"id":"request-a","type":"response","command":"prompt","success":true}` + "\n" + `{"type":"extension_ui_request"}` + "\n"},
		{name: "event after terminal", value: successfulRPCJSONL("request-a", "answer") + `{"type":"agent_settled"}` + "\n"},
		{name: "retry", value: `{"id":"request-a","type":"response","command":"prompt","success":true}` + "\n" + `{"type":"agent_start"}` + "\n" + `{"type":"turn_start"}` + "\n" + `{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"answer"}],"provider":"sessionless-openrouter","model":"stealth/ox-alpha","usage":{"input":1,"output":1},"stopReason":"stop"}}` + "\n" + `{"type":"turn_end"}` + "\n" + `{"type":"agent_end","willRetry":true}` + "\n"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			result := parseRPCJSONL([]byte(testCase.value), "request-a")
			if !result.protocolDrift {
				t.Fatalf("protocolDrift = false for %q", testCase.value)
			}
		})
	}
}

func TestRPCParserEnforcesEveryExplicitBoundary(t *testing.T) {
	t.Parallel()
	base := productionRPCLimits
	tests := []struct {
		name   string
		value  string
		limits rpcLimits
	}{
		{name: "stdout", value: "{}\n{}\n", limits: rpcLimits{outputBytes: 3, lineBytes: base.lineBytes, events: base.events, finalBytes: base.finalBytes, jsonDepth: base.jsonDepth}},
		{name: "line", value: "{}\n", limits: rpcLimits{outputBytes: base.outputBytes, lineBytes: 2, events: base.events, finalBytes: base.finalBytes, jsonDepth: base.jsonDepth}},
		{name: "events", value: `{"id":"request-a","type":"response","command":"prompt","success":true}` + "\n" + `{"type":"agent_start"}` + "\n", limits: rpcLimits{outputBytes: base.outputBytes, lineBytes: base.lineBytes, events: 1, finalBytes: base.finalBytes, jsonDepth: base.jsonDepth}},
		{name: "final", value: successfulRPCJSONL("request-a", "answer"), limits: rpcLimits{outputBytes: base.outputBytes, lineBytes: base.lineBytes, events: base.events, finalBytes: 5, jsonDepth: base.jsonDepth}},
		{name: "depth", value: `{"id":"request-a","type":"response","command":"prompt","success":true,"nested":{"value":1}}` + "\n", limits: rpcLimits{outputBytes: base.outputBytes, lineBytes: base.lineBytes, events: base.events, finalBytes: base.finalBytes, jsonDepth: 1}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			result := parseRPCJSONLWithLimits([]byte(testCase.value), "request-a", testCase.limits)
			if !result.protocolDrift {
				t.Fatalf("protocolDrift = false for %s boundary", testCase.name)
			}
		})
	}
}

func TestRPCParserRejectsUnpairedOrRepeatedMessages(t *testing.T) {
	t.Parallel()
	response := `{"id":"request-a","type":"response","command":"prompt","success":true}` + "\n" +
		`{"type":"agent_start"}` + "\n" + `{"type":"turn_start"}` + "\n"
	userStart := `{"type":"message_start","message":{"role":"user"}}` + "\n"
	userEnd := `{"type":"message_end","message":{"role":"user"}}` + "\n"
	assistantStart := `{"type":"message_start","message":{"role":"assistant"}}` + "\n"
	tests := []string{
		response + userStart + userStart,
		response + userEnd,
		response + assistantStart,
		response + userStart + userEnd + assistantStart + assistantStart,
		response + userStart + userEnd + `{"type":"message_update","assistantMessageEvent":{"type":"toolcall_delta"}}` + "\n",
	}
	for _, value := range tests {
		value := value
		t.Run(strings.TrimSpace(value[len(response):]), func(t *testing.T) {
			t.Parallel()
			if result := parseRPCJSONL([]byte(value), "request-a"); !result.protocolDrift {
				t.Fatal("ambiguous message lifecycle was accepted")
			}
		})
	}
}

func FuzzRPCParserNeverCommitsMalformedTerminal(f *testing.F) {
	f.Add([]byte(successfulRPCJSONL("request-a", "answer")))
	f.Add([]byte(`{"id":"request-a","type":"response","command":"prompt","success":true}` + "\n"))
	f.Add([]byte(`{"type":"tool_execution_start"}` + "\n"))
	f.Fuzz(func(t *testing.T, value []byte) {
		result := parseRPCJSONL(value, "request-a")
		if result.terminal && !result.accepted {
			t.Fatalf("terminal stream was not accepted: events=%d failure=%q", result.eventCount, result.failureCode)
		}
		if result.terminal && !result.providerFailed && !result.protocolDrift && len(result.final) == 0 {
			t.Fatalf("successful terminal stream has no bounded final: events=%d", result.eventCount)
		}
	})
}
