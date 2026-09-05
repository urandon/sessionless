package opencodeopenrouter

import (
	"strings"
	"testing"
)

func TestOpenCodeJSONLParserRejectsUnboundedOrAmbiguousFrames(t *testing.T) {
	t.Parallel()
	start := openCodeStepStartJSONL()
	tests := []struct {
		name  string
		value string
	}{
		{name: "missing newline", value: strings.TrimSuffix(start, "\n")},
		{name: "case folded duplicate", value: `{"type":"step_start","Type":"step_start","timestamp":1,"sessionID":"ses-a","part":{"id":"part-start","sessionID":"ses-a","messageID":"msg-a","type":"step-start"}}` + "\n"},
		{name: "missing type", value: `{"timestamp":1,"sessionID":"ses-a","part":{}}` + "\n"},
		{name: "missing timestamp", value: `{"type":"step_start","sessionID":"ses-a","part":{"id":"part-start","sessionID":"ses-a","messageID":"msg-a","type":"step-start"}}` + "\n"},
		{name: "missing session", value: `{"type":"step_start","timestamp":1,"part":{"id":"part-start","sessionID":"ses-a","messageID":"msg-a","type":"step-start"}}` + "\n"},
		{name: "invalid session", value: `{"type":"step_start","timestamp":1,"sessionID":"ses a","part":{"id":"part-start","sessionID":"ses a","messageID":"msg-a","type":"step-start"}}` + "\n"},
		{name: "unknown event", value: start + `{"type":"extension_ui_request","timestamp":2,"sessionID":"ses-a"}` + "\n"},
		{name: "event after terminal", value: successfulOpenCodeJSONL("answer") + start},
		{name: "tool use", value: start + `{"type":"tool_use","timestamp":2,"sessionID":"ses-a","part":{"id":"tool-a","sessionID":"ses-a","messageID":"msg-a","type":"tool"}}` + "\n"},
		{name: "cross session", value: start + `{"type":"text","timestamp":2,"sessionID":"ses-b","part":{"id":"part-text","sessionID":"ses-b","messageID":"msg-a","type":"text","text":"answer","time":{"start":1,"end":2}}}` + "\n"},
		{name: "missing usage", value: start + openCodeTextJSONL("answer") + `{"type":"step_finish","timestamp":3,"sessionID":"ses-a","part":{"id":"part-finish","sessionID":"ses-a","messageID":"msg-a","type":"step-finish","reason":"stop","cost":0}}` + "\n"},
		{name: "missing cache", value: start + openCodeTextJSONL("answer") + `{"type":"step_finish","timestamp":3,"sessionID":"ses-a","part":{"id":"part-finish","sessionID":"ses-a","messageID":"msg-a","type":"step-finish","reason":"stop","cost":0,"tokens":{"input":1,"output":1,"reasoning":0}}}` + "\n"},
		{name: "duplicate part id", value: start + strings.Replace(openCodeTextJSONL("answer"), `"id":"part-text"`, `"id":"part-start"`, 1)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			result := parseOpenCodeJSONL([]byte(testCase.value))
			if !result.protocolDrift {
				t.Fatalf("protocolDrift = false for %q", testCase.value)
			}
		})
	}
}

func TestOpenCodeJSONLParserAcceptsExactByteEventLineAndFinalBounds(t *testing.T) {
	t.Parallel()
	value := successfulOpenCodeJSONL("answer")
	longestLine := 0
	for _, line := range strings.SplitAfter(value, "\n") {
		if len(line) > longestLine {
			longestLine = len(line)
		}
	}
	limits := rpcLimits{outputBytes: len(value), lineBytes: longestLine, events: 3, finalBytes: len("answer"), jsonDepth: productionRPCLimits.jsonDepth}
	result := parseOpenCodeJSONLWithLimits([]byte(value), limits)
	if !result.terminal || result.protocolDrift || string(result.final) != "answer" {
		t.Fatalf("exact bounds rejected: %+v", result)
	}
	for _, smaller := range []rpcLimits{
		{outputBytes: len(value) - 1, lineBytes: longestLine, events: 3, finalBytes: len("answer"), jsonDepth: limits.jsonDepth},
		{outputBytes: len(value), lineBytes: longestLine - 1, events: 3, finalBytes: len("answer"), jsonDepth: limits.jsonDepth},
		{outputBytes: len(value), lineBytes: longestLine, events: 2, finalBytes: len("answer"), jsonDepth: limits.jsonDepth},
		{outputBytes: len(value), lineBytes: longestLine, events: 3, finalBytes: len("answer") - 1, jsonDepth: limits.jsonDepth},
	} {
		if result := parseOpenCodeJSONLWithLimits([]byte(value), smaller); !result.protocolDrift {
			t.Fatalf("below-bound stream accepted: limits=%+v result=%+v", smaller, result)
		}
	}
}

func TestOpenCodeJSONLParserEnforcesEveryExplicitBoundary(t *testing.T) {
	t.Parallel()
	base := productionRPCLimits
	tests := []struct {
		name   string
		value  string
		limits rpcLimits
	}{
		{name: "stdout", value: "{}\n{}\n", limits: rpcLimits{outputBytes: 3, lineBytes: base.lineBytes, events: base.events, finalBytes: base.finalBytes, jsonDepth: base.jsonDepth}},
		{name: "line", value: "{}\n", limits: rpcLimits{outputBytes: base.outputBytes, lineBytes: 2, events: base.events, finalBytes: base.finalBytes, jsonDepth: base.jsonDepth}},
		{name: "events", value: openCodeStepStartJSONL() + openCodeTextJSONL("answer"), limits: rpcLimits{outputBytes: base.outputBytes, lineBytes: base.lineBytes, events: 1, finalBytes: base.finalBytes, jsonDepth: base.jsonDepth}},
		{name: "final", value: successfulOpenCodeJSONL("answer"), limits: rpcLimits{outputBytes: base.outputBytes, lineBytes: base.lineBytes, events: base.events, finalBytes: 5, jsonDepth: base.jsonDepth}},
		{name: "depth", value: startAtDepthOne(), limits: rpcLimits{outputBytes: base.outputBytes, lineBytes: base.lineBytes, events: base.events, finalBytes: base.finalBytes, jsonDepth: 1}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			result := parseOpenCodeJSONLWithLimits([]byte(testCase.value), testCase.limits)
			if !result.protocolDrift {
				t.Fatalf("protocolDrift = false for %s boundary", testCase.name)
			}
		})
	}
}

func TestOpenCodeJSONLParserRejectsInvalidLifecycleAndFinishReasons(t *testing.T) {
	t.Parallel()
	start := openCodeStepStartJSONL()
	text := openCodeTextJSONL("answer")
	tests := []string{
		text,
		start + start,
		start + strings.Replace(text, `"messageID":"msg-a"`, `"messageID":"msg-b"`, 1),
		start + `{"type":"reasoning","timestamp":2,"sessionID":"ses-a","part":{"id":"reason-a","sessionID":"ses-a","messageID":"msg-a","type":"reasoning","text":"private"}}` + "\n",
		start + text + openCodeStepFinishJSONL("tool-calls"),
		start + text + strings.Replace(openCodeStepFinishJSONL("stop"), `"input":11`, `"input":4503599627370497`, 1),
	}
	for index, value := range tests {
		if result := parseOpenCodeJSONL([]byte(value)); !result.protocolDrift {
			t.Fatalf("invalid lifecycle %d was accepted", index)
		}
	}
}

func TestOpenCodeJSONLParserClassifiesTerminalProviderEvents(t *testing.T) {
	t.Parallel()
	success := parseOpenCodeJSONL([]byte(successfulOpenCodeJSONL("answer")))
	if !success.accepted || !success.terminal || success.providerFailed || success.protocolDrift || string(success.final) != "answer" ||
		success.inputTokens == nil || *success.inputTokens != 11 || success.outputTokens == nil || *success.outputTokens != 7 {
		t.Fatalf("success = %+v", success)
	}
	preaccept := parseOpenCodeJSONL([]byte(openCodeErrorJSONL()))
	if preaccept.accepted || !preaccept.terminal || preaccept.providerFailed || preaccept.protocolDrift {
		t.Fatalf("preaccept error = %+v", preaccept)
	}
	providerFailure := parseOpenCodeJSONL([]byte(openCodeStepStartJSONL() + openCodeErrorJSONL()))
	if !providerFailure.accepted || !providerFailure.terminal || !providerFailure.providerFailed || providerFailure.protocolDrift {
		t.Fatalf("provider failure = %+v", providerFailure)
	}
}

func FuzzOpenCodeJSONLParserNeverCommitsMalformedTerminal(f *testing.F) {
	f.Add([]byte(successfulOpenCodeJSONL("answer")))
	f.Add([]byte(openCodeStepStartJSONL()))
	f.Add([]byte(openCodeErrorJSONL()))
	f.Add([]byte(`{"type":"tool_use","timestamp":1,"sessionID":"ses-a"}` + "\n"))
	f.Fuzz(func(t *testing.T, value []byte) {
		result := parseOpenCodeJSONL(value)
		if result.terminal && result.failureCode == "" && !result.providerFailed && !result.protocolDrift &&
			(!result.accepted || len(result.final) == 0 || result.inputTokens == nil || result.outputTokens == nil) {
			t.Fatalf("successful terminal invariant failed: %+v", result)
		}
		if len(result.final) != 0 && !result.accepted {
			t.Fatalf("unaccepted stream retained a final: %+v", result)
		}
	})
}

func startAtDepthOne() string {
	return `{"type":"step_start","timestamp":1,"sessionID":"ses-a","part":{"id":"part-start","sessionID":"ses-a","messageID":"msg-a","type":"step-start"}}` + "\n"
}
