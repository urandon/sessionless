package codexexec

import (
	"bytes"
	"strings"
	"testing"
)

const successfulJSONL = "" +
	`{"type":"thread.started","thread_id":"native-private"}` + "\n" +
	`{"type":"turn.started"}` + "\n" +
	`{"type":"item.completed","item":{"type":"reasoning","text":"private"}}` + "\n" +
	`{"type":"item.completed","item":{"type":"agent_message","text":"bounded result"}}` + "\n" +
	`{"type":"turn.completed"}` + "\n"

func TestParseJSONLLifecycleMatrix(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		accepted bool
		terminal bool
		protocol bool
		drift    bool
		failure  string
		final    string
	}{
		{name: "success", value: successfulJSONL, accepted: true, terminal: true, final: "bounded result"},
		{name: "pre acceptance", value: `{"type":"thread.started"}` + "\n", failure: ""},
		{name: "turn before thread", value: `{"type":"turn.started"}` + "\n", protocol: true, failure: "turn_started_before_thread"},
		{name: "accepted loss", value: `{"type":"thread.started"}` + "\n" + `{"type":"turn.started"}` + "\n", accepted: true},
		{name: "duplicate terminal", value: successfulJSONL + `{"type":"turn.completed"}` + "\n", accepted: true, terminal: true, protocol: true, drift: true, failure: "event_after_terminal", final: "bounded result"},
		{name: "post terminal", value: successfulJSONL + `{"type":"item.started"}` + "\n", accepted: true, terminal: true, protocol: true, drift: true, failure: "event_after_terminal", final: "bounded result"},
		{name: "missing started item", value: `{"type":"thread.started"}` + "\n" + `{"type":"turn.started"}` + "\n" + `{"type":"item.started"}` + "\n", accepted: true, protocol: true, failure: "unexpected_effect_item"},
		{name: "effect before acceptance", value: `{"type":"thread.started"}` + "\n" + `{"type":"item.completed","item":{"type":"command_execution"}}` + "\n", protocol: true, failure: "item_before_acceptance"},
		{name: "effect item", value: `{"type":"thread.started"}` + "\n" + `{"type":"turn.started"}` + "\n" + `{"type":"item.completed","item":{"type":"command_execution"}}` + "\n", accepted: true, protocol: true, failure: "unexpected_effect_item"},
		{name: "malformed", value: "{\n", protocol: true, failure: "invalid_jsonl_event"},
		{name: "unterminated", value: `{"type":"thread.started"}`, protocol: true, failure: "jsonl_unterminated_frame"},
		{name: "duplicate key", value: `{"type":"thread.started","Type":"turn.started"}` + "\n", protocol: true, failure: "invalid_jsonl_event"},
		{name: "unknown", value: `{"type":"provider.secret_event"}` + "\n", protocol: true, failure: "unknown_jsonl_event"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			result := parseJSONL([]byte(testCase.value))
			if result.accepted != testCase.accepted || result.terminal != testCase.terminal ||
				result.protocolDrift != testCase.protocol || result.terminalDrift != testCase.drift ||
				result.failureCode != testCase.failure ||
				string(result.final) != testCase.final {
				t.Fatalf("parse result = %+v", result)
			}
		})
	}
}

func TestParseJSONLBoundsFinalAndLine(t *testing.T) {
	oversizedFinal := `{"type":"thread.started"}` + "\n" + `{"type":"turn.started"}` + "\n" +
		`{"type":"item.completed","item":{"type":"agent_message","text":"` + strings.Repeat("x", maxFinalBytes+1) + `"}}` + "\n"
	result := parseJSONL([]byte(oversizedFinal))
	if result.failureCode != "invalid_final_agent_item" || !result.accepted {
		t.Fatalf("oversized final result = %+v", result)
	}
	result = parseJSONL(append(bytes.Repeat([]byte{'x'}, maxJSONLLineBytes+1), '\n'))
	if result.failureCode != "jsonl_line_limit_exceeded" {
		t.Fatalf("oversized line result = %+v", result)
	}
}

// These failure envelopes reproduce the credential-free native 0.162.1
// unauthorized, interrupted-stream and TLS-mismatch probes from #220. Their
// private messages are deliberately replaced; only the event shape is relevant.
const failedTurnJSONL = `{"type":"turn.failed","error":{"message":"synthetic provider failure"}}` + "\n"

func TestParseJSONLFailedTurn(t *testing.T) {
	accepted := codexAcceptedJSONL()
	errorEvent := `{"type":"error","message":"synthetic provider failure"}` + "\n"
	finalItem := `{"type":"item.completed","item":{"type":"agent_message","text":"not a success"}}` + "\n"
	tests := []struct {
		name          string
		value         string
		accepted      bool
		drift         bool
		terminalDrift bool
		failure       string
	}{
		{name: "native error then failed", value: accepted + errorEvent + failedTurnJSONL, accepted: true, failure: "codex_error_event"},
		{name: "failed without notification", value: accepted + failedTurnJSONL, accepted: true, failure: "codex_turn_failed"},
		{name: "partial final then failed", value: accepted + finalItem + failedTurnJSONL, accepted: true, failure: "codex_turn_failed"},
		{name: "failure before thread", value: failedTurnJSONL, drift: true, failure: "turn_failed_before_acceptance"},
		{name: "failure before turn", value: `{"type":"thread.started"}` + "\n" + failedTurnJSONL, drift: true, failure: "turn_failed_before_acceptance"},
		{name: "missing error", value: accepted + `{"type":"turn.failed"}` + "\n", accepted: true, drift: true, failure: "invalid_failed_turn"},
		{name: "null error", value: accepted + `{"type":"turn.failed","error":null}` + "\n", accepted: true, drift: true, failure: "invalid_failed_turn"},
		{name: "string error", value: accepted + `{"type":"turn.failed","error":"failure"}` + "\n", accepted: true, drift: true, failure: "invalid_failed_turn"},
		{name: "missing message", value: accepted + `{"type":"turn.failed","error":{}}` + "\n", accepted: true, drift: true, failure: "invalid_failed_turn"},
		{name: "empty message", value: accepted + `{"type":"turn.failed","error":{"message":""}}` + "\n", accepted: true, drift: true, failure: "invalid_failed_turn"},
		{name: "typed message", value: accepted + `{"type":"turn.failed","error":{"message":1}}` + "\n", accepted: true, drift: true, failure: "invalid_failed_turn"},
		{name: "duplicate error key", value: accepted + `{"type":"turn.failed","error":{"message":"a","Message":"b"}}` + "\n", accepted: true, drift: true, failure: "invalid_jsonl_event"},
		{name: "notification alone", value: accepted + errorEvent, accepted: true, failure: "codex_error_event"},
		{name: "success after notification", value: accepted + errorEvent + finalItem + `{"type":"turn.completed"}` + "\n", accepted: true, drift: true, terminalDrift: true, failure: "codex_error_event"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			result := ParseProtocolV1([]byte(testCase.value))
			if result.Accepted != testCase.accepted || result.Terminal ||
				result.ProtocolDrift != testCase.drift || result.TerminalDrift != testCase.terminalDrift ||
				result.FailureCode != testCase.failure {
				t.Fatalf("ParseProtocolV1() = %+v, want accepted=%t success=false drift=%t terminalDrift=%t failure=%q", result, testCase.accepted, testCase.drift, testCase.terminalDrift, testCase.failure)
			}
			if !testCase.drift && strings.Contains(testCase.value, `"type":"turn.failed"`) && len(result.Final) != 0 {
				t.Fatalf("failed turn retained final candidate: %q", result.Final)
			}
		})
	}
}

func TestParseJSONLRejectsEveryFrameAfterFailedTurn(t *testing.T) {
	for _, testCase := range []struct{ name, frame string }{
		{name: "completed", frame: `{"type":"turn.completed"}` + "\n"},
		{name: "final item", frame: `{"type":"item.completed","item":{"type":"agent_message","text":"late"}}` + "\n"},
		{name: "effect item", frame: `{"type":"item.started","item":{"type":"command_execution"}}` + "\n"},
		{name: "duplicate failed", frame: failedTurnJSONL},
		{name: "turn started", frame: `{"type":"turn.started"}` + "\n"},
		{name: "error", frame: `{"type":"error","message":"late"}` + "\n"},
		{name: "malformed", frame: "{\n"},
		{name: "empty", frame: "\n"},
		{name: "unterminated", frame: "unterminated"},
		{name: "oversized", frame: strings.Repeat("x", maxJSONLLineBytes+1) + "\n"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result := ParseProtocolV1([]byte(codexAcceptedJSONL() + failedTurnJSONL + testCase.frame))
			if !result.Accepted || result.Terminal || !result.ProtocolDrift || !result.TerminalDrift ||
				result.FailureCode != "codex_turn_failed" || len(result.Final) != 0 {
				t.Fatalf("post-failure frame accepted or lost failure state: %+v", result)
			}
		})
	}
}

func TestParseProtocolV1SharesTheClosedParserWithoutAliasingInput(t *testing.T) {
	input := []byte(successfulJSONL)
	result := ParseProtocolV1(input)
	if !result.Accepted || !result.Terminal || result.ProtocolDrift || result.TerminalDrift ||
		result.FailureCode != "" || string(result.Final) != "bounded result" {
		t.Fatalf("protocol result = %+v", result)
	}
	for index := range input {
		input[index] = 0
	}
	if string(result.Final) != "bounded result" {
		t.Fatal("protocol final aliased the raw JSONL input")
	}
}

func TestStrictJSONObjectRejectsNestedCaseFoldDuplicatesAndDepth(t *testing.T) {
	if strictJSONObject([]byte(`{"type":"x","item":{"text":"a","Text":"b"}}`)) {
		t.Fatal("nested case-fold duplicate was accepted")
	}
	value := `{"type":"x","item":` + strings.Repeat(`[`, maxJSONDepth+1) + strings.Repeat(`]`, maxJSONDepth+1) + `}`
	if strictJSONObject([]byte(value)) {
		t.Fatal("over-depth object was accepted")
	}
}

func FuzzCodexJSONLParserNeverCommitsMalformedTerminal(f *testing.F) {
	f.Add([]byte(successfulJSONL))
	f.Add([]byte(`{"type":"thread.started"}` + "\n" + `{"type":"turn.started"}` + "\n"))
	f.Add([]byte(`{"type":"provider.secret_event"}` + "\n"))
	f.Add([]byte(codexAcceptedJSONL() + failedTurnJSONL))
	f.Add([]byte(codexAcceptedJSONL() + failedTurnJSONL + successfulJSONL))
	f.Fuzz(func(t *testing.T, value []byte) {
		result := ParseProtocolV1(value)
		if result.Terminal && !result.Accepted {
			t.Fatal("terminal protocol state was not accepted")
		}
		if result.Terminal && result.ProtocolDrift && !result.TerminalDrift {
			t.Fatal("terminal protocol drift was not marked terminal")
		}
		if result.Terminal && !result.ProtocolDrift && (result.FailureCode != "" || len(result.Final) == 0) {
			t.Fatalf("committable terminal lacks a final result: %+v", result)
		}
	})
}
