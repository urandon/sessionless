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
