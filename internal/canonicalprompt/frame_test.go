package canonicalprompt

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestRenderFrameV1GoldenDecoderParity(t *testing.T) {
	if HeaderV1 != "Sessionless canonical transcript v1\n" || ContractV1 != "codex-text-v1" {
		t.Fatal("canonical renderer identity/header drift")
	}
	cases := []struct {
		name, body, want string
		kind             domain.SessionEventKind
	}{
		{name: "minimal user", body: `{"version":1,"text":"hello"}`, want: "[user]\nhello\n", kind: domain.SessionEventUserMessage},
		{name: "optional nulls", body: `{"version":1,"text":"hello","origin":null,"metadata":null,"attachments":null}`, want: "[user]\nhello\n", kind: domain.SessionEventUserMessage},
		{name: "ignored fields", body: `{"version":1,"text":"hello","origin":[1,true],"metadata":{"x":"ignored"},"attachments":[]}`, want: "[user]\nhello\n", kind: domain.SessionEventUserMessage},
		{name: "whitespace unicode controls", body: `{"version":1,"text":" \nПривет 🌍\t\u202e"}`, want: "[user]\n \nПривет 🌍\t\u202e\n", kind: domain.SessionEventUserMessage},
		{name: "case folded fields", body: `{"VERSION":1,"TEXT":"hello"}`, want: "[user]\nhello\n", kind: domain.SessionEventUserMessage},
		{name: "duplicate last wins", body: `{"version":2,"version":1,"text":"first","text":"last"}`, want: "[user]\nlast\n", kind: domain.SessionEventUserMessage},
		{name: "duplicate null ignored", body: `{"version":1,"version":null,"text":"hello","text":null}`, want: "[user]\nhello\n", kind: domain.SessionEventUserMessage},
		{name: "decoder surrogate replacement", body: `{"version":1,"text":"\ud800"}`, want: "[user]\n\ufffd\n", kind: domain.SessionEventUserMessage},
		{name: "json whitespace", body: " \n{\"version\":1,\"text\":\"hello\"}\t\r\n", want: "[user]\nhello\n", kind: domain.SessionEventUserMessage},
		{name: "minimal assistant", body: `{"schema":"sessionless.assistant-message.v1","summary":"reply"}`, want: "[assistant]\nreply\n", kind: domain.SessionEventAssistantMessage},
		{name: "assistant manifest ignored", body: `{"schema":"sessionless.assistant-message.v1","summary":"reply","artifact_manifest_id":"not-a-domain-ID/"}`, want: "[assistant]\nreply\n", kind: domain.SessionEventAssistantMessage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := []byte(tc.body)
			before := bytes.Clone(input)
			got, err := RenderFrameV1(tc.kind, input)
			if err != nil || string(got) != tc.want {
				t.Fatalf("RenderFrameV1() = %q, %v; want %q, nil", got, err, tc.want)
			}
			if !bytes.Equal(input, before) {
				t.Fatal("renderer modified input")
			}
			got[0] = '!'
			again, err := RenderFrameV1(tc.kind, input)
			if err != nil || string(again) != tc.want || !bytes.Equal(input, before) {
				t.Fatal("returned frame shares mutable input/global state")
			}
		})
	}
	input := append([]byte(`{"version":1,"text":"`), 0xff)
	input = append(input, []byte(`"}`)...)
	got, err := RenderFrameV1(domain.SessionEventUserMessage, input)
	if err != nil || string(got) != "[user]\n\ufffd\n" {
		t.Fatalf("Go JSON invalid-UTF8 replacement parity = %q, %v", got, err)
	}
}

func TestRenderFrameV1RejectsUnsupportedPayload(t *testing.T) {
	cases := []struct {
		name, body string
		kind       domain.SessionEventKind
	}{
		{name: "empty", body: "", kind: domain.SessionEventUserMessage},
		{name: "malformed", body: "{", kind: domain.SessionEventUserMessage},
		{name: "unknown field", body: `{"version":1,"text":"x","extra":1}`, kind: domain.SessionEventUserMessage},
		{name: "wrong version", body: `{"version":2,"text":"x"}`, kind: domain.SessionEventUserMessage},
		{name: "decimal version", body: `{"version":1.0,"text":"x"}`, kind: domain.SessionEventUserMessage},
		{name: "wrong text type", body: `{"version":1,"text":3}`, kind: domain.SessionEventUserMessage},
		{name: "null text", body: `{"version":1,"text":null}`, kind: domain.SessionEventUserMessage},
		{name: "empty text", body: `{"version":1,"text":""}`, kind: domain.SessionEventUserMessage},
		{name: "nul", body: `{"version":1,"text":"a\u0000b"}`, kind: domain.SessionEventUserMessage},
		{name: "second value", body: `{"version":1,"text":"x"} {}`, kind: domain.SessionEventUserMessage},
		{name: "trailing garbage", body: `{"version":1,"text":"x"}x`, kind: domain.SessionEventUserMessage},
		{name: "attachment", body: `{"version":1,"text":"x","attachments":[null]}`, kind: domain.SessionEventUserMessage},
		{name: "attachment type", body: `{"version":1,"text":"x","attachments":{}}`, kind: domain.SessionEventUserMessage},
		{name: "metadata type", body: `{"version":1,"text":"x","metadata":{"x":1}}`, kind: domain.SessionEventUserMessage},
		{name: "wrong assistant schema", body: `{"schema":"other","summary":"x"}`, kind: domain.SessionEventAssistantMessage},
		{name: "empty assistant", body: `{"schema":"sessionless.assistant-message.v1","summary":""}`, kind: domain.SessionEventAssistantMessage},
		{name: "assistant unknown field", body: `{"schema":"sessionless.assistant-message.v1","summary":"x","extra":1}`, kind: domain.SessionEventAssistantMessage},
		{name: "tool call", body: `{}`, kind: domain.SessionEventToolCall},
		{name: "tool result", body: `{}`, kind: domain.SessionEventToolResult},
		{name: "notice", body: `{}`, kind: domain.SessionEventSystemNotice},
		{name: "unknown kind", body: `{}`, kind: "other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RenderFrameV1(tc.kind, []byte(tc.body))
			if !errors.Is(err, ErrInvalidFrame) || got != nil {
				t.Fatalf("RenderFrameV1() len=%d, err=%v; want nil, ErrInvalidFrame", len(got), err)
			}
		})
	}
}

func TestRenderFrameV1IndependentByteBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		text string
		want bool
	}{
		{name: "exact unicode frame", text: strings.Repeat("é", (MaxFrameBytesV1-8)/2), want: true},
		{name: "one byte over frame", text: strings.Repeat("é", (MaxFrameBytesV1-8)/2) + "a", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := json.Marshal(struct {
				Version uint32 `json:"version"`
				Text    string `json:"text"`
			}{Version: 1, Text: tc.text})
			if err != nil {
				t.Fatal(err)
			}
			frame, err := RenderFrameV1(domain.SessionEventUserMessage, payload)
			if tc.want {
				if err != nil || len(frame) != MaxFrameBytesV1 || len(HeaderV1)+len(frame) <= MaxFrameBytesV1 {
					t.Fatalf("exact frame len=%d err=%v; header must separately exceed full prompt limit", len(frame), err)
				}
			} else if !errors.Is(err, ErrInvalidFrame) || frame != nil {
				t.Fatalf("oversize frame len=%d err=%v", len(frame), err)
			}
		})
	}
	prefix, suffix := `{"version":1,"text":"a","origin":"`, `"}`
	for _, extra := range []int{0, 1} {
		payload := []byte(prefix + strings.Repeat("x", MaxPayloadBytesV1-len(prefix)-len(suffix)+extra) + suffix)
		frame, err := RenderFrameV1(domain.SessionEventUserMessage, payload)
		if extra == 0 {
			if err != nil || string(frame) != "[user]\na\n" {
				t.Fatalf("exact payload len=%d frame len=%d err=%v", len(payload), len(frame), err)
			}
		} else if !errors.Is(err, ErrInvalidFrame) || frame != nil {
			t.Fatalf("oversize payload len=%d frame len=%d err=%v", len(payload), len(frame), err)
		}
	}
}
