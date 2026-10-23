package mcpwire

import (
	"bytes"
	"testing"
)

func TestDecodeResponseFrameJSONAndSingleSSEMessage(t *testing.T) {
	l := setupLimitsFixture()
	jsonBody := []byte(`{"jsonrpc":"2.0","id":"call-1","result":{"content":[{"type":"text","text":"ok"}]}}`)
	got, frames, err := DecodeResponseFrameV1(jsonBody, "application/json", l)
	if err != nil || frames != 1 || !bytes.Equal(got, jsonBody) {
		t.Fatalf("JSON frame: frames=%d body=%s err=%v", frames, got, err)
	}
	got[0] = 'x'
	if bytes.Equal(got, jsonBody) {
		t.Fatal("decoder unexpectedly aliases source")
	}

	sse := []byte(": keepalive\r\n\r\nevent: message\r\ndata: {\"jsonrpc\":\"2.0\",\"id\":\"call-1\",\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"ok\"}]}}\r\n\r\n")
	got, frames, err = DecodeResponseFrameV1(sse, "text/event-stream; charset=utf-8", l)
	want := []byte(`{"jsonrpc":"2.0","id":"call-1","result":{"content":[{"type":"text","text":"ok"}]}}`)
	if err != nil || frames != 2 || !bytes.Equal(got, want) {
		t.Fatalf("SSE unwrap: frames=%d body=%s err=%v", frames, got, err)
	}
	r := callFixture(t)
	if _, err := r.DecodeToolResultV1(got); err != nil {
		t.Fatalf("unwrapped terminal did not correlate: %v", err)
	}
}

func TestDecodeResponseFrameRejectsMultipleTerminalMessagesAndUnsolicitedRequests(t *testing.T) {
	l := setupLimitsFixture()
	first := `{"jsonrpc":"2.0","id":"call-1","result":{"content":[{"type":"text","text":"one"}]}}`
	second := `{"jsonrpc":"2.0","id":"call-1","result":{"content":[{"type":"text","text":"two"}]}}`
	for name, body := range map[string][]byte{
		"two_terminal_sse_records": []byte("data: " + first + "\n\ndata: " + second + "\n\n"),
		"unsolicited_request":      []byte("data: {\"jsonrpc\":\"2.0\",\"method\":\"server/request\",\"id\":\"srv-1\",\"params\":{}}\n\n"),
	} {
		t.Run(name, func(t *testing.T) {
			decoded, _, err := DecodeResponseFrameV1(body, "text/event-stream", l)
			if err == nil {
				if name == "unsolicited_request" {
					init, _ := NewInitializeRequestV1("init-1", l)
					if init.ValidateInitializationV1(decoded) == nil {
						t.Fatal("unsolicited server request accepted as setup result")
					}
				} else {
					t.Fatal("multiple terminal records accepted")
				}
			}
		})
	}
	call := callFixture(t)
	decoded, _, err := DecodeResponseFrameV1([]byte("data: {\"jsonrpc\":\"2.0\",\"id\":\"other\",\"result\":{\"content\":[]}}\n\n"), "text/event-stream", l)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := call.DecodeToolResultV1(decoded); err == nil {
		t.Fatal("wrong-id terminal passed operation correlation")
	}
}

func TestDecodeResponseFrameRejectsDuplicateMalformedAndUnsupportedSSE(t *testing.T) {
	l := setupLimitsFixture()
	bad := map[string][]byte{
		"nested_duplicate_json":  []byte("data: {\"jsonrpc\":\"2.0\",\"jsonrpc\":\"2.0\",\"id\":\"x\",\"result\":{}}\n\n"),
		"duplicate_data":         []byte("data: {}\ndata: {}\n\n"),
		"duplicate_event":        []byte("event: message\nevent: message\ndata: {}\n\n"),
		"unsupported_event":      []byte("event: update\ndata: {}\n\n"),
		"id_resumption_field":    []byte("id: resume\ndata: {}\n\n"),
		"retry_resumption_field": []byte("retry: 1\ndata: {}\n\n"),
		"unterminated_line":      []byte("data: {}"),
		"bad_cr":                 []byte("data: {}\rX\n\n"),
		"empty_sse_data":         []byte("data:\n\n"),
	}
	for name, body := range bad {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeResponseFrameV1(body, "text/event-stream", l); err == nil {
				t.Fatal("invalid frame accepted")
			}
		})
	}
	for _, contentType := range []string{"text/plain", "application/json; charset=utf-16", "application/json; charset=utf-8; x=y", "text/event-stream; charset=ascii"} {
		if _, _, err := DecodeResponseFrameV1([]byte(`{}`), contentType, l); err == nil {
			t.Errorf("unsupported content type accepted: %q", contentType)
		}
	}
	if _, _, err := DecodeResponseFrameV1(nil, "application/json", l); err == nil {
		t.Fatal("empty JSON accepted")
	}
	if _, _, err := DecodeResponseFrameV1(bytes.Repeat([]byte("x"), int(l.MessageBytes)+1), "application/json", l); err == nil {
		t.Fatal("oversized frame accepted")
	}
}

func TestDecodeResponseFrameEnforcesSSELineAndRecordBudgets(t *testing.T) {
	l := setupLimitsFixture()
	l.SSELineBytes = 16
	if _, _, err := DecodeResponseFrameV1([]byte("data: {\"jsonrpc\":\"2.0\"}\n\n"), "text/event-stream", l); err == nil {
		t.Fatal("oversized SSE line accepted")
	}

	l = setupLimitsFixture()
	l.SSEEvents = 1
	body := []byte(": frame one\n\ndata: {}\n\n")
	if _, _, err := DecodeResponseFrameV1(body, "text/event-stream", l); err == nil {
		t.Fatal("comment frame bypassed inbound frame counter")
	}
}

func TestSetupResponseCorrelationRejectsWrongIDVersionAndDuplicateFieldsAfterUnwrap(t *testing.T) {
	l := setupLimitsFixture()
	init, _ := NewInitializeRequestV1("init-1", l)
	for _, jsonrpc := range []string{
		`{"jsonrpc":"2.0","id":"other","result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"s","version":"1"}}}`,
		`{"jsonrpc":"2.0","id":"init-1","result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"s","version":"1"}}}`,
		`{"jsonrpc":"2.0","jsonrpc":"2.0","id":"init-1","result":{"protocolVersion":"2025-11-25","capabilities":{"tools":{}},"serverInfo":{"name":"s","version":"1"}}}`,
	} {
		decoded, _, err := DecodeResponseFrameV1([]byte("data: "+jsonrpc+"\n\n"), "text/event-stream", l)
		if err == nil && init.ValidateInitializationV1(decoded) == nil {
			t.Errorf("wrong ID/version/duplicate terminal accepted: %s", jsonrpc)
		}
	}
}
