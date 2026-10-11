package codexexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func TestDriverCanonicalPromptUsesSharedOrderedFrames(t *testing.T) {
	request, authority := validDriverRequest(t)
	first := promptFixtureRecord(t, request)
	setPromptFixturePayload(&first, []byte(`{"version":1,"text":" \nПривет 🌍\t","origin":[1,true],"metadata":{"x":"ignored"},"attachments":[]}`))
	second := first
	second.Event.ID = "event-b"
	second.Event.Sequence = 2
	second.Event.Kind = domain.SessionEventAssistantMessage
	second.Event.AuthorUserID = nil
	runID := request.RunID
	second.Event.RunID = &runID
	second.Event.IdempotencyKey = "event-key-b"
	second.Event.Payload.Key = domain.SessionEventObjectPrefix(request.TenantID, request.SessionID, second.Event.ID) + "message.json"
	setPromptFixturePayload(&second, []byte(`{"schema":"sessionless.assistant-message.v1","summary":"reply\nline","artifact_manifest_id":"ignored"}`))
	var history []byte
	for _, record := range []sessioncontext.Record{first, second} {
		line, err := sessioncontext.EncodeRecord(record.Event, record.Payload)
		if err != nil {
			t.Fatal(err)
		}
		history = append(history, line...)
	}
	writePromptFixtureHistory(t, request, history)
	request.TriggerEventID = second.Event.ID
	request.ContextWindow.ThroughSequence = 2
	boundary := &fixtureProcessBoundary{result: successfulDriverProcess([]byte(successfulJSONL))}
	sink := &fixtureEventSink{}
	_, err := mustDriver(t, true, authority, boundary).Execute(context.Background(), request, sink)
	want := "Sessionless canonical transcript v1\n[user]\n \nПривет 🌍\t\n[assistant]\nreply\nline\n"
	if err != nil || boundary.runs != 1 || string(boundary.stdin) != want || len(sink.events) != 1 {
		t.Fatalf("Execute() err=%v runs=%d stdin=%q events=%d; want nil, 1, %q, 1", err, boundary.runs, boundary.stdin, len(sink.events), want)
	}
	if !bytes.Equal(boundary.invocation.Stdin, make([]byte, len(boundary.invocation.Stdin))) {
		t.Error("process invocation stdin was not zeroed after the boundary returned")
	}
}

func TestDriverCanonicalPromptRejectsBeforeProcess(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ports.ExecutionRequest, *sessioncontext.Record)
		raw    func([]byte) []byte
	}{
		{name: "other tenant", mutate: func(_ *ports.ExecutionRequest, record *sessioncontext.Record) {
			record.Event.TenantID = "tenant-other"
		}},
		{name: "other session", mutate: func(_ *ports.ExecutionRequest, record *sessioncontext.Record) {
			record.Event.SessionID = "session-other"
		}},
		{name: "wrong payload hash", mutate: func(_ *ports.ExecutionRequest, record *sessioncontext.Record) {
			record.Event.Payload.SHA256 = strings.Repeat("0", 64)
		}},
		{name: "wrong payload size", mutate: func(_ *ports.ExecutionRequest, record *sessioncontext.Record) {
			record.Event.Payload.Size++
		}},
		{name: "sequence gap", mutate: func(_ *ports.ExecutionRequest, record *sessioncontext.Record) {
			record.Event.Sequence = 2
		}},
		{name: "wrong record schema", mutate: func(_ *ports.ExecutionRequest, record *sessioncontext.Record) {
			record.Schema = "other"
		}},
		{name: "wrong trigger", mutate: func(request *ports.ExecutionRequest, _ *sessioncontext.Record) {
			request.TriggerEventID = "event-other"
		}},
		{name: "wrong through boundary", mutate: func(request *ports.ExecutionRequest, _ *sessioncontext.Record) {
			request.ContextWindow.ThroughSequence = 2
		}},
		{name: "unknown payload field", mutate: func(_ *ports.ExecutionRequest, record *sessioncontext.Record) {
			setPromptFixturePayload(record, []byte(`{"version":1,"text":"x","extra":1}`))
		}},
		{name: "attachment", mutate: func(_ *ports.ExecutionRequest, record *sessioncontext.Record) {
			setPromptFixturePayload(record, []byte(`{"version":1,"text":"x","attachments":[null]}`))
		}},
		{name: "tool call", mutate: func(request *ports.ExecutionRequest, record *sessioncontext.Record) {
			record.Event.Kind = domain.SessionEventToolCall
			runID := request.RunID
			record.Event.RunID = &runID
		}},
		{name: "tool result", mutate: func(request *ports.ExecutionRequest, record *sessioncontext.Record) {
			record.Event.Kind = domain.SessionEventToolResult
			runID := request.RunID
			record.Event.RunID = &runID
		}},
		{name: "system notice", mutate: func(_ *ports.ExecutionRequest, record *sessioncontext.Record) {
			record.Event.Kind = domain.SessionEventSystemNotice
		}},
		{name: "missing newline", raw: func(raw []byte) []byte { return raw[:len(raw)-1] }},
		{name: "malformed record", raw: func(_ []byte) []byte { return []byte("{\n") }},
		{name: "empty history", raw: func(_ []byte) []byte { return nil }},
		{name: "oversize history", raw: func(raw []byte) []byte {
			return append(raw[:len(raw)-1], append(bytes.Repeat([]byte{' '}, maxInstructionBytes+1-len(raw)), '\n')...)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request, authority := validDriverRequest(t)
			record := promptFixtureRecord(t, request)
			if tc.mutate != nil {
				tc.mutate(&request, &record)
			}
			// Deliberately bypass EncodeRecord for malformed source references;
			// the actual consumer must reject them before any process effect.
			raw, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			raw = append(raw, '\n')
			if tc.raw != nil {
				raw = tc.raw(raw)
			}
			writePromptFixtureHistory(t, request, raw)
			boundary := &fixtureProcessBoundary{}
			sink := &fixtureEventSink{}
			result, err := mustDriver(t, true, authority, boundary).Execute(context.Background(), request, sink)
			if err == nil || boundary.runs != 0 || boundary.stdin != nil || len(sink.events) != 0 || result.ProviderEvidence != nil {
				t.Fatalf("Execute() err=%v runs=%d stdin bytes=%d events=%d provider evidence=%t; want rejection with zero effects", err, boundary.runs, len(boundary.stdin), len(sink.events), result.ProviderEvidence != nil)
			}
		})
	}
}

func TestDriverCanonicalPromptFullHistoryLimitIsIndependentOfRenderedSize(t *testing.T) {
	request, authority := validDriverRequest(t)
	path := filepath.Join(request.WorkDir, "context", "history.jsonl")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Legal record whitespace increases source bytes without enlarging its
	// decoded payload or prompt. Thus this exercises the history limit itself.
	history := append(bytes.Clone(raw[:len(raw)-1]), bytes.Repeat([]byte{' '}, maxInstructionBytes-len(raw))...)
	history = append(history, '\n')
	if len(history) != maxInstructionBytes {
		t.Fatalf("history bytes=%d; want exactly %d", len(history), maxInstructionBytes)
	}
	writePromptFixtureHistory(t, request, history)
	boundary := &fixtureProcessBoundary{result: successfulDriverProcess([]byte(successfulJSONL))}
	_, err = mustDriver(t, true, authority, boundary).Execute(context.Background(), request, &fixtureEventSink{})
	want := "Sessionless canonical transcript v1\n[user]\npublic fixture\n"
	if err != nil || boundary.runs != 1 || string(boundary.stdin) != want {
		t.Fatalf("exact history ceiling err=%v runs=%d rendered bytes=%d; want accepted %d-byte prompt", err, boundary.runs, len(boundary.stdin), len(want))
	}
	history = append(history[:len(history)-1], ' ', '\n')
	writePromptFixtureHistory(t, request, history)
	boundary = &fixtureProcessBoundary{}
	_, err = mustDriver(t, true, authority, boundary).Execute(context.Background(), request, &fixtureEventSink{})
	if err == nil || boundary.runs != 0 || boundary.stdin != nil {
		t.Fatalf("history ceiling +1 err=%v runs=%d rendered bytes=%d; want rejection before process", err, boundary.runs, len(boundary.stdin))
	}
}

func TestDriverCanonicalPromptRejectsNonRegularOrSymlinkHistory(t *testing.T) {
	for _, target := range []string{"history", "context", "workdir", "directory"} {
		t.Run(target, func(t *testing.T) {
			request, authority := validDriverRequest(t)
			path := filepath.Join(request.WorkDir, "context", "history.jsonl")
			switch target {
			case "context":
				path = filepath.Dir(path)
			case "workdir":
				path = request.WorkDir
			}
			if target == "directory" {
				if err := os.Rename(path, path+".saved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			} else if target == "workdir" {
				link := filepath.Join(t.TempDir(), "workdir-link")
				if err := os.Symlink(path, link); err != nil {
					t.Fatal(err)
				}
				request.WorkDir = link
			} else {
				if err := os.Rename(path, path+".saved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+".saved", path); err != nil {
					t.Fatal(err)
				}
			}
			boundary := &fixtureProcessBoundary{}
			_, err := mustDriver(t, true, authority, boundary).Execute(context.Background(), request, &fixtureEventSink{})
			if err == nil || boundary.runs != 0 || boundary.stdin != nil {
				t.Fatalf("Execute() err=%v runs=%d stdin bytes=%d; want zero-effect path rejection", err, boundary.runs, len(boundary.stdin))
			}
		})
	}
}

func promptFixtureRecord(t *testing.T, request ports.ExecutionRequest) sessioncontext.Record {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(request.WorkDir, "context", "history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var record sessioncontext.Record
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

func setPromptFixturePayload(record *sessioncontext.Record, payload []byte) {
	digest := sha256.Sum256(payload)
	record.Payload = bytes.Clone(payload)
	record.Event.Payload.Size = int64(len(payload))
	record.Event.Payload.SHA256 = hex.EncodeToString(digest[:])
}

func writePromptFixtureHistory(t *testing.T, request ports.ExecutionRequest, raw []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(request.WorkDir, "context", "history.jsonl"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
