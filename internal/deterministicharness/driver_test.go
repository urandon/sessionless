package deterministicharness

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type recordingSink struct {
	events []ports.ExecutionEvent
}

func TestCaptureContextHistoryCopiesExactMaterializedBytesWhenEnabled(t *testing.T) {
	t.Parallel()
	driver, err := New(Config{Turns: 1, Artifacts: 1, CaptureContextHistory: true})
	if err != nil {
		t.Fatal(err)
	}
	workDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workDir, "outputs"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(workDir, "context"), 0o700); err != nil {
		t.Fatal(err)
	}
	history := []byte("{\"schema\":\"sessionless.context-event.v1\"}\n")
	if err := os.WriteFile(filepath.Join(workDir, "context", "history.jsonl"), history, 0o600); err != nil {
		t.Fatal(err)
	}
	request := ports.ExecutionRequest{
		TenantID: "tenant-a", RunID: "run-a", SessionID: "session-a",
		TriggerEventID: "event-a", AttemptID: "attempt-a", WorkDir: workDir,
		ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1},
	}
	result, err := driver.Execute(context.Background(), request, &recordingSink{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Outputs) != 2 || result.Outputs[1] != (ports.ExecutionOutput{
		Name: "context-history.jsonl", MediaType: "application/x-ndjson", RelativePath: "context-history.jsonl",
	}) {
		t.Fatalf("captured outputs = %+v", result.Outputs)
	}
	captured, err := os.ReadFile(filepath.Join(workDir, "outputs", "context-history.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(captured, history) {
		t.Fatalf("captured history differs: got %q want %q", captured, history)
	}
}

func TestContextHistoryCaptureIsOptIn(t *testing.T) {
	t.Parallel()
	driver, err := New(Config{Turns: 1, Artifacts: 1})
	if err != nil {
		t.Fatal(err)
	}
	workDir := t.TempDir()
	for _, name := range []string{"outputs", "context"} {
		if err := os.MkdirAll(filepath.Join(workDir, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(
		filepath.Join(workDir, "context", "history.jsonl"), []byte("should-not-be-captured\n"), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	request := ports.ExecutionRequest{
		TenantID: "tenant-a", RunID: "run-a", SessionID: "session-a",
		TriggerEventID: "event-a", AttemptID: "attempt-a", WorkDir: workDir,
		ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1},
	}
	result, err := driver.Execute(context.Background(), request, &recordingSink{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("disabled capture outputs = %+v", result.Outputs)
	}
	if _, err := os.Stat(filepath.Join(workDir, "outputs", "context-history.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disabled context capture stat error = %v, want not-exist", err)
	}
}

func (sink *recordingSink) Emit(_ context.Context, event ports.ExecutionEvent) error {
	sink.events = append(sink.events, event)
	return nil
}

func TestFailBeforeFirstTurnLeavesNoCheckpointAndResumeCanProceed(t *testing.T) {
	driver, err := New(Config{
		Turns: 2, Artifacts: 1,
		FailBeforeFirstTurn: true, RetryableFail: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	workDir := t.TempDir()
	if err := os.MkdirAll(workDir+"/outputs", 0o700); err != nil {
		t.Fatal(err)
	}
	request := ports.ExecutionRequest{
		TenantID: "tenant-a", RunID: "run-a", SessionID: "session-a",
		TriggerEventID: "event-a", AttemptID: "attempt-a",
		WorkDir: workDir,
		ContextSnapshot: domain.BlobRef{
			TenantID: "tenant-a", Key: "tenants/tenant-a/context.json",
			Size: 1, SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
	sink := &recordingSink{}
	_, err = driver.Execute(context.Background(), request, sink)
	var classified *domain.ClassifiedError
	if !errors.As(err, &classified) || !classified.Retryable() {
		t.Fatalf("error = %v, want retryable classified error", err)
	}
	if len(sink.events) != 0 {
		t.Fatalf("events before first-turn failure = %d, want 0", len(sink.events))
	}

	driver, err = New(Config{Turns: 2, Artifacts: 1})
	if err != nil {
		t.Fatal(err)
	}
	result, err := driver.Execute(context.Background(), request, sink)
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.events) != 2 || len(result.Outputs) != 1 {
		t.Fatalf("events=%d outputs=%d", len(sink.events), len(result.Outputs))
	}
	if _, err := os.Stat(filepath.Join(workDir, "outputs", "context-history.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("disabled context capture stat error = %v, want not-exist", err)
	}
}
