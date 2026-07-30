package deterministicharness

import (
	"context"
	"errors"
	"os"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type recordingSink struct {
	events []ports.ExecutionEvent
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
		TenantID: "tenant-a", RunID: "run-a", AttemptID: "attempt-a",
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
}
