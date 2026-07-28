package skeleton

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestRun(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	if err := Run("worker-codex", &output); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	var event struct {
		Status string `json:"status"`
		Build  struct {
			Component string `json:"component"`
		} `json:"build"`
	}
	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if event.Status != "skeleton_ready" || event.Build.Component != "worker-codex" {
		t.Fatalf("unexpected event: %+v", event)
	}
}
