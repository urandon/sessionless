package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIRejectsLiveFlagsAndDoesNotEchoPrivatePaths(t *testing.T) {
	for _, args := range [][]string{{}, {"-execute"}, {"-manifest", "/private/secret-fixture/missing"}, {"-manifest", "x", "extra"}} {
		var out, diagnostic bytes.Buffer
		if code := run(args, &out, &diagnostic); code == 0 || out.Len() != 0 || strings.Contains(diagnostic.String(), "secret-fixture") {
			t.Fatalf("CLI failed to reject safely: code=%d stdout=%q stderr=%q", code, out.String(), diagnostic.String())
		}
	}
}

func TestCLIPrintsDraftWithoutExecutingAnything(t *testing.T) {
	path := filepath.Join(t.TempDir(), "draft.json")
	data, err := os.ReadFile("../../docs/experiments/attached-worker-transport-draft.json")
	if err != nil {
		t.Fatalf("read public fixture: %v", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	var out, diagnostic bytes.Buffer
	if code := run([]string{"-manifest", path}, &out, &diagnostic); code != 0 {
		t.Fatalf("CLI code=%d diagnostic=%q", code, diagnostic.String())
	}
	if !strings.Contains(out.String(), `"status": "draft_not_authorized"`) || !strings.Contains(out.String(), `"scheduled_exchange_bound": 24`) {
		t.Fatalf("draft report lost status or 60-minute schedule: %s", out.String())
	}
}
