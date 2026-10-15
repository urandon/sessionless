package attachedworkeroutput

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAttemptOutputUploadRecipeIsolatesNativeEnvironment(t *testing.T) {
	// Serial test: t.Setenv is process-global. A test-owned stub replaces go;
	// hostile sentinel values can never reach the real Go/YDB/native commands.
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	makePath, err := exec.LookPath("make")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	stub := `#!/bin/sh
if [ "${YDB_CONNECTION_STRING+x}" = x ] || [ "${SESSIONLESS_E2E+x}" = x ]; then
  exit 41
fi
if [ "$SESSIONLESS_OWNED_CURRENT_YDB_GATE" != 0 ] || [ "$SESSIONLESS_OWNED_CURRENT_YDB_DISPOSABLE" != 0 ]; then
  exit 42
fi
if [ "$1" != test ]; then exit 43; fi
printf '%s\n' isolated >> "$SESSIONLESS_UPLOAD_TEST_TRACE"
`
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(stub), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("YDB_CONNECTION_STRING", "synthetic-must-not-reach-child")
	t.Setenv("SESSIONLESS_E2E", "synthetic-must-not-reach-child")
	t.Setenv("SESSIONLESS_OWNED_CURRENT_YDB_GATE", "1")
	t.Setenv("SESSIONLESS_OWNED_CURRENT_YDB_DISPOSABLE", "1")
	for _, target := range []string{"attached-worker-output-upload-test", "attached-worker-output-control-client-test"} {
		t.Run(target, func(t *testing.T) {
			trace := filepath.Join(dir, target+"-calls")
			t.Setenv("SESSIONLESS_UPLOAD_TEST_TRACE", trace)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, makePath, "--no-print-directory", target, "SESSIONLESS_GO_CACHE_ROOT="+filepath.Join(dir, "cache"))
			command.Dir, command.WaitDelay = root, time.Second
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("isolated Make target: %v; stub-only output=%s", err, output)
			}
			calls, err := os.ReadFile(trace)
			if err != nil || string(calls) != strings.Repeat("isolated\n", 2) {
				t.Fatalf("both test recipe lines not isolated: calls=%q err=%v", calls, err)
			}
		})
	}
}
