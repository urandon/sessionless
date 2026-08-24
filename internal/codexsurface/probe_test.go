package codexsurface

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCredentialFreeSurfaceProbes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX fixture")
	}
	root := t.TempDir()
	fixture := filepath.Join(root, "codex-fixture")
	script := `#!/bin/sh
case "${1-}" in
--version)
  echo 'codex-cli 0.148.0-alpha.15'
  exit 0
  ;;
exec)
  cat <<'EOF'
--json
--ephemeral
--ignore-user-config
--ignore-rules
read-only
EOF
  exit 0
  ;;
app-server)
  IFS= read -r initialize
  printf '{"id":"sessionless-1","result":{"userAgent":"sessionless_worker/0.148.0-alpha.15","codexHome":"%s","platformFamily":"unix","platformOs":"test"}}\n' "$CODEX_HOME"
  IFS= read -r initialized
  IFS= read -r account
  printf '{"id":"sessionless-2","result":{"account":null,"requiresOpenaiAuth":true}}\n'
  while IFS= read -r ignored; do :; done
  exit 0
  ;;
esac
exit 2
`
	if err := os.WriteFile(fixture, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}

	for _, surface := range []Surface{SurfaceAppServer, SurfaceExec} {
		report, err := Probe(context.Background(), surface, Config{
			Executable: fixture, Scratch: root, Iterations: 2, Timeout: 15 * time.Second,
		})
		if err != nil {
			t.Fatalf("Probe(%s) error = %v", surface, err)
		}
		if report.Iterations != 2 || report.Version != "0.148.0-alpha.15" {
			t.Fatalf("Probe(%s) = %#v", surface, report)
		}
		if surface == SurfaceAppServer && report.Status != "no_go" {
			t.Fatalf("App Server status = %q", report.Status)
		}
		if surface == SurfaceExec && report.Status != "pass" {
			t.Fatalf("exec status = %q", report.Status)
		}
	}
}

func TestSDKProbeDeadlineKillsDescendantProcessGroup(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	root := t.TempDir()
	pidFile := filepath.Join(root, "descendant.pid")
	python := filepath.Join(root, "python-stall")
	script := fmt.Sprintf("#!/bin/sh\nsleep 60 &\nchild=$!\nprintf '%%s\\n' \"$child\" > %q\nwait\n", pidFile)
	if err := os.WriteFile(python, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := Probe(context.Background(), SurfaceSDK, Config{
		PythonExecutable: python, Scratch: root, Iterations: 1, Timeout: 2 * time.Second,
	})
	if err == nil {
		t.Fatal("stalled SDK probe succeeded")
	}
	rawPID, readErr := os.ReadFile(pidFile)
	if readErr != nil {
		t.Fatal(readErr)
	}
	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	if killErr := syscall.Kill(pid, 0); killErr == nil || killErr != syscall.ESRCH {
		t.Fatalf("descendant %d survived bounded probe: %v", pid, killErr)
	}
}

func TestSDKProbeFailsClosedOnUnsafePublishedDefaults(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX fixture")
	}
	root := t.TempDir()
	python := filepath.Join(root, "python-fixture")
	script := `#!/bin/sh
printf '%s\n' '{"sdk_version":"0.147.0","runtime_version":"0.147.0","initialized":true,"account_present":false,"requires_auth":true,"experimental_default":true,"inherits_ambient_environment":true,"default_approval_accepts":true,"high_level_approval_handler":false,"restricted_read_access_supported":false,"typed_rate_limit_read":false}'
`
	if err := os.WriteFile(python, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	report, err := Probe(context.Background(), SurfaceSDK, Config{
		Executable: python, PythonExecutable: python, Scratch: root,
		Iterations: 2, Timeout: 15 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "no_go" || report.Version != "0.147.0" || report.Runtime != "0.147.0" {
		t.Fatalf("report = %#v", report)
	}
	for _, check := range report.Checks {
		if check.Name == "chatgpt_auth_required_without_ambient_credentials" {
			if !check.Pass {
				t.Fatal("isolated account route was not preserved")
			}
			return
		}
	}
	t.Fatal("missing account-route check")
}

func TestProbeMakesRelativeExecutablesAbsoluteBeforeChangingChildDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX fixture")
	}
	root := t.TempDir()
	fixture := filepath.Join(root, "codex-version")
	if err := os.WriteFile(fixture, []byte("#!/bin/sh\necho 'codex-cli 0.148.0-alpha.15'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(working, fixture)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveExecutable(relative)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(resolved) {
		t.Fatalf("resolved executable = %q", resolved)
	}
}
