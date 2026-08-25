package codexsurface

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
	fixtureDigest, err := executableSHA256(fixture)
	if err != nil {
		t.Fatal(err)
	}

	for _, surface := range []Surface{SurfaceAppServer, SurfaceExec} {
		report, err := Probe(context.Background(), surface, Config{
			Executable: fixture, ExpectedBinarySHA256: fixtureDigest,
			Scratch: root, Iterations: 2, Timeout: 15 * time.Second,
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
		if !reportCheck(report, "binary_digest_pinned") {
			t.Fatalf("Probe(%s) did not bind evidence to the fixture digest", surface)
		}
	}
}

func TestDirectProbeRejectsMismatchedBinaryDigest(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX fixture")
	}
	root := t.TempDir()
	fixture := filepath.Join(root, "codex-fixture")
	marker := filepath.Join(root, "executed")
	script := fmt.Sprintf("#!/bin/sh\nprintf 'executed' > %q\ncase \"${1-}\" in\n--version) echo 'codex-cli 0.148.0-alpha.15' ;;\nexec) printf '%%s\\n' --json --ephemeral --ignore-user-config --ignore-rules read-only ;;\nesac\n", marker)
	if err := os.WriteFile(fixture, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := Probe(context.Background(), SurfaceExec, Config{
		Executable: fixture, ExpectedBinarySHA256: strings.Repeat("0", 64),
		Scratch: root, Iterations: 1, Timeout: 15 * time.Second,
	})
	if err == nil {
		t.Fatal("mismatched artifact was accepted")
	}
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("mismatched artifact executed before rejection: %v", statErr)
	}
}

func TestSDKProbeDeadlineKillsDescendantProcessGroup(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("process-group contract is Unix-specific")
	}
	root := t.TempDir()
	python := filepath.Join(root, "python-stall")
	readyPath := filepath.Join(root, "ready.fifo")
	if err := syscall.Mkfifo(readyPath, 0o600); err != nil {
		t.Fatal(err)
	}
	readyEndpoint, err := os.OpenFile(readyPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf("#!/bin/sh\nsleep 60 &\nchild=$!\nprintf '%%s\\n' \"$child\" > %q\nwait\n", readyPath)
	if err := os.WriteFile(python, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = readyEndpoint.Close() })
	var pid int
	var hookErr error
	hooks := &probeCommandHooks{
		// This is only a watchdog for process startup under a loaded race-test
		// runner. The measured 100ms operation timeout starts after the pipe
		// barrier, so increasing this bound cannot hide a slow teardown.
		readyTimeout: 10 * time.Second,
		afterStart: func(ctx context.Context, _ *exec.Cmd) error {
			result := make(chan error, 1)
			go func() {
				line, readErr := bufio.NewReader(readyEndpoint).ReadString('\n')
				if readErr != nil {
					result <- readErr
					return
				}
				parsed, parseErr := strconv.Atoi(strings.TrimSpace(line))
				if parseErr == nil {
					pid = parsed
				}
				result <- parseErr
			}()
			select {
			case readyErr := <-result:
				hookErr = readyErr
				return readyErr
			case <-ctx.Done():
				hookErr = ctx.Err()
				_ = readyEndpoint.Close()
				return ctx.Err()
			}
		},
	}
	_, err = Probe(context.Background(), SurfaceSDK, Config{
		PythonExecutable: python, Scratch: root, Iterations: 1, Timeout: 100 * time.Millisecond,
		testCommandHooks: hooks,
	})
	if err == nil {
		t.Fatal("stalled SDK probe succeeded")
	}
	if pid <= 0 {
		t.Fatalf("fixture readiness did not report a descendant pid: %v", hookErr)
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
printf '%s\n' '{"sdk_version":"0.147.0","runtime_version":"0.147.0","initialized":true,"account_present":false,"requires_auth":true,"experimental_default":true,"inherits_ambient_environment":true,"default_approval_accepts":true,"high_level_approval_handler":false,"restricted_read_access_supported":false,"typed_rate_limit_read":false,"runtime_sha256":"19c4f144c5226a9f17c58e6f0fa854843b0f77a6eb420f40e2745a12f10f5d37","client_sha256":"76bdb1e63c62987c3530ea763e9655a06b308cbc4e18cb51958e85b6c23aec3b","api_sha256":"673defd0ccf1348a86c2bb589cb3a1a69cb315b0a3ecb29525c52f0515a82476","sandbox_sha256":"01ab6cabc1642941ba958b287c34a5475066c934b67e4fd194d78b4bb2eb27b2"}'
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
	if !reportCheck(report, "chatgpt_auth_required_without_ambient_credentials") {
		t.Fatal("isolated account route was not preserved")
	}
	for _, name := range []string{
		"api_source_digest_pinned", "client_source_digest_pinned",
		"runtime_digest_pinned", "sandbox_source_digest_pinned",
	} {
		if !reportCheck(report, name) {
			t.Fatalf("SDK provenance check %q failed", name)
		}
	}
}

func reportCheck(report Report, name string) bool {
	for _, check := range report.Checks {
		if check.Name == name {
			return check.Pass
		}
	}
	return false
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
