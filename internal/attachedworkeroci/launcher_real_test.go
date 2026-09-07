package attachedworkeroci

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

func TestRealEngineIsolationMatrix(t *testing.T) {
	if os.Getenv("SESSIONLESS_OCI_REAL_ENGINE") != "1" {
		t.Skip("set SESSIONLESS_OCI_REAL_ENGINE=1 and explicit OCI_* inputs")
	}
	required := func(name string) string {
		t.Helper()
		value := os.Getenv(name)
		if value == "" {
			t.Fatalf("%s is required", name)
		}
		return value
	}
	boundaryKind := BoundaryKind(required("SESSIONLESS_OCI_BOUNDARY"))
	dockerPath := required("SESSIONLESS_OCI_DOCKER_PATH")
	trace := &realTraceRunner{delegate: execRunner{path: dockerPath}}
	launcher, err := NewLauncher(context.Background(), Config{
		DockerPath: dockerPath, CLIConfigDir: required("SESSIONLESS_OCI_CLI_CONFIG_DIR"),
		Host: required("SESSIONLESS_OCI_HOST"), EngineID: required("SESSIONLESS_OCI_ENGINE_ID"),
		InstallationID: required("SESSIONLESS_OCI_INSTALLATION_ID"), Boundary: boundaryKind,
		Image: required("SESSIONLESS_OCI_IMAGE"), UserID: 65532, GroupID: 65532,
		DiskBytes: 16 << 20, CredentialFileBytes: 1 << 20, MemoryBytes: 128 << 20,
		PIDsLimit: 32, StopSeconds: 1,
		runner: trace,
	})
	if err != nil {
		t.Fatalf("NewLauncher() error = %v", err)
	}
	if removed, err := launcher.Reconcile(context.Background()); err != nil || removed != 0 {
		t.Fatalf("pre-test Reconcile() = %d, %v; expected a clean installation scope", removed, err)
	}
	executable := required("SESSIONLESS_OCI_PROBE")
	digest, err := attachedworkerdaemon.DigestExecutable(executable)
	if err != nil {
		t.Fatalf("DigestExecutable() error = %v", err)
	}
	scratchRoot := canonicalTempDir(t)
	forbiddenRoot := canonicalTempDir(t)
	forbiddenReadPath := filepath.Join(forbiddenRoot, "secret")
	if err := os.WriteFile(forbiddenReadPath, []byte("must-not-be-readable"), 0o600); err != nil {
		t.Fatal(err)
	}
	forbiddenWritePath := filepath.Join(forbiddenRoot, "escape")
	supervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{
		ScratchRoot: scratchRoot, Launcher: launcher, Timeout: 5 * time.Second,
		TerminationGrace: time.Second, MaxStdoutBytes: 1 << 20, MaxStderrBytes: 64 << 10,
	})
	if err != nil {
		t.Fatalf("NewSupervisor() error = %v", err)
	}
	tests := []struct {
		name      string
		arguments []string
	}{
		{name: "allowed write", arguments: []string{"allowed-write"}},
		{name: "forbidden host read", arguments: []string{"forbidden-read", forbiddenReadPath}},
		{name: "forbidden host write", arguments: []string{"forbidden-write", forbiddenWritePath}},
		{name: "network denied", arguments: []string{"network-denied"}},
		{name: "detached process removed", arguments: []string{"detached-child"}},
		{name: "disk bytes bounded", arguments: []string{"disk-bound"}},
	}
	for _, test := range tests {
		passed := t.Run(test.name, func(t *testing.T) {
			result, err := supervisor.Run(context.Background(), attachedworkerdaemon.AttemptSpec{
				Executable: executable, ExecutableDigest: digest, Arguments: test.arguments,
			})
			if err != nil {
				t.Fatalf("Run() error = %v; result = %v; docker failures = %s", err, result, trace.failures())
			}
			if result.ExitCode != 0 || string(result.Stdout) != "probe_ok\n" || !result.DescendantsReaped ||
				!result.BoundaryReleased || !result.CleanupSucceeded {
				t.Fatalf("Run() result = %v; stdout = %q", result, result.Stdout)
			}
			if removed, err := launcher.Reconcile(context.Background()); err != nil || removed != 0 {
				t.Fatalf("post-run Reconcile() = %d, %v; boundary residue existed", removed, err)
			}
		})
		if !passed {
			t.FailNow()
		}
	}
	if _, err := os.Stat(forbiddenWritePath); !os.IsNotExist(err) {
		t.Fatalf("forbidden host write residue: %v", err)
	}
}

type realTraceRunner struct {
	delegate commandRunner
	mu       sync.Mutex
	failed   []string
	inspect  string
}

func (runner *realTraceRunner) Run(ctx context.Context, directory string, environment, arguments []string) ([]byte, []byte, error) {
	stdout, stderr, err := runner.delegate.Run(ctx, directory, environment, arguments)
	command := dockerCommand(arguments)
	if len(command) >= 2 && command[0] == "container" && command[1] == "inspect" && len(stdout) != 0 {
		var inspected containerResponse
		summary := "unparseable"
		if json.Unmarshal(stdout, &inspected) == nil {
			id := inspected.ID
			if len(id) > 12 {
				id = id[:12]
			}
			summary = fmt.Sprintf("id=%s network=%s readonly=%t pid=%s ipc=%s cgroupns=%s mounts=%d tmpfs=%d memory=%d pids=%d",
				id, inspected.HostConfig.NetworkMode, inspected.HostConfig.ReadonlyRootfs,
				inspected.HostConfig.PidMode, inspected.HostConfig.IpcMode, inspected.HostConfig.CgroupnsMode,
				len(inspected.Mounts), len(inspected.HostConfig.Tmpfs), inspected.HostConfig.Memory, inspected.HostConfig.PidsLimit)
		}
		runner.mu.Lock()
		runner.inspect = summary
		runner.mu.Unlock()
	}
	if err != nil {
		runner.mu.Lock()
		message := strings.TrimSpace(string(stderr))
		if len(message) > 512 {
			message = message[:512] + "..."
		}
		if len(runner.failed) == 8 {
			runner.failed = runner.failed[1:]
		}
		commandName := dockerCommand(arguments)
		if len(commandName) > 2 {
			commandName = commandName[:2]
		}
		runner.failed = append(runner.failed, fmt.Sprintf("%s: %s", strings.Join(commandName, " "), message))
		runner.mu.Unlock()
	}
	return stdout, stderr, err
}

func (runner *realTraceRunner) Command(directory string, environment, arguments []string) *exec.Cmd {
	return runner.delegate.Command(directory, environment, arguments)
}

func (runner *realTraceRunner) failures() string {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	failed := strings.Join(runner.failed, "; ")
	if failed == "" {
		failed = "none"
	}
	return failed + " inspect=" + runner.inspect
}
