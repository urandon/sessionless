//go:build darwin || linux

package codexexec

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type fixtureIsolationLauncher struct {
	mu   sync.Mutex
	last attachedworkerdaemon.LaunchSpec
}

func (*fixtureIsolationLauncher) Profile() attachedworkerdaemon.IsolationProfile {
	return attachedworkerdaemon.IsolationProfile{
		Name: "codexexec-contract-fixture", FilesystemReadBoundary: true,
		FilesystemWriteBoundary: true, NetworkDenied: true, ProcessBoundary: true,
		DiskBytesBounded: true,
	}
}

func (launcher *fixtureIsolationLauncher) Prepare(_ context.Context, spec attachedworkerdaemon.LaunchSpec) (attachedworkerdaemon.IsolationBoundary, error) {
	launcher.mu.Lock()
	launcher.last = attachedworkerdaemon.LaunchSpec{
		Executable: spec.Executable, ExecutableDigest: spec.ExecutableDigest,
		Arguments: append([]string(nil), spec.Arguments...),
		Directory: spec.Directory, Environment: append([]string(nil), spec.Environment...),
		ReadFiles: append([]string(nil), spec.ReadFiles...), ReadRoots: append([]string(nil), spec.ReadRoots...),
		WriteFiles: append([]string(nil), spec.WriteFiles...), WriteRoots: append([]string(nil), spec.WriteRoots...),
	}
	launcher.mu.Unlock()
	command := exec.Command(spec.Executable, spec.Arguments...)
	command.Dir = spec.Directory
	command.Env = append([]string(nil), spec.Environment...)
	return &fixtureIsolationBoundary{
		command: command,
		attestation: attachedworkerdaemon.WorkloadAttestation{
			Executable: spec.Executable, ExecutableDigest: spec.ExecutableDigest,
			Arguments: append([]string(nil), spec.Arguments...),
		},
	}, nil
}

func (launcher *fixtureIsolationLauncher) snapshot() attachedworkerdaemon.LaunchSpec {
	launcher.mu.Lock()
	defer launcher.mu.Unlock()
	return launcher.last
}

type fixtureIsolationBoundary struct {
	command     *exec.Cmd
	attestation attachedworkerdaemon.WorkloadAttestation
}

func (boundary *fixtureIsolationBoundary) Command() *exec.Cmd { return boundary.command }
func (boundary *fixtureIsolationBoundary) AttestedWorkload() attachedworkerdaemon.WorkloadAttestation {
	return attachedworkerdaemon.WorkloadAttestation{
		Executable: boundary.attestation.Executable, ExecutableDigest: boundary.attestation.ExecutableDigest,
		Arguments: append([]string(nil), boundary.attestation.Arguments...),
	}
}
func (*fixtureIsolationBoundary) GracefulStop(context.Context) error  { return nil }
func (*fixtureIsolationBoundary) ForceStop(context.Context) error     { return nil }
func (*fixtureIsolationBoundary) Alive(context.Context) (bool, error) { return false, nil }
func (*fixtureIsolationBoundary) Release(context.Context) error       { return nil }

type fixtureCredentialLifecycle struct {
	base      string
	root      string
	writeBack bool
	released  bool
}

func (lifecycle *fixtureCredentialLifecycle) Issue(_ context.Context, request ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	return ports.CredentialHandle{
		HandleID: "handle-a", TenantID: request.Run.TenantID,
		SubscriptionConnectionID: request.Run.SubscriptionConnectionID,
		OwnerUserID:              request.OwnerUserID, RunID: request.Run.ID, AttemptID: request.Attempt.ID,
		WorkerID: request.Attempt.WorkerID, LeaseID: request.Lease.ID,
		LeaseFence: request.Lease.FenceToken, BindingGeneration: 7, ExpiresAt: request.ExpiresAt,
	}, nil
}

func (lifecycle *fixtureCredentialLifecycle) Materialize(context.Context, ports.CredentialHandle) (ports.CredentialMaterialization, error) {
	root, err := os.MkdirTemp(lifecycle.base, "credential-")
	if err != nil {
		return ports.CredentialMaterialization{}, err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return ports.CredentialMaterialization{}, err
	}
	lifecycle.root = root
	auth := filepath.Join(root, "auth.json")
	if err := os.WriteFile(auth, []byte(`{"fixture":true}`), 0o600); err != nil {
		return ports.CredentialMaterialization{}, err
	}
	return ports.CredentialMaterialization{RootDir: root, AuthFile: auth}, nil
}

func (lifecycle *fixtureCredentialLifecycle) WriteBack(context.Context, ports.CredentialHandle, ports.CredentialMaterialization) (ports.CredentialWriteBackResult, error) {
	lifecycle.writeBack = true
	return ports.CredentialWriteBackResult{Changed: false, Generation: 7}, nil
}

func (lifecycle *fixtureCredentialLifecycle) Release(context.Context, ports.CredentialHandle) error {
	lifecycle.released = true
	return os.RemoveAll(lifecycle.root)
}

func (*fixtureCredentialLifecycle) RevokeConnection(context.Context, ports.CredentialRevokeRequest) error {
	return nil
}

func TestAdapterComposesRealSupervisorStdinCredentialAndCleanup(t *testing.T) {
	root := canonicalTempDir(t)
	fixture := filepath.Join(root, "codex-fixture")
	script := `#!/bin/sh
expected='exec --json --ephemeral --ignore-user-config --ignore-rules --strict-config --sandbox read-only --skip-git-repo-check --color never --model gpt-fixture -'
[ "$*" = "$expected" ] || exit 81
[ -f "$CODEX_HOME/auth.json" ] || exit 82
[ -z "$OPENAI_API_KEY" ] || exit 83
IFS= read -r prompt
[ "$prompt" = 'public fixture task' ] || exit 84
printf '%s\n' '{"type":"thread.started","thread_id":"private"}'
printf '%s\n' '{"type":"turn.started"}'
printf '%s\n' '{"type":"item.completed","item":{"type":"agent_message","text":"bounded result"}}'
printf '%s\n' '{"type":"turn.completed"}'
`
	if err := os.WriteFile(fixture, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	digest, err := attachedworkerdaemon.DigestExecutable(fixture)
	if err != nil {
		t.Fatal(err)
	}
	launcher := &fixtureIsolationLauncher{}
	credentialBase := canonicalTempDir(t)
	credentials := &fixtureCredentialLifecycle{base: credentialBase}
	supervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{
		ScratchRoot: canonicalTempDir(t), Launcher: launcher,
		AllowedEnvironmentNames: []string{"CODEX_HOME"}, AllowedReadRoots: []string{credentialBase},
	})
	if err != nil {
		t.Fatal(err)
	}
	invocations, err := attachedworkerdaemon.NewInvocationRunner(attachedworkerdaemon.InvocationRunnerConfig{}, supervisor, credentials)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := New(Config{
		Enabled: true, Executable: fixture, ExecutableVersion: "fixture-v1",
		ExecutableDigest: digest, Model: "gpt-fixture",
	}, invocations)
	if err != nil {
		t.Fatal(err)
	}
	result, err := adapter.Run(context.Background(), validRequest(t))
	if err != nil || result.Lifecycle != LifecycleCompleted || !result.CleanupSucceeded ||
		!result.CredentialFinalized || string(result.FinalCandidate) != "bounded result" ||
		!credentials.writeBack || !credentials.released {
		t.Fatalf("result=%+v err=%v writeback=%t release=%t", result, err, credentials.writeBack, credentials.released)
	}
	if _, err := os.Stat(credentials.root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("credential materialization survived release: %v", err)
	}
	launch := launcher.snapshot()
	joined := strings.Join(append(append([]string(nil), launch.Arguments...), launch.Environment...), "\x00")
	if strings.Contains(joined, "public fixture task") || strings.Contains(joined, "bounded result") ||
		strings.Contains(joined, "OPENAI_API_KEY=") {
		t.Fatalf("launcher-visible contract leaked invocation material: %q", joined)
	}
	if len(launch.WriteFiles) != 1 || launch.WriteFiles[0] != filepath.Join(credentials.root, "auth.json") {
		t.Fatalf("credential write authority = %#v", launch.WriteFiles)
	}
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

var _ ports.CredentialLifecycle = (*fixtureCredentialLifecycle)(nil)
