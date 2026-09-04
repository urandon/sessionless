//go:build darwin || linux

package attachedworkerdaemon

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	helperEnabled = "SESSIONLESS_AW_DAEMON_HELPER"
	helperMode    = "SESSIONLESS_AW_DAEMON_MODE"
	helperReady   = "SESSIONLESS_AW_DAEMON_READY"
)

type fixtureLauncher struct {
	mu                sync.Mutex
	last              LaunchSpec
	name              string
	mutateAttestation func(*WorkloadAttestation)
	wrapCommand       func(LaunchSpec) *exec.Cmd
}

func (launcher *fixtureLauncher) Profile() IsolationProfile {
	name := launcher.name
	if name == "" {
		name = "test-fixture"
	}
	return IsolationProfile{
		Name: name, FilesystemReadBoundary: true,
		FilesystemWriteBoundary: true, NetworkDenied: true, ProcessBoundary: true,
		DiskBytesBounded: true,
	}
}

func TestSupervisorPinsValidatedIsolationProfile(t *testing.T) {
	supervisor, launcher, executable, digest := newFixtureSupervisor(t, SupervisorConfig{})
	launcher.name = "changed-after-validation"
	result, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
		Environment: []EnvironmentVariable{
			{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "environment"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsolationProfile != "test-fixture" {
		t.Fatalf("mutable launcher changed validated profile: %q", result.IsolationProfile)
	}
}

func TestSupervisorBoundsIsolationPreparation(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	launcher := &deadlinePrepareLauncher{}
	supervisor, err := NewSupervisor(SupervisorConfig{
		ScratchRoot: newCanonicalTempDir(t), Launcher: launcher,
		Timeout: 40 * time.Millisecond, TerminationGrace: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
	})
	if !errors.Is(err, ErrSupervisorConfig) {
		t.Fatalf("expected bounded preparation failure, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("isolation preparation was not bounded: %v", elapsed)
	}
	if !launcher.deadlineSeen || !result.CleanupSucceeded {
		t.Fatalf("preparation did not receive a deadline or clean its root: seen=%v result=%+v", launcher.deadlineSeen, result)
	}
}

type deadlinePrepareLauncher struct {
	fixtureLauncher
	deadlineSeen bool
}

func (launcher *deadlinePrepareLauncher) Prepare(ctx context.Context, _ LaunchSpec) (IsolationBoundary, error) {
	_, launcher.deadlineSeen = ctx.Deadline()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (launcher *fixtureLauncher) Prepare(_ context.Context, spec LaunchSpec) (IsolationBoundary, error) {
	launcher.mu.Lock()
	launcher.last = LaunchSpec{
		Executable: spec.Executable, ExecutableDigest: spec.ExecutableDigest,
		Arguments: append([]string(nil), spec.Arguments...),
		Directory: spec.Directory, Environment: append([]string(nil), spec.Environment...),
		ReadFiles:  append([]string(nil), spec.ReadFiles...),
		ReadRoots:  append([]string(nil), spec.ReadRoots...),
		WriteFiles: append([]string(nil), spec.WriteFiles...),
		WriteRoots: append([]string(nil), spec.WriteRoots...),
	}
	launcher.mu.Unlock()
	command := exec.Command(spec.Executable, spec.Arguments...)
	if launcher.wrapCommand != nil {
		command = launcher.wrapCommand(spec)
	}
	command.Dir = spec.Directory
	command.Env = append([]string(nil), spec.Environment...)
	attestation := WorkloadAttestation{
		Executable: spec.Executable, ExecutableDigest: spec.ExecutableDigest,
		Arguments: append([]string(nil), spec.Arguments...),
	}
	if launcher.mutateAttestation != nil {
		launcher.mutateAttestation(&attestation)
	}
	return &fixtureBoundary{command: command, attestation: attestation}, nil
}

type fixtureBoundary struct {
	command     *exec.Cmd
	attestation WorkloadAttestation
}

func (boundary *fixtureBoundary) Command() *exec.Cmd { return boundary.command }
func (boundary *fixtureBoundary) AttestedWorkload() WorkloadAttestation {
	return WorkloadAttestation{
		Executable: boundary.attestation.Executable, ExecutableDigest: boundary.attestation.ExecutableDigest,
		Arguments: append([]string(nil), boundary.attestation.Arguments...),
	}
}
func (*fixtureBoundary) GracefulStop(context.Context) error  { return nil }
func (*fixtureBoundary) ForceStop(context.Context) error     { return nil }
func (*fixtureBoundary) Alive(context.Context) (bool, error) { return false, nil }
func (*fixtureBoundary) Release(context.Context) error       { return nil }

func (launcher *fixtureLauncher) lastSpec() LaunchSpec {
	launcher.mu.Lock()
	defer launcher.mu.Unlock()
	return launcher.last
}

func TestSupervisorUsesReplacementEnvironmentAndCleansAttemptRoot(t *testing.T) {
	t.Setenv("HOST_SECRET", "must-not-cross")
	supervisor, launcher, executable, digest := newFixtureSupervisor(t, SupervisorConfig{})
	result, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
		Environment: []EnvironmentVariable{
			{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "environment"},
		},
	})
	if err != nil {
		t.Fatalf("run supervisor: %v", err)
	}
	if result.ExitCode != 0 || result.FailureCode != "" || !result.DescendantsReaped ||
		!result.CleanupSucceeded || result.IsolationProfile != "test-fixture" {
		t.Fatalf("unexpected result: %+v", result)
	}
	lines := strings.Split(strings.TrimSpace(string(result.Stdout)), "\n")
	if len(lines) != 4 || lines[0] == "" || lines[1] == "" || lines[2] == "" || lines[3] != "HOST_SECRET=" {
		t.Fatalf("unexpected replacement environment evidence: %q", result.Stdout)
	}
	launch := launcher.lastSpec()
	if got := filepath.Dir(launch.Directory); filepath.Base(got) == "" {
		t.Fatalf("attempt root was not recorded: %q", got)
	} else if _, statErr := os.Lstat(got); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("attempt root survived cleanup: %v", statErr)
	}
	if len(launch.WriteRoots) != 1 || launch.WriteRoots[0] != filepath.Dir(launch.Directory) {
		t.Fatalf("write boundary is not the exact attempt root: %#v", launch.WriteRoots)
	}
}

func TestSupervisorFeedsBoundedStdinWithoutExposingItToLauncher(t *testing.T) {
	supervisor, launcher, executable, digest := newFixtureSupervisor(t, SupervisorConfig{})
	result, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
		Environment: []EnvironmentVariable{
			{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "stdin"},
		},
		Stdin: []byte("private prompt"),
	})
	if err != nil || string(result.Stdout) != "stdin-ok\n" {
		t.Fatalf("stdin result=%+v err=%v", result, err)
	}
	launch := launcher.lastSpec()
	if strings.Contains(strings.Join(launch.Arguments, "\x00"), "private prompt") ||
		strings.Contains(strings.Join(launch.Environment, "\x00"), "private prompt") {
		t.Fatal("stdin payload crossed into launcher-visible argv/environment")
	}
	_, err = supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		Stdin: bytes.Repeat([]byte{'x'}, maxSupervisorStdinBytes+1),
	})
	if !errors.Is(err, ErrSupervisorConfig) {
		t.Fatalf("oversized stdin error = %v", err)
	}
}

func TestSupervisorRejectsAttestedWorkloadExecutableOrArgumentSubstitution(t *testing.T) {
	for name, mutate := range map[string]func(*WorkloadAttestation){
		"path": func(attestation *WorkloadAttestation) { attestation.Executable += ".other" },
		"digest": func(attestation *WorkloadAttestation) {
			attestation.ExecutableDigest[0] ^= 0xff
		},
		"arguments": func(attestation *WorkloadAttestation) {
			attestation.Arguments = append(attestation.Arguments, "--injected")
		},
	} {
		t.Run(name, func(t *testing.T) {
			supervisor, launcher, executable, digest := newFixtureSupervisor(t, SupervisorConfig{})
			launcher.mutateAttestation = mutate
			result, err := supervisor.Run(context.Background(), AttemptSpec{
				Executable: executable, ExecutableDigest: digest,
				Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
			})
			if !errors.Is(err, ErrSupervisorConfig) || !result.CleanupSucceeded {
				t.Fatalf("substitution result=%+v err=%v", result, err)
			}
		})
	}
}

func TestSupervisorKeepsOuterClientSeparateFromAttestedWorkload(t *testing.T) {
	supervisor, launcher, executable, digest := newFixtureSupervisor(t, SupervisorConfig{})
	launcher.wrapCommand = func(spec LaunchSpec) *exec.Cmd {
		// The fixture shell is only an outer client stand-in. The trusted
		// attestation remains the exact inner harness executable and argv.
		arguments := append([]string{"-c", `exec "$@"`, "fixture-wrapper", spec.Executable}, spec.Arguments...)
		return exec.Command("/bin/sh", arguments...)
	}
	result, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
		Environment: []EnvironmentVariable{
			{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "environment"},
		},
	})
	if err != nil || result.ExitCode != 0 || !result.CleanupSucceeded ||
		launcher.lastSpec().Executable != executable {
		t.Fatalf("outer wrapper result=%+v err=%v", result, err)
	}
}

func TestSupervisorRetainsProtocolPrefixWhenBoundaryTeardownFails(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	launcher := &failingTeardownLauncher{}
	supervisor, err := NewSupervisor(SupervisorConfig{
		ScratchRoot: newCanonicalTempDir(t), Launcher: launcher,
		Timeout: 2 * time.Second, TerminationGrace: 100 * time.Millisecond,
		AllowedEnvironmentNames: []string{helperEnabled, helperMode},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
		Environment: []EnvironmentVariable{
			{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "protocol-terminal"},
		},
	})
	if err == nil || !strings.Contains(string(result.Stdout), `{"type":"turn.started"}`) ||
		!strings.Contains(string(result.Stdout), `{"type":"turn.completed"}`) ||
		result.StdoutBytes != len(result.Stdout) || result.ExitCode != 0 || result.IsolationProfile == "" {
		t.Fatalf("teardown evidence result=%+v err=%v stdout=%q", result, err, result.Stdout)
	}
}

type failingTeardownLauncher struct{ fixtureLauncher }

func (launcher *failingTeardownLauncher) Prepare(ctx context.Context, spec LaunchSpec) (IsolationBoundary, error) {
	boundary, err := launcher.fixtureLauncher.Prepare(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &failingTeardownBoundary{fixtureBoundary: boundary.(*fixtureBoundary)}, nil
}

type failingTeardownBoundary struct{ *fixtureBoundary }

func (*failingTeardownBoundary) Alive(context.Context) (bool, error) {
	return false, errors.New("private inspection failure")
}

func TestSupervisorRejectsDigestDriftAndAmbientSecretRoutes(t *testing.T) {
	supervisor, _, executable, digest := newFixtureSupervisor(t, SupervisorConfig{})
	drift := digest
	drift[0] ^= 0xff
	_, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: drift,
		Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
	})
	if !errors.Is(err, ErrExecutableChanged) {
		t.Fatalf("expected digest drift rejection, got %v", err)
	}
	_, err = supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		Environment: []EnvironmentVariable{{Name: "OPENAI_API_KEY", Value: "forbidden"}},
	})
	if !errors.Is(err, ErrSupervisorConfig) {
		t.Fatalf("expected ambient secret route rejection, got %v", err)
	}
	_, err = supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		AdditionalReadRoots: []string{"/"},
	})
	if !errors.Is(err, ErrSupervisorConfig) {
		t.Fatalf("expected ambient host read-root rejection, got %v", err)
	}
}

func TestSupervisorRejectsUnscopedCredentialWriteFile(t *testing.T) {
	supervisor, _, executable, digest := newFixtureSupervisor(t, SupervisorConfig{})
	root := newCanonicalTempDir(t)
	authFile := filepath.Join(root, "auth.json")
	if err := os.WriteFile(authFile, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		credentialWriteFile: authFile,
	})
	if !errors.Is(err, ErrSupervisorConfig) {
		t.Fatalf("expected unscoped credential write rejection, got %v", err)
	}
}

func TestSupervisorBoundsOutputAndReapsTermResistantGroup(t *testing.T) {
	supervisor, _, executable, digest := newFixtureSupervisor(t, SupervisorConfig{
		MaxStdoutBytes: 128, MaxStderrBytes: 64, TerminationGrace: 100 * time.Millisecond,
	})
	result, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
		Environment: []EnvironmentVariable{
			{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "output-bomb"},
		},
	})
	if err != nil {
		t.Fatalf("run output bomb: %v", err)
	}
	if result.FailureCode != "stdout_limit_exceeded" || len(result.Stdout) != 128 ||
		!result.TermSent || !result.DescendantsReaped || !result.CleanupSucceeded {
		t.Fatalf("unexpected bounded output result: %+v", result)
	}
}

func TestSupervisorCancellationKillsTermResistantDescendant(t *testing.T) {
	ready := filepath.Join(newCanonicalTempDir(t), "ready")
	supervisor, _, executable, digest := newFixtureSupervisor(t, SupervisorConfig{
		TerminationGrace: 100 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultChannel := make(chan AttemptResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		result, err := supervisor.Run(ctx, AttemptSpec{
			Executable: executable, ExecutableDigest: digest,
			Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
			Environment: []EnvironmentVariable{
				{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "term-resistant"},
				{Name: helperReady, Value: ready},
			},
		})
		resultChannel <- result
		errorChannel <- err
	}()
	waitForFile(t, ready)
	cancel()
	result := <-resultChannel
	if err := <-errorChannel; err != nil {
		t.Fatalf("cancel supervisor: %v", err)
	}
	if !result.Cancelled || !result.TermSent || !result.KillSent ||
		!result.DescendantsReaped || !result.CleanupSucceeded {
		t.Fatalf("unexpected cancellation result: %+v", result)
	}
}

func TestSupervisorNaturalLeaderExitReapsDescendant(t *testing.T) {
	supervisor, _, executable, digest := newFixtureSupervisor(t, SupervisorConfig{
		TerminationGrace: 100 * time.Millisecond,
	})
	result, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
		Environment: []EnvironmentVariable{
			{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "leader-exit"},
		},
	})
	if err != nil {
		t.Fatalf("run natural leader exit: %v", err)
	}
	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(result.Stdout)))
	if parseErr != nil {
		t.Fatalf("parse descendant pid: %v (%q)", parseErr, result.Stdout)
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant %d survived natural leader cleanup: %v", pid, err)
	}
	if !result.TermSent || !result.DescendantsReaped || !result.CleanupSucceeded {
		t.Fatalf("unexpected natural-exit result: %+v", result)
	}
}

func TestSupervisorNaturalClientExitReapsDetachedBoundaryWorkload(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	launcher := &detachedFixtureLauncher{executable: executable}
	supervisor, err := NewSupervisor(SupervisorConfig{
		ScratchRoot: newCanonicalTempDir(t), Launcher: launcher,
		Timeout: 2 * time.Second, TerminationGrace: 100 * time.Millisecond,
		AllowedEnvironmentNames: []string{helperEnabled, helperMode},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
		Environment: []EnvironmentVariable{
			{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "environment"},
		},
	})
	if err != nil {
		t.Fatalf("run detached boundary fixture: %v", err)
	}
	if !result.BoundaryReleased || !result.CleanupSucceeded {
		t.Fatalf("boundary cleanup was not reported: %+v", result)
	}
	if err := syscall.Kill(launcher.pid(), 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("detached boundary workload survived: %v", err)
	}
}

func TestSupervisorStartFailureStillReapsDetachedBoundaryWorkload(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	launcher := &detachedFixtureLauncher{executable: executable, startFailure: true}
	supervisor, err := NewSupervisor(SupervisorConfig{
		ScratchRoot: newCanonicalTempDir(t), Launcher: launcher,
		Timeout: 2 * time.Second, TerminationGrace: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := supervisor.Run(context.Background(), AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
	})
	if err == nil || err.Error() != "start attached worker harness" {
		t.Fatalf("expected bounded start failure, got %v", err)
	}
	if !result.BoundaryReleased || !result.CleanupSucceeded {
		t.Fatalf("start failure did not clean the boundary: %+v", result)
	}
	if err := syscall.Kill(launcher.pid(), 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("detached boundary workload survived start failure: %v", err)
	}
}

type detachedFixtureLauncher struct {
	fixtureLauncher
	executable   string
	startFailure bool
	mu           sync.Mutex
	workloadPID  int
}

func (launcher *detachedFixtureLauncher) Prepare(ctx context.Context, spec LaunchSpec) (IsolationBoundary, error) {
	client, err := launcher.fixtureLauncher.Prepare(ctx, spec)
	if err != nil {
		return nil, err
	}
	if launcher.startFailure {
		failed := exec.Command(filepath.Join(spec.Directory, "missing-client"))
		failed.Dir = spec.Directory
		failed.Env = append([]string(nil), spec.Environment...)
		client.(*fixtureBoundary).command = failed
	}
	workload := exec.Command(launcher.executable, "-test.run=TestSupervisorHelperProcess")
	workload.Env = replaceEnvironment(os.Environ(), helperEnabled, "1")
	workload.Env = replaceEnvironment(workload.Env, helperMode, "child-sleep")
	configureProcessGroup(workload)
	if err := workload.Start(); err != nil {
		return nil, err
	}
	launcher.mu.Lock()
	launcher.workloadPID = workload.Process.Pid
	launcher.mu.Unlock()
	waited := make(chan error, 1)
	go func() { waited <- workload.Wait() }()
	return &detachedFixtureBoundary{
		fixtureBoundary: client.(*fixtureBoundary), processGroupID: workload.Process.Pid, waited: waited,
	}, nil
}

func (launcher *detachedFixtureLauncher) pid() int {
	launcher.mu.Lock()
	defer launcher.mu.Unlock()
	return launcher.workloadPID
}

type detachedFixtureBoundary struct {
	*fixtureBoundary
	processGroupID int
	waited         <-chan error
	mu             sync.Mutex
	reaped         bool
}

func (*detachedFixtureBoundary) GracefulStop(context.Context) error { return nil }
func (boundary *detachedFixtureBoundary) ForceStop(ctx context.Context) error {
	if err := signalProcessGroup(boundary.processGroupID, processKill); err != nil {
		return err
	}
	return boundary.wait(ctx)
}
func (boundary *detachedFixtureBoundary) Alive(context.Context) (bool, error) {
	return processGroupAlive(boundary.processGroupID)
}
func (boundary *detachedFixtureBoundary) Release(ctx context.Context) error {
	if alive, _ := processGroupAlive(boundary.processGroupID); alive {
		_ = signalProcessGroup(boundary.processGroupID, processKill)
	}
	return boundary.wait(ctx)
}

func (boundary *detachedFixtureBoundary) wait(ctx context.Context) error {
	boundary.mu.Lock()
	defer boundary.mu.Unlock()
	if boundary.reaped {
		return nil
	}
	select {
	case <-boundary.waited:
		boundary.reaped = true
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestNewSupervisorFailsClosedWithoutCompleteIsolation(t *testing.T) {
	root := newCanonicalTempDir(t)
	launcher := &incompleteLauncher{}
	_, err := NewSupervisor(SupervisorConfig{ScratchRoot: root, Launcher: launcher})
	if !errors.Is(err, ErrIsolationUnsupported) {
		t.Fatalf("expected unsupported isolation, got %v", err)
	}
}

type incompleteLauncher struct{}

func (*incompleteLauncher) Profile() IsolationProfile {
	return IsolationProfile{Name: "clean-env-only"}
}
func (*incompleteLauncher) Prepare(context.Context, LaunchSpec) (IsolationBoundary, error) {
	return nil, errors.New("must not be called")
}

func newFixtureSupervisor(
	t *testing.T,
	override SupervisorConfig,
) (*Supervisor, *fixtureLauncher, string, ExecutableDigest) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatalf("canonicalize test executable: %v", err)
	}
	digest, err := DigestExecutable(executable)
	if err != nil {
		t.Fatalf("digest test executable: %v", err)
	}
	launcher := &fixtureLauncher{}
	config := override
	config.ScratchRoot = newCanonicalTempDir(t)
	config.Launcher = launcher
	config.Timeout = 2 * time.Second
	config.AllowedEnvironmentNames = append(
		[]string{helperEnabled, helperMode, helperReady}, override.AllowedEnvironmentNames...,
	)
	supervisor, err := NewSupervisor(config)
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}
	return supervisor, launcher, executable, digest
}

func newCanonicalTempDir(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("", "sessionless-aw-daemon-test-")
	if err != nil {
		t.Fatalf("create canonical temp dir: %v", err)
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		_ = os.RemoveAll(root)
		t.Fatalf("canonicalize temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(canonical) })
	return canonical
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("helper readiness file was not created: %s", path)
}

func TestSupervisorHelperProcess(t *testing.T) {
	if os.Getenv(helperEnabled) != "1" {
		return
	}
	switch os.Getenv(helperMode) {
	case "environment":
		fmt.Printf("%s\n%s\n%s\nHOST_SECRET=%s\n", os.Getenv("HOME"), os.Getenv("TMPDIR"), os.Getenv("XDG_CONFIG_HOME"), os.Getenv("HOST_SECRET"))
	case "credential":
		content, err := os.ReadFile(filepath.Join(os.Getenv("CODEX_HOME"), "auth.json"))
		if err != nil || string(content) != "{}" {
			os.Exit(95)
		}
		fmt.Println("credential-ok")
	case "stdin":
		content, err := io.ReadAll(os.Stdin)
		if err != nil || string(content) != "private prompt" {
			os.Exit(96)
		}
		fmt.Println("stdin-ok")
	case "protocol-terminal":
		fmt.Println(`{"type":"thread.started","thread_id":"fixture"}`)
		fmt.Println(`{"type":"turn.started"}`)
		fmt.Println(`{"type":"item.completed","item":{"type":"agent_message","text":"fixture result"}}`)
		fmt.Println(`{"type":"turn.completed"}`)
	case "output-bomb":
		_, _ = os.Stdout.Write(bytes.Repeat([]byte("x"), 4096))
		time.Sleep(time.Second)
	case "term-resistant":
		signalIgnoreTerminate()
		child := exec.Command(os.Args[0], "-test.run=TestSupervisorHelperProcess")
		child.Env = replaceEnvironment(os.Environ(), helperMode, "child-sleep")
		child.Stdout = os.Stderr
		child.Stderr = os.Stderr
		if err := child.Start(); err != nil {
			os.Exit(91)
		}
		if ready := os.Getenv(helperReady); ready != "" {
			if err := os.WriteFile(ready, []byte("ready"), 0o600); err != nil {
				os.Exit(92)
			}
		}
		for {
			time.Sleep(time.Second)
		}
	case "leader-exit":
		child := exec.Command(os.Args[0], "-test.run=TestSupervisorHelperProcess")
		child.Env = replaceEnvironment(os.Environ(), helperMode, "child-sleep")
		child.Stdout = nil
		child.Stderr = nil
		if err := child.Start(); err != nil {
			os.Exit(93)
		}
		fmt.Println(child.Process.Pid)
	case "child-sleep":
		signalIgnoreTerminate()
		for {
			time.Sleep(time.Second)
		}
	default:
		os.Exit(94)
	}
	os.Exit(0)
}

func replaceEnvironment(environment []string, name, value string) []string {
	prefix := name + "="
	result := make([]string, 0, len(environment)+1)
	for _, item := range environment {
		if !strings.HasPrefix(item, prefix) {
			result = append(result, item)
		}
	}
	return append(result, prefix+value)
}

func signalIgnoreTerminate() {
	signalIgnore(syscall.SIGTERM)
}
