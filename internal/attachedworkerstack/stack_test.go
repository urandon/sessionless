package attachedworkerstack

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestNewAssemblesPinnedStackAfterResidueReconciliation(t *testing.T) {
	t.Setenv("HOME", "/ambient/home-must-not-be-used")
	t.Setenv("PATH", "/ambient/path-must-not-be-used")
	t.Setenv("DOCKER_HOST", "unix:///ambient/docker.sock")
	fixture := newFixture(t)
	events := make([]string, 0, 4)
	launcher := &fakeLauncher{events: &events, residue: 3}
	process := &fakeProcessRunner{}
	delegate := &fakeInvocationRunner{}
	deps := dependencies{
		hostOS: fixture.hostOS,
		digest: attachedworkerdaemon.DigestExecutable,
		newLauncher: func(_ context.Context, config attachedworkeroci.Config) (reconcilingLauncher, error) {
			events = append(events, "launcher")
			if config.DockerPath != fixture.manifest.OCI.DockerPath || config.Host != fixture.manifest.OCI.Host ||
				config.CLIConfigDir != fixture.manifest.OCI.CLIConfigDir || config.Image != fixture.manifest.OCI.Image ||
				config.InstallationID != fixture.manifest.OCI.InstallationID {
				t.Fatalf("launcher config drifted: %#v", config)
			}
			return launcher, nil
		},
		newSupervisor: func(config attachedworkerdaemon.SupervisorConfig) (attachedworkerdaemon.ProcessRunner, error) {
			events = append(events, "supervisor")
			if config.ScratchRoot != fixture.scratch || config.Launcher != launcher ||
				len(config.AllowedEnvironmentNames) != 1 || config.AllowedEnvironmentNames[0] != "CODEX_HOME" {
				t.Fatalf("supervisor config drifted: %#v", config)
			}
			return process, nil
		},
		newRunner: func(config attachedworkerdaemon.InvocationRunnerConfig, got attachedworkerdaemon.ProcessRunner, credentials ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
			events = append(events, "runner")
			if got != process || credentials != fixture.credentials || config.CredentialFinalizeGrace != 7*time.Second {
				t.Fatal("invocation runner dependencies drifted")
			}
			return delegate, nil
		},
	}
	stack, err := newStack(context.Background(), fixture.manifest, fixture.config, fixture.credentials, deps)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(events, ",") != "launcher,reconcile,supervisor,runner" {
		t.Fatalf("construction order = %v", events)
	}
	evidence := stack.Evidence()
	if evidence.Version != 1 || evidence.ManifestRevision != fixture.manifest.Revision ||
		evidence.Boundary != fixture.manifest.OCI.Boundary || evidence.IsolationProfile != "test-isolation" ||
		evidence.ReconciledResidue != 3 {
		t.Fatalf("unexpected evidence: %#v", evidence)
	}
	evidenceText := evidence.String()
	for _, secret := range []string{fixture.manifest.OCI.DockerPath, fixture.manifest.OCI.CLIConfigDir, fixture.manifest.OCI.Host, fixture.manifest.Harness.Executable} {
		if strings.Contains(evidenceText, secret) {
			t.Fatalf("evidence leaked local configuration %q", secret)
		}
	}
	invocation := attachedworkerdaemon.Invocation{Process: attachedworkerdaemon.AttemptSpec{
		Executable: fixture.manifest.Harness.Executable, ExecutableDigest: fixture.harnessDigest,
		Arguments: append([]string(nil), fixture.manifest.Harness.Arguments...),
	}}
	if _, err := stack.Run(context.Background(), invocation); err != nil {
		t.Fatal(err)
	}
	if delegate.calls != 1 {
		t.Fatalf("delegate calls = %d", delegate.calls)
	}
	invocation.Process.Arguments = []string{"--changed"}
	if _, err := stack.Run(context.Background(), invocation); !errors.Is(err, ErrInvocationMismatch) {
		t.Fatalf("changed argv error = %v", err)
	}
}

func TestNewFailsBeforeLauncherOnArtifactOrLocalAuthorityDrift(t *testing.T) {
	tests := map[string]func(*stackFixture){
		"docker digest":  func(f *stackFixture) { f.manifest.OCI.DockerSHA256 = strings.Repeat("0", 64) },
		"harness digest": func(f *stackFixture) { f.manifest.Harness.SHA256 = strings.Repeat("0", 64) },
		"writable docker": func(f *stackFixture) {
			if err := os.Chmod(f.manifest.OCI.DockerPath, 0o777); err != nil {
				t.Fatal(err)
			}
		},
		"writable harness": func(f *stackFixture) {
			if err := os.Chmod(f.manifest.Harness.Executable, 0o777); err != nil {
				t.Fatal(err)
			}
		},
		"oversize docker": func(f *stackFixture) {
			if err := os.Truncate(f.manifest.OCI.DockerPath, (128<<20)+1); err != nil {
				t.Fatal(err)
			}
		},
		"oversize harness": func(f *stackFixture) {
			if err := os.Truncate(f.manifest.Harness.Executable, (128<<20)+1); err != nil {
				t.Fatal(err)
			}
		},
		"wide scratch permissions": func(f *stackFixture) {
			if err := os.Chmod(f.scratch, 0o755); err != nil {
				t.Fatal(err)
			}
		},
		"unsupported boundary": func(f *stackFixture) {
			if f.hostOS == "darwin" {
				f.manifest.OCI.Boundary = attachedworkerlocal.BoundaryLinuxRootless
			} else {
				f.manifest.OCI.Boundary = attachedworkerlocal.BoundaryDarwinVM
			}
		},
		"logged out": func(f *stackFixture) {
			f.manifest.Lifecycle = attachedworkerlocal.LifecycleLoggedOut
			f.manifest.Logout = &attachedworkerlocal.LogoutReceiptV1{Version: 1, RequestID: "request-001", RequestedAt: f.manifest.UpdatedAt, CommittedAt: f.manifest.UpdatedAt, SecretState: "retired", ServerRevocation: "unknown", RemoteErasure: "unknown"}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newFixture(t)
			mutate(fixture)
			called := false
			deps := fixture.dependencies(&called)
			_, err := newStack(context.Background(), fixture.manifest, fixture.config, fixture.credentials, deps)
			if err == nil || called {
				t.Fatalf("error = %v, launcher called = %t", err, called)
			}
		})
	}
}

func TestNewRejectsSymlinkArtifactAndReconciliationAmbiguity(t *testing.T) {
	t.Run("symlink artifact", func(t *testing.T) {
		fixture := newFixture(t)
		link := filepath.Join(filepath.Dir(fixture.manifest.Harness.Executable), "harness-link")
		if err := os.Symlink(fixture.manifest.Harness.Executable, link); err != nil {
			t.Fatal(err)
		}
		fixture.manifest.Harness.Executable = link
		called := false
		_, err := newStack(context.Background(), fixture.manifest, fixture.config, fixture.credentials, fixture.dependencies(&called))
		if !errors.Is(err, ErrArtifactMismatch) || called {
			t.Fatalf("error = %v, launcher called = %t", err, called)
		}
	})
	t.Run("reconciliation ambiguity", func(t *testing.T) {
		fixture := newFixture(t)
		launcher := &fakeLauncher{reconcileErr: errors.New("ambiguous engine state")}
		deps := fixture.dependencies(nil)
		deps.newLauncher = func(context.Context, attachedworkeroci.Config) (reconcilingLauncher, error) { return launcher, nil }
		_, err := newStack(context.Background(), fixture.manifest, fixture.config, fixture.credentials, deps)
		if !errors.Is(err, ErrReconciliationRequired) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestNewReportsUnsupportedHostBeforeHostSpecificFilesystemValidation(t *testing.T) {
	fixture := newFixture(t)
	fixture.config.ScratchRoot = filepath.Join(fixture.scratch, "missing")
	deps := fixture.dependencies(nil)
	deps.hostOS = "windows"
	stack, err := newStack(context.Background(), fixture.manifest, fixture.config, fixture.credentials, deps)
	if !errors.Is(err, ErrIsolationUnsupported) || stack != nil {
		t.Fatalf("stack = %#v, error = %v, want ErrIsolationUnsupported", stack, err)
	}
}

func TestNewCancellationDoesNotReturnPartialRunner(t *testing.T) {
	fixture := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	launcher := &fakeLauncher{reconcile: func(context.Context) (int, error) { cancel(); return 0, nil }}
	deps := fixture.dependencies(nil)
	deps.newLauncher = func(context.Context, attachedworkeroci.Config) (reconcilingLauncher, error) { return launcher, nil }
	stack, err := newStack(ctx, fixture.manifest, fixture.config, fixture.credentials, deps)
	if !errors.Is(err, context.Canceled) || stack != nil {
		t.Fatalf("stack = %#v, error = %v", stack, err)
	}
}

func TestNewRejectsInvalidSupervisorBoundsBeforeRunnerAuthority(t *testing.T) {
	fixture := newFixture(t)
	launcher := &fakeLauncher{}
	runnerCalled := false
	deps := fixture.dependencies(nil)
	deps.newLauncher = func(context.Context, attachedworkeroci.Config) (reconcilingLauncher, error) { return launcher, nil }
	deps.newSupervisor = func(config attachedworkerdaemon.SupervisorConfig) (attachedworkerdaemon.ProcessRunner, error) {
		return attachedworkerdaemon.NewSupervisor(config)
	}
	deps.newRunner = func(attachedworkerdaemon.InvocationRunnerConfig, attachedworkerdaemon.ProcessRunner, ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
		runnerCalled = true
		return &fakeInvocationRunner{}, nil
	}
	fixture.config.TerminationGrace = time.Minute
	stack, err := newStack(context.Background(), fixture.manifest, fixture.config, fixture.credentials, deps)
	if !errors.Is(err, ErrInvalidConfiguration) || stack != nil || runnerCalled {
		t.Fatalf("stack = %#v, error = %v, runner called = %t", stack, err, runnerCalled)
	}
}

type stackFixture struct {
	manifest      attachedworkerlocal.ManifestV1
	config        Config
	credentials   *fakeCredentialLifecycle
	harnessDigest attachedworkerdaemon.ExecutableDigest
	scratch       string
	hostOS        string
}

func newFixture(t *testing.T) *stackFixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	private := func(name string) string {
		path := filepath.Join(root, name)
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	executable := func(name, contents string) (string, [sha256.Size]byte) {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
			t.Fatal(err)
		}
		return path, sha256.Sum256([]byte(contents))
	}
	dockerPath, dockerDigest := executable("docker", "fixture docker executable")
	harnessPath, harnessDigest := executable("worker", "fixture harness executable")
	now := time.Date(2026, time.September, 17, 8, 0, 0, 0, time.UTC)
	hostOS := runtime.GOOS
	boundary := attachedworkerlocal.BoundaryLinuxRootless
	if hostOS == "darwin" {
		boundary = attachedworkerlocal.BoundaryDarwinVM
	}
	manifest := attachedworkerlocal.ManifestV1{
		Version: 1, Revision: 9, ControlPlaneOrigin: "https://control.example",
		TenantID: "tenant-001", OwnerUserID: "user-001", WorkerID: "worker-001",
		EnrollmentGeneration: 3, ConnectionGeneration: 4, IdentityKeyFingerprint: strings.Repeat("a", 64),
		OCI: attachedworkerlocal.OCIConfigV1{
			DockerPath: dockerPath, DockerSHA256: hexDigest(dockerDigest), CLIConfigDir: private("docker-config"),
			Host: "unix:///run/docker.sock", EngineID: "engine-001-abcdef", InstallationID: "install-001",
			Boundary: boundary, Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64),
			UserID: 1000, GroupID: 1000, DiskBytes: 1 << 30, CredentialFileBytes: 8 << 10,
			MemoryBytes: 128 << 20, PIDsLimit: 64, StopSeconds: 10,
		},
		Harness:   attachedworkerlocal.HarnessConfigV1{Executable: harnessPath, SHA256: hexDigest(harnessDigest), Arguments: []string{"exec", "--json"}},
		Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: now, UpdatedAt: now,
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("fixture manifest: %v", err)
	}
	return &stackFixture{
		manifest: manifest, scratch: private("scratch"), hostOS: hostOS,
		config:      Config{ScratchRoot: filepath.Join(root, "scratch"), AllowedEnvironmentNames: []string{"CODEX_HOME"}, CredentialFinalizeGrace: 7 * time.Second},
		credentials: &fakeCredentialLifecycle{}, harnessDigest: attachedworkerdaemon.ExecutableDigest(harnessDigest),
	}
}

func (fixture *stackFixture) dependencies(called *bool) dependencies {
	return dependencies{
		hostOS: fixture.hostOS, digest: attachedworkerdaemon.DigestExecutable,
		newLauncher: func(context.Context, attachedworkeroci.Config) (reconcilingLauncher, error) {
			if called != nil {
				*called = true
			}
			return &fakeLauncher{}, nil
		},
		newSupervisor: func(attachedworkerdaemon.SupervisorConfig) (attachedworkerdaemon.ProcessRunner, error) {
			return &fakeProcessRunner{}, nil
		},
		newRunner: func(attachedworkerdaemon.InvocationRunnerConfig, attachedworkerdaemon.ProcessRunner, ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
			return &fakeInvocationRunner{}, nil
		},
	}
}

func hexDigest(value [sha256.Size]byte) string { return fmtDigest(value[:]) }

func fmtDigest(value []byte) string {
	const alphabet = "0123456789abcdef"
	result := make([]byte, len(value)*2)
	for index, octet := range value {
		result[index*2] = alphabet[octet>>4]
		result[index*2+1] = alphabet[octet&15]
	}
	return string(result)
}

type fakeLauncher struct {
	events       *[]string
	residue      int
	reconcileErr error
	reconcile    func(context.Context) (int, error)
}

func (*fakeLauncher) Profile() attachedworkerdaemon.IsolationProfile {
	return attachedworkerdaemon.IsolationProfile{Name: "test-isolation", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, NetworkDenied: true, ProcessBoundary: true, DiskBytesBounded: true}
}
func (*fakeLauncher) Prepare(context.Context, attachedworkerdaemon.LaunchSpec) (attachedworkerdaemon.IsolationBoundary, error) {
	return nil, errors.New("not used")
}
func (launcher *fakeLauncher) Reconcile(ctx context.Context) (int, error) {
	if launcher.events != nil {
		*launcher.events = append(*launcher.events, "reconcile")
	}
	if launcher.reconcile != nil {
		return launcher.reconcile(ctx)
	}
	return launcher.residue, launcher.reconcileErr
}

type fakeProcessRunner struct{}

func (*fakeProcessRunner) Run(context.Context, attachedworkerdaemon.AttemptSpec) (attachedworkerdaemon.AttemptResult, error) {
	return attachedworkerdaemon.AttemptResult{}, nil
}

type fakeInvocationRunner struct{ calls int }

func (runner *fakeInvocationRunner) Run(context.Context, attachedworkerdaemon.Invocation) (attachedworkerdaemon.InvocationResult, error) {
	runner.calls++
	return attachedworkerdaemon.InvocationResult{}, nil
}

type fakeCredentialLifecycle struct{}

func (*fakeCredentialLifecycle) Issue(context.Context, ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	return ports.CredentialHandle{}, errors.New("not used")
}
func (*fakeCredentialLifecycle) Materialize(context.Context, ports.CredentialHandle) (ports.CredentialMaterialization, error) {
	return ports.CredentialMaterialization{}, errors.New("not used")
}
func (*fakeCredentialLifecycle) WriteBack(context.Context, ports.CredentialHandle, ports.CredentialMaterialization) (ports.CredentialWriteBackResult, error) {
	return ports.CredentialWriteBackResult{}, errors.New("not used")
}
func (*fakeCredentialLifecycle) Release(context.Context, ports.CredentialHandle) error { return nil }
func (*fakeCredentialLifecycle) RevokeConnection(context.Context, ports.CredentialRevokeRequest) error {
	return nil
}

var _ ports.CredentialLifecycle = (*fakeCredentialLifecycle)(nil)
var _ attachedworkerdaemon.Runner = (*fakeInvocationRunner)(nil)
var _ attachedworkerdaemon.ProcessRunner = (*fakeProcessRunner)(nil)
var _ reconcilingLauncher = (*fakeLauncher)(nil)
