//go:build darwin || linux

package attachedworkerstack

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegenerationsession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
)

// This is a fixed synthetic process, not a model or provider. Writing the SAME
// bytes on both invocations makes the actual concrete lifecycle observe a first
// change and then an unchanged successor; the fixture never updates metadata.
const nativeGenerationJoinedAuthV3 = `{"synthetic_generation_joined":"refreshed"}`
const nativeGenerationJoinedFinalV3 = `{"answer":"two requested files","artifacts":[{"name":"data.json","media_type":"application/json","content":" { \"n\": 9007199254740993123456789 } "},{"name":"note.txt","media_type":"text/plain","content":"original text\n"}]}`

// NativeGenerationJoinedSettingsV3ForTest is installation DATA only. It contains
// no source factory, Session, accepted input, credential owner or output proof.
type NativeGenerationJoinedSettingsV3ForTest struct {
	Profile               codexexec.NativeProfileV3
	Harness               attachedworkerlocal.HarnessConfigV1
	OCI                   attachedworkerlocal.OCIConfigV1
	StackScratchRoot      string
	CredentialScratchRoot string
}

// NativeGenerationJoinedInstallationV3ForTest belongs only to Stack's test
// binary. The joined caller creates the original manifest, baseline, concrete
// lifecycle and generation owner using its actual state-store/connected path.
type NativeGenerationJoinedInstallationV3ForTest struct {
	t                     *testing.T
	settings              NativeGenerationJoinedSettingsV3ForTest
	mu                    sync.Mutex
	now                   time.Time
	launches              uint64
	credentialServiceName string
	credentialServiceInfo os.FileInfo
}

// NewNativeGenerationJoinedInstallationV3ForTest installs one closed shell and
// inert artifacts. dbAt MUST be sampled from the scenario's authoritative DB;
// no host clock, credential or session authority is inferred here.
func NewNativeGenerationJoinedInstallationV3ForTest(t *testing.T, dbAt time.Time) (*NativeGenerationJoinedInstallationV3ForTest, error) {
	t.Helper()
	if dbAt.IsZero() {
		return nil, errors.New("joined installation requires explicit DB time")
	}
	v := &NativeGenerationJoinedInstallationV3ForTest{t: t, now: dbAt.Round(0).UTC()}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		return nil, err
	}
	artifact := func(name, body string) (attachedworkerdaemon.NativeArtifactV2, error) {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte(body), 0o500); err != nil {
			return attachedworkerdaemon.NativeArtifactV2{}, err
		}
		return attachedworkerdaemon.NativeArtifactV2{Executable: path, Digest: attachedworkerdaemon.ExecutableDigest(sha256.Sum256([]byte(body)))}, nil
	}
	engine, err := artifact("inert-engine", "inert joined engine declaration; never executed")
	if err != nil {
		return nil, err
	}
	bootstrap, err := artifact("inert-bootstrap", "inert joined bootstrap declaration; never executed")
	if err != nil {
		return nil, err
	}
	relay, err := artifact("inert-relay", "inert joined relay declaration; never executed")
	if err != nil {
		return nil, err
	}
	text, err := json.Marshal(nativeGenerationJoinedFinalV3)
	if err != nil {
		return nil, err
	}
	protocol := "{\"type\":\"thread.started\",\"thread_id\":\"synthetic-generation-joined\"}\n{\"type\":\"turn.started\"}\n{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":" + string(text) + "}}\n{\"type\":\"turn.completed\"}\n"
	// Both bodies are fixed source literals without single quotes. There is no
	// caller-selected command, shell interpolation, code or provider endpoint.
	script := "#!/bin/sh\n[ -f \"$CODEX_HOME/auth.json\" ] || exit 81\nwhile IFS= read -r line; do :; done\nprintf '%s' '" + nativeGenerationJoinedAuthV3 + "' > \"$CODEX_HOME/auth.json\" || exit 82\nprintf '%s' '" + protocol + "'\n"
	workload, err := artifact("synthetic-codex", script)
	if err != nil {
		return nil, err
	}
	host, err := os.Executable()
	if err != nil {
		return nil, err
	}
	host, err = filepath.EvalSymlinks(host)
	if err != nil {
		return nil, err
	}
	hostDigest, err := attachedworkerdaemon.DigestExecutable(host)
	if err != nil {
		return nil, err
	}
	cliRoot, credentialRoot := filepath.Join(root, "cli"), filepath.Join(root, "credential-scratch")
	for _, directory := range []string{cliRoot, credentialRoot} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			return nil, err
		}
	}
	// Only a short private scratch is needed for the real Unix conduit socket.
	// Remove the exact owned root only after the original stack cleans children;
	// never recursively purge retained journal/output/credential custody.
	scratch, err := os.MkdirTemp("", "gj-")
	if err != nil {
		return nil, err
	}
	ownedScratch := scratch
	t.Cleanup(func() {
		if err := os.Remove(ownedScratch); err != nil {
			t.Errorf("joined original stack scratch residue: %v", err)
		}
		if err := v.checkCredentialScratchV3ForTest(); err != nil {
			t.Error(err)
		}
	})
	scratch, err = filepath.EvalSymlinks(scratch)
	if err != nil {
		return nil, err
	}
	platform := attachedworkerlocal.OCIConfigV1{DockerPath: engine.Executable, DockerSHA256: hex.EncodeToString(engine.Digest[:]), CLIConfigDir: cliRoot,
		Host: "unix:///run/docker.sock", EngineID: "synthetic-joined-engine", InstallationID: "synthetic-joined-installation",
		Boundary: attachedworkerlocal.BoundaryLinuxRootlessNativeV3, Image: "registry.example/synthetic-joined@sha256:" + strings.Repeat("b", 64),
		DiskBytes: 32 << 20, CredentialFileBytes: 8 << 10, MemoryBytes: 128 << 20, PIDsLimit: 32, StopSeconds: 5}
	profile := codexexec.NativeProfileV3{Backend: codexexec.Config{Enabled: true, Executable: workload.Executable, ExecutableDigest: workload.Digest,
		ExecutableVersion: "0.162.1", Model: "synthetic-generation-joined-model", Now: v.Now}, Platform: platform,
		OperatingSystem: "linux", Architecture: runtime.GOARCH, DeclaredContextWindow: 128000, OutputReserveTokens: 8192, StreamIdleTimeoutMS: 60000,
		Launch: attachedworkerdaemon.NativeLaunchV3{Version: 3, Bootstrap: bootstrap, Relay: relay,
			Companion: attachedworkerdaemon.NativeArtifactV2{Executable: host, Digest: hostDigest}, Workload: workload, SocketDestination: "/conduit/native.sock",
			ProviderHosts: []string{"auth.openai.com", "chatgpt.com"}, Limits: attachedworkerdaemon.NativeConduitLimitsV2{Connections: 2, InboundBytes: 1 << 20, OutboundBytes: 1 << 20,
				Active: time.Minute, Cleanup: 10 * time.Second, CompanionMemoryBytes: 64 << 20, CompanionGoroutines: 16}}}
	arguments, err := profile.ArgumentsV3()
	if err != nil {
		return nil, err
	}
	if _, err := profile.DescriptorV3(); err != nil {
		return nil, err
	}
	v.settings = NativeGenerationJoinedSettingsV3ForTest{Profile: profile, Harness: attachedworkerlocal.HarnessConfigV1{Executable: workload.Executable,
		SHA256: hex.EncodeToString(workload.Digest[:]), Arguments: arguments}, OCI: platform, StackScratchRoot: scratch, CredentialScratchRoot: credentialRoot}
	return v, nil
}

func (v *NativeGenerationJoinedInstallationV3ForTest) Settings() NativeGenerationJoinedSettingsV3ForTest {
	if v == nil {
		return NativeGenerationJoinedSettingsV3ForTest{}
	}
	data := v.settings
	data.Profile = data.Profile.Clone()
	data.Harness.Arguments = slices.Clone(data.Harness.Arguments)
	return data
}

// PinCredentialServiceRootV3ForTest records filesystem DATA immediately after
// the real Lifecycle constructor. It grants no credential/execution authority.
// Service.Close closes its FD but intentionally retains this service directory;
// Release owns removal of invocation/auth children, which must not survive.
func (v *NativeGenerationJoinedInstallationV3ForTest) PinCredentialServiceRootV3ForTest() error {
	if v == nil {
		return errors.New("joined credential service root unavailable")
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.credentialServiceInfo != nil {
		return errors.New("joined credential service root already pinned")
	}
	entries, err := os.ReadDir(v.settings.CredentialScratchRoot)
	if err != nil || len(entries) != 1 || !strings.HasPrefix(entries[0].Name(), "sessionless-credentials-") {
		return errors.New("joined credential service requires its sole original directory")
	}
	info, err := os.Lstat(filepath.Join(v.settings.CredentialScratchRoot, entries[0].Name()))
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("joined credential service directory is not private")
	}
	v.credentialServiceName, v.credentialServiceInfo = entries[0].Name(), info
	return nil
}

// Runs after the actual Lifecycle.Close cleanup. An empty service-root
// directory is expected; accepting any invocation/auth residue is not.
func (v *NativeGenerationJoinedInstallationV3ForTest) checkCredentialScratchV3ForTest() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	entries, err := os.ReadDir(v.settings.CredentialScratchRoot)
	if err != nil {
		return errors.New("joined credential scratch cannot be inspected")
	}
	if v.credentialServiceInfo == nil {
		if len(entries) != 0 {
			return errors.New("joined credential scratch has an unpinned entry")
		}
		return nil // Pure installation tests never construct a lifecycle.
	}
	if len(entries) != 1 || entries[0].Name() != v.credentialServiceName {
		return errors.New("joined credential scratch original service root changed")
	}
	root := filepath.Join(v.settings.CredentialScratchRoot, v.credentialServiceName)
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(v.credentialServiceInfo, info) {
		return errors.New("joined credential scratch original service directory identity changed")
	}
	children, err := os.ReadDir(root)
	if err != nil || len(children) != 0 {
		return errors.New("joined credential service retains invocation or auth residue")
	}
	return nil
}

// Filesystem assertion tests use owned empty synthetic directories only, not
// a credential lifecycle, invocation, native process or cleanup substitute.
func TestNativeGenerationJoinedCredentialScratchContract(t *testing.T) {
	t.Run("installation without lifecycle", func(t *testing.T) {
		v := &NativeGenerationJoinedInstallationV3ForTest{settings: NativeGenerationJoinedSettingsV3ForTest{CredentialScratchRoot: t.TempDir()}}
		if err := v.checkCredentialScratchV3ForTest(); err != nil {
			t.Fatal("empty installation-only scratch", err)
		}
	})
	for _, fault := range []string{"none", "auth residue", "foreign entry", "replacement", "symlink", "mode"} {
		t.Run(fault, func(t *testing.T) {
			scratch := t.TempDir()
			root := filepath.Join(scratch, "sessionless-credentials-synthetic")
			if err := os.Mkdir(root, 0o700); err != nil {
				t.Fatal("owned synthetic service root", err)
			}
			v := &NativeGenerationJoinedInstallationV3ForTest{settings: NativeGenerationJoinedSettingsV3ForTest{CredentialScratchRoot: scratch}}
			if err := v.PinCredentialServiceRootV3ForTest(); err != nil {
				t.Fatal("pin original synthetic service directory", err)
			}
			if err := v.PinCredentialServiceRootV3ForTest(); err == nil {
				t.Fatal("second pin replaced original directory identity")
			}
			switch fault {
			case "auth residue":
				if err := os.WriteFile(filepath.Join(root, "auth.json"), []byte("synthetic-residue"), 0o600); err != nil {
					t.Fatal("owned synthetic residue", err)
				}
			case "foreign entry":
				if err := os.Mkdir(filepath.Join(scratch, "unexpected-synthetic"), 0o700); err != nil {
					t.Fatal("owned unexpected directory", err)
				}
			case "replacement", "symlink":
				retained := filepath.Join(t.TempDir(), "retained-original")
				if err := os.Rename(root, retained); err != nil {
					t.Fatal("retain original owned directory identity", err)
				}
				if fault == "replacement" {
					if err := os.Mkdir(root, 0o700); err != nil {
						t.Fatal("owned replacement directory", err)
					}
				} else if err := os.Symlink(retained, root); err != nil {
					t.Fatal("owned synthetic directory symlink", err)
				}
			case "mode":
				if err := os.Chmod(root, 0o755); err != nil {
					t.Fatal("owned directory mode mutation", err)
				}
			}
			err := v.checkCredentialScratchV3ForTest()
			if (fault == "none") != (err == nil) {
				t.Fatalf("scratch assertion fault=%s denied=%t", fault, err != nil)
			}
		})
	}
}

// ObserveDBTime changes only the concrete test clock. It cannot refresh a
// baseline observation, evidence/lease expiry, credential or accepted attempt.
func (v *NativeGenerationJoinedInstallationV3ForTest) ObserveDBTime(at time.Time) error {
	if v == nil || at.IsZero() {
		return errors.New("joined clock requires nonzero DB observation")
	}
	at = at.Round(0).UTC()
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.now.IsZero() || at.Before(v.now) {
		return errors.New("joined DB clock observation moved backwards")
	}
	v.now = at
	return nil
}

func (v *NativeGenerationJoinedInstallationV3ForTest) Now() time.Time {
	if v == nil {
		return time.Time{}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.now
}

// BuildStack neither creates nor binds a Session. It supplies only the fixed
// synthetic launcher to the ORIGINAL generation stack/Supervisor/prepared owner;
// the actual supplied lifecycle pointer is also the credentials argument.
func (v *NativeGenerationJoinedInstallationV3ForTest) BuildStack(ctx context.Context, originalManifest attachedworkerlocal.ManifestV1,
	baseline domain.AttachedResourceGenerationBaselineV1, owner *attachedresourcegenerationsession.OwnerV1, lifecycle *ownerlocalcredential.Lifecycle) (*Stack, error) {
	if v == nil || v.t == nil || v.Now().IsZero() || ctx == nil || ctx.Err() != nil || owner == nil || lifecycle == nil {
		return nil, ErrInvalidConfiguration
	}
	settings := v.Settings()
	config := Config{PreparedCodexNativeGeneration: &codexexec.NativePreparedConfigV3{Profile: settings.Profile, Baseline: baseline,
		GenerationOwner: owner, Lifecycle: lifecycle, InlineOutputV2: true}, ScratchRoot: settings.StackScratchRoot,
		Timeout: 30 * time.Second, TerminationGrace: time.Second, MaxStdoutBytes: 4 << 20, MaxStderrBytes: 1 << 20,
		AllowedEnvironmentNames: []string{"CODEX_HOME"}, AllowedReadRoots: []string{settings.CredentialScratchRoot}, CredentialFinalizeGrace: 7 * time.Second}
	deps := nativeDependenciesV3{hostOS: "linux", hostArchitecture: runtime.GOARCH, digest: attachedworkerdaemon.DigestExecutable,
		newSupervisor: attachedworkerdaemon.NewSupervisorV3,
		newLauncher: func(context.Context, attachedworkeroci.Config) (nativeReconcilingLauncherV3, error) {
			return &nativeGenerationJoinedLauncherV3{installation: v}, nil
		}}
	stack, err := newNativeGenerationStackV3(ctx, originalManifest, config, lifecycle, deps)
	if err != nil {
		return nil, err
	}
	v.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := stack.Close(ctx); err != nil {
			v.t.Errorf("close original joined stack (retained custody is not erased): %v", err)
		}
	})
	return stack, nil
}

// LaunchCount counts accepted original launch preparations, not provider calls.
func (v *NativeGenerationJoinedInstallationV3ForTest) LaunchCount() uint64 {
	if v == nil {
		return 0
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.launches
}

type nativeGenerationJoinedLauncherV3 struct {
	installation *NativeGenerationJoinedInstallationV3ForTest
}

func (*nativeGenerationJoinedLauncherV3) ProfileV3() attachedworkerdaemon.IsolationProfileV3 {
	return attachedworkerdaemon.IsolationProfileV3{Version: 3, Name: "synthetic-native-generation-joined", FilesystemReadBoundary: true,
		FilesystemWriteBoundary: true, ProcessBoundary: true, DiskBytesBounded: true, Network: attachedworkerdaemon.NativeConnectUnixV2}
}
func (*nativeGenerationJoinedLauncherV3) Reconcile(context.Context) (int, error) { return 0, nil }
func (l *nativeGenerationJoinedLauncherV3) PrepareV3(ctx context.Context, envelope attachedworkerdaemon.LaunchEnvelopeV3) (attachedworkerdaemon.IsolationBoundaryV3, error) {
	if l == nil || l.installation == nil || ctx == nil || ctx.Err() != nil || envelope.Execution.ValidateV3(ctx, envelope.Native) != nil ||
		!envelope.Socket.MatchesExecutionV3(envelope.Execution) {
		return nil, errors.New("joined original launch/readset/socket mismatch")
	}
	workload := l.installation.settings.Profile.Launch.Workload
	if envelope.Workload.Executable != workload.Executable || envelope.Workload.ExecutableDigest != workload.Digest || envelope.Native.Workload != workload {
		return nil, errors.New("joined fixture permits only its fixed synthetic process")
	}
	if _, err := envelope.Socket.Path(); err != nil {
		return nil, err
	}
	l.installation.mu.Lock()
	if l.installation.launches >= 2 {
		l.installation.mu.Unlock()
		return nil, errors.New("joined fixture permits at most two original launches")
	}
	l.installation.launches++
	l.installation.mu.Unlock()
	command := exec.Command(envelope.Workload.Executable, envelope.Workload.Arguments...)
	command.Dir, command.Env = envelope.Workload.Directory, slices.Clone(envelope.Workload.Environment)
	return &nativeGenerationJoinedBoundaryV3{command: command, envelope: envelope}, nil
}

type nativeGenerationJoinedBoundaryV3 struct {
	command  *exec.Cmd
	envelope attachedworkerdaemon.LaunchEnvelopeV3
}

func (b *nativeGenerationJoinedBoundaryV3) Command() *exec.Cmd { return b.command }
func (b *nativeGenerationJoinedBoundaryV3) AttestedWorkloadV3() attachedworkerdaemon.WorkloadAttestationV3 {
	w := b.envelope.Workload
	return attachedworkerdaemon.WorkloadAttestationV3{Native: attachedworkerdaemon.WorkloadAttestation{Executable: w.Executable,
		ExecutableDigest: w.ExecutableDigest, Arguments: slices.Clone(w.Arguments)}, Bundle: b.envelope.Native.Clone(),
		Execution: b.envelope.Execution, ExecutionDigest: b.envelope.Execution.DigestV3(), Socket: b.envelope.Socket}
}
func (*nativeGenerationJoinedBoundaryV3) GracefulStop(context.Context) error  { return nil }
func (*nativeGenerationJoinedBoundaryV3) ForceStop(context.Context) error     { return nil }
func (*nativeGenerationJoinedBoundaryV3) Alive(context.Context) (bool, error) { return false, nil }
func (*nativeGenerationJoinedBoundaryV3) Release(context.Context) error       { return nil }

func TestNativeGenerationJoinedInstallationV3DataAndClock(t *testing.T) {
	at := time.Date(2026, time.October, 28, 12, 0, 0, 123, time.FixedZone("synthetic-db-offset", 3600))
	v, err := NewNativeGenerationJoinedInstallationV3ForTest(t, at)
	if err != nil {
		t.Fatal("fixed installation", err)
	}
	original := v.Settings()
	descriptor, err := original.Profile.DescriptorV3()
	if err != nil || descriptor.Validate() != nil || descriptor.BackendKind != domain.HarnessBackendCodexExecV1 {
		t.Fatalf("independent V3 descriptor: got=%+v err=%v", descriptor, err)
	}
	arguments, err := original.Profile.ArgumentsV3()
	if err != nil || !slices.Equal(arguments, original.Harness.Arguments) || original.OCI != original.Profile.Platform {
		t.Fatalf("fixed manifest/profile data mismatch: err=%v", err)
	}
	mutated := v.Settings()
	mutated.Profile.Launch.ProviderHosts[0] = "foreign.example"
	mutated.Profile.Backend.Model = "foreign-model"
	mutated.Profile.Platform.EngineID = "foreign-engine"
	mutated.Harness.Arguments[0] = "foreign-command"
	mutated.OCI.InstallationID = "foreign-installation"
	mutated.StackScratchRoot, mutated.CredentialScratchRoot = "/foreign-stack", "/foreign-credential"
	again := v.Settings()
	// Compare clock-independent declarations (function values are deliberately
	// not DeepEqual); every mutable installation slice must be independently held.
	original.Profile.Backend.Now, again.Profile.Backend.Now = nil, nil
	if !reflect.DeepEqual(original, again) {
		t.Fatal("caller mutated immutable installation data")
	}
	beforeDescriptor := descriptor
	if err := v.ObserveDBTime(at.Add(time.Second)); err != nil || !v.Now().Equal(at.Add(time.Second)) || v.Now().Location() != time.UTC {
		t.Fatalf("explicit monotone DB observation: at=%v err=%v", v.Now(), err)
	}
	if err := v.ObserveDBTime(v.Now()); err != nil {
		t.Fatal("same DB observation must be idempotent", err)
	}
	for _, invalid := range []time.Time{{}, at} {
		if err := v.ObserveDBTime(invalid); err == nil || !v.Now().Equal(at.Add(time.Second)) {
			t.Fatalf("invalid clock data changed authority: observation=%v now=%v err=%v", invalid, v.Now(), err)
		}
	}
	afterDescriptor, err := v.Settings().Profile.DescriptorV3()
	if err != nil || afterDescriptor != beforeDescriptor || v.LaunchCount() != 0 {
		t.Fatalf("clock minted new profile or process: descriptor=%+v launches=%d err=%v", afterDescriptor, v.LaunchCount(), err)
	}
	if _, err := NewNativeGenerationJoinedInstallationV3ForTest(t, time.Time{}); err == nil {
		t.Fatal("host-time fallback accepted missing DB sample")
	}
}

func TestNativeGenerationJoinedInstallationV3DoesNotCreateAuthority(t *testing.T) {
	v, err := NewNativeGenerationJoinedInstallationV3ForTest(t, time.Date(2026, time.October, 28, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	for _, ctx := range []context.Context{nil, t.Context()} {
		stack, err := v.BuildStack(ctx, attachedworkerlocal.ManifestV1{}, domain.AttachedResourceGenerationBaselineV1{}, nil, nil)
		if err == nil || stack != nil || v.LaunchCount() != 0 {
			t.Fatalf("installation promoted missing original owners: stack=%v launches=%d err=%v", stack, v.LaunchCount(), err)
		}
	}
	launcher := &nativeGenerationJoinedLauncherV3{installation: v}
	if boundary, err := launcher.PrepareV3(t.Context(), attachedworkerdaemon.LaunchEnvelopeV3{}); err == nil || boundary != nil || v.LaunchCount() != 0 {
		t.Fatalf("data fixture admitted fabricated execution: boundary=%v launches=%d err=%v", boundary, v.LaunchCount(), err)
	}
}
