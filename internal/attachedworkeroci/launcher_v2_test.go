//go:build darwin || linux

package attachedworkeroci

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/nativeharnessbootstrap"
)

func shortOwnedRoot(t *testing.T, prefix string) string {
	t.Helper()
	root, err := os.MkdirTemp("", prefix)
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(root); err != nil {
			t.Errorf("owned short root not empty/removed: %v", err)
		}
	})
	return root
}

func nativeFixture(t *testing.T) fixture {
	t.Helper()
	f := newFixture(t, "linux", true)
	f.config.UserID, f.config.GroupID = 0, 0
	// This is only an inert owned Unix inode for constructor ownership checks;
	// fakeRunner handles every engine operation. No connection is opened.
	root := shortOwnedRoot(t, "e-")
	path := filepath.Join(root, "s")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("fake endpoint close: %v", err)
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("fake endpoint residue: %v", err)
		}
	})
	f.config.Host = "unix://" + path
	return f
}

func TestNativeConstructorIsSeparateStrictRootlessTuple(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*fixture)
	}{
		{"nonzero-uid", func(f *fixture) { f.config.UserID = 65532 }},
		{"nonzero-gid", func(f *fixture) { f.config.GroupID = 65532 }},
		{"darwin", func(f *fixture) { f.config.hostOS = "darwin"; f.config.Boundary = BoundaryDarwinVM }},
		{"rootful-remap-only", func(f *fixture) {
			f.runner.info.SecurityOptions = []string{"name=seccomp,profile=builtin", "name=userns"}
		}},
		{"missing-endpoint", func(f *fixture) { f.config.Host += "-missing" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := nativeFixture(t)
				tc.mutate(&f)
				if _, err := NewNativeLauncher(t.Context(), f.config); err == nil {
					t.Fatal("ineligible native tuple accepted")
				}
			})
		})
	}
	synctest.Test(t, func(t *testing.T) {
		f := nativeFixture(t)
		if ValidateDeclarativeConfig(f.config) == nil {
			t.Fatal("V1 validator now accepts zero IDs")
		}
		if _, err := NewLauncher(t.Context(), f.config); err == nil {
			t.Fatal("V1 constructor now accepts zero IDs")
		}
		l, err := NewNativeLauncher(t.Context(), f.config)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := any(l).(attachedworkerdaemon.IsolationLauncher); ok {
			t.Fatal("native constructor implements V1 launcher")
		}
		if p := l.ProfileV2(); p.Validate() != nil || p.Network != attachedworkerdaemon.NativeConnectUnixV2 || p.Name == profileName {
			t.Fatalf("misreported native egress: %+v", p)
		}
	})
}

// Stops composition during preparation, before Supervisor can run any process.
// The Unix listener owner still comes from the real Supervisor's private path;
// there is no test constructor for an authorization/identity-bearing handle.
type nativePrepareOnly struct {
	t                    *testing.T
	launcher             *NativeLauncher
	before               func(context.Context, attachedworkerdaemon.LaunchEnvelopeV2)
	after                func(attachedworkerdaemon.IsolationBoundaryV2)
	mutate               func(*attachedworkerdaemon.LaunchEnvelopeV2)
	prepareErr           error
	boundary             attachedworkerdaemon.IsolationBoundaryV2
	result               attachedworkerdaemon.AttemptResult
	runErr               error
	expectedReleaseError error
	conduit              attachedworkerdaemon.NativeConduitV2
}

func (f *nativePrepareOnly) ProfileV2() attachedworkerdaemon.IsolationProfileV2 {
	return f.launcher.ProfileV2()
}
func (f *nativePrepareOnly) PrepareV2(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV2) (attachedworkerdaemon.IsolationBoundaryV2, error) {
	if f.before != nil {
		f.before(ctx, e)
	}
	if f.mutate != nil {
		f.mutate(&e)
	}
	b, err := f.launcher.PrepareV2(ctx, e)
	f.boundary = b
	f.prepareErr = err
	if b != nil {
		f.t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if f.conduit != nil {
				if err := f.conduit.Stop(cleanup); err != nil {
					if !errors.Is(err, f.expectedReleaseError) {
						f.t.Errorf("original fake conduit cleanup: %v", err)
					}
					return
				}
			}
			if err := b.Release(cleanup); (f.expectedReleaseError == nil && err != nil) || (f.expectedReleaseError != nil && !errors.Is(err, f.expectedReleaseError)) {
				f.t.Errorf("original fake boundary cleanup: %v", err)
			}
		})
	}
	if b != nil && err == nil && f.after != nil {
		f.after(b)
	}
	return b, errors.Join(err, errors.New("owned fixture stops before process Start"))
}

type inertNativeConduit struct{ stops int }

func (*inertNativeConduit) Start(_ context.Context, s *attachedworkerdaemon.NativeSocketV2) error {
	_, err := s.Path()
	return err
}
func (c *inertNativeConduit) Stop(context.Context) error { c.stops++; return nil }
func (*inertNativeConduit) Alive() bool                  { return false }

func nativeBundle(t *testing.T, f fixture) attachedworkerdaemon.NativeLaunchV2 {
	t.Helper()
	helper := filepath.Join(filepath.Dir(f.spec.Executable), "bootstrap-relay")
	if err := os.WriteFile(helper, []byte("pinned inert helper artifact"), 0700); err != nil {
		t.Fatal(err)
	}
	digest, err := attachedworkerdaemon.DigestExecutable(helper)
	if err != nil {
		t.Fatal(err)
	}
	artifact := attachedworkerdaemon.NativeArtifactV2{Executable: helper, Digest: digest}
	b := attachedworkerdaemon.NativeLaunchV2{Version: 2, Bootstrap: artifact, Relay: artifact, Companion: artifact, SocketDestination: "/conduit/native.sock", ProviderHosts: []string{"api.openai.com"}, ScopeDigest: f.spec.ExecutableDigest,
		Limits: attachedworkerdaemon.NativeConduitLimitsV2{Connections: 2, InboundBytes: 1024, OutboundBytes: 1024, CompanionMemoryBytes: 64 << 20, CompanionGoroutines: 4, Active: 30 * time.Second, Cleanup: time.Second}}
	b.BootstrapArguments, b.RelayArguments, err = nativeharnessbootstrap.Arguments(f.spec, b)
	if err != nil {
		t.Fatal(err)
	}
	b.PolicyDigest, err = b.InstallPolicyDigestV2()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func runNativePreparation(t *testing.T, f fixture, b attachedworkerdaemon.NativeLaunchV2, driver *nativePrepareOnly) *inertNativeConduit {
	t.Helper()
	driver.t = t
	root := shortOwnedRoot(t, "n-")
	supervisor, err := attachedworkerdaemon.NewSupervisorV2(attachedworkerdaemon.SupervisorConfigV2{Base: attachedworkerdaemon.SupervisorConfig{ScratchRoot: root, Timeout: 30 * time.Second, TerminationGrace: time.Second, AllowedReadRoots: []string{filepath.Dir(filepath.Dir(f.credential))}, AllowedEnvironmentNames: []string{"CODEX_HOME"}}, Launcher: driver})
	if err != nil {
		t.Fatal(err)
	}
	spec := attachedworkerdaemon.AttemptSpec{Executable: f.spec.Executable, ExecutableDigest: f.spec.ExecutableDigest, Arguments: slices.Clone(f.spec.Arguments), Environment: []attachedworkerdaemon.EnvironmentVariable{{Name: "CODEX_HOME", Value: filepath.Dir(f.credential)}}}
	spec, err = attachedworkerdaemon.BindPreparedCredential(spec, filepath.Dir(f.credential), f.credential)
	if err != nil {
		t.Fatal(err)
	}
	conduit := &inertNativeConduit{}
	var original attachedworkerdaemon.NativeConduitV2 = conduit
	if driver.conduit != nil {
		original = driver.conduit
	}
	driver.result, driver.runErr = supervisor.RunNativeV2(t.Context(), spec, b, original)
	if driver.runErr == nil {
		t.Fatal("prepare-only fixture unexpectedly continued")
	}
	return conduit
}

type nativeFaultRunner struct {
	base                                       *fakeRunner
	createUnknown, removeUnknown, stateUnknown bool
	stage                                      string
	stageError                                 error
}

func (r *nativeFaultRunner) Command(dir string, env, args []string) *exec.Cmd {
	return r.base.Command(dir, env, args)
}
func (r *nativeFaultRunner) Run(ctx context.Context, dir string, env, args []string) ([]byte, []byte, error) {
	out, errout, err := r.base.Run(ctx, dir, env, args)
	if err != nil {
		return out, errout, err
	}
	command := dockerCommand(args)
	if len(command) > 1 && command[0] == "container" && command[1] == "create" {
		if r.stage != "" {
			r.stageError = os.WriteFile(r.stage, []byte(`{"synthetic":"must-not-handback"}`), 0600)
			if r.stageError != nil {
				return nil, nil, r.stageError
			}
		}
		if r.createUnknown {
			return []byte(testContainerID), nil, errors.New("ambiguous synthetic create")
		}
	}
	if len(command) > 1 && command[0] == "container" && command[1] == "rm" && r.removeUnknown {
		return nil, nil, errors.New("ambiguous synthetic removal")
	}
	if len(command) > 3 && command[0] == "container" && command[1] == "inspect" && command[3] == "{{json .State}}" && r.stateUnknown {
		return nil, nil, errors.New("unknown synthetic state")
	}
	return out, errout, nil
}

func appendInspectionMount(r *containerResponse, source, dest string, rw bool) {
	r.Mounts = append(r.Mounts, struct {
		Type        string `json:"Type"`
		Source      string `json:"Source"`
		Destination string `json:"Destination"`
		RW          bool   `json:"RW"`
	}{"bind", source, dest, rw})
}

func nativeInspection(t *testing.T, f fixture, ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV2) containerResponse {
	t.Helper()
	shadow := f
	shadow.spec = e.Workload
	shadow.attempt = e.Workload.WriteRoots[0]
	r := prepareInspect(shadow)
	r.Config.User = "0:0"
	r.Config.Entrypoint = []string{e.Native.Bootstrap.Executable}
	r.Config.Cmd = slices.Clone(e.Native.BootstrapArguments)
	deadline, _ := ctx.Deadline()
	// Independent literal black-box expectation; not the production env builder.
	r.Config.Env = append(slices.Clone(e.Workload.Environment), "SESSIONLESS_NATIVE_DEADLINE_UNIX_NS="+strconv.FormatInt(deadline.UnixNano(), 10))
	r.Config.Labels[profileLabel] = nativeProfileName
	r.HostConfig.CPUPeriod, r.HostConfig.CPUQuota = 100000, 100000
	r.HostConfig.Tmpfs = map[string]string{shadow.attempt: "rw,nosuid,nodev,noexec,size=31457280,mode=0700,uid=0,gid=0"}
	appendInspectionMount(&r, e.Native.Bootstrap.Executable, e.Native.Bootstrap.Executable, false)
	if e.Native.Relay.Executable != e.Native.Bootstrap.Executable {
		appendInspectionMount(&r, e.Native.Relay.Executable, e.Native.Relay.Executable, false)
	}
	socket, err := e.Socket.Path()
	if err != nil {
		t.Fatal(err)
	}
	appendInspectionMount(&r, socket, e.Native.SocketDestination, false)
	return r
}

func TestNativePreparePinsBundleEnvAndExactCredentialBridge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := nativeFixture(t)
		l, err := NewNativeLauncher(t.Context(), f.config)
		if err != nil {
			t.Fatal(err)
		}
		b := nativeBundle(t, f)
		driver := &nativePrepareOnly{launcher: l}
		var workload attachedworkerdaemon.LaunchSpec
		var socketPath string
		var deadline time.Time
		driver.before = func(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV2) {
			workload = e.Workload
			deadline, _ = ctx.Deadline()
			socketPath, _ = e.Socket.Path()
			f.runner.containerInspect = nativeInspection(t, f, ctx, e)
		}
		driver.after = func(prepared attachedworkerdaemon.IsolationBoundaryV2) {
			command := prepared.Command()
			if command.Process != nil || !slices.Equal(command.Env, workload.Environment) {
				t.Fatal("outer command env/process changed")
			}
			observation := prepared.AttestedWorkloadV2()
			if observation.Native.Executable != workload.Executable || !slices.Equal(observation.Native.Arguments, workload.Arguments) || !reflect.DeepEqual(observation.Bundle, b) {
				t.Fatal("bootstrap misreported as native workload")
			}
			if _, ok := any(prepared).(attachedworkerdaemon.IsolationBoundary); ok {
				t.Fatal("V2 boundary exposed V1 attestation")
			}
			stage := filepath.Join(workload.WriteRoots[0], ".sessionless-auth-stage")
			info, err := os.Stat(stage)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("exact stage mode: %v %v", info, err)
			}
			if err := os.WriteFile(stage, []byte(`{"synthetic":"updated"}`), 0600); err != nil {
				t.Fatal(err)
			}
		}
		conduit := runNativePreparation(t, f, b, driver)
		if driver.prepareErr != nil {
			t.Fatalf("PrepareV2: %v", driver.prepareErr)
		}
		if conduit.stops == 0 {
			t.Fatal("aggregate supervisor omitted companion stop")
		}
		raw, err := os.ReadFile(f.credential)
		if err != nil || string(raw) != `{"synthetic":"updated"}` {
			t.Fatalf("bounded synthetic credential handback: %q %v", raw, err)
		}
		if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("original socket residue: %v", err)
		}
		var create []string
		for _, call := range f.runner.snapshot() {
			args := dockerCommand(call.arguments)
			if len(args) > 1 && args[0] == "container" && args[1] == "create" {
				create = args
			}
		}
		wantDeadline := "SESSIONLESS_NATIVE_DEADLINE_UNIX_NS=" + strconv.FormatInt(deadline.UnixNano(), 10)
		if !slices.Contains(create, wantDeadline) || slices.Contains(workload.Environment, wantDeadline) {
			t.Fatal("original deadline absent from container or leaked into outer env")
		}
		for _, flag := range []string{"--cpu-period", "--cpu-quota"} {
			i := slices.Index(create, flag)
			if i < 0 || i+1 >= len(create) || create[i+1] != "100000" {
				t.Fatalf("missing finite CPU control %s", flag)
			}
		}
		socketMount := bindMount(socketPath, b.SocketDestination, true)
		if !slices.Contains(create, socketMount) {
			t.Fatal("original exact socket mount absent")
		}
		for _, a := range create {
			if strings.HasPrefix(a, "type=bind,src="+filepath.Dir(socketPath)+",") {
				t.Fatal("private socket parent mounted")
			}
		}
	})
}

func TestNativePrepareRejectsInspectionDriftAndReleasesExactOwnedObjects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*containerResponse)
	}{
		{"cpu-quota", func(r *containerResponse) { r.HostConfig.CPUQuota++ }},
		{"cpu-period", func(r *containerResponse) { r.HostConfig.CPUPeriod++ }},
		{"non-root-mapped-user", func(r *containerResponse) { r.Config.User = "65532:65532" }},
		{"wrong-profile-label", func(r *containerResponse) { r.Config.Labels[profileLabel] = profileName }},
		{"renewed-deadline", func(r *containerResponse) {
			r.Config.Env[len(r.Config.Env)-1] = "SESSIONLESS_NATIVE_DEADLINE_UNIX_NS=1"
		}},
		{"extra-mount", func(r *containerResponse) { appendInspectionMount(r, "/unowned", "/unowned", false) }},
		{"writable-socket", func(r *containerResponse) { r.Mounts[len(r.Mounts)-1].RW = true }},
		{"substituted-socket", func(r *containerResponse) { r.Mounts[len(r.Mounts)-1].Source += "-other" }},
		{"network-bypass", func(r *containerResponse) { r.HostConfig.NetworkMode = "host" }},
		{"extra-credential-env", func(r *containerResponse) { r.Config.Env = append(r.Config.Env, "FAKE_SECRET=forbidden") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := nativeFixture(t)
				l, err := NewNativeLauncher(t.Context(), f.config)
				if err != nil {
					t.Fatal(err)
				}
				b := nativeBundle(t, f)
				driver := &nativePrepareOnly{launcher: l}
				var stage string
				driver.before = func(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV2) {
					stage = filepath.Join(e.Workload.WriteRoots[0], ".sessionless-auth-stage")
					r := nativeInspection(t, f, ctx, e)
					tc.mutate(&r)
					f.runner.containerInspect = r
				}
				runNativePreparation(t, f, b, driver)
				if !errors.Is(driver.prepareErr, ErrUnsupported) {
					t.Fatalf("inspection drift accepted: %v", driver.prepareErr)
				}
				if _, err := os.Stat(stage); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("stage survived rejected prepare: %v", err)
				}
				calls := f.runner.snapshot()
				last := dockerCommand(calls[len(calls)-1].arguments)
				if len(last) != 5 || !slices.Equal(last[:4], []string{"container", "rm", "--force", "--volumes"}) || !strings.HasPrefix(last[4], "sessionless-aw-") {
					t.Fatalf("rejected create not removed by owned name: %v", last)
				}
			})
		})
	}
}

func TestNativeReconcileCannotSelectV1Containers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := nativeFixture(t)
		l, err := NewNativeLauncher(t.Context(), f.config)
		if err != nil {
			t.Fatal(err)
		}
		f.runner.listIDs = []string{testContainerID}
		f.runner.containerInspect = prepareInspect(f)
		f.runner.containerInspect.Config.Labels[profileLabel] = profileName
		if _, err := l.Reconcile(t.Context()); err == nil {
			t.Fatal("V2 reconcile accepted V1-owned object")
		}
		for _, call := range f.runner.snapshot() {
			a := dockerCommand(call.arguments)
			if len(a) > 1 && a[0] == "container" && a[1] == "rm" {
				t.Fatal("V2 reconcile removed V1 object")
			}
			if len(a) > 1 && a[0] == "container" && a[1] == "ls" && !slices.Contains(a, "label="+profileLabel+"="+nativeProfileName) {
				t.Fatal("V2 reconciliation used V1 selector")
			}
		}
	})
}

func TestNativeReconcileRemovesOnlyItsExactProfile(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := nativeFixture(t)
		l, err := NewNativeLauncher(t.Context(), f.config)
		if err != nil {
			t.Fatal(err)
		}
		f.runner.listIDs = []string{testContainerID}
		f.runner.containerInspect = prepareInspect(f)
		f.runner.containerInspect.Config.Labels[profileLabel] = nativeProfileName
		count, err := l.Reconcile(t.Context())
		if err != nil || count != 1 {
			t.Fatalf("native reconciliation count=%d err=%v", count, err)
		}
		calls := f.runner.snapshot()
		last := dockerCommand(calls[len(calls)-1].arguments)
		if !slices.Equal(last, []string{"container", "rm", "--force", "--volumes", testContainerID}) {
			t.Fatalf("native reconcile wrong object: %v", last)
		}
	})
}

func TestNativePrepareRejectsAlteredOriginalBindingBeforeCreate(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*attachedworkerdaemon.LaunchEnvelopeV2)
	}{
		{"foreign-scope", func(e *attachedworkerdaemon.LaunchEnvelopeV2) { e.Native.ScopeDigest[0] ^= 1 }},
		{"foreign-policy", func(e *attachedworkerdaemon.LaunchEnvelopeV2) { e.Native.PolicyDigest[0] ^= 1 }},
		{"changed-sealed-provider-hosts", func(e *attachedworkerdaemon.LaunchEnvelopeV2) { e.Native.ProviderHosts = []string{"other.example"} }},
		{"unowned-socket-placeholder", func(e *attachedworkerdaemon.LaunchEnvelopeV2) { e.Socket = new(attachedworkerdaemon.NativeSocketV2) }},
		{"unsealed-bootstrap-argv", func(e *attachedworkerdaemon.LaunchEnvelopeV2) {
			e.Native.BootstrapArguments = append(e.Native.BootstrapArguments, "extra")
		}},
		{"socket-over-root", func(e *attachedworkerdaemon.LaunchEnvelopeV2) { e.Native.SocketDestination = e.Workload.WriteRoots[0] }},
		{"caller-control-env", func(e *attachedworkerdaemon.LaunchEnvelopeV2) {
			e.Workload.Environment = append(e.Workload.Environment, "SESSIONLESS_NATIVE_DEADLINE_UNIX_NS=1")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := nativeFixture(t)
				l, err := NewNativeLauncher(t.Context(), f.config)
				if err != nil {
					t.Fatal(err)
				}
				b := nativeBundle(t, f)
				driver := &nativePrepareOnly{launcher: l, mutate: tc.mutate}
				runNativePreparation(t, f, b, driver)
				if !errors.Is(driver.prepareErr, ErrConfig) {
					t.Fatalf("altered original binding accepted: %v", driver.prepareErr)
				}
				for _, call := range f.runner.snapshot() {
					a := dockerCommand(call.arguments)
					if len(a) > 1 && a[0] == "container" && a[1] == "create" {
						t.Fatal("altered binding reached container create")
					}
				}
			})
		})
	}
}

func TestNativePrepareRechecksPinnedHelperBeforeCreate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := nativeFixture(t)
		l, err := NewNativeLauncher(t.Context(), f.config)
		if err != nil {
			t.Fatal(err)
		}
		b := nativeBundle(t, f)
		driver := &nativePrepareOnly{launcher: l, before: func(_ context.Context, e attachedworkerdaemon.LaunchEnvelopeV2) {
			if err := os.WriteFile(e.Native.Relay.Executable, []byte("replacement owned helper"), 0700); err != nil {
				t.Fatal(err)
			}
		}}
		runNativePreparation(t, f, b, driver)
		if !errors.Is(driver.prepareErr, ErrConfig) {
			t.Fatalf("changed helper artifact accepted: %v", driver.prepareErr)
		}
		for _, call := range f.runner.snapshot() {
			a := dockerCommand(call.arguments)
			if len(a) > 1 && a[0] == "container" && a[1] == "create" {
				t.Fatal("changed artifact reached container create")
			}
		}
	})
}

func TestV1CreateFlagsAndRootfulUsernsBehaviorRemainUnchanged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFixture(t, "linux", true)
		f.runner.info.SecurityOptions = []string{"name=seccomp,profile=builtin", "name=userns"}
		l, err := NewLauncher(t.Context(), f.config)
		if err != nil {
			t.Fatal(err)
		}
		validated, err := validateLaunchSpec(f.spec)
		if err != nil {
			t.Fatal(err)
		}
		stage := &credentialStage{path: filepath.Join(f.attempt, ".sessionless-auth-stage"), target: f.credential, maxBytes: 1 << 20}
		got := l.createArguments("owned-name", f.spec, validated, stage)
		want := []string{"container", "create", "--pull", "never", "--name", "owned-name", "--network", "none", "--read-only", "--cap-drop", "ALL", "--no-healthcheck", "--ipc", "private", "--cgroupns", "private", "--security-opt", "no-new-privileges=true", "--pids-limit", "32", "--memory", "134217728", "--memory-swap", "134217728", "--shm-size", "1048576", "--log-driver", "none", "--restart", "no", "--stop-signal", "SIGTERM", "--stop-timeout", "5", "--init", "--user", "65532:65532", "--workdir", f.spec.Directory, "--label", profileLabel + "=" + profileName, "--label", installationLabel + "=" + f.config.InstallationID, "--label", engineLabel + "=" + f.config.EngineID, "--ulimit", "fsize=1048576:1048576", "--tmpfs", f.attempt + ":rw,nosuid,nodev,noexec,size=31457280,mode=0700,uid=65532,gid=65532", "--mount", "type=bind,src=" + f.spec.Executable + ",dst=" + f.spec.Executable + ",bind-propagation=rprivate,readonly", "--mount", "type=bind,src=" + filepath.Dir(f.credential) + ",dst=" + filepath.Dir(f.credential) + ",bind-propagation=rprivate,readonly", "--mount", "type=bind,src=" + stage.path + ",dst=" + f.credential + ",bind-propagation=rprivate", "--env", "HOME", "--env", "TMPDIR", "--entrypoint", f.spec.Executable, f.image, "exec", "--json"}
		if !slices.Equal(got, want) {
			t.Fatalf("V1 exact flags changed\ngot %q\nwant %q", got, want)
		}
		if !l.Profile().NetworkDenied || l.Profile().Name != profileName {
			t.Fatal("V1 profile changed")
		}
	})
}

func TestNativeUnknownCleanupRetainsOriginalOwnerAndStage(t *testing.T) {
	cases := []struct {
		name                                                     string
		createUnknown, inspectDrift, stateUnknown, removeUnknown bool
		partial                                                  bool
	}{
		{"partial-create-unknown", true, false, false, true, true},
		{"partial-inspect-unknown", false, true, false, true, true},
		{"prepared-state-unknown", false, false, true, false, false},
		{"prepared-remove-unknown", false, false, false, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := nativeFixture(t)
				fault := &nativeFaultRunner{base: f.runner, createUnknown: tc.createUnknown, stateUnknown: tc.stateUnknown, removeUnknown: tc.removeUnknown}
				f.config.runner = fault
				l, err := NewNativeLauncher(t.Context(), f.config)
				if err != nil {
					t.Fatal(err)
				}
				bundle := nativeBundle(t, f)
				driver := &nativePrepareOnly{launcher: l, expectedReleaseError: ErrBoundary}
				var root string
				driver.before = func(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV2) {
					root = e.Workload.WriteRoots[0]
					fault.stage = filepath.Join(root, ".sessionless-auth-stage")
					r := nativeInspection(t, f, ctx, e)
					if tc.inspectDrift {
						r.HostConfig.CPUQuota++
					}
					f.runner.containerInspect = r
				}
				runNativePreparation(t, f, bundle, driver)
				// These are synthetic engine objects only. Register explicit removal of
				// the retained, exact fixture root after observing production custody.
				if !strings.HasPrefix(filepath.Base(root), "attempt-") || !strings.HasPrefix(filepath.Base(filepath.Dir(root)), "n-") {
					t.Fatal("unexpected retained fixture root")
				}
				t.Cleanup(func() {
					if err := os.RemoveAll(root); err != nil {
						t.Errorf("retained synthetic root cleanup: %v", err)
					}
				})
				if driver.result.CleanupSucceeded || driver.result.BoundaryReleased || driver.boundary == nil || driver.runErr == nil {
					t.Fatalf("unknown cleanup certified success: %+v err=%v", driver.result, driver.runErr)
				}
				if fault.stageError != nil {
					t.Fatal(fault.stageError)
				}
				if tc.partial && driver.prepareErr == nil {
					t.Fatal("partial prepare did not fail")
				}
				if !tc.partial && driver.prepareErr != nil {
					t.Fatalf("fully prepared fixture did not prepare: %v", driver.prepareErr)
				}
				if tc.partial && driver.boundary.Command() != nil {
					t.Fatal("partial owner is startable")
				}
				stage, err := os.ReadFile(fault.stage)
				if err != nil || string(stage) != `{"synthetic":"must-not-handback"}` {
					t.Fatalf("unknown cleanup discarded original stage: %q %v", stage, err)
				}
				original, err := os.ReadFile(f.credential)
				if err != nil || string(original) != `{"token":"old"}` {
					t.Fatalf("unknown cleanup committed stage: %q %v", original, err)
				}
				calls := f.runner.snapshot()
				before := len(calls)
				cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
				err = driver.boundary.Release(cleanup)
				cancel()
				if !errors.Is(err, ErrBoundary) || len(f.runner.snapshot()) != before {
					t.Fatalf("unknown release was not sticky/idempotent: %v", err)
				}
				var name string
				for _, call := range calls {
					a := dockerCommand(call.arguments)
					if len(a) > 1 && a[0] == "container" && a[1] == "create" {
						i := slices.Index(a, "--name")
						if i >= 0 && i+1 < len(a) {
							name = a[i+1]
						}
					}
				}
				if name == "" {
					t.Fatal("missing original generated name")
				}
				want := testContainerID
				if tc.partial {
					want = name
				}
				for _, call := range calls {
					a := dockerCommand(call.arguments)
					if len(a) < 3 || a[0] != "container" {
						continue
					}
					isLifecycle := a[1] == "stop" || a[1] == "kill" || a[1] == "rm" || (len(a) > 3 && a[1] == "inspect" && a[3] == "{{json .State}}")
					if isLifecycle && a[len(a)-1] != want {
						t.Fatalf("cleanup used untrusted output identity: %v want=%s", a, want)
					}
				}
			})
		})
	}
}

func TestNativePartialPrepareNeverHandsBackStage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := nativeFixture(t)
		fault := &nativeFaultRunner{base: f.runner, createUnknown: true}
		f.config.runner = fault
		l, err := NewNativeLauncher(t.Context(), f.config)
		if err != nil {
			t.Fatal(err)
		}
		bundle := nativeBundle(t, f)
		driver := &nativePrepareOnly{launcher: l}
		driver.before = func(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV2) {
			fault.stage = filepath.Join(e.Workload.WriteRoots[0], ".sessionless-auth-stage")
			f.runner.containerInspect = nativeInspection(t, f, ctx, e)
		}
		runNativePreparation(t, f, bundle, driver)
		if !errors.Is(driver.prepareErr, ErrBoundary) || !driver.result.BoundaryReleased {
			t.Fatalf("partial owner did not clean exact object: %+v %v", driver.result, driver.prepareErr)
		}
		if fault.stageError != nil {
			t.Fatal(fault.stageError)
		}
		raw, err := os.ReadFile(f.credential)
		if err != nil || string(raw) != `{"token":"old"}` {
			t.Fatalf("partial prepare handed back stage despite never starting: %q %v", raw, err)
		}
		if _, err := os.Stat(fault.stage); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("known-removed partial owner left stage: %v", err)
		}
	})
}

// Alive=false is deliberately not a successful Stop receipt: an owned close
// can have an unknown outcome after goroutines have joined. No timeout is used.
type unknownClosedConduit struct {
	err      error
	stops    int
	liveStop bool
}

func (*unknownClosedConduit) Start(_ context.Context, s *attachedworkerdaemon.NativeSocketV2) error {
	_, err := s.Path()
	return err
}
func (c *unknownClosedConduit) Stop(ctx context.Context) error {
	c.stops++
	c.liveStop = ctx.Err() == nil
	return c.err
}
func (*unknownClosedConduit) Alive() bool { return false }

func TestNativeUnknownCompanionCloseBlocksCredentialHandback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := nativeFixture(t)
		fault := &nativeFaultRunner{base: f.runner}
		f.config.runner = fault
		l, err := NewNativeLauncher(t.Context(), f.config)
		if err != nil {
			t.Fatal(err)
		}
		bundle := nativeBundle(t, f)
		closeErr := errors.New("unknown synthetic companion Close outcome")
		conduit := &unknownClosedConduit{err: closeErr}
		driver := &nativePrepareOnly{launcher: l, conduit: conduit, expectedReleaseError: closeErr}
		var root string
		driver.before = func(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV2) {
			root = e.Workload.WriteRoots[0]
			fault.stage = filepath.Join(root, ".sessionless-auth-stage")
			f.runner.containerInspect = nativeInspection(t, f, ctx, e)
		}
		runNativePreparation(t, f, bundle, driver)
		if !strings.HasPrefix(filepath.Base(root), "attempt-") || !strings.HasPrefix(filepath.Base(filepath.Dir(root)), "n-") {
			t.Fatal("unexpected retained synthetic root")
		}
		// There is no real engine/container/companion to stop. Retire only this
		// exact synthetic root after observing retained production ownership.
		t.Cleanup(func() {
			if err := os.RemoveAll(root); err != nil {
				t.Errorf("retained synthetic root cleanup: %v", err)
			}
		})
		if driver.prepareErr != nil || fault.stageError != nil {
			t.Fatalf("genuine OCI fake preparation failed: %v %v", driver.prepareErr, fault.stageError)
		}
		if conduit.Alive() || conduit.stops == 0 || !conduit.liveStop {
			t.Fatalf("fixture did not produce immediate joined-but-unknown Stop under live budget: %+v", conduit)
		}
		if driver.result.CleanupSucceeded || driver.result.BoundaryReleased || driver.runErr == nil {
			t.Fatalf("unknown companion close certified success: %+v %v", driver.result, driver.runErr)
		}
		stage, err := os.ReadFile(fault.stage)
		if err != nil || string(stage) != `{"synthetic":"must-not-handback"}` {
			t.Errorf("unknown companion close lost stage custody: %q %v", stage, err)
		}
		original, err := os.ReadFile(f.credential)
		if err != nil || string(original) != `{"token":"old"}` {
			t.Errorf("unknown companion close handed back credentials: %q %v", original, err)
		}
		for _, call := range f.runner.snapshot() {
			a := dockerCommand(call.arguments)
			if len(a) > 1 && a[0] == "container" && a[1] == "rm" {
				t.Error("OCI Release ran after unknown companion Stop")
			}
		}
	})
}
