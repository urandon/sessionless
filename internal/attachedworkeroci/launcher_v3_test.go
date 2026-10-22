//go:build darwin || linux

package attachedworkeroci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
	"gitcode.com/urandon/sessionless/internal/nativeharnessinput"
)

var nativeV3ImageNames = [7][]string{{}, {"image-01.png"}, {"image-01.jpg"}, {"image-01.png", "image-02.png"}, {"image-01.png", "image-02.jpg"}, {"image-01.jpg", "image-02.png"}, {"image-01.jpg", "image-02.jpg"}}

func nativeBundleV3Fixture(t *testing.T, f fixture) attachedworkerdaemon.NativeLaunchV3 {
	t.Helper()
	helperRoot := shortOwnedRoot(t, "v3h-")
	path := filepath.Join(helperRoot, "helper")
	if err := os.WriteFile(path, []byte("inert V3 helper, never executed"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(path); err != nil {
			t.Errorf("owned V3 helper cleanup: %v", err)
		}
	})
	digest, err := attachedworkerdaemon.DigestExecutable(path)
	if err != nil {
		t.Fatal(err)
	}
	helper := attachedworkerdaemon.NativeArtifactV2{Executable: path, Digest: digest}
	file := func(body string) nativeharnessinput.FileV3 {
		sum := sha256.Sum256([]byte(body))
		return nativeharnessinput.FileV3{Size: uint64(len(body)), SHA256: hex.EncodeToString(sum[:])}
	}
	v := attachedworkerdaemon.NativeLaunchV3{Version: 3, Bootstrap: helper, Relay: helper, Companion: helper, Workload: attachedworkerdaemon.NativeArtifactV2{Executable: f.spec.Executable, Digest: f.spec.ExecutableDigest},
		SocketDestination: "/conduit/native.sock", ProviderHosts: []string{"chatgpt.com"}, ScopeDigest: f.spec.ExecutableDigest,
		Limits: attachedworkerdaemon.NativeConduitLimitsV2{Connections: 2, InboundBytes: 1024, OutboundBytes: 1024, CompanionMemoryBytes: 64 << 20, CompanionGoroutines: 16, Active: 30 * time.Second, Cleanup: time.Second}, Catalog: file("catalog"), Schema: file("schema")}
	for i, names := range nativeV3ImageNames {
		args := []string{"exec", "--json", "--ephemeral"}
		for _, name := range names {
			args = append(args, "--image", "/sessionless/input/"+name)
		}
		v.NativeArguments[i] = append(args, "-")
	}
	v.BootstrapArguments, v.RelayArguments, err = nativeharnessbootstrap.InstallationArgumentsV3(v)
	if err != nil {
		t.Fatal(err)
	}
	v.PolicyDigest, err = v.InstallPolicyDigestV3()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func nativeExecutionV3Fixture(t *testing.T, b attachedworkerdaemon.NativeLaunchV3, variant uint32, directory string) (*attachedworkerdaemon.NativeExecutionV3, []attachedworkerdaemon.NativeInputFileV3) {
	t.Helper()
	if directory == "" {
		directory = canonicalTempDir(t)
	}
	d := nativeharnessinput.NativeExecutionDescriptorV3{Version: 3, ScopeManifestDigest: hex.EncodeToString(b.ScopeDigest[:]), InstallDigest: hex.EncodeToString(b.PolicyDigest[:]), Variant: variant, Images: []nativeharnessinput.ImageV3{}, Catalog: b.Catalog, Schema: b.Schema}
	names := append([]string{"models.json", "output.schema.json"}, nativeV3ImageNames[variant]...)
	var files []attachedworkerdaemon.NativeInputFileV3
	for i, name := range names {
		body := "catalog"
		if i == 1 {
			body = "schema"
		}
		if i >= 2 {
			body = "image" + strconv.Itoa(i-1)
			sum := sha256.Sum256([]byte(body))
			d.Images = append(d.Images, nativeharnessinput.ImageV3{Ordinal: uint32(i - 1), Size: uint64(len(body)), SHA256: hex.EncodeToString(sum[:])})
		}
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, []byte(body), 0400); err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				t.Errorf("V3 original input close: %v", err)
			}
		})
		files = append(files, attachedworkerdaemon.NativeInputFileV3{Source: path, File: file})
	}
	v, err := attachedworkerdaemon.NewNativeExecutionV3(t.Context(), b, d, files)
	if err != nil {
		t.Fatal(err)
	}
	return v, files
}

// Like nativePrepareOnly, this executes the actual supervisor and create/inspect
// code but always returns a preparation error before any Command.Start call.
type nativePrepareOnlyV3 struct {
	t                    *testing.T
	launcher             *NativeLauncherV3
	before               func(context.Context, attachedworkerdaemon.LaunchEnvelopeV3)
	mutate               func(*attachedworkerdaemon.LaunchEnvelopeV3)
	after                func(attachedworkerdaemon.IsolationBoundaryV3)
	boundary             attachedworkerdaemon.IsolationBoundaryV3
	prepareErr, runErr   error
	result               attachedworkerdaemon.AttemptResult
	expectedReleaseError error
}

func (f *nativePrepareOnlyV3) ProfileV3() attachedworkerdaemon.IsolationProfileV3 {
	return f.launcher.ProfileV3()
}
func (f *nativePrepareOnlyV3) PrepareV3(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV3) (attachedworkerdaemon.IsolationBoundaryV3, error) {
	if f.before != nil {
		f.before(ctx, e)
	}
	if f.mutate != nil {
		f.mutate(&e)
	}
	b, err := f.launcher.PrepareV3(ctx, e)
	f.boundary, f.prepareErr = b, err
	if b != nil {
		f.t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := b.Release(cleanup)
			if (f.expectedReleaseError == nil && err != nil) || (f.expectedReleaseError != nil && !errors.Is(err, f.expectedReleaseError)) {
				f.t.Errorf("V3 exact boundary cleanup: %v", err)
			}
		})
	}
	if b != nil && err == nil && f.after != nil {
		f.after(b)
	}
	return b, errors.Join(err, errors.New("owned V3 fixture stops before process Start"))
}

func runNativePreparationV3(t *testing.T, f fixture, b attachedworkerdaemon.NativeLaunchV3, execution *attachedworkerdaemon.NativeExecutionV3, driver *nativePrepareOnlyV3) *inertNativeConduit {
	t.Helper()
	driver.t = t
	root := shortOwnedRoot(t, "v3n-")
	supervisor, err := attachedworkerdaemon.NewSupervisorV3(attachedworkerdaemon.SupervisorConfigV3{Base: attachedworkerdaemon.SupervisorConfig{ScratchRoot: root, Timeout: 30 * time.Second, TerminationGrace: time.Second, AllowedReadRoots: []string{filepath.Dir(filepath.Dir(f.credential))}, AllowedEnvironmentNames: []string{"CODEX_HOME"}}, Launcher: driver})
	if err != nil {
		t.Fatal(err)
	}
	d := execution.DescriptorV3()
	spec := attachedworkerdaemon.AttemptSpec{Executable: b.Workload.Executable, ExecutableDigest: b.Workload.Digest, Arguments: slices.Clone(b.NativeArguments[d.Variant]), Environment: []attachedworkerdaemon.EnvironmentVariable{{Name: "CODEX_HOME", Value: filepath.Dir(f.credential)}}}
	spec, err = attachedworkerdaemon.BindPreparedCredential(spec, filepath.Dir(f.credential), f.credential)
	if err != nil {
		t.Fatal(err)
	}
	conduit := &inertNativeConduit{}
	driver.result, driver.runErr = supervisor.RunNativeV3(t.Context(), spec, b, execution, conduit)
	if driver.runErr == nil {
		t.Fatal("V3 prepare-only fixture unexpectedly continued")
	}
	return conduit
}

func nativeInspectionV3(t *testing.T, f fixture, ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV3, files []attachedworkerdaemon.NativeInputFileV3) containerResponse {
	t.Helper()
	shadow := f
	shadow.spec = e.Workload
	shadow.attempt = e.Workload.WriteRoots[0]
	r := prepareInspect(shadow)
	r.Config.User = "0:0"
	r.Config.Entrypoint = []string{e.Native.Bootstrap.Executable}
	r.Config.Cmd = e.Execution.ArgumentsV3()
	deadline, _ := ctx.Deadline()
	r.Config.Env = append(slices.Clone(e.Workload.Environment), "SESSIONLESS_NATIVE_DEADLINE_UNIX_NS="+strconv.FormatInt(deadline.UnixNano(), 10))
	r.Config.Labels[profileLabel] = "sessionless.oci.docker.native-input.v3"
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
	// Expected destinations are literal independent fixture data, not the
	// production mapping/mount builder or the observed create command.
	destinations := []string{"/sessionless/profile/models.json", "/sessionless/profile/output.schema.json"}
	for _, name := range nativeV3ImageNames[e.Execution.DescriptorV3().Variant] {
		destinations = append(destinations, "/sessionless/input/"+name)
	}
	if len(destinations) != len(files) {
		t.Fatalf("fixture file count %d want%d", len(files), len(destinations))
	}
	for i, path := range destinations {
		appendInspectionMount(&r, files[i].Source, path, false)
	}
	return r
}

func nativeV3CreateCalls(r *fakeRunner) [][]string {
	var result [][]string
	for _, call := range r.snapshot() {
		args := dockerCommand(call.arguments)
		if len(args) > 1 && args[0] == "container" && args[1] == "create" {
			result = append(result, args)
		}
	}
	return result
}

func TestNativeV3PrepareSevenFixedVectorsAndOriginalHandback(t *testing.T) {
	for variant := uint32(0); variant < 7; variant++ {
		t.Run(strconv.Itoa(int(variant)), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := nativeFixture(t)
				l, err := NewNativeLauncherV3(t.Context(), f.config)
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := any(l).(attachedworkerdaemon.IsolationLauncherV2); ok {
					t.Fatal("V3 launcher implements V2 interface")
				}
				if _, ok := any(l).(attachedworkerdaemon.IsolationLauncher); ok {
					t.Fatal("V3 launcher implements V1 interface")
				}
				if l.ProfileV3().Validate() != nil || l.ProfileV3().Name != "sessionless.oci.docker.native-input.v3" {
					t.Fatalf("wrong V3 profile %+v", l.ProfileV3())
				}
				bundle := nativeBundleV3Fixture(t, f)
				execution, files := nativeExecutionV3Fixture(t, bundle, variant, "")
				driver := &nativePrepareOnlyV3{launcher: l}
				var envelope attachedworkerdaemon.LaunchEnvelopeV3
				var socket string
				var deadline time.Time
				driver.before = func(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV3) {
					envelope = e
					socket, _ = e.Socket.Path()
					deadline, _ = ctx.Deadline()
					f.runner.containerInspect = nativeInspectionV3(t, f, ctx, e, files)
				}
				driver.after = func(b attachedworkerdaemon.IsolationBoundaryV3) {
					command := b.Command()
					if command == nil || command.Process != nil || !slices.Equal(command.Env, envelope.Workload.Environment) {
						t.Fatal("V3 command was started or outer environment changed")
					}
					a := b.AttestedWorkloadV3()
					if a.Execution != execution || a.ExecutionDigest != execution.DigestV3() || a.Socket != envelope.Socket || !reflect.DeepEqual(a.Bundle, bundle) || a.Native.Executable != bundle.Workload.Executable || !slices.Equal(a.Native.Arguments, bundle.NativeArguments[variant]) {
						t.Fatal("V3 original execution/command attestation changed")
					}
					stage := filepath.Join(envelope.Workload.WriteRoots[0], ".sessionless-auth-stage")
					info, err := os.Lstat(stage)
					if err != nil || info.Mode().Perm() != 0600 {
						t.Fatalf("original auth stage info=%v error=%v", info, err)
					}
					if err := os.WriteFile(stage, []byte(`{"synthetic":"v3-updated"}`), 0600); err != nil {
						t.Fatal(err)
					}
				}
				conduit := runNativePreparationV3(t, f, bundle, execution, driver)
				if driver.prepareErr != nil || !driver.result.CleanupSucceeded || !driver.result.BoundaryReleased || conduit.stops == 0 {
					t.Fatalf("V3 prepare/cleanup %+v prepare=%v run=%v stops=%d", driver.result, driver.prepareErr, driver.runErr, conduit.stops)
				}
				raw, err := os.ReadFile(f.credential)
				if err != nil || string(raw) != `{"synthetic":"v3-updated"}` {
					t.Fatalf("original fenced auth handback %q %v", raw, err)
				}
				if _, err := os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("original V3 socket residue: %v", err)
				}
				creates := nativeV3CreateCalls(f.runner)
				if len(creates) != 1 {
					t.Fatalf("create count%d want1", len(creates))
				}
				create := creates[0]
				imageIndex := slices.Index(create, f.image)
				if imageIndex < 0 || !slices.Equal(create[imageIndex+1:], execution.ArgumentsV3()) {
					t.Fatal("actual V3 bootstrap/carrier differs from original readset")
				}
				if !slices.Contains(create, "SESSIONLESS_NATIVE_DEADLINE_UNIX_NS="+strconv.FormatInt(deadline.UnixNano(), 10)) || !slices.Contains(create, profileLabel+"=sessionless.oci.docker.native-input.v3") {
					t.Fatal("V3 deadline/profile missing from actual create")
				}
				var inputMounts int
				for _, arg := range create {
					if strings.HasPrefix(arg, "type=bind,") && strings.Contains(arg, ",dst=/sessionless/") {
						inputMounts++
						if !strings.HasSuffix(arg, ",readonly") {
							t.Errorf("writable input mapping %q", arg)
						}
					}
					for _, file := range files {
						if strings.HasPrefix(arg, "type=bind,src="+filepath.Dir(file.Source)+",") {
							t.Fatal("broad original input directory mounted")
						}
					}
				}
				if inputMounts != len(files) {
					t.Fatalf("input mounts%d want%d", inputMounts, len(files))
				}
				literalMount := func(source, destination string, readOnly bool) string {
					value := "type=bind,src=" + source + ",dst=" + destination + ",bind-propagation=rprivate"
					if readOnly {
						value += ",readonly"
					}
					return value
				}
				wantMounts := []string{literalMount(bundle.Bootstrap.Executable, bundle.Bootstrap.Executable, true), literalMount(bundle.Workload.Executable, bundle.Workload.Executable, true), literalMount(socket, bundle.SocketDestination, true)}
				destinations := []string{"/sessionless/profile/models.json", "/sessionless/profile/output.schema.json"}
				for _, name := range nativeV3ImageNames[variant] {
					destinations = append(destinations, "/sessionless/input/"+name)
				}
				for i, destination := range destinations {
					wantMounts = append(wantMounts, literalMount(files[i].Source, destination, true))
				}
				wantMounts = append(wantMounts, literalMount(filepath.Dir(f.credential), filepath.Dir(f.credential), true), literalMount(filepath.Join(envelope.Workload.WriteRoots[0], ".sessionless-auth-stage"), f.credential, false))
				var gotMounts []string
				for i, arg := range create {
					if arg == "--mount" && i+1 < len(create) {
						gotMounts = append(gotMounts, create[i+1])
					}
				}
				if !slices.Equal(gotMounts, wantMounts) {
					t.Fatalf("actual V3 exact mappings\ngot%q\nwant%q", gotMounts, wantMounts)
				}
			})
		})
	}
}

func TestNativeV3PrepareRejectsActualInspectionDrift(t *testing.T) {
	cases := []struct {
		name   string
		change func(*containerResponse)
	}{
		{"wrong-source", func(r *containerResponse) { r.Mounts[len(r.Mounts)-1].Source += "-foreign" }},
		{"writable-image", func(r *containerResponse) { r.Mounts[len(r.Mounts)-1].RW = true }},
		{"missing-image", func(r *containerResponse) { r.Mounts = r.Mounts[:len(r.Mounts)-1] }},
		{"extra-image", func(r *containerResponse) {
			appendInspectionMount(r, "/foreign/image", "/sessionless/input/image-03.png", false)
		}},
		{"wrong-suffix", func(r *containerResponse) { r.Mounts[len(r.Mounts)-1].Destination = "/sessionless/input/image-02.png" }},
		{"swapped-image-sources", func(r *containerResponse) {
			n := len(r.Mounts)
			r.Mounts[n-1].Source, r.Mounts[n-2].Source = r.Mounts[n-2].Source, r.Mounts[n-1].Source
		}},
		{"v2-profile", func(r *containerResponse) { r.Config.Labels[profileLabel] = nativeProfileName }},
		{"wrong-carrier", func(r *containerResponse) { r.Config.Cmd[len(r.Config.Cmd)-1] += " " }},
		{"missing-carrier", func(r *containerResponse) { r.Config.Cmd = r.Config.Cmd[:len(r.Config.Cmd)-2] }},
		{"extra-carrier", func(r *containerResponse) {
			r.Config.Cmd = append(r.Config.Cmd, "--execution-descriptor-json", r.Config.Cmd[len(r.Config.Cmd)-1])
		}},
		{"wrong-native-variant", func(r *containerResponse) {
			i := slices.Index(r.Config.Cmd, "/sessionless/input/image-01.png")
			if i >= 0 {
				r.Config.Cmd[i] = "/sessionless/input/image-01.jpg"
			}
		}},
		{"writable-catalog", func(r *containerResponse) {
			for i := range r.Mounts {
				if r.Mounts[i].Destination == "/sessionless/profile/models.json" {
					r.Mounts[i].RW = true
				}
			}
		}},
		{"writable-auth-root", func(r *containerResponse) {
			for i := range r.Mounts {
				if strings.HasSuffix(r.Mounts[i].Destination, "/credential") {
					r.Mounts[i].RW = true
				}
			}
		}},
		{"network-bypass", func(r *containerResponse) { r.HostConfig.NetworkMode = "host" }},
		{"helper-entrypoint", func(r *containerResponse) { r.Config.Entrypoint = []string{"/foreign/helper"} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := nativeFixture(t)
				l, err := NewNativeLauncherV3(t.Context(), f.config)
				if err != nil {
					t.Fatal(err)
				}
				bundle := nativeBundleV3Fixture(t, f)
				execution, files := nativeExecutionV3Fixture(t, bundle, 4, "")
				driver := &nativePrepareOnlyV3{launcher: l}
				var stage string
				driver.before = func(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV3) {
					stage = filepath.Join(e.Workload.WriteRoots[0], ".sessionless-auth-stage")
					r := nativeInspectionV3(t, f, ctx, e, files)
					tc.change(&r)
					f.runner.containerInspect = r
				}
				runNativePreparationV3(t, f, bundle, execution, driver)
				if !errors.Is(driver.prepareErr, ErrUnsupported) || driver.boundary == nil || driver.boundary.Command() != nil || !driver.result.BoundaryReleased {
					t.Fatalf("inspection drift not denied with retained unstartable owner: prepare=%v result=%+v", driver.prepareErr, driver.result)
				}
				if len(nativeV3CreateCalls(f.runner)) != 1 {
					t.Fatal("inspection case did not actually create/inspect fake object")
				}
				if _, err := os.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed inspection stage residue: %v", err)
				}
				raw, err := os.ReadFile(f.credential)
				if err != nil || string(raw) != `{"token":"old"}` {
					t.Fatalf("failed inspection changed original auth: %q %v", raw, err)
				}
				calls := f.runner.snapshot()
				last := dockerCommand(calls[len(calls)-1].arguments)
				if len(last) != 5 || !slices.Equal(last[:4], []string{"container", "rm", "--force", "--volumes"}) || !strings.HasPrefix(last[4], "sessionless-aw-") {
					t.Fatalf("wrong owned failed-inspection cleanup %q", last)
				}
			})
		})
	}
}

func TestNativeV3PrepareRejectsOriginalAssociationDriftBeforeCreate(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *attachedworkerdaemon.LaunchEnvelopeV3, []attachedworkerdaemon.NativeInputFileV3)
	}{
		{"copied-readset", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, _ []attachedworkerdaemon.NativeInputFileV3) {
			copied := *e.Execution
			e.Execution = &copied
		}},
		{"foreign-original-readset", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, _ []attachedworkerdaemon.NativeInputFileV3) {
			foreign, _ := nativeExecutionV3Fixture(t, e.Native, e.Execution.DescriptorV3().Variant, "")
			if foreign.DigestV3() == e.Execution.DigestV3() {
				t.Fatal("foreign fixture did not change original source association")
			}
			if foreign.ValidateV3(t.Context(), e.Native) != nil {
				t.Fatal("foreign regression requires another valid original readset, not an invalid placeholder")
			}
			e.Execution = foreign
		}},
		{"placeholder-socket", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, _ []attachedworkerdaemon.NativeInputFileV3) {
			e.Socket = new(attachedworkerdaemon.NativeSocketV2)
		}},
		{"foreign-scope", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, _ []attachedworkerdaemon.NativeInputFileV3) {
			e.Native.ScopeDigest[0] ^= 1
		}},
		{"foreign-policy", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, _ []attachedworkerdaemon.NativeInputFileV3) {
			e.Native.PolicyDigest[0] ^= 1
		}},
		{"profile-drift", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, _ []attachedworkerdaemon.NativeInputFileV3) {
			e.Native.ProviderHosts = []string{"other.example"}
		}},
		{"native-wrong-variant", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, _ []attachedworkerdaemon.NativeInputFileV3) {
			e.Workload.Arguments = slices.Clone(e.Native.NativeArguments[2])
		}},
		{"static-bootstrap-carrier", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, _ []attachedworkerdaemon.NativeInputFileV3) {
			v := e.Execution.DescriptorV3().Variant
			e.Native.BootstrapArguments[v] = append(e.Native.BootstrapArguments[v], "--execution-descriptor-json", "{}")
		}},
		{"broad-source-root", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, files []attachedworkerdaemon.NativeInputFileV3) {
			e.Workload.ReadRoots = append(e.Workload.ReadRoots, filepath.Dir(files[0].Source))
		}},
		{"inherited-control-env", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, _ []attachedworkerdaemon.NativeInputFileV3) {
			e.Workload.Environment = append(e.Workload.Environment, "SESSIONLESS_NATIVE_DEADLINE_UNIX_NS=1")
		}},
		{"source-inode-substitution", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, files []attachedworkerdaemon.NativeInputFileV3) {
			file := files[0]
			if err := os.Rename(file.Source, file.Source+"-original"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(file.Source, []byte("catalog"), 0400); err != nil {
				t.Fatal(err)
			}
		}},
		{"source-content-substitution", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, files []attachedworkerdaemon.NativeInputFileV3) {
			path := files[0].Source
			if err := os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("CATALOG"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0400); err != nil {
				t.Fatal(err)
			}
		}},
		{"source-writable", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, files []attachedworkerdaemon.NativeInputFileV3) {
			if err := os.Chmod(files[0].Source, 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"source-symlink", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, files []attachedworkerdaemon.NativeInputFileV3) {
			path := files[0].Source
			if err := os.Rename(path, path+"-original"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Base(path)+"-original", path); err != nil {
				t.Fatal(err)
			}
		}},
		{"closed-original-source", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, files []attachedworkerdaemon.NativeInputFileV3) {
			if err := files[0].File.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{"changed-helper-code", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, _ []attachedworkerdaemon.NativeInputFileV3) {
			if err := os.WriteFile(e.Native.Relay.Executable, []byte("owned replacement helper"), 0700); err != nil {
				t.Fatal(err)
			}
		}},
		{"changed-native-code", func(t *testing.T, e *attachedworkerdaemon.LaunchEnvelopeV3, _ []attachedworkerdaemon.NativeInputFileV3) {
			if err := os.WriteFile(e.Native.Workload.Executable, []byte("owned replacement native"), 0700); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := nativeFixture(t)
				l, err := NewNativeLauncherV3(t.Context(), f.config)
				if err != nil {
					t.Fatal(err)
				}
				bundle := nativeBundleV3Fixture(t, f)
				execution, files := nativeExecutionV3Fixture(t, bundle, 4, "")
				driver := &nativePrepareOnlyV3{launcher: l}
				driver.before = func(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV3) {
					f.runner.containerInspect = nativeInspectionV3(t, f, ctx, e, files)
				}
				driver.mutate = func(e *attachedworkerdaemon.LaunchEnvelopeV3) { tc.change(t, e, files) }
				runNativePreparationV3(t, f, bundle, execution, driver)
				if !errors.Is(driver.prepareErr, ErrConfig) {
					t.Fatalf("original association drift accepted: %v", driver.prepareErr)
				}
				if len(nativeV3CreateCalls(f.runner)) != 0 {
					t.Fatal("original association drift reached fake create")
				}
				if driver.boundary != nil {
					t.Fatal("precreate denial unexpectedly owns a created boundary")
				}
				raw, err := os.ReadFile(f.credential)
				if err != nil || string(raw) != `{"token":"old"}` {
					t.Fatalf("precreate denial changed original credential %q %v", raw, err)
				}
			})
		})
	}
}

func TestNativeV3PrepareRejectsStaticSocketAndCredentialInputOverlap(t *testing.T) {
	for _, kind := range []string{"socket-models", "socket-input-parent", "socket-helper", "credential-input-source"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := nativeFixture(t)
				l, err := NewNativeLauncherV3(t.Context(), f.config)
				if err != nil {
					t.Fatal(err)
				}
				bundle := nativeBundleV3Fixture(t, f)
				var sourceRoot string
				switch kind {
				case "socket-models":
					bundle.SocketDestination = "/sessionless/profile/models.json"
				case "socket-input-parent":
					bundle.SocketDestination = "/sessionless/input"
				case "socket-helper":
					bundle.SocketDestination = bundle.Relay.Executable
				case "credential-input-source":
					sourceRoot = filepath.Dir(f.credential)
				}
				bundle.BootstrapArguments, bundle.RelayArguments, err = nativeharnessbootstrap.InstallationArgumentsV3(bundle)
				if err != nil {
					t.Fatal(err)
				}
				bundle.PolicyDigest, err = bundle.InstallPolicyDigestV3()
				if err != nil {
					t.Fatal(err)
				}
				execution, files := nativeExecutionV3Fixture(t, bundle, 1, sourceRoot)
				driver := &nativePrepareOnlyV3{launcher: l}
				driver.before = func(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV3) {
					f.runner.containerInspect = nativeInspectionV3(t, f, ctx, e, files)
				}
				runNativePreparationV3(t, f, bundle, execution, driver)
				if !errors.Is(driver.prepareErr, ErrConfig) || len(nativeV3CreateCalls(f.runner)) != 0 {
					t.Fatalf("overlap passed create: prepare=%v count%d", driver.prepareErr, len(nativeV3CreateCalls(f.runner)))
				}
			})
		})
	}
}

func TestNativeV3UnknownRemovalRetainsOriginalStageWithoutHandback(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(strconv.FormatBool(partial), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := nativeFixture(t)
				fault := &nativeFaultRunner{base: f.runner, createUnknown: partial, removeUnknown: true}
				f.config.runner = fault
				l, err := NewNativeLauncherV3(t.Context(), f.config)
				if err != nil {
					t.Fatal(err)
				}
				bundle := nativeBundleV3Fixture(t, f)
				execution, files := nativeExecutionV3Fixture(t, bundle, 1, "")
				driver := &nativePrepareOnlyV3{launcher: l, expectedReleaseError: ErrBoundary}
				var root string
				driver.before = func(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV3) {
					root = e.Workload.WriteRoots[0]
					fault.stage = filepath.Join(root, ".sessionless-auth-stage")
					f.runner.containerInspect = nativeInspectionV3(t, f, ctx, e, files)
				}
				runNativePreparationV3(t, f, bundle, execution, driver)
				if !strings.HasPrefix(filepath.Base(root), "attempt-") || !strings.HasPrefix(filepath.Base(filepath.Dir(root)), "v3n-") {
					t.Fatalf("unexpected retained owned root %q", root)
				}
				t.Cleanup(func() {
					if err := os.RemoveAll(root); err != nil {
						t.Errorf("exact retained fake root cleanup: %v", err)
					}
				})
				if driver.boundary == nil || driver.result.CleanupSucceeded || driver.result.BoundaryReleased {
					t.Fatalf("unknown removal certified cleanup: result%+v prepare%v", driver.result, driver.prepareErr)
				}
				if partial && (!errors.Is(driver.prepareErr, ErrBoundary) || driver.boundary.Command() != nil) {
					t.Fatalf("partial create startable/not failed: %v", driver.prepareErr)
				}
				if !partial && driver.prepareErr != nil {
					t.Fatalf("complete prepare unexpectedly failed: %v", driver.prepareErr)
				}
				stage, err := os.ReadFile(fault.stage)
				if err != nil || string(stage) != `{"synthetic":"must-not-handback"}` {
					t.Fatalf("unknown removal lost stage: %q %v", stage, err)
				}
				original, err := os.ReadFile(f.credential)
				if err != nil || string(original) != `{"token":"old"}` {
					t.Fatalf("unknown removal committed auth: %q %v", original, err)
				}
				before := len(f.runner.snapshot())
				cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
				err = driver.boundary.Release(cleanup)
				cancel()
				if !errors.Is(err, ErrBoundary) || len(f.runner.snapshot()) != before {
					t.Fatalf("unknown release was not sticky: %v", err)
				}
				creates := nativeV3CreateCalls(f.runner)
				if len(creates) != 1 {
					t.Fatalf("unknown create count%d want1", len(creates))
				}
				index := slices.Index(creates[0], "--name")
				if index < 0 || index+1 >= len(creates[0]) {
					t.Fatal("missing original random owned name")
				}
				want := testContainerID
				if partial {
					want = creates[0][index+1]
				}
				for _, call := range f.runner.snapshot() {
					a := dockerCommand(call.arguments)
					if len(a) < 3 || a[0] != "container" {
						continue
					}
					lifecycle := a[1] == "stop" || a[1] == "kill" || a[1] == "rm" || (len(a) > 3 && a[1] == "inspect" && a[3] == "{{json .State}}")
					if lifecycle && a[len(a)-1] != want {
						t.Fatalf("cleanup selected unowned identity %q want%q", a, want)
					}
				}
			})
		})
	}
}

type nativeV3PostCreateRunner struct {
	base        *fakeRunner
	afterCreate func()
}

func (r *nativeV3PostCreateRunner) Command(dir string, env, args []string) *exec.Cmd {
	return r.base.Command(dir, env, args)
}
func (r *nativeV3PostCreateRunner) Run(ctx context.Context, dir string, env, args []string) ([]byte, []byte, error) {
	out, errout, err := r.base.Run(ctx, dir, env, args)
	command := dockerCommand(args)
	if err == nil && len(command) > 1 && command[0] == "container" && command[1] == "create" && r.afterCreate != nil {
		r.afterCreate()
	}
	return out, errout, err
}

func TestNativeV3PostCreateSourceReplacementRevokesStartAndHandback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := nativeFixture(t)
		runner := &nativeV3PostCreateRunner{base: f.runner}
		f.config.runner = runner
		l, err := NewNativeLauncherV3(t.Context(), f.config)
		if err != nil {
			t.Fatal(err)
		}
		bundle := nativeBundleV3Fixture(t, f)
		execution, files := nativeExecutionV3Fixture(t, bundle, 1, "")
		driver := &nativePrepareOnlyV3{launcher: l}
		var stage string
		driver.before = func(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV3) {
			stage = filepath.Join(e.Workload.WriteRoots[0], ".sessionless-auth-stage")
			f.runner.containerInspect = nativeInspectionV3(t, f, ctx, e, files)
			runner.afterCreate = func() {
				// Identical bytes and expected inspect paths cannot hide a new
				// source inode between initial validation and final preparation.
				path := files[0].Source
				if err := os.Rename(path, path+"-original"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("catalog"), 0400); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(stage, []byte(`{"synthetic":"never-handback"}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
		}
		runNativePreparationV3(t, f, bundle, execution, driver)
		if len(nativeV3CreateCalls(f.runner)) != 1 || !errors.Is(driver.prepareErr, ErrConfig) || driver.boundary == nil || driver.boundary.Command() != nil || !driver.result.BoundaryReleased {
			t.Fatalf("post-create substitution not rejected/owned: prepare=%v result%+v", driver.prepareErr, driver.result)
		}
		for _, call := range f.runner.snapshot() {
			a := dockerCommand(call.arguments)
			if len(a) > 1 && a[0] == "container" && a[1] == "start" {
				t.Fatal("native process was started")
			}
		}
		raw, err := os.ReadFile(f.credential)
		if err != nil || string(raw) != `{"token":"old"}` {
			t.Fatalf("revoked prepare handed back auth stage: %q %v", raw, err)
		}
		if _, err := os.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("revoked prepare stage residue: %v", err)
		}
	})
}
