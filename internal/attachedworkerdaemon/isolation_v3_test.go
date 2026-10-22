//go:build darwin || linux

package attachedworkerdaemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type nativeV3FixtureLauncher struct {
	prepare func(context.Context, LaunchEnvelopeV3) (IsolationBoundaryV3, error)
}

func (*nativeV3FixtureLauncher) ProfileV3() IsolationProfileV3 {
	return IsolationProfileV3{Version: 3, Name: "native-v3-fixture", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, ProcessBoundary: true, DiskBytesBounded: true, Network: NativeConnectUnixV2}
}
func (l *nativeV3FixtureLauncher) PrepareV3(ctx context.Context, e LaunchEnvelopeV3) (IsolationBoundaryV3, error) {
	return l.prepare(ctx, e)
}

type nativeV3FixtureBoundary struct {
	*fixtureBoundary
	envelope   LaunchEnvelopeV3
	mutate     func(*WorkloadAttestationV3)
	releaseErr error
	released   bool
}

func (b *nativeV3FixtureBoundary) AttestedWorkloadV3() WorkloadAttestationV3 {
	a := WorkloadAttestationV3{Native: b.attestation, Bundle: b.envelope.Native.Clone(), Execution: b.envelope.Execution, ExecutionDigest: b.envelope.Execution.DigestV3(), Socket: b.envelope.Socket}
	if b.mutate != nil {
		b.mutate(&a)
	}
	return a
}
func (b *nativeV3FixtureBoundary) Release(context.Context) error {
	b.released = true
	return b.releaseErr
}

func supervisorV3Fixture(t *testing.T) (*Supervisor, *nativeV3FixtureLauncher, AttemptSpec, NativeLaunchV3) {
	t.Helper()
	root, err := os.MkdirTemp("", "v3-")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("owned scratch cleanup: %v", err)
		}
	})
	bundle := installV3Fixture(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	// Native-shaped argv reaches an owned wrapper, not Codex. The wrapper runs
	// the existing bounded stdio/process fixture without passing model flags
	// into Go's test flag parser. No model, relay or container is executed.
	wrapper := filepath.Join(filepath.Dir(bundle.Workload.Executable), "native-fixture")
	quoted := "'" + strings.ReplaceAll(executable, "'", "'\"'\"'") + "'"
	if err := os.WriteFile(wrapper, []byte("#!/bin/sh\nexec "+quoted+" -test.run='^TestSupervisorHelperProcess$'\n"), 0500); err != nil {
		t.Fatal(err)
	}
	digest, err := DigestExecutable(wrapper)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Workload = NativeArtifactV2{Executable: wrapper, Digest: digest}
	bundle.Limits.Active = 5 * time.Second
	installV3Vectors(&bundle, []string{"exec", "--json", "-"})
	bundle.PolicyDigest, err = bundle.InstallPolicyDigestV3()
	if err != nil {
		t.Fatal(err)
	}
	launcher := &nativeV3FixtureLauncher{}
	supervisor, err := NewSupervisorV3(SupervisorConfigV3{Base: SupervisorConfig{ScratchRoot: root, Timeout: 5 * time.Second, TerminationGrace: 100 * time.Millisecond, AllowedEnvironmentNames: []string{helperEnabled, helperMode}}, Launcher: launcher})
	if err != nil {
		t.Fatal(err)
	}
	spec := AttemptSpec{Executable: wrapper, ExecutableDigest: digest, Environment: []EnvironmentVariable{{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "protocol-terminal"}}}
	return supervisor, launcher, spec, bundle
}

func TestNativeV3UsesOriginalSupervisorProcessAndCleanup(t *testing.T) {
	for _, variant := range []uint32{0, 4, 6} {
		t.Run(string(rune('0'+variant)), func(t *testing.T) {
			supervisor, launcher, spec, bundle := supervisorV3Fixture(t)
			execution, _ := executionV3Fixture(t, bundle, variant)
			spec.Arguments = append([]string(nil), bundle.NativeArguments[variant]...)
			var boundary *nativeV3FixtureBoundary
			launcher.prepare = func(ctx context.Context, e LaunchEnvelopeV3) (IsolationBoundaryV3, error) {
				if e.Execution != execution || e.Execution.ValidateV3(ctx, bundle) != nil {
					t.Fatal("original input binding lost at launcher")
				}
				b, err := (&fixtureLauncher{}).Prepare(ctx, e.Workload)
				if err != nil {
					return nil, err
				}
				boundary = &nativeV3FixtureBoundary{fixtureBoundary: b.(*fixtureBoundary), envelope: e}
				return boundary, nil
			}
			result, err := supervisor.RunNativeV3(t.Context(), spec, bundle, execution, &nativeBudgetConduit{})
			if err != nil || !result.CleanupSucceeded || !result.BoundaryReleased || !result.DescendantsReaped || result.ExitCode != 0 || result.IsolationProfile != "native-v3-fixture" {
				t.Fatalf("original V3 lifecycle: %v %v", result, err)
			}
			if boundary == nil || boundary.command.ProcessState == nil || !boundary.released || len(result.Stdout) == 0 {
				t.Fatalf("actual owned process/cleanup not observed: boundary=%v bytes=%d", boundary != nil, len(result.Stdout))
			}
			if _, err := supervisor.Run(t.Context(), spec); !errors.Is(err, ErrIsolationUnsupported) {
				t.Fatalf("V3 through V1 accepted: %v", err)
			}
		})
	}
}

func TestNativeV3AttestationMutationDeniesBeforeProcessStart(t *testing.T) {
	for _, mode := range []string{"execution-digest", "foreign-readset", "bundle", "socket", "native-argv", "source-replaced", "release-failed"} {
		t.Run(mode, func(t *testing.T) {
			supervisor, launcher, spec, bundle := supervisorV3Fixture(t)
			execution, files := executionV3Fixture(t, bundle, 1)
			foreign, _ := executionV3Fixture(t, bundle, 1)
			spec.Arguments = append([]string(nil), bundle.NativeArguments[1]...)
			var boundary *nativeV3FixtureBoundary
			launcher.prepare = func(ctx context.Context, e LaunchEnvelopeV3) (IsolationBoundaryV3, error) {
				b, err := (&fixtureLauncher{}).Prepare(ctx, e.Workload)
				if err != nil {
					return nil, err
				}
				boundary = &nativeV3FixtureBoundary{fixtureBoundary: b.(*fixtureBoundary), envelope: e}
				boundary.mutate = func(a *WorkloadAttestationV3) {
					switch mode {
					case "execution-digest":
						a.ExecutionDigest = ExecutableDigest{9}
					case "foreign-readset":
						a.Execution = foreign
						a.ExecutionDigest = foreign.DigestV3()
					case "bundle":
						a.Bundle.Limits.InboundBytes++
					case "socket":
						a.Socket = nil
					case "native-argv":
						a.Native.Arguments = []string{"different"}
					}
				}
				if mode == "source-replaced" {
					if err := os.Rename(files[2].Source, files[2].Source+".original"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(files[2].Source, []byte("image1"), 0400); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "release-failed" {
					boundary.releaseErr = errors.New("unknown fake release")
					return boundary, errors.New("partial fake preparation")
				}
				return boundary, nil
			}
			result, err := supervisor.RunNativeV3(t.Context(), spec, bundle, execution, &nativeBudgetConduit{})
			if err == nil || boundary == nil || boundary.command.Process != nil || !boundary.released {
				t.Fatalf("%s did not deny/cleanup before Start: result=%v err=%v", mode, result, err)
			}
			if mode == "release-failed" && result.CleanupSucceeded {
				t.Fatal("unknown handback became cleanup success")
			}
			if mode != "release-failed" && !result.CleanupSucceeded {
				t.Fatalf("clean denial leaked owned scratch: %v", result)
			}
		})
	}
}
