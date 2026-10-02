package attachedworkerpackage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerservice"
)

// This test is opt-in because it temporarily registers and starts a real
// test-owned user service. The rootless variant uses a test-owned Docker
// engine and a preloaded execution base; neither variant calls a provider.
func TestNativePlatformIntegration(t *testing.T) {
	rootless := os.Getenv("SESSIONLESS_ROOTLESS_INTEGRATION") == "1"
	if os.Getenv("SESSIONLESS_NATIVE_INTEGRATION") != "1" && !rootless {
		t.Skip("opt-in user-service integration")
	}
	if rootless && runtime.GOOS != "linux" {
		t.Fatal("rootless service integration requires Linux")
	}
	binary := os.Getenv("SESSIONLESS_ATTACHED_WORKER_BINARY")
	if binary == "" {
		t.Fatal("SESSIONLESS_ATTACHED_WORKER_BINARY is required")
	}
	canonical, err := filepath.EvalSymlinks(binary)
	if err != nil || canonical != binary {
		t.Fatalf("binary must be an exact canonical path: %v", err)
	}
	contents, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(contents)
	base := "/tmp"
	if runtime.GOOS == "darwin" {
		base = "/private/tmp"
	}
	parent, err := os.MkdirTemp(base, "aw137-native-")
	if err != nil {
		t.Fatal(err)
	}
	safeToRemove := true
	t.Cleanup(func() {
		if !safeToRemove {
			t.Errorf("preserving test root %s: exact service cleanup was not verified", parent)
			return
		}
		if err := os.RemoveAll(parent); err != nil {
			t.Errorf("remove exact test root: %v", err)
		}
	})
	store, config := packageFixtureAt(t, parent)
	if rootless {
		config.Mode = ModeRootlessContainer
		config.ContainerImage = os.Getenv("SESSIONLESS_ROOTLESS_IMAGE")
		if !imageDigest.MatchString(config.ContainerImage) {
			t.Fatal("SESSIONLESS_ROOTLESS_IMAGE must be an immutable image digest")
		}
	}
	config.BinaryPath = binary
	config.BinarySHA256 = hex.EncodeToString(hash[:])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stage, err := Plan(ctx, config, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(ctx, config, stage); err != nil {
		t.Fatal(err)
	}
	manager := osNativeManager{}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	name := nativeName(snapshot.Manifest)
	// Cleanup is limited to this test's unique temporary unit path. An
	// unexpectedly active service gets a local shutdown attempt, then an
	// exact OS stop, before the test root may be removed.
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		state, inspectErr := manager.inspect(cleanupCtx, config.Mode, name, stage.UnitPath)
		if inspectErr != nil {
			t.Errorf("cleanup inspect: %v", inspectErr)
			return
		}
		if !state.loaded {
			safeToRemove = true
			return
		}
		if state.path != stage.UnitPath {
			t.Errorf("cleanup refused foreign service path %s", state.path)
			return
		}
		if state.active {
			controlDir, dirErr := attachedworkerservice.Directory(config.StateRoot)
			if dirErr == nil {
				if _, err := attachedworkerservice.Call(cleanupCtx, controlDir, attachedworkerservice.ActionShutdown, 1); err != nil {
					t.Logf("local shutdown unavailable; using exact OS stop: %v", err)
				}
			}
			graceCtx, endGrace := context.WithTimeout(cleanupCtx, 2*time.Second)
			graceErr := waitForManagerInactive(graceCtx, manager, config, name, stage.UnitPath)
			endGrace()
			if graceErr != nil {
				if err := manager.stop(cleanupCtx, config.Mode, name, stage.UnitPath); err != nil {
					t.Errorf("cleanup exact OS stop: %v", err)
					return
				}
				if err := waitForManagerInactive(cleanupCtx, manager, config, name, stage.UnitPath); err != nil {
					t.Errorf("cleanup remained active: %v", err)
					return
				}
			}
		}
		if err := manager.unregister(cleanupCtx, config.Mode, name, stage.UnitPath); err != nil {
			t.Errorf("cleanup exact test service: %v", err)
			return
		}
		final, err := manager.inspect(cleanupCtx, config.Mode, name, stage.UnitPath)
		if err != nil || final.loaded || final.active {
			t.Errorf("cleanup registration still present: %+v %v", final, err)
			return
		}
		safeToRemove = true
	})
	baseArgs := []string{"--state-dir", config.StateRoot, "--package-mode", string(config.Mode),
		"--install-dir", config.InstallDir, "--binary", config.BinaryPath,
		"--binary-sha256", config.BinarySHA256}
	if rootless {
		baseArgs = append(baseArgs, "--container-image", config.ContainerImage)
	}
	registerArgs := append(append([]string{}, baseArgs...), "--expected-install-revision", "1", "--native-action", "register")
	var register NativePlanV1
	safeToRemove = false
	if err := json.Unmarshal(integrationCLI(t, ctx, binary, append([]string{"native-plan"}, registerArgs...)...), &register); err != nil {
		t.Fatal(err)
	}
	integrationCLI(t, ctx, binary, append(append([]string{"native-apply"}, registerArgs...), "--plan-sha256", register.PlanSHA256)...)
	var inspection NativeInspectionV1
	if err := json.Unmarshal(integrationCLI(t, ctx, binary, append([]string{"native-inspect"}, baseArgs...)...), &inspection); err != nil {
		t.Fatal(err)
	}
	if inspection.Status != "registered" || inspection.OSActive {
		t.Fatalf("default-off registration: %+v", inspection)
	}
	integrationCLI(t, ctx, binary, append(append([]string{"native-start"}, baseArgs...),
		"--expected-install-revision", "1", "--expected-registration-revision", "1")...)
	controlDir, err := attachedworkerservice.Directory(config.StateRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := waitForLiveOwner(ctx, controlDir); err != nil {
		t.Fatalf("live owner did not answer: %v", err)
	}
	integrationCLI(t, ctx, binary, "live-status", "--state-dir", config.StateRoot)
	integrationCLI(t, ctx, binary, "drain", "--state-dir", config.StateRoot, "--expected-revision", "1")
	integrationCLI(t, ctx, binary, "stop", "--state-dir", config.StateRoot, "--expected-revision", "1")
	if err := waitForInactive(ctx, config); err != nil {
		t.Fatalf("service did not become inactive: %v", err)
	}
	unregisterArgs := append(append([]string{}, baseArgs...), "--expected-install-revision", "1", "--native-action", "unregister")
	var unregister NativePlanV1
	if err := json.Unmarshal(integrationCLI(t, ctx, binary, append([]string{"native-plan"}, unregisterArgs...)...), &unregister); err != nil {
		t.Fatal(err)
	}
	integrationCLI(t, ctx, binary, append(append([]string{"native-apply"}, unregisterArgs...), "--plan-sha256", unregister.PlanSHA256)...)
	inspection, err = NativeInspect(ctx, config)
	if err != nil || inspection.Status != "unregistered" {
		t.Fatalf("unregister readback: %+v %v", inspection, err)
	}
}

func waitForManagerInactive(ctx context.Context, manager osNativeManager, config Config, name, unitPath string) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := manager.inspect(ctx, config.Mode, name, unitPath)
		if err == nil && (!state.loaded || (state.path == unitPath && !state.active)) {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

func integrationCLI(t *testing.T, ctx context.Context, binary string, args ...string) []byte {
	t.Helper()
	command := exec.CommandContext(ctx, binary, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("attached-worker %s: %v: %s", args[0], err, output)
	}
	return output
}

func waitForLiveOwner(ctx context.Context, controlDir string) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, err := attachedworkerservice.Call(ctx, controlDir, attachedworkerservice.ActionStatus, 0)
		if err == nil && response.Live && response.Foreground != nil && response.Foreground.RuntimeOwnership == "acquired" {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

func waitForInactive(ctx context.Context, config Config) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		inspection, err := NativeInspect(ctx, config)
		if err == nil && inspection.Status == "registered" && !inspection.OSActive {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), err)
		case <-ticker.C:
		}
	}
}
