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
// test-owned user service. No provider, Docker, or cloud endpoint is called.
func TestNativePlatformIntegration(t *testing.T) {
	if os.Getenv("SESSIONLESS_NATIVE_INTEGRATION") != "1" {
		t.Skip("opt-in user-service integration")
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
	t.Cleanup(func() {
		if err := os.RemoveAll(parent); err != nil {
			t.Errorf("remove exact test root: %v", err)
		}
	})
	store, config := packageFixtureAt(t, parent)
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
	// unexpectedly active service gets a local shutdown attempt first.
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		state, inspectErr := manager.inspect(cleanupCtx, config.Mode, name, stage.UnitPath)
		if inspectErr != nil {
			t.Errorf("cleanup inspect: %v", inspectErr)
			return
		}
		if !state.loaded || state.path != stage.UnitPath {
			return
		}
		if state.active {
			controlDir, dirErr := attachedworkerservice.Directory(config.StateRoot)
			if dirErr == nil {
				_, _ = attachedworkerservice.Call(cleanupCtx, controlDir, attachedworkerservice.ActionShutdown, 1)
			}
		}
		if err := manager.unregister(cleanupCtx, config.Mode, name, stage.UnitPath); err != nil {
			t.Errorf("cleanup exact test service: %v", err)
		}
	})
	baseArgs := []string{"--state-dir", config.StateRoot, "--package-mode", string(config.Mode),
		"--install-dir", config.InstallDir, "--binary", config.BinaryPath,
		"--binary-sha256", config.BinarySHA256}
	registerArgs := append(append([]string{}, baseArgs...), "--expected-install-revision", "1", "--native-action", "register")
	var register NativePlanV1
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
