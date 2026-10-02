package attachedworkerpackage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

type fakeNativeManager struct {
	state            nativeState
	registerErr      error
	unregisterErr    error
	readbackErr      error
	registerCalls    int
	unregisterCalls  int
	startCalls       int
	inspectAfterCall bool
}

type blockedStartManager struct {
	*fakeNativeManager
	entered chan struct{}
	release chan struct{}
}

func (m *blockedStartManager) start(ctx context.Context, mode Mode, name, path string) error {
	close(m.entered)
	select {
	case <-m.release:
		return m.fakeNativeManager.start(ctx, mode, name, path)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *fakeNativeManager) inspect(_ context.Context, _ Mode, _, _ string) (nativeState, error) {
	if m.inspectAfterCall && m.readbackErr != nil {
		return nativeState{}, m.readbackErr
	}
	return m.state, nil
}

func (m *fakeNativeManager) register(_ context.Context, _ Mode, _, path string) error {
	m.registerCalls++
	m.inspectAfterCall = true
	m.state = nativeState{loaded: true, path: path}
	return m.registerErr
}

func (m *fakeNativeManager) unregister(_ context.Context, _ Mode, _, _ string) error {
	m.unregisterCalls++
	m.inspectAfterCall = true
	m.state = nativeState{}
	return m.unregisterErr
}

func (m *fakeNativeManager) start(_ context.Context, _ Mode, _, _ string) error {
	m.startCalls++
	m.state.active = true
	return nil
}

func nativeFixture(t *testing.T) (*attachedworkerlocal.Store, Config, *fakeNativeManager) {
	t.Helper()
	store, config := packageFixture(t)
	ctx := context.Background()
	plan, err := Plan(ctx, config, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(ctx, config, plan); err != nil {
		t.Fatal(err)
	}
	return store, config, &fakeNativeManager{}
}

func TestRootlessNativePlanPinsStagedImageAndCreatesPrivateClientConfig(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("rootless user service is Linux-only")
	}
	_, config := packageFixture(t)
	config.Mode = ModeRootlessContainer
	config.ContainerImage = "registry.example/base@sha256:" + strings.Repeat("a", 64)
	ctx := context.Background()
	stage, err := Plan(ctx, config, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(ctx, config, stage); err != nil {
		t.Fatal(err)
	}
	manager := &fakeNativeManager{}
	plan, err := nativePlan(ctx, config, NativeRegister, 1, manager)
	if err != nil {
		t.Fatal(err)
	}
	changed := config
	changed.ContainerImage = "registry.example/base@sha256:" + strings.Repeat("b", 64)
	if _, err := nativePlan(ctx, changed, NativeRegister, 1, manager); !errors.Is(err, ErrConflict) {
		t.Fatalf("different image was accepted for staged unit: %v", err)
	}
	if _, err := applyNative(ctx, config, plan, manager); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeInspect(ctx, changed, manager); !errors.Is(err, ErrConflict) {
		t.Fatalf("different image inspected as registered: %v", err)
	}
	if _, err := nativeStart(ctx, changed, 1, 1, manager); !errors.Is(err, ErrConflict) || manager.startCalls != 0 {
		t.Fatalf("different image reached manager: %v calls=%d", err, manager.startCalls)
	}
	for _, directory := range []string{rootlessDockerConfig(config), config.StateRoot + ".control"} {
		info, err := os.Lstat(directory)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("private rootless directory %s: %v %v", directory, info, err)
		}
	}
}

func TestNativeRegistrationIsDefaultOffFencedAndReversible(t *testing.T) {
	store, config, manager := nativeFixture(t)
	ctx := context.Background()
	initial, err := nativeInspect(ctx, config, manager)
	if err != nil || initial.Status != "unregistered" || initial.OSActive {
		t.Fatalf("initial inspection: %+v %v", initial, err)
	}
	plan, err := nativePlan(ctx, config, NativeRegister, 1, manager)
	if err != nil || plan.ExpectedRegistrationRevision != 0 || plan.NextRegistrationRevision != 1 {
		t.Fatalf("register plan: %+v %v", plan, err)
	}
	if _, err := os.Stat(nativeReceiptPath(plan.UnitPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only plan wrote receipt: %v", err)
	}
	bad := plan
	bad.UnitSHA256 = strings.Repeat("0", 64)
	if _, err := applyNative(ctx, config, bad, manager); !errors.Is(err, ErrInvalid) || manager.registerCalls != 0 {
		t.Fatalf("tampered plan reached manager: %v calls=%d", err, manager.registerCalls)
	}
	lease, err := store.AcquireRuntime(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyNative(ctx, config, plan, manager); !errors.Is(err, attachedworkerlocal.ErrStateBusy) {
		t.Fatalf("register ignored runtime owner: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	registered, err := applyNative(ctx, config, plan, manager)
	if err != nil || registered.Action != NativeRegister || registered.RegistrationRevision != 1 ||
		manager.registerCalls != 1 || manager.state.active {
		t.Fatalf("register: receipt=%+v err=%v manager=%+v", registered, err, manager)
	}
	if _, err := applyNative(ctx, config, plan, manager); !errors.Is(err, ErrConflict) {
		t.Fatalf("replayed register plan: %v", err)
	}
	if _, err := Plan(ctx, config, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("staging update while registered: %v", err)
	}
	started, err := nativeStart(ctx, config, 1, 1, manager)
	if err != nil || started.Status != "registered" || !started.OSActive || manager.startCalls != 1 {
		t.Fatalf("explicit start: %+v err=%v manager=%+v", started, err, manager)
	}
	if _, err := nativeStart(ctx, config, 1, 1, manager); !errors.Is(err, attachedworkerlocal.ErrStateBusy) &&
		!errors.Is(err, ErrConflict) {
		t.Fatalf("second start was accepted: %v", err)
	}
	manager.state.active = true
	if _, err := nativePlan(ctx, config, NativeUnregister, 1, manager); !errors.Is(err, ErrConflict) {
		t.Fatalf("unregister active service: %v", err)
	}
	manager.state.active = false
	remove, err := nativePlan(ctx, config, NativeUnregister, 1, manager)
	if err != nil || remove.ExpectedRegistrationRevision != 1 {
		t.Fatalf("unregister plan: %+v %v", remove, err)
	}
	unregistered, err := applyNative(ctx, config, remove, manager)
	if err != nil || unregistered.Action != NativeUnregister || unregistered.RegistrationRevision != 2 ||
		manager.unregisterCalls != 1 || manager.state.loaded {
		t.Fatalf("unregister: receipt=%+v err=%v manager=%+v", unregistered, err, manager)
	}
	if _, err := Plan(ctx, config, 1); err != nil {
		t.Fatalf("staging remained fenced after unregister: %v", err)
	}
	if _, err := applyNative(ctx, config, remove, manager); !errors.Is(err, ErrConflict) {
		t.Fatalf("replayed unregister plan: %v", err)
	}
}

func TestNativeStartFencesConcurrentRegistrationMutation(t *testing.T) {
	_, config, fake := nativeFixture(t)
	ctx := context.Background()
	register, err := nativePlan(ctx, config, NativeRegister, 1, fake)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyNative(ctx, config, register, fake); err != nil {
		t.Fatal(err)
	}
	remove, err := nativePlan(ctx, config, NativeUnregister, 1, fake)
	if err != nil {
		t.Fatal(err)
	}
	manager := &blockedStartManager{fakeNativeManager: fake, entered: make(chan struct{}), release: make(chan struct{})}
	type startResult struct {
		inspection NativeInspectionV1
		err        error
	}
	done := make(chan startResult, 1)
	go func() {
		inspection, err := nativeStart(ctx, config, 1, 1, manager)
		done <- startResult{inspection, err}
	}()
	select {
	case <-manager.entered:
	case <-time.After(3 * time.Second):
		close(manager.release)
		t.Fatal("start did not reach manager")
	}
	if _, err := applyNative(ctx, config, remove, manager); !errors.Is(err, ErrConflict) || fake.unregisterCalls != 0 {
		close(manager.release)
		t.Fatalf("concurrent unregister reached manager: %v calls=%d", err, fake.unregisterCalls)
	}
	close(manager.release)
	result := <-done
	if result.err != nil || result.inspection.Status != "registered" || !result.inspection.OSActive ||
		result.inspection.InstallRevision != 1 || result.inspection.RegistrationRevision != 1 {
		t.Fatalf("start exact revision: %+v %v", result.inspection, result.err)
	}
}

func TestNativeRegistrationFailsClosedOnPartialMutationAndDrift(t *testing.T) {
	for _, scenario := range []string{"manager error", "readback error", "external registration", "tampered receipt", "unit drift"} {
		t.Run(scenario, func(t *testing.T) {
			_, config, manager := nativeFixture(t)
			ctx := context.Background()
			plan, err := nativePlan(ctx, config, NativeRegister, 1, manager)
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "manager error":
				manager.registerErr = errors.New("manager changed state but returned failure")
			case "readback error":
				manager.readbackErr = errors.New("readback unavailable")
			case "external registration":
				manager.state = nativeState{loaded: true, path: plan.UnitPath}
			case "tampered receipt":
				if err := os.WriteFile(nativeReceiptPath(plan.UnitPath), []byte("{}"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "unit drift":
				if err := os.WriteFile(plan.UnitPath, []byte("tampered"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			_, err = applyNative(ctx, config, plan, manager)
			if scenario == "manager error" || scenario == "readback error" {
				if !errors.Is(err, ErrAmbiguous) {
					t.Fatalf("partial mutation was not ambiguous: %v", err)
				}
			} else if !errors.Is(err, ErrConflict) || manager.registerCalls != 0 {
				t.Fatalf("drift reached manager: err=%v calls=%d", err, manager.registerCalls)
			}
			if _, statErr := os.Stat(nativeReceiptPath(plan.UnitPath)); scenario != "tampered receipt" &&
				!errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("failed register wrote success receipt: %v", statErr)
			}
			if scenario == "manager error" || scenario == "readback error" {
				if _, statErr := os.Stat(nativePendingPath(plan.UnitPath)); statErr != nil {
					t.Fatalf("ambiguous mutation lost pending marker: %v", statErr)
				}
				if _, planErr := Plan(ctx, config, 1); !errors.Is(planErr, ErrConflict) {
					t.Fatalf("staging ignored pending marker: %v", planErr)
				}
				if _, planErr := nativePlan(ctx, config, NativeRegister, 1, manager); !errors.Is(planErr, ErrConflict) {
					t.Fatalf("native retry ignored pending marker: %v", planErr)
				}
			}
		})
	}
}

func TestNativeReconcileRequiresExactPendingAndDoesNotRetryManager(t *testing.T) {
	for _, scenario := range []string{"completed", "not applied"} {
		t.Run(scenario, func(t *testing.T) {
			_, config, manager := nativeFixture(t)
			ctx := context.Background()
			plan, err := nativePlan(ctx, config, NativeRegister, 1, manager)
			if err != nil {
				t.Fatal(err)
			}
			manager.registerErr = errors.New("ambiguous OS result")
			if _, err := applyNative(ctx, config, plan, manager); !errors.Is(err, ErrAmbiguous) {
				t.Fatalf("expected ambiguous mutation: %v", err)
			}
			if scenario == "not applied" {
				manager.state = nativeState{}
			}
			if _, err := reconcileNative(ctx, config, strings.Repeat("0", 64), manager); !errors.Is(err, ErrConflict) {
				t.Fatalf("wrong plan digest reconciled: %v", err)
			}
			result, err := reconcileNative(ctx, config, plan.PlanSHA256, manager)
			if err != nil || result.Status != strings.ReplaceAll(scenario, " ", "_") || manager.registerCalls != 1 {
				t.Fatalf("reconcile result=%+v err=%v manager=%+v", result, err, manager)
			}
			if result.Status == "completed" {
				if result.Receipt == nil || result.Receipt.RegistrationRevision != 1 {
					t.Fatalf("missing completed receipt: %+v", result)
				}
			} else if result.Receipt != nil {
				t.Fatalf("unapplied operation issued receipt: %+v", result)
			}
			if _, err := os.Stat(nativePendingPath(plan.UnitPath)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("resolved marker remains: %v", err)
			}
		})
	}
}

func TestNativeOutputParsersRejectIncompleteState(t *testing.T) {
	if value := nativeProperty("path = /tmp/service.plist\nstate = waiting\n", "path"); value != "/tmp/service.plist" {
		t.Fatalf("launchd path: %q", value)
	}
	for _, data := range []string{"", "LoadState=loaded\nFragmentPath=/tmp/unit", "LoadState=loaded\nLoadState=loaded\nFragmentPath=/tmp/unit\nActiveState=inactive"} {
		if _, err := systemdProperties(data); !errors.Is(err, ErrConflict) {
			t.Fatalf("incomplete systemd state accepted: %q %v", data, err)
		}
	}
	properties, err := systemdProperties("LoadState=loaded\nFragmentPath=/tmp/unit\nActiveState=inactive\n")
	if err != nil || properties["FragmentPath"] != "/tmp/unit" {
		t.Fatalf("exact systemd state: %+v %v", properties, err)
	}
}

func TestResolveSystemdFragmentFromUserManagerLink(t *testing.T) {
	root := t.TempDir()
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	unit := filepath.Join(root, "exact.service")
	link := filepath.Join(root, "linked.service")
	if err := os.WriteFile(unit, []byte("[Service]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(unit, link); err != nil {
		t.Fatal(err)
	}
	resolved, err := resolveSystemdFragment(link)
	if err != nil || resolved != unit {
		t.Fatalf("systemd link did not resolve to exact unit: %q %v", resolved, err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "missing.service"), link); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveSystemdFragment(link); !errors.Is(err, ErrConflict) {
		t.Fatalf("missing linked target was accepted: %v", err)
	}
}
