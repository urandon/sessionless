package attachedworkerpackage

import (
	"context"
	"crypto/ed25519"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestStageExactServiceArtifactAndFencedUpdate(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("owner-local service package is Unix-only")
	}
	store, config := packageFixture(t)
	ctx := context.Background()
	plan, err := Plan(ctx, config, 0)
	if err != nil {
		t.Fatalf("plan initial package: %v", err)
	}
	if plan.ExpectedInstallRevision != 0 || plan.NextInstallRevision != 1 || plan.RollbackSHA256 != "" {
		t.Fatalf("initial package plan: %+v", plan)
	}
	if _, err := os.Stat(config.InstallDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("plan mutated install dir: %v", err)
	}
	receipt, err := Apply(ctx, config, plan)
	if err != nil || receipt.InstallRevision != 1 || receipt.Registration != "not_attempted" {
		t.Fatalf("apply initial package: receipt=%+v err=%v", receipt, err)
	}
	unit, err := os.ReadFile(plan.UnitPath)
	if err != nil || digest(unit) != plan.UnitSHA256 {
		t.Fatalf("staged unit digest: err=%v got=%s want=%s", err, digest(unit), plan.UnitSHA256)
	}
	if !strings.Contains(string(unit), config.BinaryPath) || !strings.Contains(string(unit), config.StateRoot) ||
		strings.Contains(string(unit), "Restart=always") || strings.Contains(string(unit), "KeepAlive</key><true") {
		t.Fatalf("staged unit grants wrong lifecycle or misses explicit roots: %q", unit)
	}
	if _, err := Apply(ctx, config, plan); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale plan applied twice: %v", err)
	}
	lease, err := store.AcquireRuntime(ctx)
	if err != nil {
		t.Fatalf("hold runtime owner: %v", err)
	}
	second, err := Plan(ctx, config, 1)
	if err != nil {
		t.Fatalf("plan second revision: %v", err)
	}
	if _, err := Apply(ctx, config, second); !errors.Is(err, attachedworkerlocal.ErrStateBusy) {
		t.Errorf("package apply ignored live runtime owner: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("release runtime owner: %v", err)
	}
	changed := config
	changed.BinaryPath = filepath.Join(filepath.Dir(config.BinaryPath), "attached-worker-v2")
	if err := os.WriteFile(changed.BinaryPath, []byte("#!/bin/sh\nexit 2\n"), 0o700); err != nil {
		t.Fatalf("write pinned replacement: %v", err)
	}
	encoded, err := os.ReadFile(changed.BinaryPath)
	if err != nil {
		t.Fatal(err)
	}
	changed.BinarySHA256 = digest(encoded)
	update, err := Plan(ctx, changed, 1)
	if err != nil || update.RollbackSHA256 != plan.UnitSHA256 || update.UnitSHA256 == plan.UnitSHA256 {
		t.Fatalf("plan exact update: plan=%+v err=%v", update, err)
	}
	updated, err := Apply(ctx, changed, update)
	if err != nil || updated.InstallRevision != 2 || updated.RollbackSHA256 != plan.UnitSHA256 {
		t.Fatalf("apply exact update: receipt=%+v err=%v", updated, err)
	}
	if _, err := Plan(ctx, config, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("old package revision revived after update: %v", err)
	}
}

func TestPackageFailsClosedOnTamperingAndUnsafeInputs(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("owner-local service package is Unix-only")
	}
	_, config := packageFixture(t)
	ctx := context.Background()
	plan, err := Plan(ctx, config, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Apply(ctx, config, plan); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plan.UnitPath, []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Plan(ctx, config, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("tampered staged unit accepted: %v", err)
	}
	unsafe := config
	unsafe.StateRoot += "/../other"
	if _, err := Plan(ctx, unsafe, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("noncanonical state root accepted: %v", err)
	}
	unsafe = config
	unsafe.BinarySHA256 = strings.Repeat("0", 64)
	if _, err := Plan(ctx, unsafe, 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("binary drift accepted: %v", err)
	}
	unsafe = config
	unsafe.InstallDir = filepath.Join(config.StateRoot, "units")
	if _, err := Plan(ctx, unsafe, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("install directory inside strict state root accepted: %v", err)
	}
	if _, err := os.Stat(unsafe.InstallDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsafe install directory was created: %v", err)
	}
	store, err := attachedworkerlocal.NewStore(config.StateRoot, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadSnapshot(ctx); err != nil {
		t.Fatalf("unsafe plan poisoned strict state inventory: %v", err)
	}
}

func TestServiceArtifactRenderModesRemainDefaultOff(t *testing.T) {
	store, config := packageFixture(t)
	snapshot, err := store.LoadSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manifest := snapshot.Manifest
	for _, mode := range []Mode{ModeLaunchd, ModeSystemdUser, ModeRootlessContainer} {
		candidate := config
		candidate.Mode = mode
		if mode == ModeRootlessContainer {
			candidate.ContainerImage = "registry.example/daemon@sha256:" + strings.Repeat("a", 64)
		}
		unit, err := render(candidate, manifest)
		if err != nil {
			t.Errorf("render %s: %v", mode, err)
			continue
		}
		text := string(unit)
		if strings.Contains(text, "KeepAlive</key><true") || strings.Contains(text, "Restart=on-failure") ||
			strings.Contains(text, "\"auto_restart\": true") || strings.Contains(text, "secret.json") {
			t.Errorf("%s artifact enables retry or embeds secret path: %s", mode, text)
		}
		if mode == ModeRootlessContainer {
			if !strings.Contains(text, `"manifest_revision": 1`) || !strings.Contains(text, candidate.ContainerImage) ||
				strings.Contains(text, "command") {
				t.Errorf("rootless intent claims an unverified runnable command or misses pins: %s", text)
			}
		} else if !strings.Contains(text, "--expected-revision") || !strings.Contains(text, "--binary-sha256") ||
			!strings.Contains(text, config.BinarySHA256) {
			t.Errorf("%s artifact misses startup pins: %s", mode, text)
		}
	}
}

func packageFixture(t *testing.T) (*attachedworkerlocal.Store, Config) {
	t.Helper()
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(parent, "attached-worker")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	binaryContent, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	cli := filepath.Join(parent, "docker-config")
	if err := os.Mkdir(cli, 0o700); err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	boundary := attachedworkerlocal.BoundaryLinuxRootless
	mode := ModeSystemdUser
	if runtime.GOOS == "darwin" {
		boundary = attachedworkerlocal.BoundaryDarwinVM
		mode = ModeLaunchd
	}
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	manifest := attachedworkerlocal.ManifestV1{
		Version: 1, Revision: 1, ControlPlaneOrigin: "https://control.example",
		TenantID: "tenant-001", OwnerUserID: "user-001", WorkerID: "worker-001", EnrollmentGeneration: 1,
		IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(public)),
		OCI: attachedworkerlocal.OCIConfigV1{DockerPath: binary, DockerSHA256: digest(binaryContent), CLIConfigDir: cli,
			Host: "unix:///private/tmp/sessionless-docker.sock", EngineID: "engine-001-abcdef", InstallationID: "install-001",
			Boundary: boundary, Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64), UserID: 1000, GroupID: 1000,
			DiskBytes: 1 << 30, CredentialFileBytes: 1024, MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10},
		Harness:   attachedworkerlocal.HarnessConfigV1{Executable: binary, SHA256: digest(binaryContent), Arguments: []string{}},
		Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: now, UpdatedAt: now,
	}
	secret := attachedworkerlocal.SecretRecordV1{Version: 1, ManifestRevision: 1, TenantID: manifest.TenantID,
		OwnerUserID: manifest.OwnerUserID, WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, IdentityPrivateKey: private}
	store, err := attachedworkerlocal.NewStore(filepath.Join(parent, "state"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatalf("initialize package fixture: %v", err)
	}
	return store, Config{Mode: mode, StateRoot: filepath.Join(parent, "state"), InstallDir: filepath.Join(parent, "units"),
		BinaryPath: binary, BinarySHA256: digest(binaryContent)}
}
