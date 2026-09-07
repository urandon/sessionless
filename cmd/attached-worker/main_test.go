package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestRunBoundedRedactedJSONAndInvalidFlags(t *testing.T) {
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "missing-state")
	var output bytes.Buffer
	if code := run([]string{"status", "--state-dir", root}, &output); code != 1 {
		t.Fatalf("status exit=%d output=%s", code, output.String())
	}
	var result map[string]any
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["code"] != string(attachedworkerlocal.CodeMissing) || result["secret_state"] != "unknown" {
		t.Fatalf("result=%v", result)
	}
	if strings.Contains(output.String(), "identity_private_key") || strings.Contains(output.String(), "connection_secret") {
		t.Fatalf("unredacted output=%s", output.String())
	}
	output.Reset()
	if code := run([]string{"status", "--state-dir", root, "--unknown"}, &output); code != 2 {
		t.Fatalf("invalid flag exit=%d output=%s", code, output.String())
	}
	if !strings.Contains(output.String(), `"code":"state_invalid"`) {
		t.Fatalf("invalid flag output=%s", output.String())
	}
	output.Reset()
	if code := run([]string{"logout", "--state-dir", root, "--expected-revision", "1", "--idempotency-key", "short"}, &output); code != 1 {
		t.Fatalf("invalid key exit=%d output=%s", code, output.String())
	}
}

func TestRunSuccessfulLifecycleCommandsStayBoundedAndRedacted(t *testing.T) {
	root, privateKey := initializeCLIStore(t)
	commands := [][]string{
		{"check", "--state-dir", root},
		{"status", "--state-dir", root},
		{"doctor", "--state-dir", root},
		{"uninstall-plan", "--state-dir", root},
		{"logout", "--state-dir", root, "--expected-revision", "1", "--idempotency-key", "logout-cli-001"},
		{"status", "--state-dir", root},
		{"uninstall-plan", "--state-dir", root},
	}
	encodedPrivate := fmt.Sprintf("%x", privateKey)
	for _, arguments := range commands {
		var output bytes.Buffer
		if code := run(arguments, &output); code != 0 {
			t.Errorf("run(%v) exit=%d output=%s", arguments, code, output.String())
			continue
		}
		if output.Len() == 0 || output.Len() > 32<<10 {
			t.Errorf("run(%v) output size=%d", arguments, output.Len())
		}
		var result map[string]any
		if err := json.Unmarshal(output.Bytes(), &result); err != nil || result["code"] != string(attachedworkerlocal.CodeOK) {
			t.Errorf("run(%v) result=%v err=%v", arguments, result, err)
		}
		lower := strings.ToLower(output.String())
		if strings.Contains(lower, "identity_private_key") || strings.Contains(lower, "connection_secret") ||
			strings.Contains(lower, strings.ToLower(encodedPrivate)) || strings.Contains(output.String(), root) {
			t.Errorf("run(%v) leaked private/path material: %s", arguments, output.String())
		}
	}
}

func TestRunForegroundIsExplicitlyDisabledAndLeavesNoObservation(t *testing.T) {
	root, privateKey := initializeCLIStore(t)
	var output bytes.Buffer
	if code := run([]string{"run", "--state-dir", root}, &output); code != 1 {
		t.Fatalf("run exit=%d output=%s", code, output.String())
	}
	var result map[string]any
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["code"] != "feature_disabled" || result["runtime_ownership"] != "released" ||
		result["observation_state"] != "retired" || result["network_action"] != "not_attempted" ||
		result["process_action"] != "not_attempted" || result["credential_action"] != "not_attempted" {
		t.Fatalf("run result=%v", result)
	}
	lower := strings.ToLower(output.String())
	if strings.Contains(output.String(), root) || strings.Contains(lower, "identity_private_key") ||
		strings.Contains(lower, "connection_secret") || strings.Contains(lower, fmt.Sprintf("%x", privateKey)) {
		t.Fatalf("run leaked private/path material: %s", output.String())
	}
	store, err := attachedworkerlocal.NewStore(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	status, err := store.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.DaemonObservation != "unknown" || status.ObservationRevision != 0 {
		t.Fatalf("run retained observation: %+v", status)
	}
}

func initializeCLIStore(t *testing.T) (string, ed25519.PrivateKey) {
	t.Helper()
	parent, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dockerPath := filepath.Join(parent, "docker")
	if err := os.WriteFile(dockerPath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	dockerBytes, err := os.ReadFile(dockerPath)
	if err != nil {
		t.Fatal(err)
	}
	dockerDigest := fmt.Sprintf("%x", sha256.Sum256(dockerBytes))
	cliConfig := filepath.Join(parent, "docker-config")
	if err := os.Mkdir(cliConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	boundary := attachedworkerlocal.BoundaryLinuxRootless
	if runtime.GOOS == "darwin" {
		boundary = attachedworkerlocal.BoundaryDarwinVM
	}
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	manifest := attachedworkerlocal.ManifestV1{
		Version: 1, Revision: 1, ControlPlaneOrigin: "https://control.example",
		TenantID: "tenant-001", OwnerUserID: "user-001", WorkerID: "worker-001", EnrollmentGeneration: 1,
		IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(publicKey)),
		OCI: attachedworkerlocal.OCIConfigV1{DockerPath: dockerPath, DockerSHA256: dockerDigest, CLIConfigDir: cliConfig,
			Host: "unix:///private/tmp/sessionless-docker.sock", EngineID: "engine-001-abcdef", InstallationID: "install-001",
			Boundary: boundary, Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64), UserID: 1000, GroupID: 1000,
			DiskBytes: 1 << 30, CredentialFileBytes: 1024, MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10},
		Harness:   attachedworkerlocal.HarnessConfigV1{Executable: dockerPath, SHA256: dockerDigest, Arguments: []string{}},
		Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: now, UpdatedAt: now,
	}
	secret := attachedworkerlocal.SecretRecordV1{Version: 1, ManifestRevision: 1, TenantID: manifest.TenantID,
		OwnerUserID: manifest.OwnerUserID, WorkerID: manifest.WorkerID, EnrollmentGeneration: 1, IdentityPrivateKey: privateKey}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("manifest fixture: %v (%+v)", err, manifest)
	}
	if err := secret.Validate(manifest); err != nil {
		t.Fatalf("secret fixture: %v", err)
	}
	root := filepath.Join(parent, "state")
	store, err := attachedworkerlocal.NewStore(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize(context.Background(), manifest, secret); err != nil {
		t.Fatal(err)
	}
	return root, privateKey
}
