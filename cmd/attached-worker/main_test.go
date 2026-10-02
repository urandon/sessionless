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
	"gitcode.com/urandon/sessionless/internal/attachedworkerpackage"
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

func TestServeOwnsOneDisabledForegroundAndLocalControl(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("service control requires Unix-domain sockets")
	}
	root, privateKey := initializeCLIStore(t)
	binaryPath, binaryDigest := runningTestBinary(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var output bytes.Buffer
	var exitCode int
	go func() {
		exitCode = runWithContext(ctx, []string{"serve", "--state-dir", root, "--expected-revision", "1",
			"--binary", binaryPath, "--binary-sha256", binaryDigest}, &output)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("attached-worker serve did not stop after cancellation")
		}
	})
	deadline, stopWaiting := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopWaiting()
	var status bytes.Buffer
	for {
		status.Reset()
		if runWithContext(deadline, []string{"live-status", "--state-dir", root}, &status) == 0 {
			break
		}
		if deadline.Err() != nil {
			t.Fatalf("live status not ready before deadline: %v", deadline.Err())
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.Done():
		}
	}
	if !strings.Contains(status.String(), `"feature_state":"disabled"`) ||
		!strings.Contains(status.String(), `"network_action":"not_attempted"`) ||
		!strings.Contains(status.String(), `"server_connection_state":"unknown"`) {
		t.Fatalf("service claimed unproved runtime or server authority: %s", status.String())
	}
	var rejected bytes.Buffer
	if code := run([]string{"stop", "--state-dir", root, "--expected-revision", "2"}, &rejected); code != 1 ||
		!strings.Contains(rejected.String(), `"revision_conflict"`) {
		t.Fatalf("stale stop: exit=%d output=%s", code, rejected.String())
	}
	var drained bytes.Buffer
	if code := run([]string{"drain", "--state-dir", root, "--expected-revision", "1"}, &drained); code != 0 ||
		!strings.Contains(drained.String(), `"daemon_state":"draining"`) {
		t.Fatalf("drain: exit=%d output=%s", code, drained.String())
	}
	var stopped bytes.Buffer
	if code := run([]string{"stop", "--state-dir", root, "--expected-revision", "1"}, &stopped); code != 0 ||
		!strings.Contains(stopped.String(), `"runtime_ownership":"released"`) {
		t.Fatalf("stop: exit=%d output=%s", code, stopped.String())
	}
	select {
	case <-done:
		if exitCode != 0 {
			t.Fatalf("serve exit=%d output=%s", exitCode, output.String())
		}
	case <-deadline.Done():
		t.Fatalf("serve did not exit after stop: %v", deadline.Err())
	}
	combined := status.String() + drained.String() + stopped.String() + output.String()
	if strings.Contains(combined, root) || strings.Contains(strings.ToLower(combined), fmt.Sprintf("%x", privateKey)) ||
		strings.Contains(combined, "identity_private_key") || strings.Contains(combined, "connection_secret") {
		t.Fatalf("service control leaked private material: %s", combined)
	}
	store, err := attachedworkerlocal.NewStore(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	local, err := store.Status(context.Background())
	if err != nil || local.DaemonObservation != "unknown" {
		t.Fatalf("service retained stale live observation: status=%+v err=%v", local, err)
	}
}

func TestServeRejectsStaleManifestAndWrongExecutableBeforeListening(t *testing.T) {
	root, _ := initializeCLIStore(t)
	binaryPath, binaryDigest := runningTestBinary(t)
	var stale bytes.Buffer
	if code := run([]string{"serve", "--state-dir", root, "--expected-revision", "2",
		"--binary", binaryPath, "--binary-sha256", binaryDigest}, &stale); code != 1 ||
		!strings.Contains(stale.String(), `"code":"state_conflict"`) {
		t.Fatalf("stale pinned service start: exit=%d output=%s", code, stale.String())
	}
	var wrongBinary bytes.Buffer
	if code := run([]string{"serve", "--state-dir", root, "--expected-revision", "1",
		"--binary", filepath.Join(filepath.Dir(root), "docker"), "--binary-sha256", strings.Repeat("0", 64)}, &wrongBinary); code != 1 ||
		!strings.Contains(wrongBinary.String(), `"code":"state_conflict"`) {
		t.Fatalf("unreviewed executable start: exit=%d output=%s", code, wrongBinary.String())
	}
	store, err := attachedworkerlocal.NewStore(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	status, err := store.Status(context.Background())
	if err != nil || status.DaemonObservation != "unknown" {
		t.Fatalf("rejected service left live observation: status=%+v err=%v", status, err)
	}
}

func runningTestBinary(t *testing.T) (string, string) {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, fmt.Sprintf("%x", sha256.Sum256(contents))
}

func TestPackagePlanAndApplyStayUnregistered(t *testing.T) {
	root, _ := initializeCLIStore(t)
	store, err := attachedworkerlocal.NewStore(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.LoadSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mode := string(attachedworkerpackage.ModeSystemdUser)
	if runtime.GOOS == "darwin" {
		mode = string(attachedworkerpackage.ModeLaunchd)
	}
	base := []string{"--state-dir", root, "--package-mode", mode,
		"--install-dir", filepath.Join(filepath.Dir(root), "units"),
		"--binary", snapshot.Manifest.OCI.DockerPath,
		"--binary-sha256", snapshot.Manifest.OCI.DockerSHA256}
	var planned bytes.Buffer
	if code := run(append([]string{"package-plan"}, base...), &planned); code != 0 {
		t.Fatalf("package plan exit=%d output=%s", code, planned.String())
	}
	var plan attachedworkerpackage.PlanV1
	if err := json.Unmarshal(planned.Bytes(), &plan); err != nil || plan.PlanSHA256 == "" || plan.NextInstallRevision != 1 {
		t.Fatalf("decode package plan: plan=%+v err=%v", plan, err)
	}
	var stale bytes.Buffer
	if code := run(append(append([]string{"package-apply"}, base...), "--plan-sha256", strings.Repeat("0", 64)), &stale); code != 1 ||
		!strings.Contains(stale.String(), `"state_conflict"`) {
		t.Fatalf("unreviewed package apply exit=%d output=%s", code, stale.String())
	}
	var applied bytes.Buffer
	if code := run(append(append([]string{"package-apply"}, base...), "--plan-sha256", plan.PlanSHA256), &applied); code != 0 {
		t.Fatalf("package apply exit=%d output=%s", code, applied.String())
	}
	var receipt attachedworkerpackage.ReceiptV1
	if err := json.Unmarshal(applied.Bytes(), &receipt); err != nil || receipt.InstallRevision != 1 ||
		receipt.Registration != "not_attempted" {
		t.Fatalf("package apply did not preserve default-off registration: receipt=%+v err=%v", receipt, err)
	}
}

func initializeCLIStore(t *testing.T) (string, ed25519.PrivateKey) {
	t.Helper()
	parent := shortCLIHome(t)
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

func shortCLIHome(t *testing.T) string {
	t.Helper()
	base := "/tmp"
	if runtime.GOOS == "darwin" {
		base = "/private/tmp"
	}
	parent, err := os.MkdirTemp(base, "aw137-cli-")
	if err != nil {
		t.Fatalf("create short private CLI test root: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(parent); err != nil {
			t.Errorf("remove CLI test root %q: %v", parent, err)
		}
	})
	return parent
}
