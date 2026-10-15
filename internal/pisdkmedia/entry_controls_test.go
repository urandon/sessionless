package pisdkmedia

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPreparedEntrySDKFreeControls(t *testing.T) {
	for _, mutant := range []bool{false, true} {
		t.Run(map[bool]string{false: "actual-entry", true: "eager-sdk-import-mutant"}[mutant], func(t *testing.T) { testPreparedEntrySDKFreeControls(t, mutant) })
	}
}

func testPreparedEntrySDKFreeControls(t *testing.T, mutant bool) {
	t.Helper()
	// Stage only repo-owned modules. No SDK link exists: an eager SDK import
	// fails this test before any assertion, without substituting an SDK factory.
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, asset := range fixedNativeAssets() {
		if mutant && asset.path == "internal/pisdkmedia/native/bootstrap.mjs" {
			// Only a test-owned staged copy changes. Reintroducing the exact
			// eager fixed SDK import must fail the same actual-entry test.
			asset.body = append([]byte("import '../pi/packages/coding-agent/dist/index.js';\n"), asset.body...)
		}
		path := filepath.Join(root, asset.path)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, asset.body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	control, err := os.ReadFile("native/entry_controls.test.mjs")
	if err != nil {
		t.Fatal(err)
	}
	controlPath := filepath.Join(root, "internal/pisdkmedia/native/entry_controls.test.mjs")
	if err := os.WriteFile(controlPath, control, 0600); err != nil {
		t.Fatal(err)
	}
	dataRoot := filepath.Join(root, "public-files")
	if err := os.Mkdir(dataRoot, 0700); err != nil {
		t.Fatal(err)
	}
	// Node's permission model requires globally writable FS for link creation.
	// Prepare exact test-owned links in Go instead of broadening child access.
	if err := os.Symlink(filepath.Join(dataRoot, "valid"), filepath.Join(dataRoot, "soft")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataRoot, "link-target"), []byte("PUBLIC-LINK-CONTROL"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(dataRoot, "link-target"), filepath.Join(dataRoot, "hard")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/opt/homebrew/Cellar/node@24/24.19.0/bin/node", "--permission", "--allow-fs-read="+root,
		"--allow-fs-write="+dataRoot, "--no-addons", "--disable-proto=throw", controlPath, dataRoot)
	cmd.Env = []string{}
	cmd.WaitDelay = 3 * time.Second
	var output boundedInitDiagnostic
	cmd.Stdout, cmd.Stderr = &output, &output
	err = cmd.Run()
	if mutant {
		if err == nil || !strings.Contains(output.String(), "ERR_MODULE_NOT_FOUND") {
			t.Fatalf("eager import mutant did not causally fail: %v; %s", err, output.String())
		}
		return
	}
	if err != nil {
		t.Fatalf("SDK-free entry controls: %v; bounded output: %s", err, output.String())
	}
	t.Log(strings.TrimSpace(output.String()))
}
