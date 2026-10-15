package pisdkmedia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestCompleteInitPinnedNodeOriginalCohort(t *testing.T) {
	// Same existing pin as the SPU2 process controls. No installation, SDK,
	// constructor probe, native build, network, live credential or provider.
	t.Setenv("NODE_OPTIONS", "--invalid-ambient-options-must-not-be-inherited")
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("pinned Node prerequisite:", err)
	}
	node, err = filepath.EvalSymlinks(node)
	if err != nil {
		t.Fatal(err)
	}
	probeContext, cancelProbe := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancelProbe)
	probe := exec.CommandContext(probeContext, node, "--version")
	probe.Env = []string{}
	var version boundedInitDiagnostic
	probe.Stdout, probe.Stderr = &version, &version
	if err := probe.Run(); err != nil || strings.TrimSpace(version.String()) != "v24.19.0" {
		t.Fatalf("Node version=%q err=%v; want v24.19.0", version.String(), err)
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "history", true: "snapshot-tail"}[snapshot], func(t *testing.T) {
			cfg := initFixtureConfig(t)
			p, _ := initFixtureProjection(t, cfg, snapshot)
			fs := initFixtureFS(t)
			plan, err := PrepareInitV1(t.Context(), p, []byte(initFixtureModel), 1000, fs, "api-key")
			if err != nil {
				t.Fatal("prepare:", err)
			}
			t.Cleanup(plan.Clear)
			literal := []byte("PUBLIC-NONCREDENTIAL-NODE-INIT-CONTROL")
			file, err := fs.CreateImmutable("api-key", literal, 128)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := plan.MaterializedBody(t.Context(), file, domain.FingerprintCredential(literal))
			if err != nil {
				t.Fatal("original locator:", err)
			}
			bounds, err := json.Marshal(plan.DecodeLimits())
			if err != nil {
				t.Fatal(err)
			}
			processContext, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			t.Cleanup(cancel)
			cmd := exec.CommandContext(processContext, node, "--permission", "--allow-fs-read="+filepath.Join(root, "internal/pisdkmedia/native"),
				"--allow-fs-read="+filepath.Join(root, "internal/pisdktransport"), "--no-addons", "--disable-proto=throw", filepath.Join(root, "internal/pisdkmedia/native/init.test.mjs"), string(bounds))
			cmd.Env = []string{}
			cmd.Stdin = bytes.NewReader(wire)
			var output boundedInitDiagnostic
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := cmd.Run(); err != nil {
				t.Fatalf("pinned Node complete Init: %v\n%s", err, output.String())
			}
			if !strings.Contains(output.String(), "complete Init Go -> Node PASS; negative controls=") {
				t.Fatalf("missing actual terminal control: %s", output.String())
			}
			t.Log(strings.TrimSpace(output.String()))
		})
	}
}

func TestInitSourceExactIndependentBytes(t *testing.T) {
	b, digest := InitSource()
	sum := sha256.Sum256(b)
	if len(b) == 0 || digest != hex.EncodeToString(sum[:]) {
		t.Fatal("selected asset digest mismatch")
	}
	b[0] ^= 1
	after, afterDigest := InitSource()
	if afterDigest != digest || bytes.Equal(b, after) {
		t.Fatal("asset accessor aliases mutable caller bytes")
	}
}

// Test-owned fixed diagnostics only. Bound before accepting any child output;
// no unbounded CombinedOutput of a task/credential-bearing process.
type boundedInitDiagnostic struct{ bytes.Buffer }

func (b *boundedInitDiagnostic) Write(p []byte) (int, error) {
	if len(p) > 8192-b.Len() {
		return 0, ErrProjection
	}
	return b.Buffer.Write(p)
}
