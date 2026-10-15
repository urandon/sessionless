//go:build pi_sdk_functional

package pisdkmedia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
)

type nativeFunctionalOutput struct {
	mu   sync.Mutex
	body bytes.Buffer
}

func (o *nativeFunctionalOutput) Write(b []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(b) > 64<<10-o.body.Len() {
		return 0, errors.New("synthetic native diagnostic bound exceeded")
	}
	return o.body.Write(b)
}
func (o *nativeFunctionalOutput) Bytes() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return bytes.Clone(o.body.Bytes())
}

// This NEW functional test reaches genuine AgentSession/ModelRuntime/Pi
// standard Responses/OpenAI6.40 parser with a fake physical transport. It is
// NOT the consumed constructor-only probe, a live provider or serving fit proof.
func TestPreparedPiNativeStandardFakeTransport(t *testing.T) {
	const sdkSource = "/Volumes/hubdisk/workspace/ai/sessionless/.gitcode/native-build/pi-sdk-6c4f36-catalog-9c40af/source"
	const node = "/opt/homebrew/Cellar/node@24/24.19.0/bin/node"
	for _, path := range []string{node, filepath.Join(sdkSource, "packages/coding-agent/dist/index.js"), filepath.Join(sdkSource, "node_modules/openai/index.mjs")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("existing pinned source unavailable (no install/rebuild permitted): %v", err)
		}
	}
	for _, snapshot := range []bool{false, true} {
		name := "full-history"
		if snapshot {
			name = "snapshot-and-tail"
		}
		t.Run(name, func(t *testing.T) {
			identity, input := fixtureInput(t, snapshot)
			cfg := fixtureConfig()
			cfg.FinalSchema = []byte(attachedworkeroutput.StructuredFinalSchemaV1)
			schema := sha256.Sum256(cfg.FinalSchema)
			cfg.FinalSchemaSourceDigest = hex.EncodeToString(schema[:])
			cfg.Profile.Sources.FinalSchemaDigest = cfg.FinalSchemaSourceDigest
			cfg.Profile.Limits.StructuredFinalBytes = 128 << 10
			// Explicit inherited protocol values in a NON-SERVING synthetic
			// full-cohort fixture; not a measured model/native/platform profile.
			cfg.Profile.Limits.CanonicalInputBytes = 1 << 20
			cfg.Profile.Limits.SSEEvents = 4096
			cfg.Profile.Limits.SSELineBytes = cfg.Profile.Limits.SSEEventBytes
			cfg.Profile.Limits.ProcessDiskBytes = 16 << 20
			cfg.Profile.Limits.ProcessFrameBytes = 1 << 20
			cfg.Profile.Limits.ProcessStdoutBytes = 16 << 20
			cfg.Profile.Limits.ProcessEvents = 4096
			cfg.Profile.Limits.RawFinalBytes = 256 << 10
			cfg.Profile.Limits.AnswerBytes = 32 << 10
			cfg.Profile.Limits.ArtifactBytes = 32 << 10
			cfg.Profile.Limits.ArtifactTotalBytes = 64 << 10
			cfg.Model.SupportsExplicitPromptCacheMode = false
			ledger := fixtureLedger(t, cfg.Profile.Limits)
			projection, err := CompileOriginalRequestV1(context.Background(), identity, input, cfg, ledger)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(projection.Clear)
			messages, err := projection.OriginalMessages()
			if err != nil {
				t.Fatal(err)
			}
			// macOS exposes TMPDIR through /var -> /private/var. Resolve the
			// owned directory before passing Node's exact filesystem allowlist;
			// do not grant ancestor or whole-filesystem read access to fix it.
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			work := filepath.Join(root, "work")
			agent := filepath.Join(root, "agent")
			for _, dir := range []string{work, agent, filepath.Join(root, "internal/pisdkmedia")} {
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			for _, asset := range functionalNativeAssets(t, "sdk.test.mjs") {
				path := filepath.Join(root, asset.path)
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, asset.body, 0600); err != nil {
					t.Fatal(err)
				}
				read, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(read, asset.body) {
					t.Fatal("exact private native asset stage mismatch")
				}
			}
			// Test-only source link, not production provenance/profile selection.
			if err := os.Symlink(sdkSource, filepath.Join(root, "internal/pisdkmedia/pi")); err != nil {
				t.Fatal(err)
			}
			model := map[string]any{"api": cfg.Model.API, "provider": cfg.Model.Provider, "id": cfg.Model.ID, "baseUrl": "https://synthetic.invalid", "name": "synthetic full native functional", "reasoning": cfg.Model.Reasoning, "input": []string{"text", "image"}, "contextWindow": 65536, "maxTokens": cfg.MaxOutputTokens,
				"cost": map[string]uint64{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "compat": map[string]bool{"supportsDeveloperRole": cfg.Model.SupportsDeveloperRole, "supportsExplicitPromptCacheMode": cfg.Model.SupportsExplicitPromptCacheMode}}
			fixture := map[string]any{"body": string(projection.Body()), "messages": messages, "model": model, "systemPrompt": cfg.SystemPrompt, "endpoint": cfg.Model.Endpoint, "timeoutMs": 30000, "limits": cfg.Profile.Limits, "cwd": work, "agentDir": agent}
			encoded, err := json.Marshal(fixture)
			if err != nil || len(encoded) > 1<<20 {
				t.Fatal("complete native fixture exceeds original envelope")
			}
			path := filepath.Join(root, "internal/pisdkmedia/native/functional-fixture.json")
			if err := os.WriteFile(path, encoded, 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			t.Cleanup(cancel)
			command := exec.CommandContext(ctx, node, "--permission", "--allow-fs-read="+root, "--allow-fs-read="+sdkSource, "--no-addons", "--disable-proto=throw", filepath.Join(root, "internal/pisdkmedia/native/sdk.test.mjs"))
			command.Dir = work
			command.Env = []string{"PATH=/usr/bin:/bin", "PI_OFFLINE=1", "NO_COLOR=1"}
			var output nativeFunctionalOutput
			command.Stdout = &output
			command.Stderr = &output
			if err := command.Run(); err != nil {
				t.Fatalf("new credential-free functional native path failed: %v; synthetic diagnostics: %s", err, output.Bytes())
			}
			if !bytes.Contains(output.Bytes(), []byte("NEW pinned Pi full canonical/native fake-transport functional PASS")) {
				t.Fatal("missing actual functional completion")
			}
		})
	}
}
