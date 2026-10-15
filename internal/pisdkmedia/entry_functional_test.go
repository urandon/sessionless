//go:build pi_sdk_functional && (darwin || linux)

package pisdkmedia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"golang.org/x/sys/unix"
)

// NEW full actual child entry, not the consumed constructor-only probe.
// Real original lifecycle/Init/SPU2/file/genuine Pi+SDK/parser; ONLY the Source
// allowance/physical HTTP sink are synthetic. Not SAME Supervisor/Source/native
// fit/private producer or model/cloud evidence; those joins remain required.
func TestPreparedPiNativeStdioEntry(t *testing.T) {
	const sdkSource = "/Volumes/hubdisk/workspace/ai/sessionless/.gitcode/native-build/pi-sdk-6c4f36-catalog-9c40af/source"
	const node = "/opt/homebrew/Cellar/node@24/24.19.0/bin/node"
	for _, path := range []string{node, filepath.Join(sdkSource, "packages/coding-agent/dist/index.js"), filepath.Join(sdkSource, "node_modules/openai/index.mjs")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("existing exact prerequisite missing; no install/rebuild:", err)
		}
	}
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "history", true: "snapshot-tail"}[snapshot], func(t *testing.T) {
			cfg := initFixtureConfig(t)
			cfg.FinalSchema = []byte(attachedworkeroutput.StructuredFinalSchemaV1)
			schemaHash := sha256.Sum256(cfg.FinalSchema)
			cfg.FinalSchemaSourceDigest = hex.EncodeToString(schemaHash[:])
			cfg.Profile.Sources.FinalSchemaDigest = cfg.FinalSchemaSourceDigest
			cfg.Headers[2].Value = entryPlatformUserAgent(t)
			cfg.Headers[10].Value = "30"
			cfg.Model.SupportsExplicitPromptCacheMode = false
			model := strings.Replace(initFixtureModel, `"supportsExplicitPromptCacheMode":true`, `"supportsExplicitPromptCacheMode":false`, 1)
			projection, ledger := initFixtureProjection(t, cfg, snapshot)
			credentials := initFixtureCredentials(t, projection)
			plan, err := PrepareInitV1(t.Context(), projection, []byte(model), 30000, credentials.source)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(plan.Clear)
			args := plan.EntryArguments() // prospectively funded before Issue
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err = os.Chmod(root, 0700); err != nil {
				t.Fatal(err)
			}
			for _, asset := range functionalNativeAssets(t, "entry_fake_transport.mjs") {
				path := filepath.Join(root, asset.path)
				if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, asset.body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err = os.Symlink(sdkSource, filepath.Join(root, "internal/pisdkmedia/pi")); err != nil {
				t.Fatal(err)
			}
			credentials.consume(t, func(handle ports.ProviderInvocationCredentialV1, value ports.ProviderCredentialMaterializationV1) error {
				body, err := plan.MaterializedBody(t.Context(), handle, value)
				if err != nil {
					return err
				}
				limits := pisdkphasewire.FrameLimits{InitializeBodyBytes: cfg.Profile.Limits.ProcessFrameBytes - 17,
					RequestBodyBytes: min(cfg.Profile.Limits.NativeRequestBytes, cfg.Profile.Limits.ProcessFrameBytes-146),
					FinalBodyBytes:   min(cfg.Profile.Limits.StructuredFinalBytes, cfg.Profile.Limits.RawFinalBytes, cfg.Profile.Limits.ProcessFrameBytes-17), FrameBytes: cfg.Profile.Limits.ProcessFrameBytes}
				initial, err := pisdkphasewire.EncodeFrame(pisdkphasewire.Frame{Kind: pisdkphasewire.Initialize, Body: body}, limits)
				if err != nil {
					return err
				}
				ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
				defer cancel()
				commandArgs := []string{"--permission", "--allow-fs-read=" + root, "--allow-fs-read=" + sdkSource, "--allow-fs-read=" + value.RootDir,
					"--no-addons", "--disable-proto=throw", filepath.Join(root, "internal/pisdkmedia/native/entry_fake_transport.mjs")}
				commandArgs = append(commandArgs, args[:]...)
				cmd := exec.CommandContext(ctx, node, commandArgs...)
				cmd.Dir = root
				cmd.Env = []string{"PI_OFFLINE=1", "NO_COLOR=1"}
				cmd.WaitDelay = 3 * time.Second
				var diagnostic boundedInitDiagnostic
				cmd.Stderr = &diagnostic
				stdin, err := cmd.StdinPipe()
				if err != nil {
					return err
				}
				defer stdin.Close()
				stdout, err := cmd.StdoutPipe()
				if err != nil {
					return err
				}
				defer stdout.Close()
				if err = cmd.Start(); err != nil {
					return err
				}
				waited := false
				defer func() {
					if !waited {
						cancel()
						_ = cmd.Wait()
					}
				}()
				if _, err = io.Copy(stdin, bytes.NewReader(initial)); err != nil {
					return err
				}
				request, err := readEntryFrame(stdout, limits, pisdkphasewire.PhysicalRequest)
				if err != nil {
					// Join the stderr copier before reading its bounded buffer.
					cancel()
					_ = cmd.Wait()
					waited = true
					return fmt.Errorf("entry physical frame: %w; fixed diagnostics=%s", err, diagnostic.String())
				}
				digest, err := cfg.Profile.Digest()
				if err != nil {
					return err
				}
				if pisdkphasewire.CompareBodyProfile(request, projection.Body(), digest) != nil {
					return fmt.Errorf("actual complete SDK body/profile mismatch")
				}
				// Explicit synthetic allowance only; real Source/hardF not asserted.
				use, err := pisdkphasewire.EncodeFrame(pisdkphasewire.Frame{Kind: pisdkphasewire.UseResult, Use: pisdkphasewire.UseAllowance{Allowed: true, RemainingNanoseconds: uint64(5 * time.Minute)}}, limits)
				if err != nil {
					return err
				}
				if _, err = io.Copy(stdin, bytes.NewReader(use)); err != nil {
					return err
				}
				final, err := readEntryFrame(stdout, limits, pisdkphasewire.StructuredFinal)
				if err != nil {
					return fmt.Errorf("entry final frame: %w", err)
				}
				if string(final.Body) != `{"answer":"native entry answer","artifacts":[]}` {
					return fmt.Errorf("raw provider final changed")
				}
				parsed, err := attachedworkeroutput.ParseStructuredFinalV1(ctx, final.Body, cfg.Profile.Limits, ledger)
				if err != nil || parsed.Answer != "native entry answer" {
					return fmt.Errorf("strict raw final parse: %w", err)
				}
				var extra [1]byte
				if n, err := stdout.Read(extra[:]); n != 0 || err != io.EOF {
					return fmt.Errorf("trailing stdout n=%d err=%v", n, err)
				}
				err = cmd.Wait()
				waited = true
				if err != nil {
					return fmt.Errorf("entry process: %w; fixed diagnostics=%s", err, diagnostic.String())
				}
				return nil
			})
		})
	}
}

func readEntryFrame(r io.Reader, limits pisdkphasewire.FrameLimits, kind byte) (pisdkphasewire.Frame, error) {
	var header [17]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return pisdkphasewire.Frame{}, err
	}
	h, err := pisdkphasewire.InspectHeader(header[:], limits)
	if err != nil {
		return pisdkphasewire.Frame{}, fmt.Errorf("invalid child header: %w", err)
	}
	if h.Kind != kind || h.Direction != pisdkphasewire.ChildToParent {
		return pisdkphasewire.Frame{}, fmt.Errorf("unexpected child header kind=%d direction=%d", h.Kind, h.Direction)
	}
	frame := make([]byte, int(h.TotalBytes))
	copy(frame, header[:])
	if _, err := io.ReadFull(r, frame[17:]); err != nil {
		return pisdkphasewire.Frame{}, err
	}
	return pisdkphasewire.DecodeFrame(frame, limits)
}

func entryPlatformUserAgent(t *testing.T) string {
	t.Helper()
	var name unix.Utsname
	if err := unix.Uname(&name); err != nil {
		t.Fatal(err)
	}
	var release strings.Builder
	for _, c := range name.Release {
		if c == 0 {
			break
		}
		release.WriteByte(byte(c))
	}
	arch := runtime.GOARCH
	if arch == "amd64" {
		arch = "x64"
	}
	return fmt.Sprintf("pi (%s %s; %s)", runtime.GOOS, release.String(), arch)
}
