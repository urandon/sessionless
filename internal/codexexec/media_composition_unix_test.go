//go:build darwin || linux

package codexexec

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func mediaCompositionFixture(t *testing.T, synthetic bool) (*PreparedAttachedWorkerRunnerV1, attachedworkerdaemon.Invocation, *preparedOwnerLifecycle, *fixtureIsolationLauncher) {
	t.Helper()
	text, invocation, lifecycle, launcher := preparedCompositionFixture(t, successfulJSONL)
	input := canonicalMediaFixture(t, false)
	if synthetic {
		// The actual supervised fixture reads BOTH original files and quoted
		// canonical stdin. Its fixed bytes comparison proves plumbing only.
		var script strings.Builder
		script.WriteString("#!/bin/sh\nimages=0\nfor argument in \"$@\"; do\ncase \"$argument\" in\n")
		for i, index := range []int{1, 3} {
			ext := "png"
			if i == 1 {
				ext = "jpg"
			}
			fmt.Fprintf(&script, "*/image-%02d.%s) printf '%%b' '", i+1, ext)
			for _, b := range input.CanonicalContext.Attachments[index].Body {
				fmt.Fprintf(&script, "\\%03o", b)
			}
			script.WriteString("' | /usr/bin/cmp \"$argument\" - || exit 81; images=$((images+1));;\n")
		}
		script.WriteString("esac\ndone\n[ \"$images\" -eq 2 ] || exit 82\nsaw_numeric=0\nwhile IFS= read -r line; do case \"$line\" in *90071992547409931234567890123*) saw_numeric=1;; esac; done\n[ \"$saw_numeric\" -eq 1 ] || exit 83\nprintf '%s' '{\"rotated\":true}' >\"$CODEX_HOME/auth.json\"\nprintf '%s' '")
		script.WriteString(successfulJSONL)
		script.WriteString("'\n")
		if err := os.WriteFile(text.config.Executable, []byte(script.String()), 0o700); err != nil {
			t.Fatal(err)
		}
		digest, err := attachedworkerdaemon.DigestExecutable(text.config.Executable)
		if err != nil {
			t.Fatal(err)
		}
		text.config.ExecutableDigest = digest
		invocation.Process.ExecutableDigest = digest
	}
	supervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{
		ScratchRoot: canonicalTempDir(t), Launcher: launcher, Timeout: 20 * time.Second, TerminationGrace: time.Second,
		MaxStdoutBytes: maxJSONLOutputBytes, MaxStderrBytes: maxProcessStderrBytesV1, AllowedEnvironmentNames: []string{CredentialHomeEnvironmentV1},
		AllowedReadRoots: []string{filepath.Dir(lifecycle.materialized.RootDir), text.scratch},
	})
	if err != nil {
		t.Fatal(err)
	}
	config := MediaConfigV1{Backend: text.config}
	if synthetic {
		config.Backend.Model = "sessionless-media-fixture"
		config.fixture = true
		config.Native.GuardProtocol = "sessionless.synthetic-native-media.v1"
		config.Native.GuardArtifactDigest = hex.EncodeToString(config.Backend.ExecutableDigest[:])
	}
	// This test-only factory pins a synthetic descriptor. It cannot register a
	// real media job or return native preparation/provider-guard evidence.
	runner, err := NewPreparedMediaAttachedWorkerRunnerV1(config, text.scratch, supervisor, lifecycle, attachedworkerdaemon.InvocationRunnerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	base := invocation.VerifiedInput
	// Keep original real-prepared process/credential and accepted authority;
	// only the canonical fixture media history changes.
	input.AcceptedAttempt = base.AcceptedAttempt
	input.Job.HarnessBinding = base.Job.HarnessBinding
	input.Job.HarnessBinding.Backend, err = config.DescriptorV1()
	if err != nil {
		t.Fatal(err)
	}
	input.Job.HarnessBinding.ModelID = config.Backend.Model
	input.Job.ExecutionPlacementV2 = base.Job.ExecutionPlacementV2
	input.Job.ReservationID = base.Job.ReservationID
	input.Job.CredentialOwnerUserID = base.Job.CredentialOwnerUserID
	input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	invocation.VerifiedInput = input
	invocation.Credential.IssueRequest.Run.TriggerEventID = input.Job.TriggerEventID
	invocation.Process.Arguments = processArguments(config.Backend.Model)
	if err = input.ValidateFor(invocation.Identity); err != nil {
		t.Fatalf("synthetic canonical admitted fixture=%v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runner.Close(ctx); err != nil {
			t.Errorf("media close: %v", err)
		}
	})
	return runner, invocation, lifecycle, launcher
}

func TestMediaProductionDeclarationCannotGrantIssue(t *testing.T) {
	runner, invocation, lifecycle, launcher := mediaCompositionFixture(t, false)
	result, err := runner.Run(context.Background(), invocation)
	if !errors.Is(err, ErrMediaNativeGuardUnavailable) || result.Succeeded(err) || len(lifecycle.order) != 0 || launcher.prepareCount() != 0 {
		t.Fatalf("unknown native enforcement result=%+v err=%v lifecycle=%v launches=%d", result, err, lifecycle.order, launcher.prepareCount())
	}
	entries, err := os.ReadDir(runner.scratch)
	if err != nil || len(entries) != 0 {
		t.Fatalf("ineligible source caused staging effects=%v err=%v", entries, err)
	}
}

func TestMediaSyntheticCompositionSameOwnerSupervisorReceipt(t *testing.T) {
	runner, invocation, lifecycle, launcher := mediaCompositionFixture(t, true)
	result, err := runner.Run(context.Background(), invocation)
	if !result.Succeeded(err) || launcher.prepareCount() != 1 || !reflect.DeepEqual(lifecycle.order, []string{"issue", "materialize", "writeback", "release"}) {
		t.Fatalf("synthetic actual supervised result=%+v err=%v lifecycle=%v launches=%d", result, err, lifecycle.order, launcher.prepareCount())
	}
	launch := launcher.snapshot()
	var images []string
	for i, arg := range launch.Arguments {
		if arg == "--image" {
			images = append(images, launch.Arguments[i+1])
		}
	}
	if len(images) != 2 || filepath.Base(images[0]) != "image-01.png" || filepath.Base(images[1]) != "image-02.jpg" {
		t.Fatalf("canonical image argv order=%v", images)
	}
	if strings.Contains(launch.Directory, "codex-history-") || len(launch.ReadRoots) != 3 {
		t.Fatalf("supervisor cwd/read roots changed=%+v", launch)
	}
	candidate, candidateErr := runner.ReceiptCandidate(invocation.Identity, result, err, domain.AttachedWorkerTerminalSucceeded)
	if candidateErr != nil || candidate.Summary != "bounded result" || len(candidate.Artifacts) != 0 {
		t.Fatalf("same-run finalized answer-only candidate=%+v err=%v", candidate, candidateErr)
	}
	if _, retryErr := runner.Run(context.Background(), invocation); retryErr == nil || launcher.prepareCount() != 1 || len(lifecycle.order) != 4 {
		t.Fatal("media lifecycle replayed")
	}
	entries, readErr := os.ReadDir(runner.scratch)
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("owned media residual=%v err=%v", entries, readErr)
	}
}

func TestMediaProfileFreezesPolicyAndSeparatesTextFixture(t *testing.T) {
	text := Config{Executable: "/opt/sessionless/codex", ExecutableVersion: "fixture-v1", ExecutableDigest: attachedworkerdaemon.ExecutableDigest{1}, Model: "sessionless-media-fixture"}
	media := MediaConfigV1{Backend: text, Native: NativeMediaPolicyV1{Configuration: []byte(`{"closed":true}`), ModelMetadata: []byte(`{"model":"pending"}`)}}
	frozen := media.Clone()
	descriptor, err := frozen.DescriptorV1()
	if err != nil {
		t.Fatal(err)
	}
	media.Native.Configuration[0] ^= 1
	still, err := frozen.DescriptorV1()
	if err != nil || still != descriptor {
		t.Fatal("caller mutation changed frozen native install policy")
	}
	old, err := prepareConfig(text)
	if err != nil || old == descriptor {
		t.Fatal("media silently widened legacy descriptor")
	}
	frozen.fixture = true
	frozen.Native.GuardProtocol = "sessionless.synthetic-native-media.v1"
	frozen.Native.GuardArtifactDigest = hex.EncodeToString(text.ExecutableDigest[:])
	fixture, err := frozen.DescriptorV1()
	if err != nil || fixture == descriptor || fixture == old {
		t.Fatal("synthetic artifact acquired production media descriptor")
	}
	manifest, err := frozen.ConformanceManifestV1()
	if err != nil || !strings.HasPrefix(manifest.Status, "NO-GO") || len(manifest.Tasks) != 8 || len(manifest.Missing) < 7 || manifest.Limits.ProviderTurns != 8 || manifest.Limits.MaximumCostUSDMicros != 5_000_000 {
		t.Fatalf("fixture became native conformance authority: manifest=%+v err=%v", manifest, err)
	}
}

func TestMediaUnknownNativeGuardCannotRegisterEnabledDriver(t *testing.T) {
	config := MediaConfigV1{Backend: Config{Executable: "/opt/sessionless/codex", ExecutableVersion: "pending-release", ExecutableDigest: attachedworkerdaemon.ExecutableDigest{1}, Model: "pending-vision-model"}}
	driver, err := NewDisabledMediaDriverV1(config, &fixtureAuthorityResolver{}, &fixtureProcessBoundary{})
	if err != nil {
		t.Fatal(err)
	}
	registration, err := DisabledRegistrationV1(driver)
	if err != nil || registration.Enabled || registration.Descriptor != driver.DescriptorV1() {
		t.Fatalf("disabled exact media registration=%+v err=%v", registration, err)
	}
	config.Backend.Enabled = true
	if _, err := NewDisabledMediaDriverV1(config, &fixtureAuthorityResolver{}, &fixtureProcessBoundary{}); !errors.Is(err, ErrMediaNativeGuardUnavailable) {
		t.Fatalf("declared unknown enabled artifact error=%v", err)
	}
}
