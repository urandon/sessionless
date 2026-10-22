//go:build darwin || linux

package codexexec

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// A real original Session/claim, concrete native supervisor and owned shell
// fixture. The fixture emits JSONL but never connects to the conduit. There is
// no installed Codex, engine, model, real credential or network request.
func nativeResultFixture(t *testing.T, protocol string, exitCode string) (*PreparedProcessBoundaryV1, ProcessInvocationV1) {
	t.Helper()
	return nativeResultFixtureWithLauncher(t, protocol, exitCode, &companionProcessLauncher{ready: make(chan *companionProcessBoundary, 1)})
}

func nativeResultFixtureWithLauncher(t *testing.T, protocol, exitCode string, launcher attachedworkerdaemon.IsolationLauncherV2) (*PreparedProcessBoundaryV1, ProcessInvocationV1) {
	t.Helper()
	executable := writePreparedCodexFixture(t, "#!/bin/sh\nwhile IFS= read -r line; do :; done\nprintf '%s' '"+protocol+"'\nexit "+exitCode+"\n")
	now := time.Now().UTC() // One sampled wall-clock authority for this external-process fixture.
	f := newNativeConstructorFixtureAt(t, executable, nil, now)
	request, _ := validDriverRequestAt(t, now)
	if err := os.Mkdir(request.CredentialMaterialization.RootDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(request.CredentialMaterialization.FilePath, []byte(`{"synthetic":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	// Test names exceed Darwin's Unix-socket path limit. This owned short
	// root is still unique and removed on every test exit.
	scratch, err := os.MkdirTemp("", "nr-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(scratch) })
	scratch, err = filepath.EvalSymlinks(scratch)
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := attachedworkerdaemon.NewSupervisorV2(attachedworkerdaemon.SupervisorConfigV2{Base: attachedworkerdaemon.SupervisorConfig{
		ScratchRoot: scratch, Timeout: 10 * time.Second, TerminationGrace: time.Second,
		MaxStdoutBytes: maxJSONLOutputBytes, MaxStderrBytes: maxProcessStderrBytesV1,
		AllowedEnvironmentNames: []string{CredentialHomeEnvironmentV1}, AllowedReadRoots: []string{filepath.Dir(request.CredentialMaterialization.RootDir)}},
		Launcher: launcher})
	if err != nil {
		t.Fatal(err)
	}
	f.config.Supervisor = supervisor
	entry, err := NewNativePreparedProcessBoundaryV2(t.Context(), f.config, f.session, f.input, f.bundle)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = entry.native.child.Close() })
	job := f.input.Job
	identity := ports.ExecutionIdentity{TenantID: job.TenantID, OwnerUserID: job.CredentialOwnerUserID, RunID: job.RunID, AttemptID: job.AttemptID,
		ExecutionPlacementV2: job.ExecutionPlacementV2, HarnessBinding: job.HarnessBinding.Clone(), SubstrateBinding: job.SubstrateBinding, AdmissionCostCeiling: job.AdmissionCostCeiling}
	invocation := ProcessInvocationV1{Authority: f.config.Authority, Identity: identity, Credential: request.Credential,
		CredentialMaterialization: request.CredentialMaterialization, WorkDir: request.WorkDir,
		Executable: executable, ExecutableDigest: f.config.ExecutableDigest, Arguments: f.config.Arguments,
		CredentialHomeEnvironmentName: CredentialHomeEnvironmentV1, RequirePrivateWorkingDirectory: true, RequireSanitizedEnvironment: true,
		RequireNoAmbientHome: true, RequireNoAmbientAPIKey: true, RequireProviderEffectFence: true, MaxProviderEffects: 1,
		MaxStdoutBytes: maxJSONLOutputBytes, MaxStderrBytes: maxProcessStderrBytesV1, Stdin: []byte("synthetic instruction\n")}
	return entry, invocation
}

type nativeResultReleaseLauncher struct {
	err              error
	entered, proceed chan struct{}
	once             sync.Once
}

func (*nativeResultReleaseLauncher) ProfileV2() attachedworkerdaemon.IsolationProfileV2 {
	return (&nativeConstructorLauncher{}).ProfileV2()
}
func (l *nativeResultReleaseLauncher) PrepareV2(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV2) (attachedworkerdaemon.IsolationBoundaryV2, error) {
	base := &companionProcessLauncher{ready: make(chan *companionProcessBoundary, 1)}
	b, err := base.PrepareV2(ctx, e)
	if err != nil {
		return nil, err
	}
	return &nativeResultReleaseBoundary{IsolationBoundaryV2: b, owner: l}, nil
}

type nativeResultReleaseBoundary struct {
	attachedworkerdaemon.IsolationBoundaryV2
	owner *nativeResultReleaseLauncher
}

func (b *nativeResultReleaseBoundary) Release(ctx context.Context) error {
	err := b.IsolationBoundaryV2.Release(ctx)
	if b.owner.entered != nil {
		b.owner.once.Do(func() { close(b.owner.entered) })
		select {
		case <-b.owner.proceed:
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		}
	}
	return errors.Join(err, b.owner.err)
}

func TestNativeResultUnknownCleanupRejectsForgedPublicSuccess(t *testing.T) {
	launcher := &nativeResultReleaseLauncher{err: errors.New("synthetic boundary release failure")}
	entry, invocation := nativeResultFixtureWithLauncher(t, successfulJSONL, "0", launcher)
	process, runErr := entry.Run(t.Context(), invocation)
	defer clear(process.Stdout)
	if runErr == nil || process.original == nil || process.original.process.BoundaryReleased {
		t.Fatalf("cleanup failure not witnessed: %+v err=%v", process, runErr)
	}
	defer clear(process.original.protocol.final)
	forged := successfulDriverProcess([]byte(successfulJSONL))
	forged.original = process.original
	result, err := entry.reduceNativeResult(invocation.Identity.HarnessBinding, forged)
	defer clear(result.final)
	if err != nil || result.evidence.FinishClass == domain.ProviderFinishCompletedV1 || len(result.final) != 0 || result.failedClosed {
		t.Fatalf("unknown cleanup became publishable: evidence=%+v closed=%t final_bytes=%d err=%v", result.evidence, result.failedClosed, len(result.final), err)
	}
}

func TestNativeResultCancellationDuringOriginalTeardownWins(t *testing.T) {
	launcher := &nativeResultReleaseLauncher{entered: make(chan struct{}), proceed: make(chan struct{})}
	var proceedOnce sync.Once
	unblock := func() { proceedOnce.Do(func() { close(launcher.proceed) }) }
	t.Cleanup(unblock)
	entry, invocation := nativeResultFixtureWithLauncher(t, successfulJSONL, "0", launcher)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	type completed struct {
		result ProcessResultV1
		err    error
	}
	finished := make(chan completed, 1)
	joined := false
	go func() { result, err := entry.Run(ctx, invocation); finished <- completed{result, err} }()
	t.Cleanup(func() {
		unblock()
		cancel()
		if joined {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		select {
		case got := <-finished:
			clear(got.result.Stdout)
			if got.result.original != nil {
				clear(got.result.original.protocol.final)
			}
		case <-cleanup.Done():
			t.Error("owned cancelled native fixture did not join cleanup")
		}
	})
	select {
	case <-launcher.entered:
	case <-ctx.Done():
		t.Fatal("native workload did not enter original teardown")
	}
	if err := entry.Cancel(t.Context(), invocation.Authority); err != nil {
		t.Fatal(err)
	}
	unblock()
	var got completed
	select {
	case got = <-finished:
		joined = true
	case <-ctx.Done():
		t.Fatal("cancelled native teardown did not join")
	}
	defer clear(got.result.Stdout)
	if got.result.original == nil || !got.result.original.process.Cancelled || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("same-run cancellation witness missing: %+v err=%v", got.result, got.err)
	}
	defer clear(got.result.original.protocol.final)
	result, err := entry.reduceNativeResult(invocation.Identity.HarnessBinding, got.result)
	defer clear(result.final)
	if err != nil || result.evidence.FinishClass != domain.ProviderFinishCancelledV1 || len(result.final) != 0 || result.failedClosed {
		t.Fatalf("cancelled native turn became publishable: %+v closed=%t final_bytes=%d err=%v", result.evidence, result.failedClosed, len(result.final), err)
	}
}

func TestNativeResultUsesOriginalObservationWithoutDirectEffectFiction(t *testing.T) {
	entry, invocation := nativeResultFixture(t, successfulJSONL, "0")
	process, runErr := entry.Run(t.Context(), invocation)
	defer clear(process.Stdout)
	if runErr != nil || process.original == nil || process.original.native == nil {
		t.Fatalf("original native Run: process=%+v err=%v", process, runErr)
	}
	defer clear(process.original.protocol.final)
	if process.ProviderEffects != 0 || process.ProviderEffectFenceSatisfied || entry.native.child.Current() == nil {
		t.Fatal("native run fabricated a direct-effect witness or failed to retire its original child")
	}
	result, err := entry.reduceNativeResult(invocation.Identity.HarnessBinding, process)
	defer clear(result.final)
	if err != nil || result.evidence.FinishClass != domain.ProviderFinishCompletedV1 || string(result.final) != "bounded result" || result.failedClosed {
		t.Fatalf("native reduction: evidence=%+v final_bytes=%d failed_closed=%t err=%v", result.evidence, len(result.final), result.failedClosed, err)
	}
	if result.evidence.RouteState != domain.ProviderEvidenceUnknownV1 || result.evidence.UsageProvenance != domain.ProviderUsageUnknownV1 || result.evidence.PolicyVerdict != domain.ProviderPolicyConditionalV1 {
		t.Fatal("opaque execution manufactured route, usage or unconditional policy evidence")
	}
	// False public flags cannot negate or manufacture this private observation.
	public := ProcessResultV1{original: process.original, ExitCode: 99}
	if reduced, err := entry.reduceNativeResult(invocation.Identity.HarnessBinding, public); err != nil || reduced.evidence.FinishClass != domain.ProviderFinishCompletedV1 {
		clear(reduced.final)
		t.Fatalf("public flags replaced private observation: %v", err)
	} else {
		clear(reduced.final)
	}
	for _, kind := range []string{"no original", "copied original", "foreign binding", "reconstructed boundary"} {
		t.Run(kind, func(t *testing.T) {
			candidate, binding, boundary := process, invocation.Identity.HarnessBinding.Clone(), entry
			switch kind {
			case "no original":
				candidate.original = nil
			case "copied original":
				copy := *process.original
				candidate.original = &copy
			case "foreign binding":
				binding.Resource.Revision++
			case "reconstructed boundary":
				boundary = &PreparedProcessBoundaryV1{native: entry.native, nativeIdentity: entry.nativeIdentity, authority: entry.authority}
			}
			if result, err := boundary.reduceNativeResult(binding, candidate); err == nil || len(result.final) != 0 || result.failedClosed {
				clear(result.final)
				t.Fatalf("%s minted native evidence: %v", kind, err)
			}
		})
	}
	if _, err := entry.Run(t.Context(), invocation); !errors.Is(err, ErrProcessAlreadyConsumed) {
		t.Fatalf("replay=%v want already-consumed", err)
	}
}

func TestNativeResultDistinguishesClosedFailureFromLostOutcome(t *testing.T) {
	const start = "{\"type\":\"thread.started\"}\n{\"type\":\"turn.started\"}\n"
	const notice = "{\"type\":\"error\",\"message\":\"synthetic\"}\n"
	const failed = "{\"type\":\"turn.failed\",\"error\":{\"message\":\"synthetic\"}}\n"
	for _, tc := range []struct {
		name, protocol, exit string
		finish               domain.ProviderFinishClassV1
		closed               bool
	}{
		{name: "native failure", protocol: start + failed, exit: "1", finish: domain.ProviderFinishFailedV1, closed: true},
		{name: "failure with exit zero", protocol: start + notice + failed, exit: "0", finish: domain.ProviderFinishFailedV1, closed: true},
		{name: "partial final then failure", protocol: start + "{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"partial\"}}\n" + failed, exit: "1", finish: domain.ProviderFinishFailedV1, closed: true},
		{name: "error only", protocol: start + notice, exit: "1", finish: domain.ProviderFinishUnknownV1},
		{name: "lost reply", protocol: start, exit: "1", finish: domain.ProviderFinishUnknownV1},
		{name: "extra frame after failure", protocol: start + failed + notice, exit: "1", finish: domain.ProviderFinishFailedV1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry, invocation := nativeResultFixture(t, tc.protocol, tc.exit)
			process, _ := entry.Run(t.Context(), invocation)
			defer clear(process.Stdout)
			if process.original == nil || process.original.native == nil {
				t.Fatalf("no genuine native observation: %+v", process)
			}
			defer clear(process.original.protocol.final)
			result, err := entry.reduceNativeResult(invocation.Identity.HarnessBinding, process)
			defer clear(result.final)
			if err != nil || result.evidence.FinishClass != tc.finish || result.failedClosed != tc.closed || len(result.final) != 0 {
				t.Fatalf("native failure reduction: finish=%s closed=%t final_bytes=%d err=%v want=%s/%t", result.evidence.FinishClass, result.failedClosed, len(result.final), err, tc.finish, tc.closed)
			}
			if _, err := entry.Run(context.Background(), invocation); !errors.Is(err, ErrProcessAlreadyConsumed) {
				t.Fatalf("failed/unknown replay=%v want already-consumed", err)
			}
		})
	}
}
