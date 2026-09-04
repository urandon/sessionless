//go:build darwin || linux

package serverlessisolation

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

type fixtureLauncher struct {
	mu         sync.Mutex
	allocation domain.PreparedAllocationV1
	events     []string
	prepares   int
	residue    ResidueProofV1
	residueErr error
	taint      TaintRequestV1
	taintErr   error
	releaseErr error
	output     []byte
}

func (*fixtureLauncher) Profile() attachedworkerdaemon.IsolationProfile {
	return attachedworkerdaemon.IsolationProfile{
		Name: "serverless-fixture", FilesystemReadBoundary: true,
		FilesystemWriteBoundary: true, NetworkDenied: true, ProcessBoundary: true,
		DiskBytesBounded: true,
	}
}

func (launcher *fixtureLauncher) Preflight(context.Context, domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV1, error) {
	return launcher.allocation.Clone(), nil
}

func (launcher *fixtureLauncher) Prepare(_ context.Context, spec attachedworkerdaemon.LaunchSpec) (attachedworkerdaemon.IsolationBoundary, error) {
	launcher.mu.Lock()
	launcher.prepares++
	launcher.events = append(launcher.events, "prepare")
	launcher.mu.Unlock()
	command := exec.Command(spec.Executable, spec.Arguments...)
	command.Dir = spec.Directory
	command.Env = append([]string(nil), spec.Environment...)
	return &fixtureBoundary{
		command: command, launcher: launcher,
		attestation: attachedworkerdaemon.WorkloadAttestation{
			Executable: spec.Executable, ExecutableDigest: spec.ExecutableDigest,
			Arguments: append([]string(nil), spec.Arguments...),
		},
	}, nil
}

func (launcher *fixtureLauncher) VerifyResidue(ctx context.Context, _ ResidueRequestV1) (ResidueProofV1, error) {
	launcher.record("residue")
	if err := ctx.Err(); err != nil {
		return ResidueProofV1{}, err
	}
	return launcher.residue, launcher.residueErr
}

func (launcher *fixtureLauncher) Taint(ctx context.Context, request TaintRequestV1) error {
	launcher.mu.Lock()
	defer launcher.mu.Unlock()
	launcher.events = append(launcher.events, "taint")
	launcher.taint = request
	if err := ctx.Err(); err != nil {
		return err
	}
	return launcher.taintErr
}

func (launcher *fixtureLauncher) record(event string) {
	launcher.mu.Lock()
	defer launcher.mu.Unlock()
	launcher.events = append(launcher.events, event)
}

func (launcher *fixtureLauncher) snapshot() ([]string, int, TaintRequestV1) {
	launcher.mu.Lock()
	defer launcher.mu.Unlock()
	return append([]string(nil), launcher.events...), launcher.prepares, launcher.taint
}

func (launcher *fixtureLauncher) captureOutput(value []byte) {
	launcher.mu.Lock()
	defer launcher.mu.Unlock()
	launcher.output = append([]byte(nil), value...)
}

func (launcher *fixtureLauncher) outputSnapshot() []byte {
	launcher.mu.Lock()
	defer launcher.mu.Unlock()
	return append([]byte(nil), launcher.output...)
}

type fixtureBoundary struct {
	command     *exec.Cmd
	launcher    *fixtureLauncher
	attestation attachedworkerdaemon.WorkloadAttestation
}

func (boundary *fixtureBoundary) Command() *exec.Cmd { return boundary.command }
func (boundary *fixtureBoundary) AttestedWorkload() attachedworkerdaemon.WorkloadAttestation {
	return attachedworkerdaemon.WorkloadAttestation{
		Executable:       boundary.attestation.Executable,
		ExecutableDigest: boundary.attestation.ExecutableDigest,
		Arguments:        append([]string(nil), boundary.attestation.Arguments...),
	}
}
func (*fixtureBoundary) GracefulStop(context.Context) error  { return nil }
func (*fixtureBoundary) ForceStop(context.Context) error     { return nil }
func (*fixtureBoundary) Alive(context.Context) (bool, error) { return false, nil }
func (boundary *fixtureBoundary) Release(context.Context) error {
	boundary.launcher.record("release")
	return boundary.launcher.releaseErr
}

type fixtureOutputs struct{ launcher *fixtureLauncher }

func (outputs fixtureOutputs) FinalizeOutputs(_ context.Context, request FinalizationRequestV1) (OutputFinalizationProofV1, error) {
	outputs.launcher.record("output")
	outputs.launcher.captureOutput(request.Process.Stdout)
	return OutputFinalizationProofV1{Finalized: true, NativeEventCount: 1, ArtifactBytes: 1, EvidenceBytes: 1}, nil
}

type fixtureCredentials struct{ launcher *fixtureLauncher }

func (credentials fixtureCredentials) FinalizeCredentials(context.Context, FinalizationRequestV1) (domain.CredentialFinalizationStateV1, error) {
	credentials.launcher.record("credential")
	return domain.CredentialFinalizationVerifiedV1, nil
}

type failingOutputs struct{ launcher *fixtureLauncher }

func (outputs failingOutputs) FinalizeOutputs(context.Context, FinalizationRequestV1) (OutputFinalizationProofV1, error) {
	outputs.launcher.record("output")
	return OutputFinalizationProofV1{}, errors.New("fixture output failure")
}

type oversizedOutputs struct{ launcher *fixtureLauncher }

func (outputs oversizedOutputs) FinalizeOutputs(context.Context, FinalizationRequestV1) (OutputFinalizationProofV1, error) {
	outputs.launcher.record("output")
	return OutputFinalizationProofV1{Finalized: true, NativeEventCount: 129, ArtifactBytes: 1, EvidenceBytes: 1}, nil
}

type failingCredentials struct{ launcher *fixtureLauncher }

func (credentials failingCredentials) FinalizeCredentials(context.Context, FinalizationRequestV1) (domain.CredentialFinalizationStateV1, error) {
	credentials.launcher.record("credential")
	return domain.CredentialFinalizationUnknownV1, errors.New("fixture credential failure")
}

type deadlineCredentials struct{ launcher *fixtureLauncher }

func (credentials deadlineCredentials) FinalizeCredentials(ctx context.Context, _ FinalizationRequestV1) (domain.CredentialFinalizationStateV1, error) {
	credentials.launcher.record("credential")
	<-ctx.Done()
	return domain.CredentialFinalizationUnknownV1, ctx.Err()
}

func TestSupervisorExactAttestationAndOrderedFinalization(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	prepared, issuer, allocation, now := childPreparedFixture(t, fixture)
	launcher := newFixtureLauncher(allocation, now)
	supervisor := newFixtureSupervisor(t, launcher, issuer, now)

	observed, err := supervisor.Preflight(context.Background(), prepared.Authority())
	if err != nil || !reflect.DeepEqual(observed, allocation) {
		t.Fatalf("preflight = %+v, %v", observed, err)
	}
	result, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	if err != nil || !result.StopProof.Verified() || !result.Output.Finalized ||
		result.CredentialFinalization != domain.CredentialFinalizationVerifiedV1 ||
		!result.Residue.Verified() || result.Cleanup != domain.SubstrateCleanupVerifiedV1 || result.TaintRequested ||
		len(result.Process.Stdout) != 0 || !bytes.Equal(launcher.outputSnapshot(), []byte("fixture")) {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	events, prepares, _ := launcher.snapshot()
	if prepares != 1 || !reflect.DeepEqual(events, []string{"prepare", "release", "output", "credential", "residue"}) {
		t.Fatalf("lifecycle events = %#v, prepares = %d", events, prepares)
	}
}

func TestSupervisorKeepsCleanProcessFailureSeparateFromCleanup(t *testing.T) {
	t.Parallel()
	fixture := executableFixtureBody(t, "#!/bin/sh\nexit 42\n")
	prepared, issuer, allocation, now := childPreparedFixture(t, fixture)
	launcher := newFixtureLauncher(allocation, now)
	supervisor := newFixtureSupervisor(t, launcher, issuer, now)

	result, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	if !errors.Is(err, ErrProcess) || result.Process.ExitCode != 42 ||
		result.Cleanup != domain.SubstrateCleanupVerifiedV1 || result.TaintRequested {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
}

func TestSupervisorBoundsTimeoutAndVerifiesCleanup(t *testing.T) {
	t.Parallel()
	fixture := executableFixtureBody(t, "#!/bin/sh\ntrap '' TERM\nwhile :; do :; done\n")
	prepared, issuer, allocation, now := childPreparedFixtureWithLimits(t, fixture, 100*time.Millisecond, 200*time.Millisecond, 1<<20, 1<<20)
	launcher := newFixtureLauncher(allocation, now)
	supervisor := newFixtureSupervisor(t, launcher, issuer, now)

	result, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	if !errors.Is(err, ErrProcess) || !result.Process.Deadline ||
		!result.StopProof.Verified() || result.Cleanup != domain.SubstrateCleanupVerifiedV1 || result.TaintRequested {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
}

func TestSupervisorCancelsTermResistantDescendantAndVerifiesCleanup(t *testing.T) {
	t.Parallel()
	marker := filepath.Join(canonicalTempDir(t), "started")
	fixture := executableFixtureBody(t, fmt.Sprintf(
		"#!/bin/sh\ntrap '' TERM\n( trap '' TERM; while :; do /bin/sleep 1; done ) &\nprintf ready > %s\nwait\n",
		shellQuote(marker),
	))
	prepared, issuer, allocation, now := childPreparedFixtureWithLimits(t, fixture, 10*time.Second, 200*time.Millisecond, 1<<20, 1<<20)
	launcher := newFixtureLauncher(allocation, now)
	supervisor := newFixtureSupervisor(t, launcher, issuer, now)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		result RunResultV1
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := supervisor.Run(ctx, RunSpecV1{
			Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
		})
		done <- outcome{result: result, err: err}
	}()
	waitForPath(t, marker, 30*time.Second)
	cancel()
	select {
	case observed := <-done:
		if !errors.Is(observed.err, ErrProcess) || !observed.result.Process.Cancelled ||
			!observed.result.StopProof.Verified() || observed.result.Cleanup != domain.SubstrateCleanupVerifiedV1 ||
			observed.result.TaintRequested {
			t.Fatalf("result = %+v, err = %v", observed.result, observed.err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("supervisor did not finish after cancellation")
	}
}

func TestSupervisorBoundsOutputAndStillVerifiesCleanup(t *testing.T) {
	t.Parallel()
	fixture := executableFixtureBody(t, "#!/bin/sh\nprintf '"+strings.Repeat("x", 256)+"'\n")
	prepared, issuer, allocation, now := childPreparedFixtureWithLimits(t, fixture, 30*time.Second, 200*time.Millisecond, 128, 128)
	launcher := newFixtureLauncher(allocation, now)
	supervisor := newFixtureSupervisor(t, launcher, issuer, now)

	result, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	if !errors.Is(err, ErrProcess) || result.Process.FailureCode != "stdout_limit_exceeded" ||
		result.Process.StdoutBytes != 128 || !result.StopProof.Verified() ||
		result.Cleanup != domain.SubstrateCleanupVerifiedV1 || result.TaintRequested {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
}

func TestSupervisorRejectsSubstitutionBeforePrepare(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	prepared, issuer, allocation, now := childPreparedFixture(t, fixture)
	launcher := newFixtureLauncher(allocation, now)
	supervisor := newFixtureSupervisor(t, launcher, issuer, now)

	_, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--substituted"}, Stdin: []byte("input"),
	})
	_, prepares, _ := launcher.snapshot()
	if !errors.Is(err, ErrAttestation) || prepares != 0 {
		t.Fatalf("substitution error = %v, prepares = %d", err, prepares)
	}
}

func TestSupervisorRejectsPreparedCapabilityAfterIssuerRestart(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	prepared, _, allocation, now := childPreparedFixture(t, fixture)
	launcher := newFixtureLauncher(allocation, now)
	restartedIssuer, err := serverlessharness.NewCapabilityIssuer(
		func() time.Time { return now }, bytes.NewReader(bytes.Repeat([]byte{8}, 32)),
	)
	if err != nil {
		t.Fatal(err)
	}
	supervisor := newFixtureSupervisor(t, launcher, restartedIssuer, now)
	_, err = supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	_, prepares, _ := launcher.snapshot()
	if !errors.Is(err, ErrAuthority) || prepares != 0 {
		t.Fatalf("restart error = %v, prepares = %d", err, prepares)
	}
}

func TestSupervisorRejectsSymlinkScratchRoot(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	_, issuer, allocation, now := childPreparedFixture(t, fixture)
	launcher := newFixtureLauncher(allocation, now)
	link := filepath.Join(t.TempDir(), "scratch-link")
	if err := os.Symlink(canonicalTempDir(t), link); err != nil {
		t.Fatal(err)
	}
	_, err := NewSupervisorV1(SupervisorConfigV1{
		ScratchRoot: link, Launcher: launcher, Validator: issuer,
		Outputs: fixtureOutputs{launcher: launcher}, Credentials: fixtureCredentials{launcher: launcher},
		Clock: func() time.Time { return now },
	})
	if !errors.Is(err, ErrConfig) {
		t.Fatalf("symlink scratch root error = %v", err)
	}
}

func TestPreflightRejectsEveryAttestationSubstitution(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	prepared, issuer, allocation, now := childPreparedFixture(t, fixture)
	mutations := map[string]func(*domain.PreparedAllocationV1){
		"image":         func(value *domain.PreparedAllocationV1) { value.ObservedImageDigest = digestText("wrong-image") },
		"outer harness": func(value *domain.PreparedAllocationV1) { value.ObservedOuterHarnessDigest = digestText("wrong-outer") },
		"proxy artifact": func(value *domain.PreparedAllocationV1) {
			value.ObservedProxyArtifactDigest = digestText("wrong-proxy")
		},
		"proxy identity": func(value *domain.PreparedAllocationV1) {
			value.ObservedProxyIdentityDigest = digestText("wrong-identity")
		},
		"executable": func(value *domain.PreparedAllocationV1) {
			value.ChildProcess.ExecutableDigest = digestText("wrong-executable")
		},
		"protocol": func(value *domain.PreparedAllocationV1) { value.ChildProcess.NativeProtocol = "wrong-v1" },
		"backend profile": func(value *domain.PreparedAllocationV1) {
			value.ChildProcess.BackendProfileDigest = digestText("wrong-backend")
		},
	}
	for name, mutate := range mutations {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			candidate := allocation.Clone()
			mutate(&candidate)
			launcher := newFixtureLauncher(candidate, now)
			supervisor := newFixtureSupervisor(t, launcher, issuer, now)
			_, err := supervisor.Preflight(context.Background(), prepared.Authority())
			_, prepares, _ := launcher.snapshot()
			if !errors.Is(err, ErrAttestation) || prepares != 0 {
				t.Fatalf("preflight error = %v, prepares = %d", err, prepares)
			}
		})
	}
}

func TestOutputFailureBlocksResultWithoutTaintingCleanInstance(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	prepared, issuer, allocation, now := childPreparedFixture(t, fixture)
	launcher := newFixtureLauncher(allocation, now)
	supervisor, err := NewSupervisorV1(SupervisorConfigV1{
		ScratchRoot: canonicalTempDir(t), Launcher: launcher, Validator: issuer,
		Outputs: failingOutputs{launcher: launcher}, Credentials: fixtureCredentials{launcher: launcher},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	if !errors.Is(err, ErrOutputFinalization) || result.Output.Finalized ||
		result.Cleanup != domain.SubstrateCleanupVerifiedV1 || result.TaintRequested {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
}

func TestOversizedOutputProofBlocksResultWithoutTaintingCleanInstance(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	prepared, issuer, allocation, now := childPreparedFixture(t, fixture)
	launcher := newFixtureLauncher(allocation, now)
	supervisor, err := NewSupervisorV1(SupervisorConfigV1{
		ScratchRoot: canonicalTempDir(t), Launcher: launcher, Validator: issuer,
		Outputs: oversizedOutputs{launcher: launcher}, Credentials: fixtureCredentials{launcher: launcher},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	if !errors.Is(err, ErrOutputFinalization) || result.Output.Finalized ||
		result.Cleanup != domain.SubstrateCleanupVerifiedV1 || result.TaintRequested {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
}

func TestSupervisorTaintsAfterCredentialFinalizationFailure(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	prepared, issuer, allocation, now := childPreparedFixture(t, fixture)
	launcher := newFixtureLauncher(allocation, now)
	supervisor, err := NewSupervisorV1(SupervisorConfigV1{
		ScratchRoot: canonicalTempDir(t), Launcher: launcher, Validator: issuer,
		Outputs: fixtureOutputs{launcher: launcher}, Credentials: failingCredentials{launcher: launcher},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	_, _, taint := launcher.snapshot()
	if !errors.Is(err, ErrCredentialFinalize) || result.Cleanup != domain.SubstrateCleanupFailedV1 ||
		!result.TaintRequested || !result.TaintConfirmed ||
		!reflect.DeepEqual(taint.Reasons, []TaintReasonV1{TaintCredentialFinalizeFailedV1}) {
		t.Fatalf("result = %+v, taint = %+v, err = %v", result, taint, err)
	}
}

func TestSupervisorUsesFreshEmergencyContextForTaint(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	prepared, issuer, allocation, now := childPreparedFixtureWithLimits(t, fixture, 5*time.Minute, 50*time.Millisecond, 1<<20, 1<<20)
	launcher := newFixtureLauncher(allocation, now)
	supervisor, err := NewSupervisorV1(SupervisorConfigV1{
		ScratchRoot: canonicalTempDir(t), Launcher: launcher, Validator: issuer,
		Outputs: fixtureOutputs{launcher: launcher}, Credentials: deadlineCredentials{launcher: launcher},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	_, _, taint := launcher.snapshot()
	wantReasons := []TaintReasonV1{TaintCredentialFinalizeFailedV1, TaintResidueUnknownV1}
	if !errors.Is(err, ErrCredentialFinalize) || !errors.Is(err, ErrCleanup) ||
		result.Cleanup != domain.SubstrateCleanupFailedV1 || !result.TaintRequested || !result.TaintConfirmed ||
		!reflect.DeepEqual(taint.Reasons, wantReasons) {
		t.Fatalf("result = %+v, taint = %+v, err = %v", result, taint, err)
	}
}

func TestSupervisorTaintsAfterBoundaryReleaseFailure(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	prepared, issuer, allocation, now := childPreparedFixture(t, fixture)
	launcher := newFixtureLauncher(allocation, now)
	launcher.releaseErr = errors.New("fixture release failure")
	supervisor := newFixtureSupervisor(t, launcher, issuer, now)

	result, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	_, _, taint := launcher.snapshot()
	if !errors.Is(err, ErrProcess) || result.StopProof.Verified() ||
		result.Cleanup != domain.SubstrateCleanupFailedV1 || !result.TaintRequested || !result.TaintConfirmed ||
		!reflect.DeepEqual(taint.Reasons, []TaintReasonV1{TaintProcessStopUnknownV1}) {
		t.Fatalf("result = %+v, taint = %+v, err = %v", result, taint, err)
	}
}

func TestSupervisorTaintsVerifiedProcessAfterResidueFailure(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	prepared, issuer, allocation, now := childPreparedFixture(t, fixture)
	launcher := newFixtureLauncher(allocation, now)
	launcher.residue.SocketsAbsent = false
	supervisor := newFixtureSupervisor(t, launcher, issuer, now)

	result, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	events, _, taint := launcher.snapshot()
	if !errors.Is(err, ErrCleanup) || !result.StopProof.Verified() ||
		result.Cleanup != domain.SubstrateCleanupFailedV1 || !result.TaintRequested || !result.TaintConfirmed {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
	if !reflect.DeepEqual(events, []string{"prepare", "release", "output", "credential", "residue", "taint"}) ||
		!reflect.DeepEqual(taint.Reasons, []TaintReasonV1{TaintResidueUnknownV1}) {
		t.Fatalf("events = %#v, taint = %+v", events, taint)
	}
}

func TestSupervisorReportsUnconfirmedTaint(t *testing.T) {
	t.Parallel()
	fixture := executableFixture(t)
	prepared, issuer, allocation, now := childPreparedFixture(t, fixture)
	launcher := newFixtureLauncher(allocation, now)
	launcher.residueErr = errors.New("fixture residue inspection failure")
	launcher.taintErr = errors.New("fixture taint failure")
	supervisor := newFixtureSupervisor(t, launcher, issuer, now)

	result, err := supervisor.Run(context.Background(), RunSpecV1{
		Prepared: prepared, Executable: fixture, Arguments: []string{"--fixture"}, Stdin: []byte("input"),
	})
	if !errors.Is(err, ErrCleanup) || !errors.Is(err, ErrTaintNotConfirmed) ||
		result.Cleanup != domain.SubstrateCleanupFailedV1 || !result.TaintRequested || result.TaintConfirmed {
		t.Fatalf("result = %+v, err = %v", result, err)
	}
}

func newFixtureLauncher(allocation domain.PreparedAllocationV1, now time.Time) *fixtureLauncher {
	return &fixtureLauncher{
		allocation: allocation,
		residue: ResidueProofV1{
			WorkspaceAbsent: true, ProcessesAbsent: true, SocketsAbsent: true,
			CredentialsAbsent: true, VerifiedAt: now.Add(time.Second),
		},
	}
}

func newFixtureSupervisor(t testing.TB, launcher *fixtureLauncher, issuer *serverlessharness.CapabilityIssuer, now time.Time) *SupervisorV1 {
	t.Helper()
	supervisor, err := NewSupervisorV1(SupervisorConfigV1{
		ScratchRoot: canonicalTempDir(t), Launcher: launcher, Validator: issuer,
		Outputs: fixtureOutputs{launcher: launcher}, Credentials: fixtureCredentials{launcher: launcher},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return supervisor
}

func childPreparedFixture(t testing.TB, executable string) (serverlessharness.PreparedInvocation, *serverlessharness.CapabilityIssuer, domain.PreparedAllocationV1, time.Time) {
	t.Helper()
	return childPreparedFixtureWithLimits(t, executable, 5*time.Minute, time.Minute, 1<<20, 1<<20)
}

func childPreparedFixtureWithLimits(
	t testing.TB,
	executable string,
	executionTimeout time.Duration,
	cleanupTimeout time.Duration,
	stdoutBytes uint64,
	stderrBytes uint64,
) (serverlessharness.PreparedInvocation, *serverlessharness.CapabilityIssuer, domain.PreparedAllocationV1, time.Time) {
	t.Helper()
	now := time.Unix(100, 0).UTC()
	digest, err := attachedworkerdaemon.DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	artifactDigest := hex.EncodeToString(digest[:])
	zero := uint64(0)
	limits := domain.SubstrateLimitsV1{
		InvocationTimeout: time.Hour, ExecutionTimeout: executionTimeout, CleanupTimeout: cleanupTimeout,
		CPUMillis: 1000, MemoryBytes: 256 << 20, ScratchBytes: 64 << 20,
		StdoutBytes: stdoutBytes, StderrBytes: stderrBytes, NativeEventCount: 128, ArtifactBytes: 1 << 20,
	}
	cost := domain.AdmissionCostCeilingV1{
		Version: domain.AdmissionCostCeilingVersionV1, Currency: "USD", PriceRevision: "fixture-v1",
		PriceObservedAt: now.Add(-time.Minute), PriceExpiresAt: now.Add(time.Hour), MaxDeliveries: 2,
		MaxPreEffectDurationPerDelivery: time.Minute, MaxActiveDuration: limits.ExecutionTimeout,
		MaxCleanupAndReconcileDuration: limits.CleanupTimeout, ConfiguredMemoryBytes: limits.MemoryBytes,
		ConfiguredVCPUMillis: limits.CPUMillis, MaxIngressBytes: limits.ArtifactBytes,
		MaxEgressBytes: limits.ArtifactBytes, MaxLogBytes: limits.StdoutBytes + limits.StderrBytes,
		MaxEvidenceBytes: limits.StdoutBytes, SubstratePriceState: domain.CostEvidenceKnownV1,
		ProviderPriceState: domain.ProviderPriceKnownFreeV1, MaxSubstrateAmountMicrounits: &zero,
		MaxProviderAmountMicrounits: &zero, MaxTotalAmountMicrounits: &zero,
	}
	costDigest, err := cost.Digest()
	if err != nil {
		t.Fatal(err)
	}
	substrate := domain.SubstrateBindingV1{
		Version: domain.SubstrateBindingVersionV1, Kind: domain.SubstrateDeterministicFixtureV1,
		ProfileID: "child-fixture-v1", ProfileRevision: 1, ProfileDigest: digestText("profile"),
		ProfileEvidenceExpiresAt: now.Add(time.Hour), Region: "local-fixture",
		ImageDigest: digestText("image"), OuterHarnessArtifactDigest: digestText("outer"),
		WorkloadMode: domain.SubstrateWorkloadChildProcessV1, IsolationProfileDigest: digestText("isolation"),
		EgressPolicyDigest: digestText("egress-denied"), CleanupPolicyDigest: digestText("cleanup"),
		EgressProxyArtifactDigest: digestText("proxy"), EgressProxyIdentityDigest: digestText("identity"),
		AdmissionCostCeilingDigest: costDigest, Limits: limits,
	}
	substrateDigest, err := substrate.Digest()
	if err != nil {
		t.Fatal(err)
	}
	placement, err := domain.ManagedExecutionPlacementV2(string(substrateDigest))
	if err != nil {
		t.Fatal(err)
	}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	evidenceExpiry := now.Add(time.Hour)
	backend := domain.HarnessBackendDescriptorV1{
		HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "fixture-v1",
		BackendKind: domain.HarnessBackendCodexExecV1, ArtifactKind: domain.HarnessArtifactExecutableV1,
		ArtifactDigest: artifactDigest, NativeProtocolVersion: "fixture-jsonl-v1",
		BackendProfileDigest: digestText("backend-profile"), ProviderContractKind: domain.ProviderContractInvocationV1,
		CredentialDeliveryKind: domain.ProviderCredentialDeliveryFileV1,
	}
	binding := domain.HarnessBindingV1{
		Version: domain.HarnessBindingVersionV1, TenantID: "tenant-1", OwnerUserID: "user-1",
		RunID: "run-1", AttemptID: "attempt-1", Backend: backend,
		Resource: domain.ProviderResourceBindingV1{
			Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-1", OwnerUserID: "user-1",
			Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1,
		},
		ModelVendorID: "openai", ModelID: "fixture-model", InputDataClass: domain.ProviderDataPrivateV1,
		ProviderCatalogDigest: digestText("catalog"), ProviderRouteDigest: digestText("route"),
		PrivacyPolicyDigest: digestText("privacy"), CapabilityEvidenceDigest: digestText("capability"),
		EffectivePolicyDigest: digestText("policy"), ExecutionPlacementDigest: string(placementDigest),
		EvidenceExpiresAt: &evidenceExpiry,
	}
	authority := domain.ServerlessInvocationAuthorityV1{
		Version: domain.ServerlessInvocationAuthorityVersionV1, HarnessBinding: binding,
		ExecutionPlacementV2: placement, SubstrateBinding: substrate, AdmissionCostCeiling: cost,
		Lease: domain.Lease{
			ID: "lease-1", TenantID: "tenant-1", RunID: "run-1", AttemptID: "attempt-1",
			WorkerID: "managed-worker-1", FenceToken: 1, AcquiredAt: now.Add(-time.Second), ExpiresAt: now.Add(time.Hour),
		},
		ContextManifestDigest: digestText("context"), InputManifestDigest: digestText("input"),
		InvocationDeadline: now.Add(30 * time.Minute),
	}
	reservation, err := domain.BuildAttemptEffectReservationV1(authority, "physical-claim-1", nil, now)
	if err != nil {
		t.Fatal(err)
	}
	allocation := domain.PreparedAllocationV1{
		Version: domain.PreparedAllocationVersionV1, SubstrateBindingDigest: substrateDigest,
		ObservedImageDigest: substrate.ImageDigest, ObservedOuterHarnessDigest: substrate.OuterHarnessArtifactDigest,
		ObservedProxyArtifactDigest: substrate.EgressProxyArtifactDigest,
		ObservedProxyIdentityDigest: substrate.EgressProxyIdentityDigest,
		WorkloadMode:                domain.SubstrateWorkloadChildProcessV1,
		ChildProcess: &domain.ChildProcessAttestationV1{
			ExecutableDigest: artifactDigest, ExactArgv: []string{"--fixture"},
			NativeProtocol: backend.NativeProtocolVersion, BackendProfileDigest: backend.BackendProfileDigest,
		},
	}
	issuer, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return now }, bytes.NewReader(bytes.Repeat([]byte{7}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, reservation, now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := issuer.Issue(grant, allocation)
	if err != nil {
		t.Fatal(err)
	}
	return prepared, issuer, allocation, now
}

func executableFixture(t testing.TB) string {
	t.Helper()
	return executableFixtureBody(t, "#!/bin/sh\n[ \"$1\" = --fixture ] || exit 41\ncat >/dev/null\nprintf fixture\n")
}

func executableFixtureBody(t testing.TB, body string) string {
	t.Helper()
	path := filepath.Join(canonicalTempDir(t), "fixture")
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func canonicalTempDir(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

func digestText(value string) string {
	digest := attachedworkerdaemon.ExecutableDigest{}
	copy(digest[:], strings.Repeat(value, 32/len(value)+1))
	return hex.EncodeToString(digest[:])
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func waitForPath(t testing.TB, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
