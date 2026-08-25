package credentiallifecycle

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

func TestExecCredentialMutationWriteBackAndSupervisorFailureBoundary(t *testing.T) {
	t.Run("successful process mutation is committed", func(t *testing.T) {
		fixture := newCredentialFixture(t, nil)
		handle, materialization := issueAndMaterialize(t, fixture)
		runCredentialMutationProcess(t, materialization.AuthFile, 0)

		result, err := fixture.service.WriteBack(context.Background(), handle, materialization)
		if err != nil || !result.Changed || result.Generation != 2 {
			t.Fatalf("WriteBack() = %+v, %v", result, err)
		}
		if current := fixture.state.current(fixture.binding); current.Generation != 2 ||
			current.SecretFingerprint != domain.FingerprintCredential(execRotatedCredential) {
			t.Fatalf("committed binding = %+v", current)
		}
		if err := fixture.service.Release(context.Background(), handle); err != nil {
			t.Fatalf("Release() error = %v", err)
		}
		if _, err := os.Lstat(materialization.RootDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("released materialization remains: %v", err)
		}
	})

	t.Run("ambiguous process loss preserves credential refresh only", func(t *testing.T) {
		fixture := newCredentialFixture(t, nil)
		handle, materialization := issueAndMaterialize(t, fixture)
		runCredentialMutationProcess(t, materialization.AuthFile, 23)

		result, err := fixture.service.WriteBack(context.Background(), handle, materialization)
		if err != nil || !result.Changed || result.Generation != 2 {
			t.Fatalf("WriteBack() after process loss = %+v, %v", result, err)
		}
		if err := fixture.service.Release(context.Background(), handle); err != nil {
			t.Fatalf("Release() after process loss error = %v", err)
		}
	})

	t.Run("attempt failure before writeback cannot activate released residue", func(t *testing.T) {
		fixture := newCredentialFixture(t, nil)
		handle, materialization := issueAndMaterialize(t, fixture)
		runCredentialMutationProcess(t, materialization.AuthFile, 23)
		if err := fixture.service.Release(context.Background(), handle); err != nil {
			t.Fatalf("Release() after simulated supervisor failure error = %v", err)
		}
		if current := fixture.state.current(fixture.binding); current.Generation != 1 ||
			current.SecretFingerprint != fixture.binding.SecretFingerprint {
			t.Fatalf("unwritten residue changed authoritative binding: %+v", current)
		}

		restarted, err := New(Config{
			ScratchRoot: t.TempDir(), MaxAuthBytes: 128, Clock: fixture.clock,
			IDs: testkit.NewSequenceIDGenerator("exec-restart-"),
		}, fixture.state, fixture.secrets)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = restarted.Close() })
		restartedHandle, err := restarted.Issue(context.Background(), fixture.request)
		if err != nil {
			t.Fatalf("Issue() after supervisor failure error = %v", err)
		}
		restartedMaterialization, err := restarted.Materialize(context.Background(), restartedHandle)
		if err != nil {
			t.Fatalf("Materialize() after supervisor failure error = %v", err)
		}
		content, err := os.ReadFile(restartedMaterialization.AuthFile)
		if err != nil || string(content) != string(fixture.value) {
			t.Fatalf("restart did not materialize authoritative generation: content_match=%v error=%v", string(content) == string(fixture.value), err)
		}
	})
}

var execRotatedCredential = []byte(`{"access_token":"rotated-by-exec"}`)

func issueAndMaterialize(t *testing.T, fixture credentialFixture) (ports.CredentialHandle, ports.CredentialMaterialization) {
	t.Helper()
	handle, err := fixture.service.Issue(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	materialization, err := fixture.service.Materialize(context.Background(), handle)
	if err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	return handle, materialization
}

func runCredentialMutationProcess(t *testing.T, authFile string, exitCode int) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestCredentialExecMutationHelper$", "--", authFile, strconv.Itoa(exitCode))
	command.Env = []string{"SESSIONLESS_CREDENTIAL_EXEC_HELPER=1"}
	err := command.Run()
	if exitCode == 0 && err != nil {
		t.Fatalf("credential mutation helper error = %v", err)
	}
	if exitCode != 0 {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != exitCode {
			t.Fatalf("credential mutation helper exit = %v, want %d", err, exitCode)
		}
	}
}

func TestCredentialExecMutationHelper(t *testing.T) {
	if os.Getenv("SESSIONLESS_CREDENTIAL_EXEC_HELPER") != "1" {
		return
	}
	separator := 0
	for index, value := range os.Args {
		if value == "--" {
			separator = index
			break
		}
	}
	if separator == 0 || len(os.Args) != separator+3 {
		os.Exit(90)
	}
	exitCode, err := strconv.Atoi(os.Args[separator+2])
	if err != nil || exitCode < 0 || exitCode > 125 {
		os.Exit(91)
	}
	if err := os.WriteFile(os.Args[separator+1], execRotatedCredential, 0o600); err != nil {
		os.Exit(92)
	}
	os.Exit(exitCode)
}
