//go:build darwin || linux

package attachedworkerdaemon

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestInvocationRunnerComposesCredentialWithSupervisorBoundary(t *testing.T) {
	credentialBase := newCanonicalTempDir(t)
	supervisor, _, executable, digest := newFixtureSupervisor(t, SupervisorConfig{
		AllowedEnvironmentNames: []string{"CODEX_HOME"},
		AllowedReadRoots:        []string{credentialBase},
	})
	credentials := &fakeCredentialLifecycle{base: credentialBase}
	runner, err := NewInvocationRunner(InvocationRunnerConfig{}, supervisor, credentials)
	if err != nil {
		t.Fatal(err)
	}
	invocation := validCredentialInvocation(t)
	invocation.Process = AttemptSpec{
		Executable: executable, ExecutableDigest: digest,
		Arguments: []string{"-test.run=TestSupervisorHelperProcess"},
		Environment: []EnvironmentVariable{
			{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "credential"},
		},
	}
	result, err := runner.Run(context.Background(), invocation)
	if err != nil {
		t.Fatalf("run composed invocation: %v", err)
	}
	if result.Process.ExitCode != 0 || string(result.Process.Stdout) != "credential-ok\n" ||
		!result.Process.CleanupSucceeded || !result.CredentialChanged {
		t.Fatalf("unexpected composed result: %+v", result)
	}
	if _, err := os.Stat(credentials.root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("credential root survived composed release: %v", err)
	}
}
