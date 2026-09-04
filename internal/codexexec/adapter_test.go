package codexexec

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type fixtureInvocationRunner struct {
	calls      int
	invocation attachedworkerdaemon.Invocation
	result     attachedworkerdaemon.InvocationResult
	err        error
	hook       func()
}

func (runner *fixtureInvocationRunner) Run(_ context.Context, invocation attachedworkerdaemon.Invocation) (attachedworkerdaemon.InvocationResult, error) {
	runner.calls++
	invocation.Process.Stdin = append([]byte(nil), invocation.Process.Stdin...)
	runner.invocation = invocation
	if runner.hook != nil {
		runner.hook()
	}
	return runner.result, runner.err
}

func TestAdapterBuildsExactPinnedInvocationAndReturnsContentFreeEvidence(t *testing.T) {
	runner := &fixtureInvocationRunner{result: attachedworkerdaemon.InvocationResult{
		Process: attachedworkerdaemon.AttemptResult{
			ExitCode: 0, DescendantsReaped: true, CleanupSucceeded: true, BoundaryReleased: true,
			Stdout: []byte(successfulJSONL),
		},
		CredentialGeneration: 8,
	}}
	digest := attachedworkerdaemon.ExecutableDigest{1, 2, 3}
	adapter, err := New(Config{
		Enabled: true, Executable: "/opt/sessionless/codex", ExecutableVersion: "0.148.0-alpha.15",
		ExecutableDigest: digest, Model: "gpt-fixture",
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	request := validRequest(t)
	result, err := adapter.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Lifecycle != LifecycleCompleted || !result.Accepted || !result.NativeTerminal ||
		!result.ProcessStopped || !result.CleanupSucceeded || !result.CredentialFinalized ||
		string(result.FinalCandidate) != "bounded result" || len(result.EvidenceDigest) != 64 ||
		result.BillingRoute != ObservationUnknown || result.Quota != ObservationUnknown {
		t.Fatalf("result = %+v", result)
	}
	for _, value := range runner.result.Process.Stdout {
		if value != 0 {
			t.Fatal("raw provider frames survived adapter reduction")
		}
	}
	wantArguments := []string{
		"exec", "--json", "--ephemeral", "--ignore-user-config", "--ignore-rules",
		"--strict-config", "--sandbox", "read-only", "--skip-git-repo-check",
		"--color", "never", "--model", "gpt-fixture", "-",
	}
	invocation := runner.invocation
	if !equalStringSlices(invocation.Process.Arguments, wantArguments) ||
		len(invocation.Process.Environment) != 0 || string(invocation.Process.Stdin) != "public fixture task" ||
		invocation.Credential.HomeEnvironment != "CODEX_HOME" ||
		invocation.Credential.ExpectedBindingGeneration != 7 {
		t.Fatalf("invocation = %+v", invocation)
	}
	formatted := result.String()
	for _, forbidden := range []string{"bounded result", "public fixture task", "native-private"} {
		if strings.Contains(formatted, forbidden) {
			t.Fatalf("formatted result leaked %q: %s", forbidden, formatted)
		}
	}
	if strings.Contains(request.String(), "public fixture task") || !strings.Contains(request.String(), "instruction:[redacted]") {
		t.Fatalf("request formatting leaked instruction: %s", request.String())
	}
	encoded, err := json.Marshal(request)
	if err != nil || strings.Contains(string(encoded), "public fixture task") || string(encoded) != "{}" {
		t.Fatalf("request JSON leaked invocation material: %s err=%v", encoded, err)
	}
	encoded, err = json.Marshal(runner.result.Process)
	if err != nil || strings.Contains(string(encoded), "bounded result") || strings.Contains(string(encoded), "native-private") {
		t.Fatalf("process JSON leaked raw provider output: %s err=%v", encoded, err)
	}
	changed := validRequest(t)
	changed.Instruction = []byte("different public fixture task")
	changedResult, err := adapter.Run(context.Background(), changed)
	if err != nil || changedResult.EvidenceDigest == result.EvidenceDigest {
		t.Fatalf("instruction was not bound into evidence: result=%+v err=%v", changedResult, err)
	}
	resourceChanged := validRequest(t)
	resourceChanged.Authority.ProviderResource.Revision++
	resourceChanged.Credential.ProviderResource.Revision++
	resourceRunner := &fixtureInvocationRunner{result: attachedworkerdaemon.InvocationResult{
		Process: attachedworkerdaemon.AttemptResult{
			DescendantsReaped: true, CleanupSucceeded: true, BoundaryReleased: true,
			Stdout: []byte(successfulJSONL),
		},
		CredentialGeneration: 8,
	}}
	resourceResult, err := mustAdapter(t, resourceRunner, true).Run(context.Background(), resourceChanged)
	if err != nil || resourceResult.EvidenceDigest == result.EvidenceDigest {
		t.Fatalf("provider resource revision was not bound into evidence: result=%+v err=%v", resourceResult, err)
	}
}

func TestAdapterClassifiesLifecycleWithoutRetryHint(t *testing.T) {
	tests := []struct {
		name    string
		stdout  string
		process func(*attachedworkerdaemon.AttemptResult)
		runErr  error
		want    LifecycleClass
	}{
		{name: "pre acceptance", stdout: `{"type":"thread.started"}` + "\n", want: LifecyclePreAcceptance},
		{name: "pre acceptance runner failure", stdout: `{"type":"thread.started"}` + "\n", runErr: errors.New("private runner failure"), want: LifecyclePreAcceptance},
		{name: "ambiguous", stdout: `{"type":"thread.started"}` + "\n" + `{"type":"turn.started"}` + "\n", want: LifecycleAmbiguous},
		{name: "cancelled ambiguous", stdout: `{"type":"thread.started"}` + "\n" + `{"type":"turn.started"}` + "\n", process: func(value *attachedworkerdaemon.AttemptResult) { value.Cancelled = true }, want: LifecycleCancelledAmbiguous},
		{name: "completed teardown failure", stdout: successfulJSONL, runErr: errors.New("private teardown"), want: LifecycleCompletedWithTeardownFailure},
		{name: "terminal drift", stdout: successfulJSONL + `{"type":"turn.completed"}` + "\n", want: LifecycleTerminalProtocolDrift},
		{name: "protocol drift before acceptance", stdout: `{"type":"item.started","item":{"type":"command_execution"}}` + "\n", want: LifecycleProtocolDrift},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			process := attachedworkerdaemon.AttemptResult{
				DescendantsReaped: true, CleanupSucceeded: true, BoundaryReleased: true,
				Stdout: []byte(testCase.stdout),
			}
			if testCase.process != nil {
				testCase.process(&process)
			}
			runner := &fixtureInvocationRunner{result: attachedworkerdaemon.InvocationResult{
				Process: process, CredentialGeneration: 8,
			}, err: testCase.runErr}
			adapter := mustAdapter(t, runner, true)
			result, err := adapter.Run(context.Background(), validRequest(t))
			if err != nil || result.Lifecycle != testCase.want {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if testCase.runErr != nil && result.FailureCode != FailureLocalRunner {
				t.Fatalf("runner failure code = %q", result.FailureCode)
			}
		})
	}
}

func TestAdapterSealsInstructionBeforeLongRunningInvocation(t *testing.T) {
	request := validRequest(t)
	baseline := request
	baseline.Instruction = append([]byte(nil), request.Instruction...)
	original := request.Instruction
	runner := &fixtureInvocationRunner{result: attachedworkerdaemon.InvocationResult{
		Process: attachedworkerdaemon.AttemptResult{
			DescendantsReaped: true, CleanupSucceeded: true, BoundaryReleased: true,
			Stdout: []byte(successfulJSONL),
		},
		CredentialGeneration: 8,
	}}
	runner.hook = func() { copy(original, []byte("mutated after adapter sealed")) }
	adapter := mustAdapter(t, runner, true)
	result, err := adapter.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	baselineRunner := &fixtureInvocationRunner{result: attachedworkerdaemon.InvocationResult{
		Process: attachedworkerdaemon.AttemptResult{
			DescendantsReaped: true, CleanupSucceeded: true, BoundaryReleased: true,
			Stdout: []byte(successfulJSONL),
		},
		CredentialGeneration: 8,
	}}
	baselineResult, err := mustAdapter(t, baselineRunner, true).Run(context.Background(), baseline)
	if err != nil || result.EvidenceDigest != baselineResult.EvidenceDigest ||
		string(runner.invocation.Process.Stdin) != "public fixture task" {
		t.Fatalf("sealed evidence/result mismatch result=%+v baseline=%+v err=%v", result, baselineResult, err)
	}
}

func TestAdapterFailsClosedBeforeRunner(t *testing.T) {
	runner := &fixtureInvocationRunner{}
	adapter := mustAdapter(t, runner, false)
	if _, err := adapter.Run(context.Background(), validRequest(t)); !errors.Is(err, ErrDisabled) || runner.calls != 0 {
		t.Fatalf("disabled err=%v calls=%d", err, runner.calls)
	}
	adapter = mustAdapter(t, runner, true)
	request := validRequest(t)
	request.Credential.OwnerUserID = "user-other"
	if _, err := adapter.Run(context.Background(), request); !errors.Is(err, ErrContract) || runner.calls != 0 {
		t.Fatalf("cross-owner err=%v calls=%d", err, runner.calls)
	}
	request = validRequest(t)
	request.Credential.ProviderResource.CredentialGeneration++
	if _, err := adapter.Run(context.Background(), request); !errors.Is(err, ErrContract) || runner.calls != 0 {
		t.Fatalf("credential resource drift err=%v calls=%d", err, runner.calls)
	}
	request = validRequest(t)
	request.Authority.ProviderResource.Revision++
	if _, err := adapter.Run(context.Background(), request); !errors.Is(err, ErrContract) || runner.calls != 0 {
		t.Fatalf("authority resource drift err=%v calls=%d", err, runner.calls)
	}
	request = validRequest(t)
	request.Instruction = append(request.Instruction, 0)
	if _, err := adapter.Run(context.Background(), request); !errors.Is(err, ErrContract) || runner.calls != 0 {
		t.Fatalf("invalid instruction err=%v calls=%d", err, runner.calls)
	}
	request = validRequest(t)
	request.Authority.ContextDigest = domain.AttachedWorkerContextDigest(domain.DigestAttachedWorkerCapability([]byte("other-context")))
	request.Authority.FenceToken = "invalid"
	if _, err := adapter.Run(context.Background(), request); !errors.Is(err, ErrContract) || runner.calls != 0 {
		t.Fatalf("invalid authority err=%v calls=%d", err, runner.calls)
	}
}

func TestNewRejectsUnpinnedOrAmbiguousProcessConfiguration(t *testing.T) {
	runner := &fixtureInvocationRunner{}
	valid := Config{
		Executable: "/opt/sessionless/codex", ExecutableVersion: "0.148.0-alpha.15",
		ExecutableDigest: attachedworkerdaemon.ExecutableDigest{1}, Model: "openrouter/public-model:v1",
	}
	for name, mutate := range map[string]func(*Config){
		"relative executable": func(config *Config) { config.Executable = "codex" },
		"invalid version":     func(config *Config) { config.ExecutableVersion = "version with spaces" },
		"ambiguous model":     func(config *Config) { config.Model = "model with spaces" },
		"empty digest":        func(config *Config) { config.ExecutableDigest = attachedworkerdaemon.ExecutableDigest{} },
	} {
		t.Run(name, func(t *testing.T) {
			config := valid
			mutate(&config)
			if _, err := New(config, runner); !errors.Is(err, ErrContract) {
				t.Fatalf("New() error = %v", err)
			}
		})
	}
}

func mustAdapter(t *testing.T, runner InvocationRunner, enabled bool) *Adapter {
	t.Helper()
	adapter, err := New(Config{
		Enabled: enabled, Executable: "/opt/sessionless/codex", ExecutableVersion: "0.148.0-alpha.15",
		ExecutableDigest: attachedworkerdaemon.ExecutableDigest{1}, Model: "gpt-fixture",
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func validRequest(t *testing.T) RequestV1 {
	t.Helper()
	now := time.Now().UTC().Add(-time.Minute)
	run := domain.Run{
		ID: "run-a", TenantID: "tenant-a", SessionID: "session-a", TriggerEventID: "event-a",
		SubscriptionConnectionID: "resource-a", Status: domain.RunRunning,
		IdempotencyKey: "run-key-a", StartedAt: &now, CreatedAt: now, UpdatedAt: now,
	}
	attempt := domain.Attempt{
		ID: "attempt-a", TenantID: "tenant-a", RunID: "run-a", Number: 1,
		Status: domain.AttemptRunning, WorkerID: "worker-a", CreatedAt: now, UpdatedAt: now,
	}
	lease := domain.Lease{
		ID: "lease-a", TenantID: "tenant-a", RunID: "run-a", AttemptID: "attempt-a",
		WorkerID: "worker-a", FenceToken: 9, AcquiredAt: now, ExpiresAt: now.Add(time.Hour),
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(
		"tenant-a", "user-a", "worker-a", "run-a", "attempt-a", "lease-a", 9,
	)
	if err != nil {
		t.Fatal(err)
	}
	resource := domain.ProviderResourceBindingV1{
		Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "resource-a", OwnerUserID: "user-a",
		Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 7,
	}
	return RequestV1{
		Authority: AuthorityV1{
			Version: ContractVersionV1, TenantID: "tenant-a", OwnerUserID: "user-a",
			WorkerID: "worker-a", ConnectionID: "connection-a", EnrollmentGeneration: 3,
			ConnectionGeneration: 4, RunID: "run-a", AttemptID: "attempt-a",
			ReservationID: "reservation-a", LeaseID: "lease-a", LeaseGeneration: 9,
			FenceToken: fence, LeaseExpiresAt: lease.ExpiresAt,
			ContextDigest:    domain.AttachedWorkerContextDigest(domain.DigestAttachedWorkerCapability([]byte("context"))),
			CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("capability")),
			PolicyDigest:     domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy"))),
			ProviderResource: resource,
		},
		Credential: ports.CredentialIssueRequest{
			OwnerUserID: "user-a", Run: run, Attempt: attempt, Lease: lease,
			ExpiresAt: now.Add(30 * time.Minute), ProviderResource: resource,
		},
		Instruction: []byte("public fixture task"),
	}
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
