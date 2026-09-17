package codexexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

var driverNow = time.Date(2026, time.September, 17, 8, 0, 0, 0, time.UTC)

type fixtureAuthorityResolver struct {
	authority         AuthorityV1
	err               error
	executionCalls    int
	cancellationCalls int
}

func (resolver *fixtureAuthorityResolver) ResolveExecution(_ context.Context, _ ports.ExecutionIdentity) (AuthorityV1, error) {
	resolver.executionCalls++
	return resolver.authority, resolver.err
}

func (resolver *fixtureAuthorityResolver) ResolveCancellation(_ context.Context, _ ports.ExecutionIdentity) (AuthorityV1, error) {
	resolver.cancellationCalls++
	return resolver.authority, resolver.err
}

type fixtureProcessBoundary struct {
	runs       int
	cancels    int
	invocation ProcessInvocationV1
	stdin      []byte
	result     ProcessResultV1
	runErr     error
	cancelErr  error
}

func (boundary *fixtureProcessBoundary) Run(_ context.Context, invocation ProcessInvocationV1) (ProcessResultV1, error) {
	boundary.runs++
	boundary.invocation = invocation
	boundary.stdin = append([]byte(nil), invocation.Stdin...)
	return boundary.result, boundary.runErr
}

func (boundary *fixtureProcessBoundary) Cancel(_ context.Context, _ AuthorityV1) error {
	boundary.cancels++
	return boundary.cancelErr
}

type fixtureEventSink struct {
	events []ports.ExecutionEvent
	err    error
}

func (sink *fixtureEventSink) Emit(_ context.Context, event ports.ExecutionEvent) error {
	sink.events = append(sink.events, event)
	return sink.err
}

func TestDriverExecutesExactPreparedAttachedWorkerInvocation(t *testing.T) {
	request, authority := validDriverRequest(t)
	stdout := []byte(successfulJSONL)
	boundary := &fixtureProcessBoundary{result: successfulDriverProcess(stdout)}
	resolver := &fixtureAuthorityResolver{authority: authority}
	driver := mustDriverWithResolver(t, true, resolver, boundary)
	if err := driver.Preflight(context.Background(), executionIdentity(request)); err != nil {
		t.Fatalf("Preflight() error = %v", err)
	}
	if resolver.executionCalls != 0 {
		t.Fatalf("pre-lease Preflight() authority resolutions = %d, want 0", resolver.executionCalls)
	}
	sink := &fixtureEventSink{}
	result, err := driver.Execute(context.Background(), request, sink)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if resolver.executionCalls != 1 {
		t.Fatalf("Execute() authority resolutions = %d, want 1", resolver.executionCalls)
	}
	if result.Summary != "bounded result" || result.ProviderEvidence == nil ||
		result.ProviderEvidence.FinishClass != domain.ProviderFinishCompletedV1 ||
		result.ProviderEvidence.RouteState != domain.ProviderEvidenceUnknownV1 ||
		result.ProviderEvidence.UsageProvenance != domain.ProviderUsageUnknownV1 ||
		result.ProviderEvidence.PolicyVerdict != domain.ProviderPolicyConditionalV1 {
		t.Fatalf("result = %+v", result)
	}
	if len(sink.events) != 1 || sink.events[0].Sequence != 1 ||
		sink.events[0].Boundary != "codex.turn_completed" ||
		string(sink.events[0].CheckpointState) != string(completedCheckpointV1) {
		t.Fatalf("events = %+v", sink.events)
	}
	invocation := boundary.invocation
	if invocation.Authority != authority || invocation.Identity.RunID != request.RunID ||
		invocation.WorkDir != request.WorkDir ||
		invocation.Credential.HandleID != request.Credential.HandleID ||
		invocation.CredentialMaterialization != request.CredentialMaterialization ||
		invocation.CredentialHomeEnvironmentName != CredentialHomeEnvironmentV1 ||
		!invocation.RequirePrivateWorkingDirectory || !invocation.RequireSanitizedEnvironment ||
		!invocation.RequireNoAmbientHome || !invocation.RequireNoAmbientAPIKey ||
		!invocation.RequireProviderEffectFence || invocation.MaxProviderEffects != 1 ||
		invocation.MaxStdoutBytes != maxJSONLOutputBytes ||
		invocation.MaxStderrBytes != maxProcessStderrBytesV1 {
		t.Fatalf("invocation = %+v", invocation)
	}
	wantArguments := processArguments("gpt-fixture")
	if !equalStringSlices(invocation.Arguments, wantArguments) ||
		string(boundary.stdin) != "Sessionless canonical transcript v1\n[user]\npublic fixture\n" {
		t.Fatalf("invocation argv/stdin mismatch: %+v", invocation)
	}
	for _, value := range boundary.result.Stdout {
		if value != 0 {
			t.Fatal("raw provider stdout survived reduction")
		}
	}
	encoded, err := json.Marshal(invocation)
	if err != nil || strings.Contains(string(encoded), "public fixture") ||
		strings.Contains(string(encoded), "credential-a") {
		t.Fatalf("invocation JSON leaked private material: %s err=%v", encoded, err)
	}
}

func TestDriverFailsClosedBeforeProcessOnAuthorityOrCredentialDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ports.ExecutionRequest, *AuthorityV1)
	}{
		{name: "other owner", mutate: func(_ *ports.ExecutionRequest, authority *AuthorityV1) {
			authority.OwnerUserID = "owner-other"
		}},
		{name: "other worker", mutate: func(_ *ports.ExecutionRequest, authority *AuthorityV1) {
			authority.WorkerID = "worker-other"
		}},
		{name: "stale connection generation", mutate: func(_ *ports.ExecutionRequest, authority *AuthorityV1) {
			authority.ConnectionGeneration = 0
		}},
		{name: "credential lease mismatch", mutate: func(request *ports.ExecutionRequest, _ *AuthorityV1) {
			request.Credential.LeaseID = "lease-other"
		}},
		{name: "credential generation mismatch", mutate: func(request *ports.ExecutionRequest, _ *AuthorityV1) {
			request.Credential.ProviderResource.CredentialGeneration++
		}},
		{name: "ambient credential filename", mutate: func(request *ports.ExecutionRequest, _ *AuthorityV1) {
			request.CredentialMaterialization.FilePath = filepath.Join(request.CredentialMaterialization.RootDir, "other.json")
		}},
		{name: "expired policy", mutate: func(request *ports.ExecutionRequest, _ *AuthorityV1) {
			expired := driverNow.Add(-time.Second)
			request.HarnessBinding.EvidenceExpiresAt = &expired
		}},
		{name: "expired credential", mutate: func(request *ports.ExecutionRequest, _ *AuthorityV1) {
			request.Credential.ExpiresAt = driverNow.Add(-time.Second)
		}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			request, authority := validDriverRequest(t)
			testCase.mutate(&request, &authority)
			boundary := &fixtureProcessBoundary{}
			driver := mustDriver(t, true, authority, boundary)
			if _, err := driver.Execute(context.Background(), request, &fixtureEventSink{}); err == nil {
				t.Fatal("Execute() error = nil")
			}
			if boundary.runs != 0 {
				t.Fatalf("boundary runs = %d, want 0", boundary.runs)
			}
		})
	}
}

func TestDriverMapsLifecycleWithoutRetryingAcceptedWork(t *testing.T) {
	tests := []struct {
		name       string
		stdout     string
		mutate     func(*ProcessResultV1)
		runErr     error
		acceptance domain.ProviderAcceptanceClassV1
		finish     domain.ProviderFinishClassV1
		failure    domain.ProviderExecutionFailureCodeV1
	}{
		{name: "pre acceptance", stdout: `{"type":"thread.started"}` + "\n", acceptance: domain.ProviderAcceptancePreAcceptanceV1, finish: domain.ProviderFinishFailedV1, failure: domain.ProviderExecutionFailurePreAcceptanceV1},
		{name: "accepted unknown", stdout: codexAcceptedJSONL(), acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishUnknownV1, failure: domain.ProviderExecutionFailureAcceptedUnknownV1},
		{name: "cancelled", stdout: codexAcceptedJSONL(), mutate: func(result *ProcessResultV1) { result.Cancelled = true }, acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishCancelledV1, failure: domain.ProviderExecutionFailureCancelledV1},
		{name: "protocol drift", stdout: successfulJSONL + `{"type":"turn.completed"}` + "\n", acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishFailedV1, failure: domain.ProviderExecutionFailureProtocolDriftV1},
		{name: "credential quiescence", stdout: successfulJSONL, mutate: func(result *ProcessResultV1) { result.CredentialStateQuiesced = false }, acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishFailedV1, failure: domain.ProviderExecutionFailureCredentialFinalizeV1},
		{name: "teardown", stdout: successfulJSONL, mutate: func(result *ProcessResultV1) { result.DescendantsStopped = false }, acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishFailedV1, failure: domain.ProviderExecutionFailureTeardownV1},
		{name: "runner failure", stdout: successfulJSONL, runErr: errors.New("private failure"), acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishFailedV1, failure: domain.ProviderExecutionFailureBackendV1},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			request, authority := validDriverRequest(t)
			process := successfulDriverProcess([]byte(testCase.stdout))
			if testCase.mutate != nil {
				testCase.mutate(&process)
			}
			boundary := &fixtureProcessBoundary{result: process, runErr: testCase.runErr}
			driver := mustDriver(t, true, authority, boundary)
			result, err := driver.Execute(context.Background(), request, &fixtureEventSink{})
			if err == nil || result.ProviderEvidence == nil {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			evidence := result.ProviderEvidence
			if evidence.AcceptanceClass != testCase.acceptance || evidence.FinishClass != testCase.finish ||
				evidence.FailureCode != testCase.failure || boundary.runs != 1 {
				t.Fatalf("evidence=%+v runs=%d", evidence, boundary.runs)
			}
		})
	}
}

func TestDisabledDriverDoesNotExecuteButRoutesExactCancellation(t *testing.T) {
	request, authority := validDriverRequest(t)
	expired := driverNow.Add(-time.Hour)
	request.HarnessBinding.EvidenceExpiresAt = &expired
	authority.LeaseExpiresAt = expired
	boundary := &fixtureProcessBoundary{}
	driver := mustDriver(t, false, authority, boundary)
	if state := driver.BackendProtocolState(); state != harnessconformance.BackendProtocolUnsupportedV1 {
		t.Fatalf("protocol state = %q", state)
	}
	if err := driver.Preflight(context.Background(), executionIdentity(request)); err == nil {
		t.Fatal("disabled Preflight() error = nil")
	}
	if _, err := driver.Execute(context.Background(), request, &fixtureEventSink{}); err == nil {
		t.Fatal("disabled Execute() error = nil")
	}
	if err := driver.Cancel(context.Background(), executionIdentity(request)); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if boundary.runs != 0 || boundary.cancels != 1 {
		t.Fatalf("effects runs=%d cancels=%d", boundary.runs, boundary.cancels)
	}
	registration, err := DisabledRegistrationV1(driver)
	if err != nil || registration.Enabled || registration.Driver != driver {
		t.Fatalf("registration=%+v err=%v", registration, err)
	}
}

func mustDriver(t *testing.T, enabled bool, authority AuthorityV1, boundary *fixtureProcessBoundary) *Driver {
	t.Helper()
	return mustDriverWithResolver(t, enabled, &fixtureAuthorityResolver{authority: authority}, boundary)
}

func mustDriverWithResolver(
	t *testing.T,
	enabled bool,
	resolver *fixtureAuthorityResolver,
	boundary *fixtureProcessBoundary,
) *Driver {
	t.Helper()
	driver, err := NewDriver(Config{
		Enabled: enabled, Executable: "/opt/sessionless/codex",
		ExecutableVersion: "0.148.0-alpha.15",
		ExecutableDigest:  attachedworkerdaemon.ExecutableDigest{1},
		Model:             "gpt-fixture", Now: func() time.Time { return driverNow },
	}, resolver, boundary)
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func validDriverRequest(t *testing.T) (ports.ExecutionRequest, AuthorityV1) {
	t.Helper()
	workDir := t.TempDir()
	contextDir := filepath.Join(workDir, "context")
	if err := os.Mkdir(contextDir, 0o700); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"version":1,"text":"public fixture"}`)
	payloadDigest := sha256.Sum256(payload)
	author := domain.UserID("owner-a")
	event := domain.SessionEvent{
		ID: "event-a", TenantID: "tenant-a", SessionID: "session-a", Sequence: 1,
		Kind: domain.SessionEventUserMessage, AuthorUserID: &author,
		IdempotencyKey: "event-key-a",
		Payload: domain.BlobRef{
			TenantID: "tenant-a",
			Key:      domain.SessionEventObjectPrefix("tenant-a", "session-a", "event-a") + "message.json",
			Size:     int64(len(payload)), SHA256: hex.EncodeToString(payloadDigest[:]),
		},
		CreatedAt: driverNow.Add(-time.Minute),
	}
	history, err := sessioncontext.EncodeRecord(event, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contextDir, "history.jsonl"), history, 0o600); err != nil {
		t.Fatal(err)
	}
	placement := domain.ExecutionPlacementV2{
		Version:        domain.ExecutionPlacementVersionV2,
		Kind:           domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied,
		OwnerUserID:    "owner-a", WorkerID: "worker-a",
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("8", 64)),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(strings.Repeat("9", 64)),
	}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	expires := driverNow.Add(time.Hour)
	resource := domain.ProviderResourceBindingV1{
		Kind:       domain.ProviderResourceSubscriptionV1,
		ResourceID: "subscription-a", OwnerUserID: "owner-a", Revision: 3,
		CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 7,
	}
	config := Config{
		Enabled: true, Executable: "/opt/sessionless/codex",
		ExecutableVersion: "0.148.0-alpha.15",
		ExecutableDigest:  attachedworkerdaemon.ExecutableDigest{1}, Model: "gpt-fixture",
	}
	descriptor, err := prepareConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.HarnessBindingV1{
		Version: 1, TenantID: "tenant-a", OwnerUserID: "owner-a",
		RunID: "run-a", AttemptID: "attempt-a", Backend: descriptor,
		Resource: resource, ModelVendorID: ModelVendorIDV1, ModelID: "gpt-fixture",
		InputDataClass:           domain.ProviderDataPrivateV1,
		ProviderCatalogDigest:    strings.Repeat("1", 64),
		ProviderRouteDigest:      strings.Repeat("2", 64),
		PrivacyPolicyDigest:      strings.Repeat("3", 64),
		CapabilityEvidenceDigest: strings.Repeat("4", 64),
		EffectivePolicyDigest:    strings.Repeat("5", 64),
		ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &expires,
	}
	credentialRoot := filepath.Join(workDir, "credential")
	request := ports.ExecutionRequest{
		TenantID: "tenant-a", OwnerUserID: "owner-a", RunID: "run-a",
		SessionID: "session-a", TriggerEventID: "event-a", AttemptID: "attempt-a",
		WorkDir: workDir, ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1},
		ExecutionPlacementV2: placement, HarnessBinding: binding,
		Credential: ports.ProviderInvocationCredentialV1{
			HandleID: "credential-a", TenantID: "tenant-a", OwnerUserID: "owner-a",
			RunID: "run-a", AttemptID: "attempt-a", WorkerID: "worker-a",
			LeaseID: "lease-a", LeaseFence: 5, ProviderResource: resource,
			ExpiresAt: driverNow.Add(30 * time.Minute),
		},
		CredentialMaterialization: ports.ProviderCredentialMaterializationV1{
			Kind:    domain.ProviderCredentialDeliveryFileV1,
			RootDir: credentialRoot, FilePath: filepath.Join(credentialRoot, "auth.json"),
		},
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(
		"tenant-a", "owner-a", "worker-a", "run-a", "attempt-a", "lease-a", 5,
	)
	if err != nil {
		t.Fatal(err)
	}
	authority := AuthorityV1{
		Version: ContractVersionV1, TenantID: "tenant-a", OwnerUserID: "owner-a",
		WorkerID: "worker-a", ConnectionID: "connection-a",
		EnrollmentGeneration: 2, ConnectionGeneration: 3,
		RunID: "run-a", AttemptID: "attempt-a", ReservationID: "reservation-a",
		LeaseID: "lease-a", LeaseGeneration: 5, FenceToken: fence,
		LeaseExpiresAt:   expires,
		ContextDigest:    domain.AttachedWorkerContextDigest(strings.Repeat("7", 64)),
		CapabilityDigest: placement.CapabilityDigest, PolicyDigest: placement.PolicyDigest,
		ProviderResource: resource,
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("request invalid: %v", err)
	}
	if err := authority.Validate(); err != nil {
		t.Fatalf("authority invalid: %v", err)
	}
	return request, authority
}

func successfulDriverProcess(stdout []byte) ProcessResultV1 {
	return ProcessResultV1{
		ExitCode: 0, StdoutBytes: len(stdout), ProcessStopped: true,
		DescendantsStopped: true, CleanupSucceeded: true, PrivateStateRemoved: true,
		CredentialStateQuiesced: true, ProviderEffectFenceSatisfied: true,
		ProviderEffects: 1, Stdout: stdout,
	}
}

func codexAcceptedJSONL() string {
	return `{"type":"thread.started","thread_id":"native-private"}` + "\n" +
		`{"type":"turn.started"}` + "\n"
}
