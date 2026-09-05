package piopenrouter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

type fakeBoundary struct {
	calls      int
	cancels    int
	invocation ProcessInvocationV1
	result     ProcessResultV1
	err        error
}

func (boundary *fakeBoundary) Run(_ context.Context, invocation ProcessInvocationV1) (ProcessResultV1, error) {
	boundary.calls++
	invocation.Stdin = append([]byte(nil), invocation.Stdin...)
	invocation.Arguments = append([]string(nil), invocation.Arguments...)
	invocation.Environment = append([]EnvironmentV1(nil), invocation.Environment...)
	invocation.GeneratedFiles = cloneGeneratedFiles(invocation.GeneratedFiles)
	boundary.invocation = invocation
	return boundary.result, boundary.err
}

func (boundary *fakeBoundary) Cancel(context.Context, ports.ExecutionIdentity) error {
	boundary.cancels++
	return boundary.err
}

type recordingSink struct {
	events []ports.ExecutionEvent
	err    error
}

func (sink *recordingSink) Emit(_ context.Context, event ports.ExecutionEvent) error {
	sink.events = append(sink.events, event)
	return sink.err
}

func TestProfileGeneratesExactClosedPiConfiguration(t *testing.T) {
	t.Parallel()
	profile := validProfile(false)
	driver, err := NewDriver(profile, &fakeBoundary{})
	if err != nil {
		t.Fatal(err)
	}
	registration, err := DisabledRegistrationV1(driver)
	if err != nil {
		t.Fatal(err)
	}
	if registration.Enabled || registration.Descriptor != driver.DescriptorV1() ||
		registration.Descriptor.BackendKind != domain.HarnessBackendPiV1 ||
		registration.Descriptor.CredentialDeliveryKind != domain.ProviderCredentialDeliveryEnvironmentV1 {
		t.Fatalf("registration = %+v", registration)
	}
	files, err := generatedFiles(profile)
	if err != nil {
		t.Fatal(err)
	}
	wantModels := `{"providers":{"sessionless-openrouter":{"baseUrl":"https://openrouter.ai/api/v1","apiKey":"$OPENROUTER_API_KEY","api":"openai-completions","models":[{"id":"stealth/ox-alpha","name":"Sessionless Ox Alpha canary","reasoning":true,"input":["text"],"compat":{"openRouterRouting":{"allow_fallbacks":false,"require_parameters":true,"only":["stealth"]}}}]}}}`
	wantSettings := `{"defaultProjectTrust":"never","enableInstallTelemetry":false,"enableAnalytics":false,"defaultTools":[],"compaction":{"enabled":false},"retry":{"enabled":false,"maxRetries":0,"provider":{"timeoutMs":120000,"maxRetries":0}}}`
	if len(files) != 2 || files[0].Name != "models.json" || string(files[0].Content) != wantModels ||
		files[1].Name != "settings.json" || string(files[1].Content) != wantSettings {
		t.Fatalf("generated files differ: %#v", files)
	}
	wantArguments := []string{
		"--mode", "rpc", "--provider", ProviderIDV1, "--model", ModelIDV1,
		"--thinking", "off", "--no-session", "--no-tools", "--no-extensions",
		"--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--no-approve",
	}
	if !reflect.DeepEqual(processArguments(), wantArguments) {
		t.Fatalf("arguments = %q, want %q", processArguments(), wantArguments)
	}
}

func TestDriverExecutesOneBoundedPromptAndSealsUnknownRoute(t *testing.T) {
	t.Parallel()
	rawOutput := []byte(successfulRPCJSONL("sessionless-attempt-a", "bounded result"))
	boundary := &fakeBoundary{result: ProcessResultV1{
		ExitCode: 0, StdoutBytes: len(rawOutput), ProcessStopped: true, CleanupSucceeded: true, CredentialFinalized: true,
		Stdout: rawOutput,
	}}
	driver := mustDriver(t, validProfile(true), boundary)
	request := validExecutionRequest(t, driver)
	sink := &recordingSink{}
	result, err := driver.Execute(context.Background(), request, sink)
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary != "bounded result" || result.ProviderEvidence == nil {
		t.Fatalf("result = %+v", result)
	}
	evidence := result.ProviderEvidence
	if evidence.AcceptanceClass != domain.ProviderAcceptanceAcceptedV1 ||
		evidence.FinishClass != domain.ProviderFinishCompletedV1 ||
		evidence.RouteState != domain.ProviderEvidenceUnknownV1 ||
		evidence.ActualModelID != "" || evidence.TransportProvider != "" ||
		evidence.UsageProvenance != domain.ProviderUsageProviderReportedV1 ||
		evidence.InputTokens == nil || *evidence.InputTokens != 11 ||
		evidence.OutputTokens == nil || *evidence.OutputTokens != 7 ||
		evidence.ValidateForBinding(request.HarnessBinding) != nil {
		t.Fatalf("evidence = %+v", evidence)
	}
	if len(sink.events) != 1 || sink.events[0].Boundary != "pi.agent_settled" || sink.events[0].Sequence != 1 {
		t.Fatalf("events = %+v", sink.events)
	}
	invocation := boundary.invocation
	if boundary.calls != 1 || invocation.Credential.HandleID != "credential-a" ||
		invocation.CredentialMaterialization.EnvironmentName != CredentialEnvironmentV1 ||
		invocation.ConfigDirectoryEnvironmentName != "PI_CODING_AGENT_DIR" ||
		!invocation.RequirePrivateWorkingDirectory || !invocation.RequireSanitizedEnvironment ||
		!reflect.DeepEqual(invocation.Arguments, processArguments()) ||
		!reflect.DeepEqual(invocation.Environment, processEnvironment()) {
		t.Fatalf("invocation authority/config differs: %+v", invocation)
	}
	var command struct {
		ID      string `json:"id"`
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	if json.Unmarshal(invocation.Stdin, &command) != nil || command.ID != "sessionless-attempt-a" ||
		command.Type != "prompt" || command.Message != "Sessionless canonical transcript v1\n[user]\npublic fixture\n" {
		t.Fatalf("RPC command metadata differs: id=%q type=%q message_len=%d", command.ID, command.Type, len(command.Message))
	}
	for index, value := range rawOutput {
		if value != 0 {
			t.Fatalf("raw provider frame byte %d survived reduction", index)
		}
	}
	formatted := invocation.String() + boundary.result.String()
	if strings.Contains(formatted, "public fixture") || strings.Contains(formatted, "bounded result") || !strings.Contains(formatted, "[redacted") {
		t.Fatalf("formatted boundary values leaked content: %s", formatted)
	}
	encoded, err := json.Marshal(invocation)
	if err != nil || strings.Contains(string(encoded), "public fixture") || strings.Contains(string(encoded), "credential-a") {
		t.Fatalf("invocation JSON leaked private material: %s err=%v", encoded, err)
	}
}

func TestDriverDoesNotRetryACompletedProviderEffectAfterSinkFailure(t *testing.T) {
	t.Parallel()
	rawOutput := []byte(successfulRPCJSONL("sessionless-attempt-a", "bounded result"))
	boundary := &fakeBoundary{result: ProcessResultV1{
		ExitCode: 0, StdoutBytes: len(rawOutput), ProcessStopped: true, CleanupSucceeded: true, CredentialFinalized: true,
		Stdout: rawOutput,
	}}
	driver := mustDriver(t, validProfile(true), boundary)
	result, err := driver.Execute(context.Background(), validExecutionRequest(t, driver), &recordingSink{err: errors.New("sink unavailable")})
	if err == nil || result.Summary != "" || result.ProviderEvidence == nil ||
		result.ProviderEvidence.FinishClass != domain.ProviderFinishCompletedV1 || boundary.calls != 1 {
		t.Fatalf("result=%+v err=%v process_calls=%d", result, err, boundary.calls)
	}
	var classified *domain.ClassifiedError
	if !errors.As(err, &classified) || classified.Retryable() {
		t.Fatalf("sink failure classification = %v, want terminal", err)
	}
}

func TestDriverFailsClosedBeforeProcessStart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*ports.ExecutionRequest)
	}{
		{name: "wrong credential environment", mutate: func(request *ports.ExecutionRequest) { request.CredentialMaterialization.EnvironmentName = "OTHER_KEY" }},
		{name: "ambient MCP authority", mutate: func(request *ports.ExecutionRequest) { request.AllowedMCPServers = []string{"ambient"} }},
		{name: "private data class", mutate: func(request *ports.ExecutionRequest) {
			request.HarnessBinding.InputDataClass = domain.ProviderDataPrivateV1
		}},
		{name: "credential outlives evidence", mutate: func(request *ports.ExecutionRequest) {
			request.Credential.ExpiresAt = request.HarnessBinding.EvidenceExpiresAt.Add(time.Second)
		}},
		{name: "attachment input", mutate: func(request *ports.ExecutionRequest) { request.InputArtifacts = []domain.Artifact{{}} }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			boundary := &fakeBoundary{}
			driver := mustDriver(t, validProfile(true), boundary)
			request := validExecutionRequest(t, driver)
			testCase.mutate(&request)
			if _, err := driver.Execute(context.Background(), request, &recordingSink{}); err == nil {
				t.Fatal("invalid authority reached a successful execution")
			}
			if boundary.calls != 0 {
				t.Fatalf("process calls = %d, want 0", boundary.calls)
			}
		})
	}
}

func TestDriverPreservesTerminalLifecycleClassesWithoutRetry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		stdout      string
		mutate      func(*ProcessResultV1)
		boundaryErr error
		wantAccept  domain.ProviderAcceptanceClassV1
		wantFinish  domain.ProviderFinishClassV1
		wantFailure domain.ProviderExecutionFailureCodeV1
	}{
		{name: "accepted outcome unknown", stdout: `{"id":"sessionless-attempt-a","type":"response","command":"prompt","success":true}` + "\n", wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishUnknownV1, wantFailure: domain.ProviderExecutionFailureAcceptedUnknownV1},
		{name: "pre acceptance runner failure", stdout: `{"id":"sessionless-attempt-a","type":"response","command":"prompt","success":false,"error":"private"}` + "\n", boundaryErr: errors.New("private runner failure"), wantAccept: domain.ProviderAcceptancePreAcceptanceV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailurePreAcceptanceV1},
		{name: "protocol drift", stdout: `{"id":"sessionless-attempt-a","type":"response","command":"prompt","success":true}` + "\n" + `{"type":"tool_execution_start"}` + "\n", wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureProtocolDriftV1},
		{name: "cancelled", stdout: `{"id":"sessionless-attempt-a","type":"response","command":"prompt","success":true}` + "\n", mutate: func(result *ProcessResultV1) { result.Cancelled = true }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishCancelledV1, wantFailure: domain.ProviderExecutionFailureCancelledV1},
		{name: "deadline", stdout: successfulRPCJSONL("sessionless-attempt-a", "answer"), mutate: func(result *ProcessResultV1) { result.Deadline = true }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishCancelledV1, wantFailure: domain.ProviderExecutionFailureCancelledV1},
		{name: "stdout accounting mismatch", stdout: successfulRPCJSONL("sessionless-attempt-a", "answer"), mutate: func(result *ProcessResultV1) { result.StdoutBytes++ }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureBackendV1},
		{name: "stderr limit", stdout: successfulRPCJSONL("sessionless-attempt-a", "answer"), mutate: func(result *ProcessResultV1) { result.StderrBytes = maxStderrBytes + 1 }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureBackendV1},
		{name: "cleanup failure", stdout: successfulRPCJSONL("sessionless-attempt-a", "answer"), mutate: func(result *ProcessResultV1) { result.CleanupSucceeded = false }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureTeardownV1},
		{name: "process not stopped", stdout: successfulRPCJSONL("sessionless-attempt-a", "answer"), mutate: func(result *ProcessResultV1) { result.ProcessStopped = false }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureTeardownV1},
		{name: "credential finalization failure", stdout: successfulRPCJSONL("sessionless-attempt-a", "answer"), mutate: func(result *ProcessResultV1) { result.CredentialFinalized = false }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureCredentialFinalizeV1},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			process := ProcessResultV1{ProcessStopped: true, CleanupSucceeded: true, CredentialFinalized: true, Stdout: []byte(testCase.stdout), StdoutBytes: len(testCase.stdout)}
			if testCase.mutate != nil {
				testCase.mutate(&process)
			}
			boundary := &fakeBoundary{result: process, err: testCase.boundaryErr}
			driver := mustDriver(t, validProfile(true), boundary)
			request := validExecutionRequest(t, driver)
			result, err := driver.Execute(context.Background(), request, &recordingSink{})
			if err == nil || result.ProviderEvidence == nil {
				t.Fatalf("result=%+v err=%v, want sealed failure evidence", result, err)
			}
			evidence := result.ProviderEvidence
			if evidence.AcceptanceClass != testCase.wantAccept || evidence.FinishClass != testCase.wantFinish || evidence.FailureCode != testCase.wantFailure {
				t.Fatalf("lifecycle = (%s,%s,%s), want (%s,%s,%s)", evidence.AcceptanceClass, evidence.FinishClass, evidence.FailureCode, testCase.wantAccept, testCase.wantFinish, testCase.wantFailure)
			}
			if err := evidence.ValidateForBinding(request.HarnessBinding); err != nil {
				t.Fatalf("failure evidence invalid: %v", err)
			}
		})
	}
}

func TestDisabledDriverCannotStartButCanRouteCancellation(t *testing.T) {
	t.Parallel()
	boundary := &fakeBoundary{}
	driver := mustDriver(t, validProfile(false), boundary)
	request := validExecutionRequest(t, driver)
	identity := executionIdentity(request)
	if err := driver.Preflight(context.Background(), identity); !errors.Is(err, ErrDisabled) {
		t.Fatalf("preflight error = %v, want ErrDisabled", err)
	}
	if _, err := driver.Execute(context.Background(), request, &recordingSink{}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("execute error = %v, want ErrDisabled", err)
	}
	if err := driver.Cancel(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	if boundary.calls != 0 || boundary.cancels != 1 || driver.BackendProtocolState() != harnessconformance.BackendProtocolUnsupportedV1 {
		t.Fatalf("calls=%d cancels=%d protocol=%s", boundary.calls, boundary.cancels, driver.BackendProtocolState())
	}
}

func TestNewDriverRejectsUnpinnedProfiles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*ProfileV1)
	}{
		{name: "relative executable", mutate: func(profile *ProfileV1) { profile.Executable = "pi" }},
		{name: "empty executable digest", mutate: func(profile *ProfileV1) { profile.ExecutableDigest = attachedworkerdaemon.ExecutableDigest{} }},
		{name: "unreviewed source revision", mutate: func(profile *ProfileV1) { profile.SourceRevision = strings.Repeat("f", 40) }},
		{name: "unbounded provider timeout", mutate: func(profile *ProfileV1) { profile.ProviderTimeoutMS = 300_001 }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			profile := validProfile(false)
			testCase.mutate(&profile)
			if _, err := NewDriver(profile, &fakeBoundary{}); !errors.Is(err, ErrContract) {
				t.Fatalf("NewDriver() error = %v, want ErrContract", err)
			}
		})
	}
}

func mustDriver(t *testing.T, profile ProfileV1, boundary ProcessBoundaryV1) *Driver {
	t.Helper()
	driver, err := NewDriver(profile, boundary)
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func validProfile(enabled bool) ProfileV1 {
	return ProfileV1{
		Enabled: enabled, Executable: "/opt/sessionless/pi", ExecutableVersion: "0.60.0",
		ExecutableDigest: attachedworkerdaemon.ExecutableDigest{1, 2, 3}, SourceRevision: PiSourceRevisionV1,
		ProviderTimeoutMS: 120_000,
	}
}

func validExecutionRequest(t *testing.T, driver *Driver) ports.ExecutionRequest {
	t.Helper()
	workDir := t.TempDir()
	contextDir := filepath.Join(workDir, "context")
	if err := os.Mkdir(contextDir, 0o700); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"version":1,"text":"public fixture"}`)
	payloadDigest := sha256.Sum256(payload)
	author := domain.UserID("user-a")
	event := domain.SessionEvent{
		ID: "event-a", TenantID: "tenant-a", SessionID: "session-a", Sequence: 1,
		Kind: domain.SessionEventUserMessage, AuthorUserID: &author, IdempotencyKey: "event-key-a",
		Payload: domain.BlobRef{
			TenantID: "tenant-a", Key: domain.SessionEventObjectPrefix("tenant-a", "session-a", "event-a") + "message.json",
			Size: int64(len(payload)), SHA256: hex.EncodeToString(payloadDigest[:]),
		},
		CreatedAt: time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC),
	}
	history, err := sessioncontext.EncodeRecord(event, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(contextDir, "history.jsonl"), history, 0o600); err != nil {
		t.Fatal(err)
	}
	placement := domain.ExecutionPlacementV2{
		Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: "user-a", WorkerID: "worker-a",
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("8", 64)),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(strings.Repeat("9", 64)),
	}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	binding := domain.HarnessBindingV1{
		Version: 1, TenantID: "tenant-a", OwnerUserID: "user-a", RunID: "run-a", AttemptID: "attempt-a",
		Backend: driver.DescriptorV1(),
		Resource: domain.ProviderResourceBindingV1{
			Kind: domain.ProviderResourceRouterAccountV1, ResourceID: "openrouter-a", OwnerUserID: "user-a",
			Revision: 3, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 7,
		},
		ModelVendorID: ModelVendorIDV1, ModelID: ModelIDV1, InputDataClass: domain.ProviderDataExternallyShareableV1,
		ProviderCatalogDigest: strings.Repeat("1", 64), ProviderRouteDigest: strings.Repeat("2", 64),
		PrivacyPolicyDigest: strings.Repeat("3", 64), CapabilityEvidenceDigest: strings.Repeat("4", 64),
		EffectivePolicyDigest: strings.Repeat("5", 64), ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &expires,
	}
	request := ports.ExecutionRequest{
		TenantID: "tenant-a", OwnerUserID: "user-a", RunID: "run-a", SessionID: "session-a",
		TriggerEventID: "event-a", AttemptID: "attempt-a", WorkDir: workDir,
		ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1}, ExecutionPlacementV2: placement, HarnessBinding: binding,
		Credential: ports.ProviderInvocationCredentialV1{
			HandleID: "credential-a", TenantID: "tenant-a", OwnerUserID: "user-a", RunID: "run-a", AttemptID: "attempt-a",
			WorkerID: "worker-a", LeaseID: "lease-a", LeaseFence: 5, ProviderResource: binding.Resource, ExpiresAt: expires,
		},
		CredentialMaterialization: ports.ProviderCredentialMaterializationV1{
			Kind: domain.ProviderCredentialDeliveryEnvironmentV1, EnvironmentName: CredentialEnvironmentV1,
		},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("test request invalid: %v", err)
	}
	return request
}

func executionIdentity(request ports.ExecutionRequest) ports.ExecutionIdentity {
	return ports.ExecutionIdentity{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, RunID: request.RunID, AttemptID: request.AttemptID,
		ExecutionPlacementV2: request.ExecutionPlacementV2, HarnessBinding: request.HarnessBinding,
	}
}

func successfulRPCJSONL(requestID, final string) string {
	lines := []string{
		`{"id":"` + requestID + `","type":"response","command":"prompt","success":true}`,
		`{"type":"agent_start"}`,
		`{"type":"turn_start"}`,
		`{"type":"message_start","message":{"role":"user"}}`,
		`{"type":"message_end","message":{"role":"user","content":"fixture","timestamp":1}}`,
		`{"type":"message_start","message":{"role":"assistant"}}`,
		`{"type":"message_end","message":{"role":"assistant","content":[{"type":"thinking","thinking":"private"},{"type":"text","text":"` + final + `"}],"api":"openai-completions","provider":"sessionless-openrouter","model":"stealth/ox-alpha","responseModel":"stealth/ox-alpha","usage":{"input":11,"output":7,"cacheRead":0,"cacheWrite":0,"totalTokens":18,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":2}}`,
		`{"type":"turn_end","message":{"role":"assistant"},"toolResults":[]}`,
		`{"type":"agent_end","messages":[],"willRetry":false}`,
		`{"type":"agent_settled"}`,
	}
	return strings.Join(lines, "\n") + "\n"
}

func cloneGeneratedFiles(files []GeneratedFileV1) []GeneratedFileV1 {
	clone := make([]GeneratedFileV1, len(files))
	for index, file := range files {
		clone[index] = GeneratedFileV1{Name: file.Name, Content: append([]byte(nil), file.Content...)}
	}
	return clone
}
