package codexopenrouter

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

func TestProfileGeneratesExactClosedCodexConfiguration(t *testing.T) {
	t.Parallel()
	profile := validProfile(false)
	driver := mustDriver(t, profile, &fakeBoundary{})
	registration, err := DisabledRegistrationV1(driver)
	if err != nil {
		t.Fatal(err)
	}
	if registration.Enabled || registration.Descriptor != driver.DescriptorV1() ||
		registration.Descriptor.BackendKind != domain.HarnessBackendCodexOpenRouterV1 ||
		registration.Descriptor.BackendKind == domain.HarnessBackendCodexExecV1 ||
		registration.Descriptor.CredentialDeliveryKind != domain.ProviderCredentialDeliveryEnvironmentV1 {
		t.Fatalf("registration = %+v", registration)
	}
	files, err := generatedFiles(profile)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Name != "config.toml" || files[1].Name != "models.json" {
		t.Fatalf("generated files = %#v", files)
	}
	config := string(files[0].Content)
	for _, exact := range []string{
		`model = "stealth/ox-alpha"`, `model_provider = "openrouter"`,
		`model_catalog_json = "/sessionless/invocation/codex-home/models.json"`,
		`base_url = "https://openrouter.ai/api/v1"`, `env_key = "OPENROUTER_API_KEY"`,
		`wire_api = "responses"`, `request_max_retries = 0`, `stream_max_retries = 0`,
		`persistence = "none"`, `web_search = "disabled"`, `multi_agent = false`,
	} {
		if !strings.Contains(config, exact) {
			t.Fatalf("generated config missing %q:\n%s", exact, config)
		}
	}
	for _, forbidden := range []string{"experimental_bearer_token", "\nauth =", "mcp_servers", "notify", "proxy"} {
		if strings.Contains(config, forbidden) {
			t.Fatalf("generated config contains forbidden %q", forbidden)
		}
	}
	var catalog modelCatalogV1
	if err := json.Unmarshal(files[1].Content, &catalog); err != nil || len(catalog.Models) != 1 {
		t.Fatalf("catalog decode = %+v, err=%v", catalog, err)
	}
	model := catalog.Models[0]
	if model.Slug != ModelIDV1 || model.ShellType != "disabled" || model.ContextWindow != profile.ContextWindow ||
		model.TruncationPolicy.Limit != profile.ContextWindow-profile.MaxOutputTokens || model.SupportsParallelTools {
		t.Fatalf("model catalog entry = %+v", model)
	}
	if !reflect.DeepEqual(processArguments(), []string{
		"exec", "--json", "--ephemeral", "--ignore-rules",
		"--strict-config", "--sandbox", "read-only", "--skip-git-repo-check",
		"--color", "never", "--model", ModelIDV1, "-",
	}) {
		t.Fatalf("arguments = %q", processArguments())
	}
}

func TestDriverExecutesOneBoundedPromptWithUnknownRoute(t *testing.T) {
	t.Parallel()
	rawOutput := []byte(successfulCodexJSONL("bounded result"))
	boundary := &fakeBoundary{result: successfulProcess(rawOutput)}
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
	if evidence.AcceptanceClass != domain.ProviderAcceptanceAcceptedV1 || evidence.FinishClass != domain.ProviderFinishCompletedV1 ||
		evidence.RouteState != domain.ProviderEvidenceUnknownV1 || evidence.ActualModelID != "" ||
		evidence.UsageProvenance != domain.ProviderUsageUnknownV1 || evidence.ValidateForBinding(request.HarnessBinding) != nil {
		t.Fatalf("evidence = %+v", evidence)
	}
	if len(sink.events) != 1 || sink.events[0].Boundary != "codex.turn_completed" || sink.events[0].Sequence != 1 {
		t.Fatalf("events = %+v", sink.events)
	}
	invocation := boundary.invocation
	if boundary.calls != 1 || invocation.Credential.HandleID != "credential-a" ||
		invocation.CredentialMaterialization.EnvironmentName != CredentialEnvironmentV1 ||
		invocation.ConfigDirectoryEnvironmentName != ConfigEnvironmentV1 || invocation.ConfigDirectoryMountPath != ConfigMountPathV1 ||
		!invocation.RequirePrivateWorkingDirectory || !invocation.RequireSanitizedEnvironment || !invocation.RequireNoAmbientHome ||
		!invocation.RequireProviderEffectFence || invocation.MaxProviderEffects != 1 ||
		!reflect.DeepEqual(invocation.Arguments, processArguments()) || !reflect.DeepEqual(invocation.Environment, processEnvironment()) {
		t.Fatalf("invocation authority/config differs: %+v", invocation)
	}
	if string(invocation.Stdin) != "Sessionless canonical transcript v1\n[user]\npublic fixture\n" {
		t.Fatalf("stdin = %q", invocation.Stdin)
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

func TestDriverFailsClosedBeforeProcessStart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*ports.ExecutionRequest)
	}{
		{name: "subscription resource", mutate: func(request *ports.ExecutionRequest) {
			request.HarnessBinding.Resource.Kind = domain.ProviderResourceSubscriptionV1
		}},
		{name: "wrong credential environment", mutate: func(request *ports.ExecutionRequest) { request.CredentialMaterialization.EnvironmentName = "OTHER_KEY" }},
		{name: "ambient MCP authority", mutate: func(request *ports.ExecutionRequest) { request.AllowedMCPServers = []string{"ambient"} }},
		{name: "private data class", mutate: func(request *ports.ExecutionRequest) {
			request.HarnessBinding.InputDataClass = domain.ProviderDataPrivateV1
		}},
		{name: "credential outlives evidence", mutate: func(request *ports.ExecutionRequest) {
			request.Credential.ExpiresAt = request.HarnessBinding.EvidenceExpiresAt.Add(time.Second)
		}},
		{name: "attachment input", mutate: func(request *ports.ExecutionRequest) { request.InputArtifacts = []domain.Artifact{{}} }},
		{name: "resume checkpoint", mutate: func(request *ports.ExecutionRequest) { request.ResumeCheckpoint = &domain.Checkpoint{} }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			boundary := &fakeBoundary{}
			driver := mustDriver(t, validProfile(true), boundary)
			request := validExecutionRequest(t, driver)
			testCase.mutate(&request)
			if _, err := driver.Execute(context.Background(), request, &recordingSink{}); err == nil {
				t.Fatal("invalid authority reached successful execution")
			}
			if boundary.calls != 0 {
				t.Fatalf("process calls = %d, want 0", boundary.calls)
			}
		})
	}
}

func TestDriverRejectsSymlinkedContextDirectoryBeforeProcessStart(t *testing.T) {
	t.Parallel()
	boundary := &fakeBoundary{}
	driver := mustDriver(t, validProfile(true), boundary)
	request := validExecutionRequest(t, driver)
	contextPath := filepath.Join(request.WorkDir, "context")
	pinnedPath := filepath.Join(request.WorkDir, "pinned-context")
	if err := os.Rename(contextPath, pinnedPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(pinnedPath, contextPath); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Execute(context.Background(), request, &recordingSink{}); err == nil {
		t.Fatal("symlinked context directory reached successful execution")
	}
	if boundary.calls != 0 {
		t.Fatalf("process calls = %d, want 0", boundary.calls)
	}
}

func TestDriverPreservesCodexLifecycleWithoutRetry(t *testing.T) {
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
		{name: "accepted outcome unknown", stdout: codexAcceptedJSONL(), wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishUnknownV1, wantFailure: domain.ProviderExecutionFailureAcceptedUnknownV1},
		{name: "pre acceptance failure", stdout: `{"type":"thread.started","thread_id":"private"}` + "\n", boundaryErr: errors.New("private"), wantAccept: domain.ProviderAcceptancePreAcceptanceV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailurePreAcceptanceV1},
		{name: "protocol drift", stdout: codexAcceptedJSONL() + `{"type":"item.completed","item":{"type":"command_execution"}}` + "\n", wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureProtocolDriftV1},
		{name: "provider failure", stdout: codexAcceptedJSONL() + `{"type":"error","message":"private"}` + "\n", wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureProviderFailedV1},
		{name: "cancelled", stdout: codexAcceptedJSONL(), mutate: func(result *ProcessResultV1) { result.Cancelled = true }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishCancelledV1, wantFailure: domain.ProviderExecutionFailureCancelledV1},
		{name: "credential finalization", stdout: successfulCodexJSONL("answer"), mutate: func(result *ProcessResultV1) { result.CredentialFinalized = false }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureCredentialFinalizeV1},
		{name: "teardown", stdout: successfulCodexJSONL("answer"), mutate: func(result *ProcessResultV1) { result.DescendantsStopped = false }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureTeardownV1},
		{name: "second provider effect", stdout: successfulCodexJSONL("answer"), mutate: func(result *ProcessResultV1) { result.ProviderEffects = 2 }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureBackendV1},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			raw := []byte(testCase.stdout)
			process := successfulProcess(raw)
			if testCase.mutate != nil {
				testCase.mutate(&process)
			}
			driver := mustDriver(t, validProfile(true), &fakeBoundary{result: process, err: testCase.boundaryErr})
			request := validExecutionRequest(t, driver)
			result, err := driver.Execute(context.Background(), request, &recordingSink{})
			if err == nil || result.ProviderEvidence == nil {
				t.Fatalf("result=%+v err=%v, want sealed failure evidence", result, err)
			}
			evidence := result.ProviderEvidence
			if evidence.AcceptanceClass != testCase.wantAccept || evidence.FinishClass != testCase.wantFinish || evidence.FailureCode != testCase.wantFailure {
				t.Fatalf("lifecycle=(%s,%s,%s), want=(%s,%s,%s)", evidence.AcceptanceClass, evidence.FinishClass, evidence.FailureCode, testCase.wantAccept, testCase.wantFinish, testCase.wantFailure)
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
		t.Fatalf("preflight error = %v", err)
	}
	if _, err := driver.Execute(context.Background(), request, &recordingSink{}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("execute error = %v", err)
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
		{name: "relative executable", mutate: func(profile *ProfileV1) { profile.Executable = "codex" }},
		{name: "empty digest", mutate: func(profile *ProfileV1) { profile.ExecutableDigest = attachedworkerdaemon.ExecutableDigest{} }},
		{name: "unbounded timeout", mutate: func(profile *ProfileV1) { profile.ProviderTimeoutMS = 300_001 }},
		{name: "output consumes context", mutate: func(profile *ProfileV1) { profile.MaxOutputTokens = profile.ContextWindow }},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			profile := validProfile(false)
			testCase.mutate(&profile)
			if _, err := NewDriver(profile, &fakeBoundary{}); !errors.Is(err, ErrContract) {
				t.Fatalf("NewDriver error = %v", err)
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
		Enabled: enabled, Executable: "/opt/sessionless/codex", ExecutableVersion: "0.153.0",
		ExecutableDigest: attachedworkerdaemon.ExecutableDigest{1, 2, 3}, ProviderTimeoutMS: 120_000,
		ContextWindow: defaultContextWindowV1, MaxOutputTokens: defaultMaxOutputTokensV1,
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
		Backend: driver.DescriptorV1(), Resource: domain.ProviderResourceBindingV1{
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

func successfulProcess(stdout []byte) ProcessResultV1 {
	return ProcessResultV1{
		ExitCode: 0, StdoutBytes: len(stdout), ProcessStopped: true, DescendantsStopped: true,
		CleanupSucceeded: true, PrivateStateRemoved: true, CredentialFinalized: true,
		ProviderEffectFenceSatisfied: true, ProviderEffects: 1, Stdout: stdout,
	}
}

func successfulCodexJSONL(final string) string {
	encoded, err := json.Marshal(final)
	if err != nil {
		panic(err)
	}
	return codexAcceptedJSONL() + `{"type":"item.completed","item":{"type":"agent_message","text":` + string(encoded) + `}}` + "\n" + `{"type":"turn.completed"}` + "\n"
}

func codexAcceptedJSONL() string {
	return `{"type":"thread.started","thread_id":"native-private"}` + "\n" + `{"type":"turn.started"}` + "\n"
}

func cloneGeneratedFiles(files []GeneratedFileV1) []GeneratedFileV1 {
	clone := make([]GeneratedFileV1, len(files))
	for index, file := range files {
		clone[index] = GeneratedFileV1{Name: file.Name, Content: append([]byte(nil), file.Content...)}
	}
	return clone
}
