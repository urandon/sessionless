package opencodeopenrouter

import (
	"bytes"
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
	invocation.PrivateDirectories = append([]PrivateDirectoryV1(nil), invocation.PrivateDirectories...)
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

func TestProfileGeneratesClosedOpenCodeConfiguration(t *testing.T) {
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
		registration.Descriptor.BackendKind != domain.HarnessBackendOpenCodeV1 ||
		registration.Descriptor.CredentialDeliveryKind != domain.ProviderCredentialDeliveryEnvironmentV1 {
		t.Fatalf("registration = %+v", registration)
	}
	files, err := generatedFiles(profile)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].DirectoryEnvironmentName != configDirectoryEnvironmentV1 ||
		files[0].RelativeDirectory != configRelativeDirectoryV1 || files[0].Name != "opencode.json" {
		t.Fatalf("generated files differ: %#v", files)
	}
	var config configFileV1
	if err := json.Unmarshal(files[0].Content, &config); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(config)
	if err != nil || !bytes.Equal(canonical, files[0].Content) {
		t.Fatalf("generated config is not canonical: err=%v", err)
	}
	router := config.Provider[ProviderIDV1]
	model := router.Models[ModelIDV1]
	agent := config.Agent["sessionless"]
	if config.Schema != "https://opencode.ai/config.json" || config.Autoupdate || config.Share != "disabled" || config.Snapshot ||
		!reflect.DeepEqual(config.EnabledProviders, []string{ProviderIDV1}) || config.Model != modelSelectorV1 || config.SmallModel != modelSelectorV1 ||
		config.DefaultAgent != "sessionless" || config.SubagentDepth != 0 || len(config.Plugin) != 0 || len(config.Command) != 0 ||
		len(config.Skills.Paths) != 0 || len(config.Skills.URLs) != 0 || len(config.MCP) != 0 || config.Formatter || config.LSP ||
		len(config.Instructions) != 0 || config.Permission != "deny" || config.Tools["*"] || config.Compaction.Auto || config.Compaction.Prune ||
		config.Experimental.OpenTelemetry || len(config.Experimental.PrimaryTools) != 0 || config.Experimental.ContinueLoopOnDeny ||
		!reflect.DeepEqual(router.Environment, []string{CredentialEnvironmentV1}) || !reflect.DeepEqual(router.Whitelist, []string{ModelIDV1}) ||
		router.Options.BaseURL != OpenRouterBaseURLV1 || router.Options.Timeout != 120_000 || router.Options.HeaderTimeout != 120_000 || router.Options.ChunkTimeout != 120_000 ||
		model.ID != ModelIDV1 || model.Reasoning || model.ToolCall || !reflect.DeepEqual(model.Modalities.Input, []string{"text"}) || !reflect.DeepEqual(model.Modalities.Output, []string{"text"}) ||
		!reflect.DeepEqual(model.Options.Provider.Only, []string{ModelVendorIDV1}) || model.Options.Provider.AllowFallbacks || !model.Options.Provider.RequireParameters ||
		agent.Mode != "primary" || agent.Model != modelSelectorV1 || agent.Steps != 1 || agent.Permission != "deny" || agent.Tools["*"] {
		t.Fatalf("generated config is not closed: %+v", config)
	}
	if strings.Contains(string(files[0].Content), "credential-a") {
		t.Fatal("generated config contains credential material")
	}
	wantArguments := []string{
		"--pure", "run", "--format", "json", "--model", modelSelectorV1,
		"--agent", "sessionless", "--title", "sessionless-invocation", "--no-thinking",
	}
	if !reflect.DeepEqual(processArguments(), wantArguments) {
		t.Fatalf("arguments = %q, want %q", processArguments(), wantArguments)
	}
	wantEnvironment := []EnvironmentV1{
		{Name: "OPENCODE_DISABLE_AUTOUPDATE", Value: "1"},
		{Name: "OPENCODE_DISABLE_AUTOCOMPACT", Value: "1"},
		{Name: "OPENCODE_DISABLE_DEFAULT_PLUGINS", Value: "1"},
		{Name: "OPENCODE_DISABLE_MODELS_FETCH", Value: "1"},
		{Name: "OPENCODE_DISABLE_PROJECT_CONFIG", Value: "1"},
		{Name: "OPENCODE_DISABLE_PRUNE", Value: "1"},
		{Name: "OPENCODE_PURE", Value: "1"},
		{Name: "OPENCODE_CLIENT", Value: "sessionless"},
		{Name: "DO_NOT_TRACK", Value: "1"},
		{Name: "NO_COLOR", Value: "1"},
	}
	wantPrivateDirectories := []PrivateDirectoryV1{
		{EnvironmentName: "HOME", Purpose: "home"},
		{EnvironmentName: "XDG_DATA_HOME", Purpose: "data"},
		{EnvironmentName: "XDG_CACHE_HOME", Purpose: "cache"},
		{EnvironmentName: "XDG_CONFIG_HOME", Purpose: "config", ReadOnlyAfterMaterialization: true},
		{EnvironmentName: "XDG_STATE_HOME", Purpose: "state"},
		{EnvironmentName: "TMPDIR", Purpose: "temporary"},
	}
	if !reflect.DeepEqual(processEnvironment(), wantEnvironment) || !reflect.DeepEqual(privateDirectories(), wantPrivateDirectories) {
		t.Fatalf("closed environment isolation differs: env=%+v dirs=%+v", processEnvironment(), privateDirectories())
	}
}

func TestProfileDigestSealsGeneratedConfigAndIsolationProfile(t *testing.T) {
	t.Parallel()
	base := validProfile(false)
	baseDriver := mustDriver(t, base, &fakeBoundary{})
	enabled := base
	enabled.Enabled = true
	if got := mustDriver(t, enabled, &fakeBoundary{}).DescriptorV1(); got != baseDriver.DescriptorV1() {
		t.Fatal("reversible feature gate changed immutable descriptor")
	}
	changedTimeout := base
	changedTimeout.ProviderTimeoutMS++
	if got := mustDriver(t, changedTimeout, &fakeBoundary{}).DescriptorV1(); got.BackendProfileDigest == baseDriver.DescriptorV1().BackendProfileDigest {
		t.Fatal("generated config timeout was not sealed into profile digest")
	}
	changedArtifact := base
	changedArtifact.ExecutableDigest[0]++
	got := mustDriver(t, changedArtifact, &fakeBoundary{}).DescriptorV1()
	if got.ArtifactDigest == baseDriver.DescriptorV1().ArtifactDigest || got.BackendProfileDigest == baseDriver.DescriptorV1().BackendProfileDigest {
		t.Fatal("executable mutation was not sealed into descriptor")
	}
}

func TestDriverExecutesOneBoundedPromptAndSealsUnknownRoute(t *testing.T) {
	t.Parallel()
	rawOutput := []byte(successfulOpenCodeJSONL("bounded result"))
	boundary := &fakeBoundary{result: ProcessResultV1{
		ExitCode: 0, StdoutBytes: len(rawOutput), ProcessStopped: true, DescendantsStopped: true,
		CleanupSucceeded: true, PrivateStateRemoved: true, CredentialFinalized: true,
		ProviderEffectFenceSatisfied: true, ProviderEffects: 1,
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
	if len(sink.events) != 1 || sink.events[0].Boundary != "opencode.step_finished" || sink.events[0].Sequence != 1 {
		t.Fatalf("events = %+v", sink.events)
	}
	invocation := boundary.invocation
	if boundary.calls != 1 || invocation.Credential.HandleID != "credential-a" ||
		invocation.CredentialMaterialization.EnvironmentName != CredentialEnvironmentV1 ||
		!invocation.RequirePrivateWorkingDirectory || !invocation.RequireSanitizedEnvironment || !invocation.RequireNoAmbientHome ||
		!invocation.RequireProviderEffectFence || invocation.MaxProviderEffects != 1 ||
		!reflect.DeepEqual(invocation.Arguments, processArguments()) ||
		!reflect.DeepEqual(invocation.Environment, processEnvironment()) ||
		!reflect.DeepEqual(invocation.PrivateDirectories, privateDirectories()) {
		t.Fatalf("invocation authority/config differs: %+v", invocation)
	}
	if string(invocation.Stdin) != "Sessionless canonical transcript v1\n[user]\npublic fixture\n" ||
		len(invocation.GeneratedFiles) != 1 || invocation.GeneratedFiles[0].DirectoryEnvironmentName != configDirectoryEnvironmentV1 ||
		invocation.GeneratedFiles[0].RelativeDirectory != configRelativeDirectoryV1 {
		t.Fatalf("OpenCode invocation metadata differs: %+v", invocation)
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
	rawOutput := []byte(successfulOpenCodeJSONL("bounded result"))
	boundary := &fakeBoundary{result: ProcessResultV1{
		ExitCode: 0, StdoutBytes: len(rawOutput), ProcessStopped: true, DescendantsStopped: true,
		CleanupSucceeded: true, PrivateStateRemoved: true, CredentialFinalized: true,
		ProviderEffectFenceSatisfied: true, ProviderEffects: 1,
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
		{name: "accepted outcome unknown", stdout: openCodeStepStartJSONL(), wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishUnknownV1, wantFailure: domain.ProviderExecutionFailureAcceptedUnknownV1},
		{name: "pre acceptance runner failure", stdout: openCodeErrorJSONL(), boundaryErr: errors.New("private runner failure"), wantAccept: domain.ProviderAcceptancePreAcceptanceV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailurePreAcceptanceV1},
		{name: "protocol drift", stdout: openCodeStepStartJSONL() + `{"type":"tool_use","timestamp":2,"sessionID":"ses-a","part":{"id":"tool-a","sessionID":"ses-a","messageID":"msg-a","type":"tool"}}` + "\n", wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureProtocolDriftV1},
		{name: "provider failure", stdout: openCodeStepStartJSONL() + openCodeErrorJSONL(), wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureProviderFailedV1},
		{name: "cancelled", stdout: openCodeStepStartJSONL(), mutate: func(result *ProcessResultV1) { result.Cancelled = true }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishCancelledV1, wantFailure: domain.ProviderExecutionFailureCancelledV1},
		{name: "deadline", stdout: successfulOpenCodeJSONL("answer"), mutate: func(result *ProcessResultV1) { result.Deadline = true }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishCancelledV1, wantFailure: domain.ProviderExecutionFailureCancelledV1},
		{name: "stdout accounting mismatch", stdout: successfulOpenCodeJSONL("answer"), mutate: func(result *ProcessResultV1) { result.StdoutBytes++ }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureBackendV1},
		{name: "stderr limit", stdout: successfulOpenCodeJSONL("answer"), mutate: func(result *ProcessResultV1) { result.StderrBytes = maxStderrBytes + 1 }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureBackendV1},
		{name: "cleanup failure", stdout: successfulOpenCodeJSONL("answer"), mutate: func(result *ProcessResultV1) { result.CleanupSucceeded = false }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureTeardownV1},
		{name: "process not stopped", stdout: successfulOpenCodeJSONL("answer"), mutate: func(result *ProcessResultV1) { result.ProcessStopped = false }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureTeardownV1},
		{name: "descendant not stopped", stdout: successfulOpenCodeJSONL("answer"), mutate: func(result *ProcessResultV1) { result.DescendantsStopped = false }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureTeardownV1},
		{name: "private state retained", stdout: successfulOpenCodeJSONL("answer"), mutate: func(result *ProcessResultV1) { result.PrivateStateRemoved = false }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureTeardownV1},
		{name: "credential finalization failure", stdout: successfulOpenCodeJSONL("answer"), mutate: func(result *ProcessResultV1) { result.CredentialFinalized = false }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureCredentialFinalizeV1},
		{name: "provider effect fence unproven", stdout: successfulOpenCodeJSONL("answer"), mutate: func(result *ProcessResultV1) { result.ProviderEffectFenceSatisfied = false }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureBackendV1},
		{name: "multiple provider effects", stdout: successfulOpenCodeJSONL("answer"), mutate: func(result *ProcessResultV1) { result.ProviderEffects = 2 }, wantAccept: domain.ProviderAcceptanceAcceptedV1, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureBackendV1},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			process := ProcessResultV1{ProcessStopped: true, DescendantsStopped: true, CleanupSucceeded: true, PrivateStateRemoved: true, CredentialFinalized: true, ProviderEffectFenceSatisfied: true, ProviderEffects: 1, Stdout: []byte(testCase.stdout), StdoutBytes: len(testCase.stdout)}
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
		Enabled: enabled, Executable: "/opt/sessionless/opencode", ExecutableVersion: "1.2.3",
		ExecutableDigest: attachedworkerdaemon.ExecutableDigest{1, 2, 3}, SourceRevision: OpenCodeSourceRevisionV1,
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

func successfulOpenCodeJSONL(final string) string {
	return openCodeStepStartJSONL() + openCodeTextJSONL(final) + openCodeStepFinishJSONL("stop")
}

func openCodeStepStartJSONL() string {
	return `{"type":"step_start","timestamp":1,"sessionID":"ses-a","part":{"id":"part-start","sessionID":"ses-a","messageID":"msg-a","type":"step-start"}}` + "\n"
}

func openCodeTextJSONL(final string) string {
	encoded, err := json.Marshal(final)
	if err != nil {
		panic(err)
	}
	return `{"type":"text","timestamp":2,"sessionID":"ses-a","part":{"id":"part-text","sessionID":"ses-a","messageID":"msg-a","type":"text","text":` + string(encoded) + `,"time":{"start":1,"end":2}}}` + "\n"
}

func openCodeStepFinishJSONL(reason string) string {
	return `{"type":"step_finish","timestamp":3,"sessionID":"ses-a","part":{"id":"part-finish","sessionID":"ses-a","messageID":"msg-a","type":"step-finish","reason":"` + reason + `","cost":0,"tokens":{"input":11,"output":7,"reasoning":0,"cache":{"read":0,"write":0}}}}` + "\n"
}

func openCodeErrorJSONL() string {
	return `{"type":"error","timestamp":4,"sessionID":"ses-a","error":{"name":"ProviderAuthError"}}` + "\n"
}

func cloneGeneratedFiles(files []GeneratedFileV1) []GeneratedFileV1 {
	clone := make([]GeneratedFileV1, len(files))
	for index, file := range files {
		clone[index] = GeneratedFileV1{DirectoryEnvironmentName: file.DirectoryEnvironmentName, RelativeDirectory: file.RelativeDirectory, Name: file.Name, Content: append([]byte(nil), file.Content...)}
	}
	return clone
}
