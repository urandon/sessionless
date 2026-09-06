package directopenrouter

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

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

type fakeBoundary struct {
	calls      int
	cancels    int
	invocation InvocationV1
	result     ResultV1
	err        error
}

func (boundary *fakeBoundary) Invoke(_ context.Context, invocation InvocationV1) (ResultV1, error) {
	boundary.calls++
	invocation.Headers = append([]HeaderV1(nil), invocation.Headers...)
	invocation.Body = append([]byte(nil), invocation.Body...)
	boundary.invocation = invocation
	result := boundary.result
	if result.RequestBytesWritten == 1 {
		result.RequestBytesWritten = uint64(len(invocation.Body))
	}
	return result, boundary.err
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

func TestDriverExecutesOneExactBoundedProviderEffect(t *testing.T) {
	t.Parallel()
	response := successResponse("bounded result")
	boundary := &fakeBoundary{result: successfulTransport(response)}
	driver := mustDriver(t, validProfile(true), boundary)
	request := validExecutionRequest(t, driver)
	sink := &recordingSink{}
	result, err := driver.Execute(context.Background(), request, sink)
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary != "bounded result" || result.ProviderEvidence == nil || boundary.calls != 1 {
		t.Fatalf("result=%+v calls=%d", result, boundary.calls)
	}
	evidence := result.ProviderEvidence
	if evidence.FinishClass != domain.ProviderFinishCompletedV1 || evidence.RouteState != domain.ProviderEvidenceSupportedV1 ||
		evidence.ActualModelVendorID != ModelVendorIDV1 || evidence.ActualModelID != ModelIDV1 ||
		evidence.TransportKind != domain.ProviderTransportRouterAPIV1 || evidence.TransportProvider != TransportProviderV1 ||
		evidence.UpstreamProviderID != ProviderIDV1 || evidence.EndpointID != EndpointIDV1 ||
		evidence.UsageProvenance != domain.ProviderUsageProviderReportedV1 || evidence.ValidateForBinding(request.HarnessBinding) != nil {
		t.Fatalf("evidence = %+v", evidence)
	}
	invocation := boundary.invocation
	if invocation.URL != EndpointURLV1 || invocation.Method != "POST" || !reflect.DeepEqual(invocation.Headers, requestHeaders()) ||
		invocation.AuthorizationHeaderName != "Authorization" || invocation.AuthorizationScheme != "Bearer" ||
		!invocation.RequireTLSValidation || !invocation.DenyAmbientCredentials || !invocation.DenyRedirects || !invocation.DenyAmbientProxy ||
		!invocation.DenyCookies || !invocation.DenyCompression || !invocation.RequireProviderEffectFence ||
		invocation.MaxProviderEffects != 1 || invocation.Credential.HandleID != "credential-a" ||
		invocation.CredentialMaterialization.Kind != domain.ProviderCredentialDeliveryDirectV1 {
		t.Fatalf("invocation = %+v", invocation)
	}
	if len(sink.events) != 1 || sink.events[0].Boundary != "openrouter.chat_completed" ||
		sink.events[0].InputTokens == nil || *sink.events[0].InputTokens != 11 {
		t.Fatalf("events = %+v", sink.events)
	}
	formatted := invocation.String() + boundary.result.String()
	if strings.Contains(formatted, "public fixture") || strings.Contains(formatted, "bounded result") || !strings.Contains(formatted, "[redacted") {
		t.Fatalf("formatted boundary leaked content: %s", formatted)
	}
	encoded, err := json.Marshal(invocation)
	if err != nil || strings.Contains(string(encoded), "public fixture") || strings.Contains(string(encoded), "credential-a") {
		t.Fatalf("JSON leaked private content: %s err=%v", encoded, err)
	}
}

func TestDriverFailsClosedBeforeNetworkForAuthorityMutations(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*ports.ExecutionRequest){
		"wrong delivery": func(value *ports.ExecutionRequest) {
			value.CredentialMaterialization.Kind = domain.ProviderCredentialDeliveryEnvironmentV1
			value.CredentialMaterialization.EnvironmentName = "OPENROUTER_API_KEY"
		},
		"wrong model": func(value *ports.ExecutionRequest) { value.HarnessBinding.ModelID = "other" },
		"private input": func(value *ports.ExecutionRequest) {
			value.HarnessBinding.InputDataClass = domain.ProviderDataPrivateV1
		},
		"credential generation": func(value *ports.ExecutionRequest) { value.Credential.ProviderResource.CredentialGeneration++ },
		"credential expiry": func(value *ports.ExecutionRequest) {
			value.Credential.ExpiresAt = value.HarnessBinding.EvidenceExpiresAt.Add(time.Second)
		},
		"ambient MCP": func(value *ports.ExecutionRequest) { value.AllowedMCPServers = []string{"ambient"} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			boundary := &fakeBoundary{}
			driver := mustDriver(t, validProfile(true), boundary)
			request := validExecutionRequest(t, driver)
			mutate(&request)
			if _, err := driver.Execute(context.Background(), request, &recordingSink{}); err == nil || boundary.calls != 0 {
				t.Fatalf("err=%v calls=%d", err, boundary.calls)
			}
		})
	}
}

func TestDriverNeverRetriesAmbiguousOrRejectedEffects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		result      ResultV1
		err         error
		wantFinish  domain.ProviderFinishClassV1
		wantFailure domain.ProviderExecutionFailureCodeV1
	}{
		{name: "post-write unknown", result: ResultV1{AcceptanceClass: domain.ProviderAcceptanceAcceptedV1, RequestBytesWritten: 1, CleanupSucceeded: true, CredentialFinalized: true, ProviderEffectFenceSatisfied: true, ProviderEffects: 1}, err: errors.New("transport lost"), wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureProviderFailedV1},
		{name: "cancelled", result: ResultV1{AcceptanceClass: domain.ProviderAcceptanceAcceptedV1, Cancelled: true, CleanupSucceeded: true, CredentialFinalized: true, ProviderEffectFenceSatisfied: true, ProviderEffects: 1}, err: context.Canceled, wantFinish: domain.ProviderFinishCancelledV1, wantFailure: domain.ProviderExecutionFailureCancelledV1},
		{name: "redirect", result: ResultV1{AcceptanceClass: domain.ProviderAcceptanceAcceptedV1, StatusCode: 302, Redirected: true, CleanupSucceeded: true, CredentialFinalized: true, ProviderEffectFenceSatisfied: true, ProviderEffects: 1}, wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailureProviderFailedV1},
		{name: "pre acceptance", result: ResultV1{AcceptanceClass: domain.ProviderAcceptancePreAcceptanceV1}, err: errors.New("dial denied"), wantFinish: domain.ProviderFinishFailedV1, wantFailure: domain.ProviderExecutionFailurePreAcceptanceV1},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			boundary := &fakeBoundary{result: testCase.result, err: testCase.err}
			driver := mustDriver(t, validProfile(true), boundary)
			result, err := driver.Execute(context.Background(), validExecutionRequest(t, driver), &recordingSink{})
			if err == nil || boundary.calls != 1 || result.ProviderEvidence == nil ||
				result.ProviderEvidence.FinishClass != testCase.wantFinish || result.ProviderEvidence.FailureCode != testCase.wantFailure {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, boundary.calls)
			}
		})
	}
}

func TestMalformedSuccessfulResponseFailsAsProtocolDriftWithoutRetry(t *testing.T) {
	t.Parallel()
	response := append(successResponse("answer"), []byte(` {}`)...)
	boundary := &fakeBoundary{result: successfulTransport(response)}
	driver := mustDriver(t, validProfile(true), boundary)
	result, err := driver.Execute(context.Background(), validExecutionRequest(t, driver), &recordingSink{})
	if err == nil || boundary.calls != 1 || result.ProviderEvidence == nil ||
		result.ProviderEvidence.AcceptanceClass != domain.ProviderAcceptanceAcceptedV1 ||
		result.ProviderEvidence.FinishClass != domain.ProviderFinishFailedV1 ||
		result.ProviderEvidence.FailureCode != domain.ProviderExecutionFailureProtocolDriftV1 {
		t.Fatalf("result=%+v err=%v calls=%d", result, err, boundary.calls)
	}
	for index, value := range response {
		if value != 0 {
			t.Fatalf("provider body byte %d survived reduction", index)
		}
	}
}

func TestCompletedEffectIsNotRetriedAfterSinkFailure(t *testing.T) {
	t.Parallel()
	response := successResponse("answer")
	boundary := &fakeBoundary{result: successfulTransport(response)}
	driver := mustDriver(t, validProfile(true), boundary)
	result, err := driver.Execute(context.Background(), validExecutionRequest(t, driver), &recordingSink{err: errors.New("sink")})
	if err == nil || boundary.calls != 1 || result.Summary != "" || result.ProviderEvidence == nil ||
		result.ProviderEvidence.FinishClass != domain.ProviderFinishCompletedV1 {
		t.Fatalf("result=%+v err=%v calls=%d", result, err, boundary.calls)
	}
}

func TestDisabledRegistrationFailsClosed(t *testing.T) {
	t.Parallel()
	boundary := &fakeBoundary{}
	driver := mustDriver(t, validProfile(false), boundary)
	registration, err := DisabledRegistrationV1(driver)
	if err != nil || registration.Enabled || registration.Descriptor != driver.DescriptorV1() ||
		registration.Descriptor.CredentialDeliveryKind != domain.ProviderCredentialDeliveryDirectV1 ||
		driver.BackendProtocolState() != harnessconformance.BackendProtocolUnsupportedV1 {
		t.Fatalf("registration=%+v err=%v", registration, err)
	}
	request := validExecutionRequest(t, driver)
	if _, err := driver.Execute(context.Background(), request, &recordingSink{}); !errors.Is(err, ErrDisabled) || boundary.calls != 0 {
		t.Fatalf("err=%v calls=%d", err, boundary.calls)
	}
	identity := ports.ExecutionIdentity{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, RunID: request.RunID, AttemptID: request.AttemptID,
		ExecutionPlacementV2: request.ExecutionPlacementV2, HarnessBinding: request.HarnessBinding,
	}
	if err := driver.Cancel(context.Background(), identity); err != nil || boundary.cancels != 1 {
		t.Fatalf("disabled adapter must preserve teardown routing: err=%v cancels=%d", err, boundary.cancels)
	}
}

func TestProfileIsImmutableAcrossFeatureGateAndRejectsUnboundedValues(t *testing.T) {
	t.Parallel()
	disabled := mustDriver(t, validProfile(false), &fakeBoundary{})
	enabled := mustDriver(t, validProfile(true), &fakeBoundary{})
	if disabled.DescriptorV1() != enabled.DescriptorV1() ||
		disabled.DescriptorV1().ArtifactKind != domain.HarnessArtifactEmbeddedProfileV1 ||
		disabled.DescriptorV1().NativeProtocolVersion != NativeProtocolVersionV1 {
		t.Fatalf("descriptors differ: disabled=%+v enabled=%+v", disabled.DescriptorV1(), enabled.DescriptorV1())
	}
	for _, profile := range []ProfileV1{
		{RequestTimeoutMS: 0, MaxCompletionTokens: 8192},
		{RequestTimeoutMS: 300_001, MaxCompletionTokens: 8192},
		{RequestTimeoutMS: 1, MaxCompletionTokens: 15},
		{RequestTimeoutMS: 1, MaxCompletionTokens: 65_537},
	} {
		if _, err := NewDriver(profile, &fakeBoundary{}); !errors.Is(err, ErrContract) {
			t.Fatalf("profile %+v error=%v", profile, err)
		}
	}
}

func TestContextHistorySymlinkFailsBeforeNetwork(t *testing.T) {
	t.Parallel()
	boundary := &fakeBoundary{}
	driver := mustDriver(t, validProfile(true), boundary)
	request := validExecutionRequest(t, driver)
	history := filepath.Join(request.WorkDir, "context", "history.jsonl")
	real := filepath.Join(request.WorkDir, "history-real.jsonl")
	content, err := os.ReadFile(history)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(real, content, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(history); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, history); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Execute(context.Background(), request, &recordingSink{}); err == nil || boundary.calls != 0 {
		t.Fatalf("err=%v calls=%d", err, boundary.calls)
	}
}

func mustDriver(t *testing.T, profile ProfileV1, boundary HTTPBoundaryV1) *Driver {
	t.Helper()
	driver, err := NewDriver(profile, boundary)
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func validProfile(enabled bool) ProfileV1 {
	return ProfileV1{Enabled: enabled, RequestTimeoutMS: 120_000, MaxCompletionTokens: 8192}
}

func successfulTransport(response []byte) ResultV1 {
	return ResultV1{
		StatusCode: 200, ContentType: "application/json", AcceptanceClass: domain.ProviderAcceptanceAcceptedV1,
		RequestBytesWritten: 1, ResponseBytes: uint64(len(response)), CleanupSucceeded: true, CredentialFinalized: true,
		ProviderEffectFenceSatisfied: true, ProviderEffects: 1, Body: response,
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
		Payload:   domain.BlobRef{TenantID: "tenant-a", Key: domain.SessionEventObjectPrefix("tenant-a", "session-a", "event-a") + "message.json", Size: int64(len(payload)), SHA256: hex.EncodeToString(payloadDigest[:])},
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
		Version: 1, TenantID: "tenant-a", OwnerUserID: "user-a", RunID: "run-a", AttemptID: "attempt-a", Backend: driver.DescriptorV1(),
		Resource:      domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceRouterAccountV1, ResourceID: "openrouter-a", OwnerUserID: "user-a", Revision: 3, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 7},
		ModelVendorID: ModelVendorIDV1, ModelID: ModelIDV1, InputDataClass: domain.ProviderDataExternallyShareableV1,
		ProviderCatalogDigest: strings.Repeat("1", 64), ProviderRouteDigest: strings.Repeat("2", 64), PrivacyPolicyDigest: strings.Repeat("3", 64), CapabilityEvidenceDigest: strings.Repeat("4", 64), EffectivePolicyDigest: strings.Repeat("5", 64), ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &expires,
	}
	request := ports.ExecutionRequest{
		TenantID: "tenant-a", OwnerUserID: "user-a", RunID: "run-a", SessionID: "session-a", TriggerEventID: "event-a", AttemptID: "attempt-a", WorkDir: workDir,
		ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1}, ExecutionPlacementV2: placement, HarnessBinding: binding,
		Credential:                ports.ProviderInvocationCredentialV1{HandleID: "credential-a", TenantID: "tenant-a", OwnerUserID: "user-a", RunID: "run-a", AttemptID: "attempt-a", WorkerID: "worker-a", LeaseID: "lease-a", LeaseFence: 5, ProviderResource: binding.Resource, ExpiresAt: expires},
		CredentialMaterialization: ports.ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryDirectV1},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("request invalid: %v", err)
	}
	return request
}
