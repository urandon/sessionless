package sessionlessharness_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

type recordingDriver struct {
	executeCalls   int
	cancelCalls    int
	preflightCalls int
	preflightErr   error
	executeErr     error
	executeResult  ports.ExecutionResult
	cancelErr      error
}

func (driver *recordingDriver) Preflight(_ context.Context, identity ports.ExecutionIdentity) error {
	driver.preflightCalls++
	if driver.preflightErr != nil {
		return driver.preflightErr
	}
	return identity.Validate()
}

func (driver *recordingDriver) Execute(_ context.Context, request ports.ExecutionRequest, _ ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	driver.executeCalls++
	evidence, err := (domain.ProviderExecutionEvidenceV1{
		AcceptanceClass: domain.ProviderAcceptanceAcceptedV1, FinishClass: domain.ProviderFinishCompletedV1,
		RouteState: domain.ProviderEvidenceSupportedV1, ActualModelVendorID: request.HarnessBinding.ModelVendorID, ActualModelID: request.HarnessBinding.ModelID,
		TransportKind:     domain.ProviderTransportLocalCLIV1,
		TransportProvider: "sessionless", UpstreamProviderID: "local", EndpointID: "recording-fixture",
		PolicyVerdict: domain.ProviderPolicyGoV1, UsageProvenance: domain.ProviderUsageUnknownV1,
	}).SealForBinding(request.HarnessBinding)
	if err != nil {
		return ports.ExecutionResult{}, err
	}
	result := driver.executeResult
	if result.Summary == "" {
		result.Summary = "ok"
	}
	result.ProviderEvidence = &evidence
	return result, driver.executeErr
}
func (driver *recordingDriver) Cancel(_ context.Context, _ ports.ExecutionIdentity) error {
	driver.cancelCalls++
	return driver.cancelErr
}

type noopSink struct{}

func (noopSink) Emit(context.Context, ports.ExecutionEvent) error { return nil }

func registryFixture(t *testing.T) (*sessionlessharness.Registry, *recordingDriver, ports.ExecutionRequest) {
	t.Helper()
	now := time.Unix(10, 0).UTC()
	driver := &recordingDriver{}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return now }, sessionlessharness.Registration{
		Descriptor:      sessionlessharness.DeterministicFixtureDescriptorV1(),
		Enabled:         true,
		ValidateBinding: sessionlessharness.ValidateDeterministicFixtureBindingV1,
		Driver:          driver,
	})
	if err != nil {
		t.Fatal(err)
	}
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(
		"tenant-1", "user-1", "run-1", "attempt-1", "subscription-1", time.Unix(10, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return registry, driver, ports.ExecutionRequest{
		TenantID: "tenant-1", OwnerUserID: "user-1", RunID: "run-1", SessionID: "session-1", TriggerEventID: "event-1", AttemptID: "attempt-1",
		WorkDir: "/tmp/sessionless-registry-test", ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1},
		ExecutionPlacementV2: authority.ExecutionPlacementV2, HarnessBinding: authority.HarnessBinding,
		SubstrateBinding: cloneSubstrateBindingForTest(authority.SubstrateBinding), AdmissionCostCeiling: cloneAdmissionCostCeilingForTest(authority.AdmissionCostCeiling),
	}
}

func TestRegistryUsesOnlyExactSealedBackend(t *testing.T) {
	t.Parallel()
	registry, driver, request := registryFixture(t)
	if _, err := registry.Execute(context.Background(), request, noopSink{}); err != nil {
		t.Fatal(err)
	}
	if driver.executeCalls != 1 {
		t.Fatalf("execute calls = %d, want 1", driver.executeCalls)
	}
	identity := identityForRequest(request)
	if err := registry.Cancel(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	if driver.cancelCalls != 1 {
		t.Fatalf("cancel calls = %d, want 1", driver.cancelCalls)
	}
}

func TestRegistryNeverFallsBackOnNearMatch(t *testing.T) {
	t.Parallel()
	registry, driver, request := registryFixture(t)
	request.HarnessBinding.Backend.BackendProfileDigest = strings.Repeat("a", 64)
	if _, err := registry.Execute(context.Background(), request, noopSink{}); err == nil {
		t.Fatal("near-match backend resolved")
	}
	if driver.executeCalls != 0 {
		t.Fatal("fallback backend was invoked")
	}
}

func TestRegistryRejectsDuplicateAndEmptyRegistration(t *testing.T) {
	t.Parallel()
	descriptor := sessionlessharness.DeterministicFixtureDescriptorV1()
	driver := &recordingDriver{}
	if _, err := sessionlessharness.NewRegistry(time.Now); err == nil {
		t.Fatal("empty registry accepted")
	}
	if _, err := sessionlessharness.NewRegistry(time.Now,
		sessionlessharness.Registration{Descriptor: descriptor, Enabled: true, ValidateBinding: sessionlessharness.ValidateDeterministicFixtureBindingV1, Driver: driver},
		sessionlessharness.Registration{Descriptor: descriptor, Enabled: true, ValidateBinding: sessionlessharness.ValidateDeterministicFixtureBindingV1, Driver: &recordingDriver{}},
	); err == nil {
		t.Fatal("duplicate registry descriptor accepted")
	}
}

func TestRegistryRejectsDynamicResourceMutationBeforeDriver(t *testing.T) {
	t.Parallel()
	registry, driver, request := registryFixture(t)
	request.HarnessBinding.Resource.ResourceID = "other-fixture"
	if _, err := registry.Execute(context.Background(), request, noopSink{}); err == nil {
		t.Fatal("resource mutation reached exact backend")
	}
	if driver.executeCalls != 0 {
		t.Fatal("driver called after resource mismatch")
	}
}

func invocationIdentity(t *testing.T, expires time.Time) ports.ExecutionIdentity {
	t.Helper()
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2("tenant-1", "user-1", "run-1", "attempt-1", "subscription-1", time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	placement := authority.ExecutionPlacementV2
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendCodexExecV1, ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "codex-jsonl.v1", BackendProfileDigest: strings.Repeat("2", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryFileV1}
	binding := domain.HarnessBindingV1{Version: 1, TenantID: "tenant-1", OwnerUserID: "user-1", RunID: "run-1", AttemptID: "attempt-1", Backend: descriptor, Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-1", OwnerUserID: "user-1", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}, ModelVendorID: "openai", ModelID: "codex-model", InputDataClass: domain.ProviderDataPrivateV1, ProviderCatalogDigest: strings.Repeat("3", 64), ProviderRouteDigest: strings.Repeat("4", 64), PrivacyPolicyDigest: strings.Repeat("5", 64), CapabilityEvidenceDigest: strings.Repeat("6", 64), EffectivePolicyDigest: strings.Repeat("7", 64), ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &expires}
	return ports.ExecutionIdentity{
		TenantID: "tenant-1", OwnerUserID: "user-1", RunID: "run-1", AttemptID: "attempt-1", ExecutionPlacementV2: placement, HarnessBinding: binding,
		SubstrateBinding: cloneSubstrateBindingForTest(authority.SubstrateBinding), AdmissionCostCeiling: cloneAdmissionCostCeilingForTest(authority.AdmissionCostCeiling),
	}
}

func TestRegistryFreshnessGatesStartButNotCancellation(t *testing.T) {
	t.Parallel()
	now := time.Unix(100, 0).UTC()
	expires := now.Add(time.Minute)
	identity := invocationIdentity(t, expires)
	driver := &recordingDriver{}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return now }, sessionlessharness.Registration{Descriptor: identity.HarnessBinding.Backend, Enabled: true, ValidateBinding: func(domain.HarnessBindingV1) sessionlessharness.FailureCode { return "" }, Driver: driver})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Preflight(context.Background(), identity); err != nil {
		t.Fatal(err)
	}
	now = expires
	if err := registry.Preflight(context.Background(), identity); err == nil {
		t.Fatal("expired evidence reached start preflight")
	}
	if err := registry.Cancel(context.Background(), identity); err != nil {
		t.Fatalf("expired active binding could not route cancellation: %v", err)
	}
	if driver.cancelCalls != 1 {
		t.Fatalf("cancel calls=%d, want 1", driver.cancelCalls)
	}
}

func TestRegistryDisabledRegistrationCannotStartButCanCancel(t *testing.T) {
	t.Parallel()
	now := time.Unix(100, 0).UTC()
	identity := invocationIdentity(t, now.Add(time.Minute))
	driver := &recordingDriver{}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return now }, sessionlessharness.Registration{
		Descriptor: identity.HarnessBinding.Backend, Enabled: false,
		ValidateBinding: func(domain.HarnessBindingV1) sessionlessharness.FailureCode { return "" }, Driver: driver,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Preflight(context.Background(), identity); err == nil ||
		!strings.Contains(err.Error(), string(sessionlessharness.FailureHarnessBackendDisabled)) {
		t.Fatalf("disabled preflight error = %v", err)
	}
	if driver.preflightCalls != 0 {
		t.Fatalf("disabled driver preflight calls = %d, want 0", driver.preflightCalls)
	}
	if err := registry.Cancel(context.Background(), identity); err != nil {
		t.Fatalf("disabled registration cancellation = %v", err)
	}
	if driver.cancelCalls != 1 {
		t.Fatalf("disabled driver cancel calls = %d, want 1", driver.cancelCalls)
	}
}

func TestRegistryRejectsOwnerPlacementAndInvalidValidatorCode(t *testing.T) {
	t.Parallel()
	now := time.Unix(100, 0).UTC()
	identity := invocationIdentity(t, now.Add(time.Minute))
	driver := &recordingDriver{}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return now }, sessionlessharness.Registration{Descriptor: identity.HarnessBinding.Backend, Enabled: true, ValidateBinding: func(domain.HarnessBindingV1) sessionlessharness.FailureCode {
		return sessionlessharness.FailureCode("private_provider_detail")
	}, Driver: driver})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Preflight(context.Background(), identity); err == nil || strings.Contains(err.Error(), "private_provider_detail") {
		t.Fatalf("validator error not sanitized: %v", err)
	}
	registry, err = sessionlessharness.NewRegistry(func() time.Time { return now }, sessionlessharness.Registration{Descriptor: identity.HarnessBinding.Backend, Enabled: true, ValidateBinding: func(domain.HarnessBindingV1) sessionlessharness.FailureCode { return "" }, Driver: driver})
	if err != nil {
		t.Fatal(err)
	}
	owner := identity
	owner.OwnerUserID = "user-2"
	if err := registry.Preflight(context.Background(), owner); err == nil {
		t.Fatal("cross-owner identity accepted")
	}
	placement := identity
	placement.ExecutionPlacementV2 = domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: "user-1", WorkerID: "worker-1", CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("8", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("9", 64))}
	if err := registry.Preflight(context.Background(), placement); err == nil {
		t.Fatal("cross-placement identity accepted")
	}
	driver.preflightErr = errors.New("private backend preflight detail")
	if err := registry.Preflight(context.Background(), identity); err == nil || strings.Contains(err.Error(), "private backend") {
		t.Fatalf("backend preflight error not sanitized: %v", err)
	}
	if driver.preflightCalls != 1 {
		t.Fatalf("backend preflight calls=%d, want 1", driver.preflightCalls)
	}
}

func TestRegistrySelectsExactBackendAmongMultipleRegistrations(t *testing.T) {
	t.Parallel()
	now := time.Unix(100, 0).UTC()
	identity := invocationIdentity(t, now.Add(time.Minute))
	for _, reverse := range []bool{false, true} {
		reverse := reverse
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			t.Parallel()
			codex := &recordingDriver{}
			fixture := &recordingDriver{}
			fixtureRegistration := sessionlessharness.Registration{Descriptor: sessionlessharness.DeterministicFixtureDescriptorV1(), Enabled: true, ValidateBinding: sessionlessharness.ValidateDeterministicFixtureBindingV1, Driver: fixture}
			codexRegistration := sessionlessharness.Registration{Descriptor: identity.HarnessBinding.Backend, Enabled: true, ValidateBinding: func(domain.HarnessBindingV1) sessionlessharness.FailureCode { return "" }, Driver: codex}
			registrations := []sessionlessharness.Registration{fixtureRegistration, codexRegistration}
			if reverse {
				registrations[0], registrations[1] = registrations[1], registrations[0]
			}
			registry, err := sessionlessharness.NewRegistry(func() time.Time { return now }, registrations...)
			if err != nil {
				t.Fatal(err)
			}
			if err := registry.Cancel(context.Background(), identity); err != nil {
				t.Fatal(err)
			}
			if codex.cancelCalls != 1 || fixture.cancelCalls != 0 {
				t.Fatalf("exact/fallback calls=%d/%d", codex.cancelCalls, fixture.cancelCalls)
			}
		})
	}
}

func TestRegistrySanitizesBackendErrorsAndRetainsValidFailureEvidence(t *testing.T) {
	t.Parallel()
	registry, driver, request := registryFixture(t)
	driver.executeErr = errors.New("private provider body token=secret")
	driver.executeResult = ports.ExecutionResult{
		Summary:    "private provider response",
		Outputs:    []ports.ExecutionOutput{{Name: "private-output", MediaType: "text/plain", RelativePath: "secret/path.txt"}},
		ToolEvents: []ports.ExecutionToolEvent{{Kind: domain.SessionEventToolResult, CallID: "call-private", ToolName: "private-tool", Payload: []byte("raw provider frame secret")}},
	}
	result, err := registry.Execute(context.Background(), request, noopSink{})
	if err == nil || strings.Contains(err.Error(), "private provider") || result.ProviderEvidence == nil || result.Summary != "" || len(result.Outputs) != 0 || len(result.ToolEvents) != 0 {
		t.Fatalf("execute error/evidence not bounded: result=%+v err=%v", result, err)
	}
	driver.executeErr = &domain.ClassifiedError{
		Kind: domain.ErrorRetryable, Code: "private_retry_code",
		Operation: "private_operation", Cause: errors.New("private retry detail"),
	}
	_, err = registry.Execute(context.Background(), request, noopSink{})
	var classified *domain.ClassifiedError
	if !errors.As(err, &classified) || !classified.Retryable() ||
		classified.Code != string(sessionlessharness.FailureHarnessBackendFailed) ||
		strings.Contains(err.Error(), "private") {
		t.Fatalf("retry class was not preserved and sanitized: %v", err)
	}
	driver.cancelErr = errors.New("private cancel response")
	identity := identityForRequest(request)
	if err := registry.Cancel(context.Background(), identity); err == nil || strings.Contains(err.Error(), "private cancel") {
		t.Fatalf("cancel error leaked: %v", err)
	}
}

var _ ports.HarnessDriver = (*recordingDriver)(nil)

func identityForRequest(request ports.ExecutionRequest) ports.ExecutionIdentity {
	return ports.ExecutionIdentity{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, RunID: request.RunID, AttemptID: request.AttemptID,
		ExecutionPlacementV2: request.ExecutionPlacementV2, HarnessBinding: request.HarnessBinding.Clone(),
		SubstrateBinding: cloneSubstrateBindingForTestPointer(request.SubstrateBinding), AdmissionCostCeiling: cloneAdmissionCostCeilingForTestPointer(request.AdmissionCostCeiling),
	}
}

func cloneSubstrateBindingForTest(value domain.SubstrateBindingV1) *domain.SubstrateBindingV1 {
	clone := value
	return &clone
}

func cloneSubstrateBindingForTestPointer(value *domain.SubstrateBindingV1) *domain.SubstrateBindingV1 {
	if value == nil {
		return nil
	}
	return cloneSubstrateBindingForTest(*value)
}

func cloneAdmissionCostCeilingForTest(value domain.AdmissionCostCeilingV1) *domain.AdmissionCostCeilingV1 {
	clone := value.Clone()
	return &clone
}

func cloneAdmissionCostCeilingForTestPointer(value *domain.AdmissionCostCeilingV1) *domain.AdmissionCostCeilingV1 {
	if value == nil {
		return nil
	}
	return cloneAdmissionCostCeilingForTest(*value)
}
