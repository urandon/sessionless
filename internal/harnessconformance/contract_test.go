package harnessconformance

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func deterministicFixture(t *testing.T, operation OperationV1) FixtureV1 {
	t.Helper()
	placement := domain.ManagedExecutionPlacementV1()
	binding, err := sessionlessharness.NewDeterministicFixtureBindingV1(
		"tenant-conformance", "user-conformance", "run-conformance", "attempt-conformance", "subscription-conformance",
		placement, time.Unix(10, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return FixtureV1{Version: VersionV1, FixtureID: "deterministic-execute", Placement: placement, Binding: binding, Operation: operation, Expected: ExpectedV1{RegistryContract: RegistryContractPassV1, BackendProtocol: BackendProtocolSkippedV1}}
}

type providerFixtureProfile struct {
	fixtureID         string
	backendKind       domain.HarnessBackendKindV1
	artifactKind      domain.HarnessArtifactKindV1
	nativeProtocol    string
	deliveryKind      domain.ProviderCredentialDeliveryKindV1
	resourceKind      domain.ProviderResourceKindV1
	resourceID        string
	modelVendorID     string
	modelID           string
	transportKind     domain.ProviderTransportKindV1
	transportProvider string
	upstreamProvider  string
	endpointID        string
	billingKind       domain.ProviderBillingKindV1
}

func openRouterFixture(t *testing.T) FixtureV1 {
	return providerFixture(t, providerFixtureProfile{
		fixtureID: "openrouter-ox-alpha-public", backendKind: domain.HarnessBackendDirectOpenRouterV1,
		artifactKind: domain.HarnessArtifactEmbeddedProfileV1, nativeProtocol: "openai-compatible.v1",
		deliveryKind: domain.ProviderCredentialDeliveryDirectV1, resourceKind: domain.ProviderResourceRouterAccountV1,
		resourceID: "openrouter-account-a", modelVendorID: "stealth", modelID: "ox-alpha",
		transportKind: domain.ProviderTransportRouterAPIV1, transportProvider: "openrouter",
		upstreamProvider: "stealth", endpointID: "stealth", billingKind: domain.ProviderBillingRouterAccountV1,
	})
}

func providerFixture(t *testing.T, profile providerFixtureProfile) FixtureV1 {
	t.Helper()
	observed := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	expires := observed.Add(time.Hour)
	placement := domain.ManagedExecutionPlacementV1()
	placementDigest, err := domain.ExecutionPlacementDigestV1(placement)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := domain.HarnessBackendDescriptorV1{
		HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: profile.backendKind,
		ArtifactKind: profile.artifactKind, ArtifactDigest: strings.Repeat("1", 64),
		NativeProtocolVersion: profile.nativeProtocol, BackendProfileDigest: strings.Repeat("2", 64),
		ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: profile.deliveryKind,
	}
	resource := domain.ProviderResourceBindingV1{Kind: profile.resourceKind, ResourceID: profile.resourceID, OwnerUserID: "user-conformance", Revision: 3, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 7}
	scope := domain.ProviderEvidenceScopeV1{TenantID: "tenant-conformance", OwnerUserID: "user-conformance", Resource: resource, Backend: descriptor}
	catalog := domain.ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "openrouter-2026-08-26", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: profile.modelVendorID, ModelID: profile.modelID, CatalogDigest: strings.Repeat("3", 64)}}, ObservedAt: observed, ExpiresAt: expires}
	route := domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceSupportedV1, PolicyID: "public-openrouter", Revision: 1, FallbackPolicy: domain.ProviderFallbackDenyV1, Routes: []domain.ProviderRouteV1{{BackendKind: profile.backendKind, ModelVendorID: profile.modelVendorID, TransportKind: profile.transportKind, TransportProvider: profile.transportProvider, UpstreamProviderID: profile.upstreamProvider, EndpointID: profile.endpointID, BillingKind: profile.billingKind, BillingAuthority: resource.ResourceID, ModelID: profile.modelID}}, ObservedAt: observed, ExpiresAt: expires}
	capability := domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: profile.modelVendorID, ModelID: profile.modelID, MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, ToolCalling: domain.ProviderEvidenceUnknownV1, StructuredOutput: domain.ProviderEvidenceUnknownV1, ObservedAt: observed, ExpiresAt: expires}
	privacy := domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: profile.modelVendorID, ModelID: profile.modelID, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataExternallyShareableV1, domain.ProviderDataPublicV1}, TrainingUse: domain.ProviderEvidenceUnknownV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, PolicyRevision: "reviewed-public-data-only", ObservedAt: observed, ExpiresAt: expires}
	price := domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceSupportedV1, ModelVendorID: profile.modelVendorID, ModelID: profile.modelID, Currency: "USD", ObservedAt: observed, ExpiresAt: expires}
	catalogDigest, _ := catalog.Digest()
	routeDigest, _ := route.Digest()
	capabilityDigest, _ := capability.Digest()
	privacyDigest, _ := privacy.Digest()
	priceDigest, _ := price.Digest()
	policy := domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: "public-data-only", Revision: 1, DecisionOwner: "sessionless-policy", EvidenceSource: "reviewed-openrouter-terms", Verdict: domain.ProviderPolicyGoV1, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataExternallyShareableV1, domain.ProviderDataPublicV1}, CapabilityEvidenceDigest: string(capabilityDigest), PrivacyEvidenceDigest: string(privacyDigest), PriceObservationDigest: string(priceDigest), RoutePolicyDigest: string(routeDigest), ObservedAt: observed, ExpiresAt: expires}
	policyDigest, _ := policy.Digest()
	binding := domain.HarnessBindingV1{Version: 1, TenantID: "tenant-conformance", OwnerUserID: "user-conformance", RunID: "run-conformance", AttemptID: "attempt-conformance", Backend: descriptor, Resource: resource, ModelVendorID: profile.modelVendorID, ModelID: profile.modelID, InputDataClass: domain.ProviderDataExternallyShareableV1, ProviderCatalogDigest: string(catalogDigest), ProviderRouteDigest: string(routeDigest), PrivacyPolicyDigest: string(privacyDigest), CapabilityEvidenceDigest: string(capabilityDigest), EffectivePolicyDigest: string(policyDigest), ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &expires}
	bundle := EvidenceBundleV1{Catalog: catalog, Route: route, Capability: capability, Privacy: privacy, Price: price, Policy: policy}
	fixture := FixtureV1{Version: 1, FixtureID: profile.fixtureID, Placement: placement, Binding: binding, EvidenceBundle: &bundle, Operation: OperationExecuteV1, Expected: ExpectedV1{RegistryContract: RegistryContractPassV1, BackendProtocol: BackendProtocolSkippedV1}}
	if err := fixture.Validate(); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestRunnerUsesProductionRegistryWithoutClaimingNativeProtocolSupport(t *testing.T) {
	for _, fixture := range []FixtureV1{deterministicFixture(t, OperationExecuteV1), openRouterFixture(t)} {
		t.Run(fixture.FixtureID, func(t *testing.T) {
			recorder := &SideEffectRecorder{}
			registration, driver, err := RegistrationForFixture(fixture, recorder)
			if err != nil {
				t.Fatal(err)
			}
			registry, err := sessionlessharness.NewRegistry(func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration)
			if err != nil {
				t.Fatal(err)
			}
			result, err := (Runner{Registry: registry, SideEffects: recorder, BackendProtocol: driver}).Run(context.Background(), fixture)
			if err != nil {
				t.Fatal(err)
			}
			if result.RegistryContract != RegistryContractPassV1 || result.BackendProtocol != BackendProtocolSkippedV1 || result.ProviderExecutionEvidenceDigest == "" {
				t.Fatalf("unexpected result: %+v", result)
			}
			if result.SideEffects.ValidatorCalls != 2 || result.SideEffects.DriverPreflights != 1 || result.SideEffects.DriverExecutes != 1 || result.SideEffects.CredentialReads != 0 || result.SideEffects.ProcessStarts != 0 || result.SideEffects.NetworkStarts != 0 || result.SideEffects.Retries != 0 {
				t.Fatalf("unexpected bounded counters: %+v", result.SideEffects)
			}
			for _, check := range result.Checks {
				if !check.Passed {
					t.Fatalf("failed conformance check: %+v", check)
				}
			}
			if err := result.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFixtureDriverCannotClaimNativeProtocolSupport(t *testing.T) {
	fixture := openRouterFixture(t)
	fixture.FixtureID = "openrouter-fake-native-claim"
	fixture.Expected.BackendProtocol = BackendProtocolSupportedV1
	recorder := &SideEffectRecorder{}
	registration, driver, err := RegistrationForFixture(fixture, recorder)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := (Runner{Registry: registry, SideEffects: recorder, BackendProtocol: driver}).Run(context.Background(), fixture); err == nil {
		t.Fatalf("fixture driver falsely sealed native support: %+v", result)
	}
}

func TestRunnerRejectsObservedCredentialMaterialization(t *testing.T) {
	fixture := deterministicFixture(t, OperationExecuteV1)
	recorder := &SideEffectRecorder{}
	registration, driver, err := RegistrationForFixture(fixture, recorder)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(time.Now, registration)
	if err != nil {
		t.Fatal(err)
	}
	recorder.RecordCredentialMaterialization()
	if result, err := (Runner{Registry: registry, SideEffects: recorder, BackendProtocol: driver}).Run(context.Background(), fixture); err == nil {
		t.Fatalf("side-effecting conformance result was sealed: %+v", result)
	}
}

func TestOpenRouterPublicAndExternallyShareablePass(t *testing.T) {
	for _, dataClass := range []domain.ProviderDataClassV1{domain.ProviderDataPublicV1, domain.ProviderDataExternallyShareableV1} {
		t.Run(string(dataClass), func(t *testing.T) {
			fixture := openRouterFixture(t)
			fixture.FixtureID = "openrouter-" + string(dataClass)
			fixture.Binding.InputDataClass = dataClass
			recorder := &SideEffectRecorder{}
			registration, driver, err := RegistrationForFixture(fixture, recorder)
			if err != nil {
				t.Fatal(err)
			}
			registry, err := sessionlessharness.NewRegistry(func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration)
			if err != nil {
				t.Fatal(err)
			}
			result, err := (Runner{Registry: registry, SideEffects: recorder, BackendProtocol: driver}).Run(context.Background(), fixture)
			if err != nil || result.RegistryContract != RegistryContractPassV1 {
				t.Fatalf("public-safe OpenRouter fixture failed: result=%+v err=%v", result, err)
			}
		})
	}
}

func reviewedProviderFixtures(t *testing.T) []FixtureV1 {
	t.Helper()
	return []FixtureV1{
		providerFixture(t, providerFixtureProfile{fixtureID: "codex-subscription-file", backendKind: domain.HarnessBackendCodexExecV1, artifactKind: domain.HarnessArtifactExecutableV1, nativeProtocol: "codex-jsonl.v1", deliveryKind: domain.ProviderCredentialDeliveryFileV1, resourceKind: domain.ProviderResourceSubscriptionV1, resourceID: "codex-subscription-a", modelVendorID: "openai", modelID: "codex-model", transportKind: domain.ProviderTransportLocalCLIV1, transportProvider: "codex", upstreamProvider: "openai", endpointID: "codex-cli", billingKind: domain.ProviderBillingSubscriptionV1}),
		providerFixture(t, providerFixtureProfile{fixtureID: "codex-openrouter-environment", backendKind: domain.HarnessBackendCodexExecV1, artifactKind: domain.HarnessArtifactExecutableV1, nativeProtocol: "codex-jsonl.v1", deliveryKind: domain.ProviderCredentialDeliveryEnvironmentV1, resourceKind: domain.ProviderResourceRouterAccountV1, resourceID: "openrouter-account-a", modelVendorID: "stealth", modelID: "ox-alpha", transportKind: domain.ProviderTransportLocalCLIV1, transportProvider: "codex", upstreamProvider: "stealth", endpointID: "openrouter-responses", billingKind: domain.ProviderBillingRouterAccountV1}),
		providerFixture(t, providerFixtureProfile{fixtureID: "opencode-openrouter-environment", backendKind: domain.HarnessBackendOpenCodeV1, artifactKind: domain.HarnessArtifactExecutableV1, nativeProtocol: "opencode-jsonl.v1", deliveryKind: domain.ProviderCredentialDeliveryEnvironmentV1, resourceKind: domain.ProviderResourceRouterAccountV1, resourceID: "openrouter-account-a", modelVendorID: "stealth", modelID: "ox-alpha", transportKind: domain.ProviderTransportLocalCLIV1, transportProvider: "opencode", upstreamProvider: "stealth", endpointID: "openrouter", billingKind: domain.ProviderBillingRouterAccountV1}),
		providerFixture(t, providerFixtureProfile{fixtureID: "pi-openrouter-environment", backendKind: domain.HarnessBackendPiV1, artifactKind: domain.HarnessArtifactExecutableV1, nativeProtocol: "pi-rpc.v1", deliveryKind: domain.ProviderCredentialDeliveryEnvironmentV1, resourceKind: domain.ProviderResourceRouterAccountV1, resourceID: "openrouter-account-a", modelVendorID: "stealth", modelID: "ox-alpha", transportKind: domain.ProviderTransportLocalCLIV1, transportProvider: "pi", upstreamProvider: "stealth", endpointID: "openrouter", billingKind: domain.ProviderBillingRouterAccountV1}),
		openRouterFixture(t),
	}
}

func TestExactMultiRegistrationMatrixHasNoDefaultPriorityOrFallback(t *testing.T) {
	fixtures := reviewedProviderFixtures(t)
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "forward", true: "reverse"}[reverse], func(t *testing.T) {
			registrations := make([]sessionlessharness.Registration, 0, len(fixtures)+1)
			recorders := make(map[string]*SideEffectRecorder, len(fixtures))
			for _, fixture := range fixtures {
				recorder := &SideEffectRecorder{}
				registration, _, err := RegistrationForFixture(fixture, recorder)
				if err != nil {
					t.Fatal(err)
				}
				registrations = append(registrations, registration)
				recorders[fixture.FixtureID] = recorder
			}
			if reverse {
				for left, right := 0, len(registrations)-1; left < right; left, right = left+1, right-1 {
					registrations[left], registrations[right] = registrations[right], registrations[left]
				}
			}
			registry, err := sessionlessharness.NewRegistry(func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registrations...)
			if err != nil {
				t.Fatal(err)
			}
			for _, fixture := range fixtures {
				result, err := (Runner{Registry: registry, SideEffects: recorders[fixture.FixtureID]}).Run(context.Background(), fixture)
				if err != nil || result.RegistryContract != RegistryContractPassV1 {
					t.Fatalf("%s exact route failed: result=%+v err=%v", fixture.FixtureID, result, err)
				}
			}
		})
	}
}

func TestExpiredExecutionPrivateDataAndCancellationGates(t *testing.T) {
	public := openRouterFixture(t)

	t.Run("expired execution before side effects", func(t *testing.T) {
		recorder := &SideEffectRecorder{}
		registration, _, err := RegistrationForFixture(public, recorder)
		if err != nil {
			t.Fatal(err)
		}
		registry, err := sessionlessharness.NewRegistry(func() time.Time { return public.Binding.EvidenceExpiresAt.UTC() }, registration)
		if err != nil {
			t.Fatal(err)
		}
		expired := public.Clone()
		expired.FixtureID = "openrouter-expired"
		expired.Expected = ExpectedV1{RegistryContract: RegistryContractNoGoV1, BackendProtocol: BackendProtocolSkippedV1, FailureCode: sessionlessharness.FailureProviderEvidenceExpired}
		result, err := (Runner{Registry: registry, SideEffects: recorder}).Run(context.Background(), expired)
		if err != nil || result.FailureCode != sessionlessharness.FailureProviderEvidenceExpired || result.SideEffects != (SideEffectsV1{}) {
			t.Fatalf("expired execution result=%+v err=%v", result, err)
		}
	})

	t.Run("cancellation remains routable after expiry", func(t *testing.T) {
		cancel := public.Clone()
		cancel.FixtureID = "openrouter-cancel-after-expiry"
		cancel.Operation = OperationCancelV1
		recorder := &SideEffectRecorder{}
		registration, driver, err := RegistrationForFixture(cancel, recorder)
		if err != nil {
			t.Fatal(err)
		}
		registry, err := sessionlessharness.NewRegistry(func() time.Time { return cancel.Binding.EvidenceExpiresAt.UTC() }, registration)
		if err != nil {
			t.Fatal(err)
		}
		result, err := (Runner{Registry: registry, SideEffects: recorder, BackendProtocol: driver}).Run(context.Background(), cancel)
		if err != nil || result.RegistryContract != RegistryContractPassV1 || result.SideEffects.DriverCancels != 1 || result.SideEffects.CredentialReads != 0 || result.SideEffects.ProcessStarts != 0 || result.SideEffects.NetworkStarts != 0 {
			t.Fatalf("cancel result=%+v err=%v", result, err)
		}
	})

	t.Run("private direct OpenRouter is policy denied", func(t *testing.T) {
		private := public.Clone()
		private.FixtureID = "openrouter-private-no-go"
		private.Binding.InputDataClass = domain.ProviderDataPrivateV1
		private.EvidenceBundle.Policy.Verdict = domain.ProviderPolicyNoGoV1
		private.EvidenceBundle.Policy.AllowedDataClasses = nil
		policyDigest, err := private.EvidenceBundle.Policy.Digest()
		if err != nil {
			t.Fatal(err)
		}
		private.Binding.EffectivePolicyDigest = string(policyDigest)
		private.Expected = ExpectedV1{RegistryContract: RegistryContractNoGoV1, BackendProtocol: BackendProtocolSkippedV1, FailureCode: sessionlessharness.FailureEffectivePolicyMismatch}
		recorder := &SideEffectRecorder{}
		registration, driver, err := RegistrationForFixture(private, recorder)
		if err != nil {
			t.Fatal(err)
		}
		registry, err := sessionlessharness.NewRegistry(func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration)
		if err != nil {
			t.Fatal(err)
		}
		result, err := (Runner{Registry: registry, SideEffects: recorder, BackendProtocol: driver}).Run(context.Background(), private)
		if err != nil || result.FailureCode != sessionlessharness.FailureEffectivePolicyMismatch || result.SideEffects.DriverPreflights != 0 || result.SideEffects.DriverExecutes != 0 || result.SideEffects.CredentialReads != 0 || result.SideEffects.ProcessStarts != 0 || result.SideEffects.NetworkStarts != 0 {
			t.Fatalf("private-data result=%+v err=%v", result, err)
		}
	})
}

func TestRunnerRejectsNearMatchWithoutDriverSideEffects(t *testing.T) {
	base := deterministicFixture(t, OperationPreflightV1)
	recorder := &SideEffectRecorder{}
	registration, _, err := RegistrationForFixture(base, recorder)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(time.Now, registration)
	if err != nil {
		t.Fatal(err)
	}
	mutated := base.Clone()
	mutated.FixtureID = "deterministic-near-match"
	mutated.Binding.Backend.BackendProfileDigest = strings.Repeat("f", 64)
	mutated.Expected = ExpectedV1{RegistryContract: RegistryContractNoGoV1, BackendProtocol: BackendProtocolSkippedV1, FailureCode: sessionlessharness.FailureHarnessBackendUnsupported}
	result, err := (Runner{Registry: registry, SideEffects: recorder}).Run(context.Background(), mutated)
	if err != nil {
		t.Fatal(err)
	}
	if result.RegistryContract != RegistryContractNoGoV1 || result.FailureCode != sessionlessharness.FailureHarnessBackendUnsupported {
		t.Fatalf("unexpected no-go result: %+v", result)
	}
	if result.SideEffects != (SideEffectsV1{}) {
		t.Fatalf("rejected binding caused side effects: %+v", result.SideEffects)
	}
}

func TestAuthorityMutationMatrixFailsClosedBeforeDriver(t *testing.T) {
	base := openRouterFixture(t)
	recorder := &SideEffectRecorder{}
	registration, _, err := RegistrationForFixture(base, recorder)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration)
	if err != nil {
		t.Fatal(err)
	}
	type mutation struct {
		want  sessionlessharness.FailureCode
		apply func(*ports.ExecutionIdentity)
	}
	mutations := map[string]mutation{
		"owner": {want: sessionlessharness.FailureHarnessBackendMismatch, apply: func(value *ports.ExecutionIdentity) {
			value.OwnerUserID, value.HarnessBinding.OwnerUserID, value.HarnessBinding.Resource.OwnerUserID = "user-other", "user-other", "user-other"
		}},
		"run": {want: sessionlessharness.FailureHarnessBackendMismatch, apply: func(value *ports.ExecutionIdentity) {
			value.RunID, value.HarnessBinding.RunID = "run-other", "run-other"
		}},
		"attempt": {want: sessionlessharness.FailureHarnessBackendMismatch, apply: func(value *ports.ExecutionIdentity) {
			value.AttemptID, value.HarnessBinding.AttemptID = "attempt-other", "attempt-other"
		}},
		"resource kind": {want: sessionlessharness.FailureProviderResourceMismatch, apply: func(value *ports.ExecutionIdentity) {
			value.HarnessBinding.Resource.Kind = domain.ProviderResourceAPIAccountV1
		}},
		"resource revision":     {want: sessionlessharness.FailureProviderRevisionMismatch, apply: func(value *ports.ExecutionIdentity) { value.HarnessBinding.Resource.Revision++ }},
		"credential generation": {want: sessionlessharness.FailureCredentialGeneration, apply: func(value *ports.ExecutionIdentity) { value.HarnessBinding.Resource.CredentialGeneration++ }},
		"model vendor":          {want: sessionlessharness.FailureProviderCatalogExpired, apply: func(value *ports.ExecutionIdentity) { value.HarnessBinding.ModelVendorID = "other-vendor" }},
		"model":                 {want: sessionlessharness.FailureProviderCatalogExpired, apply: func(value *ports.ExecutionIdentity) { value.HarnessBinding.ModelID = "other-model" }},
		"catalog": {want: sessionlessharness.FailureProviderCatalogExpired, apply: func(value *ports.ExecutionIdentity) {
			value.HarnessBinding.ProviderCatalogDigest = strings.Repeat("a", 64)
		}},
		"route": {want: sessionlessharness.FailureProviderRouteMismatch, apply: func(value *ports.ExecutionIdentity) {
			value.HarnessBinding.ProviderRouteDigest = strings.Repeat("b", 64)
		}},
		"privacy": {want: sessionlessharness.FailurePrivacyPolicyMismatch, apply: func(value *ports.ExecutionIdentity) {
			value.HarnessBinding.PrivacyPolicyDigest = strings.Repeat("c", 64)
		}},
		"capability": {want: sessionlessharness.FailureCapabilityMismatch, apply: func(value *ports.ExecutionIdentity) {
			value.HarnessBinding.CapabilityEvidenceDigest = strings.Repeat("d", 64)
		}},
		"policy": {want: sessionlessharness.FailureEffectivePolicyMismatch, apply: func(value *ports.ExecutionIdentity) {
			value.HarnessBinding.EffectivePolicyDigest = strings.Repeat("e", 64)
		}},
		"private data": {want: sessionlessharness.FailureEffectivePolicyMismatch, apply: func(value *ports.ExecutionIdentity) {
			value.HarnessBinding.InputDataClass = domain.ProviderDataPrivateV1
		}},
		"artifact": {want: sessionlessharness.FailureHarnessBackendUnsupported, apply: func(value *ports.ExecutionIdentity) {
			value.HarnessBinding.Backend.ArtifactDigest = strings.Repeat("f", 64)
		}},
		"placement": {want: sessionlessharness.FailurePlacementMismatch, apply: func(value *ports.ExecutionIdentity) {
			value.ExecutionPlacement = domain.ExecutionPlacementV1{Version: domain.ExecutionPlacementVersionV1, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: value.OwnerUserID, WorkerID: "worker-other", CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("8", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("9", 64))}
			digest, digestErr := domain.ExecutionPlacementDigestV1(value.ExecutionPlacement)
			if digestErr != nil {
				t.Fatal(digestErr)
			}
			value.HarnessBinding.ExecutionPlacementDigest = string(digest)
		}},
	}
	for name, test := range mutations {
		t.Run(name, func(t *testing.T) {
			before := recorder.Snapshot()
			identity := fixtureIdentity(base.Binding, base.Placement)
			test.apply(&identity)
			err := registry.Preflight(context.Background(), identity)
			var classified *domain.ClassifiedError
			if !errors.As(err, &classified) || classified.Code != string(test.want) {
				t.Fatalf("error=%v, want stable code %q", err, test.want)
			}
			after := recorder.Snapshot()
			if after.DriverPreflights != before.DriverPreflights || after.DriverExecutes != before.DriverExecutes || after.DriverCancels != before.DriverCancels || after.CredentialReads != 0 || after.ProcessStarts != 0 || after.NetworkStarts != 0 {
				t.Fatalf("mutation reached side effect: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestEvidenceBundleBaitSwitchFailsAndUnknownPriceDiffersFromKnownFree(t *testing.T) {
	base := openRouterFixture(t)
	mutations := map[string]func(*FixtureV1){
		"catalog": func(value *FixtureV1) { value.EvidenceBundle.Catalog.Models[0].CatalogDigest = strings.Repeat("a", 64) },
		"route":   func(value *FixtureV1) { value.EvidenceBundle.Route.Routes[0].EndpointID = "other-endpoint" },
		"capability": func(value *FixtureV1) {
			value.EvidenceBundle.Capability.ToolCalling = domain.ProviderEvidenceSupportedV1
		},
		"privacy": func(value *FixtureV1) { value.EvidenceBundle.Privacy.PolicyRevision = "other-policy" },
		"price": func(value *FixtureV1) {
			value.EvidenceBundle.Price.State = domain.ProviderEvidenceUnknownV1
			value.EvidenceBundle.Price.Currency = ""
		},
		"policy": func(value *FixtureV1) { value.EvidenceBundle.Policy.Verdict = domain.ProviderPolicyConditionalV1 },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			mutate(&candidate)
			if candidate.Validate() == nil {
				t.Fatal("evidence bait-switch accepted without rebinding")
			}
		})
	}

	vendorMix := base.Clone()
	vendorMix.FixtureID = "openrouter-vendor-mix"
	vendorMix.EvidenceBundle.Capability.ModelVendorID = "other-vendor"
	capabilityDigest, err := vendorMix.EvidenceBundle.Capability.Digest()
	if err != nil {
		t.Fatal(err)
	}
	vendorMix.Binding.CapabilityEvidenceDigest = string(capabilityDigest)
	vendorMix.EvidenceBundle.Policy.CapabilityEvidenceDigest = string(capabilityDigest)
	policyDigest, err := vendorMix.EvidenceBundle.Policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	vendorMix.Binding.EffectivePolicyDigest = string(policyDigest)
	if err := vendorMix.Validate(); err == nil {
		t.Fatal("re-digested cross-vendor evidence bundle accepted")
	}
	zero := uint64(0)
	wrongTransport, err := (domain.ProviderExecutionEvidenceV1{
		AcceptanceClass: domain.ProviderAcceptanceAcceptedV1, FinishClass: domain.ProviderFinishCompletedV1,
		RouteState: domain.ProviderEvidenceSupportedV1, ActualModelVendorID: base.Binding.ModelVendorID, ActualModelID: base.Binding.ModelID,
		TransportKind: domain.ProviderTransportDirectAPIV1, TransportProvider: "openrouter", UpstreamProviderID: "stealth", EndpointID: "stealth",
		PolicyVerdict: base.EvidenceBundle.Policy.Verdict, UsageProvenance: domain.ProviderUsageHarnessMeasuredV1,
		InputTokens: &zero, OutputTokens: &zero,
	}).SealForBinding(base.Binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := base.EvidenceBundle.ValidateExecutionEvidence(base.Binding, wrongTransport); err == nil {
		t.Fatal("execution evidence with wrong transport kind matched admitted route")
	}

	knownFreeDigest, err := base.EvidenceBundle.Price.Digest()
	if err != nil {
		t.Fatal(err)
	}
	unknown := base.Clone()
	unknown.FixtureID = "openrouter-price-unknown"
	unknown.EvidenceBundle.Price.State = domain.ProviderEvidenceUnknownV1
	unknown.EvidenceBundle.Price.Currency = ""
	unknownPriceDigest, err := unknown.EvidenceBundle.Price.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if unknownPriceDigest == knownFreeDigest {
		t.Fatal("unknown price collapsed into known zero/free")
	}
	unknown.EvidenceBundle.Policy.PriceObservationDigest = string(unknownPriceDigest)
	policyDigest, err = unknown.EvidenceBundle.Policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	unknown.Binding.EffectivePolicyDigest = string(policyDigest)
	if err := unknown.Validate(); err != nil {
		t.Fatalf("model-scoped unknown price rejected: %v", err)
	}
}

func TestConformanceResultSanitizesBackendFailureAndRetainsEvidence(t *testing.T) {
	fixture := openRouterFixture(t)
	fixture.FixtureID = "openrouter-private-error"
	fixture.Expected = ExpectedV1{RegistryContract: RegistryContractNoGoV1, BackendProtocol: BackendProtocolSkippedV1, FailureCode: sessionlessharness.FailureHarnessBackendFailed}
	recorder := &SideEffectRecorder{}
	registration, driver, err := RegistrationForFixture(fixture, recorder)
	if err != nil {
		t.Fatal(err)
	}
	driver.executeErr = errors.New("provider body secret-marker raw-frame-marker")
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration)
	if err != nil {
		t.Fatal(err)
	}
	result, err := (Runner{Registry: registry, SideEffects: recorder, BackendProtocol: driver}).Run(context.Background(), fixture)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeResultV1(result)
	if err != nil {
		t.Fatal(err)
	}
	if result.ProviderExecutionEvidenceDigest == "" || result.FailureCode != sessionlessharness.FailureHarnessBackendFailed || strings.Contains(string(encoded), "secret-marker") || strings.Contains(string(encoded), "raw-frame-marker") {
		t.Fatalf("unsafe failure result: %s", encoded)
	}
}
