package codexopenrouter

import (
	"context"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func TestNativeCodexOpenRouterPreflightPassesClosedHarnessConformance(t *testing.T) {
	t.Parallel()
	driver := mustDriver(t, validProfile(true), &fakeBoundary{})
	fixture := nativeCodexConformanceFixture(t, driver, false)
	registration, err := registrationV1(driver, true)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(
		func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration,
	)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &harnessconformance.SideEffectRecorder{}
	result, err := (harnessconformance.Runner{
		Registry: registry, SideEffects: recorder, BackendProtocol: driver,
	}).Run(context.Background(), fixture)
	if err != nil {
		t.Fatal(err)
	}
	if result.RegistryContract != harnessconformance.RegistryContractPassV1 ||
		result.BackendProtocol != harnessconformance.BackendProtocolSupportedV1 || result.FailureCode != "" ||
		result.SideEffects != (harnessconformance.SideEffectsV1{}) || result.Validate() != nil {
		t.Fatalf("conformance result = %+v", result)
	}
}

func TestDisabledCodexOpenRouterRegistrationFailsClosedConformance(t *testing.T) {
	t.Parallel()
	driver := mustDriver(t, validProfile(false), &fakeBoundary{})
	fixture := nativeCodexConformanceFixture(t, driver, false)
	fixture.FixtureID = "codex-openrouter-disabled-preflight"
	fixture.Expected = harnessconformance.ExpectedV1{
		RegistryContract: harnessconformance.RegistryContractNoGoV1,
		BackendProtocol:  harnessconformance.BackendProtocolUnsupportedV1,
		FailureCode:      sessionlessharness.FailureHarnessBackendDisabled,
	}
	registration, err := DisabledRegistrationV1(driver)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(
		func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration,
	)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &harnessconformance.SideEffectRecorder{}
	result, err := (harnessconformance.Runner{
		Registry: registry, SideEffects: recorder, BackendProtocol: driver,
	}).Run(context.Background(), fixture)
	if err != nil {
		t.Fatal(err)
	}
	if result.RegistryContract != harnessconformance.RegistryContractNoGoV1 ||
		result.BackendProtocol != harnessconformance.BackendProtocolUnsupportedV1 ||
		result.FailureCode != sessionlessharness.FailureHarnessBackendDisabled ||
		result.SideEffects != (harnessconformance.SideEffectsV1{}) || result.Validate() != nil {
		t.Fatalf("conformance result = %+v", result)
	}
}

func TestNearMatchCodexRegistrationAndAuthorityMutationsFailBeforeEffects(t *testing.T) {
	t.Parallel()
	exactDriver := mustDriver(t, validProfile(true), &fakeBoundary{})
	base := nativeCodexConformanceFixture(t, exactDriver, false)
	nearProfile := validProfile(true)
	nearProfile.ExecutableDigest[0]++
	nearDriver := mustDriver(t, nearProfile, &fakeBoundary{})
	nearRegistration, err := registrationV1(nearDriver, true)
	if err != nil {
		t.Fatal(err)
	}
	nearRegistry, err := sessionlessharness.NewRegistry(
		func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, nearRegistration,
	)
	if err != nil {
		t.Fatal(err)
	}
	nearFixture := base.Clone()
	nearFixture.FixtureID = "codex-openrouter-near-match-registration"
	nearFixture.Expected = harnessconformance.ExpectedV1{
		RegistryContract: harnessconformance.RegistryContractNoGoV1,
		BackendProtocol:  harnessconformance.BackendProtocolSupportedV1,
		FailureCode:      sessionlessharness.FailureHarnessBackendUnsupported,
	}
	recorder := &harnessconformance.SideEffectRecorder{}
	result, err := (harnessconformance.Runner{
		Registry: nearRegistry, SideEffects: recorder, BackendProtocol: nearDriver,
	}).Run(context.Background(), nearFixture)
	if err != nil || result.FailureCode != sessionlessharness.FailureHarnessBackendUnsupported ||
		result.SideEffects != (harnessconformance.SideEffectsV1{}) {
		t.Fatalf("near-match result=%+v err=%v", result, err)
	}

	registration, err := registrationV1(exactDriver, true)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(
		func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration,
	)
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*harnessconformance.FixtureV1){
		"artifact": func(value *harnessconformance.FixtureV1) {
			value.Binding.Backend.ArtifactDigest = strings.Repeat("b", 64)
		},
		"profile": func(value *harnessconformance.FixtureV1) {
			value.Binding.Backend.BackendProfileDigest = strings.Repeat("b", 64)
		},
		"protocol": func(value *harnessconformance.FixtureV1) {
			value.Binding.Backend.NativeProtocolVersion = "codex-jsonl.v2"
		},
		"model": func(value *harnessconformance.FixtureV1) { value.Binding.ModelID = "stealth/near-match" },
		"route": func(value *harnessconformance.FixtureV1) { value.Binding.ProviderRouteDigest = strings.Repeat("b", 64) },
		"policy": func(value *harnessconformance.FixtureV1) {
			value.Binding.EffectivePolicyDigest = strings.Repeat("b", 64)
		},
		"placement": func(value *harnessconformance.FixtureV1) {
			value.Binding.ExecutionPlacementDigest = strings.Repeat("b", 64)
		},
		"credential-generation": func(value *harnessconformance.FixtureV1) { value.Binding.Resource.CredentialGeneration++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			candidate.FixtureID = "codex-openrouter-mutation-" + name
			mutate(&candidate)
			effects := &harnessconformance.SideEffectRecorder{}
			if _, err := (harnessconformance.Runner{
				Registry: registry, SideEffects: effects, BackendProtocol: exactDriver,
			}).Run(context.Background(), candidate); err == nil {
				t.Fatal("mutated fixture reached conformance execution")
			}
			if snapshot := effects.Snapshot(); snapshot != (harnessconformance.SideEffectsV1{}) {
				t.Fatalf("mutation reached side effects: %+v", snapshot)
			}
		})
	}
}

func TestPolicyRequiringKnownRouteRejectsCodexUnknownRouteEvidence(t *testing.T) {
	t.Parallel()
	driver := mustDriver(t, validProfile(true), &fakeBoundary{})
	unknown := nativeCodexConformanceFixture(t, driver, false)
	known := nativeCodexConformanceFixture(t, driver, true)
	parsed := codexexec.ParseProtocolV1([]byte(successfulCodexJSONL("answer")))
	process := successfulProcess([]byte(successfulCodexJSONL("answer")))
	unknownEvidence, err := reduceEvidence(unknown.Binding, parsed, process, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := unknown.EvidenceBundle.ValidateExecutionEvidence(unknown.Binding, unknownEvidence); err != nil {
		t.Fatalf("unknown-route policy rejected explicit unknown evidence: %v", err)
	}
	knownEvidence, err := reduceEvidence(known.Binding, parsed, process, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := known.EvidenceBundle.ValidateExecutionEvidence(known.Binding, knownEvidence); err == nil {
		t.Fatal("known-route policy accepted unavailable Codex route observation")
	}
}

func nativeCodexConformanceFixture(t *testing.T, driver *Driver, knownRoute bool) harnessconformance.FixtureV1 {
	t.Helper()
	observed := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	expires := observed.Add(time.Hour)
	authority, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(
		"tenant-conformance", "user-conformance", "run-conformance", "attempt-conformance", "subscription-conformance", observed,
	)
	if err != nil {
		t.Fatal(err)
	}
	placementDigest, err := domain.ExecutionPlacementDigest(authority.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	resource := domain.ProviderResourceBindingV1{
		Kind: domain.ProviderResourceRouterAccountV1, ResourceID: "openrouter-account-a", OwnerUserID: "user-conformance",
		Revision: 3, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 7,
	}
	scope := domain.ProviderEvidenceScopeV1{
		TenantID: "tenant-conformance", OwnerUserID: "user-conformance", Resource: resource, Backend: driver.DescriptorV1(),
	}
	catalog := domain.ProviderCatalogObservationV1{
		Version: 1, Scope: scope, CatalogRevision: "openrouter-2026-08-26",
		Models:     []domain.ProviderCatalogModelV1{{ModelVendorID: ModelVendorIDV1, ModelID: ModelIDV1, CatalogDigest: strings.Repeat("a", 64)}},
		ObservedAt: observed, ExpiresAt: expires,
	}
	route := domain.ProviderRoutePolicyV1{
		Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1,
		FallbackPolicy: domain.ProviderFallbackDenyV1, ObservedAt: observed, ExpiresAt: expires,
	}
	if knownRoute {
		route.State, route.PolicyID, route.Revision = domain.ProviderEvidenceSupportedV1, "ox-alpha-stealth-only", 1
		route.Routes = []domain.ProviderRouteV1{{
			BackendKind: domain.HarnessBackendCodexOpenRouterV1, ModelVendorID: ModelVendorIDV1, ModelID: ModelIDV1,
			TransportKind: domain.ProviderTransportLocalCLIV1, TransportProvider: "codex",
			UpstreamProviderID: "stealth", EndpointID: "openrouter",
			BillingKind: domain.ProviderBillingRouterAccountV1, BillingAuthority: resource.ResourceID,
		}}
	}
	capability := domain.ProviderCapabilityEvidenceV1{
		Version: 1, Scope: scope, ModelVendorID: ModelVendorIDV1, ModelID: ModelIDV1,
		MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1},
		ToolCalling:      domain.ProviderEvidenceUnsupportedV1, StructuredOutput: domain.ProviderEvidenceUnknownV1,
		ObservedAt: observed, ExpiresAt: expires,
	}
	privacy := domain.ProviderPrivacyEvidenceV1{
		Version: 1, Scope: scope, ModelVendorID: ModelVendorIDV1, ModelID: ModelIDV1,
		AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataExternallyShareableV1},
		TrainingUse:        domain.ProviderEvidenceUnknownV1,
		RetentionHours:     domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1},
		PolicyRevision:     "stealth-eula-restrictive", ObservedAt: observed, ExpiresAt: expires,
	}
	price := domain.ProviderPriceObservationV1{
		Version: 1, Scope: scope, State: domain.ProviderEvidenceSupportedV1,
		ModelVendorID: ModelVendorIDV1, ModelID: ModelIDV1, Currency: "USD", ObservedAt: observed, ExpiresAt: expires,
	}
	catalogDigest, err := catalog.Digest()
	if err != nil {
		t.Fatal(err)
	}
	routeDigest, err := route.Digest()
	if err != nil {
		t.Fatal(err)
	}
	capabilityDigest, err := capability.Digest()
	if err != nil {
		t.Fatal(err)
	}
	privacyDigest, err := privacy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	priceDigest, err := price.Digest()
	if err != nil {
		t.Fatal(err)
	}
	policy := domain.ProviderPolicyEvidenceV1{
		Version: 1, Scope: scope, PolicyID: "externally-shareable-canary", Revision: 1,
		DecisionOwner: "sessionless-policy", EvidenceSource: "reviewed-openrouter-terms",
		Verdict:                  domain.ProviderPolicyConditionalV1,
		AllowedDataClasses:       []domain.ProviderDataClassV1{domain.ProviderDataExternallyShareableV1},
		CapabilityEvidenceDigest: string(capabilityDigest), PrivacyEvidenceDigest: string(privacyDigest),
		PriceObservationDigest: string(priceDigest), RoutePolicyDigest: string(routeDigest),
		ObservedAt: observed, ExpiresAt: expires,
	}
	policyDigest, err := policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.HarnessBindingV1{
		Version: 1, TenantID: "tenant-conformance", OwnerUserID: "user-conformance", RunID: "run-conformance", AttemptID: "attempt-conformance",
		Backend: driver.DescriptorV1(), Resource: resource, ModelVendorID: ModelVendorIDV1, ModelID: ModelIDV1,
		InputDataClass: domain.ProviderDataExternallyShareableV1, ProviderCatalogDigest: string(catalogDigest),
		ProviderRouteDigest: string(routeDigest), PrivacyPolicyDigest: string(privacyDigest),
		CapabilityEvidenceDigest: string(capabilityDigest), EffectivePolicyDigest: string(policyDigest),
		ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &expires,
	}
	bundle := harnessconformance.EvidenceBundleV1{
		Catalog: catalog, Route: route, Capability: capability, Privacy: privacy, Price: price, Policy: policy,
	}
	fixture := harnessconformance.FixtureV1{
		Version: harnessconformance.VersionV1, FixtureID: "codex-openrouter-native-preflight",
		Placement: authority.ExecutionPlacementV2, Binding: binding,
		SubstrateBinding: authority.SubstrateBinding, AdmissionCostCeiling: authority.AdmissionCostCeiling,
		EvidenceBundle: &bundle, Operation: harnessconformance.OperationPreflightV1,
		Expected: harnessconformance.ExpectedV1{
			RegistryContract: harnessconformance.RegistryContractPassV1,
			BackendProtocol:  harnessconformance.BackendProtocolSupportedV1,
		},
	}
	if err := fixture.Validate(); err != nil {
		t.Fatal(err)
	}
	return fixture
}
