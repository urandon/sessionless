package directopenrouter

import (
	"context"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func TestNativeDirectOpenRouterPreflightPassesClosedHarnessConformance(t *testing.T) {
	t.Parallel()
	driver := mustDriver(t, validProfile(true), &fakeBoundary{})
	fixture := nativeConformanceFixture(t, driver)
	registration, err := registrationV1(driver, true)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &harnessconformance.SideEffectRecorder{}
	result, err := (harnessconformance.Runner{Registry: registry, SideEffects: recorder, BackendProtocol: driver}).Run(context.Background(), fixture)
	if err != nil {
		t.Fatal(err)
	}
	if result.RegistryContract != harnessconformance.RegistryContractPassV1 ||
		result.BackendProtocol != harnessconformance.BackendProtocolSupportedV1 || result.FailureCode != "" ||
		result.SideEffects != (harnessconformance.SideEffectsV1{}) || result.Validate() != nil {
		t.Fatalf("conformance result = %+v", result)
	}
}

func TestDisabledDirectOpenRouterRegistrationFailsClosedConformance(t *testing.T) {
	t.Parallel()
	driver := mustDriver(t, validProfile(false), &fakeBoundary{})
	fixture := nativeConformanceFixture(t, driver)
	fixture.FixtureID = "direct-openrouter-disabled-preflight"
	fixture.Expected = harnessconformance.ExpectedV1{
		RegistryContract: harnessconformance.RegistryContractNoGoV1,
		BackendProtocol:  harnessconformance.BackendProtocolUnsupportedV1,
		FailureCode:      sessionlessharness.FailureHarnessBackendDisabled,
	}
	registration, err := DisabledRegistrationV1(driver)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &harnessconformance.SideEffectRecorder{}
	result, err := (harnessconformance.Runner{Registry: registry, SideEffects: recorder, BackendProtocol: driver}).Run(context.Background(), fixture)
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

func TestAuthorityAndEvidenceMutationsFailBeforeCredentialOrNetworkEffects(t *testing.T) {
	t.Parallel()
	driver := mustDriver(t, validProfile(true), &fakeBoundary{})
	base := nativeConformanceFixture(t, driver)
	registration, err := registrationV1(driver, true)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return time.Date(2026, 8, 26, 12, 1, 0, 0, time.UTC) }, registration)
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*harnessconformance.FixtureV1){
		"binding": func(value *harnessconformance.FixtureV1) {
			value.Binding.Backend.BackendProfileDigest = strings.Repeat("b", 64)
		},
		"catalog":               func(value *harnessconformance.FixtureV1) { value.EvidenceBundle.Catalog.CatalogRevision = "mutated" },
		"route":                 func(value *harnessconformance.FixtureV1) { value.EvidenceBundle.Route.Revision++ },
		"privacy":               func(value *harnessconformance.FixtureV1) { value.EvidenceBundle.Privacy.PolicyRevision = "mutated" },
		"price":                 func(value *harnessconformance.FixtureV1) { value.EvidenceBundle.Price.Currency = "EUR" },
		"policy":                func(value *harnessconformance.FixtureV1) { value.EvidenceBundle.Policy.Revision++ },
		"credential generation": func(value *harnessconformance.FixtureV1) { value.Binding.Resource.CredentialGeneration++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			candidate.FixtureID = "direct-openrouter-mutated-" + strings.ReplaceAll(name, " ", "-")
			mutate(&candidate)
			effects := &harnessconformance.SideEffectRecorder{}
			if _, err := (harnessconformance.Runner{Registry: registry, SideEffects: effects, BackendProtocol: driver}).Run(context.Background(), candidate); err == nil {
				t.Fatal("mutated authority reached conformance success")
			}
			if snapshot := effects.Snapshot(); snapshot != (harnessconformance.SideEffectsV1{}) {
				t.Fatalf("mutation reached side effects: %+v", snapshot)
			}
		})
	}
}

func nativeConformanceFixture(t *testing.T, driver *Driver) harnessconformance.FixtureV1 {
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
	scope := domain.ProviderEvidenceScopeV1{TenantID: "tenant-conformance", OwnerUserID: "user-conformance", Resource: resource, Backend: driver.DescriptorV1()}
	catalog := domain.ProviderCatalogObservationV1{
		Version: 1, Scope: scope, CatalogRevision: "openrouter-2026-08-26",
		Models:     []domain.ProviderCatalogModelV1{{ModelVendorID: ModelVendorIDV1, ModelID: ModelIDV1, CatalogDigest: strings.Repeat("a", 64)}},
		ObservedAt: observed, ExpiresAt: expires,
	}
	route := domain.ProviderRoutePolicyV1{
		Version: 1, Scope: scope, State: domain.ProviderEvidenceSupportedV1, PolicyID: "ox-alpha-stealth-only", Revision: 1,
		FallbackPolicy: domain.ProviderFallbackDenyV1,
		Routes: []domain.ProviderRouteV1{{
			BackendKind: domain.HarnessBackendDirectOpenRouterV1, ModelVendorID: ModelVendorIDV1, ModelID: ModelIDV1,
			TransportKind: domain.ProviderTransportRouterAPIV1, TransportProvider: TransportProviderV1,
			UpstreamProviderID: ProviderIDV1, EndpointID: EndpointIDV1,
			BillingKind: domain.ProviderBillingRouterAccountV1, BillingAuthority: resource.ResourceID,
		}},
		ObservedAt: observed, ExpiresAt: expires,
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
		TrainingUse:        domain.ProviderEvidenceUnknownV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1},
		PolicyRevision: "stealth-eula-restrictive", ObservedAt: observed, ExpiresAt: expires,
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
		DecisionOwner: "sessionless-policy", EvidenceSource: "reviewed-openrouter-terms", Verdict: domain.ProviderPolicyConditionalV1,
		AllowedDataClasses:       []domain.ProviderDataClassV1{domain.ProviderDataExternallyShareableV1},
		CapabilityEvidenceDigest: string(capabilityDigest), PrivacyEvidenceDigest: string(privacyDigest),
		PriceObservationDigest: string(priceDigest), RoutePolicyDigest: string(routeDigest), ObservedAt: observed, ExpiresAt: expires,
	}
	policyDigest, err := policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.HarnessBindingV1{
		Version: 1, TenantID: "tenant-conformance", OwnerUserID: "user-conformance", RunID: "run-conformance", AttemptID: "attempt-conformance",
		Backend: driver.DescriptorV1(), Resource: resource, ModelVendorID: ModelVendorIDV1, ModelID: ModelIDV1,
		InputDataClass: domain.ProviderDataExternallyShareableV1, ProviderCatalogDigest: string(catalogDigest), ProviderRouteDigest: string(routeDigest),
		PrivacyPolicyDigest: string(privacyDigest), CapabilityEvidenceDigest: string(capabilityDigest), EffectivePolicyDigest: string(policyDigest),
		ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &expires,
	}
	fixture := harnessconformance.FixtureV1{
		Version: harnessconformance.VersionV1, FixtureID: "direct-openrouter-native-preflight",
		Placement: authority.ExecutionPlacementV2, Binding: binding, SubstrateBinding: authority.SubstrateBinding,
		AdmissionCostCeiling: authority.AdmissionCostCeiling,
		EvidenceBundle:       &harnessconformance.EvidenceBundleV1{Catalog: catalog, Route: route, Capability: capability, Privacy: privacy, Price: price, Policy: policy},
		Operation:            harnessconformance.OperationPreflightV1,
		Expected:             harnessconformance.ExpectedV1{RegistryContract: harnessconformance.RegistryContractPassV1, BackendProtocol: harnessconformance.BackendProtocolSupportedV1},
	}
	if err := fixture.Validate(); err != nil {
		t.Fatal(err)
	}
	return fixture
}
