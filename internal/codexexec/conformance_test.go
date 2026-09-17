package codexexec

import (
	"context"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func TestCodexSubscriptionPreflightPassesAttachedWorkerHarnessConformance(t *testing.T) {
	t.Parallel()
	request, authority := validDriverRequest(t)
	driver := mustDriver(t, true, authority, &fixtureProcessBoundary{})
	fixture := subscriptionConformanceFixture(t, driver, request)
	withManagedAuthority := fixture.Clone()
	withManagedAuthority.SubstrateBinding.Version = domain.SubstrateBindingVersionV1
	if err := withManagedAuthority.Validate(); err == nil {
		t.Fatal("attached-worker fixture accepted managed substrate authority")
	}
	registration, err := registrationV1(driver, true)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return driverNow }, registration)
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
		result.BackendProtocol != harnessconformance.BackendProtocolSupportedV1 ||
		result.FailureCode != "" || result.SideEffects != (harnessconformance.SideEffectsV1{}) ||
		result.Validate() != nil {
		t.Fatalf("conformance result = %+v", result)
	}
}

func subscriptionConformanceFixture(
	t *testing.T,
	driver *Driver,
	request ports.ExecutionRequest,
) harnessconformance.FixtureV1 {
	t.Helper()
	observed := driverNow.Add(-time.Minute)
	expires := driverNow.Add(time.Hour)
	resource := request.HarnessBinding.Resource
	scope := domain.ProviderEvidenceScopeV1{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID,
		Resource: resource, Backend: driver.DescriptorV1(),
	}
	catalog := domain.ProviderCatalogObservationV1{
		Version: 1, Scope: scope, CatalogRevision: "openai-subscription-2026-09",
		Models: []domain.ProviderCatalogModelV1{{
			ModelVendorID: ModelVendorIDV1, ModelID: "gpt-fixture",
			CatalogDigest: strings.Repeat("a", 64),
		}},
		ObservedAt: observed, ExpiresAt: expires,
	}
	route := domain.ProviderRoutePolicyV1{
		Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1,
		FallbackPolicy: domain.ProviderFallbackDenyV1,
		ObservedAt:     observed, ExpiresAt: expires,
	}
	capability := domain.ProviderCapabilityEvidenceV1{
		Version: 1, Scope: scope, ModelVendorID: ModelVendorIDV1, ModelID: "gpt-fixture",
		MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1},
		ToolCalling:      domain.ProviderEvidenceUnsupportedV1,
		StructuredOutput: domain.ProviderEvidenceUnknownV1,
		ObservedAt:       observed, ExpiresAt: expires,
	}
	privacy := domain.ProviderPrivacyEvidenceV1{
		Version: 1, Scope: scope, ModelVendorID: ModelVendorIDV1, ModelID: "gpt-fixture",
		AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1},
		TrainingUse:        domain.ProviderEvidenceUnknownV1,
		RetentionHours:     domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1},
		PolicyRevision:     "OAI-SUB-2026-09-PLUS-PRO-LOCAL",
		ObservedAt:         observed, ExpiresAt: expires,
	}
	price := domain.ProviderPriceObservationV1{
		Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1,
		ModelVendorID: ModelVendorIDV1, ModelID: "gpt-fixture",
		ObservedAt: observed, ExpiresAt: expires,
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
		Version: 1, Scope: scope, PolicyID: "OAI-SUB-2026-09-PLUS-PRO-LOCAL", Revision: 1,
		DecisionOwner: "sessionless-policy", EvidenceSource: "reviewed-openai-subscription-policy",
		Verdict:                  domain.ProviderPolicyConditionalV1,
		AllowedDataClasses:       []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1},
		CapabilityEvidenceDigest: string(capabilityDigest),
		PrivacyEvidenceDigest:    string(privacyDigest),
		PriceObservationDigest:   string(priceDigest), RoutePolicyDigest: string(routeDigest),
		ObservedAt: observed, ExpiresAt: expires,
	}
	policyDigest, err := policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	binding := request.HarnessBinding.Clone()
	binding.ProviderCatalogDigest = string(catalogDigest)
	binding.ProviderRouteDigest = string(routeDigest)
	binding.PrivacyPolicyDigest = string(privacyDigest)
	binding.CapabilityEvidenceDigest = string(capabilityDigest)
	binding.EffectivePolicyDigest = string(policyDigest)
	binding.EvidenceExpiresAt = &expires
	bundle := harnessconformance.EvidenceBundleV1{
		Catalog: catalog, Route: route, Capability: capability,
		Privacy: privacy, Price: price, Policy: policy,
	}
	fixture := harnessconformance.FixtureV1{
		Version:   harnessconformance.VersionV1,
		FixtureID: "codex-subscription-attached-worker-preflight",
		Placement: request.ExecutionPlacementV2, Binding: binding,
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
