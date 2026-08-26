package domain_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestProviderAuthorityDTOsContainNoSecretOrPayloadFields(t *testing.T) {
	t.Parallel()
	for _, value := range []any{domain.ProviderRouterResourceV1{}, domain.ProviderRoutePolicyV1{}, domain.ProviderCatalogObservationV1{}, domain.ProviderCapabilityEvidenceV1{}, domain.ProviderPrivacyEvidenceV1{}, domain.ProviderPriceObservationV1{}, domain.ProviderPolicyEvidenceV1{}, domain.HarnessBindingV1{}} {
		walkProviderAuthorityType(t, reflect.TypeOf(value), map[reflect.Type]bool{})
	}
}

func walkProviderAuthorityType(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.PkgPath() != "gitcode.com/urandon/sessionless/internal/domain" || seen[typ] {
		return
	}
	seen[typ] = true
	if typ.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := strings.ToLower(field.Name + " " + field.Tag.Get("json"))
		for _, forbidden := range []string{"secret", "api_key", "bearer", "credential_material", "prompt", "instruction", "provider_response", "raw_body"} {
			if strings.Contains(name, forbidden) {
				t.Fatalf("%s exposes forbidden field %s", typ.Name(), field.Name)
			}
		}
		walkProviderAuthorityType(t, field.Type, seen)
	}
}

func providerEvidenceScope() domain.ProviderEvidenceScopeV1 {
	return domain.ProviderEvidenceScopeV1{
		TenantID: "tenant-a", OwnerUserID: "user-a",
		Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceRouterAccountV1, ResourceID: "openrouter-account-a", OwnerUserID: "user-a", Revision: 7, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 3},
		Backend:  domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendDirectOpenRouterV1, ArtifactKind: domain.HarnessArtifactEmbeddedProfileV1, ArtifactDigest: strings.Repeat("a", 64), NativeProtocolVersion: "openai-compatible.v1", BackendProfileDigest: strings.Repeat("b", 64), ProviderContractKind: domain.ProviderContractInvocationV1},
	}
}

func TestProviderEvidencePreservesAxesUnknownAndKnownFree(t *testing.T) {
	t.Parallel()
	observed := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	expires := observed.Add(time.Hour)
	scope := providerEvidenceScope()
	capability := domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "stealth", ModelID: "ox-alpha", MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, ToolCalling: domain.ProviderEvidenceUnknownV1, StructuredOutput: domain.ProviderEvidenceUnsupportedV1, ObservedAt: observed, ExpiresAt: expires}
	privacy := domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "stealth", ModelID: "ox-alpha", AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataExternallyShareableV1, domain.ProviderDataPublicV1}, TrainingUse: domain.ProviderEvidenceUnknownV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, PolicyRevision: "stealth-terms-2026-08", ObservedAt: observed, ExpiresAt: expires}
	price := domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceSupportedV1, ModelVendorID: "stealth", ModelID: "ox-alpha", Currency: "USD", ObservedAt: observed, ExpiresAt: expires}
	for name, value := range map[string]interface{ Validate() error }{"capability": capability, "privacy": privacy, "known-free-price": price} {
		if err := value.Validate(); err != nil {
			t.Fatalf("%s rejected: %v", name, err)
		}
	}
	unknownPrice := domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, ModelVendorID: "stealth", ModelID: "ox-alpha", ObservedAt: observed, ExpiresAt: expires}
	if err := unknownPrice.Validate(); err != nil {
		t.Fatalf("unknown price rejected: %v", err)
	}
	unknownRoute := domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, FallbackPolicy: domain.ProviderFallbackDenyV1, ObservedAt: observed, ExpiresAt: expires}
	if err := unknownRoute.Validate(); err != nil {
		t.Fatalf("unknown route rejected: %v", err)
	}
	if _, err := price.Digest(); err != nil {
		t.Fatal(err)
	}
	bad := price
	bad.Currency = "usd"
	if bad.Validate() == nil {
		t.Fatal("noncanonical currency accepted")
	}
}

func TestProviderPolicyAndRouteBindOwnerResourceAndIndependentAxes(t *testing.T) {
	t.Parallel()
	observed := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	expires := observed.Add(time.Hour)
	scope := providerEvidenceScope()
	route := domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceSupportedV1, PolicyID: "public-research-route", Revision: 2, FallbackPolicy: domain.ProviderFallbackDenyV1, Routes: []domain.ProviderRouteV1{{BackendKind: domain.HarnessBackendDirectOpenRouterV1, ModelVendorID: "stealth", TransportKind: domain.ProviderTransportRouterAPIV1, TransportProvider: "openrouter", UpstreamProviderID: "stealth", EndpointID: "stealth", BillingKind: domain.ProviderBillingRouterAccountV1, BillingAuthority: "openrouter-account-a", ModelID: "ox-alpha"}}, ObservedAt: observed, ExpiresAt: expires}
	routeDigest, err := route.Digest()
	if err != nil {
		t.Fatal(err)
	}
	policy := domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: "public-only", Revision: 1, DecisionOwner: "sessionless-policy", EvidenceSource: "reviewed-openrouter-terms", Verdict: domain.ProviderPolicyGoV1, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataExternallyShareableV1, domain.ProviderDataPublicV1}, CapabilityEvidenceDigest: strings.Repeat("c", 64), PrivacyEvidenceDigest: strings.Repeat("d", 64), PriceObservationDigest: strings.Repeat("e", 64), RoutePolicyDigest: string(routeDigest), ObservedAt: observed, ExpiresAt: expires}
	unknownPolicy := policy.Clone()
	unknownPolicy.Verdict = domain.ProviderPolicyNoGoV1
	unknownPolicy.AllowedDataClasses = nil
	unknownPolicy.CapabilityEvidenceDigest = ""
	unknownPolicy.PrivacyEvidenceDigest = ""
	unknownPolicy.PriceObservationDigest = ""
	unknownPolicy.RoutePolicyDigest = ""
	if err := unknownPolicy.Validate(); err != nil {
		t.Fatalf("no-go policy without fabricated evidence rejected: %v", err)
	}
	base, err := policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	mutations := []func(*domain.ProviderPolicyEvidenceV1){func(v *domain.ProviderPolicyEvidenceV1) { v.Scope.OwnerUserID = "user-b" }, func(v *domain.ProviderPolicyEvidenceV1) { v.Scope.Resource.Revision++ }, func(v *domain.ProviderPolicyEvidenceV1) { v.Verdict = domain.ProviderPolicyNoGoV1 }, func(v *domain.ProviderPolicyEvidenceV1) { v.RoutePolicyDigest = strings.Repeat("f", 64) }}
	for i, mutate := range mutations {
		candidate := policy.Clone()
		mutate(&candidate)
		digest, err := candidate.Digest()
		if err == nil && digest == base {
			t.Fatalf("mutation %d did not reject or change digest", i)
		}
	}
	clone := route.Clone()
	clone.Routes[0].ModelID = "other"
	if route.Routes[0].ModelID == clone.Routes[0].ModelID {
		t.Fatal("route clone aliased slice")
	}
}

func TestProviderCatalogRejectsCrossResourceBaitSwitchAndUnsortedModels(t *testing.T) {
	t.Parallel()
	observed := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	value := domain.ProviderCatalogObservationV1{Version: 1, Scope: providerEvidenceScope(), CatalogRevision: "openrouter-2026-08-26", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: "stealth", ModelID: "ox-alpha", CatalogDigest: strings.Repeat("a", 64)}}, ObservedAt: observed, ExpiresAt: observed.Add(time.Hour)}
	base, err := value.Digest()
	if err != nil {
		t.Fatal(err)
	}
	mutated := value.Clone()
	mutated.Scope.Resource.ResourceID = "other-account"
	other, err := mutated.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if other == base {
		t.Fatal("cross-resource catalog digest did not change")
	}
	mutated = value.Clone()
	mutated.Models = append(mutated.Models, domain.ProviderCatalogModelV1{ModelVendorID: "a", ModelID: "first", CatalogDigest: strings.Repeat("b", 64)})
	if mutated.Validate() == nil {
		t.Fatal("unsorted catalog accepted")
	}
}
