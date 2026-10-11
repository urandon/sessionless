package domain_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Independent domain fixture: no registry, conformance runner or provider call.
func providerBundleFixture(t *testing.T) (domain.ProviderEvidenceBundleV1, domain.HarnessBindingV2) {
	t.Helper()
	scope := providerEvidenceScope()
	observed := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	expires := observed.Add(time.Hour)
	bundle := domain.ProviderEvidenceBundleV1{
		Catalog:    domain.ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "reviewed-catalog", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: "stealth", ModelID: "ox-alpha", CatalogDigest: strings.Repeat("a", 64)}}, ObservedAt: observed, ExpiresAt: expires},
		Route:      domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceSupportedV1, PolicyID: "reviewed-route", Revision: 2, FallbackPolicy: domain.ProviderFallbackDenyV1, Routes: []domain.ProviderRouteV1{{BackendKind: scope.Backend.BackendKind, ModelVendorID: "stealth", ModelID: "ox-alpha", TransportKind: domain.ProviderTransportRouterAPIV1, TransportProvider: "router", UpstreamProviderID: "upstream", EndpointID: "reviewed-endpoint", BillingKind: domain.ProviderBillingRouterAccountV1, BillingAuthority: scope.Resource.ResourceID}}, ObservedAt: observed, ExpiresAt: expires},
		Capability: domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "stealth", ModelID: "ox-alpha", MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceSupportedV1, Value: 8192}, ToolCalling: domain.ProviderEvidenceSupportedV1, StructuredOutput: domain.ProviderEvidenceSupportedV1, ObservedAt: observed, ExpiresAt: expires},
		Privacy:    domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "stealth", ModelID: "ox-alpha", AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, TrainingUse: domain.ProviderEvidenceFalseV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceSupportedV1, Value: 24}, PolicyRevision: "reviewed-privacy", ObservedAt: observed, ExpiresAt: expires},
		Price:      domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceSupportedV1, ModelVendorID: "stealth", ModelID: "ox-alpha", Currency: "USD", InputMicrounitsPerMTokens: 1, OutputMicrounitsPerMTokens: 2, ObservedAt: observed, ExpiresAt: expires},
		Policy:     domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: "reviewed-private-policy", Revision: 3, DecisionOwner: "review-owner", EvidenceSource: "reviewed-terms", Verdict: domain.ProviderPolicyGoV1, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, ObservedAt: observed, ExpiresAt: expires},
	}
	binding := domain.HarnessBindingV2{Version: domain.HarnessBindingVersionV2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, RunID: "bundle-run", AttemptID: "bundle-attempt", Backend: scope.Backend, Resource: scope.Resource, ModelVendorID: "stealth", ModelID: "ox-alpha", InputDataClass: domain.ProviderDataPrivateV1, ExecutionPlacementDigest: strings.Repeat("e", 64), EvidenceExpiresAt: &expires}
	sealProviderBundleFixture(t, &bundle, &binding)
	if err := bundle.ValidateForBinding(binding); err != nil {
		t.Fatalf("independent bundle fixture: %v", err)
	}
	return bundle, binding
}

func sealProviderBundleFixture(t *testing.T, bundle *domain.ProviderEvidenceBundleV1, binding *domain.HarnessBindingV2) {
	t.Helper()
	for _, item := range []struct {
		digest func() (domain.ProviderEvidenceDigestV1, error)
		fields []*string
	}{
		{bundle.Catalog.Digest, []*string{&binding.ProviderCatalogDigest}},
		{bundle.Route.Digest, []*string{&binding.ProviderRouteDigest, &bundle.Policy.RoutePolicyDigest}},
		{bundle.Capability.Digest, []*string{&binding.CapabilityEvidenceDigest, &bundle.Policy.CapabilityEvidenceDigest}},
		{bundle.Privacy.Digest, []*string{&binding.PrivacyPolicyDigest, &bundle.Policy.PrivacyEvidenceDigest}},
		{bundle.Price.Digest, []*string{&bundle.Policy.PriceObservationDigest}},
		{func() (domain.ProviderEvidenceDigestV1, error) { return bundle.Policy.Digest() }, []*string{&binding.EffectivePolicyDigest}},
	} {
		digest, err := item.digest()
		if err != nil {
			t.Fatalf("fixture digest: %v", err)
		}
		for _, field := range item.fields {
			*field = string(digest)
		}
	}
}

func TestProviderEvidenceBundleExactScopeAndModels(t *testing.T) {
	t.Parallel()
	for _, source := range []string{"catalog", "route", "capability", "privacy", "price", "policy"} {
		for _, axis := range []string{"tenant", "owner", "resource", "revision", "generation", "backend"} {
			t.Run(source+"/"+axis, func(t *testing.T) {
				bundle, binding := providerBundleFixture(t)
				scopes := map[string]*domain.ProviderEvidenceScopeV1{"catalog": &bundle.Catalog.Scope, "route": &bundle.Route.Scope, "capability": &bundle.Capability.Scope, "privacy": &bundle.Privacy.Scope, "price": &bundle.Price.Scope, "policy": &bundle.Policy.Scope}
				scope := scopes[source]
				switch axis {
				case "tenant":
					scope.TenantID = "foreign-tenant"
				case "owner":
					scope.OwnerUserID, scope.Resource.OwnerUserID = "foreign-user", "foreign-user"
				case "resource":
					scope.Resource.ResourceID = "foreign-resource"
				case "revision":
					scope.Resource.Revision++
				case "generation":
					scope.Resource.CredentialGeneration++
				case "backend":
					scope.Backend.ArtifactDigest = strings.Repeat("f", 64)
				}
				if err := bundle.ValidateForBinding(binding); err == nil {
					t.Fatalf("accepted foreign %s %s", source, axis)
				}
			})
		}
	}
	for _, source := range []string{"catalog", "capability", "privacy", "price"} {
		for _, axis := range []string{"vendor", "model"} {
			t.Run(source+"/"+axis, func(t *testing.T) {
				bundle, binding := providerBundleFixture(t)
				models := map[string][2]*string{"catalog": {&bundle.Catalog.Models[0].ModelVendorID, &bundle.Catalog.Models[0].ModelID}, "capability": {&bundle.Capability.ModelVendorID, &bundle.Capability.ModelID}, "privacy": {&bundle.Privacy.ModelVendorID, &bundle.Privacy.ModelID}, "price": {&bundle.Price.ModelVendorID, &bundle.Price.ModelID}}
				index := 0
				if axis == "model" {
					index = 1
				}
				*models[source][index] = "other"
				if bundle.ValidateForBinding(binding) == nil {
					t.Fatalf("accepted foreign %s %s", source, axis)
				}
			})
		}
	}
}

func TestProviderEvidenceBundleDigestAndWindowFences(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"catalog", "route", "capability", "privacy", "policy", "policy-capability", "policy-privacy", "policy-price", "policy-route"} {
		t.Run(field, func(t *testing.T) {
			bundle, binding := providerBundleFixture(t)
			fields := map[string]*string{"catalog": &binding.ProviderCatalogDigest, "route": &binding.ProviderRouteDigest, "capability": &binding.CapabilityEvidenceDigest, "privacy": &binding.PrivacyPolicyDigest, "policy": &binding.EffectivePolicyDigest, "policy-capability": &bundle.Policy.CapabilityEvidenceDigest, "policy-privacy": &bundle.Policy.PrivacyEvidenceDigest, "policy-price": &bundle.Policy.PriceObservationDigest, "policy-route": &bundle.Policy.RoutePolicyDigest}
			*fields[field] = strings.Repeat("f", 64)
			if bundle.ValidateForBinding(binding) == nil {
				t.Fatalf("accepted changed %s digest", field)
			}
		})
	}
	for _, source := range []string{"catalog", "route", "capability", "privacy", "price", "policy"} {
		for _, invalid := range []bool{false, true} {
			name := source + "/earlier"
			if invalid {
				name = source + "/invalid"
			}
			t.Run(name, func(t *testing.T) {
				bundle, binding := providerBundleFixture(t)
				windows := map[string]*time.Time{"catalog": &bundle.Catalog.ExpiresAt, "route": &bundle.Route.ExpiresAt, "capability": &bundle.Capability.ExpiresAt, "privacy": &bundle.Privacy.ExpiresAt, "price": &bundle.Price.ExpiresAt, "policy": &bundle.Policy.ExpiresAt}
				*windows[source] = bundle.Catalog.ObservedAt.Add(30 * time.Minute)
				if invalid {
					*windows[source] = bundle.Catalog.ObservedAt
				} else {
					sealProviderBundleFixture(t, &bundle, &binding)
				}
				if bundle.ValidateForBinding(binding) == nil {
					t.Fatalf("accepted %s without exact minimum expiry", name)
				}
				if !invalid {
					expiry := *windows[source]
					binding.EvidenceExpiresAt = &expiry
					if err := bundle.ValidateForBinding(binding); err != nil {
						t.Fatalf("exact minimum %s rejected: %v", source, err)
					}
				}
			})
		}
	}
}

func TestProviderEvidenceBundleCloneAndWireShape(t *testing.T) {
	t.Parallel()
	bundle, binding := providerBundleFixture(t)
	before, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	clone := bundle.Clone()
	clone.Catalog.Models[0].ModelID = "changed"
	clone.Route.Routes[0].EndpointID = "changed"
	clone.Privacy.AllowedDataClasses[0] = domain.ProviderDataPublicV1
	clone.Policy.AllowedDataClasses[0] = domain.ProviderDataPublicV1
	after, err := json.Marshal(bundle)
	if err != nil || string(before) != string(after) || bundle.ValidateForBinding(binding) != nil {
		t.Fatalf("clone altered original: %v", err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(before, &wire); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"catalog", "route", "capability", "privacy", "price", "policy"} {
		if len(wire[field]) == 0 {
			t.Fatalf("lost canonical field %s", field)
		}
	}
	if len(wire) != 6 {
		t.Fatalf("wire shape widened: %d fields", len(wire))
	}
	walkProviderAuthorityType(t, reflect.TypeOf(bundle), map[reflect.Type]bool{})
}

func TestProviderEvidenceBundleConsistencyIsNotPermission(t *testing.T) {
	t.Parallel()
	bundle, binding := providerBundleFixture(t)
	// Preserve the extracted validator's structural-only semantics: a denied
	// policy is valid evidence, not permission to execute private context.
	bundle.Policy.Verdict = domain.ProviderPolicyNoGoV1
	bundle.Policy.AllowedDataClasses = nil
	sealProviderBundleFixture(t, &bundle, &binding)
	if err := bundle.ValidateForBinding(binding); err != nil {
		t.Fatalf("structurally consistent no-go evidence rejected: %v", err)
	}
	// Expired-but-well-formed evidence is also representable. Callers must
	// separately enforce current DB-time windows before any admission/effect.
	if err := binding.ValidateAt(bundle.Catalog.ExpiresAt); err == nil {
		t.Fatal("structural consistency bypassed canonical expiry fence")
	}
}

func TestProviderEvidenceBundleExecutionComparator(t *testing.T) {
	t.Parallel()
	bundle, binding := providerBundleFixture(t)
	route := bundle.Route.Routes[0]
	base := domain.ProviderExecutionEvidenceV1{AcceptanceClass: domain.ProviderAcceptanceAcceptedV1, FinishClass: domain.ProviderFinishCompletedV1, RouteState: domain.ProviderEvidenceSupportedV1, ActualModelVendorID: binding.ModelVendorID, ActualModelID: binding.ModelID, TransportKind: route.TransportKind, TransportProvider: route.TransportProvider, UpstreamProviderID: route.UpstreamProviderID, EndpointID: route.EndpointID, PolicyVerdict: domain.ProviderPolicyGoV1, UsageProvenance: domain.ProviderUsageUnknownV1}
	for _, test := range []struct {
		name   string
		mutate func(*domain.ProviderExecutionEvidenceV1)
		valid  bool
	}{
		{"exact", func(*domain.ProviderExecutionEvidenceV1) {}, true},
		{"endpoint", func(v *domain.ProviderExecutionEvidenceV1) { v.EndpointID = "other" }, false},
		{"transport", func(v *domain.ProviderExecutionEvidenceV1) { v.TransportProvider = "other" }, false},
		{"upstream", func(v *domain.ProviderExecutionEvidenceV1) { v.UpstreamProviderID = "other" }, false},
		{"verdict", func(v *domain.ProviderExecutionEvidenceV1) { v.PolicyVerdict = domain.ProviderPolicyConditionalV1 }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := base.Clone()
			test.mutate(&value)
			evidence, err := value.SealForBinding(binding)
			if err != nil {
				t.Fatalf("sealed observation: %v", err)
			}
			if err := bundle.ValidateExecutionEvidence(binding, evidence); (err == nil) != test.valid {
				t.Fatalf("execution %s accepted%v want%v: %v", test.name, err == nil, test.valid, err)
			}
		})
	}
}
