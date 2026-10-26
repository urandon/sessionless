package attachedresourcegeneration_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegeneration"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func registryBaseline(t *testing.T, index, extraModels int) domain.AttachedResourceGenerationBaselineV1 {
	t.Helper()
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	expires := at.Add(time.Hour)
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: fmt.Sprintf("subscription-%d", index), OwnerUserID: "owner", Revision: 7, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 3}
	backend := domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendCodexExecV1, ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("a", 64), NativeProtocolVersion: "native-v3", BackendProfileDigest: strings.Repeat("b", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryFileV1}
	scope := domain.ProviderEvidenceScopeV1{TenantID: "tenant", OwnerUserID: resource.OwnerUserID, Resource: resource, Backend: backend}
	bundle := domain.ProviderEvidenceBundleV1{
		Catalog:    domain.ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "reviewed-catalog", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: "openai", ModelID: "approved-model", CatalogDigest: strings.Repeat("a", 64)}}, ObservedAt: at, ExpiresAt: expires},
		Route:      domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceSupportedV1, PolicyID: "approved-route", Revision: 1, FallbackPolicy: domain.ProviderFallbackDenyV1, Routes: []domain.ProviderRouteV1{{BackendKind: backend.BackendKind, ModelVendorID: "openai", ModelID: "approved-model", TransportKind: domain.ProviderTransportLocalCLIV1, TransportProvider: "codex", UpstreamProviderID: "openai", EndpointID: "approved-route", BillingKind: domain.ProviderBillingSubscriptionV1, BillingAuthority: resource.ResourceID}}, ObservedAt: at, ExpiresAt: expires},
		Capability: domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "openai", ModelID: "approved-model", MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, ToolCalling: domain.ProviderEvidenceUnknownV1, StructuredOutput: domain.ProviderEvidenceUnsupportedV1, ObservedAt: at, ExpiresAt: expires},
		Privacy:    domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "openai", ModelID: "approved-model", AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}, TrainingUse: domain.ProviderEvidenceUnknownV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, PolicyRevision: "reviewed-terms", ObservedAt: at, ExpiresAt: expires},
		Price:      domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, ModelVendorID: "openai", ModelID: "approved-model", ObservedAt: at, ExpiresAt: expires},
		Policy:     domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: "approved-policy", Revision: 1, DecisionOwner: "operator", EvidenceSource: "reviewed-terms", Verdict: domain.ProviderPolicyGoV1, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}, ObservedAt: at, ExpiresAt: expires},
	}
	for i := range extraModels {
		bundle.Catalog.Models = append(bundle.Catalog.Models, domain.ProviderCatalogModelV1{ModelVendorID: "zz-vendor", ModelID: fmt.Sprintf("model-%03d-%s", i, strings.Repeat("m", 190)), CatalogDigest: strings.Repeat("a", 64)})
	}
	sort.Slice(bundle.Catalog.Models, func(i, j int) bool {
		a, b := bundle.Catalog.Models[i], bundle.Catalog.Models[j]
		return a.ModelVendorID+"\x00"+a.ModelID < b.ModelVendorID+"\x00"+b.ModelID
	})
	binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, Backend: backend, Resource: resource, ModelVendorID: "openai", ModelID: "approved-model", InputDataClass: domain.ProviderDataPublicV1, EvidenceExpiresAt: &expires}
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
			t.Fatalf("independent baseline fixture digest: %v", err)
		}
		for _, field := range item.fields {
			*field = string(digest)
		}
	}
	baseline := domain.AttachedResourceGenerationBaselineV1{Version: 1, TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, SubscriptionConnectionID: domain.SubscriptionConnectionID(resource.ResourceID), WorkerID: domain.AttachedWorkerID(fmt.Sprintf("worker-%d", index)), EnrollmentGeneration: 2, BaselineRevision: resource.Revision, CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("c", 64)), GenerationOnlyDelegation: true, Binding: binding, Evidence: bundle}
	if err := baseline.Validate(); err != nil {
		t.Fatalf("independent complete baseline fixture: %v", err)
	}
	return baseline
}

func registryConfig(t *testing.T, baselines ...domain.AttachedResourceGenerationBaselineV1) []byte {
	t.Helper()
	raw, err := json.Marshal(struct {
		Version   uint32                                        `json:"version"`
		Baselines []domain.AttachedResourceGenerationBaselineV1 `json:"baselines"`
	}{1, baselines})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestRegistryOwnsIndependentExactScopedCopies(t *testing.T) {
	t.Parallel()
	b := registryBaseline(t, 1, 0)
	want := b.Clone()
	registry, err := attachedresourcegeneration.NewRegistry([]domain.AttachedResourceGenerationBaselineV1{b})
	if err != nil {
		t.Fatal(err)
	}
	b.Evidence.Catalog.Models[0].ModelID = "caller-mutated"
	*b.Binding.EvidenceExpiresAt = time.Time{}
	got, found := registry.Lookup(want.TenantID, want.OwnerUserID, want.SubscriptionConnectionID)
	if !found || !reflect.DeepEqual(got, want) || registry.Len() != 1 || !registry.ContainsWorker(want.TenantID, want.OwnerUserID, want.WorkerID) {
		t.Fatalf("exact immutable lookup failed: found=%t count=%d", found, registry.Len())
	}
	got.Evidence.Route.Routes[0].EndpointID = "lookup-mutated"
	*got.Binding.EvidenceExpiresAt = time.Time{}
	again, _ := registry.Lookup(want.TenantID, want.OwnerUserID, want.SubscriptionConnectionID)
	if !reflect.DeepEqual(again, want) {
		t.Fatal("lookup exposed mutable registry alias")
	}
	for _, key := range []struct {
		tenant   domain.TenantID
		owner    domain.UserID
		resource domain.SubscriptionConnectionID
	}{{"foreign", want.OwnerUserID, want.SubscriptionConnectionID}, {want.TenantID, "foreign", want.SubscriptionConnectionID}, {want.TenantID, want.OwnerUserID, "foreign"}} {
		if value, found := registry.Lookup(key.tenant, key.owner, key.resource); found || !reflect.DeepEqual(value, domain.AttachedResourceGenerationBaselineV1{}) {
			t.Fatal("foreign resource lookup returned baseline")
		}
	}
	if registry.ContainsWorker("foreign", want.OwnerUserID, want.WorkerID) || registry.ContainsWorker(want.TenantID, "foreign", want.WorkerID) || registry.ContainsWorker(want.TenantID, want.OwnerUserID, "foreign") {
		t.Fatal("worker collision lookup escaped exact owner/tenant scope")
	}
	var empty *attachedresourcegeneration.Registry
	if empty.Len() != 0 || empty.ContainsWorker(want.TenantID, want.OwnerUserID, want.WorkerID) {
		t.Fatal("nil registry enabled authority")
	}
	if _, found := empty.Lookup(want.TenantID, want.OwnerUserID, want.SubscriptionConnectionID); found {
		t.Fatal("nil registry returned baseline")
	}
}

func TestRegistryRejectsCollisionsAndBounds(t *testing.T) {
	t.Parallel()
	b := registryBaseline(t, 1, 0)
	for _, tc := range []struct {
		name      string
		baselines []domain.AttachedResourceGenerationBaselineV1
	}{
		{"empty", nil}, {"duplicate resource", []domain.AttachedResourceGenerationBaselineV1{b, b.Clone()}},
		{"worker collision", []domain.AttachedResourceGenerationBaselineV1{b, func() domain.AttachedResourceGenerationBaselineV1 {
			other := registryBaseline(t, 2, 0)
			other.WorkerID = b.WorkerID
			return other
		}()}},
		{"invalid delegation", []domain.AttachedResourceGenerationBaselineV1{func() domain.AttachedResourceGenerationBaselineV1 {
			other := b.Clone()
			other.GenerationOnlyDelegation = false
			return other
		}()}},
		{"too many", make([]domain.AttachedResourceGenerationBaselineV1, attachedresourcegeneration.MaxBaselinesV1+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if registry, err := attachedresourcegeneration.NewRegistry(tc.baselines); registry != nil || !errors.Is(err, attachedresourcegeneration.ErrInvalidConfig) {
				t.Fatalf("invalid registry=%v err=%v", registry, err)
			}
		})
	}
	maximum := make([]domain.AttachedResourceGenerationBaselineV1, attachedresourcegeneration.MaxBaselinesV1)
	for i := range maximum {
		maximum[i] = registryBaseline(t, i, 0)
	}
	registry, err := attachedresourcegeneration.NewRegistry(maximum)
	if err != nil || registry.Len() != attachedresourcegeneration.MaxBaselinesV1 {
		t.Fatalf("inclusive count bound: %v", err)
	}
	large := make([]domain.AttachedResourceGenerationBaselineV1, 32)
	for i := range large {
		large[i] = registryBaseline(t, i, 100)
	}
	encoded := registryConfig(t, large...)
	if len(encoded) <= attachedresourcegeneration.MaxConfigBytesV1 {
		t.Fatalf("total-size fixture too small: %d", len(encoded))
	}
	if registry, err := attachedresourcegeneration.NewRegistry(large); registry != nil || !errors.Is(err, attachedresourcegeneration.ErrInvalidConfig) {
		t.Fatalf("oversized direct constructor accepted: %v", err)
	}
}

func TestRegistryConcurrentLookupsHaveNoMutableAliases(t *testing.T) {
	t.Parallel()
	b := registryBaseline(t, 1, 0)
	registry, err := attachedresourcegeneration.NewRegistry([]domain.AttachedResourceGenerationBaselineV1{b})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 8 {
				copy, found := registry.Lookup(b.TenantID, b.OwnerUserID, b.SubscriptionConnectionID)
				if !found {
					t.Error("concurrent exact lookup absent")
					return
				}
				copy.Evidence.Catalog.Models[0].ModelID = "local"
				*copy.Binding.EvidenceExpiresAt = time.Time{}
			}
		})
	}
	wg.Wait()
	got, found := registry.Lookup(b.TenantID, b.OwnerUserID, b.SubscriptionConnectionID)
	if !found || !reflect.DeepEqual(got, b) {
		t.Fatal("concurrent lookup mutation changed shared registry")
	}
}

func TestRegistryRejectsOtherwiseValidOversizedIndividualBaseline(t *testing.T) {
	t.Parallel()
	b := registryBaseline(t, 1, 100)
	for i := 100; i < 200; i++ {
		b.Evidence.Catalog.Models = append(b.Evidence.Catalog.Models, domain.ProviderCatalogModelV1{ModelVendorID: "zz-vendor", ModelID: fmt.Sprintf("model-%03d-%s", i, strings.Repeat("m", 190)), CatalogDigest: strings.Repeat("a", 64)})
	}
	digest, err := b.Evidence.Catalog.Digest()
	if err != nil {
		t.Fatalf("oversize baseline must retain valid catalog semantics: %v", err)
	}
	b.Binding.ProviderCatalogDigest = string(digest)
	raw, err := json.Marshal(b)
	if err != nil || len(raw) <= domain.AttachedResourceGenerationBaselineMaxBytesV1 {
		t.Fatalf("individual-size fixture: bytes=%d err=%v", len(raw), err)
	}
	if err := b.Validate(); err == nil || !strings.Contains(err.Error(), "bounded complete baseline") {
		t.Fatalf("individual fixture failed outside intended encoding bound: %v", err)
	}
	if registry, err := attachedresourcegeneration.NewRegistry([]domain.AttachedResourceGenerationBaselineV1{b}); registry != nil || !errors.Is(err, attachedresourcegeneration.ErrInvalidConfig) {
		t.Fatalf("oversize individual constructor accepted: %v", err)
	}
	config := registryConfig(t, b)
	if len(config) >= attachedresourcegeneration.MaxConfigBytesV1 {
		t.Fatal("individual test accidentally exceeds independent total config bound")
	}
	if registry, err := attachedresourcegeneration.DecodeRegistryConfigV1(config); registry != nil || !errors.Is(err, attachedresourcegeneration.ErrInvalidConfig) {
		t.Fatalf("oversize individual file config accepted: %v", err)
	}
}
