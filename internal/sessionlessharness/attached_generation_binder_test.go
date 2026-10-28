package sessionlessharness_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegeneration"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

type generationBinderStore struct {
	*attachedBinderStore
	head      domain.AttachedResourceGenerationHeadV1
	headFound bool
	headErr   error
	headReads int
}

func (s *generationBinderStore) LoadAttachedResourceGenerationHeadV1(_ context.Context, tenant domain.TenantID, owner domain.UserID, resource domain.SubscriptionConnectionID) (domain.AttachedResourceGenerationHeadV1, bool, error) {
	s.headReads++
	if tenant != s.worker.TenantID || owner != s.worker.OwnerUserID || resource != "resource-a" {
		return domain.AttachedResourceGenerationHeadV1{}, false, errors.New("foreign preflight scope")
	}
	return s.head, s.headFound, s.headErr
}

func generationBinderFixture(t *testing.T) (sessionlessharness.AttachedResourceBinderConfig, *generationBinderStore, ports.HarnessBindingRequest, domain.AttachedResourceGenerationBaselineV1) {
	t.Helper()
	config, store, request := attachedBinderFixture(t)
	pin := config.Resources[0]
	binding := pin.Binding.Clone()
	binding.ModelVendorID, binding.ModelID = "openai", "approved-model"
	at, expiry := request.At, *binding.EvidenceExpiresAt
	scope := domain.ProviderEvidenceScopeV1{TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, Resource: binding.Resource, Backend: binding.Backend}
	bundle := domain.ProviderEvidenceBundleV1{
		Catalog:    domain.ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "reviewed-catalog", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, CatalogDigest: binding.ProviderCatalogDigest}}, ObservedAt: at, ExpiresAt: expiry},
		Route:      domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, FallbackPolicy: domain.ProviderFallbackDenyV1, ObservedAt: at, ExpiresAt: expiry},
		Capability: domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, ToolCalling: domain.ProviderEvidenceUnknownV1, StructuredOutput: domain.ProviderEvidenceUnknownV1, ObservedAt: at, ExpiresAt: expiry},
		Privacy:    domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, AllowedDataClasses: []domain.ProviderDataClassV1{binding.InputDataClass}, TrainingUse: domain.ProviderEvidenceUnknownV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, PolicyRevision: "reviewed-terms", ObservedAt: at, ExpiresAt: expiry},
		Price:      domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, ObservedAt: at, ExpiresAt: expiry},
		Policy:     domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: "approved-policy", Revision: 1, DecisionOwner: "operator", EvidenceSource: "reviewed-terms", Verdict: domain.ProviderPolicyGoV1, AllowedDataClasses: []domain.ProviderDataClassV1{binding.InputDataClass}, ObservedAt: at, ExpiresAt: expiry},
	}
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
			t.Fatal(err)
		}
		for _, field := range item.fields {
			*field = string(digest)
		}
	}
	baseline := domain.AttachedResourceGenerationBaselineV1{Version: 1, TenantID: pin.TenantID, OwnerUserID: pin.OwnerUserID,
		SubscriptionConnectionID: pin.SubscriptionConnectionID, WorkerID: pin.WorkerID, EnrollmentGeneration: pin.EnrollmentGeneration,
		BaselineRevision: binding.Resource.Revision, CapabilityDigest: pin.CapabilityDigest, GenerationOnlyDelegation: true, Binding: binding, Evidence: bundle}
	registry, err := attachedresourcegeneration.NewRegistry([]domain.AttachedResourceGenerationBaselineV1{baseline})
	if err != nil {
		t.Fatalf("complete baseline fixture: %v", err)
	}
	config.Resources, config.Generations = nil, registry
	return config, &generationBinderStore{attachedBinderStore: store, headFound: true,
		head: domain.AttachedResourceGenerationHeadV1{Version: 1, OwnerUserID: pin.OwnerUserID, WorkerID: pin.WorkerID,
			EnrollmentGeneration: pin.EnrollmentGeneration, BaselineRevision: baseline.BaselineRevision, CredentialGeneration: 4}}, request, baseline
}

func TestAttachedGenerationBinderUsesPublishedHeadAndPreservesPreviousBinding(t *testing.T) {
	t.Parallel()
	config, store, request, baseline := generationBinderFixture(t)
	binder, err := sessionlessharness.NewAttachedResourceBinder(config, store)
	if err != nil {
		t.Fatal(err)
	}
	first, err := binder.BindHarness(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	want, _, err := domain.ProjectAttachedResourceGenerationV1(baseline, 4)
	if err != nil {
		t.Fatal(err)
	}
	if first.HarnessBinding.Resource.CredentialGeneration != 4 || first.HarnessBinding.EffectivePolicyDigest != want.EffectivePolicyDigest ||
		first.ExecutionPlacementV2.PolicyDigest != domain.AttachedWorkerPolicyDigest(want.EffectivePolicyDigest) || store.headReads != 1 || store.reads != 2 {
		t.Fatalf("wrong fresh binding/head reads: generation=%d head=%d reads=%d", first.HarnessBinding.Resource.CredentialGeneration, store.headReads, store.reads)
	}
	saved := first.HarnessBinding.Clone()
	store.head.CredentialGeneration++
	request.RunID, request.AttemptID = "next-run", "next-attempt"
	next, err := binder.BindHarness(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if next.HarnessBinding.Resource.CredentialGeneration != 5 || next.HarnessBinding.EffectivePolicyDigest == first.HarnessBinding.EffectivePolicyDigest || !reflect.DeepEqual(first.HarnessBinding, saved) {
		t.Fatal("new admission mutated or reused previous generation authority")
	}
	if err := next.ValidateForScope(request); err != nil {
		t.Fatal(err)
	}
}

func TestAttachedGenerationBinderDeniesMissingRevokedForeignOrExpiredWithoutFallback(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*generationBinderStore, *ports.HarnessBindingRequest)
	}{
		{"missing", func(s *generationBinderStore, _ *ports.HarnessBindingRequest) { s.headFound = false }},
		{"backend", func(s *generationBinderStore, _ *ports.HarnessBindingRequest) {
			s.headErr = errors.New("private backend")
		}},
		{"revoked", func(s *generationBinderStore, _ *ports.HarnessBindingRequest) { s.head.Revoked = true }},
		{"owner", func(s *generationBinderStore, _ *ports.HarnessBindingRequest) { s.head.OwnerUserID = "foreign" }},
		{"worker", func(s *generationBinderStore, _ *ports.HarnessBindingRequest) { s.head.WorkerID = "foreign" }},
		{"enrollment", func(s *generationBinderStore, _ *ports.HarnessBindingRequest) { s.head.EnrollmentGeneration++ }},
		{"baseline", func(s *generationBinderStore, _ *ports.HarnessBindingRequest) { s.head.BaselineRevision++ }},
		{"zero", func(s *generationBinderStore, _ *ports.HarnessBindingRequest) { s.head.CredentialGeneration = 0 }},
		{"expired", func(_ *generationBinderStore, r *ports.HarnessBindingRequest) { r.At = r.At.Add(24 * time.Hour) }},
		{"future observation", func(_ *generationBinderStore, r *ports.HarnessBindingRequest) { r.At = r.At.Add(-time.Second) }},
		{"foreign scope", func(_ *generationBinderStore, r *ports.HarnessBindingRequest) { r.OwnerUserID = "foreign" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, store, request, _ := generationBinderFixture(t)
			binder, err := sessionlessharness.NewAttachedResourceBinder(config, store)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(store, &request)
			got, err := binder.BindHarness(t.Context(), request)
			if err == nil || !reflect.DeepEqual(got, ports.ExecutionAuthorityV2{}) {
				t.Fatalf("invalid generation admitted: err=%v", err)
			}
		})
	}
}

func TestAttachedGenerationBinderRejectsAmbiguousConfiguration(t *testing.T) {
	t.Parallel()
	config, store, _, _ := generationBinderFixture(t)
	if _, err := sessionlessharness.NewAttachedResourceBinder(config, store.attachedBinderStore); err == nil {
		t.Fatal("missing head reader accepted")
	}
	disabled := config
	disabled.Enabled = false
	if _, err := sessionlessharness.NewAttachedResourceBinder(disabled, store); err == nil {
		t.Fatal("registry silently ignored while disabled")
	}
	for _, workerCollision := range []bool{false, true} {
		fixed, _, _ := attachedBinderFixture(t)
		if workerCollision {
			fixed.Resources[0].SubscriptionConnectionID = "other-resource"
			fixed.Resources[0].Binding.Resource.ResourceID = "other-resource"
		} else {
			fixed.Resources[0].WorkerID = "other-worker"
		}
		fixed.Generations = config.Generations
		if _, err := sessionlessharness.NewAttachedResourceBinder(fixed, store); err == nil {
			t.Fatalf("fixed/dynamic collision accepted: worker=%t", workerCollision)
		}
	}
}
