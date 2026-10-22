//go:build darwin || linux

package codexexec

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// This is only a metadata fixture. Neither its structural validity nor the
// local helper's verdict provides a genuine server/Session admission.
func nativePolicyFixtureV2(t *testing.T) (*nativeFrozenProfileV2, domain.ProviderEvidenceBundleV1, domain.HarnessBindingV2, time.Time) {
	t.Helper()
	p, err := freezeNativeProfileV2(nativeProfileFixtureV2(t))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	observed, expires := now.Add(-time.Minute), now.Add(time.Hour)
	binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2,
		TenantID: "tenant-native-policy", OwnerUserID: "owner-native-policy", RunID: "run-native-policy", AttemptID: "attempt-native-policy", Backend: p.descriptor,
		Resource:      domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-native-policy", OwnerUserID: "owner-native-policy", Revision: 7, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 3},
		ModelVendorID: ModelVendorIDV1, ModelID: p.profile.Backend.Model, InputDataClass: domain.ProviderDataPrivateV1, ExecutionPlacementDigest: strings.Repeat("e", 64)}
	scope := domain.ProviderEvidenceScopeV1{TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, Resource: binding.Resource, Backend: binding.Backend}
	bundle := domain.ProviderEvidenceBundleV1{
		Catalog:    domain.ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "native-test-catalog", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, CatalogDigest: strings.Repeat("a", 64)}}, ObservedAt: observed, ExpiresAt: expires},
		Route:      domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, FallbackPolicy: domain.ProviderFallbackDenyV1, ObservedAt: observed, ExpiresAt: expires},
		Capability: domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, ToolCalling: domain.ProviderEvidenceUnsupportedV1, StructuredOutput: domain.ProviderEvidenceUnknownV1, ObservedAt: observed, ExpiresAt: expires},
		Privacy:    domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, TrainingUse: domain.ProviderEvidenceUnknownV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, PolicyRevision: "native-test-privacy", ObservedAt: observed, ExpiresAt: expires},
		Price:      domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, ObservedAt: observed, ExpiresAt: expires},
		Policy:     domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: "OAI-SUB-NATIVE-ENVELOPE-V2", Revision: 1, DecisionOwner: "sessionless-policy", EvidenceSource: "reviewed-native-envelope-v2", Verdict: domain.ProviderPolicyConditionalV1, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, ObservedAt: observed, ExpiresAt: expires},
	}
	nativePolicyRebindV2(t, &bundle, &binding)
	if err := bundle.ValidateForBinding(binding); err != nil {
		t.Fatalf("baseline metadata bundle binding: %v", err)
	}
	return p, bundle, binding, now
}

// Recompute only explicit metadata references. This is never an acceptance
// helper: each test separately asserts structural and native semantic outcomes.
func nativePolicyRebindV2(t *testing.T, b *domain.ProviderEvidenceBundleV1, binding *domain.HarnessBindingV2) {
	t.Helper()
	digest := func(value interface {
		Digest() (domain.ProviderEvidenceDigestV1, error)
	}) string {
		v, err := value.Digest()
		if err != nil {
			t.Fatalf("test metadata digest: %v", err)
		}
		return string(v)
	}
	binding.ProviderCatalogDigest = digest(b.Catalog)
	binding.ProviderRouteDigest = digest(b.Route)
	binding.CapabilityEvidenceDigest = digest(b.Capability)
	binding.PrivacyPolicyDigest = digest(b.Privacy)
	b.Policy.CapabilityEvidenceDigest = binding.CapabilityEvidenceDigest
	b.Policy.PrivacyEvidenceDigest = binding.PrivacyPolicyDigest
	b.Policy.PriceObservationDigest = digest(b.Price)
	b.Policy.RoutePolicyDigest = binding.ProviderRouteDigest
	binding.EffectivePolicyDigest = digest(b.Policy)
	expires := b.Catalog.ExpiresAt
	for _, candidate := range []time.Time{b.Route.ExpiresAt, b.Capability.ExpiresAt, b.Privacy.ExpiresAt, b.Price.ExpiresAt, b.Policy.ExpiresAt} {
		if candidate.Before(expires) {
			expires = candidate
		}
	}
	binding.EvidenceExpiresAt = &expires
}

func TestNativePolicyV2ClosedSemanticsAndExactBundle(t *testing.T) {
	const semantics = "native-envelope-v2/r1;owner-local-openai-chatgpt-file;single-consumed-invocation;no-transport-or-native-replay;tools-denied;original-text-json-png-jpeg;context-fit/native-transformed-bytes/native-allocations/internal-sampling-count/actual-route/token-usage/price-cost=unknown;route-unknown-fallback-deny;price-unknown;full-evidence-scope-digests-min-expiry;unchanged-privacy-and-data-class;original-session-current;limits=min-profile-job-lease-caller-resource;no-cloud-custody"
	if NativeEnvelopePolicyIDV2 != "OAI-SUB-NATIVE-ENVELOPE-V2" || NativeEnvelopePolicyRevisionV2 != 1 || nativePolicySemanticsV2 != semantics {
		t.Fatal("closed native policy identity/semantics changed without an explicit successor")
	}
	p, b, binding, now := nativePolicyFixtureV2(t)
	if err := validateNativeEnvelopeEvidenceV2(p, b, binding, now); err != nil {
		t.Fatalf("matching local metadata denied: %v", err)
	}
	changed := b.Clone()
	changed.Catalog.CatalogRevision = "changed-catalog"
	if validateNativeBundleShapeV2(changed) != nil {
		t.Fatal("changed catalog is not structurally valid")
	}
	if changed.ValidateForBinding(binding) == nil || validateNativeEnvelopeEvidenceV2(p, changed, binding, now) == nil {
		t.Fatal("changed valid catalog bypassed original sealed binding")
	}
	rebound := binding.Clone()
	nativePolicyRebindV2(t, &changed, &rebound)
	if changed.ValidateForBinding(rebound) != nil || validateNativeEnvelopeEvidenceV2(p, changed, rebound, now) != nil {
		t.Fatal("correctly rebound unchanged native semantics denied")
	}
	if rebound.ProviderCatalogDigest == binding.ProviderCatalogDigest {
		t.Fatal("changed catalog did not change binding identity")
	}
}

func TestNativePolicyV2RehashedForeignSemanticsRemainDenied(t *testing.T) {
	cases := []struct {
		name   string
		change func(*domain.ProviderEvidenceBundleV1)
	}{
		{"different-policy", func(b *domain.ProviderEvidenceBundleV1) { b.Policy.PolicyID = "another-conditional-policy" }},
		{"different-revision", func(b *domain.ProviderEvidenceBundleV1) { b.Policy.Revision++ }},
		{"different-decision-owner", func(b *domain.ProviderEvidenceBundleV1) { b.Policy.DecisionOwner = "caller-policy" }},
		{"different-source", func(b *domain.ProviderEvidenceBundleV1) { b.Policy.EvidenceSource = "unreviewed-native-envelope" }},
		{"go-is-not-native-conditional", func(b *domain.ProviderEvidenceBundleV1) { b.Policy.Verdict = domain.ProviderPolicyGoV1 }},
		{"no-go", func(b *domain.ProviderEvidenceBundleV1) { b.Policy.Verdict = domain.ProviderPolicyNoGoV1 }},
		{"unknown-policy", func(b *domain.ProviderEvidenceBundleV1) { b.Policy.Verdict = domain.ProviderPolicyUnknownV1 }},
		{"known-price", func(b *domain.ProviderEvidenceBundleV1) {
			b.Price.State = domain.ProviderEvidenceSupportedV1
			b.Price.Currency = "USD"
		}},
		{"tools-supported", func(b *domain.ProviderEvidenceBundleV1) {
			b.Capability.ToolCalling = domain.ProviderEvidenceSupportedV1
		}},
		{"tools-unknown", func(b *domain.ProviderEvidenceBundleV1) { b.Capability.ToolCalling = domain.ProviderEvidenceUnknownV1 }},
		{"privacy-does-not-allow-private", func(b *domain.ProviderEvidenceBundleV1) {
			b.Privacy.AllowedDataClasses = []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}
		}},
		{"known-route-cannot-be-erased", func(b *domain.ProviderEvidenceBundleV1) {
			b.Route.State = domain.ProviderEvidenceSupportedV1
			b.Route.PolicyID = "exact-route"
			b.Route.Revision = 1
			b.Route.Routes = []domain.ProviderRouteV1{{BackendKind: b.Route.Scope.Backend.BackendKind, ModelVendorID: ModelVendorIDV1, ModelID: b.Capability.ModelID, TransportKind: domain.ProviderTransportLocalCLIV1, TransportProvider: "openai", UpstreamProviderID: "openai", EndpointID: "chatgpt", BillingKind: domain.ProviderBillingSubscriptionV1, BillingAuthority: b.Route.Scope.Resource.ResourceID}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, b, binding, now := nativePolicyFixtureV2(t)
			tc.change(&b)
			nativePolicyRebindV2(t, &b, &binding)
			if err := b.ValidateForBinding(binding); err != nil {
				t.Fatalf("negative must reach semantic guard with structurally rebound evidence: %v", err)
			}
			if validateNativeEnvelopeEvidenceV2(p, b, binding, now) == nil {
				t.Fatal("structurally valid/rehashed non-native semantics accepted")
			}
		})
	}
}

func TestNativePolicyV2ScopeModelResourceAndDigestMismatch(t *testing.T) {
	cases := []struct {
		name   string
		change func(*domain.ProviderEvidenceBundleV1, *domain.HarnessBindingV2)
	}{
		{"catalog-tenant", func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) {
			b.Catalog.Scope.TenantID = "other-tenant"
		}},
		{"route-owner", func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) {
			b.Route.Scope.OwnerUserID = "other-owner"
			b.Route.Scope.Resource.OwnerUserID = "other-owner"
		}},
		{"capability-resource", func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) {
			b.Capability.Scope.Resource.ResourceID = "other-resource"
		}},
		{"privacy-resource-revision", func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) {
			b.Privacy.Scope.Resource.Revision++
		}},
		{"price-credential-generation", func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) {
			b.Price.Scope.Resource.CredentialGeneration++
		}},
		{"policy-backend", func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) {
			b.Policy.Scope.Backend.BackendProfileDigest = strings.Repeat("f", 64)
		}},
		{"catalog-model", func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) {
			b.Catalog.Models[0].ModelID = "other-model"
		}},
		{"capability-model", func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) {
			b.Capability.ModelID = "other-model"
		}},
		{"privacy-vendor", func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) {
			b.Privacy.ModelVendorID = "other-vendor"
		}},
		{"price-model", func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) { b.Price.ModelID = "other-model" }},
		{"binding-generation", func(_ *domain.ProviderEvidenceBundleV1, b *domain.HarnessBindingV2) {
			b.Resource.CredentialGeneration++
		}},
		{"binding-model", func(_ *domain.ProviderEvidenceBundleV1, b *domain.HarnessBindingV2) { b.ModelID = "other-model" }},
		{"binding-backend", func(_ *domain.ProviderEvidenceBundleV1, b *domain.HarnessBindingV2) {
			b.Backend.BackendProfileDigest = strings.Repeat("f", 64)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, b, binding, now := nativePolicyFixtureV2(t)
			tc.change(&b, &binding)
			if err := validateNativeBundleShapeV2(b); err != nil {
				t.Fatalf("negative individual metadata shape invalid: %v", err)
			}
			if b.ValidateForBinding(binding) == nil || validateNativeEnvelopeEvidenceV2(p, b, binding, now) == nil {
				t.Fatal("scope/model/resource mismatch accepted")
			}
		})
	}
	for _, name := range []string{"catalog", "route", "capability", "privacy", "policy"} {
		t.Run("digest-"+name, func(t *testing.T) {
			p, b, binding, now := nativePolicyFixtureV2(t)
			value := strings.Repeat("f", 64)
			switch name {
			case "catalog":
				binding.ProviderCatalogDigest = value
			case "route":
				binding.ProviderRouteDigest = value
			case "capability":
				binding.CapabilityEvidenceDigest = value
			case "privacy":
				binding.PrivacyPolicyDigest = value
			case "policy":
				binding.EffectivePolicyDigest = value
			}
			if binding.Validate() != nil {
				t.Fatal("digest negative is not syntactically valid")
			}
			if validateNativeEnvelopeEvidenceV2(p, b, binding, now) == nil {
				t.Fatal("caller-selected valid digest replaced exact evidence")
			}
		})
	}
}

func nativePolicyWindowsV2(b *domain.ProviderEvidenceBundleV1) [][2]*time.Time {
	return [][2]*time.Time{{&b.Catalog.ObservedAt, &b.Catalog.ExpiresAt}, {&b.Route.ObservedAt, &b.Route.ExpiresAt}, {&b.Capability.ObservedAt, &b.Capability.ExpiresAt}, {&b.Privacy.ObservedAt, &b.Privacy.ExpiresAt}, {&b.Price.ObservedAt, &b.Price.ExpiresAt}, {&b.Policy.ObservedAt, &b.Policy.ExpiresAt}}
}

func TestNativePolicyV2ExactMinimumExpiryAndNoFutureObservation(t *testing.T) {
	for i, name := range []string{"catalog", "route", "capability", "privacy", "price", "policy"} {
		t.Run(name, func(t *testing.T) {
			p, b, binding, now := nativePolicyFixtureV2(t)
			minimum := now.Add(time.Minute)
			*nativePolicyWindowsV2(&b)[i][1] = minimum
			nativePolicyRebindV2(t, &b, &binding)
			if !binding.EvidenceExpiresAt.Equal(minimum) || b.ValidateForBinding(binding) != nil || validateNativeEnvelopeEvidenceV2(p, b, binding, now) != nil {
				t.Fatal("exact minimum evidence window denied")
			}
			for _, at := range []time.Time{minimum, minimum.Add(time.Nanosecond)} {
				if validateNativeEnvelopeEvidenceV2(p, b, binding, at) == nil {
					t.Fatal("expired/exact-boundary evidence accepted")
				}
			}
			for _, delta := range []time.Duration{-time.Nanosecond, time.Nanosecond} {
				wrong := binding.Clone()
				expiry := minimum.Add(delta)
				wrong.EvidenceExpiresAt = &expiry
				if validateNativeEnvelopeEvidenceV2(p, b, wrong, now) == nil {
					t.Fatal("nonminimum sealed expiry accepted")
				}
			}
			future := now.Add(time.Second)
			*nativePolicyWindowsV2(&b)[i][0] = future
			nativePolicyRebindV2(t, &b, &binding)
			if b.ValidateForBinding(binding) != nil {
				t.Fatal("future-observation negative not structurally valid")
			}
			if validateNativeEnvelopeEvidenceV2(p, b, binding, now) == nil {
				t.Fatal("future evidence observation accepted")
			}
		})
	}
}

func TestNativePolicyV2ContextAndClosedInputShape(t *testing.T) {
	for _, size := range []uint64{127999, 128000, 128001} {
		p, b, binding, now := nativePolicyFixtureV2(t)
		b.Capability.MaxContextTokens = domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceSupportedV1, Value: size}
		nativePolicyRebindV2(t, &b, &binding)
		if b.ValidateForBinding(binding) != nil {
			t.Fatal("context metadata invalid")
		}
		got := validateNativeEnvelopeEvidenceV2(p, b, binding, now)
		wantPass := size >= p.profile.DeclaredContextWindow
		if (got == nil) != wantPass {
			t.Fatalf("model maximum=%d declared=%d error=%v", size, p.profile.DeclaredContextWindow, got)
		}
	}
	p, b, binding, now := nativePolicyFixtureV2(t)
	for name, change := range map[string]func(*domain.ProviderEvidenceBundleV1, *domain.HarnessBindingV2){
		"fallback-allow": func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) { b.Route.FallbackPolicy = "allow" },
		"unknown-route-with-entries": func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) {
			b.Route.Routes = []domain.ProviderRouteV1{{EndpointID: "invented"}}
		},
		"policy-private-excluded": func(b *domain.ProviderEvidenceBundleV1, _ *domain.HarnessBindingV2) {
			b.Policy.AllowedDataClasses = []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}
		},
		"missing-expiry": func(_ *domain.ProviderEvidenceBundleV1, b *domain.HarnessBindingV2) { b.EvidenceExpiresAt = nil },
		"non-single-turn": func(_ *domain.ProviderEvidenceBundleV1, b *domain.HarnessBindingV2) {
			b.Profile = domain.HarnessExecutionOwnedAgentV2
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate, sealed := b.Clone(), binding.Clone()
			change(&candidate, &sealed)
			if validateNativeEnvelopeEvidenceV2(p, candidate, sealed, now) == nil {
				t.Fatal("invalid closed input accepted")
			}
		})
	}
	if validateNativeEnvelopeEvidenceV2(nil, b, binding, now) == nil || validateNativeEnvelopeEvidenceV2(p, b, binding, time.Time{}) == nil {
		t.Fatal("missing profile/time accepted")
	}
}

type nativeCredentialMetadataReaderV2 struct {
	binding      domain.CredentialBinding
	found        bool
	err          error
	calls        int
	tenant       domain.TenantID
	connection   domain.SubscriptionConnectionID
	beforeReturn func()
}

func (r *nativeCredentialMetadataReaderV2) LoadCredentialBinding(_ context.Context, tenant domain.TenantID, connection domain.SubscriptionConnectionID) (domain.CredentialBinding, bool, error) {
	r.calls++
	r.tenant, r.connection = tenant, connection
	if r.beforeReturn != nil {
		r.beforeReturn()
	}
	return r.binding, r.found, r.err
}

func nativeCredentialFixtureV2(t *testing.T) (AuthorityV1, domain.CredentialBinding, time.Time) {
	t.Helper()
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	fence, err := domain.NewAttachedWorkerFenceTokenV1("tenant-native-policy", "owner-native-policy", "worker-native-policy", "run-native-policy", "attempt-native-policy", "lease-native-policy", 9)
	if err != nil {
		t.Fatal(err)
	}
	a := AuthorityV1{Version: 1, TenantID: "tenant-native-policy", OwnerUserID: "owner-native-policy", WorkerID: "worker-native-policy", ConnectionID: "connection-native-policy", EnrollmentGeneration: 3, ConnectionGeneration: 4, RunID: "run-native-policy", AttemptID: "attempt-native-policy", ReservationID: "reservation-native-policy", LeaseID: "lease-native-policy", LeaseGeneration: 9, FenceToken: fence, LeaseExpiresAt: now.Add(time.Hour), ContextDigest: domain.AttachedWorkerContextDigest(strings.Repeat("a", 64)), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("b", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("c", 64)), ProviderResource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-native-policy", OwnerUserID: "owner-native-policy", Revision: 7, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 3}}
	ref, err := domain.NewCredentialSecretRef("inert-metadata-locator-not-secret-bytes")
	if err != nil {
		t.Fatal(err)
	}
	b := domain.CredentialBinding{Version: 1, TenantID: a.TenantID, SubscriptionConnectionID: domain.SubscriptionConnectionID(a.ProviderResource.ResourceID), OwnerUserID: a.OwnerUserID, Provider: "openai", AuthMode: "chatgpt", SecretRef: ref, SecretFingerprint: domain.CredentialFingerprint(strings.Repeat("a", 64)), Entitlement: domain.EntitlementActive, Generation: 3, UpdatedAt: now.Add(-time.Minute)}
	if a.Validate() != nil || b.Validate() != nil {
		t.Fatal("credential metadata fixture invalid")
	}
	return a, b, now
}

func TestNativePolicyV2CredentialMetadataOnlyExactCurrentOAuth(t *testing.T) {
	a, b, now := nativeCredentialFixtureV2(t)
	reader := &nativeCredentialMetadataReaderV2{binding: b, found: true}
	if err := nativeCredentialCurrentV2(context.Background(), reader, a, now); err != nil {
		t.Fatalf("current canonical metadata denied: %v", err)
	}
	if reader.calls != 1 || reader.tenant != a.TenantID || string(reader.connection) != a.ProviderResource.ResourceID {
		t.Fatalf("wrong canonical metadata lookup: calls%d tenant%s connection%s", reader.calls, reader.tenant, reader.connection)
	}
	// This fake implements only metadata loading; it has no secret-store API.
	cases := []struct {
		name   string
		change func(*domain.CredentialBinding)
	}{
		{"tenant", func(b *domain.CredentialBinding) { b.TenantID = "other-tenant" }},
		{"owner", func(b *domain.CredentialBinding) { b.OwnerUserID = "other-owner" }},
		{"connection", func(b *domain.CredentialBinding) { b.SubscriptionConnectionID = "other-subscription" }},
		{"generation", func(b *domain.CredentialBinding) { b.Generation++ }},
		{"provider", func(b *domain.CredentialBinding) { b.Provider = "openrouter" }},
		{"api-mode", func(b *domain.CredentialBinding) { b.AuthMode = "api_key" }},
		{"future-updated", func(b *domain.CredentialBinding) { b.UpdatedAt = now.Add(time.Nanosecond) }},
		{"revoked", func(b *domain.CredentialBinding) {
			b.Revoked = true
			b.SecretRef = domain.CredentialSecretRef{}
			b.SecretFingerprint = ""
			b.Entitlement = domain.EntitlementDisconnected
		}},
		{"inactive", func(b *domain.CredentialBinding) { b.Entitlement = domain.EntitlementInactive }},
		{"unknown", func(b *domain.CredentialBinding) { b.Entitlement = domain.EntitlementUnknown }},
		{"reauth-required", func(b *domain.CredentialBinding) { b.Entitlement = domain.EntitlementReauthRequired }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := b
			tc.change(&changed)
			if err := changed.Validate(); err != nil {
				t.Fatalf("negative must be valid canonical metadata: %v", err)
			}
			r := &nativeCredentialMetadataReaderV2{binding: changed, found: true}
			if !errors.Is(nativeCredentialCurrentV2(context.Background(), r, a, now), ErrAuthorityUnavailable) || r.calls != 1 {
				t.Fatal("foreign/stale/inactive metadata accepted")
			}
		})
	}
	for name, r := range map[string]*nativeCredentialMetadataReaderV2{"not-found": {binding: b}, "read-error": {binding: b, found: true, err: errors.New("controlled metadata failure")}, "invalid-metadata": {binding: domain.CredentialBinding{}, found: true}} {
		t.Run(name, func(t *testing.T) {
			if nativeCredentialCurrentV2(context.Background(), r, a, now) == nil {
				t.Fatal("missing/error/invalid metadata accepted")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &nativeCredentialMetadataReaderV2{binding: b, found: true}
	if nativeCredentialCurrentV2(ctx, r, a, now) == nil || r.calls != 0 {
		t.Fatal("canceled lookup reached canonical metadata")
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	r = &nativeCredentialMetadataReaderV2{binding: b, found: true, beforeReturn: cancel}
	if nativeCredentialCurrentV2(ctx, r, a, now) == nil || r.calls != 1 {
		t.Fatal("cancellation during metadata read authorized current credentials")
	}
	for name, call := range map[string]func() error{"nil-context": func() error { return nativeCredentialCurrentV2(nil, reader, a, now) }, "nil-reader": func() error { return nativeCredentialCurrentV2(context.Background(), nil, a, now) }, "zero-time": func() error { return nativeCredentialCurrentV2(context.Background(), reader, a, time.Time{}) }, "invalid-authority": func() error { return nativeCredentialCurrentV2(context.Background(), reader, AuthorityV1{}, now) }} {
		t.Run(name, func(t *testing.T) {
			if call() == nil {
				t.Fatal("missing credential metadata precondition accepted")
			}
		})
	}
}
