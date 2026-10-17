package domain_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// These are data-validation fixtures, not installed profiles or execution grants.
func localContractBinding(t *testing.T) domain.HarnessBindingV2 {
	t.Helper()
	b := deterministicManagedAuthority(t, "tenant-1", "user-1", "run-1", "attempt-1", time.Unix(10, 0).UTC()).HarnessBinding
	b.Backend.BackendKind = domain.HarnessBackendDirectOpenAICompatibleV1
	b.Backend.ProviderContractKind = domain.ProviderContractConfiguredEndpointV1
	b.Backend.NativeProtocolVersion = domain.ConfiguredInferenceProtocolChatCompletionsV1
	b.Resource.Kind = domain.ProviderResourceLocalEndpointV1
	b.Resource.ResourceID = "local-resource"
	expires := time.Unix(100, 0).UTC()
	b.EvidenceExpiresAt = &expires
	return b
}

func TestLocalProviderCredentialTupleIsClosedAndFinite(t *testing.T) {
	t.Parallel()
	for _, bearer := range []bool{false, true} {
		t.Run(map[bool]string{false: "none", true: "bearer"}[bearer], func(t *testing.T) {
			b := localContractBinding(t)
			if bearer {
				b.Resource.CredentialMode, b.Resource.CredentialGeneration = domain.ProviderCredentialInvocationV1, 1
				b.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryDirectV1
			}
			if err := b.ValidateAt(time.Unix(20, 0).UTC()); err != nil {
				t.Fatalf("explicit local tuple: %v", err)
			}
			for name, mutate := range map[string]func(*domain.HarnessBindingV2){
				"missing evidence": func(v *domain.HarnessBindingV2) { v.EvidenceExpiresAt = nil },
				"fixture contract": func(v *domain.HarnessBindingV2) {
					v.Backend.ProviderContractKind = domain.ProviderContractCredentiallessFixtureV1
				},
				"old invocation contract": func(v *domain.HarnessBindingV2) { v.Backend.ProviderContractKind = domain.ProviderContractInvocationV1 },
				"old backend":             func(v *domain.HarnessBindingV2) { v.Backend.BackendKind = domain.HarnessBackendDirectOpenRouterV1 },
				"fixture resource":        func(v *domain.HarnessBindingV2) { v.Resource.Kind = domain.ProviderResourceCredentiallessFixtureV1 },
				"API resource":            func(v *domain.HarnessBindingV2) { v.Resource.Kind = domain.ProviderResourceAPIAccountV1 },
				"file delivery": func(v *domain.HarnessBindingV2) {
					v.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
				},
				"environment delivery": func(v *domain.HarnessBindingV2) {
					v.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryEnvironmentV1
				},
				"credential mismatch": func(v *domain.HarnessBindingV2) {
					v.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryDirectV1
					if bearer {
						v.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryNoneV1
					}
				},
				"generation mismatch": func(v *domain.HarnessBindingV2) {
					v.Resource.CredentialGeneration = 1
					if bearer {
						v.Resource.CredentialGeneration = 0
					}
				},
				"native executable": func(v *domain.HarnessBindingV2) { v.Backend.ArtifactKind = domain.HarnessArtifactExecutableV1 },
				"wire fallback":     func(v *domain.HarnessBindingV2) { v.Backend.NativeProtocolVersion = "openai-responses.v1" },
			} {
				t.Run(name, func(t *testing.T) {
					candidate := b.Clone()
					mutate(&candidate)
					if err := candidate.Validate(); err == nil {
						t.Fatal("invalid local authority accepted")
					}
				})
			}
			if err := b.ValidateAt(*b.EvidenceExpiresAt); err == nil {
				t.Fatal("real no-auth evidence cannot use fixture expiry exemption")
			}
		})
	}
}

func TestLocalProviderRouteRetainsOwnerLocalBilling(t *testing.T) {
	t.Parallel()
	b := localContractBinding(t)
	route := domain.ProviderRoutePolicyV1{Version: 1, Scope: domain.ProviderEvidenceScopeV1{TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, Backend: b.Backend, Resource: b.Resource}, State: domain.ProviderEvidenceSupportedV1, PolicyID: "local-route", Revision: 1, FallbackPolicy: domain.ProviderFallbackDenyV1, ObservedAt: time.Unix(10, 0).UTC(), ExpiresAt: *b.EvidenceExpiresAt,
		Routes: []domain.ProviderRouteV1{{BackendKind: b.Backend.BackendKind, ModelVendorID: b.ModelVendorID, ModelID: b.ModelID, TransportKind: domain.ProviderTransportDirectAPIV1, TransportProvider: "owner-operator", UpstreamProviderID: "owner-model", EndpointID: "installed-endpoint", BillingKind: domain.ProviderBillingOwnerLocalV1, BillingAuthority: b.Resource.ResourceID}}}
	if err := route.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, billing := range []domain.ProviderBillingKindV1{domain.ProviderBillingNoneV1, domain.ProviderBillingAPIAccountV1, domain.ProviderBillingRouterAccountV1} {
		t.Run(string(billing), func(t *testing.T) {
			v := route.Clone()
			v.Routes[0].BillingKind = billing
			if err := v.Validate(); err == nil {
				t.Fatal("local billing relabelled")
			}
		})
	}
	v := route.Clone()
	v.Routes[0].TransportKind = domain.ProviderTransportRouterAPIV1
	if err := v.Validate(); err == nil {
		t.Fatal("local route became router transport")
	}
}

func localSubstrateFixture(t *testing.T) domain.SubstrateBindingV1 {
	t.Helper()
	s := deterministicManagedAuthority(t, "tenant-1", "user-1", "run-1", "attempt-1", time.Unix(10, 0).UTC()).SubstrateBinding
	s.Kind, s.Region, s.ImageDigest, s.WorkloadMode = domain.SubstrateLocalDevelopmentV1, "", "", domain.SubstrateWorkloadInProcessDirectV1
	s.LocalInstallationID, s.LocalInstallationRevision = "local-installation", 1
	s.LocalHostIdentityDigest, s.LocalNetworkNamespaceIdentityDigest = strings.Repeat("a", 64), strings.Repeat("b", 64)
	return s
}

func TestLocalSubstrateHasNoInventedImageOrRegion(t *testing.T) {
	t.Parallel()
	s := localSubstrateFixture(t)
	digest, err := s.Digest()
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*domain.SubstrateBindingV1){
		"host":         func(v *domain.SubstrateBindingV1) { v.LocalHostIdentityDigest = strings.Repeat("c", 64) },
		"namespace":    func(v *domain.SubstrateBindingV1) { v.LocalNetworkNamespaceIdentityDigest = strings.Repeat("d", 64) },
		"installation": func(v *domain.SubstrateBindingV1) { v.LocalInstallationID = "other-installation" },
		"revision":     func(v *domain.SubstrateBindingV1) { v.LocalInstallationRevision++ },
		"worker":       func(v *domain.SubstrateBindingV1) { v.OuterHarnessArtifactDigest = strings.Repeat("e", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			v := s
			mutate(&v)
			got, err := v.Digest()
			if err != nil || got == digest {
				t.Fatalf("identity mutation digest=%s err=%v", got, err)
			}
		})
	}
	for name, mutate := range map[string]func(*domain.SubstrateBindingV1){
		"image":                func(v *domain.SubstrateBindingV1) { v.ImageDigest = strings.Repeat("c", 64) },
		"region":               func(v *domain.SubstrateBindingV1) { v.Region = "cloud-region" },
		"child":                func(v *domain.SubstrateBindingV1) { v.WorkloadMode = domain.SubstrateWorkloadChildProcessV1 },
		"missing host":         func(v *domain.SubstrateBindingV1) { v.LocalHostIdentityDigest = "" },
		"missing namespace":    func(v *domain.SubstrateBindingV1) { v.LocalNetworkNamespaceIdentityDigest = "" },
		"missing installation": func(v *domain.SubstrateBindingV1) { v.LocalInstallationRevision = 0 },
		"image kind":           func(v *domain.SubstrateBindingV1) { v.Kind = domain.SubstrateYandexServerlessContainerV1 },
	} {
		t.Run(name, func(t *testing.T) {
			v := s
			mutate(&v)
			if err := v.Validate(); err == nil {
				t.Fatal("false local substrate claim accepted")
			}
		})
	}
	image := deterministicManagedAuthority(t, "tenant-1", "user-1", "run-1", "attempt-1", time.Unix(10, 0).UTC()).SubstrateBinding
	image.ImageDigest = ""
	if err := image.Validate(); err == nil {
		t.Fatal("old image-backed profile accepted missing image")
	}
	if err := (domain.PreparedAllocationV1{Version: 1}).ValidateForBinding(s, localContractBinding(t).Backend); err == nil {
		t.Fatal("old image allocation attested local executable")
	}
}

func TestLocalOwnedAgentBindingRetainsExactPolicy(t *testing.T) {
	t.Parallel()
	p := agentLoopPolicyFixture()
	p.Backend.BackendKind, p.Backend.ProviderContractKind = domain.HarnessBackendDirectOpenAICompatibleV1, domain.ProviderContractConfiguredEndpointV1
	p.Backend.NativeProtocolVersion, p.Backend.CredentialDeliveryKind = domain.ConfiguredInferenceProtocolChatCompletionsV1, domain.ProviderCredentialDeliveryNoneV1
	p.Resource.Kind, p.Resource.CredentialMode, p.Resource.CredentialGeneration = domain.ProviderResourceLocalEndpointV1, domain.ProviderCredentialNoneV1, 0
	expires := p.ExpiresAt
	b := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionOwnedAgentV2, AgentLoopPolicy: &p, TenantID: p.TenantID, OwnerUserID: p.OwnerUserID, RunID: p.RunID, AttemptID: p.AttemptID, Backend: p.Backend, Resource: p.Resource, ModelVendorID: p.ModelVendorID, ModelID: p.ModelID, InputDataClass: p.InputDataClass, ProviderCatalogDigest: p.ProviderCatalogDigest, ProviderRouteDigest: p.ProviderRouteDigest, CapabilityEvidenceDigest: p.CapabilityEvidenceDigest, PrivacyPolicyDigest: p.PrivacyPolicyDigest, EffectivePolicyDigest: p.EffectivePolicyDigest, ExecutionPlacementDigest: strings.Repeat("9", 64), EvidenceExpiresAt: &expires}
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	v := b.Clone()
	v.AgentLoopPolicy.Resource.Revision++
	if err := v.Validate(); err == nil {
		t.Fatal("independently valid stale policy accepted")
	}
}
