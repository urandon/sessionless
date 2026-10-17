package domain_test

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Scripted metadata only: this builder is not an installed operator source.
func localInstallationGrantFixture(t testing.TB) ports.OwnedProfilePublicationGrantV1 {
	t.Helper()
	p, at := ownedCurrentProfileFixture(t)
	c := configuredInferenceProfileFixture(t)
	c.TenantID, c.OwnerUserID, c.Resource.OwnerUserID = p.Locator.TenantID, p.Locator.OwnerUserID, p.Locator.OwnerUserID
	c.IssuedAt, c.ExpiresAt = at, p.Window.ExpiresAt
	c.ModelWindowTokens = p.ModelWindowTokens
	c.Limits.MaxInputTokens, c.Limits.MaxOutputTokens = p.Limits.MaxInputTokensPerCall, p.Limits.MaxOutputTokensPerCall
	c.OriginalSource = p.InstalledManifest
	p.Resource = c.Resource
	p.Locator.ResourceKind, p.Locator.ResourceID = c.Resource.Kind, c.Resource.ResourceID
	p.Locator.ModelVendorID, p.Locator.ModelID = c.ModelVendorID, c.ModelID
	p.Backend.BackendKind, p.Backend.ProviderContractKind = domain.HarnessBackendDirectOpenAICompatibleV1, domain.ProviderContractConfiguredEndpointV1
	p.Backend.NativeProtocolVersion, p.Backend.CredentialDeliveryKind = c.NativeProtocolVersion, domain.ProviderCredentialDeliveryNoneV1
	p.Backend.BackendProfileDigest, _ = c.Digest()
	p.Locator.BackendProfileDigest = p.Backend.BackendProfileDigest
	p.ProviderCatalogDigest, p.ProviderRouteDigest, p.PrivacyPolicyDigest, p.EffectivePolicyDigest = c.ProviderCatalogDigest, c.ProviderRouteDigest, c.PrivacyPolicyDigest, c.EffectivePolicyDigest
	p.CapabilityEvidenceDigest, p.TokenEstimatorVersion, p.CalibrationDigest = c.CapabilityEvidence.Digest, c.TokenEstimatorVersion, c.CalibrationDigest
	p.InstallationID, p.InstallationRevision = c.LocalInstallationID, c.LocalInstallationRevision
	s := &p.Substrate
	s.Kind, s.WorkloadMode, s.ImageDigest, s.Region = domain.SubstrateLocalDevelopmentV1, domain.SubstrateWorkloadInProcessDirectV1, "", ""
	s.OuterHarnessArtifactDigest, s.LocalInstallationID, s.LocalInstallationRevision = c.WorkerExecutableDigest, c.LocalInstallationID, c.LocalInstallationRevision
	s.LocalHostIdentityDigest, s.LocalNetworkNamespaceIdentityDigest, s.ProfileEvidenceExpiresAt = c.LocalHostIdentityDigest, c.LocalNetworkNamespaceIdentityDigest, c.ExpiresAt
	p.BillingOwnerUserID = c.OwnerUserID
	p.CostCeiling.ProviderPriceState, p.CostCeiling.SubstratePriceState = domain.ProviderPriceKnownFreeV1, domain.CostEvidenceKnownV1
	p.CostCeiling.MaxProviderAmountMicrounits, p.CostCeiling.MaxSubstrateAmountMicrounits = new(uint64), new(uint64)
	p.SubstratePriceState = domain.ExternalServicePriceKnownFreeV1
	p.ProviderTermsDigest, p.SubstrateTermsDigest = c.OperatorTerms.Digest, c.OperatorTerms.Digest
	p.CostCeiling.MCPToolCost.BillingOwnerUserID, p.CostCeiling.WebSearchCost.BillingOwnerUserID = c.OwnerUserID, c.OwnerUserID
	s.AdmissionCostCeilingDigest, _ = p.CostCeiling.Digest()
	g := ports.OwnedProfilePublicationGrantV1{Profile: p, IssuedAt: at, ExpiresAt: at.Add(time.Second), Authenticator: bytes.Repeat([]byte{1}, 32), LocalInstallation: &ports.OwnedLocalInstallationIntentV1{Profile: c}}
	if err := g.Validate(); err != nil {
		t.Fatalf("scripted local publication fixture: %v", err)
	}
	return g
}

func replacementLocalInstallationFixture(t testing.TB) ports.OwnedProfilePublicationGrantV1 {
	t.Helper()
	g := localInstallationGrantFixture(t)
	old := g.Profile.Clone()
	digest, err := old.Digest()
	if err != nil {
		t.Fatal(err)
	}
	g.LocalInstallation.Predecessor = &ports.OwnedProfilePublicationReceiptV1{Profile: old, Digest: digest}
	g.LocalInstallation.RetirementMutationID = "retire-old-local"
	g.Profile.Resource.Revision++
	g.Profile.ProfileRevision++
	g.Profile.InstallationRevision++
	g.Profile.Substrate.LocalInstallationRevision++
	g.Profile.MutationID = "install-new-local"
	g.Profile.ReviewReceipt.Digest = strings.Repeat("e", 64)
	c := &g.LocalInstallation.Profile
	c.Resource, c.ProfileRevision, c.LocalInstallationRevision = g.Profile.Resource, g.Profile.ProfileRevision, g.Profile.InstallationRevision
	c.BaseURL = "http://127.0.0.1:11435/v1"
	g.Profile.Backend.BackendProfileDigest, _ = c.Digest()
	g.Profile.Locator.BackendProfileDigest = g.Profile.Backend.BackendProfileDigest
	if err := g.Validate(); err != nil {
		t.Fatalf("scripted local replacement fixture: %v", err)
	}
	return g
}

func TestLocalInstallationPublicationExactConfiguration(t *testing.T) {
	t.Parallel()
	base := localInstallationGrantFixture(t)
	for name, mutate := range map[string]func(*ports.OwnedProfilePublicationGrantV1){
		"missing_intent": func(g *ports.OwnedProfilePublicationGrantV1) { g.LocalInstallation = nil },
		"fixture_tuple": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.Profile.Resource.Kind = domain.ProviderResourceCredentiallessFixtureV1
		},
		"endpoint": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Profile.BaseURL = "http://127.0.0.1:11435/v1"
		},
		"source": func(g *ports.OwnedProfilePublicationGrantV1) { g.Profile.InstalledManifest.Ref = "other-source" },
		"terms":  func(g *ports.OwnedProfilePublicationGrantV1) { g.Profile.ProviderTermsDigest = strings.Repeat("e", 64) },
		"host": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.Profile.Substrate.LocalHostIdentityDigest = strings.Repeat("e", 64)
		},
		"worker": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.Profile.Substrate.OuterHarnessArtifactDigest = strings.Repeat("e", 64)
		},
		"capability": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.Profile.CapabilityEvidenceDigest = strings.Repeat("e", 64)
		},
		"model": func(g *ports.OwnedProfilePublicationGrantV1) { g.Profile.Locator.ModelID = "other-model" },
		"expiry": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Profile.ExpiresAt = g.LocalInstallation.Profile.ExpiresAt.Add(-time.Second)
		},
		"grant_before_source":   func(g *ports.OwnedProfilePublicationGrantV1) { g.IssuedAt = g.IssuedAt.Add(-time.Second) },
		"fresh_revision":        func(g *ports.OwnedProfilePublicationGrantV1) { g.Profile.Resource.Revision = 2 },
		"unexpected_retirement": func(g *ports.OwnedProfilePublicationGrantV1) { g.LocalInstallation.RetirementMutationID = "retire" },
		"unknown_tools": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Profile.Capabilities.ToolCalls = domain.ProviderEvidenceUnknownV1
		},
		"context_ceiling": func(g *ports.OwnedProfilePublicationGrantV1) { g.LocalInstallation.Profile.Limits.RequestBytes /= 2 },
	} {
		t.Run(name, func(t *testing.T) {
			g := base.Clone()
			mutate(&g)
			if err := g.Validate(); err == nil {
				t.Error("mismatched local publication accepted")
			}
		})
	}
	if err := base.LocalInstallation.Profile.ValidateForOwnedProfile(base.Profile); err != nil {
		t.Fatalf("exact configured head: %v", err)
	}
	bearer := base.Clone()
	bearer.LocalInstallation.Profile.Auth = domain.ConfiguredInferenceAuthBearerV1
	bearer.LocalInstallation.Profile.Resource.CredentialMode = domain.ProviderCredentialInvocationV1
	bearer.LocalInstallation.Profile.Resource.CredentialGeneration = 1
	bearer.Profile.Resource = bearer.LocalInstallation.Profile.Resource
	bearer.Profile.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryDirectV1
	bearer.Profile.Backend.BackendProfileDigest, _ = bearer.LocalInstallation.Profile.Digest()
	bearer.Profile.Locator.BackendProfileDigest = bearer.Profile.Backend.BackendProfileDigest
	if err := bearer.Validate(); err != nil {
		t.Fatalf("explicit positive-generation Bearer local tuple: %v", err)
	}
	// Local no-invoice terms do not make independent MCP/search allocations free.
	if *base.Profile.CostCeiling.MCPToolCost.MaxAmountMicrounits == 0 || *base.Profile.CostCeiling.WebSearchCost.MaxAmountMicrounits == 0 {
		t.Fatal("test lost separately funded external service coverage")
	}
}

func TestLocalInstallationCorrelationsBeyondMatchingConfigDigest(t *testing.T) {
	t.Parallel()
	base := localInstallationGrantFixture(t)
	for name, mutate := range map[string]func(*ports.OwnedProfilePublicationGrantV1){
		"tool_support": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Profile.Capabilities.ToolCalls = domain.ProviderEvidenceUnknownV1
		},
		"request_ceiling": func(g *ports.OwnedProfilePublicationGrantV1) { g.LocalInstallation.Profile.Limits.RequestBytes /= 2 },
		"original_window": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Profile.ExpiresAt = g.LocalInstallation.Profile.ExpiresAt.Add(-time.Second)
		},
		"original_source": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Profile.OriginalSource.Ref = "different-actual-source"
		},
		"operator_terms": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Profile.OperatorTerms.Digest = strings.Repeat("e", 64)
		},
		"host_placement": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Profile.LocalHostIdentityDigest = strings.Repeat("e", 64)
		},
	} {
		t.Run(name, func(t *testing.T) {
			g := base.Clone()
			mutate(&g)
			if err := g.LocalInstallation.Profile.Validate(); err != nil {
				t.Fatalf("mutation must remain a valid configured declaration: %v", err)
			}
			g.Profile.Backend.BackendProfileDigest, _ = g.LocalInstallation.Profile.Digest()
			g.Profile.Locator.BackendProfileDigest = g.Profile.Backend.BackendProfileDigest
			if err := g.Profile.ValidateInitial(); err != nil {
				t.Fatalf("mutation must remain a valid structural head: %v", err)
			}
			if err := g.LocalInstallation.Profile.ValidateForOwnedProfile(g.Profile); err == nil {
				t.Error("matching digest bypassed exact configuration correlation")
			}
		})
	}
}

func TestLocalInstallationReplacementOriginalReceiptAndRevisions(t *testing.T) {
	t.Parallel()
	base := replacementLocalInstallationFixture(t)
	for name, mutate := range map[string]func(*ports.OwnedProfilePublicationGrantV1){
		"receipt_digest": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Predecessor.Digest = strings.Repeat("e", 64)
		},
		"same_locator": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Predecessor.Profile.Locator = g.Profile.Locator
		},
		"foreign_tenant": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Predecessor.Profile.Locator.TenantID = "foreign"
		},
		"revoked_predecessor": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Predecessor.Profile.State = domain.OwnedAgentCurrentRevokedV1
		},
		"resource_skip":     func(g *ports.OwnedProfilePublicationGrantV1) { g.Profile.Resource.Revision++ },
		"profile_skip":      func(g *ports.OwnedProfilePublicationGrantV1) { g.Profile.ProfileRevision++ },
		"installation_skip": func(g *ports.OwnedProfilePublicationGrantV1) { g.Profile.InstallationRevision++ },
		"generation_overflow": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Predecessor.Profile.RevocationGeneration = math.MaxUint64
		},
		"missing_retirement": func(g *ports.OwnedProfilePublicationGrantV1) { g.LocalInstallation.RetirementMutationID = "" },
		"same_retirement": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.RetirementMutationID = g.Profile.MutationID
		},
		"same_review": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.Profile.ReviewReceipt = g.LocalInstallation.Predecessor.Profile.ReviewReceipt
		},
		"same_locator_generation": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.ExpectedGeneration = 1
			g.ExpectedDigest = g.LocalInstallation.Predecessor.Digest
			g.Profile.RevocationGeneration = 2
		},
	} {
		t.Run(name, func(t *testing.T) {
			g := base.Clone()
			mutate(&g)
			if err := g.Validate(); err == nil {
				t.Error("invalid two-head replacement accepted")
			}
		})
	}
}

func TestLocalInstallationIntentSealsCloneAndRetirement(t *testing.T) {
	t.Parallel()
	base := replacementLocalInstallationFixture(t)
	raw, err := base.IntentBytes()
	if err != nil {
		t.Fatal(err)
	}
	clone := base.Clone()
	clone.LocalInstallation.Profile.ResponseModelIDs[0] = "clone-model"
	clone.LocalInstallation.Profile.AllowedDataClasses[0] = domain.ProviderDataPrivateV1
	clone.LocalInstallation.Predecessor.Profile.InputModalities[0] = domain.AgentInputImageV1
	*clone.LocalInstallation.Predecessor.Profile.CostCeiling.MCPToolCost.MaxAmountMicrounits++
	clone.Authenticator[0]++
	again, err := base.IntentBytes()
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatalf("clone mutated original sealed intent: %v", err)
	}
	for name, mutate := range map[string]func(*ports.OwnedProfilePublicationGrantV1){
		"retirement": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.RetirementMutationID = "different-retirement"
		},
		"receipt_replay": func(g *ports.OwnedProfilePublicationGrantV1) { g.LocalInstallation.Predecessor.Replayed = true },
		"server": func(g *ports.OwnedProfilePublicationGrantV1) {
			g.LocalInstallation.Profile.ServerVersion = "other-version"
			g.Profile.Backend.BackendProfileDigest, _ = g.LocalInstallation.Profile.Digest()
			g.Profile.Locator.BackendProfileDigest = g.Profile.Backend.BackendProfileDigest
		},
	} {
		t.Run(name, func(t *testing.T) {
			g := base.Clone()
			mutate(&g)
			changed, err := g.IntentBytes()
			if err != nil || bytes.Equal(raw, changed) {
				t.Fatalf("valid %s mutation not sealed: %v", name, err)
			}
		})
	}
}

func TestFixturePublicationIntentBytesRemainUnchanged(t *testing.T) {
	t.Parallel()
	p, at := ownedCurrentProfileFixture(t)
	g := ports.OwnedProfilePublicationGrantV1{Profile: p, IssuedAt: at, ExpiresAt: at.Add(time.Second), Authenticator: bytes.Repeat([]byte{1}, 32)}
	raw, err := g.IntentBytes()
	if err != nil {
		t.Fatal(err)
	}
	profile, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(struct {
		Kind               string          `json:"kind"`
		Profile            json.RawMessage `json:"profile"`
		ExpectedGeneration uint64          `json:"expected_generation"`
		ExpectedDigest     string          `json:"expected_digest"`
		IssuedAt           time.Time       `json:"issued_at"`
		ExpiresAt          time.Time       `json:"expires_at"`
	}{"sessionless.owned-profile-publication.v1", profile, 0, "", at, at.Add(time.Second)})
	if err != nil || !bytes.Equal(raw, want) {
		t.Fatalf("fixture intent changed: err=%v", err)
	}
}
