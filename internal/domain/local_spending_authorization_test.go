package domain_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Scripted declaration construction, not proof of operator source approval.
func localSpendingFixture(t testing.TB, p domain.OwnedAgentProfileCurrentV1, kind domain.AttemptSpendingComponentKindV1) domain.AttemptSpendingAuthorizationCurrentV1 {
	t.Helper()
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	v := domain.AttemptSpendingAuthorizationCurrentV1{
		Version: 1, Locator: domain.AttemptSpendingAuthorizationLocatorV1{TenantID: p.Locator.TenantID, AttemptID: "local-attempt", ComponentKind: kind, BillingOwnerUserID: p.BillingOwnerUserID}, RunID: "local-run", SessionID: "local-session", OwnerUserID: p.Locator.OwnerUserID,
		SourceKind: domain.AttemptSpendingOwnerLocalSourceV1, SourceInstallationID: p.InstallationID, SourceInstallationRevision: p.InstallationRevision, SourceProfile: domain.OwnedAgentProfileReferenceV1{Locator: p.Locator, RevocationGeneration: p.RevocationGeneration, Digest: digest},
		ResourceID: p.Resource.ResourceID, ResourceRevision: p.Resource.Revision, ResourceGeneration: p.Resource.CredentialGeneration, ProfileID: string(p.Locator.Profile), ProfileRevision: p.ProfileRevision, ScopedGrantDigest: p.EffectivePolicyDigest, Currency: p.CostCeiling.Currency, PriceRevision: p.CostCeiling.PriceRevision, PriceEvidenceDigest: p.ProviderPriceDigest, TermsEvidenceDigest: p.ProviderTermsDigest, PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: new(uint64), RevocationGeneration: 1, State: domain.OwnedAgentCurrentActiveV1, Window: p.Window, UpdatedAt: p.UpdatedAt, MutationID: "publish-local-" + string(kind),
	}
	switch kind {
	case domain.AttemptSpendingSubstrateV1:
		v.ResourceID, v.ResourceRevision, v.ResourceGeneration = p.Substrate.ProfileID, p.Substrate.ProfileRevision, p.RevocationGeneration
		v.ProfileID, v.ProfileRevision, v.ScopedGrantDigest, v.PriceEvidenceDigest, v.TermsEvidenceDigest = p.Substrate.ProfileID, p.Substrate.ProfileRevision, p.Substrate.ProfileDigest, p.SubstratePriceDigest, p.SubstrateTermsDigest
	case domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1:
		service := p.CostCeiling.MCPToolCost
		if kind == domain.AttemptSpendingWebSearchV1 {
			service = p.CostCeiling.WebSearchCost
		}
		v.ResourceID, v.ResourceRevision, v.ResourceGeneration = service.ResourceID, service.ResourceRevision, service.RevocationGeneration
		v.ProfileID, v.ProfileRevision, v.ScopedGrantDigest, v.PriceRevision, v.PriceEvidenceDigest, v.TermsEvidenceDigest, v.PriceState = service.ProfileID, service.ProfileRevision, service.ScopedGrantDigest, service.PriceRevision, service.PriceEvidenceDigest, service.TermsEvidenceDigest, service.PriceState
		amount := *service.MaxAmountMicrounits
		v.MaxAmountMicrounits = &amount
	}
	bindOwnedSpendingSource(t, &v)
	if err := v.ValidateInitial(); err != nil {
		t.Fatalf("scripted local %s initial: %v", kind, err)
	}
	if err := v.ValidateAgainstProfile(p, p.Window.PublishedAt); err != nil {
		t.Fatalf("scripted local %s profile correlation: %v", kind, err)
	}
	return v
}

func localSpendingProfileFixture(t testing.TB, bearer bool) domain.OwnedAgentProfileCurrentV1 {
	t.Helper()
	g := localInstallationGrantFixture(t)
	if bearer {
		g.LocalInstallation.Profile.Auth = domain.ConfiguredInferenceAuthBearerV1
		g.LocalInstallation.Profile.Resource.CredentialMode = domain.ProviderCredentialInvocationV1
		g.LocalInstallation.Profile.Resource.CredentialGeneration = 7
		g.Profile.Resource = g.LocalInstallation.Profile.Resource
		g.Profile.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryDirectV1
		g.Profile.Backend.BackendProfileDigest, _ = g.LocalInstallation.Profile.Digest()
		g.Profile.Locator.BackendProfileDigest = g.Profile.Backend.BackendProfileDigest
		if err := g.Validate(); err != nil {
			t.Fatalf("local Bearer profile: %v", err)
		}
	}
	return g.Profile
}

func TestLocalSpendingAllFourComponentsAndExecutionSource(t *testing.T) {
	t.Parallel()
	for _, bearer := range []bool{false, true} {
		name := "none"
		if bearer {
			name = "bearer"
		}
		t.Run(name, func(t *testing.T) {
			p := localSpendingProfileFixture(t, bearer)
			kinds := [4]domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1}
			var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
			for index, kind := range kinds {
				spending[index] = localSpendingFixture(t, p, kind)
			}
			if spending[0].ResourceGeneration != p.Resource.CredentialGeneration || *spending[0].MaxAmountMicrounits != 0 || *spending[1].MaxAmountMicrounits != 0 || *spending[2].MaxAmountMicrounits == 0 || *spending[3].MaxAmountMicrounits == 0 {
				t.Fatal("local provider/substrate zero invoice changed separate external service allocation")
			}
			scope := domain.AttemptExecutionSourceScopeV1{TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID, RunID: "local-run", AttemptID: "local-attempt", SessionID: "local-session"}
			pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, p, spending, p.Window.PublishedAt)
			if err != nil {
				t.Fatalf("local four-component source composition: %v", err)
			}
			if pkg.Binding.EvidenceExpiresAt == nil || !pkg.Binding.EvidenceExpiresAt.Equal(p.Window.ExpiresAt) {
				t.Fatal("local package did not preserve the exact original installed evidence expiry")
			}
			withoutEvidence := pkg.Binding.Clone()
			withoutEvidence.EvidenceExpiresAt = nil
			if err := withoutEvidence.Validate(); err == nil {
				t.Fatal("local package accepted the fixture-only absent evidence expiry")
			}
			if err := pkg.Source.ValidateAgainstCurrentSources(p, spending, p.Window.PublishedAt); err != nil {
				t.Fatalf("exact local source current comparison: %v", err)
			}
			raw, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := domain.DecodeAttemptExecutionSourceV1(raw)
			if err != nil || !reflect.DeepEqual(decoded, pkg.Source) {
				t.Fatalf("local source codec: err=%v", err)
			}
			if bytes.Count(raw, []byte(`"source_kind":"owner_local_installation"`)) != 4 {
				t.Fatal("source codec did not preserve all four original local funding kinds")
			}
		})
	}
}

func TestLocalSpendingExactCurrentSourceAndComponentFacts(t *testing.T) {
	t.Parallel()
	p := localSpendingProfileFixture(t, false)
	for _, kind := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
		t.Run(string(kind), func(t *testing.T) {
			base := localSpendingFixture(t, p, kind)
			for name, mutate := range map[string]func(*domain.AttemptSpendingAuthorizationCurrentV1){
				"fixture_kind": func(v *domain.AttemptSpendingAuthorizationCurrentV1) {
					v.SourceKind = domain.AttemptSpendingFixtureSourceV1
				},
				"fixture_locator": func(v *domain.AttemptSpendingAuthorizationCurrentV1) {
					v.SourceProfile.Locator.ResourceKind = domain.ProviderResourceCredentiallessFixtureV1
				},
				"foreign_owner":     func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.OwnerUserID = "foreign-owner" },
				"foreign_payer":     func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.Locator.BillingOwnerUserID = "foreign-payer" },
				"source_generation": func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.SourceProfile.RevocationGeneration++ },
				"source_digest": func(v *domain.AttemptSpendingAuthorizationCurrentV1) {
					v.SourceProfile.Digest = strings.Repeat("e", 64)
				},
				"installation_revision": func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.SourceInstallationRevision++ },
				"resource_revision":     func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.ResourceRevision++ },
				"resource_generation":   func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.ResourceGeneration++ },
				"profile_revision":      func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.ProfileRevision++ },
				"terms":                 func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.TermsEvidenceDigest = strings.Repeat("e", 64) },
				"price":                 func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.PriceEvidenceDigest = strings.Repeat("e", 64) },
				"price_revision":        func(v *domain.AttemptSpendingAuthorizationCurrentV1) { v.PriceRevision = "different-price" },
				"amount":                func(v *domain.AttemptSpendingAuthorizationCurrentV1) { *v.MaxAmountMicrounits++ },
				"expiry_extension": func(v *domain.AttemptSpendingAuthorizationCurrentV1) {
					v.Window.ExpiresAt = v.Window.ExpiresAt.Add(time.Second)
				},
				"before_profile": func(v *domain.AttemptSpendingAuthorizationCurrentV1) {
					v.Window.ObservedAt = v.Window.ObservedAt.Add(-time.Second)
				},
			} {
				t.Run(name, func(t *testing.T) {
					v := base.Clone()
					mutate(&v)
					bindOwnedSpendingSource(t, &v)
					if err := v.ValidateAgainstProfile(p, p.Window.PublishedAt); err == nil {
						t.Error("mutated local component accepted")
					}
				})
			}
			if err := base.ValidateAt(base.Window.ExpiresAt); err == nil {
				t.Error("exclusive local source expiry accepted")
			}
			if kind == domain.AttemptSpendingMCPToolV1 || kind == domain.AttemptSpendingWebSearchV1 {
				free := base.Clone()
				free.PriceState, free.MaxAmountMicrounits = domain.ExternalServicePriceKnownFreeV1, new(uint64)
				bindOwnedSpendingSource(t, &free)
				if err := free.Validate(); err != nil {
					t.Fatalf("explicit structural free declaration should remain representable: %v", err)
				}
				if err := free.ValidateAgainstProfile(p, p.Window.PublishedAt); err == nil {
					t.Error("local zero invoice silently zeroed separately priced external service")
				}
			}
		})
	}
}

func TestLocalSpendingCannotUsePaidOrUnrelatedInstallation(t *testing.T) {
	t.Parallel()
	local := localSpendingProfileFixture(t, false)
	fixture, at := ownedCurrentProfileFixture(t)
	fixtureSource := ownedSpendingFixture(t, fixture, domain.AttemptSpendingProviderV1)
	fixtureSource.SourceKind = domain.AttemptSpendingOwnerLocalSourceV1
	bindOwnedSpendingSource(t, &fixtureSource)
	if err := fixtureSource.ValidateAgainstProfile(fixture, at); err == nil {
		t.Error("fixture funding promoted to real local source")
	}
	for name, mutate := range map[string]func(*domain.OwnedAgentProfileCurrentV1){
		"other_backend": func(p *domain.OwnedAgentProfileCurrentV1) {
			p.Backend.BackendKind = domain.HarnessBackendDirectOpenRouterV1
		},
		"other_contract": func(p *domain.OwnedAgentProfileCurrentV1) {
			p.Backend.ProviderContractKind = domain.ProviderContractCredentiallessFixtureV1
		},
		"other_substrate":     func(p *domain.OwnedAgentProfileCurrentV1) { p.Substrate.Kind = domain.SubstrateDeterministicFixtureV1 },
		"separate_host_terms": func(p *domain.OwnedAgentProfileCurrentV1) { p.SubstrateTermsDigest = strings.Repeat("e", 64) },
		"paid_provider": func(p *domain.OwnedAgentProfileCurrentV1) {
			p.CostCeiling.ProviderPriceState = domain.ProviderPriceKnownV1
			*p.CostCeiling.MaxProviderAmountMicrounits = 1
			*p.CostCeiling.MaxTotalAmountMicrounits++
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := local.Clone()
			mutate(&p)
			p.Substrate.AdmissionCostCeilingDigest, _ = p.CostCeiling.Digest()
			v := localSpendingFixture(t, local, domain.AttemptSpendingProviderV1)
			v.SourceProfile.Digest, _ = p.Digest()
			bindOwnedSpendingSource(t, &v)
			if err := v.ValidateAgainstProfile(p, p.Window.PublishedAt); err == nil {
				t.Error("unrelated or non-local no-invoice installation accepted")
			}
		})
	}
}

func TestLocalSpendingCanonicalSourceCloneAndHistoricalRevocation(t *testing.T) {
	t.Parallel()
	p := localSpendingProfileFixture(t, false)
	v := localSpendingFixture(t, p, domain.AttemptSpendingWebSearchV1)
	raw, err := v.SourceCanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["source_kind"]) != `"owner_local_installation"` {
		t.Fatal("canonical envelope omitted original source kind")
	}
	clone := v.Clone()
	*clone.MaxAmountMicrounits++
	again, err := v.SourceCanonicalBytes()
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatalf("clone mutated original funding source: %v", err)
	}
	revoked := v.Clone()
	revoked.State, revoked.RevocationGeneration, revoked.UpdatedAt, revoked.MutationID = domain.OwnedAgentCurrentRevokedV1, 2, v.Window.ExpiresAt.Add(time.Second), "revoke-local-history"
	if err := revoked.ValidateTransitionFrom(v); err != nil {
		t.Fatalf("retained local revoke after original expiry: %v", err)
	}
	historical, err := revoked.SourceCanonicalBytes()
	if err != nil || !bytes.Equal(raw, historical) {
		t.Fatalf("revocation rewrote original local source envelope: %v", err)
	}
	if err := revoked.ValidateAt(revoked.UpdatedAt); err == nil {
		t.Error("historical revoked source became current permission")
	}
	encoded, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := domain.DecodeAttemptSpendingAuthorizationCurrentV1(encoded)
	if err != nil || !reflect.DeepEqual(decoded, v) {
		t.Fatalf("canonical local funding codec: %v", err)
	}
}
