package domain_test

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func agentLoopPolicyFixture() domain.AgentLoopPolicyV1 {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	substrateAmount, providerAmount, totalAmount := uint64(10), uint64(20), uint64(100)
	return domain.AgentLoopPolicyV1{
		Version: domain.AgentLoopPolicyVersionV1, TenantID: "tenant-189", OwnerUserID: "owner-189", RunID: "run-189", AttemptID: "attempt-189",
		Profile: domain.AgentLoopOwnedPublicV1, ProfileRevision: 1,
		Backend: domain.HarnessBackendDescriptorV1{
			HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "owned-loop.v1", BackendKind: domain.HarnessBackendDirectOpenRouterV1,
			ArtifactKind: domain.HarnessArtifactEmbeddedProfileV1, ArtifactDigest: strings.Repeat("a", 64), NativeProtocolVersion: "structured.v1",
			BackendProfileDigest: strings.Repeat("b", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryDirectV1,
		},
		Resource:      domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceAPIAccountV1, ResourceID: "resource-189", OwnerUserID: "owner-189", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1},
		ModelVendorID: "fixture-vendor", ModelID: "fixture-model", ProviderCatalogDigest: strings.Repeat("c", 64), ProviderRouteDigest: strings.Repeat("d", 64),
		CapabilityEvidenceDigest: strings.Repeat("e", 64), PrivacyPolicyDigest: strings.Repeat("f", 64), EffectivePolicyDigest: strings.Repeat("1", 64),
		InputModalities: []domain.AgentInputModalityV1{domain.AgentInputTextV1}, InputDataClass: domain.ProviderDataPublicV1, InstructionVersion: "owned-instructions.v1",
		CompactionPolicyDigest: strings.Repeat("2", 64), MCPToolGrantDigest: strings.Repeat("3", 64), WebSearchGrantDigest: strings.Repeat("4", 64),
		Limits: domain.AgentLoopLimitsV1{
			MaxConcurrentOperations: 1, MaxModelCalls: 8, MaxCompactionCalls: 2, MaxToolDispatches: 4, MaxWebSearches: 2,
			MaxInputTokensPerCall: 32768, MaxAggregateInputTokens: 131072, MaxOutputTokensPerCall: 2048, MaxAggregateOutputTokens: 16384,
			MaxActiveDuration: 120 * time.Second, MaxNetworkOperationDuration: 30 * time.Second, MaxCleanupDuration: 30 * time.Second,
			MaxContextViewBytes: 1 << 20, MaxSummaryBytes: 16 << 10, MaxToolArgumentBytes: 4 << 10, MaxToolResultBytes: 64 << 10,
			MaxMCPOutboundMessages: 12, MaxMCPCatalogPages: 1, MaxMCPInboundFrames: 128, MaxMCPInboundBytes: 256 << 10, MaxOperations: 12, MaxAggregateBodyBytes: 1 << 20,
		},
		AdmissionCostCeiling: domain.AdmissionCostCeilingV2{
			Version: domain.AdmissionCostCeilingVersionV2, Currency: "USD", PriceRevision: "offline-price.v1", PriceObservedAt: at.Add(-time.Hour), PriceExpiresAt: at.Add(time.Hour),
			MaxDeliveries: 1, MaxPreEffectDurationPerDelivery: time.Second, MaxActiveDuration: 120 * time.Second, MaxCleanupAndReconcileDuration: 30 * time.Second,
			ConfiguredMemoryBytes: 1 << 20, ConfiguredVCPUMillis: 100, MaxIngressBytes: 1 << 20, MaxEgressBytes: 1 << 20, MaxLogBytes: 1 << 20, MaxEvidenceBytes: 1 << 20,
			SubstratePriceState: domain.CostEvidenceKnownV1, ProviderPriceState: domain.ProviderPriceKnownV1,
			MaxSubstrateAmountMicrounits: &substrateAmount, MaxProviderAmountMicrounits: &providerAmount, MaxTotalAmountMicrounits: &totalAmount,
			MCPToolCost:   agentLoopServiceCostFixture(domain.ExternalServiceMCPToolV1, strings.Repeat("3", 64), 30, at),
			WebSearchCost: agentLoopServiceCostFixture(domain.ExternalServiceWebSearchV1, strings.Repeat("4", 64), 40, at),
		},
		IssuedAt: at, ExpiresAt: at.Add(30 * time.Minute),
	}
}

// Scripted evidence, not a claim about the current price of any public service.
func agentLoopServiceCostFixture(kind domain.ExternalServiceKindV1, grant string, amount uint64, at time.Time) *domain.ExternalServiceCostCeilingV1 {
	return &domain.ExternalServiceCostCeilingV1{
		Version: domain.ExternalServiceCostCeilingVersionV1, Kind: kind, TenantID: "tenant-189",
		OwnerUserID: "owner-189", BillingOwnerUserID: "billing-189",
		ResourceID: string(kind) + "-resource", ResourceRevision: 1, RevocationGeneration: 1,
		ProfileID: string(kind) + "-profile", ProfileRevision: 1, ScopedGrantDigest: grant,
		Currency: "USD", PriceRevision: "scripted-service-price.v1", PriceObservedAt: at.Add(-time.Hour), PriceExpiresAt: at.Add(time.Hour),
		PriceEvidenceDigest: strings.Repeat("8", 64), TermsEvidenceDigest: strings.Repeat("9", 64),
		PriceState: domain.ExternalServicePriceKnownPaidV1, MaxAmountMicrounits: &amount,
	}
}

func TestAgentLoopPolicySealsExactExternalServiceAuthority(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"mcp", "search"} {
		for _, axis := range []string{"missing", "tenant", "owner", "grant"} {
			t.Run(kind+"/"+axis, func(t *testing.T) {
				policy := agentLoopPolicyFixture()
				slot := &policy.AdmissionCostCeiling.MCPToolCost
				if kind == "search" {
					slot = &policy.AdmissionCostCeiling.WebSearchCost
				}
				switch axis {
				case "missing":
					*policy.AdmissionCostCeiling.MaxTotalAmountMicrounits -= *(*slot).MaxAmountMicrounits
					*slot = nil
				case "tenant":
					// Keep the enclosing cost internally consistent but outside the policy.
					policy.AdmissionCostCeiling.MCPToolCost.TenantID = "other-tenant"
					policy.AdmissionCostCeiling.WebSearchCost.TenantID = "other-tenant"
				case "owner":
					policy.AdmissionCostCeiling.MCPToolCost.OwnerUserID = "other-owner"
					policy.AdmissionCostCeiling.WebSearchCost.OwnerUserID = "other-owner"
				case "grant":
					(*slot).ScopedGrantDigest = strings.Repeat("7", 64)
				}
				if err := policy.AdmissionCostCeiling.Validate(); err != nil {
					t.Fatalf("independently valid cost fixture: %v", err)
				}
				if err := policy.Validate(); err == nil {
					t.Fatalf("owned policy accepted changed %s service %s authority", kind, axis)
				}
			})
		}
	}
}

// Construct a coherent V2 authority for tests joining a fixture substrate to
// an owned policy. All price/terms evidence remains scripted, not live proof.
func withOwnedServiceCostFixture(t testing.TB, outer domain.AdmissionCostCeilingV2, policy domain.AgentLoopPolicyV1) domain.AdmissionCostCeilingV2 {
	t.Helper()
	cost := outer.Clone()
	for _, service := range []struct {
		source *domain.ExternalServiceCostCeilingV1
		slot   **domain.ExternalServiceCostCeilingV1
		grant  string
	}{
		{policy.AdmissionCostCeiling.MCPToolCost, &cost.MCPToolCost, policy.MCPToolGrantDigest},
		{policy.AdmissionCostCeiling.WebSearchCost, &cost.WebSearchCost, policy.WebSearchGrantDigest},
	} {
		component := service.source.Clone()
		component.TenantID, component.OwnerUserID, component.ScopedGrantDigest = policy.TenantID, policy.OwnerUserID, service.grant
		component.PriceObservedAt, component.PriceExpiresAt = cost.PriceObservedAt, cost.PriceExpiresAt
		component.Currency = cost.Currency
		*cost.MaxTotalAmountMicrounits += *component.MaxAmountMicrounits
		*service.slot = &component
	}
	if err := cost.Validate(); err != nil {
		t.Fatalf("composed scripted service cost: %v", err)
	}
	return cost
}

func TestAgentLoopPolicyAcceptsMaximumAndSmallerCeilings(t *testing.T) {
	t.Parallel()
	maximum := agentLoopPolicyFixture()
	if err := maximum.ValidateAt(maximum.IssuedAt); err != nil {
		t.Fatalf("accepted maximum policy at issuance: %v", err)
	}
	smaller := maximum.Clone()
	smaller.Limits = domain.AgentLoopLimitsV1{
		MaxConcurrentOperations: 1, MaxModelCalls: 2, MaxCompactionCalls: 1, MaxToolDispatches: 2, MaxWebSearches: 1,
		MaxInputTokensPerCall: 100, MaxAggregateInputTokens: 200, MaxOutputTokensPerCall: 10, MaxAggregateOutputTokens: 20,
		MaxActiveDuration: 10 * time.Second, MaxNetworkOperationDuration: time.Second, MaxCleanupDuration: time.Second,
		MaxContextViewBytes: 100, MaxSummaryBytes: 10, MaxToolArgumentBytes: 10, MaxToolResultBytes: 20,
		MaxMCPOutboundMessages: 1, MaxMCPCatalogPages: 1, MaxMCPInboundFrames: 1, MaxMCPInboundBytes: 20, MaxOperations: 4, MaxAggregateBodyBytes: 100,
	}
	if err := smaller.ValidateAt(smaller.IssuedAt); err != nil {
		t.Fatalf("smaller required ceilings must remain accepted: %v", err)
	}
}

func TestAgentLoopPolicyLimitsRejectEveryZeroAndOverCap(t *testing.T) {
	t.Parallel()
	base := agentLoopPolicyFixture().Limits
	fields := reflect.TypeOf(base)
	for index := range fields.NumField() {
		name := fields.Field(index).Name
		for _, boundary := range []string{"zero", "above_cap", "overflow"} {
			t.Run(name+"/"+boundary, func(t *testing.T) {
				candidate := base
				field := reflect.ValueOf(&candidate).Elem().Field(index)
				if field.Kind() == reflect.Uint64 {
					value := uint64(0)
					if boundary == "above_cap" {
						value = field.Uint() + 1
					}
					if boundary == "overflow" {
						value = ^uint64(0)
					}
					field.SetUint(value)
				} else {
					value := int64(0)
					if boundary == "above_cap" {
						value = field.Int() + 1
					}
					if boundary == "overflow" {
						value = -1
					}
					field.SetInt(value)
				}
				if err := candidate.Validate(); err == nil {
					t.Fatalf("limits field %s %s unexpectedly accepted: %+v", name, boundary, candidate)
				}
			})
		}
	}
}

func TestAgentLoopPolicyLimitsRejectInconsistentCeilings(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*domain.AgentLoopLimitsV1){
		"compaction_outside_models":          func(v *domain.AgentLoopLimitsV1) { v.MaxModelCalls = 1 },
		"search_outside_tools":               func(v *domain.AgentLoopLimitsV1) { v.MaxToolDispatches = 1 },
		"operations_outside_dispatches":      func(v *domain.AgentLoopLimitsV1) { v.MaxModelCalls = 7 },
		"input_less_than_call":               func(v *domain.AgentLoopLimitsV1) { v.MaxAggregateInputTokens = 1 },
		"input_above_possible_reservations":  func(v *domain.AgentLoopLimitsV1) { v.MaxInputTokensPerCall = 1 },
		"output_less_than_call":              func(v *domain.AgentLoopLimitsV1) { v.MaxAggregateOutputTokens = 1 },
		"output_above_possible_reservations": func(v *domain.AgentLoopLimitsV1) { v.MaxOutputTokensPerCall = 1 },
		"summary_outside_view":               func(v *domain.AgentLoopLimitsV1) { v.MaxContextViewBytes = 1 },
		"arguments_outside_bodies":           func(v *domain.AgentLoopLimitsV1) { v.MaxAggregateBodyBytes = 1 },
		"result_outside_bodies":              func(v *domain.AgentLoopLimitsV1) { v.MaxAggregateBodyBytes = v.MaxToolArgumentBytes },
		"network_outside_active":             func(v *domain.AgentLoopLimitsV1) { v.MaxActiveDuration = time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := agentLoopPolicyFixture().Limits
			change(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatalf("inconsistent %s accepted: %+v", name, candidate)
			}
		})
	}
}

func TestAgentLoopPolicyRequiredFieldsAndClosedProfile(t *testing.T) {
	t.Parallel()
	base := agentLoopPolicyFixture()
	for name, change := range map[string]func(*domain.AgentLoopPolicyV1){
		"zero_policy":            func(v *domain.AgentLoopPolicyV1) { *v = domain.AgentLoopPolicyV1{} },
		"version":                func(v *domain.AgentLoopPolicyV1) { v.Version = 0 },
		"profile_absent":         func(v *domain.AgentLoopPolicyV1) { v.Profile = "" },
		"profile_unknown":        func(v *domain.AgentLoopPolicyV1) { v.Profile = "owned_agent_private_v1" },
		"profile_revision":       func(v *domain.AgentLoopPolicyV1) { v.ProfileRevision = 0 },
		"tenant":                 func(v *domain.AgentLoopPolicyV1) { v.TenantID = "" },
		"owner":                  func(v *domain.AgentLoopPolicyV1) { v.OwnerUserID = "" },
		"run":                    func(v *domain.AgentLoopPolicyV1) { v.RunID = "" },
		"attempt":                func(v *domain.AgentLoopPolicyV1) { v.AttemptID = "" },
		"backend":                func(v *domain.AgentLoopPolicyV1) { v.Backend = domain.HarnessBackendDescriptorV1{} },
		"resource":               func(v *domain.AgentLoopPolicyV1) { v.Resource = domain.ProviderResourceBindingV1{} },
		"foreign_resource_owner": func(v *domain.AgentLoopPolicyV1) { v.Resource.OwnerUserID = "other-owner" },
		"model_vendor":           func(v *domain.AgentLoopPolicyV1) { v.ModelVendorID = "" },
		"model":                  func(v *domain.AgentLoopPolicyV1) { v.ModelID = "" },
		"instruction":            func(v *domain.AgentLoopPolicyV1) { v.InstructionVersion = "" },
		"data_class_absent":      func(v *domain.AgentLoopPolicyV1) { v.InputDataClass = "" },
		"private_data":           func(v *domain.AgentLoopPolicyV1) { v.InputDataClass = domain.ProviderDataPrivateV1 },
		"shareable_data":         func(v *domain.AgentLoopPolicyV1) { v.InputDataClass = domain.ProviderDataExternallyShareableV1 },
		"modalities_absent":      func(v *domain.AgentLoopPolicyV1) { v.InputModalities = nil },
		"unknown_modality":       func(v *domain.AgentLoopPolicyV1) { v.InputModalities = []domain.AgentInputModalityV1{"audio"} },
		"duplicate_modality": func(v *domain.AgentLoopPolicyV1) {
			v.InputModalities = []domain.AgentInputModalityV1{domain.AgentInputTextV1, domain.AgentInputTextV1}
		},
		"unordered_modalities": func(v *domain.AgentLoopPolicyV1) {
			v.InputModalities = []domain.AgentInputModalityV1{domain.AgentInputTextV1, domain.AgentInputImageV1}
		},
		"issued_at":    func(v *domain.AgentLoopPolicyV1) { v.IssuedAt = time.Time{} },
		"expires_at":   func(v *domain.AgentLoopPolicyV1) { v.ExpiresAt = time.Time{} },
		"empty_window": func(v *domain.AgentLoopPolicyV1) { v.ExpiresAt = v.IssuedAt },
		"beyond_price_window": func(v *domain.AgentLoopPolicyV1) {
			v.ExpiresAt = v.AdmissionCostCeiling.PriceExpiresAt.Add(time.Nanosecond)
		},
		"before_price_observation": func(v *domain.AgentLoopPolicyV1) {
			v.IssuedAt = v.AdmissionCostCeiling.PriceObservedAt.Add(-time.Nanosecond)
		},
		"noncanonical_year": func(v *domain.AgentLoopPolicyV1) {
			v.ExpiresAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			v.AdmissionCostCeiling.PriceExpiresAt = v.ExpiresAt
		},
		"cost":                  func(v *domain.AgentLoopPolicyV1) { v.AdmissionCostCeiling = domain.AdmissionCostCeilingV2{} },
		"active_cost_coverage":  func(v *domain.AgentLoopPolicyV1) { v.AdmissionCostCeiling.MaxActiveDuration = time.Second },
		"cleanup_cost_coverage": func(v *domain.AgentLoopPolicyV1) { v.AdmissionCostCeiling.MaxCleanupAndReconcileDuration = time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			change(&candidate)
			if _, err := domain.BuildAgentLoopPolicyV1(candidate); err == nil {
				t.Fatalf("invalid authority %s accepted: %+v", name, candidate)
			}
		})
	}
	for _, name := range []string{"ProviderCatalogDigest", "ProviderRouteDigest", "CapabilityEvidenceDigest", "PrivacyPolicyDigest", "EffectivePolicyDigest", "CompactionPolicyDigest", "MCPToolGrantDigest", "WebSearchGrantDigest"} {
		for _, input := range []string{"", "not-sha256", strings.Repeat("A", 64)} {
			t.Run(name+"/"+input, func(t *testing.T) {
				candidate := base.Clone()
				reflect.ValueOf(&candidate).Elem().FieldByName(name).SetString(input)
				if _, err := candidate.Digest(); err == nil {
					t.Fatalf("noncanonical required digest %s=%q accepted", name, input)
				}
			})
		}
	}
}

func TestAgentLoopPolicyEveryNestedAuthorityFieldIsRequired(t *testing.T) {
	t.Parallel()
	base := agentLoopPolicyFixture()
	timeType := reflect.TypeOf(time.Time{})
	var collect func(reflect.Type, []int, string) map[string][]int
	collect = func(value reflect.Type, path []int, prefix string) map[string][]int {
		result := make(map[string][]int)
		for index := range value.NumField() {
			field := value.Field(index)
			fieldPath := append(append([]int(nil), path...), index)
			name := prefix + field.Name
			if field.Type.Kind() == reflect.Struct && field.Type != timeType {
				for nestedName, nestedPath := range collect(field.Type, fieldPath, name+"/") {
					result[nestedName] = nestedPath
				}
			} else {
				result[name] = fieldPath
			}
		}
		return result
	}
	for name, path := range collect(reflect.TypeOf(base), nil, "") {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			field := reflect.ValueOf(&candidate).Elem().FieldByIndex(path)
			field.SetZero()
			if err := candidate.Validate(); err == nil {
				t.Fatalf("missing required authority field %s accepted", name)
			}
		})
	}
}

func TestAgentLoopPolicyTimeAndUnknownCostAreNotStructuralFreshness(t *testing.T) {
	t.Parallel()
	base := agentLoopPolicyFixture()
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, at := range map[string]time.Time{"zero": {}, "before_issuance": base.IssuedAt.Add(-time.Nanosecond), "at_expiry": base.ExpiresAt, "after_expiry": base.ExpiresAt.Add(time.Nanosecond)} {
		t.Run(name, func(t *testing.T) {
			if err := base.ValidateAt(at); err == nil {
				t.Fatalf("ValidateAt(%s) accepted outside policy window [%s,%s)", at, base.IssuedAt, base.ExpiresAt)
			}
		})
	}
	if err := base.ValidateAt(base.ExpiresAt.Add(-time.Nanosecond)); err != nil {
		t.Fatalf("last instant before expiry: %v", err)
	}
	unknown := base.Clone()
	unknown.AdmissionCostCeiling.ProviderPriceState = domain.ProviderPriceUnknownV1
	unknown.AdmissionCostCeiling.MaxProviderAmountMicrounits, unknown.AdmissionCostCeiling.MaxTotalAmountMicrounits = nil, nil
	if err := unknown.Validate(); err != nil {
		t.Fatalf("unknown cost remains structurally representable: %v", err)
	}
	if _, err := unknown.Digest(); err != nil {
		t.Fatalf("immutable unknown-cost policy remains digestible: %v", err)
	}
	if err := unknown.ValidateAt(base.IssuedAt); err == nil {
		t.Fatal("structural unknown price authorized time-dependent admission")
	}
}

func TestAgentLoopPolicyBuildAndCloneIsolateSourceAndCanonicalizeUTC(t *testing.T) {
	t.Parallel()
	source := agentLoopPolicyFixture()
	zone := time.FixedZone("fixture-offset", 3*60*60)
	source.IssuedAt, source.ExpiresAt = source.IssuedAt.In(zone), source.ExpiresAt.In(zone)
	source.AdmissionCostCeiling.PriceObservedAt = source.AdmissionCostCeiling.PriceObservedAt.In(zone)
	source.AdmissionCostCeiling.PriceExpiresAt = source.AdmissionCostCeiling.PriceExpiresAt.In(zone)
	sealed, err := domain.BuildAgentLoopPolicyV1(source)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := sealed.Digest()
	if err != nil {
		t.Fatal(err)
	}
	for name, at := range map[string]time.Time{"issued": sealed.IssuedAt, "expiry": sealed.ExpiresAt, "price_observed": sealed.AdmissionCostCeiling.PriceObservedAt, "price_expiry": sealed.AdmissionCostCeiling.PriceExpiresAt} {
		if at.Location() != time.UTC {
			t.Errorf("%s location=%s, want UTC", name, at.Location())
		}
	}
	source.InputModalities[0] = domain.AgentInputImageV1
	*source.AdmissionCostCeiling.MaxProviderAmountMicrounits = 999
	cloned := sealed.Clone()
	cloned.InputModalities[0] = domain.AgentInputFileV1
	*cloned.AdmissionCostCeiling.MaxSubstrateAmountMicrounits = 888
	*cloned.AdmissionCostCeiling.MaxTotalAmountMicrounits = 777
	unchanged, err := sealed.Digest()
	if err != nil || unchanged != digest {
		t.Fatalf("source/clone mutation changed sealed policy digest: got=%s want=%s err=%v", unchanged, digest, err)
	}
	canonicalDigest, err := agentLoopPolicyFixture().Digest()
	if err != nil || canonicalDigest != digest {
		t.Fatalf("UTC-normalized instants changed digest: got=%s want=%s err=%v", digest, canonicalDigest, err)
	}
}

func TestAgentLoopPolicyDigestBindsEverySealedField(t *testing.T) {
	t.Parallel()
	base := agentLoopPolicyFixture()
	// Interior ceilings allow each independent limit to change while valid.
	base.Limits.MaxOperations = 10
	base.Limits.MaxInputTokensPerCall, base.Limits.MaxAggregateInputTokens = 1000, 4000
	base.Limits.MaxOutputTokensPerCall, base.Limits.MaxAggregateOutputTokens = 100, 400
	base.Limits.MaxActiveDuration, base.Limits.MaxNetworkOperationDuration, base.Limits.MaxCleanupDuration = 100*time.Second, 20*time.Second, 20*time.Second
	baseDigest, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*domain.AgentLoopPolicyV1){
		"tenant": func(v *domain.AgentLoopPolicyV1) {
			v.TenantID = "other-tenant"
			v.AdmissionCostCeiling.MCPToolCost.TenantID, v.AdmissionCostCeiling.WebSearchCost.TenantID = v.TenantID, v.TenantID
		},
		"owner": func(v *domain.AgentLoopPolicyV1) {
			v.OwnerUserID, v.Resource.OwnerUserID = "other-owner", "other-owner"
			v.AdmissionCostCeiling.MCPToolCost.OwnerUserID, v.AdmissionCostCeiling.WebSearchCost.OwnerUserID = v.OwnerUserID, v.OwnerUserID
		},
		"run":              func(v *domain.AgentLoopPolicyV1) { v.RunID = "other-run" },
		"attempt":          func(v *domain.AgentLoopPolicyV1) { v.AttemptID = "other-attempt" },
		"profile_revision": func(v *domain.AgentLoopPolicyV1) { v.ProfileRevision++ },
		"harness_version":  func(v *domain.AgentLoopPolicyV1) { v.Backend.HarnessVersion = "owned-loop.v2" },
		"backend_kind":     func(v *domain.AgentLoopPolicyV1) { v.Backend.BackendKind = domain.HarnessBackendCodexExecV1 },
		"artifact_kind":    func(v *domain.AgentLoopPolicyV1) { v.Backend.ArtifactKind = domain.HarnessArtifactExecutableV1 },
		"artifact_digest":  func(v *domain.AgentLoopPolicyV1) { v.Backend.ArtifactDigest = strings.Repeat("9", 64) },
		"native_protocol":  func(v *domain.AgentLoopPolicyV1) { v.Backend.NativeProtocolVersion = "structured.v2" },
		"backend_profile":  func(v *domain.AgentLoopPolicyV1) { v.Backend.BackendProfileDigest = strings.Repeat("9", 64) },
		"credential_delivery": func(v *domain.AgentLoopPolicyV1) {
			v.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
		},
		"resource_kind":         func(v *domain.AgentLoopPolicyV1) { v.Resource.Kind = domain.ProviderResourceRouterAccountV1 },
		"resource_id":           func(v *domain.AgentLoopPolicyV1) { v.Resource.ResourceID = "other-resource" },
		"resource_revision":     func(v *domain.AgentLoopPolicyV1) { v.Resource.Revision++ },
		"credential_generation": func(v *domain.AgentLoopPolicyV1) { v.Resource.CredentialGeneration++ },
		"model_vendor":          func(v *domain.AgentLoopPolicyV1) { v.ModelVendorID = "other-vendor" },
		"model_id":              func(v *domain.AgentLoopPolicyV1) { v.ModelID = "other-model" },
		"modalities": func(v *domain.AgentLoopPolicyV1) {
			v.InputModalities = []domain.AgentInputModalityV1{domain.AgentInputImageV1, domain.AgentInputTextV1}
		},
		"instruction":   func(v *domain.AgentLoopPolicyV1) { v.InstructionVersion = "owned-instructions.v2" },
		"cost_revision": func(v *domain.AgentLoopPolicyV1) { v.AdmissionCostCeiling.PriceRevision = "offline-price.v2" },
		"cost_amount": func(v *domain.AgentLoopPolicyV1) {
			*v.AdmissionCostCeiling.MaxProviderAmountMicrounits++
			*v.AdmissionCostCeiling.MaxTotalAmountMicrounits++
		},
		"issued_at":  func(v *domain.AgentLoopPolicyV1) { v.IssuedAt = v.IssuedAt.Add(time.Nanosecond) },
		"expires_at": func(v *domain.AgentLoopPolicyV1) { v.ExpiresAt = v.ExpiresAt.Add(-time.Nanosecond) },
	}
	for _, field := range []string{"ProviderCatalogDigest", "ProviderRouteDigest", "CapabilityEvidenceDigest", "PrivacyPolicyDigest", "EffectivePolicyDigest", "CompactionPolicyDigest", "MCPToolGrantDigest", "WebSearchGrantDigest"} {
		mutations[field] = func(v *domain.AgentLoopPolicyV1) {
			reflect.ValueOf(v).Elem().FieldByName(field).SetString(strings.Repeat("9", 64))
			switch field {
			case "MCPToolGrantDigest":
				v.AdmissionCostCeiling.MCPToolCost.ScopedGrantDigest = v.MCPToolGrantDigest
			case "WebSearchGrantDigest":
				v.AdmissionCostCeiling.WebSearchCost.ScopedGrantDigest = v.WebSearchGrantDigest
			}
		}
	}
	limitsType := reflect.TypeOf(base.Limits)
	for index := range limitsType.NumField() {
		name := limitsType.Field(index).Name
		if name == "MaxConcurrentOperations" || name == "MaxMCPCatalogPages" {
			continue
		} // closed at one; invalid changes covered above
		mutations["limits/"+name] = func(v *domain.AgentLoopPolicyV1) {
			field := reflect.ValueOf(&v.Limits).Elem().Field(index)
			if field.Kind() == reflect.Uint64 {
				field.SetUint(field.Uint() - 1)
			} else {
				field.SetInt(field.Int() - 1)
			}
		}
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			mutate(&candidate)
			changed, err := candidate.Digest()
			if err != nil {
				t.Fatalf("valid sealed-field mutation %s rejected: %v", name, err)
			}
			if changed == baseDigest {
				t.Fatalf("sealed-field mutation %s did not change digest %s", name, baseDigest)
			}
		})
	}
}

func TestAgentLoopPolicyDigestConstructionIsIndependentAndDomainSeparated(t *testing.T) {
	t.Parallel()
	// Independent sources are completed without a policy, binding or invocation.
	independent := func(source string) string { sum := sha256.Sum256([]byte(source)); return hex.EncodeToString(sum[:]) }
	policy := agentLoopPolicyFixture()
	policy.CompactionPolicyDigest = independent("fixture.compaction.v1")
	policy.MCPToolGrantDigest = independent("fixture.scoped-mcp-grant.v1")
	policy.WebSearchGrantDigest = independent("fixture.scoped-search-grant.v1")
	policy.AdmissionCostCeiling.MCPToolCost.ScopedGrantDigest = policy.MCPToolGrantDigest
	policy.AdmissionCostCeiling.WebSearchCost.ScopedGrantDigest = policy.WebSearchGrantDigest
	digest, err := policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if err := digest.Validate(); err != nil {
		t.Fatalf("constructed policy digest: %v", err)
	}
	cost, err := policy.AdmissionCostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	for name, other := range map[string]string{"compaction": policy.CompactionPolicyDigest, "mcp": policy.MCPToolGrantDigest, "search": policy.WebSearchGrantDigest, "cost": string(cost)} {
		if string(digest) == other {
			t.Errorf("policy digest collapsed into independent %s digest", name)
		}
	}
	for _, forbidden := range []string{"AgentLoopPolicyDigest", "HarnessBindingDigest", "InvocationAuthorityDigest", "ExecutionPlacementDigest"} {
		if _, exists := reflect.TypeOf(policy).FieldByName(forbidden); exists {
			t.Errorf("enclosing authority field %s makes policy construction cyclic", forbidden)
		}
	}
	if err := domain.AgentLoopPolicyDigestV1(strings.Repeat("A", 64)).Validate(); err == nil {
		t.Fatal("uppercase policy digest accepted")
	}
}
