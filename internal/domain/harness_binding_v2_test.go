package domain_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func ownedAgentBindingFixture(t *testing.T) domain.HarnessBindingV2 {
	t.Helper()
	policy := agentLoopPolicyFixture()
	placement := domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementManaged,
		FallbackPolicy: domain.ExecutionFallbackDenied, SubstrateBindingDigest: strings.Repeat("5", 64)}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatalf("digest managed placement: %v", err)
	}
	expires := policy.ExpiresAt
	binding := domain.HarnessBindingV2{
		Version: domain.HarnessBindingVersionV2, Profile: domain.HarnessExecutionOwnedAgentV2, AgentLoopPolicy: &policy,
		TenantID: policy.TenantID, OwnerUserID: policy.OwnerUserID, RunID: policy.RunID, AttemptID: policy.AttemptID,
		Backend: policy.Backend, Resource: policy.Resource, ModelVendorID: policy.ModelVendorID, ModelID: policy.ModelID,
		InputDataClass: policy.InputDataClass, ProviderCatalogDigest: policy.ProviderCatalogDigest, ProviderRouteDigest: policy.ProviderRouteDigest,
		CapabilityEvidenceDigest: policy.CapabilityEvidenceDigest, PrivacyPolicyDigest: policy.PrivacyPolicyDigest,
		EffectivePolicyDigest: policy.EffectivePolicyDigest, ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &expires,
	}
	if err := binding.ValidateAt(policy.IssuedAt); err != nil {
		t.Fatalf("valid synthetic owned binding: %v", err)
	}
	return binding
}

func TestHarnessBindingV2RejectsLegacyWireAndMissingProfile(t *testing.T) {
	t.Parallel()
	base := deterministicManagedAuthority(t, "tenant-v2", "owner-v2", "run-v2", "attempt-v2", time.Unix(10, 0).UTC()).HarnessBinding
	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatalf("marshal successor binding: %v", err)
	}
	for _, tc := range []struct{ name, wire string }{
		{"legacy_v1", strings.Replace(string(encoded), `"version":2`, `"version":1`, 1)},
		{"missing_profile", strings.Replace(string(encoded), `"profile":"single_turn",`, "", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var candidate domain.HarnessBindingV2
			if err := json.Unmarshal([]byte(tc.wire), &candidate); err != nil {
				t.Fatalf("decode structurally valid %s JSON: %v", tc.name, err)
			}
			if err := candidate.Validate(); err == nil {
				t.Fatalf("%s wire accepted: %+v", tc.name, candidate)
			}
			if _, err := candidate.Digest(); err == nil {
				t.Fatalf("%s wire obtained canonical authority digest", tc.name)
			}
		})
	}
}

func TestHarnessBindingV2RequiresExactProfilePolicyUnion(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*domain.HarnessBindingV2){
		"owned_missing_policy": func(v *domain.HarnessBindingV2) { v.AgentLoopPolicy = nil },
		"single_with_policy":   func(v *domain.HarnessBindingV2) { v.Profile = domain.HarnessExecutionSingleTurnV2 },
		"unknown_profile":      func(v *domain.HarnessBindingV2) { v.Profile = "unreviewed" },
		"policy_past_evidence": func(v *domain.HarnessBindingV2) {
			at := v.AgentLoopPolicy.ExpiresAt.Add(-time.Second)
			v.EvidenceExpiresAt = &at
		},
		"private_data": func(v *domain.HarnessBindingV2) { v.InputDataClass = domain.ProviderDataPrivateV1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := ownedAgentBindingFixture(t)
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatalf("invalid profile-policy union %s accepted", name)
			}
		})
	}
}

func TestHarnessBindingV2RejectsEveryPolicyAuthorityAxisMismatch(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"TenantID", "OwnerUserID", "RunID", "AttemptID", "ModelVendorID", "ModelID", "ProviderCatalogDigest", "ProviderRouteDigest", "CapabilityEvidenceDigest", "PrivacyPolicyDigest", "EffectivePolicyDigest"} {
		t.Run(field, func(t *testing.T) {
			candidate := ownedAgentBindingFixture(t)
			value := reflect.ValueOf(candidate.AgentLoopPolicy).Elem().FieldByName(field)
			value.SetString(strings.ReplaceAll(value.String(), "1", "6") + "x")
			if strings.HasSuffix(field, "Digest") {
				value.SetString(strings.Repeat("6", 64))
			}
			if field == "OwnerUserID" {
				candidate.AgentLoopPolicy.Resource.OwnerUserID = candidate.AgentLoopPolicy.OwnerUserID
			}
			if err := candidate.AgentLoopPolicy.Validate(); err != nil {
				t.Fatalf("independently valid mismatched policy %s: %v", field, err)
			}
			if err := candidate.Validate(); err == nil {
				t.Fatalf("mismatched policy %s accepted", field)
			}
		})
	}
	for name, mutate := range map[string]func(*domain.AgentLoopPolicyV1){
		"backend":    func(v *domain.AgentLoopPolicyV1) { v.Backend.ArtifactDigest = strings.Repeat("6", 64) },
		"resource":   func(v *domain.AgentLoopPolicyV1) { v.Resource.Revision++ },
		"data_class": func(v *domain.AgentLoopPolicyV1) { v.InputDataClass = domain.ProviderDataExternallyShareableV1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := ownedAgentBindingFixture(t)
			mutate(candidate.AgentLoopPolicy)
			if err := candidate.Validate(); err == nil {
				t.Fatalf("mismatched policy %s accepted", name)
			}
		})
	}
}

func TestHarnessBindingV2SealsFullPolicyDigestAndClonesPolicy(t *testing.T) {
	t.Parallel()
	base := ownedAgentBindingFixture(t)
	want, err := base.Digest()
	if err != nil {
		t.Fatalf("base binding digest: %v", err)
	}
	for name, mutate := range map[string]func(*domain.AgentLoopPolicyV1){
		"instruction": func(v *domain.AgentLoopPolicyV1) { v.InstructionVersion = "instructions.v2" },
		"grant":       func(v *domain.AgentLoopPolicyV1) { v.WebSearchGrantDigest = strings.Repeat("7", 64) },
		"limits":      func(v *domain.AgentLoopPolicyV1) { v.Limits.MaxActiveDuration-- },
		"modalities": func(v *domain.AgentLoopPolicyV1) {
			v.InputModalities = []domain.AgentInputModalityV1{domain.AgentInputFileV1, domain.AgentInputTextV1}
		},
		"cost": func(v *domain.AgentLoopPolicyV1) {
			*v.AdmissionCostCeiling.MaxProviderAmountMicrounits += 1
			*v.AdmissionCostCeiling.MaxTotalAmountMicrounits += 1
		},
		"expiry": func(v *domain.AgentLoopPolicyV1) { v.ExpiresAt = v.ExpiresAt.Add(-time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			mutate(candidate.AgentLoopPolicy)
			got, err := candidate.Digest()
			if err != nil || got == want {
				t.Fatalf("policy mutation %s binding digest=%q base=%q err=%v", name, got, want, err)
			}
			unchanged, err := base.Digest()
			if err != nil || unchanged != want {
				t.Fatalf("clone mutation %s changed original authority: digest=%q want=%q err=%v", name, unchanged, want, err)
			}
		})
	}
}

func TestHarnessBindingV2RejectsSubstitutedMonetaryAuthorityAndExpiry(t *testing.T) {
	t.Parallel()
	binding := ownedAgentBindingFixture(t)
	cost := binding.AgentLoopPolicy.AdmissionCostCeiling.Clone()
	if err := binding.ValidateAdmissionCostCeiling(&cost); err != nil {
		t.Fatalf("exact admitted cost rejected: %v", err)
	}
	*cost.MaxTotalAmountMicrounits++
	*cost.MaxProviderAmountMicrounits++
	if err := cost.Validate(); err != nil {
		t.Fatalf("independently valid changed ceiling: %v", err)
	}
	if err := binding.ValidateAdmissionCostCeiling(&cost); err == nil {
		t.Fatal("independently valid substituted ceiling accepted")
	}
	if err := binding.ValidateAdmissionCostCeiling(nil); err == nil {
		t.Fatal("owned policy accepted without outer ceiling")
	}
	if err := binding.ValidateAt(binding.AgentLoopPolicy.ExpiresAt); err == nil {
		t.Fatal("owned policy accepted at exclusive expiry")
	}
}
