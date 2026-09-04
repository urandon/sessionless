package domain_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestHarnessBindingDigestBindsEveryAuthorityField(t *testing.T) {
	t.Parallel()
	base := deterministicManagedAuthority(t, "tenant-1", "user-1", "run-1", "attempt-1", time.Unix(10, 0).UTC()).HarnessBinding
	baseDigest, err := base.Digest()
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*domain.HarnessBindingV1){
		"version": func(value *domain.HarnessBindingV1) { value.Version++ },
		"tenant":  func(value *domain.HarnessBindingV1) { value.TenantID = "tenant-2" },
		"owner": func(value *domain.HarnessBindingV1) {
			value.OwnerUserID, value.Resource.OwnerUserID = "user-2", "user-2"
		},
		"run":             func(value *domain.HarnessBindingV1) { value.RunID = "run-2" },
		"attempt":         func(value *domain.HarnessBindingV1) { value.AttemptID = "attempt-2" },
		"harness version": func(value *domain.HarnessBindingV1) { value.Backend.HarnessVersion = "2" },
		"harness kind":    func(value *domain.HarnessBindingV1) { value.Backend.HarnessKind = "other" },
		"backend kind":    func(value *domain.HarnessBindingV1) { value.Backend.BackendKind = domain.HarnessBackendCodexExecV1 },
		"artifact kind":   func(value *domain.HarnessBindingV1) { value.Backend.ArtifactKind = domain.HarnessArtifactExecutableV1 },
		"provider contract": func(value *domain.HarnessBindingV1) {
			value.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
		},
		"credential delivery": func(value *domain.HarnessBindingV1) {
			value.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
		},
		"artifact":      func(value *domain.HarnessBindingV1) { value.Backend.ArtifactDigest = strings.Repeat("a", 64) },
		"protocol":      func(value *domain.HarnessBindingV1) { value.Backend.NativeProtocolVersion = "other.v1" },
		"profile":       func(value *domain.HarnessBindingV1) { value.Backend.BackendProfileDigest = strings.Repeat("b", 64) },
		"resource":      func(value *domain.HarnessBindingV1) { value.Resource.ResourceID = "fixture-2" },
		"resource kind": func(value *domain.HarnessBindingV1) { value.Resource.Kind = domain.ProviderResourceSubscriptionV1 },
		"revision":      func(value *domain.HarnessBindingV1) { value.Resource.Revision++ },
		"credential mode": func(value *domain.HarnessBindingV1) {
			value.Resource.CredentialMode = domain.ProviderCredentialInvocationV1
		},
		"credential generation": func(value *domain.HarnessBindingV1) { value.Resource.CredentialGeneration++ },
		"model vendor":          func(value *domain.HarnessBindingV1) { value.ModelVendorID = "other-vendor" },
		"model":                 func(value *domain.HarnessBindingV1) { value.ModelID = "fixture-model-2" },
		"input data class":      func(value *domain.HarnessBindingV1) { value.InputDataClass = domain.ProviderDataPublicV1 },
		"catalog":               func(value *domain.HarnessBindingV1) { value.ProviderCatalogDigest = strings.Repeat("c", 64) },
		"route":                 func(value *domain.HarnessBindingV1) { value.ProviderRouteDigest = strings.Repeat("d", 64) },
		"privacy":               func(value *domain.HarnessBindingV1) { value.PrivacyPolicyDigest = strings.Repeat("e", 64) },
		"capability":            func(value *domain.HarnessBindingV1) { value.CapabilityEvidenceDigest = strings.Repeat("f", 64) },
		"policy":                func(value *domain.HarnessBindingV1) { value.EffectivePolicyDigest = strings.Repeat("1", 64) },
		"placement":             func(value *domain.HarnessBindingV1) { value.ExecutionPlacementDigest = strings.Repeat("2", 64) },
		"expiry": func(value *domain.HarnessBindingV1) {
			expiry := time.Unix(20, 0).UTC()
			value.EvidenceExpiresAt = &expiry
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			mutate(&candidate)
			digest, err := candidate.Digest()
			if err != nil {
				return
			}
			if digest == baseDigest {
				t.Fatal("authority mutation did not change digest")
			}
		})
	}
}

func TestHarnessBindingCredentialModesAreExplicit(t *testing.T) {
	t.Parallel()
	binding := deterministicManagedAuthority(t, "tenant-1", "user-1", "run-1", "attempt-1", time.Unix(10, 0).UTC()).HarnessBinding
	for name, mutate := range map[string]func(*domain.HarnessBindingV1){
		"zero":                     func(value *domain.HarnessBindingV1) { *value = domain.HarnessBindingV1{} },
		"none positive generation": func(value *domain.HarnessBindingV1) { value.Resource.CredentialGeneration = 1 },
		"none subscription":        func(value *domain.HarnessBindingV1) { value.Resource.Kind = domain.ProviderResourceSubscriptionV1 },
		"provider without credential": func(value *domain.HarnessBindingV1) {
			value.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := binding.Clone()
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("invalid credential authority accepted")
			}
		})
	}
}

func TestHarnessBindingCloneOwnsOptionalEvidenceTime(t *testing.T) {
	t.Parallel()
	binding := deterministicManagedAuthority(t, "tenant-1", "user-1", "run-1", "attempt-1", time.Unix(10, 0).UTC()).HarnessBinding
	expires := time.Unix(20, 0).UTC()
	binding.EvidenceExpiresAt = &expires
	clone := binding.Clone()
	*binding.EvidenceExpiresAt = binding.EvidenceExpiresAt.Add(time.Hour)
	if clone.EvidenceExpiresAt.Equal(*binding.EvidenceExpiresAt) {
		t.Fatal("clone retained optional evidence-time alias")
	}
}
