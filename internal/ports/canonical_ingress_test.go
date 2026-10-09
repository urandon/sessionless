package ports_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func TestExecutionAuthorityUnionPreservesManagedAndRejectsMixedAttached(t *testing.T) {
	t.Parallel()
	request := ports.HarnessBindingRequest{TenantID: "tenant-a", OwnerUserID: "owner-a", RunID: "run-a", AttemptID: "attempt-a", SubscriptionConnectionID: "resource-a", At: time.Unix(100, 0).UTC()}
	managed, err := sessionlessharness.NewDeterministicFixtureBinderV1().BindHarness(context.Background(), request)
	if err != nil || managed.ValidateForScope(request) != nil {
		t.Fatalf("managed union=%+v err=%v", managed, err)
	}
	attached := managed.Clone()
	attached.SubstrateBinding, attached.AdmissionCostCeiling = nil, nil
	attached.ExecutionPlacementV2 = domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: request.OwnerUserID, WorkerID: "worker-a", CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("a", 64)),
		PolicyDigest: domain.AttachedWorkerPolicyDigest(attached.HarnessBinding.EffectivePolicyDigest)}
	digest, err := domain.ExecutionPlacementDigest(attached.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	attached.HarnessBinding.ExecutionPlacementDigest = string(digest)
	attached.HarnessBinding.Backend.BackendKind = domain.HarnessBackendCodexExecV1
	attached.HarnessBinding.Backend.ArtifactKind = domain.HarnessArtifactExecutableV1
	attached.HarnessBinding.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
	attached.HarnessBinding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
	attached.HarnessBinding.Resource = domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: string(request.SubscriptionConnectionID), OwnerUserID: request.OwnerUserID,
		Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	expiry := request.At.Add(time.Hour)
	attached.HarnessBinding.EvidenceExpiresAt = &expiry
	if err := attached.ValidateForScope(request); err != nil {
		t.Fatalf("attached union: %v", err)
	}
	for _, testCase := range []struct {
		name   string
		mutate func(*ports.ExecutionAuthorityV2)
	}{
		{name: "substrate", mutate: func(authority *ports.ExecutionAuthorityV2) { authority.SubstrateBinding = managed.SubstrateBinding }},
		{name: "cost", mutate: func(authority *ports.ExecutionAuthorityV2) {
			authority.AdmissionCostCeiling = managed.AdmissionCostCeiling
		}},
		{name: "cross owner", mutate: func(authority *ports.ExecutionAuthorityV2) { authority.ExecutionPlacementV2.OwnerUserID = "owner-b" }},
		{name: "wrong resource", mutate: func(authority *ports.ExecutionAuthorityV2) {
			authority.HarnessBinding.Resource.ResourceID = "resource-b"
		}},
		{name: "wrong policy", mutate: func(authority *ports.ExecutionAuthorityV2) {
			authority.HarnessBinding.EffectivePolicyDigest = strings.Repeat("b", 64)
		}},
		{name: "fallback", mutate: func(authority *ports.ExecutionAuthorityV2) { authority.ExecutionPlacementV2.FallbackPolicy = "allow" }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			mutated := attached.Clone()
			testCase.mutate(&mutated)
			if err := mutated.ValidateForScope(request); err == nil {
				t.Fatalf("mixed authority accepted: %+v", mutated)
			}
		})
	}
	managed.SubstrateBinding = nil
	if err := managed.ValidateForScope(request); err == nil {
		t.Fatal("managed union without substrate accepted")
	}
}
