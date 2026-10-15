package attachedworkerdaemon_test

import (
	"context"
	"strings"
	"testing"
	"time"

	daemon "gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
)

type apiReceiverClock struct{ now time.Time }

func (c apiReceiverClock) Now() time.Time { return c.now }

type apiReceiverIDs struct{}

func (apiReceiverIDs) NewID(context.Context, ports.IDKind) (string, error) {
	return "synthetic-real-receiver-handle", nil
}

type apiReceiverBindings struct {
	ports.ProviderCredentialBindingStore
	binding domain.ProviderCredentialBindingV1
}

func (b apiReceiverBindings) LoadProviderCredential(context.Context, ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
	return b.binding, true, nil
}

type apiReceiverSecrets struct {
	ports.ProviderCredentialSecretStore
}

func (apiReceiverSecrets) RecoverProviderCredentialCandidate(context.Context, domain.ProviderCredentialBindingV1) (bool, error) {
	return true, nil
}

type apiReceiverPlanner struct{}

func (apiReceiverPlanner) PlanProviderCredentialDelivery(context.Context, domain.HarnessBindingV2) (ports.ProviderCredentialDeliveryTemplateV1, error) {
	return ports.ProviderCredentialDeliveryTemplateV1{Kind: domain.ProviderCredentialDeliveryDirectV1}, nil
}

func TestAPIOwnerGenuineIssueCanonicalExpiry(t *testing.T) {
	// Genuine original Issue and Release, isolated direct delivery: no file
	// descriptor, secret read, external store/network or production Pi activation.
	now := time.Unix(100, 0).UTC()
	evidence := now.Add(time.Hour)
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceRouterAccountV1, ResourceID: "synthetic-router", OwnerUserID: "owner", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	binding := domain.HarnessBindingV2{Version: domain.HarnessBindingVersionV2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: "tenant", OwnerUserID: "owner", RunID: "run", AttemptID: "attempt", Resource: resource,
		Backend:       domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendDirectOpenRouterV1, ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "openrouter-http.v1", BackendProfileDigest: strings.Repeat("2", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryDirectV1},
		ModelVendorID: "synthetic", ModelID: "synthetic", InputDataClass: domain.ProviderDataPublicV1, ProviderCatalogDigest: strings.Repeat("3", 64), ProviderRouteDigest: strings.Repeat("4", 64), PrivacyPolicyDigest: strings.Repeat("5", 64), CapabilityEvidenceDigest: strings.Repeat("6", 64), EffectivePolicyDigest: strings.Repeat("7", 64), ExecutionPlacementDigest: strings.Repeat("8", 64), EvidenceExpiresAt: &evidence}
	ref, err := domain.NewCredentialSecretRef("lockbox/synthetic/receiver")
	if err != nil {
		t.Fatal(err)
	}
	stored := domain.ProviderCredentialBindingV1{Version: domain.ProviderCredentialBindingVersionV1, TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, ResourceKind: resource.Kind, ResourceID: resource.ResourceID, ResourceRevision: resource.Revision, CredentialGeneration: resource.CredentialGeneration, CandidateMutationID: "synthetic-mutation", State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte("public-synthetic-marker")), UpdatedAt: now}
	service, err := providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: apiReceiverClock{now}, IDs: apiReceiverIDs{}, MaxSecretBytes: 1024}, apiReceiverPlanner{}, apiReceiverBindings{binding: stored}, apiReceiverSecrets{})
	if err != nil {
		t.Fatal(err)
	}
	request := ports.ProviderCredentialIssueRequestV1{HarnessBinding: binding, WorkerID: "worker", LeaseID: "lease", LeaseFence: 1, ExpiresAt: now.Add(30*time.Minute + time.Nanosecond).In(time.FixedZone("synthetic", 3600))}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	handle, err := service.IssueProviderCredential(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.ReleaseProviderCredential(ctx, handle); err != nil {
			t.Error(err)
		}
	})
	if !daemon.APIHandleMatchesForTest(handle, request) {
		t.Error("genuine canonical receiver handle rejected")
	}
	for _, mutate := range []func(*ports.ProviderInvocationCredentialV1){func(h *ports.ProviderInvocationCredentialV1) { h.ExpiresAt = h.ExpiresAt.Add(time.Microsecond) }, func(h *ports.ProviderInvocationCredentialV1) { h.LeaseFence++ }, func(h *ports.ProviderInvocationCredentialV1) { h.ProviderResource.CredentialGeneration++ }} {
		foreign := handle
		mutate(&foreign)
		if daemon.APIHandleMatchesForTest(foreign, request) {
			t.Error("foreign canonical handle accepted")
		}
	}
}
