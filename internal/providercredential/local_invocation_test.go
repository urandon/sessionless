package providercredential

import (
	"context"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestConfiguredLocalBearerReusesOriginalCredentialLifecycle(t *testing.T) {
	for _, size := range []int{32, 4096, 4097} {
		t.Run(map[int]string{32: "small", 4096: "boundary", 4097: "oversize"}[size], func(t *testing.T) {
			service, bindings, secrets, request, _ := invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
			var current domain.ProviderCredentialBindingV1
			for key, value := range bindings.bindings {
				current = value
				delete(bindings.bindings, key)
			}
			current.ResourceKind, current.ResourceID = domain.ProviderResourceLocalEndpointV1, "synthetic-local-resource"
			marker := strings.Repeat("x", size)
			current.SecretFingerprint = domain.FingerprintCredential([]byte(marker))
			bindings.bindings[locatorFor(current)] = current
			secrets.values[current.SecretRef] = []byte(marker)
			resource, err := current.ResourceBinding()
			if err != nil {
				t.Fatal(err)
			}
			b := &request.HarnessBinding
			b.Resource = resource
			b.Backend.BackendKind = domain.HarnessBackendDirectOpenAICompatibleV1
			b.Backend.ArtifactKind = domain.HarnessArtifactEmbeddedProfileV1
			b.Backend.ProviderContractKind = domain.ProviderContractConfiguredEndpointV1
			b.Backend.NativeProtocolVersion = domain.ConfiguredInferenceProtocolChatCompletionsV1
			handle, err := service.IssueProviderCredential(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			callbacks := 0
			err = service.MaterializeProviderCredential(t.Context(), handle, func(plan ports.ProviderCredentialMaterializationV1, bytes []byte) error {
				callbacks++
				if plan.Kind != domain.ProviderCredentialDeliveryDirectV1 || len(bytes) != size {
					t.Fatalf("local direct projection kind=%s bytes=%d", plan.Kind, len(bytes))
				}
				return nil
			})
			want := 1
			if size > 4096 {
				want = 0
			}
			if (err == nil) != (size <= 4096) || callbacks != want || secrets.readCalls != 1 {
				t.Fatalf("size=%d err=%v callbacks=%d reads=%d", size, err, callbacks, secrets.readCalls)
			}
			if err := service.ReleaseProviderCredential(context.Background(), handle); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConfiguredLocalNoAuthCannotIssueCredential(t *testing.T) {
	service, bindings, secrets, request, _ := invocationFixture(t, domain.ProviderCredentialDeliveryDirectV1)
	b := &request.HarnessBinding
	b.Backend.BackendKind = domain.HarnessBackendDirectOpenAICompatibleV1
	b.Backend.ArtifactKind = domain.HarnessArtifactEmbeddedProfileV1
	b.Backend.ProviderContractKind = domain.ProviderContractConfiguredEndpointV1
	b.Backend.NativeProtocolVersion = domain.ConfiguredInferenceProtocolChatCompletionsV1
	b.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryNoneV1
	b.Resource.Kind, b.Resource.CredentialMode, b.Resource.CredentialGeneration = domain.ProviderResourceLocalEndpointV1, domain.ProviderCredentialNoneV1, 0
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := service.IssueProviderCredential(t.Context(), request); err == nil {
		t.Fatal("no-auth issued a credential handle")
	}
	if bindings.loadCalls != 0 || secrets.readCalls != 0 || len(service.handles) != 0 {
		t.Fatalf("no-auth custody calls=%d reads=%d handles=%d", bindings.loadCalls, secrets.readCalls, len(service.handles))
	}
}
