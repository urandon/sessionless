package attachedworkerdaemontransport

import (
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// NewNativeSubscriptionBoundMaterializerV1 explicitly selects the ordinary
// Codex subscription/file metadata path. All original content, accepted-head,
// credential identity, fence, generation and time checks remain mandatory.
// No fixture-provider flag, credential Issue effect or process is enabled.
func NewNativeSubscriptionBoundMaterializerV1(source SealedInputSource, maxBytes int, now func() time.Time) (*BoundMaterializer, error) {
	materializer, err := NewBoundMaterializer(source, maxBytes, now)
	if err != nil {
		return nil, err
	}
	materializer.nativeSubscription = true
	return materializer, nil
}

func (materializer *BoundMaterializer) supportsProvider(job domain.WorkerJob) bool {
	if materializer.nativeSubscription {
		return !materializer.allowTestProvider &&
			job.HarnessBinding.Backend.BackendKind == domain.HarnessBackendCodexExecV1 &&
			job.HarnessBinding.Backend.ProviderContractKind == domain.ProviderContractInvocationV1 &&
			job.HarnessBinding.Backend.CredentialDeliveryKind == domain.ProviderCredentialDeliveryFileV1 &&
			job.HarnessBinding.Resource.Kind == domain.ProviderResourceSubscriptionV1
	}
	provider := job.HarnessBinding.Backend.ProviderContractKind == domain.ProviderContractInvocationV1
	return provider && materializer.allowTestProvider ||
		!provider && job.HarnessBinding.Backend.ProviderContractKind == domain.ProviderContractCredentiallessFixtureV1
}
